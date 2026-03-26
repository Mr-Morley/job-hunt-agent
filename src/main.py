"""
Orchestrator — scrape → classify → persist → export.

Usage:
    python -m src.main [--dry-run]

  --dry-run : scrape and classify, print top results, do not write to DB or export JSON.

Pipeline (normal run):
  1. Scrape LinkedIn + Careers24 + Google Jobs for every (title, location) pair.
  2. Query the DB for already-scored listing IDs — skip re-classifying those.
  3. Classify new listings with claude-haiku (0-10 relevance score).
  4. Upsert ALL listings to PostgreSQL (updates last_seen; keeps best score).
  5. Purge listings not seen in >365 days.
  6. Export the last 90 days of listings (score ≥ 4) to docs/data/jobs.json.
     GitHub Pages auto-deploys that file on every push — no manual step needed.

DATABASE_URL must be set in the environment (postgres:// or postgresql:// URI).
If absent the pipeline still runs but skips DB persistence and JSON export.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date

from dotenv import load_dotenv

from .agent.classifier import JobClassifier
from .config import SEARCH_CONFIG
from .scrapers import (
    Careers24Scraper,
    GoogleJobsScraper,
    JobListing,
    LinkedInScraper,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def run_scrapers() -> list[JobListing]:
    scrapers = [
        LinkedInScraper(),
        Careers24Scraper(),
        GoogleJobsScraper(),
    ]
    all_listings: list[JobListing] = []
    seen_ids: set[str] = set()

    for title in SEARCH_CONFIG.job_titles:
        for location in SEARCH_CONFIG.locations:
            for scraper in scrapers:
                logger.info("Scraping [%s] '%s' in '%s'…", scraper.name, title, location)
                try:
                    listings = scraper.scrape(title, location, max_results=SEARCH_CONFIG.results_per_query)
                except Exception as exc:
                    logger.error("Scraper %s failed: %s", scraper.name, exc)
                    listings = []

                new = 0
                for listing in listings:
                    if listing.id not in seen_ids:
                        seen_ids.add(listing.id)
                        all_listings.append(listing)
                        new += 1
                logger.info("  → %d new (total: %d)", new, len(all_listings))

    return all_listings


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify(listings: list[JobListing], skip_ids: set[str] | None = None) -> list[JobListing]:
    """
    Score listings. IDs in skip_ids are returned as-is (score=0); the DB
    upsert uses GREATEST so the stored score is preserved for those rows.
    This avoids paying to re-classify listings already in the DB.
    """
    if not listings:
        return []

    if skip_ids:
        to_score    = [l for l in listings if l.id not in skip_ids]
        already_had = [l for l in listings if l.id in skip_ids]
        logger.info("Skipping %d known listings; classifying %d new.", len(already_had), len(to_score))
    else:
        to_score    = listings
        already_had = []

    if to_score:
        classifier = JobClassifier()
        scored = classifier.score_many(to_score, SEARCH_CONFIG)
        above = sum(1 for j in scored if j.relevance_score >= SEARCH_CONFIG.min_score)
        logger.info("%d/%d new listing(s) at or above threshold (min %d).", above, len(scored), SEARCH_CONFIG.min_score)
    else:
        scored = []
        logger.info("No new listings to classify.")

    return scored + already_had


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _get_repo():
    if not os.getenv("DATABASE_URL"):
        logger.warning("DATABASE_URL not set — running without persistence.")
        return None
    try:
        from .db import JobRepository
        return JobRepository()
    except Exception as exc:
        logger.warning("DB connection failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Job Hunt Agent")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape and classify only; skip DB and JSON export")
    args = parser.parse_args(argv)

    logger.info("=== Job Hunt Agent — %s ===", date.today().isoformat())

    # 1. Scrape
    raw = run_scrapers()
    if not raw:
        logger.warning("No listings scraped.")
        return 0

    # 2. Connect to DB and get known IDs (to skip re-classification)
    repo = _get_repo()
    skip_ids = repo.get_scored_ids([l.id for l in raw]) if repo else set()

    # 3. Classify new listings
    scored = classify(raw, skip_ids=skip_ids or None)

    if args.dry_run:
        logger.info("Dry-run — skipping DB write and export.")
        relevant = sorted(
            [j for j in scored if j.relevance_score >= SEARCH_CONFIG.min_score],
            key=lambda j: j.relevance_score, reverse=True,
        )
        for job in relevant:
            print(f"[{job.relevance_score:2d}] {job.title} — {job.company} ({job.source})")
            print(f"       {job.url}")
        return 0

    # 4. Persist — the board reads live from Supabase, no export step needed
    if repo:
        repo.upsert_many(scored)
        repo.delete_old(keep_days=365)

    return 0


if __name__ == "__main__":
    sys.exit(main())
