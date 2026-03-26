"""
Orchestrator — ties scrapers → classifier → DB → summarizer → notifier together.

Usage:
    python -m src.main [--dry-run] [--no-email]

  --dry-run   : scrape and classify but do not persist to DB, call the
                summarizer, or send email
  --no-email  : build the digest but print it to stdout instead of sending

Database (optional):
    Set DATABASE_URL in the environment to enable PostgreSQL persistence.
    When present the pipeline:
      - Upserts all scored listings into the DB on every run.
      - Builds the email digest from *unnotified* DB rows (deduplicated
        across days — you will never receive the same listing twice).
      - Marks emailed listings as notified.
      - Exports recent listings to docs/data/jobs.json for GitHub Pages.
      - Purges listings older than 365 days.

    When DATABASE_URL is absent the pipeline falls back to the original
    in-memory behaviour (useful for local dev without a database).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from .agent.classifier import JobClassifier
from .agent.summarizer import JobSummarizer
from .config import SEARCH_CONFIG
from .notifier.email_sender import EmailSender
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

_JSON_EXPORT_PATH = Path(__file__).parent.parent / "docs" / "data" / "jobs.json"


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def run_scrapers() -> list[JobListing]:
    """Run all scrapers for every (title, location) combination."""
    scrapers = [
        LinkedInScraper(),    # primary — full descriptions via detail pages
        Careers24Scraper(),   # SA supplement
        GoogleJobsScraper(),  # opportunistic
    ]

    all_listings: list[JobListing] = []
    seen_ids: set[str] = set()

    for title in SEARCH_CONFIG.job_titles:
        for location in SEARCH_CONFIG.locations:
            for scraper in scrapers:
                logger.info(
                    "Scraping [%s] for '%s' in '%s'…",
                    scraper.name,
                    title,
                    location,
                )
                try:
                    listings = scraper.scrape(
                        title,
                        location,
                        max_results=SEARCH_CONFIG.results_per_query,
                    )
                except Exception as exc:
                    logger.error(
                        "Scraper %s raised unexpectedly: %s", scraper.name, exc
                    )
                    listings = []

                new = 0
                for listing in listings:
                    if listing.id not in seen_ids:
                        seen_ids.add(listing.id)
                        all_listings.append(listing)
                        new += 1
                logger.info("  → %d new listing(s) (total: %d)", new, len(all_listings))

    return all_listings


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify(
    listings: list[JobListing], skip_ids: set[str] | None = None
) -> list[JobListing]:
    """
    Score listings via the classifier API.

    Listings whose IDs are in *skip_ids* are returned as-is (score stays 0;
    the DB upsert will use GREATEST to keep the existing stored score).
    This avoids re-paying for classification of listings seen on prior days.
    """
    if not listings:
        return []

    if skip_ids:
        to_score = [l for l in listings if l.id not in skip_ids]
        already_known = [l for l in listings if l.id in skip_ids]
        logger.info(
            "Skipping %d already-scored listing(s); classifying %d new ones.",
            len(already_known),
            len(to_score),
        )
    else:
        to_score = listings
        already_known = []

    if to_score:
        classifier = JobClassifier()
        scored_new = classifier.score_many(to_score, SEARCH_CONFIG)
        above = sum(1 for j in scored_new if j.relevance_score >= SEARCH_CONFIG.min_score)
        logger.info(
            "%d/%d new listing(s) at or above threshold (min score %d)",
            above, len(scored_new), SEARCH_CONFIG.min_score,
        )
    else:
        scored_new = []
        logger.info("No new listings to classify.")

    return scored_new + already_known


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_repo():
    """Return a JobRepository if DATABASE_URL is set, else None."""
    if not os.getenv("DATABASE_URL"):
        return None
    try:
        from .db import JobRepository
        return JobRepository()
    except Exception as exc:
        logger.warning("Could not connect to database: %s", exc)
        return None


def _export_json(repo) -> None:
    """Write recent listings from the DB to docs/data/jobs.json."""
    try:
        records = repo.export_recent(days=90, min_score=4)
        _JSON_EXPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        _JSON_EXPORT_PATH.write_text(json.dumps(records, indent=2, default=str))
        logger.info("Exported %d listing(s) to %s", len(records), _JSON_EXPORT_PATH)
    except Exception as exc:
        logger.warning("JSON export failed: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Job Hunt Agent")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape and classify only; skip DB, summariser and email",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Generate digest but print to stdout instead of sending",
    )
    args = parser.parse_args(argv)

    logger.info("=== Job Hunt Agent — %s ===", date.today().isoformat())

    # 1. Scrape
    raw_listings = run_scrapers()
    if not raw_listings:
        logger.warning("No listings scraped — exiting early.")
        return 0

    # 2. Classify — skip listings already scored in the DB
    repo = _get_repo()
    skip_ids: set[str] = set()
    if repo:
        skip_ids = repo.get_scored_ids([l.id for l in raw_listings])
    scored = classify(raw_listings, skip_ids=skip_ids or None)

    if args.dry_run:
        logger.info("Dry-run mode — stopping before DB and summariser.")
        relevant = [j for j in scored if j.relevance_score >= SEARCH_CONFIG.min_score]
        for job in sorted(relevant, key=lambda j: j.relevance_score, reverse=True):
            print(f"[{job.relevance_score:2d}] {job.title} — {job.company} ({job.source})")
            print(f"      {job.url}")
        return 0

    # 3. Persist to database (if configured)
    if repo:
        repo.upsert_many(scored)
        repo.delete_old(keep_days=365)

    # 4. Determine which listings to include in the digest
    _MAX_DIGEST = 12  # cap Opus input — top picks only, sorted by score
    if repo:
        # Only listings never sent before
        digest_listings = repo.get_unnotified(SEARCH_CONFIG.min_score)[:_MAX_DIGEST]
    else:
        # Fallback: in-memory filter (listings are re-emailed each run)
        digest_listings = sorted(
            [j for j in scored if j.relevance_score >= SEARCH_CONFIG.min_score],
            key=lambda j: j.relevance_score,
            reverse=True,
        )[:_MAX_DIGEST]

    if not digest_listings:
        logger.info("No new listings to include in the digest.")
        if repo:
            _export_json(repo)
        return 0

    # 5. Summarise
    logger.info("Generating digest for %d listing(s)…", len(digest_listings))
    summarizer = JobSummarizer(pages_url=os.getenv("PAGES_URL", ""))
    digest = summarizer.generate_digest(digest_listings)
    logger.info(
        "Digest ready: %d top picks, %d total.",
        digest.top_count,
        digest.total_count,
    )

    # 6. Notify
    _send_or_print(digest, args)

    # Mark listings as notified so they won't appear in future digests
    if repo and not args.no_email:
        repo.mark_notified([j.id for j in digest_listings])

    # 7. Export JSON for GitHub Pages
    if repo:
        _export_json(repo)

    return 0


def _send_or_print(digest, args: argparse.Namespace) -> None:
    if args.no_email:
        print("\n" + "=" * 60)
        print(digest.plain_body)
        print("=" * 60 + "\n")
    else:
        sender = EmailSender()
        sender.send_digest(digest)


if __name__ == "__main__":
    sys.exit(main())
