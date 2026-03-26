"""
PostgreSQL persistence layer for job listings.

Uses a single table with an upsert strategy:
  - New listings are inserted with notified=False.
  - Existing listings have last_seen refreshed; score is updated only if higher.
  - After emailing a digest, listings are marked notified=True so they are
    never re-sent, even if they reappear in tomorrow's scrape.
  - Listings older than keep_days are purged to stay within ~1 year of history.

Requires DATABASE_URL in the environment (standard libpq connection string or
postgres:// URI, e.g. postgresql://user:pass@host:5432/dbname).
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta

import psycopg2
import psycopg2.extras

from ..scrapers.base import JobListing

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS job_listings (
    id               VARCHAR(12)  PRIMARY KEY,
    title            TEXT         NOT NULL,
    company          TEXT         NOT NULL,
    location         TEXT         NOT NULL,
    url              TEXT         NOT NULL,
    description      TEXT         NOT NULL DEFAULT '',
    salary           TEXT         NOT NULL DEFAULT '',
    date_posted      TEXT         NOT NULL DEFAULT '',
    source           TEXT         NOT NULL DEFAULT '',
    relevance_score  INTEGER      NOT NULL DEFAULT 0,
    relevance_reason TEXT         NOT NULL DEFAULT '',
    first_seen       DATE         NOT NULL DEFAULT CURRENT_DATE,
    last_seen        DATE         NOT NULL DEFAULT CURRENT_DATE,
    notified         BOOLEAN      NOT NULL DEFAULT FALSE
);
"""

# On conflict: refresh last_seen; upgrade score + reason if improved;
# update description/salary only when the new value is non-empty.
_UPSERT = """
INSERT INTO job_listings
    (id, title, company, location, url, description, salary, date_posted,
     source, relevance_score, relevance_reason, first_seen, last_seen, notified)
VALUES
    (%(id)s, %(title)s, %(company)s, %(location)s, %(url)s,
     %(description)s, %(salary)s, %(date_posted)s, %(source)s,
     %(relevance_score)s, %(relevance_reason)s,
     CURRENT_DATE, CURRENT_DATE, FALSE)
ON CONFLICT (id) DO UPDATE SET
    last_seen        = CURRENT_DATE,
    relevance_score  = GREATEST(EXCLUDED.relevance_score, job_listings.relevance_score),
    relevance_reason = CASE
        WHEN EXCLUDED.relevance_score >= job_listings.relevance_score
             THEN EXCLUDED.relevance_reason
        ELSE job_listings.relevance_reason
    END,
    description      = CASE
        WHEN EXCLUDED.description != '' THEN EXCLUDED.description
        ELSE job_listings.description
    END,
    salary           = CASE
        WHEN EXCLUDED.salary != '' THEN EXCLUDED.salary
        ELSE job_listings.salary
    END
;
"""

_UNNOTIFIED = """
SELECT id, title, company, location, url, description, salary,
       date_posted, source, relevance_score, relevance_reason
FROM   job_listings
WHERE  notified = FALSE
  AND  relevance_score >= %(min_score)s
ORDER BY relevance_score DESC, first_seen DESC
;
"""

_MARK_NOTIFIED = """
UPDATE job_listings SET notified = TRUE WHERE id = ANY(%(ids)s);
"""

_EXPORT_RECENT = """
SELECT id, title, company, location, url, salary, date_posted, source,
       relevance_score, relevance_reason,
       first_seen::text AS first_seen,
       last_seen::text  AS last_seen
FROM   job_listings
WHERE  last_seen  >= %(cutoff)s
  AND  relevance_score >= %(min_score)s
ORDER BY relevance_score DESC, first_seen DESC
;
"""

_DELETE_OLD = """
DELETE FROM job_listings WHERE last_seen < %(cutoff)s;
"""


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class JobRepository:
    """
    Thin data-access layer around the job_listings table.

    All methods are synchronous; each call opens/closes its own connection
    (acceptable for a once-daily batch script — no connection pool needed).
    """

    def __init__(self, database_url: str | None = None) -> None:
        raw = database_url or os.environ["DATABASE_URL"]
        # psycopg2 requires postgresql:// — Supabase gives postgres://
        if raw.startswith("postgres://"):
            raw = "postgresql://" + raw[len("postgres://"):]
        self._dsn = raw
        self._ensure_table()
        logger.info("JobRepository connected to database.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upsert_many(self, listings: list[JobListing]) -> int:
        """Insert or update a batch of listings. Returns the count written."""
        if not listings:
            return 0
        rows = [_listing_to_row(l) for l in listings]
        with self._conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, _UPSERT, rows, page_size=100)
            conn.commit()
        logger.info("Upserted %d listing(s) to database.", len(rows))
        return len(rows)

    def get_unnotified(self, min_score: int) -> list[JobListing]:
        """Return listings not yet emailed that meet the relevance threshold."""
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_UNNOTIFIED, {"min_score": min_score})
                rows = cur.fetchall()
        listings = [_row_to_listing(r) for r in rows]
        logger.info(
            "Found %d unnotified listing(s) with score >= %d.", len(listings), min_score
        )
        return listings

    def mark_notified(self, listing_ids: list[str]) -> None:
        """Mark a list of listing IDs as notified so they are never re-emailed."""
        if not listing_ids:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_MARK_NOTIFIED, {"ids": listing_ids})
            conn.commit()
        logger.info("Marked %d listing(s) as notified.", len(listing_ids))

    def get_scored_ids(self, ids: list[str]) -> set[str]:
        """Return which of the given listing IDs are already scored in the DB."""
        if not ids:
            return set()
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM job_listings WHERE id = ANY(%s) AND relevance_score > 0",
                    (ids,),
                )
                return {row[0] for row in cur.fetchall()}

    def export_recent(self, *, days: int = 90, min_score: int = 4) -> list[dict]:
        """
        Return dicts suitable for JSON export.

        Uses a lower min_score than the email threshold so the Pages board
        shows a broader range of listings (users can filter there).
        """
        cutoff = date.today() - timedelta(days=days)
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_EXPORT_RECENT, {"cutoff": cutoff, "min_score": min_score})
                return [dict(r) for r in cur.fetchall()]

    def delete_old(self, *, keep_days: int = 365) -> int:
        """Purge listings last seen more than keep_days ago."""
        cutoff = date.today() - timedelta(days=keep_days)
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_DELETE_OLD, {"cutoff": cutoff})
                deleted = cur.rowcount
            conn.commit()
        if deleted:
            logger.info("Purged %d listing(s) older than %d days.", deleted, keep_days)
        return deleted

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _conn(self):
        return psycopg2.connect(self._dsn)

    def _ensure_table(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE)
            conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _listing_to_row(listing: JobListing) -> dict:
    return {
        "id": listing.id,
        "title": listing.title,
        "company": listing.company,
        "location": listing.location,
        "url": listing.url,
        "description": listing.description,
        "salary": listing.salary,
        "date_posted": listing.date_posted,
        "source": listing.source,
        "relevance_score": listing.relevance_score,
        "relevance_reason": listing.relevance_reason,
    }


def _row_to_listing(row: dict) -> JobListing:
    return JobListing(
        title=row["title"],
        company=row["company"],
        location=row["location"],
        url=row["url"],
        description=row.get("description", ""),
        salary=row.get("salary", ""),
        date_posted=row.get("date_posted", ""),
        source=row.get("source", ""),
        relevance_score=row["relevance_score"],
        relevance_reason=row.get("relevance_reason", ""),
    )
