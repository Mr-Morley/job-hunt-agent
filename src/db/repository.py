"""
PostgreSQL persistence layer for job listings.

Deduplication key: SHA-1 of the normalised URL (query-params stripped).
The same job posted on LinkedIn and Careers24 → same id → one row.

On conflict the upsert:
  - Refreshes last_seen so "still live" jobs stay current.
  - Upgrades relevance_score only if the new score is higher.
  - Updates description/salary only when the incoming value is non-empty.

Listings not seen for keep_days (default 365) are purged automatically.
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
    city             TEXT         DEFAULT NULL,
    country          TEXT         DEFAULT NULL,
    location         TEXT         NOT NULL,          -- "City, Country" display string
    url              TEXT         NOT NULL UNIQUE,   -- belt-and-braces dedup
    description      TEXT         NOT NULL DEFAULT '',
    salary           TEXT         DEFAULT NULL,          -- NULL when not advertised
    date_posted      TEXT         NOT NULL DEFAULT '',
    source           TEXT         NOT NULL DEFAULT '',
    relevance_score  INTEGER      NOT NULL DEFAULT 0,
    relevance_reason TEXT         NOT NULL DEFAULT '',
    first_seen       DATE         NOT NULL DEFAULT CURRENT_DATE,
    last_seen        DATE         NOT NULL DEFAULT CURRENT_DATE
);
"""

# Idempotent migration: add UNIQUE on url if an older table lacks it.
_ADD_URL_UNIQUE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'job_listings_url_key'
    ) THEN
        ALTER TABLE job_listings ADD CONSTRAINT job_listings_url_key UNIQUE (url);
    END IF;
END$$;
"""

# Drop the old notified column if it exists (clean up legacy schema).
_DROP_NOTIFIED = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'job_listings' AND column_name = 'notified'
    ) THEN
        ALTER TABLE job_listings DROP COLUMN notified;
    END IF;
END$$;
"""

# Allow salary to be NULL (old schema had NOT NULL DEFAULT '').
_NULLABLE_SALARY = """
DO $$
BEGIN
    ALTER TABLE job_listings ALTER COLUMN salary DROP NOT NULL;
    UPDATE job_listings SET salary = NULL WHERE salary = '';
EXCEPTION WHEN OTHERS THEN NULL;  -- already nullable, ignore
END$$;
"""

_UPSERT = """
INSERT INTO job_listings
    (id, title, company, location, url, description, salary, date_posted,
     source, relevance_score, relevance_reason, first_seen, last_seen)
VALUES
    (%(id)s, %(title)s, %(company)s, %(location)s, %(url)s,
     %(description)s, %(salary)s, %(date_posted)s, %(source)s,
     %(relevance_score)s, %(relevance_reason)s,
     CURRENT_DATE, CURRENT_DATE)
ON CONFLICT (id) DO UPDATE SET
    last_seen        = CURRENT_DATE,
    -- Keep richer location string (longer = more specific)
    location         = CASE
        WHEN length(EXCLUDED.location) > length(job_listings.location)
             THEN EXCLUDED.location
        ELSE job_listings.location
    END,
    relevance_score  = GREATEST(EXCLUDED.relevance_score, job_listings.relevance_score),
    relevance_reason = CASE
        WHEN EXCLUDED.relevance_score >= job_listings.relevance_score
             THEN EXCLUDED.relevance_reason
        ELSE job_listings.relevance_reason
    END,
    -- Prefer longer (richer) description; fall back to existing if new is empty
    description      = CASE
        WHEN length(EXCLUDED.description) > length(job_listings.description)
             THEN EXCLUDED.description
        ELSE job_listings.description
    END,
    salary           = CASE
        WHEN EXCLUDED.salary != '' THEN EXCLUDED.salary
        ELSE job_listings.salary
    END,
    date_posted      = CASE
        WHEN EXCLUDED.date_posted != '' THEN EXCLUDED.date_posted
        ELSE job_listings.date_posted
    END
;
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
                cur.execute(_ADD_URL_UNIQUE)
                cur.execute(_DROP_NOTIFIED)
                cur.execute(_NULLABLE_SALARY)
            conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _listing_to_row(listing: JobListing) -> dict:
    return {
        "id": listing.id,
        "title": listing.title,
        "company": listing.company,
        "location": listing.normalised_location,  # strip sub-region noise
        "url": listing.url,
        "description": listing.description,
        "salary": listing.salary or None,      # store NULL, not empty string
        "date_posted": listing.date_posted or None,
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
