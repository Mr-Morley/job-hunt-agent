"""
Abstract base classes for all job scrapers.
"""
from __future__ import annotations

import hashlib
import re
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class JobListing:
    """A normalised job listing returned by any scraper."""

    title: str
    company: str
    location: str
    url: str
    description: str = ""
    salary: str = ""
    date_posted: str = ""
    source: str = ""

    # Populated by the classifier
    relevance_score: int = 0
    relevance_reason: str = ""

    @property
    def id(self) -> str:
        """
        Stable deduplication key based on the normalised URL.

        Using the URL alone (not title+company) means the same job found by
        multiple scrapers on different days maps to a single DB row.
        Tracking query-string parameters are stripped so LinkedIn ?refId=…
        variants don't generate phantom duplicates.
        """
        return hashlib.sha1(self._normalise_url(self.url).encode()).hexdigest()[:12]

    @property
    def normalised_location(self) -> str:
        """Clean display string — always 'City, Country'."""
        loc = " ".join(self.location.split()).strip()
        parts = [p.strip() for p in loc.split(",")]
        if len(parts) >= 3:
            return f"{parts[0]}, {parts[-1]}"
        return loc

    @property
    def city(self) -> str | None:
        """First component of the normalised location, or None for remote/unknown."""
        parts = [p.strip() for p in self.normalised_location.split(",")]
        c = parts[0] if parts else ""
        return c if c and c.lower() not in ("remote", "hybrid", "various", "") else None

    @property
    def country(self) -> str | None:
        """Last component of the normalised location."""
        parts = [p.strip() for p in self.normalised_location.split(",")]
        return parts[-1] if len(parts) >= 2 else None

    @staticmethod
    def _normalise_url(url: str) -> str:
        """Strip tracking/session query params; keep path intact."""
        try:
            p = urllib.parse.urlparse(url)
            # Keep only query params that are part of the resource identity
            # (LinkedIn job IDs are in the path; Careers24 slugs are in the path)
            clean = urllib.parse.urlunparse(
                (p.scheme, p.netloc, p.path.rstrip("/"), "", "", "")
            )
            return clean.lower()
        except Exception:
            return url.lower()

    def __repr__(self) -> str:
        return (
            f"JobListing(title={self.title!r}, company={self.company!r}, "
            f"location={self.location!r}, source={self.source!r})"
        )


class BaseScraper(ABC):
    """
    Abstract base class for all job-board scrapers.

    Subclasses must implement `scrape()`.  They should NOT require any
    authentication — only public, unauthenticated search pages are supported.
    """

    #: Human-readable name used in logs and the digest
    name: str = "unknown"

    def __init__(self, *, timeout: int = 15, max_retries: int = 2) -> None:
        self.timeout = timeout
        self.max_retries = max_retries

    @abstractmethod
    def scrape(
        self,
        job_title: str,
        location: str,
        *,
        max_results: int = 10,
    ) -> list[JobListing]:
        """
        Scrape public search results for *job_title* in *location*.

        Returns a (possibly empty) list of JobListing objects.
        Implementations must never raise — log and return [] on failure.
        """

    # ------------------------------------------------------------------
    # Helpers subclasses can use
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(text: str) -> str:
        """Strip excess whitespace from scraped text."""
        return " ".join(text.split())
