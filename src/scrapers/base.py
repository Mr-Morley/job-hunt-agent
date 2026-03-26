"""
Abstract base classes for all job scrapers.
"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


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
        """Stable hash so we can deduplicate across scraper runs."""
        key = f"{self.title}|{self.company}|{self.url}"
        return hashlib.sha1(key.encode()).hexdigest()[:12]

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
