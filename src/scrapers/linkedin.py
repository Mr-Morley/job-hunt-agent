"""
LinkedIn public job search scraper.

Uses LinkedIn's guest jobs API endpoint — the same one their own frontend
calls, publicly accessible with no login or API key.

Endpoint:  GET /jobs-guest/jobs/api/seeMoreJobPostings/search
           ?keywords=<title>&location=<location>&start=<offset>&sortBy=DD

Returns HTML <li> fragments, 10 per page.  We paginate up to max_results
and fetch each job's detail page for the full description.
"""
from __future__ import annotations

import logging
import re
import time
import urllib.parse
from typing import Optional

import httpx
from bs4 import BeautifulSoup, Tag

from .base import BaseScraper, JobListing

logger = logging.getLogger(__name__)

_SEARCH_API = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
_DETAIL_URL = "https://www.linkedin.com/jobs/view/{job_id}/"
_PAGE_SIZE = 10  # LinkedIn returns exactly 10 per call

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


class LinkedInScraper(BaseScraper):
    """
    Scrapes LinkedIn's public (unauthenticated) job search results.

    For each search query:
      1. Pages through the guest search API to collect basic cards.
      2. Fetches each job's detail page for the full description,
         seniority level, employment type, and listed skills.

    Set fetch_details=False to skip detail fetches (faster but less data).
    """

    name = "LinkedIn"

    def __init__(
        self,
        *,
        timeout: int = 15,
        max_retries: int = 2,
        fetch_details: bool = True,
        detail_delay: float = 0.5,
    ) -> None:
        super().__init__(timeout=timeout, max_retries=max_retries)
        self.fetch_details = fetch_details
        self.detail_delay = detail_delay  # polite pause between detail fetches

    def scrape(
        self,
        job_title: str,
        location: str,
        *,
        max_results: int = 25,
    ) -> list[JobListing]:
        listings: list[JobListing] = []
        seen_ids: set[str] = set()
        start = 0

        with httpx.Client(
            headers=_HEADERS, follow_redirects=True, timeout=self.timeout
        ) as client:
            while len(listings) < max_results:
                page = self._fetch_search_page(client, job_title, location, start)
                if not page:
                    break

                cards = BeautifulSoup(page, "html.parser").select("li")
                if not cards:
                    break

                new_on_page = 0
                for card in cards:
                    if len(listings) >= max_results:
                        break
                    job_id = self._extract_job_id(card)
                    if not job_id or job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    listing = self._parse_card(card, location, job_id)
                    if not listing:
                        continue

                    if self.fetch_details:
                        time.sleep(self.detail_delay)
                        self._enrich_from_detail(client, listing, job_id)

                    listings.append(listing)
                    new_on_page += 1

                logger.info(
                    "LinkedIn page start=%d: %d new listings (total %d)",
                    start, new_on_page, len(listings),
                )

                if new_on_page == 0:
                    break  # No new results — stop paginating

                start += _PAGE_SIZE

        logger.info(
            "LinkedIn: %d listings for %r in %r", len(listings), job_title, location
        )
        return listings

    # ------------------------------------------------------------------
    # Search API
    # ------------------------------------------------------------------

    def _fetch_search_page(
        self, client: httpx.Client, job_title: str, location: str, start: int
    ) -> Optional[str]:
        params = {
            "keywords": job_title,
            "location": location,
            "start": start,
            "sortBy": "DD",  # Most recent first
        }
        url = f"{_SEARCH_API}?{urllib.parse.urlencode(params)}"

        for attempt in range(1, self.max_retries + 2):
            try:
                resp = client.get(url)
                resp.raise_for_status()
                return resp.text
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                logger.warning(
                    "LinkedIn search HTTP %s (start=%d, attempt %d): %s",
                    code, start, attempt, url,
                )
                if code in (429, 999) and attempt <= self.max_retries:
                    time.sleep(3 ** attempt)
                else:
                    return None
            except httpx.RequestError as exc:
                logger.warning("LinkedIn search request error: %s", exc)
                if attempt <= self.max_retries:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    # ------------------------------------------------------------------
    # Card parsing (search result)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_job_id(card: Tag) -> Optional[str]:
        urn_el = card.select_one("[data-entity-urn]")
        if urn_el:
            urn = str(urn_el.get("data-entity-urn", ""))
            if urn:
                return urn.split(":")[-1]
        # Fallback: job ID in the href
        link = card.select_one("a[href*='/jobs/view/']")
        if link:
            m = re.search(r"/jobs/view/(\d+)", str(link.get("href", "")))
            if m:
                return m.group(1)
        return None

    def _parse_card(
        self, card: Tag, default_location: str, job_id: str
    ) -> Optional[JobListing]:
        # Title
        title_el = card.select_one("h3.base-search-card__title")
        if not title_el:
            return None
        title = self._clean(title_el.get_text())
        if not title:
            return None

        # Company
        company_el = card.select_one("h4.base-search-card__subtitle")
        company = self._clean(company_el.get_text()) if company_el else "Unknown"

        # Location
        loc_el = card.select_one("span.job-search-card__location")
        location = self._clean(loc_el.get_text()) if loc_el else default_location

        # Date posted (datetime attr is machine-readable)
        time_el = card.select_one("time")
        date_posted = ""
        if time_el:
            date_posted = str(time_el.get("datetime", self._clean(time_el.get_text())))

        # Badge (e.g. "Actively Hiring", "Easy Apply")
        badge_el = card.select_one("span.job-posting-benefits__text")
        badge = self._clean(badge_el.get_text()) if badge_el else ""

        url = _DETAIL_URL.format(job_id=job_id)

        return JobListing(
            title=title,
            company=company,
            location=location,
            url=url,
            date_posted=date_posted,
            description=badge,  # will be replaced by full description in enrich step
            source=self.name,
        )

    # ------------------------------------------------------------------
    # Detail page enrichment
    # ------------------------------------------------------------------

    def _enrich_from_detail(
        self, client: httpx.Client, listing: JobListing, job_id: str
    ) -> None:
        """Fetch the job detail page and fill in description + metadata."""
        url = _DETAIL_URL.format(job_id=job_id)
        for attempt in range(1, self.max_retries + 2):
            try:
                resp = client.get(url)
                resp.raise_for_status()
                self._parse_detail(resp.text, listing)
                return
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                logger.debug(
                    "LinkedIn detail HTTP %s for job %s (attempt %d)",
                    code, job_id, attempt,
                )
                if code in (429, 999) and attempt <= self.max_retries:
                    time.sleep(3 ** attempt)
                else:
                    return
            except httpx.RequestError as exc:
                logger.debug("LinkedIn detail request error for %s: %s", job_id, exc)
                if attempt <= self.max_retries:
                    time.sleep(2)
                else:
                    return

    def _parse_detail(self, html: str, listing: JobListing) -> None:
        soup = BeautifulSoup(html, "html.parser")

        # Full description
        desc_el = (
            soup.select_one("div.show-more-less-html__markup")
            or soup.select_one("div.description__text")
            or soup.select_one("section.description div")
        )
        if desc_el:
            listing.description = self._clean(desc_el.get_text(" "))[:2000]

        # Salary (sometimes shown in the criteria list)
        salary_el = soup.select_one(
            "span.compensation__salary, div.compensation__salary-range"
        )
        if salary_el:
            listing.salary = self._clean(salary_el.get_text())

        # Job criteria items: Seniority level, Employment type, Industries, etc.
        criteria: dict[str, str] = {}
        for item in soup.select("li.description__job-criteria-item"):
            header = item.select_one("h3")
            value = item.select_one("span")
            if header and value:
                key = self._clean(header.get_text()).lower()
                criteria[key] = self._clean(value.get_text())

        if criteria:
            # Append structured metadata to the description so the classifier
            # has the full picture
            meta_lines = [f"{k.title()}: {v}" for k, v in criteria.items()]
            listing.description = (
                listing.description
                + ("\n\n" if listing.description else "")
                + "\n".join(meta_lines)
            )
