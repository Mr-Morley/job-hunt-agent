"""
Careers24 scraper — South African job board (www.careers24.com).

URL pattern: /jobs/lc-{city-slug}/kw-{keyword-slug}/?sort=dateposted
No authentication required.
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

_BASE = "https://www.careers24.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-ZA,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_CITY_SLUGS: dict[str, str] = {
    "cape town": "cape-town",
    "johannesburg": "johannesburg",
    "stellenbosch": "stellenbosch",
    "pretoria": "pretoria",
    "durban": "durban",
    "sandton": "sandton",
}


def _city_slug(location: str) -> Optional[str]:
    lower = location.lower()
    for name, slug in _CITY_SLUGS.items():
        if name in lower:
            return slug
    return None


def _keyword_slug(title: str) -> str:
    """Convert a job title to a Careers24 keyword slug."""
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


class Careers24Scraper(BaseScraper):
    """Scrapes Careers24 public job listings (South Africa focused)."""

    name = "Careers24"

    def scrape(
        self,
        job_title: str,
        location: str,
        *,
        max_results: int = 10,
    ) -> list[JobListing]:
        if "south africa" not in location.lower():
            logger.debug("Careers24: skipping non-SA location: %s", location)
            return []

        city = _city_slug(location)
        kw = _keyword_slug(job_title)

        if city:
            url = f"{_BASE}/jobs/lc-{city}/kw-{kw}/?sort=dateposted"
        else:
            url = f"{_BASE}/jobs/kw-{kw}/?sort=dateposted"

        logger.info("Careers24 scraping: %s", url)
        html = self._fetch(url)
        if not html:
            return []

        return self._parse(html, location)[:max_results]

    def _fetch(self, url: str) -> Optional[str]:
        for attempt in range(1, self.max_retries + 2):
            try:
                with httpx.Client(
                    headers=_HEADERS, follow_redirects=True, timeout=self.timeout
                ) as client:
                    resp = client.get(url)
                    resp.raise_for_status()
                    return resp.text
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "Careers24 HTTP %s on attempt %d: %s",
                    exc.response.status_code,
                    attempt,
                    url,
                )
                if attempt <= self.max_retries:
                    time.sleep(2 ** attempt)
            except httpx.RequestError as exc:
                logger.warning("Careers24 request error: %s", exc)
                if attempt <= self.max_retries:
                    time.sleep(2 ** attempt)
        return None

    def _parse(self, html: str, default_location: str) -> list[JobListing]:
        soup = BeautifulSoup(html, "html.parser")
        listings: list[JobListing] = []

        cards: list[Tag] = soup.select("div.job-card")
        if not cards:
            logger.warning("Careers24: no job cards found — structure may have changed")
            return []

        for card in cards:
            listing = self._parse_card(card, default_location)
            if listing:
                listings.append(listing)

        logger.info("Careers24: parsed %d listings", len(listings))
        return listings

    def _parse_card(self, card: Tag, default_location: str) -> Optional[JobListing]:
        # Title + URL — the anchor wraps an h2
        link_el = card.select_one("div.job-card-head a[href]")
        if not link_el:
            return None

        href = str(link_el.get("href", ""))
        job_url = href if href.startswith("http") else _BASE + href.split("?")[0]

        title_el = link_el.find("h2") or link_el
        title = self._clean(title_el.get_text())
        if not title:
            return None

        # Location — first <li> inside the left meta column
        left_col = card.select_one("div.col-6.job-card-left, div.job-card-left")
        job_location = default_location
        if left_col:
            first_li = left_col.select_one("li")
            if first_li:
                loc_text = self._clean(first_li.get_text())
                if loc_text:
                    job_location = loc_text

        # Company — img alt text inside the /now-hiring/ link
        company_img = card.select_one("a[href*='/now-hiring/'] img")
        if company_img and company_img.get("alt"):
            company = self._clean(str(company_img["alt"]))
        else:
            company = "Unknown"

        # Date — "Posted: DD Mon YYYY" inside the left column
        date_posted = ""
        if left_col:
            raw = left_col.get_text()
            m = re.search(r"Posted:\s*(\d+ \w+ \d+)", raw)
            if m:
                date_posted = m.group(1)

        return JobListing(
            title=title,
            company=company,
            location=job_location,
            url=job_url,
            date_posted=date_posted,
            source=self.name,
        )
