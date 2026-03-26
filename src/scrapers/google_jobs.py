"""
Google Jobs scraper.

Targets Google's "jobs" rich-result search (ibp=htl;jobs).
This surface is public and requires no authentication, but it relies
heavily on JavaScript rendering.  This implementation parses the
initial server-rendered JSON-LD embedded in the page as a fallback.

Reliability note: Google may return a CAPTCHA or 429 in CI environments.
The scraper returns [] gracefully in those cases.
"""
from __future__ import annotations

import json
import logging
import re
import time
import urllib.parse
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup

from .base import BaseScraper, JobListing

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class GoogleJobsScraper(BaseScraper):
    """
    Scrapes Google's public job search rich results.

    Parses JSON-LD structured data and inline script state embedded
    by Google's server-side render.  Falls back to BeautifulSoup card
    parsing when JSON-LD is unavailable.
    """

    name = "Google Jobs"

    def scrape(
        self,
        job_title: str,
        location: str,
        *,
        max_results: int = 10,
    ) -> list[JobListing]:
        query = f"{job_title} jobs in {location}"
        params = {"q": query, "ibp": "htl;jobs", "hl": "en"}
        url = f"https://www.google.com/search?{urllib.parse.urlencode(params)}"
        logger.info("Google Jobs scraping: %s", url)

        html = self._fetch(url)
        if not html:
            return []

        listings = self._parse_json_ld(html, location)
        if not listings:
            listings = self._parse_html_cards(html, location)

        logger.info("Google Jobs: parsed %d listings", len(listings))
        return listings[:max_results]

    def _fetch(self, url: str) -> Optional[str]:
        for attempt in range(1, self.max_retries + 2):
            try:
                with httpx.Client(
                    headers=_DEFAULT_HEADERS,
                    follow_redirects=True,
                    timeout=self.timeout,
                ) as client:
                    resp = client.get(url)
                    if resp.status_code == 429:
                        logger.warning("Google Jobs: rate-limited on attempt %d", attempt)
                        if attempt <= self.max_retries:
                            time.sleep(5 * attempt)
                        continue
                    resp.raise_for_status()
                    return resp.text
            except httpx.HTTPStatusError as exc:
                logger.warning("Google Jobs HTTP %s: %s", exc.response.status_code, url)
                if attempt <= self.max_retries:
                    time.sleep(2 ** attempt)
            except httpx.RequestError as exc:
                logger.warning("Google Jobs request error: %s", exc)
                if attempt <= self.max_retries:
                    time.sleep(2 ** attempt)
        return None

    def _parse_json_ld(self, html: str, location: str) -> list[JobListing]:
        """Extract JobPosting structured data from JSON-LD script blocks."""
        soup = BeautifulSoup(html, "html.parser")
        listings: list[JobListing] = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data: Any = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            # Data can be a single object or a list
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                type_ = item.get("@type", "")
                if type_ == "JobPosting":
                    listing = self._listing_from_json_ld(item, location)
                    if listing:
                        listings.append(listing)
                elif type_ == "ItemList":
                    for element in item.get("itemListElement", []):
                        if isinstance(element, dict) and element.get("@type") == "JobPosting":
                            listing = self._listing_from_json_ld(element, location)
                            if listing:
                                listings.append(listing)

        return listings

    def _listing_from_json_ld(self, item: dict[str, Any], location: str) -> Optional[JobListing]:
        title = item.get("title", "").strip()
        if not title:
            return None

        company_data = item.get("hiringOrganization", {})
        company = (
            company_data.get("name", "Unknown") if isinstance(company_data, dict) else "Unknown"
        )

        loc_data = item.get("jobLocation", {})
        if isinstance(loc_data, list):
            loc_data = loc_data[0] if loc_data else {}
        addr = loc_data.get("address", {}) if isinstance(loc_data, dict) else {}
        if isinstance(addr, str):
            job_location = addr
        elif isinstance(addr, dict):
            parts = [
                addr.get("addressLocality", ""),
                addr.get("addressRegion", ""),
                addr.get("addressCountry", ""),
            ]
            job_location = ", ".join(p for p in parts if p) or location
        else:
            job_location = location

        url = item.get("url", "")
        if not url:
            return None

        description = BeautifulSoup(
            item.get("description", ""), "html.parser"
        ).get_text(" ")
        salary_data = item.get("baseSalary", {})
        salary = ""
        if isinstance(salary_data, dict):
            value = salary_data.get("value", {})
            if isinstance(value, dict):
                min_v = value.get("minValue", "")
                max_v = value.get("maxValue", "")
                currency = salary_data.get("currency", "")
                if min_v and max_v:
                    salary = f"{currency} {min_v}–{max_v}"
        date_posted = item.get("datePosted", "")

        return JobListing(
            title=self._clean(title),
            company=self._clean(company),
            location=self._clean(job_location),
            url=url,
            description=self._clean(description)[:500],
            salary=salary,
            date_posted=str(date_posted),
            source=self.name,
        )

    def _parse_html_cards(self, html: str, location: str) -> list[JobListing]:
        """
        Fallback HTML parser for Google's job card elements.
        These selectors are fragile — treat results as best-effort.
        """
        soup = BeautifulSoup(html, "html.parser")
        listings: list[JobListing] = []

        cards = soup.select("div.iFjolb, div[data-hveid] div.BjJfJf, li.LL4J2")
        for card in cards:
            title_el = card.select_one("div.BjJfJf, div.vNEEBe")
            company_el = card.select_one("div.vNEEBe, div.nJlQNd")
            loc_el = card.select_one("div.Qk80Jf")
            link_el = card.select_one("a")

            if not (title_el and link_el and link_el.get("href")):
                continue

            listings.append(
                JobListing(
                    title=self._clean(title_el.get_text()),
                    company=self._clean(company_el.get_text()) if company_el else "Unknown",
                    location=self._clean(loc_el.get_text()) if loc_el else location,
                    url=str(link_el["href"]),
                    source=self.name,
                )
            )

        return listings
