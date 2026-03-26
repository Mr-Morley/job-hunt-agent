"""
Daily digest summarizer using the Anthropic SDK.

Takes a list of scored JobListings and produces a well-formatted HTML
(and plain-text) email digest using claude-opus-4-6.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import anthropic

from ..scrapers.base import JobListing

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an assistant that writes clear, concise daily job-hunt digest emails
for a recent graduate looking for their first role in data engineering or
software engineering.

You will receive a list of scored job listings.  Write:
1. A short introductory sentence (1–2 sentences) summarising today's finds.
2. Grouped sections by relevance tier: "Top Picks (8–10)", "Worth a Look (6–7)".
   Within each group, list jobs in descending score order.
3. For each job include:
   - Job title and company (bold)
   - Location and salary (if known)
   - One-sentence reason why it's relevant
   - The URL as a clickable link
4. A closing encouragement sentence.

Format the output as clean HTML suitable for an email body (use <h2>, <ul>,
<li>, <a href="...">, <strong>, <p> tags).  Do not include <html>/<body>/<head>.
Keep the tone friendly and professional.
"""


@dataclass
class DigestResult:
    html_body: str
    plain_body: str
    top_count: int  # number of score 8–10 listings
    total_count: int


class JobSummarizer:
    """
    Generates a daily HTML digest from a list of scored JobListings.
    """

    def __init__(self, *, model: str = "claude-opus-4-6") -> None:
        self._client = anthropic.Anthropic()
        self._model = model

    def generate_digest(self, listings: list[JobListing]) -> DigestResult:
        """
        Build a daily digest.  *listings* should already be scored and
        filtered to those above the minimum threshold.
        """
        if not listings:
            return DigestResult(
                html_body="<p>No matching jobs found today. Check back tomorrow!</p>",
                plain_body="No matching jobs found today. Check back tomorrow!",
                top_count=0,
                total_count=0,
            )

        sorted_listings = sorted(listings, key=lambda j: j.relevance_score, reverse=True)
        top_count = sum(1 for j in sorted_listings if j.relevance_score >= 8)

        prompt = self._build_prompt(sorted_listings)

        try:
            html_body = self._call_api(prompt)
        except Exception as exc:
            logger.error("Summarizer API call failed: %s", exc)
            html_body = self._fallback_html(sorted_listings)

        plain_body = self._html_to_plain(html_body, sorted_listings)

        return DigestResult(
            html_body=html_body,
            plain_body=plain_body,
            top_count=top_count,
            total_count=len(sorted_listings),
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_prompt(self, listings: list[JobListing]) -> str:
        today = date.today().strftime("%A, %d %B %Y")
        lines = [f"Today's date: {today}", "", "Scored job listings:"]
        for i, job in enumerate(listings, 1):
            lines += [
                f"\n{i}. [{job.relevance_score}/10] {job.title} at {job.company}",
                f"   Location: {job.location}",
                f"   Salary: {job.salary or 'not specified'}",
                f"   Reason: {job.relevance_reason}",
                f"   URL: {job.url}",
                f"   Source: {job.source}",
            ]
        return "\n".join(lines)

    def _call_api(self, prompt: str) -> str:
        with self._client.messages.stream(
            model=self._model,
            max_tokens=4096,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            return stream.get_final_message().content[0].text  # type: ignore[union-attr]

    def _fallback_html(self, listings: list[JobListing]) -> str:
        """Minimal HTML digest used when the API call fails."""
        today = date.today().strftime("%d %B %Y")
        items = "".join(
            f"<li><strong>{j.title}</strong> — {j.company}, {j.location} "
            f"[{j.relevance_score}/10] <a href='{j.url}'>View</a></li>"
            for j in listings
        )
        return (
            f"<h2>Job Digest — {today}</h2>"
            f"<p>Found {len(listings)} relevant listing(s).</p>"
            f"<ul>{items}</ul>"
        )

    @staticmethod
    def _html_to_plain(html: str, listings: list[JobListing]) -> str:
        """Very rough HTML → plain text for the multipart fallback."""
        from bs4 import BeautifulSoup

        text = BeautifulSoup(html, "html.parser").get_text(separator="\n")
        # Append raw URLs so plain-text readers can still click them
        text += "\n\nAll links:\n"
        for job in listings:
            text += f"  {job.title} ({job.company}): {job.url}\n"
        return text
