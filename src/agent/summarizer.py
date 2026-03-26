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
   - The URL as a clickable "Apply" link

Format the output as clean HTML suitable for an email body.
Use only: <h2>, <ul>, <li>, <a href="...">, <strong>, <em>, <p> tags.
Do NOT include <html>, <body>, <head>, or any inline styles — those are
added by the template wrapper.
Keep the tone friendly and professional.
"""

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f5;font-family:Arial,Helvetica,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f5;padding:32px 0">
  <tr><td align="center">
    <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">

      <!-- Header -->
      <tr><td style="background:#1d4ed8;padding:28px 32px">
        <h1 style="margin:0;color:#ffffff;font-size:22px">Job Hunt Digest</h1>
        <p style="margin:6px 0 0;color:#bfdbfe;font-size:14px">{date_label}</p>
      </td></tr>

      {pages_banner}

      <!-- Body -->
      <tr><td style="padding:28px 32px;color:#111827;font-size:15px;line-height:1.6">
        {body_html}
      </td></tr>

      <!-- Footer -->
      <tr><td style="background:#f9fafb;padding:20px 32px;border-top:1px solid #e5e7eb">
        <p style="margin:0;font-size:12px;color:#6b7280;text-align:center">
          Powered by Claude · listings sourced from LinkedIn &amp; Careers24
        </p>
      </td></tr>

    </table>
  </td></tr>
</table>
</body>
</html>
"""

_PAGES_BANNER = """\
<tr><td style="background:#eff6ff;padding:14px 32px;border-bottom:1px solid #bfdbfe">
  <p style="margin:0;font-size:14px;color:#1e40af">
    Browse the full board with filters:&nbsp;
    <a href="{pages_url}" style="color:#1d4ed8;font-weight:bold">{pages_url}</a>
  </p>
</td></tr>
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

    Pass pages_url to include a "View Full Board" banner linking to the
    GitHub Pages job board at the top of every email.
    """

    def __init__(self, *, model: str = "claude-opus-4-6", pages_url: str = "") -> None:
        self._client = anthropic.Anthropic()
        self._model = model
        self._pages_url = pages_url

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
            body_html = self._call_api(prompt)
        except Exception as exc:
            logger.error("Summarizer API call failed: %s", exc)
            body_html = self._fallback_html(sorted_listings)

        html_body = self._wrap_template(body_html)
        plain_body = self._html_to_plain(body_html, sorted_listings)

        return DigestResult(
            html_body=html_body,
            plain_body=plain_body,
            top_count=top_count,
            total_count=len(sorted_listings),
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _wrap_template(self, body_html: str) -> str:
        today = date.today().strftime("%A, %d %B %Y")
        pages_banner = (
            _PAGES_BANNER.format(pages_url=self._pages_url)
            if self._pages_url
            else ""
        )
        return _HTML_TEMPLATE.format(
            date_label=today,
            pages_banner=pages_banner,
            body_html=body_html,
        )

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
