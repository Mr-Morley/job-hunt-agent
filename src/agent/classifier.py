"""
Job relevance classifier using the Anthropic SDK.

Scores each JobListing 0–10 based on how well it matches the search
config (titles, keywords, and seniority level).

Uses claude-haiku-4-5 by default: fast, cheap, and more than capable for
a simple 0-10 scoring task.  Structured JSON output via output_config
guarantees valid JSON without fragile regex stripping.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import anthropic

from ..config import SearchConfig
from ..scrapers.base import JobListing

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an expert job-relevance classifier for a recent graduate with 0–2 years of
experience targeting entry-level data engineering and data science roles.

Score each listing 0–10:

  0–2  : Completely irrelevant (wrong field, unrelated industry, or clearly senior-only)
  3–5  : Tangentially relevant (some matching skills but significant mismatches)
  6–7  : Relevant (mostly matches preferences, minor gaps)
  8–10 : Highly relevant (right level, right skills, right location)

SENIORITY RULES — apply these strictly before scoring anything else:
- Titles containing "Senior", "Lead", "Principal", "Staff", "Head of", "Director",
  or "Manager" cap the score at 3 UNLESS the description explicitly mentions a
  graduate programme, junior track, or rotational scheme.
- "Mid-level" or "intermediate" roles cap the score at 5.
- "Junior", "Graduate", "Associate", "Entry-level", or "Trainee" titles score
  normally — no cap.
- Internships score normally if the skills align.

Respond with a JSON object exactly like this (no extra text):
{"score": <integer 0-10>, "reason": "<one sentence explaining the key factor>"}
"""


def _listing_to_text(listing: JobListing, config: SearchConfig) -> str:
    lines = [
        f"Title: {listing.title}",
        f"Company: {listing.company}",
        f"Location: {listing.location}",
    ]
    if listing.salary:
        lines.append(f"Salary: {listing.salary}")
    if listing.description:
        lines.append(f"Description: {listing.description[:800]}")
    lines.append("")
    lines.append(f"Candidate is looking for: {', '.join(config.job_titles)}")
    lines.append(f"Desired locations: {', '.join(config.locations)}")
    lines.append(f"Key skills/keywords: {', '.join(config.keywords)}")
    return "\n".join(lines)


class JobClassifier:
    """
    Classifies job listings using claude-haiku-4-5.

    Haiku is fast and cheap for batch scoring; no thinking needed for a
    simple 0-10 relevance score.  Structured output_config enforces valid
    JSON so there's nothing fragile to parse.
    """

    def __init__(self, *, model: str = "claude-haiku-4-5") -> None:
        self._client = anthropic.Anthropic()
        self._model = model

    def score(self, listing: JobListing, config: SearchConfig) -> JobListing:
        """
        Annotate *listing* with a relevance_score and relevance_reason.
        Returns the same listing object (mutated in place) for convenience.
        """
        prompt = _listing_to_text(listing, config)
        try:
            result = self._classify(prompt)
            listing.relevance_score = result["score"]
            listing.relevance_reason = result["reason"]
        except Exception as exc:
            logger.warning("Classifier error for %r: %s", listing.title, exc)
            listing.relevance_score = 0
            listing.relevance_reason = f"Classification failed: {exc}"
        return listing

    def score_many(
        self,
        listings: list[JobListing],
        config: SearchConfig,
    ) -> list[JobListing]:
        """Score a batch of listings, skipping duplicates by URL."""
        seen_urls: set[str] = set()
        results: list[JobListing] = []
        for listing in listings:
            if listing.url in seen_urls:
                continue
            seen_urls.add(listing.url)
            self.score(listing, config)
            results.append(listing)
        return results

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _classify(self, prompt: str) -> dict[str, Any]:
        response = self._client.messages.create(
            model=self._model,
            max_tokens=512,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
            output_config={
                "format": {
                    "type": "json_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "score": {"type": "integer"},
                            "reason": {"type": "string"},
                        },
                        "required": ["score", "reason"],
                        "additionalProperties": False,
                    },
                }
            },
        )

        text = next(
            (b.text for b in response.content if b.type == "text"), None
        )
        if not text:
            raise ValueError("No text block in classifier response")

        data: dict[str, Any] = json.loads(text)
        score = int(data.get("score", 0))
        reason = str(data.get("reason", ""))
        if not 0 <= score <= 10:
            raise ValueError(f"Score out of range: {score}")
        return {"score": score, "reason": reason}
