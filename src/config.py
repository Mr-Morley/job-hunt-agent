"""
Search preferences and configuration for the job-hunt agent.
All user-tunable settings live here; secrets come from .env.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SearchConfig:
    job_titles: list[str]
    locations: list[str]
    keywords: list[str]
    # Minimum relevance score (0-10) to include a job in the digest
    min_score: int = 6
    # Max results per scraper per (title, location) combo
    results_per_query: int = 10


# ---------------------------------------------------------------------------
# Primary configuration — edit this to change what gets scraped / scored
# ---------------------------------------------------------------------------

SEARCH_CONFIG = SearchConfig(
    job_titles=[
        "junior data engineer",
        "data scientist",
        "junior systems engineer",
        "graduate engineer",
    ],
    locations=[
        # South Africa
        "Cape Town, South Africa",
        "Johannesburg, South Africa",
        "Stellenbosch, South Africa",
        # Ireland
        "Dublin, Ireland",
        "Cork, Ireland",
    ],
    keywords=[
        # Technologies / skills to look for in job descriptions
        "Python",
        "SQL",
        "data pipeline",
        "ETL",
        "cloud",
        "AWS",
        "GCP",
        "Azure",
        "Spark",
        "dbt",
        "Airflow",
        "machine learning",
        "analytics",
        "Linux",
        "Docker",
    ],
    min_score=6,
    results_per_query=10,
)
