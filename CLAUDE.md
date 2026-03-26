# job-hunt-agent — Project Conventions

## What this project does

Scrapes public job boards daily, scores listings with Claude, and emails a
ranked digest to the owner.  No authentication to any job board is used.

## Running locally

```bash
# 1. Install deps
pip install -r requirements.txt

# 2. Set up environment
cp .env.example .env
# edit .env with your ANTHROPIC_API_KEY and email credentials

# 3. Dry run (no API calls, no email)
python -m src.main --dry-run

# 4. Full run, print digest to stdout instead of emailing
python -m src.main --no-email

# 5. Full run with email
python -m src.main
```

## Project layout

```
src/
  config.py          — edit this to change search preferences
  main.py            — orchestrator entry point
  scrapers/
    base.py          — JobListing dataclass + BaseScraper ABC
    indeed.py        — Indeed scraper (primary, most reliable)
    linkedin.py      — LinkedIn scraper (may be blocked in CI)
    careers24.py     — Careers24 (South Africa only)
    google_jobs.py   — Google Jobs via JSON-LD + HTML fallback
  agent/
    classifier.py    — scores listings 0–10 via claude-opus-4-6
    summarizer.py    — generates HTML email digest via claude-opus-4-6
  notifier/
    email_sender.py  — SMTP/Gmail sender
```

## Conventions

- **Python 3.10+**, type hints on all public APIs.
- **Scrapers never raise** — they catch all exceptions internally and
  return `[]` on failure so the pipeline keeps running.
- **No authentication** to any external service except Anthropic and SMTP.
- **httpx** for all HTTP requests (sync client, explicit timeouts).
- **beautifulsoup4 + lxml** for HTML parsing.
- **anthropic SDK** directly — no LangChain or other frameworks.
- `SEARCH_CONFIG` in `src/config.py` is the single place to change what
  job titles, locations, or keywords are targeted.
- `relevance_score` is the canonical field for filtering/sorting;
  `min_score` in `SEARCH_CONFIG` controls the threshold.

## Adding a new scraper

1. Create `src/scrapers/myboard.py` with a class inheriting `BaseScraper`.
2. Implement `scrape(job_title, location, *, max_results)`.
3. Add it to `src/scrapers/__init__.py`.
4. Import and instantiate it in `src/main.py::run_scrapers()`.

## GitHub Actions secrets required

| Secret             | Description                          |
|--------------------|--------------------------------------|
| `ANTHROPIC_API_KEY`| Anthropic API key                    |
| `EMAIL_SENDER`     | Gmail address used to send           |
| `EMAIL_PASSWORD`   | Gmail App Password                   |
| `EMAIL_RECIPIENT`  | Address that receives the digest     |
