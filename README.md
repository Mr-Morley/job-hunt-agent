# job-hunt-agent

A Python agent that scrapes public job boards daily, scores listings for
relevance with Claude (Anthropic SDK), and emails you a ranked digest.

## Features

- Scrapes **Indeed**, **LinkedIn**, **Careers24**, and **Google Jobs** — no login required
- Scores each listing 0–10 using `claude-opus-4-6` with adaptive thinking
- Generates a formatted HTML email digest
- Runs automatically at 08:00 UTC via GitHub Actions

## Quick start

```bash
git clone <repo>
cd job-hunt-agent

pip install -r requirements.txt

cp .env.example .env
# Fill in ANTHROPIC_API_KEY, EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT

# Test without sending email
python -m src.main --no-email
```

## Configuration

Edit `src/config.py` to change job titles, locations, keywords, and
the minimum relevance score.

## GitHub Actions setup

1. Fork / push this repo.
2. Go to **Settings → Secrets and variables → Actions**.
3. Add: `ANTHROPIC_API_KEY`, `EMAIL_SENDER`, `EMAIL_PASSWORD`, `EMAIL_RECIPIENT`.
4. The workflow in `.github/workflows/daily_scan.yml` runs at 08:00 UTC daily.
   You can also trigger it manually from the Actions tab.

## Architecture

```
scrapers (httpx + bs4)
    ↓  raw JobListing objects
classifier (claude-opus-4-6)
    ↓  scored + filtered listings
summarizer (claude-opus-4-6)
    ↓  HTML/plain-text digest
email_sender (SMTP/Gmail)
```

## Requirements

- Python 3.10+
- An [Anthropic API key](https://console.anthropic.com/)
- A Gmail account with an [App Password](https://myaccount.google.com/apppasswords)

## Limitations

- Job boards may change their HTML structure, breaking scrapers.
  The scrapers are written defensively and return `[]` on failure.
- LinkedIn and Google Jobs apply bot-detection; success rate in
  GitHub Actions CI may be lower than running locally.
- Indeed is the most reliable scraper and should work consistently.
