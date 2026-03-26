"""
Email notifier — sends the daily digest via SMTP/Gmail.

Credentials are read from environment variables (set in .env):
  EMAIL_SENDER   — the "from" address (e.g. yourname@gmail.com)
  EMAIL_PASSWORD — Gmail App Password (NOT your regular password)
  EMAIL_RECIPIENT — the "to" address
  SMTP_HOST      — default: smtp.gmail.com
  SMTP_PORT      — default: 587
"""
from __future__ import annotations

import logging
import os
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from ..agent.summarizer import DigestResult

logger = logging.getLogger(__name__)


class EmailSender:
    def __init__(self) -> None:
        self.sender = os.environ["EMAIL_SENDER"]
        self.password = os.environ["EMAIL_PASSWORD"]
        self.recipient = os.environ.get("EMAIL_RECIPIENT", self.sender)
        self.smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    def send_digest(self, digest: DigestResult) -> None:
        """Compose and send the daily job digest email."""
        today = date.today().strftime("%d %B %Y")
        subject = (
            f"Job Digest {today} — "
            f"{digest.top_count} top pick(s), {digest.total_count} total"
        )

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = self.recipient

        msg.attach(MIMEText(digest.plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(digest.html_body, "html", "utf-8"))

        logger.info(
            "Sending digest to %s (%d listings)…",
            self.recipient,
            digest.total_count,
        )
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(self.sender, self.password)
                smtp.sendmail(self.sender, self.recipient, msg.as_string())
            logger.info("Digest sent successfully.")
        except smtplib.SMTPException as exc:
            logger.error("Failed to send email: %s", exc)
            raise
