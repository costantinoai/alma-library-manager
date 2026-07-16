"""EmailNotifier — the email-digest delivery client.

Mirrors :class:`alma.slack.client.SlackNotifier`: a graceful, never-raising
sender with an ``is_configured`` gate and an async ``send_paper_alert`` that the
alert engine calls. It consumes the **same paper-dict shape** the Slack block
builder does (``title, authors, year, journal/venue, url/pub_url/doi,
publication_date, abstract, alert_source``) so no caller has to change — the
only new code is the HTML/text rendering.

Delivery uses the stdlib (``smtplib`` + ``email.message``) run in a worker
thread (``asyncio.to_thread``) so the async digest path never blocks. No new
dependency is added.
"""
from __future__ import annotations

import asyncio
import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr
from html import escape

logger = logging.getLogger(__name__)

# Mirror Slack's chunking ceiling so a huge fire stays readable. Email has no
# hard size limit like Slack blocks, so this is generous.
_MAX_PAPERS_PER_EMAIL = 50


class EmailNotifier:
    """SMTP digest sender. Constructed via :func:`get_email_notifier`."""

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int = 587,
        username: str | None = None,
        password: str | None = None,
        from_addr: str | None = None,
        recipients: list[str] | None = None,
        use_tls: bool = True,
        timeout: float = 20.0,
    ) -> None:
        self._host = (host or "").strip() or None
        self._port = int(port or 587)
        self._username = (username or "").strip() or None
        self._password = password or None
        self._from = (from_addr or self._username or "").strip() or None
        self._recipients = [r for r in (recipients or []) if r]
        self._use_tls = bool(use_tls)
        self._timeout = timeout

    @property
    def is_configured(self) -> bool:
        """True when we have enough to actually send (host + from + recipients).

        Mirrors ``SlackNotifier.is_configured`` — every caller uses this to skip
        gracefully rather than raise when email isn't set up.
        """
        return bool(self._host and self._from and self._recipients)

    def resolve_recipients(self, recipients: list[str] | None = None) -> list[str]:
        resolved = [r for r in (recipients or self._recipients) if r]
        if not resolved:
            raise ValueError("No email recipients configured")
        return resolved

    # ---- Public send API (async, mirrors SlackNotifier) ------------------
    async def send_paper_alert(
        self,
        recipients: list[str] | None,
        papers: list[dict],
        alert_name: str,
    ) -> bool:
        """Render *papers* as an HTML+text digest and email it.

        Returns True on success, False on failure (never raises) so the alert
        evaluator only commits ``alerted_publications`` on a real send.
        """
        if not self.is_configured:
            logger.warning("Email/SMTP not configured; skipping paper alert")
            return False
        if not papers:
            logger.info("No papers to email for alert '%s'", alert_name)
            return True

        to = self.resolve_recipients(recipients)
        capped = papers[:_MAX_PAPERS_PER_EMAIL]
        total = len(papers)
        shown = len(capped)
        subject = f"ALMa · {alert_name} — {total} new paper{'s' if total != 1 else ''}"
        html = self._build_html(capped, alert_name, total=total, shown=shown)
        text = self._build_text(capped, alert_name, total=total, shown=shown)
        return await asyncio.to_thread(self._send, to, subject, html, text)

    async def send_test_message(self, recipients: list[str] | None = None) -> bool:
        if not self.is_configured:
            logger.warning("Email/SMTP not configured; cannot send test email")
            return False
        to = self.resolve_recipients(recipients)
        subject = "ALMa · test email"
        html = (
            "<div style=\"font-family:Georgia,serif;color:#14233a\">"
            "<h2 style=\"margin:0 0 8px\">ALMa</h2>"
            "<p>Your email digest channel is configured correctly. "
            "New-paper digests will arrive here.</p></div>"
        )
        text = "ALMa — your email digest channel is configured correctly."
        return await asyncio.to_thread(self._send, to, subject, html, text)

    def test_connection(self) -> bool:
        """SMTP handshake + login check, no message sent (plugin test path)."""
        if not self._host:
            return False
        try:
            with self._connect() as server:
                if self._username and self._password:
                    server.login(self._username, self._password)
            return True
        except Exception as exc:  # noqa: BLE001 — surface as False, log detail
            logger.warning("SMTP test connection failed: %s", exc)
            return False

    # ---- SMTP plumbing ---------------------------------------------------
    def _connect(self):
        """Open an SMTP connection (implicit-TLS on 465, STARTTLS otherwise)."""
        if self._port == 465:
            ctx = ssl.create_default_context()
            return smtplib.SMTP_SSL(self._host, self._port, timeout=self._timeout, context=ctx)
        server = smtplib.SMTP(self._host, self._port, timeout=self._timeout)
        server.ehlo()
        if self._use_tls:
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        return server

    def _send(self, to: list[str], subject: str, html: str, text: str) -> bool:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = formataddr(("ALMa", self._from)) if self._from else ""
        msg["To"] = ", ".join(to)
        msg.set_content(text)
        msg.add_alternative(html, subtype="html")
        try:
            with self._connect() as server:
                if self._username and self._password:
                    server.login(self._username, self._password)
                server.send_message(msg)
            logger.info("Emailed digest '%s' to %d recipient(s)", subject, len(to))
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("Email send failed: %s", exc)
            return False

    # ---- Rendering (the only genuinely new code) -------------------------
    @staticmethod
    def _paper_url(paper: dict) -> str:
        url = str(paper.get("url") or paper.get("pub_url") or paper.get("doi") or "").strip()
        if url.startswith("10."):
            url = f"https://doi.org/{url}"
        return url

    @staticmethod
    def _authors_text(paper: dict) -> str:
        authors = str(paper.get("authors") or "").strip()
        parts = [a.strip() for a in authors.split(",") if a.strip()]
        if len(parts) > 4:
            return f"{parts[0]}, [+{len(parts) - 2}], {parts[-1]}"
        return ", ".join(parts)

    @staticmethod
    def _meta_text(paper: dict) -> str:
        bits: list[str] = []
        pub_date = str(paper.get("publication_date") or "").strip()
        year = str(paper.get("year") or "").strip()
        journal = str(paper.get("journal") or paper.get("venue") or "").strip()
        if pub_date:
            bits.append(pub_date)
        elif year:
            bits.append(year)
        if journal:
            bits.append(journal)
        return " · ".join(bits)

    def _build_html(self, papers: list[dict], alert_name: str, *, total: int, shown: int) -> str:
        rows: list[str] = []
        for paper in papers:
            title = escape(str(paper.get("title") or "Untitled"))
            url = self._paper_url(paper)
            title_html = (
                f'<a href="{escape(url)}" style="color:#1e5b86;text-decoration:none">{title}</a>'
                if url
                else title
            )
            authors = escape(self._authors_text(paper))
            meta = escape(self._meta_text(paper))
            source = escape(str(paper.get("alert_source") or "").strip())
            abstract = escape(str(paper.get("abstract") or "").strip())
            if len(abstract) > 320:
                abstract = abstract[:317] + "…"
            parts = [
                f'<p style="margin:0 0 4px;font-size:15px;font-weight:600">{title_html}</p>'
            ]
            if authors:
                parts.append(f'<p style="margin:0;font-size:13px;color:#475569">{authors}</p>')
            if meta:
                parts.append(f'<p style="margin:0;font-size:12px;color:#64748b">{meta}</p>')
            if source:
                parts.append(
                    f'<p style="margin:4px 0 0;font-size:12px;color:#1e5b86">Match: {source}</p>'
                )
            if abstract:
                parts.append(
                    f'<p style="margin:6px 0 0;font-size:13px;color:#334155;line-height:1.5">{abstract}</p>'
                )
            rows.append(
                '<li style="margin:0 0 16px;padding:0 0 16px;border-bottom:1px solid #e7e0cf;'
                f'list-style:none">{"".join(parts)}</li>'
            )
        more = (
            f'<p style="font-size:12px;color:#64748b">…and {total - shown} more.</p>'
            if total > shown
            else ""
        )
        return (
            '<div style="font-family:Georgia,\'Times New Roman\',serif;max-width:640px;'
            'margin:0 auto;color:#14233a">'
            f'<h2 style="font-size:20px;margin:0 0 2px">ALMa</h2>'
            f'<p style="margin:0 0 16px;font-size:13px;color:#64748b">'
            f'{escape(alert_name)} — {total} new paper{"s" if total != 1 else ""}</p>'
            f'<ul style="margin:0;padding:0">{"".join(rows)}</ul>'
            f'{more}'
            '<p style="font-size:11px;color:#94a3b8;margin-top:20px">'
            'Sent by ALMa, your personal research companion.</p>'
            '</div>'
        )

    def _build_text(self, papers: list[dict], alert_name: str, *, total: int, shown: int) -> str:
        lines = [f"ALMa — {alert_name} — {total} new paper(s)", ""]
        for paper in papers:
            lines.append(str(paper.get("title") or "Untitled"))
            authors = self._authors_text(paper)
            if authors:
                lines.append(f"  {authors}")
            meta = self._meta_text(paper)
            if meta:
                lines.append(f"  {meta}")
            url = self._paper_url(paper)
            if url:
                lines.append(f"  {url}")
            source = str(paper.get("alert_source") or "").strip()
            if source:
                lines.append(f"  Match: {source}")
            lines.append("")
        if total > shown:
            lines.append(f"…and {total - shown} more.")
        return "\n".join(lines)


def get_email_notifier() -> EmailNotifier:
    """Build an :class:`EmailNotifier` from current config / secret store.

    Returns a dry-run notifier (``is_configured == False``) when SMTP isn't
    fully set up — exactly the graceful pattern of :func:`get_slack_notifier`.
    """
    from alma.config import (
        get_smtp_from,
        get_smtp_host,
        get_smtp_password,
        get_smtp_port,
        get_smtp_recipients,
        get_smtp_use_tls,
        get_smtp_username,
    )

    return EmailNotifier(
        host=get_smtp_host(),
        port=get_smtp_port(),
        username=get_smtp_username(),
        password=get_smtp_password(),
        from_addr=get_smtp_from(),
        recipients=get_smtp_recipients(),
        use_tls=get_smtp_use_tls(),
    )
