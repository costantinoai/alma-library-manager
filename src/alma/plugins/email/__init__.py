"""SMTP integration plugin for the core Alerts delivery protocol."""

from __future__ import annotations

import sqlite3
from typing import Annotated

from pydantic import BaseModel, Field

from alma.plugins.configs import StrictPluginConfig
from alma.plugins.manifest import SEND, PluginManifest


class EmailConfig(StrictPluginConfig):
    host: str = Field("", title="SMTP host", json_schema_extra={"x-alma-order": 10})
    port: Annotated[int, Field(ge=1, le=65535)] = Field(
        587,
        title="Port",
        json_schema_extra={"x-alma-order": 20},
    )
    username: str = Field("", title="Username", json_schema_extra={"x-alma-order": 30})
    password: str = Field(
        "",
        title="Password",
        description="SMTP password or app key. Stored in the secret store.",
        json_schema_extra={"x-alma-secret": True, "x-alma-order": 40},
    )
    from_address: str = Field(
        "",
        title="From address",
        json_schema_extra={"x-alma-order": 50},
    )
    recipients: str = Field(
        "",
        title="Recipients",
        description="Comma-separated recipient addresses.",
        json_schema_extra={"x-alma-order": 60},
    )
    use_tls: bool = Field(
        True,
        title="Use STARTTLS",
        json_schema_extra={"x-alma-order": 70},
    )


def read_config(_db: sqlite3.Connection | None) -> EmailConfig:
    from alma.config import get_setting
    from alma.core.secrets import SECRET_SMTP_PASSWORD, get_secret, mask_secret

    return EmailConfig(
        host=str(get_setting("smtp_host") or ""),
        port=int(get_setting("smtp_port", 587) or 587),
        username=str(get_setting("smtp_username") or ""),
        password=mask_secret(get_secret(SECRET_SMTP_PASSWORD), suffix=4) or "",
        from_address=str(get_setting("smtp_from") or ""),
        recipients=str(get_setting("smtp_to") or ""),
        use_tls=bool(get_setting("smtp_use_tls", True)),
    )


def write_config(_db: sqlite3.Connection | None, config: BaseModel) -> None:
    from alma.config import update_settings
    from alma.core.secrets import SECRET_SMTP_PASSWORD, delete_secret, set_secret

    parsed = EmailConfig.model_validate(config)
    password = parsed.password.strip()
    if not password.startswith("****"):
        if password:
            set_secret(SECRET_SMTP_PASSWORD, password)
        else:
            delete_secret(SECRET_SMTP_PASSWORD)
    update_settings(
        {
            "smtp_host": parsed.host.strip() or None,
            "smtp_port": parsed.port,
            "smtp_username": parsed.username.strip() or None,
            "smtp_from": parsed.from_address.strip() or None,
            "smtp_to": parsed.recipients.strip() or None,
            "smtp_use_tls": parsed.use_tls,
        }
    )


def status(_db: sqlite3.Connection | None) -> dict:
    from alma.mailer.client import get_email_notifier

    configured = bool(get_email_notifier().is_configured)
    return {
        "configured": configured,
        "can_send": configured,
        "can_receive": False,
    }


async def send_alert(papers: list[dict], alert_name: str) -> bool:
    from alma.mailer.client import get_email_notifier

    return await get_email_notifier().send_paper_alert(
        recipients=None,
        papers=papers,
        alert_name=alert_name,
    )


async def test_connection() -> dict:
    from alma.mailer.client import get_email_notifier

    notifier = get_email_notifier()
    try:
        ok = await notifier.send_test_message()
    except Exception as exc:  # pragma: no cover - network failure boundary
        return {
            "ok": False,
            "error": str(exc),
            "message": f"Email test failed: {exc}",
        }
    recipients = ", ".join(notifier.resolve_recipients()) if ok else ""
    return {
        "ok": ok,
        "target": recipients or None,
        "message": (
            f"Test email sent to {recipients}"
            if ok
            else "SMTP rejected the test email; check host, port, and credentials."
        ),
    }


EMAIL_PLUGIN = PluginManifest(
    id="email",
    display_name="Email",
    version="2.0.0",
    description="Deliver alert digests over SMTP.",
    kind="integration",
    capabilities=(SEND,),
    config_model=EmailConfig,
    read_config=read_config,
    write_config=write_config,
    status_factory=status,
    docs_path="/concepts/channels/",
    alert_sender=send_alert,
    connection_tester=test_connection,
    action_ids=("test",),
)

__all__ = ["EMAIL_PLUGIN", "EmailConfig"]
