"""Email delivery for ALMa digests.

A sibling of ``alma.slack`` — the email-digest channel. Named ``mailer`` (not
``email``) so it never shadows the Python stdlib ``email`` package that the
client imports (``email.message.EmailMessage``).
"""

from alma.mailer.client import EmailNotifier, get_email_notifier

__all__ = ["EmailNotifier", "get_email_notifier"]
