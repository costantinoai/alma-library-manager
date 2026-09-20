---
title: Email
description: Point ALMa at an SMTP server so alert digests arrive in your mailbox.
---

# Email

The Email plugin delivers [alert](../concepts/alerts.md) digests over SMTP. It
is send-only — nothing arrives back into ALMa through it — and you can use it
instead of, or alongside, [Slack](slack.md).

## Set it up

**Settings → Plugins → Email**, switch it on, and fill the fields that appear:

| Field | What to put in it |
|---|---|
| **SMTP host** | Your provider's mail server — `smtp.gmail.com`, `smtp.fastmail.com`. |
| **Port** | `587` for STARTTLS (the default) or `465` for implicit TLS. |
| **Username** | The SMTP auth user, often your full email address. Leave blank for an unauthenticated relay. |
| **Password** | Your app password or SMTP key. |
| **From address** | The sender address. Defaults to the username when blank. |
| **Recipients** | One or more addresses, comma-separated. |
| **Use STARTTLS** | Recommended on port 587; ignored on 465. |

ALMa picks the transport from the port: 465 opens an SSL connection directly,
any other port issues `STARTTLS` when the switch is on.

Everything except the password is written to `data/settings.json` (`smtp_host`,
`smtp_port`, `smtp_username`, `smtp_from`, `smtp_to`, `smtp_use_tls`). The
**password** goes to the secret store (`data/secrets.json`, key
`smtp.password`) — never to `settings.json` — and comes back masked. Leave the
masked value in place to keep the stored password; clearing the field deletes
it.

Each field also accepts an environment-variable override for a headless setup:
`SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_FROM`, `SMTP_TO`,
`SMTP_PASSWORD`. See the
[Configuration reference](../reference/configuration.md#email-smtp).

!!! tip "App passwords"
    Most providers refuse your account password over SMTP. Generate an
    app-specific password in their security settings and paste that instead.

## Check it works

**Send test email** runs the test on the scheduler pool (Activity op key
`integrations.email.test`) through the same `EmailNotifier` real digests use.
The toast names the recipients on success, or the SMTP error on failure.

The test only runs while the plugin is on. Off, it is refused outright — see
[what is true of every plugin](index.md#what-is-true-of-every-plugin).

## What arrives

One HTML-plus-plaintext message per digest fire, with the same papers Slack
would get. A message is capped at **50 papers**; a larger fire is truncated
with an "…and N more" line rather than split into several emails.

Delivery is tracked per channel, so a failed email doesn't consume papers a
successful Slack send already delivered, and vice versa — those papers stay
eligible for the next fire.

## Related

* [Setting up alerts](../user-guide/setting-up-alerts.md) — build the digests
  this plugin delivers
* [Alerts](../concepts/alerts.md) — the matching, scheduling and dedup model
* [Plugins](index.md) — what is true of every plugin
