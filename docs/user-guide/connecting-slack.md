---
title: Connecting Slack
description: Register the ALMa Slack app, grant it permissions, install it in your workspace, and hand ALMa the token. Do this once; both alerts and capture use it.
---

# Connecting Slack

ALMa talks to Slack in two directions, and **both use the same app and the same
token**:

| Direction | What it does | Guide |
|---|---|---|
| **Outbound** | ALMa posts digests of new papers to a channel | [Setting up alerts](setting-up-alerts.md) |
| **Inbound** | You post a paper link from your phone; ALMa captures it | [Capturing from your phone](capturing-from-your-phone.md) |

Do this page once. Then follow whichever of the two guides you want — or both.

## What you are actually setting up

ALMa is not in the Slack App Directory — it runs on *your* machine, so there is
nothing to install from a marketplace. Instead you register a small private app
inside your own workspace.

Four things are involved, and keeping them straight makes every step obvious:

| Thing | What it is | Lives at |
|---|---|---|
| **The app** | A private integration you register. Owns the permissions and issues the token. Only your workspace can see it. | [api.slack.com/apps](https://api.slack.com/apps) |
| **The bot user** | The account the app creates *inside* your workspace — `@ALMa` in the member list. This is what you invite to a channel. | Your Slack sidebar, under **Apps** |
| **The bot token** | A secret string starting `xoxb-`. ALMa sends it with every request to prove who it is. | Copied once, pasted into ALMa Settings |
| **Scopes** | The permissions attached to that token. A token can only do what its scopes allow. | Set on the app, **before** you install |

Two rules cause almost every setup failure:

1. **Scopes are fixed at install time.** Adding a scope afterwards does nothing
   until you *reinstall*. This page grants everything up front so you install
   exactly once.
2. **A bot can only read channels it has been added to.** Installing the app
   grants it nothing by itself — you invite the bot to specific channels later.

!!! info "Why a bot token and not your own account"
    Slack also issues *user* tokens, which act as you and can read everything
    you can, including your DMs. ALMa uses a **bot** token, which sees only the
    channels you explicitly add it to. That is also why capture uses a channel
    rather than a message-to-self: no bot can read your self-DM.

---

## Step 1 — Create the app

1. Go to **[api.slack.com/apps](https://api.slack.com/apps)** and sign in with
   the account you use for your workspace.
2. **Create New App** → **From scratch**.
3. **App Name**: `ALMa`. **Pick a workspace**: yours. → **Create App**.

You land on the app's settings page. Nothing has reached your workspace yet —
that is step 3.

---

## Step 2 — Grant the permissions

Left sidebar → **OAuth & Permissions** → scroll to **Scopes → Bot Token
Scopes** → **Add an OAuth Scope**, once per row you need.

**Always add this one:**

| Scope | What it lets ALMa do |
|---|---|
| `chat:write` | Post messages — alert digests, and the reply explaining a link it couldn't identify |

**Add these too if you want [capture](capturing-from-your-phone.md)** (sending
papers to yourself from your phone):

| Scope | What it lets ALMa do |
|---|---|
| `channels:read` | Look up a channel by name |
| `groups:read` | Same, for **private** channels |
| `groups:history` | Read messages in a **private** capture channel |
| `reactions:write` | Add the 📥 / 📚 / ❓ receipt to your message |
| `channels:history` | Read messages in a **public** capture channel — only if yours is public |

**Add these if you want alerts sent to a person as a DM** rather than a channel:

| Scope | What it lets ALMa do |
|---|---|
| `users:read` | Resolve a display name like `Andrea Costantino` |
| `im:write` | Open the DM to send into |

!!! warning "Add `channels:read` even for a private channel"
    ALMa resolves a channel name by listing public *and* private channels in one
    call, so Slack demands both `channels:read` and `groups:read`. With only one,
    the call fails `missing_scope` and capture silently finds nothing.

    Prefer to grant less? Skip `channels:read` and give ALMa the channel **ID**
    (`C…`, from **Channel details → About → Channel ID**) instead of the name —
    an ID is used directly and never triggers a channel listing.

---

## Step 3 — Install it into your workspace

This is the step that actually adds ALMa to Slack.

1. Scroll to the top of **OAuth & Permissions**.
2. **Install to Workspace**.
3. Slack shows exactly what you granted → **Allow**.

`@ALMa` now exists in your workspace, in the sidebar under **Apps**.

!!! note "Coming back later to add a scope?"
    Add it, then use **Reinstall to Workspace** on the same page. Until you
    reinstall, the new scope has no effect — the token stays valid and the call
    just keeps failing. Your token does **not** change when you reinstall, so
    there is nothing to re-paste into ALMa.

---

## Step 4 — Give ALMa the token

1. At the top of **OAuth & Permissions**, copy the **Bot User OAuth Token**
   (starts `xoxb-`).

    !!! danger "Treat this like a password"
        Anyone holding it can act as your bot. If it leaks, hit **Regenerate**
        on that page and paste the new one into ALMa.

2. In ALMa: **Settings → Channels → Slack Bot Token** → paste → **Save**.

ALMa keeps it in its secret store (`data/secrets.json`, gitignored, mode
`0600`) — never in `settings.json`, never in `.env` — and shows only a masked
version afterwards.

---

## Step 5 — Check it works

**Settings → Channels → Test Slack Connection.** A success toast means the token
and scopes are good.

## Next

* **Receive digests** → [Setting up alerts](setting-up-alerts.md)
* **Send yourself papers from your phone** →
  [Capturing from your phone](capturing-from-your-phone.md)
