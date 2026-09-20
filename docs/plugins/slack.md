---
title: Slack
description: Register the ALMa Slack app, grant it scopes, install it, hand ALMa the token — and point it at a channel for digests, for capture, or both.
---

# Slack

ALMa talks to Slack in two directions, and **both use the same app and the same
token**:

| Direction | What it does | The feature it serves |
|---|---|---|
| **Outbound** | ALMa posts digests of new papers to a channel or a DM | [Alerts](../concepts/alerts.md) — see [Setting up alerts](../user-guide/setting-up-alerts.md) |
| **Inbound** | You post a paper link from your phone; ALMa captures it | [Inbox](../concepts/inbox.md) — see [Capturing from your phone](../user-guide/capturing-from-your-phone.md) |

Steps 1–5 below are the setup, done once. Then do step 6, step 7, or both,
depending on which directions you want.

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

**Add these too if you want capture** (sending papers to yourself from your
phone):

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

## Step 4 — Switch the plugin on and give ALMa the token

1. At the top of **OAuth & Permissions**, copy the **Bot User OAuth Token**
   (starts `xoxb-`).

    !!! danger "Treat this like a password"
        Anyone holding it can act as your bot. If it leaks, hit **Regenerate**
        on that page and paste the new one into ALMa.

2. In ALMa: **Settings → Plugins → Slack**, switch it **on** — the fields
   appear — paste the token into **Bot token**, and **Save plugin settings**.

ALMa keeps the token in its secret store (`data/secrets.json`, gitignored, mode
`0600`) — never in `settings.json`, never in `.env` — and shows only a masked
version afterwards. Leave the masked value in place when you edit other fields;
it keeps the stored token.

---

## Step 5 — Check it works

**Settings → Plugins → Slack → Test connection.** ALMa queues a job (visible in
**Activity** as `integrations.slack.test`) that posts an "ALMa — Connection
Test" message through the same code path real digests use. The toast names the
resolved target on success, or the precise Slack error on failure.

The test only runs while the plugin is on. Off, it is refused outright — see
[what is true of every plugin](index.md#what-is-true-of-every-plugin).

---

## Step 6 — Send digests to a channel or a DM

**Send alerts to** accepts any of:

* a channel name — `general` or `#general`;
* a user display name — `Andrea Costantino`, which resolves to a DM;
* a Slack ID — `C0123…` for a channel, `U0123…` for a user.

Resolution to an ID happens at send time and is cached for the lifetime of the
backend process, so a wrong name produces a precise `channel_not_found` in the
Activity row rather than a generic failure.

For a channel target, invite the bot to that channel first (`/invite @ALMa`) —
a bot cannot post where it is not a member.

Building the digests themselves is a separate job:
**→ [Setting up alerts](../user-guide/setting-up-alerts.md)**

---

## Step 7 — Capture papers from a channel

### Create the capture channel and add the bot **to that channel**

Now in the **Slack app itself** (not the developer site):

1. Click **+** beside **Channels** in the sidebar → **Create a channel**.
2. **Name**: `alma-inbox`. **Visibility**: **Private**. → **Create**.
3. Open `#alma-inbox` and send this message in the channel:

        /invite @ALMa

    Use your bot's actual name if you named the app something else — Slack
    autocompletes it as you type. If nothing autocompletes, the app is not
    installed — see [step 3](#step-3-install-it-into-your-workspace).

4. Slack confirms: *"@ALMa was added to #alma-inbox"*.

**That invite is the membership that matters.** The bot can now read
`#alma-inbox` — and still nothing else: not your other channels, not your DMs.

!!! warning "Use a channel for nothing else"
    Every message here is treated as a capture attempt. Don't reuse a channel
    you chat in.

### Tell ALMa which channel to read

**Settings → Plugins → Slack → Capture papers from** → type `alma-inbox`
(bare name, **no `#`**) → **Save plugin settings**.

Empty field = capture off.

!!! note "Why this is separate from 'Send alerts to'"
    **Send alerts to** is where ALMa **posts** digests. **Capture papers from**
    is what it **reads**. They can be the same channel — the poller skips any
    message carrying a `bot_id`, so ALMa never re-reads its own digests. A
    dedicated capture channel is still better: digests are long and frequent,
    and they bury the links you send.

### Prove it works

1. In Slack, post a paper link into `#alma-inbox`:

        https://doi.org/10.1038/s41586-019-1666-5

2. In ALMa: **Settings → Plugins → Slack → Check capture now** — runs the check
   immediately instead of waiting up to 5 minutes.
3. Expect the toast **"Captured 1 paper"**. (With nothing new to capture you
   get **"Connected to #alma-inbox"** instead — that is also a success, and it
   confirms the token, scopes, channel and bot membership are all correct.)
4. In Slack, your message now shows a 📥 reaction.
5. Open **Home** — the paper is in the **Inbox** section.

From then on, just post links: ALMa checks every five minutes on its own. What
to expect on your phone, what it recognises, and how to triage what lands is
the feature guide:
**→ [Capturing from your phone](../user-guide/capturing-from-your-phone.md)**

---

## Troubleshooting

| What you saw | Cause | Fix |
|---|---|---|
| `Slack token not configured` | No token saved | Redo [step 4](#step-4-switch-the-plugin-on-and-give-alma-the-token) |
| `invalid_auth` | Token wrong, or regenerated in Slack | Re-copy it from **OAuth & Permissions** and save it again |
| `missing_scope` | A scope is absent, or the app wasn't reinstalled after you added one | Recheck [step 2](#step-2-grant-the-permissions), then **Reinstall to Workspace** |
| `channel_not_found: '…'` | The resolver tried `conversations.list` then `users.list` and found no match | Check the spelling, that the bot is in the channel, and that `channels:read` / `groups:read` / `users:read` are granted |
| `not_in_channel` | The bot isn't a member of the channel | `/invite @ALMa` in that channel |
| `Slack API rejected the test message` | Token and resolution are fine, but the bot cannot post to that target | Usually channel membership; for a user DM, check `im:write` |
| *"No capture channel is configured"* | **Capture papers from** is empty, or didn't save | Re-enter the bare channel name and save |
| ❓ instead of 📥 on your message | The link held no identifiable paper | Read ALMa's thread reply |

## Related

* [Setting up alerts](../user-guide/setting-up-alerts.md) — build the digests
* [Capturing from your phone](../user-guide/capturing-from-your-phone.md) — use
  the inbound direction
* [Plugins](index.md) — what is true of every plugin
