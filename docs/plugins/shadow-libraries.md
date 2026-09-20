---
title: Shadow libraries
description: The opt-in Sci-Hub and Anna's Archive source — finding working addresses, adding them, and what ALMa does with what they send back.
---

# Shadow libraries

A separate, opt-in PDF source for Sci-Hub and Anna's Archive. It is off by
default, ships with **no addresses at all**, runs only after every
[open-access source](open-access-pdfs.md) has failed, and only when you ask for
a particular paper's PDF. Whether using these services is legal depends on
where you live.

## Find working addresses

Mirror addresses move often, and ALMa does not guess them. Check what is up
before you add anything:

| What | Where | Kind |
|---|---|---|
| Live status of every mirror, refreshed every few minutes | [SLUM — Sci-Hub](https://open-slum.org/scihub.html) · [SLUM — Anna's Archive](https://open-slum.org/annas.html) | third-party monitor |
| Sci-Hub's official addresses | [Wikipedia: Sci-Hub](https://en.wikipedia.org/wiki/Sci-Hub) (infobox) | encyclopaedia |
| Anna's Archive's official addresses | [Wikipedia: Anna's Archive](https://en.wikipedia.org/wiki/Anna%27s_Archive) (infobox), and the *mirrors* entry of the FAQ on any working Anna's Archive address (`/faq#mirrors`) | encyclopaedia, official |

On 19 September 2026 these were the official addresses:

* **Sci-Hub**: `https://sci-hub.ru`, `https://sci-hub.su`, `https://sci-hub.box`
  (`sci-hub.st` had an expired certificate, `sci-hub.red` was down, `sci-hub.se`
  was blocked).
* **Sci-Net**: `https://sci-net.xyz` — Sci-Hub's own site for papers its
  database lacks, newer ones especially. It answers the same `/<doi>` address
  as a mirror, so add it to the Sci-Hub list.
* **Anna's Archive**: `https://annas-archive.gl`, `https://annas-archive.pk`,
  `https://annas-archive.gd`.

### Alternative mirrors

When the official mirrors are walled from your network, unofficial copies can
still serve the file. On 19 September 2026 `https://sci-hub.ren`,
`https://sci-hub.al` and `https://sci-hub.ee` served PDFs (their files come
from `sci.bban.top`). They are **third-party sites** — their pages load pop-up
and advertising scripts — so add them **after** the official ones: they are
tried only when every official address has failed.

!!! warning "Fraudulent look-alikes"
    Search results are full of sites that copy these names. Anna's Archive
    itself lists `annas-archive.su` and `annas-archive.io` as fraudulent. Add
    an address only when SLUM, Wikipedia or this page names it.

## Add them

1. **Settings → Plugins → Shadow libraries**, switch it on — the fields
   appear, and each one links the status page and the official list above.
2. In **Sci-Hub mirrors**, type the addresses you want tried, in order,
   comma-separated — for example
   `https://sci-net.xyz, https://sci-hub.ru, https://sci-hub.su, https://sci-hub.box, https://sci-hub.ren, https://sci-hub.al, https://sci-hub.ee`
   (official ones first, [alternatives](#alternative-mirrors) last).
3. In **Anna's Archive addresses**, add its addresses only if you have a
   [member key](#annas-archive-member-key): without one, its paper pages sit
   behind a browser check that ALMa cannot pass, and every fetch would spend a
   few seconds being refused.
4. **Save plugin settings**, then press **Test connection**.

## Test connection

**Test connection** fetches one well-known paywalled paper (LeCun, Bengio &
Hinton, *Deep learning*, Nature 2015) through every address, exactly as a real
fetch would, and lists each one:

| Test says | Meaning | What to do |
|---|---|---|
| serves PDFs | a fetch through this address works | keep it, near the front |
| behind a bot check | the address answers, but with a captcha or browser check ALMa does not solve | keep it last, or remove it; checks come and go |
| up, but does not have the test paper | the site sent you to its home page (Sci-Net does this for papers it lacks) | keep it — it will have other papers |
| refused (HTTP …) / unreachable — … | down, blocked by your network, or an expired certificate | remove it, or try again later |

The test runs only while the plugin is on; off, it is refused outright — see
[what is true of every plugin](index.md#what-is-true-of-every-plugin).

The same checks run on every fetch: a paper's PDF section (and the tab that
opens when you tap its PDF icon) lists, under **Sci-Hub**, what each mirror
answered, and **Activity** shows one line per mirror as it is tried.

### Without the browser

You can set this up through the same API the Settings page uses (on the Docker
install, from the machine itself). Read the configuration first and send it
back changed: the member key comes back masked (`****…`), and sending the
masked value keeps it — an empty one would delete it.

```bash
B=http://127.0.0.1:8000/api/v1/plugins/shadow_libraries
curl -s $B/config \
  | jq '.config + {scihub_mirrors: "https://sci-net.xyz, https://sci-hub.ru, https://sci-hub.su, https://sci-hub.box, https://sci-hub.ren, https://sci-hub.al, https://sci-hub.ee"}' \
  | curl -s -X PUT $B/config -H 'Content-Type: application/json' -d @-
curl -s -X PUT $B/enabled -H 'Content-Type: application/json' -d '{"enabled": true}'
curl -s -X POST $B/test   # → an Activity job
```

Follow the test with `GET /api/v1/activity/<job_id>`: its `result.results`
holds the table above.

## Anna's Archive member key

Anna's Archive gives each paying member a secret key. With it, ALMa logs in,
opens the paper's SciDB page and asks the member download API for a direct
link.

1. On a working Anna's Archive address, open **Donate** and choose a
   membership.
2. Your **Account** page (`/account`) shows the key behind **show**
   (`/account/secret_key`). Keep it private — it *is* your login.
3. In ALMa, **Show advanced controls** on the Shadow libraries card, paste it
   into **Anna's Archive member key**, and save. It is kept in the secret
   store, never in `settings.json`.

If a fetch then says *still asked for a browser check after logging in — check
the member key*, the key was not accepted; *member download refused: No
downloads left* means today's download allowance is used up.

## What ALMa does with a mirror's content

ALMa treats every mirror — official or not — and every publisher page as
hostile input. Nothing it sends is used before it has been checked:

* **Addresses.** Every address a page, a redirect or an API hands back is
  checked before ALMa connects: web addresses only, ports 80/443 only, no
  embedded passwords, no disguised hosts — and the name must resolve to public
  internet addresses only. Your own machines (`localhost`, your home network,
  your tailnet's `100.x` addresses) can never be reached through a mirror,
  and the connection is pinned to the addresses that were checked. Redirects
  are followed one at a time, each checked the same way.
* **Pages.** A mirror's page is read, never run: ALMa takes only the address
  of its PDF viewer (never its advertising links), from a page capped at 2 MB,
  in a safe character set, within a time limit.
* **Files.** A downloaded file must really be a PDF, is capped at 100 MB and
  three minutes, and is opened by the PDF reader in a separate, locked-down
  process (limited memory, CPU time and files, no access to ALMa's settings or
  keys). Anything in it that can act on its own when opened — scripts,
  program launches, form submissions, embedded files, links to files on your
  computer, data hidden before or after the PDF — is removed before the file
  is kept; Activity names what was removed. Web links and the text stay.
* **The paper check.** The file must then name *this* paper (its DOI, or its
  title) on its first pages; otherwise it is thrown away and the next address
  is tried.
* **Serving.** Your devices get the file as a PDF and nothing else, and a link
  tapped inside it does not reveal ALMa's address.

A PDF you attach or import yourself is checked and read the same way, but
kept exactly as you gave it; Activity warns if it contains active content.

## Related

* [Open-access PDFs](open-access-pdfs.md) — the legal sources, tried first
* [Reading PDFs](../user-guide/reading-pdfs.md) — opening, attaching and
  importing PDFs
* [Plugins](index.md) — what is true of every plugin
