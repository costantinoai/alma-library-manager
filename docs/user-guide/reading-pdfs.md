---
title: Reading PDFs
description: Let ALMa find, keep and serve each paper's PDF — and open it on your phone.
---

# Reading PDFs

ALMa can keep one PDF per paper on the machine it runs on and serve it to any
device you use — your laptop, or your phone over [Tailscale](#on-your-phone).
A PDF gets there one of three ways:

* **ALMa finds it** when you tap the PDF icon on a paper, trying open-access
  sources one by one (and, only if you switch them on,
  [shadow libraries](#shadow-libraries)).
* **You attach it** to a paper you already have.
* **You import it**: drop PDFs you already own and ALMa works out which paper
  each one is, saves that paper to your Library, and keeps the file with it.

Every one of these runs as an operation you can follow in **Activity**, step by
step ("Asking Unpaywall…").

## Turn on a PDF source

Finding PDFs is off until you switch a source on:

1. **Settings → Plugins → Open-access PDFs**, switch it on.
2. (Recommended) Add a **contact email** in Settings → Connections — Unpaywall,
   one of the best sources, only answers requests that carry one.

Open-access PDFs tries, in this order: **arXiv**, **PubMed Central**,
**OpenAlex**, **Unpaywall**, **Crossref**, the **publisher's own page**, and
**Semantic Scholar**. It stops at the first file whose text names the paper.
Under *advanced*, *Use OpenAlex's cached PDFs* adds OpenAlex's own copies of
open-access papers as a last resort — each costs $0.01 of your OpenAlex budget
and needs an OpenAlex API key.

## Shadow libraries

**Settings → Plugins → Shadow libraries** is a separate, opt-in source for
Sci-Hub and Anna's Archive. It is off by default, ships with no addresses,
runs only after every open-access source has failed, and only when you ask for
a paper's PDF. Whether using these services is legal depends on where you
live.

### Find working addresses

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

!!! warning "Look-alike sites"
    Search results are full of sites that copy these names. Anna's Archive
    itself lists `annas-archive.su` and `annas-archive.io` as fraudulent; the
    `sci-hub.ren` / `.al` / `.ee` family is a third-party copy whose pages load
    pop-up and advertising scripts. Add only addresses that SLUM or Wikipedia list.

### Add them

1. **Settings → Plugins → Shadow libraries**. Each field links the status
   page and the official list above.
2. In **Sci-Hub mirrors**, type the addresses you want tried, in order,
   comma-separated — for example
   `https://sci-net.xyz, https://sci-hub.ru, https://sci-hub.su, https://sci-hub.box`.
   Put first the ones that work from your network.
3. In **Anna's Archive addresses**, add its addresses only if you have a
   [member key](#annas-archive-member-key): without one, its paper pages sit
   behind a browser check that ALMa cannot pass, and every fetch would spend a
   few seconds being refused.
4. **Save plugin settings**, switch the plugin **on**, and press **Test
   connection**.

**Test connection** fetches one well-known paywalled paper (LeCun, Bengio &
Hinton, *Deep learning*, Nature 2015) through every address, exactly as a real
fetch would, and lists each one:

| Test says | Meaning | What to do |
|---|---|---|
| serves PDFs | a fetch through this address works | keep it, near the front |
| behind a bot check | the address answers, but with a captcha or browser check ALMa does not solve | keep it last, or remove it; checks come and go |
| up, but does not have the test paper | the site sent you to its home page (Sci-Net does this for papers it lacks) | keep it — it will have other papers |
| refused (HTTP …) / unreachable — … | down, blocked by your network, or an expired certificate | remove it, or try again later |

The same checks run on every fetch: a paper's PDF section (and the tab that
opens when you tap its PDF icon) lists, under **Sci-Hub**, what each mirror
answered, and **Activity** shows one line per mirror as it is tried.

Setting it up without the browser, through the same API the Settings page
uses (on the Docker install, from the machine itself):

```bash
curl -X PUT http://127.0.0.1:8000/api/v1/plugins/shadow_libraries/config \
  -H 'Content-Type: application/json' \
  -d '{"scihub_mirrors": "https://sci-net.xyz, https://sci-hub.ru, https://sci-hub.su, https://sci-hub.box",
       "annas_archive_domains": "", "member_key": ""}'
curl -X PUT http://127.0.0.1:8000/api/v1/plugins/shadow_libraries/enabled \
  -H 'Content-Type: application/json' -d '{"enabled": true}'
curl -X POST http://127.0.0.1:8000/api/v1/plugins/shadow_libraries/test   # → an Activity job
```

Follow the test with `GET /api/v1/activity/<job_id>`: its `result.results`
holds the table above.

### Anna's Archive member key

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

## Open a paper's PDF

* **On a paper card or a table row**, tap the small **document icon**. It opens
  a new tab: if ALMa already has the PDF, the tab becomes the PDF at once;
  otherwise it shows each source being asked, then opens the file.
* **In a paper's details**, the **PDF** section shows the kept file (pages,
  size, and whether its text confirmed the paper) with **Open PDF**, or
  **Find PDF** if there is none yet.

If nothing is found, the tab says what each source answered ("Unpaywall: no
copy listed", "Publisher page: not a PDF" — and, for a source with several
mirrors, what each mirror said) and offers **Try again** or **Attach a
PDF…**. It never searches again on its own.

## Attach your own PDF

In a paper's details, **Attach PDF…** (or **Replace…**) uploads a file from
your computer or phone. Your choice wins: if the file's text names a different
paper, it is still kept but marked *PDF names another paper*.

**Wrong PDF** removes the kept file and remembers it, so ALMa never picks that
exact file for this paper again.

## Import PDFs you already have

**Library → Import Papers → PDF files**: drop one or many PDFs. For each one
ALMa:

1. reads the DOI (or arXiv id, or title) from the file;
2. looks for that paper **among the papers you already have** — an existing
   paper always gets the file; importing never creates a duplicate;
3. otherwise finds the paper online and saves it to your Library;
4. keeps the PDF with it.

A file it cannot place (a scan with no text, for example) waits on its row:
type its DOI or title and press **Retry**. The upload is kept for a day.

## Where the files live

PDFs are stored next to your database in a folder you can browse:

```
pdfs/
  2024/Maniquet 2024 - Recurrent issues with deep neural network models.pdf
  undated/Doe - A paper without a year.pdf
```

In the Docker install that is `/app/data/pdfs` on the `alma-data` volume. A
file is named once, when it is stored, and never overwritten; a daily
housekeeping pass removes files no paper points to any more. Backups
(`Settings → Library management`) cover the database, not the PDFs — they can
always be fetched or attached again.

## On your phone

ALMa has no login of its own and listens only on the computer it runs on, so
reach it from your phone over [Tailscale](https://tailscale.com/), which only
lets your own devices in. With the Docker install on `127.0.0.1:8000`, run on
that computer:

```
tailscale serve --bg 8000
```

and open the `https://<machine>.<tailnet>.ts.net` address it prints on your
phone. Tap a paper's PDF icon: the file opens in the phone's own PDF viewer,
where you can read it, save it to Files or share it.
