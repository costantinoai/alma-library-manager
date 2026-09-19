---
title: Reading PDFs
description: Let ALMa find, keep and serve each paper's PDF — and open it on your phone.
---

# Reading PDFs

ALMa can keep one PDF per paper on the machine it runs on and serve it to any
device you use — your laptop, or your phone over [Tailscale](#on-your-phone).
A PDF gets there one of three ways:

* **ALMa finds it** when you tap the PDF icon on a paper, trying open-access
  sources one by one (and, only if you switch them on, shadow libraries).
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

!!! note "Shadow libraries"
    **Settings → Plugins → Shadow libraries** is a separate, opt-in source for
    Sci-Hub and Anna's Archive mirrors. It is off by default, has no addresses
    until you add them, and runs only after every open-access source has
    failed. Mirror addresses change often — use **Test** to see which of yours
    are up. Whether using these services is legal depends on where you live.

## Open a paper's PDF

* **On a paper card or a table row**, tap the small **document icon**. It opens
  a new tab: if ALMa already has the PDF, the tab becomes the PDF at once;
  otherwise it shows each source being asked, then opens the file.
* **In a paper's details**, the **PDF** section shows the kept file (pages,
  size, and whether its text confirmed the paper) with **Open PDF**, or
  **Find PDF** if there is none yet.

If nothing is found, the tab says what each source answered ("Unpaywall: no
copy listed", "Publisher page: not a PDF") and offers **Try again** or
**Attach a PDF…**. It never searches again on its own.

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
