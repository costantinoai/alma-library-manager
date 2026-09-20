---
title: Open-access PDFs
description: Switch on the legal sources ALMa tries when you ask for a paper's PDF.
---

# Open-access PDFs

Keeping a paper's PDF is core to ALMa. This plugin supplies the legal sources
it searches when you tap a paper's PDF icon and it has no file yet. Until you
switch it on, ALMa looks nowhere — it only serves files it already has, or ones
you attach yourself.

## Turn it on

1. **Settings → Plugins → Open-access PDFs**, switch it on.
2. (Recommended) Add a **contact email** in **Settings → Connections**.
   Unpaywall, one of the best sources, only answers requests that carry one;
   without it the plugin card says so and that source sits out.

There is nothing else to configure, and no connection test: none of the sources
needs an account.

## What it tries, in order

**arXiv**, **PubMed Central**, **OpenAlex**, **Unpaywall**, **Crossref**, the
**publisher's own page**, and **Semantic Scholar**. ALMa stops at the first
file whose text names the paper — a download that turns out to be a different
article, or a landing page dressed as a PDF, is discarded and the next source
is tried.

Every attempt is an operation you can follow in **Activity**, step by step
("Asking Unpaywall…"), and the tab that opens when you tap the icon lists what
each source answered.

## Advanced: OpenAlex's cached PDFs

Under **Show advanced controls**, *Use OpenAlex's cached PDFs* adds OpenAlex's
own copies of open-access papers as a last resort, for when a publisher blocks
the download. Each PDF costs $0.01 of your OpenAlex budget and needs an OpenAlex
API key, so it is off unless you ask for it.

## When everything here fails

The tab tells you what each source answered and offers **Try again** or
**Attach a PDF…**. It never searches again on its own.

If you also switch on [Shadow libraries](shadow-libraries.md), those addresses
are tried after every source on this page has failed — never before.

## Related

* [Reading PDFs](../user-guide/reading-pdfs.md) — opening, attaching and
  importing PDFs, and reading them on your phone
* [Plugins](index.md) — what is true of every plugin
