/*
 * ALMa connector — paper identification.
 *
 * One file, three consumers:
 *   1. Injected into the open tab (scripting.executeScript) to read the
 *      page DOM — this is the Zotero-style "translator" layer.
 *   2. Loaded by the popup as a <script> so it can post-process PDF bytes.
 *   3. Required by the Node test suite (see extension/test/extract.test.js).
 *
 * It therefore attaches everything to `globalThis.almaExtract` and never
 * uses ES-module syntax, so it runs unchanged in a page, a popup, and Node.
 *
 * Identification strategy, in priority order (mirrors how Zotero finds a
 * paper from a publisher page or a PDF):
 *   A. Embedded citation metadata — Highwire (`citation_*`), Dublin Core
 *      (`dc.*` / `DC.*`), PRISM (`prism.*`), bepress, EPrints, Open Graph.
 *      This covers the large majority of publisher landing pages.
 *   B. DOI / arXiv id parsed from the page URL (publisher and preprint
 *      URLs frequently embed the identifier).
 *   C. DOI found in `doi.org` links or visible page text.
 *   D. PDF fallback: when the tab is a PDF, derive the id from the URL
 *      (arXiv/biorxiv/etc.) and, failing that, scan the PDF bytes for a
 *      DOI in XMP metadata or plain text.
 *
 * The DOI is the high-value output: ALMa resolves it via OpenAlex to get
 * canonical metadata, so we don't need to scrape perfectly — we need the
 * identifier. Scraped title/authors/year are the fallback for DOI-less
 * pages.
 */
(function (root) {
  "use strict";

  // Crossref's recommended DOI shape, case-insensitive. Kept deliberately
  // permissive on the suffix; we trim trailing punctuation afterwards.
  const DOI_CORE = "10\\.\\d{4,9}\\/[^\\s\"'<>]+";
  const DOI_ANYWHERE = new RegExp(DOI_CORE, "gi");

  // Punctuation that commonly trails a DOI in prose / HTML but isn't part
  // of it (sentence enders, closing brackets, entity starts).
  function cleanDoi(raw) {
    if (!raw) return "";
    let doi = String(raw).trim();
    // Strip leading wrapping punctuation/quotes, then a scheme / "doi:" prefix.
    doi = doi.replace(/^[([{<'"\s]+/, "");
    doi = doi.replace(/^(?:https?:\/\/(?:dx\.)?doi\.org\/)/i, "");
    doi = doi.replace(/^(?:doi:\s*)/i, "");
    doi = doi.replace(/^(?:info:doi\/)/i, "");
    // Trim trailing junk.
    doi = doi.replace(/[).,;:'"\]}>]+$/g, "");
    // Drop anything after whitespace (defensive).
    doi = doi.split(/\s/)[0];
    return /^10\.\d{4,9}\/\S+$/.test(doi) ? doi : "";
  }

  function firstDoiInText(text) {
    if (!text) return "";
    const matches = String(text).match(DOI_ANYWHERE);
    if (!matches) return "";
    for (const m of matches) {
      const doi = cleanDoi(m);
      if (doi) return doi;
    }
    return "";
  }

  // ---- arXiv ----------------------------------------------------------
  function arxivIdFromUrl(url) {
    if (!url) return "";
    // arxiv.org/abs/2310.01234  ·  /pdf/2310.01234(v2)(.pdf)  ·  old style
    // hep-th/9901001
    const m =
      url.match(/arxiv\.org\/(?:abs|pdf)\/((?:\d{4}\.\d{4,5})(?:v\d+)?)/i) ||
      url.match(/arxiv\.org\/(?:abs|pdf)\/([a-z-]+\/\d{7})(?:v\d+)?/i);
    if (!m) return "";
    return m[1].replace(/v\d+$/i, "");
  }

  function arxivToDoi(arxivId) {
    if (!arxivId) return "";
    // arXiv mints DataCite DOIs of the form 10.48550/arXiv.<id> (2022+).
    // OpenAlex indexes these, so it's the cleanest resolution key.
    return "10.48550/arXiv." + arxivId;
  }

  // ---- URL-embedded DOI ----------------------------------------------
  // Preprint hosts append a version + format to the DOI inside the URL,
  // e.g. biorxiv "…713175v1.full.pdf" or "…123456v2" — strip those so we
  // recover the bare DOI.
  function stripUrlDoiSuffix(doi) {
    return String(doi || "").replace(
      /(v\d+)?(\.full-text|\.full|\.abstract|\.article-info|\.supplementary-material)?(\.pdf|\.html?|\.xml)?$/i,
      ""
    );
  }

  function doiFromUrl(url) {
    if (!url) return "";
    let decoded = url;
    try {
      decoded = decodeURIComponent(url);
    } catch (e) {
      /* keep raw */
    }
    // 1) Explicit DOI markers: doi.org/10.x , /doi/(abs|full|pdf/)10.x , ?doi=10.x
    let m = decoded.match(
      /(?:doi\.org\/|\/doi\/(?:abs\/|full\/|pdf\/)?|[?&]doi=)(10\.\d{4,9}\/[^\s?#&]+)/i
    );
    if (m) return cleanDoi(stripUrlDoiSuffix(m[1]));

    // 2) DOI embedded directly in the path — biorxiv / medRxiv
    //    (/content/10.x/…v1.full.pdf), OSF, and similar preprint hosts.
    m = decoded.match(/\/(10\.\d{4,9}\/[^\s?#]+)/);
    if (m) return cleanDoi(stripUrlDoiSuffix(m[1]));

    return "";
  }

  function looksLikePdfUrl(url) {
    if (!url) return false;
    return (
      /\.pdf(\?|#|$)/i.test(url) ||
      /arxiv\.org\/pdf\//i.test(url) ||
      /\/doi\/pdf\//i.test(url)
    );
  }

  // ---- Meta-tag harvesting -------------------------------------------
  // Build name -> [values]. Matches both `name=` and `property=` (og:).
  function harvestMeta(doc) {
    const map = {};
    let nodes = [];
    try {
      nodes = doc.querySelectorAll("meta");
    } catch (e) {
      return map;
    }
    nodes.forEach(function (el) {
      const key = (
        el.getAttribute("name") ||
        el.getAttribute("property") ||
        ""
      )
        .trim()
        .toLowerCase();
      if (!key) return;
      const val = (el.getAttribute("content") || "").trim();
      if (!val) return;
      (map[key] || (map[key] = [])).push(val);
    });
    return map;
  }

  function firstMeta(map, keys) {
    for (const k of keys) {
      const arr = map[k.toLowerCase()];
      if (arr && arr.length && arr[0]) return arr[0];
    }
    return "";
  }

  function allMeta(map, keys) {
    for (const k of keys) {
      const arr = map[k.toLowerCase()];
      if (arr && arr.length) return arr.slice();
    }
    return [];
  }

  function yearFromString(s) {
    if (!s) return null;
    const m = String(s).match(/(19|20)\d{2}/);
    return m ? parseInt(m[0], 10) : null;
  }

  // ---- Main page extractor -------------------------------------------
  // `doc` is a Document (or test stub), `url` the page URL.
  function extractFromDocument(doc, url) {
    const meta = harvestMeta(doc);
    const detectedVia = [];

    // -- DOI (highest value) --
    let doi = cleanDoi(
      firstMeta(meta, [
        "citation_doi",
        "bepress_citation_doi",
        "dc.identifier.doi",
        "prism.doi",
        "prism:doi",
        "doi",
      ])
    );
    if (doi) detectedVia.push("citation meta");

    // dc.identifier sometimes carries "doi:10.x" among other ids.
    if (!doi) {
      for (const v of allMeta(meta, ["dc.identifier", "dc.identifier.doi"])) {
        const d = firstDoiInText(v);
        if (d) {
          doi = d;
          detectedVia.push("dc.identifier");
          break;
        }
      }
    }

    // arXiv id (also gives us a DOI if we don't have one).
    let arxivId = firstMeta(meta, ["citation_arxiv_id"]) || arxivIdFromUrl(url);
    if (arxivId) detectedVia.push("arXiv id");

    if (!doi) {
      const urlDoi = doiFromUrl(url);
      if (urlDoi) {
        doi = urlDoi;
        detectedVia.push("page URL");
      }
    }

    if (!doi && arxivId) {
      doi = arxivToDoi(arxivId);
    }

    // doi.org links, then a bounded scan of body text.
    if (!doi) {
      try {
        const links = doc.querySelectorAll('a[href*="doi.org/10."]');
        for (const a of links) {
          const d = doiFromUrl(a.getAttribute("href") || "");
          if (d) {
            doi = d;
            detectedVia.push("doi.org link");
            break;
          }
        }
      } catch (e) {
        /* no DOM links */
      }
    }
    if (!doi) {
      let bodyText = "";
      try {
        bodyText = (doc.body && doc.body.innerText) || "";
      } catch (e) {
        /* ignore */
      }
      const d = firstDoiInText(bodyText.slice(0, 20000));
      if (d) {
        doi = d;
        detectedVia.push("page text");
      }
    }

    // -- Title --
    let title = firstMeta(meta, [
      "citation_title",
      "dc.title",
      "prism.title",
      "eprints.title",
      "bepress_citation_title",
      "og:title",
      "twitter:title",
    ]);
    if (!title) {
      try {
        title = (doc.title || "").trim();
      } catch (e) {
        title = "";
      }
    }

    // -- Authors (citation_author repeats; dc.creator repeats) --
    let authorList = allMeta(meta, [
      "citation_author",
      "bepress_citation_author",
      "dc.creator",
      "dc.contributor",
    ]);
    let authors = "";
    if (authorList.length) {
      authors = authorList.join(", ");
    } else {
      // Single-field variants use ';' or ',' separators.
      authors = firstMeta(meta, ["citation_authors", "authors"]);
    }

    // -- Year --
    const year = yearFromString(
      firstMeta(meta, [
        "citation_publication_date",
        "citation_date",
        "citation_cover_date",
        "citation_online_date",
        "prism.publicationdate",
        "prism.coverdate",
        "dc.date",
        "dc.date.issued",
        "citation_year",
        "article:published_time",
      ])
    );

    // -- Journal / venue --
    const journal = firstMeta(meta, [
      "citation_journal_title",
      "prism.publicationname",
      "citation_conference_title",
      "citation_inbook_title",
      "dc.source",
      "og:site_name",
    ]);

    // -- Abstract --
    const abstract = firstMeta(meta, [
      "citation_abstract",
      "dc.description",
      "prism.teaser",
      "eprints.abstract",
      "og:description",
      "description",
    ]);

    const openalexId = firstMeta(meta, ["citation_openalex_id"]); // rare

    const isPdf =
      looksLikePdfUrl(url) ||
      (firstMeta(meta, ["citation_pdf_url"]) === url) ||
      isPdfViewer(doc);

    return {
      doi: doi || "",
      arxivId: arxivId || "",
      openalexId: openalexId || "",
      title: title || "",
      authors: authors || "",
      year: year,
      journal: journal || "",
      abstract: abstract || "",
      url: url || "",
      isPdf: !!isPdf,
      // What we managed to read; useful for the popup's "Detected via …".
      hasMetadata: !!(title || doi || authorList.length),
      detectedVia: detectedVia,
    };
  }

  // Firefox/Chrome render PDFs in a viewer whose DOM has no citation meta.
  function isPdfViewer(doc) {
    try {
      const ct =
        doc.contentType ||
        (doc.querySelector && doc.querySelector("embed[type='application/pdf']") && "application/pdf");
      if (ct === "application/pdf") return true;
      // pdf.js viewer body id.
      if (doc.getElementById && doc.getElementById("viewer")) {
        const app = doc.querySelector && doc.querySelector("#viewerContainer");
        if (app) return true;
      }
    } catch (e) {
      /* ignore */
    }
    return false;
  }

  // ---- PDF byte scanning ---------------------------------------------
  // Decode bytes as latin1 (1 byte = 1 char) and look for a DOI. XMP
  // metadata blocks and many PDF text streams are uncompressed, so this
  // catches a useful fraction without a full PDF parser. (Compressed-only
  // text needs pdf.js — see README "Known limitations".)
  function extractDoiFromPdfText(text) {
    if (!text) return "";
    // Prefer an explicit XMP DOI element if present.
    const xmp =
      text.match(/<prism:doi>\s*(10\.\d{4,9}\/[^\s<]+)\s*<\/prism:doi>/i) ||
      text.match(/<dc:identifier>\s*(?:doi:)?\s*(10\.\d{4,9}\/[^\s<]+)\s*<\/dc:identifier>/i) ||
      text.match(/\/doi\s*\(\s*(10\.\d{4,9}\/[^\s)]+)\s*\)/i); // PDF /doi info key
    if (xmp) {
      const d = cleanDoi(xmp[1]);
      if (d) return d;
    }
    return firstDoiInText(text);
  }

  function extractDoiFromPdfBytes(arrayBuffer) {
    try {
      const bytes = new Uint8Array(arrayBuffer);
      let s = "";
      // Build a latin1 string in chunks (avoid call-stack limits).
      const CHUNK = 0x8000;
      for (let i = 0; i < bytes.length; i += CHUNK) {
        s += String.fromCharCode.apply(
          null,
          bytes.subarray(i, Math.min(i + CHUNK, bytes.length))
        );
      }
      return extractDoiFromPdfText(s);
    } catch (e) {
      return "";
    }
  }

  root.almaExtract = {
    cleanDoi: cleanDoi,
    firstDoiInText: firstDoiInText,
    arxivIdFromUrl: arxivIdFromUrl,
    arxivToDoi: arxivToDoi,
    doiFromUrl: doiFromUrl,
    looksLikePdfUrl: looksLikePdfUrl,
    extractFromDocument: extractFromDocument,
    extractDoiFromPdfText: extractDoiFromPdfText,
    extractDoiFromPdfBytes: extractDoiFromPdfBytes,
  };

  // Node test support.
  if (typeof module !== "undefined" && module.exports) {
    module.exports = root.almaExtract;
  }
})(typeof globalThis !== "undefined" ? globalThis : this);
