/*
 * Node test suite for the connector's paper-identification logic
 * (extension/lib/extract.js). No browser, no deps — a tiny Document stub
 * feeds realistic publisher / arXiv / PDF fixtures through the same code
 * that runs injected in the page.
 *
 * Run: node extension/test/extract.test.js
 */
"use strict";

const assert = require("assert");
const A = require("../lib/extract.js");

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed++;
    console.log("  ok  - " + name);
  } catch (e) {
    console.error("FAIL  - " + name + "\n        " + e.message);
    process.exitCode = 1;
  }
}

// --- Minimal Document stub ------------------------------------------------
function meta(attrs) {
  return {
    getAttribute(k) {
      return attrs[k] !== undefined ? attrs[k] : null;
    },
  };
}
function link(href) {
  return { getAttribute: (k) => (k === "href" ? href : null) };
}
function makeDoc({ metas = [], links = [], body = "", title = "", contentType = "" } = {}) {
  return {
    title,
    contentType,
    body: { innerText: body },
    getElementById: () => null,
    querySelector: () => null,
    querySelectorAll(sel) {
      if (sel === "meta") return metas;
      if (sel.indexOf("a[href") === 0) return links;
      return [];
    },
  };
}

// --- DOI cleaning ---------------------------------------------------------
test("cleanDoi strips scheme, doi: prefix and trailing punctuation", () => {
  assert.strictEqual(A.cleanDoi("https://doi.org/10.1038/s41586-021-03819-2"), "10.1038/s41586-021-03819-2");
  assert.strictEqual(A.cleanDoi("doi:10.1145/3292500.3330701."), "10.1145/3292500.3330701");
  assert.strictEqual(A.cleanDoi("(10.1101/2020.01.01.123456)"), "10.1101/2020.01.01.123456");
  assert.strictEqual(A.cleanDoi("not a doi"), "");
});

// --- Highwire / Dublin Core publisher page --------------------------------
test("publisher page with citation_* meta yields full metadata", () => {
  const doc = makeDoc({
    title: "Some Nature page | Nature",
    metas: [
      meta({ name: "citation_doi", content: "10.1038/s41586-021-03819-2" }),
      meta({ name: "citation_title", content: "Highly accurate protein structure prediction" }),
      meta({ name: "citation_author", content: "Jumper, John" }),
      meta({ name: "citation_author", content: "Evans, Richard" }),
      meta({ name: "citation_author", content: "Pritzel, Alexander" }),
      meta({ name: "citation_journal_title", content: "Nature" }),
      meta({ name: "citation_publication_date", content: "2021/08/26" }),
      meta({ name: "citation_abstract", content: "Proteins are essential ..." }),
    ],
  });
  const r = A.extractFromDocument(doc, "https://www.nature.com/articles/s41586-021-03819-2");
  assert.strictEqual(r.doi, "10.1038/s41586-021-03819-2");
  assert.strictEqual(r.title, "Highly accurate protein structure prediction");
  assert.strictEqual(r.authors, "Jumper, John, Evans, Richard, Pritzel, Alexander");
  assert.strictEqual(r.journal, "Nature");
  assert.strictEqual(r.year, 2021);
  assert.ok(r.abstract.startsWith("Proteins"));
  assert.ok(r.hasMetadata);
});

// --- Dublin Core "doi:" identifier ---------------------------------------
test("dc.identifier carrying doi: is recognised", () => {
  const doc = makeDoc({
    metas: [
      meta({ name: "DC.identifier", content: "doi:10.1371/journal.pone.0123456" }),
      meta({ name: "DC.title", content: "A PLOS paper" }),
    ],
  });
  const r = A.extractFromDocument(doc, "https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0123456");
  assert.strictEqual(r.doi, "10.1371/journal.pone.0123456");
  assert.strictEqual(r.title, "A PLOS paper");
});

// --- arXiv abstract page --------------------------------------------------
test("arXiv abs URL resolves to a 10.48550 DOI", () => {
  const doc = makeDoc({
    title: "[1706.03762] Attention Is All You Need",
    metas: [meta({ name: "citation_title", content: "Attention Is All You Need" })],
  });
  const r = A.extractFromDocument(doc, "https://arxiv.org/abs/1706.03762");
  assert.strictEqual(r.arxivId, "1706.03762");
  assert.strictEqual(r.doi, "10.48550/arXiv.1706.03762");
  assert.strictEqual(r.title, "Attention Is All You Need");
});

// --- arXiv PDF viewer (no meta) ------------------------------------------
test("arXiv PDF URL (no meta) still identifies the paper", () => {
  const doc = makeDoc({ contentType: "application/pdf" });
  const r = A.extractFromDocument(doc, "https://arxiv.org/pdf/1706.03762v5");
  assert.strictEqual(r.arxivId, "1706.03762");
  assert.strictEqual(r.doi, "10.48550/arXiv.1706.03762");
  assert.strictEqual(r.isPdf, true);
});

// --- DOI from publisher URL path -----------------------------------------
test("DOI embedded in a /doi/ URL is parsed when meta is absent", () => {
  const doc = makeDoc({});
  const r = A.extractFromDocument(doc, "https://dl.acm.org/doi/10.1145/3292500.3330701");
  assert.strictEqual(r.doi, "10.1145/3292500.3330701");
});

test("doi.org URL is parsed", () => {
  const doc = makeDoc({});
  const r = A.extractFromDocument(doc, "https://doi.org/10.1109/CVPR.2016.90");
  assert.strictEqual(r.doi, "10.1109/CVPR.2016.90");
});

// --- Preprint hosts: DOI embedded in the path + version/format suffix -----
test("biorxiv .full.pdf URL → DOI with version/format suffix stripped", () => {
  assert.strictEqual(
    A.doiFromUrl("https://www.biorxiv.org/content/10.64898/2026.03.23.713175v1.full.pdf"),
    "10.64898/2026.03.23.713175"
  );
});

test("biorxiv abstract URL strips the version suffix", () => {
  assert.strictEqual(
    A.doiFromUrl("https://www.biorxiv.org/content/10.1101/2020.01.01.123456v2"),
    "10.1101/2020.01.01.123456"
  );
});

test("medrxiv .full URL strips suffix", () => {
  assert.strictEqual(
    A.doiFromUrl("https://www.medrxiv.org/content/10.1101/2021.05.05.21256000v1.full"),
    "10.1101/2021.05.05.21256000"
  );
});

test("a non-DOI article URL yields no DOI (no false match)", () => {
  assert.strictEqual(A.doiFromUrl("https://www.nature.com/articles/s41586-021-03819-2"), "");
});

// --- Non-preprint publisher PDFs with the DOI in the URL path -------------
test("Wiley /doi/pdf/ URL", () => {
  assert.strictEqual(A.doiFromUrl("https://onlinelibrary.wiley.com/doi/pdf/10.1002/anie.202012345"), "10.1002/anie.202012345");
});

test("Wiley /doi/epdf/ URL (caught by generic path match)", () => {
  assert.strictEqual(A.doiFromUrl("https://onlinelibrary.wiley.com/doi/epdf/10.1002/anie.202012345"), "10.1002/anie.202012345");
});

test("publisher PDF with DOI in path + .pdf suffix", () => {
  assert.strictEqual(A.doiFromUrl("https://example.com/articles/10.5555/abc.123.pdf"), "10.5555/abc.123");
});

test("biorxiv PDF page (no meta) is identified from the URL", () => {
  const doc = makeDoc({ contentType: "application/pdf" });
  const r = A.extractFromDocument(doc, "https://www.biorxiv.org/content/10.64898/2026.03.23.713175v1.full.pdf");
  assert.strictEqual(r.doi, "10.64898/2026.03.23.713175");
  assert.strictEqual(r.isPdf, true);
  assert.ok(r.detectedVia.includes("page URL"));
});

// --- doi.org link in body -------------------------------------------------
test("doi.org link in the page body is used as fallback", () => {
  const doc = makeDoc({
    links: [link("https://doi.org/10.5555/abcd.1234")],
  });
  const r = A.extractFromDocument(doc, "https://example.org/landing");
  assert.strictEqual(r.doi, "10.5555/abcd.1234");
});

// --- DOI in visible text --------------------------------------------------
test("DOI in page text is found as last resort", () => {
  const doc = makeDoc({ body: "Cite as: https://doi.org/10.1016/j.cell.2020.01.001 (2020)." });
  const r = A.extractFromDocument(doc, "https://example.org/x");
  assert.strictEqual(r.doi, "10.1016/j.cell.2020.01.001");
});

// --- PDF byte scanning ----------------------------------------------------
test("extractDoiFromPdfText finds XMP prism:doi", () => {
  const xmp = '<rdf:Description><prism:doi>10.7717/peerj.4375</prism:doi></rdf:Description>';
  assert.strictEqual(A.extractDoiFromPdfText(xmp), "10.7717/peerj.4375");
});

test("extractDoiFromPdfText finds plain-text DOI in PDF stream", () => {
  const stream = "BT /F1 10 Tf (https://doi.org/10.1093/nar/gkaa1100) Tj ET";
  assert.strictEqual(A.extractDoiFromPdfText(stream), "10.1093/nar/gkaa1100");
});

test("extractDoiFromPdfBytes decodes an ArrayBuffer", () => {
  const text = "%PDF-1.5 ... doi:10.1162/neco.1997.9.8.1735 ...";
  const buf = new Uint8Array([...text].map((c) => c.charCodeAt(0))).buffer;
  assert.strictEqual(A.extractDoiFromPdfBytes(buf), "10.1162/neco.1997.9.8.1735");
});

// --- URL classifier -------------------------------------------------------
test("looksLikePdfUrl", () => {
  assert.ok(A.looksLikePdfUrl("https://x.org/a.pdf"));
  assert.ok(A.looksLikePdfUrl("https://arxiv.org/pdf/2310.01234"));
  assert.ok(A.looksLikePdfUrl("https://onlinelibrary.wiley.com/doi/pdf/10.1002/x"));
  assert.ok(!A.looksLikePdfUrl("https://x.org/article/123"));
});

// --- Toolbar badge predicate (background.js uses doiFromUrl||arxivIdFromUrl) -
test("badge predicate: paper URLs are detected, others are not", () => {
  const has = (u) => !!(A.doiFromUrl(u) || A.arxivIdFromUrl(u));
  ["https://arxiv.org/abs/1706.03762",
   "https://doi.org/10.1038/s41586-021-03819-2",
   "https://dl.acm.org/doi/10.1145/3292500.3330701",
   "https://www.biorxiv.org/content/10.64898/2026.03.23.713175v1.full.pdf",
  ].forEach((u) => assert.ok(has(u), "should badge: " + u));
  ["https://www.google.com/search?q=x",
   "https://news.ycombinator.com/",
   "https://www.nature.com/articles/s41586-021-03819-2", // DOI only in meta, not URL
  ].forEach((u) => assert.ok(!has(u), "should not badge: " + u));
});

console.log("\n" + passed + " checks passed.");
