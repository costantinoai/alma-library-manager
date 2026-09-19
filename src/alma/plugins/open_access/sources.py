"""Open-access PDF sources — each a thin reader over ALMa's existing adapter.

Every source only turns a :class:`PaperRef` into :class:`PdfCandidate` rows
(network, never the database); the core downloads, validates, verifies and
stores. Order (see :func:`build_sources`) puts direct, identifier-keyed hosts
first and guesswork last:

1. arXiv            — the paper (or its group's preprint) has an arXiv id
2. PubMed Central   — PMCID via Europe PMC, file from the PMC open-access S3 bucket
3. OpenAlex         — every location's ``pdf_url`` (free single-record lookup)
4. Unpaywall        — every OA location's PDF (needs a contact email)
5. Crossref         — the publisher's full-text links typed ``application/pdf``
6. Publisher page   — the DOI's landing page; the core follows ``citation_pdf_url``
7. OpenAlex full text (opt-in, paid) — OpenAlex's cached copy of an OA work
8. Semantic Scholar — ``openAccessPdf`` (often empty or a landing page)
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from alma.application.pdf_schema import PaperRef, PdfCandidate, SourceSkippedError

_OA_SELECT = "id,doi,best_oa_location,locations,has_content,content_urls"
_VERSIONS = {"vor": "publishedVersion", "am": "acceptedVersion"}
# Crossref full-text links on these hosts need a publisher TDM token.
_TOKEN_ONLY_HOSTS = ("api.elsevier.com", "api.wiley.com")


def _openalex_work(ref: PaperRef) -> dict | None:
    """The work's OA fields from one free OpenAlex singleton lookup (cached)."""
    from alma.openalex.http import get_client

    if ref.openalex_id:
        key = ref.openalex_id.rsplit("/", 1)[-1]
    elif ref.doi:
        key = f"doi:{ref.doi}"
    else:
        return None
    return get_client().get_singleton(key, select=_OA_SELECT)


@dataclass
class ArxivSource:
    id: str = "arxiv"
    label: str = "arXiv"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        ids = [i for i in (ref.arxiv_id, *ref.group_arxiv_ids) if i]
        # export.arxiv.org shares the `arxiv` client's 1-request-per-3-s budget.
        return [
            PdfCandidate(url=f"https://export.arxiv.org/pdf/{arxiv_id}", source_id=self.id,
                         version="submittedVersion", client="arxiv")
            for arxiv_id in dict.fromkeys(ids)
        ]


@dataclass
class PmcSource:
    id: str = "pmc"
    label: str = "PubMed Central"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        from alma.core.http_sources import get_source_http_client
        from alma.discovery.europe_pmc import lookup_by_doi

        if not ref.doi:
            return []
        record = lookup_by_doi(ref.doi)
        pmcid = str((record or {}).get("pmcid") or "").upper()
        if not pmcid.startswith("PMC"):
            return []
        listing = get_source_http_client("pmc_oa").get(
            "/", params={"list-type": "2", "prefix": f"metadata/{pmcid}."}, timeout=20
        )
        listing.raise_for_status()
        versions = [int(v) for v in re.findall(rf"<Key>metadata/{pmcid}\.(\d+)\.json</Key>", listing.text)]
        if not versions:
            return []
        name = f"{pmcid}.{max(versions)}"
        return [PdfCandidate(url=f"https://pmc-oa-opendata.s3.amazonaws.com/{name}/{name}.pdf",
                             source_id=self.id, version="publishedVersion", client="pmc_oa")]


@dataclass
class OpenAlexSource:
    id: str = "openalex"
    label: str = "OpenAlex"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        work = _openalex_work(ref)
        if not work:
            return []
        locations = [work.get("best_oa_location") or {}, *(work.get("locations") or [])]
        out: list[PdfCandidate] = []
        for loc in locations:
            url = str((loc or {}).get("pdf_url") or "").strip()
            if url and url not in {c.url for c in out}:
                out.append(PdfCandidate(url=url, source_id=self.id, version=str(loc.get("version") or ""),
                                        license=str(loc.get("license") or "")))
        return out


@dataclass
class UnpaywallSource:
    id: str = "unpaywall"
    label: str = "Unpaywall"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        from alma.discovery.unpaywall import fetch_oa_locations, unpaywall_available

        if not ref.doi:
            return []
        if not unpaywall_available():
            raise SourceSkippedError("needs a contact email — add one in Settings → Connections")
        locations = fetch_oa_locations(ref.doi)
        pdfs = [loc for loc in locations if loc.url_for_pdf]
        # A repository landing page usually advertises its PDF; the core follows it.
        pages = [loc for loc in locations if not loc.url_for_pdf and loc.host_type == "repository"
                 and loc.url_for_landing_page]
        out: list[PdfCandidate] = []
        for loc in pdfs:
            out.append(PdfCandidate(url=loc.url_for_pdf, source_id=self.id, version=loc.version,
                                    license=loc.license))
        for loc in pages:
            out.append(PdfCandidate(url=loc.url_for_landing_page, source_id=self.id, version=loc.version,
                                    license=loc.license))
        return out


@dataclass
class CrossrefSource:
    id: str = "crossref"
    label: str = "Crossref"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        from alma.discovery.crossref import fetch_work_message

        if not ref.doi:
            return []
        message = fetch_work_message(ref.doi) or {}
        out: list[PdfCandidate] = []
        for link in message.get("link") or []:
            url = str(link.get("URL") or "").strip()
            if not url or "pdf" not in str(link.get("content-type") or "").lower():
                continue
            if any(host in url for host in _TOKEN_ONLY_HOSTS):
                continue
            out.append(PdfCandidate(url=url, source_id=self.id,
                                    version=_VERSIONS.get(str(link.get("content-version") or ""), "")))
        return out


@dataclass
class PublisherPageSource:
    id: str = "publisher"
    label: str = "Publisher page"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        urls = [f"https://doi.org/{ref.doi}"] if ref.doi else []
        for extra in (ref.oa_url, ref.url):
            if extra and "doi.org/" not in extra and extra not in urls:
                urls.append(extra)
        return [PdfCandidate(url=u, source_id=self.id) for u in urls]


@dataclass
class OpenAlexContentSource:
    id: str = "openalex_content"
    label: str = "OpenAlex full text"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        from alma.config import get_openalex_api_key
        from alma.openalex.http import get_client

        if not get_openalex_api_key():
            raise SourceSkippedError("needs an OpenAlex API key — add one in Settings → Connections")
        client = get_client()
        work = _openalex_work(ref)
        if not work or not (work.get("has_content") or {}).get("pdf"):
            return []
        work_id = str(work.get("id") or "").rsplit("/", 1)[-1]
        url = str((work.get("content_urls") or {}).get("pdf") or "")
        return [PdfCandidate(url=url, source_id=self.id,
                             opener=lambda: client.download_content(work_id))]


@dataclass
class SemanticScholarSource:
    id: str = "semantic_scholar"
    label: str = "Semantic Scholar"
    tier: str = "open"

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        from alma.discovery.semantic_scholar import fetch_open_access_pdf

        if ref.doi:
            lookup = f"DOI:{ref.doi}"
        elif ref.arxiv_id:
            lookup = f"ARXIV:{ref.arxiv_id}"
        elif ref.semantic_scholar_id:
            lookup = ref.semantic_scholar_id
        else:
            return []
        pdf = fetch_open_access_pdf(lookup)
        if not pdf:
            return []
        return [PdfCandidate(url=str(pdf["url"]), source_id=self.id, license=str(pdf.get("license") or ""))]


def build_sources(*, use_openalex_content: bool) -> list:
    """The plugin's sources in run order (see module docstring)."""
    sources: list = [
        ArxivSource(),
        PmcSource(),
        OpenAlexSource(),
        UnpaywallSource(),
        CrossrefSource(),
        PublisherPageSource(),
    ]
    if use_openalex_content:
        sources.append(OpenAlexContentSource())
    sources.append(SemanticScholarSource())
    return sources
