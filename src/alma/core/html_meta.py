"""One stdlib HTML reader for the metadata publishers put in their pages.

Several flows read the same few things out of an arbitrary publisher page:
abstract recovery reads ``<meta name="citation_abstract">`` & co., the PDF
pipeline reads ``citation_pdf_url`` and the ``<object>/<embed>/<iframe>``
a viewer page wraps its PDF in, and a mirror page is read the same way. This
module parses a page ONCE into a :class:`HtmlSnapshot` and offers small
projections over it, so no caller hand-rolls another ``HTMLParser`` subclass.

Stdlib only (``html.parser``) — no bs4/lxml dependency in core. Attribute
values arrive already entity-decoded (``convert_charrefs``). Parsing never
raises: malformed markup yields whatever was read before the error.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

# Tags whose attributes the snapshot keeps. Anything else is skipped, so a
# multi-megabyte page costs a single pass and a small result.
_KEPT_TAGS = frozenset({"meta", "link", "iframe", "embed", "object", "a"})
# A hostile page can carry any number of tags; keep bounded lists (anchors,
# the noisiest, tighter still) so a 2 MB page costs one pass and a small result.
_MAX_ANCHORS = 500
_MAX_TAGS = 2000


@dataclass(frozen=True)
class HtmlSnapshot:
    """The metadata-bearing tags of one page, in document order.

    ``metas`` pairs a lower-cased ``name``/``property``/``http-equiv`` with its
    ``content``. The other tuples hold each tag's attributes (lower-cased
    names) in document order.
    """

    metas: tuple[tuple[str, str], ...] = ()
    links: tuple[dict[str, str], ...] = ()
    embeds: tuple[tuple[str, dict[str, str]], ...] = ()  # iframe / embed / object
    anchors: tuple[dict[str, str], ...] = ()
    title: str = ""


class _Collector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.metas: list[tuple[str, str]] = []
        self.links: list[dict[str, str]] = []
        self.embeds: list[tuple[str, dict[str, str]]] = []
        self.anchors: list[dict[str, str]] = []
        self.title_parts: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "title":
            self._in_title = True
            return
        if tag not in _KEPT_TAGS:
            return
        attr_map = {str(k).lower(): (v or "").strip() for k, v in attrs}
        if tag == "meta":
            name = (
                attr_map.get("name") or attr_map.get("property") or attr_map.get("http-equiv") or ""
            ).lower()
            if name and len(self.metas) < _MAX_TAGS:
                self.metas.append((name, attr_map.get("content", "")))
        elif tag == "link":
            if len(self.links) < _MAX_TAGS:
                self.links.append(attr_map)
        elif tag == "a":
            if len(self.anchors) < _MAX_ANCHORS and attr_map.get("href"):
                self.anchors.append(attr_map)
        elif len(self.embeds) < _MAX_TAGS:
            self.embeds.append((tag, attr_map))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)


def parse_html(text: str) -> HtmlSnapshot:
    """Parse ``text`` once into a :class:`HtmlSnapshot`. Never raises."""
    collector = _Collector()
    try:
        collector.feed(text or "")
        collector.close()
    except Exception:  # malformed markup: keep what was read before the error
        pass
    return HtmlSnapshot(
        metas=tuple(collector.metas),
        links=tuple(collector.links),
        embeds=tuple(collector.embeds),
        anchors=tuple(collector.anchors),
        title=" ".join("".join(collector.title_parts).split()),
    )


def meta_values(snapshot: HtmlSnapshot, names: Iterable[str]) -> list[str]:
    """``content`` of every ``<meta>`` whose name is in ``names``, in page order."""
    wanted = {n.lower() for n in names}
    return [content for name, content in snapshot.metas if name in wanted and content]


def absolute_url(raw: str, base_url: str) -> str:
    """Resolve an attribute URL against the page it came from.

    Handles protocol-relative ``//host/x`` and relative paths, and drops the
    fragment (viewer hints like ``#view=FitH`` are not part of the resource).
    Returns ``""`` — never raises — for anything that is not a plain http(s)
    URL after resolution (malformed, credentials, backslashes). The fetch
    itself vets the address again (``core.url_safety``).
    """
    value = (raw or "").strip()
    if not value or "\\" in value or value.lower().startswith(("javascript:", "data:", "mailto:")):
        return ""
    try:
        resolved = urljoin(base_url, value) if base_url else value
        parts = urlsplit(resolved)
        parts.port  # noqa: B018 — raises ValueError on a malformed port
    except ValueError:  # "http://[x", ":99999" — a hostile page's malformed link
        return ""
    if parts.scheme not in ("http", "https") or not parts.netloc or "@" in parts.netloc:
        return ""
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))


def _viewer_target(url: str) -> str:
    """The PDF a pdf.js viewer URL displays (``…/viewer.html?file=<url>``), else ``""``.

    Repositories and Anna's Archive wrap their PDF in Mozilla's pdf.js viewer;
    the viewer page itself is HTML, the file it opens is the PDF.
    """
    if not url:
        return ""
    parts = urlsplit(url)
    if not parts.path.lower().endswith("/viewer.html"):
        return ""
    target = (parse_qs(parts.query).get("file") or [""])[0]
    return absolute_url(target, url) if target else ""


def pdf_urls(snapshot: HtmlSnapshot, base_url: str, *, include_anchors: bool = True) -> list[str]:
    """Every URL the page advertises as its PDF, most authoritative first.

    Order: ``citation_pdf_url`` (the Google Scholar / Highwire convention),
    ``<link type="application/pdf">``, a PDF ``<object data>``, the viewer
    element ``#pdf`` (iframe/embed), any ``<embed>``/``<iframe>`` typed or
    named as a PDF or wrapping one in a pdf.js viewer, then anchors whose
    target ends in ``.pdf``. A pdf.js viewer URL is replaced by the file it
    opens. Deduplicated, absolute, fragment-free. ``include_anchors=False``
    skips the anchors (a mirror page's anchors are its advertising).
    """
    ordered: list[str] = []

    def add(raw: str) -> None:
        url = absolute_url(raw, base_url)
        url = _viewer_target(url) or url
        if url and url not in ordered:
            ordered.append(url)

    for value in meta_values(snapshot, ("citation_pdf_url",)):
        add(value)
    for attrs in snapshot.links:
        if attrs.get("type", "").lower() == "application/pdf":
            add(attrs.get("href", ""))
    for tag, attrs in snapshot.embeds:
        if tag == "object" and attrs.get("type", "").lower() == "application/pdf":
            add(attrs.get("data", ""))
    for tag, attrs in snapshot.embeds:
        if tag in ("iframe", "embed") and attrs.get("id", "").lower() == "pdf":
            add(attrs.get("src", ""))
    for tag, attrs in snapshot.embeds:
        src = attrs.get("src", "") or attrs.get("data", "")
        typed_pdf = attrs.get("type", "").lower() == "application/pdf"
        resolved = absolute_url(src, base_url)
        looks_pdf = bool(resolved) and urlsplit(resolved).path.lower().endswith(".pdf")
        in_viewer = bool(_viewer_target(resolved))
        if tag in ("iframe", "embed", "object") and (typed_pdf or looks_pdf or in_viewer):
            add(src)
    if include_anchors:
        for attrs in snapshot.anchors:
            href = attrs.get("href", "")
            if absolute_url(href, base_url) and urlsplit(href).path.lower().endswith(".pdf"):
                add(href)
    return ordered
