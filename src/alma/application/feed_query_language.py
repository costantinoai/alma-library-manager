"""Boolean keyword expressions for Feed query monitors."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import re

_WORD_RE = re.compile(r"[a-z0-9]+")


class FeedQuerySyntaxError(ValueError):
    """Raised when a keyword monitor expression is malformed."""


def _normalize_word(value: str) -> str:
    token = str(value or "").strip().lower()
    return token


def _literal_terms(value: str) -> tuple[str, ...]:
    terms = tuple(
        term
        for term in (_normalize_word(token) for token in _WORD_RE.findall(str(value or "").lower()))
        if term
    )
    if not terms:
        raise FeedQuerySyntaxError("Keyword expressions cannot contain empty terms")
    return terms


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str


@dataclass(frozen=True)
class _Literal:
    surface: str
    terms: tuple[str, ...]


@dataclass(frozen=True)
class _And:
    children: tuple["_Expr", ...]


@dataclass(frozen=True)
class _Or:
    children: tuple["_Expr", ...]


@dataclass(frozen=True)
class _Not:
    child: "_Expr"


_Expr = _Literal | _And | _Or | _Not


def _tokenize(expression: str) -> tuple[_Token, ...]:
    tokens: list[_Token] = []
    text = str(expression or "")
    idx = 0
    length = len(text)
    while idx < length:
        char = text[idx]
        if char.isspace():
            idx += 1
            continue
        if char == "(":
            tokens.append(_Token("LPAREN", "("))
            idx += 1
            continue
        if char == ")":
            tokens.append(_Token("RPAREN", ")"))
            idx += 1
            continue
        if char == '"':
            end = idx + 1
            while end < length and text[end] != '"':
                end += 1
            if end >= length:
                raise FeedQuerySyntaxError("Unterminated quoted phrase in keyword expression")
            inner = " ".join(text[idx + 1:end].split())
            if not inner:
                raise FeedQuerySyntaxError("Quoted keyword phrases cannot be empty")
            tokens.append(_Token("LITERAL", f'"{inner}"'))
            idx = end + 1
            continue

        end = idx
        while end < length and (not text[end].isspace()) and text[end] not in "()":
            end += 1
        raw = text[idx:end]
        upper = raw.upper()
        if upper in {"AND", "OR", "NOT"}:
            tokens.append(_Token(upper, upper))
        else:
            tokens.append(_Token("LITERAL", raw))
        idx = end
    return tuple(tokens)


class _Parser:
    def __init__(self, tokens: tuple[_Token, ...]) -> None:
        self._tokens = tokens
        self._idx = 0

    def parse(self) -> _Expr:
        if not self._tokens:
            raise FeedQuerySyntaxError("Keyword expression cannot be empty")
        node = self._parse_or()
        if self._peek() is not None:
            token = self._peek()
            raise FeedQuerySyntaxError(f"Unexpected token '{token.value}' in keyword expression")
        return node

    def _peek(self) -> _Token | None:
        if self._idx >= len(self._tokens):
            return None
        return self._tokens[self._idx]

    def _consume(self, kind: str | None = None) -> _Token:
        token = self._peek()
        if token is None:
            raise FeedQuerySyntaxError("Unexpected end of keyword expression")
        if kind is not None and token.kind != kind:
            raise FeedQuerySyntaxError(f"Expected {kind}, found '{token.value}'")
        self._idx += 1
        return token

    def _parse_or(self) -> _Expr:
        children = [self._parse_and()]
        while self._peek() is not None and self._peek().kind == "OR":
            self._consume("OR")
            children.append(self._parse_and())
        if len(children) == 1:
            return children[0]
        flattened: list[_Expr] = []
        for child in children:
            if isinstance(child, _Or):
                flattened.extend(child.children)
            else:
                flattened.append(child)
        return _Or(tuple(flattened))

    def _parse_and(self) -> _Expr:
        children = [self._parse_not()]
        while True:
            token = self._peek()
            if token is None or token.kind in {"OR", "RPAREN"}:
                break
            if token.kind == "AND":
                self._consume("AND")
                token = self._peek()
                if token is None or token.kind in {"AND", "OR", "RPAREN"}:
                    raise FeedQuerySyntaxError("AND must be followed by another keyword term")
            children.append(self._parse_not())
        if len(children) == 1:
            return children[0]
        flattened: list[_Expr] = []
        for child in children:
            if isinstance(child, _And):
                flattened.extend(child.children)
            else:
                flattened.append(child)
        return _And(tuple(flattened))

    def _parse_not(self) -> _Expr:
        token = self._peek()
        if token is not None and token.kind == "NOT":
            self._consume("NOT")
            child = self._parse_not()
            return _Not(child)
        return self._parse_primary()

    def _parse_primary(self) -> _Expr:
        token = self._peek()
        if token is None:
            raise FeedQuerySyntaxError("Keyword expression ended before a term was found")
        if token.kind == "LPAREN":
            self._consume("LPAREN")
            node = self._parse_or()
            if self._peek() is None or self._peek().kind != "RPAREN":
                raise FeedQuerySyntaxError("Unbalanced parentheses in keyword expression")
            self._consume("RPAREN")
            return node
        if token.kind != "LITERAL":
            raise FeedQuerySyntaxError(f"Unexpected token '{token.value}' in keyword expression")
        literal = self._consume("LITERAL").value
        terms = _literal_terms(literal)
        return _Literal(surface=literal, terms=terms)


def _render(node: _Expr, parent_prec: int = 0) -> str:
    if isinstance(node, _Literal):
        return node.surface
    if isinstance(node, _Not):
        rendered = _render(node.child, 3)
        if isinstance(node.child, (_And, _Or)):
            rendered = f"({rendered})"
        return f"NOT {rendered}"
    if isinstance(node, _And):
        rendered = " AND ".join(_render(child, 2) for child in node.children)
        return f"({rendered})" if parent_prec > 2 else rendered
    if isinstance(node, _Or):
        rendered = " OR ".join(_render(child, 1) for child in node.children)
        return f"({rendered})" if parent_prec > 1 else rendered
    raise TypeError(f"Unsupported keyword expression node: {type(node)!r}")


def _collect_positive_literals(node: _Expr, *, negated: bool = False) -> list[tuple[str, ...]]:
    if isinstance(node, _Literal):
        return [] if negated else [node.terms]
    if isinstance(node, _Not):
        return _collect_positive_literals(node.child, negated=not negated)
    if isinstance(node, (_And, _Or)):
        out: list[tuple[str, ...]] = []
        for child in node.children:
            out.extend(_collect_positive_literals(child, negated=negated))
        return out
    return []


def _compile(expression: str) -> tuple[str, _Expr, tuple[tuple[str, ...], ...]]:
    parser = _Parser(_tokenize(expression))
    ast = parser.parse()
    normalized = _render(ast)
    positive_literals = tuple(_collect_positive_literals(ast))
    if not positive_literals:
        raise FeedQuerySyntaxError("Keyword expression must include at least one non-NOT term")
    return normalized, ast, positive_literals


@lru_cache(maxsize=256)
def _compile_cached(expression: str) -> tuple[str, _Expr, tuple[tuple[str, ...], ...]]:
    return _compile(expression)


def normalize_keyword_expression(expression: str) -> str:
    """Validate and normalize a boolean keyword expression."""
    normalized, _, _ = _compile_cached(str(expression or ""))
    return normalized


def keyword_retrieval_query(expression: str) -> str:
    """Build a broad retrieval query from the positive literals in the expression."""
    _, _, positive_literals = _compile_cached(str(expression or ""))
    parts: list[str] = []
    seen: set[str] = set()
    for literal in positive_literals:
        for term in literal:
            if term in seen:
                continue
            seen.add(term)
            parts.append(term)
    return " ".join(parts)


def keyword_expression_matches(*, expression: str, title: str | None, abstract: str | None) -> bool:
    """Evaluate a boolean keyword expression against title + abstract text."""
    _, ast, _ = _compile_cached(str(expression or ""))
    tokens = tuple(
        token
        for token in (_normalize_word(part) for part in _WORD_RE.findall(f"{title or ''} {abstract or ''}".lower()))
        if token
    )
    token_set = set(tokens)

    def matches_literal(node: _Literal) -> bool:
        if len(node.terms) == 1:
            return node.terms[0] in token_set
        literal_len = len(node.terms)
        for start in range(0, max(0, len(tokens) - literal_len + 1)):
            if tokens[start:start + literal_len] == node.terms:
                return True
        return False

    def evaluate(node: _Expr) -> bool:
        if isinstance(node, _Literal):
            return matches_literal(node)
        if isinstance(node, _Not):
            return not evaluate(node.child)
        if isinstance(node, _And):
            return all(evaluate(child) for child in node.children)
        if isinstance(node, _Or):
            return any(evaluate(child) for child in node.children)
        raise TypeError(f"Unsupported keyword expression node: {type(node)!r}")

    return evaluate(ast)
