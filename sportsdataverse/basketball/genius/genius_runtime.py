"""Runtime for the generated Genius Sports LiveStats wrappers (three keyless hosts, one proxy variable).

Hosts (sdv-internal-refs ``genius-livestats/``, measured 2026-10-12):

* ``fibalivestats.dcd.shared.geniussports.com``: ``/data/{matchId}/data.json`` (the full game),
  ``/data/competition/{compId}.json`` (a recent window) and the ``/u/{ORG}/{matchId}/`` HTML shell.
* ``hosted.dcd.shared.geniussports.com``: the hosted WebStats pages (competition chooser, schedules, and the
  ``/embednf/`` JSON-wrapped variant for orgs whose plain pages answer HTTP 500).
* ``widget.wh.geniussports.com``: a league site's results widget (JavaScript).

**Proxy.** CloudFront refuses datacenter IPs on the first two hosts ("Request blocked", a ~919 byte 403).
Set ``GENIUS_PROXY`` to an ``http(s)://user:pass@host:port`` URL and every request goes through it. The value is
read at call time by :func:`sportsdataverse.basketball.basketball_runtime.proxy_from_env`, may carry credentials,
and is never logged, printed or put in an error message (an error names the variable and the cause class only).

**Two different 403s** (never read one as the other):

* CloudFront "Request blocked": the egress IP is refused, so the answer is unknown (``AssetFetchError``, which
  says to set ``GENIUS_PROXY``). Never retried.
* S3 ``AccessDenied`` (~300 B): the object does not exist (a wrong match id, a wrong ORG on ``/u/``, or a game from
  before ``data.json`` existed): ``NoDataError``.

**Error vocabulary.** ``NoDataError``: HTTP 404, the S3 ``AccessDenied`` 403, an empty 200 from the hosted
host (an org without hosted pages, or an unknown ORG), a recent-games window with no games, a schedule or widget
with no matches. ``ValueError``: HTTP 400 / 422, a URL that is not a Genius route. ``AssetFetchError``: every other
failure, which means the answer is unknown, not empty:

* a non-200 (429 / 5xx after three attempts, honouring ``Retry-After``; the HTTP 500 "A website error has occurred"
  page, an intermittent host-side failure), the CloudFront block, a dead connection. A dead connection and a 429 /
  5xx carry ``transient = True``; every other ``AssetFetchError`` is a definitive refusal;
* an error envelope or a body of the wrong kind (not JSON, an HTML page where JSON was asked, a page cut off before
  ``</html>``, a truncated ``data.json``);
* **a body for another scope than the one asked for**: a recent-games window of another competition, a ``/u/``
  shell for another match, a competition chooser or schedule for another ORG or competition, a schedule block whose
  link names another match, a widget whose games belong to another webcast ORG;
* an unfinished game: ``data.json`` is final only once its play-by-play holds the ``game`` / ``end`` row. A live game
  is refused so a half-played box score is never read as the result.

``data.json`` names neither its match nor its competition, so its scope cannot be checked beyond shape and the
finished-game row; the runtime stamps ``_request`` (the ids asked for) on the body so the frames know their
``match_id``.

Every attempt waits its turn on :data:`sportsdataverse.basketball.basketball_runtime.SESSION` (at least 1 s per host),
retries included.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.basketball.genius.genius_parsers import _NotParsed, _page_record
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["PROXY_ENV", "WIDGET_KEYS"]

#: Name of the environment variable that holds the optional proxy URL.
PROXY_ENV = "GENIUS_PROXY"
LIVE = "fibalivestats.dcd.shared.geniussports.com"
HOSTED = "hosted.dcd.shared.geniussports.com"
WIDGET = "widget.wh.geniussports.com"
#: 403 is NOT retried: the two Genius 403s are definitive answers. Neither is 500: the "website error" page is an
#: intermittent origin failure that sticks to a URL for minutes (2026-10-18: one ESAKE URL failed four times over
#: two minutes while the WBBL/SLB/BAL/NZNBL URLs that failed earlier that day answered 200), so an immediate
#: retry only adds load; the error is marked transient so a caller retries later.
RETRY_STATUSES = frozenset({408, 429, 502, 503, 504})
ATTEMPTS = 3
#: Public site-widget keys (embedded in each league site's page) -> the webcast ORG their games carry.
WIDGET_KEYS: Dict[str, str] = {
    "RFJIN037I07QV520B7WEXKV1GYNA31": "ESAKE",
    "TXMFPAVMWV2N5ML0BJYODXOEWWIQKS": "FRB",
    "D2PNKMOFTN2CRLP1HIXPX89RH3GUMC": "NZN",
}

_ROUTES: Tuple[Tuple[str, "re.Pattern[str]", str], ...] = (
    (LIVE, re.compile(r"^/data/(\d+)/data\.json$"), "data"),
    (LIVE, re.compile(r"^/data/competition/(\d+)\.json$"), "competition"),
    (LIVE, re.compile(r"^/u/([A-Za-z0-9]+)/(\d+)/?$"), "u"),
    (HOSTED, re.compile(r"^/([A-Za-z0-9]+)/en/schedule/?$"), "chooser"),
    (HOSTED, re.compile(r"^/([A-Za-z0-9]+)/en/competition/(\d+)/schedule/?$"), "schedule"),
    (HOSTED, re.compile(r"^/embednf/([A-Za-z0-9]+)/en/competition/(\d+)/schedule/?$"), "embednf"),
    (WIDGET, re.compile(r"^/widget/?$"), "widget"),
)


def _route(url: str) -> Tuple[str, Tuple[str, ...]]:
    parts = urlsplit(url)
    for host, pattern, kind in _ROUTES:
        m = pattern.match(parts.path)
        if parts.hostname == host and m:
            return kind, m.groups()
    raise ValueError(f"not a Genius route: {_rt._where(url)}")


# ------------------------------------------------------------------------------ transport
def _body_text(resp: Any) -> str:
    """The response body as text, read as UTF-8 when the server names no charset.

    ``requests`` decodes a ``text/html`` answer without a ``charset`` as ISO-8859-1, which turns the
    UTF-8 pages Genius serves (e.g. French and Greek team and competition names) into mojibake. A
    declared charset is honoured; bytes that are not valid UTF-8 fall back to ``resp.text``.
    """
    content = getattr(resp, "content", None)
    headers = getattr(resp, "headers", None) or {}
    ctype = next((str(v) for k, v in headers.items() if str(k).lower() == "content-type"), "").lower()
    if isinstance(content, bytes) and "charset=" not in ctype:
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            pass
    return getattr(resp, "text", "") or ""


def _fetch(url: str, params: Optional[Dict[str, Any]]) -> str:
    """The body text of a 200 answer, or the error the answer calls for (see the module docstring)."""
    where = _rt._where(url)
    proxy = _bb.proxy_from_env(PROXY_ENV)
    try:
        resp = download(
            url,
            params=params or None,
            session=_bb.SESSION,
            proxy=proxy,
            logger=_bb._QUIET,
            retry_statuses=RETRY_STATUSES,
            num_retries=ATTEMPTS - 1,
        )
    except OSError as exc:  # the transport error can quote the proxy URL: name the class only
        err = AssetFetchError(f"{where}: fetch failed after retries ({type(exc).__name__}); proxy={PROXY_ENV}")
        err.transient = True
        raise err from None
    status = getattr(resp, "status_code", None)
    _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
    text = _body_text(resp)
    if status == 200:
        return text
    low = text.lower()
    if status == 403 and "request blocked" in low and "cloudfront" in low:
        via = f"the {PROXY_ENV} proxy" if proxy else f"this egress IP; set {PROXY_ENV} to an http(s) proxy URL"
        raise AssetFetchError(f"{where}: CloudFront refused {via} (HTTP 403 'Request blocked')")
    if status == 403 and "accessdenied" in low:
        raise NoDataError(
            f"{where}: no such object (S3 AccessDenied: a wrong id or ORG, or a game from before data.json)"
        )
    if status == 500 and "website error" in low:
        err = AssetFetchError(
            f"{where} answered HTTP 500 'A website error has occurred': an intermittent Genius host-side failure "
            "(it sticks to a URL for minutes; retry later)"
        )
        err.transient = True
        raise err
    try:
        _rt._check_status(url, status, text)  # 404 -> NoDataError, 400/422 -> ValueError, else AssetFetchError
    except AssetFetchError as exc:
        exc.transient = status in RETRY_STATUSES or (isinstance(status, int) and status >= 500)
        raise
    raise AssetFetchError(f"{where} answered HTTP {status}, not 200")


def _json(url: str, text: str) -> Any:
    if not text.strip():
        raise AssetFetchError(f"{_rt._where(url)} answered HTTP 200 with an empty body")
    try:
        return json.loads(text)
    except ValueError:
        raise AssetFetchError(f"{_rt._where(url)}: the body is not JSON (cut off, or a page instead of data)") from None


def _page(kind: str, url: str, text: str) -> Dict[str, Any]:
    try:
        return _page_record(kind, text)
    except _NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{_rt._where(url)}: {exc}") from None
        raise AssetFetchError(f"{_rt._where(url)}: not the page asked for: {exc}") from None


def _finished(body: Dict[str, Any]) -> bool:
    """True when the play-by-play holds the ``game`` / ``end`` action (pbp is newest first)."""
    return any(isinstance(r, dict) and r.get("actionType") == "game" and r.get("subType") == "end" for r in body["pbp"])


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality: ``'5' != 5`` and ``True != 1``."""
    return type(got) is type(want) and got == want


def _wrong(url: str, what: str, got: Any, want: Any) -> AssetFetchError:
    return AssetFetchError(f"{_rt._where(url)}: not the {what} asked for (answered {got!r}, asked {want!r})")


# ------------------------------------------------------------------------------ the getter
@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a Genius route, refuse every wrong answer and return its body (or page record).

    Args:
        url: the URL a generated wrapper built.
        params: query parameters; ``None`` values are dropped (the widget key rides in the URL's query).
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        ``data.json``: the body ``dict`` with ``_request = {"match_id": ...}``. The competition window: the
        JSON list. ``/u/``, the chooser, schedules and the widget: the page record ``dict`` (the refs capture
        shape); the chooser and ``/u/`` carry ``_request = {"org": ...}``.

    Raises:
        NoDataError, ValueError, AssetFetchError: see the module docstring.
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a Genius request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    kind, ids = _route(url)
    text = _fetch(url, q)
    if kind == "data":
        body = _json(url, text)
        tm = body.get("tm") if isinstance(body, dict) else None
        if not (isinstance(tm, dict) and {"1", "2"} <= set(tm) and isinstance(body.get("pbp"), list)):
            raise AssetFetchError(
                f"{_rt._where(url)}: data.json without tm.1, tm.2 and pbp[] (an error envelope or stub)"
            )
        if not _finished(body):
            raise AssetFetchError(
                f"{_rt._where(url)}: the game is not finished (no pbp game/end row); only final games are read"
            )
        body["_request"] = {"match_id": int(ids[0])}
        return body
    if kind == "competition":
        rows = _json(url, text)
        if not isinstance(rows, list):
            raise AssetFetchError(f"{_rt._where(url)}: expected a JSON list of games")
        if not rows:
            raise NoDataError(f"{_rt._where(url)}: the recent-games window is empty")
        for r in rows:
            if not isinstance(r, dict) or not _same(r.get("competitionId"), int(ids[0])):
                raise _wrong(url, "competition", r.get("competitionId") if isinstance(r, dict) else r, int(ids[0]))
        return rows
    if kind == "widget":
        rec = _page("widget", url, text)
        orgs = {m.get("webcast_org") for m in rec["matches"]}
        key = urlsplit(url).query
        want = WIDGET_KEYS.get(key)
        if len(orgs) != 1 or (want is not None and orgs != {want}):
            raise _wrong(url, "widget", sorted(str(o) for o in orgs), want)
        return rec
    rec = _page("chooser" if kind == "chooser" else kind, url, text)
    if kind == "u":
        org, match_id = ids[0], int(ids[1])
        if not _same(rec["match_id"], match_id):
            raise _wrong(url, "match", rec["match_id"], match_id)
        rec["_request"] = {"org": org}
    elif kind == "chooser":
        org = ids[0]
        for c in rec["competitions"]:
            if c.get("org") != org:
                raise _wrong(url, "org", c.get("org"), org)
        rec["_request"] = {"org": org}
    else:  # schedule / embednf
        org, comp_id = ids[0], int(ids[1])
        # A "To be determined" slot (an unfilled play-off game: FFBB 21849) has no team or match link, so it names
        # no scope. It takes the one the page's other blocks prove; a page of nothing but such slots is refused.
        empty = [m for m in rec["matches"] if m.get("comp_id") is None and m.get("org") is None]
        if len(empty) == len(rec["matches"]):
            raise AssetFetchError(f"{_rt._where(url)}: no block names a competition, so the page cannot be checked")
        for m in empty:
            m["comp_id"], m["org"] = comp_id, org
        for m in rec["matches"]:
            if not _same(m.get("comp_id"), comp_id):
                raise _wrong(url, "competition", m.get("comp_id"), comp_id)
            if m.get("org") != org:
                raise _wrong(url, "org", m.get("org"), org)
            if m.get("link_match_id") is not None and m["link_match_id"] != m["match_id"]:
                raise AssetFetchError(f"{_rt._where(url)}: match {m['match_id']} links to {m['link_match_id']}")
        rec["_request"] = {"org": org, "comp_id": comp_id}
    return rec
