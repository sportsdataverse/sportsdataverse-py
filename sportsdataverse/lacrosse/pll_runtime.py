"""Runtime getter for the Premier Lacrosse League stats API (``stats.premierlacrosseleague.com/api``).

Runtime only: every ``pll_*`` wrapper sends one GET when it is called. No release assets are
built from this API, and nothing here bulk-downloads or backfills it.

The stats site is a Next.js app whose ``/api/*`` routes proxy the league's stats service. **No
token is involved**: the upstream key stays on the site's server and the stats routes send no
``Authorization``. What the proxy checks is the caller's origin: without the stats site's own
``Referer`` it answers HTTP 403 ``{"error": "Origin not allowed"}``. :func:`_get` sends that
``Referer`` (the header a browser on the page sends) and a browser User-Agent; there is no key to
configure and no environment variable is read.

On top of :func:`sportsdataverse._codegen_runtime._get` (retries, backoff and the error
vocabulary: 404 -> ``NoDataError``, 400 / 422 -> ``ValueError``, any other non-2xx -> ``AssetFetchError``):

* requests are spaced at least :data:`MIN_INTERVAL` seconds apart in a process;
* a boolean query value goes as ``true`` / ``false`` (the service validates it), and a list
  repeats its key (``stats=points&stats=assists``), as the site sends them;
* the answer must be the ``{"data": {...}}`` envelope; its ``data`` object is returned, with the
  request stamped under :data:`REQUEST_KEY` (the slugs in the path and the query values), because
  several bodies (play-by-play, box scores, rosters) do not say which game, team or season they
  answer. The parsers turn the stamp into ``event_slug`` / ``team_slug`` / ``season`` columns.
"""

from __future__ import annotations

import contextlib
import itertools
import re
import threading
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError

__all__: list = []

HOST = "https://stats.premierlacrosseleague.com"
#: The gate: the proxy answers 403 "Origin not allowed" to any other Referer, or none.
GATE_HEADERS = {
    "Referer": f"{HOST}/",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36",
}
#: Key the request is stamped under in the returned ``data`` object.
REQUEST_KEY = "_request"
#: Seconds kept between two requests from this process (the API publishes no limit; stay gentle).
MIN_INTERVAL = 0.5

_lock = threading.Lock()
_last = [0.0]
# path segment -> stamp key of the segment after it; "season-stats" / "stats" after players are routes, not slugs
_SLUG_AFTER = {
    "events": "event_slug",
    "games": "event_slug",
    "teams": "team_slug",
    "players": "player_slug",
    "glg": "league",
}
_NOT_SLUGS = frozenset({"season-stats", "stats"})
#: Every route the wrappers build. A slug is letters, digits and ``_ . @ -``: one that carries ``/``, ``?``
#: or ``#`` would change the route or add query parameters, so it is refused before sending.
_SLUG = r"[A-Za-z0-9_.@-]+"
ROUTE = re.compile(
    rf"/api/v4/(?:events|games)(?:/{_SLUG}(?:/(?:playlogs|play-by-plays|players/stats|teams/stats|insights))?)?"
    rf"|/api/v4/standings|/api/v4/teams/(?:season-stats|{_SLUG}/(?:stats|roster))"
    rf"|/api/v4/players/(?:season-stats|stats/(?:advanced|season-leaders|career-leaders)|{_SLUG}(?:/stats/seasons)?)"
    rf"|/api/glg/(?:intlm|intlw)/(?:games|standings)|/api/wll/v1/events"
)
# 403 is a refused origin, not load: answered at once, never backed off (download retries 403 by default).
_RETRY = frozenset({429, 500, 502, 503, 504})


@contextlib.contextmanager
def _throttle() -> Iterator[None]:
    """Hold the process's one request slot across the send: requests never overlap, and each starts
    at least :data:`MIN_INTERVAL` seconds after the previous one ended."""
    with _lock:
        wait = _last[0] + MIN_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            yield
        finally:
            _last[0] = time.monotonic()


def slug(value: Any) -> str:
    """``value`` as a route slug (letters, digits, ``_ . @ -``), checked before it enters a URL path.

    Raises:
        ValueError: anything else (a ``/``, ``?`` or ``#`` would change the route or the query).
    """
    text = str(value)
    if not re.fullmatch(_SLUG, text):
        raise ValueError(f"not a PLL slug: {value!r} (letters, digits, _ . @ - only)")
    return text


def _wire(params: dict | None) -> dict[str, Any]:
    """Query values as the site sends them: ``True`` -> ``"true"`` (in a list too); a list repeats its key."""

    def one(v: Any) -> Any:
        return str(v).lower() if isinstance(v, bool) else v

    return {
        k: ([one(x) for x in v] if isinstance(v, list) else one(v)) for k, v in (params or {}).items() if v is not None
    }


def _stamp(url: str, params: dict[str, Any]) -> dict[str, Any]:
    """The request's path slugs and query values (``{"event_slug": "2026-ev-4"}``, ``{"year": 2026, ...}``)."""
    parts = [p for p in urlsplit(url).path.split("/") if p]
    out: dict[str, Any] = {}
    for here, nxt in itertools.pairwise(parts):
        key = _SLUG_AFTER.get(here)
        if key and nxt not in _NOT_SLUGS and key not in out:
            out[key] = nxt
    out.update(params)
    return out


@_rt._no_content_is_empty
def _get(url: str, params: dict | None = None, **kwargs: Any) -> dict[str, Any]:
    """GET a stats route with the site's Referer; return its ``data`` object, request stamped.

    Args:
        url: the fully built route URL.
        params: query parameters; ``None`` values are dropped.
        **kwargs: forwarded to the HTTP getter; ``headers`` merge over the gate headers.

    Returns:
        The response's ``data`` object with :data:`REQUEST_KEY` added; ``{}`` on a 204 / 205.

    Raises:
        NoDataError: the API answered 404 (an unknown game, team or player slug, a route it
            does not serve, or a 2019-2020 game slug on a per-game route).
        ValueError: the API answered 400 / 422 (a missing year, a season segment or stat name
            outside its allowed list).
        ValueError: also, before sending, a URL that is not one of the wrappers' routes (a slug
            carrying ``/``, ``?`` or ``#``).
        AssetFetchError: the 403 origin refusal (not retried), any other non-2xx after retries, or
            a 200 that is not the ``{"data": {...}}`` envelope.
    """
    parts = urlsplit(url)
    if parts.query or parts.fragment or not ROUTE.fullmatch(parts.path):
        raise ValueError(
            f"not a PLL stats route the wrappers build: {parts.path!r} (a slug is letters, digits, _ . @ -)"
        )
    wire = _wire(params)
    headers = {**GATE_HEADERS, **(kwargs.pop("headers", None) or {})}
    try:
        with _throttle():
            body = _rt._get(
                url,
                params=wire,
                headers=headers,
                transport=_rt._no_content_transport(),
                retry_statuses=_RETRY,
                **kwargs,
            )
    except AssetFetchError as exc:
        if "HTTP 403" in str(exc) and "Origin not allowed" in str(exc):
            raise AssetFetchError(
                f"{exc} -- the PLL stats proxy refused the request's origin; it answers only requests "
                f"carrying Referer {GATE_HEADERS['Referer']}"
            ) from exc
        raise
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        what = body.get("error") if isinstance(body, dict) and "error" in body else type(data).__name__
        raise AssetFetchError(f"{_rt._where(url)}: not the PLL {{data: {{...}}}} envelope ({what!r})")
    return {**data, REQUEST_KEY: _stamp(url, wire)}
