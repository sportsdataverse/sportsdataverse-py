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

import threading
import time
from typing import Any, Dict, Optional
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


def _throttle() -> None:
    with _lock:
        wait = _last[0] + MIN_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.monotonic()


def _wire(params: Optional[dict]) -> Dict[str, Any]:
    """Query values as the site sends them: ``True`` -> ``"true"``; lists and other values unchanged."""
    return {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in (params or {}).items() if v is not None}


def _stamp(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """The request's path slugs and query values (``{"event_slug": "2026-ev-4"}``, ``{"year": 2026, ...}``)."""
    parts = [p for p in urlsplit(url).path.split("/") if p]
    out: Dict[str, Any] = {}
    for here, nxt in zip(parts, parts[1:]):
        key = _SLUG_AFTER.get(here)
        if key and nxt not in _NOT_SLUGS and key not in out:
            out[key] = nxt
    out.update(params)
    return out


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
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
        AssetFetchError: the 403 origin refusal, any other non-2xx after retries, or a 200 that
            is not the ``{"data": {...}}`` envelope.
    """
    wire = _wire(params)
    headers = {**GATE_HEADERS, **(kwargs.pop("headers", None) or {})}
    _throttle()
    try:
        body = _rt._get(url, params=wire, headers=headers, **kwargs)
    except AssetFetchError as exc:
        if "HTTP 403" in str(exc):
            raise AssetFetchError(
                f"{_rt._where(url)}: the PLL stats proxy refused the request's origin (403); "
                f"it answers only requests carrying Referer {GATE_HEADERS['Referer']}"
            ) from exc
        raise
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        what = body.get("error") if isinstance(body, dict) and "error" in body else type(data).__name__
        raise AssetFetchError(f"{_rt._where(url)}: not the PLL {{data: {{...}}}} envelope ({what!r})")
    return {**data, REQUEST_KEY: _stamp(url, wire)}
