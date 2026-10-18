"""Runtime for the eSports hokejovyzapis S3 families (``del_esports``, ``icehl_esports``,
``eihl_esports``, ``metal_esports``).

The four leagues' scoring system writes one key layout into four public S3 buckets on
``s3.eu-west-1.amazonaws.com`` (``de.`` DEL, ``icehl.`` ICE Hockey League, ``eihl.`` EIHL,
``den.`` Metal Ligaen). Keyless JSON. Two contracts sit on top of the shared getter:

* **Season.** Every bucket keys a season by its START year (``2026`` = 2026/27). The wrappers
  take the END year, like every sdv hockey family, and :func:`season_start_year` maps it at
  the boundary (``season=2027`` requests ``.../2026/...``).
* **Refusals** (the refs ``capture.py`` rules). A missing key answers S3 ``404 NoSuchKey``
  XML (``NoDataError``); a bucket that forbids the read answers ``403 AccessDenied`` XML
  (``AssetFetchError``); both come from the shared getter. On top of it :func:`_get` raises
  ``NoDataError`` for a 200 that carries no rows (``[]``, ``{}``, a schedule with no
  ``matches``, a game with no events, and the shot shells Denmark writes for every game:
  ``{"match": {"shots": [], "status": "LIVE"}}``), and ``AssetFetchError`` for an error
  envelope, a body of the wrong shape (a list where the schedule, shot, widget or
  period-events route answers an object), or a body for another season or game than the URL
  asked for.

Requests to the shared host are spaced at least one second apart
(:func:`sportsdataverse.hockey.euro_hockey_runtime._get`).
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as _eh

__all__: List[str] = []

#: A 200 whose keys are all of these is an error envelope, not data (refs capture.py).
ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status"})

_SCHEDULE = re.compile(r"/league-matches/(\d+)/\d+\.json$")
_SHOTS = re.compile(r"/visualization/shots/(\d+)\.json$")
_WIDGET = re.compile(r"/widget/esports/match/(\d+)\.json$")
_EVENTS = re.compile(r"/period-events\.json$")
#: Routes whose body is a JSON object; anything else there is a wrong-shape answer.
_OBJECT_ROUTES = (_SCHEDULE, _SHOTS, _WIDGET, _EVENTS)


def season_start_year(season: Any) -> int:
    """Map a season's END year (the sdv key) to the START year the buckets use.

    Args:
        season: season END year, e.g. ``2027`` for 2026/27.

    Returns:
        The START year, ``season - 1``.

    Raises:
        ValueError: ``season`` is not a four-digit year.

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_schedule` - a route this feeds.
        * :func:`sportsdataverse.hockey.icehl.icehl_schedule` - a route this feeds.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    end = int(season)
    if not 1000 <= end <= 9999:
        raise ValueError(f"season must be a four-digit END year (2027 = 2026/27); got {season!r}")
    return end - 1


def _has_rows(body: Any) -> bool:
    """A non-empty list, or a dict holding at least one non-empty list or dict."""
    if isinstance(body, list):
        return bool(body)
    return isinstance(body, dict) and any(isinstance(v, (list, dict)) and v for v in body.values())


def _dig(obj: Any, *keys: str) -> Any:
    for k in keys:
        obj = obj.get(k) if isinstance(obj, dict) else None
    return obj


def _check(url: str, body: Any) -> None:
    """Raise unless ``body`` holds rows for exactly what ``url`` asked (see the module docstring)."""
    where = _rt._where(url)
    if isinstance(body, dict) and body and set(body) <= ERROR_KEYS:
        raise AssetFetchError(f"{where}: answered an error envelope {sorted(body)}")
    if not _has_rows(body):
        raise NoDataError(f"{where}: answered a body with no rows")
    path = url.split("?", 1)[0]
    if any(r.search(path) for r in _OBJECT_ROUTES) and not isinstance(body, dict):
        raise AssetFetchError(f"{where}: expected a JSON object, the body is a {type(body).__name__}")
    if m := _SCHEDULE.search(path):
        matches = _dig(body, "matches")
        if not (isinstance(matches, list) and matches):
            raise NoDataError(f"{where}: the schedule lists no matches")
        seasons = {str(x.get("season")) for x in matches if isinstance(x, dict)}
        if seasons != {m.group(1)}:
            raise AssetFetchError(f"{where}: asked for season {m.group(1)}, the body holds {sorted(seasons)[:3]}")
    elif m := _SHOTS.search(path):
        shots = _dig(body, "match", "shots")
        if not (isinstance(shots, list) and shots):
            raise NoDataError(f"{where}: the shot file is an empty shell (no shots recorded)")
        if str(_dig(body, "match", "id")) != m.group(1):
            raise AssetFetchError(
                f"{where}: asked for match {m.group(1)}, the body is match {_dig(body, 'match', 'id')}"
            )
    elif m := _WIDGET.search(path):
        if str(_dig(body, "data", "gameData", "id")) != m.group(1):
            got = _dig(body, "data", "gameData", "id")
            raise AssetFetchError(f"{where}: asked for match {m.group(1)}, the body is match {got}")
    elif _EVENTS.search(path) and not any(isinstance(v, list) and v for v in body.values()):
        raise NoDataError(f"{where}: the game has no events")


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a bucket key as JSON, throttled, and refuse a body without rows for the request.

    Args:
        url: fully-built object URL.
        params: query-string parameters (none of the data keys take any).
        **kwargs: forwarded to the shared getter.

    Returns:
        The parsed JSON body.

    Raises:
        NoDataError: the key does not exist (404 ``NoSuchKey``) or the body has no rows.
        AssetFetchError: any other non-2xx (403 ``AccessDenied``), an error envelope, a body of
            the wrong shape, or a body for another season or game.
    """
    body = _eh._get(url, params, **kwargs)
    _check(url, body)
    return body
