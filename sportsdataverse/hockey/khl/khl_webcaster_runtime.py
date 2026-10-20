"""Runtime for the generated KHL webcaster wrappers (``khl.api.webcaster.pro/api/khl_mobile``).

The webcaster API is keyless JSON (``GET <route>.json?locale=en``). On top of the shared getter:

* **Season -> stage.** Neither KHL backend takes a year. The public ``season`` argument is the
  season's END year (2026 = 2025/26) plus a ``stage`` (``regular`` / ``playoff``); :func:`stage_for`
  maps it to a webcaster ``stage_id`` (and a khl.ru ``tnId``) through the season table that
  ``data.json`` publishes in ``stages_v2`` (``khl_id`` = tnId). The table is fetched once and cached
  in-process (:func:`season_table`; ``season_table.cache_clear()`` refetches). There is no arithmetic
  rule to fall back on: the "regular is even, playoff is regular + 1" pattern of recent seasons does
  not hold for 2008/09 (160 / 165) or 2009/10 (167 / 168).
* **Proxy.** From some networks large bodies (``tables_v2``, ~1.2 MB) stall after ~16 KB. An optional
  proxy URL in the ``KHL_WEBCASTER_PROXY`` environment variable is used for every request; it is
  never logged or put in an error message (a failed proxied fetch is reported without its cause).
* **Refusals.** A missing id answers HTTP 200 ``{"error": {"code": 404, ...}}``: code 404 raises
  :class:`~sportsdataverse.errors.NoDataError`, any other error envelope
  :class:`~sportsdataverse.errors.AssetFetchError`; an empty object is ``NoDataError``. A WRONG
  answer is a failed fetch, ``AssetFetchError``: a page object that answers another id than the one
  requested (``event_v2`` / ``team_v2``), an ``events_v2`` row of another stage, or a ``players_v2``
  row of a player not asked for. An empty list is a real answer (``events_v2`` past the last page)
  and is returned as is.
* **Redactions.** ``bets`` (betting odds plus a bookmaker affiliate link on events), ``fonbet`` and
  ``remote_ip`` (the caller's IP, echoed by ``data.json``) are removed from every body.

Everything else is the package error vocabulary of the shared getter (404 -> ``NoDataError``,
400/422 -> ``ValueError``, any other failure -> ``AssetFetchError``), through
:data:`sportsdataverse.hockey.euro_hockey_runtime.SESSION`: every attempt, retries included, starts at
least one second after the host's last answer finished.
"""

from __future__ import annotations

import functools
import logging
import os
from typing import Any, Dict, Optional, Tuple

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey.euro_hockey_runtime import SESSION
from sportsdataverse.hockey.khl.khl_webcaster_parsers import parse_khl_webcaster_stages

__all__ = ["season_table", "stage_for"]

BASE = "https://khl.api.webcaster.pro/api/khl_mobile"
PROXY_ENV = "KHL_WEBCASTER_PROXY"
STAGES = ("regular", "playoff")
#: Keys removed from every body at any depth (betting odds / affiliate links, the caller's IP).
REDACTED = frozenset({"bets", "fonbet", "remote_ip"})
_ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status"})

# A proxied request logs nothing through dl_utils: its retry lines quote the transport exception,
# which can carry the proxy URL (credentials included).
_QUIET = logging.getLogger("sdv.khl_webcaster.proxied")
_QUIET.addHandler(logging.NullHandler())
_QUIET.propagate = False


def _strip(obj: Any) -> Any:
    """``obj`` without the :data:`REDACTED` keys, at any depth."""
    if isinstance(obj, list):
        return [_strip(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _strip(v) for k, v in obj.items() if k not in REDACTED}
    return obj


def _fetch(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
    """One GET on the throttled :data:`SESSION` (every retry waits too), through ``$KHL_WEBCASTER_PROXY`` when set."""
    kwargs.setdefault("session", SESSION)
    proxy = os.environ.get(PROXY_ENV, "").strip()
    if not proxy:  # a 204 / 205 raises _rt._NoContent: ``_get`` answers {} (EX-SYNC-a)
        return _rt._get(url, params, transport=_rt._no_content_transport(), **kwargs)
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    kwargs.setdefault("proxy", {"http": proxy, "https": proxy})
    kwargs.setdefault("logger", _QUIET)
    try:
        return _rt._download_json(_rt._no_content_transport(download), url, params=clean, **kwargs)
    except AssetFetchError as exc:
        cause = type(exc.__cause__).__name__ if exc.__cause__ is not None else "a non-2xx or unreadable answer"
        raise AssetFetchError(f"{_rt._where(url)}: the fetch through {PROXY_ENV} failed ({cause})") from None


@functools.lru_cache(maxsize=1)
@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-1)
def season_table() -> Tuple[Dict[str, Any], ...]:
    """Every regular-season and playoff stage, from ``data.json`` ``stages_v2`` (cached in-process).

    Returns:
        One dict per stage: ``season`` (END year), ``type``, ``tournament_id`` (khl.ru tnId) and
        ``stage_id`` (webcaster id), newest first.

    Raises:
        NoDataError: ``data.json`` lists no stage.
        AssetFetchError: ``data.json`` could not be fetched.

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_stages` - the season table this reads.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    raw = _fetch(f"{BASE}/data.json", {"locale": "en"})
    stages = parse_khl_webcaster_stages(raw)
    rows = [r for r in stages.iter_rows(named=True) if r["type"] in STAGES and r["season"] is not None]
    if not rows:
        raise NoDataError(f"{_rt._where(BASE + '/data.json')}: stages_v2 lists no regular or playoff stage")
    return tuple(rows)


def stage_for(season: Any, stage: Optional[str] = "regular") -> Dict[str, Any]:
    """The stage of a season: its webcaster ``stage_id`` and khl.ru ``tournament_id``.

    Args:
        season: the season's END year (2026 = 2025/26).
        stage: ``regular`` (default) or ``playoff``.

    Returns:
        The season-table row (``season``, ``type``, ``tournament_id``, ``stage_id``).

    Raises:
        ValueError: ``season`` is not a year or ``stage`` is not ``regular`` / ``playoff``.
        NoDataError: the KHL has no such stage (a season before 2008/09, a playoff not yet drawn).

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_stages` - the season table this reads.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    kind = stage or "regular"
    if kind not in STAGES:
        raise ValueError(f"stage must be one of {STAGES}, got {stage!r}")
    try:
        year = int(season)
    except (TypeError, ValueError):
        raise ValueError(f"season must be the season's END year as an int (2026 = 2025/26), got {season!r}") from None
    table = season_table()
    for row in table:
        if row["season"] == year and row["type"] == kind:
            return dict(row)
    years = sorted({r["season"] for r in table})
    raise NoDataError(
        f"the KHL has no {kind} stage for season {year} (END year); the season table spans {years[0]}-{years[-1]}"
    )


def _check(url: str, raw: Any, params: Dict[str, Any]) -> None:
    """Raise on an error envelope or on a body scoped to something other than the request."""
    where = _rt._where(url)
    if isinstance(raw, dict) and raw and set(raw) <= _ERROR_KEYS and "error" in raw:
        err = raw.get("error")
        code = err.get("code") if isinstance(err, dict) else None
        message = _rt._excerpt(str(err.get("message") if isinstance(err, dict) else err))
        if str(code) == "404":
            raise NoDataError(f"{where} answered an error envelope: 404 {message}")
        raise AssetFetchError(f"{where} answered an error envelope: {code} {message}")
    if isinstance(raw, dict) and not raw:
        raise NoDataError(f"{where} answered an empty object")
    route = url.rsplit("/", 1)[-1]
    wanted = params.get("id")
    if route in ("event_v2.json", "team_v2.json") and wanted is not None:
        got = (raw.get(route.split("_", 1)[0]) or {}).get("id") if isinstance(raw, dict) else None
        if str(got) != str(wanted):
            raise AssetFetchError(f"{where}: asked for id {wanted}, answered {got}")
    if route == "events_v2.json" and params.get("stage_id") is not None and isinstance(raw, list):
        other = {str((r.get("event") or {}).get("stage_id")) for r in raw if isinstance(r, dict)} - {
            str(params["stage_id"])
        }
        if other:
            raise AssetFetchError(f"{where}: asked for stage {params['stage_id']}, answered stage(s) {sorted(other)}")
    ids = params.get("q[id_in][]")
    if route == "players_v2.json" and ids is not None and isinstance(raw, list):
        asked = {str(i) for i in (ids if isinstance(ids, (list, tuple)) else [ids])}
        other = {str((r.get("player") or {}).get("id")) for r in raw if isinstance(r, dict)} - asked
        if other:
            raise AssetFetchError(f"{where}: asked for players {sorted(asked)}, answered {sorted(other)}")


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a webcaster route as JSON (``season`` / ``stage`` resolved to ``stage_id``), then verify it.

    Args:
        url: the route URL.
        params: query parameters; ``season`` (END year) and ``stage`` are replaced by ``stage_id``
            unless a ``stage_id`` is given; ``None`` values are dropped.
        **kwargs: forwarded to the shared getter (``timeout`` ...).

    Returns:
        The body, with the :data:`REDACTED` keys removed.

    Raises:
        NoDataError: 404, an error envelope with code 404, an empty object or an unknown season.
        ValueError: 400 / 422, or a malformed ``season`` / ``stage``.
        AssetFetchError: a body scoped to another id / stage / player than the one requested, any
            other error envelope or failed fetch.
    """
    query = {k: v for k, v in (params or {}).items() if v is not None}
    season, stage = query.pop("season", None), query.pop("stage", None)
    if season is not None and "stage_id" not in query:
        query["stage_id"] = stage_for(season, stage)["stage_id"]
    raw = _fetch(url, query, **kwargs)
    _check(url, raw, query)
    return _strip(raw)
