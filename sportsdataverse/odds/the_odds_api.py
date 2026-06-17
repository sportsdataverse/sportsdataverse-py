"""The Odds API (`api.the-odds-api.com/v4`) wrappers.

Read-only wrappers over `The Odds API <https://the-odds-api.com>`__ v4 -- live and
historical sports betting odds, scores, events and participants across a wide range
of bookmakers. Mirrors the surface of the sister R package
`oddsapiR <https://oddsapir.sportsdataverse.org>`__ (``toa_*``), and follows the
sdv-py parser contract: every wrapper returns a tidy ``polars.DataFrame`` by default
(``return_as_pandas=True`` for pandas), or the raw JSON when ``return_parsed=False``.

Auth: The Odds API authenticates with an ``apiKey`` query parameter. It resolves
from the ``api_key`` argument, else the ``ODDS_API_KEY`` environment variable (the
same variable :mod:`oddsapiR` uses). Get a free key at
https://the-odds-api.com/#get-access. The HTTP call goes through the shared
:func:`sportsdataverse.dl_utils.download` gateway like every other wrapper.

Every call returns ``x-requests-remaining`` / ``x-requests-used`` quota headers;
the most recent pair is cached and readable via :func:`toa_usage` without spending
quota.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from sportsdataverse.dl_utils import download
from sportsdataverse.odds.the_odds_api_parsers import (
    parse_toa_event_markets,
    parse_toa_event_odds,
    parse_toa_event_odds_history,
    parse_toa_events,
    parse_toa_events_history,
    parse_toa_odds,
    parse_toa_odds_history,
    parse_toa_participants,
    parse_toa_scores,
    parse_toa_sports,
)

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports (PEP 563 defers eval)
    import pandas as pd
    import polars as pl

DataFrameT = Union["pl.DataFrame", "pd.DataFrame"]

__all__ = [
    "toa_sports",
    "toa_sports_odds",
    "toa_sports_scores",
    "toa_sports_events",
    "toa_event_odds",
    "toa_event_markets",
    "toa_sports_participants",
    "toa_sports_odds_history",
    "toa_sports_events_history",
    "toa_event_odds_history",
    "toa_usage",
]

TOA_BASE = "https://api.the-odds-api.com/v4"

# Most-recent quota snapshot, refreshed from response headers on every call so
# toa_usage() can report it without spending a request.
_USAGE: Dict[str, Optional[int]] = {"requests_remaining": None, "requests_used": None, "last_cost": None}


def _toa_key(api_key: Optional[str]) -> str:
    """Resolve the API key: explicit ``api_key`` arg, else ``ODDS_API_KEY`` env."""
    key = api_key or os.environ.get("ODDS_API_KEY")
    if not key:
        raise ValueError(
            "The Odds API key not found. Pass api_key=... or set the ODDS_API_KEY "
            "environment variable. Get a free key at https://the-odds-api.com/#get-access.",
        )
    return key


def _bool_str(value: Optional[bool]) -> Optional[str]:
    """Render a bool as the lowercase ``"true"``/``"false"`` the API expects; pass ``None`` through."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _toa_get(path: str, params: Optional[Dict] = None, api_key: Optional[str] = None, **kwargs) -> Union[Dict, List]:
    """GET a JSON payload from ``api.the-odds-api.com``, injecting the API key.

    Args:
        path: Path under ``/v4`` (e.g. ``"sports/americanfootball_nfl/odds"``).
        params: Query params; ``None`` values are stripped before the request.
        api_key: Explicit key (else ``ODDS_API_KEY`` env).
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The parsed JSON body (a ``list`` for the list endpoints, a ``dict`` for the
        per-event and historical-snapshot endpoints).
    """
    merged = {"apiKey": _toa_key(api_key)}
    merged.update({k: v for k, v in (params or {}).items() if v is not None})
    resp = download(url=f"{TOA_BASE}/{path}", params=merged, **kwargs)
    if resp is None:
        return {}
    headers = getattr(resp, "headers", None) or {}
    for hdr, slot in (("x-requests-remaining", "requests_remaining"), ("x-requests-used", "requests_used")):
        try:
            _USAGE[slot] = int(headers.get(hdr))
        except (TypeError, ValueError):
            pass
    cost = headers.get("x-requests-last") if hasattr(headers, "get") else None
    try:
        _USAGE["last_cost"] = int(cost)
    except (TypeError, ValueError):
        pass
    return resp.json()


def toa_usage(return_as_pandas: bool = False) -> DataFrameT:
    """Return the cached API-key quota from the most recent call (no network/quota cost).

    Reads the ``x-requests-remaining`` / ``x-requests-used`` headers captured on the
    last :mod:`sportsdataverse.odds` call; all values are ``None`` until a request
    has been made in this session.

    Args:
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A one-row ``polars`` (or ``pandas``) ``DataFrame`` with ``requests_remaining``,
        ``requests_used`` and ``last_cost`` (credits the last call consumed).

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports, toa_usage
            _ = toa_sports()
            toa_usage()
    """
    import pandas as pd
    import polars as pl

    df = pd.DataFrame([dict(_USAGE)])
    return df if return_as_pandas else pl.from_pandas(df)


def toa_sports(
    all_sports: Optional[bool] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, List]:
    """List the sports/leagues available from The Odds API (``/v4/sports``). Quota: free.

    Args:
        all_sports: When ``True``, include out-of-season sports too (default returns
            only in-season). Sent as the ``all`` query flag.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A ``polars``/``pandas`` ``DataFrame`` (one row per sport) by default; the raw
        JSON ``list`` when ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports
            toa_sports(all_sports=True).head()
    """
    raw = _toa_get("sports", params={"all": _bool_str(all_sports)}, api_key=api_key, **kwargs)
    return parse_toa_sports(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_sports_odds(
    sport: str = "americanfootball_nfl",
    regions: str = "us",
    markets: Optional[str] = "h2h",
    odds_format: Optional[str] = "american",
    date_format: Optional[str] = "iso",
    event_ids: Optional[str] = None,
    bookmakers: Optional[str] = None,
    commence_time_from: Optional[str] = None,
    commence_time_to: Optional[str] = None,
    include_links: Optional[bool] = None,
    include_sids: Optional[bool] = None,
    include_bet_limits: Optional[bool] = None,
    include_rotation_numbers: Optional[bool] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, List]:
    """Current odds for a sport (``/v4/sports/{sport}/odds``), one row per outcome.

    Args:
        sport: Sport key from :func:`toa_sports` (e.g. ``"americanfootball_nfl"``).
        regions: Comma-separated bookmaker regions (``us``/``us2``/``uk``/``eu``/``au``).
        markets: Comma-separated markets (``h2h``, ``spreads``, ``totals``, ``outrights``, ...).
        odds_format: ``"american"`` or ``"decimal"``.
        date_format: ``"iso"`` or ``"unix"``.
        event_ids: Optional comma-separated event ids to filter to.
        bookmakers: Comma-separated bookmaker keys (takes precedence over ``regions``).
        commence_time_from: ISO8601 lower bound on event commence time.
        commence_time_to: ISO8601 upper bound on event commence time.
        include_links: Include bookmaker/market/outcome deep links.
        include_sids: Include bookmaker-specific source ids.
        include_bet_limits: Include bet limits where exchanges expose them.
        include_rotation_numbers: Include rotation numbers where available.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A long-form ``polars``/``pandas`` ``DataFrame`` (one row per
        event x bookmaker x market x outcome) by default; raw JSON ``list`` when
        ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_odds
            toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="h2h,spreads").head()
    """
    params = {
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
        "eventIds": event_ids,
        "bookmakers": bookmakers,
        "commenceTimeFrom": commence_time_from,
        "commenceTimeTo": commence_time_to,
        "includeLinks": _bool_str(include_links),
        "includeSids": _bool_str(include_sids),
        "includeBetLimits": _bool_str(include_bet_limits),
        "includeRotationNumbers": _bool_str(include_rotation_numbers),
    }
    raw = _toa_get(f"sports/{sport}/odds", params=params, api_key=api_key, **kwargs)
    return parse_toa_odds(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_sports_scores(
    sport: str = "americanfootball_nfl",
    days_from: Optional[int] = None,
    date_format: Optional[str] = "iso",
    event_ids: Optional[str] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, List]:
    """Live + recently-completed scores for a sport (``/v4/sports/{sport}/scores``).

    Args:
        sport: Sport key from :func:`toa_sports`.
        days_from: Include completed games from this many days ago (1-3). Omit for
            live + upcoming only.
        date_format: ``"iso"`` or ``"unix"``.
        event_ids: Optional comma-separated event ids to filter to.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A ``polars``/``pandas`` ``DataFrame`` (one row per event) by default; raw JSON
        ``list`` when ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_scores
            toa_sports_scores(sport="americanfootball_nfl", days_from=3).head()
    """
    params = {"daysFrom": days_from, "dateFormat": date_format, "eventIds": event_ids}
    raw = _toa_get(f"sports/{sport}/scores", params=params, api_key=api_key, **kwargs)
    return parse_toa_scores(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_sports_events(
    sport: str = "americanfootball_nfl",
    date_format: Optional[str] = "iso",
    event_ids: Optional[str] = None,
    commence_time_from: Optional[str] = None,
    commence_time_to: Optional[str] = None,
    include_rotation_numbers: Optional[bool] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, List]:
    """Upcoming + live events for a sport (``/v4/sports/{sport}/events``). Quota: free.

    Args:
        sport: Sport key from :func:`toa_sports`.
        date_format: ``"iso"`` or ``"unix"``.
        event_ids: Optional comma-separated event ids to filter to.
        commence_time_from: ISO8601 lower bound on event commence time.
        commence_time_to: ISO8601 upper bound on event commence time.
        include_rotation_numbers: Include rotation numbers where available.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A ``polars``/``pandas`` ``DataFrame`` (one row per event) by default; raw JSON
        ``list`` when ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_events
            toa_sports_events(sport="americanfootball_nfl").head()
    """
    params = {
        "dateFormat": date_format,
        "eventIds": event_ids,
        "commenceTimeFrom": commence_time_from,
        "commenceTimeTo": commence_time_to,
        "includeRotationNumbers": _bool_str(include_rotation_numbers),
    }
    raw = _toa_get(f"sports/{sport}/events", params=params, api_key=api_key, **kwargs)
    return parse_toa_events(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_event_odds(
    sport: str,
    event_id: str,
    regions: str = "us",
    markets: Optional[str] = "h2h",
    odds_format: Optional[str] = "american",
    date_format: Optional[str] = "iso",
    bookmakers: Optional[str] = None,
    include_links: Optional[bool] = None,
    include_sids: Optional[bool] = None,
    include_bet_limits: Optional[bool] = None,
    include_multipliers: Optional[bool] = None,
    include_rotation_numbers: Optional[bool] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, Dict]:
    """Odds for a single event, incl. player-prop markets
    (``/v4/sports/{sport}/events/{eventId}/odds``).

    Args:
        sport: Sport key from :func:`toa_sports`.
        event_id: Event id from :func:`toa_sports_events`.
        regions: Comma-separated bookmaker regions.
        markets: Comma-separated markets (event-level markets include player props).
        odds_format: ``"american"`` or ``"decimal"``.
        date_format: ``"iso"`` or ``"unix"``.
        bookmakers: Comma-separated bookmaker keys (takes precedence over ``regions``).
        include_links: Include deep links.
        include_sids: Include bookmaker source ids.
        include_bet_limits: Include bet limits where available.
        include_multipliers: Include SGP multipliers where available.
        include_rotation_numbers: Include rotation numbers where available.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A long-form ``polars``/``pandas`` ``DataFrame`` (one row per
        bookmaker x market x outcome) by default; raw JSON ``dict`` when
        ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_events, toa_event_odds
            eid = toa_sports_events(sport="americanfootball_nfl", return_parsed=False)[0]["id"]
            toa_event_odds(sport="americanfootball_nfl", event_id=eid, markets="player_pass_tds").head()
    """
    params = {
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
        "bookmakers": bookmakers,
        "includeLinks": _bool_str(include_links),
        "includeSids": _bool_str(include_sids),
        "includeBetLimits": _bool_str(include_bet_limits),
        "includeMultipliers": _bool_str(include_multipliers),
        "includeRotationNumbers": _bool_str(include_rotation_numbers),
    }
    raw = _toa_get(f"sports/{sport}/events/{event_id}/odds", params=params, api_key=api_key, **kwargs)
    return parse_toa_event_odds(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_event_markets(
    sport: str,
    event_id: str,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    date_format: Optional[str] = "iso",
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, Dict]:
    """Markets available for a single event
    (``/v4/sports/{sport}/events/{eventId}/markets``). Quota: free.

    Args:
        sport: Sport key from :func:`toa_sports`.
        event_id: Event id from :func:`toa_sports_events`.
        regions: Comma-separated bookmaker regions.
        bookmakers: Comma-separated bookmaker keys (takes precedence over ``regions``).
        date_format: ``"iso"`` or ``"unix"``.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A ``polars``/``pandas`` ``DataFrame`` (one row per bookmaker x available
        market) by default; raw JSON ``dict`` when ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_events, toa_event_markets
            eid = toa_sports_events(sport="americanfootball_nfl", return_parsed=False)[0]["id"]
            toa_event_markets(sport="americanfootball_nfl", event_id=eid).head()
    """
    params = {"regions": regions, "bookmakers": bookmakers, "dateFormat": date_format}
    raw = _toa_get(f"sports/{sport}/events/{event_id}/markets", params=params, api_key=api_key, **kwargs)
    return parse_toa_event_markets(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_sports_participants(
    sport: str = "americanfootball_nfl",
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, List]:
    """Teams / participants for a sport (``/v4/sports/{sport}/participants``). Quota: free.

    Args:
        sport: Sport key from :func:`toa_sports`.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A ``polars``/``pandas`` ``DataFrame`` (one row per participant) by default;
        raw JSON ``list`` when ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_participants
            toa_sports_participants(sport="americanfootball_nfl").head()
    """
    raw = _toa_get(f"sports/{sport}/participants", params={}, api_key=api_key, **kwargs)
    return parse_toa_participants(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_sports_odds_history(
    sport: str = "americanfootball_nfl",
    date: str = "2023-11-29T22:45:00Z",
    regions: str = "us",
    markets: Optional[str] = "h2h",
    odds_format: Optional[str] = "american",
    date_format: Optional[str] = "iso",
    event_ids: Optional[str] = None,
    bookmakers: Optional[str] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, Dict]:
    """Historical odds snapshot for a sport
    (``/v4/historical/sports/{sport}/odds``). Paid plans only.

    Args:
        sport: Sport key from :func:`toa_sports`.
        date: ISO8601 timestamp of the snapshot to fetch (the API returns the
            nearest snapshot at or before this time).
        regions: Comma-separated bookmaker regions.
        markets: Comma-separated markets.
        odds_format: ``"american"`` or ``"decimal"``.
        date_format: ``"iso"`` or ``"unix"``.
        event_ids: Optional comma-separated event ids to filter to.
        bookmakers: Comma-separated bookmaker keys.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A long-form ``polars``/``pandas`` ``DataFrame`` (one row per outcome, stamped
        with the snapshot timestamps) by default; the raw JSON snapshot ``dict`` when
        ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_odds_history
            toa_sports_odds_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z").head()
    """
    params = {
        "date": date,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
        "eventIds": event_ids,
        "bookmakers": bookmakers,
    }
    raw = _toa_get(f"historical/sports/{sport}/odds", params=params, api_key=api_key, **kwargs)
    return parse_toa_odds_history(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_sports_events_history(
    sport: str = "americanfootball_nfl",
    date: str = "2023-11-29T22:45:00Z",
    date_format: Optional[str] = "iso",
    event_ids: Optional[str] = None,
    commence_time_from: Optional[str] = None,
    commence_time_to: Optional[str] = None,
    include_rotation_numbers: Optional[bool] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, Dict]:
    """Historical events snapshot for a sport
    (``/v4/historical/sports/{sport}/events``). Paid plans only.

    Args:
        sport: Sport key from :func:`toa_sports`.
        date: ISO8601 timestamp of the snapshot to fetch.
        date_format: ``"iso"`` or ``"unix"``.
        event_ids: Optional comma-separated event ids to filter to.
        commence_time_from: ISO8601 lower bound on event commence time.
        commence_time_to: ISO8601 upper bound on event commence time.
        include_rotation_numbers: Include rotation numbers where available.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A ``polars``/``pandas`` ``DataFrame`` (one row per event, stamped with the
        snapshot timestamps) by default; the raw JSON snapshot ``dict`` when
        ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_sports_events_history
            toa_sports_events_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z").head()
    """
    params = {
        "date": date,
        "dateFormat": date_format,
        "eventIds": event_ids,
        "commenceTimeFrom": commence_time_from,
        "commenceTimeTo": commence_time_to,
        "includeRotationNumbers": _bool_str(include_rotation_numbers),
    }
    raw = _toa_get(f"historical/sports/{sport}/events", params=params, api_key=api_key, **kwargs)
    return parse_toa_events_history(raw, return_as_pandas=return_as_pandas) if return_parsed else raw


def toa_event_odds_history(
    sport: str,
    event_id: str,
    date: str = "2023-11-29T22:45:00Z",
    regions: str = "us",
    markets: Optional[str] = "h2h",
    odds_format: Optional[str] = "american",
    date_format: Optional[str] = "iso",
    bookmakers: Optional[str] = None,
    include_rotation_numbers: Optional[bool] = None,
    include_multipliers: Optional[bool] = None,
    api_key: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Union[DataFrameT, Dict]:
    """Historical odds snapshot for a single event
    (``/v4/historical/sports/{sport}/events/{eventId}/odds``). Paid plans only.

    Args:
        sport: Sport key from :func:`toa_sports`.
        event_id: Event id from :func:`toa_sports_events_history`.
        date: ISO8601 timestamp of the snapshot to fetch.
        regions: Comma-separated bookmaker regions.
        markets: Comma-separated markets (event-level markets include player props).
        odds_format: ``"american"`` or ``"decimal"``.
        date_format: ``"iso"`` or ``"unix"``.
        bookmakers: Comma-separated bookmaker keys.
        include_rotation_numbers: Include rotation numbers where available.
        include_multipliers: Include SGP multipliers where available.
        api_key: The Odds API key (else ``ODDS_API_KEY`` env).
        return_parsed: Parse to a tidy DataFrame (default). ``False`` returns raw JSON.
        return_as_pandas: With ``return_parsed``, return pandas instead of polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A long-form ``polars``/``pandas`` ``DataFrame`` (one row per
        bookmaker x market x outcome, stamped with the snapshot timestamps) by
        default; the raw JSON snapshot ``dict`` when ``return_parsed=False``.

    Example:
        Quick start::

            from sportsdataverse.odds import toa_event_odds_history
            toa_event_odds_history(sport="americanfootball_nfl", event_id="...",
                date="2023-11-29T22:45:00Z").head()
    """
    params = {
        "date": date,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
        "bookmakers": bookmakers,
        "includeRotationNumbers": _bool_str(include_rotation_numbers),
        "includeMultipliers": _bool_str(include_multipliers),
    }
    raw = _toa_get(f"historical/sports/{sport}/events/{event_id}/odds", params=params, api_key=api_key, **kwargs)
    return parse_toa_event_odds_history(raw, return_as_pandas=return_as_pandas) if return_parsed else raw
