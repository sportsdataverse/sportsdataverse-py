"""Polars parsers for The Odds API (``api.the-odds-api.com/v4``).

Each ``parse_toa_*`` flattens one :mod:`sportsdataverse.odds.the_odds_api` wrapper's
raw JSON into a tidy frame. The odds-bearing endpoints (``/odds`` and the per-event
``/events/{id}/odds``) are unrolled to **long** form -- one row per
event x bookmaker x market x outcome -- which is the shape downstream modelling
wants; the list endpoints (``/sports``, ``/events``, ``/scores``,
``/participants``) flatten one row per record.

All parsers follow the shared ``*_parsers.py`` contract:
``pandas.json_normalize(..., sep="_")`` for one-pass flattening, list-valued cells
stringified so polars can ingest the frame, and column names snake-cased via
:func:`sportsdataverse.dl_utils.underscore`. Every parser returns a
``polars.DataFrame`` by default; pass ``return_as_pandas=True`` for pandas.

The historical endpoints wrap their payload in a snapshot envelope
(``{timestamp, previous_timestamp, next_timestamp, data}``); the ``*_history``
parsers unwrap ``data`` and stamp each row with the snapshot ``timestamp``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Union

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports (PEP 563 defers eval)
    import pandas as pd
    import polars as pl

DataFrameT = Union["pl.DataFrame", "pd.DataFrame"]

__all__ = [
    "parse_toa_sports",
    "parse_toa_odds",
    "parse_toa_scores",
    "parse_toa_events",
    "parse_toa_event_odds",
    "parse_toa_event_markets",
    "parse_toa_participants",
    "parse_toa_odds_history",
    "parse_toa_events_history",
    "parse_toa_event_odds_history",
]


def _to_frame(records: List, return_as_pandas: bool) -> DataFrameT:
    """Flatten a list of (possibly nested) dicts into a polars/pandas DataFrame.

    Follows the shared parser contract: ``pandas.json_normalize`` for one-pass
    flattening, list-valued cells stringified so polars accepts the frame, and
    columns snake-cased via :func:`sportsdataverse.dl_utils.underscore`.

    Args:
        records: A list of JSON record dicts. ``None`` / empty / malformed yields a
            zero-row frame rather than raising.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame`` with flattened, snake-cased columns.
    """
    import pandas as pd
    import polars as pl

    from sportsdataverse.dl_utils import underscore

    if not records:
        return pd.DataFrame() if return_as_pandas else pl.DataFrame()
    try:
        df = pd.json_normalize(records, sep="_")
    except Exception:  # noqa: BLE001 -- malformed payload -> zero-row frame, never raise
        return pd.DataFrame() if return_as_pandas else pl.DataFrame()
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:  # noqa: BLE001 -- fall back to all-string object cols
        df2 = df.copy()
        for col in [c for c in df2.columns if df2[c].dtype == "object"]:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _flatten_odds(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Unroll a list of event dicts into long ``one row per outcome`` records.

    Each event carries ``bookmakers[].markets[].outcomes[]``; this explodes that
    nesting into flat rows tagged with the event, bookmaker, market and outcome
    fields. Events with no bookmakers still yield one bare event row so they are
    not silently dropped.
    """
    rows: List[Dict[str, Any]] = []
    for ev in events or []:
        if not isinstance(ev, dict):
            continue
        base = {
            "event_id": ev.get("id"),
            "sport_key": ev.get("sport_key"),
            "sport_title": ev.get("sport_title"),
            "commence_time": ev.get("commence_time"),
            "home_team": ev.get("home_team"),
            "away_team": ev.get("away_team"),
        }
        bookmakers = ev.get("bookmakers") or []
        if not bookmakers:
            rows.append(dict(base))
            continue
        for bk in bookmakers:
            bk_base = {
                **base,
                "bookmaker_key": bk.get("key"),
                "bookmaker_title": bk.get("title"),
                "bookmaker_last_update": bk.get("last_update"),
            }
            markets = bk.get("markets") or []
            if not markets:
                rows.append(dict(bk_base))
                continue
            for mk in markets:
                mk_base = {
                    **bk_base,
                    "market_key": mk.get("key"),
                    "market_last_update": mk.get("last_update"),
                }
                outcomes = mk.get("outcomes") or []
                if not outcomes:
                    rows.append(dict(mk_base))
                    continue
                for oc in outcomes:
                    rows.append(
                        {
                            **mk_base,
                            "outcome_name": oc.get("name"),
                            "outcome_description": oc.get("description"),
                            "outcome_price": oc.get("price"),
                            "outcome_point": oc.get("point"),
                            "outcome_link": oc.get("link"),
                            "outcome_sid": oc.get("sid"),
                        },
                    )
    return rows


def _unwrap_snapshot(raw: Any) -> tuple[Any, Dict[str, Any]]:
    """Split a historical snapshot envelope into ``(data, snapshot_meta)``.

    Historical endpoints return ``{timestamp, previous_timestamp, next_timestamp,
    data}``; current endpoints return the bare payload. Returns the inner payload
    plus a dict of the snapshot timestamps (empty for non-historical payloads).
    """
    if isinstance(raw, dict) and "data" in raw:
        meta = {
            "snapshot_timestamp": raw.get("timestamp"),
            "previous_timestamp": raw.get("previous_timestamp"),
            "next_timestamp": raw.get("next_timestamp"),
        }
        return raw.get("data"), meta
    return raw, {}


def parse_toa_sports(raw: List[Dict], return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/sports`` -> one row per available sport/league.

    Args:
        raw: Raw JSON list from :func:`sportsdataverse.odds.toa_sports`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per sport
        (``key``, ``group``, ``title``, ``description``, ``active``, ``has_outcomes``).

    Example:
        >>> from sportsdataverse.odds import toa_sports, parse_toa_sports
        >>> parse_toa_sports(toa_sports(return_parsed=False)).head()
    """
    return _to_frame(raw if isinstance(raw, list) else [], return_as_pandas)


def parse_toa_odds(raw: List[Dict], return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/sports/{sport}/odds`` -> long form, one row per outcome.

    Args:
        raw: Raw JSON list of events from :func:`sportsdataverse.odds.toa_sports_odds`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per
        event x bookmaker x market x outcome.

    Example:
        >>> from sportsdataverse.odds import toa_sports_odds, parse_toa_odds
        >>> raw = toa_sports_odds(sport="americanfootball_nfl", regions="us", return_parsed=False)
        >>> parse_toa_odds(raw).head()
    """
    return _to_frame(_flatten_odds(raw if isinstance(raw, list) else []), return_as_pandas)


def parse_toa_scores(raw: List[Dict], return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/sports/{sport}/scores`` -> one row per event (scores flattened).

    Args:
        raw: Raw JSON list of events from :func:`sportsdataverse.odds.toa_sports_scores`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per event with
        ``completed`` / ``scores`` / ``last_update``.

    Example:
        >>> from sportsdataverse.odds import toa_sports_scores, parse_toa_scores
        >>> raw = toa_sports_scores(sport="americanfootball_nfl", days_from=3, return_parsed=False)
        >>> parse_toa_scores(raw).head()
    """
    return _to_frame(raw if isinstance(raw, list) else [], return_as_pandas)


def parse_toa_events(raw: List[Dict], return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/sports/{sport}/events`` -> one row per event.

    Args:
        raw: Raw JSON list of events from :func:`sportsdataverse.odds.toa_sports_events`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per event
        (``id``, ``sport_key``, ``commence_time``, ``home_team``, ``away_team``).

    Example:
        >>> from sportsdataverse.odds import toa_sports_events, parse_toa_events
        >>> parse_toa_events(toa_sports_events(sport="americanfootball_nfl", return_parsed=False)).head()
    """
    return _to_frame(raw if isinstance(raw, list) else [], return_as_pandas)


def parse_toa_event_odds(raw: Union[Dict, List], return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/sports/{sport}/events/{eventId}/odds`` -> long form (single event).

    Args:
        raw: Raw JSON single-event object from
            :func:`sportsdataverse.odds.toa_event_odds`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per
        bookmaker x market x outcome for the event.

    Example:
        >>> from sportsdataverse.odds import toa_event_odds, parse_toa_event_odds
        >>> raw = toa_event_odds(sport="americanfootball_nfl", event_id="...", regions="us", return_parsed=False)
        >>> parse_toa_event_odds(raw).head()
    """
    events = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []
    return _to_frame(_flatten_odds(events), return_as_pandas)


def parse_toa_event_markets(raw: Union[Dict, List], return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/sports/{sport}/events/{eventId}/markets`` -> one row per bookmaker market.

    Args:
        raw: Raw JSON single-event object from
            :func:`sportsdataverse.odds.toa_event_markets`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per
        event x bookmaker x available market.

    Example:
        >>> from sportsdataverse.odds import toa_event_markets, parse_toa_event_markets
        >>> raw = toa_event_markets(sport="americanfootball_nfl", event_id="...", regions="us", return_parsed=False)
        >>> parse_toa_event_markets(raw).head()
    """
    events = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []
    rows: List[Dict[str, Any]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        base = {
            "event_id": ev.get("id"),
            "sport_key": ev.get("sport_key"),
            "sport_title": ev.get("sport_title"),
            "commence_time": ev.get("commence_time"),
            "home_team": ev.get("home_team"),
            "away_team": ev.get("away_team"),
        }
        for bk in ev.get("bookmakers") or []:
            for mk in bk.get("markets") or []:
                rows.append(
                    {
                        **base,
                        "bookmaker_key": bk.get("key"),
                        "bookmaker_title": bk.get("title"),
                        "market_key": mk.get("key"),
                        "market_last_update": mk.get("last_update"),
                    },
                )
    return _to_frame(rows, return_as_pandas)


def parse_toa_participants(raw: List[Dict], return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/sports/{sport}/participants`` -> one row per participant.

    Args:
        raw: Raw JSON list from :func:`sportsdataverse.odds.toa_sports_participants`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per participant
        (``full_name`` plus any ids the sport exposes).

    Example:
        >>> from sportsdataverse.odds import toa_sports_participants, parse_toa_participants
        >>> parse_toa_participants(toa_sports_participants(sport="americanfootball_nfl", return_parsed=False)).head()
    """
    return _to_frame(raw if isinstance(raw, list) else [], return_as_pandas)


def parse_toa_odds_history(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/historical/sports/{sport}/odds`` -> long form with snapshot timestamps.

    Args:
        raw: Raw JSON snapshot envelope from
            :func:`sportsdataverse.odds.toa_sports_odds_history`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per outcome, each
        stamped with the snapshot ``snapshot_timestamp`` / ``previous_timestamp`` /
        ``next_timestamp``.

    Example:
        >>> from sportsdataverse.odds import toa_sports_odds_history, parse_toa_odds_history
        >>> raw = toa_sports_odds_history(sport="americanfootball_nfl", regions="us",
        ...     date="2023-11-29T22:45:00Z", return_parsed=False)
        >>> parse_toa_odds_history(raw).head()
    """
    data, meta = _unwrap_snapshot(raw)
    events = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    rows = [{**meta, **row} for row in _flatten_odds(events)]
    return _to_frame(rows, return_as_pandas)


def parse_toa_events_history(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/historical/sports/{sport}/events`` -> one row per event with snapshot ts.

    Args:
        raw: Raw JSON snapshot envelope from
            :func:`sportsdataverse.odds.toa_sports_events_history`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per event, stamped with
        the snapshot timestamps.

    Example:
        >>> from sportsdataverse.odds import toa_sports_events_history, parse_toa_events_history
        >>> raw = toa_sports_events_history(sport="americanfootball_nfl",
        ...     date="2023-11-29T22:45:00Z", return_parsed=False)
        >>> parse_toa_events_history(raw).head()
    """
    data, meta = _unwrap_snapshot(raw)
    events = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    rows = [{**meta, **ev} for ev in events if isinstance(ev, dict)]
    return _to_frame(rows, return_as_pandas)


def parse_toa_event_odds_history(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """``/v4/historical/sports/{sport}/events/{eventId}/odds`` -> long form (single event).

    Args:
        raw: Raw JSON snapshot envelope from
            :func:`sportsdataverse.odds.toa_event_odds_history`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per
        bookmaker x market x outcome, stamped with the snapshot timestamps.

    Example:
        >>> from sportsdataverse.odds import toa_event_odds_history, parse_toa_event_odds_history
        >>> raw = toa_event_odds_history(sport="americanfootball_nfl", event_id="...",
        ...     regions="us", date="2023-11-29T22:45:00Z", return_parsed=False)
        >>> parse_toa_event_odds_history(raw).head()
    """
    data, meta = _unwrap_snapshot(raw)
    events = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    rows = [{**meta, **row} for row in _flatten_odds(events)]
    return _to_frame(rows, return_as_pandas)
