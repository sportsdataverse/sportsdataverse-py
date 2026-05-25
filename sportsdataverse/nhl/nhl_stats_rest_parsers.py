"""sportsdataverse.nhl.nhl_stats_rest_parsers — polars parsers for the
NHL Stats REST API at ``api.nhle.com/stats/rest/``.

**Documentation**:

* NHL Stats REST endpoint reference: https://py.sportsdataverse.org/docs/nhl/stats-rest
* Parsers overview: https://py.sportsdataverse.org/docs/parsers/

Every Stats REST endpoint ships its rows under the same top-level
``{data: [...], total: N}`` shape, so a single generic parser
:func:`parse_nhl_stats_rest` handles all 21 wrappers in
:mod:`sportsdataverse.nhl.nhl_stats_rest`.

The meta endpoints (``stats_rest_config``, ``stats_rest_componentSeason``,
``stats_rest_ping``) return non-``data``-keyed payloads and are not in
the registry — they pass through as raw ``Dict``.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    return df


def _to_output(df: pd.DataFrame, return_as_pandas: bool):
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        df2 = df.copy()
        for col in df2.select_dtypes(include="object").columns:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _empty_frame(return_as_pandas: bool = False):
    df = pd.DataFrame()
    return df if return_as_pandas else pl.DataFrame()


def parse_nhl_stats_rest(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Parse any NHL Stats REST response into a tidy frame.

    Every Stats REST endpoint ships ``{data: [{...}, ...], total: N}``.
    This parser unwraps ``data`` and flattens it via
    :func:`pandas.json_normalize`.

    Args:
        payload: Raw JSON dict from any ``nhl_stats_rest_*`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per record. Zero rows
        for meta payloads (``config``, ``componentSeason``, ``ping``)
        that don't carry a ``data`` array.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(rows, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# Map nhl_stats_rest_* wrapper names to the parser. Every data endpoint
# maps to parse_nhl_stats_rest; meta endpoints (config / componentSeason /
# ping / glossary) are intentionally excluded — they return raw Dict
# even when callers use the parser layer.
NHL_STATS_REST_ENDPOINT_PARSERS = {
    "nhl_stats_rest_country":             parse_nhl_stats_rest,
    "nhl_stats_rest_draft":               parse_nhl_stats_rest,
    "nhl_stats_rest_franchise":           parse_nhl_stats_rest,
    "nhl_stats_rest_game":                parse_nhl_stats_rest,
    "nhl_stats_rest_glossary":            parse_nhl_stats_rest,
    "nhl_stats_rest_goalie_report":       parse_nhl_stats_rest,
    "nhl_stats_rest_leaders_goalies":     parse_nhl_stats_rest,
    "nhl_stats_rest_leaders_skaters":     parse_nhl_stats_rest,
    "nhl_stats_rest_milestones_goalies":  parse_nhl_stats_rest,
    "nhl_stats_rest_milestones_skaters":  parse_nhl_stats_rest,
    "nhl_stats_rest_players":             parse_nhl_stats_rest,
    "nhl_stats_rest_season":              parse_nhl_stats_rest,
    "nhl_stats_rest_shiftcharts":         parse_nhl_stats_rest,
    "nhl_stats_rest_skater_report":       parse_nhl_stats_rest,
    "nhl_stats_rest_team":                parse_nhl_stats_rest,
    "nhl_stats_rest_team_by_id":          parse_nhl_stats_rest,
    "nhl_stats_rest_team_report":         parse_nhl_stats_rest,
}


def parser_for_nhl_stats_rest(fn_name: str):
    """Return the registered parser for an ``nhl_stats_rest_*`` wrapper.

    Falls back to :func:`parse_nhl_stats_rest` (the generic ``data``-
    array flattener) for any unregistered name, so the caller always
    gets a DataFrame-returning callable.

    Args:
        fn_name: The ``__name__`` of any ``nhl_stats_rest_*`` wrapper.

    Returns:
        Parser callable. Never ``None``.
    """
    return NHL_STATS_REST_ENDPOINT_PARSERS.get(fn_name, parse_nhl_stats_rest)
