"""Parsers for Baseball Savant / Statcast payloads. Universal sdv-py parser
contract: polars by default, pandas via return_as_pandas, snake-case columns,
zero-row frame on empty/malformed input."""

from __future__ import annotations

import json
import re
from io import StringIO
from typing import Dict

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(str(c)).replace(".", "_") for c in df.columns]
    return df


def _empty_frame(return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    return pd.DataFrame() if return_as_pandas else pl.DataFrame()


def _to_output(df: pd.DataFrame, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        # polars rejected a mixed/list-valued column — stringify object columns so the frame still converts (sdv-py parser convention).
        df2 = df.copy()
        for col in [c for c in df2.columns if df2[c].dtype == "object"]:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _csv_to_frame(text: str, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    if not text or not text.strip():
        return _empty_frame(return_as_pandas)
    try:
        df = pd.read_csv(StringIO(text))
    except Exception:
        return _empty_frame(return_as_pandas)
    if df.empty:
        return _empty_frame(return_as_pandas)
    return _to_output(_snake_columns(df), return_as_pandas)


def _html_script_json(html: str, var_name: str) -> Dict:
    if not html:
        return {}
    m = re.search(rf"var\s+{re.escape(var_name)}\s*=\s*", html)
    if not m:
        return {}
    try:
        obj, _ = json.JSONDecoder().raw_decode(html, m.end())
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def parse_mlb_statcast_search(payload, return_as_pandas: bool = False):
    """Parse a Statcast search CSV payload into a tidy frame.

    Args:
        payload: CSV text returned by a Savant `/search` endpoint (``csv=true``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per search result; zero rows on empty input.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_search
            df = parse_mlb_statcast_search(csv_text)
    """
    return _csv_to_frame(payload if isinstance(payload, str) else "", return_as_pandas)


def parse_mlb_statcast_leaderboard(payload, return_as_pandas: bool = False):
    """Parse a Statcast leaderboard CSV payload into a tidy frame.

    Args:
        payload: CSV text returned by a Savant ``/leaderboard/*`` endpoint (``csv=true``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per leaderboard entry; zero rows on empty input.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_leaderboard
            df = parse_mlb_statcast_leaderboard(csv_text)
    """
    return _csv_to_frame(payload if isinstance(payload, str) else "", return_as_pandas)


def parse_mlb_statcast_gamefeed(payload: Dict, return_as_pandas: bool = False):
    """Parse a Statcast gamefeed JSON payload into a tidy frame of tracked events.

    Args:
        payload: JSON dict returned by a Savant `/game` gamefeed endpoint.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per tracked event; zero rows on empty input.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_gamefeed
            df = parse_mlb_statcast_gamefeed(payload_dict)
    """
    events = (payload or {}).get("events")
    if not isinstance(events, list) or not events:
        return _empty_frame(return_as_pandas)
    df = pd.json_normalize(events, sep="_")
    return _to_output(_snake_columns(df), return_as_pandas)


def parse_mlb_statcast_player(payload: str, return_as_pandas: bool = False):
    """Parse a Statcast player page HTML into a tidy frame of player metrics.

    Args:
        payload: HTML page text returned by a Savant player page endpoint.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per metric; zero rows on empty input.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_player
            df = parse_mlb_statcast_player(html_text)
    """
    rows = _html_script_json(payload or "", "serverVals").get("rows")
    if not isinstance(rows, list) or not rows:
        return _empty_frame(return_as_pandas)
    df = pd.json_normalize(rows, sep="_")
    return _to_output(_snake_columns(df), return_as_pandas)
