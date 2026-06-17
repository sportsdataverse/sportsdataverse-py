"""Parsers for Baseball Savant / Statcast payloads. Universal sdv-py parser
contract: polars by default, pandas via return_as_pandas, snake-case columns,
zero-row frame on empty/malformed input."""

from __future__ import annotations

import json
import re
from io import StringIO
from typing import Dict, List, Union

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


def _html_decode_var(html: str, var_name: str) -> Union[Dict, List, None]:
    """Extract the JSON value assigned to ``var_name`` in an embedded ``<script>``.

    Savant pages embed data as ``var serverVals = {...}`` (player pages) or
    ``const data = [...]`` (the ``fielding-run-value`` / ``statcast-park-factors``
    leaderboards). Handles ``var`` / ``let`` / ``const`` / ``window.`` / bare
    assignment, decodes either an object or an array via balanced-brace
    ``raw_decode`` (so nested Savant payloads are not truncated), and skips any
    same-named assignment whose body fails to decode. The leading
    ``(?<![\\w$.])`` lookbehind anchors a word boundary so a request for ``data``
    does not match ``methods_data``. Returns ``None`` when absent/unparseable.
    """
    if not html:
        return None
    pat = rf"(?<![\w$.])(?:(?:var|let|const)\s+|window\.)?{re.escape(var_name)}\s*=\s*"
    for m in re.finditer(pat, html):
        try:
            obj, _ = json.JSONDecoder().raw_decode(html, m.end())
        except Exception:
            continue
        if isinstance(obj, (dict, list)):
            return obj
    return None


def _html_script_json(html: str, var_name: str) -> Dict:
    """Object-only convenience over :func:`_html_decode_var` (``{}`` if not a dict)."""
    obj = _html_decode_var(html, var_name)
    return obj if isinstance(obj, dict) else {}


def parse_mlb_statcast_search(payload: object, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
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


def parse_mlb_statcast_leaderboard(payload: object, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
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


def parse_mlb_statcast_gamefeed(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Parse a Statcast gamefeed (``/gf``) JSON payload into a tidy per-pitch frame.

    The Savant ``/gf`` feed carries the game's pitch-by-pitch tracking under the
    ``team_home`` and ``team_away`` arrays (one rich object per pitch:
    ``pitch_type``, ``start_speed``, ``launch_speed``, ``launch_angle``,
    ``plate_x``/``plate_z``, ``des``, ``events``, …). This concatenates both
    sides into one frame, one row per pitch. When neither side is present it
    falls back to the ``exit_velocity`` array (batted-ball events only).

    Args:
        payload: JSON dict returned by :func:`mlb_statcast_gamefeed`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per pitch; zero rows on empty input.

    Example:
        Quick start::

            from sportsdataverse.mlb import mlb_statcast_gamefeed, parse_mlb_statcast_gamefeed
            df = parse_mlb_statcast_gamefeed(mlb_statcast_gamefeed(game_pk=745444, return_parsed=False))
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    rows: List[dict] = []
    for side in ("team_home", "team_away"):
        v = payload.get(side)
        if isinstance(v, list):
            rows.extend(v)
    if not rows:
        ev = payload.get("exit_velocity")
        if isinstance(ev, list):
            rows = ev
    if not rows:
        return _empty_frame(return_as_pandas)
    df = pd.json_normalize(rows, sep="_")
    return _to_output(_snake_columns(df), return_as_pandas)


def parse_mlb_statcast_schedule(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Parse the Savant ``/schedule`` JSON into a tidy frame of one row per game.

    The feed wraps the standard MLB Stats API schedule under
    ``schedule.dates[].games[]`` (plus a ``wpa`` array). This flattens every
    game across all dates into one row, snake-cased.

    Args:
        payload: JSON dict returned by :func:`mlb_statcast_schedule`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per scheduled game; zero rows on empty input.

    Example:
        Quick start::

            from sportsdataverse.mlb import mlb_statcast_schedule
            df = mlb_statcast_schedule()
    """
    sched = payload.get("schedule") if isinstance(payload, dict) else None
    dates = sched.get("dates") if isinstance(sched, dict) else None
    if not isinstance(dates, list):
        return _empty_frame(return_as_pandas)
    games: List[dict] = []
    for d in dates:
        if isinstance(d, dict) and isinstance(d.get("games"), list):
            games.extend(d["games"])
    if not games:
        return _empty_frame(return_as_pandas)
    df = pd.json_normalize(games, sep="_")
    return _to_output(_snake_columns(df), return_as_pandas)


def parse_mlb_statcast_html_leaderboard(payload: str, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Parse an HTML-embedded-JSON Statcast leaderboard into a tidy frame.

    A couple of leaderboards (``fielding-run-value``, ``statcast-park-factors``)
    return ``text/html`` even with ``csv=true``; the rows live in an embedded
    ``const data = [...]`` ``<script>`` array. This extracts and flattens that
    array.

    Args:
        payload: HTML page text returned by the leaderboard endpoint.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per leaderboard entry; zero rows on empty input.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_html_leaderboard
            df = parse_mlb_statcast_html_leaderboard(html_text)
    """
    rows = _html_decode_var(payload if isinstance(payload, str) else "", "data")
    if not isinstance(rows, list) or not rows:
        return _empty_frame(return_as_pandas)
    df = pd.json_normalize(rows, sep="_")
    return _to_output(_snake_columns(df), return_as_pandas)


def parse_mlb_statcast_player(
    payload: str, section: str = "statcast", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Parse a Savant player page into a tidy frame of one of its embedded tables.

    The ``/savant-player/{id}`` page embeds a large ``var serverVals = {...}`` blob
    whose array-valued keys are the page's data tables. ``section`` selects which
    one — default ``"statcast"`` (the seasonal Statcast aggregate, ~260 metrics per
    season row). Other useful sections include ``"statcastGameLogs"`` (per-game
    batted-ball logs), ``"statcastHistogram"``, ``"zones"``, ``"pitchDetails"``,
    ``"sprayChart"``, ``"fielderPositioning"``, and ``"statcastLeader"``.

    Args:
        payload: HTML page text (e.g. from ``mlb_statcast_player(..., raw=True)``).
        section: name of the ``serverVals`` array key to flatten (default ``"statcast"``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per record in ``section``; zero rows
        when the page or section is absent.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_player
            df = parse_mlb_statcast_player(html_text)                       # seasonal aggregate
            logs = parse_mlb_statcast_player(html_text, section="statcastGameLogs")
    """
    rows = _html_script_json(payload or "", "serverVals").get(section)
    if not isinstance(rows, list) or not rows:
        return _empty_frame(return_as_pandas)
    df = pd.json_normalize(rows, sep="_")
    return _to_output(_snake_columns(df), return_as_pandas)
