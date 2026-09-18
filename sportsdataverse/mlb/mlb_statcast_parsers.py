"""Parsers for Baseball Savant / Statcast payloads. Universal sdv-py parser
contract: polars by default, pandas via return_as_pandas, snake-case columns,
zero-row frame on empty/malformed input."""

from __future__ import annotations

import json
import re
import warnings
from io import StringIO
from typing import Dict, Iterable, List, Optional, Set, Union

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


#: Savant CSV columns holding MLBAM integer ids (players, game). pandas reads an integer
#: column with any blank cell as float64 -- ``on_1b`` is blank whenever first base is
#: empty -- so these are pinned back to nullable Int64 at the parse boundary.
_MLBAM_ID_COLUMNS = ("batter", "pitcher", "on_1b", "on_2b", "on_3b", *(f"fielder_{i}" for i in range(2, 10)), "game_pk")


def _pin_id_columns(df: pd.DataFrame) -> List[str]:
    """Cast the MLBAM id columns present in ``df`` to nullable ``Int64``, in place.

    Integer-read, integral float-read, and digit-string columns (the ``/gf`` JSON
    serializes ``game_pk`` as ``"745444"``) are cast. A column holding a non-integral
    or non-numeric value is left as read (never truncated or nulled) and its name is
    returned.
    """
    uncast: List[str] = []
    for col in _MLBAM_ID_COLUMNS:
        if col not in df.columns:
            continue
        s = df[col]
        if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            try:
                s = pd.to_numeric(s)
            except (ValueError, TypeError):
                uncast.append(col)
                continue
        if pd.api.types.is_float_dtype(s) and not (s.dropna() % 1 == 0).all():
            uncast.append(col)
        elif pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
            df[col] = s.astype("Int64")
    return uncast


def _warn_uncast_ids(cols: Iterable[str], stacklevel: int) -> None:
    """Warn once about id columns left uncast; ``stacklevel`` is counted from this helper's caller."""
    if cols:
        warnings.warn(
            f"Savant CSV id columns {sorted(cols)} hold non-integral or non-numeric values; "
            "left as read, not cast to Int64.",
            stacklevel=stacklevel + 1,
        )


def _csv_to_frame(
    text: str, return_as_pandas: bool = False, uncast_ids: Optional[Set[str]] = None
) -> pl.DataFrame | pd.DataFrame:
    """Read a Savant CSV into a snake-cased frame with its MLBAM id columns pinned to ``Int64``.

    When ``uncast_ids`` is given, id columns left uncast are added to it so a
    multi-chunk caller can warn once; otherwise this warns, attributed to the
    caller of the public parser.

    A header-only body keeps its columns (the documented schema) with the id
    columns ``Int64``; the remaining columns have no values to infer a dtype from
    and carry polars' ``Null``, which widens into a populated frame's dtype rather
    than forcing it to ``String``.
    """
    if not text or not text.strip():
        return _empty_frame(return_as_pandas)
    try:
        df = pd.read_csv(StringIO(text))
    except Exception:
        return _empty_frame(return_as_pandas)
    # A header-only body (no games in the window) reads as 0 rows x 119 columns: keep the
    # columns so the empty frame carries the documented schema with its ids pinned.
    df = _snake_columns(df)
    uncast = _pin_id_columns(df)
    if uncast_ids is None:
        _warn_uncast_ids(uncast, stacklevel=3)  # _csv_to_frame <- parse_mlb_statcast_* <- caller
    else:
        uncast_ids.update(uncast)
    out = _to_output(df, return_as_pandas)
    if not return_as_pandas and out.height == 0:
        # No rows to infer dtypes from: pandas reads every un-pinned column as object, which
        # lands as polars String and would silently widen a populated frame's Float64 columns
        # to String in a caller's concat (or make a strict concat raise). Null is polars'
        # unknown dtype -- it widens to whatever the populated side holds, so an empty-window
        # result composes with a populated one instead of poisoning it.
        out = out.with_columns(pl.col(c).cast(pl.Null) for c in out.columns if out.schema[c] == pl.String)
    return out


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
        A polars (or pandas) DataFrame, one row per search result; zero rows on empty
        input (a header-only response keeps its columns, ids ``Int64``).

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
        A polars (or pandas) DataFrame, one row per leaderboard entry; zero rows on empty
        input (a header-only response keeps its columns, ids ``Int64``).

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
        The MLBAM id columns (``game_pk``, ``batter``, ``pitcher``, …) are ``Int64``
        when every value parses as an integer -- the same dtype
        :func:`parse_mlb_statcast_search` gives them, so the two join.

    Warns:
        UserWarning: When an MLBAM id column holds a non-integral or non-numeric
            value. That column is left as read (never truncated or nulled), so it
            stays ``String``/``Float64`` and a join against the search parser's
            ``Int64`` raises ``SchemaError`` rather than silently dropping rows.

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
    df = _snake_columns(pd.json_normalize(rows, sep="_"))
    _warn_uncast_ids(_pin_id_columns(df), stacklevel=2)  # parse_mlb_statcast_gamefeed <- caller
    return _to_output(df, return_as_pandas)


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
