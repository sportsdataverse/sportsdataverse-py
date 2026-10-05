"""Parsers for the Bart Torvik (T-Rank) data-file wrappers.

One generic CSV parser covers every self-describing Torvik file
(``{year}_team_results.csv``, ``{year}_fffinal.csv``, and their women's
``/ncaaw`` mirrors): the header row is cleaned to snake_case (janitor-style:
``%`` -> ``_percent``, leading digits get an ``x`` prefix, duplicates get a
``_2``/``_3`` suffix) and the rows are type-inferred. The output keeps the
``team`` / ``conf`` columns the basketball crosswalks consume.
"""

from __future__ import annotations

import csv
import io
import json
import re
from typing import TYPE_CHECKING, Any, Dict, List, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_torvik_csv", "parse_torvik_game_stats", "parse_torvik_player_stats", "parse_torvik_game_schedule"]


#: Document markers that mean "this body is markup, not a data file". Matched
#: against the start of the payload, so a CSV cell containing "<html>" mid-file
#: is unaffected.
_HTML_MARKERS = ("<!doctype", "<html", "<?xml", "<head", "<body")


def _clean_col(name: str) -> str:
    """janitor::make_clean_names-style cleaner for a single Torvik CSV header."""
    s = str(name).strip().replace("%", " percent ")
    # fold a digit-glued capital ("3P", "2p%D") to lowercase so it stays one
    # token ("x3p_percent"), instead of splitting to "x3_p_percent"
    s = re.sub(r"(?<=\d)([A-Z])(?=[^a-z]|$)", lambda m: m.group(1).lower(), s)
    s = underscore(s)
    s = re.sub(r"[^0-9a-z]+", "_", s.lower()).strip("_")
    s = re.sub(r"_+", "_", s) or "col"
    if s[0].isdigit():
        s = "x" + s
    return s


def _clean_cols(names: List[str]) -> List[str]:
    """Clean a header row and de-duplicate repeats with ``_2``/``_3`` suffixes."""
    out: List[str] = []
    seen: dict = {}
    for n in names:
        c = _clean_col(n)
        seen[c] = seen.get(c, 0) + 1
        out.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    return out


def parse_torvik_csv(payload: object, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse a Torvik CSV-with-header payload into a tidy frame.

    Args:
        payload: CSV text returned by a barttorvik.com data-file endpoint
            (e.g. ``{year}_team_results.csv`` or ``{year}_fffinal.csv``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per team, with snake-cased,
        de-duplicated column names; zero rows on empty/malformed input.

    Raises:
        ValueError: The payload is an HTML document rather than a data file.
            barttorvik.com answers a transient outage with an HTML page and
            HTTP 200, and an HTML body parses as a one-column CSV whose header
            is the DOCTYPE -- which reaches the caller as a baffling
            ``ColumnNotFoundError: unable to find column "team"`` several
            frames away. Say what actually happened instead.
        None: other malformed input (non-text payload, header-only body,
            unterminated quoted field) yields a zero-row frame rather than
            raising, so callers can chain without a null-check. Only a
            ``pandas`` import failure under ``return_as_pandas=True``
            propagates.

    Example:
        Quick start::

            from sportsdataverse.mbb import torvik_ratings
            from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv
            df = parse_torvik_csv(torvik_ratings(year=2025, return_parsed=False))

        See Also:
            * `hoopR`_ - R sister package for men's college basketball
            * `wehoop`_ - R sister package for women's basketball (``/ncaaw`` mirror)
            * `Bart Torvik`_ - data origin (T-Rank)

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _Bart Torvik: https://barttorvik.com
    """
    text = payload if isinstance(payload, str) else ""
    # An HTML body is not an empty data file and must not be read as one.
    # barttorvik.com serves an HTML page with HTTP 200 during an outage; the
    # DOCTYPE then becomes the sole column name and the failure surfaces far
    # from here. The crosswalk builders wrap this in require_source(), whose
    # contract is exactly that a payload which will not render is a build
    # failure rather than a silently empty source.
    # Match real document markers, not a bare "<": a CSV whose first header
    # cell is "<team>" is still a CSV, and a lone "<" belongs on the zero-row
    # path below. A UTF-8 BOM ahead of the DOCTYPE is stripped first.
    if text.lstrip("﻿").lstrip()[:200].lower().startswith(_HTML_MARKERS):
        snippet = " ".join(text.split())[:120]
        raise ValueError(
            "expected a barttorvik.com CSV data file but received an HTML "
            f"document (starts: {snippet!r}). The host answers a transient "
            "outage with an HTML page and HTTP 200; retry, or check the "
            "endpoint URL for that season."
        )
    if not text.strip() or "\n" not in text.strip():
        df = pl.DataFrame()
    else:
        try:
            header = next(iter(pl.read_csv(io.StringIO(text), has_header=False, n_rows=1).rows()))
            names = _clean_cols([str(h) if h is not None and str(h).strip() else "unnamed" for h in header])
            df = pl.read_csv(
                io.StringIO(text),
                has_header=False,
                skip_rows=1,
                new_columns=names,
                infer_schema_length=10000,
            )
        except Exception:  # noqa: BLE001 -- structurally broken CSV (e.g. unterminated
            # quoted field) is "no data", same contract as an empty payload above.
            df = pl.DataFrame()
    if return_as_pandas:
        return df.to_pandas()
    return df


#: The 31 positional fields of ``getgamestats.php?json=1`` (one team-game per row).
GAME_STATS_COLS = [
    "date", "type", "team", "conf", "opp", "venue", "result", "adj_oe", "adj_de",
    "oe", "off_efg", "off_to", "off_or", "off_ftr", "de", "def_efg", "def_to",
    "def_or", "def_ftr", "game_score", "opp_conf", "quad", "year", "tempo",
    "muid", "coach", "opp_coach", "margin", "win_prob", "game_stats", "overtimes",
]  # fmt: skip

#: The 67 positional fields of ``getadvstats.php?csv=1`` (one player per row).
PLAYER_STATS_COLS = [
    "player_name", "team", "conf", "games", "min_pct", "o_rtg", "usage", "e_fg",
    "ts_pct", "orb_pct", "drb_pct", "ast_pct", "to_pct", "ftm", "fta", "ft_pct",
    "two_pm", "two_pa", "two_p_pct", "three_pm", "three_pa", "three_p_pct",
    "blk_pct", "stl_pct", "ftr", "class", "height", "number", "porpag", "adj_oe",
    "pfr", "year", "player_id", "hometown", "rec_rank", "ast_to", "rim_made",
    "rim_attempts", "mid_made", "mid_attempts", "rim_pct", "mid_pct", "dunks_made",
    "dunks_attempts", "dunks_pct", "pick", "drtg", "adrtg", "dporpag", "stops",
    "bpm", "obpm", "dbpm", "gbpm", "minutes", "ogbpm", "dgbpm", "oreb", "dreb",
    "treb", "ast", "stl", "blk", "pts", "role", "threat", "recruit_date",
]  # fmt: skip

_GAME_STATS_STR = {
    "date",
    "team",
    "conf",
    "opp",
    "venue",
    "result",
    "opp_conf",
    "muid",
    "coach",
    "opp_coach",
    "game_stats",
}
_GAME_STATS_INT = {"type", "quad", "year", "overtimes"}
_PLAYER_STATS_STR = {"player_name", "team", "conf", "class", "height", "hometown", "role", "recruit_date"}
_PLAYER_STATS_INT = {"games", "number", "year", "player_id"}


def _reject_html(text: str, what: str) -> None:
    if text.lstrip("﻿").lstrip()[:200].lower().startswith(_HTML_MARKERS):
        snippet = " ".join(text.split())[:120]
        raise ValueError(
            f"expected a barttorvik.com {what} but received an HTML document (starts: {snippet!r}). "
            "The host answers a transient outage with an HTML page and HTTP 200; retry."
        )


def _positional_frame(rows: List[list], cols: List[str], strs: set, ints: set) -> pl.DataFrame:
    """Build a typed frame from positional rows; extra cells keep ``field_<n>`` names."""
    schema: Dict[str, Any] = {c: pl.Utf8 if c in strs else pl.Int64 if c in ints else pl.Float64 for c in cols}
    width = max([len(r) for r in rows], default=0)
    for i in range(len(cols), width):
        schema[f"field_{i}"] = pl.Utf8
    data = {}
    for i, (name, dt) in enumerate(schema.items()):
        vals = [r[i] if i < len(r) else None for r in rows]
        if dt == pl.Utf8:
            vals = [None if v is None else str(v) for v in vals]
        # strict=False: Torvik mixes int and float cells in numeric columns
        data[name] = pl.Series(name, vals, dtype=dt, strict=False)
    return pl.DataFrame(data, schema=schema)


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    return df.to_pandas() if return_as_pandas else df


def parse_torvik_game_stats(payload: Any, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse ``getgamestats.php?year=&json=1`` into one row per team-game.

    The payload is a headerless JSON array of 31-element arrays, so the column
    names are positional (:data:`GAME_STATS_COLS`). ``game_stats`` is a
    JSON-encoded string of per-game box-score counts exactly as Torvik ships
    it; ``game_date`` is ``date`` (``%m/%d/%y``) parsed to a ``Date``.

    Args:
        payload: Raw JSON text or the already-decoded list of rows.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame; zero rows (full schema) on empty or
        malformed input.

    Raises:
        ValueError: The payload is an HTML document rather than a data file.

    Example:
        Quick start::

            from sportsdataverse.mbb import torvik_game_stats
            df = torvik_game_stats(year=2025)

        Pipeline next step::

            df.filter(pl.col("team") == "Houston").select("game_date", "opp", "result").head()

        See Also:
            * `hoopR`_ - R sister package (``torvik_game_stats``)
            * `Bart Torvik`_ - data origin (T-Rank)

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _Bart Torvik: https://barttorvik.com
    """
    rows: Any = payload
    if isinstance(payload, str):
        _reject_html(payload, "game-stats JSON file")
        try:
            rows = json.loads(payload)
        except ValueError:
            rows = []
    rows = [r for r in rows if isinstance(r, list)] if isinstance(rows, list) else []
    df = _positional_frame(rows, GAME_STATS_COLS, _GAME_STATS_STR, _GAME_STATS_INT)
    df = df.with_columns(pl.col("date").str.to_date("%m/%d/%y", strict=False).alias("game_date"))
    return _out(df, return_as_pandas)


def parse_torvik_player_stats(payload: Any, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse ``getadvstats.php?year=&csv=1`` into one row per player.

    The payload is a headerless 67-column CSV, so the column names are
    positional (:data:`PLAYER_STATS_COLS`). ``player_id`` is ``Int64``; splits
    Torvik leaves blank are null.

    Args:
        payload: Raw CSV text.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame; zero rows (full schema) on empty input.

    Raises:
        ValueError: The payload is an HTML document rather than a data file.

    Example:
        Quick start::

            from sportsdataverse.mbb import torvik_player_stats
            df = torvik_player_stats(year=2025)

        Pipeline next step::

            df.sort("bpm", descending=True).select("player_name", "team", "bpm").head()

        See Also:
            * `hoopR`_ - R sister package (``torvik_player_stats``)
            * `Bart Torvik`_ - data origin (T-Rank)

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _Bart Torvik: https://barttorvik.com
    """
    text = payload if isinstance(payload, str) else ""
    _reject_html(text, "player-stats CSV file")
    rows = [[v or None for v in r] for r in csv.reader(io.StringIO(text)) if r]
    return _out(_positional_frame(rows, PLAYER_STATS_COLS, _PLAYER_STATS_STR, _PLAYER_STATS_INT), return_as_pandas)


def parse_torvik_game_schedule(payload: Any, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse ``{year}_super_sked.json`` into one row per scheduled/played game.

    Public wrapper over the crosswalk's private
    :func:`~sportsdataverse._crosswalk_basketball_sources.parse_super_sked` (55
    positional text fields plus ``game_date`` and ``year``). The generated
    wrapper does not pass the request year to the parser, so ``year`` is
    inferred per row from ``game_date``: a date in July or later maps to the
    next calendar year (the season end-year), a date before July to the same
    year, and a row with a null ``game_date`` gets a null ``year``. It can
    therefore differ from the ``year`` you requested only for games Torvik
    dates outside the season. Rows with an unparseable date are kept (null
    ``game_date``); the crosswalk's ``bart_super_sked`` drops them.

    Args:
        payload: Raw JSON text or the already-decoded list of rows.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame; zero rows (full schema) on empty input.

    Raises:
        ValueError: The payload is an HTML document rather than a data file.

    Example:
        Quick start::

            from sportsdataverse.mbb import torvik_game_schedule
            df = torvik_game_schedule(year=2025)

        Pipeline next step::

            df.filter(pl.col("team1") == "Houston").select("game_date", "team2", "result").head()

        See Also:
            * `hoopR`_ - R sister package (``torvik_game_schedule``)
            * `Bart Torvik`_ - data origin (T-Rank)

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _Bart Torvik: https://barttorvik.com
    """
    from sportsdataverse._crosswalk_basketball_sources import parse_super_sked

    if isinstance(payload, str):
        _reject_html(payload, "super-schedule JSON file")
    return _out(parse_super_sked(payload, None), return_as_pandas)
