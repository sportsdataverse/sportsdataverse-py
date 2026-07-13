"""NCAA basketball individual-stats box scores (bigballR ``scrape_box`` port).

Faithful polars port of bigballR's box-score surface against
``https://stats.ncaa.org/contests/{game_id}/individual_stats``:

* :func:`parse_ncaa_bb_box` -- ``scrape_box`` (``bigballR/R/all_functions.R:3492-3601``)
  fused with the per-row cleaning half of ``get_box_scores``
  (``all_functions.R:3603-3678``): table walk, name normalization, tolerant
  header rename, DNP filter, numeric coercion, ``MM:SS`` -> decimal minutes,
  and the five shooting rates. The R pipeline splits these across two
  functions but every step up to the optional aggregation is per-game
  row-wise, so one pure core keeps the surface minimal.
* :func:`ncaa_mbb_box_scores` -- the ``get_box_scores`` multi-game driver
  (error-isolation per game + optional ``multi_games`` aggregation).

The core is league-agnostic: wbigballR's fork (``wbigballR/R/all_functions.R:
3288-3452``) diverges only by transport, site-era background rows, and a
strict (error-prone) header rename -- per the divergence spec's fix #8 the
tolerant rename DICT applied via intersection is adopted here, extended with
the women's-page ``PF`` header so both leagues emit the same ``fouls``
column. ``scrape_box_score.R`` (``read_tables``) is dead code upstream (it
returns before touching the network) and is intentionally not ported.

Divergences from R (documented):

* ``PF`` -> ``Fouls`` added to the rename map (wbigballR left ``PF`` as an
  uncleaned character column; here both leagues get numeric ``fouls``).
* ``multi_games=True``: R groups by the ``Pos`` column, which current markup
  no longer ships (R hard-errors). The port aggregates on the available keys
  ``(player, clean_name, team)``.
* No ``Sys.sleep(2)`` pacing / progress ``message()`` -- pacing and caching
  belong to the injected :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher`.
"""

from __future__ import annotations

import math
import re
from fractions import Fraction
from typing import TYPE_CHECKING, Iterable, Literal, Optional, Protocol, Union, overload

import polars as pl
from bs4 import Tag

from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
from sportsdataverse.mbb.mbb_ncaa_html import parse_html

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["ncaa_mbb_box_scores", "parse_ncaa_bb_box"]

# R: gsub("[^[:alnum:] ]", "", x) under a UTF-8 locale keeps unicode letters;
# \w (unicode) minus underscore is the Python equivalent.
_NON_ALNUM_RE = re.compile(r"[^\w ]|_")
_WS_RE = re.compile(r"\s+")
#: Suffix-strip chain shared with scrape_game / get_team_roster
#: (all_functions.R:3559-3565) -- the pbp/roster join key normalization.
_SUFFIX_PATTERN = r"(\.JR\.|\.SR\.|\.J\.R\.|\.JR\.|JR\.|SR\.|\.SR|\.JR|\.SR|\.III|\.II|\.IV)$"
_SUFFIX_RE = re.compile(_SUFFIX_PATTERN)
_SUFFIX_RE_I = re.compile(_SUFFIX_PATTERN, re.IGNORECASE)

#: Tolerant header rename (all_functions.R:3574-3581), applied via
#: intersection per divergence-spec fix #8. Handles both site header
#: generations (ORebs/DRebs vs OffReb/DefReb/Min). ``PF`` is a port
#: extension: the women's individual_stats page ships ``PF`` where the
#: men's ships ``Fouls``.
_HEADER_RENAME: "dict[str, str]" = {
    "3FG": "TPM",
    "3FGA": "TPA",
    "FT": "FTM",
    "ORebs": "ORB",
    "DRebs": "DRB",
    "TotReb": "TRB",
    "TechFouls": "Tech",
    "OffReb": "ORB",
    "DefReb": "DRB",
    "Min": "MP",
    "PF": "Fouls",  # ponytail: port extension, see module docstring
}

#: get_box_scores count columns (all_functions.R:3639-3641): '' -> 0,
#: strip '/', then as.numeric.
_COUNT_COLS = (
    "G", "FGM", "FGA", "TPM", "TPA", "FTM", "FTA", "PTS", "ORB", "DRB",
    "TRB", "AST", "TO", "STL", "BLK", "Fouls", "DQ", "Tech",
)  # fmt: skip

#: get_box_scores select order (all_functions.R:3650-3652), any_of semantics
#: (absent site columns silently drop).
_SELECT_ORDER = (
    "Game_ID", "Box_ID", "Player", "CleanName", "Team", "Pos", "MP", "G",
    "PTS", "ORB", "DRB", "TRB", "AST", "TO", "STL", "BLK", "FGA", "FGM",
    "FG.", "TPA", "TPM", "TP.", "FTA", "FTM", "FT.", "TS.", "eFG.",
    "Fouls", "DQ", "Tech",
)  # fmt: skip

#: R contract name -> sdv-py snake_case contract.
_TO_SNAKE: "dict[str, str]" = {
    "Game_ID": "game_id",
    "Box_ID": "box_id",
    "Player": "player",
    "CleanName": "clean_name",
    "Team": "team",
    "Pos": "pos",
    "MP": "mp",
    "G": "g",
    "PTS": "pts",
    "ORB": "orb",
    "DRB": "drb",
    "TRB": "trb",
    "AST": "ast",
    "TO": "to",
    "STL": "stl",
    "BLK": "blk",
    "FGA": "fga",
    "FGM": "fgm",
    "FG.": "fg_pct",
    "TPA": "tpa",
    "TPM": "tpm",
    "TP.": "tp_pct",
    "FTA": "fta",
    "FTM": "ftm",
    "FT.": "ft_pct",
    "TS.": "ts_pct",
    "eFG.": "efg_pct",
    "Fouls": "fouls",
    "DQ": "dq",
    "Tech": "tech",
}

_PCT_COLS = ("fg_pct", "tp_pct", "ft_pct", "ts_pct", "efg_pct")

#: Documented empty-frame schema (fixture-era markup: no Pos / G columns).
_EMPTY_SCHEMA: "dict[str, pl.DataType]" = {
    _TO_SNAKE[c]: (pl.Utf8 if c in ("Game_ID", "Box_ID", "Player", "CleanName", "Team") else pl.Float64)
    for c in _SELECT_ORDER
    if c not in ("Pos", "G")
}


class _SupportsFetchIndividualStats(Protocol):
    """Structural type for the injected fetcher (``NcaaFetcher`` satisfies it)."""

    def fetch_game_individual_stats(self, contest_id: object) -> str: ...  # noqa: D102


def _player_key(name: str) -> str:
    """bigballR ``Player`` normalization (all_functions.R:3556-3560).

    Strip non-alphanumerics, uppercase, whitespace -> ``.``, strip suffix,
    trim -- byte-matching scrape_game's pbp join key.
    """
    fmt = _NON_ALNUM_RE.sub("", name)
    fmt = _WS_RE.sub(".", fmt).upper()
    return _SUFFIX_RE.sub("", fmt).strip()


def _clean_name(name: str) -> str:
    """bigballR ``CleanName`` (all_functions.R:3562-3563): raw name, suffix regex ignore-case, trim."""
    return _SUFFIX_RE_I.sub("", name).strip()


def _r_round1(x: float) -> float:
    """R >= 4.0 ``round(x, 1)``: nearest tenth-*candidate double*, exact ties to even.

    R's post-4.0.0 algorithm picks whichever of ``floor(10x)/10`` /
    ``ceil(10x)/10`` (as representable doubles) is closer to ``x``, breaking
    an exact tie toward the even scaled digit. Neither Python ``round()``
    (half-even on the scaled binary value) nor polars ``.round()``
    (half-away-from-zero) reproduces it -- e.g. R rounds ``7.45`` (7:27) down
    to 7.4 but ``35.45`` (35:27) up to 35.5. Verified against an Rscript
    grid over every MM:SS in 0:00-59:59.
    """
    f = Fraction(x)
    lo_i = math.floor(f * 10)
    lo, hi = lo_i / 10, (lo_i + 1) / 10
    d_lo, d_hi = f - Fraction(lo), Fraction(hi) - f
    if d_lo < d_hi:
        return lo
    if d_hi < d_lo:
        return hi
    return lo if lo_i % 2 == 0 else hi


def _mp_minutes(mp: str) -> Optional[float]:
    """``MM:SS`` -> decimal minutes at 1 dp (all_functions.R:3642).

    R: ``round(as.numeric(gsub(":(.*)", "", MP)) + as.numeric(gsub("(.*):",
    "", MP))/60, 1)`` -- minutes = before the first colon, seconds = after
    the last; unparseable -> null (R ``NA``).
    """
    mp = mp.replace("/", "")
    head = re.sub(r":(.*)", "", mp)
    tail = re.sub(r"(.*):", "", mp)
    try:
        value = float(head) + float(tail) / 60
    except ValueError:
        return None  # R as.numeric -> NA
    return _r_round1(value)


def _table_rows(table: Tag) -> "list[list[str]]":
    """All ``tr`` rows of a table as trimmed cell-text lists (XML::readHTMLTable semantics)."""
    rows: "list[list[str]]" = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"], recursive=False)
        if cells:
            rows.append([c.get_text().strip() for c in cells])
    return rows


def _side_frame(table: Tag, team: str) -> pl.DataFrame:
    """One per-team player table -> string-stage frame (all_functions.R:3538-3554).

    Cut at the ``TEAM`` pseudo-row (fallback ``nrow - 2`` when zero/multiple
    matches), stamp ``Team``, drop the ``Avg`` column.
    """
    rows = _table_rows(table)
    header, data = rows[0], rows[1:]
    ncol = len(header)
    padded: "list[list[Optional[str]]]" = [[*row, *([None] * (ncol - len(row)))][:ncol] for row in data]
    name_idx = header.index("Name")
    team_rows = [i for i, row in enumerate(padded) if row[name_idx] == "TEAM"]
    end = team_rows[0] if len(team_rows) == 1 else len(padded) - 2
    padded = padded[:end]
    cols: "dict[str, list[Optional[str]]]" = {h: [row[i] for row in padded] for i, h in enumerate(header) if h != "Avg"}
    cols["Team"] = [team] * len(padded)
    return pl.DataFrame(cols, schema_overrides={c: pl.Utf8 for c in cols})


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_EMPTY_SCHEMA)


def parse_ncaa_bb_box(html: str, game_id: str) -> pl.DataFrame:
    """Parse one ``/contests/{id}/individual_stats`` page to a tidy box-score frame.

    Faithful port of bigballR ``scrape_box`` (``bigballR/R/all_functions.R:
    3492-3601``) plus the per-row cleaning of ``get_box_scores``
    (``:3636-3652``): background table rows 3/4 carry the away/home team
    names, tables 4/5 (1-based) the away/home player tables; home rows bind
    first. League-agnostic -- the same core parses men's and women's pages
    (wbigballR ``all_functions.R:3288-3376`` divergences are site-era, not
    league; see module docstring).

    Args:
        html: Raw individual_stats page HTML.
        game_id: NCAA contest id; stamped into ``game_id`` / ``box_id``
            (kept ``Utf8`` -- opaque id, never arithmetic).

    Returns:
        polars.DataFrame: One row per player who logged minutes, columns
        ``game_id, box_id, player, clean_name, team, mp, pts, orb, drb, trb,
        ast, to, stl, blk, fga, fgm, fg_pct, tpa, tpm, tp_pct, fta, ftm,
        ft_pct, ts_pct, efg_pct, fouls, dq, tech``. Zero-row frame with that
        schema when the page has no tables.

    Example:
        Parse a saved page::

            from sportsdataverse.mbb.mbb_ncaa_box_stats import parse_ncaa_bb_box
            df = parse_ncaa_bb_box(open("individual_stats_6470186.html").read(), "6470186")
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("team") == "Illinois").head()

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's college basketball (R)
    """
    soup = parse_html(html)
    tables = soup.find_all("table")
    if len(tables) < 5:
        return _empty_frame()

    background = _table_rows(tables[0])
    away_team = background[2][0]  # R background[3,1] (issue96, 10/17/2025)
    home_team = background[3][0]  # R background[4,1]

    home = _side_frame(tables[4], home_team)
    away = _side_frame(tables[3], away_team)
    box = pl.concat([home, away], how="diagonal")  # home rows FIRST (R bind_rows(home, away))

    names = box.get_column("Name").to_list()
    box = box.with_columns(
        pl.Series("CleanName", [_clean_name(n or "") for n in names], dtype=pl.Utf8),
        pl.Series("Player", [_player_key(n or "") for n in names], dtype=pl.Utf8),
        pl.lit(str(game_id)).alias("Game_ID"),
        pl.lit(str(game_id)).alias("Box_ID"),
    )
    box = box.rename({k: v for k, v in _HEADER_RENAME.items() if k in box.columns})
    box = box.filter(pl.col("Player") != "TEAM.TEAM")

    # --- get_box_scores per-row cleaning (all_functions.R:3636-3648) ---
    box = box.filter(pl.col("MP").is_not_null() & (pl.col("MP") != ""))
    box = box.with_columns(
        pl.when(pl.col(c) == "")
        .then(pl.lit("0"))
        .otherwise(pl.col(c))
        .str.replace_all("/", "", literal=True)
        .cast(pl.Float64, strict=False)
        .alias(c)
        for c in _COUNT_COLS
        if c in box.columns
    )
    box = box.with_columns(pl.col("MP").map_elements(_mp_minutes, return_dtype=pl.Float64).alias("MP"))
    box = box.with_columns(
        (pl.col("FGM") / pl.col("FGA")).alias("FG."),
        (pl.col("TPM") / pl.col("TPA")).alias("TP."),
        (pl.col("FTM") / pl.col("FTA")).alias("FT."),
        ((pl.col("PTS") / 2) / (pl.col("FGA") + 0.475 * pl.col("FTA"))).alias("TS."),
        ((pl.col("FGM") + 0.5 * pl.col("TPM")) / pl.col("FGA")).alias("eFG."),
    )
    # R: across(where(is.numeric), x[is.nan(x)] <- 0) -- NaN -> 0, Inf survives.
    float_cols = [c for c, dtype in box.schema.items() if dtype == pl.Float64]
    box = box.with_columns(pl.col(c).fill_nan(0.0) for c in float_cols)

    keep = [c for c in _SELECT_ORDER if c in box.columns]
    return box.select(keep).rename({k: v for k, v in _TO_SNAKE.items() if k in keep})


def _aggregate_multi_games(df: pl.DataFrame) -> pl.DataFrame:
    """``multi.games = TRUE`` aggregation (all_functions.R:3654-3676).

    R groups by ``(Player, CleanName, Team, Pos)`` -- broken on current
    markup, which no longer ships ``Pos`` -- so the port groups on the
    available keys, sums the counters, sets ``g`` = games, recomputes the
    rates from the sums, and zeroes NaN (Inf survives, like R).
    """
    keys = ["player", "clean_name", "team"]
    counters = [c for c, dtype in df.schema.items() if dtype == pl.Float64 and c not in _PCT_COLS]
    out = (
        df.group_by(keys, maintain_order=True)
        .agg(
            *(pl.col(c).sum() for c in counters),
            pl.len().cast(pl.Float64).alias("g"),
        )
        .with_columns(
            (pl.col("fgm") / pl.col("fga")).alias("fg_pct"),
            (pl.col("tpm") / pl.col("tpa")).alias("tp_pct"),
            (pl.col("ftm") / pl.col("fta")).alias("ft_pct"),
            ((pl.col("pts") / 2) / (pl.col("fga") + 0.475 * pl.col("fta"))).alias("ts_pct"),
            ((pl.col("fgm") + 0.5 * pl.col("tpm")) / pl.col("fga")).alias("efg_pct"),
        )
    )
    out = out.with_columns(pl.col(c).fill_nan(0.0) for c, dtype in out.schema.items() if dtype == pl.Float64)
    order = [
        "player", "clean_name", "team", "mp", "g", "pts", "orb", "drb", "trb",
        "ast", "to", "stl", "blk", "fga", "fgm", "fg_pct", "tpa", "tpm",
        "tp_pct", "fta", "ftm", "ft_pct", "ts_pct", "efg_pct", "fouls", "dq", "tech",
    ]  # fmt: skip
    # dplyr::summarise orders by the grouping keys; mirror deterministically.
    return out.select(c for c in order if c in out.columns).sort(keys)


def _scrape_many(game_ids: "list[str]", fetcher: _SupportsFetchIndividualStats) -> "list[pl.DataFrame]":
    """Per-game loop with R's error isolation (all_functions.R:3614-3634)."""
    frames: "list[pl.DataFrame]" = []
    removed: "list[str]" = []
    for gid in game_ids:
        try:
            frames.append(parse_ncaa_bb_box(fetcher.fetch_game_individual_stats(gid), gid))
        except Exception as exc:  # noqa: BLE001 -- R tryCatch: report and continue
            print(f"Error with game id: {gid} // {exc}")
            removed.append(gid)
    if removed:
        print(f"{','.join(removed)} removed")
    return frames


@overload
def ncaa_mbb_box_scores(
    game_ids: Union[str, int, Iterable[Union[str, int]]],
    *,
    multi_games: bool = ...,
    fetcher: Optional[_SupportsFetchIndividualStats] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_box_scores(
    game_ids: Union[str, int, Iterable[Union[str, int]]],
    *,
    multi_games: bool = ...,
    fetcher: Optional[_SupportsFetchIndividualStats] = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...


@overload
def ncaa_mbb_box_scores(
    game_ids: Union[str, int, Iterable[Union[str, int]]],
    *,
    multi_games: bool = False,
    fetcher: Optional[_SupportsFetchIndividualStats] = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]": ...
def ncaa_mbb_box_scores(
    game_ids: Union[str, int, Iterable[Union[str, int]]],
    *,
    multi_games: bool = False,
    fetcher: Optional[_SupportsFetchIndividualStats] = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Box scores for one or more NCAA games (bigballR ``get_box_scores`` port).

    Multi-game driver over :func:`parse_ncaa_bb_box`
    (``bigballR/R/all_functions.R:3603-3678``): drops null ids, isolates
    per-game errors (failed ids are reported and skipped), binds rows, and
    optionally aggregates across games.

    Args:
        game_ids: One id or an iterable of NCAA contest ids.
        multi_games: When ``True``, aggregate one row per
            ``(player, clean_name, team)`` -- counters summed, ``g`` = games
            played, rates recomputed from the sums (R's ``multi.games``;
            grouping adapted per module docstring).
        fetcher: Optional injected fetcher exposing
            ``fetch_game_individual_stats`` (for offline replay/tests).
            Defaults to a fresh ``NcaaFetcher.with_browser()`` context per
            call -- stats.ncaa.org sits behind an Akamai challenge that the
            plain transport cannot clear.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas with ``return_as_pandas=True``): per-game
        rows in the :func:`parse_ncaa_bb_box` contract, or the aggregated
        ``multi_games`` contract.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_box_stats import ncaa_mbb_box_scores
            df = ncaa_mbb_box_scores(["6470186", "6479639"])
            print(df.shape)

        Season aggregate for a scraped id list::

            agg = ncaa_mbb_box_scores(ids, multi_games=True)

        Offline with an injected fetcher::

            df = ncaa_mbb_box_scores("6470186", fetcher=my_fetcher)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's college basketball (R)
    """
    if isinstance(game_ids, (str, int)):
        game_ids = [game_ids]
    ids = [str(g) for g in game_ids if g is not None]

    if fetcher is None:
        with NcaaFetcher.with_browser() as browser_fetcher:
            frames = _scrape_many(ids, browser_fetcher)
    else:
        frames = _scrape_many(ids, fetcher)

    df = pl.concat(frames, how="diagonal_relaxed") if frames else _empty_frame()
    if multi_games:
        df = _aggregate_multi_games(df)
    if return_as_pandas:
        return df.to_pandas()
    return df
