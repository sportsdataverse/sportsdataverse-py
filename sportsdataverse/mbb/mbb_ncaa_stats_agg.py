"""bigballR stat-aggregation transforms — player stats, minutes, team stats.

Faithful polars ports of bigballR's stat-aggregation family (PURE transforms
of the 35-column snake_case play-by-play contract — no network):

* ``ncaa_mbb_player_stats`` — ``get_player_stats``
  (``bigballR/R/all_functions.R:2810-3177``), including the internal
  ``get_mins`` minutes/offensive-possession helper
  (``all_functions.R:3240-3263``).
* ``ncaa_mbb_team_stats`` — ``get_team_stats`` (``all_functions.R:2530-2538``):
  blanks the ten on-court columns, then delegates per game to ``get_lineups``
  — reused here via :func:`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_lineups`.

The transforms are league-agnostic (the WBB parity suite feeds the same
functions), named ``ncaa_mbb_*`` for the module's home package.

Faithful quirks / deliberate deviations (documented, semantics preserved):

* **Two lag scopes** in ``get_player_stats``: ``BLK_rim``/``BLK_mid``/
  ``BLK_three`` lag the WHOLE frame UNGROUPED inside a pre-group ``mutate``
  (``all_functions.R:2851-2853``) — in a multi-game frame the lag leaks
  across game boundaries — while ``PBACKA``/``PBACKM`` call ``lag()`` inside
  ``summarise`` and therefore lag WITHIN the ``(ID, ..., Player_1)`` group
  (``all_functions.R:2868-2873``). Both are ported as-is: ``.shift(1)``
  without / with ``.over(...)``.
* **``fix_tip_in`` flag** — rim stats test the literal ``"Tip-In"``
  (``all_functions.R:2827`` et al.) but the scrape vocabulary emits
  ``"Tip In"``, so tip-ins silently vanish from all RIM/PBACK stats in R.
  ``fix_tip_in=True`` (default) counts them; pass ``False`` for faithful
  oracle equality (same convention as :mod:`~sportsdataverse.mbb.mbb_ncaa_lineups`).
* **Rates are recomputed from summed counters** in the ``multi_games`` path
  — never averaged (``all_functions.R:3106-3153``). The per-game values the
  sums consume are the ROUNDED game-level numbers, exactly as in R; base R
  ``sum()`` accumulates in long double, stood in for by ``math.fsum``.
* R's ``x[is.na(x)] <- 0`` cleanup is reproduced exactly where R runs it
  (stat columns, after rounding). ``Inf`` is NOT zeroed there — R's
  ``is.na`` matches NA/NaN only (and no ratio here can produce Inf: every
  numerator is bounded by its denominator).
* Row order: dplyr ``summarise`` emits groups in C-locale sorted key order,
  matched by polars' byte-wise sort. ``ncaa_mbb_team_stats`` orders games by
  the Utf8 ``game_id`` byte sort where R sorts a numeric ID — identical for
  equal-width ids (all committed fixtures).
"""

from __future__ import annotations

import math
from typing import Literal, Sequence, Union, overload

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_ncaa_lineups import (
    LINEUPS_RENAME,
    LINEUPS_TRANSITION_COLUMNS,
    RIM_TYPES_FIXED,
    RIM_TYPES_LITERAL,
    STAT_COLUMNS,
    _r_round,
    ncaa_mbb_lineups,
)

__all__ = [
    "PLAYER_GAME_STATS_COLUMNS",
    "PLAYER_GAME_STATS_SIMPLE_COLUMNS",
    "PLAYER_STATS_COLUMNS",
    "PLAYER_STATS_SIMPLE_COLUMNS",
    "STATS_AGG_RENAME",
    "TEAM_STATS_COLUMNS",
    "TEAM_STATS_RENAME",
    "TEAM_STATS_TRANSITION_COLUMNS",
    "ncaa_mbb_player_stats",
    "ncaa_mbb_team_stats",
]

#: get_player_stats summarise group (``all_functions.R:2855`` / ``:2819``),
#: in the snake contract. Order doubles as the game-level sort order.
_G6: tuple[str, ...] = ("game_id", "game_date", "home", "away", "event_team", "player_1")
_GAME_KEYS: tuple[str, ...] = ("game_id", "game_date", "home", "away", "team", "player")
_LINEUP_COLS: tuple[str, ...] = tuple(f"{side}_{i}" for side in ("home", "away") for i in range(1, 6))

_SPECIAL_SNAKE = {"ID": "game_id", "Date": "game_date", "MINS": "mins", "oPOSS": "o_poss"}


def _snake(r: str) -> str:
    """bigballR R column name -> sdv-py snake (LINEUPS_RENAME conventions:
    the ``.`` percentage marker becomes ``_pct`` in place)."""
    return _SPECIAL_SNAKE.get(r, r.replace(".", "_pct").lower())


#: Core per-player block in R select order (``all_functions.R:3047-3048``).
_R_CORE: tuple[str, ...] = (
    "MINS",
    "oPOSS",
    "PTS",
    "ORB",
    "DRB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "TS.",
    "eFG.",
    "FGM",
    "FGA",
    "FG.",
    "TPM",
    "TPA",
    "TP.",
    "FTM",
    "FTA",
    "FT.",
    "RIMM",
    "RIMA",
    "RIM.",
    "MIDM",
    "MIDA",
    "MID.",
)
#: Transition / half-court split block order (``all_functions.R:3052-3054``).
_R_SPLIT: tuple[str, ...] = (
    "PTS",
    "ORB",
    "DRB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "TS.",
    "eFG.",
    "FGM",
    "FGA",
    "FG.",
    "TPM",
    "TPA",
    "TP.",
    "FTM",
    "FTA",
    "FT.",
    "RIMM",
    "RIMA",
    "RIM.",
    "MIDM",
    "MIDA",
    "MID.",
)
_R_PCT: tuple[str, ...] = (
    "pct_FGA_trans",
    "pct_TPA_trans",
    "pct_RIMA_trans",
    "pct_FGM_trans",
    "pct_TPM_trans",
    "pct_RIMM_trans",
    "pct_FGM_ast",
    "pct_TPM_ast",
    "pct_RIMM_ast",
)
_R_AST: tuple[str, ...] = ("PTS_ast", "FGM_ast", "TPM_ast", "RIMM_ast", "MIDM_ast")
_R_UNAST: tuple[str, ...] = (
    "PTS_unast",
    "eFG._unast",
    "FGM_unast",
    "FGA_unast",
    "FG._unast",
    "TPM_unast",
    "TPA_unast",
    "TP._unast",
    "RIMM_unast",
    "RIMA_unast",
    "RIM._unast",
    "MIDM_unast",
    "MIDA_unast",
    "MID._unast",
)
#: The 109 stat columns of the full contract (``all_functions.R:3046-3059``).
_R_FULL_STATS: tuple[str, ...] = (
    *_R_CORE,
    "PBACKM",
    "PBACKA",
    "PBACK.",
    "BLK_rim",
    "BLK_mid",
    "BLK_three",
    *_R_PCT,
    *(f"{c}_trans" for c in _R_SPLIT),
    *(f"{c}_half" for c in _R_SPLIT),
    *_R_AST,
    *_R_UNAST,
)
#: ``simple=True`` multi-game stat order (``all_functions.R:3097-3099``).
_R_SIMPLE_MULTI_STATS: tuple[str, ...] = (
    "MINS",
    "oPOSS",
    "PTS",
    "ORB",
    "DRB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "PCT_FGA_trans",
    "PCT_FGM_ast",
    "TS.",
    "eFG.",
    "FGM",
    "FGA",
    "FG.",
    "TPM",
    "TPA",
    "TP.",
    "FTM",
    "FTA",
    "FT.",
    "RIMM",
    "RIMA",
    "RIM.",
    "MIDM",
    "MIDA",
    "MID.",
)

_FULL_STATS: tuple[str, ...] = tuple(_snake(r) for r in _R_FULL_STATS)
_SIMPLE_GAME_STATS: tuple[str, ...] = (*(_snake(r) for r in _R_CORE), "fga_trans", "fgm_ast")
_SIMPLE_MULTI_STATS: tuple[str, ...] = tuple(_snake(r) for r in _R_SIMPLE_MULTI_STATS)

#: 113-column ``multi_games=True`` contract (oracle ``player_stats.csv``).
PLAYER_STATS_COLUMNS: tuple[str, ...] = ("player", "team", "gp", "gs", *_FULL_STATS)
#: 33-column ``multi_games=True, simple=True`` contract.
PLAYER_STATS_SIMPLE_COLUMNS: tuple[str, ...] = ("player", "team", "gp", "gs", *_SIMPLE_MULTI_STATS)
#: 115-column ``multi_games=False`` per-game contract (``all_functions.R:3046-3059``).
PLAYER_GAME_STATS_COLUMNS: tuple[str, ...] = (*_GAME_KEYS, *_FULL_STATS)
#: 35-column ``multi_games=False, simple=True`` contract (``all_functions.R:2990-2992``).
PLAYER_GAME_STATS_SIMPLE_COLUMNS: tuple[str, ...] = (*_GAME_KEYS, *_SIMPLE_GAME_STATS)

#: 73-column team-stats contract (oracle ``team_stats.csv``): the per-game
#: get_lineups output with P1:P5 dropped and the do() group keys prepended.
TEAM_STATS_COLUMNS: tuple[str, ...] = ("game_id", "home", "away", "team", *STAT_COLUMNS)
#: ``include_transition=True`` variant (base + splits, lineup keys dropped).
TEAM_STATS_TRANSITION_COLUMNS: tuple[str, ...] = (
    "game_id",
    "home",
    "away",
    "team",
    *LINEUPS_TRANSITION_COLUMNS[6:],
)


def _build_rename() -> dict[str, str]:
    out = {
        "Player": "player",
        "Team": "team",
        "GP": "gp",
        "GS": "gs",
        "ID": "game_id",
        "Date": "game_date",
        "Home": "home",
        "Away": "away",
        "PCT_FGA_trans": "pct_fga_trans",
        "PCT_FGM_ast": "pct_fgm_ast",
    }
    out.update({r: _snake(r) for r in _R_FULL_STATS})
    return out


#: bigballR get_player_stats R column names -> sdv-py snake_case (covers the
#: full, simple, per-game and multi-game contracts). Shared with the parity
#: tests so the mapping can never diverge.
STATS_AGG_RENAME: dict[str, str] = _build_rename()

#: get_team_stats R column names -> snake: the do() group keys plus the
#: get_lineups stat surface (reuses the lineups module's shared map).
TEAM_STATS_RENAME: dict[str, str] = {"ID": "game_id", "Home": "home", "Away": "away", **LINEUPS_RENAME}


def _sum_na(e: pl.Expr) -> pl.Expr:
    """R ``sum(x)`` WITHOUT ``na.rm`` — one missing value poisons the sum
    (polars ``sum`` always skips nulls, so the guard is explicit)."""
    return pl.when(e.is_null().any()).then(pl.lit(None, dtype=pl.Float64)).otherwise(e.sum().cast(pl.Float64))


def _zero_na(cols: Sequence[str]) -> list[pl.Expr]:
    """R ``x[is.na(x)] <- 0`` (``all_functions.R:2993/:3060/:3172``): NA and
    NaN zeroed; Inf deliberately untouched (R ``is.na(Inf)`` is FALSE)."""
    return [pl.when(pl.col(c).is_null() | pl.col(c).is_nan()).then(0.0).otherwise(pl.col(c)).alias(c) for c in cols]


def _rate_exprs(sfx: str = "", *, ft_ts: bool = True) -> list[pl.Expr]:
    """The shot-rate mutate block shared by the base (``all_functions.R:
    2996-3005``), ``_unast`` (``:3009-3015``, no FT/TS), ``_trans``/``_half``
    (``:3017-3034``) and multi-game (``:3106-3143``) passes. All rates derive
    from the (possibly summed) counters — never from averaged rates."""

    def c(name: str) -> pl.Expr:
        return pl.col(f"{name}{sfx}")

    out = [
        (c("fgm") / c("fga")).alias(f"fg_pct{sfx}"),
        (c("tpm") / c("tpa")).alias(f"tp_pct{sfx}"),
    ]
    if ft_ts:
        out += [
            (c("ftm") / c("fta")).alias(f"ft_pct{sfx}"),
            ((c("pts") / 2) / (c("fga") + 0.475 * c("fta"))).alias(f"ts_pct{sfx}"),
        ]
    out += [
        ((c("fgm") + 0.5 * c("tpm")) / c("fga")).alias(f"efg_pct{sfx}"),
        (c("rimm") / c("rima")).alias(f"rim_pct{sfx}"),
        (c("fga") - c("tpa") - c("rima")).alias(f"mida{sfx}"),
        (c("fgm") - c("tpm") - c("rimm")).alias(f"midm{sfx}"),
        ((c("fgm") - c("rimm") - c("tpm")) / (c("fga") - c("rima") - c("tpa"))).alias(f"mid_pct{sfx}"),
    ]
    return out


def _pct_exprs() -> list[pl.Expr]:
    """The pct_* shot-mix block (``all_functions.R:3035-3043`` == ``:3144-3152``).
    The attempt rates divide by FGA and the made rates by FGM — including the
    TP/RIM variants (sic, faithful to R)."""
    return [
        (pl.col("fga_trans") / pl.col("fga")).alias("pct_fga_trans"),
        (pl.col("tpa_trans") / pl.col("fga")).alias("pct_tpa_trans"),
        (pl.col("rima_trans") / pl.col("fga")).alias("pct_rima_trans"),
        (pl.col("fgm_trans") / pl.col("fgm")).alias("pct_fgm_trans"),
        (pl.col("tpm_trans") / pl.col("fgm")).alias("pct_tpm_trans"),
        (pl.col("rimm_trans") / pl.col("fgm")).alias("pct_rimm_trans"),
        (pl.col("fgm_ast") / pl.col("fgm")).alias("pct_fgm_ast"),
        (pl.col("tpm_ast") / pl.col("tpm")).alias("pct_tpm_ast"),
        (pl.col("rimm_ast") / pl.col("rimm")).alias("pct_rimm_ast"),
    ]


def _get_mins(pbp: pl.DataFrame) -> pl.DataFrame:
    """Port of ``get_mins`` (``all_functions.R:3240-3263``).

    Long-pivots the ten on-court columns and aggregates per (game, player):
    ``mins`` = summed event lengths / 60 (R ``sum`` without ``na.rm``) and
    ``o_poss`` = distinct possession numbers while the player's side has the
    ball (R ``n_distinct`` counts NA as a value; polars ``n_unique`` matches).
    """
    long = (
        pbp.select(
            "game_id",
            (pl.col("home") == pl.col("poss_team")).alias("_home_poss"),
            "event_length",
            "poss_num",
            *_LINEUP_COLS,
        )
        .unpivot(
            on=list(_LINEUP_COLS),
            index=["game_id", "_home_poss", "event_length", "poss_num"],
            variable_name="_slot",
            value_name="player",
        )
        .with_columns(
            # all_functions.R:3248-3252 case_when — a null home_poss fails
            # both guarded branches and falls through to FALSE.
            pl.when((pl.col("_home_poss") == True) & pl.col("_slot").str.starts_with("home"))  # noqa: E712
            .then(True)
            .when((pl.col("_home_poss") == False) & pl.col("_slot").str.starts_with("away"))  # noqa: E712
            .then(True)
            .otherwise(False)
            .alias("_offense")
        )
    )
    out = long.group_by(["game_id", "player"]).agg(
        (_sum_na(pl.col("event_length")) / 60).alias("mins"),
        pl.col("poss_num").filter(pl.col("_offense") == True).n_unique().cast(pl.Int64).alias("o_poss"),  # noqa: E712
    )
    # all_functions.R:3261 — mutate_if(is.numeric, round, 3).
    return out.with_columns(_r_round(pl.col("mins"), 3).alias("mins"))


def _count_aggs(rim_types: Sequence[str], simple: bool) -> list[pl.Expr]:
    """The get_player_stats summarise block (``all_functions.R:2820-2841``
    simple / ``:2856-2939`` full). ``na.rm = T`` sums map to polars' native
    null-skipping sums (a null product contributes 0 either way)."""
    made = (pl.col("event_result") == "made").cast(pl.Int64)
    # R `%in%` yields FALSE (not NA) for missing values — fill_null(False).
    fg = pl.col("shot_value").is_in([2, 3]).fill_null(False).cast(pl.Int64)
    tp = (pl.col("shot_value") == 3).cast(pl.Int64)
    ft = (pl.col("shot_value") == 1).cast(pl.Int64)
    rim = pl.col("event_type").is_in(list(rim_types)).fill_null(False).cast(pl.Int64)
    sv = pl.col("shot_value")
    t = pl.col("is_transition").cast(pl.Int64)
    h = 1 - t
    ast = pl.col("player_2").is_not_null().cast(pl.Int64)
    una = pl.col("player_2").is_null().cast(pl.Int64)

    def et(name: str) -> pl.Expr:
        return (pl.col("event_type") == name).cast(pl.Int64)

    base = [
        (made * sv).sum().alias("pts"),
        fg.sum().alias("fga"),
        (fg * made).sum().alias("fgm"),
        tp.sum().alias("tpa"),
        (tp * made).sum().alias("tpm"),
        rim.sum().alias("rima"),
        (rim * made).sum().alias("rimm"),
    ]
    tail = [
        ft.sum().alias("fta"),
        (ft * made).sum().alias("ftm"),
        et("Offensive Rebound").sum().alias("orb"),
        et("Defensive Rebound").sum().alias("drb"),
        et("Turnover").sum().alias("tov"),
        et("Steal").sum().alias("stl"),
        et("Blocked Shot").sum().alias("blk"),
        et("Commits Foul").sum().alias("pf"),
    ]
    if simple:
        return [
            *base,
            *tail,
            (fg * t).sum().alias("fga_trans"),
            (fg * made * ast).sum().alias("fgm_ast"),
        ]

    # PBACK lag scope: `lag(Event_Type)` INSIDE summarise operates on the
    # grouped rows, i.e. within the (game, ..., player) group in frame order
    # (all_functions.R:2868-2873) — precomputed as _lag_grp.
    pback = (pl.col("_lag_grp") == "Offensive Rebound").cast(pl.Int64)

    def split(w: pl.Expr, sfx: str) -> list[pl.Expr]:
        # all_functions.R:2900-2936 — the isTransition / (1-isTransition)
        # weighted re-run of the counting block.
        return [
            (made * sv * w).sum().alias(f"pts{sfx}"),
            (fg * w).sum().alias(f"fga{sfx}"),
            (fg * made * w).sum().alias(f"fgm{sfx}"),
            (ft * w).sum().alias(f"fta{sfx}"),
            (ft * made * w).sum().alias(f"ftm{sfx}"),
            (tp * w).sum().alias(f"tpa{sfx}"),
            (tp * made * w).sum().alias(f"tpm{sfx}"),
            (rim * w).sum().alias(f"rima{sfx}"),
            (rim * made * w).sum().alias(f"rimm{sfx}"),
            (et("Offensive Rebound") * w).sum().alias(f"orb{sfx}"),
            (et("Defensive Rebound") * w).sum().alias(f"drb{sfx}"),
            (et("Turnover") * w).sum().alias(f"tov{sfx}"),
            (et("Steal") * w).sum().alias(f"stl{sfx}"),
            (et("Blocked Shot") * w).sum().alias(f"blk{sfx}"),
        ]

    return [
        *base,
        (rim * pback).sum().alias("pbacka"),
        (rim * made * pback).sum().alias("pbackm"),
        *tail,
        # Assisted / unassisted (all_functions.R:2882-2899).
        (made * sv * ast).sum().alias("pts_ast"),
        (fg * made * ast).sum().alias("fgm_ast"),
        (tp * made * ast).sum().alias("tpm_ast"),
        (rim * made * ast).sum().alias("rimm_ast"),
        (made * sv * una).sum().alias("pts_unast"),
        (fg * una).sum().alias("fga_unast"),
        (fg * made * una).sum().alias("fgm_unast"),
        (tp * una).sum().alias("tpa_unast"),
        (tp * made * una).sum().alias("tpm_unast"),
        (rim * una).sum().alias("rima_unast"),
        (rim * made * una).sum().alias("rimm_unast"),
        *split(t, "_trans"),
        *split(h, "_half"),
        # BLK_rim/mid/three were mutated pre-group with the UNGROUPED lag
        # (all_functions.R:2851-2853) — here just summed per group.
        pl.col("_blk_rim").sum().alias("blk_rim"),
        pl.col("_blk_mid").sum().alias("blk_mid"),
        pl.col("_blk_three").sum().alias("blk_three"),
    ]


def _fsum_groups(df: pl.DataFrame, keys: Sequence[str], cols: Sequence[str]) -> pl.DataFrame:
    """R ``summarise_if(is.numeric, sum)`` per group with base R's long-double
    accumulation (``math.fsum``), plus ``GP = n()`` (``all_functions.R:
    3101-3105``)."""
    out = df.group_by(list(keys)).agg(
        pl.len().cast(pl.Int64).alias("gp"),
        *[pl.col(c) for c in cols],
    )
    return out.with_columns(
        [pl.col(c).map_elements(lambda s: math.fsum(s), return_dtype=pl.Float64).alias(c) for c in cols]
    )


def _starters(pbp: pl.DataFrame) -> pl.DataFrame:
    """GS: how often a player occupies one of the ten on-court slots of each
    game's FIRST row (``all_functions.R:3065-3073``)."""
    first = pbp.group_by("game_id", maintain_order=True).agg([pl.col(c).first() for c in _LINEUP_COLS])
    return (
        first.unpivot(on=list(_LINEUP_COLS), index="game_id", value_name="player")
        .drop_nulls("player")
        .group_by("player")
        .agg(pl.len().cast(pl.Int64).alias("gs"))
    )


@overload
def ncaa_mbb_player_stats(
    pbp: pl.DataFrame,
    *,
    multi_games: bool = ...,
    simple: bool = ...,
    fix_tip_in: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_player_stats(
    pbp: pl.DataFrame,
    *,
    multi_games: bool = ...,
    simple: bool = ...,
    fix_tip_in: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def ncaa_mbb_player_stats(
    pbp: pl.DataFrame,
    *,
    multi_games: bool = False,
    simple: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]: ...
def ncaa_mbb_player_stats(
    pbp: pl.DataFrame,
    *,
    multi_games: bool = False,
    simple: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Aggregate bigballR-contract play-by-play into per-player box stats.

    Port of bigballR ``get_player_stats`` (``all_functions.R:2810-3177``) +
    its ``get_mins`` helper (``:3240-3263``). Counting stats are summarised
    per (game, team, player), assists counted from ``player_2``, minutes and
    offensive possessions derived from the ten on-court columns, and rates
    (FG%, TS%, eFG%, rim/mid splits, ...) computed from the counters and
    rounded to 3 decimals with R's ``round`` semantics. With
    ``multi_games=True`` the per-game rows are summed per (player, team),
    every rate is recomputed from the summed counters (never averaged), and
    ``GP``/``GS`` are appended.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract. May span multiple games.
        multi_games: When True, aggregate across games per (player, team) —
            the season-stat surface. When False (default, R parity), treat
            each game separately and keep the game id columns.
        simple: When True, return the reduced 33-column (multi) / 35-column
            (per-game) surface without the transition / assisted / putback /
            block-location splits.
        fix_tip_in: When True (default), rim and putback stats count the
            scrape engine's real ``"Tip In"`` vocabulary. When False,
            reproduce R's literal ``"Tip-In"`` test (``all_functions.R:2827``)
            — tip-ins silently excluded — for oracle parity.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or ``pd.DataFrame``): one row per player+team
        (+game when ``multi_games=False``). Columns follow
        ``PLAYER_STATS_COLUMNS`` / ``PLAYER_STATS_SIMPLE_COLUMNS`` /
        ``PLAYER_GAME_STATS_COLUMNS`` / ``PLAYER_GAME_STATS_SIMPLE_COLUMNS``.
        Rows sorted by the group keys (byte order, matching dplyr's C-locale
        group order). Empty input yields an empty frame with the documented
        schema.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_player_stats
            season = ncaa_mbb_player_stats(pbp, multi_games=True)
            print(season.shape)

        Reduced surface, pandas out::

            df_pd = ncaa_mbb_player_stats(pbp, multi_games=True, simple=True, return_as_pandas=True)

        Pipeline next step (one line)::

            season.filter(pl.col("mins") > 50).sort("pts", descending=True).head()

    See Also:
        * `bigballR`_ -- R source of the stat-aggregation engine.
        * `hoopR`_ -- men's college basketball data in R.

    .. _bigballR: https://github.com/jflancer/bigballR
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    rim_types = RIM_TYPES_FIXED if fix_tip_in else RIM_TYPES_LITERAL

    work = pbp
    if not simple:
        lag_u = pl.col("event_type").shift(1)  # UNGROUPED lag (all_functions.R:2851-2853)
        blocked = (pl.col("event_type") == "Blocked Shot").cast(pl.Int64)
        work = work.with_columns(
            (blocked * lag_u.is_in(list(rim_types)).fill_null(False).cast(pl.Int64)).alias("_blk_rim"),
            (blocked * (lag_u == "Two Point Jumper").cast(pl.Int64)).alias("_blk_mid"),
            (blocked * (lag_u == "Three Point Jumper").cast(pl.Int64)).alias("_blk_three"),
            pl.col("event_type").shift(1).over(list(_G6)).alias("_lag_grp"),  # within-group lag (R:2868-2873)
        )

    stats = (
        work.group_by(list(_G6))
        .agg(_count_aggs(rim_types, simple))
        # all_functions.R:2844/:2942 — `Player_1 != "TEAM"` also drops NA.
        .filter(pl.col("player_1") != "TEAM")
        .rename({"event_team": "team", "player_1": "player"})
    )

    t = pl.col("is_transition").cast(pl.Int64)
    assist_aggs: list[pl.Expr] = [pl.len().cast(pl.Int64).alias("ast")]
    if not simple:
        # all_functions.R:2961-2962 — sums WITHOUT na.rm (NA poisons).
        assist_aggs += [_sum_na(t).alias("ast_trans"), _sum_na(1 - t).alias("ast_half")]
    assists = (
        pbp.group_by(["game_id", "player_2"])
        .agg(assist_aggs)
        .filter(pl.col("player_2").is_not_null())
        .rename({"player_2": "player"})
    )

    game = (
        stats.join(assists, on=["game_id", "player"], how="left")
        .join(_get_mins(pbp), on=["game_id", "player"], how="left")
        .with_columns(pl.col("ast").fill_null(0))  # all_functions.R:2975
    )

    if simple:
        game = game.with_columns(_rate_exprs())
        game_cols = PLAYER_GAME_STATS_SIMPLE_COLUMNS
        game_stats = _SIMPLE_GAME_STATS
    else:
        game = game.with_columns(
            *_rate_exprs(),
            (pl.col("pbackm") / pl.col("pbacka")).alias("pback_pct"),
            (pl.col("fgm_ast") - pl.col("tpm_ast") - pl.col("rimm_ast")).alias("midm_ast"),
            *_rate_exprs("_unast", ft_ts=False),
            *_rate_exprs("_trans"),
            *_rate_exprs("_half"),
            *_pct_exprs(),
        )
        game_cols = PLAYER_GAME_STATS_COLUMNS
        game_stats = _FULL_STATS

    # all_functions.R:2989/:3045 round-3 -> :2990/:3046 select -> :2993/:3060
    # NA->0 on the stat columns.
    game = (
        # Cast counters to Float64 first — _fround passes 0 through as-is, so
        # an Int64 input would leak an int into the Float64 map output.
        game.with_columns([_r_round(pl.col(c).cast(pl.Float64), 3).alias(c) for c in game_stats])
        .with_columns(_zero_na(game_stats))
        .select(list(game_cols))
        .sort(list(_GAME_KEYS), nulls_last=True)
    )

    if not multi_games:
        return game.to_pandas() if return_as_pandas else game

    # all_functions.R:3064-3173 — sum the (rounded) game rows per
    # (player, team), recompute every rate from the summed counters.
    multi = _fsum_groups(game, ["player", "team"], game_stats)
    if simple:
        multi = multi.with_columns(
            *_rate_exprs(),
            (pl.col("fga_trans") / pl.col("fga")).alias("pct_fga_trans"),
            (pl.col("fgm_ast") / pl.col("fgm")).alias("pct_fgm_ast"),
        )
        out_cols = PLAYER_STATS_SIMPLE_COLUMNS
        round_cols = [*game_stats, "pct_fga_trans", "pct_fgm_ast"]
    else:
        # NOTE: midm_ast is NOT recomputed in the multi pass (R:3106-3153) —
        # it stays the sum of the per-game values (integers, so equivalent).
        multi = multi.with_columns(
            *_rate_exprs(),
            (pl.col("pbackm") / pl.col("pbacka")).alias("pback_pct"),
            *_rate_exprs("_unast", ft_ts=False),
            *_rate_exprs("_trans"),
            *_rate_exprs("_half"),
            *_pct_exprs(),
        )
        out_cols = PLAYER_STATS_COLUMNS
        round_cols = list(game_stats)

    stat_cols = [c for c in out_cols if c not in ("player", "team", "gp", "gs")]
    multi = (
        multi.with_columns([_r_round(pl.col(c).cast(pl.Float64), 3).alias(c) for c in round_cols])
        .join(_starters(pbp), on="player", how="left")  # all_functions.R:3156
        .with_columns(pl.col("gs").fill_null(0))  # multi_game[is.na(...)] <- 0 (R:3172)
        .with_columns(_zero_na(stat_cols))
        .select(list(out_cols))
        .sort(["player", "team"], nulls_last=True)
    )
    return multi.to_pandas() if return_as_pandas else multi


@overload
def ncaa_mbb_team_stats(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = ...,
    fix_tip_in: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_team_stats(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = ...,
    fix_tip_in: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def ncaa_mbb_team_stats(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]: ...
def ncaa_mbb_team_stats(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Aggregate bigballR-contract play-by-play into per-team game stats.

    Port of bigballR ``get_team_stats`` (``all_functions.R:2530-2538``): the
    ten on-court columns are blanked so every row shares one "lineup", then
    ``get_lineups`` (:func:`ncaa_mbb_lineups`) runs per game and the lineup
    key columns are dropped — yielding two rows (one per team) per game.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract. May span multiple games.
        include_transition: When True, append the ``_trans``/``_half`` split
            surface plus ``o_trans_pct``/``d_trans_pct``.
        fix_tip_in: When True (default), rim stats count the scrape engine's
            real ``"Tip In"`` vocabulary; ``False`` reproduces R's literal
            ``"Tip-In"`` bug for oracle parity.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or ``pd.DataFrame``) with one row per team per game
        — ``TEAM_STATS_COLUMNS`` (73) or ``TEAM_STATS_TRANSITION_COLUMNS``
        with ``include_transition=True``. Games ordered by the Utf8
        ``game_id`` byte sort (R's do() sorts a numeric ID — identical for
        equal-width ids), teams within a game byte-sorted. Empty input
        yields an empty frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_team_stats
            teams = ncaa_mbb_team_stats(pbp)
            print(teams.shape)

        Transition splits, pandas out::

            df_pd = ncaa_mbb_team_stats(pbp, include_transition=True, return_as_pandas=True)

        Pipeline next step (one line)::

            teams.sort("netrtg", descending=True).head()

    See Also:
        * `bigballR`_ -- R source of the team-stats delegation.
        * `hoopR`_ -- men's college basketball data in R.

    .. _bigballR: https://github.com/jflancer/bigballR
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    cols = TEAM_STATS_TRANSITION_COLUMNS if include_transition else TEAM_STATS_COLUMNS
    # all_functions.R:2532-2533 — blank the lineup columns.
    work = pbp.with_columns([pl.lit("").alias(c) for c in _LINEUP_COLS])
    groups = work.partition_by(["game_id", "home", "away"], as_dict=True)
    parts: list[pl.DataFrame] = []
    for key in sorted(groups):  # dplyr do() emits groups in sorted key order
        gid, home, away = key
        lineups = ncaa_mbb_lineups(groups[key], include_transition=include_transition, fix_tip_in=fix_tip_in)
        parts.append(
            # all_functions.R:2537 — select(-P1:-P5); group keys prepended.
            lineups.drop(["p1", "p2", "p3", "p4", "p5"]).with_columns(
                pl.lit(gid).alias("game_id"),
                pl.lit(home).alias("home"),
                pl.lit(away).alias("away"),
            )
        )
    if parts:
        out = pl.concat(parts).select(list(cols))
    else:
        schema = {c: (pl.Utf8 if c in ("game_id", "home", "away", "team") else pl.Float64) for c in cols}
        out = pl.DataFrame(schema=schema)
    return out.to_pandas() if return_as_pandas else out
