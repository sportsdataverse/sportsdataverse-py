"""bigballR lineup-aggregation transforms — lineups, filters, combos, on/off.

Faithful polars ports of bigballR's lineup family (all PURE transforms of the
35-column snake_case play-by-play contract / the lineups frame they emit —
no network):

* ``ncaa_mbb_lineups`` — ``get_lineups`` (``bigballR/R/all_functions.R:1945-2521``).
* ``ncaa_mbb_player_lineups`` — ``get_player_lineups`` (``all_functions.R:2761-2792``).
* ``ncaa_mbb_player_combos`` — ``get_player_combos`` + the unexported
  ``team_comb`` helper (``bigballR/R/get_player_combos.R:19-38`` + ``:42-191``).
* ``ncaa_mbb_on_off`` — ``on_off_generator`` (``all_functions.R:2555-2749``).

The transforms are league-agnostic (the WBB parity suite feeds the same
functions), named ``ncaa_mbb_*`` for the module's home package.

Deliberate deviations from the R output (documented, semantics preserved):

* **Possession key** — R builds a "globally unique" possession id via
  ``as.numeric(paste0(ID, Poss_Num))`` (``all_functions.R:1976``), which is
  collision-prone across games (``ID=123, Poss=45`` == ``ID=1234, Poss=5``).
  The port keys on the separator-joined composite ``game_id + "_" + poss_num``
  — collision-free — while reproducing R's distinct-count semantics. The
  fixture game ids never collide under R's scheme, so parity is unaffected.
* **``fix_tip_in`` flag** — ``get_lineups`` (and ``team_comb``) classify rim
  attempts with the literal ``"Tip-In"`` (``all_functions.R:2012``), but the
  scrape vocabulary emits ``"Tip In"`` (space) — tip-ins silently vanish from
  all rim stats in R. ``fix_tip_in=True`` (default) counts ``"Tip In"``
  correctly; pass ``False`` for faithful oracle equality.
* **No chr coercion** — R's row-wise ``apply()`` lineup sort coerces the whole
  frame to character (``all_functions.R:1955-1964``); only the C-locale byte
  sort semantics are reproduced, dtypes are kept.
* **``include_transition`` dead switch** — ``get_player_combos.R:28-30`` guards
  with ``colnames(...) %in% c("_trans","_half")``, an exact match that can
  never hit a real column name, so R silently forces ``include_transition=F``
  and the ``team_comb`` transition branch is unreachable. The port implements
  the INTENT (suffix match on ``_trans``/``_half`` column names) but defaults
  ``include_transition=False`` so parity with R holds.
* R's blanket ``x[is.na(x)] <- 0`` / ``Inf -> 0`` cleanup is reproduced
  exactly where R does it — a 0 in a ratio column may mean "no attempts",
  not "0%" (information R already discards).
"""

from __future__ import annotations

import itertools
import math
from typing import Literal, Optional, Sequence, Union, overload

import pandas as pd
import polars as pl

__all__ = [
    "LINEUPS_COLUMNS",
    "LINEUPS_RENAME",
    "LINEUPS_TRANSITION_COLUMNS",
    "ON_OFF_COLUMNS",
    "RIM_TYPES_FIXED",
    "RIM_TYPES_LITERAL",
    "STAT_COLUMNS",
    "ncaa_mbb_lineups",
    "ncaa_mbb_on_off",
    "ncaa_mbb_player_combos",
    "ncaa_mbb_player_lineups",
]

#: Rim-attempt event types with the scrape engine's real "Tip In" spelling.
RIM_TYPES_FIXED: tuple[str, ...] = ("Dunk", "Layup", "Hook", "Tip In")

#: Rim-attempt event types as literally written in R (``all_functions.R:2012``)
#: — ``"Tip-In"`` never matches the pbp vocabulary, so tip-ins are dropped.
RIM_TYPES_LITERAL: tuple[str, ...] = ("Dunk", "Layup", "Hook", "Tip-In")

_P_COLS: tuple[str, ...] = ("p1", "p2", "p3", "p4", "p5")
_KEY_COLS: tuple[str, ...] = (*_P_COLS, "team")
_HOME_COLS: tuple[str, ...] = tuple(f"home_{i}" for i in range(1, 6))
_AWAY_COLS: tuple[str, ...] = tuple(f"away_{i}" for i in range(1, 6))
_LINEUP_INPUT_COLS: tuple[str, ...] = _HOME_COLS + _AWAY_COLS

#: Counting stats in R summarise order (``all_functions.R:1979-2037``).
_COUNT: tuple[str, ...] = (
    "mins",
    "o_mins",
    "d_mins",
    "o_poss",
    "d_poss",
    "pts",
    "d_pts",
    "fga",
    "d_fga",
    "fgm",
    "d_fgm",
    "tpa",
    "d_tpa",
    "tpm",
    "d_tpm",
    "fta",
    "d_fta",
    "ftm",
    "d_ftm",
    "rima",
    "d_rima",
    "rimm",
    "d_rimm",
    "orb",
    "d_orb",
    "drb",
    "d_drb",
    "blk",
    "d_blk",
    "to",
    "d_to",
    "ast",
    "d_ast",
)

#: Derived ratios in R mutate order (``all_functions.R:2356-2406``).
_RATIO: tuple[str, ...] = (
    "ortg",
    "drtg",
    "netrtg",
    "fg_pct",
    "d_fg_pct",
    "tpp",
    "d_tpp",
    "ftp",
    "d_ftp",
    "efg_pct",
    "d_efg_pct",
    "ts_pct",
    "d_ts_pct",
    "rim_pct",
    "d_rim_pct",
    "mid_pct",
    "d_mid_pct",
    "tp_rate",
    "d_tp_rate",
    "rim_rate",
    "d_rim_rate",
    "mid_rate",
    "d_mid_rate",
    "ft_rate",
    "d_ft_rate",
    "ast_rate",
    "d_ast_rate",
    "to_rate",
    "d_to_rate",
    "blk_rate",
    "o_blk_rate",
    "orb_pct",
    "drb_pct",
    "time_per_poss",
    "d_time_per_poss",
)

#: The 69 stat columns of the base contract, in R's final ``select`` order
#: (``all_functions.R:2411`` — ``P1:Team, Mins:dPOSS, ORTG:NETRTG, everything()``).
STAT_COLUMNS: tuple[str, ...] = (
    *_COUNT[:5],
    *_RATIO[:3],
    *_COUNT[5:],
    "e_poss",
    *_RATIO[3:],
)

#: 75-column base lineups contract.
LINEUPS_COLUMNS: tuple[str, ...] = (*_KEY_COLS, *STAT_COLUMNS)

#: 213-column ``include_transition=True`` contract
#: (``all_functions.R:2512`` select order).
LINEUPS_TRANSITION_COLUMNS: tuple[str, ...] = (
    *LINEUPS_COLUMNS,
    "o_trans_pct",
    "d_trans_pct",
    *(f"{c}_trans" for c in _COUNT),
    *(f"{c}_half" for c in _COUNT),
    *(f"{c}_trans" for c in _RATIO),
    *(f"{c}_half" for c in _RATIO),
)

#: 70-column on/off contract (``all_functions.R:2663`` select order).
ON_OFF_COLUMNS: tuple[str, ...] = ("status", *STAT_COLUMNS)

_R_COUNT: tuple[str, ...] = (
    "Mins",
    "oMins",
    "dMins",
    "oPOSS",
    "dPOSS",
    "PTS",
    "dPTS",
    "FGA",
    "dFGA",
    "FGM",
    "dFGM",
    "TPA",
    "dTPA",
    "TPM",
    "dTPM",
    "FTA",
    "dFTA",
    "FTM",
    "dFTM",
    "RIMA",
    "dRIMA",
    "RIMM",
    "dRIMM",
    "ORB",
    "dORB",
    "DRB",
    "dDRB",
    "BLK",
    "dBLK",
    "TO",
    "dTO",
    "AST",
    "dAST",
)
_R_RATIO: tuple[str, ...] = (
    "ORTG",
    "DRTG",
    "NETRTG",
    "FG.",
    "dFG.",
    "TPP",
    "dTPP",
    "FTP",
    "dFTP",
    "eFG.",
    "deFG.",
    "TS.",
    "dTS.",
    "RIM.",
    "dRIM.",
    "MID.",
    "dMID.",
    "TPrate",
    "dTPrate",
    "RIMrate",
    "dRIMrate",
    "MIDrate",
    "dMIDrate",
    "FTrate",
    "dFTrate",
    "ASTrate",
    "dASTrate",
    "TOrate",
    "dTOrate",
    "BLKrate",
    "oBLKrate",
    "ORB.",
    "DRB.",
    "TimePerPoss",
    "dTimePerPoss",
)


def _build_rename() -> dict[str, str]:
    base = dict(zip(_R_COUNT + _R_RATIO, _COUNT + _RATIO))
    out: dict[str, str] = {
        **{f"P{i}": f"p{i}" for i in range(1, 6)},
        "Team": "team",
        "Status": "status",
        "ePOSS": "e_poss",
        "oTransPCT": "o_trans_pct",
        "dTransPCT": "d_trans_pct",
        **base,
    }
    for sfx in ("_trans", "_half"):
        out.update({f"{r}{sfx}": f"{s}{sfx}" for r, s in base.items()})
    return out


#: bigballR R output column names -> sdv-py snake_case (covers the base,
#: transition, player_combos and on_off contracts). Shared with the parity
#: tests so the mapping can never diverge.
LINEUPS_RENAME: dict[str, str] = _build_rename()


def _derived_exprs(sfx: str = "") -> list[pl.Expr]:
    """The get_lineups ratio block (``all_functions.R:2356-2406``), verbatim.

    ``netrtg`` is derived from the UNROUNDED ortg/drtg (dplyr mutate is
    sequential and rounding happens later). ``e_poss`` is deliberately not
    here — ``team_comb`` never recomputes it.
    """

    def c(name: str) -> pl.Expr:
        return pl.col(f"{name}{sfx}")

    ortg = c("pts") / c("o_poss") * 100
    drtg = c("d_pts") / c("d_poss") * 100
    mid_n = c("fgm") - c("rimm") - c("tpm")
    mid_d = c("fga") - c("rima") - c("tpa")
    d_mid_n = c("d_fgm") - c("d_rimm") - c("d_tpm")
    d_mid_d = c("d_fga") - c("d_rima") - c("d_tpa")
    return [
        ortg.alias(f"ortg{sfx}"),
        drtg.alias(f"drtg{sfx}"),
        (ortg - drtg).alias(f"netrtg{sfx}"),
        (c("fgm") / c("fga")).alias(f"fg_pct{sfx}"),
        (c("d_fgm") / c("d_fga")).alias(f"d_fg_pct{sfx}"),
        (c("tpm") / c("tpa")).alias(f"tpp{sfx}"),
        (c("d_tpm") / c("d_tpa")).alias(f"d_tpp{sfx}"),
        (c("ftm") / c("fta")).alias(f"ftp{sfx}"),
        (c("d_ftm") / c("d_fta")).alias(f"d_ftp{sfx}"),
        ((c("fgm") + 0.5 * c("tpm")) / c("fga")).alias(f"efg_pct{sfx}"),
        ((c("d_fgm") + 0.5 * c("d_tpm")) / c("d_fga")).alias(f"d_efg_pct{sfx}"),
        ((c("pts") / 2) / (c("fga") + 0.475 * c("fta"))).alias(f"ts_pct{sfx}"),
        ((c("d_pts") / 2) / (c("d_fga") + 0.475 * c("d_fta"))).alias(f"d_ts_pct{sfx}"),
        (c("rimm") / c("rima")).alias(f"rim_pct{sfx}"),
        (c("d_rimm") / c("d_rima")).alias(f"d_rim_pct{sfx}"),
        (mid_n / mid_d).alias(f"mid_pct{sfx}"),
        (d_mid_n / d_mid_d).alias(f"d_mid_pct{sfx}"),
        (c("tpa") / c("fga")).alias(f"tp_rate{sfx}"),
        (c("d_tpa") / c("d_fga")).alias(f"d_tp_rate{sfx}"),
        (c("rima") / c("fga")).alias(f"rim_rate{sfx}"),
        (c("d_rima") / c("d_fga")).alias(f"d_rim_rate{sfx}"),
        (mid_d / c("fga")).alias(f"mid_rate{sfx}"),
        (d_mid_d / c("d_fga")).alias(f"d_mid_rate{sfx}"),
        (c("fta") / c("fga")).alias(f"ft_rate{sfx}"),
        (c("d_fta") / c("d_fga")).alias(f"d_ft_rate{sfx}"),
        (c("ast") / c("fgm")).alias(f"ast_rate{sfx}"),
        (c("d_ast") / c("d_fgm")).alias(f"d_ast_rate{sfx}"),
        (c("to") / c("o_poss")).alias(f"to_rate{sfx}"),
        (c("d_to") / c("d_poss")).alias(f"d_to_rate{sfx}"),
        (c("blk") / c("d_fga")).alias(f"blk_rate{sfx}"),
        (c("d_blk") / c("fga")).alias(f"o_blk_rate{sfx}"),
        (c("orb") / (c("orb") + c("d_drb"))).alias(f"orb_pct{sfx}"),
        (c("drb") / (c("drb") + c("d_orb"))).alias(f"drb_pct{sfx}"),
        (c("o_mins") / c("o_poss") * 60).alias(f"time_per_poss{sfx}"),
        (c("d_mins") / c("d_poss") * 60).alias(f"d_time_per_poss{sfx}"),
    ]


def _fround(x: float, digits: int) -> float:
    """Faithful port of R >= 4.0.0 ``fround`` (``src/nmath/fround.c``).

    R rounds entirely in double precision, but NOT by scaling alone: the two
    candidates are the BACK-CONVERTED doubles ``xd = floor(x*10^d)/10^d`` and
    ``xu = ceil(x*10^d)/10^d``, the nearer one to ``x`` wins, and an exact
    distance tie goes to the even integer. This makes ``round(0.475, 2)``
    0.48 but ``round(22.755, 2)`` 22.75 — neither exact half-to-even nor any
    decimal-representation rule reproduces both. Verified against R 4.5.3 on
    a 1024-case fuzz (digits 0/2/3, +-2 ulp around ``.xx5`` boundaries).
    """
    if not math.isfinite(x) or x == 0.0:
        return x
    if digits == 0:
        # R: nearbyint(x) — round half to even on the double itself.
        fl, ce = math.floor(x), math.ceil(x)
        if x - fl < ce - x:
            return float(fl)
        if ce - x < x - fl:
            return float(ce)
        return float(fl if fl % 2 == 0 else ce)
    sgn = 1.0
    if x < 0.0:
        sgn, x = -1.0, -x
    # fround's early return: so many digits that no rounding is needed.
    if math.log10(2) * (0.5 + math.frexp(x)[1] - 1) + digits > 15:
        return sgn * x
    pow10 = 10.0**digits
    x10 = x * pow10
    i10 = math.floor(x10)
    xd = i10 / pow10
    xu = math.ceil(x10) / pow10
    du = xu - x
    dd = x - xd
    return sgn * (xu if (du < dd or (math.fmod(i10, 2.0) == 1.0 and du == dd)) else xd)


def _r_round(e: pl.Expr, digits: int) -> pl.Expr:
    """Elementwise R ``round(x, digits)`` (see ``_fround``); nulls skipped."""
    return e.map_elements(lambda v: _fround(v, digits), return_dtype=pl.Float64)


def _fsum_row(df: pl.DataFrame, num_cols: Sequence[str]) -> dict[str, list[float]]:
    """R ``summarise(across(where(is.numeric), sum))`` — base R ``sum()``
    accumulates in 80-bit long double, so the exactly-rounded ``math.fsum``
    is the faithful stand-in (a plain double left-fold is what R does NOT
    do). Empty selections sum to 0.0 (R ``sum(numeric(0))``)."""
    return {c: [math.fsum(df[c].to_list())] for c in num_cols}


def _eposs_expr() -> pl.Expr:
    """``ePOSS`` (``all_functions.R:2354``); R ``round()`` = half-to-even."""
    own = _r_round(pl.col("fga") + 0.475 * pl.col("fta") - pl.col("orb") + pl.col("to"), 0)
    opp = _r_round(pl.col("d_fga") + 0.475 * pl.col("d_fta") - pl.col("d_orb") + pl.col("d_to"), 0)
    return ((own + opp) / 2).alias("e_poss")


def _zero_bad(cols: Sequence[str]) -> list[pl.Expr]:
    """R's blanket ``x[is.na(x)] <- 0`` + ``Inf -> 0`` cleanup, per column."""
    return [
        pl.when(pl.col(c).is_null() | pl.col(c).is_nan() | pl.col(c).is_infinite())
        .then(0.0)
        .otherwise(pl.col(c))
        .alias(c)
        for c in cols
    ]


def _count_aggs(
    us: str,
    them: str,
    rim_types: Sequence[str],
    *,
    sfx: str = "",
    w: Optional[pl.Expr] = None,
    away_mins: bool = False,
) -> list[pl.Expr]:
    """One venue's counting summarise block (``all_functions.R:1979-2037``).

    ``w`` is the transition/half-court 0-1 weight column (``None`` = base
    pass). ``away_mins`` reproduces the away branch's ``sum(Event_Length/60)``
    (``all_functions.R:2187``) vs the home branch's ``sum(Event_Length)/60``
    — mathematically equal but not bit-identical in floating point.
    """

    def wm(e: pl.Expr) -> pl.Expr:
        return e if w is None else e * w

    us_e = (pl.col("event_team") == pl.col(us)).cast(pl.Int64)
    them_e = (pl.col("event_team") == pl.col(them)).cast(pl.Int64)
    made = (pl.col("event_result") == "made").cast(pl.Int64)
    fg = pl.col("shot_value").is_in([2, 3]).cast(pl.Int64)
    tp = (pl.col("shot_value") == 3).cast(pl.Int64)
    ft = (pl.col("shot_value") == 1).cast(pl.Int64)
    rim = pl.col("event_type").is_in(list(rim_types)).cast(pl.Int64)
    orb = (pl.col("event_type") == "Offensive Rebound").cast(pl.Int64)
    drb = (pl.col("event_type") == "Defensive Rebound").cast(pl.Int64)
    blk = (pl.col("event_type") == "Blocked Shot").cast(pl.Int64)
    tov = (pl.col("event_type") == "Turnover").cast(pl.Int64)
    ast = pl.col("player_2").is_not_null().cast(pl.Int64)
    on_o = pl.col("_okey").is_not_null().cast(pl.Int64)
    on_d = pl.col("_dkey").is_not_null().cast(pl.Int64)
    okey = pl.col("_okey") if w is None else pl.col("_okey").filter(w == 1)
    dkey = pl.col("_dkey") if w is None else pl.col("_dkey").filter(w == 1)
    mins = wm(pl.col("event_length") / 60).sum() if away_mins else wm(pl.col("event_length")).sum() / 60

    def two(name: str, e_us: pl.Expr, e_them: pl.Expr) -> list[pl.Expr]:
        return [
            wm(e_us).sum().alias(f"{name}{sfx}"),
            wm(e_them).sum().alias(f"d_{name}{sfx}"),
        ]

    shot = pl.col("shot_value")
    return [
        mins.alias(f"mins{sfx}"),
        (wm(pl.col("event_length") * on_o).sum() / 60).alias(f"o_mins{sfx}"),
        (wm(pl.col("event_length") * on_d).sum() / 60).alias(f"d_mins{sfx}"),
        okey.drop_nulls().n_unique().alias(f"o_poss{sfx}"),
        dkey.drop_nulls().n_unique().alias(f"d_poss{sfx}"),
        *two("pts", us_e * made * shot, them_e * made * shot),
        *two("fga", fg * us_e, fg * them_e),
        *two("fgm", fg * us_e * made, fg * them_e * made),
        *two("tpa", tp * us_e, tp * them_e),
        *two("tpm", tp * us_e * made, tp * them_e * made),
        *two("fta", ft * us_e, ft * them_e),
        *two("ftm", ft * us_e * made, ft * them_e * made),
        *two("rima", rim * us_e, rim * them_e),
        *two("rimm", made * rim * us_e, made * rim * them_e),
        *two("orb", orb * us_e, orb * them_e),
        *two("drb", drb * us_e, drb * them_e),
        *two("blk", blk * us_e, blk * them_e),
        *two("to", tov * us_e, tov * them_e),
        *two("ast", ast * us_e, ast * them_e),
    ]


def _venue_counts(
    df: pl.DataFrame,
    us: str,
    them: str,
    rim_types: Sequence[str],
    include_transition: bool,
) -> pl.DataFrame:
    """Aggregate one venue's lineups (home or away pass of ``get_lineups``)."""
    key = pl.concat_str([pl.col("game_id"), pl.col("poss_num").cast(pl.Utf8)], separator="_")
    work = df.with_columns(
        pl.when((pl.col("poss_team") == pl.col(us)) & pl.col("poss_num").is_not_null()).then(key).alias("_okey"),
        pl.when((pl.col("poss_team") == pl.col(them)) & pl.col("poss_num").is_not_null()).then(key).alias("_dkey"),
        pl.col("is_transition").cast(pl.Int64).alias("_tw"),
        (1 - pl.col("is_transition").cast(pl.Int64)).alias("_hw"),
    )
    aggs = _count_aggs(us, them, rim_types, away_mins=(us == "away"))
    if include_transition:
        aggs += _count_aggs(us, them, rim_types, sfx="_trans", w=pl.col("_tw"))
        aggs += _count_aggs(us, them, rim_types, sfx="_half", w=pl.col("_hw"))
    group = [f"{us}_{i}" for i in range(1, 6)] + [us]
    out = work.group_by(group).agg(aggs)
    out = out.rename({**{f"{us}_{i}": f"p{i}" for i in range(1, 6)}, us: "team"})
    cnt = [c for c in out.columns if c not in _KEY_COLS]
    return out.with_columns([pl.col(c).cast(pl.Float64) for c in cnt])


@overload
def ncaa_mbb_lineups(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = ...,
    fix_tip_in: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_lineups(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = ...,
    fix_tip_in: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def ncaa_mbb_lineups(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]: ...
def ncaa_mbb_lineups(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Aggregate bigballR-contract play-by-play into per-lineup stats.

    Port of bigballR ``get_lineups`` (``all_functions.R:1945-2521``). Rows
    with any missing on-court player and substitution rows are dropped, each
    row's home/away five are byte-sorted so a lineup always occupies the same
    columns, and the home + away passes are combined per ``(p1..p5, team)``.
    Ratios are derived from the summed counters, rounded to 3 decimals, and
    NA/Inf are zeroed exactly where R does it.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract (``parse_ncaa_bb_game_pbp`` output). May span multiple
            games.
        include_transition: When True, append the ``_trans``/``_half`` split
            surface plus ``o_trans_pct``/``d_trans_pct`` (213 columns total).
        fix_tip_in: When True (default), rim stats count the scrape engine's
            real ``"Tip In"`` vocabulary. When False, reproduce R's literal
            ``"Tip-In"`` test (``all_functions.R:2012``) — tip-ins silently
            excluded — for oracle parity.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or ``pd.DataFrame``) with one row per lineup+team —
        75 columns (``LINEUPS_COLUMNS``) or 213 with
        ``include_transition=True`` (``LINEUPS_TRANSITION_COLUMNS``), rows
        sorted by ``p1..p5, team``. Empty input yields an empty frame with
        the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_lineups
            lineups = ncaa_mbb_lineups(pbp)
            print(lineups.shape)

        Transition/half-court splits, pandas out::

            df_pd = ncaa_mbb_lineups(pbp, include_transition=True, return_as_pandas=True)

        Pipeline next step (one line)::

            lineups.filter(pl.col("mins") > 10).sort("netrtg", descending=True).head()

    See Also:
        * `bigballR`_ -- R source of the lineup engine.
        * `hoopR`_ -- men's college basketball data in R.

    .. _bigballR: https://github.com/jflancer/bigballR
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    rim_types = RIM_TYPES_FIXED if fix_tip_in else RIM_TYPES_LITERAL

    # all_functions.R:1947-1952 — drop rows with missing on-court players and
    # substitution rows. R's `!Event_Type %in% c(...)` keeps NA event types.
    df = pbp.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in _LINEUP_INPUT_COLS]))
    is_sub = pl.col("event_type").is_in(["Enters Game", "Leaves Game"]).fill_null(False)
    df = df.filter(is_sub == False)  # noqa: E712 — explicit bool mask

    # all_functions.R:1955-1964 — row-wise alphabetical (C-locale byte) sort of
    # each five; R chr-coerces the whole frame doing so, we keep dtypes.
    df = (
        df.with_columns(
            pl.concat_list([pl.col(c) for c in _HOME_COLS]).list.sort().alias("_hs"),
            pl.concat_list([pl.col(c) for c in _AWAY_COLS]).list.sort().alias("_as"),
        )
        .with_columns(
            [pl.col("_hs").list.get(i).alias(c) for i, c in enumerate(_HOME_COLS)]
            + [pl.col("_as").list.get(i).alias(c) for i, c in enumerate(_AWAY_COLS)]
        )
        .drop("_hs", "_as")
    )

    home = _venue_counts(df, "home", "away", rim_types, include_transition)
    away = _venue_counts(df, "away", "home", rim_types, include_transition)
    cnt_cols = [c for c in home.columns if c not in _KEY_COLS]

    # all_functions.R:2349-2351 — cross-venue combine; dplyr emits groups in
    # C-locale sorted key order, matched by polars' byte-wise sort.
    out = (
        pl.concat([home, away])
        .group_by(list(_KEY_COLS))
        .agg([pl.col(c).sum() for c in cnt_cols])
        .sort(list(_KEY_COLS), nulls_last=True)
    )

    derived = [_eposs_expr()] + _derived_exprs()
    round_cols = list(STAT_COLUMNS)
    if include_transition:
        derived += _derived_exprs("_trans") + _derived_exprs("_half")
        round_cols += [f"{c}{s}" for s in ("_trans", "_half") for c in (*_COUNT, *_RATIO)]

    # Derive from UNROUNDED counters, then round-3 everything, then NA/Inf->0
    # (all_functions.R:2352-2414 and, for the trans frame, :2419-2502).
    out = (
        out.with_columns(derived)
        .with_columns([_r_round(pl.col(c), 3).alias(c) for c in round_cols])
        .with_columns(_zero_bad(round_cols))
    )

    if include_transition:
        # all_functions.R:2504-2515 — oTransPCT/dTransPCT are computed after
        # the join and are NOT rounded; NA/Inf->0 still applies.
        out = out.with_columns(
            (pl.col("o_poss_trans") / pl.col("o_poss")).alias("o_trans_pct"),
            (pl.col("d_poss_trans") / pl.col("d_poss")).alias("d_trans_pct"),
        ).with_columns(_zero_bad(["o_trans_pct", "d_trans_pct"]))
        out = out.select(list(LINEUPS_TRANSITION_COLUMNS))
    else:
        out = out.select(list(LINEUPS_COLUMNS))

    if return_as_pandas:
        return out.to_pandas()
    return out


def _norm_players(players: Union[str, Sequence[str], None]) -> Optional[list[str]]:
    if players is None:
        return None
    if isinstance(players, str):
        return [players]
    return list(players)


def _filter_lineups(
    lineups: pl.DataFrame,
    included: Optional[list[str]],
    excluded: Optional[list[str]],
) -> pl.DataFrame:
    """Row-filter primitive of ``get_player_lineups`` (``all_functions.R:2766-2790``)."""
    if included is None and excluded is None:
        return lineups
    masks: list[pl.Expr] = []
    for p in included or []:
        masks.append(pl.any_horizontal([pl.col(c) == pl.lit(p) for c in _P_COLS]))
    for p in excluded or []:
        masks.append(
            pl.any_horizontal([pl.col(c) == pl.lit(p) for c in _P_COLS]) == False  # noqa: E712
        )
    return lineups.filter(pl.all_horizontal(masks))


@overload
def ncaa_mbb_player_lineups(
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = ...,
    excluded: Union[str, Sequence[str], None] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_player_lineups(
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = ...,
    excluded: Union[str, Sequence[str], None] = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def ncaa_mbb_player_lineups(
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]: ...
def ncaa_mbb_player_lineups(
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Filter a lineups frame by on-court player membership.

    Port of bigballR ``get_player_lineups`` (``all_functions.R:2761-2792``):
    keep rows where every ``included`` player is on the court (in ``p1..p5``)
    and no ``excluded`` player is. With both filters ``None`` the input is
    returned unchanged (R's ``Included = NA, Excluded = NA`` passthrough).
    Membership is tested by name against ``p1..p5`` (R tests the positional
    first five columns); row order is preserved.

    Args:
        lineups: Lineups frame from ``ncaa_mbb_lineups`` (any frame with
            ``p1..p5`` works).
        included: Player name(s) that must ALL be on the court.
        excluded: Player name(s) that must NONE be on the court.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        Row-subset of ``lineups``; schema unchanged.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_lineups
            on = ncaa_mbb_player_lineups(lineups, included="KEATON.WAGLER")
            print(on.shape)

        Included + excluded combination::

            df = ncaa_mbb_player_lineups(lineups, included=["A.PLAYER"], excluded=["B.PLAYER"])

        Pipeline next step (one line)::

            on.select(pl.col("mins").sum())

    See Also:
        * `bigballR`_ -- R source of the lineup filter.

    .. _bigballR: https://github.com/jflancer/bigballR
    """
    out = _filter_lineups(lineups, _norm_players(included), _norm_players(excluded))
    if return_as_pandas:
        return out.to_pandas()
    return out


def _drop_split_cols(lineups: pl.DataFrame) -> pl.DataFrame:
    """R ``select(-matches("_trans|_half"))`` — the snake contract's split
    columns all carry a ``_trans``/``_half`` SUFFIX, and suffix matching also
    reproduces R keeping ``oTransPCT``/``dTransPCT`` (no ``_trans`` in the R
    name, no suffix here)."""
    drop = [c for c in lineups.columns if c.endswith(("_trans", "_half"))]
    return lineups.drop(drop) if drop else lineups


def _team_comb(
    team_lineups: pl.DataFrame,
    *,
    min_mins: float,
    n: int,
    include_transition: bool,
    num_cols: list[str],
) -> pl.DataFrame:
    """Per-team combo enumeration + aggregation (``get_player_combos.R:42-191``)."""
    rows = team_lineups.select(list(_P_COLS)).rows()
    sets = [frozenset(r) for r in rows]
    players = sorted({p for r in rows for p in r})
    mins_vals: list[float] = team_lineups["mins"].to_list()

    kept: list[tuple[str, ...]] = []
    for combo in itertools.combinations(players, n):
        cs = set(combo)
        total = math.fsum(m for s, m in zip(sets, mins_vals) if cs <= s)
        # get_player_combos.R:50-55 — strictly greater than the threshold.
        if total > min_mins:
            kept.append(combo)

    p_out = [f"p{i}" for i in range(1, n + 1)]
    frames: list[pl.DataFrame] = []
    for combo in kept:
        cs = set(combo)
        mask = pl.Series([cs <= s for s in sets])
        summed = pl.DataFrame(_fsum_row(team_lineups.filter(mask), num_cols))
        # get_player_combos.R:60-100 — recompute the ratio block (ePOSS is
        # NOT recomputed; the summed value stands), then
        # `ifelse(is.infinite(x) | is.na(x), 0, round(x, 2))` — round-2 here,
        # not the round-3 of get_lineups / on_off_generator.
        summed = summed.with_columns(_derived_exprs())
        summed = summed.with_columns([_r_round(pl.col(c), 2).alias(c) for c in num_cols]).with_columns(
            _zero_bad(num_cols)
        )
        if include_transition:
            # get_player_combos.R:102-180 — unreachable in R (dead switch);
            # ported intent: re-derive the split ratios + trans share, then
            # round-3 everything (R has no extra NA/Inf cleanup here).
            summed = summed.with_columns(
                _derived_exprs("_trans")
                + _derived_exprs("_half")
                + [
                    (pl.col("o_poss_trans") / pl.col("o_poss")).alias("o_trans_pct"),
                    (pl.col("d_poss_trans") / pl.col("d_poss")).alias("d_trans_pct"),
                ]
            ).with_columns([_r_round(pl.col(c), 3).alias(c) for c in num_cols])
        summed = summed.with_columns([pl.lit(p).alias(name) for p, name in zip(combo, p_out)])
        frames.append(summed.select(p_out + num_cols))

    if not frames:
        schema = {**dict.fromkeys(p_out, pl.Utf8), **dict.fromkeys(num_cols, pl.Float64)}
        return pl.DataFrame(schema=schema)
    return pl.concat(frames)


@overload
def ncaa_mbb_player_combos(
    lineups: pl.DataFrame,
    *,
    n: int = ...,
    min_mins: float = ...,
    included: Union[str, Sequence[str], None] = ...,
    excluded: Union[str, Sequence[str], None] = ...,
    include_transition: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_player_combos(
    lineups: pl.DataFrame,
    *,
    n: int = ...,
    min_mins: float = ...,
    included: Union[str, Sequence[str], None] = ...,
    excluded: Union[str, Sequence[str], None] = ...,
    include_transition: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def ncaa_mbb_player_combos(
    lineups: pl.DataFrame,
    *,
    n: int = 2,
    min_mins: float = 0,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    include_transition: bool = False,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]: ...
def ncaa_mbb_player_combos(
    lineups: pl.DataFrame,
    *,
    n: int = 2,
    min_mins: float = 0,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    include_transition: bool = False,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Team stats for every n-player combination on the court together.

    Port of bigballR ``get_player_combos`` + ``team_comb``
    (``get_player_combos.R:19-38`` + ``:42-191``). Combos are enumerated per
    team over the byte-sorted unique player pool (lexicographic
    ``gtools::combinations`` order), filtered to combos whose lineups total
    strictly more than ``min_mins`` minutes, then each combo's lineup rows
    are summed and the ratio block re-derived. **Rounding is 2 decimals**
    here (R rounds 3 in ``get_lineups`` / ``on_off_generator``) and ``e_poss``
    is the SUM of the per-lineup estimates, not a recompute — both faithful.

    R's ``include_transition`` switch is dead code (an exact-match guard at
    ``get_player_combos.R:28-30`` always forces it back to ``FALSE``); the
    port implements the suffix-match intent but keeps the ``False`` default,
    which is the only R-reachable behavior.

    Args:
        lineups: Lineups frame from ``ncaa_mbb_lineups``.
        n: Combination size, 1-5.
        min_mins: Keep combos with total on-court minutes strictly greater
            than this (summed over the rounded per-lineup ``mins``).
        included: Player name(s) that must be on the court in every lineup
            considered.
        excluded: Player name(s) that must be off the court in every lineup
            considered.
        include_transition: Re-derive the ``_trans``/``_half`` ratio surface
            (requires a transition lineups frame; forced ``False`` otherwise).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or ``pd.DataFrame``) with one row per combo:
        ``team, p1..pn`` + the stat surface of the input frame. Teams appear
        in byte-sorted order.

    Raises:
        ValueError: If ``n`` is not an integer from 1 to 5.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_combos
            duos = ncaa_mbb_player_combos(lineups, n=2, min_mins=5)
            print(duos.shape)

        Anchored on one player::

            trios = ncaa_mbb_player_combos(lineups, n=3, included="KEATON.WAGLER")

        Pipeline next step (one line)::

            duos.sort("netrtg", descending=True).head()

    See Also:
        * `bigballR`_ -- R source of the combo engine.

    .. _bigballR: https://github.com/jflancer/bigballR
    """
    if n not in (1, 2, 3, 4, 5):
        raise ValueError("n must be an integer from 1 to 5")
    df = lineups if include_transition else _drop_split_cols(lineups)
    if include_transition and not any(c.endswith(("_trans", "_half")) for c in df.columns):
        include_transition = False
    base = _filter_lineups(df, _norm_players(included), _norm_players(excluded))
    num_cols = [c for c, dt in base.schema.items() if dt.is_numeric()]
    p_out = [f"p{i}" for i in range(1, n + 1)]

    parts: list[pl.DataFrame] = []
    for team in sorted(base["team"].unique().to_list()):
        sub = base.filter(pl.col("team") == team)
        part = _team_comb(
            sub,
            min_mins=min_mins,
            n=n,
            include_transition=include_transition,
            num_cols=num_cols,
        )
        if part.height:
            parts.append(part.with_columns(pl.lit(team).alias("team")).select(["team", *p_out, *num_cols]))

    if parts:
        out = pl.concat(parts)
    else:
        schema = {
            "team": pl.Utf8,
            **dict.fromkeys(p_out, pl.Utf8),
            **dict.fromkeys(num_cols, pl.Float64),
        }
        out = pl.DataFrame(schema=schema)
    if return_as_pandas:
        return out.to_pandas()
    return out


@overload
def ncaa_mbb_on_off(
    players: Union[str, Sequence[str]],
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = ...,
    excluded: Union[str, Sequence[str], None] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_on_off(
    players: Union[str, Sequence[str]],
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = ...,
    excluded: Union[str, Sequence[str], None] = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def ncaa_mbb_on_off(
    players: Union[str, Sequence[str]],
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]: ...
def ncaa_mbb_on_off(
    players: Union[str, Sequence[str]],
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Team stats for every on/off combination of the given players.

    Port of bigballR ``on_off_generator`` (``all_functions.R:2555-2749``,
    ``include_transition=F`` path). For k players, all ``2^k`` on/off
    assignments are enumerated in R's ``expand.grid`` order (first player
    varies fastest; first row all-On, last all-Off); each combination sums
    the lineups whose membership matches exactly, re-derives the full ratio
    block (including ``e_poss``), rounds to 3 decimals, and zeroes NA/Inf.
    Combinations matching zero lineups produce an all-zero row.

    Faithful R quirk (kept, flagged): when ``included``/``excluded`` is
    passed, the base lineup set comes from the membership filter ONLY — the
    inferred-team filter is skipped (``all_functions.R:2584-2589``), so an
    included player on another team would leak that team's lineups in.

    Args:
        players: Player name(s) to split on (the ``Status`` axis).
        lineups: Lineups frame from ``ncaa_mbb_lineups``.
        included: Optional membership filter forwarded to
            ``ncaa_mbb_player_lineups`` (replaces the team filter).
        excluded: Optional membership filter forwarded to
            ``ncaa_mbb_player_lineups`` (replaces the team filter).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or ``pd.DataFrame``) with ``2^k`` rows — ``status``
        (e.g. ``"A.PLAYER On | B.PLAYER Off"``) + the 69 stat columns
        (``ON_OFF_COLUMNS``).

    Raises:
        ValueError: If the players' team cannot be uniquely inferred from
            the lineups (R's ``"ERROR- Player team not found"``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_on_off
            split = ncaa_mbb_on_off("KEATON.WAGLER", lineups)
            print(split.shape)

        Two-player interaction::

            duo = ncaa_mbb_on_off(["A.PLAYER", "B.PLAYER"], lineups)

        Pipeline next step (one line)::

            split.select("status", "netrtg")

    See Also:
        * `bigballR`_ -- R source of the on/off generator.

    .. _bigballR: https://github.com/jflancer/bigballR
    """
    plist = _norm_players(players) or []
    if not plist:
        raise ValueError("players must name at least one player")
    df = _drop_split_cols(lineups)

    # all_functions.R:2568-2582 — infer the team from the players' lineups.
    on_any = pl.any_horizontal([pl.col(c).is_in(plist) for c in _P_COLS])
    teams = df.filter(on_any)["team"].unique().to_list()
    if len(teams) != 1:
        raise ValueError("Player team not found")

    inc = _norm_players(included)
    exc = _norm_players(excluded)
    if inc is not None or exc is not None:
        data = _filter_lineups(df, inc, exc)
    else:
        data = df.filter(pl.col("team") == teams[0])

    rows = data.select(list(_P_COLS)).rows()
    member = [[p in r for r in rows] for p in plist]
    num_cols = [c for c, dt in data.schema.items() if dt.is_numeric()]
    k = len(plist)

    frames: list[pl.DataFrame] = []
    for i in range(2**k):
        # expand.grid(rep(list(c(T, F)), k)) — first player varies fastest.
        combo = [((i >> j) & 1) == 0 for j in range(k)]
        mask = pl.Series([all(member[j][r] == combo[j] for j in range(k)) for r in range(len(rows))])
        summed = pl.DataFrame(_fsum_row(data.filter(mask), num_cols))
        status = " | ".join(f"{p} {'On' if on else 'Off'}" for p, on in zip(plist, combo))
        frames.append(summed.with_columns(pl.lit(status).alias("status")))

    out = pl.concat(frames)
    # all_functions.R:2623-2663 — recompute ePOSS + ratio block on the summed
    # counters, round-3, reorder, then NA/Inf->0.
    out = (
        out.with_columns([_eposs_expr()] + _derived_exprs())
        .with_columns([_r_round(pl.col(c), 3).alias(c) for c in num_cols])
        .with_columns(_zero_bad(num_cols))
    )
    head = ["mins", "o_mins", "d_mins", "o_poss", "d_poss", "ortg", "drtg", "netrtg", "pts"]
    order = ["status"] + head + [c for c in num_cols if c not in head]
    out = out.select(order)
    if return_as_pandas:
        return out.to_pandas()
    return out
