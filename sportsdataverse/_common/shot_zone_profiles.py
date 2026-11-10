"""Season shot profile by zone (BB-D35): rim / mid / corner 3 / above-the-break 3, for and against.

The league-agnostic core behind ``{nba,wnba}_player_shot_zones`` / ``_team_shot_zones`` and
``{mbb,wbb}_player_shot_zones`` / ``_team_shot_zones``. Each league adapter maps its shots onto
:data:`SHOT_ROWS_SCHEMA` (one row per field-goal attempt) with the zone already classified by that
league's existing classifier; this module only counts and ranks.

* **Zones** (:data:`ZONES`). ``rim`` is the restricted area (within 4 ft of the hoop), ``mid`` every
  other two, ``corner_3`` a three from the baseline band below the break, ``above_break_3`` every
  other three. A null ``zone`` is an attempt with no real location (the WNBA's 1997-2010 hoop-centre
  placeholders): it counts in ``fga`` and ``unlocated_fga``, never in a zone, so the four zone
  ``_fga`` plus ``unlocated_fga`` add up to ``fga``.
* **Rates** per zone ``z``: ``{z}_fg_pct`` (makes over attempts, ``_n`` the zone's attempts),
  ``{z}_freq`` (the zone's share of the located attempts, ``_n`` the located attempts) and
  ``{z}_ast_pct`` (the share of the zone's makes that were assisted, ``_n`` the makes whose assist
  is known: every make for the pros, the makes linked to a play-by-play row for college).
* **Ranks** follow BB-D2 (:mod:`sportsdataverse._common.league_context`): ``<m>_rank`` 1 = highest
  (an allowed field-goal percentage ranks lowest first: 1 is the stingiest defense), ``<m>_pct`` the
  Weibull percentile, within the season, among rows whose ``<m>_n`` reaches its gate in
  :data:`PLAYER_MIN_N` / :data:`TEAM_MIN_N` (the split-half r = 0.5 sample, fitted by
  ``tools/fit_shot_zone_gates.py``), and null under the cohort floor.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import math

import polars as pl

from sportsdataverse._common.league_context import (
    MIN_COHORT_PLAYERS,
    MIN_COHORT_TEAMS,
    attach_ranks,
    attach_sample_sizes,
)

__all__ = [
    "PLAYER_MIN_N",
    "RATES",
    "SHOT_ROWS_SCHEMA",
    "TEAM_MIN_N",
    "ZONES",
    "player_shot_zones",
    "shot_rows_schema",
    "team_shot_zones",
    "zone_columns",
]

ZONES: tuple[str, ...] = ("rim", "mid", "corner_3", "above_break_3")
#: The three rates of every zone, as ``{zone}_{rate}`` columns.
RATES: tuple[str, ...] = ("fg_pct", "freq", "ast_pct")

#: One row per field-goal attempt. ``team_id`` / ``opp_team_id`` / ``player_id`` keep the league's
#: own id dtype (the adapters fix it); ``zone`` is one of :data:`ZONES` or null (no location);
#: ``assisted`` is null on a miss and on a make whose assist is unknown.
#: The pros' dtypes; college ``player_id`` is the stats.ncaa.org id, ``pl.Utf8``
#: (:func:`shot_rows_schema`).
SHOT_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "opp_team_id": pl.Int64(),
    "player_id": pl.Int64(),
    "player_name": pl.Utf8(),
    "zone": pl.Utf8(),
    "made": pl.Boolean(),
    "assisted": pl.Boolean(),
}


def shot_rows_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """:data:`SHOT_ROWS_SCHEMA` with ``player_id`` in ``player_dtype``."""
    return {**SHOT_ROWS_SCHEMA, "player_id": player_dtype}


# Fitted by tools/fit_shot_zone_gates.py (train seasons of its SPANS; tests/fixtures/bb_shot_zone_gates
# holds the curves); refit and paste, never hand-tune. Keys are the ``{zone}_{rate}`` columns; a
# team's allowed rate is ``opp_{zone}_{rate}``. A rate missing from a league's table never came near
# half signal on its train seasons, or fell under a third signal on the later holdout seasons
# (``frozen_gates`` there), and is never ranked: a three-point make rate inside one season (every
# league but the WBB team above-the-break rate), and a few assisted shares and WNBA team rates.
PLAYER_MIN_N: dict[str, dict[str, float]] = {
    "nba": {
        "rim_fg_pct": 70,
        "rim_freq": 5,
        "rim_ast_pct": 8,
        "mid_fg_pct": 140,
        "mid_freq": 15,
        "mid_ast_pct": 5,
        "corner_3_freq": 13,
        "corner_3_ast_pct": 14,
        "above_break_3_freq": 6,
        "above_break_3_ast_pct": 5,
    },
    "wnba": {
        "rim_fg_pct": 86,
        "rim_freq": 8,
        "rim_ast_pct": 9,
        "mid_fg_pct": 200,
        "mid_freq": 10,
        "mid_ast_pct": 5,
        "corner_3_freq": 10,
        "above_break_3_freq": 4,
    },
    "mbb": {
        "rim_fg_pct": 67,
        "rim_freq": 7,
        "rim_ast_pct": 7,
        "mid_fg_pct": 120,
        "mid_freq": 13,
        "mid_ast_pct": 6,
        "corner_3_freq": 10,
        "above_break_3_freq": 5,
        "above_break_3_ast_pct": 6,
    },
    "wbb": {
        "rim_fg_pct": 75,
        "rim_freq": 9,
        "rim_ast_pct": 6,
        "mid_fg_pct": 110,
        "mid_freq": 9,
        "mid_ast_pct": 5,
        "corner_3_freq": 11,
        "above_break_3_freq": 4,
        "above_break_3_ast_pct": 9,
    },
}
TEAM_MIN_N: dict[str, dict[str, float]] = {
    "nba": {
        "rim_fg_pct": 320,
        "opp_rim_fg_pct": 470,
        "rim_freq": 370,
        "opp_rim_freq": 780,
        "rim_ast_pct": 110,
        "mid_fg_pct": 660,
        "opp_mid_fg_pct": 1100,
        "mid_freq": 200,
        "opp_mid_freq": 550,
        "mid_ast_pct": 99,
        "opp_mid_ast_pct": 570,
        "corner_3_freq": 630,
        "opp_corner_3_freq": 420,
        "corner_3_ast_pct": 250,
        "above_break_3_freq": 200,
        "opp_above_break_3_freq": 1300,
        "above_break_3_ast_pct": 50,
        "opp_above_break_3_ast_pct": 470,
    },
    "wnba": {
        "rim_fg_pct": 170,
        "opp_rim_fg_pct": 220,
        "rim_freq": 260,
        "opp_rim_freq": 360,
        "rim_ast_pct": 92,
        "mid_fg_pct": 470,
        "opp_mid_freq": 730,
        "opp_mid_ast_pct": 140,
        "corner_3_freq": 530,
        "opp_corner_3_freq": 1500,
        "above_break_3_freq": 110,
        "above_break_3_ast_pct": 36,
    },
    "mbb": {
        "rim_fg_pct": 290,
        "opp_rim_fg_pct": 240,
        "rim_freq": 320,
        "opp_rim_freq": 450,
        "rim_ast_pct": 68,
        "opp_rim_ast_pct": 190,
        "mid_fg_pct": 360,
        "opp_mid_fg_pct": 550,
        "mid_freq": 210,
        "opp_mid_freq": 380,
        "mid_ast_pct": 63,
        "opp_mid_ast_pct": 120,
        "corner_3_freq": 350,
        "opp_corner_3_freq": 350,
        "corner_3_ast_pct": 41,
        "above_break_3_freq": 170,
        "opp_above_break_3_freq": 460,
        "above_break_3_ast_pct": 53,
        "opp_above_break_3_ast_pct": 140,
    },
    "wbb": {
        "rim_fg_pct": 190,
        "opp_rim_fg_pct": 260,
        "rim_freq": 330,
        "opp_rim_freq": 370,
        "rim_ast_pct": 61,
        "opp_rim_ast_pct": 140,
        "mid_fg_pct": 230,
        "opp_mid_fg_pct": 500,
        "mid_freq": 150,
        "opp_mid_freq": 480,
        "mid_ast_pct": 68,
        "opp_mid_ast_pct": 150,
        "corner_3_freq": 280,
        "opp_corner_3_freq": 430,
        "above_break_3_fg_pct": 360,
        "above_break_3_freq": 120,
        "opp_above_break_3_freq": 600,
        "above_break_3_ast_pct": 54,
        "opp_above_break_3_ast_pct": 120,
    },
}


def zone_columns() -> list[str]:
    """The ``{zone}_{rate}`` rate columns, zone by zone."""
    return [f"{z}_{r}" for z in ZONES for r in RATES]


def _counts(prefix: str = "") -> list[pl.Expr]:
    """FGA / FGM / unlocated and, per zone, attempts, makes, assisted and assist-known makes."""
    made = pl.col("made") == True  # noqa: E712
    out = [
        pl.len().cast(pl.Int64).alias(f"{prefix}fga"),
        made.sum().cast(pl.Int64).alias(f"{prefix}fgm"),
        pl.col("zone").is_null().sum().cast(pl.Int64).alias(f"{prefix}unlocated_fga"),
    ]
    for z in ZONES:
        inz = pl.col("zone") == z
        out += [
            inz.sum().cast(pl.Int64).alias(f"{prefix}{z}_fga"),
            (inz & made).sum().cast(pl.Int64).alias(f"{prefix}{z}_fgm"),
            (inz & made & (pl.col("assisted") == True)).sum().cast(pl.Int64).alias(f"{prefix}{z}_ast"),  # noqa: E712
            (inz & made & pl.col("assisted").is_not_null()).sum().cast(pl.Int64).alias(f"_{prefix}{z}_known"),
        ]
    return out


def _ratio(num: str, den: str) -> pl.Expr:
    return pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).cast(pl.Float64)


def _rates(prefix: str = "") -> tuple[list[pl.Expr], dict[str, pl.Expr]]:
    """The rate columns and their ``_n`` denominators."""
    p = prefix
    located = pl.col(f"{p}fga") - pl.col(f"{p}unlocated_fga")
    exprs, dens = [], {}
    for z in ZONES:
        exprs += [
            _ratio(f"{p}{z}_fgm", f"{p}{z}_fga").alias(f"{p}{z}_fg_pct"),
            pl.when(located > 0).then(pl.col(f"{p}{z}_fga") / located).cast(pl.Float64).alias(f"{p}{z}_freq"),
            _ratio(f"{p}{z}_ast", f"_{p}{z}_known").alias(f"{p}{z}_ast_pct"),
        ]
        dens[f"{p}{z}_fg_pct"] = pl.col(f"{p}{z}_fga")
        dens[f"{p}{z}_freq"] = located
        dens[f"{p}{z}_ast_pct"] = pl.col(f"_{p}{z}_known")
    return exprs, dens


def _gates(table: dict[str, dict[str, float]], level: str, metrics: list[str]) -> dict[str, float]:
    """Each metric's gate; a metric with no fitted gate is never ranked (``inf``)."""
    if level not in table:
        raise ValueError(f"level must be one of {sorted(table)}, got {level!r}")
    return {m: table[level].get(m, math.inf) for m in metrics}


def player_shot_zones(rows: pl.DataFrame, *, level: str) -> pl.DataFrame:
    """One row per ``(season, team_id, player_id)``: the zone counts, rates, ``_n`` and ranks.

    Args:
        rows: Field-goal attempts in :data:`SHOT_ROWS_SCHEMA` (ids in the league's dtype). Rows
            with a null ``player_id`` or ``team_id`` are left out.
        level: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``: the gate table's key.

    Returns:
        ``season``, ``team_id``, ``player_id``, ``player_name``, ``games``, ``fga``, ``fgm``,
        ``unlocated_fga``, per zone ``{z}_fga`` / ``{z}_fgm`` / ``{z}_ast`` / ``{z}_fg_pct`` /
        ``{z}_freq`` / ``{z}_ast_pct``, then each rate's ``_n``, ``_rank`` and ``_pct``, sorted by
        season, team and player. Empty input returns those columns with zero rows.
    """
    metrics = zone_columns()
    keyed = rows.filter(pl.col("player_id").is_not_null() & pl.col("team_id").is_not_null())
    g = keyed.group_by("season", "team_id", "player_id").agg(
        pl.col("player_name").drop_nulls().max().alias("player_name"),
        pl.col("game_id").n_unique().cast(pl.Int64).alias("games"),
        *_counts(),
    )
    exprs, dens = _rates()
    g = attach_sample_sizes(g.with_columns(exprs), dens)
    g = attach_ranks(g, metrics, min_cohort=MIN_COHORT_PLAYERS, min_n=_gates(PLAYER_MIN_N, level, metrics))
    return g.drop(pl.selectors.starts_with("_")).sort("season", "team_id", "player_id")


def team_shot_zones(rows: pl.DataFrame, *, level: str) -> pl.DataFrame:
    """One row per ``(season, team_id)``: the team's zone profile and its opponents' (``opp_``).

    Args:
        rows: Field-goal attempts in :data:`SHOT_ROWS_SCHEMA`. A row counts for its ``team_id`` and
            against its ``opp_team_id``; a null side is skipped (a non-D1 shooter still counts
            against his D1 opponent).
        level: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.

    Returns:
        ``season``, ``team_id``, ``games``, then the shooting columns of
        :func:`player_shot_zones` for the team and with an ``opp_`` prefix for its opponents, each
        rate's ``_n``, ``_rank`` and ``_pct`` (the allowed ``opp_{z}_fg_pct`` ranks lowest first).
        Sorted by season and team; empty input returns those columns with zero rows.
    """
    own = (
        rows.filter(pl.col("team_id").is_not_null())
        .group_by("season", "team_id")
        .agg(pl.col("game_id").n_unique().cast(pl.Int64).alias("_games_for"), *_counts())
    )
    opp = (
        rows.filter(pl.col("opp_team_id").is_not_null())
        .group_by("season", pl.col("opp_team_id").alias("team_id"))
        .agg(pl.col("game_id").n_unique().cast(pl.Int64).alias("_games_against"), *_counts("opp_"))
    )
    g = own.join(opp, on=["season", "team_id"], how="full", coalesce=True)
    counts = [c for c in g.columns if c not in ("season", "team_id") and not c.startswith("_games")]
    g = g.with_columns(
        pl.max_horizontal("_games_for", "_games_against").fill_null(0).alias("games"),
        *[pl.col(c).fill_null(0) for c in counts],
    )
    own_rates, own_dens = _rates()
    opp_rates, opp_dens = _rates("opp_")
    g = attach_sample_sizes(g.with_columns(own_rates + opp_rates), {**own_dens, **opp_dens})
    metrics = zone_columns() + [f"opp_{m}" for m in zone_columns()]
    g = attach_ranks(
        g,
        metrics,
        min_cohort=MIN_COHORT_TEAMS,
        lower_is_better=[f"opp_{z}_fg_pct" for z in ZONES],
        min_n=_gates(TEAM_MIN_N, level, metrics),
    )
    lead = ["season", "team_id", "games"]
    return g.select(*lead, pl.exclude(lead)).drop(pl.selectors.starts_with("_")).sort("season", "team_id")
