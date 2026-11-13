"""Lineup seasons (BB-D4): the shared core behind ``{lg}_lineup_seasons``.

One row per ``(season, team_id, n_players, lineup_id)`` for the 2-, 3- and 5-man units of a team.
Everything rolls up the BB-D6 game impact box (:mod:`sportsdataverse._common.game_impact`):

* **Possessions, minutes and points** are :func:`~sportsdataverse._common.game_impact.game_lineups_used`
  summed over the season. A possession belongs to the lineup on the floor when it starts, points to
  the lineup on the floor at the scoring action, free throws to the lineup at the foul. So each
  possession belongs to exactly one five-man unit and the five-man rows of a team add up to the
  team's totals (stints with fewer than five known players aside).
* A **2- or 3-man unit** is every five-man unit that contains it, summed: a team's 2-man rows add
  up to ten times its five-man totals (each five has ten pairs), and so do its 3-man rows.
* **Four factors** come from per-row event counts (:func:`lineup_count_schema`) that each league
  adapter builds: from the possessions release for the NBA and WNBA (each count on the lineup that
  started the possession), from the play-by-play events for college (each count on the lineup at the
  event, a free throw on the lineup at the foul). For and against, as fractions: eFG%
  ``(FGM + 0.5 * 3PM) / FGA``, turnovers per possession, offensive rebounds over offensive-rebound
  chances (own OREB plus the opponent's DREB) and ``FTA / FGA``.
* **College shot zones** (``zones=True``): the rim, mid-range and three-point share of field-goal
  attempts and the make rate of each, for and against. The rim is a dunk, layup, hook or tip-in and
  the mid-range every other two, the classification of ``ncaa_{mbb,wbb}_lineups``.

**League context** (BB-D2, :mod:`sportsdataverse._common.league_context`): every rate gets
``<m>_rank`` (1 = best), the Weibull ``<m>_pct`` and ``<m>_n`` (the rate's own denominator). Ranks are
within the season and unit size, league-wide, among units with at least ``min_poss`` possessions
(offense plus defense) whose ``<m>_n`` also reaches the league's team gate in
:data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N` (the sample at which the rate is half signal;
most five-man units never reach the rating gates), and null below
:data:`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` qualifiers. The rim and mid-range
rates have no measured gate and rank on ``min_poss`` alone. The shot
zone shares (``rim_rate``, ``mid_rate``, ``fg3_rate`` and their ``opp_`` twins) describe a style,
not a quality: they rank high first on both sides.

**Bases** (BB-G8): ``pts_per75`` / ``opp_pts_per75`` / ``plus_minus_per75`` are 0.75 times
``off_rtg`` / ``def_rtg`` / ``net_rtg``, with the rating's ``_rank`` / ``_pct`` / ``_n`` copied, so a
board's basis select never converts. There is no per-game basis: a unit plays stretches of many
games, never a game of its own, so a per-game rate would only measure how often it was used.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

from itertools import combinations

import polars as pl

from sportsdataverse._common.game_impact import AWAY_SLOTS, HOME_SLOTS, game_lineups_used, games_with_possessions
from sportsdataverse._common.league_context import (
    LEAGUE_AVERAGES_SCHEMA,
    MIN_COHORT_PLAYERS,
    attach_per75,
    attach_ranks,
    attach_sample_sizes,
    league_averages,
)
from sportsdataverse._common.rate_gates import team_gates

__all__ = [
    "COUNTS",
    "SIZES",
    "ZONE_COUNTS",
    "lineup_count_schema",
    "lineup_seasons",
    "lineup_seasons_league_averages",
    "lineup_seasons_schema",
]

#: The unit sizes a season table carries.
SIZES: tuple[int, ...] = (2, 3, 5)
#: Four-factor event counts, one ``home_<c>`` and one ``away_<c>`` column each. ``dreb`` is a
#: side's own defensive rebounds.
COUNTS: tuple[str, ...] = ("fga", "fgm", "fg3a", "fg3m", "fta", "oreb", "dreb", "tov")
#: College shot-zone counts: rim and mid-range attempts and makes.
ZONE_COUNTS: tuple[str, ...] = ("rima", "rimm", "mida", "midm")
_SLOTS = HOME_SLOTS + AWAY_SLOTS
#: Per-75 scoring -> its per-100 rating (:func:`~sportsdataverse._common.league_context.attach_per75`).
_PER_75: dict[str, str] = {"pts_per75": "off_rtg", "opp_pts_per75": "def_rtg", "plus_minus_per75": "net_rtg"}
_TOTALS = ("seconds", "poss", "opp_poss", "pts", "opp_pts")


def lineup_count_schema(player_dtype: pl.DataType, *, zones: bool = False) -> dict[str, pl.DataType]:
    """The count-row frame :func:`lineup_seasons` reads: ten slots plus each side's event counts."""
    counts = COUNTS + (ZONE_COUNTS if zones else ())
    return {
        "season": pl.Int64(),
        "game_id": pl.Utf8(),
        "home_team_id": pl.Int64(),
        "away_team_id": pl.Int64(),
        **dict.fromkeys(_SLOTS, player_dtype),
        **{f"{side}_{c}": pl.Int64() for side in ("home", "away") for c in counts},
    }


#: Rated metric -> (sample-size expression, lower is better).
_RATED: dict[str, tuple[pl.Expr, bool]] = {
    "off_rtg": (pl.col("poss"), False),
    "def_rtg": (pl.col("opp_poss"), True),
    "net_rtg": (pl.col("poss") + pl.col("opp_poss"), False),
    "efg_pct": (pl.col("o_fga"), False),
    "tov_pct": (pl.col("poss"), True),
    "orb_pct": (pl.col("o_oreb") + pl.col("d_dreb"), False),
    "ft_rate": (pl.col("o_fga"), False),
    "opp_efg_pct": (pl.col("d_fga"), True),
    "opp_tov_pct": (pl.col("opp_poss"), False),
    "opp_orb_pct": (pl.col("d_oreb") + pl.col("o_dreb"), True),
    "opp_ft_rate": (pl.col("d_fga"), True),
}
_ZONE_RATED: dict[str, tuple[pl.Expr, bool]] = {
    "rim_rate": (pl.col("o_fga"), False),
    "mid_rate": (pl.col("o_fga"), False),
    "fg3_rate": (pl.col("o_fga"), False),
    "rim_pct": (pl.col("o_rima"), False),
    "mid_pct": (pl.col("o_mida"), False),
    "fg3_pct": (pl.col("o_fg3a"), False),
    "opp_rim_rate": (pl.col("d_fga"), False),
    "opp_mid_rate": (pl.col("d_fga"), False),
    "opp_fg3_rate": (pl.col("d_fga"), False),
    "opp_rim_pct": (pl.col("d_rima"), True),
    "opp_mid_pct": (pl.col("d_mida"), True),
    "opp_fg3_pct": (pl.col("d_fg3a"), True),
}


def _qualified(min_poss: int) -> pl.Expr:
    """The rank gate: offensive plus defensive possessions of at least ``min_poss``."""
    return pl.col("poss") + pl.col("opp_poss") >= min_poss


def _rated(zones: bool) -> dict[str, tuple[pl.Expr, bool]]:
    return {**_RATED, **(_ZONE_RATED if zones else {})}


def lineup_seasons_schema(player_dtype: pl.DataType, *, zones: bool = False) -> dict[str, pl.DataType]:
    """Columns of :func:`lineup_seasons` for a league whose player ids are ``player_dtype``."""
    rated = _rated(zones)
    return {
        "season": pl.Int64(),
        "team_id": pl.Int64(),
        "n_players": pl.Int64(),
        "lineup_id": pl.Utf8(),
        **{f"player_{i}": player_dtype for i in range(1, 6)},
        "minutes": pl.Float64(),
        "poss": pl.Int64(),
        "opp_poss": pl.Int64(),
        "pts": pl.Int64(),
        "opp_pts": pl.Int64(),
        "plus_minus": pl.Int64(),
        **{m: pl.Float64() for m in rated},
        **{f"{m}_rank": pl.Float64() for m in rated},
        **{f"{m}_pct": pl.Float64() for m in rated},
        **{f"{m}_n": pl.Int64() for m in rated},
        **{m: pl.Float64() for m in _PER_75},
        **{f"{m}_rank": pl.Float64() for m in _PER_75},
        **{f"{m}_pct": pl.Float64() for m in _PER_75},
        **{f"{m}_n": pl.Int64() for m in _PER_75},
    }


def _count_lineups(counts: pl.DataFrame, names: tuple[str, ...]) -> pl.DataFrame:
    """Season totals of each side's counts (``o_*``) and its opponent's (``d_*``) per five-man unit."""
    stints = counts.group_by(["season", "game_id", "home_team_id", "away_team_id", *_SLOTS]).agg(
        pl.col(f"{s}_{c}").sum() for s in ("home", "away") for c in names
    )
    sides = pl.concat(
        [
            stints.select(
                "season",
                pl.col(f"{us}_team_id").alias("team_id"),
                pl.concat_list(slots).list.drop_nulls().list.unique().list.sort().alias("players"),
                *[pl.col(f"{us}_{c}").alias(f"o_{c}") for c in names],
                *[pl.col(f"{them}_{c}").alias(f"d_{c}") for c in names],
            )
            for us, them, slots in (("home", "away", HOME_SLOTS), ("away", "home", AWAY_SLOTS))
        ]
    ).filter(pl.col("team_id").is_not_null() & (pl.col("players").list.len() == 5))
    sides = sides.with_columns(pl.col("players").cast(pl.List(pl.Utf8)).list.join("-").alias("lineup_id"))
    return sides.group_by(["season", "team_id", "lineup_id"]).agg(pl.exclude("players").sum())


def _units(fives: pl.DataFrame, value_cols: list[str]) -> pl.DataFrame:
    """Every 2-, 3- and 5-man subset of each five-man unit, summed over the fives that contain it."""
    parts = []
    for k in SIZES:
        for idx in combinations(range(5), k):  # the five are sorted, so each subset stays sorted
            players = [pl.col(f"player_{i + 1}") for i in idx]
            parts.append(
                fives.select(
                    "season",
                    "team_id",
                    pl.lit(k, dtype=pl.Int64).alias("n_players"),
                    pl.concat_list(players).alias("players"),
                    *value_cols,
                )
            )
    # ponytail: one row per (five, subset) before the group-by, ~21x the fives; a season of college
    # fives is a few hundred thousand rows, so this stays in memory.
    stacked = pl.concat(parts)
    return (
        stacked.with_columns(pl.col("players").cast(pl.List(pl.Utf8)).list.join("-").alias("lineup_id"))
        .group_by(["season", "team_id", "n_players", "lineup_id"])
        .agg(pl.col("players").first(), *[pl.col(c).sum() for c in value_cols])
        .with_columns(pl.col("players").list.get(i, null_on_oob=True).alias(f"player_{i + 1}") for i in range(5))
    )


def _rates(units: pl.DataFrame, zones: bool) -> pl.DataFrame:
    """The ratings, four factors and (``zones``) shot-zone rates, null where a denominator is 0."""
    efg = {s: (pl.col(f"{s}_fgm") + 0.5 * pl.col(f"{s}_fg3m")) / pl.col(f"{s}_fga") for s in ("o", "d")}
    exprs = {
        "off_rtg": 100 * pl.col("pts") / pl.col("poss"),
        "def_rtg": 100 * pl.col("opp_pts") / pl.col("opp_poss"),
        "efg_pct": efg["o"],
        "tov_pct": pl.col("o_tov") / pl.col("poss"),
        "orb_pct": pl.col("o_oreb") / (pl.col("o_oreb") + pl.col("d_dreb")),
        "ft_rate": pl.col("o_fta") / pl.col("o_fga"),
        "opp_efg_pct": efg["d"],
        "opp_tov_pct": pl.col("d_tov") / pl.col("opp_poss"),
        "opp_orb_pct": pl.col("d_oreb") / (pl.col("d_oreb") + pl.col("o_dreb")),
        "opp_ft_rate": pl.col("d_fta") / pl.col("d_fga"),
    }
    if zones:
        for s, pre in (("o", ""), ("d", "opp_")):
            exprs |= {
                f"{pre}rim_rate": pl.col(f"{s}_rima") / pl.col(f"{s}_fga"),
                f"{pre}mid_rate": pl.col(f"{s}_mida") / pl.col(f"{s}_fga"),
                f"{pre}fg3_rate": pl.col(f"{s}_fg3a") / pl.col(f"{s}_fga"),
                f"{pre}rim_pct": pl.col(f"{s}_rimm") / pl.col(f"{s}_rima"),
                f"{pre}mid_pct": pl.col(f"{s}_midm") / pl.col(f"{s}_mida"),
                f"{pre}fg3_pct": pl.col(f"{s}_fg3m") / pl.col(f"{s}_fg3a"),
            }
    finite = [pl.when(e.is_finite()).then(e).alias(m) for m, e in exprs.items()]  # 0/0 and x/0 -> null
    return (
        units.with_columns(finite)
        .with_columns((pl.col("off_rtg") - pl.col("def_rtg")).alias("net_rtg"))
        .with_columns(
            # float clock sums depend on the summation order; rounding keeps rebuilds identical
            (pl.col("seconds") / 60).round(6).alias("minutes"),
            (pl.col("pts") - pl.col("opp_pts")).alias("plus_minus"),
        )
    )


def lineup_seasons(
    rows: pl.DataFrame,
    counts: pl.DataFrame,
    *,
    player_dtype: pl.DataType,
    min_poss: int,
    zones: bool = False,
    level: str | None = None,
) -> pl.DataFrame:
    """Season 2-, 3- and 5-man units: possessions, minutes, ORtg/DRtg/net and the four factors.

    Args:
        rows: Game-impact rows (:func:`sportsdataverse._common.game_impact.impact_row_schema`).
        counts: Count rows in :func:`lineup_count_schema` (extra columns are ignored). Only the
            games whose ``rows`` carry possessions are used, in ``rows`` and ``counts`` alike.
        player_dtype: The league's player id dtype.
        min_poss: The rank gate: a unit is ranked when ``poss + opp_poss`` is at least this.
        zones: Add the college shot-zone rates (``counts`` must carry :data:`ZONE_COUNTS`).
        level: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``: rank a rate only when its ``<m>_n``
            also reaches that league's :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate.
            ``None`` applies no sample gate (the league adapters always pass one).

    Returns:
        One row per ``(season, team_id, n_players, lineup_id)`` in :func:`lineup_seasons_schema`.
        ``lineup_id`` is the unit's player ids, sorted and joined with ``-``; ``player_1..5`` are
        those ids in that order (null past ``n_players``). Sorted by season, unit size, team and
        possessions (most first). Empty input returns that schema with zero rows.

    Raises:
        KeyError: If ``counts`` is missing a schema column, or ``level`` is unknown.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_impact import nba_impact_rows
            from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
            pbp, gl = L.load_nba_stats_pbp(2024), L.load_nba_stats_game_lineups(2024)
            poss, sched = L.load_nba_stats_possessions(2024), L.load_nba_stats_schedules(2024)
            units = lineup_seasons(nba_impact_rows(pbp, gl, poss, sched), nba_lineup_count_rows(poss, sched),
                                   player_dtype=pl.Int64(), min_poss=400)
    """
    schema = lineup_seasons_schema(player_dtype, zones=zones)
    rated = _rated(zones)
    gates = team_gates(level, rated)
    count_schema = lineup_count_schema(player_dtype, zones=zones)
    missing = sorted(set(count_schema) - set(counts.columns))
    if missing:
        raise KeyError(f"counts missing columns: {missing}")
    # A game counts only with both play-by-play and possessions: without possessions its points
    # and minutes would land on units with no possessions, without play-by-play (the impact rows
    # then carry none of its possessions) its shots and turnovers would.
    rows = games_with_possessions(rows)
    played = rows.select("season", "game_id").unique()
    counts = counts.select(list(count_schema)).cast(count_schema).join(played, on=["season", "game_id"], how="semi")
    used = game_lineups_used(rows, player_dtype=player_dtype)
    if used.height == 0:
        return pl.DataFrame(schema=schema)
    names = COUNTS + (ZONE_COUNTS if zones else ())
    fives = used.group_by(["season", "team_id", "lineup_id"]).agg(
        *[pl.col(f"player_{i}").first() for i in range(1, 6)], *[pl.col(c).sum() for c in _TOTALS]
    )
    tallies = _count_lineups(counts, names)
    keys = ["season", "team_id", "lineup_id"]
    assert all(fives.schema[k] == tallies.schema[k] for k in keys), "key dtypes differ between units and counts"
    # A unit with no count rows (it never started a possession, say) has zero events, not unknown ones.
    count_cols = [f"{s}_{c}" for s in ("o", "d") for c in names]
    fives = fives.join(tallies, on=keys, how="left", validate="1:1").with_columns(pl.col(count_cols).fill_null(0))
    lower = [m for m, (_, low) in rated.items() if low]
    return (
        _rates(_units(fives, [*_TOTALS, *count_cols]), zones)
        .pipe(attach_sample_sizes, {m: e for m, (e, _) in rated.items()})
        .pipe(
            attach_ranks,
            list(rated),
            min_cohort=MIN_COHORT_PLAYERS,
            by=("season", "n_players"),
            lower_is_better=lower,
            qualifier=_qualified(min_poss),
            min_n=gates,
        )
        .pipe(attach_per75, _PER_75)
        .select(list(schema))
        .cast(schema)
        .sort(["season", "n_players", "team_id", "poss", "lineup_id"], descending=[False, False, False, True, False])
    )


def lineup_seasons_league_averages(
    table: pl.DataFrame, *, level: str, min_poss: int, zones: bool = False
) -> pl.DataFrame:
    """The league-average rows of a lineup-seasons table, one block per unit size.

    The football F1 rows of :func:`~sportsdataverse._common.league_context.league_averages` over
    the units the ranks use (``poss + opp_poss >= min_poss``, and each rate's ``level`` sample
    gate), so ``n`` is the cohort count. ``category`` is ``lineup_seasons_<n>man`` and
    ``qualifier_min`` is ``min_poss``.

    Args:
        table: Output of a ``*_lineup_seasons`` function.
        level: The population label (``"nba"``, ``"wnba"``, ``"mbb"``, ``"wbb"``), which also
            picks the sample gates of :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N`.
        min_poss: The same gate the table was ranked with.
        zones: Summarise the college shot-zone rates too.

    Returns:
        One row per ``(season, unit size, metric)`` in
        :data:`~sportsdataverse._common.league_context.LEAGUE_AVERAGES_SCHEMA`. An empty input
        returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_seasons
            lineup_seasons_league_averages(nba_lineup_seasons(2025), level="nba", min_poss=400)
    """
    gates = team_gates(level, _rated(zones))
    frames = [
        league_averages(
            table.filter(pl.col("n_players") == k),
            list(_rated(zones)),
            level=level,
            entity="lineup",
            category=f"lineup_seasons_{k}man",
            qualifier=_qualified(min_poss),
            qualifier_min=float(min_poss),
            min_n=gates,
        )
        for k in SIZES
    ]
    return pl.concat(frames) if frames else pl.DataFrame(schema=LEAGUE_AVERAGES_SCHEMA)
