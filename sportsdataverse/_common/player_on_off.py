"""Player on/off and WOWY pairs (BB-D5): the shared core behind ``{lg}_player_on_off`` and ``{lg}_player_wowy``.

Both roll up the BB-D6 game impact rows (:mod:`sportsdataverse._common.game_impact`) over a season,
with its additive rule: a possession belongs to the lineup on the floor when it starts, points to
the lineup at the scoring action, free throws to the lineup at the foul. So every possession of a
team is on the floor for exactly one set of its players, and

* **on/off** (:func:`player_on_off`): a player's ``*_on`` totals are the stints he was on the floor
  for, his ``*_off`` totals the rest of his team's season. ``poss_on + poss_off`` is the team's
  possessions, and so are ``opp_poss``, ``pts``, ``opp_pts`` and ``minutes``. The off side covers
  the team's whole season, the games he missed included, and so the games before he joined or after
  he left: for a player traded or signed mid-season (``games`` well short of ``team_games``) the
  on-off difference also carries how the team changed over the season. He has one row per team.
  (stats.nba.com's on/off limits the off side to the games he was on the roster.)
* **WOWY** (:func:`player_wowy`): for two teammates A and B, the team's season split four ways:
  both on the floor (``_both``), A without B (``_a_only``), B without A (``_b_only``) and neither
  (``_neither``). The four add up to the team's totals. Every pair of the team's players is a row,
  the pairs that never shared the floor too (all ``_both`` totals 0).

A game counts only with both play-by-play and possessions
(:func:`~sportsdataverse._common.game_impact.games_with_possessions`). A stint with an unknown
player (a gap in the source lineup) still counts for the team, so it is off the floor for everyone.

Per side: ``minutes``, ``poss`` / ``opp_poss`` (offensive / defensive possessions), ``pts`` /
``opp_pts`` (points scored / allowed), ``plus_minus`` and ``off_rtg`` / ``def_rtg`` / ``net_rtg`` per
100 possessions (null with no possessions). On/off adds ``<m>_diff`` (on minus off) and the BB-D2
league context: ``<m>_rank`` / ``<m>_pct`` for the on and diff ratings, league-wide within the
season among players with at least ``min_poss`` possessions (offense plus defense) both on and off
the floor, and ``<m>_n`` for every rating. WOWY carries no ranks: a pair is not a cohort, and its
possession columns are the sample sizes.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.game_impact import _sides, games_with_possessions
from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks, attach_sample_sizes

__all__ = [
    "WOWY_SIDES",
    "player_on_off",
    "player_on_off_schema",
    "player_wowy",
    "player_wowy_schema",
]

_TOTALS = ("seconds", "poss", "opp_poss", "pts", "opp_pts")
_RATINGS = ("off_rtg", "def_rtg", "net_rtg")
_SIDE_COLS: dict[str, pl.DataType] = {
    "minutes": pl.Float64(),
    "poss": pl.Int64(),
    "opp_poss": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
    "plus_minus": pl.Int64(),
    **dict.fromkeys(_RATINGS, pl.Float64()),
}
#: The four WOWY splits of a team's season for a pair (A, B).
WOWY_SIDES: tuple[str, ...] = ("both", "a_only", "b_only", "neither")
#: Ranked on/off rating -> lower is better. The off-court ratings get ``_n`` but no rank: a high
#: off-court rating is not the player's doing.
_RANKED = {
    "off_rtg_on": False,
    "def_rtg_on": True,
    "net_rtg_on": False,
    "off_rtg_diff": False,
    "def_rtg_diff": True,
    "net_rtg_diff": False,
}
#: Rating -> its sample size, on one side.
_N = {"off_rtg": ("poss",), "def_rtg": ("opp_poss",), "net_rtg": ("poss", "opp_poss")}


def _n_expr(rating: str, side: str) -> pl.Expr:
    return pl.sum_horizontal([pl.col(f"{c}_{side}") for c in _N[rating]])


def player_on_off_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`player_on_off` for a league whose player ids are ``player_dtype``."""
    sides = {f"{c}_{s}": t for s in ("on", "off") for c, t in _SIDE_COLS.items()}
    return {
        "season": pl.Int64(),
        "team_id": pl.Int64(),
        "player_id": player_dtype,
        "games": pl.Int64(),
        "team_games": pl.Int64(),
        **sides,
        **{f"{r}_diff": pl.Float64() for r in _RATINGS},
        **{f"{m}_rank": pl.Float64() for m in _RANKED},
        **{f"{m}_pct": pl.Float64() for m in _RANKED},
        **{f"{r}_{s}_n": pl.Int64() for s in ("on", "off", "diff") for r in _RATINGS},
    }


def player_wowy_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`player_wowy` for a league whose player ids are ``player_dtype``."""
    return {
        "season": pl.Int64(),
        "team_id": pl.Int64(),
        "player_id_a": player_dtype,
        "player_id_b": player_dtype,
        **{f"{c}_{s}": t for s in WOWY_SIDES for c, t in _SIDE_COLS.items()},
    }


def _side_rates(frame: pl.DataFrame, side: str) -> pl.DataFrame:
    """Minutes, +/- and the three ratings from one side's summed totals (``<total>_<side>``)."""

    def c(name: str) -> pl.Expr:
        return pl.col(f"{name}_{side}")

    per100 = {"off_rtg": 100 * c("pts") / c("poss"), "def_rtg": 100 * c("opp_pts") / c("opp_poss")}
    return frame.with_columns(
        # float clock sums depend on the summation order; rounding keeps rebuilds identical
        (c("seconds") / 60).round(6).alias(f"minutes_{side}"),
        (c("pts") - c("opp_pts")).alias(f"plus_minus_{side}"),
        *[pl.when(e.is_finite()).then(e).alias(f"{m}_{side}") for m, e in per100.items()],
    ).with_columns((c("off_rtg") - c("def_rtg")).alias(f"net_rtg_{side}"))


def _season_totals(rows: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(team totals, per-player on totals, per-player-set totals) of each team-season."""
    sides = _sides(games_with_possessions(rows))
    sums = [pl.col(c).sum() for c in _TOTALS]
    team = sides.group_by(["season", "team_id"]).agg(*sums, pl.col("game_id").n_unique().alias("team_games"))
    on = (
        sides.explode("players")  # an empty set explodes to null (or nothing): it is nobody's stint
        .drop_nulls("players")
        .group_by(["season", "team_id", "players"])
        .agg(*sums, pl.col("game_id").n_unique().alias("games"))
        .rename({"players": "player_id"})
    )
    sets = sides.group_by(["season", "team_id", "players"]).agg(sums)
    return team, on, sets


def player_on_off(rows: pl.DataFrame, *, player_dtype: pl.DataType, min_poss: int) -> pl.DataFrame:
    """Per player and team-season: the team with him on the floor and without him.

    Args:
        rows: Game-impact rows (:func:`sportsdataverse._common.game_impact.impact_row_schema`).
        player_dtype: The league's player id dtype.
        min_poss: The rank gate: a player is ranked when ``poss + opp_poss`` is at least this both
            on and off the floor.

    Returns:
        One row per ``(season, team_id, player_id)`` in :func:`player_on_off_schema`: ``games`` he
        was on the floor in and ``team_games`` the team played, the ``_on`` and ``_off`` totals and ratings, ``<rating>_diff`` (on
        minus off), the ranks and the sample sizes. ``<total>_on + <total>_off`` is the team's
        season total. Sorted by season, team and minutes on (most first). Empty input returns that
        schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_impact import nba_impact_rows
            pbp, gl = L.load_nba_stats_pbp(2024), L.load_nba_stats_game_lineups(2024)
            poss, sched = L.load_nba_stats_possessions(2024), L.load_nba_stats_schedules(2024)
            player_on_off(nba_impact_rows(pbp, gl, poss, sched), player_dtype=pl.Int64(), min_poss=1000)
    """
    schema = player_on_off_schema(player_dtype)
    team, on, _ = _season_totals(rows)
    if on.height == 0:
        return pl.DataFrame(schema=schema)
    keys = ["season", "team_id"]
    assert all(on.schema[k] == team.schema[k] for k in keys), "key dtypes differ between players and teams"
    out = on.rename({c: f"{c}_on" for c in _TOTALS}).join(
        team.rename({c: f"{c}_team" for c in _TOTALS}), on=keys, how="left", validate="m:1"
    )
    out = out.with_columns((pl.col(f"{c}_team") - pl.col(f"{c}_on")).alias(f"{c}_off") for c in _TOTALS)
    out = _side_rates(_side_rates(out, "on"), "off").with_columns(
        (pl.col(f"{r}_on") - pl.col(f"{r}_off")).alias(f"{r}_diff") for r in _RATINGS
    )
    # ponytail: min_poss defaults to BB-D4's lineup gate (400 NBA, 200 elsewhere), reused on purpose so
    # one rank gate reads the same across the season tables; a larger one if on-off ranks prove too noisy.
    gate = pl.min_horizontal(pl.col("poss_on") + pl.col("opp_poss_on"), pl.col("poss_off") + pl.col("opp_poss_off"))
    n = {f"{r}_{s}": _n_expr(r, s) for s in ("on", "off") for r in _RATINGS}
    # a difference is as good as its thinner side
    n |= {f"{r}_diff": pl.min_horizontal(_n_expr(r, "on"), _n_expr(r, "off")) for r in _RATINGS}
    return (
        out.pipe(
            attach_ranks,
            list(_RANKED),
            min_cohort=MIN_COHORT_PLAYERS,
            by=("season",),
            lower_is_better=[m for m, low in _RANKED.items() if low],
            qualifier=gate >= min_poss,
        )
        .pipe(attach_sample_sizes, n)
        .select(list(schema))
        .cast(schema)
        .sort(["season", "team_id", "minutes_on", "player_id"], descending=[False, False, True, False])
    )


def player_wowy(rows: pl.DataFrame, *, player_dtype: pl.DataType) -> pl.DataFrame:
    """Per pair of teammates and team-season: the team with both, either one alone, and neither on the floor.

    Args:
        rows: Game-impact rows (:func:`sportsdataverse._common.game_impact.impact_row_schema`).
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, team_id, player_id_a, player_id_b)`` in :func:`player_wowy_schema`,
        ``player_id_a`` sorting before ``player_id_b``, for every pair of players who were on the
        floor for the team that season. For each split in :data:`WOWY_SIDES` the totals and
        ratings; the four splits of a total add up to the team's season total. Sorted by season,
        team and minutes together (most first). Empty input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_impact import nba_impact_rows
            pbp, gl = L.load_nba_stats_pbp(2024), L.load_nba_stats_game_lineups(2024)
            poss, sched = L.load_nba_stats_possessions(2024), L.load_nba_stats_schedules(2024)
            player_wowy(nba_impact_rows(pbp, gl, poss, sched), player_dtype=pl.Int64())
    """
    schema = player_wowy_schema(player_dtype)
    team, on, sets = _season_totals(rows)
    if on.height == 0:
        return pl.DataFrame(schema=schema)
    keys = ["season", "team_id"]
    # Minutes together: every pair inside each set of players, summed over the sets.
    members = sets.with_row_index("_set").explode("players").drop_nulls("players")
    together = (
        members.join(members.select("_set", pl.col("players").alias("player_id_b")), on="_set")
        .filter(pl.col("players") < pl.col("player_id_b"))
        .rename({"players": "player_id_a"})
        .group_by([*keys, "player_id_a", "player_id_b"])
        .agg(pl.col(c).sum().alias(f"{c}_both") for c in _TOTALS)
    )
    # ponytail: every pair of a team's players, ~n^2/2 rows per team-season (a few hundred).
    a = on.select(*keys, pl.col("player_id").alias("player_id_a"), *[pl.col(c).alias(f"{c}_a") for c in _TOTALS])
    b = on.select(*keys, pl.col("player_id").alias("player_id_b"), *[pl.col(c).alias(f"{c}_b") for c in _TOTALS])
    pairs = a.join(b, on=keys).filter(pl.col("player_id_a") < pl.col("player_id_b"))
    pairs = pairs.join(together, on=[*keys, "player_id_a", "player_id_b"], how="left", validate="1:1")
    pairs = pairs.join(
        team.drop("team_games").rename({c: f"{c}_team" for c in _TOTALS}), on=keys, how="left", validate="m:1"
    )
    pairs = pairs.with_columns(pl.col(f"{c}_both").fill_null(0) for c in _TOTALS).with_columns(
        *[(pl.col(f"{c}_a") - pl.col(f"{c}_both")).alias(f"{c}_a_only") for c in _TOTALS],
        *[(pl.col(f"{c}_b") - pl.col(f"{c}_both")).alias(f"{c}_b_only") for c in _TOTALS],
        *[
            (pl.col(f"{c}_team") - pl.col(f"{c}_a") - pl.col(f"{c}_b") + pl.col(f"{c}_both")).alias(f"{c}_neither")
            for c in _TOTALS
        ],
    )
    for side in WOWY_SIDES:
        pairs = _side_rates(pairs, side)
    return (
        pairs.select(list(schema))
        .cast(schema)
        .sort(
            ["season", "team_id", "minutes_both", "player_id_a", "player_id_b"],
            descending=[False, False, True, False, False],
        )
    )
