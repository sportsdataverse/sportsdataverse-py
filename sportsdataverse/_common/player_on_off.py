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
  The ``*_off_span`` columns limit the off side to his span with the team, an approximation of
  his time on its roster (the window stats.nba.com's on/off uses) from the game dates: it opens
  at his first game for the team, or the day after his last game for a team he played for
  earlier that season (traded in), and closes the day before his first game for a later team
  (traded out), else at the season's end. The games he missed inside it count, so a player
  injured for the rest of the season keeps the whole season after his debut; a player signed
  mid-season starts at his debut. ``<rating>_diff_span`` is on minus that off side,
  ``team_games_span`` counts the span's team games and ``span_first_date`` /
  ``span_last_date`` are its first and last. A game with no date is left out of the span (the
  ``*_on`` totals keep it). A player with one team and a debut in its first game has a span of
  the whole season (``*_off_span`` equals ``*_off``).
* **WOWY** (:func:`player_wowy`): for two teammates A and B, the team's season split four ways:
  both on the floor (``_both``), A without B (``_a_only``), B without A (``_b_only``) and neither
  (``_neither``). The four splits' totals add up to the team's (the ratings are not additive). Every pair of the team's players is a row,
  the pairs that never shared the floor too (all ``_both`` totals 0).

A game counts only with both play-by-play and possessions
(:func:`~sportsdataverse._common.game_impact.games_with_possessions`). A stint with a gap in the
source lineup still counts for the team: its known players are on the floor for it, everyone else
off it (a stint with no known player is off the floor for everyone).

Per side: ``minutes``, ``poss`` / ``opp_poss`` (offensive / defensive possessions), ``pts`` /
``opp_pts`` (points scored / allowed), ``plus_minus`` and ``off_rtg`` / ``def_rtg`` / ``net_rtg`` per
100 possessions (null with no possessions). On/off adds ``<m>_diff`` (on minus off) and the BB-D2
league context: ``<m>_rank`` / ``<m>_pct`` for the on and diff ratings, league-wide within the
season among players with at least ``min_poss`` possessions (offense plus defense) both on and off
the floor whose ``<m>_n`` also reaches the league's gate in
:data:`~sportsdataverse._common.rate_gates.ON_OFF_MIN_N` (the sample at which the rating is half
signal: most rotation players clear the on-court gates, few players clear a difference gate in a
season and none in college), and ``<m>_n`` for every rating (a difference's is its thinner side's). WOWY carries no ranks: a pair is not a cohort, and its
possession columns are the sample sizes.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.game_impact import _sides, games_with_possessions
from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks, attach_sample_sizes
from sportsdataverse._common.rate_gates import on_off_gates

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
#: :func:`_span_off`'s columns besides the keys.
_SPAN_RAW: dict[str, pl.DataType] = {
    "team_games_span": pl.Int64(),
    "span_first_date": pl.Date(),
    "span_last_date": pl.Date(),
    **{f"{c}_off_span": pl.Float64() if c == "seconds" else pl.Int64() for c in _TOTALS},
}


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
        "team_games_span": pl.Int64(),
        "span_first_date": pl.Date(),
        "span_last_date": pl.Date(),
        **{f"{c}_off_span": t for c, t in _SIDE_COLS.items()},
        **{f"{r}_diff_span": pl.Float64() for r in _RATINGS},
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


def _span_off(rows: pl.DataFrame, game_dates: pl.DataFrame) -> pl.DataFrame:
    """Per player and team-season: the team's games in his span with it, minus his own stints.

    The span opens at his first game for the team, or, when he played for another team earlier
    that season, the day after his last game there (he was traded in); it closes the day before
    his first game for a later team (traded out), else at the team's last game. Returns
    ``season, team_id, player_id``, ``team_games_span``, ``span_first_date`` /
    ``span_last_date`` (the span's first and last team game) and ``<total>_off_span``.
    """
    keys = ["season", "team_id"]
    dates = game_dates.select(pl.col("game_id").cast(pl.Utf8), pl.col("game_date").cast(pl.Date))
    dates = dates.drop_nulls().unique("game_id")
    dated = _sides(games_with_possessions(rows)).join(dates, on="game_id", how="inner", validate="m:1")
    sums = [pl.col(c).sum() for c in _TOTALS]
    team_games = dated.group_by([*keys, "game_id", "game_date"]).agg(sums)
    player_games = (
        dated.explode("players")
        .drop_nulls("players")
        .group_by([*keys, "players", "game_date"])
        .agg(sums)
        .rename({"players": "player_id"})
    )
    stints = player_games.group_by([*keys, "player_id"]).agg(
        pl.col("game_date").min().alias("_first"),
        pl.col("game_date").max().alias("_last"),
        *[pl.col(c).sum().alias(f"{c}_in") for c in _TOTALS],
    )
    # His other teams that season bound the span: the last game of an earlier one, the first of a later one.
    other = stints.select("season", "player_id", pl.col("team_id").alias("_t"), "_first", "_last")
    pairs = (
        stints.select(*keys, "player_id", "_first", "_last")
        .join(other, on=["season", "player_id"], suffix="_o")
        .filter(pl.col("_t") != pl.col("team_id"))
    )
    bounds = pairs.group_by([*keys, "player_id"]).agg(
        pl.col("_last_o").filter(pl.col("_last_o") < pl.col("_first")).max().alias("_prev"),
        pl.col("_first_o").filter(pl.col("_first_o") > pl.col("_last")).min().alias("_next"),
    )
    window = stints.join(bounds, on=[*keys, "player_id"], how="left", validate="1:1").select(
        *keys,
        "player_id",
        pl.coalesce(pl.col("_prev") + pl.duration(days=1), pl.col("_first")).alias("_start"),
        (pl.col("_next") - pl.duration(days=1)).alias("_end"),
    )
    # ponytail: every player x every team game, then the window filter: ~20 x 82 rows a team-season.
    in_span = (
        window.join(team_games, on=keys)
        .filter(pl.col("game_date") >= pl.col("_start"), (pl.col("game_date") <= pl.col("_end")).fill_null(True))
        .group_by([*keys, "player_id"])
        .agg(
            *[pl.col(c).sum().alias(f"{c}_span") for c in _TOTALS],
            pl.len().cast(pl.Int64).alias("team_games_span"),
            pl.col("game_date").min().alias("span_first_date"),
            pl.col("game_date").max().alias("span_last_date"),
        )
    )
    out = stints.join(in_span, on=[*keys, "player_id"], how="left", validate="1:1")
    return out.select(
        *keys,
        "player_id",
        "team_games_span",
        "span_first_date",
        "span_last_date",
        *[(pl.col(f"{c}_span") - pl.col(f"{c}_in")).alias(f"{c}_off_span") for c in _TOTALS],
    )


def player_on_off(
    rows: pl.DataFrame,
    *,
    player_dtype: pl.DataType,
    min_poss: int,
    level: str | None = None,
    game_dates: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Per player and team-season: the team with him on the floor and without him.

    Args:
        rows: Game-impact rows (:func:`sportsdataverse._common.game_impact.impact_row_schema`).
        player_dtype: The league's player id dtype.
        min_poss: The rank gate: a player is ranked when ``poss + opp_poss`` is at least this both
            on and off the floor.
        level: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``: rank a rating only when its ``<m>_n``
            also reaches that league's :data:`~sportsdataverse._common.rate_gates.ON_OFF_MIN_N`
            gate. ``None`` applies no sample gate (the league adapters always pass one).
        game_dates: ``game_id`` (String) and ``game_date`` (Date) of the games, for the span
            columns. ``None`` leaves them null (the league adapters always pass it).

    Returns:
        One row per ``(season, team_id, player_id)`` in :func:`player_on_off_schema`: ``games`` he
        was on the floor in and ``team_games`` the team played, the ``_on`` and ``_off`` totals and ratings, ``<rating>_diff`` (on
        minus off), the ranks and the sample sizes, then his span with the team
        (``team_games_span``, ``span_first_date`` / ``span_last_date``) and the off side limited
        to it (``<total>_off_span``, ``<rating>_off_span``, ``<rating>_diff_span``; not ranked).
        ``<total>_on + <total>_off`` is the team's season total. Sorted by season, team and
        minutes on (most first). Empty input returns that schema with zero rows.

    Raises:
        KeyError: If ``level`` is unknown.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_impact import nba_impact_rows
            pbp, gl = L.load_nba_stats_pbp(2024), L.load_nba_stats_game_lineups(2024)
            poss, sched = L.load_nba_stats_possessions(2024), L.load_nba_stats_schedules(2024)
            player_on_off(nba_impact_rows(pbp, gl, poss, sched), player_dtype=pl.Int64(), min_poss=1000)
    """
    schema = player_on_off_schema(player_dtype)
    gates = on_off_gates(level, _RANKED)
    team, on, _ = _season_totals(rows)
    if on.height == 0:
        return pl.DataFrame(schema=schema)
    keys = ["season", "team_id"]
    assert all(on.schema[k] == team.schema[k] for k in keys), "key dtypes differ between players and teams"
    out = on.rename({c: f"{c}_on" for c in _TOTALS}).join(
        team.rename({c: f"{c}_team" for c in _TOTALS}), on=keys, how="left", validate="m:1"
    )
    out = out.with_columns((pl.col(f"{c}_team") - pl.col(f"{c}_on")).alias(f"{c}_off") for c in _TOTALS)
    if game_dates is None:
        span = pl.DataFrame(schema={**{k: out.schema[k] for k in [*keys, "player_id"]}, **_SPAN_RAW})
    else:
        span = _span_off(rows, game_dates)
    out = out.join(span, on=[*keys, "player_id"], how="left", validate="1:1")
    out = _side_rates(_side_rates(_side_rates(out, "on"), "off"), "off_span").with_columns(
        *[(pl.col(f"{r}_on") - pl.col(f"{r}_off")).alias(f"{r}_diff") for r in _RATINGS],
        *[(pl.col(f"{r}_on") - pl.col(f"{r}_off_span")).alias(f"{r}_diff_span") for r in _RATINGS],
    )
    # min_poss (BB-D4's lineup floor, 400 NBA, 200 elsewhere) keeps the cohort; the per-rating
    # reliability gates (ON_OFF_MIN_N, fitted) decide which ratings are ranked.
    gate = pl.min_horizontal(pl.col("poss_on") + pl.col("opp_poss_on"), pl.col("poss_off") + pl.col("opp_poss_off"))
    n = {f"{r}_{s}": _n_expr(r, s) for s in ("on", "off") for r in _RATINGS}
    # a difference is as good as its thinner side
    n |= {f"{r}_diff": pl.min_horizontal(_n_expr(r, "on"), _n_expr(r, "off")) for r in _RATINGS}
    return (
        out.pipe(attach_sample_sizes, n)
        .pipe(
            attach_ranks,
            list(_RANKED),
            min_cohort=MIN_COHORT_PLAYERS,
            by=("season",),
            lower_is_better=[m for m, low in _RANKED.items() if low],
            qualifier=gate >= min_poss,
            min_n=gates,
        )
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
        ratings; the four splits of a total add up to the team's season total (recompute a rating
        from summed totals: ratings do not add). Sorted by season,
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
