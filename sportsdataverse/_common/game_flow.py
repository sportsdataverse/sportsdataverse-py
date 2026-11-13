"""Game flow and sequence stats (BB-D27): the shared core behind ``{lg}_game_flow`` and ``{lg}_team_season_flow``.

Two inputs, built by the league adapters:

* **Score rows** (:data:`SCORE_ROWS_SCHEMA`), one per play-by-play event in game order, carrying the
  running score after it. Every score-based stat reads these: lead changes, ties, time leading,
  largest lead, runs, droughts, the time-weighted margin and win probability. The running score is
  the play-by-play's own, carried forward (each adapter documents its source's gaps), so a game's
  last row is its final score.
* **Possession rows**: BB-D3's :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`,
  for the stops. A stop is a defensive possession on which the offense scores no points.

Definitions, per team and game:

* ``lead_sec`` / ``trail_sec`` / ``tied_sec`` -- game clock with the team ahead, behind, level. The
  score after an event holds until the next event (the last one until ``game_seconds``); the clock
  before the first event is tied. ``lead_pct`` is ``lead_sec / game_seconds``.
* ``lead_changes`` -- how often the leader flipped. A tie is not a lead, so A ahead, tied, B ahead
  is one change, and the first lead is not a change (GEI's rule in ``nba_game_page``).
  ``times_tied`` -- how often the score became level, the 0-0 start not counted. Both are game
  totals, the same on the two rows of a game.
* ``largest_lead`` / ``largest_deficit`` -- the team's biggest lead and biggest deficit (0 if
  never ahead / behind). ``comeback`` is the largest deficit of a game the team won, else 0.
* ``max_run`` -- the most points the team scored without the opponent scoring (an n-0 run), and
  ``runs_10`` the number of its runs of :data:`RUN_POINTS` (10) or more (``opp_runs_10`` the
  opponent's, a non-D1 opponent's included). A score correction (the
  running score going down) does not break a run; it comes off the run when it is the running
  team's own score, so a run is that team's net points.
* ``longest_drought_sec`` -- the longest stretch of game clock between two of the team's scores,
  counting from the tip to its first score and from its last score to the end.
* ``avg_margin`` / ``avg_wp`` -- **game control**: the team's score margin and win probability,
  each averaged over the game clock (a margin held for a minute counts sixty times one held for a
  second). The win probability is the league's in-game model (the WP behind ``nba_game_page``) with
  a home-court-only prior (0.5 at a neutral site), so it reads the game, not the betting line. As in
  ``score_win_prob``, the ball side is the event's team, so a team-less row (a period start, a
  null team) is scored as the away side's ball.
* ``def_poss`` / ``stops`` -- the team's defensive possessions and the scoreless ones. A **kill**
  is three straight stops and a **double kill** six: a run of ``n`` straight stops adds
  ``n // 3`` kills and ``n // 6`` double kills (six straight stops are two kills and a double
  kill). A run of stops goes on across period breaks and ends when the opponent scores.

Per team and season (:func:`team_season_flow`) the totals are summed, the time-based ones weighted by
game clock, and the rated metrics get BB-D2's ``<m>_rank`` / ``<m>_pct`` / ``<m>_n``
(:mod:`sportsdataverse._common.league_context`), within the season, among teams with at least half
the most games, with a floor of five teams. One game is not a cohort, so the game table has no ranks.
``kills_per75`` (BB-G8's per-75 basis) is 0.75 times ``kills_per_100`` with its rank, percentile and
``_n`` copied. Kills are the one count with a per-possession rate: runs and double kills ship per game
and as totals only.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

from typing import Final

import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_TEAMS, attach_per75, attach_ranks, attach_sample_sizes

__all__ = [
    "GAME_FLOW_SCHEMA",
    "KILL_STOPS",
    "RUN_POINTS",
    "SCORE_ROWS_SCHEMA",
    "TEAM_SEASON_FLOW_SCHEMA",
    "attach_home_wp",
    "game_flow",
    "team_season_flow",
]

#: A run of this many unanswered points or more counts in ``runs_10``.
RUN_POINTS: Final[int] = 10
#: Straight stops in a kill; a double kill is twice as many.
KILL_STOPS: Final[int] = 3

#: The play-by-play frame the adapters build: one row per event, in game order (``order``).
#: ``elapsed`` is the game clock run when the event happens (seconds from the tip), ``sec_left``
#: the win-probability clock (seconds left in regulation; in an overtime, in that period) and
#: ``game_seconds`` the game's length, regulation plus the overtimes played. ``home_score`` /
#: ``away_score`` are the running score after the event; ``event_team_id`` the event's team (the
#: model's ball side) and ``home_wp`` the home win probability after it (:func:`attach_home_wp`).
SCORE_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "order": pl.Int64(),
    "home_team_id": pl.Int64(),
    "away_team_id": pl.Int64(),
    "neutral_site": pl.Boolean(),
    "elapsed": pl.Float64(),
    "sec_left": pl.Float64(),
    "game_seconds": pl.Float64(),
    "event_team_id": pl.Int64(),
    "home_score": pl.Int64(),
    "away_score": pl.Int64(),
}

GAME_FLOW_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "opp_team_id": pl.Int64(),
    "is_home": pl.Boolean(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
    "won": pl.Boolean(),
    "game_seconds": pl.Float64(),
    "lead_sec": pl.Float64(),
    "trail_sec": pl.Float64(),
    "tied_sec": pl.Float64(),
    "lead_pct": pl.Float64(),
    "lead_changes": pl.Int64(),
    "times_tied": pl.Int64(),
    "largest_lead": pl.Int64(),
    "largest_deficit": pl.Int64(),
    "comeback": pl.Int64(),
    "max_run": pl.Int64(),
    "runs_10": pl.Int64(),
    "opp_runs_10": pl.Int64(),
    "longest_drought_sec": pl.Float64(),
    "avg_margin": pl.Float64(),
    "avg_wp": pl.Float64(),
    "def_poss": pl.Int64(),
    "stops": pl.Int64(),
    "kills": pl.Int64(),
    "double_kills": pl.Int64(),
}

#: Rated season metric -> (sample-size expression, lower is better).
_RATED: dict[str, tuple[pl.Expr, bool]] = {
    "avg_margin": (pl.col("games"), False),
    "avg_wp": (pl.col("_wp_games"), False),
    "lead_pct": (pl.col("games"), False),
    "runs_10_pg": (pl.col("games"), False),
    "opp_runs_10_pg": (pl.col("games"), True),
    "avg_longest_drought_sec": (pl.col("games"), True),
    "stop_pct": (pl.col("def_poss"), False),
    "kills_pg": (pl.col("_poss_games"), False),
    "double_kills_pg": (pl.col("_poss_games"), False),
    "kills_per_100": (pl.col("def_poss"), False),
}
_LOWER_IS_BETTER = tuple(m for m, (_, low) in _RATED.items() if low)
# ponytail: BB-D3's gate (half the most games), so a college non-D1 opponent is never ranked.
_QUALIFIER = pl.col("games") >= 0.5 * pl.col("games").max().over("season")

TEAM_SEASON_FLOW_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "games": pl.Int64(),
    "wins": pl.Int64(),
    "trail_pct": pl.Float64(),
    "tied_pct": pl.Float64(),
    "lead_changes_pg": pl.Float64(),
    "times_tied_pg": pl.Float64(),
    "largest_lead": pl.Int64(),
    "largest_comeback": pl.Int64(),
    "max_run": pl.Int64(),
    "runs_10": pl.Int64(),
    "opp_runs_10": pl.Int64(),
    "def_poss": pl.Int64(),
    "stops": pl.Int64(),
    "kills": pl.Int64(),
    "double_kills": pl.Int64(),
    **{m: pl.Float64() for m in _RATED},
    **{f"{m}_rank": pl.Float64() for m in _RATED},
    **{f"{m}_pct": pl.Float64() for m in _RATED},
    **{f"{m}_n": pl.Int64() for m in _RATED},
    # BB-G8's per-75 basis: 0.75 * kills_per_100, its rank, percentile and sample copied
    "kills_per75": pl.Float64(),
    "kills_per75_rank": pl.Float64(),
    "kills_per75_pct": pl.Float64(),
    "kills_per75_n": pl.Int64(),
}

_G = "game_id"
_NCAA = {"mbb": "mens", "wbb": "womens"}
_NBA = {"nba": "00", "wnba": "10"}


def attach_home_wp(rows: pl.DataFrame, *, league: str) -> pl.DataFrame:
    """Add ``home_wp``: the league's in-game home win probability after each score row.

    The models behind ``nba_game_page.score_win_prob`` (NBA / WNBA booster, college booster) on
    ``sec_left``, the running score and the event team, with a home-court-only prior from
    ``nba_game_page.home_prior`` (0.5 at a neutral site). Row order is kept.

    Args:
        rows: Score rows (:data:`SCORE_ROWS_SCHEMA`, without ``home_wp``).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.

    Returns:
        ``rows`` with ``home_wp`` (Float64) appended.

    Raises:
        ValueError: If ``league`` is not one of the four.
    """
    if league not in {**_NBA, **_NCAA}:
        raise ValueError(f"league must be one of {sorted({**_NBA, **_NCAA})}, got {league!r}")
    from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob
    from sportsdataverse.nba.nba_game_page import home_prior
    from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob

    if rows.height == 0:
        return rows.with_columns(pl.lit(None, dtype=pl.Float64).alias("home_wp"))
    indexed = rows.with_row_index("_row")
    parts = []
    for (neutral,), part in indexed.group_by(pl.col("neutral_site").fill_null(False)):
        prior, _ = home_prior(None, None, league=league, neutral_site=bool(neutral))
        feats = part.select(
            pl.col("sec_left").alias("start_game_seconds_remaining"),
            "home_score",
            "away_score",
            pl.col("event_team_id").alias("team_id"),
            "home_team_id",
        )
        if league in _NCAA:
            wp = mbb_in_game_win_prob(feats, prior, league=_NCAA[league])["home_win_prob"]
        else:
            wp = nba_in_game_win_prob(feats, prior, league_id=_NBA[league])["home_win_prob"]
        parts.append(part.select("_row", wp.alias("home_wp")))
    return rows.with_columns(pl.concat(parts).sort("_row")["home_wp"].cast(pl.Float64))


def _states(rows: pl.DataFrame) -> pl.DataFrame:
    """Each score row with its margin, the clock it holds for (``_dur``) and what changed."""
    # a clock that runs back (a feed glitch) must not count the same seconds twice
    t = pl.col("elapsed").cum_max().over(_G)
    nxt = t.shift(-1).over(_G).fill_null(pl.col("game_seconds"))
    return rows.with_columns(
        (pl.col("home_score") - pl.col("away_score")).alias("_m"),
        (nxt - t).clip(lower_bound=0.0).alias("_dur"),
        (pl.col("home_score") - pl.col("home_score").shift(1).over(_G).fill_null(0)).alias("_dh"),
        (pl.col("away_score") - pl.col("away_score").shift(1).over(_G).fill_null(0)).alias("_da"),
    ).with_columns(
        pl.when(pl.col("_m") != 0).then(pl.col("_m").sign()).forward_fill().shift(1).over(_G).alias("_prev_lead"),
        pl.col("_m").shift(1).over(_G).fill_null(0).alias("_prev_m"),
        # one side scoring alone; both moving at once (a glitch) answers any run
        pl.when((pl.col("_dh") > 0) & (pl.col("_da") <= 0))
        .then(pl.lit("home"))
        .when((pl.col("_da") > 0) & (pl.col("_dh") <= 0))
        .then(pl.lit("away"))
        .when((pl.col("_dh") > 0) | (pl.col("_da") > 0))
        .then(pl.lit("both"))
        .alias("_scorer"),
    )


def _game_totals(st: pl.DataFrame) -> pl.DataFrame:
    """One row per game: the score-based totals from the home side, plus game control."""
    m, dur = pl.col("_m"), pl.col("_dur")
    sign = m.sign()
    lead = (sign != 0) & pl.col("_prev_lead").is_not_null() & (sign != pl.col("_prev_lead"))
    wp_w = pl.col("home_wp").is_not_null()
    return st.group_by(_G).agg(
        pl.col("season", "home_team_id", "away_team_id", "game_seconds").first(),
        pl.col("home_score").last().alias("home_pts"),
        pl.col("away_score").last().alias("away_pts"),
        dur.filter(m > 0).sum().alias("home_lead_sec"),
        dur.filter(m < 0).sum().alias("away_lead_sec"),
        (dur.filter(m == 0).sum() + pl.col("elapsed").first()).alias("tied_sec"),
        lead.sum().cast(pl.Int64).alias("lead_changes"),
        ((m == 0) & (pl.col("_prev_m") != 0)).sum().cast(pl.Int64).alias("times_tied"),
        m.max().clip(lower_bound=0).alias("home_largest_lead"),
        (-m).max().clip(lower_bound=0).alias("away_largest_lead"),
        ((m * dur).sum() / pl.col("game_seconds").first()).alias("home_avg_margin"),
        ((pl.col("home_wp") * dur).filter(wp_w).sum() / dur.filter(wp_w).sum()).alias("home_avg_wp"),
    )


def _runs(st: pl.DataFrame) -> pl.DataFrame:
    """``(game_id, side) -> max_run, runs_10``: unanswered points per scoring run.

    A run is the scores of one side in a row; a correction (a score going down) stays in the run
    in progress and counts only against that run's side, so a run is its side's net points.
    """
    scorer = pl.col("_scorer")
    changes = st.filter((pl.col("_dh") != 0) | (pl.col("_da") != 0)).with_columns(
        scorer.forward_fill().shift(1).over(_G).alias("_prev")
    )
    changes = changes.with_columns(
        (scorer.is_not_null() & (scorer != pl.col("_prev")).fill_null(True))
        .cast(pl.Int64)
        .cum_sum()
        .over(_G)
        .alias("_run"),
        scorer.forward_fill().over(_G).alias("side"),
    )
    runs = (
        changes.filter(pl.col("side") != "both")
        .group_by(_G, "_run")
        .agg(pl.col("side").first(), pl.when(pl.col("side") == "home").then("_dh").otherwise("_da").sum().alias("_n"))
    )
    return runs.group_by(_G, "side").agg(
        pl.col("_n").max().cast(pl.Int64).alias("max_run"),
        (pl.col("_n") >= RUN_POINTS).sum().cast(pl.Int64).alias("runs_10"),
    )


def _droughts(st: pl.DataFrame) -> pl.DataFrame:
    """``(game_id, side) -> longest_drought_sec``: the longest gap between a side's scores."""
    games = st.group_by(_G).agg(pl.col("game_seconds").first())
    parts = []
    for side, delta in (("home", "_dh"), ("away", "_da")):
        marks = pl.concat(
            [
                games.select(_G, pl.lit(0.0).alias("t")),
                st.filter(pl.col(delta) > 0).select(_G, pl.col("elapsed").alias("t")),
                games.select(_G, pl.col("game_seconds").alias("t")),
            ]
        )
        parts.append(
            marks.sort(_G, "t")
            .group_by(_G)
            .agg(pl.col("t").diff().max().alias("longest_drought_sec"))
            .with_columns(pl.lit(side).alias("side"))
        )
    return pl.concat(parts)


def _kills(poss: pl.DataFrame) -> pl.DataFrame:
    """``(game_id, team_id) -> def_poss, stops, kills, double_kills`` for the defense."""
    team = pl.col("defense_team_id")
    stop = (pl.col("points") == 0).fill_null(False)
    streaks = (
        poss.filter(team.is_not_null())
        .sort(_G, "period", "possession_number")
        .with_columns(stop.alias("_stop"))
        .with_columns((~pl.col("_stop")).cum_sum().over(_G, "defense_team_id").alias("_streak"))
        .group_by(_G, "defense_team_id", "_streak")
        .agg(pl.len().alias("_n"), pl.col("_stop").sum().alias("_stops"))
    )
    return streaks.group_by(_G, pl.col("defense_team_id").alias("team_id")).agg(
        pl.col("_n").sum().cast(pl.Int64).alias("def_poss"),
        pl.col("_stops").sum().cast(pl.Int64).alias("stops"),
        (pl.col("_stops") // KILL_STOPS).sum().cast(pl.Int64).alias("kills"),
        (pl.col("_stops") // (2 * KILL_STOPS)).sum().cast(pl.Int64).alias("double_kills"),
    )


def _sides(games: pl.DataFrame) -> pl.DataFrame:
    """Each game twice, once from each team's side."""
    frames = []
    for us, them, sgn in (("home", "away", 1.0), ("away", "home", -1.0)):
        frames.append(
            games.select(
                "season",
                _G,
                pl.col(f"{us}_team_id").alias("team_id"),
                pl.col(f"{them}_team_id").alias("opp_team_id"),
                pl.lit(us == "home").alias("is_home"),
                pl.lit(us).alias("side"),
                pl.col(f"{us}_pts").alias("pts"),
                pl.col(f"{them}_pts").alias("opp_pts"),
                "game_seconds",
                pl.col(f"{us}_lead_sec").alias("lead_sec"),
                pl.col(f"{them}_lead_sec").alias("trail_sec"),
                "tied_sec",
                "lead_changes",
                "times_tied",
                pl.col(f"{us}_largest_lead").alias("largest_lead"),
                pl.col(f"{them}_largest_lead").alias("largest_deficit"),
                (sgn * pl.col("home_avg_margin")).alias("avg_margin"),
                (pl.col("home_avg_wp") if us == "home" else 1.0 - pl.col("home_avg_wp")).alias("avg_wp"),
            )
        )
    return pl.concat(frames)


def game_flow(score_rows: pl.DataFrame, possessions: pl.DataFrame) -> pl.DataFrame:
    """Per team and game: runs, droughts, kills, lead changes, time leading and game control.

    Args:
        score_rows: Score rows (:data:`SCORE_ROWS_SCHEMA` plus ``home_wp`` from
            :func:`attach_home_wp`; extra columns are ignored).
        possessions: Possession rows with ``game_id``, ``period``, ``possession_number``,
            ``defense_team_id`` and ``points`` (BB-D3's ``RATE_ROWS_SCHEMA``). Only games in
            ``score_rows`` are used; a game with no possessions gets null stops and kills.

    Returns:
        One row per ``(season, game_id, team_id)`` in :data:`GAME_FLOW_SCHEMA`, sorted by game and
        then home before away. A side with no team id (a college non-D1 opponent) gets no row. An
        empty input returns that schema with zero rows.

    Raises:
        KeyError: If ``score_rows`` is missing a schema column.
    """
    missing = sorted({*SCORE_ROWS_SCHEMA, "home_wp"} - set(score_rows.columns))
    if missing:
        raise KeyError(f"score_rows missing columns: {missing}")
    if score_rows.height == 0:
        return pl.DataFrame(schema=GAME_FLOW_SCHEMA)
    rows = score_rows.select([*SCORE_ROWS_SCHEMA, "home_wp"]).cast({**SCORE_ROWS_SCHEMA, "home_wp": pl.Float64()})
    st = _states(rows.sort(_G, "order"))
    poss = possessions.select(
        pl.col(_G).cast(pl.Utf8), "period", "possession_number", pl.col("defense_team_id").cast(pl.Int64), "points"
    )
    out = (
        _sides(_game_totals(st))
        .join(_runs(st), on=[_G, "side"], how="left")
        .join(_droughts(st), on=[_G, "side"], how="left")
        .join(_kills(poss.join(st.select(_G).unique(), on=_G, how="semi")), on=[_G, "team_id"], how="left")
        .filter(pl.col("team_id").is_not_null())
        .with_columns(
            (pl.col("pts") > pl.col("opp_pts")).alias("won"),
            (pl.col("lead_sec") / pl.col("game_seconds")).alias("lead_pct"),
            pl.col("max_run", "runs_10").fill_null(0),
        )
        # before the team filter, so a non-D1 opponent's runs still count against the D1 side
        .with_columns((pl.col("runs_10").sum().over(_G) - pl.col("runs_10")).alias("opp_runs_10"))
        .with_columns(pl.when(pl.col("won")).then(pl.col("largest_deficit")).otherwise(0).alias("comeback"))
    )
    return out.sort(_G, "is_home", descending=[False, True]).select(list(GAME_FLOW_SCHEMA)).cast(GAME_FLOW_SCHEMA)


def team_season_flow(games: pl.DataFrame) -> pl.DataFrame:
    """Per team and season: the game flow totals, rates, ranks, percentiles and ``_n``.

    Args:
        games: Output of :func:`game_flow`.

    Returns:
        One row per ``(season, team_id)`` in :data:`TEAM_SEASON_FLOW_SCHEMA`, sorted by season and
        team. ``lead_pct`` / ``trail_pct`` / ``tied_pct`` and ``avg_margin`` / ``avg_wp`` are
        weighted by game clock; the ``_pg`` columns are per game; ``stop_pct`` is ``stops /
        def_poss`` and ``kills_per_100`` kills per 100 defensive possessions (both null with no
        possessions); ``kills_per75`` is 0.75 times it, with its rank, percentile and ``_n``. ``opp_runs_10`` counts the opponents' 10-point runs. An empty input returns
        that schema with zero rows.
    """
    if games.height == 0:
        return pl.DataFrame(schema=TEAM_SEASON_FLOW_SCHEMA)
    g = games
    secs = pl.col("game_seconds")
    wp_secs = secs.filter(pl.col("avg_wp").is_not_null())
    agg = g.group_by("season", "team_id").agg(
        pl.len().cast(pl.Int64).alias("games"),
        pl.col("def_poss").is_not_null().sum().cast(pl.Int64).alias("_poss_games"),
        pl.col("avg_wp").is_not_null().sum().cast(pl.Int64).alias("_wp_games"),
        pl.col("won").sum().cast(pl.Int64).alias("wins"),
        (pl.col("lead_sec").sum() / secs.sum()).alias("lead_pct"),
        (pl.col("trail_sec").sum() / secs.sum()).alias("trail_pct"),
        (pl.col("tied_sec").sum() / secs.sum()).alias("tied_pct"),
        ((pl.col("avg_margin") * secs).sum() / secs.sum()).alias("avg_margin"),
        ((pl.col("avg_wp") * secs).sum() / wp_secs.sum()).alias("avg_wp"),
        pl.col("lead_changes").mean().alias("lead_changes_pg"),
        pl.col("times_tied").mean().alias("times_tied_pg"),
        pl.col("largest_lead").max(),
        pl.col("comeback").max().alias("largest_comeback"),
        pl.col("max_run").max(),
        pl.col("runs_10").sum(),
        pl.col("opp_runs_10").sum(),
        pl.col("runs_10").mean().alias("runs_10_pg"),
        pl.col("opp_runs_10").mean().alias("opp_runs_10_pg"),
        pl.col("longest_drought_sec").mean().alias("avg_longest_drought_sec"),
        *[pl.col(c).sum() for c in ("def_poss", "stops", "kills", "double_kills")],
        pl.col("kills").mean().alias("kills_pg"),
        pl.col("double_kills").mean().alias("double_kills_pg"),
    )
    rates = agg.with_columns(
        pl.when(pl.col("def_poss") > 0).then(pl.col("stops") / pl.col("def_poss")).alias("stop_pct"),
        pl.when(pl.col("def_poss") > 0).then(100 * pl.col("kills") / pl.col("def_poss")).alias("kills_per_100"),
    )
    return (
        rates.pipe(
            attach_ranks,
            list(_RATED),
            min_cohort=MIN_COHORT_TEAMS,
            by=("season",),
            lower_is_better=_LOWER_IS_BETTER,
            qualifier=_QUALIFIER,
        )
        .pipe(attach_sample_sizes, {m: e for m, (e, _) in _RATED.items()})
        .pipe(attach_per75, {"kills_per75": "kills_per_100"})
        .sort("season", "team_id")
        .select(list(TEAM_SEASON_FLOW_SCHEMA))
        .cast(TEAM_SEASON_FLOW_SCHEMA)
    )
