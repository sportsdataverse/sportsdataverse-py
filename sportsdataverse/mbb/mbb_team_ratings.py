"""Adjusted-efficiency team-rating engine (model ①) for the prediction stack.

Computes opponent-adjusted offensive/defensive efficiency (KenPom / Torvik
style) from schedule results + per-team boxscores. The pieces:

* :func:`raw_game_efficiency` -- per-team, per-game possessions + raw
  offensive/defensive efficiency.
* :func:`adjust_efficiency` -- iterative opponent adjustment → AdjO / AdjD /
  AdjEM (Task 1.2).
* :func:`adjust_tempo` -- opponent-adjusted tempo (Task 1.3).
* :func:`mbb_team_ratings` -- public per-team-season entry point (Task 1.4).

Algorithms here are league-agnostic; league-specific constants (HFA, tempo /
efficiency baselines) come from
:func:`sportsdataverse.mbb.mbb_prediction_constants.get_constants`.
"""

from __future__ import annotations

import polars as pl


def raw_game_efficiency(schedule: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
    """Per-team, per-game possessions + raw offensive/defensive efficiency.

    Possessions use the standard box estimate averaged over both teams::

        team_poss = FGA - OREB + TO + 0.44 * FTA
        poss      = 0.5 * (team_poss + opp_poss)
        off_eff   = 100 * pts     / poss
        def_eff   = 100 * opp_pts / poss

    The opponent is derived by self-joining ``team_box`` on ``game_id`` (each game
    has exactly two team rows); home/neutral context comes from ``schedule``.

    Args:
        schedule: Frame with ``game_id, season, date, home_team_id, away_team_id,
            neutral_site`` (ids as strings or ints; cast to ``Utf8`` here).
        team_box: Per-team boxscore with ``game_id, team_id,
            field_goals_attempted, offensive_rebounds, turnovers,
            free_throws_attempted, team_score``.

    Returns:
        One row per (game_id, team_id): ``game_id, season, date, team_id,
        opp_team_id, is_home, neutral_site, poss, off_eff, def_eff``. Empty
        input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
            from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency
            eff = raw_game_efficiency(load_mbb_schedule([2024]), load_mbb_team_boxscore([2024]))
    """
    box = team_box.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("team_id").cast(pl.Utf8),
        pl.col("field_goals_attempted").cast(pl.Float64).alias("fga"),
        pl.col("offensive_rebounds").cast(pl.Float64).alias("orb"),
        pl.col("turnovers").cast(pl.Float64).alias("tov"),
        pl.col("free_throws_attempted").cast(pl.Float64).alias("fta"),
        pl.col("team_score").cast(pl.Float64).alias("pts"),
    ).with_columns((pl.col("fga") - pl.col("orb") + pl.col("tov") + 0.44 * pl.col("fta")).alias("team_poss"))

    opp = box.select(
        "game_id",
        pl.col("team_id").alias("opp_team_id"),
        pl.col("team_poss").alias("opp_poss"),
        pl.col("pts").alias("opp_pts"),
    )
    paired = (
        box.join(opp, on="game_id")
        .filter(pl.col("team_id") != pl.col("opp_team_id"))
        .with_columns((0.5 * (pl.col("team_poss") + pl.col("opp_poss"))).alias("poss"))
        .with_columns(
            (100.0 * pl.col("pts") / pl.col("poss")).alias("off_eff"),
            (100.0 * pl.col("opp_pts") / pl.col("poss")).alias("def_eff"),
        )
    )

    sched = schedule.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("date").cast(pl.Date),
        pl.col("home_team_id").cast(pl.Utf8),
        pl.col("away_team_id").cast(pl.Utf8),
        pl.col("neutral_site").cast(pl.Boolean),
    )

    return (
        paired.join(sched, on="game_id", how="inner")
        .with_columns((pl.col("team_id") == pl.col("home_team_id")).alias("is_home"))
        .select(
            "game_id",
            "season",
            "date",
            "team_id",
            "opp_team_id",
            "is_home",
            "neutral_site",
            "poss",
            "off_eff",
            "def_eff",
        )
        .sort("game_id", "team_id")
    )
