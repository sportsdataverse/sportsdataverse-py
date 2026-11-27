"""PWHL game story: how the winner won it, in goals.

The PWHL adapter of :mod:`sportsdataverse._common.game_story`, on ``load_pwhl_pbp`` (HockeyTech's plays, in
game order), ``load_pwhl_schedules`` (the teams and the official final, a shootout's goal included),
``load_pwhl_skater_boxscores`` (points) and, when given, ``load_pwhl_xg_pbp`` (each shot's expected goals
from the published proxy model, :mod:`sportsdataverse.pwhl.pwhl_xg_proxy`).

* **Accounting** (every goal once; the winner-minus-loser worths add up to the margin exactly):
  ``even_strength_goals``, ``power_play_goals``, ``short_handed_goals``, ``empty_net_goals``,
  ``penalty_shot_goals`` (HockeyTech's goal flags) and ``shootout`` (1 for the shootout winner).
* **Splits**, with the xG shots: ``xg`` (the shots' expected goals) and ``goaltending`` (a side's goals
  saved above expected: the opponent's xG on the side's net, empty nets left out, less the goals allowed).

The package has no PWHL in-game win-probability model yet, so the moment is the ``deciding_score``: the
winner's goal that put it ahead for good (the shootout, when the game went to one). Periods are ``P1``-``P3``,
``OT``, ``OT2`` ... and ``SO``; the top performer is the side's points leader (ties: goals).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_story import game_story

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["pwhl_game_story"]

_REGULATION = 3
_EDGES = (
    ("even_strength_goals", "accounting"),
    ("power_play_goals", "accounting"),
    ("short_handed_goals", "accounting"),
    ("empty_net_goals", "accounting"),
    ("penalty_shot_goals", "accounting"),
    ("shootout", "accounting"),
    ("xg", "split"),
    ("goaltending", "split"),
)


def _flag(col: str) -> pl.Expr:
    """A HockeyTech ``"1"`` / ``"0"`` flag as a boolean (null as false)."""
    return pl.col(col).cast(pl.Utf8).fill_null("0") == "1"


def _period(col: str = "period_of_game") -> pl.Expr:
    """``"1"``..``"3"`` -> 1..3; ``"OT1"`` -> 4, ``"OT2"`` -> 5; later periods keep their number."""
    p = pl.col(col).cast(pl.Utf8)
    n = p.str.replace("OT", "").cast(pl.Int64, strict=False)
    return pl.when(p.str.starts_with("OT")).then(_REGULATION + n).otherwise(n)


def _edges(goals: pl.DataFrame, games: pl.DataFrame, xg: pl.DataFrame | None) -> pl.DataFrame:
    kind = (
        pl.when(_flag("empty_net"))
        .then(pl.lit("empty_net_goals"))
        .when(_flag("penalty_shot"))
        .then(pl.lit("penalty_shot_goals"))
        .when(_flag("power_play"))
        .then(pl.lit("power_play_goals"))
        .when(_flag("short_handed"))
        .then(pl.lit("short_handed_goals"))
        .otherwise(pl.lit("even_strength_goals"))
    )
    found = [
        goals.group_by("game_id", "team_id", kind.alias("edge")).agg(pl.len().cast(pl.Float64).alias("value")),
        games.filter(pl.col("_so_goal")).select(
            "game_id",
            pl.when(pl.col("home_score") > pl.col("away_score"))
            .then("home_team_id")
            .otherwise("away_team_id")
            .alias("team_id"),
            pl.lit("shootout").alias("edge"),
            pl.lit(1.0).alias("value"),
        ),
    ]
    edges = [e for e in _EDGES if xg is not None or e[1] == "accounting"]
    if xg is not None:
        sides = games.select("game_id", "home_team_id", "away_team_id")
        shots = (
            xg.select(
                pl.col("game_id").cast(pl.Int64),
                pl.col("team_id").cast(pl.Int64),
                pl.col("xg").cast(pl.Float64),
                pl.col("goal").fill_null(False),
                _flag("empty_net").alias("_en"),
            )
            .filter(pl.col("xg").is_not_null())
            .join(sides, on="game_id", how="inner")
            .with_columns(
                pl.when(pl.col("team_id") == pl.col("home_team_id"))
                .then("away_team_id")
                .otherwise("home_team_id")
                .alias("opp_team_id")
            )
        )
        found.append(
            shots.group_by("game_id", "team_id")
            .agg(pl.col("xg").sum().alias("value"))
            .with_columns(pl.lit("xg").alias("edge"))
        )
        found.append(
            shots.filter(pl.col("_en") == False)
            .group_by("game_id", pl.col("opp_team_id").alias("team_id"))
            .agg((pl.col("xg").sum() - pl.col("goal").sum()).alias("value"))
            .with_columns(pl.lit("goaltending").alias("edge"))
        )
    cols = ["game_id", "team_id", "edge", "value"]
    found_df = pl.concat([f.select(cols) for f in found], how="vertical_relaxed")
    teams = pl.concat(
        [
            games.select("game_id", pl.col("home_team_id").alias("team_id")),
            games.select("game_id", pl.col("away_team_id").alias("team_id")),
        ]
    )
    order = {e: i for i, (e, _) in enumerate(edges)}
    kinds = dict(edges)
    return (
        teams.join(pl.DataFrame({"edge": list(order)}), how="cross")
        .join(found_df, on=["game_id", "team_id", "edge"], how="left")
        .with_columns(pl.col("value").fill_null(0.0))
        .with_columns(
            pl.col("value").alias("points"),
            pl.col("edge").replace_strict(order, return_dtype=pl.Int64).alias("edge_order"),
            pl.col("edge").replace_strict(kinds, return_dtype=pl.Utf8).alias("kind"),
            pl.lit("goals").alias("unit"),
        )
    )


def _inputs(
    pbp: pl.DataFrame, schedule: pl.DataFrame, skater_box: pl.DataFrame, xg_shots: pl.DataFrame | None
) -> tuple[pl.DataFrame, ...]:
    """The core's four inputs (games, edges, plays, players) for :func:`pwhl_game_story`."""
    sched = schedule.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
        pl.col("home_score").cast(pl.Int64),
        pl.col("away_score").cast(pl.Int64),
        pl.col("game_status").cast(pl.Utf8).alias("_status"),
    ).unique("game_id", keep="first")
    p = (
        pbp.drop("home_team_id", "away_team_id", strict=False)
        .with_row_index("_r")
        .with_columns(
            pl.col("game_id").cast(pl.Int64),
            pl.col("team_id").cast(pl.Int64, strict=False),
            _period().alias("_p"),
        )
        .join(
            sched.select("game_id", "home_team_id", "away_team_id"),
            on="game_id",
            how="inner",
        )
        .filter(pl.col("_p").is_not_null())  # the shootout's attempts carry no period; its goal is added below
        .sort("game_id", "_r")
    )
    is_goal = (pl.col("event") == "goal") & pl.col("team_id").is_not_null()
    p = p.with_columns(
        (is_goal & (pl.col("team_id") == pl.col("home_team_id"))).cast(pl.Int64).cum_sum().over("game_id").alias("_hs"),
        (is_goal & (pl.col("team_id") == pl.col("away_team_id"))).cast(pl.Int64).cum_sum().over("game_id").alias("_as"),
    )
    last = p.group_by("game_id").agg(
        pl.col("_hs").last().alias("_ph"),
        pl.col("_as").last().alias("_pa"),
        pl.col("_p").max().alias("_pmax"),
        pl.col("_r").max().alias("_rmax"),
    )
    games = sched.join(last, on="game_id", how="inner").with_columns(
        pl.lit(_REGULATION, dtype=pl.Int64).alias("regulation_periods"),
        # the schedule says a game went to a shootout; the final credits its winner one goal no play carries
        (pl.col("_status").str.contains("SO").fill_null(False) & (pl.col("home_score") != pl.col("away_score"))).alias(
            "_so_goal"
        ),
    )
    so_rows = games.filter(pl.col("_so_goal")).select(
        "game_id",
        pl.lit(None, dtype=pl.Int64).alias("play_id"),
        (pl.col("_rmax") + 1).cast(pl.Int64).alias("order"),
        (pl.col("_pmax") + 1).cast(pl.Int64).alias("period"),
        pl.lit(None, dtype=pl.Utf8).alias("clock"),
        pl.when(pl.col("home_score") > pl.col("away_score"))
        .then("home_team_id")
        .otherwise("away_team_id")
        .alias("team_id"),
        pl.lit("Shootout").alias("text"),
        "home_score",
        "away_score",
        pl.lit(None, dtype=pl.Float64).alias("wpa"),
        pl.lit("SO").alias("period_label"),
    )
    name = pl.concat_str(
        [pl.col("player_name_first"), pl.col("player_name_last")],
        separator=" ",
        ignore_nulls=True,
    )
    plays = p.select(
        "game_id",
        pl.col("_r").cast(pl.Int64).alias("play_id"),
        pl.col("_r").cast(pl.Int64).alias("order"),
        pl.col("_p").alias("period"),
        pl.col("time_of_period").cast(pl.Utf8).alias("clock"),
        "team_id",
        (pl.col("event").cast(pl.Utf8) + pl.lit(": ") + name.fill_null("")).alias("text"),
        pl.col("_hs").alias("home_score"),
        pl.col("_as").alias("away_score"),
        pl.lit(None, dtype=pl.Float64).alias("wpa"),
        pl.lit(None, dtype=pl.Utf8).alias("period_label"),
    )
    players = skater_box.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("player_id").cast(pl.Int64, strict=False),
        pl.concat_str(
            [pl.col("first_name"), pl.col("last_name")],
            separator=" ",
            ignore_nulls=True,
        ).alias("player_name"),
        pl.lit("points").alias("metric"),
        pl.col("points").cast(pl.Float64).alias("value"),
        pl.col("goals").cast(pl.Float64).alias("tiebreak"),
    )
    goals = p.filter(is_goal)
    return games, _edges(goals, games, xg_shots), pl.concat([plays, so_rows], how="vertical_relaxed"), players


def pwhl_game_story(
    pbp: pl.DataFrame,
    schedule: pl.DataFrame,
    skater_box: pl.DataFrame,
    xg_shots: pl.DataFrame | None = None,
    *,
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """How the winner won it, for one or many PWHL games: edges in goals, the moment, periods, runs, performers.

    Args:
        pbp: ``load_pwhl_pbp`` plays, one or many games, in the release's game order.
        schedule: ``load_pwhl_schedules`` rows: the home and away team ids and the official final.
        skater_box: ``load_pwhl_skater_boxscores`` rows (points, goals).
        xg_shots: ``load_pwhl_xg_pbp`` rows; without them the ``xg`` and ``goaltending`` edges are left out.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}``, the frames of
        :data:`~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`. The goals-by-situation edges add up to
        the margin exactly. The moment's ``rule`` is ``deciding_score`` (no PWHL in-game win probability).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.pwhl import (
                load_pwhl_pbp, load_pwhl_schedules, load_pwhl_skater_boxscores, load_pwhl_xg_pbp,
            )
            from sportsdataverse.pwhl.pwhl_game_story import pwhl_game_story
            story = pwhl_game_story(
                load_pwhl_pbp([2026]), load_pwhl_schedules([2026]), load_pwhl_skater_boxscores([2026]),
                load_pwhl_xg_pbp([2026]),
            )
            story["edges"].filter(pl.col("rank") == 1)

        Every shootout::

            story["periods"].filter(pl.col("period_label") == "SO")

        See Also:
            * `fastRhockey`_ -- the R companion for NHL and PWHL play-by-play

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    games, edges, plays, players = _inputs(pbp, schedule, skater_box, xg_shots)
    story = game_story(games, edges, plays, players)
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in story.items()}
    return story
