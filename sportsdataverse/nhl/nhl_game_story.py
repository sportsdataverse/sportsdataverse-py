"""NHL game story: how the winner won it, in goals.

The hockey adapter of :mod:`sportsdataverse._common.game_story`, on the ``load_nhl_pbp_full`` play
frame (its ``xg`` is the published expected-goals boosters' score, :func:`~sportsdataverse.nhl.nhl_xg.nhl_xg`),
the skater box (team ids and points) and the schedule (the official final, a shootout's extra goal
included).

* **Accounting** (every goal once, so the winner-minus-loser worths add up to the margin exactly):
  ``even_strength_goals``, ``power_play_goals``, ``short_handed_goals`` (the scorer had as many,
  more or fewer skaters on the ice), ``empty_net_goals``, ``penalty_shot_goals`` and ``shootout`` (1 for
  the shootout winner, the goal the final credits it).
* **Splits**, each in expected goals: ``xg`` (every unblocked shot), ``xg_5v5``, ``high_danger_xg``
  (shots :func:`~sportsdataverse.nhl.nhl_xg.add_shot_geometry` rates high danger) and ``goaltending``:
  a side's goals saved above expected, the opponent's xG on the side's goalie less the goals he allowed
  (empty nets and the shootout left out).

The moment is the play with the largest home win-probability change from
:func:`~sportsdataverse.nhl.nhl_market.nhl_in_game_win_prob`, with a home-ice prior
(:func:`~sportsdataverse.nhl.nhl_market.win_prob_from_margin` of the league's ``hfa``). Periods are
``P1``-``P3``, ``OT`` and ``SO``. The top performer is the side's points leader (ties: goals).

The published boosters over-predict xG before 2024-25 (see :func:`~sportsdataverse.nhl.nhl_xg.nhl_xg`);
both sides of a game carry the same bias, so a difference is less affected than a sum.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_story import game_story
from sportsdataverse.nhl.nhl_market import nhl_in_game_win_prob, win_prob_from_margin
from sportsdataverse.nhl.nhl_prediction_constants import get_constants
from sportsdataverse.nhl.nhl_xg import add_shot_geometry

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nhl_game_story"]

_REGULATION = 3
_EDGES = (
    ("even_strength_goals", "accounting"),
    ("power_play_goals", "accounting"),
    ("short_handed_goals", "accounting"),
    ("empty_net_goals", "accounting"),
    ("penalty_shot_goals", "accounting"),
    ("shootout", "accounting"),
    ("xg", "split"),
    ("xg_5v5", "split"),
    ("high_danger_xg", "split"),
    ("goaltending", "split"),
)
_SO = pl.col("period_type") == "SHOOTOUT"


def _five(col: str) -> pl.Expr:
    """A side's skaters, at most five: an extra attacker for a pulled goalie is not a man advantage."""
    return pl.min_horizontal(pl.col(col), pl.lit(5))


def _sides(skater_box: pl.DataFrame) -> pl.DataFrame:
    """``game_id -> home_team_id, away_team_id`` from the skater box."""
    t = skater_box.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("home_away").cast(pl.Utf8),
    ).unique()
    home = t.filter(pl.col("home_away") == "home").select("game_id", pl.col("team_id").alias("home_team_id"))
    away = t.filter(pl.col("home_away") == "away").select("game_id", pl.col("team_id").alias("away_team_id"))
    return home.join(away, on="game_id", how="inner").unique("game_id", keep="first")


def _events(pbp: pl.DataFrame, sides: pl.DataFrame) -> pl.DataFrame:
    """The plays in order with the event team's id, the opponent's, and each side's skaters."""
    p = pbp.with_columns(pl.col("game_id").cast(pl.Int64)).join(sides, on="game_id", how="inner")
    home = pl.col("event_team_type") == "home"
    away = pl.col("event_team_type") == "away"
    return p.sort("game_id", "event_idx").with_columns(
        pl.when(home).then("home_team_id").when(away).then("away_team_id").alias("team_id"),
        pl.when(home).then("away_team_id").when(away).then("home_team_id").alias("opp_team_id"),
        pl.when(home).then("home_skaters").otherwise("away_skaters").alias("_own"),
        pl.when(home).then("away_skaters").otherwise("home_skaters").alias("_opp"),
    )


def _edges(ev: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """:data:`~sportsdataverse._common.game_story.EDGE_ROWS_SCHEMA` rows for every game and side."""
    goals = ev.filter(
        (pl.col("event_type") == "GOAL") & (_SO.fill_null(False) == False) & pl.col("team_id").is_not_null()
    )
    kind = (
        pl.when(pl.col("empty_net").fill_null(False) == True)
        .then(pl.lit("empty_net_goals"))
        .when(pl.col("strength_state") == "1v0")
        .then(pl.lit("penalty_shot_goals"))
        # a pulled goalie is not a man advantage: count at most five skaters a side (6v5 is even strength)
        .when(_five("_own") > _five("_opp"))
        .then(pl.lit("power_play_goals"))
        .when(_five("_own") < _five("_opp"))
        .then(pl.lit("short_handed_goals"))
        .otherwise(pl.lit("even_strength_goals"))
    )
    g = goals.group_by("game_id", "team_id", kind.alias("edge")).agg(pl.len().cast(pl.Float64).alias("value"))
    # the shootout's goal: the official final less the plays' last score, credited to its winner
    so = games.filter(pl.col("_so_goal")).select(
        "game_id",
        pl.when(pl.col("home_score") > pl.col("away_score"))
        .then("home_team_id")
        .otherwise("away_team_id")
        .alias("team_id"),
        pl.lit("shootout").alias("edge"),
        pl.lit(1.0).alias("value"),
    )
    shots = add_shot_geometry(
        ev.filter(pl.col("xg").is_not_null() & (_SO.fill_null(False) == False) & pl.col("team_id").is_not_null())
    )
    xg = [
        shots.group_by("game_id", "team_id")
        .agg(pl.col("xg").sum().alias("value"))
        .with_columns(pl.lit("xg").alias("edge")),
        shots.filter(pl.col("strength_state") == "5v5")
        .group_by("game_id", "team_id")
        .agg(pl.col("xg").sum().alias("value"))
        .with_columns(pl.lit("xg_5v5").alias("edge")),
        shots.filter(pl.col("shot_danger") == "high")
        .group_by("game_id", "team_id")
        .agg(pl.col("xg").sum().alias("value"))
        .with_columns(pl.lit("high_danger_xg").alias("edge")),
    ]
    # goaltending: the shots a side's goalie faced (the opponent's, net not empty) -> xG less goals
    faced = shots.filter(pl.col("empty_net").fill_null(False) == False)
    gsax = (
        faced.group_by("game_id", pl.col("opp_team_id").alias("team_id"))
        .agg((pl.col("xg").sum() - (pl.col("event_type") == "GOAL").sum()).alias("value"))
        .with_columns(pl.lit("goaltending").alias("edge"))
    )
    cols = ["game_id", "team_id", "edge", "value"]
    found = pl.concat([x.select(cols) for x in (g, so, *xg, gsax)], how="vertical_relaxed")
    # every side gets every edge, 0 when it had none of it
    teams = pl.concat(
        [
            games.select("game_id", pl.col("home_team_id").alias("team_id")),
            games.select("game_id", pl.col("away_team_id").alias("team_id")),
        ]
    )
    order = {e: i for i, (e, _) in enumerate(_EDGES)}
    kinds = dict(_EDGES)
    grid = teams.join(pl.DataFrame({"edge": list(order)}), how="cross")
    return (
        grid.join(found, on=["game_id", "team_id", "edge"], how="left")
        .with_columns(pl.col("value").fill_null(0.0))
        .with_columns(
            pl.col("value").alias("points"),
            pl.col("edge").replace_strict(order, return_dtype=pl.Int64).alias("edge_order"),
            pl.col("edge").replace_strict(kinds, return_dtype=pl.Utf8).alias("kind"),
            pl.lit("goals").alias("unit"),
        )
    )


def _wp(ev: pl.DataFrame, league: str) -> pl.DataFrame:
    """``ev`` plus ``_wp`` (home win probability after the play) and ``wpa`` (its change; the first from the prior).

    The in-game model reads the clock and the score, so a shootout attempt (clock 0, the score level) is left
    unscored: the shootout's swing is carried by the one row that settles it (see :func:`nhl_game_story`).
    """
    prior = win_prob_from_margin(get_constants(league).hfa, league=league)
    wp = nhl_in_game_win_prob(ev, prior, league=league)["home_win_prob"]
    return ev.with_columns(pl.when(_SO.fill_null(False)).then(None).otherwise(wp).alias("_wp")).with_columns(
        (pl.col("_wp") - pl.col("_wp").shift(1).over("game_id").fill_null(prior)).alias("wpa"),
        pl.lit(prior).alias("_prior"),
    )


def _inputs(pbp: pl.DataFrame, schedule: pl.DataFrame, skater_box: pl.DataFrame) -> tuple[pl.DataFrame, ...]:
    """The core's four inputs (games, edges, plays, players) for :func:`nhl_game_story`."""
    sides = _sides(skater_box)
    finals = schedule.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("home_score").cast(pl.Int64),
        pl.col("away_score").cast(pl.Int64),
    ).unique("game_id", keep="first")
    ev = _wp(_events(pbp, sides).join(finals.select("game_id"), on="game_id", how="semi"), "nhl")
    last = ev.group_by("game_id").agg(
        pl.col("_wp").drop_nulls().last().fill_null(pl.col("_prior").first()).alias("_last_wp"),
        pl.col("home_score").drop_nulls().last().alias("_ph"),
        pl.col("away_score").drop_nulls().last().alias("_pa"),
        _SO.any().alias("_has_so"),
        pl.col("period").max().alias("_pmax"),
        pl.col("event_idx").max().alias("_imax"),
    )
    games = (
        sides.join(finals, on="game_id", how="inner")
        .join(last, on="game_id", how="inner")
        .with_columns(
            pl.lit(_REGULATION, dtype=pl.Int64).alias("regulation_periods"),
            # a shootout (its attempts are in the plays) adds one goal to the final that no play carries
            (pl.col("_has_so").fill_null(False) & (pl.col("home_score") != pl.col("away_score"))).alias("_so_goal"),
        )
    )
    so_rows = games.filter(pl.col("_so_goal")).select(
        "game_id",
        pl.lit(None, dtype=pl.Int64).alias("play_id"),
        (pl.col("_imax") + 1).cast(pl.Int64).alias("order"),
        pl.col("_pmax").cast(pl.Int64).alias("period"),
        pl.lit(None, dtype=pl.Utf8).alias("clock"),
        pl.when(pl.col("home_score") > pl.col("away_score"))
        .then("home_team_id")
        .otherwise("away_team_id")
        .alias("team_id"),
        pl.lit("Shootout").alias("text"),
        "home_score",
        "away_score",
        # the shootout settles it: from the win probability at the end of overtime to 1 or 0
        ((pl.col("home_score") > pl.col("away_score")).cast(pl.Float64) - pl.col("_last_wp")).alias("wpa"),
        pl.lit("SO").alias("period_label"),
    )
    plays = ev.select(
        "game_id",
        pl.col("event_idx").cast(pl.Int64).alias("play_id"),
        pl.col("event_idx").cast(pl.Int64).alias("order"),
        pl.col("period").cast(pl.Int64),
        pl.col("period_time").cast(pl.Utf8).alias("clock"),
        pl.col("team_id").cast(pl.Int64),
        pl.col("description").cast(pl.Utf8).alias("text"),
        pl.col("home_score").cast(pl.Int64),
        pl.col("away_score").cast(pl.Int64),
        pl.col("wpa").cast(pl.Float64),
        pl.when(_SO).then(pl.lit("SO")).alias("period_label"),
    )
    plays = pl.concat([plays, so_rows], how="vertical_relaxed")
    players = skater_box.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("player_id").cast(pl.Int64),
        pl.col("player_name").cast(pl.Utf8),
        pl.lit("points").alias("metric"),
        pl.col("points").cast(pl.Float64).alias("value"),
        pl.col("goals").cast(pl.Float64).alias("tiebreak"),
    )
    return games, _edges(ev, games), plays, players


def nhl_game_story(
    pbp: pl.DataFrame,
    schedule: pl.DataFrame,
    skater_box: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """How the winner won it, for one or many NHL games: edges in goals, the moment, periods, runs, performers.

    Args:
        pbp: ``load_nhl_pbp_full`` plays (with its ``xg``), one or many games.
        schedule: ``load_nhl_schedules`` rows (``game_id``, ``home_score``, ``away_score``): the official
            finals, a shootout's goal included.
        skater_box: ``load_nhl_skater_boxscores`` rows: each side's team id and the players' points.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}``, the frames of
        :data:`~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`. Edges are the goals by
        situation (which add up to the margin exactly, the shootout's goal included) and the
        expected-goals splits, in goals and ranked. Periods are ``P1``-``P3``, ``OT`` (``OT2`` ... in
        the playoffs) and ``SO``. A game needs its schedule row and both skater-box sides.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl import load_nhl_pbp_full, load_nhl_schedules, load_nhl_skater_boxscores
            from sportsdataverse.nhl.nhl_game_story import nhl_game_story
            gid = 2025020001
            one = lambda d: d.filter(pl.col("game_id") == gid)
            story = nhl_game_story(
                one(load_nhl_pbp_full([2026])), one(load_nhl_schedules([2026])), one(load_nhl_skater_boxscores([2026]))
            )
            story["edges"].head(3)

        The shootouts among a season's games (the frames as above, for the whole season)::

            story["periods"].filter(pl.col("period_label") == "SO")

        See Also:
            * `fastRhockey`_ -- the R companion for NHL and PWHL play-by-play

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    games, edges, plays, players = _inputs(pbp, schedule, skater_box)
    story = game_story(games, edges, plays, players)
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in story.items()}
    return story
