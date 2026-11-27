"""MLB game story: how the winner won it, in runs.

The baseball adapter of :mod:`sportsdataverse._common.game_story`, on the published MLB releases:
``load_mlb_pbp`` (one row per plate appearance), ``load_mlb_schedule`` (the teams and the official final),
``load_mlb_wpa`` (each PA's win-probability added, from :mod:`sportsdataverse.mlb.mlb_win_expectancy`) and
``load_mlb_re24_matrix`` (the season's run expectancy by base-out state, from
:mod:`sportsdataverse.mlb.mlb_run_expectancy`).

A PA's **RE24** is the run expectancy of the state after it, less the state before it, plus the runs it
scored (the state after the half's last PA is worth 0). The base-out state before each PA comes from
:func:`~sportsdataverse.mlb.mlb_run_expectancy.pbp_base_out_states`.

* **Accounting**: ``starting_pitching`` and ``bullpen``, the RE24 each staff allowed, negated (runs saved
  against expectation); the PA's pitcher is or is not his side's first pitcher of the game. Together they are
  the winner's batting RE24 less the loser's: the margin, less the run expectancy of an empty-base, no-out
  start for each half-inning the winner batted beyond the loser (a home win that skips the 9th's bottom), so
  close to the margin, not equal to it.
* **Splits**: ``home_runs`` (runs scored on home runs) and ``risp_hits`` (value: hits with a runner on second
  or third; points: the runs scored on those PAs).

The moment is the PA with the largest absolute WPA (earliest on ties). ``load_mlb_wpa`` keys a PA's WPA to the next PA's index
(its ``wpa`` at ``i`` is the change from before PA ``i - 1`` to before PA ``i``, and the last PA's effect sits
on a closing row), so the adapter moves each one back to the PA that made it. The top performer is the
player with the most WPA as batter (his side's view) and pitcher (the other side's WPA, negated). The
releases carry ids, not names, so ``player_name`` is null. Periods are innings ``1``-``9``; extra innings keep
their number and are marked overtime.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_story import game_story
from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mlb_game_story"]

_REGULATION = 9
_EDGES = (
    ("starting_pitching", "accounting"),
    ("bullpen", "accounting"),
    ("home_runs", "split"),
    ("risp_hits", "split"),
)
_HITS = ("Single", "Double", "Triple", "Home Run")


def _automatic_runner(half: list[str]) -> pl.Expr:
    """``base_state`` with an extra half's automatic runner on 2nd where the release pbp alone shows him.

    The release pbp has no runner movements, so when he scores or is put out on the half's first PA
    nothing after it shows he was there. That PA's own result does: runs beyond the batter's own (a home
    run's one), or outs beyond the batter's (none if he reached), need a runner on base before it.
    """
    hr = (pl.col("event") == "Home Run").fill_null(False)
    first = pl.col("at_bat_index") == pl.col("at_bat_index").min().over(half)
    batter_out = pl.when(hr | (pl.col("_batter_on") == True)).then(0).otherwise(1)
    implied = (pl.col("runs_on_play") > hr.cast(pl.Int64)) | (pl.col("_outs_after") > batter_out)
    return (
        pl.when(first & (pl.col("inning") > _REGULATION) & (pl.col("base_state") == "___") & implied.fill_null(False))
        .then(pl.lit("_2_"))
        .otherwise(pl.col("base_state"))
        .alias("base_state")
    )


def _states(pbp: pl.DataFrame, re24: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """Each PA with its batting and fielding team, pitcher, RE24 and runs."""
    statsapi = pbp.select(
        pl.col("game_pk").cast(pl.Int64).cast(pl.Utf8).alias("game_id"),
        pl.col("inning").alias("about_inning"),
        pl.col("half_inning").alias("about_half_inning"),
        pl.col("at_bat_index").alias("about_at_bat_index"),
        pl.col("outs").alias("count_outs"),
        pl.col("home_score").alias("result_home_score"),
        pl.col("away_score").alias("result_away_score"),
        pl.col("post_on_first_id").alias("matchup_post_on_first_id"),
        pl.col("post_on_second_id").alias("matchup_post_on_second_id"),
        pl.col("post_on_third_id").alias("matchup_post_on_third_id"),
        pl.col("batter_id").alias("matchup_batter_id"),
    )
    st = pbp_base_out_states(statsapi).with_columns(pl.col("game_id").cast(pl.Int64))
    pa = pbp.select(
        pl.col("game_pk").cast(pl.Int64).alias("game_id"),
        pl.col("at_bat_index").cast(pl.Int64),
        pl.col("batter_id").cast(pl.Int64),
        pl.col("pitcher_id").cast(pl.Int64),
        pl.col("event").cast(pl.Utf8),
        pl.col("description").cast(pl.Utf8),
        pl.col("home_score").cast(pl.Int64),
        pl.col("away_score").cast(pl.Int64),
        pl.col("outs").cast(pl.Int64).alias("_outs_after"),
        pl.any_horizontal(
            (pl.col(c).cast(pl.Int64) == pl.col("batter_id").cast(pl.Int64)).fill_null(False)
            for c in ("post_on_first_id", "post_on_second_id", "post_on_third_id")
        ).alias("_batter_on"),
    ).unique(["game_id", "at_bat_index"], keep="first")
    re = re24.select(
        pl.col("season").cast(pl.Int64),
        pl.col("base_state").cast(pl.Utf8),
        pl.col("outs").cast(pl.Int64),
        pl.col("re"),
    )
    half = ["game_id", "inning", "half"]
    out = (
        st.join(pa, on=["game_id", "at_bat_index"], how="left")
        .with_columns(_automatic_runner(half))
        .join(
            games.select("game_id", "season", "home_team_id", "away_team_id"),
            on="game_id",
            how="inner",
        )
        .join(
            re,
            left_on=["season", "base_state", "outs_start"],
            right_on=["season", "base_state", "outs"],
            how="left",
        )
        .sort("game_id", "at_bat_index")
        .with_columns(
            # the state after a PA is the next PA's; after the half's last PA it is worth 0
            pl.when(pl.col("at_bat_index") == pl.col("at_bat_index").max().over(half))
            .then(pl.lit(0.0))
            .otherwise(pl.col("re").shift(-1).over(half))
            .alias("_re_end"),
            pl.when(pl.col("half") == "top").then("away_team_id").otherwise("home_team_id").alias("bat_team_id"),
            pl.when(pl.col("half") == "top").then("home_team_id").otherwise("away_team_id").alias("field_team_id"),
        )
        .with_columns((pl.col("_re_end") - pl.col("re") + pl.col("runs_on_play")).alias("re24"))
    )
    missing = sorted(set(out["season"].drop_nulls().to_list()) - set(re["season"].drop_nulls().to_list()))
    if missing:
        raise ValueError(f"re24 has no run expectancy for season(s) {missing}: pass load_mlb_re24_matrix for them")
    # a side's starter: the first pitcher it used
    starter = (
        out.sort("game_id", "at_bat_index")
        .group_by("game_id", "field_team_id", maintain_order=True)
        .agg(pl.col("pitcher_id").drop_nulls().first().alias("_starter"))
    )
    return out.join(starter, on=["game_id", "field_team_id"], how="left")


def _edges(st: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    starter = (pl.col("pitcher_id") == pl.col("_starter")).fill_null(False)
    staff = [
        st.filter(mask)
        .group_by("game_id", pl.col("field_team_id").alias("team_id"))
        .agg((-pl.col("re24").sum()).alias("value"))
        .with_columns(pl.lit(name).alias("edge"))
        for name, mask in (("starting_pitching", starter), ("bullpen", starter == False))
    ]
    risp = (pl.col("base_state").str.contains("2") | pl.col("base_state").str.contains("3")) & pl.col("event").is_in(
        _HITS
    )
    splits = [
        st.filter(pl.col("event") == "Home Run")
        .group_by("game_id", pl.col("bat_team_id").alias("team_id"))
        .agg(
            pl.col("runs_on_play").sum().cast(pl.Float64).alias("value"),
            pl.col("runs_on_play").sum().cast(pl.Float64).alias("points"),
        )
        .with_columns(pl.lit("home_runs").alias("edge")),
        st.filter(risp)
        .group_by("game_id", pl.col("bat_team_id").alias("team_id"))
        .agg(
            pl.len().cast(pl.Float64).alias("value"),
            pl.col("runs_on_play").sum().cast(pl.Float64).alias("points"),
        )
        .with_columns(pl.lit("risp_hits").alias("edge")),
    ]
    cols = ["game_id", "team_id", "edge", "value", "points"]
    found = pl.concat(
        [
            *(s.with_columns(pl.col("value").alias("points")).select(cols) for s in staff),
            *(s.select(cols) for s in splits),
        ],
        how="vertical_relaxed",
    )
    teams = pl.concat(
        [
            games.select("game_id", pl.col("home_team_id").alias("team_id")),
            games.select("game_id", pl.col("away_team_id").alias("team_id")),
        ]
    )
    order = {e: i for i, (e, _) in enumerate(_EDGES)}
    return (
        teams.join(pl.DataFrame({"edge": list(order)}), how="cross")
        .join(found, on=["game_id", "team_id", "edge"], how="left")
        .with_columns(pl.col("value", "points").fill_null(0.0))
        .with_columns(
            pl.col("edge").replace_strict(order, return_dtype=pl.Int64).alias("edge_order"),
            pl.col("edge").replace_strict(dict(_EDGES), return_dtype=pl.Utf8).alias("kind"),
            pl.lit("runs").alias("unit"),
        )
    )


def _inputs(
    pbp: pl.DataFrame, schedule: pl.DataFrame, wpa: pl.DataFrame, re24: pl.DataFrame
) -> tuple[pl.DataFrame, ...]:
    """The core's four inputs (games, edges, plays, players) for :func:`mlb_game_story`."""
    games = (
        schedule.select(
            pl.col("game_pk").cast(pl.Int64).alias("game_id"),
            pl.col("season").cast(pl.Int64),
            pl.col("home_team_id").cast(pl.Int64),
            pl.col("away_team_id").cast(pl.Int64),
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
        )
        .unique("game_id", keep="first")
        .join(
            pbp.select(pl.col("game_pk").cast(pl.Int64).alias("game_id")).unique(),
            on="game_id",
            how="semi",
        )
        .with_columns(pl.lit(_REGULATION, dtype=pl.Int64).alias("regulation_periods"))
    )
    st = _states(pbp, re24, games)
    # load_mlb_wpa keys a PA's effect to the next index: move it back to the PA that made it
    w = wpa.select(
        pl.col("game_id").cast(pl.Int64),
        (pl.col("at_bat_index").cast(pl.Int64) - 1).alias("at_bat_index"),
        pl.col("wpa"),
    )
    st = st.join(w, on=["game_id", "at_bat_index"], how="left")
    plays = st.select(
        "game_id",
        pl.col("at_bat_index").alias("play_id"),
        pl.col("at_bat_index").alias("order"),
        pl.col("inning").alias("period"),
        pl.col("half").cast(pl.Utf8).alias("clock"),
        pl.col("bat_team_id").alias("team_id"),
        pl.coalesce("description", "event").alias("text"),
        "home_score",
        "away_score",
        pl.col("wpa").cast(pl.Float64),
        pl.col("inning").cast(pl.Utf8).alias("period_label"),  # an extra inning keeps its number
    )
    home_bat = pl.col("bat_team_id") == pl.col("home_team_id")
    bat_wpa = pl.when(home_bat).then(pl.col("wpa")).otherwise(-pl.col("wpa"))
    players = (
        pl.concat(
            [
                st.select(
                    "game_id",
                    pl.col("bat_team_id").alias("team_id"),
                    pl.col("batter_id").alias("player_id"),
                    bat_wpa.alias("_w"),
                ),
                st.select(
                    "game_id",
                    pl.col("field_team_id").alias("team_id"),
                    pl.col("pitcher_id").alias("player_id"),
                    (-bat_wpa).alias("_w"),
                ),
            ]
        )
        .filter(pl.col("player_id").is_not_null())
        .group_by("game_id", "team_id", "player_id")
        .agg(
            pl.col("_w").sum().alias("value"),
            pl.len().cast(pl.Float64).alias("tiebreak"),
        )
        .with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("player_name"),
            pl.lit("wpa").alias("metric"),
        )
    )
    return games, _edges(st, games), plays, players


def mlb_game_story(
    pbp: pl.DataFrame,
    schedule: pl.DataFrame,
    wpa: pl.DataFrame,
    re24: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """How the winner won it, for one or many MLB games: edges in runs, the moment, innings, runs, performers.

    Args:
        pbp: ``load_mlb_pbp`` plate appearances, one or many games.
        schedule: ``load_mlb_schedule`` rows (``game_pk``, ``season``, the team ids and the final score).
        wpa: ``load_mlb_wpa`` rows for the same games.
        re24: ``load_mlb_re24_matrix`` rows for the games' seasons.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}``, the frames of
        :data:`~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`: the staffs' runs saved
        (``starting_pitching``, ``bullpen``) and the ``home_runs`` / ``risp_hits`` splits, in runs and
        ranked; the largest-WPA plate appearance; innings (an extra inning keeps its number); each side's
        largest unanswered run; the top WPA player (``player_name`` null: the releases carry ids only).

    Raises:
        ValueError: If ``re24`` has no run expectancy for one of the games' seasons.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.mlb import load_mlb_pbp, load_mlb_re24_matrix, load_mlb_schedule, load_mlb_wpa
            from sportsdataverse.mlb.mlb_game_story import mlb_game_story
            sched = load_mlb_schedule([2025]).filter(pl.col("detailed_state") == "Final")
            story = mlb_game_story(load_mlb_pbp([2025]), sched, load_mlb_wpa([2025]), load_mlb_re24_matrix([2025]))
            story["edges"].filter(pl.col("rank") == 1)

        Every walk-off's moment::

            story["moment"].filter(pl.col("winner_wpa") > 0.4)

        See Also:
            * `baseballr`_ -- the R companion for MLB data

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    games, edges, plays, players = _inputs(pbp, schedule, wpa, re24)
    story = game_story(games, edges, plays, players)
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in story.items()}
    return story
