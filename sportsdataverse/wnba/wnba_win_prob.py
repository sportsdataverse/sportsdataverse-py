"""Season win-probability enrichment for WNBA -- pbp-dataset WP columns (in place).

The pregame closed forms and the in-game win-probability scorer already exist
on the NBA prediction spine, bound to ``league_id="10"``
(:mod:`sportsdataverse.wnba.wnba_game_predict`). This module *productionizes*
them by **enriching the pbp dataset in place**: for every game in a season it
computes a leakage-free pregame anchor from as-of team ratings, scores every
play through the bundled WNBA in-game artifact, and appends two columns
(``pregame_home_prob``, ``home_win_prob``) to the ``load_wnba_pbp`` frame --
every original column is preserved, so the output overwrites the season's
``play_by_play_<season>.parquet`` release asset with WP joined in.

The orchestration mirrors :mod:`sportsdataverse.mbb.mbb_win_prob` (weekly
as-of buckets + an HFA-only fallback anchor for opening-week games), but
reuses the NBA ratings engine (:mod:`sportsdataverse.nba.nba_team_ratings`)
and closed forms directly rather than the NCAA per-league ratings dispatch --
the NBA spine already has an ``as_of_date`` split built in
(:func:`~sportsdataverse.nba.nba_team_ratings.nba_team_ratings`); this module
recomputes the weekly-bucketed ratings from the raw pieces
(:func:`~sportsdataverse.nba.nba_team_ratings.raw_game_efficiency` +
``adjust_efficiency``/``adjust_pace``) so a season's ~25 as-of ratings snapshots
don't each re-hit the network via the season-loader entry point.

* :func:`_pregame_probs` -- per-game pregame home win probability, built from
  opponent-adjusted WNBA ratings computed on games strictly before the Monday
  of each game's week.
* :func:`_compile_season_wp` -- appends the two WP columns to the pbp frame.
* :func:`build_wnba_season_wp` -- loads pbp/schedule/team-box for a season and
  runs the two cores.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Literal, Union, overload

import polars as pl

from sportsdataverse.nba.nba_team_ratings import _normalize_schedule
from sportsdataverse.wnba.wnba_game_predict import (
    wnba_in_game_win_prob,
    wnba_predict_games,
    wnba_predict_margin,
    wnba_win_prob_from_margin,
)
from sportsdataverse.wnba.wnba_loaders import load_wnba_pbp, load_wnba_schedule, load_wnba_team_boxscore
from sportsdataverse.wnba.wnba_team_ratings import adjust_efficiency, adjust_pace, raw_game_efficiency

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["build_wnba_season_wp"]

# The two Float64 columns appended to the pbp frame. The release enriches the
# pbp dataset *in place*, so the output is the full ``load_wnba_pbp`` frame
# (all columns, original dtypes) with exactly these two columns added.
_WP_COLS: tuple[str, str] = ("pregame_home_prob", "home_win_prob")

_PREGAME_SCHEMA: dict[str, pl.DataType] = {"game_id": pl.Utf8, "pregame_home_prob": pl.Float64}


def _pregame_probs(schedule: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
    """Per-game pregame home win probability, leakage-free weekly as-of.

    Args:
        schedule: A ``_normalize_schedule``'d schedule (``game_id, season, date,
            home_team_id, away_team_id, home_score, away_score`` and optionally
            ``neutral_site``). Completed games only are used.
        team_box: Per-team boxscore with ``game_id, game_date, team_id`` and the
            possession inputs (:func:`raw_game_efficiency`).

    Returns:
        One row per game that has a rating for both teams at its week cutoff:
        ``game_id`` (Utf8) + ``pregame_home_prob`` (Float64). Games without an
        as-of rating are absent (the caller supplies the fallback anchor).
    """
    if schedule.is_empty() or team_box.is_empty() or "game_date" not in team_box.columns:
        return pl.DataFrame(schema=_PREGAME_SCHEMA)

    results = schedule.filter(pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null())
    if results.height == 0:
        return pl.DataFrame(schema=_PREGAME_SCHEMA)

    # ID discipline: raw_game_efficiency casts team ids to Utf8, so ratings'
    # team_id is Utf8. Match the games' join keys to it up front -- the raw
    # ESPN schedule ships Int32 ids and wnba_predict_games' dtype guard would
    # otherwise raise on the live path.
    results = results.with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("home_team_id").cast(pl.Utf8),
        pl.col("away_team_id").cast(pl.Utf8),
    )
    if "neutral_site" not in results.columns:
        results = results.with_columns(pl.lit(False).alias("neutral_site"))
    results = results.with_columns(pl.col("date").dt.truncate("1w").alias("_cutoff"))

    frames: list[pl.DataFrame] = []
    for (cutoff,), week in results.group_by("_cutoff", maintain_order=True):
        prior = results.filter(pl.col("date") < cutoff)
        if prior.height == 0:
            continue
        eff = raw_game_efficiency(prior, team_box.filter(pl.col("game_date") < cutoff))
        if eff.height == 0:
            continue
        ratings = (
            adjust_efficiency(eff, league_id="10")
            .join(adjust_pace(eff, league_id="10"), on=["season", "team_id"], how="left")
            .select("team_id", "adj_off_rtg", "adj_def_rtg", "adj_net_rtg", "adj_pace")
        )
        preds = wnba_predict_games(week.select("game_id", "home_team_id", "away_team_id", "neutral_site"), ratings)
        frames.append(preds.select("game_id", pl.col("home_win_prob").alias("pregame_home_prob")))

    if not frames:
        return pl.DataFrame(schema=_PREGAME_SCHEMA)
    # A degenerate early-season week can make the ratings fixed point emit a NaN
    # adj_net_rtg -> NaN margin -> NaN prob. NaN is NOT null in polars, so
    # is_not_null alone lets it through; drop non-finite too so those games take
    # the fallback anchor instead of publishing a NaN pregame_home_prob.
    return pl.concat(frames).filter(
        pl.col("pregame_home_prob").is_not_null() & pl.col("pregame_home_prob").is_not_nan()
    )


def _compile_season_wp(pbp: pl.DataFrame, schedule: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
    """Append ``pregame_home_prob`` + ``home_win_prob`` to a season's pbp.

    The release enriches the pbp dataset **in place**, so every input column is
    preserved (names + dtypes) and exactly the two WP columns are added.

    Args:
        pbp: ``load_wnba_pbp`` frame (needs ``game_id, game_play_number,
            start_game_seconds_remaining, home_score, away_score, team_id,
            home_team_id``; all other columns pass through untouched).
        schedule: ``_normalize_schedule``'d schedule for the same season.
        team_box: Per-team boxscore for the same season.

    Returns:
        The pbp frame with :data:`_WP_COLS` appended (both ``Float64``), sorted
        by ``game_id`` then ``game_play_number``. Empty pbp returns unchanged.
    """
    if pbp.height == 0:
        return pbp

    pregame = _pregame_probs(schedule, team_box)
    pmap = dict(zip(pregame.get_column("game_id").to_list(), pregame.get_column("pregame_home_prob").to_list()))
    # No-prior-information anchor for games without an as-of rating (opening days,
    # unrated opponent): the HFA-only home prob. home_pace/away_pace are
    # irrelevant here -- the margin's pace-scaled term is multiplied by a zero
    # net-rating difference, so any finite nonzero pace leaves only the HFA term.
    fallback = wnba_win_prob_from_margin(wnba_predict_margin(0.0, 0.0, home_pace=1.0, away_pace=1.0, neutral=False))

    frames: list[pl.DataFrame] = []
    for (gid,), sub in pbp.group_by("game_id", maintain_order=True):
        p0 = pmap.get(str(gid), fallback)
        if not math.isfinite(p0):  # never publish a non-finite anchor (belt-and-suspenders vs _pregame_probs)
            p0 = fallback
        wp = wnba_in_game_win_prob(sub, p0)  # row-aligned with sub
        frames.append(
            sub.with_columns(
                pl.lit(p0, dtype=pl.Float64).alias("pregame_home_prob"),
                wp.get_column("home_win_prob").cast(pl.Float64),
            )
        )

    return pl.concat(frames).sort(["game_id", "game_play_number"])


def _load_wnba_frames(season: int) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(pbp, normalized schedule, team_box) for a season -- the mockable I/O seam."""
    return load_wnba_pbp([season]), _normalize_schedule(load_wnba_schedule([season])), load_wnba_team_boxscore([season])


@overload
def build_wnba_season_wp(season: int, *, return_as_pandas: Literal[False] = False) -> pl.DataFrame: ...


@overload
def build_wnba_season_wp(season: int, *, return_as_pandas: Literal[True]) -> "pd.DataFrame": ...


def build_wnba_season_wp(season: int, *, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """A WNBA season's play-by-play with win-probability columns joined in.

    Loads the season's play-by-play, schedule, and team boxscores, builds a
    leakage-free weekly as-of pregame anchor per game from the WNBA ratings
    engine (``league_id="10"``), scores every play through the bundled
    in-game win-probability artifact, and returns the full ``load_wnba_pbp``
    frame with ``pregame_home_prob`` + ``home_win_prob`` appended -- the
    enrich-in-place shape that overwrites the season's
    ``play_by_play_<season>.parquet`` release asset.

    Args:
        season: Season year (e.g. ``2024``); bounded by ``load_wnba_pbp``
            release availability.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The season's ``load_wnba_pbp`` frame (every column preserved) with the
        two :data:`_WP_COLS` appended (both ``Float64``), sorted by ``game_id``
        then ``game_play_number``.

    Example:
        Quick start::

            from sportsdataverse.wnba import build_wnba_season_wp
            wp = build_wnba_season_wp(2024)
            wp.select("game_id", "game_play_number", "home_win_prob").head()

        Pandas output::

            wp_pd = build_wnba_season_wp(2024, return_as_pandas=True)

    See Also:
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    pbp, schedule, team_box = _load_wnba_frames(season)
    out = _compile_season_wp(pbp, schedule, team_box)
    return out.to_pandas() if return_as_pandas else out
