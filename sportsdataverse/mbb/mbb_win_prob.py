"""Season win-probability compile helper -- the per-play home-WP release.

The in-game win-probability model already exists and is oracle-gated
(:func:`sportsdataverse.mbb.mbb_game_predict.mbb_in_game_win_prob`, prediction
stack T1.0). This module *productionizes* it into a per-play release: for every
game in a season it computes a leakage-free pregame anchor, scores every play
through the bundled artifact, and attaches display metadata (ids, team names,
running scores, play sequence).

Two pure cores + a loader wrapper (mirrors ``mbb_team_ratings`` /
``mbb_bracketology``):

* :func:`_pregame_probs` -- per-game pregame home win probability, built from
  opponent-adjusted ratings computed on games **strictly before the Monday of
  each game's week** (``date.dt.truncate("1w")``). This is the same
  leakage-free as-of split the pregame gate is validated on
  (``test_mbb_prediction_backtest``); it is bucketed to week granularity so the
  fixed point runs ~25x/season instead of once per game.
* :func:`_compile_season_wp` -- the identity transform: score each game's plays
  and hstack the metadata. Because the ``home_win_prob`` column is
  ``mbb_in_game_win_prob``'s output unchanged, the compile helper inherits the
  model's decile calibration (gated in ``test_mbb_prediction_backtest``).
* :func:`build_mbb_season_wp` -- loads pbp/schedule/team-box for a season and
  runs the two cores. WBB is the ``league="womens"`` shim in ``wbb_win_prob``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union, overload

import polars as pl

from sportsdataverse.mbb.mbb_game_predict import (
    mbb_in_game_win_prob,
    mbb_predict_games,
    predict_margin,
    win_prob_from_margin,
)
from sportsdataverse.mbb.mbb_team_ratings import (
    _league_loaders,
    _normalize_schedule,
    adjust_efficiency,
    adjust_tempo,
    raw_game_efficiency,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["build_mbb_season_wp"]

# Output contract (one row per play). Ids are Utf8 join keys; scores/sequence
# are widened to Int64 so the schema is stable across the Int32 releases.
_WP_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64,
    "game_id": pl.Utf8,
    "game_play_number": pl.Int64,
    "game_date": pl.Date,
    "home_team_name": pl.Utf8,
    "away_team_name": pl.Utf8,
    "home_score": pl.Int64,
    "away_score": pl.Int64,
    "pregame_home_prob": pl.Float64,
    "home_win_prob": pl.Float64,
}

_PREGAME_SCHEMA: dict[str, pl.DataType] = {"game_id": pl.Utf8, "pregame_home_prob": pl.Float64}


def _pregame_probs(schedule: pl.DataFrame, team_box: pl.DataFrame, *, league: str = "mens") -> pl.DataFrame:
    """Per-game pregame home win probability, leakage-free weekly as-of.

    Args:
        schedule: A ``_normalize_schedule``'d schedule (``game_id, season, date,
            home_team_id, away_team_id, home_score, away_score`` and optionally
            ``neutral_site``). Completed games only are used.
        team_box: Per-team boxscore with ``game_id, game_date, team_id`` and the
            possession inputs (:func:`raw_game_efficiency`).
        league: ``"mens"`` or ``"womens"``.

    Returns:
        One row per game that has a rating for both teams at its week cutoff:
        ``game_id`` (Utf8) + ``pregame_home_prob`` (Float64). Games without an
        as-of rating are absent (the caller supplies the fallback anchor).
    """
    results = schedule.filter(pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null())
    if results.height == 0:
        return pl.DataFrame(schema=_PREGAME_SCHEMA)

    # ID discipline: raw_game_efficiency casts team ids to Utf8, so ratings'
    # team_id is Utf8. Match the games' join keys to it up front -- the raw
    # ESPN schedule ships Int32 ids and mbb_predict_games' dtype guard would
    # otherwise raise on the live path (the backtest reads a pre-cast fixture).
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
            adjust_efficiency(eff, league=league)
            .join(adjust_tempo(eff, league=league), on=["season", "team_id"], how="left")
            .select("team_id", "adj_o", "adj_d", "adj_em", "adj_tempo")
        )
        preds = mbb_predict_games(
            week.select("game_id", "home_team_id", "away_team_id", "neutral_site"),
            ratings,
            league=league,
        )
        frames.append(preds.select("game_id", pl.col("home_win_prob").alias("pregame_home_prob")))

    if not frames:
        return pl.DataFrame(schema=_PREGAME_SCHEMA)
    return pl.concat(frames).filter(pl.col("pregame_home_prob").is_not_null())


def _compile_season_wp(
    pbp: pl.DataFrame, schedule: pl.DataFrame, team_box: pl.DataFrame, *, league: str = "mens"
) -> pl.DataFrame:
    """Score every play in a season's pbp and attach display metadata.

    Args:
        pbp: ``load_mbb_pbp`` frame (``game_id, season, game_play_number,
            game_date, home_team_name, away_team_name, home_score, away_score,
            start_game_seconds_remaining, team_id, home_team_id``).
        schedule: ``_normalize_schedule``'d schedule for the same season.
        team_box: Per-team boxscore for the same season.
        league: ``"mens"`` or ``"womens"``.

    Returns:
        One row per play (:data:`_WP_SCHEMA`), sorted by ``game_id`` then
        ``game_play_number``. Empty pbp returns the zero-row schema.
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=_WP_SCHEMA)

    pregame = _pregame_probs(schedule, team_box, league=league)
    pmap = dict(zip(pregame.get_column("game_id").to_list(), pregame.get_column("pregame_home_prob").to_list()))
    # No-prior-information anchor for games without an as-of rating (opening days,
    # unrated opponent): the HFA-only home prob. It washes out within minutes of
    # tip-off, so a single scalar suffices.
    # ponytail: flat HFA anchor, not per-game neutral-aware -- early-season
    # neutral games are negligible and the anchor is dominated by in-game state.
    fallback = win_prob_from_margin(predict_margin(0.0, 0.0, neutral=False, league=league), league=league)

    frames: list[pl.DataFrame] = []
    for (gid,), sub in pbp.group_by("game_id", maintain_order=True):
        p0 = pmap.get(str(gid), fallback)
        wp = mbb_in_game_win_prob(sub, p0, league=league)  # row-aligned with sub
        frames.append(
            sub.select(
                pl.col("season").cast(pl.Int64),
                pl.col("game_id").cast(pl.Utf8),
                pl.col("game_play_number").cast(pl.Int64),
                pl.col("game_date").cast(pl.Date),
                pl.col("home_team_name").cast(pl.Utf8),
                pl.col("away_team_name").cast(pl.Utf8),
                pl.col("home_score").cast(pl.Int64),
                pl.col("away_score").cast(pl.Int64),
            ).with_columns(
                pl.lit(p0, dtype=pl.Float64).alias("pregame_home_prob"),
                wp.get_column("home_win_prob"),
            )
        )

    return pl.concat(frames).select(list(_WP_SCHEMA)).sort(["game_id", "game_play_number"])


def _load_league_frames(season: int, league: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(pbp, normalized schedule, team_box) for a season -- the mockable I/O seam."""
    load_schedule, load_team_box = _league_loaders(league)
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_pbp as load_pbp  # noqa: PLC0415
    else:
        from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp as load_pbp  # noqa: PLC0415
    return load_pbp([season]), _normalize_schedule(load_schedule([season])), load_team_box([season])


@overload
def build_mbb_season_wp(
    season: int, *, league: str = "mens", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def build_mbb_season_wp(season: int, *, league: str = "mens", return_as_pandas: Literal[True]) -> "pd.DataFrame": ...


def build_mbb_season_wp(
    season: int, *, league: str = "mens", return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per-play home win probability for a full season (the WP release table).

    Loads the season's play-by-play, schedule, and team boxscores, builds a
    leakage-free weekly as-of pregame anchor per game, and scores every play
    through the bundled in-game win-probability artifact.

    Args:
        season: Season year (e.g. ``2024``); bounded by ``load_mbb_pbp`` release
            availability (``>= 2002``).
        league: ``"mens"`` or ``"womens"`` (selects the loaders + constants).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per play (:data:`_WP_SCHEMA`): ``season, game_id,
        game_play_number, game_date, home_team_name, away_team_name,
        home_score, away_score, pregame_home_prob, home_win_prob``.

    Example:
        Quick start::

            from sportsdataverse.mbb import build_mbb_season_wp
            wp = build_mbb_season_wp(2024)
            wp.filter(pl.col("game_id") == "401638643").sort("game_play_number")

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    pbp, schedule, team_box = _load_league_frames(season, league)
    out = _compile_season_wp(pbp, schedule, team_box, league=league)
    return out.to_pandas() if return_as_pandas else out
