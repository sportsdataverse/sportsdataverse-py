"""College pregame projections: every game's rated pregame number, as of its date.

The game page's pregame card and the bracket print the producer's closed-form projection
(:func:`~sportsdataverse.mbb.mbb_game_predict.mbb_predict_games`:
``margin = em_scale * (home AdjEM - away AdjEM) + hfa``, no home term at a neutral site, and
``P(home) = Phi(margin / margin_sd)``, with the fitted
:data:`~sportsdataverse.mbb.mbb_prediction_constants.LEAGUE_CONSTANTS`). This module publishes
that number per game so the site reads it instead of computing it.

**The rating.** The adjusted-efficiency engine (:mod:`sportsdataverse.mbb.mbb_team_ratings`)
over the season's completed games dated strictly before ``as_of_date``, with the season sim's
preseason prior blended in (:func:`~sportsdataverse.mbb.mbb_team_ratings.load_preseason_prior`):
each Division I team is shrunk toward last season's regressed rating as if it had played
``prior_games`` extra games at it (4 men, 3 women; ``tools/fit_preseason_prior.py``). Before a
team's first game the rating is the prior alone. ``prior_games=0`` turns the blend off, which
gives the season-to-date ``mbb_ratings`` number the card prints today.

**As of.** ``as_of_date`` is the game's date, or the day after the last completed game for a
game not yet reached, so an upcoming game is rated on every game played so far.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._common.ratings import id_text
from sportsdataverse.mbb.mbb_game_predict import mbb_predict_games
from sportsdataverse.mbb.mbb_prediction_constants import PRESEASON_PRIOR, get_constants
from sportsdataverse.mbb.mbb_team_ratings import (
    _league_loaders,
    _normalize_schedule,
    adjust_efficiency,
    load_preseason_prior,
    raw_game_efficiency,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["college_pregame_projections", "mbb_pregame_projections"]

#: :func:`mbb_pregame_projections` output, one row per ESPN game.
PREGAME_PROJECTION_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int32(),
    "game_id": pl.Int32(),
    "game_date": pl.Date(),
    "as_of_date": pl.Date(),
    "home_team_id": pl.Utf8(),
    "away_team_id": pl.Utf8(),
    "neutral_site": pl.Boolean(),
    "home_rating": pl.Float64(),
    "away_rating": pl.Float64(),
    "home_rank": pl.Int64(),
    "away_rank": pl.Int64(),
    "home_games": pl.Int64(),
    "away_games": pl.Int64(),
    "home_rating_raw": pl.Float64(),
    "away_rating_raw": pl.Float64(),
    "prior_games": pl.Float64(),
    "hfa": pl.Float64(),
    "exp_home_margin": pl.Float64(),
    "rated_home_prob": pl.Float64(),
}


def _rated(eff: pl.DataFrame, league: str, prior: pl.DataFrame | None, prior_games: float) -> pl.DataFrame:
    """``team_id, adj_o, adj_d, adj_em, games, rank`` from ``eff`` (and the prior); rank dense on adj_em."""
    if eff.height == 0 and (prior is None or prior.height == 0 or prior_games <= 0):
        return pl.DataFrame(
            schema={"team_id": pl.Utf8, "adj_o": pl.Float64, "adj_d": pl.Float64, "adj_em": pl.Float64}
            | {"games": pl.Int64, "rank": pl.Int64}
        )
    out = adjust_efficiency(eff, league=league, prior=prior, prior_weight=prior_games)
    return out.select(
        "team_id",
        "adj_o",
        "adj_d",
        pl.col("adj_em").fill_nan(None),
        "games",
        pl.col("adj_em").fill_nan(None).rank(method="dense", descending=True).cast(pl.Int64).alias("rank"),
    )


def _rate_day(
    games: pl.DataFrame, before: pl.DataFrame, league: str, prior: pl.DataFrame | None, pg: float
) -> pl.DataFrame:
    """``games`` (one as-of date) with each side's rating, rank, games and raw rating, and the prediction."""
    raw = _rated(before, league, None, 0.0)
    blend = raw if prior is None else _rated(before, league, prior, pg)
    assert blend.schema["team_id"] == games.schema["home_team_id"], "team_id dtype differs"
    pred = mbb_predict_games(
        games.select("game_id", "home_team_id", "away_team_id", "neutral_site"),
        blend.select("team_id", "adj_o", "adj_d", "adj_em", pl.lit(None, dtype=pl.Float64).alias("adj_tempo")),
        league=league,
    ).select("game_id", "exp_margin", "home_win_prob")
    out = games
    for side in ("home", "away"):
        out = out.join(
            blend.select(
                pl.col("team_id").alias(f"{side}_team_id"),
                pl.col("adj_em").alias(f"{side}_rating"),
                pl.col("rank").alias(f"{side}_rank"),
                pl.col("games").alias(f"{side}_games"),
            ),
            on=f"{side}_team_id",
            how="left",
        ).join(
            raw.select(pl.col("team_id").alias(f"{side}_team_id"), pl.col("adj_em").alias(f"{side}_rating_raw")),
            on=f"{side}_team_id",
            how="left",
        )
    return out.join(pred, on="game_id", how="left", validate="1:1")


def college_pregame_projections(
    schedule: pl.DataFrame,
    team_box: pl.DataFrame,
    *,
    league: str = "mens",
    prior: pl.DataFrame | None = None,
    prior_games: float | None = None,
) -> pl.DataFrame:
    """Rate every game of one season's schedule from the completed games before it.

    Args:
        schedule: One season's ESPN schedule (``load_mbb_schedule`` rows, or already
            ``_normalize_schedule``-d), scheduled games included.
        team_box: That season's ESPN team box.
        league: ``"mens"`` / ``"womens"``.
        prior: The season's preseason prior
            (:func:`~sportsdataverse.mbb.mbb_team_ratings.load_preseason_prior`); ``None`` or
            empty rates on this season's games alone.
        prior_games: The prior's weight in games; ``None`` takes the fitted
            :data:`~sportsdataverse.mbb.mbb_prediction_constants.PRESEASON_PRIOR` weight, ``0``
            turns the blend off.

    Returns:
        One row per game in :data:`PREGAME_PROJECTION_SCHEMA`, by date. The rating, rank and
        prediction are null for a side with no rating yet (a non-Division I team before its
        first game, or any team before the first game with no prior); ``prior_games`` is 0
        when no prior was applied.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_pregame_projection import college_pregame_projections
            out = college_pregame_projections(sched, box, prior_games=0)
    """
    pg = PRESEASON_PRIOR[league].weight if prior_games is None else float(prior_games)
    pri = prior if prior is not None and prior.height and pg > 0 else None
    pg = pg if pri is not None else 0.0
    sched = _normalize_schedule(schedule)
    if sched.height == 0:
        return pl.DataFrame(schema=PREGAME_PROJECTION_SCHEMA)
    eff = raw_game_efficiency(sched, team_box)
    last = eff.get_column("date").max() if eff.height else None
    games = (
        sched.select(
            "season",
            id_text(sched, "game_id"),
            pl.col("date").alias("game_date"),
            id_text(sched, "home_team_id"),
            id_text(sched, "away_team_id"),
            pl.col("neutral_site").fill_null(False),
        )
        .unique("game_id", keep="first", maintain_order=True)
        .with_columns(
            (
                pl.min_horizontal("game_date", pl.lit(last + datetime.timedelta(days=1)))
                if isinstance(last, datetime.date)
                else pl.col("game_date")
            ).alias("as_of_date")
        )
    )
    c = get_constants(league)
    frames = [
        _rate_day(sub, eff.filter(pl.col("date") < cutoff), league, pri, pg)
        for (cutoff,), sub in games.group_by("as_of_date", maintain_order=True)
    ]
    rated = pl.concat(frames, how="diagonal_relaxed")
    return (
        rated.with_columns(
            pl.col("game_id").cast(pl.Int32),
            pl.lit(pg).alias("prior_games"),
            pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(c.hfa).alias("hfa"),  # noqa: E712
            pl.col("exp_margin").alias("exp_home_margin"),
            pl.col("home_win_prob").fill_nan(None).alias("rated_home_prob"),
        )
        .with_columns(pl.col("home_games", "away_games").fill_null(0))
        .select(list(PREGAME_PROJECTION_SCHEMA))
        .cast(PREGAME_PROJECTION_SCHEMA)
        .sort("game_date", "game_id")
    )


@overload
def mbb_pregame_projections(
    season: int, *, league: str = "mens", prior_games: float | None = None, return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def mbb_pregame_projections(
    season: int, *, league: str = "mens", prior_games: float | None = None, return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def mbb_pregame_projections(
    season: int, *, league: str = "mens", prior_games: float | None = None, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Rated pregame projection for every ESPN game of a college season, as of each game's date.

    The number the game page's pregame card prints: the closed form on each team's adjusted
    efficiency margin before the game, with the preseason prior blended in; see the module
    docstring for the model and the as-of boundary.

    Args:
        season: The season, named by the year it ends (``2027`` is 2026-27).
        league: ``"mens"`` (MBB) or ``"womens"`` (WBB).
        prior_games: The prior's weight in games; ``None`` (default) takes the fitted weight
            (4 men, 3 women), ``0`` turns the blend off.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per game of the ESPN schedule release (scheduled games included):
        ``season``, ``game_id``, ``game_date``, ``as_of_date`` (the ratings use completed games
        dated strictly before it), ``home_team_id`` / ``away_team_id`` (ESPN ids, Utf8, the
        ``mbb_ratings`` key), ``neutral_site``, each side's ``rating`` (AdjEM with the prior),
        ``rank`` (dense, 1 best), ``games`` and ``rating_raw`` (AdjEM without the prior),
        ``prior_games``, ``hfa`` (points, 0 at a neutral site), ``exp_home_margin`` (points)
        and ``rated_home_prob``.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_pregame_projection import mbb_pregame_projections
            out = mbb_pregame_projections(2026)
            out.select("game_id", "game_date", "exp_home_margin", "rated_home_prob").head()

        Women's, with no blend::

            mbb_pregame_projections(2026, league="womens", prior_games=0)
    """
    if league not in ("mens", "womens"):
        raise ValueError(f"league must be 'mens' or 'womens', got {league!r}")
    load_schedule, load_team_box = _league_loaders(league)
    schedule = load_schedule([season])
    prior = load_preseason_prior(season, schedule, league=league) if prior_games != 0 else None
    out = college_pregame_projections(
        schedule, load_team_box([season]), league=league, prior=prior, prior_games=prior_games
    )
    return out.to_pandas() if return_as_pandas else out
