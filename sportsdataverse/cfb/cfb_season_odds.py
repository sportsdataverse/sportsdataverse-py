"""Ratings-driven season Monte Carlo for college football (T2.1 Phase 4).

Feeds a ratings-based ``compute_results`` closure into the shipped
:func:`sportsdataverse.cfb.cfb_simulations.cfb_simulations` engine (nflseedR-style
season/standings/CFP-bracket machinery) so conference-title / playoff / championship
odds fall out of the same predictors as the pregame model. The engine is reused
whole; this module only supplies the per-game result sampler and (Task 4.2) the
public wrapper.
"""

from __future__ import annotations


import datetime
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.cfb.cfb_loaders import load_cfb_schedule
from sportsdataverse.cfb.cfb_prediction_constants import get_constants
from sportsdataverse.cfb.cfb_ratings import cfb_ratings
from sportsdataverse.cfb.cfb_simulations import ComputeResultsFn, cfb_simulations
from sportsdataverse.cfb.cfb_standings import cfb_games_from_schedule

__all__ = ["cfb_season_odds", "make_ratings_compute_results"]

_ODDS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "exp_wins": pl.Float64,
    "conf_title_prob": pl.Float64,
    "playoff_prob": pl.Float64,
    "first_round_bye_prob": pl.Float64,
    "cfp_champ_prob": pl.Float64,
}


def make_ratings_compute_results(ratings: pl.DataFrame, *, era: str = "modern") -> ComputeResultsFn:
    """Build a ``cfb_simulations`` ``compute_results`` closure from fixed ratings.

    The returned closure implements the engine's results contract -- ``(teams, games,
    week_num, *, rng, **kwargs) -> {"teams", "games"}`` -- filling every unplayed
    ``week == week_num`` game's ``result`` with a sampled home margin
    ``round(Normal(exp_margin, margin_sd))``, where ``exp_margin`` is
    :func:`cfb_game_predict.predict_margin` on the two teams' ``adj_net`` (home-field
    applied unless ``neutral``). Unlike the default elo sampler the ratings are
    **fixed**, so ``teams`` passes through unchanged (no elo update). Postseason games
    (``game_type != "REG"``) re-break a sampled tie by win probability.

    Args:
        ratings: A :func:`cfb_ratings.cfb_ratings`-style frame with ``team_id`` and
            ``adj_net``. Teams absent from it are treated as league-average (0.0).
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.

    Returns:
        A ``compute_results`` callable suitable for ``cfb_simulations(...,
        compute_results=...)``.

    Example:
        Quick start::

            import numpy as np, polars as pl
            from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
            cr = make_ratings_compute_results(pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.3, -0.3]}))
            teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
            games = pl.DataFrame({"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"],
                                  "neutral": [0], "result": [None]})
            cr(teams, games, 1, rng=np.random.default_rng(0))["games"]

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the season-simulation engine reused here.
    """
    c = get_constants(era)
    net = dict(zip(ratings["team_id"].to_list(), ratings["adj_net"].to_list()))
    ns, hfa_epa, md = c.net_points_scale, c.hfa_epa, c.margin_sd

    def compute_results(
        teams: pl.DataFrame, games: pl.DataFrame, week_num: int, *, rng: np.random.Generator, **kwargs: object
    ) -> dict[str, pl.DataFrame]:
        g = games.with_columns(pl.col("result").cast(pl.Float64))
        hn = g["home_team"].replace_strict(net, default=0.0, return_dtype=pl.Float64).to_numpy()
        an = g["away_team"].replace_strict(net, default=0.0, return_dtype=pl.Float64).to_numpy()
        neutral = g["neutral"].to_numpy()

        # exp_margin = predict_margin(home_adj_net, away_adj_net, neutral), vectorized:
        # net_scale * (home_net - away_net + 2*hfa_epa on non-neutral fields).
        exp = ns * ((hn - an) + np.where(neutral == 1, 0.0, 2.0 * hfa_epa))
        n = g.height
        raw = exp + rng.normal(0.0, md, n)
        # round away from zero (matches the engine's _round_out margin convention).
        sim_result = np.where(raw > 0, np.ceil(raw), np.where(raw < 0, np.floor(raw), 0.0))

        if "game_type" in g.columns:
            gt = g["game_type"].to_numpy()
            wp = norm.cdf(exp / md)
            tie = (gt != "REG") & (sim_result == 0.0)
            sim_result = np.where(tie, np.where(rng.uniform(0.0, 1.0, n) < wp, 1.0, -1.0), sim_result)

        g = g.with_columns(pl.Series("_sim_result", sim_result, dtype=pl.Float64)).with_columns(
            pl.when((pl.col("week") == week_num) & pl.col("result").is_null())
            .then(pl.col("_sim_result"))
            .otherwise(pl.col("result"))
            .cast(pl.Float64)
            .alias("result")
        )
        return {"teams": teams, "games": g.select(games.columns)}

    return compute_results


@overload
def cfb_season_odds(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    n_sims: int = ...,
    playoff_seeds: int = ...,
    seed: int = ...,
    era: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def cfb_season_odds(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    n_sims: int = ...,
    playoff_seeds: int = ...,
    seed: int = ...,
    era: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_season_odds(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    n_sims: int = 10000,
    playoff_seeds: int = 12,
    seed: int = 0,
    era: str = "modern",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Ratings-driven season Monte Carlo: conference / playoff / championship odds.

    Thin wrapper over :func:`cfb_simulations.cfb_simulations` -- it builds the ratings
    with :func:`cfb_ratings.cfb_ratings`, converts the schedule to the engine format
    with :func:`cfb_standings.cfb_games_from_schedule` (re-keyed on ESPN ``team_id`` so
    the ratings align), and feeds :func:`make_ratings_compute_results` as the sampler.
    All season / standings / bracket machinery is reused; unplayed games are simulated,
    played games (before ``as_of_date``) are kept.

    Args:
        seasons: A single season or list of seasons.
        as_of_date: Leakage boundary forwarded to :func:`cfb_ratings.cfb_ratings`;
            games are kept/simulated from the schedule as-is. ``None`` uses the full season.
        n_sims: Number of simulated seasons.
        playoff_seeds: CFP field size.
        seed: RNG seed for reproducibility.
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per team: ``season``, ``team_id`` (Utf8), ``exp_wins``,
        ``conf_title_prob``, ``playoff_prob``, ``first_round_bye_prob``,
        ``cfp_champ_prob`` (Float64 probabilities in [0, 1]). Zero-row (typed) when
        no ratings/schedule are available.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_season_odds import cfb_season_odds
            odds = cfb_season_odds(2023, n_sims=2000)
            odds.sort("cfp_champ_prob", descending=True).head()

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the simulation engine reused here.
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    # cfb_ratings takes a RatingsConfig, not an era; era feeds the sampler/get_constants.
    ratings = cfb_ratings(seasons, as_of_date=as_of_date)
    schedule = load_cfb_schedule(season_list)
    if ratings.is_empty() or schedule.is_empty():
        empty = pl.DataFrame(schema=_ODDS_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    # Re-key the engine on ESPN team_id (Utf8) so it aligns with ratings.team_id -- the
    # schedule ships numeric home_id/away_id alongside the display names.
    schedule = schedule.with_columns(
        pl.col("home_id").cast(pl.Utf8).alias("home_team"),
        pl.col("away_id").cast(pl.Utf8).alias("away_team"),
    )
    # Keep only the engine's core columns: cfb_games_from_schedule also emits
    # home_points/away_points (SEC capped-scoring tiebreaker), but cfb_simulations
    # concatenates generated conf-champ/bracket games that lack them -> width mismatch.
    # The ratings sampler doesn't use that tiebreaker rung, so dropping them is safe.
    games = cfb_games_from_schedule(schedule).select(
        "season", "week", "game_type", "home_team", "away_team", "result", "neutral"
    )
    teams = (
        schedule.select(team=pl.col("home_team"), conference=pl.col("home_conference"))
        .vstack(schedule.select(team=pl.col("away_team"), conference=pl.col("away_conference")))
        .drop_nulls("team")
        .unique(subset=["team"], keep="first")
    )
    assert ratings.schema["team_id"] == teams.schema["team"] == pl.Utf8, (
        f"ratings team_id {ratings.schema['team_id']} != engine team {teams.schema['team']}"
    )

    sim = cfb_simulations(
        games,
        teams,
        compute_results=make_ratings_compute_results(ratings, era=era),
        simulations=n_sims,
        playoff_seeds=playoff_seeds,
        seed=seed,
    )
    overall = sim["overall"]
    assert isinstance(overall, pl.DataFrame)
    season_value = season_list[0] if len(season_list) == 1 else None
    out = overall.select(
        pl.lit(season_value).cast(pl.Int64).alias("season"),
        pl.col("team").alias("team_id"),
        pl.col("wins").alias("exp_wins"),
        pl.col("won_conf").alias("conf_title_prob"),
        pl.col("made_playoff").alias("playoff_prob"),
        pl.col("first_round_bye").alias("first_round_bye_prob"),
        pl.col("won_cfp").alias("cfp_champ_prob"),
    ).sort("cfp_champ_prob", "conf_title_prob", "exp_wins", descending=True)
    return out.to_pandas() if return_as_pandas else out
