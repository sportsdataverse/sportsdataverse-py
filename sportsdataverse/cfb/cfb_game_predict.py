"""Closed-form CFB pregame predictors (T2.1 Phase 2).

Turns the opponent-adjusted ratings from :mod:`cfb_ratings` into the three
pregame numbers a betting/preview surface needs: expected margin, home win
probability, and expected total points. Everything here is closed-form -- no
model artifact, no fit at call time -- so the only tunables are the era config in
:data:`cfb_prediction_constants.CFB_CONSTANTS` (``net_points_scale``, ``margin_sd``,
``total_intercept``, ``total_scale``, ``total_pace_scale`` fitted on the 2023
backtest by ``dev/cfb_prediction/fit_pregame.py``; ``hfa_epa`` is the ratings
ridge's native home coefficient).

The win-probability model treats a game's realized margin as
``Normal(exp_margin, margin_sd**2)``: ``P(home wins) = P(margin > 0) =
Phi(exp_margin / margin_sd)``. That single Gaussian is why ``margin_sd`` is the
only free knob for probabilities.
"""

from __future__ import annotations

from typing import Literal, overload

import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.cfb.cfb_prediction_constants import get_constants

__all__ = ["cfb_predict_games", "predict_margin", "predict_total", "win_prob_from_margin"]

# Output column contract for :func:`cfb_predict_games`.
_PREDICT_COLUMNS = [
    "game_id",
    "home_team_id",
    "away_team_id",
    "neutral_site",
    "exp_margin",
    "home_win_prob",
    "exp_total",
]


def predict_margin(
    home_adj_net: float,
    away_adj_net: float,
    neutral: bool,
    *,
    era: str = "modern",
) -> float:
    """Expected home scoring margin from the two net ratings.

    Args:
        home_adj_net: Home team's opponent-adjusted net rating (``adj_net`` from
            :func:`cfb_ratings.efficiency_ratings`).
        away_adj_net: Away team's opponent-adjusted net rating.
        neutral: Whether the game is at a neutral site (no home-field advantage).
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS` supplying
            the fitted ``net_points_scale`` and ``hfa_epa``.

    Returns:
        The expected margin (home minus away), in points:
        ``net_points_scale * (home_adj_net - away_adj_net + 2 * hfa_epa)`` on a home
        field, or without the ``2 * hfa_epa`` term on a neutral one.
        ``net_points_scale`` converts the EPA-per-play rating differential into
        points; the HFA is the ratings ridge's native home coefficient applied
        component-wise (home_off +hfa_epa, home_def -hfa_epa => net +2*hfa_epa), an
        EPA-scale additive that lands in the margin (~1.27 pt) and leaves totals
        untouched. See :func:`predict_total`.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_game_predict import predict_margin
            predict_margin(0.30, 0.10, neutral=False)
    """
    c = get_constants(era)
    hfa_net = 0.0 if neutral else 2.0 * c.hfa_epa
    return c.net_points_scale * (home_adj_net - away_adj_net + hfa_net)


def win_prob_from_margin(exp_margin: float, *, era: str = "modern") -> float:
    """Home win probability from an expected margin via the Gaussian CDF.

    Args:
        exp_margin: Expected home margin in points (e.g. from :func:`predict_margin`).
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS` supplying
            ``margin_sd``.

    Returns:
        ``Phi(exp_margin / margin_sd)`` -- the probability the home team wins under
        a ``Normal(exp_margin, margin_sd**2)`` margin model. ``0.5`` at a zero
        expected margin.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_game_predict import win_prob_from_margin
            win_prob_from_margin(7.0)
    """
    return float(norm.cdf(exp_margin / get_constants(era).margin_sd))


def predict_total(
    home_adj_off: float,
    home_adj_def: float,
    away_adj_off: float,
    away_adj_def: float,
    game_pace: float,
    *,
    era: str = "modern",
) -> float:
    """Expected combined point total from the four efficiency ratings + tempo.

    Fitted linear model ``total_intercept + total_scale * sum4 + total_pace_scale *
    game_pace``, where ``sum4 = home_adj_off + away_adj_def + away_adj_off +
    home_adj_def``. The four ratings are summed because each side's scoring rises
    with its own offense and with the opponent's EPA-*allowed* (``adj_def`` is
    lower = better defense). ``game_pace`` (the matchup's expected scrimmage plays,
    ``home_off_pace * away_off_pace / league_avg_pace``) enters because a total is a
    *sum* -- tempo scales both sides' points the same way, so it compounds into the
    total (whereas in the margin, a differential, pace cancels). All three
    coefficients are fitted on 2023 actual totals.

    Args:
        home_adj_off: Home offense adjusted EPA/play (``adj_off_epa``).
        home_adj_def: Home defense adjusted EPA/play allowed (``adj_def_epa``).
        away_adj_off: Away offense adjusted EPA/play.
        away_adj_def: Away defense adjusted EPA/play allowed.
        game_pace: Expected scrimmage plays for the matchup, i.e.
            ``home_off_pace * away_off_pace / league_avg_pace`` from the ratings'
            ``off_pace`` column (:func:`cfb_predict_games` computes this for you).
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS` supplying
            the fitted ``total_intercept`` / ``total_scale`` / ``total_pace_scale``.

    Returns:
        The expected combined total points.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_game_predict import predict_total
            predict_total(0.20, -0.05, 0.10, 0.02, game_pace=66.0)
    """
    c = get_constants(era)
    sum4 = home_adj_off + away_adj_def + away_adj_off + home_adj_def
    return c.total_intercept + c.total_scale * sum4 + c.total_pace_scale * game_pace


@overload
def cfb_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    era: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def cfb_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    era: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    era: str = "modern",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Predict a whole schedule of games from a ratings frame (vectorized).

    Applies the three closed-form predictors across every row of ``games`` in
    one pass. ``ratings`` is joined twice -- once on ``home_team_id`` and once on
    ``away_team_id`` -- so each game carries both teams' ``adj_net`` / ``adj_off_epa``
    / ``adj_def_epa`` / ``off_pace``. The totals model's ``game_pace`` factor is
    computed here as ``home_off_pace * away_off_pace / league_avg_pace``, where the
    league average is the mean ``off_pace`` of the passed ratings frame.

    Args:
        games: Schedule frame with ``game_id``, ``home_team_id``, ``away_team_id``,
            and ``neutral_site`` columns. The two team-id columns must share the
            dtype of ``ratings["team_id"]`` (asserted before the join).
        ratings: A :func:`cfb_ratings.cfb_ratings`-style frame with ``team_id``,
            ``adj_net``, ``adj_off_epa``, ``adj_def_epa``, and ``off_pace``.
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per game with ``game_id``, ``home_team_id``, ``away_team_id``,
        ``neutral_site``, ``exp_margin``, ``home_win_prob``, ``exp_total``.

    Raises:
        AssertionError: If either team-id join key's dtype disagrees with
            ``ratings["team_id"]``.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
            from sportsdataverse.cfb import cfb_ratings
            from sportsdataverse.cfb.cfb_schedule import cfb_schedule  # schedule loader
            ratings = cfb_ratings(2023)
            preds = cfb_predict_games(schedule_2023, ratings)

    See Also:
        * `cfbfastR`_ -- the R companion whose ratings/prediction surface this mirrors.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    key_dtype = ratings.schema["team_id"]
    assert games.schema["home_team_id"] == key_dtype, (
        f"home_team_id dtype {games.schema['home_team_id']} != ratings team_id {key_dtype}"
    )
    assert games.schema["away_team_id"] == key_dtype, (
        f"away_team_id dtype {games.schema['away_team_id']} != ratings team_id {key_dtype}"
    )

    c = get_constants(era)
    rate_cols = ratings.select("team_id", "adj_net", "adj_off_epa", "adj_def_epa", "off_pace")
    home = rate_cols.rename({col: f"home_{col}" for col in rate_cols.columns if col != "team_id"})
    away = rate_cols.rename({col: f"away_{col}" for col in rate_cols.columns if col != "team_id"})

    # League-average tempo for the pace factor, taken from the ratings frame itself
    # (no magic constant). game_pace = home_off_pace * away_off_pace / league_avg.
    league_avg_pace = ratings["off_pace"].mean()
    league_avg_pace = float(league_avg_pace) if league_avg_pace else 1.0

    joined = games.join(home, left_on="home_team_id", right_on="team_id", how="left").join(
        away, left_on="away_team_id", right_on="team_id", how="left"
    )

    with_margin = joined.with_columns(
        exp_margin=(
            c.net_points_scale
            * (
                pl.col("home_adj_net")
                - pl.col("away_adj_net")
                + pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(2.0 * c.hfa_epa)  # noqa: E712
            )
        ),
        exp_total=c.total_intercept
        + c.total_scale
        * (
            pl.col("home_adj_off_epa")
            + pl.col("away_adj_def_epa")
            + pl.col("away_adj_off_epa")
            + pl.col("home_adj_def_epa")
        )
        + c.total_pace_scale * (pl.col("home_off_pace") * pl.col("away_off_pace") / league_avg_pace),
    )
    win_prob = norm.cdf(with_margin["exp_margin"].to_numpy() / c.margin_sd)
    out = with_margin.with_columns(home_win_prob=pl.Series("home_win_prob", win_prob, dtype=pl.Float64)).select(
        _PREDICT_COLUMNS
    )
    return out.to_pandas() if return_as_pandas else out
