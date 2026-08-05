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


#: Spread of `adj_net` the fitted constants assume. MEASURED on the 2014-2025
#: corpus the refit ran against (n=13,580 team-games): sd 0.2646, p05 -0.400,
#: p95 +0.413. Not estimated from memory -- a first draft of this line guessed
#: 0.1049, which would have made the gate fire at 2.5x on correct data. A
#: mis-calibrated guard is worse than none: it teaches readers to ignore it.
_FITTED_ADJ_NET_SD = 0.2646
#: Tolerance before the ratings are considered to have moved out from under the
#: constants. 1.6x is wide enough for ordinary season-to-season variation and
#: far tighter than the ~1.9x drift that silently invalidated the previous fit.
_SCALE_DRIFT_TOL = 1.6


def assert_rating_scale(ratings: pl.DataFrame, *, era: str = "modern", tol: float = _SCALE_DRIFT_TOL) -> float:
    """Warn if the ratings have drifted off the scale the constants were fit on.

    THE FAILURE THIS PREVENTS. ``net_points_scale`` is a frozen statement about
    a relationship between two things: rating units and points. When the
    ratings change -- a different ridge penalty, a rescale, a rebuilt corpus --
    the constant silently becomes wrong while every function keeps returning
    plausible numbers. That is exactly what happened: the shipped 44.5367 was
    fit on 2026-07-28, the ridge lambda moved on 08-01, the corpus was rebuilt
    on 08-02, and nothing failed. Measured out-of-sample the result was a
    calibration slope of 0.55 -- predictions stretched nearly 2x wider than
    reality -- for two days, undetected.

    Returns the observed/fitted sd ratio and emits a ``UserWarning`` when it
    leaves ``[1/tol, tol]``. Deliberately a warning, not an exception: a
    legitimate rescale should not break every caller, but it must not pass in
    silence either.
    """
    import warnings

    if "adj_net" not in ratings.columns or ratings.height < 30:
        return 1.0
    sd = ratings["adj_net"].drop_nulls().std()
    if not sd:
        return 1.0
    ratio = float(sd) / _FITTED_ADJ_NET_SD
    if ratio > tol or ratio < 1.0 / tol:
        warnings.warn(
            f"cfb_game_predict: adj_net sd is {float(sd):.4f}, "
            f"{ratio:.2f}x the {_FITTED_ADJ_NET_SD:.4f} the constants for era "
            f"'{era}' were fit against. net_points_scale and the games-played "
            "curve are calibrated to the fitted scale, so predictions will be "
            "mis-scaled by roughly this factor. Refit with "
            "`cfb_higher_models.fit_pregame` (cfbfastR-cfb-data) before "
            "trusting the output.",
            UserWarning,
            stacklevel=3,
        )
    return ratio


def slope_for_games(games_played: float | None, *, era: str = "modern") -> float:
    """Points per unit of rating differential, given how many games back it.

    A single slope is wrong. An as-of rating built on two games is a far
    noisier predictor than one built on twelve, and OLS slopes attenuate
    toward zero as predictor noise grows -- so the correct multiplier is
    smaller early and grows through the season. Measured, walk-forward on
    2014-2025:

        0-3 games -> 10.62      6-7 games -> 42.00
        4-5 games -> 26.06      8+  games -> 54.49

    Passing ``None`` returns the flat ``net_points_scale``, which is the
    average over that curve and the right choice when games-played is unknown.
    """
    c = get_constants(era)
    if games_played is None:
        return c.net_points_scale
    for key, slope in c.slope_by_games.items():
        lo, hi = (int(x) for x in key.split("-"))
        if lo <= games_played <= hi:
            return slope
    return c.net_points_scale


def predict_margin(
    home_adj_net: float,
    away_adj_net: float,
    neutral: bool,
    *,
    era: str = "modern",
    games_played: float | None = None,
) -> float:
    """Expected home scoring margin from the two net ratings.

    Args:
        home_adj_net: Home team's opponent-adjusted net rating (``adj_net`` from
            :func:`cfb_ratings.efficiency_ratings`).
        away_adj_net: Away team's opponent-adjusted net rating.
        neutral: Whether the game is at a neutral site (no home-field advantage).
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS` supplying
            the fitted slope, ``hfa_points`` and the attenuation curve.
        games_played: Games behind the WEAKER of the two as-of ratings. Supplying
            it selects the games-played slope (see :func:`slope_for_games`) and
            is worth ~0.6 MAE; omitting it falls back to the flat
            ``net_points_scale``, which is the average over the curve.

    Returns:
        The expected margin (home minus away), in points:
        ``slope * (home_adj_net - away_adj_net) + hfa_points`` on a home field,
        or without the HFA term on a neutral one.

        HFA is added in POINTS, not routed through the slope. The previous form
        multiplied an EPA-scale ``2 * hfa_epa`` by ``net_points_scale``, which
        tied the two together and let them drift apart unnoticed -- the shipped
        pair implied ~1.65 points against a measured ~3.0.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_game_predict import predict_margin
            predict_margin(0.30, 0.10, neutral=False)

        With games-played, which selects the attenuation-corrected slope::

            predict_margin(0.30, 0.10, neutral=False, games_played=9)
    """
    c = get_constants(era)
    slope = slope_for_games(games_played, era=era)
    hfa = 0.0 if neutral else c.hfa_points
    return slope * (home_adj_net - away_adj_net) + hfa


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

    assert_rating_scale(ratings, era=era)

    # `games` if present selects the attenuation-corrected slope per row; the
    # weaker (smaller) of the two ratings' game counts is the binding one.
    if "home_games" in joined.columns and "away_games" in joined.columns:
        slope_expr = pl.min_horizontal("home_games", "away_games").map_elements(
            lambda g: slope_for_games(g, era=era), return_dtype=pl.Float64
        )
    else:
        slope_expr = pl.lit(c.net_points_scale)

    with_margin = joined.with_columns(
        exp_margin=(
            slope_expr * (pl.col("home_adj_net") - pl.col("away_adj_net"))
            + pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(c.hfa_points)  # noqa: E712
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
