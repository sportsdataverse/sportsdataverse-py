"""Closed-form CFB pregame predictors (T2.1 Phase 2).

Turns the opponent-adjusted ratings from :mod:`cfb_ratings` into the three
pregame numbers a betting/preview surface needs: expected margin, home win
probability, and expected total points. Everything here is closed-form -- no
model artifact, no fit at call time -- so the only tunable is the era config in
:data:`cfb_prediction_constants.CFB_CONSTANTS` (``hfa`` and ``margin_sd`` are
fitted on the 2023 backtest in Task 2.3).

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

# ponytail: league scoring level for predict_total, in EPA-per-drive-equivalent
# units. adj_off/adj_def carry the ridge intercept (absolute EPA/play), so their
# difference for an average matchup is ~0; this offset lifts an average game to a
# realistic ~55-point total. It is a seeded scale, not a fitted one -- Task 2.3's
# total gate can promote it to a fitted PredictConfig field if the MAE demands it.
_TOTAL_BASELINE = 4.5


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
            the home-field advantage.

    Returns:
        The expected margin (home minus away), in points: the rating differential
        plus the era HFA on non-neutral fields.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_game_predict import predict_margin
            predict_margin(0.30, 0.10, neutral=False)
    """
    hfa = 0.0 if neutral else get_constants(era).hfa
    return home_adj_net - away_adj_net + hfa


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
    *,
    era: str = "modern",
) -> float:
    """Expected combined point total from the four efficiency ratings.

    Each side's expected points come from its own offense against the opponent's
    defense: ``avg_drives * points_per_epa * 0.5 * (own_adj_off + opp_adj_def +
    baseline)``. Both ``adj_off`` and ``adj_def`` are absolute EPA/play carrying
    the ridge intercept, and ``adj_def`` is *EPA allowed* (lower = better defense),
    so summing own offense with the opponent's EPA-allowed is the correct matchup:
    a strong opposing defense (very negative ``adj_def``) *lowers* this side's
    expected points. :data:`_TOTAL_BASELINE` sets the league scoring level.

    (This sums ``opp_adj_def`` rather than subtracting it as the design note
    sketched -- the shipped :func:`cfb_ratings.efficiency_ratings` emits
    ``adj_def_epa`` as EPA-allowed/lower-is-better, so subtraction would invert
    the defensive effect.)

    Args:
        home_adj_off: Home offense adjusted EPA/play (``adj_off_epa``).
        home_adj_def: Home defense adjusted EPA/play allowed (``adj_def_epa``).
        away_adj_off: Away offense adjusted EPA/play.
        away_adj_def: Away defense adjusted EPA/play allowed.
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS` supplying
            ``avg_drives`` and ``points_per_epa``.

    Returns:
        The expected combined total points (home side + away side).

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_game_predict import predict_total
            predict_total(0.20, -0.05, 0.10, 0.02)
    """
    c = get_constants(era)
    scale = c.avg_drives * c.points_per_epa * 0.5
    home_pts = scale * (home_adj_off + away_adj_def + _TOTAL_BASELINE)
    away_pts = scale * (away_adj_off + home_adj_def + _TOTAL_BASELINE)
    return home_pts + away_pts


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
    / ``adj_def_epa``.

    Args:
        games: Schedule frame with ``game_id``, ``home_team_id``, ``away_team_id``,
            and ``neutral_site`` columns. The two team-id columns must share the
            dtype of ``ratings["team_id"]`` (asserted before the join).
        ratings: A :func:`cfb_ratings.cfb_ratings`-style frame with ``team_id``,
            ``adj_net``, ``adj_off_epa``, and ``adj_def_epa``.
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
    """
    key_dtype = ratings.schema["team_id"]
    assert games.schema["home_team_id"] == key_dtype, (
        f"home_team_id dtype {games.schema['home_team_id']} != ratings team_id {key_dtype}"
    )
    assert games.schema["away_team_id"] == key_dtype, (
        f"away_team_id dtype {games.schema['away_team_id']} != ratings team_id {key_dtype}"
    )

    c = get_constants(era)
    rate_cols = ratings.select("team_id", "adj_net", "adj_off_epa", "adj_def_epa")
    home = rate_cols.rename({col: f"home_{col}" for col in rate_cols.columns if col != "team_id"})
    away = rate_cols.rename({col: f"away_{col}" for col in rate_cols.columns if col != "team_id"})

    scale = c.avg_drives * c.points_per_epa * 0.5
    joined = games.join(home, left_on="home_team_id", right_on="team_id", how="left").join(
        away, left_on="away_team_id", right_on="team_id", how="left"
    )

    with_margin = joined.with_columns(
        exp_margin=(
            pl.col("home_adj_net")
            - pl.col("away_adj_net")
            + pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(c.hfa)  # noqa: E712
        ),
        exp_total=scale
        * (
            pl.col("home_adj_off_epa")
            + pl.col("away_adj_def_epa")
            + pl.col("away_adj_off_epa")
            + pl.col("home_adj_def_epa")
            + 2 * _TOTAL_BASELINE
        ),
    )
    win_prob = norm.cdf(with_margin["exp_margin"].to_numpy() / c.margin_sd)
    out = with_margin.with_columns(home_win_prob=pl.Series("home_win_prob", win_prob, dtype=pl.Float64)).select(
        _PREDICT_COLUMNS
    )
    return out.to_pandas() if return_as_pandas else out
