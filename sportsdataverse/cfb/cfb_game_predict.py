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

from scipy.stats import norm

from sportsdataverse.cfb.cfb_prediction_constants import get_constants

__all__ = ["predict_margin", "predict_total", "win_prob_from_margin"]

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
