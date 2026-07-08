"""Closed-form pregame spread / total / win-prob + market edge (model 2 of T4.2).

Reads the native ratings from :mod:`sportsdataverse.nfl.nfl_ratings` and the
fitted era constants from :mod:`sportsdataverse.nfl.nfl_prediction_constants`.
No bundled artifact and no market input: the predictions are pure functions of
the native ratings. ``market_edge`` (native minus market) is a **display
output** derived from a caller-supplied odds frame -- odds never feed
``exp_margin`` / ``exp_total`` / ``home_win_prob``.
"""

from __future__ import annotations

from scipy.stats import norm

from sportsdataverse.nfl.nfl_prediction_constants import get_constants

__all__ = ["predict_margin", "predict_total", "win_prob_from_margin"]


def predict_margin(home_adj_net: float, away_adj_net: float, neutral: bool, *, era: str = "modern") -> float:
    """Expected home scoring margin from two net ratings.

    ``points_per_net * (home_adj_net - away_adj_net)`` plus the era HFA on
    non-neutral fields.

    Args:
        home_adj_net: Home team's ``adj_net`` (EPA/play units).
        away_adj_net: Away team's ``adj_net``.
        neutral: True drops the home-field advantage.
        era: Constants era key (default ``"modern"``).

    Returns:
        Expected home margin in points (positive = home favored).

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_market import predict_margin
            predict_margin(0.10, -0.05, False)
    """
    cfg = get_constants(era)
    return cfg.points_per_net * (home_adj_net - away_adj_net) + (0.0 if neutral else cfg.hfa)


def win_prob_from_margin(exp_margin: float, *, era: str = "modern") -> float:
    """Home win probability from an expected margin (Gaussian margin model).

    Args:
        exp_margin: Expected home margin in points.
        era: Constants era key (supplies ``margin_sd``).

    Returns:
        ``Phi(exp_margin / margin_sd)`` in ``[0, 1]``.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_market import win_prob_from_margin
            win_prob_from_margin(3.0)
    """
    cfg = get_constants(era)
    return float(norm.cdf(exp_margin / cfg.margin_sd))


def predict_total(
    home_adj_off: float,
    home_adj_def: float,
    away_adj_off: float,
    away_adj_def: float,
    *,
    era: str = "modern",
) -> float:
    """Expected combined point total from the four efficiency components.

    ``avg_total + total_scale * ((home_adj_off - away_adj_def) +
    (away_adj_off - home_adj_def))`` -- each offense measured against the
    defense it faces.

    Args:
        home_adj_off: Home ``adj_off_epa``.
        home_adj_def: Home ``adj_def_epa`` (lower = better defense).
        away_adj_off: Away ``adj_off_epa``.
        away_adj_def: Away ``adj_def_epa``.
        era: Constants era key.

    Returns:
        Expected combined total in points.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_market import predict_total
            predict_total(0.10, -0.02, 0.05, 0.01)
    """
    cfg = get_constants(era)
    matchup_sum = (home_adj_off - away_adj_def) + (away_adj_off - home_adj_def)
    return cfg.avg_total + cfg.total_scale * matchup_sum
