"""Closed-form pregame game predictions (margin / win prob / total).

Phase 2 of the MBB/WBB prediction & tournament stack: turns the
opponent-adjusted team ratings (``mbb_team_ratings``) into per-game expected
margin, home win probability, and expected total via the standard
efficiency-margin closed forms. All league-specific numbers (home-court
advantage, margin sigma, tempo/efficiency anchors) come from
:func:`sportsdataverse.mbb.mbb_prediction_constants.get_constants`, so the
functions are league-agnostic (``league="mens"`` / ``"womens"``).
"""

from __future__ import annotations

from scipy.stats import norm

from sportsdataverse.mbb.mbb_prediction_constants import get_constants

__all__ = [
    "predict_margin",
    "predict_total",
    "win_prob_from_margin",
]


def predict_margin(
    home_adj_em: float,
    away_adj_em: float,
    neutral: bool = False,
    *,
    league: str = "mens",
) -> float:
    """Expected home-minus-away margin from two adjusted efficiency margins.

    Args:
        home_adj_em: Home team's adjusted efficiency margin (points / 100 poss).
        away_adj_em: Away team's adjusted efficiency margin.
        neutral: True for a neutral-site game (no home-court advantage).
        league: ``"mens"`` or ``"womens"`` (selects the fitted HFA).

    Returns:
        Expected margin in points (positive favors the home team).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import predict_margin
            predict_margin(20.0, 10.0)
    """
    hfa = 0.0 if neutral else get_constants(league).hfa
    return float(home_adj_em - away_adj_em + hfa)


def win_prob_from_margin(exp_margin: float, *, league: str = "mens") -> float:
    """Home win probability from an expected margin (normal-CDF closed form).

    Args:
        exp_margin: Expected home-minus-away margin in points.
        league: ``"mens"`` or ``"womens"`` (selects the fitted margin sigma).

    Returns:
        Probability the home team wins, in ``(0, 1)``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import win_prob_from_margin
            win_prob_from_margin(5.0)
    """
    return float(norm.cdf(exp_margin / get_constants(league).margin_sd))


def predict_total(
    home_adj_o: float,
    home_adj_d: float,
    away_adj_o: float,
    away_adj_d: float,
    home_tempo: float,
    away_tempo: float,
    *,
    league: str = "mens",
) -> float:
    """Expected total points from adjusted efficiencies and tempos.

    Expected possessions are ``home_tempo * away_tempo / avg_tempo``; each
    side's expected points per 100 possessions blend its offense with the
    opponent's defense (``0.5 * (off + opp_def)``).

    Args:
        home_adj_o: Home adjusted offensive efficiency (points / 100 poss).
        home_adj_d: Home adjusted defensive efficiency.
        away_adj_o: Away adjusted offensive efficiency.
        away_adj_d: Away adjusted defensive efficiency.
        home_tempo: Home adjusted tempo (possessions / game).
        away_tempo: Away adjusted tempo.
        league: ``"mens"`` or ``"womens"`` (selects the tempo anchor).

    Returns:
        Expected combined points scored by both teams.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import predict_total
            predict_total(110.0, 95.0, 105.0, 100.0, 68.0, 66.0)
    """
    c = get_constants(league)
    poss = home_tempo * away_tempo / c.avg_tempo
    home_pts = 0.5 * (home_adj_o + away_adj_d) * poss / 100.0
    away_pts = 0.5 * (away_adj_o + home_adj_d) * poss / 100.0
    return float(home_pts + away_pts)
