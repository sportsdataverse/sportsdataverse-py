"""Prediction-stack shared constants + validation metrics (league-agnostic).

Home of the small, dependency-light pieces every module in the MBB/WBB
prediction & tournament stack shares:

* **Validation metrics** (:func:`brier_score`, :func:`log_loss_score`,
  :func:`spearman_corr`, :func:`mae`, :func:`calibration_table`) used by the
  phase oracle/backtest gates.

Later phases extend this module with the per-league constants table
(``LEAGUE_CONSTANTS`` / :func:`get_constants`) and the as-of-date leakage
split (:func:`as_of_ratings_split`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

from sportsdataverse._common.metrics import (
    as_of_ratings_split as as_of_ratings_split,
    brier_score as brier_score,
    calibration_table as calibration_table,
    log_loss_score as log_loss_score,
    mae as mae,
    spearman_corr as spearman_corr,
)

__all__ = [
    "LEAGUE_CONSTANTS",
    "LeagueConstants",
    "PRESEASON_PRIOR",
    "PreseasonPrior",
    "as_of_ratings_split",
    "brier_score",
    "calibration_table",
    "get_constants",
    "log_loss_score",
    "mae",
    "spearman_corr",
]


@dataclass(frozen=True)
class LeagueConstants:
    """Per-league fitted constants for the prediction & tournament stack.

    Algorithms in the stack are league-agnostic; every men's/women's-specific
    number lives here so a WBB caller is a by-reference shim plus this table
    (the same pattern ``wbb_rapm`` / ``wbb_ratings`` already use).

    Attributes:
        hfa: Home-court advantage in points (fitted on the 2024 backtest).
        margin_sd: Std. dev. of the game-margin residual (fitted on the 2024
            backtest; the Brier-minimizing sigma agrees to within 0.04).
        em_scale: Slope applied to the AdjEM difference when predicting a game
            margin. AdjEM is per-100-possessions, so a game margin scales by
            ~tempo/100 (~0.67); the fitted value is lower still because the
            as-of AdjEM estimate is noisy and the optimal predictive slope is
            attenuated (regression dilution). Fitted jointly with ``hfa``.
        avg_tempo: League baseline possessions per game (adjusted-tempo anchor).
        avg_efficiency: League baseline points per 100 possessions.
        quad_thresholds: NET-style quadrant opponent-rank upper bounds, keyed by
            venue (``home`` / ``neutral`` / ``away``) then ``q1`` / ``q2`` / ``q3``
            (Quad 4 is any opponent ranked worse than ``q3``).
        bubble_adj_em: AdjEM of a bubble-quality team on THIS engine's scale
            (mean of engine ranks 40-50 on the fit season) -- the WAB baseline.
        in_game_wp_artifact: Filename of the bundled in-game-WP coefficients under
            ``sportsdataverse/mbb/models`` (fitted + committed in Phase 3).
    """

    hfa: float
    margin_sd: float
    em_scale: float
    avg_tempo: float
    avg_efficiency: float
    quad_thresholds: dict[str, dict[str, int]]
    bubble_adj_em: float
    in_game_wp_artifact: str


# NET quadrant opponent-rank upper bounds by venue (Quad 4 = worse than q3). The men's and
# women's committees use different ranges: NCAA, "Breaking down the NCAA Division I men's and
# women's basketball committees' selection criteria" (2025-03-05),
# https://www.ncaa.org/news/march-basketball/breaking-down-the-ncaa-division-i-mens-and-womens-basketball-committees-selection-criteria/
_NET_QUAD_THRESHOLDS: dict[str, dict[str, int]] = {
    "home": {"q1": 30, "q2": 75, "q3": 160},
    "neutral": {"q1": 50, "q2": 100, "q3": 200},
    "away": {"q1": 75, "q2": 135, "q3": 240},
}
_NET_QUAD_THRESHOLDS_WOMENS: dict[str, dict[str, int]] = {
    "home": {"q1": 25, "q2": 55, "q3": 90},
    "neutral": {"q1": 35, "q2": 65, "q3": 105},
    "away": {"q1": 45, "q2": 80, "q3": 130},
}

# Both leagues' hfa / margin_sd / em_scale / avg_tempo / bubble_adj_em were
# fitted on their 2024 as-of-date backtests (``dev/mbb_prediction/fit_pregame.py``,
# joint least squares of actual margin on em_diff + non-neutral indicator;
# mens: 4,359 eligible games, womens: 4,133 with ``PRED_LEAGUE=womens``; each
# iterated to a fixed point because the ratings engine reads hfa/avg_tempo).
# Quad thresholds are the NCAA's published NET ranges, per league.
LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    "mens": LeagueConstants(
        hfa=2.9281,
        margin_sd=11.2196,
        em_scale=0.5766,
        avg_tempo=69.6255,
        avg_efficiency=104.0,
        quad_thresholds=_NET_QUAD_THRESHOLDS,
        bubble_adj_em=20.394,
        in_game_wp_artifact="mbb_in_game_wp.ubj",
    ),
    "womens": LeagueConstants(
        hfa=2.6432,
        margin_sd=12.1716,
        em_scale=0.6230,
        avg_tempo=71.8212,
        avg_efficiency=95.0,
        quad_thresholds=_NET_QUAD_THRESHOLDS_WOMENS,
        bubble_adj_em=27.472,
        in_game_wp_artifact="wbb_in_game_wp.ubj",
    ),
}


class PreseasonPrior(NamedTuple):
    """The college preseason prior and the season sim's rating noise, per league.

    Attributes:
        regression: Multiplies a team's prior-season final AdjO / AdjD deviation from the
            league average (1 carries it over in full, 0 starts everyone average).
        new_team_quantile: A program new to Division I starts at this quantile of last
            season's Division I AdjEM.
        weight: The prior's weight in pseudo-games: a team with ``g`` games so far is rated
            ``g / (g + weight)`` on this season and the rest on the prior.
        rating_sd: The season sim draws one AdjEM shift per team per simulated season,
            ``N(0, rating_sd ** 2 / (g + weight))``.
    """

    regression: float
    new_team_quantile: float
    weight: float
    rating_sd: float


#: Fit by ``tools/fit_preseason_prior.py`` (2026-11-03) on an as-of-date holdout: weekly cutoffs
#: from the first game (week 0 = preseason), the ratings through each cutoff scoring every later
#: Division I regular-season game (conference tournaments included) with the shipped game model
#: above. ``regression`` (0.1 grid) and ``weight`` (1..25 games) minimize TRAIN log loss; train
#: 2012-2020 and 2022 (2021, the COVID season, is not scored), holdout 2023-2026, scored once
#: against the engine with no prior (paired SE clustered by game):
#:
#: * men: 0.9 and 4 games. Train 0.66376 -> 0.56140. Holdout weeks 0-6 0.73128 -> 0.58139
#:   (-0.14989 +- 0.00306; week 0 0.658 -> 0.594, week 1 1.245 -> 0.586), the season 0.66935 ->
#:   0.57985 (-0.08950 +- 0.00198).
#: * women: 0.6 and 3 games. Train 1.10104 -> 0.55162. Holdout weeks 0-6 0.67948 -> 0.52260
#:   (-0.15687 +- 0.00357), the season 0.61239 -> 0.51926 (-0.09313 +- 0.00224).
#: * ``new_team_quantile``: the 24 programs in their first Division I season in the train years
#:   finished at a median 0.23 percentile of Division I AdjEM, in both leagues.
#: * ``rating_sd`` is 1.25 x the one-team single-game AdjEM noise the game model implies,
#:   ``margin_sd / (em_scale * sqrt 2)`` (13.76 men, 13.82 women): the multiplier (0..1.75 by
#:   0.25) minimizes the TRAIN CRPS of each team's remaining regular-season wins in a 400-season
#:   sim of the shift and game model. Holdout CRPS 1.375 -> 1.338 men, 1.303 -> 1.261 women;
#:   weeks 0-6 the 10-90% band covers 0.83 / 0.84 of final wins (0.65 / 0.65 with no shift).
#:   Widening a game's own margin sd instead did not help (train log loss flat, then worse).
PRESEASON_PRIOR: dict[str, PreseasonPrior] = {
    "mens": PreseasonPrior(regression=0.9, new_team_quantile=0.23, weight=4.0, rating_sd=17.2),
    "womens": PreseasonPrior(regression=0.6, new_team_quantile=0.23, weight=3.0, rating_sd=17.27),
}


def get_constants(league: str) -> LeagueConstants:
    """Return the :class:`LeagueConstants` for a league.

    Args:
        league: Either ``"mens"`` or ``"womens"``.

    Returns:
        The league's :class:`LeagueConstants`.

    Raises:
        ValueError: If ``league`` is not a known key of ``LEAGUE_CONSTANTS``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_prediction_constants import get_constants
            get_constants("mens").hfa
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError:
        raise ValueError(f"Unknown league {league!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from None
