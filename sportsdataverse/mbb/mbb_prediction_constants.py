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


# NET-style quadrant opponent-rank upper bounds by venue (Quad 4 = worse than q3).
# Men's and women's Division I both use the NET with this quadrant structure; the
# thresholds are seeded identically and may be re-fit per league later.
_NET_QUAD_THRESHOLDS: dict[str, dict[str, int]] = {
    "home": {"q1": 30, "q2": 75, "q3": 160},
    "neutral": {"q1": 50, "q2": 100, "q3": 200},
    "away": {"q1": 75, "q2": 135, "q3": 240},
}

# Both leagues' hfa / margin_sd / em_scale / avg_tempo / bubble_adj_em were
# fitted on their 2024 as-of-date backtests (``dev/mbb_prediction/fit_pregame.py``,
# joint least squares of actual margin on em_diff + non-neutral indicator;
# mens: 4,359 eligible games, womens: 4,133 with ``PRED_LEAGUE=womens``; each
# iterated to a fixed point because the ratings engine reads hfa/avg_tempo).
# Quad thresholds are canonical NET definitions.
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
        quad_thresholds=_NET_QUAD_THRESHOLDS,
        bubble_adj_em=27.472,
        in_game_wp_artifact="wbb_in_game_wp.ubj",
    ),
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
