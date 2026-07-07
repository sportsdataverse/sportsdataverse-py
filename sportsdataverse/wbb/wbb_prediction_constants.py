"""Women's college basketball prediction-stack constants + validation metrics.

Thin shim over :mod:`sportsdataverse.mbb.mbb_prediction_constants` -- the
metric functions and the leakage split are league-agnostic and re-exported
**by reference**; the women's fitted constants live in the shared
``LEAGUE_CONSTANTS["womens"]`` entry (fitted on the WBB 2024 as-of backtest
via ``dev/mbb_prediction/fit_pregame.py`` with ``PRED_LEAGUE=womens``).

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_prediction_constants import get_constants
        get_constants("womens").hfa

See Also:
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_prediction_constants import (
    LEAGUE_CONSTANTS,
    LeagueConstants,
    as_of_ratings_split,
    brier_score,
    calibration_table,
    get_constants,
    log_loss_score,
    mae,
    spearman_corr,
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
