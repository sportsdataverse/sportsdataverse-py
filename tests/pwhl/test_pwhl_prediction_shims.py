"""Tests for the PWHL prediction-spine shims (Phase 5) -- by-reference
re-exports of the NHL prediction spine + women's-league constants.
"""

from __future__ import annotations


import sportsdataverse.nhl.nhl_prediction_constants as nhl_const
import sportsdataverse.pwhl.pwhl_prediction_constants as pwhl_const


def test_metric_functions_are_the_nhl_functions_by_reference():
    assert pwhl_const.brier_score is nhl_const.brier_score
    assert pwhl_const.log_loss_score is nhl_const.log_loss_score
    assert pwhl_const.spearman_corr is nhl_const.spearman_corr
    assert pwhl_const.mae is nhl_const.mae
    assert pwhl_const.calibration_table is nhl_const.calibration_table
    assert pwhl_const.as_of_ratings_split is nhl_const.as_of_ratings_split


def test_get_constants_pinned_to_pwhl():
    const = pwhl_const.get_constants()
    assert const == nhl_const.get_constants("pwhl")


def test_pwhl_shrink_k_stronger_than_nhl():
    assert pwhl_const.get_constants().shrink_k > nhl_const.get_constants("nhl").shrink_k


def test_pwhl_get_constants_uses_shared_registry():
    # pwhl_const.get_constants() must read from the SAME LEAGUE_CONSTANTS
    # table as nhl_prediction_constants (no duplicated/forked constants row).
    assert pwhl_const.LEAGUE_CONSTANTS is nhl_const.LEAGUE_CONSTANTS
