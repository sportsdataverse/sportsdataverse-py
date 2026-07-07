"""Identity tests: the wbb rapm shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_rapm_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_rapm as m
    from sportsdataverse.wbb import wbb_rapm as w

    assert w.RapmConfig is m.RapmConfig
    assert w.DEFAULT_RAPM_CONFIG is m.DEFAULT_RAPM_CONFIG
    assert w.RapmPriorInfo is m.RapmPriorInfo
    assert w.RapmPlayerContext is m.RapmPlayerContext
    assert w.build_priors is m.build_priors
    assert w.build_player_context is m.build_player_context
    assert w.calc_player_weights is m.calc_player_weights
    assert w.calc_lineup_outputs is m.calc_lineup_outputs
    assert w.slow_regression is m.slow_regression
    assert w.calculate_rapm is m.calculate_rapm
    assert w.calc_slow_pseudo_inverse is m.calc_slow_pseudo_inverse
    assert w.calculate_predicted_out is m.calculate_predicted_out
    assert w.calculate_residual_error is m.calculate_residual_error
    assert w.calculate_sd_rapm is m.calculate_sd_rapm
    assert w.RapmProcessingInputs is m.RapmProcessingInputs
    assert w.build_weak_prior_from_rapm is m.build_weak_prior_from_rapm
    assert w.apply_weak_priors is m.apply_weak_priors
    assert w.pick_ridge_regression is m.pick_ridge_regression
    assert w.AFFECTED_PARTIAL_FIELDNAMES is m.AFFECTED_PARTIAL_FIELDNAMES
    assert w.ON_OFF_REPORT_REPLACEMENT_KEYS is m.ON_OFF_REPORT_REPLACEMENT_KEYS
    assert w.inject_rapm_into_players is m.inject_rapm_into_players
    assert w.RapmPreProcDiagnostics is m.RapmPreProcDiagnostics
    assert w.calc_collinearity_diag is m.calc_collinearity_diag


def test_wbb_rapm_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_rapm as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    # Every re-exported public mbb_rapm function/TypedDict/constant needed by
    # downstream callers is covered -- see test_wbb_rapm_shim_is_mbb_core for
    # the per-symbol identity assertions this parity check summarizes.
    assert len(w.__all__) == 23
