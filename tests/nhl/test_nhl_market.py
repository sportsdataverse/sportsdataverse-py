"""Tests for :mod:`sportsdataverse.nhl.nhl_market` (Phase 2 -- pregame closed form)."""

from __future__ import annotations


from sportsdataverse.nhl.nhl_market import (
    expected_goals,
    predict_margin,
    predict_total,
    win_prob_from_margin,
)


def test_expected_goals_neutral_drops_hfa():
    eg_home_neutral, eg_away_neutral = expected_goals(2.8, 2.2, 2.5, 2.4, True, league="nhl")
    eg_home, eg_away = expected_goals(2.8, 2.2, 2.5, 2.4, False, league="nhl")
    assert eg_home_neutral < eg_home
    assert eg_away_neutral > eg_away


def test_expected_goals_formula():
    from sportsdataverse.nhl.nhl_prediction_constants import get_constants

    hfa = get_constants("nhl").hfa
    eg_home, eg_away = expected_goals(2.8, 2.2, 2.5, 2.4, False, league="nhl")
    assert abs(eg_home - (0.5 * (2.8 + 2.4) + hfa / 2.0)) < 1e-9
    assert abs(eg_away - (0.5 * (2.5 + 2.2) - hfa / 2.0)) < 1e-9


def test_predict_margin_symmetric_teams_is_zero_at_neutral():
    margin = predict_margin(2.5, 2.5, 2.5, 2.5, True, league="nhl")
    assert abs(margin) < 1e-9


def test_win_prob_symmetric_is_half():
    assert abs(win_prob_from_margin(0.0, league="nhl") - 0.5) < 1e-9


def test_win_prob_wider_sigma_pulls_toward_half(monkeypatch):
    import sportsdataverse.nhl.nhl_prediction_constants as const_mod

    narrow = win_prob_from_margin(1.0, league="nhl")

    from dataclasses import replace

    wide_constants = replace(const_mod.LEAGUE_CONSTANTS["nhl"], margin_sd=10.0)
    monkeypatch.setitem(const_mod.LEAGUE_CONSTANTS, "nhl", wide_constants)
    wide = win_prob_from_margin(1.0, league="nhl")
    assert abs(wide - 0.5) < abs(narrow - 0.5)


def test_predict_total_higher_for_high_xgf_teams():
    low = predict_total(2.0, 2.0, 2.0, 2.0, True, league="nhl")
    high = predict_total(3.2, 2.0, 3.2, 2.0, True, league="nhl")
    assert high > low


def test_predict_total_applies_total_scale_variance_correction():
    from sportsdataverse.nhl.nhl_prediction_constants import get_constants

    const = get_constants("nhl")
    eg_home, eg_away = expected_goals(2.8, 2.2, 2.5, 2.4, False, league="nhl")
    raw_total = eg_home + eg_away
    expected = const.avg_total_goals + const.total_scale * (raw_total - const.avg_total_goals)
    total = predict_total(2.8, 2.2, 2.5, 2.4, False, league="nhl")
    assert abs(total - expected) < 1e-9


def test_predict_total_identity_when_total_scale_is_one(monkeypatch):
    import sportsdataverse.nhl.nhl_prediction_constants as const_mod
    from dataclasses import replace

    identity_constants = replace(const_mod.LEAGUE_CONSTANTS["nhl"], total_scale=1.0)
    monkeypatch.setitem(const_mod.LEAGUE_CONSTANTS, "nhl", identity_constants)
    eg_home, eg_away = expected_goals(2.8, 2.2, 2.5, 2.4, False, league="nhl")
    total = predict_total(2.8, 2.2, 2.5, 2.4, False, league="nhl")
    assert abs(total - (eg_home + eg_away)) < 1e-9
