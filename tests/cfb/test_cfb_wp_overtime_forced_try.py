"""The forced two-point try after an overtime touchdown.

sdv-internal-refs rules/cfb.yaml: ``cfb-1996-overtime-introduced`` (third overtime on),
``cfb-2019-overtime-shootout-from-fifth`` (still the third; the fifth on is a shootout),
``cfb-2021-overtime-shootout-from-third`` (the second; the third on is a shootout).

The closed forms use the card's own ``p_extra_point`` / ``p_two_point`` and the drive
model's touchdown probability from the 25; nothing is refitted.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_two_point import TWO_PT_MODEL_AVAILABLE, get_2pt_probs
from sportsdataverse.cfb.cfb_wp_overtime import (
    OT_TWO_POINT_FORCED_FROM,
    _p25,
    _params,
    ot_forced_two_point,
    ot_live_wp,
    ot_possession_over_wp,
    ot_touchdown_wp,
    tie_value,
)
from tests.cfb.test_cfb_2pt import _row
from tests.cfb.test_cfb_wp_endgame import HOLDOUT, _brier, _logloss


def test_rule_table_is_the_catalogue():
    assert OT_TWO_POINT_FORCED_FROM == ((1996, 3), (2019, 3), (2021, 2))


@pytest.mark.parametrize(
    ("season", "period", "forced"),
    [
        (2018, 5, False),  # first overtime: a kick under every rule
        (2018, 6, False),  # second overtime, pre-2021: kick allowed
        (2018, 7, True),  # third overtime, pre-2021: forced
        (2004, 9, True),  # original 1996 rule: every overtime from the third
        (2019, 6, False),  # 2019-20: unchanged for the second
        (2019, 7, True),  # 2019-20: forced from the third
        (2020, 8, True),  # 2019-20: the fourth
        (2021, 5, False),  # 2021+: first overtime still kicks
        (2021, 6, True),  # 2021+: forced from the second
        (2025, 6, True),
        (2025, 4, False),  # regulation
        (1995, 7, False),  # before overtime existed
    ],
)
def test_forced_two_point_by_season_and_period(season, period, forced):
    assert ot_forced_two_point([season], [period]).tolist() == [forced]


def test_unknown_season_or_period_allows_the_kick():
    assert ot_forced_two_point([np.nan, 2024], [6, np.nan]).tolist() == [False, False]


def test_second_team_trailing_by_six_hand_check():
    # The second team, down 6, scores. Where the kick is allowed it takes the better of the
    # kick (0.92 to win, else overtime at tie) and two; where the try is forced it converts at
    # p_two_point or goes to another period. tie = 0.5: 0.96 against 0.73.
    p = _params()
    kick = p["p_xp"] * 1.0 + (1 - p["p_xp"]) * 0.5
    two = p["p_2pt"] * 1.0 + (1 - p["p_2pt"]) * 0.5
    assert kick == pytest.approx(0.96) and two == pytest.approx(0.73)
    assert ot_touchdown_wp([-6], [0.5], [True]) == pytest.approx([max(kick, two)])
    assert ot_touchdown_wp([-6], [0.5], [True], [True]) == pytest.approx([two])


def test_first_team_up_six_after_a_failed_try_hand_check():
    # The first team's possession ends at +6 (touchdown, try failed). The other team answers
    # from the 25: a touchdown (drive-model probability a) brings it to level, and ITS try is
    # the one the period's rule governs; a field goal or nothing loses. tie = 0.3.
    a = _p25()[0]
    p = _params()
    t = 0.3
    other_tie = 1.0 - t
    kick_try = max(p["p_xp"] + (1 - p["p_xp"]) * other_tie, p["p_2pt"] + (1 - p["p_2pt"]) * other_tie)
    two_try = p["p_2pt"] + (1 - p["p_2pt"]) * other_tie
    assert ot_possession_over_wp([6], [t], [False]) == pytest.approx([1.0 - a * kick_try])
    assert ot_possession_over_wp([6], [t], [False], [True]) == pytest.approx([1.0 - a * two_try])
    assert 1.0 - a * two_try > 1.0 - a * kick_try  # a forced try is worth less to the team answering


def test_forced_flag_changes_only_the_periods_it_names():
    # The second team, tied, with the ball: a touchdown wins outright under both rules, so
    # the forced flag must not move it; the first team, tied, is the mirror case.
    t = tie_value([3.0])
    for second in (True, False):
        base = ot_live_wp([0], [1], [10], [25], t, [second])
        assert ot_live_wp([0], [1], [10], [25], t, [second], [False]) == pytest.approx(base)


def test_real_2022_2025_overtime_holdout_is_better_calibrated_with_the_rule():
    # tests/cfb/fixtures/wp_overtime/README.md: 2022-25 overtime scrimmage snaps. Every one is
    # 2021+, so the rule forces the try from period 6 (the second overtime): 199 rows.
    f = pl.read_parquet(HOLDOUT / "overtime_2022_2025.parquet")
    y = f["won"].cast(pl.Float64).to_numpy()
    args = (
        f["pos_score_diff_start"].to_numpy(),
        f["start.down"].to_numpy(),
        f["start.distance"].to_numpy(),
        f["start.yardsToEndzone"].to_numpy(),
        tie_value(f["start.pos_team_spread"].to_numpy()),
        f["ot_second"].to_numpy(),
    )
    forced = ot_forced_two_point(f["season"].to_numpy(), f["period"].to_numpy())
    assert forced.sum() == (f["period"] >= 6).sum() >= 150
    kick, rule = ot_live_wp(*args), ot_live_wp(*args, forced)
    assert np.array_equal(kick[~forced], rule[~forced])  # the first overtime is untouched
    assert (np.abs(rule[forced] - kick[forced]) > 1e-9).sum() >= 100
    # Observed: Brier 0.2290 -> 0.2133, log loss 0.658 -> 0.626 on the forced rows.
    assert _brier(rule[forced], y[forced]) < _brier(kick[forced], y[forced]) - 0.01
    assert _logloss(rule[forced], y[forced]) < _logloss(kick[forced], y[forced]) - 0.02


@pytest.mark.skipif(not TWO_PT_MODEL_AVAILABLE, reason="two_pt_model not bundled")
def test_two_point_surface_has_no_kick_in_a_forced_period():
    # Second overtime, 2024: the try is forced, so there is no extra point to recommend.
    # The same state in the first overtime still chooses.
    rows = pl.DataFrame(
        [
            _row(sd=0, tsr=0, adj=0, period=6, season=2024),
            _row(sd=0, tsr=0, adj=0, period=5, season=2024),
            _row(sd=0, tsr=0, adj=0, period=6, season=2018),
        ]
    )
    out = get_2pt_probs(rows)
    assert np.isnan(out.loc[0, "xp_wp"]) and np.isnan(out.loc[0, "two_pt_wp_diff"])
    assert out.loc[0, "two_pt_recommendation"] == "go_for_2"
    assert np.isfinite(out.loc[0, "two_pt_wp"])
    for i in (1, 2):
        assert np.isfinite(out.loc[i, "xp_wp"]) and out.loc[i, "two_pt_recommendation"] in {"go_for_2", "kick_xp"}
