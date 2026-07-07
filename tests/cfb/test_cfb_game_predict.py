"""Closed-form pregame predictor tests (T2.1 Task 2.1).

Every assertion is hand-computable from the seeded ``CFB_CONSTANTS["modern"]``
so the math is pinned independently of any fitted values.
"""

from __future__ import annotations

import math

from scipy.stats import norm

from sportsdataverse.cfb.cfb_game_predict import (
    predict_margin,
    predict_total,
    win_prob_from_margin,
)
from sportsdataverse.cfb.cfb_prediction_constants import get_constants

_C = get_constants("modern")


def test_predict_margin_neutral_carries_no_hfa() -> None:
    """On a neutral field the margin is exactly the rating differential."""
    assert predict_margin(0.30, 0.10, neutral=True) == 0.30 - 0.10


def test_predict_margin_home_adds_hfa() -> None:
    """A home game adds the era HFA to the rating differential."""
    assert predict_margin(0.30, 0.10, neutral=False) == (0.30 - 0.10) + _C.hfa


def test_win_prob_equal_strength_neutral_is_half() -> None:
    """Equal teams on a neutral field are a coin flip."""
    m = predict_margin(0.20, 0.20, neutral=True)
    assert win_prob_from_margin(m) == 0.5


def test_win_prob_symmetric_home_beats_half() -> None:
    """Equal teams, home field -> win prob = Phi(hfa/sigma) > 0.5."""
    m = predict_margin(0.20, 0.20, neutral=False)
    p = win_prob_from_margin(m)
    assert p == float(norm.cdf(_C.hfa / _C.margin_sd))
    assert p > 0.5


def test_win_prob_is_monotonic_in_margin() -> None:
    """A bigger expected margin never lowers the win probability."""
    assert win_prob_from_margin(-14.0) < win_prob_from_margin(0.0) < win_prob_from_margin(14.0)


def test_predict_total_offense_beats_defense() -> None:
    """Two strong offenses outscore two strong defenses.

    Strong offense = high ``adj_off_epa``; strong defense = low (very negative)
    ``adj_def_epa`` (fewer EPA allowed). The offensive matchup must yield a
    larger expected total than the defensive one.
    """
    off_heavy = predict_total(0.30, 0.05, 0.30, 0.05)  # both offenses elite, defenses meh
    def_heavy = predict_total(-0.05, -0.30, -0.05, -0.30)  # both defenses elite, offenses meh
    assert off_heavy > def_heavy


def test_predict_total_is_finite_and_positive() -> None:
    """A league-average matchup produces a sane, positive total."""
    t = predict_total(0.0, 0.0, 0.0, 0.0)
    assert math.isfinite(t)
    assert t > 0.0
