"""Unit tests for sportsdataverse.wexp.market (vig removal, blends, conversions).

Hand-computed values; real-captured-market fixtures are exercised in
test_market_oracle.py (Phase 0c).
"""

import math

import pytest

from sportsdataverse.wexp.market import (
    devig_multiplicative,
    devig_shin,
    logit_blend,
    moneyline_pair_prob,
    prob_from_american,
    prob_from_decimal,
    spread_to_prob,
)


def test_prob_from_american_favorite_and_dog():
    # -110: risk 110 to win 100 -> 110/210
    assert prob_from_american(-110) == pytest.approx(110 / 210)
    # +150: risk 100 to win 150 -> 100/250
    assert prob_from_american(150) == pytest.approx(100 / 250)


def test_prob_from_decimal():
    assert prob_from_decimal(2.0) == pytest.approx(0.5)
    assert prob_from_decimal(1.25) == pytest.approx(0.8)


def test_devig_multiplicative_sums_to_one_and_preserves_ratio():
    p = devig_multiplicative([0.55, 0.55])
    assert sum(p) == pytest.approx(1.0)
    assert p[0] == pytest.approx(0.5)
    p = devig_multiplicative([0.6, 0.5])
    assert sum(p) == pytest.approx(1.0)
    assert p[0] / p[1] == pytest.approx(0.6 / 0.5)


def test_devig_shin_sums_to_one_and_shrinks_longshot():
    raw = [prob_from_american(-450), prob_from_american(400)]  # heavy fav + longshot w/ vig
    mult = devig_multiplicative(raw)
    shin = devig_shin(raw)
    assert sum(shin) == pytest.approx(1.0, abs=1e-9)
    # Shin attributes overround disproportionately to the longshot. STRICT
    # inequality: equality would mean the silent multiplicative fallback ran
    # instead of the Shin solver (observed real gap on this input: 0.00552).
    assert shin[1] < mult[1] - 1e-4
    # And with zero vig, Shin == raw.
    novig = devig_shin([0.7, 0.3])
    assert novig[0] == pytest.approx(0.7, abs=1e-9)


def test_spread_to_prob_home_positive_convention():
    # spread is the expected HOME margin: positive = home favored.
    assert spread_to_prob(0.0, sigma=13.45) == pytest.approx(0.5)
    p_fav = spread_to_prob(7.0, sigma=13.45)
    assert 0.5 < p_fav < 1.0
    assert spread_to_prob(-7.0, sigma=13.45) == pytest.approx(1 - p_fav)


def test_logit_blend_endpoints_and_symmetry():
    assert logit_blend(0.6, 0.7, weight_a=1.0) == pytest.approx(0.6)
    assert logit_blend(0.6, 0.7, weight_a=0.0) == pytest.approx(0.7)
    # blending in logit space: blend of p and p is p
    assert logit_blend(0.42, 0.42, weight_a=0.7) == pytest.approx(0.42)
    # 70/30 blend sits between the inputs
    b = logit_blend(0.6, 0.7, weight_a=0.7)
    assert 0.6 < b < 0.7
    # verify it is the logit-space blend, not the linear one
    expected = 1 / (1 + math.exp(-(0.7 * math.log(0.6 / 0.4) + 0.3 * math.log(0.7 / 0.3))))
    assert b == pytest.approx(expected)


def test_moneyline_pair_prob_multiplicative_default():
    p = moneyline_pair_prob(-150, 130)
    raw_h, raw_a = prob_from_american(-150), prob_from_american(130)
    assert p == pytest.approx(raw_h / (raw_h + raw_a))
    p_shin = moneyline_pair_prob(-150, 130, method="shin")
    assert 0.5 < p_shin < 1.0
    # strict: the Shin path must actually differ from the multiplicative one
    assert p_shin != p
    with pytest.raises(ValueError):
        moneyline_pair_prob(-150, 130, method="nope")


def test_fair_win_probs_two_way_and_three_way():
    from sportsdataverse.wexp.market import fair_win_probs

    probs, note = fair_win_probs(0.55, 0.50)
    assert note is None and probs == pytest.approx([0.55 / 1.05, 0.50 / 1.05])
    probs, note = fair_win_probs(0.45, 0.30, 0.30, draw_possible=True)
    assert note is None and sum(probs) == pytest.approx(1.0) and len(probs) == 3


def test_fair_win_probs_refuses_an_unpriced_draw():
    """A two-way de-vig where a draw is possible would push the draw's share onto both teams."""
    from sportsdataverse.wexp.market import fair_win_probs

    probs, note = fair_win_probs(0.45, 0.30, draw_possible=True)
    assert probs is None and "draw is possible" in note
    assert fair_win_probs(None, 0.5) == (None, "missing home price")
    assert fair_win_probs(0.5, None) == (None, "missing away price")
    assert fair_win_probs(1.0, 0.1)[0] is None
    with pytest.raises(ValueError, match="unknown devig method"):
        fair_win_probs(0.5, 0.5, method="power")
