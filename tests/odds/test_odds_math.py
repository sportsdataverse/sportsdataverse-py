"""Tests for the odds/market math utilities (WS5)."""

from __future__ import annotations

import numpy as np
import pytest

from sportsdataverse.odds.odds_math import (
    american_to_decimal,
    american_to_prob,
    build_correlation_matrix,
    calc_stats,
    combine_legs,
    decimal_to_american,
    fractional_to_american,
    frozen_dist,
    market_margin,
    no_vig_probs,
    parse_american,
    prob_over,
    prob_to_american,
    sample_using_copula,
)

# ---------------------------------------------------------------- conversion


def test_parse_american_forms() -> None:
    assert parse_american("+150") == 150
    assert parse_american("-110") == -110
    assert parse_american("EVEN") == 100
    assert parse_american(-200) == -200
    with pytest.raises(ValueError):
        parse_american("abc")
    with pytest.raises(ValueError):
        parse_american(0)


def test_conversion_round_trips() -> None:
    assert american_to_decimal(150) == pytest.approx(2.5)
    assert american_to_decimal(-110) == pytest.approx(1.909090, rel=1e-5)
    assert decimal_to_american(2.5) == 150
    assert decimal_to_american(1.909090909) == -110
    assert fractional_to_american("3/2") == 150
    assert fractional_to_american("1/4") == -400
    assert prob_to_american(0.6) == -150
    with pytest.raises(ValueError):
        decimal_to_american(0.9)
    with pytest.raises(ValueError):
        fractional_to_american("3-2")
    with pytest.raises(ValueError):
        prob_to_american(1.5)


def test_implied_prob_and_margin() -> None:
    assert american_to_prob(-110) == pytest.approx(110 / 210)
    assert american_to_prob(150) == pytest.approx(0.4)
    assert market_margin([-110, -110]) == pytest.approx(2 * (110 / 210) - 1)
    fair = no_vig_probs([-110, -110])
    assert fair == pytest.approx([0.5, 0.5])
    assert sum(no_vig_probs([-200, 150, 400])) == pytest.approx(1.0)


# ------------------------------------------------------------ distributions


def test_calc_stats_known_vector() -> None:
    s = calc_stats(np.array([10.0, 12.0, 12.0, 15.0]))
    assert s["pdf"][12.0] == pytest.approx(0.5)
    assert s["cdf"][12.0] == pytest.approx(0.75)
    assert s["mean"] == pytest.approx(12.25)
    assert s["median"] == pytest.approx(12.0)
    with pytest.raises(ValueError):
        calc_stats(np.array([1.0]), stats=("nope",))


def test_prob_over() -> None:
    assert prob_over(np.array([10, 12, 15, 20]), 12.5) == pytest.approx(0.5)
    assert np.isnan(prob_over(np.array([]), 1.0))


def test_combine_legs_same_world_correlation() -> None:
    pts = np.array([20.0, 30.0, 10.0])
    reb = np.array([10.0, 12.0, 4.0])
    pra = combine_legs([pts, reb])
    assert pra.tolist() == [30.0, 42.0, 14.0]
    assert combine_legs([pts, reb], operand="-").tolist() == [10.0, 18.0, 6.0]
    with pytest.raises(ValueError):
        combine_legs([])
    with pytest.raises(ValueError):
        combine_legs([pts, np.array([1.0])])
    with pytest.raises(ValueError):
        combine_legs([pts], operand="/")


def test_frozen_dist_families() -> None:
    assert frozen_dist("poisson", mu=5.5).mean() == pytest.approx(5.5)
    assert frozen_dist("norm", loc=10, scale=2).ppf(0.5) == pytest.approx(10.0)
    with pytest.raises(ValueError):
        frozen_dist("cauchy")


# -------------------------------------------------------------------- copula


def test_build_correlation_matrix() -> None:
    corr = build_correlation_matrix(3, {(0, 1): 0.35, (2, 1): -0.2})
    assert corr[0, 1] == corr[1, 0] == pytest.approx(0.35)
    assert corr[1, 2] == corr[2, 1] == pytest.approx(-0.2)
    assert np.allclose(np.diag(corr), 1.0)
    with pytest.raises(ValueError):
        build_correlation_matrix(2, {(0, 0): 0.5})
    with pytest.raises(ValueError):
        build_correlation_matrix(2, {(0, 1): 1.5})


def test_copula_marginals_and_correlation() -> None:
    rng = np.random.default_rng(7)
    corr = build_correlation_matrix(2, {(0, 1): 0.8})
    draws = sample_using_copula(
        corr,
        [frozen_dist("norm", loc=20.0, scale=5.0), frozen_dist("norm", loc=8.0, scale=2.0)],
        n=20_000,
        rng=rng,
    )
    assert draws.shape == (20_000, 2)
    # marginals preserved
    assert draws[:, 0].mean() == pytest.approx(20.0, abs=0.2)
    assert draws[:, 1].std() == pytest.approx(2.0, abs=0.1)
    # correlation transported through the copula
    observed = np.corrcoef(draws[:, 0], draws[:, 1])[0, 1]
    assert observed == pytest.approx(0.8, abs=0.05)


def test_copula_independent_and_discrete() -> None:
    rng = np.random.default_rng(11)
    corr = build_correlation_matrix(2, {})
    draws = sample_using_copula(
        corr,
        [frozen_dist("poisson", mu=22.0), frozen_dist("poisson", mu=8.5)],
        n=20_000,
        rng=rng,
    )
    # discrete ppf produced sane integer-valued samples
    assert np.all(draws >= 0)
    assert np.allclose(draws, np.round(draws))
    assert draws[:, 0].mean() == pytest.approx(22.0, abs=0.3)
    observed = np.corrcoef(draws[:, 0], draws[:, 1])[0, 1]
    assert abs(observed) < 0.05


def test_copula_shape_mismatch() -> None:
    with pytest.raises(ValueError, match="does not match"):
        sample_using_copula(np.eye(3), [frozen_dist("norm", loc=0, scale=1)], n=10)


def test_correlated_parlay_prices_higher_than_independent() -> None:
    rng = np.random.default_rng(3)
    dists = [frozen_dist("poisson", mu=22.0), frozen_dist("poisson", mu=8.5)]
    corr_hi = build_correlation_matrix(2, {(0, 1): 0.6})
    corr_no = build_correlation_matrix(2, {})
    hi = sample_using_copula(corr_hi, dists, n=30_000, rng=rng)
    no = sample_using_copula(corr_no, dists, n=30_000, rng=rng)
    p_hi = float(((hi[:, 0] > 24.5) & (hi[:, 1] > 9.5)).mean())
    p_no = float(((no[:, 0] > 24.5) & (no[:, 1] > 9.5)).mean())
    # positively correlated legs hit together more often — the whole point
    assert p_hi > p_no
