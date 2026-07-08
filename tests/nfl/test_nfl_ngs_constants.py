"""Unit tests for the NGS shrinkage/metric engine (offline, synthetic)."""

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_ngs_constants import (
    empirical_bayes_shrink,
    expected_separation_ridge,
    mae,
    next_season_stability,
    spearman_corr,
)


def test_high_n_barely_shrinks():
    x = np.array([2.0, -2.0, 1.0, -1.0])
    n = np.array([1e6, 1e6, 1e6, 1e6])
    shrunk, rel = empirical_bayes_shrink(x, n)
    assert np.all(rel > 0.999)
    assert np.allclose(shrunk, x, atol=1e-3)


def test_low_n_shrinks_toward_mean_and_pulls_inward():
    x = np.array([10.0, -10.0, 0.2, -0.2])
    n = np.array([2.0, 2.0, 500.0, 500.0])
    shrunk, rel = empirical_bayes_shrink(x, n)
    mu = float(np.sum(n * x) / n.sum())
    # every row pulled toward mu (never past it), i.e. |shrunk-mu| <= |x-mu|
    assert np.all(np.abs(shrunk - mu) <= np.abs(x - mu) + 1e-9)
    # low-n extremes shrink harder than high-n rows
    assert rel[0] < rel[2]


def test_reliability_monotone_in_n():
    # Non-degenerate panel (all-equal x would give sigma2 == 0 and rel == 1
    # everywhere): low-n rows are noisier, so the fitted sigma2 > 0 and
    # reliability must rise with n.
    x = np.array([10.0, -10.0, 1.0, -1.0, 0.1, -0.1])
    n = np.array([2.0, 2.0, 50.0, 50.0, 5000.0, 5000.0])
    _, rel = empirical_bayes_shrink(x, n)
    assert rel[0] < rel[2] < rel[4]


def test_empty_input_returns_empty():
    shrunk, rel = empirical_bayes_shrink(np.array([]), np.array([]))
    assert shrunk.size == 0 and rel.size == 0


def test_ridge_recovers_linear_fit():
    rng = np.random.default_rng(0)
    feats = rng.normal(size=(50, 2))
    y = 1.0 + 2.0 * feats[:, 0] - 0.5 * feats[:, 1]
    expected, beta = expected_separation_ridge(y, feats, np.ones(50), lam=1e-8)
    assert np.allclose(expected, y, atol=1e-6)
    assert np.allclose(beta, [1.0, 2.0, -0.5], atol=1e-6)


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_next_season_stability_perfect():
    cur = pl.DataFrame({"player_gsis_id": ["a", "b", "c"], "v": [1.0, 2.0, 3.0]})
    nxt = pl.DataFrame({"player_gsis_id": ["a", "b", "c"], "v2": [2.0, 4.0, 6.0]})
    assert abs(next_season_stability(cur, nxt, "player_gsis_id", "v", "v2") - 1.0) < 1e-9


def test_next_season_stability_too_few_rows_is_nan():
    cur = pl.DataFrame({"player_gsis_id": ["a", "b"], "v": [1.0, 2.0]})
    nxt = pl.DataFrame({"player_gsis_id": ["a"], "v2": [2.0]})
    assert np.isnan(next_season_stability(cur, nxt, "player_gsis_id", "v", "v2"))


def test_shrink_with_known_sigma2_exact():
    # d2 = [1, 1], sigma2/n = 0.5 -> tau2 = mean(1 - 0.5) = 0.5 -> rel = 0.5
    x = np.array([2.0, 0.0])
    n = np.array([10.0, 10.0])
    shrunk, rel = empirical_bayes_shrink(x, n, prior_mean=1.0, sigma2=5.0)
    assert np.allclose(rel, [0.5, 0.5])
    assert np.allclose(shrunk, [1.5, 0.5])


def test_weekly_sigma2_pooled_exact():
    from sportsdataverse.nfl.nfl_ngs_constants import weekly_sigma2

    weekly = pl.DataFrame(
        {
            "player_gsis_id": ["a", "a", "b", "b"],
            "v": [1.0, 3.0, 0.0, 4.0],
            "n": [1.0, 1.0, 3.0, 1.0],
        }
    )
    # a: xbar=2, ss=2, dof=1; b: xbar=1, ss=3*1+1*9=12, dof=1 -> 14/2 = 7
    assert abs(weekly_sigma2(weekly, "v", "n") - 7.0) < 1e-9


def test_weekly_sigma2_unidentified_returns_none():
    from sportsdataverse.nfl.nfl_ngs_constants import weekly_sigma2

    single = pl.DataFrame({"player_gsis_id": ["a", "b"], "v": [1.0, 2.0], "n": [5.0, 5.0]})
    assert weekly_sigma2(single, "v", "n") is None
    assert weekly_sigma2(pl.DataFrame(), "v", "n") is None
