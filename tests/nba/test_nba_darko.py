"""Tests for AgingCurve + fit_aging_curve (Task 2) and Kalman filter (Task 3)."""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_darko import (
    AgingCurve,
    _forecast,
    _kalman_filter,
    fit_aging_curve,
)


def test_fit_aging_curve_recovers_planted_deltas() -> None:
    """Aging-curve meta-oracle: planted ±1.0/yr drift should be recovered within 0.4."""
    # planted true aging delta: +1.0/yr before age 27, -1.0/yr after
    rng = np.random.default_rng(0)
    rows, arows = [], []
    for pid in range(200):
        start_age = int(rng.integers(20, 34))
        skill = float(rng.normal(0, 3))
        for k, season in enumerate(range(2016, 2022)):
            age = start_age + k
            rows.append({"player_id": pid, "season": season, "rating": skill + rng.normal(0, 0.5)})
            arows.append({"player_id": pid, "season": season, "age": float(age)})
            skill += 1.0 if age < 27 else -1.0  # planted drift applied going forward
    curve = fit_aging_curve(pl.DataFrame(rows), pl.DataFrame(arows), smooth=1)
    assert curve.delta(23) > 0.5 and curve.delta(30) < -0.5  # recovers sign + rough magnitude
    assert abs(curve.delta(23) - 1.0) < 0.4 and abs(curve.delta(30) + 1.0) < 0.4


def test_aging_curve_delta_outside_range_is_zero() -> None:
    """delta() returns 0.0 for ages not in delta_by_age."""
    c = AgingCurve(delta_by_age={25: 0.5, 26: 0.3})
    assert c.delta(99) == 0.0 and c.delta(10) == 0.0


def test_kalman_recovers_latent_and_forecasts() -> None:
    """Kalman filter denoises a random-walk latent skill and forecasts sanely."""
    rng = np.random.default_rng(1)
    curve = AgingCurve(delta_by_age={a: 0.0 for a in range(20, 40)})  # no aging for this test
    true_skill = 5.0
    ratings, ages, weights, latents = [], [], [], []
    s = true_skill
    for k in range(8):
        ratings.append(s + rng.normal(0, 1.0))
        ages.append(25.0 + k)
        weights.append(1.0)
        latents.append(s)
        s += rng.normal(0, 0.3)  # small random-walk skill drift
    s_final, P_final, s_preds, innov_vars = _kalman_filter(
        np.array(ratings), np.array(ages), np.array(weights), curve, q=0.09, obs_base=1.0
    )
    # filtered final skill is closer to the true latent than the last noisy observation
    assert abs(s_final - latents[-1]) < abs(ratings[-1] - latents[-1]) + 0.5
    proj, sd = _forecast(s_final, P_final, ages[-1], curve, q=0.09)
    assert sd > 0 and abs(proj - latents[-1]) < 2.0
