"""Tests for AgingCurve + fit_aging_curve (Task 2 of NBA DARKO sub-project)."""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_darko import AgingCurve, fit_aging_curve


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
