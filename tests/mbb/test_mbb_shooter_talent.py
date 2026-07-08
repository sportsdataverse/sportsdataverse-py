"""Tests for shooter true-talent (EB-regressed over-expected) + the k fit."""

import numpy as np
import polars as pl

from sportsdataverse.mbb.mbb_shooter_talent import (
    fit_shrinkage_k,
    mbb_shooter_talent,
    talent_split_mse,
)


def _shots(shooter, n, made, xmake=0.5, pv=2):
    return pl.DataFrame(
        {
            "shooter_id": [shooter] * n,
            "made": [True] * made + [False] * (n - made),
            "point_value": [pv] * n,
            "xmake": [xmake] * n,
            "xpoints": [xmake * pv] * n,
        }
    ).with_columns(pl.col("point_value").cast(pl.Int8))


def test_talent_regression_by_sample_size():
    big = _shots("BIG", 400, 240)  # +0.10 over 0.5 expectation
    tiny = _shots("TINY", 5, 4)  # +0.30 over expectation
    out = mbb_shooter_talent(pl.concat([big, tiny]), k=200.0)
    b = out.filter(pl.col("shooter_id") == "BIG").row(0, named=True)
    t = out.filter(pl.col("shooter_id") == "TINY").row(0, named=True)
    assert abs(b["oe_pct"] - 0.10) < 1e-9
    assert abs(t["oe_pct"] - 0.30) < 1e-9
    # 400/(400+200)=2/3 of the signal survives; 5/205 nearly none
    assert abs(b["oe_pct_regressed"] - 0.10 * 400 / 600) < 1e-9
    assert t["oe_pct_regressed"] < 0.01
    # points over expected: made*pv - sum(xpoints) = 240*2 - 400*1.0 = 80
    assert abs(b["points_over_expected"] - 80.0) < 1e-9
    assert abs(b["poe_per_100"] - 20.0) < 1e-9
    assert out.schema["shooter_id"] == pl.Utf8 and out.schema["n_shots"] == pl.Int64


def test_talent_empty_input():
    out = mbb_shooter_talent(pl.DataFrame(schema={"shooter_id": pl.Utf8}))
    assert out.height == 0 and "oe_pct_regressed" in out.columns


def _synthetic_population(seed=0, n_shooters=300, shots_each=100):
    rng = np.random.default_rng(seed)
    talent = rng.normal(0.0, 0.05, n_shooters)
    frames = []
    for i, t in enumerate(talent):
        p = float(np.clip(0.5 + t, 0.01, 0.99))
        made = int(rng.binomial(shots_each, p))
        frames.append(_shots(f"S{i}", shots_each, made))
    return pl.concat(frames)


def test_fit_shrinkage_k_recovers_sane_value():
    scored = _synthetic_population()
    k = fit_shrinkage_k(scored, seed=0)
    assert 10.0 < k < 2000.0, k
    # regressed-with-k beats raw (k=0) on the held-out half
    assert talent_split_mse(scored, k=k, seed=0) < talent_split_mse(scored, k=1e-9, seed=0)
