"""Tests for the umpire strike-zone logistic (synthetic zone-sanity check)."""

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_umpire_zone import fit_zone_model, mlb_umpire_bias, mlb_umpire_called_strike_prob


def _synth(n=4000, seed=0):
    rng = np.random.default_rng(seed)
    px = rng.uniform(-1.5, 1.5, n)
    pz = rng.uniform(1.0, 4.0, n)
    sz_top = np.full(n, 3.4)
    sz_bot = np.full(n, 1.6)
    z_norm = (pz - sz_bot) / (sz_top - sz_bot)
    in_zone = (np.abs(px) < 0.83) & (z_norm > 0) & (z_norm < 1)
    called = (rng.random(n) < np.where(in_zone, 0.92, 0.06)).astype(int)
    desc = np.where(called == 1, "called_strike", "ball")
    return pl.DataFrame(
        {
            "plate_x": px,
            "plate_z": pz,
            "sz_top": sz_top,
            "sz_bot": sz_bot,
            "description": desc,
            "pitch_type": ["FF"] * n,
            "umpire_id": ["U1"] * n,
        }
    )


def test_center_high_far_low():
    p = _synth()
    prob = mlb_umpire_called_strike_prob(p)
    df = p.hstack(prob)
    center = df.filter((pl.col("plate_x").abs() < 0.1) & (pl.col("plate_z").is_between(2.4, 2.6)))[
        "called_strike_prob"
    ].mean()
    far = df.filter(pl.col("plate_x").abs() > 1.3)["called_strike_prob"].mean()
    assert center >= 0.9 and far <= 0.1


def test_bias_flags_generous_umpire():
    p1 = _synth(seed=0).with_columns(pl.lit("U_neutral").alias("umpire_id"))
    p2 = _synth(seed=1).with_columns(pl.lit("U_generous").alias("umpire_id"))
    # Flip a chunk of U_generous's "ball" calls (outside the zone) to
    # "called_strike" -- a strike-generous umpire calling more strikes than
    # the zone-based model expects.
    rng = np.random.default_rng(2)
    flip = (p2["description"] == "ball") & pl.Series(rng.random(p2.height) < 0.3)
    p2 = p2.with_columns(
        pl.when(flip).then(pl.lit("called_strike")).otherwise(pl.col("description")).alias("description")
    )
    both = pl.concat([p1, p2])

    model = fit_zone_model(both)
    bias = mlb_umpire_bias(both, model=model)
    generous = bias.filter(pl.col("umpire_id") == "U_generous")["bias"][0]
    neutral = bias.filter(pl.col("umpire_id") == "U_neutral")["bias"][0]
    assert generous > neutral
    assert generous > 0
