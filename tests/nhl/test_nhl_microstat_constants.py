from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nhl.nhl_microstat_constants import (
    fit_shot_xg,
    get_constants,
    rel_error,
    spearman_corr,
    split_half_stability,
)


def test_constants_both_leagues_resolve() -> None:
    for lg in ("nhl", "pwhl"):
        c = get_constants(lg)
        assert c.pp_goal_value > 0
        assert set(c.edge_component_weights) >= {"top_speed", "distance_km", "speed_bursts_20"}


def test_unknown_league_raises() -> None:
    with pytest.raises(ValueError):
        get_constants("khl")


def test_spearman_monotonic_is_one() -> None:
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([9.0, 20.0, 30.0, 44.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_rel_error_manual() -> None:
    assert abs(rel_error(1.1, 1.0) - 0.1) < 1e-9


def test_split_half_stability_perfect() -> None:
    ev = pl.DataFrame(
        {
            "player_id": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "half": [0, 1, 0, 1, 0, 1, 0, 1],
            "num": [1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0],
            "den": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        }
    )
    s = split_half_stability(ev, id_col="player_id", half_col="half", num_col="num", den_col="den")
    assert abs(s - 1.0) < 1e-9


def test_fit_shot_xg_ranks_and_calibrates() -> None:
    rng = np.random.default_rng(0)
    n = 4000
    x = rng.uniform(30, 89, n)  # closer to 89 = closer to net
    y = rng.uniform(-40, 40, n)
    dist = np.sqrt((89 - x) ** 2 + y**2)
    p = 1 / (1 + np.exp((dist - 25) / 6))
    goal = rng.random(n) < p
    pbp = pl.DataFrame(
        {
            "type_desc_key": np.where(goal, "goal", "shot-on-goal"),
            "x_coord": x,
            "y_coord": y,
            "shot_type": ["wrist"] * n,
        }
    )
    m = fit_shot_xg(pbp)
    xg = m.predict(pbp).to_numpy()
    close = pbp["x_coord"].to_numpy() > 80
    assert xg[close].mean() > xg[~close].mean()
    assert abs(xg.sum() - goal.sum()) / goal.sum() < 0.05


def test_fit_shot_xg_empty_and_small_fallback() -> None:
    empty = pl.DataFrame(schema={"type_desc_key": pl.Utf8, "x_coord": pl.Float64, "y_coord": pl.Float64})
    m = fit_shot_xg(empty)
    assert m.predict(empty).len() == 0

    small = pl.DataFrame(
        {
            "type_desc_key": ["goal", "shot-on-goal", "shot-on-goal"],
            "x_coord": [85.0, 40.0, 50.0],
            "y_coord": [0.0, 10.0, -5.0],
            "shot_type": ["wrist", "wrist", "wrist"],
        }
    )
    m_small = fit_shot_xg(small)
    preds = m_small.predict(small)
    assert preds.len() == 3
    assert preds.n_unique() == 1  # constant fallback rate


def test_fit_shot_xg_predict_null_shot_type_no_crash() -> None:
    # blocked-shot events carry a null shot_type in real captures; predict()
    # must not propagate that into a NaN feature matrix (regression test for
    # a bug found fitting faceoff-zone weights on the real corpus, Task 1.4).
    rng = np.random.default_rng(1)
    n = 300
    x = rng.uniform(30, 89, n)
    y = rng.uniform(-40, 40, n)
    pbp = pl.DataFrame(
        {
            "type_desc_key": ["goal"] * (n // 2) + ["shot-on-goal"] * (n - n // 2),
            "x_coord": x,
            "y_coord": y,
            "shot_type": ["wrist"] * n,
        }
    )
    model = fit_shot_xg(pbp)
    blocked = pl.DataFrame(
        {
            "type_desc_key": ["blocked-shot"] * 5,
            "x_coord": [60.0, 55.0, 70.0, 65.0, 80.0],
            "y_coord": [5.0, -5.0, 10.0, 0.0, -10.0],
            "shot_type": [None] * 5,
        }
    )
    preds = model.predict(blocked)
    assert preds.len() == 5
    assert not preds.is_nan().any()
