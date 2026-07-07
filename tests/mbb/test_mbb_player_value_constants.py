"""Tests for the player-value spine harness (``mbb_player_value_constants``)."""

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_player_value_constants import (
    as_of_season_split,
    bootstrap_ari,
    get_player_value_constants,
    kmeans_fit,
    load_artifact,
    logistic_fit,
    mae,
    player_per100_features,
    ridge_cv_lambda,
    ridge_fit,
    roc_auc,
    save_artifact,
    spearman_corr,
)


def test_spearman_antimonotonic_is_minus_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 5.0, 2.0, 1.0])
    assert abs(spearman_corr(a, b) + 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_roc_auc_perfect_and_random():
    y = np.array([0, 0, 1, 1])
    assert abs(roc_auc(y, np.array([0.1, 0.2, 0.8, 0.9])) - 1.0) < 1e-9
    assert abs(roc_auc(y, np.array([0.9, 0.8, 0.2, 0.1])) - 0.0) < 1e-9


def test_as_of_split_excludes_target_and_later():
    df = pl.DataFrame({"season": [2021, 2022, 2023], "x": [1, 2, 3]})
    out = as_of_season_split(df, 2023)
    assert out["season"].to_list() == [2021, 2022]


def test_per100_features_hand_computed():
    # one player, known counts so every rate is hand-computable
    ss = pl.DataFrame(
        {
            "player_id": ["1"],
            "season": [2024],
            "team_id": ["10"],
            "minutes": [200.0],
            "field_goals_made": [90.0],
            "field_goals_attempted": [100.0],
            "three_point_field_goals_made": [20.0],
            "free_throws_attempted": [50.0],
            "turnovers": [30.0],
            "points": [260.0],
            "fga_rim": [40.0],
            "fga_mid": [25.0],
            "fga_three": [35.0],
            "offensive_rebounds": [20.0],
            "defensive_rebounds": [60.0],
            "assists": [80.0],
            "blocks": [10.0],
            "steals": [15.0],
        }
    )
    out = player_per100_features(ss)
    r = out.row(0, named=True)
    assert r["player_id"] == "1" and out.schema["player_id"] == pl.Utf8
    # ts% = pts / (2*(fga + 0.44*fta)) = 260 / (2*(100+22))
    assert abs(r["ts_pct"] - 260.0 / (2 * (100.0 + 0.44 * 50.0))) < 1e-6
    # efg% = (fgm + 0.5*3pm)/fga = (90+10)/100
    assert abs(r["efg_pct"] - 1.0) < 1e-6
    assert abs(r["three_share"] - 0.35) < 1e-6
    assert abs(r["ftr"] - 0.5) < 1e-6
    assert abs(r["pts_per100"] - 100.0 * 260.0 / 200.0) < 1e-6


def test_per100_features_empty_input_schema():
    out = player_per100_features(pl.DataFrame())
    assert out.height == 0
    assert out.schema["player_id"] == pl.Utf8
    assert "ts_pct" in out.columns


def test_get_player_value_constants_unknown_raises():
    with pytest.raises(ValueError):
        get_player_value_constants("nba")
    assert get_player_value_constants("mens").bundle_prefix == "mbb"
    assert get_player_value_constants("womens").bundle_prefix == "wbb"


def test_ridge_recovers_line_at_low_lambda():
    rng = np.random.default_rng(0)
    X = rng.normal(size=(200, 2))
    beta = np.array([1.0, -2.0, 0.5])  # intercept, b1, b2
    y = beta[0] + X @ beta[1:]
    est = ridge_fit(X, y, lam=1e-6)
    assert np.allclose(est, beta, atol=1e-2)


def test_ridge_cv_prefers_low_lambda_on_noiseless_data():
    rng = np.random.default_rng(2)
    X = rng.normal(size=(120, 2))
    y = 2.0 + X @ np.array([1.0, -1.0])
    groups = np.repeat([2021, 2022, 2023], 40)
    lam = ridge_cv_lambda(X, y, groups, [0.001, 1000.0])
    assert lam == 0.001


def test_kmeans_separates_two_blobs():
    rng = np.random.default_rng(1)
    X = np.vstack([rng.normal(-5, 0.3, (50, 2)), rng.normal(5, 0.3, (50, 2))])
    centers, labels = kmeans_fit(X, k=2, seed=0)
    assert len(set(labels[:50])) == 1 and len(set(labels[50:])) == 1
    assert labels[0] != labels[50]
    assert centers.shape == (2, 2)


def test_logistic_separable():
    X = np.array([[-2.0], [-1.0], [1.0], [2.0]])
    y = np.array([0, 0, 1, 1])
    coef = logistic_fit(X, y, lam=1e-3)
    p = 1 / (1 + np.exp(-(coef[0] + X[:, 0] * coef[1])))
    assert p[0] < 0.5 and p[-1] > 0.5


def test_bootstrap_ari_stable_blobs_near_one():
    rng = np.random.default_rng(3)
    X = np.vstack([rng.normal(-5, 0.3, (40, 2)), rng.normal(5, 0.3, (40, 2))])
    score = bootstrap_ari(lambda Z: kmeans_fit(Z, 2, seed=0), X, n_boot=5, seed=0)
    assert score > 0.95


def test_artifact_save_load_roundtrip(tmp_path, monkeypatch):
    import sportsdataverse.mbb.mbb_player_value_constants as pvc

    monkeypatch.setattr(pvc, "_models_dir_file", lambda name: tmp_path / f"{name}.json")
    payload = {"league": "mens", "coef": [1.0, 2.0], "lambda": 3.0}
    save_artifact("t_roundtrip", payload)
    assert load_artifact("t_roundtrip") == payload
