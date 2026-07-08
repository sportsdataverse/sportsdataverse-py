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


def test_aggregate_player_seasons_sums_and_shot_shares(monkeypatch):
    import sportsdataverse.mbb.mbb_player_value_constants as pvc

    box = pl.DataFrame(
        {
            "athlete_id": [7, 7, 8],
            "athlete_display_name": ["A Guard", "A Guard", "B Big"],
            "season": [2025, 2025, 2025],
            "team_id": [10, 10, 10],
            "minutes": [30.0, 20.0, 25.0],
            "field_goals_made": [5, 3, 4],
            "field_goals_attempted": [10, 6, 8],
            "three_point_field_goals_made": [2, 1, 0],
            "three_point_field_goals_attempted": [5, 3, 0],
            "free_throws_made": [2, 2, 2],
            "free_throws_attempted": [4, 2, 4],
            "offensive_rebounds": [1, 0, 4],
            "defensive_rebounds": [3, 2, 6],
            "assists": [6, 4, 1],
            "steals": [2, 1, 0],
            "blocks": [0, 0, 3],
            "turnovers": [2, 1, 2],
            "points": [14, 9, 10],
        }
    )
    # REAL 2025 release vocabulary: type_text is only
    # JumpShot/LayUpShot/DunkShot/TipShot/MadeFreeThrow -- threes are
    # distinguished by score_value (populated on misses too); free throws
    # must NOT count as field-goal attempts.
    shots = pl.DataFrame(
        {
            "athlete_id_1": [7, 7, 7, 7, 8, 8, 8],
            "season": [2025] * 7,
            "type_text": [
                "JumpShot",
                "JumpShot",
                "LayUpShot",
                "MadeFreeThrow",
                "DunkShot",
                "TipShot",
                "MadeFreeThrow",
            ],
            "score_value": [3, 2, 2, 1, 2, 2, 1],
        }
    )
    monkeypatch.setattr(pvc, "_load_player_box", lambda seasons, league: box)
    monkeypatch.setattr(pvc, "_load_shots", lambda seasons, league: shots)
    out = pvc.aggregate_player_seasons([2025])
    assert out.schema["player_id"] == pl.Utf8
    a = out.filter(pl.col("player_id") == "7").row(0, named=True)
    assert a["minutes"] == 50.0 and a["field_goals_attempted"] == 16.0 and a["points"] == 23.0
    assert (a["fga_rim"], a["fga_mid"], a["fga_three"]) == (1.0, 1.0, 1.0)
    b = out.filter(pl.col("player_id") == "8").row(0, named=True)
    assert (b["fga_rim"], b["fga_mid"], b["fga_three"]) == (2.0, 0.0, 0.0)
    # composes with the feature builder
    feats = player_per100_features(out)
    assert feats.height == 2


def test_aggregate_player_seasons_mixed_shot_eras(monkeypatch):
    """A season absent from the shots release keeps box-derived splits while
    the covered season uses classified shots (wbb: shots floor at 2026)."""
    import sportsdataverse.mbb.mbb_player_value_constants as pvc

    box = pl.DataFrame(
        {
            "athlete_id": [7, 7],
            "athlete_display_name": ["A Guard", "A Guard"],
            "season": [2025, 2026],
            "team_id": [10, 10],
            "minutes": [30.0, 30.0],
            "field_goals_made": [5, 5],
            "field_goals_attempted": [10, 10],
            "three_point_field_goals_made": [2, 2],
            "three_point_field_goals_attempted": [4, 4],
            "free_throws_made": [2, 2],
            "free_throws_attempted": [2, 2],
            "offensive_rebounds": [1, 1],
            "defensive_rebounds": [3, 3],
            "assists": [6, 6],
            "steals": [2, 2],
            "blocks": [0, 0],
            "turnovers": [2, 2],
            "points": [14, 14],
        }
    )
    shots = pl.DataFrame(
        {
            "athlete_id_1": [7, 7],
            "season": [2026, 2026],
            "type_text": ["LayUpShot", "JumpShot"],
            "score_value": [2, 3],
        }
    )
    monkeypatch.setattr(pvc, "_load_player_box", lambda seasons, league: box)
    monkeypatch.setattr(pvc, "_load_shots", lambda seasons, league: shots)
    out = pvc.aggregate_player_seasons([2025, 2026])
    a25 = out.filter(pl.col("season") == 2025).row(0, named=True)
    a26 = out.filter(pl.col("season") == 2026).row(0, named=True)
    # 2025 (uncovered): box fallback -- three = 3PA, mid = fga - 3pa, rim = 0
    assert (a25["fga_rim"], a25["fga_mid"], a25["fga_three"]) == (0.0, 6.0, 4.0)
    # 2026 (covered): classified shots
    assert (a26["fga_rim"], a26["fga_mid"], a26["fga_three"]) == (1.0, 0.0, 1.0)


def test_aggregate_player_seasons_no_shots_data(monkeypatch):
    import sportsdataverse.mbb.mbb_player_value_constants as pvc

    box = pl.DataFrame(
        {
            "athlete_id": [7],
            "athlete_display_name": ["A Guard"],
            "season": [2019],
            "team_id": [10],
            "minutes": [30.0],
            "field_goals_made": [5],
            "field_goals_attempted": [10],
            "three_point_field_goals_made": [2],
            "three_point_field_goals_attempted": [5],
            "free_throws_made": [2],
            "free_throws_attempted": [4],
            "offensive_rebounds": [1],
            "defensive_rebounds": [3],
            "assists": [6],
            "steals": [2],
            "blocks": [0],
            "turnovers": [2],
            "points": [14],
        }
    )
    monkeypatch.setattr(pvc, "_load_player_box", lambda seasons, league: box)

    def _raise(seasons, league):
        raise ValueError("season cannot be less than 2025")

    monkeypatch.setattr(pvc, "_load_shots", _raise)
    out = pvc.aggregate_player_seasons([2019])
    r = out.row(0, named=True)
    # three from the box; rim/mid unavailable -> two-point attempts fold into mid
    assert r["fga_three"] == 5.0 and r["fga_rim"] == 0.0 and r["fga_mid"] == 5.0


def test_artifact_save_load_roundtrip(tmp_path, monkeypatch):
    import sportsdataverse.mbb.mbb_player_value_constants as pvc

    monkeypatch.setattr(pvc, "_models_dir_file", lambda name: tmp_path / f"{name}.json")
    payload = {"league": "mens", "coef": [1.0, 2.0], "lambda": 3.0}
    save_artifact("t_roundtrip", payload)
    assert load_artifact("t_roundtrip") == payload
