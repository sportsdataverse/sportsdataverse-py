from __future__ import annotations
import numpy as np
import polars as pl
from sportsdataverse.nba.nba_model_validation import (
    FitResult,
    RidgeRapmModel,
    _design_with_ids,
    _synthetic_possessions,
    predict_points,
    _OFF,
    _DEF,
)
from sportsdataverse.nba.nba_rapm import build_rapm_design


def _toy_possessions() -> pl.DataFrame:
    # 2 possessions, players 1..10; offense {1..5} vs defense {6..10}, then swapped.
    return pl.DataFrame(
        {
            "game_id": ["001", "001"],
            "offense_team_id": [100, 200],
            **{c: v for c, v in zip(_OFF, [[1, 6], [2, 7], [3, 8], [4, 9], [5, 10]])},
            **{c: v for c, v in zip(_DEF, [[6, 1], [7, 2], [8, 3], [9, 4], [10, 5]])},
            "points": [2, 0],
        }
    )


def test_ridge_model_returns_fitresult_with_matching_shape():
    poss = _toy_possessions()
    X, y, pids = build_rapm_design(poss)
    fit = RidgeRapmModel().fit(X, y)
    assert isinstance(fit, FitResult)
    assert fit.coef.shape == (2 * len(pids),)
    assert isinstance(fit.intercept, float)
    assert fit.posterior is None


def test_design_with_ids_maps_to_train_columns_and_zeros_unknowns():
    poss = _toy_possessions()
    _, _, pids = build_rapm_design(poss)  # pids == [1..10]
    # a test possession with a NEW player (99) on offense -> unknown -> contributes 0
    test_poss = pl.DataFrame(
        {
            "game_id": ["002"],
            "offense_team_id": [100],
            **{c: [v] for c, v in zip(_OFF, [1, 2, 3, 4, 99])},
            **{c: [v] for c, v in zip(_DEF, [6, 7, 8, 9, 10])},
            "points": [3],
        }
    )
    X_test, y_test = _design_with_ids(test_poss, pids)
    assert X_test.shape == (1, 2 * len(pids))
    # exactly 9 ones (4 known off + 5 known def); player 99 has no column
    assert X_test.nnz == 9
    assert y_test.tolist() == [3.0]


def test_predict_points_is_linear():
    poss = _toy_possessions()
    X, y, pids = build_rapm_design(poss)
    fit = RidgeRapmModel().fit(X, y)
    pred = predict_points(X, fit)
    assert pred.shape == (2,)
    np.testing.assert_allclose(pred, X @ fit.coef + fit.intercept)


def _planted_ratings(seed: int = 0):
    rng = np.random.default_rng(seed)
    # team A players 1..8, team B players 9..16; ratings ~ per-possession points contribution
    o = {p: float(rng.normal(0, 0.05)) for p in range(1, 17)}
    d = {p: float(rng.normal(0, 0.05)) for p in range(1, 17)}
    return o, d


def test_synthetic_possessions_schema_and_determinism():
    o, d = _planted_ratings()
    a = _synthetic_possessions(o, d, n_games=4, poss_per_game=50, noise_sd=0.3, seed=7)
    b = _synthetic_possessions(o, d, n_games=4, poss_per_game=50, noise_sd=0.3, seed=7)
    assert a.equals(b)  # deterministic
    for col in ["game_id", "offense_team_id", "points", *_OFF, *_DEF]:
        assert col in a.columns
    assert a.height == 4 * 50 * 2  # both teams take poss_per_game each
    assert set(a["offense_team_id"].unique().to_list()) == {100, 200}


def test_ridge_recovers_planted_ratings_in_sample():
    # sanity: with enough possessions RidgeCV coef correlates with planted ratings
    o, d = _planted_ratings()
    poss = _synthetic_possessions(o, d, n_games=60, poss_per_game=100, noise_sd=0.3, seed=1)
    X, y, pids = build_rapm_design(poss)
    fit = RidgeRapmModel().fit(X, y)
    P = len(pids)
    planted_o = np.array([o[p] for p in pids])
    corr = np.corrcoef(fit.coef[:P], planted_o)[0, 1]
    assert corr > 0.5  # ridge shrinks, but signal present
