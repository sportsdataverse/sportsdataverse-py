from __future__ import annotations
import numpy as np
import polars as pl
from sportsdataverse.nba.nba_model_validation import (
    FitResult,
    RidgeRapmModel,
    _design_with_ids,
    predict_points,
)
from sportsdataverse.nba.nba_rapm import build_rapm_design

_OFF = [f"off_player_{i}" for i in range(1, 6)]
_DEF = [f"def_player_{i}" for i in range(1, 6)]


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
