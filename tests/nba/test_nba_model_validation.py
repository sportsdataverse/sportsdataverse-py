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


# ---------------------------------------------------------------------------
# Task 3: Oracle ① holdout retrodiction tests
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_model_validation import retrodiction, RetrodictionResult  # noqa: E402


class _NoSkillModel:
    """Fits an intercept only; all player coefficients are exactly 0 (no skill)."""

    def fit(self, X, y):
        return FitResult(coef=np.zeros(X.shape[1]), intercept=float(np.mean(y)))


def test_retrodiction_planted_skill_beats_baseline():
    o, d = _planted_ratings()
    poss = _synthetic_possessions(o, d, n_games=80, poss_per_game=100, noise_sd=0.3, seed=2)
    res = retrodiction(RidgeRapmModel(), poss, k_folds=5, seed=0)
    assert isinstance(res, RetrodictionResult)
    assert res.game_margin_corr > 0.2  # real out-of-sample signal
    assert res.game_margin_rmse < res.baseline_rmse  # beats intercept-only


def test_retrodiction_no_skill_model_matches_baseline():
    o, d = _planted_ratings()
    poss = _synthetic_possessions(o, d, n_games=80, poss_per_game=100, noise_sd=0.3, seed=3)
    res = retrodiction(_NoSkillModel(), poss, k_folds=5, seed=0)
    # a no-skill (zero-coef) model can't beat the mean out-of-sample
    assert abs(res.game_margin_corr) < 0.15
    assert res.game_margin_rmse >= res.baseline_rmse - 1e-6


def test_retrodiction_never_raises_on_empty():
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "offense_team_id": pl.Int64,
            "points": pl.Int64,
            **{c: pl.Int64 for c in _OFF + _DEF},
        }
    )
    res = retrodiction(RidgeRapmModel(), empty, k_folds=5, seed=0)
    assert res.n_test_games == 0


# ---------------------------------------------------------------------------
# Task 4: Oracle ② split-half reliability tests
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_model_validation import reliability, ReliabilityResult  # noqa: E402


def test_reliability_rises_with_possession_count():
    o, d = _planted_ratings()
    small = _synthetic_possessions(o, d, n_games=20, poss_per_game=40, noise_sd=0.3, seed=4)
    large = _synthetic_possessions(o, d, n_games=120, poss_per_game=100, noise_sd=0.3, seed=4)
    r_small = reliability(RidgeRapmModel(), small, seed=0)
    r_large = reliability(RidgeRapmModel(), large, seed=0)
    assert isinstance(r_small, ReliabilityResult)
    assert r_large.split_half_corr > r_small.split_half_corr  # more data -> more stable
    assert -1.0 <= r_small.split_half_corr <= 1.0


def test_reliability_never_raises_on_empty():
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "offense_team_id": pl.Int64,
            "points": pl.Int64,
            **{c: pl.Int64 for c in _OFF + _DEF},
        }
    )
    assert reliability(RidgeRapmModel(), empty, seed=0).n_shared_players == 0


# ---------------------------------------------------------------------------
# Task 5: Oracle ③ cross-season predictivity tests
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_model_validation import cross_season, CrossSeasonResult  # noqa: E402


def test_cross_season_predicts_next_season_with_shared_players():
    o, d = _planted_ratings(seed=0)
    season_n = _synthetic_possessions(o, d, n_games=60, poss_per_game=100, noise_sd=0.3, seed=10)
    # season N+1: same ratings (persistent skill) + 4 new players 17..20 replace 1..4
    o2 = {**o, 17: 0.04, 18: -0.03, 19: 0.02, 20: -0.01}
    d2 = {**d, 17: 0.01, 18: -0.02, 19: 0.03, 20: 0.00}
    season_np1 = _synthetic_possessions(o2, d2, n_games=60, poss_per_game=100, noise_sd=0.3, seed=11)
    res = cross_season(RidgeRapmModel(), [season_n, season_np1])
    assert isinstance(res, CrossSeasonResult)
    assert res.rating_corr > 0.3  # last year's rating forecasts this year's
    assert 0.0 <= res.coverage_pct <= 100.0
    assert res.n_shared_players > 0


def test_cross_season_single_season_is_nan():
    o, d = _planted_ratings()
    one = _synthetic_possessions(o, d, n_games=10, poss_per_game=50, noise_sd=0.3, seed=1)
    res = cross_season(RidgeRapmModel(), [one])
    assert np.isnan(res.rating_corr)


# ---------------------------------------------------------------------------
# Task 6: Oracle ④ interval calibration tests
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_model_validation import calibration, CalibrationResult  # noqa: E402


class _PosteriorModel:
    """RidgeCV point fit + a Gaussian posterior of chosen width (calibration knob)."""

    def __init__(self, sd_scale: float, n_samples: int = 200, seed: int = 0):
        self._sd_scale, self._n, self._seed = sd_scale, n_samples, seed

    def fit(self, X, y):
        base = RidgeRapmModel().fit(X, y)
        rng = np.random.default_rng(self._seed)
        # true residual scale ~ from fit; posterior width = sd_scale * that
        resid = float(np.std(y - (X @ base.coef + base.intercept)))
        sd = self._sd_scale * resid / np.sqrt(max(1, X.shape[0]))
        post = base.coef + rng.normal(0, sd, size=(self._n, base.coef.shape[0]))
        return FitResult(coef=base.coef, intercept=base.intercept, posterior=post)


def test_calibration_none_for_point_model():
    o, d = _planted_ratings()
    poss = _synthetic_possessions(o, d, n_games=20, poss_per_game=60, noise_sd=0.3, seed=5)
    assert calibration(RidgeRapmModel(), poss) is None


def test_calibration_returns_curve_for_posterior_model():
    o, d = _planted_ratings()
    poss = _synthetic_possessions(o, d, n_games=40, poss_per_game=80, noise_sd=0.3, seed=6)
    res = calibration(_PosteriorModel(sd_scale=1.0), poss, levels=(0.5, 0.9))
    assert isinstance(res, CalibrationResult)
    assert res.levels == [0.5, 0.9]
    assert all(0.0 <= c <= 1.0 for c in res.coverage)
    # an over-confident model (tiny posterior) should under-cover at 0.9
    tight = calibration(_PosteriorModel(sd_scale=0.05), poss, levels=(0.9,))
    assert tight.coverage[0] < 0.9
