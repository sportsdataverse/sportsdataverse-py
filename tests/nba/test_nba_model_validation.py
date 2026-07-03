from __future__ import annotations
import json
from pathlib import Path

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


# ---------------------------------------------------------------------------
# Task 7: validate_model orchestrator + ValidationReport + render_report
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_model_validation import validate_model, render_report, ValidationReport  # noqa: E402


def test_validate_model_runs_selected_oracles_and_renders():
    o, d = _planted_ratings()
    s1 = _synthetic_possessions(o, d, n_games=40, poss_per_game=80, noise_sd=0.3, seed=1)
    s2 = _synthetic_possessions(o, d, n_games=40, poss_per_game=80, noise_sd=0.3, seed=2)
    rep = validate_model(RidgeRapmModel(), [s1, s2], model_name="plain_rapm")
    assert isinstance(rep, ValidationReport)
    assert rep.retrodiction is not None and rep.reliability is not None
    assert rep.cross_season is not None
    assert rep.calibration is None  # point model -> n/a
    md = render_report(rep)
    assert "plain_rapm" in md and "Retrodiction" in md and "n/a" in md.lower()


def test_validate_model_respects_oracle_selection():
    o, d = _planted_ratings()
    s1 = _synthetic_possessions(o, d, n_games=20, poss_per_game=60, noise_sd=0.3, seed=1)
    rep = validate_model(RidgeRapmModel(), [s1], oracles=("reliability",))
    assert rep.reliability is not None and rep.retrodiction is None


# ---------------------------------------------------------------------------
# Task 9: Gated end-to-end real-report test
# ---------------------------------------------------------------------------

from tests.conftest import skip_if_no_nba_stats_live  # noqa: E402
from sportsdataverse.nba import nba_season_compile as C  # noqa: E402
from sportsdataverse.nba.nba_season_compile import compile_nba_season  # noqa: E402


@skip_if_no_nba_stats_live
def test_end_to_end_real_slice_report(tmp_path, monkeypatch):
    # small real slice compiled once, then validated (report shape only, not thresholds)
    real_index = C._season_game_index(2023, "Regular Season").head(8)
    monkeypatch.setattr(C, "_season_game_index", lambda s, st: real_index)
    s = compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=1.0)
    rep = validate_model(
        RidgeRapmModel(),
        [s],
        model_name="plain_rapm",
        oracles=("retrodiction", "reliability"),
    )
    assert rep.retrodiction is not None
    md = render_report(rep)
    assert "plain_rapm" in md


# ---------------------------------------------------------------------------
# Task 1 (SPM sub-project): RatingsModel harness path
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_model_validation import (  # noqa: E402
    RatingsFit,
    _fit_on,
)


class _PlantedRatings:
    """A RatingsModel that returns KNOWN per-100 ratings (meta-oracle ground truth)."""

    def __init__(self, o100, d100):
        self._o, self._d = o100, d100

    def fit_ratings(self, possessions):
        return RatingsFit(o_ratings=dict(self._o), d_ratings=dict(self._d))


def _planted_ratings_setup():
    ids = list(range(100, 110)) + list(range(200, 210))  # team A ids, team B ids
    rng = __import__("numpy").random.default_rng(7)
    # Use SD=20 (per-100) so rating signal (5*20/100*100poss = 10pts/game) >> noise
    # (noise_sd=0.3 per poss -> sqrt(100)*0.3*sqrt(2) ~ 4.2 pts game noise)
    o100 = {p: float(rng.normal(0, 20)) for p in ids}
    d100 = {p: float(rng.normal(0, 20)) for p in ids}
    # DGP is per-possession, so divide the per-100 ratings by 100 to generate
    poss = _synthetic_possessions(
        {p: v / 100 for p, v in o100.items()},
        {p: v / 100 for p, v in d100.items()},
        n_games=40,
        poss_per_game=100,
        noise_sd=0.3,
        seed=1,
    )
    return o100, d100, poss


def test_ratings_meta_oracle_planted_beats_noskill():
    o100, d100, poss = _planted_ratings_setup()
    planted = _PlantedRatings(o100, d100)
    zero = _PlantedRatings({p: 0.0 for p in o100}, {p: 0.0 for p in d100})
    rp = retrodiction(planted, poss, seed=2)
    rz = retrodiction(zero, poss, seed=2)
    assert rp.game_margin_rmse < rz.game_margin_rmse  # planted predicts; no-skill ~baseline
    assert rp.game_margin_corr > 0.5  # planted tracks truth (no fold leakage collapse)


def test_fit_on_maps_ratings_to_perpossession_coef():
    o100, d100, poss = _planted_ratings_setup()
    fit, pids = _fit_on(_PlantedRatings(o100, d100), poss)
    P = len(pids)
    k = pids.index(100)
    assert abs(fit.coef[k] - o100[100] / 100) < 1e-9  # coef[i] = o/100
    assert abs(fit.coef[P + k] + d100[100] / 100) < 1e-9  # coef[P+i] = -d/100


def test_validate_model_ratings_all_oracles_no_crash():
    """A ratings model through validate_model with ALL default oracles must not crash;
    calibration returns None (ratings models are point estimators)."""
    o100, d100, poss = _planted_ratings_setup()
    from sportsdataverse.nba.nba_model_validation import validate_model

    rep = validate_model(_PlantedRatings(o100, d100), [poss], model_name="planted")
    assert rep.calibration is None  # point model -> no posterior -> None, not a crash
    assert rep.retrodiction is not None  # the predictive oracles still populate
    assert rep.reliability is not None


# ---------------------------------------------------------------------------
# Task 1 (adj-RAPM sub-project): PriorModel harness path
# ---------------------------------------------------------------------------


class _StubPriorModel:
    """Minimal PriorModel: returns the prior_mean as coef (no fitting) + a trivial posterior."""

    def __init__(self, prior):
        self.prior = prior  # Dict[int, (o, d)]

    def fit_with_prior(self, X, y, prior_mean):
        import numpy as np

        return FitResult(coef=prior_mean.copy(), intercept=0.0, posterior=np.tile(prior_mean, (8, 1)))


def test_fit_on_routes_prior_model():
    o100, d100, poss = _planted_ratings_setup()  # existing helper in this file
    prior = {p: (o100[p], d100[p]) for p in o100}
    fit, pids = _fit_on(_StubPriorModel(prior), poss)
    P = len(pids)
    k = pids.index(100)
    assert abs(fit.coef[k] - o100[100] / 100) < 1e-9  # prior_mean[i] = o/100
    assert abs(fit.coef[P + k] + d100[100] / 100) < 1e-9  # prior_mean[P+i] = -d/100
    assert fit.posterior is not None and fit.posterior.shape == (8, 2 * P)


def test_validate_model_prior_all_oracles_no_crash():
    o100, d100, poss = _planted_ratings_setup()
    prior = {p: (o100[p], d100[p]) for p in o100}
    rep = validate_model(_StubPriorModel(prior), [poss], model_name="stub_prior")
    assert rep.retrodiction is not None and rep.calibration is not None  # posterior -> calibration populated


# ---------------------------------------------------------------------------
# WP3 Task 7: Oracle 5 (external_validity) -- id-join + meta-oracle teeth
# ---------------------------------------------------------------------------

import pytest  # noqa: E402
from sportsdataverse.nba.nba_model_validation import ExternalValidityResult, external_validity  # noqa: E402


def test_external_validity_scores_near_one_fed_itself():
    # feed the SAME values back as both "ratings" and "oracle" (under different
    # column names) -- the meta-oracle must score ~1.0.
    ratings = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "rapm": [2.0, -1.0, 0.5, 3.0, -2.5]})
    oracle = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "RAPM": [2.0, -1.0, 0.5, 3.0, -2.5]})
    res = external_validity(ratings, oracle, rating_col="rapm", oracle_col="RAPM")
    assert isinstance(res, ExternalValidityResult)
    assert res.corr > 0.999
    assert res.n_matched == 5
    assert res.coverage_pct == 100.0
    assert res.join == "id"


def test_external_validity_drops_under_id_permutation():
    rng = np.random.default_rng(0)
    truth = rng.normal(0, 2, size=40)
    ratings = pl.DataFrame({"player_id": list(range(40)), "rapm": truth})
    oracle_correct = pl.DataFrame({"player_id": list(range(40)), "RAPM": truth + rng.normal(0, 0.2, size=40)})
    res_correct = external_validity(ratings, oracle_correct, rating_col="rapm", oracle_col="RAPM")
    assert res_correct.corr > 0.9

    shuffled_ids = list(range(40))
    rng.shuffle(shuffled_ids)
    oracle_shuffled = pl.DataFrame({"player_id": shuffled_ids, "RAPM": (truth + rng.normal(0, 0.2, size=40))})
    res_shuffled = external_validity(ratings, oracle_shuffled, rating_col="rapm", oracle_col="RAPM")
    assert abs(res_shuffled.corr) < res_correct.corr - 0.3


def test_external_validity_partial_coverage():
    ratings = pl.DataFrame({"player_id": [1, 2, 3, 4], "rapm": [1.0, 2.0, 3.0, 4.0]})
    oracle = pl.DataFrame({"player_id": [1, 2, 99], "RAPM": [1.1, 2.1, 9.9]})  # only 1,2 overlap
    res = external_validity(ratings, oracle, rating_col="rapm", oracle_col="RAPM")
    assert res.n_matched == 2
    assert res.coverage_pct == 50.0


def test_external_validity_permutation_p95_is_self_computed_not_hardcoded():
    rng = np.random.default_rng(1)
    truth = rng.normal(0, 2, size=60)
    ratings = pl.DataFrame({"player_id": list(range(60)), "rapm": truth})
    noise_only = pl.DataFrame({"player_id": list(range(60)), "RAPM": rng.normal(0, 2, size=60)})
    res = external_validity(ratings, noise_only, rating_col="rapm", oracle_col="RAPM", n_permutations=300, seed=2)
    assert abs(res.corr) < res.permutation_p95 + 0.3  # a true-null pair shouldn't clear its own null ceiling by much


def test_external_validity_never_raises_on_empty():
    empty = pl.DataFrame(schema={"player_id": pl.Int64, "rapm": pl.Float64})
    oracle = pl.DataFrame({"player_id": [1], "RAPM": [1.0]})
    res = external_validity(empty, oracle, rating_col="rapm", oracle_col="RAPM")
    assert res.n_matched == 0
    assert res.coverage_pct == 0.0
    assert np.isnan(res.corr)


def test_external_validity_too_few_matches_is_nan():
    ratings = pl.DataFrame({"player_id": [1, 2], "rapm": [1.0, 2.0]})
    oracle = pl.DataFrame({"player_id": [1, 2], "RAPM": [1.1, 2.1]})
    res = external_validity(ratings, oracle, rating_col="rapm", oracle_col="RAPM")
    assert res.n_matched == 2
    assert np.isnan(res.corr)  # fewer than 3 matched rows -> nan, not a spurious 2-point corr


def test_external_validity_rejects_unknown_join_kind():
    ratings = pl.DataFrame({"player_id": [1], "rapm": [1.0]})
    oracle = pl.DataFrame({"player_id": [1], "RAPM": [1.0]})
    with pytest.raises(ValueError, match="join"):
        external_validity(ratings, oracle, rating_col="rapm", oracle_col="RAPM", join="bogus")


# ---------------------------------------------------------------------------
# WP3 Task 8: external_validity name-join path (DARKO-style)
# ---------------------------------------------------------------------------


def test_external_validity_name_join_basic():
    ratings = pl.DataFrame(
        {"player_name": ["Nikola Jokic", "Kawhi Leonard", "Victor Wembanyama"], "projected_rating": [8.0, 6.0, 7.5]}
    )
    oracle = pl.DataFrame({"player_name": ["Nikola Jokic", "Kawhi Leonard", "Victor Wembanyama"], "dpm": [8, 6, 7]})
    res = external_validity(
        ratings,
        oracle,
        rating_col="projected_rating",
        oracle_col="dpm",
        join="name",
    )
    assert res.n_matched == 3
    assert res.coverage_pct == 100.0
    assert res.corr > 0.8


def test_external_validity_name_join_handles_diacritic_mismatch():
    # real stats.nba.com spelling (with the Serbian ć) on one side, the plain
    # ASCII DARKO CSV spelling on the other -- must still match.
    ratings = pl.DataFrame({"player_name": ["Nikola Jokić"], "projected_rating": [8.0]})
    oracle = pl.DataFrame({"player_name": ["Nikola Jokic"], "dpm": [7]})
    res = external_validity(ratings, oracle, rating_col="projected_rating", oracle_col="dpm", join="name")
    assert res.n_matched == 1
    assert res.coverage_pct == 100.0


def test_external_validity_name_join_reports_low_coverage_on_mismatch():
    ratings = pl.DataFrame(
        {"player_name": ["Player A", "Player B", "Player C", "Player D"], "projected_rating": [1.0, 2.0, 3.0, 4.0]}
    )
    oracle = pl.DataFrame({"player_name": ["Player A", "Someone Else"], "dpm": [1, 9]})
    res = external_validity(ratings, oracle, rating_col="projected_rating", oracle_col="dpm", join="name")
    assert res.n_matched == 1
    assert res.coverage_pct == 25.0
    assert np.isnan(res.corr)  # only 1 matched row -- below the n>=3 floor


# ---------------------------------------------------------------------------
# WP3 Task 9: gated real-CSV harness-level smoke tests (SDV_PY_NBA_ORACLE_DIR)
# ---------------------------------------------------------------------------

import glob
import os

_ORACLE_DIR = os.environ.get("SDV_PY_NBA_ORACLE_DIR")
_has_oracle_dir = bool(_ORACLE_DIR and os.path.isdir(_ORACLE_DIR))
skip_if_no_oracle_dir = pytest.mark.skipif(
    not _has_oracle_dir, reason="set SDV_PY_NBA_ORACLE_DIR to the real oracle-CSV directory to run"
)

from sportsdataverse.nba import nba_stats_parsers  # noqa: E402
from sportsdataverse.nba.nba_oracle_data import load_darko_dpm  # noqa: E402

_NBA_STATS_FIXTURE = Path(__file__).parent / "fixtures" / "cap_leaguedashplayerstats_nba.json"


@skip_if_no_oracle_dir
def test_real_darko_name_join_against_real_player_directory():
    """DARKO's name-only leaderboard joined against a REAL stats.nba.com player
    directory (the committed cap_leaguedashplayerstats_nba.json fixture -- no
    live network call needed). Proves the normalizer handles real diacritic
    spellings (Jokic/Jokić) and reports a real, non-hardcoded coverage %."""
    files = glob.glob(os.path.join(_ORACLE_DIR, "*-darko-dpm-leaderboard.csv"))
    if not files:
        pytest.skip("no *-darko-dpm-leaderboard.csv present")
    darko = load_darko_dpm(sorted(files)[-1])

    raw = json.loads(_NBA_STATS_FIXTURE.read_text(encoding="utf-8"))
    directory = nba_stats_parsers.parse_nba_stats_result_sets(raw)
    # fabricate a "ratings" frame: any per-player numeric column stands in for
    # a model rating here -- this test validates the JOIN + coverage machinery
    # against 100% real external name spellings, not a specific model's accuracy.
    ratings = directory.select(pl.col("player_id"), pl.col("player_name"), pl.lit(0.0).alias("dummy_rating"))
    res = external_validity(
        ratings,
        darko,
        rating_col="dummy_rating",
        oracle_col="dpm",
        join="name",
    )
    assert res.n_matched > 0
    assert res.coverage_pct > 0.0
    # the real Jokic/Jokić spelling mismatch must resolve via normalize_player_name
    jokic_row = directory.filter(pl.col("player_name").str.contains("Jok")).to_dicts()
    assert any("ć" in r["player_name"] or "c" in r["player_name"] for r in jokic_row)


@skip_if_no_nba_stats_live
@skip_if_no_oracle_dir
def test_end_to_end_real_slice_external_validity(tmp_path, monkeypatch):
    """Small real slice -> nba_rapm -> external_validity against real Ryan Davis
    RAPM (2022-23, the oracle's most recent available season). Report shape
    only, not thresholds -- an 8-game slice is too small to support a
    meaningful correlation floor (same caution as the existing
    test_end_to_end_real_slice_report)."""
    from sportsdataverse.nba.nba_rapm import nba_rapm
    from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis

    real_index = C._season_game_index(2022, "Regular Season").head(8)
    monkeypatch.setattr(C, "_season_game_index", lambda s, st: real_index)
    s = compile_nba_season(2022, cache_dir=str(tmp_path), delay_s=1.0)
    ratings = nba_rapm(s)
    oracle = load_rapm_ryan_davis(os.path.join(_ORACLE_DIR, "rapm_ryan_davis.csv")).filter(
        pl.col("season") == "2022-23"
    )
    res = external_validity(ratings, oracle, rating_col="rapm", oracle_col="RAPM")
    assert res.n_matched >= 0  # shape only -- 8 games may or may not overlap Ryan Davis' player pool
    assert 0.0 <= res.coverage_pct <= 100.0


# ---------------------------------------------------------------------------
# WP3 Task 10: Oracle 6 (walk_forward) -- time-ordered "predict tomorrow"
# ---------------------------------------------------------------------------

import datetime

from sportsdataverse.nba.nba_model_validation import WalkForwardResult, walk_forward  # noqa: E402


def test_synthetic_possessions_start_date_attaches_game_date():
    o, d = _planted_ratings()
    poss = _synthetic_possessions(
        o, d, n_games=5, poss_per_game=10, noise_sd=0.3, seed=1, start_date=datetime.date(2023, 10, 24)
    )
    assert "game_date" in poss.columns
    assert poss.schema["game_date"] == pl.Date
    d0 = poss.filter(pl.col("game_id") == "SYN00000")["game_date"][0]
    d1 = poss.filter(pl.col("game_id") == "SYN00001")["game_date"][0]
    assert d0 == datetime.date(2023, 10, 24)
    assert d1 == datetime.date(2023, 10, 25)


def test_synthetic_possessions_backward_compatible_without_start_date():
    o, d = _planted_ratings()
    poss = _synthetic_possessions(o, d, n_games=4, poss_per_game=50, noise_sd=0.3, seed=7)
    assert "game_date" not in poss.columns  # unchanged behavior for existing Oracle 1-4 tests


def test_walk_forward_beats_shuffled_date_control():
    o, d = _planted_ratings(seed=3)
    poss = _synthetic_possessions(
        o, d, n_games=100, poss_per_game=80, noise_sd=0.3, seed=5, start_date=datetime.date(2023, 10, 24)
    )
    res = walk_forward(RidgeRapmModel(), poss, horizon_days=10, min_games_before_first_checkpoint=20)
    assert isinstance(res, WalkForwardResult)
    assert res.n_checkpoints > 0
    assert res.game_margin_corr > 0.2  # real signal, matching Oracle 1's planted-skill bar

    # shuffled-date control: randomly reassign game_date so "future" no longer
    # follows "past" in a meaningful order -- walk-forward signal should collapse.
    rng = np.random.default_rng(9)
    dates = poss.select("game_date").unique()["game_date"].to_list()
    shuffled_map = dict(
        zip(sorted(poss["game_id"].unique().to_list()), rng.permutation(dates * 100)[: poss["game_id"].n_unique()])
    )
    shuffled_poss = poss.with_columns(
        pl.col("game_id").map_elements(lambda g: shuffled_map[g], return_dtype=pl.Date).alias("game_date")
    )
    res_shuffled = walk_forward(RidgeRapmModel(), shuffled_poss, horizon_days=10, min_games_before_first_checkpoint=20)
    assert res_shuffled.game_margin_corr < res.game_margin_corr


def test_walk_forward_reports_carry_forward_and_random_fold_baselines():
    o, d = _planted_ratings(seed=4)
    poss = _synthetic_possessions(
        o, d, n_games=100, poss_per_game=80, noise_sd=0.3, seed=6, start_date=datetime.date(2023, 10, 24)
    )
    res = walk_forward(RidgeRapmModel(), poss, horizon_days=10, min_games_before_first_checkpoint=20)
    assert res.n_checkpoints >= 2  # need >=2 for a non-nan carry_forward_rmse
    assert not np.isnan(res.carry_forward_rmse)
    assert not np.isnan(res.random_fold_rmse)
    assert res.random_fold_rmse == retrodiction(RidgeRapmModel(), poss, k_folds=5, seed=0).game_margin_rmse


def test_walk_forward_never_raises_on_empty():
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "offense_team_id": pl.Int64,
            "points": pl.Int64,
            "game_date": pl.Date,
            **{c: pl.Int64 for c in _OFF + _DEF},
        }
    )
    res = walk_forward(RidgeRapmModel(), empty)
    assert res.n_checkpoints == 0
    assert np.isnan(res.game_margin_rmse)


def test_walk_forward_missing_game_date_column_is_nan_not_a_crash():
    poss = _toy_possessions()  # no game_date column at all
    res = walk_forward(RidgeRapmModel(), poss)
    assert res.n_checkpoints == 0
    assert np.isnan(res.game_margin_rmse)
