"""Tests for nba_spm: SpmCoefficients, train_spm, nba_spm (pure math, no live fetch)."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_spm import SPM_FEATURES, SpmCoefficients, nba_spm, train_spm


def test_train_spm_recovers_known_linear_map():
    rng = np.random.default_rng(0)
    n = 400
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame({"player_id": np.arange(n), **feats, "min": np.full(n, 500.0), "gp": np.full(n, 20)})
    # planted linear map: o_rapm = 2*pts - 1.5*tov ; d_rapm = 1.2*stl + 0.8*blk
    o = 2.0 * feats["pts"] - 1.5 * feats["tov"]
    d = 1.2 * feats["stl"] + 0.8 * feats["blk"]
    target = pl.DataFrame({"player_id": np.arange(n), "o_rapm": o, "d_rapm": d})
    coef = train_spm(bf, target, alpha=0.01)
    out = nba_spm(bf, coef)
    # recovered predictions correlate near-perfectly with the planted targets
    assert np.corrcoef(out["ospm"].to_numpy(), o)[0, 1] > 0.99
    assert np.corrcoef(out["dspm"].to_numpy(), d)[0, 1] > 0.99
    assert set(out.columns) == {"player_id", "ospm", "dspm", "spm", "min", "gp"}


def test_spm_features_match_box_logs_stats():
    from sportsdataverse.nba.nba_box_logs import _STATS

    assert SPM_FEATURES == list(_STATS)


def test_spm_coefficients_frozen():
    coef = SpmCoefficients(
        o_coef=np.zeros(len(SPM_FEATURES)),
        d_coef=np.zeros(len(SPM_FEATURES)),
        o_intercept=0.0,
        d_intercept=0.0,
        feature_names=SPM_FEATURES,
    )
    with pytest.raises((AttributeError, TypeError)):
        coef.o_intercept = 1.0  # type: ignore[misc]


def test_nba_spm_output_schema():
    """nba_spm returns the documented column set with correct dtypes."""
    rng = np.random.default_rng(42)
    n = 10
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 200.0),
            "gp": np.full(n, 10, dtype=np.int64),
        }
    )
    coef = SpmCoefficients(
        o_coef=np.ones(len(SPM_FEATURES)),
        d_coef=np.ones(len(SPM_FEATURES)) * 0.5,
        o_intercept=1.0,
        d_intercept=0.5,
        feature_names=SPM_FEATURES,
    )
    out = nba_spm(bf, coef)
    assert out.schema["player_id"] == pl.Int64
    assert out.schema["ospm"] == pl.Float64
    assert out.schema["dspm"] == pl.Float64
    assert out.schema["spm"] == pl.Float64
    assert out.schema["min"] == pl.Float64
    assert out.schema["gp"] == pl.Int64
    assert len(out) == n


def test_nba_spm_return_as_pandas():
    import pandas as pd

    rng = np.random.default_rng(7)
    n = 5
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 100.0),
            "gp": np.full(n, 5, dtype=np.int64),
        }
    )
    coef = SpmCoefficients(
        o_coef=np.zeros(len(SPM_FEATURES)),
        d_coef=np.zeros(len(SPM_FEATURES)),
        o_intercept=0.0,
        d_intercept=0.0,
        feature_names=SPM_FEATURES,
    )
    out_pd = nba_spm(bf, coef, return_as_pandas=True)
    assert isinstance(out_pd, pd.DataFrame)
    assert set(out_pd.columns) == {"player_id", "ospm", "dspm", "spm", "min", "gp"}


def test_train_spm_inner_join_on_player_id():
    """train_spm inner-joins: players in box_features but not in target are excluded."""
    rng = np.random.default_rng(1)
    n = 50
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 300.0),
            "gp": np.full(n, 15, dtype=np.int64),
        }
    )
    # target only covers half the players
    target = pl.DataFrame(
        {
            "player_id": np.arange(25, dtype=np.int64),
            "o_rapm": rng.normal(0, 1, 25),
            "d_rapm": rng.normal(0, 1, 25),
        }
    )
    coef = train_spm(bf, target, alpha=10.0)
    assert isinstance(coef, SpmCoefficients)
    assert len(coef.feature_names) == len(SPM_FEATURES)


def test_train_spm_custom_feature_names():
    """train_spm respects a custom feature_names subset."""
    rng = np.random.default_rng(2)
    n = 100
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 400.0),
            "gp": np.full(n, 20, dtype=np.int64),
        }
    )
    target = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            "o_rapm": rng.normal(0, 1, n),
            "d_rapm": rng.normal(0, 1, n),
        }
    )
    subset = ["pts", "ast", "tov"]
    coef = train_spm(bf, target, feature_names=subset, alpha=10.0)
    assert coef.feature_names == subset
    assert len(coef.o_coef) == 3
    assert len(coef.d_coef) == 3


def test_nba_spm_enforces_int64_id_and_gp():
    n = 30
    feats = {f: np.linspace(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": pl.Series(range(n), dtype=pl.Int32),
            **feats,
            "min": np.full(n, 100.0),
            "gp": pl.Series([5] * n, dtype=pl.Int32),
        }
    )
    target = pl.DataFrame(
        {
            "player_id": range(n),
            "o_rapm": np.zeros(n),
            "d_rapm": np.zeros(n),
        }
    )
    out = nba_spm(bf, train_spm(bf, target, alpha=1.0))
    assert out.schema["player_id"] == pl.Int64
    assert out.schema["gp"] == pl.Int64


# ---------------------------------------------------------------------------
# Task 4: NbaSpmModel adapter + head-to-head integration
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_spm import NbaSpmModel
from sportsdataverse.nba.nba_model_validation import validate_model, RidgeRapmModel, _synthetic_possessions


def _spm_setup():
    # synthetic possessions whose ids double as the box-log player_ids
    ids = list(range(100, 110)) + list(range(200, 210))
    rng = np.random.default_rng(3)
    o = {p: float(rng.normal(0, 0.03)) for p in ids}
    d = {p: float(rng.normal(0, 0.03)) for p in ids}
    poss = _synthetic_possessions(o, d, n_games=30, poss_per_game=40, noise_sd=0.3, seed=4)
    # trivial box logs: each player one game, one team, features = their o rating signal
    game_ids = poss["game_id"].unique().to_list()
    return ids, poss, game_ids


def test_nba_spm_model_fold_restriction_and_validate(monkeypatch):
    from sportsdataverse.nba.nba_spm import SPM_FEATURES, train_spm

    ids, poss, game_ids = _spm_setup()
    # build box logs covering all games; player features constant per player
    rows_p, rows_t = [], []
    for gi in game_ids:
        for p in ids:
            team = 1 if p < 200 else 2
            rows_p.append(
                {"game_id": gi, "team_id": team, "player_id": p, "min": 24.0, **{s: float(p % 7) for s in SPM_FEATURES}}
            )
        for team in (1, 2):
            rows_t.append(
                {"game_id": gi, "team_id": team, "min": 240.0, "fga": 85.0, "oreb": 10.0, "tov": 13.0, "fta": 20.0}
            )
    player_logs = pl.DataFrame(rows_p)
    team_logs = pl.DataFrame(rows_t)
    # a coefficients object (fit on a tiny synthetic target)
    from sportsdataverse.nba.nba_box_logs import box_features

    bf = box_features(player_logs, team_logs)
    target = pl.DataFrame({"player_id": bf["player_id"], "o_rapm": np.zeros(bf.height), "d_rapm": np.zeros(bf.height)})
    coef = train_spm(bf, target, alpha=1.0)
    model = NbaSpmModel(coef, player_logs, team_logs)
    # fold restriction: a 2-game fold's ratings come from only those games
    rf_all = model.fit_ratings(poss)
    rf_two = model.fit_ratings(poss.filter(pl.col("game_id").is_in(game_ids[:2])))
    assert set(rf_all.o_ratings) and set(rf_two.o_ratings)  # both produce ratings
    # head-to-head: SPM and RAPM both run through the SAME validate_model
    rep_spm = validate_model(model, [poss], model_name="spm", oracles=("retrodiction",))
    rep_rapm = validate_model(RidgeRapmModel(), [poss], model_name="plain_rapm", oracles=("retrodiction",))
    assert rep_spm.model_name == "spm" and rep_rapm.model_name == "plain_rapm"
    assert rep_spm.retrodiction is not None and rep_rapm.retrodiction is not None
