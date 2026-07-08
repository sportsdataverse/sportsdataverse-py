"""Oracle gate tests for the MBB player-value spine (offline, fixture-driven).

Phase-1 gate: the shipped box-BPM must rank players like Barttorvik's
published BPM (the external oracle). All inputs are committed fixtures
(``tests/fixtures/mbb_player_value/`` -- see its README for provenance), so
the gate runs offline in CI; ``aggregate_player_seasons`` / ``mbb_team_ratings``
are monkeypatched to the frozen 2025 captures.

Observed at fit time (2026-07-07, n=2,532 joined players): Spearman 0.8849
(between-team 0.9858, within-team 0.7095), MAE 1.39 BPM points. Gates are set
below the observed values with headroom for fixture re-captures -- NEVER
lower a gate to make a regression pass; debug the model.

EvanMiya (the plan's secondary oracle) is login-walled with no capturable
endpoint -- not asserted here. The independent 125-game NCAA RAPM validation
correlation is documented in the model artifact / dev notes instead.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import importlib

# resolve the MODULE (the package star-export shadows the attr with the fn)
bpm_mod = importlib.import_module("sportsdataverse.mbb.mbb_box_bpm")
from sportsdataverse.mbb.mbb_player_value_constants import mae, spearman_corr

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_player_value"

GATE_SPEARMAN = 0.80  # plan gate; observed 0.8849
GATE_MAE = 2.0  # BPM points; observed 1.39


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", "", s.lower())).strip()


@pytest.fixture(scope="module")
def box_bpm_2025() -> pl.DataFrame:
    agg = pl.read_parquet(_FIX / "player_seasons_2025.parquet")
    ratings = pl.read_parquet(_FIX / "team_ratings_2025.parquet")
    # module-scoped manual patch (mocker is function-scoped); restored after
    orig = (bpm_mod.aggregate_player_seasons, bpm_mod.mbb_team_ratings)
    bpm_mod.aggregate_player_seasons = lambda seasons, league="mens": agg  # type: ignore[assignment]
    bpm_mod.mbb_team_ratings = lambda seasons, league="mens", **kw: ratings  # type: ignore[assignment]
    try:
        out = bpm_mod.mbb_box_bpm(2025)
    finally:
        bpm_mod.aggregate_player_seasons, bpm_mod.mbb_team_ratings = orig
    return out


@pytest.fixture(scope="module")
def joined(box_bpm_2025: pl.DataFrame) -> pl.DataFrame:
    bart = pl.read_parquet(_FIX / "barttorvik_bpm_2025.parquet").filter(
        (pl.col("team_id").is_not_null()) & (pl.col("min_per") >= 30.0)
    )
    mine = box_bpm_2025.filter(pl.col("min") >= 200.0).with_columns(
        pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("player_norm")
    )
    bart = bart.with_columns(
        pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("player_norm"),
        pl.col("team_id").cast(pl.Utf8),
    )
    assert mine.schema["team_id"] == bart.schema["team_id"] == pl.Utf8
    return mine.join(
        bart.select("team_id", "player_norm", pl.col("bpm").alias("bart_bpm")),
        on=["team_id", "player_norm"],
        how="inner",
    )


def test_box_bpm_gate_spearman_vs_barttorvik(joined: pl.DataFrame) -> None:
    assert joined.height >= 2000, f"oracle join collapsed: n={joined.height}"
    r = spearman_corr(joined.get_column("box_bpm").to_numpy(), joined.get_column("bart_bpm").to_numpy())
    assert r >= GATE_SPEARMAN, f"box-BPM vs Barttorvik spearman {r:.4f} < gate {GATE_SPEARMAN}"


def test_box_bpm_gate_mae_vs_barttorvik(joined: pl.DataFrame) -> None:
    m = mae(joined.get_column("box_bpm").to_numpy(), joined.get_column("bart_bpm").to_numpy())
    assert m <= GATE_MAE, f"box-BPM vs Barttorvik MAE {m:.2f} > gate {GATE_MAE}"


def test_box_bpm_sane_scale(box_bpm_2025: pl.DataFrame) -> None:
    """Qualified players live on a plausible BPM scale (roughly -15..+15)."""
    q = box_bpm_2025.filter(pl.col("min") >= 200.0)
    vals = q.get_column("box_bpm").to_numpy()
    assert np.isfinite(vals).all()
    assert float(np.abs(vals).max()) < 25.0
    # minutes-weighted league mean should sit near 0 (centering, not per-team sum)
    w = q.get_column("min").to_numpy()
    assert abs(float(np.average(vals, weights=w))) < 1.5


# ---------------------------------------------------------------------------
# Phase 2 -- archetype gates (stability + hand-labeled fixture)
# ---------------------------------------------------------------------------

GATE_ARI = 0.70

arch_mod = importlib.import_module("sportsdataverse.mbb.mbb_archetypes")


@pytest.fixture(scope="module")
def archetypes_2025() -> pl.DataFrame:
    agg = pl.read_parquet(_FIX / "player_seasons_2025.parquet")
    orig = arch_mod.aggregate_player_seasons
    arch_mod.aggregate_player_seasons = lambda seasons, league="mens": agg  # type: ignore[assignment]
    try:
        return arch_mod.mbb_archetypes(2025)
    finally:
        arch_mod.aggregate_player_seasons = orig


def test_archetype_labeled_fixture_gate(archetypes_2025: pl.DataFrame) -> None:
    """Every hand-labeled unambiguous player-season lands in its expected
    archetype (11 role-certain 2025 players; see fixtures README)."""
    labeled = pl.read_parquet(_FIX / "archetype_labeled.parquet")
    j = labeled.join(archetypes_2025, on=["player_id", "season"], how="left")
    assert j.height == labeled.height
    misses = j.filter((pl.col("archetype").is_null()) | (pl.col("archetype") != pl.col("expected_archetype")))
    assert misses.height == 0, misses.select("player_id", "expected_archetype", "archetype").to_dicts()


def test_archetype_stability_bootstrap_ari() -> None:
    """Re-fitting on bootstrap resamples reproduces the clustering (ARI gate)."""
    from functools import partial

    from sportsdataverse.mbb.mbb_player_value_constants import (
        bootstrap_ari,
        kmeans_fit,
        load_artifact,
        player_per100_features,
    )

    art = load_artifact("mbb_archetypes")
    agg = pl.read_parquet(_FIX / "player_seasons_2025.parquet")
    feats = (
        player_per100_features(agg)
        .filter(pl.col("min") >= float(art["min_minutes"]))
        .join(
            agg.select("player_id", "season", "team_id", "position"), on=["player_id", "season", "team_id"], how="left"
        )
        .with_columns(
            pl.when(pl.col("position").fill_null("").str.contains("(?i)C"))
            .then(1.0)
            .when(pl.col("position").fill_null("").str.contains("(?i)F"))
            .then(0.5)
            .otherwise(0.0)
            .alias("pos_score")
        )
        .sort("player_id", "season", "team_id")
    )
    cols = art["feature_cols"]
    mu = np.array([art["feature_mean"][c] for c in cols])
    sd = np.array([art["feature_sd"][c] for c in cols])
    Z = (feats.select(cols).fill_null(0.0).to_numpy() - mu) / sd
    ari = bootstrap_ari(partial(kmeans_fit, k=int(art["k"]), seed=0), Z, n_boot=10, seed=0)
    assert ari >= GATE_ARI, f"bootstrap ARI {ari:.3f} < gate {GATE_ARI}"


# ---------------------------------------------------------------------------
# Phases 3-5 -- recruiting / transfer / draft gates (offline recomputation of
# the fitters' held-out measurements from committed fixtures)
# ---------------------------------------------------------------------------

GATE_RECRUITING_SPEARMAN = 0.45  # observed LOSO 0.586 / 0.539
GATE_TRANSFER_SPEARMAN = 0.55  # observed 0.648 (held-out half)
GATE_DRAFT_AUC = 0.80  # observed 0.973 / 0.971
GATE_DRAFT_PICK_SPEARMAN = 0.55  # observed 0.630 / 0.665

_SEASONS = [2025, 2026]


def _agg_all() -> pl.DataFrame:
    return pl.concat([pl.read_parquet(_FIX / f"player_seasons_{s}.parquet") for s in _SEASONS], how="diagonal_relaxed")


def _ratings_all() -> pl.DataFrame:
    return pl.concat([pl.read_parquet(_FIX / f"team_ratings_{s}.parquet") for s in _SEASONS], how="diagonal_relaxed")


@pytest.fixture(scope="module")
def bpm_all() -> pl.DataFrame:
    agg, ratings = _agg_all(), _ratings_all()
    orig = (bpm_mod.aggregate_player_seasons, bpm_mod.mbb_team_ratings)
    bpm_mod.aggregate_player_seasons = lambda seasons, league="mens": agg  # type: ignore[assignment]
    bpm_mod.mbb_team_ratings = lambda seasons, league="mens", **kw: ratings  # type: ignore[assignment]
    try:
        return bpm_mod.mbb_box_bpm(_SEASONS)
    finally:
        bpm_mod.aggregate_player_seasons, bpm_mod.mbb_team_ratings = orig


def test_recruiting_gate_loso_spearman(bpm_all: pl.DataFrame) -> None:
    """Held-out-class Spearman(exp freshman bpm, realized) -- as-of holds by
    construction (features are pre-arrival recruiting inputs only)."""
    from sportsdataverse.mbb.mbb_player_value_constants import load_artifact, ridge_fit

    art = load_artifact("mbb_recruiting")
    recruits = pl.read_parquet(_FIX / "recruits_2025_2026.parquet").filter(pl.col("composite").is_not_null())
    recruits = recruits.with_columns(
        pl.col("height_in").fill_null(pl.col("height_in").median().over("season")),
        pl.col("rank_nat").cast(pl.Float64).fill_null(float(art["bubble_rank"])).log().alias("log_rank"),
        pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("_pn"),
    )
    realized = bpm_all.filter(pl.col("min") >= float(art["min_minutes"])).with_columns(
        pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("_pn")
    )
    assert recruits.schema["team_id"] == realized.schema["team_id"] == pl.Utf8
    j = recruits.join(
        realized.select("_pn", "team_id", "season", "box_bpm"), on=["_pn", "team_id", "season"], how="inner"
    )
    # name-only fallback for recruits whose school ref was missing/changed
    # (mirrors the fitter; keep="none" refuses ambiguous duplicate names)
    unmatched = recruits.join(j.select("_pn", "season").unique(), on=["_pn", "season"], how="anti")
    fb = unmatched.drop("team_id").join(
        realized.select("_pn", "season", "box_bpm", "team_id").unique(subset=["_pn", "season"], keep="none"),
        on=["_pn", "season"],
        how="inner",
    )
    j = pl.concat([j.select(fb.columns), fb], how="vertical_relaxed")
    assert j.height >= 200, f"recruit->freshman join collapsed: n={j.height}"
    assert "box_bpm" not in art["feature_cols"], "as-of: production must not enter X"
    X = j.select(art["feature_cols"]).to_numpy()
    y = j.get_column("box_bpm").to_numpy()
    seas = j.get_column("season").to_numpy()
    for s in _SEASONS:
        tr, te = seas != s, seas == s
        b = ridge_fit(X[tr], y[tr], float(art["lambda"]))
        pred = np.hstack([np.ones((int(te.sum()), 1)), X[te]]) @ b
        r = spearman_corr(pred, y[te])
        assert r >= GATE_RECRUITING_SPEARMAN, f"recruiting held-out class {s}: spearman {r:.4f} < gate"


def test_transfer_gate_beats_baseline(bpm_all: pl.DataFrame) -> None:
    """Held-out half: MAE(proj) < MAE(post=pre baseline) and Spearman gate."""
    import hashlib

    from sportsdataverse.mbb.mbb_player_value_constants import load_artifact, ridge_fit
    from sportsdataverse.mbb.mbb_transfer_projection import transfer_cohort

    art = load_artifact("mbb_transfer")
    q = bpm_all.filter(pl.col("min") >= float(art["min_minutes"]))
    cohort = transfer_cohort(q.select("player_id", "team_id", "season"))
    j = cohort.join(
        q.select(
            "player_id",
            pl.col("season").alias("from_season"),
            pl.col("team_id").alias("from_team_id"),
            pl.col("box_bpm").alias("pre_box_bpm"),
        ),
        on=["player_id", "from_season", "from_team_id"],
        how="inner",
    ).join(
        q.select(
            "player_id",
            pl.col("season").alias("to_season"),
            pl.col("team_id").alias("to_team_id"),
            pl.col("box_bpm").alias("post_box_bpm"),
        ),
        on=["player_id", "to_season", "to_team_id"],
        how="inner",
    )
    assert j.height >= 500, f"transfer cohort collapsed: n={j.height}"
    X = j.select(art["feature_cols"]).to_numpy()
    y = j.get_column("post_box_bpm").to_numpy()
    ids = j.get_column("player_id").to_list()
    te = np.array([int(hashlib.md5(pid.encode()).hexdigest(), 16) % 2 == 0 for pid in ids])
    tr = ~te
    b = ridge_fit(X[tr], y[tr], float(art["lambda"]))
    pred = np.hstack([np.ones((int(te.sum()), 1)), X[te]]) @ b
    m_proj, m_base = mae(pred, y[te]), mae(X[te][:, 0], y[te])
    r = spearman_corr(pred, y[te])
    assert m_proj < m_base, f"transfer proj MAE {m_proj:.3f} !< naive-pre baseline {m_base:.3f}"
    assert r >= GATE_TRANSFER_SPEARMAN, f"transfer held-out spearman {r:.4f} < gate"


def test_draft_gate_both_heads_beat_baseline(bpm_all: pl.DataFrame) -> None:
    """LOSO draft classes: prob AUC + pick Spearman, both above gate and
    above the recruit-rank-only baseline."""
    from sportsdataverse.mbb.mbb_player_value_constants import (
        load_artifact,
        logistic_fit,
        ridge_fit,
        roc_auc,
    )

    art = load_artifact("mbb_draft")
    arch_art = load_artifact("mbb_archetypes")
    agg = _agg_all()
    orig = arch_mod.aggregate_player_seasons
    arch_mod.aggregate_player_seasons = lambda seasons, league="mens": agg  # type: ignore[assignment]
    try:
        arch = arch_mod.mbb_archetypes(_SEASONS).select("player_id", "season", "team_id", "archetype")
    finally:
        arch_mod.aggregate_player_seasons = orig

    uni = (
        bpm_all.filter(pl.col("min") >= float(art["min_minutes"]))
        .join(arch, on=["player_id", "season", "team_id"], how="left")
        .with_columns(pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("_pn"))
    )
    recruits = pl.read_parquet(_FIX / "recruits_2025_2026.parquet").with_columns(
        pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("_pn")
    )
    rec_feats = recruits.group_by("_pn").agg(
        pl.col("composite").max(), pl.col("rank_nat").min(), pl.col("recruiting_class").min()
    )
    rosters = (
        pl.read_parquet(_FIX / "rosters_2025_2026.parquet")
        .select(pl.col("player_id").cast(pl.Utf8), pl.col("season").cast(pl.Int64), "class", "height_in")
        .unique(subset=["player_id", "season"], keep="first")
    )
    draft = pl.read_parquet(_FIX / "draft_2025_2026.parquet").with_columns(
        pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("_pn")
    )
    uni = (
        uni.join(rec_feats, on="_pn", how="left")
        .join(rosters, on=["player_id", "season"], how="left")
        .join(
            draft.select("_pn", pl.col("draft_year").alias("season"), pl.col("overall").alias("pick_overall")),
            on=["_pn", "season"],
            how="left",
        )
    )
    labels = list(art["archetype_labels"])
    assert labels == list(arch_art["labels"])
    feats = uni.with_columns(
        pl.when(pl.col("recruiting_class").is_not_null())
        .then((pl.col("season") - pl.col("recruiting_class")) == 1)
        .otherwise(pl.col("class").fill_null("") == "Freshman")
        .cast(pl.Float64)
        .alias("is_fr"),
        pl.when(pl.col("recruiting_class").is_not_null())
        .then((pl.col("season") - pl.col("recruiting_class")) == 2)
        .otherwise(pl.col("class").fill_null("") == "Sophomore")
        .cast(pl.Float64)
        .alias("is_so"),
        pl.col("height_in").fill_null(pl.col("height_in").median()),
        *[
            (pl.col("archetype").fill_null("") == lab).cast(pl.Float64).alias(f"arch_{i}")
            for i, lab in enumerate(labels[:-1])
        ],
        pl.col("composite").fill_null(float(art["median_composite"])),
        pl.col("rank_nat").cast(pl.Float64).fill_null(float(art["bubble_rank"])).log().alias("log_rank"),
    )
    X = feats.select(art["feature_cols"]).to_numpy()
    Z = (X - X.mean(0)) / np.maximum(X.std(0), 1e-9)
    y = feats.get_column("pick_overall").is_not_null().to_numpy().astype(int)
    picks = feats.get_column("pick_overall").to_numpy()
    seas = feats.get_column("season").to_numpy()
    Xb = feats.select("composite", "log_rank").to_numpy()
    Zb = (Xb - Xb.mean(0)) / np.maximum(Xb.std(0), 1e-9)
    assert int(y.sum()) >= 80, f"drafted-label join collapsed: n={int(y.sum())}"

    for s in _SEASONS:
        tr, te = seas != s, seas == s
        bh = logistic_fit(Z[tr], y[tr], float(art["lambda_prob"]))
        p = 1 / (1 + np.exp(-(np.hstack([np.ones((int(te.sum()), 1)), Z[te]]) @ bh)))
        auc = roc_auc(y[te], p)
        bb = logistic_fit(Zb[tr], y[tr], float(art["lambda_prob"]))
        pb = 1 / (1 + np.exp(-(np.hstack([np.ones((int(te.sum()), 1)), Zb[te]]) @ bb)))
        assert auc >= GATE_DRAFT_AUC, f"draft prob AUC {auc:.4f} < gate (held-out {s})"
        assert auc > roc_auc(y[te], pb), f"draft prob does not beat recruit-rank baseline (held-out {s})"
        dr_tr, dr_te = (y == 1) & tr, (y == 1) & te
        bp = ridge_fit(Z[dr_tr], np.log(picks[dr_tr].astype(float)), float(art["lambda_pick"]))
        pred = np.hstack([np.ones((int(dr_te.sum()), 1)), Z[dr_te]]) @ bp
        r = spearman_corr(pred, picks[dr_te].astype(float))
        bpb = ridge_fit(Zb[dr_tr], np.log(picks[dr_tr].astype(float)), float(art["lambda_pick"]))
        r_b = spearman_corr(np.hstack([np.ones((int(dr_te.sum()), 1)), Zb[dr_te]]) @ bpb, picks[dr_te].astype(float))
        assert r >= GATE_DRAFT_PICK_SPEARMAN, f"draft pick spearman {r:.4f} < gate (held-out {s})"
        assert r > r_b, f"draft pick head does not beat recruit-rank baseline (held-out {s})"
