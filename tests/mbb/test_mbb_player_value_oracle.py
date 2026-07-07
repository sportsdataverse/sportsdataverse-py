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
