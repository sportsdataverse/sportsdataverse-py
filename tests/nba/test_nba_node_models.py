"""Gates for the per-node models (models2shelf) — real-fixture oracles."""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    OUTCOMES,
    build_shelf,
    possessions_from_pbp,
    shelf_from_parquet,
    shelf_to_parquet,
    simulate_ensemble,
)
from sportsdataverse.nba.nba_possession_sim.node_models import (
    fit_outcome_node_model,
    fit_rebound_node_model,
    models_to_shelf,
)
from sportsdataverse.nba.nba_possession_sim.shelf import Shelf

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAME_IDS = ("0022100001", "0022200001", "0022300001")
FULL_GRID = 9 * 4 * 4  # score-diff bins x periods x clock bins


@pytest.fixture(scope="module")
def events() -> pl.DataFrame:
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return possessions_from_pbp(pl.concat(frames, how="diagonal_relaxed"))


@pytest.fixture(scope="module")
def model_shelf(events: pl.DataFrame) -> Shelf:
    return models_to_shelf(events)


def test_full_grid_coverage_and_valid_pmfs(model_shelf: Shelf) -> None:
    assert len(model_shelf.outcome_pmfs) == FULL_GRID
    for key, pmf in model_shelf.outcome_pmfs.items():
        assert set(pmf) == set(OUTCOMES), key
        assert sum(pmf.values()) == pytest.approx(1.0, abs=1e-6), key
    assert model_shelf.oreb_rates is not None
    assert len(model_shelf.oreb_rates) == FULL_GRID
    assert all(0.0 < r < 1.0 for r in model_shelf.oreb_rates.values())
    assert model_shelf.meta["shelf_kind"] == "models2shelf"


def test_zero_fallback_by_construction(model_shelf: Shelf, events: pl.DataFrame) -> None:
    model_shelf.reset_coverage()
    ens = simulate_ensemble(model_shelf, n_sim=150, seed=7)
    assert ens["n_sim"] == 150
    assert model_shelf.fallback_rate() == 0.0  # every reachable key is modeled


def test_model_smooths_but_tracks_empirical(events: pl.DataFrame, model_shelf: Shelf) -> None:
    empirical = build_shelf(events)
    outcomes = events.filter(pl.col("kind") == "outcome")
    keyed = outcomes.with_columns(
        pl.struct(["score_diff", "period", "clock_seconds"])
        .map_elements(
            lambda s: __import__(
                "sportsdataverse.nba.nba_possession_sim.keygen", fromlist=["gamestate_key"]
            ).gamestate_key(s["score_diff"], s["period"], s["clock_seconds"]),
            return_dtype=pl.Utf8,
        )
        .alias("key")
    )
    key_counts = keyed.group_by("key").agg(pl.len().alias("n"))
    well_observed = key_counts.filter(pl.col("n") >= 10)["key"].to_list()
    assert well_observed, "fixture should have at least one dense gamestate"
    l1s = []
    for key in well_observed:
        emp = empirical.outcome_pmfs[key]
        mod = model_shelf.outcome_pmfs[key]
        l1s.append(sum(abs(emp[o] - mod[o]) for o in OUTCOMES))
    # per-key: an n~10-20 empirical PMF is itself noisy — the model must stay
    # in the same neighborhood, not memorize the noise (max possible L1 = 2)
    assert float(np.mean(l1s)) < 1.0
    # global: the count-weighted average of the model's PMFs over OBSERVED
    # keys must closely match the empirical global distribution — smoothing
    # redistributes within cells, it must not shift the overall mix
    weights = {row["key"]: row["n"] for row in key_counts.to_dicts()}
    total_n = sum(weights.values())
    blended = {o: sum(model_shelf.outcome_pmfs[k][o] * n for k, n in weights.items()) / total_n for o in OUTCOMES}
    global_l1 = sum(abs(blended[o] - empirical.all_pmf[o]) for o in OUTCOMES)
    assert global_l1 < 0.15


def test_model_shelf_distribution_within_reason(events: pl.DataFrame, model_shelf: Shelf) -> None:
    real_total_per_game = events.group_by("game_id").agg(pl.col("points").sum())["points"].mean()
    ens = simulate_ensemble(model_shelf, n_sim=150, seed=11)
    assert ens["mean_total"] == pytest.approx(float(real_total_per_game), rel=0.25)


def test_parquet_round_trip_preserves_node_models(model_shelf: Shelf, tmp_path: pathlib.Path) -> None:
    path = shelf_to_parquet(model_shelf, tmp_path / "model_shelf.parquet")
    loaded = shelf_from_parquet(path)
    assert loaded.outcome_pmfs == model_shelf.outcome_pmfs
    assert loaded.oreb_rates == model_shelf.oreb_rates
    assert loaded.meta["shelf_kind"] == "models2shelf"


def test_fit_functions_standalone(events: pl.DataFrame) -> None:
    outcome_model = fit_outcome_node_model(events)
    assert hasattr(outcome_model, "predict_proba")
    rebound_model = fit_rebound_node_model(events)
    assert rebound_model is not None
    with pytest.raises(ValueError, match="no outcome events"):
        fit_outcome_node_model(events.head(0))
