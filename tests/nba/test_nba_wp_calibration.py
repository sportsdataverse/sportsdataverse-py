"""Calibration-at-scale gates for the tabulated WP surface.

The self-calibration walk is the Markov self-consistency check: a surface
tabulated from the engine's own paths MUST be calibrated on held-out paths,
so the reliability gate catches binning/tabulation/shrinkage bugs (it
already caught raw tabulation's regression-to-the-mean overconfidence).
The realized fixture games then anchor the surface against real score
paths, and the committed artifact pins both curves exactly (drift gate).
Thresholds were pinned from observed values — never lower them.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    fit_wp_surface,
    held_out_calibration,
    real_path_snapshots,
    simulate_score_paths,
)
from tools.calibration import build as calibration_build

ARTIFACT_PATH = pathlib.Path("tests/fixtures/calibration/nba_wp_calibration.json")


@pytest.fixture(scope="module")
def sim():
    raw = calibration_build.fixture_raw()
    shelf = calibration_build.fixture_shelf(raw)
    artifact = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
    result = held_out_calibration(shelf, **{k: v for k, v in calibration_build.PARAMS.items()})
    return shelf, raw, artifact, result


def test_held_out_self_calibration(sim) -> None:
    _shelf, _raw, artifact, result = sim
    assert result.baseline.beat_baseline
    assert result.score < 0.17  # observed 0.1653
    big = result.calibration.filter(pl.col("n") >= 400)
    assert big.height >= 8
    gap = float((big["mean_pred"] - big["mean_actual"]).abs().max())
    assert gap <= 0.05  # observed 0.0367
    # the public API reproduces the committed artifact's numbers exactly
    assert round(result.score, 10) == artifact["self"]["brier"]
    assert result.n == artifact["self"]["n_snapshots"]


def test_surface_sanity_and_fallback(sim) -> None:
    shelf, _raw, _artifact, _result = sim
    paths = simulate_score_paths(shelf, n_sim=120, seed=7)
    surface = fit_wp_surface(paths)
    # buzzer states saturate
    assert surface.predict(30.0, 10) > 0.95
    assert surface.predict(30.0, -10) < 0.05
    # the drift-diffusion prior is strictly monotone in margin
    priors = [surface.prior(1440.0, m) for m in range(-16, 17, 4)]
    assert all(a < b for a, b in zip(priors, priors[1:]))
    # shrunk cells stay (weakly) monotone at mid-game
    preds = [surface.predict(1440.0, m) for m in (-16, -8, 0, 8, 16)]
    assert all(b - a > -0.02 for a, b in zip(preds, preds[1:]))
    # a state no cell covers falls back to the prior exactly
    t_bucket = int(2879.0 // surface.time_bin_seconds)
    assert (t_bucket, 24) not in surface.cells
    assert surface.predict(2879.0, 24) == surface.prior(2879.0, 24)


def test_validation_errors(sim) -> None:
    shelf, _raw, _artifact, _result = sim
    with pytest.raises(ValueError, match="n_sim"):
        simulate_score_paths(shelf, n_sim=0)
    with pytest.raises(ValueError, match="missing columns"):
        fit_wp_surface(pl.DataFrame({"path_id": [1]}))
    paths = simulate_score_paths(shelf, n_sim=2, seed=7)
    with pytest.raises(ValueError, match="empty"):
        fit_wp_surface(paths.head(0))
    with pytest.raises(ValueError, match="time_bin_seconds"):
        fit_wp_surface(paths, time_bin_seconds=0.0)
    with pytest.raises(ValueError, match="shrinkage"):
        fit_wp_surface(paths, shrinkage=-1.0)
    with pytest.raises(ValueError, match="n_train"):
        held_out_calibration(shelf, n_train=0, n_eval=1)
    with pytest.raises(ValueError, match="missing columns"):
        real_path_snapshots(pl.DataFrame({"game_id": ["x"]}))


def test_real_fixture_paths(sim) -> None:
    shelf, raw, artifact, _result = sim
    snaps = real_path_snapshots(raw)
    assert snaps.columns == ["path_id", "seconds_remaining", "margin", "home_win"]
    assert snaps["path_id"].n_unique() == 3
    assert snaps.height == artifact["real"]["n_snapshots"]
    assert artifact["real"]["beat_baseline"] is True
    assert artifact["real"]["brier"] < 0.15  # observed 0.1415
    # decided endgames price the realized winner
    for final in artifact["real"]["finals"]:
        assert final["home_win"] is True
        assert final["pred"] >= 0.99  # observed >= 0.9993
    # margins forward-fill through non-scoring rows: never null
    assert snaps["margin"].null_count() == 0


def test_determinism_and_seed_sensitivity(sim) -> None:
    shelf, _raw, _artifact, _result = sim
    a = held_out_calibration(shelf, n_train=30, n_eval=15, seed=7)
    b = held_out_calibration(shelf, n_train=30, n_eval=15, seed=7)
    assert a.score == b.score
    assert np.array_equal(a.predictions["prediction"].to_numpy(), b.predictions["prediction"].to_numpy())
    c = held_out_calibration(shelf, n_train=30, n_eval=15, seed=11)
    assert c.score != a.score


def test_committed_artifact_matches_rebuild(sim) -> None:
    _shelf, _raw, artifact, _result = sim
    rebuilt = calibration_build.build_artifact()
    assert rebuilt == artifact, (
        "calibration artifact drifted from the engine/surface; regenerate deliberately: "
        "run: uv run python -m tools.calibration.build"
    )


def test_artifact_carries_a_registry_card(sim) -> None:
    _shelf, _raw, artifact, _result = sim
    card = artifact["card"]
    assert card["name"] == "nba_wp_surface"
    assert card["source_features"] == ["path_id", "seconds_remaining", "margin", "home_win"]
    assert sorted(card["feature_fingerprint"]) == sorted(card["source_features"])
    assert card["trained_seasons"] == [2021, 2022, 2023]
    assert card["metrics"]["self_brier"] == artifact["self"]["brier"]
    assert card["metrics"]["real_brier"] == artifact["real"]["brier"]
    assert card["metrics"]["n_cells"] == float(artifact["surface"]["n_cells"])
