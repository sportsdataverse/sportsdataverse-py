"""Build the committed WP calibration-at-scale artifact.

Walks the NBA possession engine's simulated score paths through the
tabulated WP surface twice — held-out self-calibration (the Markov
self-consistency check) and the realized fixture games' score paths — and
writes both reliability curves plus the Brier/beat-baseline numbers to
``tests/fixtures/calibration/nba_wp_calibration.json``. Everything is
seeded and deterministic, so the committed artifact is an exact-equality
drift gate: regenerate deliberately with
``uv run python -m tools.calibration.build``.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import Any, Dict, List

import polars as pl

from sportsdataverse.modeling.eval import BacktestResult, backtest
from sportsdataverse.modeling.registry.model_registry import make_card
from sportsdataverse.nba.nba_possession_sim import (
    build_shelf,
    fit_wp_surface,
    possessions_from_pbp,
    real_path_snapshots,
    simulate_score_paths,
)
from sportsdataverse.nba.nba_possession_sim.expanded_nodes import aux_params_from_pbp
from sportsdataverse.nba.nba_possession_sim.shelf import Shelf
from sportsdataverse.nba.nba_possession_sim.wp_surface import WPSurface

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT_PATH = REPO_ROOT / "tests" / "fixtures" / "calibration" / "nba_wp_calibration.json"

#: The pinned capture games the shelf is built from (same trio as the
#: rendered sim fixtures).
GAME_IDS = ("0022100001", "0022200001", "0022300001")

#: Pinned artifact parameters — the drift test rebuilds with exactly these.
PARAMS: Dict[str, Any] = {
    "n_train": 240,
    "n_eval": 160,
    "seed": 7,
    "time_bin_seconds": 60.0,
    "margin_cap": 24,
    "shrinkage": 20.0,
}

_ROUND = 10


def _rounded(value: Any) -> Any:
    """Round every float in a JSON-shaped structure to a stable precision."""
    if isinstance(value, float):
        return round(value, _ROUND)
    if isinstance(value, dict):
        return {key: _rounded(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_rounded(item) for item in value]
    return value


def fixture_raw() -> pl.DataFrame:
    """The three committed playbyplayv3 captures as one raw actions frame."""
    frames = []
    for game_id in GAME_IDS:
        path = REPO_ROOT / "tests" / "fixtures" / "nba_engine" / game_id / "playbyplayv3.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        actions = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(actions, infer_schema_length=None).with_columns(pl.lit(game_id).alias("game_id")))
    return pl.concat(frames, how="diagonal_relaxed")


def fixture_shelf(raw: pl.DataFrame) -> Shelf:
    """The PMF shelf the fixtures pin (aux params included)."""
    shelf = build_shelf(possessions_from_pbp(raw))
    shelf.aux = aux_params_from_pbp(raw)
    return shelf


def _score_snapshots(surface: WPSurface, snapshots: pl.DataFrame) -> BacktestResult:
    units = list(snapshots.iter_rows(named=True))
    return backtest(
        units,
        lambda unit: surface.predict(unit["seconds_remaining"], unit["margin"]),
        lambda unit: 1.0 if unit["home_win"] else 0.0,
        metric="brier",
        label_fn=lambda unit: unit["path_id"],
    )


def _result_block(result: BacktestResult) -> Dict[str, Any]:
    reliability: List[Dict[str, Any]] = [] if result.calibration is None else result.calibration.to_dicts()
    return {
        "brier": result.score,
        "baseline_brier": result.baseline.baseline_metric,
        "beat_baseline": result.baseline.beat_baseline,
        "n_snapshots": result.n,
        "reliability": reliability,
    }


def build_artifact() -> Dict[str, Any]:
    """Deterministically rebuild the calibration artifact payload."""
    raw = fixture_raw()
    shelf = fixture_shelf(raw)
    n_train, n_eval = int(PARAMS["n_train"]), int(PARAMS["n_eval"])

    paths = simulate_score_paths(shelf, n_sim=n_train + n_eval, seed=int(PARAMS["seed"]))
    surface = fit_wp_surface(
        paths.filter(pl.col("path_id") < n_train),
        time_bin_seconds=float(PARAMS["time_bin_seconds"]),
        margin_cap=int(PARAMS["margin_cap"]),
        shrinkage=float(PARAMS["shrinkage"]),
    )
    self_result = _score_snapshots(surface, paths.filter(pl.col("path_id") >= n_train))

    real_snaps = real_path_snapshots(raw)
    real_result = _score_snapshots(surface, real_snaps)
    finals = real_snaps.group_by("path_id", maintain_order=True).last()
    final_rows = [
        {
            "game_id": row["path_id"],
            "margin": row["margin"],
            "home_win": row["home_win"],
            "pred": surface.predict(row["seconds_remaining"], row["margin"]),
        }
        for row in finals.iter_rows(named=True)
    ]

    # registry card: fingerprint the train paths so an engine change shows
    # up as named drifted input columns, not just a different artifact
    card = make_card(
        "nba_wp_surface",
        paths.filter(pl.col("path_id") < n_train),
        features=["path_id", "seconds_remaining", "margin", "home_win"],
        training_script="tools/calibration/build.py",
        trained_seasons=[2000 + int(game_id[3:5]) for game_id in GAME_IDS],
        metrics={
            "self_brier": self_result.score,
            "real_brier": real_result.score,
            "n_cells": float(len(surface.cells)),
        },
    )

    artifact = {
        "params": {**PARAMS, "games": list(GAME_IDS)},
        "surface": {
            "n_cells": len(surface.cells),
            "n_paths": surface.n_paths,
            "drift_mu": surface.drift_mu,
            "drift_sigma": surface.drift_sigma,
            "total_seconds": surface.total_seconds,
        },
        "self": _result_block(self_result),
        "real": {**_result_block(real_result), "finals": final_rows},
        "card": dataclasses.asdict(card),
    }
    return _rounded(artifact)


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    artifact = build_artifact()
    OUT_PATH.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(REPO_ROOT)}")
    print(
        "self brier={self[brier]} beat={self[beat_baseline]} | real brier={real[brier]} beat={real[beat_baseline]}".format(
            **artifact
        )
    )


if __name__ == "__main__":
    main()
