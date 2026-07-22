"""Sim-engine throughput benchmarks — stdlib timing, no new deps.

Un-blocks the WS6 perf-benchmark item without adding a benchmarking
dependency: every benchmark builds its inputs from the committed REAL
capture fixtures (the same payloads the engine gates run on), warms up
once, then times ``reps`` runs of a fixed batch and reports units/second.

Usage::

    uv run python -m tools.benchmarks.bench_sims
    uv run python -m tools.benchmarks.bench_sims --reps 5 --json dev/bench.json

Wall-clock throughput is machine-dependent by nature, so the committed
tests only validate the registry and (behind ``SDV_PY_BENCH=1``) enforce
generous catastrophic-regression floors — an order of magnitude under the
observed dev-box numbers, not perf targets.
"""

from __future__ import annotations

import argparse
import json
import platform
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import polars as pl

FIXTURES = Path("tests/fixtures")

#: Units/sec floors for the env-gated regression test — set ~20-30x under
#: the 2026-07 dev-box observations (326/119/653/6320/301/22), so only a
#: catastrophic slowdown trips them.
FLOORS: Dict[str, float] = {
    "basketball_classify": 10.0,
    "basketball_game_render": 4.0,
    "football_game_pbp": 20.0,
    "mlb_game": 200.0,
    "nhl_game_pbp": 10.0,
    "mixed_effects_fit": 1.0,
}


def _read(rel: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


def _v3_raw() -> pl.DataFrame:
    frames = []
    for gid in ("0022100001", "0022200001", "0022300001"):
        payload = _read(f"nba_engine/{gid}/playbyplayv3.json")
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return pl.concat(frames, how="diagonal_relaxed")


def _setup_basketball_classify(scale: float) -> Tuple[Callable[[], None], float, str]:
    from sportsdataverse.nba.nba_possession_sim import possessions_from_pbp

    raw = _v3_raw()

    def run() -> None:
        possessions_from_pbp(raw)

    return run, 3.0, "games classified"


def _setup_basketball_game_render(scale: float) -> Tuple[Callable[[], None], float, str]:
    from sportsdataverse.nba.nba_possession_sim import PlayerAttribution, build_shelf, possessions_from_pbp
    from sportsdataverse.nba.nba_possession_sim.expanded_nodes import aux_params_from_pbp
    from sportsdataverse.nba.nba_possession_sim.render import player_names_from_pbp, simulate_game_actions
    from sportsdataverse.nba.nba_possession_sim.shelf import player_game_logs_from_pbp

    raw = _v3_raw()
    shelf = build_shelf(possessions_from_pbp(raw))
    shelf.aux = aux_params_from_pbp(raw)
    logs = player_game_logs_from_pbp(raw)
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1])
    names = player_names_from_pbp(raw)
    n = max(1, int(3 * scale))

    def run() -> None:
        rng = np.random.default_rng(7)
        for _ in range(n):
            simulate_game_actions(shelf, att, names, rng)

    return run, float(n), "rendered games"


def _setup_football_game_pbp(scale: float) -> Tuple[Callable[[], None], float, str]:
    from sportsdataverse.nfl.nfl_drive_sim import (
        build_football_shelf,
        plays_from_espn_drives,
        simulate_football_game_pbp,
    )

    shelf = build_football_shelf(plays_from_espn_drives(_read("espn/summary_nfl.json")))
    n = max(1, int(10 * scale))

    def run() -> None:
        rng = np.random.default_rng(7)
        for _ in range(n):
            simulate_football_game_pbp(shelf, rng)

    return run, float(n), "games"


def _setup_mlb_game(scale: float) -> Tuple[Callable[[], None], float, str]:
    from sportsdataverse.mlb.mlb_at_bat_sim import at_bats_from_pbp, build_at_bat_pmf, simulate_mlb_game

    pmf = build_at_bat_pmf(at_bats_from_pbp(_read("mlb_api/play_by_play_745282.json")))
    n = max(1, int(100 * scale))

    def run() -> None:
        rng = np.random.default_rng(7)
        for _ in range(n):
            simulate_mlb_game(pmf, rng)

    return run, float(n), "games"


def _setup_nhl_game_pbp(scale: float) -> Tuple[Callable[[], None], float, str]:
    from sportsdataverse.nhl.nhl_game_sim import build_nhl_shelf, events_from_nhl_pbp, simulate_nhl_game_pbp

    shelf = build_nhl_shelf(events_from_nhl_pbp(_read("nhl_api_web/pbp_2024_scf_g7.json")))
    n = max(1, int(20 * scale))

    def run() -> None:
        rng = np.random.default_rng(7)
        for _ in range(n):
            simulate_nhl_game_pbp(shelf, rng)

    return run, float(n), "games"


def _setup_mixed_effects_fit(scale: float) -> Tuple[Callable[[], None], float, str]:
    from sportsdataverse._common.mixed_effects import fit_random_intercepts
    from sportsdataverse.nba.nba_possession_sim.shelf import player_game_logs_from_pbp

    logs = player_game_logs_from_pbp(_v3_raw())
    n = max(1, int(5 * scale))

    def run() -> None:
        for _ in range(n):
            fit_random_intercepts(logs, response="pts", group="player_id")

    return run, float(n), "fits"


BENCHMARKS: Dict[str, Callable[[float], Tuple[Callable[[], None], float, str]]] = {
    "basketball_classify": _setup_basketball_classify,
    "basketball_game_render": _setup_basketball_game_render,
    "football_game_pbp": _setup_football_game_pbp,
    "mlb_game": _setup_mlb_game,
    "nhl_game_pbp": _setup_nhl_game_pbp,
    "mixed_effects_fit": _setup_mixed_effects_fit,
}


def run_benchmark(name: str, *, reps: int = 3, scale: float = 1.0) -> Dict[str, Any]:
    """Time one benchmark and return its result row.

    Args:
        name: A :data:`BENCHMARKS` key.
        reps: Timed repetitions (after one warmup run).
        scale: Batch-size multiplier (the smoke test uses a small one).

    Returns:
        Dict with ``name``, ``unit``, ``units``, ``reps``, ``units_per_s``
        (best rep), ``ms_per_unit``, and per-rep ``seconds``.
    """
    run, units, unit = BENCHMARKS[name](scale)
    run()  # warmup
    seconds: List[float] = []
    for _ in range(reps):
        start = time.perf_counter()
        run()
        seconds.append(time.perf_counter() - start)
    best = min(seconds)
    return {
        "name": name,
        "unit": unit,
        "units": units,
        "reps": reps,
        "seconds": [round(s, 4) for s in seconds],
        "units_per_s": round(units / best, 2) if best > 0 else float("inf"),
        "ms_per_unit": round(1000.0 * best / units, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="sdv-py sim throughput benchmarks")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--json", type=str, default=None, help="write results to this path")
    parser.add_argument("--only", nargs="*", default=None, help="benchmark names to run")
    args = parser.parse_args()
    names = args.only or list(BENCHMARKS)
    results = []
    print(f"{'benchmark':26} {'units/s':>10} {'ms/unit':>10}  unit")
    for name in names:
        row = run_benchmark(name, reps=args.reps, scale=args.scale)
        results.append(row)
        print(f"{row['name']:26} {row['units_per_s']:>10} {row['ms_per_unit']:>10}  {row['unit']}")
    if args.json:
        payload = {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "numpy": np.__version__,
            "polars": pl.__version__,
            "results": results,
        }
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json).write_text(json.dumps(payload, indent=1), encoding="utf-8")
        print(f"wrote {args.json}")


if __name__ == "__main__":
    main()
