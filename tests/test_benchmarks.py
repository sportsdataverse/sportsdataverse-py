"""Benchmark-harness gates.

The registry/smoke test always runs; the throughput floors are wall-clock
and machine-dependent, so they run only under ``SDV_PY_BENCH=1``.
"""

from __future__ import annotations

import os

import pytest

from tools.benchmarks import bench_sims


def test_registry_and_smoke() -> None:
    assert set(bench_sims.BENCHMARKS) == set(bench_sims.FLOORS)
    row = bench_sims.run_benchmark("mlb_game", reps=1, scale=0.01)
    assert row["units"] >= 1 and row["units_per_s"] > 0
    assert row["unit"] == "games"


@pytest.mark.skipif(not os.environ.get("SDV_PY_BENCH"), reason="set SDV_PY_BENCH=1 to run wall-clock floors")
@pytest.mark.parametrize("name", sorted(bench_sims.BENCHMARKS))
def test_throughput_floors(name: str) -> None:
    row = bench_sims.run_benchmark(name, reps=2)
    assert row["units_per_s"] > bench_sims.FLOORS[name], row
