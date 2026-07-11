"""Build the coarse full-season 2024 reference EV x LA grid (T6.2, Task 0.1 step 1
addendum -- ordering note: run this AFTER Task 1.1 lands, since it calls
``mlb_expected_stats.build_outcome_grid``).

Run (from repo root, network required):

    SDV_PY_LIVE_TESTS=1 PYTHONIOENCODING=utf-8 uv run python dev/mlb_hitting/build_reference_grid.py

Produces ``tests/fixtures/mlb_hitting/reference_grid_2024.parquet`` -- the
coarse full-season 2024 EV x LA grid, used by the offline oracle gate so it
never needs to rebuild the grid over a full-season live pull.
"""

from __future__ import annotations

import sys
from pathlib import Path

from sportsdataverse.mlb.mlb_expected_stats import _add_value_columns, build_outcome_grid
from sportsdataverse.mlb.mlb_hitting_constants import pull_statcast_season

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "mlb_hitting"


def main() -> None:
    print("Pulling full 2024 season batter Statcast (date-chunked) ...")
    season = pull_statcast_season(2024)
    print("full season pull shape =", season.shape)
    assert season.height > 500_000, f"expected a full season > 500k pitches, got {season.height}"

    grid = build_outcome_grid(_add_value_columns(season))
    print("reference_grid_2024 shape =", grid.shape)
    grid.write_parquet(FIXTURES / "reference_grid_2024.parquet")
    print("Wrote reference_grid_2024.parquet")


if __name__ == "__main__":
    sys.exit(main())
