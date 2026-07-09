"""Build the Task 4.2 player-props backtest fixtures from real season 2024
skater boxscores (`load_nhl_skater_boxscores` only publishes seasons >= 2024
-- season 2024 == the 2023-24 season; see the fixtures README).

Run once (downloads full 2024 pbp/schedule/skater-boxscores; no live-API
gate needed):

    uv run python dev/nhl_prediction/build_player_props_backtest_fixture.py

Writes:
  tests/fixtures/nhl_prediction/player_props_mae_2024.parquet
    (stat, mae, n -- MAE(proj_mean, realized) per stat family)
  tests/fixtures/nhl_prediction/player_props_p_over_calibration_2024.parquet
    (stat, bin_mid, mean_pred, mean_actual, n -- p_over calibration against a
    fixed per-stat-family "line" = the stat's overall median, since ESPN
    propbets lines are confirmed unavailable for NHL historical games)
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nhl.nhl_loaders import load_nhl_skater_boxscores
from sportsdataverse.nhl.nhl_player_props import _p_over, nhl_player_props
from sportsdataverse.nhl.nhl_prediction_constants import calibration_table, mae

SEASON = 2024
FIXTURES_DIR = "tests/fixtures/nhl_prediction"
STAT_COLUMN = {"shots": "shots_on_goal", "points": "points"}


def main() -> None:
    print(f"Computing nhl_player_props({SEASON}) as-of backtest (real network)...")
    props = nhl_player_props(SEASON, stats=("shots", "points"))
    print(f"  -> {props.shape}")

    print(f"Loading season {SEASON} realized skater boxscores...")
    box = load_nhl_skater_boxscores([SEASON])
    box = box.with_columns(
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8), pl.col("player_id").cast(pl.Int64).cast(pl.Utf8)
    )

    mae_rows = []
    cal_frames = []
    for stat, col in STAT_COLUMN.items():
        realized = box.select(pl.col("game_id"), pl.col("player_id"), pl.col(col).cast(pl.Float64).alias("realized"))
        stat_props = props.filter(pl.col("stat") == stat)
        m = stat_props.join(realized, on=["game_id", "player_id"], how="inner")
        print(f"{stat}: matched {m.height} player-games")

        err = mae(m["proj_mean"].to_numpy(), m["realized"].to_numpy())
        mae_rows.append({"stat": stat, "mae": err, "n": m.height})
        print(f"  MAE(proj_mean, realized) = {err:.4f}")

        # Synthetic fixed "line" = the stat's overall median + 0.5 (ESPN
        # propbets lines confirmed unavailable for NHL historical games --
        # see README). The +0.5 half-integer offset avoids exact ties with
        # the realized integer count at the median (a real prop line is
        # always a half-integer for exactly this reason).
        line = float(m["realized"].median()) + 0.5
        p_over = np.array([_p_over(mean, line) for mean in m["proj_mean"].to_list()])
        actual_over = (m["realized"].to_numpy() > line).astype(float)
        cal = calibration_table(actual_over, p_over, n_bins=10).with_columns(pl.lit(stat).alias("stat"))
        cal_frames.append(cal)
        print(f"  synthetic line={line}, calibration table:")
        print(cal)

    pl.DataFrame(mae_rows).write_parquet(f"{FIXTURES_DIR}/player_props_mae_2024.parquet")
    pl.concat(cal_frames, how="vertical_relaxed").write_parquet(
        f"{FIXTURES_DIR}/player_props_p_over_calibration_2024.parquet"
    )
    print("Wrote fixtures.")


if __name__ == "__main__":
    main()
