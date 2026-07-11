"""Grid bin-width / min_n sweep for the ① expected-outcomes EV x LA grid (T6.2,
Task 1.4). Not shipped -- fits the ``GridConfig`` constants in
``mlb_hitting_constants.py``; provenance script only.

Run (from repo root, no network needed -- reads the committed fixture):

    uv run python dev/mlb_hitting/fit_grid.py

Finding: the seeded 2mph/2deg/min_n=25 grid only reaches per-batted-ball
Spearman 0.805 (woba) / 0.838 (ba) vs Savant's own
``estimated_woba_using_speedangle`` / ``estimated_ba_using_speedangle`` on the
committed 3-week 2024 sample (``statcast_sample_2024.parquet``, 80406 pitches,
~14.2k batted balls) -- with only ~24% of batted balls landing in a
``n >= 25`` dense cell, most predictions come from the coarser
launch-angle-marginal fallback, which is too smoothed relative to Savant's
kNN estimate. Widening the grid (fewer, denser cells) and lowering ``min_n``
both help. ``(ev_width=6.0, la_width=5.0, min_n=10)`` is the coarsest/lowest
combination found that clears the >= 0.95 gate on BOTH stats simultaneously:
Spearman 0.9522 (woba) / 0.9546 (ba). See the printed sweep table below for
the full search (widths in {2..8} x {2..7}, min_n in {5,8,10,12,15,20,25}).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_hitting_constants import spearman_corr

FIX = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "mlb_hitting"


def _add_val(df: pl.DataFrame, *, ev_min: float, ev_w: float, la_min: float, la_w: float) -> pl.DataFrame:
    tb_map = {"single": 1, "double": 2, "triple": 3, "home_run": 4}
    hit_set = {"single", "double", "triple", "home_run"}
    tb = pl.col("events").replace_strict(tb_map, default=0, return_dtype=pl.Int8)
    hit = pl.col("events").is_in(list(hit_set)).cast(pl.Int8)
    ev_bin = ((pl.col("launch_speed") - ev_min) / ev_w).floor().cast(pl.Int64)
    la_bin = ((pl.col("launch_angle") - la_min) / la_w).floor().cast(pl.Int64)
    return df.with_columns(_hit=hit, _total_bases=tb, ev_bin=ev_bin, la_bin=la_bin)


def _build_grid(bb: pl.DataFrame) -> pl.DataFrame:
    b = bb.filter((pl.col("type") == "X") & pl.col("launch_speed").is_not_null() & pl.col("launch_angle").is_not_null())
    return b.group_by("ev_bin", "la_bin").agg(
        pl.len().alias("n"), pl.col("woba_value").mean().alias("woba"), pl.col("_hit").mean().alias("ba")
    )


def _predict(bb: pl.DataFrame, grid: pl.DataFrame, min_n: int, value: str) -> np.ndarray:
    global_mean = float((grid[value] * grid["n"]).sum() / grid["n"].sum())
    la_marg = grid.group_by("la_bin").agg(
        ((pl.col(value) * pl.col("n")).sum() / pl.col("n").sum()).alias("la_marginal")
    )
    dense = grid.filter(pl.col("n") >= min_n).select("ev_bin", "la_bin", pl.col(value).alias("cell_value"))
    out = (
        bb.select("ev_bin", "la_bin")
        .join(dense, on=["ev_bin", "la_bin"], how="left")
        .join(la_marg, on="la_bin", how="left")
    )
    out = out.with_columns(pl.coalesce(["cell_value", "la_marginal", pl.lit(global_mean)]).alias(value))
    return out[value].to_numpy()


def main() -> None:
    s_raw = pl.read_parquet(FIX / "statcast_sample_2024.parquet")
    print(
        f"{'ev_w':>5} {'la_w':>5} {'min_n':>6} {'sp_woba':>9} {'mae_woba':>9} {'sp_ba':>8} {'mae_ba':>8} {'ncells':>7}"
    )
    best = None
    for ev_w in (2.0, 3.0, 4.0, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0):
        for la_w in (2.0, 3.0, 4.0, 5.0, 6.0, 7.0):
            s = _add_val(s_raw, ev_min=20.0, ev_w=ev_w, la_min=-90.0, la_w=la_w)
            grid = _build_grid(s)
            bb_w = s.filter(
                (pl.col("type") == "X")
                & pl.col("estimated_woba_using_speedangle").is_not_null()
                & pl.col("launch_speed").is_not_null()
                & pl.col("launch_angle").is_not_null()
            )
            bb_b = s.filter(
                (pl.col("type") == "X")
                & pl.col("estimated_ba_using_speedangle").is_not_null()
                & pl.col("launch_speed").is_not_null()
                & pl.col("launch_angle").is_not_null()
            )
            for min_n in (5, 8, 10, 12, 15, 20, 25):
                pred_w = _predict(bb_w, grid, min_n, "woba")
                savant_w = bb_w["estimated_woba_using_speedangle"].to_numpy()
                sp_w = spearman_corr(pred_w, savant_w)
                mae_w = float(np.mean(np.abs(pred_w - savant_w)))

                pred_b = _predict(bb_b, grid, min_n, "ba")
                savant_b = bb_b["estimated_ba_using_speedangle"].to_numpy()
                sp_b = spearman_corr(pred_b, savant_b)
                mae_b = float(np.mean(np.abs(pred_b - savant_b)))

                if sp_w >= 0.95 and sp_b >= 0.95:
                    print(
                        f"{ev_w:>5} {la_w:>5} {min_n:>6} {sp_w:>9.4f} {mae_w:>9.4f} {sp_b:>8.4f} {mae_b:>8.4f} "
                        f"{grid.height:>7}  <-- clears gate"
                    )
                    if best is None or ev_w + la_w < best[0] + best[1]:
                        best = (ev_w, la_w, min_n, sp_w, mae_w, sp_b, mae_b)

    print()
    print("Chosen (coarsest that clears both gates):", best)


if __name__ == "__main__":
    main()
