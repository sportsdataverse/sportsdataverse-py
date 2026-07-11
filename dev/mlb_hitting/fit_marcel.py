"""Marcel regression-constant backtest sweep (T6.2, Task 4.3, Step 2). Not
shipped -- fits ``DEFAULT_REGRESSION_PA`` in ``mlb_batter_projection.py``;
provenance script only.

Run (from repo root, network required -- pulls 4 full MLB seasons):

    SDV_PY_LIVE_TESTS=1 PYTHONIOENCODING=utf-8 uv run python dev/mlb_hitting/fit_marcel.py

Builds player-season xwOBA history for 2021-2024 via ``mlb_expected_stats``
(one full-season pull per year), then for each candidate ``regression_pa``
projects 2024 from the as-of (2021-2023) history and measures out-of-sample
error against the REALIZED 2024 xwOBA (from the same 2024
``mlb_expected_stats`` pull -- not the Savant leaderboard, so the comparison
is apples-to-apples with the model's own metric). Prints the sweep table and
the chosen ``regression_pa`` + observed ``league_xwoba``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mlb.mlb_batter_projection import aging_curve, marcel_projection
from sportsdataverse.mlb.mlb_expected_stats import mlb_expected_stats
from sportsdataverse.mlb.mlb_hitting_constants import as_of_seasons_split, mae, spearman_corr

CACHE = Path(__file__).parent / "_marcel_history_cache.parquet"


def _load_or_pull_history() -> pl.DataFrame:
    if CACHE.exists():
        print(f"Loading cached history from {CACHE}")
        return pl.read_parquet(CACHE)

    frames = []
    for yr in (2021, 2022, 2023, 2024):
        print(f"Pulling {yr} full-season batter Statcast for expected-stats history ...")
        df = mlb_expected_stats(f"{yr}-01-01", f"{yr}-12-01")
        print(f"  {yr}: {df.height} player-seasons")
        frames.append(df)
    history = pl.concat(frames, how="diagonal_relaxed")
    # NOTE: mlb_expected_stats doesn't carry age -- the backtest here validates
    # the REGRESSION strength in isolation (age_adj=0 for every projection,
    # since no age join is available offline); the shipped default still
    # applies the fitted aging curve when real ages are supplied by the caller.
    history = history.with_columns(pl.lit(0).cast(pl.Int64).alias("age"))
    history.write_parquet(CACHE)
    return history


def main() -> None:
    history = _load_or_pull_history()
    asof = as_of_seasons_split(history, 2024)
    realized = history.filter(pl.col("season") == 2024).select("batter", pl.col("xwoba").alias("realized_xwoba"))
    print(f"as-of (2021-2023) player-seasons: {asof.height}; realized 2024 player-seasons: {realized.height}")

    aging = aging_curve(asof, min_pa=100)  # degenerate (age=0 placeholder) -- isolates the regression term

    naive = (
        asof.sort("batter", "season")
        .group_by("batter")
        .agg(pl.col("xwoba").last().alias("last_season_xwoba"))
        .join(realized, on="batter", how="inner")
    )
    naive_corr = spearman_corr(naive["last_season_xwoba"].to_numpy(), naive["realized_xwoba"].to_numpy())
    naive_mae = mae(naive["last_season_xwoba"].to_numpy(), naive["realized_xwoba"].to_numpy())
    print(f"naive (last season = projection) baseline: spearman={naive_corr:.4f} mae={naive_mae:.4f}")

    print(f"{'regression_pa':>14} {'spearman':>10} {'mae':>8}")
    best = None
    for reg_pa in (200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1500.0, 2000.0, 3000.0):
        proj = marcel_projection(asof, 2024, aging, regression_pa=reg_pa)
        joined = proj.join(realized, on="batter", how="inner")
        sp = spearman_corr(joined["proj_xwoba"].to_numpy(), joined["realized_xwoba"].to_numpy())
        m = mae(joined["proj_xwoba"].to_numpy(), joined["realized_xwoba"].to_numpy())
        print(f"{reg_pa:>14.0f} {sp:>10.4f} {m:>8.4f}")
        if best is None or m < best[1]:
            best = (reg_pa, m, sp)

    league_xwoba = float((asof["xwoba"] * asof["pa"]).sum() / asof["pa"].sum())
    print()
    print(f"Chosen regression_pa (min OOS MAE): {best[0]:.0f} (mae={best[1]:.4f}, spearman={best[2]:.4f})")
    print(f"Observed league_xwoba (2021-2023, PA-weighted): {league_xwoba:.4f}")
    print(f"Naive baseline for comparison: spearman={naive_corr:.4f} mae={naive_mae:.4f}")


if __name__ == "__main__":
    main()
