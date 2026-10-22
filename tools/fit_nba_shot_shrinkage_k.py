# tools/fit_nba_shot_shrinkage_k.py
"""Fit the NBA shooter-talent shrinkage k on the 2022-23 fixture (split-half).

Splits each player's shots by within-player row parity, computes raw
make%-above-expected in each half, and picks k minimizing cross-half MSE of
the k-regressed first half vs the raw second half. The printed k is
``TALENT_SHRINKAGE_K["00"]`` (G League reuses it) in
``sportsdataverse/nba/nba_shot_value_constants.py``. The WNBA entry (``"10"``)
is a seeded placeholder: no WNBA shot fixture is committed, so this script
cannot fit it.

Moved verbatim (wrapped in ``main``) from its untracked home,
``dev/nba_shot_value/fit_shrinkage_k.py`` (2026-07-08); it reads only the
committed ``tests/fixtures/nba_shot_value`` captures.

Usage::

    uv run python tools/fit_nba_shot_shrinkage_k.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
from scipy.optimize import minimize_scalar

from sportsdataverse.nba.nba_shot_value import score_shot_xpoints

FIX = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "nba_shot_value"


def main() -> float:
    shots = pl.read_parquet(FIX / "shotchart_2023.parquet")
    la = pl.read_parquet(FIX / "league_averages_2023.parquet")
    scored = score_shot_xpoints(shots, la).with_columns((pl.int_range(pl.len()).over("player_id") % 2).alias("half"))

    def half_stats(h: int) -> pl.DataFrame:
        return (
            scored.filter(pl.col("half") == h)
            .group_by("player_id")
            .agg(
                pl.len().alias("n"),
                ((pl.col("shot_made_flag").sum() - pl.col("base_fg_pct").sum()) / pl.len()).alias("raw"),
            )
        )

    a = half_stats(0).join(half_stats(1), on="player_id", suffix="_b")
    a = a.filter((pl.col("n") >= 25) & (pl.col("n_b") >= 25))
    n1, r1, r2 = a["n"].to_numpy(), a["raw"].to_numpy(), a["raw_b"].to_numpy()

    def cross_half_mse(k: float) -> float:
        shrunk = r1 * n1 / (n1 + k)
        return float(np.mean((shrunk - r2) ** 2))

    res = minimize_scalar(cross_half_mse, bounds=(1.0, 1000.0), method="bounded")
    k = float(res.x)
    shrunk = r1 * n1 / (n1 + k)
    print(f"players in split: {len(n1)}")
    print(f"fitted k = {k:.1f}")
    print(f"reliability raw    r = {np.corrcoef(r1, r2)[0, 1]:.4f}")
    print(f"reliability shrunk r = {np.corrcoef(shrunk, r2)[0, 1]:.4f}")
    return k


if __name__ == "__main__":
    main()
