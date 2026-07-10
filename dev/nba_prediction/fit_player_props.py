"""Fit the player-prop distribution constants (Phase 5, Task 5.2).

Gitignored working script. Run:

    uv run python dev/nba_prediction/fit_player_props.py

From ``player_box_logs_2024.parquet`` it estimates, over rotation players
(>= 15 games, >= 15 mpg), each player's mean stat + realized single-game
variance, then fits (a) the points SD law ``sd = a + b*sqrt(mu)`` by least
squares of per-player realized pts-SD on sqrt(mean), and (b) the mean count
over-dispersion ``var/mean`` for reb/ast/fg3m. Paste ``_PTS_SD_A/_B`` and
``_COUNT_DISPERSION`` into ``sportsdataverse/nba/nba_player_props.py``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_prediction"


def fit() -> None:
    logs = pl.read_parquet(FIXTURE_DIR / "player_box_logs_2024.parquet").drop_nulls("minutes")
    per = logs.group_by("player_id").agg(
        pl.len().alias("g"),
        pl.col("minutes").mean().alias("mpg"),
        pl.col("pts").mean().alias("pts_mu"),
        pl.col("pts").std().alias("pts_sd"),
        *[pl.col(s).mean().alias(f"{s}_mu") for s in ("reb", "ast", "fg3m")],
        *[pl.col(s).var().alias(f"{s}_var") for s in ("reb", "ast", "fg3m")],
    )
    rot = per.filter((pl.col("g") >= 15) & (pl.col("mpg") >= 15.0)).drop_nulls("pts_sd")

    mu = rot["pts_mu"].to_numpy()
    sd = rot["pts_sd"].to_numpy()
    b, a = np.polyfit(np.sqrt(mu), sd, 1)  # sd = a + b*sqrt(mu)
    print(f"points SD law: a={a:.3f} b={b:.3f}  (n={rot.height})")

    for s in ("reb", "ast", "fg3m"):
        disp = float((rot[f"{s}_var"] / rot[f"{s}_mu"].clip(1e-6)).median())
        print(f"{s} dispersion (var/mean, median): {disp:.3f}")


if __name__ == "__main__":
    fit()
