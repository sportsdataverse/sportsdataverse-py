"""Fit the player-prop distribution constants (Phase 5, Task 5.2), per league.

Run:

    uv run python tools/models/nba_prediction/fit_player_props.py [nba|wnba]

From ``player_box_logs_2024.parquet`` (NBA) or ``wnba_player_box_logs_2024.parquet`` (WNBA) it
estimates, over rotation players (>= 15 games, >= 15 mpg scaled by game length: 12.5 mpg for
40-minute WNBA games), each player's mean stat + realized single-game variance, then fits (a) the
points SD law ``sd = a + b*sqrt(mu)`` by least squares of per-player realized pts-SD on
sqrt(mean), and (b) the count over-dispersion ``var/mean`` for reb/ast/fg3m (median over players;
the shared value is the mean of the three). Paste ``pts_sd_a/pts_sd_b/count_dispersion`` into the
league's row of ``sportsdataverse/nba/nba_prediction_constants.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "nba_prediction"
LEAGUES = {"nba": ("player_box_logs_2024.parquet", 48), "wnba": ("wnba_player_box_logs_2024.parquet", 40)}


def fit(league: str = "nba") -> dict[str, float]:
    """Fit and print the league's prop constants; returns them rounded to 3 places."""
    fixture, game_minutes = LEAGUES[league]
    logs = pl.read_parquet(FIXTURE_DIR / fixture).drop_nulls("minutes")
    per = logs.group_by("player_id").agg(
        pl.len().alias("g"),
        pl.col("minutes").mean().alias("mpg"),
        pl.col("pts").mean().alias("pts_mu"),
        pl.col("pts").std().alias("pts_sd"),
        *[pl.col(s).mean().alias(f"{s}_mu") for s in ("reb", "ast", "fg3m")],
        *[pl.col(s).var().alias(f"{s}_var") for s in ("reb", "ast", "fg3m")],
    )
    min_mpg = 15.0 * game_minutes / 48
    rot = per.filter((pl.col("g") >= 15) & (pl.col("mpg") >= min_mpg)).drop_nulls("pts_sd")

    mu = rot["pts_mu"].to_numpy()
    sd = rot["pts_sd"].to_numpy()
    b, a = np.polyfit(np.sqrt(mu), sd, 1)  # sd = a + b*sqrt(mu)
    print(f"{league} points SD law: a={a:.3f} b={b:.3f}  (n={rot.height}, mpg >= {min_mpg:g})")
    disps = []
    for s in ("reb", "ast", "fg3m"):
        disp = float((rot[f"{s}_var"] / rot[f"{s}_mu"].clip(1e-6)).median())
        disps.append(disp)
        print(f"{s} dispersion (var/mean, median): {disp:.3f}")
    out = {
        "pts_sd_a": round(float(a), 3),
        "pts_sd_b": round(float(b), 3),
        "count_dispersion": round(float(np.mean(disps)), 2),
    }
    print(out)
    return out


if __name__ == "__main__":
    fit(sys.argv[1] if len(sys.argv) > 1 else "nba")
