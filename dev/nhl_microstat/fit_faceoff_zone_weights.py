"""Fit `faceoff_zone_weights` (Task 1.4) from post-faceoff xG on the corpus.

For each faceoff, sum the xG (via `fit_shot_xg`) of every shot taken BY THE
FACEOFF-WINNING TEAM within `entry_window_s` seconds after it (shots by the
opponent reflect the opponent's ensuing possession, not the value the
winning team gained -- an early version of this script didn't filter by
team and got a backwards N > O ranking as a result). The per-zone value is
the SUM of that xG averaged over EVERY faceoff of that zone (including ones
with zero shots in the window) -- not the mean xG-per-shot, which would
reward zones with fewer but higher-quality shots (e.g. neutral-zone rush
chances) over zones that generate more total shot volume (e.g. sustained
O-zone pressure). Normalize to the O-zone value -> the zone-weight ratio.

Run: uv run python dev/nhl_microstat/fit_faceoff_zone_weights.py
"""

from __future__ import annotations

import os
import sys

import polars as pl

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sportsdataverse.nhl.nhl_microstat_constants import fit_shot_xg  # noqa: E402

ENTRY_WINDOW_S = 8.0
SHOT_TYPES = ("goal", "shot-on-goal", "missed-shot", "blocked-shot")


def _seconds(time_in_period: str) -> int:
    m, s = time_in_period.split(":")
    return int(m) * 60 + int(s)


def main() -> None:
    pbp_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "tests", "fixtures", "nhl_microstat", "pbp_2024_slice.parquet"
    )
    pbp = pl.read_parquet(pbp_path)
    pbp = pbp.with_columns(pl.col("time_in_period").map_elements(_seconds, return_dtype=pl.Int64).alias("_secs"))

    xg_model = fit_shot_xg(pbp)
    shots = pbp.filter(pl.col("type_desc_key").is_in(SHOT_TYPES))
    shots = shots.with_columns(xg_model.predict(shots).alias("xg"))

    faceoffs = (
        pbp.filter(pl.col("type_desc_key") == "faceoff")
        .with_row_index("fo_id")
        .select(
            "fo_id",
            "game_id",
            "period",
            "_secs",
            pl.col("zone_code").alias("fo_zone"),
            pl.col("event_owner_team_id").alias("winner_team_id"),
        )
    )

    joined = faceoffs.join(
        shots.select("game_id", "period", "_secs", "xg", pl.col("event_owner_team_id").alias("shot_team_id")),
        on=["game_id", "period"],
        how="inner",
    )
    joined = joined.filter(
        (pl.col("_secs_right") >= pl.col("_secs"))
        & (pl.col("_secs_right") <= pl.col("_secs") + ENTRY_WINDOW_S)
        & (pl.col("shot_team_id") == pl.col("winner_team_id"))
    )

    # Sum xG per faceoff (zero shots -> no row, filled in via the left join below)
    # so the denominator is every faceoff of that zone, not just the ones that
    # happened to generate a shot in the window.
    per_faceoff_xg = joined.group_by("fo_id").agg(pl.col("xg").sum().alias("total_xg"))
    with_xg = faceoffs.join(per_faceoff_xg, on="fo_id", how="left").with_columns(pl.col("total_xg").fill_null(0.0))

    per_zone = with_xg.group_by("fo_zone").agg(
        pl.col("total_xg").mean().alias("mean_xg_per_faceoff"), pl.len().alias("n")
    )
    print(per_zone.sort("fo_zone"))

    o_row = per_zone.filter(pl.col("fo_zone") == "O")
    o_val = o_row["mean_xg_per_faceoff"][0] if o_row.height else 1.0
    weights = {row["fo_zone"]: row["mean_xg_per_faceoff"] / o_val for row in per_zone.iter_rows(named=True)}
    print("normalized weights:", weights)


if __name__ == "__main__":
    main()
