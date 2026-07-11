"""Capture the real-capture 2024 Savant oracle corpus for the T6.3
fielding/catching/baserunning model spine.

Scratch / provenance script (dev/ is gitignored) -- run once to (re)produce
the 12 committed parquet fixtures under tests/fixtures/mlb_fielding/. See
that directory's README.md for the full provenance narrative (this script's
inline comments document the *why*; the README is the durable record).

Run::

    SDV_PY_LIVE_TESTS=1 uv run python dev/mlb_fielding/capture_oracle.py

Deviations from a literal read of the plan (recorded here + README):
- The plan's Task 0.1 names a single day (``pitches_2024-06-15.parquet``)
  then flags that a day is likely too sparse for the Phase 1.3 catcher
  gate (min 500 takes/catcher). We capture the whole month directly
  (``pitches_2024-06.parquet``) to avoid a second live round-trip.
- ``bip_2024.parquet`` is derived from the SAME month capture (filter
  ``type == "X"``) rather than a separate live pull -- one fewer network
  round-trip, same real-capture guarantee (no synthetic rows).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mlb.mlb_statcast import (
    mlb_statcast_leaderboard_arm_strength,
    mlb_statcast_leaderboard_baserunning_run_value,
    mlb_statcast_leaderboard_basestealing_run_value,
    mlb_statcast_leaderboard_catch_probability,
    mlb_statcast_leaderboard_catcher_blocking,
    mlb_statcast_leaderboard_catcher_framing,
    mlb_statcast_leaderboard_catcher_throwing,
    mlb_statcast_leaderboard_outs_above_average,
    mlb_statcast_leaderboard_poptime,
    mlb_statcast_leaderboard_sprint_speed,
)
from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "mlb_fielding"

REQUIRED_PITCH_COLS = [
    "plate_x",
    "plate_z",
    "sz_top",
    "sz_bot",
    "description",
    "balls",
    "strikes",
    "stand",
    "p_throws",
    "fielder_2",
    "batter",
    "pitcher",
    "delta_run_exp",
]
REQUIRED_BIP_COLS = [
    "hc_x",
    "hc_y",
    "hit_distance_sc",
    "launch_angle",
    "launch_speed",
    "hit_location",
    "events",
]

LEADERBOARDS = {
    "lb_catcher_framing_2024": mlb_statcast_leaderboard_catcher_framing,
    "lb_catcher_blocking_2024": mlb_statcast_leaderboard_catcher_blocking,
    "lb_catcher_throwing_2024": mlb_statcast_leaderboard_catcher_throwing,
    "lb_poptime_2024": mlb_statcast_leaderboard_poptime,
    "lb_arm_strength_2024": mlb_statcast_leaderboard_arm_strength,
    "lb_oaa_2024": mlb_statcast_leaderboard_outs_above_average,
    "lb_catch_probability_2024": mlb_statcast_leaderboard_catch_probability,
    "lb_sprint_speed_2024": mlb_statcast_leaderboard_sprint_speed,
    "lb_baserunning_rv_2024": mlb_statcast_leaderboard_baserunning_run_value,
    "lb_basestealing_rv_2024": mlb_statcast_leaderboard_basestealing_run_value,
}


def _cast_id_cols(df: pl.DataFrame) -> pl.DataFrame:
    """Cast every column literally named / ending ``player_id`` to Utf8 via Int64 (never float->str)."""
    casts = []
    for col in df.columns:
        if col == "player_id" or col.endswith("_id"):
            casts.append(pl.col(col).cast(pl.Int64, strict=False).cast(pl.Utf8).alias(col))
    return df.with_columns(casts) if casts else df


#: Columns actually consumed across all six models (run_values, framing,
#: catcher_defense, fielding_oaa, baserunning, stolen_base). The raw Savant
#: search payload ships 119 columns; a full-width month capture blew past
#: the repo's 10 MB large-file pre-commit limit (18.9 MB). Selecting this
#: subset before writing shrinks the fixture to ~5 MB with no loss of
#: information any model reads.
_PITCH_FIXTURE_COLS = [
    "plate_x",
    "plate_z",
    "sz_top",
    "sz_bot",
    "stand",
    "p_throws",
    "description",
    "balls",
    "strikes",
    "delta_run_exp",
    "events",
    "des",
    "type",
    "game_pk",
    "at_bat_number",
    "game_date",
    "batter",
    "pitcher",
    "on_1b",
    "on_2b",
    "on_3b",
    "hc_x",
    "hc_y",
    "hit_distance_sc",
    "launch_angle",
    "launch_speed",
    "hit_location",
] + [f"fielder_{i}" for i in range(1, 10)]


def main() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    print("Capturing pitch-level month 2024-06 (mlb_statcast_search)...")
    pitches = mlb_statcast_search("2024-06-01", "2024-06-30", season=2024)
    print(f"  -> {pitches.height} pitches, {len(pitches.columns)} columns")
    missing = [c for c in REQUIRED_PITCH_COLS if c not in pitches.columns]
    assert not missing, f"pitches fixture missing required columns: {missing}"

    dre_coverage = 0.0
    if "delta_run_exp" in pitches.columns and pitches.height:
        dre_coverage = float(pitches["delta_run_exp"].is_not_null().mean() or 0.0)
    print(f"  delta_run_exp non-null coverage: {dre_coverage:.3f}")

    pitches = pitches.select([c for c in _PITCH_FIXTURE_COLS if c in pitches.columns])
    pitches.write_parquet(FIXTURE_DIR / "pitches_2024-06.parquet", compression="zstd", compression_level=15)

    print("Deriving BIP subset (type == 'X')...")
    type_col = "type" if "type" in pitches.columns else None
    if type_col is not None:
        bip = pitches.filter(pl.col(type_col) == "X")
    else:
        # Fallback: description-based in-play detection if 'type' is absent.
        bip = pitches.filter(pl.col("description").str.contains("(?i)hit_into_play"))
    print(f"  -> {bip.height} balls in play")
    missing_bip = [c for c in REQUIRED_BIP_COLS if c not in bip.columns]
    assert not missing_bip, f"bip fixture missing required columns: {missing_bip}"
    bip.write_parquet(FIXTURE_DIR / "bip_2024.parquet")

    for name, fn in LEADERBOARDS.items():
        print(f"Capturing {name}...")
        df = fn(year=2024)
        df = _cast_id_cols(df)
        print(f"  -> {df.height} rows; columns: {list(df.columns)}")
        df.write_parquet(FIXTURE_DIR / f"{name}.parquet")

    print("Done. Fixtures written to", FIXTURE_DIR)


if __name__ == "__main__":
    main()
