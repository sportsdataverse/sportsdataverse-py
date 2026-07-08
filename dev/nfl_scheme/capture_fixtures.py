"""Capture the scheme/ST oracle fixture corpus (run once, network required).

Writes tests/fixtures/nfl_scheme/*.parquet from the real nflverse loaders +
the shipped xpass model.  Provenance: tests/fixtures/nfl_scheme/README.md.
"""

from pathlib import Path

import polars as pl

from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_loaders import (
    load_nfl_pbp,
    load_nfl_pbp_participation,
    load_nfl_pfr_advstats,
)

OUT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nfl_scheme"
OUT.mkdir(parents=True, exist_ok=True)

UTF8_COLS = (
    "game_id",
    "posteam",
    "defteam",
    "kicker_player_id",
    "punter_player_id",
    "home_team",
    "stadium",
    "play_type",
    "pfr_player_id",
    "team",
)
INT_COLS = ("play_id", "season", "week")

KEEP = [
    "game_id",
    "play_id",
    "season",
    "week",
    "posteam",
    "defteam",
    "play_type",
    "pass",
    "rush",
    "qb_dropback",
    "qb_scramble",
    "sack",
    "qb_hit",
    "epa",
    "xpass",
    "pass_oe",
    "down",
    "ydstogo",
    "yardline_100",
    "score_differential",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "wp",
    "shotgun",
    "no_huddle",
    "pass_length",
    "pass_location",
    "run_location",
    "run_gap",
    "kick_distance",
    "return_yards",
    "field_goal_result",
    "kicker_player_id",
    "kicker_player_name",
    "punter_player_id",
    "roof",
    "temp",
    "wind",
    "stadium",
    "home_team",
    "drive",
    "touchback",
]


def _pin_ids(df: pl.DataFrame) -> pl.DataFrame:
    casts = []
    for c in UTF8_COLS:
        if c in df.columns:
            casts.append(pl.col(c).cast(pl.Utf8))
    for c in INT_COLS:
        if c in df.columns:
            casts.append(pl.col(c).cast(pl.Int64))
    return df.with_columns(casts) if casts else df


def main() -> None:
    if (OUT / "pbp_2021_2023_slice.parquet").exists():
        print("pbp fixtures already captured; skipping to participation/pfr", flush=True)
        _capture_participation_and_pfr()
        _report()
        return
    print("loading pbp 2019-2023 ...", flush=True)
    pbp = calculate_xpass(load_nfl_pbp([2019, 2020, 2021, 2022, 2023]))
    pbp = _pin_ids(pbp)
    keep = [c for c in KEEP if c in pbp.columns]
    missing = [c for c in KEEP if c not in pbp.columns]
    print("KEEP columns missing from loader pbp:", missing, flush=True)

    slice_21_23 = pbp.filter(pl.col("season").is_in([2021, 2022, 2023])).select(keep)
    slice_21_23.write_parquet(OUT / "pbp_2021_2023_slice.parquet")

    fg = pbp.filter(pl.col("play_type") == "field_goal").select(keep)
    fg.write_parquet(OUT / "fg_attempts_2019_2023.parquet")

    _capture_participation_and_pfr()
    _report()


def _capture_participation_and_pfr() -> None:
    if (OUT / "participation_2021_2023.parquet").exists():
        print("participation already captured; skipping", flush=True)
        _capture_pfr()
        return
    print("loading participation 2021-2023 ...", flush=True)
    # NOTE: load_nfl_pbp_participation([2021, 2022, 2023]) crashes on a
    # cross-season schema drift (vertical concat, width 20 vs 26) — flagged
    # upstream; per-season + diagonal_relaxed here.
    part = pl.concat(
        [load_nfl_pbp_participation([s]) for s in (2021, 2022, 2023)],
        how="diagonal_relaxed",
    )
    part = _pin_ids(part)
    pkeep = [
        c
        for c in (
            "nflverse_game_id",
            "game_id",
            "play_id",
            "offense_personnel",
            "defense_personnel",
            "offense_formation",
            "defenders_in_box",
            "number_of_pass_rushers",
        )
        if c in part.columns
    ]
    print("participation columns kept:", pkeep, flush=True)
    part.select(pkeep).write_parquet(OUT / "participation_2021_2023.parquet")
    _capture_pfr()


def _capture_pfr() -> None:
    print("loading pfr advstats def/season ...", flush=True)
    pfr = _pin_ids(load_nfl_pfr_advstats([2023], stat_type="def", summary_level="season"))
    pfr.filter(pl.col("season") == 2023).write_parquet(OUT / "pfr_advstats_2023.parquet")
    print("pfr columns:", pfr.columns, flush=True)


def _report() -> None:
    for p in sorted(OUT.glob("*.parquet")):
        print(p.name, pl.read_parquet(p).shape, flush=True)


if __name__ == "__main__":
    main()
