"""Capture a small real NGS slice for offline oracle tests.

Provenance: loader ``load_nfl_nextgen_stats`` (nflverse nextgen_stats release
parquet), seasons 2022 + 2023, season-level rows only (``week == 0``).
Run once: ``uv run python dev/nfl_ngs/capture_fixtures.py`` (network).
"""

import polars as pl

from sportsdataverse.nfl import load_nfl_nextgen_stats

REC_COLS = [
    "season",
    "week",
    "player_gsis_id",
    "player_display_name",
    "player_position",
    "team_abbr",
    "avg_cushion",
    "avg_separation",
    "avg_intended_air_yards",
    "receptions",
    "targets",
    "avg_yac",
    "avg_expected_yac",
    "avg_yac_above_expectation",
]
RUSH_COLS = [
    "season",
    "week",
    "player_gsis_id",
    "player_display_name",
    "player_position",
    "team_abbr",
    "rush_attempts",
    "rush_yards",
    "expected_rush_yards",
    "rush_yards_over_expected",
    "rush_yards_over_expected_per_att",
    "percent_attempts_gte_eight_defenders",
]


def _slice(stat_type: str, cols: list, out: str, weekly: bool = False) -> None:
    df = load_nfl_nextgen_stats(seasons=[2022, 2023], stat_type=stat_type)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"MISSING upstream columns for {stat_type}: {missing}")
    week_filter = pl.col("week") > 0 if weekly else pl.col("week") == 0
    df = (
        df.filter(week_filter)
        .select([c for c in cols if c in df.columns])
        .with_columns(
            pl.col("player_gsis_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
        )
    )
    df.write_parquet(out)
    print(out, df.shape)


if __name__ == "__main__":
    _slice("receiving", REC_COLS, "tests/fixtures/nfl_ngs/ngs_receiving_2022_2023.parquet")
    _slice("rushing", RUSH_COLS, "tests/fixtures/nfl_ngs/ngs_rushing_2022_2023.parquet")
    # Weekly rows: identify sigma2 (within-player sampling variance) for the
    # EB prior — the season-only panel's 1/n spread is too narrow to identify
    # the tau2/sigma2 OLS on rushing (all qualified rushers have similar n).
    _slice(
        "receiving",
        REC_COLS,
        "tests/fixtures/nfl_ngs/ngs_receiving_weekly_2022_2023.parquet",
        weekly=True,
    )
    _slice(
        "rushing",
        RUSH_COLS,
        "tests/fixtures/nfl_ngs/ngs_rushing_weekly_2022_2023.parquet",
        weekly=True,
    )
