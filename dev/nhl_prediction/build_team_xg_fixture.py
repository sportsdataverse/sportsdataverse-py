"""Build tests/fixtures/nhl_prediction/team_xg_2023.parquet -- the raw
even-strength per-team-per-game xG frame (Task 1.1's ``team_game_xg_rates``
output) used by the Task 1.4 oracle gate. Run once the real season pbp is
downloaded (no live-API gate needed -- these are public data-release
parquet downloads, not gated wrapper calls):

    uv run python dev/nhl_prediction/build_team_xg_fixture.py
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_loaders import load_nhl_pbp_full, load_nhl_schedules
from sportsdataverse.nhl.nhl_team_ratings import team_game_xg_rates

FIXTURES_DIR = "tests/fixtures/nhl_prediction"
SEASON = 2023


def main() -> None:
    pbp = load_nhl_pbp_full([SEASON])
    schedule = load_nhl_schedules([SEASON])
    sched = schedule.filter(pl.col("game_type") == "R").select(
        pl.col("game_id"),
        pl.col("season"),
        pl.col("game_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
        pl.col("home_team_abbr").alias("home_abbr"),
        pl.col("away_team_abbr").alias("away_abbr"),
        pl.lit(False).alias("neutral_site"),
        pl.col("home_score").cast(pl.Int64).alias("home_goals"),
        pl.col("away_score").cast(pl.Int64).alias("away_goals"),
    )
    rates = team_game_xg_rates(pbp, sched)
    out_path = f"{FIXTURES_DIR}/team_xg_2023.parquet"
    rates.write_parquet(out_path)
    print(f"wrote {out_path}: {rates.shape}")


if __name__ == "__main__":
    main()
