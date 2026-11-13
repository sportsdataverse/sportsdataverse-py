"""Write the college pregame projection fixtures (tests/mbb/test_mbb_pregame_projection.py).

The 2026-27 men's season as released on the day it is run, cut to the games dated through
``--through`` (played and scheduled), the team box of those games, and the season's preseason
prior from the 2025-26 release. Usage::

    uv run python tools/models/mbb_prediction/build_pregame_projection_fixture.py --through 2026-11-20
"""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path

import polars as pl

from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
from sportsdataverse.mbb.mbb_team_ratings import load_preseason_prior

FIX = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "mbb_prediction"
SEASON = 2027
SCHED_COLS = ["game_id", "season", "game_date", "home_id", "away_id", "neutral_site", "status_type_completed"]
BOX_COLS = [
    "game_id",
    "season",
    "game_date",
    "team_id",
    "team_score",
    "field_goals_attempted",
    "offensive_rebounds",
    "turnovers",
    "total_turnovers",
    "team_turnovers",
    "free_throws_attempted",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--through", required=True, type=datetime.date.fromisoformat)
    args = ap.parse_args()
    raw = load_mbb_schedule([SEASON])
    sched = raw.filter(pl.col("game_date") <= args.through).select(SCHED_COLS).sort("game_date", "game_id")
    box = load_mbb_team_boxscore([SEASON])
    box = box.join(sched.select("game_id"), on="game_id", how="semi").select([c for c in BOX_COLS if c in box.columns])
    prior = load_preseason_prior(SEASON, raw)
    sched.write_parquet(FIX / f"pregame_projection_schedule_{SEASON}.parquet")
    box.write_parquet(FIX / f"pregame_projection_team_box_{SEASON}.parquet")
    prior.write_parquet(FIX / f"pregame_projection_prior_{SEASON}.parquet")
    print(sched.height, box.height, prior.height)


if __name__ == "__main__":
    main()
