"""Scratch capture script for the T3.5 play-type/impact oracle corpus (dev/, not committed to docs).

Pinned endpoint/param names (Task 0.1 Step 1):
  - nba_stats_synergyplaytypes(league_id, season, play_type_nullable, player_or_team_abbreviation="T"|"P",
    type_grouping_nullable="Offensive"|"Defensive")  -- grouping param IS present (confirmed by signature
    grep), so we split by calling it twice per play type rather than splitting a combined payload.
  - nba_stats_leagueseasonmatchups(league_id, season, per_mode_simple="Totals") -> columns include
    PARTIAL_POSS, PLAYER_PTS (confirmed by capture below; see README for the exact raw names captured).
  - nba_stats_leaguedashplayerstats(league_id, season, measure_type_detailed_defense="Base"|"Advanced").
  - nba_stats_leaguegamelog(league_id, season, player_or_team_abbreviation="T").

Run: SDV_PY_NBA_STATS_LIVE=1 uv run python dev/nba_playtype/capture_oracle.py
"""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_stats import (
    nba_stats_leaguedashplayerstats,
    nba_stats_leaguegamelog,
    nba_stats_leagueseasonmatchups,
    nba_stats_synergyplaytypes,
)

SEASON = "2023-24"
LEAGUE_ID = "00"
OUT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_playtype"
OUT.mkdir(parents=True, exist_ok=True)

SYNERGY_PLAY_TYPES = [
    "Transition",
    "Isolation",
    "PRBallHandler",
    "PRRollman",
    "Postup",
    "Spotup",
    "Handoff",
    "Cut",
    "OffScreen",
    "OffRebound",
    "Misc",
]


def _synergy(grouping: str, entity: str) -> pl.DataFrame:
    frames = []
    for pt in SYNERGY_PLAY_TYPES:
        df = nba_stats_synergyplaytypes(
            league_id=LEAGUE_ID,
            season=SEASON,
            play_type_nullable=pt,
            player_or_team_abbreviation=entity,
            type_grouping_nullable=grouping,
        )
        print(f"  synergy {grouping}/{entity}/{pt}: {df.height} rows, cols={df.columns[:8]}")
        frames.append(df)
        time.sleep(0.5)
    return pl.concat(frames, how="diagonal_relaxed")


def main() -> None:
    print("Synergy offensive team...")
    off_team = _synergy("Offensive", "T")
    print("Synergy defensive team...")
    def_team = _synergy("Defensive", "T")
    print("Synergy offensive player...")
    off_player = _synergy("Offensive", "P")

    off_team.write_parquet(OUT / "synergy_off_team_raw_2024.parquet")
    def_team.write_parquet(OUT / "synergy_def_team_raw_2024.parquet")
    off_player.write_parquet(OUT / "synergy_off_player_raw_2024.parquet")

    print("League season matchups...")
    matchups = nba_stats_leagueseasonmatchups(league_id=LEAGUE_ID, season=SEASON, per_mode_simple="Totals")
    print(f"  matchups: {matchups.height} rows, cols={matchups.columns}")
    matchups.write_parquet(OUT / "matchups_raw_2024.parquet")

    print("Leaguedash Base...")
    base = nba_stats_leaguedashplayerstats(league_id=LEAGUE_ID, season=SEASON, measure_type_detailed_defense="Base")
    print(f"  base: {base.height} rows, cols={base.columns}")
    base.write_parquet(OUT / "leaguedash_base_raw_2024.parquet")

    print("Leaguedash Advanced...")
    adv = nba_stats_leaguedashplayerstats(league_id=LEAGUE_ID, season=SEASON, measure_type_detailed_defense="Advanced")
    print(f"  adv: {adv.height} rows, cols={adv.columns}")
    adv.write_parquet(OUT / "leaguedash_adv_raw_2024.parquet")

    print("League game log...")
    gamelog = nba_stats_leaguegamelog(league_id=LEAGUE_ID, season=SEASON, player_or_team_abbreviation="T")
    print(f"  gamelog: {gamelog.height} rows, cols={gamelog.columns}")
    gamelog.write_parquet(OUT / "gamelog_raw_2024.parquet")

    print("Done. Raw payloads captured -- run normalize_oracle.py next to build the contract fixtures.")


if __name__ == "__main__":
    main()
