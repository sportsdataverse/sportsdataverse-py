"""`helper_wnba_player_season_stats` (shared WBB code) reads the SEASON row, not the career row (#1087).

ESPN's athlete-stats payload carries every season the player has played: each category's flat
`totals` array is the CAREER row, and the season is the `statistics[]` entry tagged with that year.
Real captures from `wehoop-wnba-raw`; provenance in `tests/fixtures/season_producers/README.md`. Offline.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.wbb.wbb_season_producers import helper_wbb_player_season_stats
from sportsdataverse.wnba.wnba_season_producers import helper_wnba_player_season_stats

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "season_producers"


def _load(athlete_id: int) -> dict:
    return json.loads((FIX / f"wnba_player_season_stats_2025_{athlete_id}.json").read_text(encoding="utf-8"))


def _stat(df: pl.DataFrame, category: str, name: str) -> str:
    return df.filter((pl.col("category") == category) & (pl.col("stat_name") == name))["display_value"].item()


def test_collier_2025_is_her_season_not_her_career():
    df = helper_wnba_player_season_stats(_load(3917450), season=2025, athlete_id=3917450)
    assert _stat(df, "averages", "gamesPlayed") == "33"  # career totals row says 193
    assert _stat(df, "averages", "avgPoints") == "22.9"  # career 18.4
    assert _stat(df, "totals", "points") == "755"  # career 3542
    assert df["season"].unique().to_list() == [2025]
    assert df["team_id"].unique().to_list() == [8]
    assert df["team_display_name"].unique().to_list() == ["Minnesota Lynx"]


def test_collier_earlier_season_reads_that_season():
    df = helper_wbb_player_season_stats(_load(3917450), season=2019, athlete_id=3917450)
    assert _stat(df, "averages", "gamesPlayed") == "34"  # her rookie season
    assert df.height == 43


def test_traded_player_takes_the_whole_season_totals_row():
    df = helper_wnba_player_season_stats(_load(1107), season=2025, athlete_id=1107)
    assert _stat(df, "averages", "gamesPlayed") == "15"  # career 131
    assert _stat(df, "totals", "points") == "77"  # career 771
    # the Totals row names no team; with no roster identity the team stays null, never the
    # first-career team (Mystics) the old code picked.
    assert df["team_display_name"].null_count() == df.height


def test_season_absent_from_the_payload_yields_no_rows():
    assert helper_wnba_player_season_stats(_load(3917450), season=2018, athlete_id=3917450).is_empty()


def test_payload_without_statistics_rows_falls_back_to_totals():
    payload = _load(3917450)
    for cat in payload["categories"]:
        del cat["statistics"]
    df = helper_wbb_player_season_stats(payload, season=2025, athlete_id=3917450)
    assert _stat(df, "averages", "gamesPlayed") == "193"
