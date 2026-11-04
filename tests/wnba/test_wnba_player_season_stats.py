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


def test_an_empty_statistics_list_is_a_season_with_no_row():
    payload = _load(3917450)
    for cat in payload["categories"]:
        cat["statistics"] = []
    assert helper_wbb_player_season_stats(payload, season=2025, athlete_id=3917450).is_empty()


def test_one_category_without_statistics_does_not_emit_career_values():
    """Another category's per-season rows show the payload is not legacy: no flat career fallback."""
    payload = _load(3917450)
    del payload["categories"][2]["statistics"]
    df = helper_wbb_player_season_stats(payload, season=2025, athlete_id=3917450)
    assert set(df["category"].unique().to_list()) == {"averages", "totals"}
    assert helper_wbb_player_season_stats(payload, season=2018, athlete_id=3917450).is_empty()


def test_traded_player_without_a_totals_row_takes_the_last_stint_and_its_team():
    payload = _load(1107)
    for cat in payload["categories"]:
        cat["statistics"] = [s for s in cat["statistics"] if "totals" not in s["teamSlug"].lower()]
    stint = next(s for s in reversed(payload["categories"][0]["statistics"]) if s["season"]["year"] == 2025)
    team = payload["teams"][stint["teamSlug"]]
    # team_id and team_display_name both name the stint's team, whatever the roster lookup says
    ident = {"1107": {"team_id": 999, "team_display_name": "Roster Team"}}
    df = helper_wnba_player_season_stats(payload, season=2025, athlete_id=1107, identity_lookup=ident)
    assert df["team_id"].unique().to_list() == [int(stint["teamId"])]
    assert df["team_display_name"].unique().to_list() == [team["displayName"]]


def test_traded_players_totals_row_takes_both_team_fields_from_the_roster():
    ident = {"1107": {"team_id": 5, "team_display_name": "Indiana Fever"}}
    df = helper_wnba_player_season_stats(_load(1107), season=2025, athlete_id=1107, identity_lookup=ident)
    assert df.select("team_id", "team_display_name").unique().rows() == [(5, "Indiana Fever")]
