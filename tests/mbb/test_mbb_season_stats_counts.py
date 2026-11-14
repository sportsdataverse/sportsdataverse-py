"""College season stats: numeric made / attempted counts, whole-season team totals, opponents' totals.

Real captures (provenance in ``tests/fixtures/season_producers/README.md``), offline:

* a Texas guard's ESPN career-stats payload, whose 2025-26 FT line is the text ``"83-111"``;
* Duke 2025-26's team statistics from ESPN's site endpoint (34 games, the regular season) and from
  Core v2's season type 0 (38 games, the whole season);
* the 2025-26 team box rows of Duke's 38 games (both teams' rows).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.mbb import (
    helper_mbb_player_season_stats,
    helper_mbb_team_season_opponent_stats,
    helper_mbb_team_season_stats,
)
from sportsdataverse.wbb import wbb_team_stats

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "season_producers"
DUKE = 150


def _gz(name: str) -> dict:
    return json.loads(gzip.decompress((FIX / name).read_bytes()))


def _value(df: pl.DataFrame, category: str, name: str) -> float | None:
    hit = df.filter((pl.col("category") == category) & (pl.col("stat_name") == name))
    assert hit.height == 1, (category, name, hit.height)
    return hit["value"].item()


def test_player_made_attempted_text_gets_numeric_counts() -> None:
    payload = json.loads((FIX / "mbb_player_season_stats_4432243.json").read_text(encoding="utf-8"))
    df = helper_mbb_player_season_stats(payload, season=2026, athlete_id=4432243)
    # the source rows stay as ESPN ships them: text, value null
    pair = df.filter(pl.col("stat_name") == "freeThrowsMade-freeThrowsAttempted")
    assert pair["display_value"].to_list() == ["83-111"] and pair["value"].to_list() == [None]
    assert _value(df, "totals", "freeThrowsMade") == 83.0
    assert _value(df, "totals", "freeThrowsAttempted") == 111.0
    assert _value(df, "totals", "fieldGoalsMade") == 184.0
    assert _value(df, "totals", "fieldGoalsAttempted") == 388.0
    assert _value(df, "totals", "threePointFieldGoalsMade") == 52.0
    assert _value(df, "averages", "avgFreeThrowsMade") == 2.3
    assert _value(df, "averages", "avgFreeThrowsAttempted") == 3.1
    ft = df.filter(pl.col("stat_name").is_in(["freeThrowsMade", "freeThrowsAttempted"]))
    assert ft["stat_label"].to_list() == ["FTM", "FTA"]
    assert ft["stat_display_name"].to_list() == ["Free Throws Made", "Free Throws Attempted"]
    assert ft["stat_description"].to_list() == [
        "The number of times a free throw was made.",
        "The number of times a free throw was attempted.",
    ]
    # six made-attempted stats (three totals, three averages), two counts each; nothing else moves
    assert df.height == 44 + 12
    assert df.select("category", "stat_name").is_duplicated().sum() == 0


@pytest.fixture
def espn(monkeypatch: pytest.MonkeyPatch):
    """Serve the captured site and Core v2 payloads by URL; ``fail_core`` makes Core v2 a 404."""
    state = {"fail_core": None, "urls": []}

    def fake(_transport, url: str, **_kw):
        state["urls"].append(url)
        if "site.web.api" in url:
            return _gz("mbb_team_stats_2026_150_site.json.gz")
        if state["fail_core"]:
            raise state["fail_core"](f"{url}: failed")
        assert url.endswith(f"/seasons/2026/types/0/teams/{DUKE}/statistics"), url
        return _gz("mbb_team_stats_2026_150_core_type0.json.gz")

    monkeypatch.setattr(wbb_team_stats, "_download_json", fake)
    return state


def test_college_team_stats_are_the_whole_season(espn) -> None:
    raw = wbb_team_stats._espn_basketball_team_stats("mens-college-basketball", DUKE, 2026, raw=True)
    assert raw["sdv_season_type"] == 0
    df = helper_mbb_team_season_stats(raw, season=2026, team_id=DUKE)
    # 38 games, the team box's count (and every game-level table's): the site endpoint said 34
    assert _value(df, "general", "gamesPlayed") == 38.0
    assert _value(df, "offensive", "fieldGoalsMade") == 1079.0
    assert _value(df, "offensive", "freeThrowsMade") == 612.0
    assert _value(df, "offensive", "freeThrowsAttempted") == 840.0
    assert df["team_display_name"].unique().to_list() == ["Duke Blue Devils"]  # the site's team block


def test_site_categories_stay_when_core_has_no_whole_season(espn) -> None:
    espn["fail_core"] = NoDataError  # Core v2's 404 "No stats found"
    raw = wbb_team_stats._espn_basketball_team_stats("mens-college-basketball", DUKE, 2026, raw=True)
    assert "sdv_season_type" not in raw
    assert _value(helper_mbb_team_season_stats(raw, season=2026, team_id=DUKE), "general", "gamesPlayed") == 34.0


def test_a_failed_core_fetch_raises_rather_than_mix_scopes(espn) -> None:
    espn["fail_core"] = AssetFetchError
    with pytest.raises(AssetFetchError):
        wbb_team_stats._espn_basketball_team_stats("mens-college-basketball", DUKE, 2026, raw=True)


def test_pro_team_stats_read_only_the_site_endpoint(espn) -> None:
    raw = wbb_team_stats._espn_basketball_team_stats("nba", DUKE, 2026, raw=True)
    assert "sdv_season_type" not in raw and len(espn["urls"]) == 1


def test_opponents_totals_come_from_the_other_row_of_each_game() -> None:
    tb = pl.read_parquet(FIX / "mbb_team_box_2026_duke_games.parquet")
    duke = tb.filter(pl.col("team_id") == DUKE)
    core = helper_mbb_team_season_stats(_gz("mbb_team_stats_2026_150_core_type0.json.gz"), season=2026, team_id=DUKE)
    # the team box is the whole season's scope: Duke's own rows sum to Core v2's type 0 totals
    assert duke["field_goals_made"].sum() == _value(core, "offensive", "fieldGoalsMade")
    assert duke["free_throws_made"].sum() == _value(core, "offensive", "freeThrowsMade")

    opp = helper_mbb_team_season_opponent_stats(tb, season=2026)
    assert opp.columns == core.columns and opp.schema == core.schema
    mine = opp.filter(pl.col("team_id") == DUKE)
    assert mine["category"].unique().to_list() == ["opponent"]
    assert mine.height == 7
    # points allowed: Duke's own rows name them in opponent_team_score, a column the sum never reads
    assert _value(mine, "opponent", "points") == duke["opponent_team_score"].sum()
    them = tb.filter(pl.col("team_id") != DUKE)
    assert _value(mine, "opponent", "freeThrowsMade") == them["free_throws_made"].sum()
    assert _value(mine, "opponent", "freeThrowsAttempted") == them["free_throws_attempted"].sum()
    assert _value(mine, "opponent", "fieldGoalsMade") == them["field_goals_made"].sum()
    assert mine["team_display_name"].unique().to_list() == ["Duke Blue Devils"]
    # every opponent's own "opponent" rows are Duke's numbers in their games against Duke
    unc = 153  # North Carolina: two regular-season games and the ACC tournament
    games = duke.filter(pl.col("opponent_team_id") == unc)
    assert _value(opp.filter(pl.col("team_id") == unc), "opponent", "freeThrowsMade") == games["free_throws_made"].sum()
    assert opp.select("team_id", "stat_name").is_duplicated().sum() == 0


def test_empty_team_box_has_no_opponent_rows() -> None:
    assert helper_mbb_team_season_opponent_stats(pl.DataFrame(), season=2026).is_empty()
