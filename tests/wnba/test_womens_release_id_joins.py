"""Cross-loader joins on the WNBA / WBB release ids, offline (wehoop parity audit 06, P0-4).

WNBA ``game_rosters.game_id``, ``officials.game_id``, ``rosters.athlete_id``, WBB ``game_rosters.game_id``
(2004-2007) and the crosswalk ``espn_*`` ids are published as String while pbp, box and schedule carry them as
Int32; the WNBA crosswalk's ``wnba_*`` team / player bridge ids are String while WNBA Stats ships them Int64.
A String key cannot join an integer one (``SchemaError``). The loaders cast them at the boundary (``id_int32`` /
``id_int64`` in ``releases.yaml``). Real release rows: ``tests/fixtures/basketball_release_ids/README.md``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.wbb.wbb_loaders as wbb
import sportsdataverse.wnba.wnba_loaders as wnba

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "basketball_release_ids"
# release asset (tag/file) -> fixture
ASSETS = {
    "espn_wnba_game_rosters/game_rosters_2025.parquet": "wnba_game_rosters_2025",
    "espn_wnba_officials/officials_2025.parquet": "wnba_officials_2025",
    "espn_wnba_schedules/wnba_schedule_2025.parquet": "wnba_schedule_2025",
    "espn_wnba_player_boxscores/player_box_2025.parquet": "wnba_player_box_2025",
    "espn_wnba_rosters/rosters_2025.parquet": "wnba_rosters_2025",
    "wnba_crosswalk/wnba_schedule_crosswalk_2025.parquet": "wnba_schedule_crosswalk_2025",
    "wnba_crosswalk/wnba_team_crosswalk_2025.parquet": "wnba_team_crosswalk_2025",
    "wnba_crosswalk/wnba_player_crosswalk_2025.parquet": "wnba_player_crosswalk_2025",
    "wnba_stats_rosters/rosters_2025.parquet": "wnba_stats_rosters_2025",
    "espn_womens_college_basketball_game_rosters/game_rosters_2005.parquet": "wbb_game_rosters_2005",
    "espn_womens_college_basketball_player_boxscores/player_box_2005.parquet": "wbb_player_box_2005",
    "wbb_crosswalk/wbb_schedule_crosswalk_2026.parquet": "wbb_schedule_crosswalk_2026",
    "espn_womens_college_basketball_schedules/wbb_schedule_2026.parquet": "wbb_schedule_2026",
}


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    def read(url: str) -> pl.DataFrame:
        return pl.read_parquet(FIX / f"{ASSETS['/'.join(url.split('/')[-2:])]}.parquet")

    monkeypatch.setattr(wbb, "_read_release_parquet", read)
    monkeypatch.setattr(wnba, "_read_release_parquet", read)


def _join(left: pl.DataFrame, right: pl.DataFrame, left_on: str, right_on: str, dtype=pl.Int32) -> pl.DataFrame:
    assert left.schema[left_on] == right.schema[right_on] == dtype, (left.schema[left_on], right.schema[right_on])
    return left.join(right, left_on=left_on, right_on=right_on)


def test_the_fixtures_carry_the_published_string_ids() -> None:
    """The fixtures are the published shape, so the tests below exercise the cast, not a pre-cast copy."""
    schema = lambda name: pl.read_parquet_schema(FIX / f"{name}.parquet")  # noqa: E731
    assert schema("wnba_game_rosters_2025")["game_id"] == pl.String
    assert schema("wnba_officials_2025")["game_id"] == pl.String
    assert schema("wnba_rosters_2025")["athlete_id"] == pl.String
    assert schema("wnba_schedule_crosswalk_2025")["espn_game_id"] == pl.String
    assert schema("wnba_schedule_crosswalk_2025")["wnba_home_team_id"] == pl.String
    assert schema("wnba_team_crosswalk_2025")["wnba_team_id"] == pl.String
    assert schema("wnba_player_crosswalk_2025")["wnba_player_id"] == pl.String
    assert schema("wbb_game_rosters_2005")["game_id"] == pl.String
    assert schema("wbb_schedule_crosswalk_2026")["espn_game_id"] == pl.String


def test_wnba_game_rosters_and_officials_join_the_schedule() -> None:
    sched = wnba.load_wnba_schedule(2025).select("game_id")
    rosters, officials = wnba.load_wnba_game_rosters(2025), wnba.load_wnba_officials(2025)
    assert _join(rosters, sched, "game_id", "game_id").height == rosters.height == 23
    assert _join(officials, sched, "game_id", "game_id").height == officials.height == 4


def test_wnba_rosters_athlete_id_joins_the_player_box() -> None:
    box, rosters = wnba.load_wnba_player_boxscore(2025), wnba.load_wnba_rosters(2025)
    matched = _join(box.select("athlete_id").unique(), rosters.select("athlete_id"), "athlete_id", "athlete_id")
    assert matched.height == 15  # the season-end roster; 8 of the 23 box athletes had left by then


def test_wnba_crosswalk_bridges_espn_to_wnba_stats() -> None:
    cw, sched = wnba.load_wnba_schedule_crosswalk(2025), wnba.load_wnba_schedule(2025)
    for col in ("espn_game_id", "home_espn_team_id", "away_espn_team_id"):
        assert cw.schema[col] == pl.Int32, col
    assert cw.schema["wnba_game_id"] == pl.String  # WNBA Stats' own game id is text ("1042500304"), left alone
    joined = _join(cw, sched.select("game_id", "home_id"), "espn_game_id", "game_id")
    assert joined.height == 1 and (joined["home_espn_team_id"] == joined["home_id"]).all()

    teams = wnba.load_wnba_team_crosswalk(2025)
    assert teams.schema["espn_team_id"] == pl.Int32
    on_home = _join(cw, teams.select("wnba_team_id"), "wnba_home_team_id", "wnba_team_id", pl.Int64)
    assert on_home.height == 1

    players, stats = wnba.load_wnba_player_crosswalk(2025), wnba.load_wnba_stats_rosters(2025)
    assert players.schema["espn_athlete_id"] == pl.Int32
    matched = _join(players, stats.select("player_id"), "wnba_player_id", "player_id", pl.Int64)
    assert matched.height > 0.5 * players.height


def test_wbb_old_game_rosters_join_the_player_box() -> None:
    rosters, box = wbb.load_wbb_game_rosters(2005), wbb.load_wbb_player_boxscore(2005)
    keys = ["game_id", "athlete_id"]
    joined = _join(rosters.select(keys), box.select(keys), "game_id", "game_id").filter(
        pl.col("athlete_id") == pl.col("athlete_id_right")
    )
    assert joined.height == box.height == 10


def test_wbb_schedule_crosswalk_joins_the_schedule() -> None:
    cw, sched = wbb.load_wbb_schedule_crosswalk(2026), wbb.load_wbb_schedule(2026)
    for col in ("espn_game_id", "home_espn_team_id", "away_espn_team_id"):
        assert cw.schema[col] == pl.Int32, col
    assert _join(cw, sched.select("game_id"), "espn_game_id", "game_id").height == 1
