"""Cross-loader joins on the ESPN basketball release ids, offline.

``game_rosters.game_id``, ``rosters.athlete_id`` and the crosswalk / NCAA ``espn_*`` ids are published as String
while pbp, box, schedule and shots carry the same ids as Int32, and a String key cannot join an Int32 one
(``SchemaError``). The loaders cast them to Int32 at the boundary (``id_int32`` in ``releases.yaml``). These
tests feed real release rows (``tests/fixtures/basketball_release_ids/README.md``) through the generated
loaders and join them.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.mbb.mbb_loaders as mbb
import sportsdataverse.nba.nba_loaders as nba

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "basketball_release_ids"
# release asset (tag/file) -> fixture
ASSETS = {
    "espn_mens_college_basketball_game_rosters/game_rosters_2026.parquet": "mbb_game_rosters_2026",
    "espn_mens_college_basketball_schedules/mbb_schedule_2026.parquet": "mbb_schedule_2026",
    "espn_mens_college_basketball_player_boxscores/player_box_2026.parquet": "mbb_player_box_2026",
    "espn_mens_college_basketball_rosters/rosters_2026.parquet": "mbb_rosters_2026",
    "mbb_crosswalk/mbb_schedule_crosswalk_2026.parquet": "mbb_schedule_crosswalk_2026",
    "ncaa_mbb_team_box/ncaa_mbb_team_box_2026.parquet": "ncaa_mbb_team_box_2026",
    "espn_nba_game_rosters/game_rosters_2026.parquet": "nba_game_rosters_2026",
    "espn_nba_schedules/nba_schedule_2026.parquet": "nba_schedule_2026",
    "espn_nba_player_boxscores/player_box_2026.parquet": "nba_player_box_2026",
    "espn_nba_rosters/rosters_2026.parquet": "nba_rosters_2026",
}


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    def read(url: str) -> pl.DataFrame:
        return pl.read_parquet(FIX / f"{ASSETS['/'.join(url.split('/')[-2:])]}.parquet")

    monkeypatch.setattr(mbb, "_read_release_parquet", read)
    monkeypatch.setattr(nba, "_read_release_parquet", read)


def _join(left: pl.DataFrame, right: pl.DataFrame, left_on: str, right_on: str) -> pl.DataFrame:
    assert left.schema[left_on] == right.schema[right_on] == pl.Int32, (left.schema[left_on], right.schema[right_on])
    return left.join(right, left_on=left_on, right_on=right_on)


def test_the_fixtures_carry_the_published_string_ids() -> None:
    """The fixtures are the published shape, so the tests below exercise the cast, not a pre-cast copy."""
    assert pl.read_parquet_schema(FIX / "mbb_game_rosters_2026.parquet")["game_id"] == pl.String
    assert pl.read_parquet_schema(FIX / "mbb_rosters_2026.parquet")["athlete_id"] == pl.String
    assert pl.read_parquet_schema(FIX / "mbb_schedule_crosswalk_2026.parquet")["espn_game_id"] == pl.String
    assert pl.read_parquet_schema(FIX / "ncaa_mbb_team_box_2026.parquet")["espn_game_id"] == pl.String
    assert pl.read_parquet_schema(FIX / "nba_game_rosters_2026.parquet")["game_id"] == pl.String
    assert pl.read_parquet_schema(FIX / "nba_rosters_2026.parquet")["athlete_id"] == pl.String


def test_mbb_game_rosters_join_the_schedule_and_player_box() -> None:
    rosters, sched, box = (
        mbb.load_mbb_game_rosters(2026),
        mbb.load_mbb_schedule(2026),
        mbb.load_mbb_player_boxscore(2026),
    )
    on_sched = _join(rosters, sched.select("game_id"), "game_id", "game_id")
    assert on_sched.height == rosters.height == 61
    keys = ["game_id", "athlete_id"]
    assert rosters.schema["athlete_id"] == box.schema["athlete_id"] == pl.Int32
    on_box = _join(rosters.select(keys), box.select(keys), "game_id", "game_id").filter(
        pl.col("athlete_id") == pl.col("athlete_id_right")
    )
    assert on_box.height == box.height == 61


def test_mbb_rosters_athlete_id_joins_the_player_box() -> None:
    rosters, box = mbb.load_mbb_rosters(2026), mbb.load_mbb_player_boxscore(2026)
    matched = _join(box.select("athlete_id").unique(), rosters.select("athlete_id"), "athlete_id", "athlete_id")
    assert matched.height > 0.9 * box["athlete_id"].n_unique()


def test_mbb_schedule_crosswalk_joins_the_schedule() -> None:
    cw, sched = mbb.load_mbb_schedule_crosswalk(2026), mbb.load_mbb_schedule(2026)
    for col in ("espn_game_id", "home_espn_team_id", "away_espn_team_id"):
        assert cw.schema[col] == pl.Int32, col
    joined = _join(cw, sched.select("game_id", "home_id"), "espn_game_id", "game_id")
    assert joined.height == 2
    assert (joined["home_espn_team_id"] == joined["home_id"]).all()


def test_ncaa_team_box_bridges_to_the_espn_schedule() -> None:
    ncaa, sched = mbb.load_ncaa_mbb_team_box(2026), mbb.load_mbb_schedule(2026)
    for col in ("espn_game_id", "home_espn_team_id", "away_espn_team_id", "team_espn_team_id"):
        assert ncaa.schema[col] == pl.Int32, col
    assert ncaa.schema["contest_id"] == pl.String  # NCAA's own id is opaque text, left alone
    joined = _join(ncaa, sched.select("game_id"), "espn_game_id", "game_id")
    assert joined.height == ncaa.height == 4


def test_nba_game_rosters_and_rosters_join_the_box() -> None:
    rosters, sched, box = (
        nba.load_nba_game_rosters(2026),
        nba.load_nba_schedule(2026),
        nba.load_nba_player_boxscore(2026),
    )
    assert _join(rosters, sched.select("game_id"), "game_id", "game_id").height == rosters.height == 26
    season_rosters = nba.load_nba_rosters(2026)
    matched = _join(box.select("athlete_id").unique(), season_rosters.select("athlete_id"), "athlete_id", "athlete_id")
    assert matched.height > 0
