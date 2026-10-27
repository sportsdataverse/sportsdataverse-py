"""End-to-end native PBP build tests + REQUIRED_COLUMNS contract check."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl.shield_pbp.build import build_pbp

GAME = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield" / "2024_01_BAL_KC.json.gz"
pytestmark = pytest.mark.skipif(not GAME.exists(), reason="2024_01_BAL_KC fixture not present")

# nfl-data's Track 6 ``model_training.play_level.ingest.REQUIRED_COLUMNS`` (the
# EP/WP/CP training contract the native build must satisfy), inlined verbatim.
REQUIRED_COLUMNS = [
    "game_id",
    "play_id",
    "qtr",
    "game_half",
    "down",
    "yardline_100",
    "ydstogo",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
    "posteam",
    "defteam",
    "home_team",
    "season",
    "roof",
    "spread_line",
    "score_differential",
    "sp",
    "touchdown",
    "td_team",
    "field_goal_result",
    "safety",
    "result",
    "home_score",
    "away_score",
    "air_yards",
    "pass_location",
    "complete_pass",
    "incomplete_pass",
    "interception",
    "receiver_player_id",
    "receiver_player_name",
    "qb_hit",
]


def _df():
    game = json.load(gzip.open(GAME, "rt", encoding="utf-8"))
    return build_pbp(game, roof="outdoors", spread_line=-3.0, total_line=44.5)


def test_required_columns_present():
    df = _df()
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    assert not missing, f"native PBP missing REQUIRED_COLUMNS: {missing}"


def test_score_differential_progression():
    df = _df()
    # Pre-snap on the very first scrimmage play, score is 0-0.
    first = df.filter(pl.col("down").is_not_null()).head(1)
    assert first["score_differential"][0] == 0
    # Score differential takes on multiple values over the game.
    assert df["score_differential"].n_unique() > 3


def test_timeouts_in_range():
    df = _df()
    for col in ("posteam_timeouts_remaining", "defteam_timeouts_remaining"):
        vals = df.filter(pl.col(col).is_not_null())[col]
        assert vals.min() >= 0 and vals.max() <= 3


def test_final_scores_and_result():
    df = _df()
    # KC (home) beat BAL (away) 27-20 in the 2024 opener.
    assert df["home_score"].unique().to_list() == [27]
    assert df["away_score"].unique().to_list() == [20]
    assert df["result"].unique().to_list() == [7]


def test_field_goal_result_and_roof_spread():
    df = _df()
    fg = set(df.filter(pl.col("field_goal_result").is_not_null())["field_goal_result"].unique().to_list())
    assert "made" in fg
    assert df["roof"].unique().to_list() == ["outdoors"]
    assert df["spread_line"].unique().to_list() == [-3.0]
    assert df["total_line"].unique().to_list() == [44.5]


def test_pass_location_populated_on_completions():
    df = _df()
    comp = df.filter(pl.col("complete_pass") == 1)
    locs = set(comp.filter(pl.col("pass_location").is_not_null())["pass_location"].unique().to_list())
    assert locs <= {"left", "middle", "right"}
    assert len(locs) >= 2  # a real game has passes to multiple locations


def test_build_pipeline_ordering_contract():
    """Pin the build.py step order (parse -> ... -> add_labels -> add_drive_detail
    -> add_series_data). add_drive_detail/add_series_data return typed nulls
    instead of raising when a required column is missing (drives.py/series.py),
    so a future reorder or upstream column rename would silently null
    fixed_drive/series/drive columns across the whole game while every other
    test stayed green. This asserts the columns those two steps produce are
    actually populated on a real game."""
    df = _df()
    assert df["fixed_drive"].null_count() == 0
    fixed_drive = df["fixed_drive"].to_list()
    assert fixed_drive == sorted(fixed_drive)  # monotone non-decreasing within the game
    assert df["series"].null_count() == 0
    assert set(df["pass"].unique().to_list()) == {0, 1}
    assert set(df["rush"].unique().to_list()) == {0, 1}
    # null exactly where play_type is (R's if_else passes NA through)
    assert (df["qb_dropback"].is_null() == df["play_type"].is_null()).all()


# ---------------------------------------------------------------------------
# nflverse parity on a real 2015 game: a pick-six, a relocated club, Shield's own drive records
# ---------------------------------------------------------------------------

DET_SD = GAME.parent / "2015_01_DET_SD.json.gz"
DET_SD_NFLVERSE = GAME.parent / "2015_01_DET_SD_nflverse.parquet"


def _det_sd(**overrides):
    game = json.loads(gzip.open(DET_SD).read())
    game.update(overrides)
    return game


def _vs_nflverse(name: str) -> pl.DataFrame:
    ours = build_pbp(json.loads(gzip.open(GAME.parent / f"{name}.json.gz").read()))
    nv = pl.read_parquet(GAME.parent / f"{name}_nflverse.parquet").with_columns(pl.col("play_id").cast(pl.Int64))
    return ours.with_columns(pl.col("play_id").cast(pl.Int64)).join(nv, on=["game_id", "play_id"], suffix="_nv")


_DRIVE_COLS = [
    "drive",
    "drive_play_count",
    "drive_time_of_possession",
    "drive_first_downs",
    "drive_inside20",
    "drive_ended_with_score",
    "drive_quarter_start",
    "drive_quarter_end",
    "drive_yards_penalized",
    "drive_start_transition",
    "drive_end_transition",
    "drive_game_clock_start",
    "drive_game_clock_end",
    "drive_start_yard_line",
    "drive_end_yard_line",
    "drive_play_id_started",
    "drive_play_id_ended",
]
_TEAM_COLS = ["home_team", "away_team", "td_team", "week"]
#: Compared only where both sides have a value: nflverse gives some END QUARTER / END GAME rows a posteam (and so a
#: drive) that Shield never states, and types a few plays our parser leaves untyped (a null qb_dropback).
_BOTH_SET = ["posteam", "defteam", "fixed_drive", "series", "qb_dropback"]
#: (oracle game, columns held null-strict: a one-sided null is a mismatch). 2015 is the 2003+ GSIS era; 2002 the
#: 2001-2002 one, whose few plays outside nflverse's drives (80 a season) are compared where both are set.
_ORACLES = {
    "2015_01_DET_SD": _DRIVE_COLS + _TEAM_COLS,
    "2002_16_STL_SEA": _TEAM_COLS,
}


#: Known divergences, not checked: nflverse counts a 2001-2002 scramble as a dropback when ITS text says "scrambles";
#: Shield's text for the same play often does not, so the scramble comes from the charting backfill, which nflfastR
#: computes qb_dropback ahead of (106 such 2002 plays, against 282 the other way before the order was fixed).
_SKIP = {("2002_16_STL_SEA", "qb_dropback")}


@pytest.mark.parametrize(
    ("game", "col"),
    [
        (g, c)
        for g, strict in _ORACLES.items()
        for c in strict + [c for c in _DRIVE_COLS + _BOTH_SET if c not in strict]
        if (g, c) not in _SKIP
    ],
)
def test_matches_nflverse(game, col):
    j = _vs_nflverse(game)
    a, b = j[col], j[f"{col}_nv"]
    if a.dtype.is_numeric():
        a, b = a.cast(pl.Float64), b.cast(pl.Float64)
    same = (a == b).fill_null(False) | (a.is_null() & b.is_null())
    if col not in _ORACLES[game]:
        same = same | a.is_null() | b.is_null()
    assert j.height > 100 and b.is_not_null().sum() >= 1, col  # td_team: a game has a handful
    assert same.all(), j.filter(~same).select("play_id", col, f"{col}_nv").head(5)


def test_the_pick_six_try_belongs_to_the_scorer():
    # 928: Rivers intercepted, returned for DET's touchdown; 950: the try falls between Shield's drive ranges.
    # Without its posteam, the kickoff back to SD (966) never opened a new fixed drive.
    rows = {r["play_id"]: r for r in build_pbp(_det_sd()).to_dicts()}
    assert rows[950]["posteam"] == "DET"
    assert rows[966]["fixed_drive"] == rows[928]["fixed_drive"] + 1


def test_relocated_club_takes_its_current_abbreviation_but_the_game_id_does_not():
    df = build_pbp(_det_sd())
    assert df["game_id"].unique().to_list() == ["2015_01_DET_SD"]
    assert set(df["home_team"].unique()) == {"LAC"} and "SD" not in set(df["posteam"].drop_nulls())


def test_postseason_week_continues_the_season():
    df = build_pbp(_det_sd(seasonType="POST", week=1))
    assert df["week"].unique().to_list() == [18]  # 2015 had 17 regular-season weeks


def test_a_game_with_no_drive_records_keeps_the_drive_dtypes():
    # the pregame payload has no drive record: its frame must still stack with an in-game one
    pregame = build_pbp(json.loads(gzip.open(GAME.parent / "2026_02_DET_BUF_pregame.json.gz").read()))
    ingame = build_pbp(json.loads(gzip.open(GAME.parent / "2026_02_DET_BUF_ingame_q2.json.gz").read()))
    cols = [c for c in ingame.columns if c == "drive" or c.startswith("drive_")]
    for c in cols:
        assert pregame.schema[c] == ingame.schema[c] != pl.Null, c
    assert (
        pl.concat([pregame.select(cols), ingame.select(cols)], how="vertical").height == pregame.height + ingame.height
    )


def test_playstats_fold_relocations_and_continue_playoff_weeks():
    from sportsdataverse.nfl.shield_pbp.playstats import build_playstats_frame

    stats = build_playstats_frame(_det_sd(seasonType="POST", week=1))
    assert "SD" not in set(stats["team_abbr"].drop_nulls()) and "LAC" in set(stats["team_abbr"].drop_nulls())
    assert stats["week"].unique().to_list() == [18]


def test_a_play_whose_drive_has_no_record_keeps_the_aggregate():
    # a live game's open drive: the play names drive N, the feed has no record for it yet
    game = _det_sd()
    last = max(d["sequence"] for d in game["driveChart"]["drives"])
    game["driveChart"]["drives"] = [d for d in game["driveChart"]["drives"] if d["sequence"] != last]
    df = build_pbp(game)
    open_drive = df.filter(
        pl.col("play_id").is_in([p["playId"] for p in game["driveChart"]["plays"] if p.get("driveSequence") == last])
    )
    assert open_drive.height > 0 and open_drive["drive_play_count"].is_not_null().all()
    # a play between drives (no drive number) stays null, as nflverse's does
    between = [p["playId"] for p in game["driveChart"]["plays"] if not p.get("driveSequence")]
    assert df.filter(pl.col("play_id").is_in(between))["drive_play_count"].null_count() > 0
