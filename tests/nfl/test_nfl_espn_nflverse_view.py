"""``espn_nfl_pbp_nflverse``: ESPN processor output in nflverse ``play_by_play`` shape, scored against nflverse.

Oracle: ``fixtures/nflverse/espn_nflverse_view.parquet`` (nflverse's play-by-play for 2024_04_PHI_TB,
2014_11_MIN_CHI and 2007_14_SD_TEN, the games of the committed ESPN captures 401671740, 400554340 and
271209010) and ``espn_nflverse_view_schedule.parquet`` (their nflverse schedule rows). Provenance:
``fixtures/README.md``. The captures run through ``NFLPlayProcess`` offline, so the view is checked on the
processor's own frame (which carries no ``nflverse_game_id``: the schedule's ``espn`` id supplies it).
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess, espn_nfl_pbp_nflverse

FIX = Path(__file__).parent / "fixtures"
GAMES = {401671740: "2024_04_PHI_TB", 400554340: "2014_11_MIN_CHI", 271209010: "2007_14_SD_TEN"}
#: Captures for single rules, kept out of the whole-game capture set (``fixtures/espn_nflverse_view/``):
#: 2019_13_OAK_KC has a defensive two-point return its touchdown row counts; 2024_01_NYJ_SF has a split
#: two-point pass and run (and a whole game ESPN clocks after the snap, so it is not in the floor set).
EXTRA = {401128067: "2019_13_OAK_KC", 401671696: "2024_01_NYJ_SF"}

#: Every view column nflverse also has must agree on >= 97% of each game's joined plays, except these
#: (floor = the lowest observed game rate, rounded down; the whole-season rates are in the PR body):
FLOORS = {
    # the processor's own timeout count; before 2014 ESPN lists few timeouts (2007 game: 0.54-0.62)
    "home_timeouts_remaining": 0.54,
    "away_timeouts_remaining": 0.57,
    "posteam_timeouts_remaining": 0.61,
    "defteam_timeouts_remaining": 0.58,
    # pre-2009 stub texts ("Rush", "Pass Completion") carry no direction / completion / yards (2007 game)
    "pass_length": 0.64,
    "pass_location": 0.64,
    "run_location": 0.64,
    "complete_pass": 0.82,
    "incomplete_pass": 0.82,
    "passing_yards": 0.82,
    "receiving_yards": 0.82,
    "yards_gained": 0.95,
    "punt_attempt": 0.91,
    "touchback": 0.95,
    # ESPN stamps scoring plays (and whole 2024 games) with the clock after the snap (2024 game: 0.905)
    "quarter_seconds_remaining": 0.90,
    "half_seconds_remaining": 0.90,
    "game_seconds_remaining": 0.90,
    "time": 0.90,
    # ESPN's 2007 drive splits differ from the NFL feed's
    "drive": 0.96,
    "drive_time_of_possession": 0.91,
    "drive_ended_with_score": 0.96,
    "ydsnet": 0.96,
    "penalty_yards": 0.96,
}
#: ESPN's text is not the NFL feed's description: carried for the text rules, never equal.
UNSCORED = {"desc"}
#: nflfastR's ``add_game_data`` columns: straight from the schedule, so exact.
SCHEDULE_COLUMNS = (
    "old_game_id",
    "game_date",
    "location",
    "result",
    "total",
    "home_score",
    "away_score",
    "div_game",
    "roof",
    "surface",
    "temp",
    "wind",
    "home_coach",
    "away_coach",
    "game_stadium",
    "stadium_id",
    "spread_line",
    "total_line",
)


@functools.cache
def _plays(espn_game_id: int) -> pl.DataFrame:
    path = FIX / ("espn_nflverse_view" if espn_game_id in EXTRA else "") / f"summary_{espn_game_id}_trimmed.json.gz"
    summary = json.load(gzip.open(path, "rt", encoding="utf-8"))
    proc = NFLPlayProcess(gameId=espn_game_id, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame


def _schedule() -> pl.DataFrame:
    return pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view_schedule.parquet")


@functools.cache
def _view() -> pl.DataFrame:
    return pl.concat([espn_nfl_pbp_nflverse(_plays(g), _schedule()) for g in GAMES])


def _joined() -> pl.DataFrame:
    nv = pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view.parquet")
    view = _view()
    assert view.schema["game_id"] == nv.schema["game_id"] == pl.Utf8
    assert view.schema["play_id"] == nv.schema["play_id"] == pl.Int64
    return view.join(nv, on=["game_id", "play_id"], how="inner", suffix="__nv")


def _rate(j: pl.DataFrame, c: str) -> float:
    a, b = pl.col(c), pl.col(f"{c}__nv")
    if j.schema[c].is_numeric():
        eq = (a.cast(pl.Float64) - b.cast(pl.Float64)).abs() <= 1e-4
    else:
        eq = a.cast(pl.Utf8) == b.cast(pl.Utf8)
    return j.select((eq | (a.is_null() & b.is_null())).fill_null(False).mean()).item()


def test_keys_join_nflverse_and_teams_are_nflverse_abbreviations():
    view, j = _view(), _joined()
    assert view.select("game_id", "play_id").n_unique() == view.height
    assert set(view["game_id"].unique()) == set(GAMES.values())
    # every ESPN play of the three games joins except the split tries and ESPN-only rows (497 of 540)
    assert j.height >= 490
    old = j.filter(pl.col("game_id") == "2007_14_SD_TEN")
    # the game id keeps "SD"; nflverse's play-by-play (and the view) write the franchise's "LAC"
    assert set(old["home_team"].unique()) == {"TEN"} and set(old["away_team"].unique()) == {"LAC"}
    assert set(old["posteam"].drop_nulls().unique()) == {"LAC", "TEN"}


@pytest.mark.parametrize("game_id", list(GAMES.values()))
def test_every_shared_column_meets_its_floor(game_id):
    j = _joined().filter(pl.col("game_id") == game_id)
    shared = [c for c in _view().columns if f"{c}__nv" in j.columns and c not in UNSCORED]
    assert len(shared) >= 100
    low = {c: round(r, 4) for c in shared if (r := _rate(j, c)) < FLOORS.get(c, 0.97)}
    assert low == {}, f"{game_id}: columns below their floor: {low}"


def test_schedule_columns_are_exact():
    j = _joined()
    rates = {c: _rate(j, c) for c in SCHEDULE_COLUMNS}
    assert rates == {c: 1.0 for c in SCHEDULE_COLUMNS}


def test_split_try_rows_and_touchdown_scores():
    """2024: each folded try is its own extra_point / two-point row; the touchdown row leaves its points out."""
    view = _view().filter(pl.col("game_id") == "2024_04_PHI_TB")
    nv = pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view.parquet").filter(pl.col("game_id") == "2024_04_PHI_TB")
    tries = view.filter(pl.col("play_type_nfl").is_in(["XP_KICK", "PAT2"]))
    assert tries.height == nv.filter(pl.col("play_type_nfl").is_in(["XP_KICK", "PAT2"])).height == 6
    assert tries["posteam"].null_count() == 0  # the scoring side, from the split's numeric team id
    tds = _joined().filter((pl.col("game_id") == "2024_04_PHI_TB") & (pl.col("touchdown") == 1))
    assert tds.height > 0
    for c in ("posteam_score_post", "total_home_score", "total_away_score"):
        assert (tds[c] == tds[f"{c}__nv"]).all(), c


def test_missing_game_id_and_float_ids_are_refused():
    plays = _plays(401671740)
    with pytest.raises(ValueError, match="nflverse_game_id"):
        espn_nfl_pbp_nflverse(plays)
    with pytest.raises(ValueError, match="lack"):
        espn_nfl_pbp_nflverse(plays.drop("start.pos_team.id"), _schedule())
    with pytest.raises(ValueError, match="Float64"):
        espn_nfl_pbp_nflverse(plays, _schedule().with_columns(pl.col("espn").cast(pl.Float64)))


def test_a_declined_penalty_is_no_penalty():
    """nflverse's ``penalty`` is 0 on a play whose only penalty was declined (ESPN flags it)."""
    j = _joined().filter(pl.col("desc").str.contains("(?i)declined") & (pl.col("penalty__nv") == 0))
    assert j.height >= 3
    assert (j["penalty"] == 0).all()


def test_a_frame_already_split_gives_the_same_view():
    """``split_pat_rows`` output passed in (either ``split_pats``) is the view of the raw frame."""
    from sportsdataverse.nfl import split_pat_rows

    plays, sched = _plays(401671740), _schedule()
    want = espn_nfl_pbp_nflverse(plays, sched)
    for split in (True, False):
        assert espn_nfl_pbp_nflverse(split_pat_rows(plays), sched, split_pats=split).equals(want)


def test_a_schedule_without_the_game_is_refused():
    other = _schedule().filter(pl.col("game_id") != "2024_04_PHI_TB")
    with pytest.raises(ValueError, match="401671740"):
        espn_nfl_pbp_nflverse(_plays(401671740), other)


def _nflverse(game_id: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view.parquet").filter(pl.col("game_id") == game_id)


def test_a_float_game_id_is_refused_when_nflverse_game_id_is_there():
    """The release shape (``nflverse_game_id`` already on the rows) still goes through the game-id dtype guard."""
    plays = _plays(401671740).with_columns(pl.lit("2024_04_PHI_TB").alias("nflverse_game_id"))
    assert espn_nfl_pbp_nflverse(plays)["game_id"].unique().to_list() == ["2024_04_PHI_TB"]
    with pytest.raises(ValueError, match="game_id is Float64"):
        espn_nfl_pbp_nflverse(plays.with_columns(pl.col("game_id").cast(pl.Float64)))


def test_split_two_point_tries_keep_pass_and_run():
    """A split two-point row carries no two_point_pass flag; its play_type still follows nflverse's (in game order)."""
    view = espn_nfl_pbp_nflverse(_plays(401671696), _schedule())
    ours = view.filter(pl.col("play_type_nfl") == "PAT2").sort("espn_play_id")
    nv = _nflverse("2024_01_NYJ_SF").filter(pl.col("play_type_nfl") == "PAT2").sort("play_id")
    assert ours.height == nv.height == 2
    assert ours["play_type"].to_list() == nv["play_type"].to_list() == ["run", "pass"]


def test_a_defensive_two_point_return_is_scored_on_the_try_row():
    """2019_13_OAK_KC: KC returns LV's blocked PAT for 2. ESPN's touchdown row counts them; nflverse's PAT row does."""
    view = espn_nfl_pbp_nflverse(_plays(401128067), _schedule())
    cols = ["posteam_score", "defteam_score", "posteam_score_post", "defteam_score_post", "total_home_score"]
    td = view.filter(pl.col("play_id") == 3688)
    pat = view.filter(pl.col("espn_play_id") == td["espn_play_id"][0] + 1)
    nv = _nflverse("2019_13_OAK_KC")
    nv_td, nv_pat = nv.filter(pl.col("play_id") == 3688), nv.filter(pl.col("play_id") == 3713)
    assert pat["play_type_nfl"].to_list() == nv_pat["play_type_nfl"].to_list() == ["XP_KICK"]
    for c in cols:
        assert td[c][0] == nv_td[c][0], c
        assert pat[c][0] == nv_pat[c][0], c
