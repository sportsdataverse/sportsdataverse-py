"""NFL receiver attribution: an interception's intended receiver is targeted; a text fragment is no name.

Two 2026 week-10 games (``fixtures/box_convention/``, ESPN site summaries trimmed as
``fixtures/README.md`` describes, captured 2026-11-19) against the oracle nflverse publishes
for them, ``load_player_stats(2026, "week")`` (``nflverse_targets_2026_w10.csv``: every player
with a target on the four teams, joined to the box's spelling of his name):

* 401873053 NE @ DET -- three interceptions "intended for" A.St. Brown, J.Williams and M.Hollins,
  and "T.DeVito pass incomplete short left." whose receiver the old chain read as "short left.".
* 401873055 CAR @ NO -- "... T.Shough pass incomplete short right [P.Umanmielen]. ** Injury Update:
  CAR-D.Lloyd has returned to the game.", read as "theme."; C.Olave's interception target.

Four ``nflverse-dev/nfl-raw`` captures (``nfl/espn/raw/<season>/<id>.json.gz`` at 1782be66, trimmed the same
way) pin the cases around them: 320909007 (DEN-PIT 2012, spelled-out receivers), 401326443 (TEN-IND 2021,
T.Lewis fumbles his interception return), 401326410 (TB-PHI 2021, a lateral on an interception return) and
321230025 (ARI-SF 2012, "Pass intended for A.Roberts, ball deflected & caught by S.Kelemete").
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import nfl_pbp
from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures" / "box_convention"
GAMES = (401873053, 401873055)
NFLVERSE = pl.read_csv(FIX / "nflverse_targets_2026_w10.csv")


@functools.cache
def _process(gid: int):
    with gzip.open(FIX / f"summary_{gid}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = NFLPlayProcess(gameId=gid, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    box = proc.run_processing_pipeline()["advBoxScore"]
    return proc.plays_frame, box


@pytest.mark.parametrize("gid", GAMES)
def test_targets_match_nflverse(gid):
    _plays, box = _process(gid)
    lines = {(int(r["pos_team"]), r["receiver_player_name"]): r["Tar"] for r in box["receiver"]}
    oracle = NFLVERSE.filter(pl.col("game_id") == gid)
    assert oracle.height > 10
    for team, name, targets in oracle.select("team_id", "receiver_player_name", "targets").iter_rows():
        assert lines.get((team, name)) == targets, name
    # and no named receiver nflverse does not have
    named = {k for k in lines if k[1] is not None}
    assert named == set(oracle.select("team_id", "receiver_player_name").iter_rows())


def test_interceptions_name_the_intended_receiver():
    plays, _box = _process(401873053)
    picks = plays.filter(pl.col("int") == True).sort("receiver_player_name")  # noqa: E712
    assert picks["receiver_player_name"].to_list() == ["A.St. Brown", "J.Williams", "M.Hollins"]
    assert picks["target"].all() and not picks["completion"].any()
    assert (picks["yds_receiving"] == 0).all()


@pytest.mark.parametrize("gid", GAMES)
def test_every_receiver_name_is_name_shaped(gid):
    plays, _box = _process(gid)
    names = plays["receiver_player_name"].drop_nulls().unique().to_list()
    assert names
    assert all(pl.Series(names).str.contains(nfl_pbp._NFL_NAME_SHAPED)), names
    assert not {"short left.", "theme."} & set(names)


@pytest.mark.parametrize(
    "fragment",
    ["short left.", "theme.", "stop", "", "St. Johnson. Dropped near sideline at BUF."],
)
def test_text_fragments_are_not_name_shaped(fragment):
    assert not pl.Series([fragment]).str.contains(nfl_pbp._NFL_NAME_SHAPED).item()


@pytest.mark.parametrize("name", ["A.St. Brown", "J.Williams", "Amani Toomer", "T.J.Houshmandzadeh", "Bri.Thomas"])
def test_names_are_name_shaped(name):
    assert pl.Series([name]).str.contains(nfl_pbp._NFL_NAME_SHAPED).item()


# --- the receiver on older text forms and around an interception (nfl-raw captures) ---------


def test_spelled_out_receivers_pass_the_name_gate():
    """2012 "pass complete to Eric Decker for 13 yards": the chain's value, trailing space and all."""
    plays, _box = _process(320909007)
    caught = plays.filter(pl.col("completion") == True)  # noqa: E712
    assert {"Eric Decker", "Demaryius Thomas", "Antonio Brown"} <= set(caught["receiver_player_name"].drop_nulls())
    # only the three "DEN. Ben Roethlisberger pass complete Isaac Redman" fragments lose their receiver
    assert caught["receiver_player_name"].null_count() == 3


def test_a_fumble_on_the_return_is_the_returners():
    plays, box = _process(401326443)
    pick = plays.filter((pl.col("int") == True) & pl.col("text").str.contains("FUMBLES"))  # noqa: E712
    assert pick.select("receiver_player_name", "fumble_player_name").rows() == [("A.Firkser", "T.Lewis")]
    firkser = next(r for r in box["receiver"] if r["receiver_player_name"] == "A.Firkser")
    assert (firkser["Tar"], firkser["Fum"]) == (2, 0)


def test_a_lateral_on_the_return_moves_no_receiving_yards():
    _plays, box = _process(401326410)
    lines = {r["receiver_player_name"]: r for r in box["receiver"]}
    assert (lines["A.Brown"]["Tar"], lines["A.Brown"]["Yds"]) == (13, 93.0)
    assert "D.Slay" not in lines  # the defender who took the lateral is no receiver


def test_intended_for_on_a_completion_does_not_move_the_catch():
    plays, _box = _process(321230025)
    play = plays.filter(pl.col("text").str.contains("caught by S.Kelemete"))
    assert play.select("completion", "receiver_player_name").rows() == [(True, "S.Kelemete")]
