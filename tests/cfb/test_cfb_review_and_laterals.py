"""Replay reversals and laterals in the ESPN CFB processor.

A review appends the call it reversed to the ruling that stood ("... CALL OVERTURNED. (Original
Play: ... TOUCHDOWN)", 2009-16 "(Originally ruled a touchdown, overturned after replay review.)"),
and ESPN keeps the reversed call's type. A lateral is one row whose participants name the
recipient as the receiver, rusher or returner. Captures: ``fixtures/README.md`` (``review_laterals/``).
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures" / "review_laterals"  # outside the whole-game oracle's glob
# Processing a default-odds capture warns DefaultOddsWarning incidentally; the market plays no part
# in what these tests read. tests/cfb/test_cfb_pbp_offline.py::test_default_odds_warn asserts it.
pytestmark = pytest.mark.filterwarnings(
    r"ignore:CFB game \d+. no pregame spread is available:sportsdataverse.errors.DefaultOddsWarning"
)


@functools.cache
def _plays(game_id: int) -> pl.DataFrame:
    aux = FIX / f"lateral_aux_{game_id}.json.gz"
    kw = json.loads(gzip.decompress(aux.read_bytes())) if aux.exists() else {}
    proc = CFBPlayProcess(gameId=game_id, join_participants=False, **kw)
    proc.espn_cfb_pbp(summary=json.loads(gzip.decompress((FIX / f"summary_{game_id}_trimmed.json.gz").read_bytes())))
    proc.run_processing_pipeline()
    return proc.plays_frame


def _play(game_id: int, play_id: int) -> dict:
    return _plays(game_id).filter(pl.col("id") == play_id).row(0, named=True)


def test_a_lateral_reception_credits_the_catcher_and_names_the_lateral():
    """401856666: "#11 J.Garcia pass complete short right to #12 D.Haughton caught at TENN45, for 2
    yards ... lateral to #6 C.Nettles for 9 yards gain". ESPN's participants name Nettles the
    receiver and leave Haughton out; the catch, its 2 yards and its id are Haughton's."""
    play = _play(401856666, 401856666432)
    assert (play["receiver_player_name"], play["receiver_player_id"], play["yds_receiving"]) == (
        "D.Haughton",
        5085504,
        2,
    )
    assert (play["lateral_play"], play["lateral_player_name"], play["lateral_player_id"]) == (
        True,
        "C.Nettles",
        5300489,
    )
    assert play["passer_player_name"] == "Jake Garcia"  # the roles a lateral does not touch keep ESPN's names
    assert _plays(401856666)["lateral_play"].sum() == 1


def test_a_kickoff_return_lateral_keeps_the_returner():
    """401858440: "#2 N.Devereaux return 0 yards to the EMU00 lateral to #24 J.Mattord for 28 yards"."""
    play = _play(401858440, 401858440642)
    assert (play["kickoff_return_player_name"], play["kickoff_return_player_id"]) == ("N.Devereaux", 5083205)
    assert (play["lateral_player_name"], play["lateral_player_id"]) == ("J.Mattord", 5125733)


@pytest.mark.parametrize(
    ("game_id", "play_id", "stood"),
    [
        # "pass incomplete deep left ... (Original Play: ... TOUCHDOWN)"
        (401856671, 401856671229, "Pass Incompletion"),
        (401752747, 401752747245, "Rush"),  # 2025 "Short of the goal line". CALL OVERTURNED.
        (303580202, 303580202160, "Pass Incompletion"),  # 2010 "(Originally ruled a touchdown, overturned ...)"
    ],
)
def test_a_touchdown_replay_took_away_scores_nothing(game_id, play_id, stood):
    play = _play(game_id, play_id)
    assert "touchdown" in play["text"].lower()  # the reversed call still names it
    assert play["type.text"] == stood
    assert (play["td_play"], play["pass_td"], play["rush_td"], play["scoringPlay"]) == (False, False, False, False)
    assert play["EP_end"] < 6


def test_a_reversed_call_is_no_part_of_the_yardage():
    """401856671229 read the reversed catch's "for 29 yards" as receiving yards on an incompletion."""
    assert _play(401856671, 401856671229)["yds_receiving"] is None


def test_a_fumble_replay_took_away_is_no_fumble():
    """313160024: "Tyler Gaffney rush for 7 yards to the Oregn 44. Ruling on field of a fumble was
    reviewed and overturned." """
    play = _play(313160024, 313160024047)
    assert (play["fumble_vec"], play["yds_rushed"]) == (False, 7)
