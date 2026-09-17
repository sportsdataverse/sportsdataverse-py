"""Game-state columns ``NFLPlayProcess`` derives on real ESPN data: timeouts,
roof, field position, clock, and the pipeline's re-run / input contract.

Fixture: ``tests/nfl/fixtures/summary_401872922.json`` (CLE @ JAX, 2026 REG week
1), processed offline through ``espn_nfl_pbp(summary=...)``.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures" / "summary_401872922.json"
GAME_ID = 401872922
JAX, CLE = 30, 5  # home, away


@pytest.fixture(scope="module")
def summary() -> dict:
    return json.loads(FIX.read_text())


@pytest.fixture(scope="module")
def frame(summary) -> pl.DataFrame:
    proc = NFLPlayProcess(gameId=GAME_ID)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame


def _row(frame: pl.DataFrame, play_id: int) -> dict:
    return frame.filter(pl.col("id") == play_id).row(0, named=True)


# ---------------------------------------------------------------------------
# N1 -- timeouts are charged from the team token ESPN writes, league codes included
# ---------------------------------------------------------------------------

_HOME = ("JAX", "Jacksonville", "Jaguars", "Jacksonville")
_AWAY = ("CLE", "Cleveland", "Browns", "Cleveland")


@pytest.mark.parametrize(
    ("text", "home", "away", "side"),
    [
        # 2008+ shape; the league code differs from ESPN's abbreviation for CLE/BAL/HOU/ARI/WSH/LAR/STL
        ("Timeout #1 by CLV at 04:52.", _HOME, _AWAY, "away"),
        ("Timeout #2 by JAX at 01:45.", _HOME, _AWAY, "home"),
        (
            "Timeout #1 by BLT at 00:24.",
            ("LAC", "Los Angeles", "Chargers", "Los Angeles"),
            ("BAL", "Baltimore", "Ravens", "Baltimore"),
            "away",
        ),
        (
            "Timeout #3 by LA at 00:12.",
            ("LAR", "Los Angeles", "Rams", "Los Angeles"),
            ("ARI", "Arizona", "Cardinals", "Arizona"),
            "home",
        ),
        (
            "Timeout #2 by BUF at 01:15. injury in last 2 minutes",
            ("NE", "New England", "Patriots", "New England"),
            ("BUF", "Buffalo", "Bills", "Buffalo"),
            "away",
        ),
        ("Timeout #2 NYJ", ("NYJ", "New York", "Jets", "New York"), ("NYG", "New York", "Giants", "New York"), "home"),
        # 2002-2007 shape: a location, the abbreviation, or ESPN's nickname
        (
            "Atlanta timeout; 02:42 remaining 2nd quarter",
            ("GB", "Green Bay", "Packers", "Green Bay"),
            ("ATL", "Atlanta", "Falcons", "Atlanta"),
            "away",
        ),
        (
            "SF timeout; 02:21 remaining 2nd quarter",
            ("SF", "San Francisco", "49ers", "San Francisco"),
            ("SEA", "Seattle", "Seahawks", "Seattle"),
            "home",
        ),
        (
            "Indy timeout; 10:49 remaining 2nd quarter",
            ("IND", "Indianapolis", "Colts", "Indianapolis"),
            ("TEN", "Tennessee", "Titans", "Tennessee"),
            "home",
        ),
        (
            "Philly timeout; 03:30 remaining 4th quarter",
            ("STL", "St. Louis", "Rams", "St. Louis"),
            ("PHI", "Philadelphia", "Eagles", "Philadelphia"),
            "away",
        ),
        (
            "Timeout DENVER BRONCOS, clock 9:52.",
            ("DEN", "Denver", "Broncos", "Denver"),
            ("HOU", "Houston", "Texans", "Houston"),
            "home",
        ),
        # the whole-text substring match charged these to the wrong side (LV in CLV, LA in Cleveland)
        ("Timeout #1 by CLV at 04:52.", ("LV", "Las Vegas", "Raiders", "Las Vegas"), _AWAY, "away"),
        (
            "Cleveland timeout; 01:46 remaining 2nd quarter",
            _AWAY,
            ("LAR", "Los Angeles", "Rams", "Los Angeles"),
            "home",
        ),
        # unattributable rows charge nobody
        ("TimeOut", _HOME, _AWAY, None),
        ("Timeout NFC, clock 12:00", _HOME, _AWAY, None),
        ("", _HOME, _AWAY, None),
        (None, _HOME, _AWAY, None),
    ],
)
def test_timeout_side_parses_the_team_token(text, home, away, side):
    from sportsdataverse.nfl.nfl_pbp import _nfl_timeout_side

    assert _nfl_timeout_side(text, home, away) == side


def test_legacy_code_timeouts_are_charged_on_the_fixture(frame):
    # ESPN writes "Timeout #N by CLV": all three CLE timeouts, none for JAX
    t = frame.filter(pl.col("type.text") == "Timeout").sort("id")
    assert t["text"].to_list() == [
        "Timeout #1 by CLV at 04:52.",
        "Timeout #2 by CLV at 01:45.",
        "Timeout #1 by CLV at 00:50.",
    ]
    assert t["awayTimeoutCalled"].to_list() == [True, True, True]
    assert t["homeTimeoutCalled"].to_list() == [False, False, False]
    assert _row(frame, 4018729221632)["end.awayTeamTimeouts"] == 2
    assert _row(frame, 4018729221802)["end.awayTeamTimeouts"] == 1
    # CLE is on defense for JAX's punt right after its second timeout
    punt = _row(frame, 4018729221815)
    assert (punt["start.posTeamTimeouts"], punt["start.defPosTeamTimeouts"]) == (3, 1)
    assert frame["end.homeTeamTimeouts"].unique().to_list() == [3]


# ---------------------------------------------------------------------------
# N2 -- each team's timeouts reset to 3 at the start of the second half
# ---------------------------------------------------------------------------


def test_timeouts_reset_at_the_start_of_the_second_half(frame):
    f = frame.sort("game_play_number")
    first_h2 = f.filter(pl.col("period") == 3).row(0, named=True)
    last_h1 = f.filter(pl.col("period") == 2).row(-1, named=True)
    assert last_h1["end.awayTeamTimeouts"] == 1
    assert (first_h2["start.homeTeamTimeouts"], first_h2["start.awayTeamTimeouts"]) == (3, 3)
    assert (first_h2["start.posTeamTimeouts"], first_h2["start.defPosTeamTimeouts"]) == (3, 3)
    assert f.filter(pl.col("period") >= 3)["start.awayTeamTimeouts"].min() == 2
