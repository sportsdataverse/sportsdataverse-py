"""Game-state columns ``NFLPlayProcess`` derives on real ESPN data: timeouts,
roof, field position, clock, and the pipeline's re-run / input contract.

Fixture: ``tests/nfl/fixtures/summary_401872922.json`` (CLE @ JAX, 2026 REG week
1), processed offline through ``espn_nfl_pbp(summary=...)``.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures" / "summary_401872922.json"
GAME_ID = 401872922
JAX, CLE = 30, 5  # home, away


@pytest.fixture(scope="module")
def summary() -> dict:
    return json.loads(FIX.read_text())


def _recorded_run(summary: dict):
    """Run the pipeline offline, recording the frames/matrices handed to the models."""
    import sportsdataverse.nfl.nfl_fourth_down as fd
    import sportsdataverse.nfl.nfl_pbp as mod

    seen: dict = {"fourth": [], "two_pt": [], "xpass": [], "matrices": []}

    def record(key, fn):
        def wrapper(df, *a, **k):
            seen[key].append(df)
            return fn(df, *a, **k)

        return wrapper

    real_dmatrix = mod.DMatrix

    def dmatrix(data, *a, **k):
        seen["matrices"].append((list(k.get("feature_names") or []), np.asarray(data)))
        return real_dmatrix(data, *a, **k)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(fd, "get_4th_down_probs", record("fourth", fd.get_4th_down_probs))
        mp.setattr(fd, "get_2pt_probs", record("two_pt", fd.get_2pt_probs))
        mp.setattr(mod, "calculate_xpass", record("xpass", mod.calculate_xpass))
        mp.setattr(mod, "DMatrix", dmatrix)
        proc = NFLPlayProcess(gameId=GAME_ID)
        proc.espn_nfl_pbp(summary=summary)
        out = proc.run_processing_pipeline()
    return proc, out, seen


@pytest.fixture(scope="module")
def run(summary):
    return _recorded_run(summary)


@pytest.fixture(scope="module")
def frame(run) -> pl.DataFrame:
    return run[0].plays_frame


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


# ---------------------------------------------------------------------------
# N7 -- the nflverse-shape decision views carry nflverse's spread_line sign
# ---------------------------------------------------------------------------


def test_decision_views_carry_home_favoured_positive_spread_line(run):
    # DraftKings: JAX (home) -8.5. nflverse spread_line is positive when the home
    # team is favoured (the convention the nfl4th models were trained on).
    _, out, seen = run
    assert np.ravel(out["homeTeamSpread"]).tolist() == [8.5]
    assert seen["fourth"] and seen["two_pt"]
    for key in ("fourth", "two_pt"):
        assert seen[key][0]["spread_line"].unique().to_list() == [8.5], key


# ---------------------------------------------------------------------------
# N3 -- one game-level roof reaches every model (EP, CP, xpass, 4th down, 2pt)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dome_run(summary):
    import copy

    indoor = copy.deepcopy(summary)
    indoor["gameInfo"]["venue"]["indoor"] = True
    pristine = copy.deepcopy(indoor)
    return (*_recorded_run(indoor), indoor, pristine)


def _roof_seen(seen: dict) -> set:
    """Every roof value the models received: frame columns and one-hot matrix rows."""
    roofs = set()
    for key in ("fourth", "two_pt", "xpass"):
        for view in seen[key]:
            roofs |= set(view["roof"].to_list())
    names = {(1, 0, 0): "retractable", (0, 1, 0): "dome", (0, 0, 1): "outdoors"}
    for features, x in seen["matrices"]:
        if "dome" in features:
            cols = x[:, [features.index(c) for c in ("retractable", "dome", "outdoors")]]
            roofs |= {names.get(tuple(int(v) for v in row), str(row)) for row in cols}
    return roofs


def test_roof_defaults_to_outdoors_for_every_model(run, frame):
    # ESPN's summary venue carries no roof field: the documented default applies everywhere
    _, _, seen = run
    assert "indoor" not in run[1]["gameInfo"]["venue"]
    assert frame["roof"].unique().to_list() == ["outdoors"]
    assert seen["xpass"] and seen["matrices"]
    assert _roof_seen(seen) == {"outdoors"}


def test_indoor_venue_is_a_dome_for_every_model(dome_run, frame):
    proc, _, seen, _, _ = dome_run
    assert proc.plays_frame["roof"].unique().to_list() == ["dome"]
    assert _roof_seen(seen) == {"dome"}
    ep = frame.select("id", "EP_start").join(proc.plays_frame.select("id", "EP_start"), on="id", suffix="_dome")
    assert (ep["EP_start"] - ep["EP_start_dome"]).abs().max() > 0.01


# ---------------------------------------------------------------------------
# N4 -- a second run returns the processed result; the caller's summary is untouched
# ---------------------------------------------------------------------------


def test_rerun_returns_the_result_and_the_supplied_summary_is_not_mutated(dome_run):
    proc, out, _, passed, pristine = dome_run
    assert passed == pristine
    again = proc.run_processing_pipeline()
    assert again is out


def test_rerun_after_a_corrupt_short_circuit_returns_the_same_payload(summary):
    import copy

    pregame = copy.deepcopy(summary)
    pregame["drives"] = {}
    proc = NFLPlayProcess(gameId=GAME_ID)
    proc.espn_nfl_pbp(summary=pregame)
    first = proc.run_processing_pipeline()
    assert first is not None and first["plays"] == []
    assert proc.run_processing_pipeline() is first
