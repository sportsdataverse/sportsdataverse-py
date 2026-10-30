"""ESPN-path NFL fixes from the 2026-10-29 Game on Paper parity rerun against nflfastR / nflverse.

Each case is a real play or game. The expected values are nflfastR's (``complete_pass``,
``play_type``) or nflverse ``stats_player_week``'s for the same game, not this package's output.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import nfl_pbp
from sportsdataverse.nfl.ep_wp import _add_success_first_down
from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess, _one_name_per_player

FIX = Path(__file__).parent / "fixtures"
_FLAGS = NFLPlayProcess._NFLPlayProcess__add_rush_pass_flags

# (type.text, text, rush, pass): nflfastR scores the first three as runs
RUSH_CASES = [
    # 2009_02_HOU_TEN: a run with no direction, typed by its fumble
    (
        "Fumble Recovery (Opponent)",
        "K.Collins to TEN 35 for -3 yards. FUMBLES, RECOVERED by HST-J.Zgonina at TEN 39.",
        True,
        False,
    ),
    # 2009: an aborted snap, ESPN's plain "Fumble" type
    (
        "Fumble",
        "T.Hightower FUMBLES (Aborted) at ARZ 47, recovered by ARZ-A.Boldin at ARZ 47. A.Boldin to ARZ 47 for no gain (K.Dawson).",
        True,
        False,
    ),
    # 2009: a run typed "Safety" (the fumble was recovered for a touchdown in the end zone)
    (
        "Safety",
        "R.Grant right end to GB -1 for -2 yards (L.Delmas). FUMBLES (L.Delmas), RECOVERED by DET-J.Dizon at GB -1. TOUCHDOWN.",
        True,
        False,
    ),
    # 2019_09_DET_OAK: an aborted snap the QB then threw: a pass, not a run
    (
        "Fumble Recovery (Own)",
        "(13:42) (Shotgun) D.Carr Aborted. A.James FUMBLES at DET 19, recovered by OAK-D.Carr at DET 30. D.Carr pass incomplete.",
        False,
        True,
    ),
    # an aborted snap from punt or field-goal formation is a run too (nflfastR: rush 1, 2009 K.Huber)
    ("Fumble Recovery (Own)", "(Punt formation) FUMBLES (Aborted) at NE 30, recovered by NE-J.Slater.", True, False),
    (
        "Fumble Recovery (Own)",
        "(Field Goal formation) K.Huber FUMBLES (Aborted) at DEN 10, and recovers at DEN 44.",
        True,
        False,
    ),
    # a run whose flag is pass interference is still a run
    (
        "Fumble Recovery (Opponent)",
        "K.Collins to TEN 35 for -3 yards. FUMBLES, RECOVERED by HST-J.Zgonina. PENALTY on HST, Defensive Pass Interference.",
        True,
        False,
    ),
]


@pytest.mark.parametrize(("type_text", "text", "rush", "pass_"), RUSH_CASES)
def test_fumble_typed_run_flags(type_text: str, text: str, rush: bool, pass_: bool) -> None:
    out = _FLAGS(NFLPlayProcess.__new__(NFLPlayProcess), pl.DataFrame({"type.text": [type_text], "text": [text]}))
    assert (out["rush"][0], out["pass"][0]) == (rush, pass_)


def _process(name: str, game_id: int) -> tuple[pl.DataFrame, dict]:
    summary = json.load(gzip.open(FIX / name, "rt", encoding="utf-8"))
    proc = NFLPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    game = proc.run_processing_pipeline()
    return proc.plays_frame, game["advBoxScore"]


def test_2009_pass_typed_completions_match_nflfastr() -> None:
    """2009_13_NYJ_BUF: ESPN types 16 of its passes plain "Pass"; nflfastR counts 17 completions.

    Before the fix only "Pass Reception"-style types were completions, so the box showed a handful.
    """
    plays, box = _process("era_play_order/summary_291203002_trimmed.json.gz", 291203002)
    assert int(plays["completion"].sum()) == 17
    assert {r["passer_player_name"]: r["Comp"] for r in box["pass"]} == {
        "R.Fitzpatrick": 9,
        "M.Sanchez": 7,
        "K.Clemens": 1,
    }


def test_nullified_pass_is_no_attempt_and_scoring_summary_passer() -> None:
    """2021_13_PHI_NYJ, against nflverse stats_player_week: Zach Wilson 23/38/226, Gardner Minshew 20/25/242.

    A penalty-nullified pass ESPN typed "Pass Incompletion" / "Pass Reception" counted as an attempt, and
    "Dallas Goedert Pass From Gardner Minshew II for 36 Yrds" credited the receiver (the "II" broke the match).
    """
    plays, box = _process("kickoff_misfiled_401326517_trimmed.json.gz", 401326517)
    nullified = plays.filter((pl.col("pass") == True) & (pl.col("penalty_no_play") == True))  # noqa: E712
    assert nullified.height > 0
    assert not nullified["pass_attempt"].any() and not nullified["completion"].any() and not nullified["target"].any()
    assert nullified["yds_receiving"].sum() == 0
    got = {r["passer_player_name"]: (r["Comp"], r["Att"], r["Yds"]) for r in box["pass"]}
    assert got == {"Z.Wilson": (23, 38, 226.0), "G.Minshew": (20, 25, 242.0)}


def test_nflverse_line_when_espn_has_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """2009 games carry no ESPN line; nflverse's spread_line (home margin) takes the place of home by 2.5."""
    from sportsdataverse.nfl.shield_pbp import to_espn_summary

    # the asset's espn id arrives numeric here: the filter casts it
    sched = pl.DataFrame({"espn": [290913001, 290913002], "spread_line": [-6.5, 3.0], "total_line": [41.0, 44.5]})
    seen = []
    monkeypatch.setattr(to_espn_summary, "_nflverse_schedule", lambda seasons: seen.append(seasons) or sched)
    proc = NFLPlayProcess.__new__(NFLPlayProcess)
    proc.gameId = 290913001
    # away favored by 6.5: the home team is not the favorite
    assert proc.__helper__nflverse_odds_information__(2009) == (6.5, 41.0, False, True)
    proc.gameId = 290913002
    assert proc.__helper__nflverse_odds_information__(2009) == (3.0, 44.5, True, True)
    proc.gameId = 1
    assert proc.__helper__nflverse_odds_information__(2009) is None
    assert seen == [(2009,)] * 3
    # no season in the header: no request
    assert proc.__helper__nflverse_odds_information__(None) is None
    assert len(seen) == 3


def test_nflverse_line_fetch_failure_keeps_default(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.nfl.shield_pbp import to_espn_summary

    def boom(*a, **k):
        raise ConnectionError("offline")

    monkeypatch.setattr(to_espn_summary, "_nflverse_schedule", boom)
    proc = NFLPlayProcess.__new__(NFLPlayProcess)
    proc.gameId = 290913001
    assert proc.__helper__nflverse_odds_information__(2009) is None


def test_supplied_summary_never_asks_nflverse(monkeypatch: pytest.MonkeyPatch) -> None:
    """Offline (a supplied summary) keeps the documented default and makes no request."""
    calls = []
    monkeypatch.setattr(NFLPlayProcess, "__helper__nflverse_odds_information__", lambda self, *a: calls.append(1))
    _process("era_play_order/summary_291203002_trimmed.json.gz", 291203002)
    assert calls == []


def test_one_name_per_player() -> None:
    """2019_07_JAX_CIN: ESPN id 14012 as "Andy Dalton" on 49 plays and "A.Dalton" on one."""
    df = pl.DataFrame(
        {
            "passer_player_id": [14012] * 3 + [None],
            "passer_player_name": ["Andy Dalton", "Andy Dalton", "A.Dalton", "No Id"],
            "rusher_player_id": [None] * 4,
            "rusher_player_name": [None] * 4,
        },
        schema_overrides={"rusher_player_id": pl.Int64, "rusher_player_name": pl.Utf8},
    )
    out = _one_name_per_player(df)
    assert out["passer_player_name"].to_list() == ["Andy Dalton", "Andy Dalton", "Andy Dalton", "No Id"]
    # the QBR box's athlete_name follows the unified name
    qbr = _one_name_per_player(df.with_columns(athlete_name=pl.col("passer_player_name")))
    assert qbr["athlete_name"].to_list() == ["Andy Dalton", "Andy Dalton", "Andy Dalton", "No Id"]


def test_success_and_first_down_follow_nflfastr() -> None:
    df = pl.DataFrame(
        {
            "epa": [0.4, -0.2, None, 0.0],
            "first_down_rush": [1.0, 0.0, 0.0, None],
            "first_down_pass": [0.0, 0.0, 0.0, 0.0],
            "first_down_penalty": [0.0, 0.0, 1.0, 0.0],
        }
    )
    out = _add_success_first_down(df)
    assert out["success"].to_list() == [1.0, 0.0, None, 0.0]
    # R's NA logic: a null part with no 1 is unknown, not 0
    assert out["first_down"].to_list() == [1.0, 0.0, 1.0, None]
    # without every part there is no first_down at all
    assert "first_down" not in _add_success_first_down(df.drop("first_down_penalty")).columns
    # a frame that already carries them is left alone
    kept = _add_success_first_down(out.with_columns(pl.lit(9.0).alias("success")))
    assert kept["success"].to_list() == [9.0] * 4


def test_module_regexes_are_bound() -> None:
    """The new text rules are module constants the processor reads (a typo would silently match nothing)."""
    assert nfl_pbp._NFL_PASS_TYPE_COMPLETE_RE and nfl_pbp._NFL_FUMBLE_ROW_RUN_RE and nfl_pbp._NFL_NOT_A_RUN_RE


def test_cp_scores_only_nflfastr_valid_passes() -> None:
    """nflfastR's add_cp leaves throwaways (no receiver) and spikes NA; scoring them gave a CPOE near -61
    on 832 2024 passes and pulled team CPOE ~3 points under nflverse's."""
    from sportsdataverse.nfl.ep_wp import calculate_completion_probability

    rows = {
        "season": [2024] * 4,
        "air_yards": [8.0, 8.0, 0.0, 80.0],
        "ydstogo": [10.0] * 4,
        "down": [1.0] * 4,
        "yardline_100": [60.0] * 4,
        "posteam": ["NE"] * 4,
        "home_team": ["NE"] * 4,
        "roof": ["outdoors"] * 4,
        "qb_hit": [0.0] * 4,
        "pass_location": ["left", "left", "left", "left"],
        "complete_pass": [1.0, 0.0, 0.0, 1.0],
        "incomplete_pass": [0.0, 1.0, 1.0, 0.0],
        "interception": [0.0] * 4,
        "qb_spike": [0.0, 0.0, 1.0, 0.0],
        # a completion, a throwaway (no receiver), a spike, an out-of-range air yards
        "receiver_player_id": ["00-1", None, None, "00-1"],
    }
    out = calculate_completion_probability(pl.DataFrame(rows))
    assert out["cp"].is_not_null().to_list() == [True, False, False, False]
    assert out["cpoe"].is_not_null().to_list() == [True, False, False, False]


def test_cp_reads_the_target_off_desc() -> None:
    """nflfastR's prepare_cp_data takes the receiver from desc when the id and name columns are empty."""
    from sportsdataverse.nfl.ep_wp import calculate_completion_probability

    rows = {
        "season": [2009] * 3,
        "air_yards": [8.0] * 3,
        "ydstogo": [10.0] * 3,
        "down": [1.0] * 3,
        "yardline_100": [60.0] * 3,
        "posteam": ["NE"] * 3,
        "home_team": ["NE"] * 3,
        "roof": ["outdoors"] * 3,
        "qb_hit": [0.0] * 3,
        "pass_location": ["left"] * 3,
        "complete_pass": [0.0] * 3,
        "incomplete_pass": [1.0] * 3,
        "interception": [0.0] * 3,
        "receiver_player_id": [None, None, None],
        "desc": [
            "(9:12) T.Brady pass incomplete short left to 81-R.Moss.",
            "(9:12) T.Brady pass incomplete deep right intended for W.Welker.",
            "(9:12) T.Brady pass incomplete short left.",
        ],
    }
    out = calculate_completion_probability(pl.DataFrame(rows, schema_overrides={"receiver_player_id": pl.Utf8}))
    assert out["cp"].is_not_null().to_list() == [True, True, False]


def test_aborted_snap_run_is_the_fumblers_carry() -> None:
    """2009_13_NYJ_BUF: "R.Fitzpatrick FUMBLES (Aborted) ... M.Lynch to NYJ 42 for 1 yard" is Fitzpatrick's
    carry in nflfastR (as on 85 of the 86 2009 aborted-snap runs a teammate finished), not Lynch's."""
    plays, _ = _process("era_play_order/summary_291203002_trimmed.json.gz", 291203002)
    row = plays.filter(pl.col("text").str.contains("Aborted"))
    assert row.select("rush", "rusher_player_name").rows() == [(True, "R.Fitzpatrick")]
