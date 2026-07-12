"""Tests for the college baseball/softball base-out-state substrate + RE24/WPA (T7.3, model 5)."""

import json

import polars as pl
import pytest

from sportsdataverse.baseball.college_baseball_constants import (
    BASE_STATES,
    get_college_baseball_constants,
)
from sportsdataverse.baseball.college_run_expectancy import (
    college_baseball_re24,
    college_baseball_state,
    college_baseball_wpa,
    run_value,
)


def _play_result(
    atbat_id: int,
    seq: int,
    *,
    inning: int,
    half: str,
    outs: int,
    away: int,
    home: int,
    on_first: bool = False,
    on_second: bool = False,
    on_third: bool = False,
    team_id: int = 10,
):
    participants = [{"type": "batter"}]
    if on_first:
        participants.append({"type": "onFirst"})
    if on_second:
        participants.append({"type": "onSecond"})
    if on_third:
        participants.append({"type": "onThird"})
    return {
        "type": {"text": "Play Result"},
        "atBatId": str(atbat_id),
        "sequenceNumber": str(seq),
        "outs": outs,
        "period": {"type": half, "number": inning},
        "team": {"$ref": f"http://sports.core.api.espn.com/v2/.../teams/{team_id}?lang=en"},
        "participants": participants,
        "awayScore": away,
        "homeScore": home,
    }


def _payload(items, event_id="999999"):
    return {
        "$ref": f"http://sports.core.api.espn.com/v2/sports/baseball/leagues/college-baseball/events/{event_id}/competitions/{event_id}/plays?lang=en",
        "count": len(items),
        "items": items,
    }


def _half_inning_payload():
    # Mirrors mlb_run_expectancy's synthetic half-inning fixture:
    #  PA0: bases empty, 0 out -> single (runner to 1st), 0 runs, 0 out after
    #  PA1: runner on 1st, 0 out -> HR (2 runs), bases empty after, 0 out after
    #  PA2: bases empty, 0 out -> strikeout, 0 runs, 1 out after
    return _payload(
        [
            _play_result(1001, 3, inning=1, half="Top", outs=0, away=0, home=0, on_first=True),
            _play_result(1002, 2, inning=1, half="Top", outs=0, away=2, home=0),
            _play_result(1003, 3, inning=1, half="Top", outs=1, away=2, home=0),
        ]
    )


def test_state_reconstruction_synthetic_half():
    out = college_baseball_state(_half_inning_payload(), league="college_baseball").sort("play_seq")
    rows = out.to_dicts()
    assert [r["base_state"] for r in rows] == ["___", "1__", "___"]
    assert [r["outs"] for r in rows] == [0, 0, 0]
    assert [r["runs_before"] for r in rows] == [0, 0, 2]
    assert [r["runs_after"] for r in rows] == [0, 2, 2]
    assert all(r["batting_team_id"] == "10" for r in rows)


def _two_halves_payload():
    #  top    PA0: away HR (away 1-0), 0 out after
    #  top    PA1: strikeout, 1 out after
    #  bottom PA2: home HR (tie 1-1), 0 out after  <- first PA of a new half
    #  bottom PA3: strikeout, 1 out after
    return _payload(
        [
            _play_result(2001, 1, inning=1, half="Top", outs=0, away=1, home=0),
            _play_result(2002, 1, inning=1, half="Top", outs=1, away=1, home=0),
            _play_result(2101, 1, inning=1, half="Bottom", outs=0, away=1, home=1),
            _play_result(2102, 1, inning=1, half="Bottom", outs=1, away=1, home=1),
        ]
    )


def test_cross_half_inning_score_carry_and_reset():
    rows = college_baseball_state(_two_halves_payload(), league="college_baseball").sort("play_seq").to_dicts()
    # runs_before carries the score across the half boundary: PA2 (bottom 1st,
    # first PA of the half) starts at 1 run total (the top half's HR), not 0.
    assert [r["runs_before"] for r in rows] == [0, 1, 1, 2]
    assert [r["runs_after"] for r in rows] == [1, 1, 2, 2]
    # base/outs reset at the half boundary (PA2 starts fresh at ___/0).
    assert [r["base_state"] for r in rows] == ["___", "___", "___", "___"]
    assert [r["outs"] for r in rows] == [0, 0, 0, 0]
    assert [r["score_diff"] for r in rows] == [0, -1, -1, 0]


def test_multi_row_atbat_takes_terminal_state():
    # A caught-stealing sub-event splits a second "Play Result" row off the
    # same atBatId as the batter's own result -- the row with the higher
    # (outs, sequenceNumber) must win as the PA's authoritative post-state.
    payload = _payload(
        [
            _play_result(3001, 5, inning=1, half="Top", outs=0, away=0, home=0, on_first=True),
            # same atBatId, a later sub-event row (runner caught stealing) with
            # higher outs and no participants left on base
            {
                **_play_result(3001, 6, inning=1, half="Top", outs=1, away=0, home=0),
            },
            _play_result(3002, 1, inning=1, half="Top", outs=1, away=0, home=0),
        ]
    )
    rows = college_baseball_state(payload, league="college_baseball").sort("play_seq").to_dicts()
    assert len(rows) == 2
    # PA 3002's pre-play state must reflect PA 3001's terminal (outs=1, no
    # runner) row, not its first ("Play Result") row (outs=0, runner on 1st).
    assert rows[1]["base_state"] == "___"
    assert rows[1]["outs"] == 1


def test_state_empty_payload_returns_typed_empty_frame():
    out = college_baseball_state(_payload([]), league="college_softball")
    assert out.height == 0
    assert set(out.columns) == {
        "game_id",
        "inning",
        "half",
        "base_state",
        "outs",
        "runs_before",
        "runs_after",
        "batting_team_id",
        "play_seq",
        "score_diff",
    }


def test_unknown_league_raises():
    with pytest.raises(ValueError):
        get_college_baseball_constants("college_hockey")
    with pytest.raises(ValueError):
        college_baseball_state(_payload([]), league="mlb")


@pytest.mark.parametrize(
    "league,fixture,expected_pa,expected_total_runs",
    [
        ("college_baseball", "tests/fixtures/league_ports/college_baseball_game_plays.json", 71, 7),
        ("college_softball", "tests/fixtures/league_ports/college_softball_game_plays.json", 59, 9),
    ],
)
def test_real_fixture_state_invariants(league, fixture, expected_pa, expected_total_runs):
    """Feasibility lock-in: the real ESPN capture reconstructs cleanly end to end."""
    with open(fixture) as f:
        raw = json.load(f)
    state = college_baseball_state(raw, league=league)
    assert state.height == expected_pa
    assert state.null_count().sum_horizontal()[0] == 0
    assert state["outs"].min() >= 0 and state["outs"].max() <= 2
    assert set(state["base_state"].unique().to_list()).issubset(set(BASE_STATES))
    # runs_after climbs monotonically within a game and tops out at the real final score.
    assert state.sort("play_seq")["runs_after"].is_sorted()
    assert state["runs_after"].max() == expected_total_runs


def _synthetic_multi_game_corpus() -> pl.DataFrame:
    """A larger synthetic corpus so the RE24 matrix has enough states/samples
    to exercise monotonicity meaningfully (the two real fixtures are single
    games -- correctness-only, not statistically powered; see the oracle
    test + progress ledger for that honest downscope)."""
    # Bases load one runner at a time (PA1-3, no runs yet) so that the 4th
    # PA's PRE-play state is genuinely bases-loaded/0-out; the grand slam
    # happens ON that 4th PA so its runs land in the "123"/0 state's own
    # runs-rest-of-inning (not the PA that merely loaded the bases).
    frames = []
    for g in range(1, 21):
        event_id = f"G{g}"
        items = [
            _play_result(g * 100 + 1, 1, inning=1, half="Top", outs=0, away=0, home=0, on_first=True),
            _play_result(g * 100 + 2, 1, inning=1, half="Top", outs=0, away=0, home=0, on_first=True, on_second=True),
            _play_result(
                g * 100 + 3,
                1,
                inning=1,
                half="Top",
                outs=0,
                away=0,
                home=0,
                on_first=True,
                on_second=True,
                on_third=True,
            ),
            _play_result(g * 100 + 4, 1, inning=1, half="Top", outs=0, away=4, home=0),  # grand slam, bases clear
            _play_result(g * 100 + 5, 1, inning=1, half="Top", outs=1, away=4, home=0),
            _play_result(g * 100 + 6, 1, inning=1, half="Top", outs=2, away=4, home=0),
            _play_result(g * 100 + 7, 1, inning=1, half="Top", outs=3, away=4, home=0),
        ]
        frames.append(college_baseball_state(_payload(items, event_id=event_id), league="college_baseball"))
    return pl.concat(frames)


def test_re24_monotone_in_outs_and_bases_loaded_beats_empty():
    state = _synthetic_multi_game_corpus()
    matrix = college_baseball_re24(league="college_baseball", state=state)
    assert matrix.height <= 24
    assert matrix["run_expectancy"].min() >= 0.0

    for bs in matrix["base_state"].unique().to_list():
        r = matrix.filter(pl.col("base_state") == bs).sort("outs")["run_expectancy"].to_list()
        assert all(earlier >= later - 1e-9 for earlier, later in zip(r, r[1:])), (
            f"RE not monotone non-increasing in outs for base_state={bs!r}: {r}"
        )

    loaded_0 = matrix.filter((pl.col("base_state") == "123") & (pl.col("outs") == 0))["run_expectancy"][0]
    empty_2 = matrix.filter((pl.col("base_state") == "___") & (pl.col("outs") == 2))["run_expectancy"][0]
    assert loaded_0 > empty_2


def test_re24_matrix_schema_stable_between_empty_and_fitted_paths():
    # The empty path carries the documented _MATRIX_SCHEMA (n: Int64); the
    # fitted path must match it exactly -- pl.len() natively emits UInt32,
    # which would silently diverge the two paths' schemas (join-key dtype
    # discipline: one canonical dtype per column, fixed at the boundary).
    empty = college_baseball_re24(league="college_baseball", state=None)
    with open("tests/fixtures/league_ports/college_baseball_game_plays.json") as f:
        raw = json.load(f)
    state = college_baseball_state(raw, league="college_baseball")
    fitted = college_baseball_re24(league="college_baseball", state=state)
    assert fitted.schema == empty.schema


def test_run_value_reexport_matches_manual():
    m = pl.DataFrame({"base_state": ["___", "1__"], "outs": [0, 0], "re": [0.48, 0.86], "n": [1, 1]})
    assert abs(run_value("___", 0, "1__", 0, 0, m) - 0.38) < 1e-9


def test_run_value_accepts_fitted_college_matrix():
    # The exported run_value must work against THIS module's own fitted
    # matrix (column "run_expectancy"), not just an MLB-shaped frame
    # (column "re") -- the interop footgun the raw re-export shipped with.
    with open("tests/fixtures/league_ports/college_baseball_game_plays.json") as f:
        raw = json.load(f)
    state = college_baseball_state(raw, league="college_baseball")
    matrix = college_baseball_re24(league="college_baseball", state=state)
    re0 = matrix.filter((pl.col("base_state") == "___") & (pl.col("outs") == 0))["run_expectancy"][0]
    re1 = matrix.filter((pl.col("base_state") == "___") & (pl.col("outs") == 1))["run_expectancy"][0]
    assert abs(run_value("___", 0, "___", 1, 0, matrix) - (re1 - re0)) < 1e-9


def test_base_encoding_second_and_third_distinct():
    # A pre_2 <-> pre_3 encoding swap would slip through every other test:
    # put a runner on ONLY second, then ONLY third, and exact-assert "_2_"
    # and "__3" as the next PA's pre-play state.
    payload = _payload(
        [
            _play_result(4001, 1, inning=1, half="Top", outs=0, away=0, home=0, on_second=True),
            _play_result(4002, 1, inning=1, half="Top", outs=0, away=0, home=0, on_third=True),
            _play_result(4003, 1, inning=1, half="Top", outs=1, away=0, home=0),
        ]
    )
    rows = college_baseball_state(payload, league="college_baseball").sort("play_seq").to_dicts()
    assert [r["base_state"] for r in rows] == ["___", "_2_", "__3"]


def test_college_baseball_wpa_telescopes_to_terminal_outcome():
    with open("tests/fixtures/league_ports/college_baseball_game_plays.json") as f:
        raw = json.load(f)
    state = college_baseball_state(raw, league="college_baseball")
    results = pl.DataFrame({"game_id": ["401874444"], "home_score": [4], "away_score": [3]})
    wpa = college_baseball_wpa(league="college_baseball", state=state, results=results)

    assert wpa.height == state.height
    assert wpa["wpa"].null_count() == 0
    # Single-perspective (home) WPA telescopes EXACTLY to
    # final_home_win_exp - 0.5: the synthetic terminal anchor's wpa (the
    # jump to the actual 1.0/0.0 outcome) is folded into the last real PA's
    # wpa, and home won here, so the game sum is exactly +0.5.
    assert abs(wpa["wpa"].sum() - 0.5) < 1e-9


def test_college_baseball_wpa_empty_inputs_return_typed_empty_frame():
    out = college_baseball_wpa(league="college_softball", state=None, results=None)
    assert out.height == 0
    assert set(out.columns) == {"game_id", "play_seq", "re_before", "re_after", "run_value", "wpa"}
