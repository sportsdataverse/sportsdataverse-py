"""Shim tests -- by-reference wrappers fixing `league` (T7.3, model 5, Task 5.3)."""

import json

import polars as pl

from sportsdataverse.baseball.college_baseball import (
    college_baseball_re24,
    college_baseball_state,
    college_baseball_wpa,
)
from sportsdataverse.baseball.college_softball import (
    college_softball_re24,
    college_softball_state,
    college_softball_wpa,
)


def test_college_baseball_shim_matches_core_on_real_fixture():
    with open("tests/fixtures/league_ports/college_baseball_game_plays.json") as f:
        raw = json.load(f)
    state = college_baseball_state(raw)
    assert state.height == 71
    matrix = college_baseball_re24(state=state)
    assert matrix.height <= 24
    results = pl.DataFrame({"game_id": ["401874444"], "home_score": [4], "away_score": [3]})
    wpa = college_baseball_wpa(state=state, results=results)
    assert wpa.height == state.height


def test_college_softball_shim_matches_core_on_real_fixture():
    with open("tests/fixtures/league_ports/college_softball_game_plays.json") as f:
        raw = json.load(f)
    state = college_softball_state(raw)
    assert state.height == 59
    matrix = college_softball_re24(state=state)
    assert matrix.height <= 24
    results = pl.DataFrame({"game_id": ["401873598"], "home_score": [5], "away_score": [4]})
    wpa = college_softball_wpa(state=state, results=results)
    assert wpa.height == state.height
