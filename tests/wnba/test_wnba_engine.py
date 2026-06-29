"""Offline parse-parity tests for sportsdataverse.wnba.wnba_engine.

Monkeypatches the three module-level _fetch_* helpers so no network calls are
made.  All assertions use the real columns emitted by the shared nba/ cores.
"""

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

import sportsdataverse.wnba.wnba_engine as W

FXR = pathlib.Path("tests/fixtures/wnba_engine")
GAMES = ["1022400001", "1022400003"]


def _patch(monkeypatch, gid: str) -> None:
    fx = FXR / gid
    monkeypatch.setattr(W, "_fetch_pbp", lambda g: json.loads((fx / "playbyplayv3.json").read_text()))
    monkeypatch.setattr(W, "_fetch_rotation", lambda g: json.loads((fx / "gamerotation.json").read_text()))
    monkeypatch.setattr(W, "_fetch_box", lambda g: json.loads((fx / "boxscoretraditionalv3.json").read_text()))


def test_wnba_enhanced_pbp_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_enhanced_pbp() returns a non-empty frame with the ENHANCED_PBP_SCHEMA columns."""
    _patch(monkeypatch, GAMES[0])
    df = W.wnba_enhanced_pbp(GAMES[0])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    for c in ("game_id", "action_number", "period", "seconds_remaining", "is_substitution", "team_id"):
        assert c in df.columns, f"missing column {c!r}"


def test_wnba_enhanced_pbp_second_game(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_enhanced_pbp() works for the second fixture game."""
    _patch(monkeypatch, GAMES[1])
    df = W.wnba_enhanced_pbp(GAMES[1])
    assert df.height > 0
    assert df.schema["game_id"] == pl.Utf8


def test_wnba_on_court_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_on_court() returns a non-empty frame with home_player_1..5 / away_player_1..5."""
    _patch(monkeypatch, GAMES[0])
    oc = W.wnba_on_court(GAMES[0])
    assert isinstance(oc, pl.DataFrame)
    assert oc.height > 0
    cols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]
    for c in cols:
        assert c in oc.columns, f"missing column {c!r}"


def test_wnba_possessions_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_possessions() returns a non-empty possession frame with lineup columns."""
    _patch(monkeypatch, GAMES[0])
    poss = W.wnba_possessions(GAMES[0])
    assert isinstance(poss, pl.DataFrame)
    assert poss.height > 0
    for c in ("points", "offense_team_id", "off_player_1", "def_player_1"):
        assert c in poss.columns, f"missing column {c!r}"


def test_wnba_enhanced_pbp_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_enhanced_pbp(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = W.wnba_enhanced_pbp(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "game_id" in result.columns


def test_wnba_on_court_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_on_court(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = W.wnba_on_court(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "home_player_1" in result.columns


def test_wnba_possessions_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_possessions(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = W.wnba_possessions(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "off_player_1" in result.columns
