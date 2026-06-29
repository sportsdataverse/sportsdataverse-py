"""Offline parse-parity tests for sportsdataverse.nbagl.nbagl_engine.

Monkeypatches the three module-level _fetch_* helpers so no network calls are
made.  All assertions use the real columns emitted by the shared nba/ cores.
"""

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

import sportsdataverse.nbagl.nbagl_engine as G
from sportsdataverse.nba.nba_rapm import RAPM_SCHEMA

FXR = pathlib.Path("tests/fixtures/nbagl_engine")
GAMES = ["2022400003", "2022400009"]


def _patch(monkeypatch: pytest.MonkeyPatch, gid: str) -> None:
    fx = FXR / gid
    monkeypatch.setattr(G, "_fetch_pbp", lambda g: json.loads((fx / "playbyplayv3.json").read_text()))
    monkeypatch.setattr(G, "_fetch_rotation", lambda g: json.loads((fx / "gamerotation.json").read_text()))
    monkeypatch.setattr(G, "_fetch_box", lambda g: json.loads((fx / "boxscoretraditionalv3.json").read_text()))


def _raw_box(gid: str) -> dict:
    return json.loads((FXR / gid / "boxscoretraditionalv3.json").read_text())


def _raw_rotation(gid: str) -> dict:
    return json.loads((FXR / gid / "gamerotation.json").read_text())


def test_nbagl_enhanced_pbp_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_enhanced_pbp() returns a non-empty frame with the ENHANCED_PBP_SCHEMA columns."""
    _patch(monkeypatch, GAMES[0])
    df = G.nbagl_enhanced_pbp(GAMES[0])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    for c in ("game_id", "action_number", "period", "seconds_remaining", "is_substitution", "team_id"):
        assert c in df.columns, f"missing column {c!r}"


def test_nbagl_enhanced_pbp_second_game(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_enhanced_pbp() works for the second fixture game."""
    _patch(monkeypatch, GAMES[1])
    df = G.nbagl_enhanced_pbp(GAMES[1])
    assert df.height > 0
    assert df.schema["game_id"] == pl.Utf8


def test_nbagl_on_court_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_on_court() returns a non-empty frame with home_player_1..5 / away_player_1..5."""
    _patch(monkeypatch, GAMES[0])
    oc = G.nbagl_on_court(GAMES[0])
    assert isinstance(oc, pl.DataFrame)
    assert oc.height > 0
    cols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]
    for c in cols:
        assert c in oc.columns, f"missing column {c!r}"


def test_nbagl_possessions_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_possessions() returns a non-empty possession frame with lineup columns."""
    _patch(monkeypatch, GAMES[0])
    poss = G.nbagl_possessions(GAMES[0])
    assert isinstance(poss, pl.DataFrame)
    assert poss.height > 0
    for c in ("points", "offense_team_id", "off_player_1", "def_player_1"):
        assert c in poss.columns, f"missing column {c!r}"


def test_nbagl_enhanced_pbp_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_enhanced_pbp(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = G.nbagl_enhanced_pbp(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "game_id" in result.columns


def test_nbagl_on_court_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_on_court(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = G.nbagl_on_court(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_nbagl_possessions_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_possessions(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = G.nbagl_possessions(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Task 3: nbagl_rapm_from_games() tests
# ---------------------------------------------------------------------------


def test_nbagl_rapm_from_games_empty_list() -> None:
    """Empty game_ids returns a zero-row frame with RAPM_SCHEMA, no network call, no raise."""
    out = G.nbagl_rapm_from_games([])
    assert out.height == 0
    assert dict(out.schema) == RAPM_SCHEMA


def test_nbagl_rapm_from_games_skips_empty_games(monkeypatch: pytest.MonkeyPatch) -> None:
    """A game whose possession frame is empty is silently skipped; valid game still produces output."""
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
    from sportsdataverse.nba.nba_lineups import (
        boxscore_home_away,
        parse_rotation_resultsets,
        players_on_court_from_rotation,
    )
    from sportsdataverse.nba.nba_possessions import attach_possession_lineups, build_possessions

    def _game_poss_gl(gid: str) -> pl.DataFrame:
        enh = enhanced_pbp_from_payload(
            json.loads((FXR / gid / "playbyplayv3.json").read_text()),
            league_id="20",
        )
        home, away = boxscore_home_away(_raw_box(gid))
        oc = players_on_court_from_rotation(
            enh,
            parse_rotation_resultsets(_raw_rotation(gid)),
            home_team_id=home,
            away_team_id=away,
        )
        return attach_possession_lineups(build_possessions(enh), oc, enh, home_team_id=home)

    by_game: dict[str, pl.DataFrame] = {
        "bad_game": pl.DataFrame(),
        GAMES[0]: _game_poss_gl(GAMES[0]),
    }
    monkeypatch.setattr(G, "_fetch_possessions", lambda gid: by_game[gid])
    out = G.nbagl_rapm_from_games(["bad_game", GAMES[0]])
    assert out.height > 0
    assert dict(out.schema) == RAPM_SCHEMA


def test_nbagl_rapm_from_games_all_empty_nonempty_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-empty game_ids but every fetch empty → hits `if not frames` guard, returns RAPM_SCHEMA frame."""
    monkeypatch.setattr(G, "_fetch_possessions", lambda gid: pl.DataFrame())
    out = G.nbagl_rapm_from_games(["x", "y"])
    assert out.height == 0
    assert dict(out.schema) == RAPM_SCHEMA


# ---------------------------------------------------------------------------
# Task 3: public export smoke
# ---------------------------------------------------------------------------


def test_nbagl_engine_public_exports() -> None:
    """nbagl_enhanced_pbp / nbagl_on_court / nbagl_possessions / nbagl_rapm_from_games are importable from sportsdataverse.nbagl."""
    from sportsdataverse.nbagl import (  # noqa: F401
        nbagl_enhanced_pbp,
        nbagl_on_court,
        nbagl_possessions,
        nbagl_rapm_from_games,
    )
