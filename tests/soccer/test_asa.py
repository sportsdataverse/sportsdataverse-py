"""Offline tests for the American Soccer Analysis (``asa``) stem.

Asserts the parsers against the real trimmed captures in
``tests/fixtures/asa/`` (never synthetic payloads), plus the generated wrappers'
URL construction and the empty/malformed contract. No network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest

from sportsdataverse.soccer import asa
from sportsdataverse.soccer.asa_parsers import parse_asa, parse_asa_goals_added

FIXTURES = Path(__file__).parents[1] / "fixtures" / "asa"

# Every route's ids are base62 strings; a float-origin cast would corrupt them.
_ID_COLUMNS = ("player_id", "team_id", "game_id", "stadium_id", "manager_id", "referee_id")


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


class _Recorder:
    """Stand-in for the runtime ``_get`` that records the URL + params."""

    def __init__(self, payload: Any = None) -> None:
        self.payload = payload if payload is not None else []
        self.url: str = ""
        self.params: Dict[str, Any] = {}

    def __call__(self, url: str, params: Any = None, **kwargs: Any) -> Any:
        self.url = url
        self.params = params or {}
        return self.payload


@pytest.fixture()
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(asa, "_get", rec)
    return rec


# ---------------------------------------------------------------------------
# parse_asa -- flat top-level arrays
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stem", "expected"),
    [
        ("teams", ["team_id", "team_name", "team_short_name", "team_abbreviation"]),
        ("games", ["game_id", "home_team_id", "away_team_id", "referee_id", "stadium_id"]),
        ("players_salaries", ["player_id", "team_id", "season_name", "base_salary"]),
    ],
)
def test_parse_asa_columns(stem: str, expected: List[str]) -> None:
    df = parse_asa(_load(stem))
    assert df.height == 3
    assert set(expected) <= set(df.columns)


def test_parse_asa_ids_are_utf8() -> None:
    for stem in ("teams", "players", "games", "players_salaries"):
        df = parse_asa(_load(stem))
        for col in _ID_COLUMNS:
            if col in df.columns:
                assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"


def test_parse_asa_multi_team_player_id_list_is_comma_joined() -> None:
    """``players/xgoals`` serializes ``team_id`` as a LIST for traded players."""
    raw = _load("players_xgoals")
    assert any(isinstance(row["team_id"], list) for row in raw), "capture no longer covers the list case"
    df = parse_asa(raw)
    assert df.schema["team_id"] == pl.String
    assert df["team_id"].str.contains(",").any()
    # never the Python repr of a list
    assert not df["team_id"].str.starts_with("[").any()


def test_parse_asa_salary_ids_never_stringify_a_float() -> None:
    df = parse_asa(_load("players_salaries"))
    assert not df["player_id"].str.ends_with(".0").any()


def test_parse_asa_return_as_pandas() -> None:
    pdf = parse_asa(_load("teams"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3


@pytest.mark.parametrize("raw", [None, [], {}, "not json", 17, [None, None]])
def test_parse_asa_empty_or_malformed_is_zero_rows(raw: Any) -> None:
    df = parse_asa(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


# ---------------------------------------------------------------------------
# parse_asa_goals_added -- nested data[] breakdown
# ---------------------------------------------------------------------------


def test_parse_asa_goals_added_players_splits_summary_and_actions() -> None:
    tables = parse_asa_goals_added(_load("players_goals-added"))
    assert set(tables) == {"summary", "actions"}
    assert tables["summary"].height == 3
    assert "data" not in tables["summary"].columns
    actions = tables["actions"]
    assert actions.height == 18  # 3 players x 6 action types
    assert {"player_id", "action_type", "goals_added_raw", "goals_added_above_avg"} <= set(actions.columns)
    assert actions.schema["player_id"] == pl.String


def test_parse_asa_goals_added_teams_carry_for_and_against() -> None:
    actions = parse_asa_goals_added(_load("teams_goals-added"))["actions"]
    assert {"team_id", "action_type", "goals_added_for", "goals_added_against"} <= set(actions.columns)
    assert actions.schema["team_id"] == pl.String


@pytest.mark.parametrize("raw", [None, [], {}, "nope"])
def test_parse_asa_goals_added_empty_keeps_both_keys(raw: Any) -> None:
    tables = parse_asa_goals_added(raw)
    assert set(tables) == {"summary", "actions"}
    assert all(t.height == 0 for t in tables.values())


def test_parse_asa_goals_added_return_as_pandas() -> None:
    tables = parse_asa_goals_added(_load("teams_goals-added"), return_as_pandas=True)
    assert all(type(t).__module__.startswith("pandas") for t in tables.values())


# ---------------------------------------------------------------------------
# generated wrappers
# ---------------------------------------------------------------------------


def test_wrapper_builds_the_league_scoped_url(recorder: _Recorder) -> None:
    asa.asa_teams(league_slug="nwsl")
    assert recorder.url == "https://app.americansocceranalysis.com/api/v1/nwsl/teams"


def test_wrapper_passes_stat_filters_as_query_params(recorder: _Recorder) -> None:
    asa.asa_players_xgoals(league_slug="mls", season_name="2023", minimum_minutes="500")
    assert recorder.url == "https://app.americansocceranalysis.com/api/v1/mls/players/xgoals"
    assert recorder.params["season_name"] == "2023"
    assert recorder.params["minimum_minutes"] == "500"


def test_wrapper_returns_raw_when_not_parsed(recorder: _Recorder) -> None:
    recorder.payload = _load("teams")
    assert asa.asa_teams(league_slug="mls", return_parsed=False) == recorder.payload


def test_wrapper_parses_by_default(recorder: _Recorder) -> None:
    recorder.payload = _load("teams")
    df = asa.asa_teams(league_slug="mls")
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3


def test_every_wrapper_is_league_scoped() -> None:
    """All 15 routes are ``/{league_slug}/...`` -- none may lose the segment."""
    assert len(asa.__all__) == 15
    for name in asa.__all__:
        assert "{league_slug}" in getattr(asa, name).__doc__ or "league_slug" in getattr(asa, name).__doc__
