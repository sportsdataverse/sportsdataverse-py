"""Offline tests for the FIFA public API v3 (``fifa``) stem.

Asserts the parser against the real truncated captures in ``tests/fixtures/fifa/``
(never synthetic payloads), the generated endpoint YAML + returns-schemas, and the
empty/malformed contract. The wrapper tests skip until ``generate.py`` has rendered
``sportsdataverse.soccer.fifa`` (plan Task 6). No network.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest
import yaml

from sportsdataverse.soccer.fifa_parsers import _is_fifa_id, parse_fifa

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "fifa"
ENDPOINTS_YAML = ROOT / "tools" / "codegen" / "endpoints" / "fifa.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "fifa"

FIXTURE_STEMS = sorted(p.stem for p in FIXTURES.glob("*.json"))

# ``IdCompetition`` / ``IdSeason`` / ... snake_case to ``id_*``; nested sides prefix it.
_KNOWN_IDS = ("id_competition", "id_season", "id_team", "id_match", "id_player", "home_id_team", "away_id_team")

_SNAKE = re.compile(r"^[a-z0-9_]+$")

EXPECTED_SHORTS = {
    "calendar_matches",
    "competitions",
    "competition",
    "live_football",
    "players_search",
    "seasons",
    "stadiums",
    "teams_search",
    "team",
}


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(ENDPOINTS_YAML.read_text(encoding="utf-8"))


class _Recorder:
    """Stand-in for the runtime ``_get`` that records the URL + params."""

    def __init__(self, payload: Any = None) -> None:
        self.payload = payload if payload is not None else {}
        self.url: str = ""
        self.params: Dict[str, Any] = {}

    def __call__(self, url: str, params: Any = None, **kwargs: Any) -> Any:
        self.url = url
        self.params = params or {}
        return self.payload


@pytest.fixture()
def fifa_module() -> Any:
    return pytest.importorskip("sportsdataverse.soccer.fifa", reason="rendered by generate.py in Task 6")


@pytest.fixture()
def recorder(fifa_module: Any, monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(fifa_module, "_get", rec)
    return rec


# ---------------------------------------------------------------------------
# parse_fifa -- every committed capture
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stem", FIXTURE_STEMS)
def test_every_fixture_parses_to_a_tidy_frame(stem: str) -> None:
    df = parse_fifa(_load(stem))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, stem
    assert all(_SNAKE.match(c) for c in df.columns), [c for c in df.columns if not _SNAKE.match(c)]
    for col in df.columns:
        if _is_fifa_id(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"
    for col in _KNOWN_IDS:
        if col in df.columns:
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"


def test_envelope_rows_come_from_results() -> None:
    """List routes answer ``{ContinuationToken, ContinuationHash, Results: [...]}``."""
    raw = _load("competitions")
    assert set(raw) == {"ContinuationToken", "ContinuationHash", "Results"}
    df = parse_fifa(raw)
    assert df.height == len(raw["Results"]) == 3
    assert "results" not in df.columns
    assert "continuation_token" not in df.columns
    assert df["id_competition"].n_unique() == 3
    assert df.schema["id_competition"] == pl.String


def test_page_object_is_one_row() -> None:
    """``/competitions/{idCompetition}`` is a wide object -> one row, not one row per key."""
    raw = _load("competitions__17")
    df = parse_fifa(raw)
    assert df.height == 1
    assert df["id_competition"].item() == "17"
    assert {"name", "gender", "competition_type", "properties_id_ifes"} <= set(df.columns)


def test_localised_name_lists_are_stringified() -> None:
    """``Name`` / ``DisplayName`` are ``[{Locale, Description}]`` lists -> one JSON string cell."""
    df = parse_fifa(_load("teams__43922"))
    assert df.height == 1
    cell = df["name"].item()
    assert isinstance(cell, str)
    decoded = json.loads(cell)
    assert decoded and set(decoded[0]) == {"Locale", "Description"}


def test_ids_never_stringify_a_float() -> None:
    df = parse_fifa(_load("calendar__matches"))
    for col in ("id_match", "id_season", "id_competition", "home_id_team", "away_id_team"):
        assert df.schema[col] == pl.String
        assert not df[col].str.ends_with(".0").any()


def test_json_normalize_gaps_are_null_not_nan() -> None:
    """A nested object present on some rows only (live ``BallPossession``) leaves nulls, never ``"nan"``."""
    raw = _load("live__football")
    assert [r["BallPossession"] is None for r in raw["Results"]] == [False, True, True]
    df = parse_fifa(raw)
    assert df["ball_possession_intervals"].to_list() == ["[]", None, None]
    for col, dtype in df.schema.items():
        if dtype == pl.String:
            assert not (df[col] == "nan").any(), col


def test_id_keyed_map_rows_carry_id() -> None:
    """Shared row rule: ``{key: {...}, ...}`` -> rows with the key in ``id``."""
    df = parse_fifa({"a": {"x": 1}, "b": {"x": 2}})
    assert df.height == 2
    assert df["id"].to_list() == ["a", "b"]


@pytest.mark.parametrize(
    "raw",
    [None, [], {}, "x", 17, [None], {"ContinuationToken": "t", "ContinuationHash": "h", "Results": []}],
)
def test_empty_payload_is_zero_row_frame(raw: Any) -> None:
    df = parse_fifa(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_return_as_pandas() -> None:
    pdf = parse_fifa(_load("seasons"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 1


# ---------------------------------------------------------------------------
# generated endpoint YAML + returns-schemas
# ---------------------------------------------------------------------------


def test_yaml_lists_every_captured_route() -> None:
    doc = _yaml()
    unverified = [p for p in SCHEMA_DIR.glob("*.yaml") if "unverified" in yaml.safe_load(p.read_text(encoding="utf-8"))]
    assert len(doc["endpoints"]) == len(FIXTURE_STEMS) + len(unverified) == 9
    assert {e["short"] for e in doc["endpoints"]} == EXPECTED_SHORTS
    for ep in doc["endpoints"]:
        schema = yaml.safe_load((SCHEMA_DIR / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
        assert schema["schema"] == ep["short"]
        assert schema["columns"], ep["short"]
        assert all(c["description"] for c in schema["columns"]), ep["short"]


def test_no_path_token_named_league_or_sport() -> None:
    """The renderer substitutes ``{league}`` / ``{sport}`` as ESPN slugs (the ASA lesson)."""
    for ep in _yaml()["endpoints"]:
        names = {p["name"] for p in ep.get("path_params", [])}
        assert not {"league", "sport"} & names, ep["path"]
        assert "{league}" not in ep["path"] and "{sport}" not in ep["path"], ep["path"]


def test_yaml_family_keys() -> None:
    doc = _yaml()
    assert doc["api"] == "fifa"
    assert doc["host"] == "https://api.fifa.com/api/v3"
    assert doc["name_pattern"] == "fifa_{short}"
    assert doc["parser_module"] == "soccer.fifa_parsers"
    assert doc["passthrough_query"] is False
    assert "x-pagination" in doc["docstring"]["raw_doc"]


def test_required_query_params_are_marked_in_the_description() -> None:
    by_short = {e["short"]: e for e in _yaml()["endpoints"]}
    seasons = {p["name"]: p for p in by_short["seasons"]["extra_params"]}
    assert seasons["id_competition"]["query_key"] == "idCompetition"
    assert seasons["id_competition"]["description"].startswith("Required.")
    assert not seasons["language"]["description"].startswith("Required.")
    competition = by_short["competition"]
    assert competition["path"] == "/competitions/{id_competition}"
    assert competition["path_params"] == [
        {
            "description": "Competition id; 17 = FIFA World Cup (see /competitions).",
            "name": "id_competition",
            "required": True,
            "type": "str",
        },
    ]
    assert competition["example_args"] == {"id_competition": "17", "language": "en"}


# ---------------------------------------------------------------------------
# generated wrappers (skipped until Task 6 renders sportsdataverse/soccer/fifa.py)
# ---------------------------------------------------------------------------


def test_wrapper_builds_the_path_scoped_url(fifa_module: Any, recorder: _Recorder) -> None:
    fifa_module.fifa_competition(id_competition="17", language="en")
    assert recorder.url == "https://api.fifa.com/api/v3/competitions/17"
    assert recorder.params["language"] == "en"


def test_wrapper_sends_wire_query_keys(fifa_module: Any, recorder: _Recorder) -> None:
    fifa_module.fifa_seasons(id_competition="17", count="3")
    assert recorder.url == "https://api.fifa.com/api/v3/seasons"
    assert recorder.params["idCompetition"] == "17"
    assert recorder.params["count"] == "3"


def test_wrapper_parses_by_default(fifa_module: Any, recorder: _Recorder) -> None:
    recorder.payload = _load("competitions")
    df = fifa_module.fifa_competitions()
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3
    assert fifa_module.fifa_competitions(return_parsed=False) == recorder.payload
