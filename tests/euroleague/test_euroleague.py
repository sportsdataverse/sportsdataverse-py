"""Offline tests for the EuroLeague (``euroleague``) flat-API family.

Asserts the parser against the real trimmed captures in ``tests/fixtures/euroleague/``
(never synthetic payloads), the generated endpoint YAML, and the empty/malformed
contract. No network. The generated wrapper tests are skipped until Task 6 renders
``sportsdataverse/euroleague/euroleague.py`` on the combined wave tree.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest
import yaml

from sportsdataverse.euroleague.euroleague_parsers import parse_euroleague

FIXTURES = Path(__file__).parents[1] / "fixtures" / "euroleague"
YAML_PATH = Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "euroleague.yaml"
SCHEMA_DIR = Path(__file__).parents[2] / "tools" / "codegen" / "schemas" / "native" / "euroleague"

_STEMS = sorted(p.stem for p in FIXTURES.glob("*.json"))
_ENVELOPE_STEMS = [s for s in _STEMS if not s.endswith("__stats")]
_GAME_STATS = "competitions__E__seasons__E2025__games__1__stats"

# Every ``id`` / ``*_id`` / ``code`` / ``*_code`` column is an opaque join key.
_ID = re.compile(r"(^|_)(id|code)$")
_SNAKE = re.compile(r"^[a-z0-9_]+$")


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


class _Recorder:
    """Stand-in for the runtime ``_get`` that records the URL + params."""

    def __init__(self, payload: Any = None) -> None:
        self.payload = payload if payload is not None else {"total": 0, "data": []}
        self.url: str = ""
        self.params: Dict[str, Any] = {}

    def __call__(self, url: str, params: Any = None, **kwargs: Any) -> Any:
        self.url = url
        self.params = params or {}
        return self.payload


@pytest.fixture()
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    # Rendered by generate.py in Task 6; absent on this branch.
    mod = pytest.importorskip("sportsdataverse.euroleague.euroleague")
    rec = _Recorder()
    monkeypatch.setattr(mod, "_get", rec)
    return rec


# ---------------------------------------------------------------------------
# parse_euroleague -- every committed capture
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stem", _STEMS)
def test_every_fixture_parses_to_a_tidy_frame(stem: str) -> None:
    df = parse_euroleague(_load(stem))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0
    for col in df.columns:
        assert _SNAKE.match(col), f"{stem}.{col} is not snake_case"
        if _ID.search(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"


@pytest.mark.parametrize("stem", _ENVELOPE_STEMS)
def test_envelope_rows_come_from_data(stem: str) -> None:
    """List routes answer ``{"total": n, "data": [...]}``: rows are ``data``, not the envelope."""
    raw = _load(stem)
    assert set(raw) == {"total", "data"}, "capture no longer carries the envelope"
    df = parse_euroleague(raw)
    assert df.height == len(raw["data"])
    assert "data" not in df.columns
    assert "total" not in df.columns


def test_page_object_is_one_row() -> None:
    """The box score is a ``{"local", "road"}`` page object -> ONE row, not one per side."""
    raw = _load(_GAME_STATS)
    assert set(raw) == {"local", "road"}
    df = parse_euroleague(raw)
    assert df.height == 1
    assert {"local_coach_code", "local_total_points", "road_total_points", "local_players"} <= set(df.columns)
    assert df.schema["local_coach_code"] == pl.String
    # the nested player list survives as a JSON cell, never a Python repr
    assert df["local_players"].item().startswith("[{")


def test_game_code_is_utf8_without_a_float_suffix() -> None:
    """``gameCode`` is an integer on the wire; the join key must read ``"406"``, never ``"406.0"``."""
    raw = _load("competitions__E__seasons__E2025__games")
    assert isinstance(raw["data"][0]["gameCode"], int)
    df = parse_euroleague(raw)
    assert df.schema["game_code"] == pl.String
    assert not df["game_code"].str.ends_with(".0").any()
    assert df["game_code"][0] == str(raw["data"][0]["gameCode"])


def test_nested_objects_flatten_to_prefixed_columns() -> None:
    df = parse_euroleague(_load("competitions__E__seasons__E2025__clubs"))
    assert {"code", "name", "country_code", "country_name", "images_crest"} <= set(df.columns)
    assert df.schema["country_code"] == pl.String


@pytest.mark.parametrize("raw", [None, [], {}, "x", 17, {"total": 0, "data": []}, [None]])
def test_empty_payload_is_zero_row_frame(raw: Any) -> None:
    df = parse_euroleague(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_return_as_pandas() -> None:
    pdf = parse_euroleague(_load("competitions"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3


# ---------------------------------------------------------------------------
# generated endpoint YAML + schemas (gen_euroleague.py output)
# ---------------------------------------------------------------------------


def test_yaml_lists_every_captured_route() -> None:
    doc = _yaml()
    unverified = [
        p for p in SCHEMA_DIR.glob("*.yaml") if yaml.safe_load(p.read_text(encoding="utf-8")).get("unverified")
    ]
    assert len(doc["endpoints"]) == len(_STEMS) + len(unverified)
    assert sorted(e["short"] for e in doc["endpoints"]) == sorted(
        ["clubs", "competitions", "game_stats", "games", "people", "rounds", "seasons"],
    )
    assert doc["getter_module"] == "sportsdataverse.euroleague.euroleague_runtime"
    assert doc["host"] == "https://api-live.euroleague.net/v2"


def test_no_path_token_named_league_or_sport() -> None:
    """The renderer blanks ``{league}``/``{sport}`` as ESPN slugs; no EuroLeague route may use them."""
    for ep in _yaml()["endpoints"]:
        tokens = set(re.findall(r"\{(\w+)\}", ep["path"]))
        names = {p["name"] for p in ep.get("path_params", [])}
        assert not {"league", "sport"} & (tokens | names), ep["path"]
        assert tokens == names, f"{ep['short']}: path tokens {tokens} != path_params {names}"


def test_every_endpoint_schema_has_columns() -> None:
    for ep in _yaml()["endpoints"]:
        schema = yaml.safe_load((SCHEMA_DIR / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
        assert schema["columns"], ep["short"]
        assert ep["parser"] == "parse_euroleague"


# ---------------------------------------------------------------------------
# generated wrappers (skipped until generate.py has rendered the module)
# ---------------------------------------------------------------------------


def test_wrapper_builds_the_nested_url(recorder: _Recorder) -> None:
    from sportsdataverse.euroleague import euroleague

    euroleague.euroleague_clubs(competition_code="E", season_code="E2025")
    assert recorder.url == "https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/clubs"


def test_wrapper_passes_paging_as_query_params(recorder: _Recorder) -> None:
    from sportsdataverse.euroleague import euroleague

    euroleague.euroleague_games(competition_code="E", season_code="E2025", limit="3")
    assert recorder.url == "https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/games"
    assert recorder.params["limit"] == "3"
