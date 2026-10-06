"""Offline tests for the FotMob (``fotmob``) stem.

Asserts the parser against the real truncated captures in ``tests/fixtures/fotmob/``
(never synthetic payloads), the generated endpoint YAML + returns-schemas, and the
empty/malformed contract. No network. The wrapper-level tests skip until
``generate.py`` has rendered ``sportsdataverse/soccer/fotmob.py`` (on the combined wave tree).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest
import yaml

from sportsdataverse.soccer._frames import is_id_name
from sportsdataverse.soccer.fotmob_parsers import ID_KEY, parse_fotmob

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "fotmob"
ENDPOINTS_YAML = ROOT / "tools" / "codegen" / "endpoints" / "fotmob.yaml"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas"

FIXTURE_STEMS = sorted(p.stem for p in FIXTURES.glob("*.json"))
HOST = "https://www.fotmob.com/api/data"

_SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
_TOKEN = re.compile(r"\{([^}]+)\}")

# Routes whose body is an envelope: exactly one list of records + scalar/dict metadata.
_ENVELOPES = [
    ("matches", "leagues"),
    ("tlnews", "data"),
    ("top-transfers", "transfers"),
    ("team-of-the-week__rounds", "rounds"),
]


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def _doc() -> Dict[str, Any]:
    return yaml.safe_load(ENDPOINTS_YAML.read_text(encoding="utf-8"))


def _by_short() -> Dict[str, Dict[str, Any]]:
    return {ep["short"]: ep for ep in _doc()["endpoints"]}


def _capture_stem(path: str) -> str:
    """The reference repo's slug rule (``tools/capture.py``): ``/search/suggest`` -> ``search__suggest``."""
    return path.strip("/").replace("api/", "").replace("/", "__")


# ---------------------------------------------------------------------------
# parse_fotmob -- every capture
# ---------------------------------------------------------------------------


def test_fixture_dir_covers_the_fourteen_captured_routes() -> None:
    assert len(FIXTURE_STEMS) == 14


@pytest.mark.parametrize("stem", FIXTURE_STEMS)
def test_every_capture_parses_to_a_tidy_frame(stem: str) -> None:
    df = parse_fotmob(_load(stem))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0 and df.width > 0
    not_snake = [c for c in df.columns if not _SNAKE.match(c)]
    assert not not_snake, not_snake
    for col in df.columns:
        if is_id_name(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"


@pytest.mark.parametrize("raw", [None, {}, [], "x", 0, [None, None]])
def test_empty_payload_is_zero_row_frame(raw: Any) -> None:
    df = parse_fotmob(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_return_as_pandas() -> None:
    pdf = parse_fotmob(_load("matches"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3


def test_integer_ids_are_utf8_and_never_a_float_string() -> None:
    df = parse_fotmob(_load("matches"))
    assert df.schema["id"] == pl.String
    assert df["id"].str.contains(r"^\d+$").all()
    assert not df["id"].str.ends_with(".0").any()


# ---------------------------------------------------------------------------
# A wide page object is ONE row
# ---------------------------------------------------------------------------


def test_page_object_is_one_row() -> None:
    raw = _load("leagues")
    assert isinstance(raw, dict) and len(raw) > 5
    df = parse_fotmob(raw)
    assert df.height == 1
    assert "details_id" in df.columns
    assert df.schema["details_id"] == pl.String
    # list-valued cells are JSON-encoded, never exploded into rows
    assert df["tabs"].item().startswith("[")


def test_match_details_single_string_list_is_not_an_envelope() -> None:
    """``matchDetails`` carries one list (``nav``, tab names) -- strings, not records."""
    raw = _load("matchDetails")
    assert [v for v in raw.values() if isinstance(v, list)] == [raw["nav"]]
    df = parse_fotmob(raw)
    assert df.height == 1
    assert df.schema["general_match_id"] == pl.String


def test_page_object_flattens_one_level_deep() -> None:
    """Deeper containers stay JSON cells: FotMob's name-keyed maps never become columns."""
    raw = _load("matchDetails")
    assert isinstance(raw["header"]["events"], dict)  # goal scorers keyed by surname
    df = parse_fotmob(raw)
    assert "header_events" in df.columns
    assert json.loads(df["header_events"].item()) == raw["header"]["events"]
    assert not [c for c in df.columns if c.startswith("header_events_")]


# ---------------------------------------------------------------------------
# An id-keyed map: rows from the values, key in an id column
# ---------------------------------------------------------------------------


def test_tvlistings_map_rows_carry_id() -> None:
    raw = _load("tvlistings")
    assert all(k.isdigit() for k in raw), "capture no longer covers the id-keyed map case"
    df = parse_fotmob(raw)
    assert df.height == sum(len(v) for v in raw.values())
    assert df.schema[ID_KEY] == pl.String
    assert set(df[ID_KEY]) == set(raw)
    assert df.schema["match_id"] == pl.String
    assert (df[ID_KEY] == df["match_id"]).all()


# ---------------------------------------------------------------------------
# An envelope with one record list: rows are that list
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("stem", "key"), _ENVELOPES)
def test_envelope_rows_come_from_the_single_list(stem: str, key: str) -> None:
    raw = _load(stem)
    df = parse_fotmob(raw)
    assert df.height == len(raw[key])
    assert key not in df.columns


# ---------------------------------------------------------------------------
# generated YAML + schemas
# ---------------------------------------------------------------------------


def test_family_header() -> None:
    doc = _doc()
    assert doc["api"] == "fotmob"
    assert doc["host"] == HOST
    assert doc["module"] == "fotmob"
    assert doc["name_pattern"] == "fotmob_{short}"
    assert doc["parser_module"] == "soccer.fotmob_parsers"
    assert doc["passthrough_query"] is False
    assert doc["runtime_imports"] == ["_get"]


def test_no_path_token_named_league_or_sport() -> None:
    """The renderer substitutes ESPN slugs for ``{league}``/``{sport}``."""
    for ep in _doc()["endpoints"]:
        tokens = set(_TOKEN.findall(ep["path"]))
        names = {p["name"] for p in ep.get("path_params", [])}
        assert not ({"league", "sport"} & (tokens | names)), ep["path"]


def test_yaml_lists_every_captured_route() -> None:
    eps = _doc()["endpoints"]
    shorts = [ep["short"] for ep in eps]
    assert len(shorts) == len(set(shorts))
    unverified: List[str] = []
    for ep in eps:
        schema_path = SCHEMAS / f"{ep['returns_schema']}.yaml"
        assert schema_path.exists(), ep["short"]
        schema = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
        if "unverified" in schema:
            unverified.append(ep["short"])
        assert ep["parser"] == "parse_fotmob"
        assert ep["returns_schema"] == f"native/fotmob/{ep['short']}"
    assert len(eps) == len(FIXTURE_STEMS) + len(unverified)
    assert unverified == []  # every spec route has a committed capture


def test_schema_columns_are_the_parser_output_on_the_capture() -> None:
    for ep in _doc()["endpoints"]:
        schema = yaml.safe_load((SCHEMAS / f"{ep['returns_schema']}.yaml").read_text(encoding="utf-8"))
        df = parse_fotmob(_load(_capture_stem(ep["path"])))
        assert [c["name"] for c in schema["columns"]] == df.columns, ep["short"]


def test_trending_news_is_the_only_host_override() -> None:
    by_short = _by_short()
    assert by_short["trending_news"]["host"] == "https://www.fotmob.com"
    assert by_short["trending_news"]["path"] == "/api/trendingnews"
    assert [s for s, ep in by_short.items() if "host" in ep] == ["trending_news"]


def test_shorts_follow_the_plan() -> None:
    expected = {
        "all_leagues",
        "audio_matches",
        "leagues",
        "match_details",
        "matches",
        "player_data",
        "search_suggest",
        "table",
        "teams",
        "tlnews",
        "top_transfers",
        "totw_rounds",
        "trending_news",
        "tvlistings",
    }
    assert set(_by_short()) == expected


def test_required_query_params_are_flagged_in_the_description() -> None:
    leagues = {p["name"]: p for p in _by_short()["leagues"]["extra_params"]}
    assert leagues["id"]["description"].startswith("Required.")
    assert not leagues["tab"]["description"].startswith("Required.")
    assert leagues["time_zone"]["query_key"] == "timeZone"
    assert all(p["type"] == "str" for p in leagues.values())


def test_example_args_use_python_names() -> None:
    by_short = _by_short()
    assert by_short["leagues"]["example_args"] == {"id": "47", "tab": "overview", "type": "league", "time_zone": "UTC"}
    assert by_short["match_details"]["example_args"] == {"match_id": "4813647"}
    assert "example_args" not in by_short["all_leagues"]


# ---------------------------------------------------------------------------
# generated wrappers (skipped until generate.py renders sportsdataverse/soccer/fotmob.py)
# ---------------------------------------------------------------------------


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
def wrapper(monkeypatch: pytest.MonkeyPatch) -> tuple:
    mod = pytest.importorskip(
        "sportsdataverse.soccer.fotmob", reason="rendered by generate.py on the combined wave tree"
    )
    rec = _Recorder()
    monkeypatch.setattr(mod, "_get", rec)
    return mod, rec


def test_wrapper_trending_news_requests_the_root_host(wrapper: tuple) -> None:
    mod, rec = wrapper
    mod.fotmob_trending_news(lang="en")
    assert rec.url == "https://www.fotmob.com/api/trendingnews"
    assert rec.params == {"lang": "en"}


def test_wrapper_leagues_sends_wire_query_keys(wrapper: tuple) -> None:
    mod, rec = wrapper
    rec.payload = _load("leagues")
    df = mod.fotmob_leagues(id="47", tab="overview", type="league", time_zone="UTC")
    assert rec.url == f"{HOST}/leagues"
    assert rec.params == {"id": "47", "tab": "overview", "type": "league", "timeZone": "UTC"}
    assert isinstance(df, pl.DataFrame) and df.height == 1
