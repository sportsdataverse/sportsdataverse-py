"""Offline tests for the UEFA front-end API (``uefa``) flat-API family.

Asserts the parser against the real truncated captures in ``tests/fixtures/uefa/``
(never synthetic payloads), the generated endpoint YAML (host overrides, path
tokens, getter), the runtime's browser headers, and the empty/malformed contract.
No network. The wrapper tests skip until Task 6 runs ``generate.py`` and
``sportsdataverse.soccer.uefa`` exists.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

import polars as pl
import pytest
import yaml

from sportsdataverse.soccer import uefa_runtime
from sportsdataverse.soccer._frames import is_id_name
from sportsdataverse.soccer.uefa_parsers import parse_uefa

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "uefa"
ENDPOINTS_YAML = ROOT / "tools" / "codegen" / "endpoints" / "uefa.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "uefa"

FAMILY_HOST = "https://comp.uefa.com"

# short -> committed capture (sdv-internal-refs/uefa/captures, capture.py's slug).
CAPTURES: Dict[str, str] = {
    "competitions": "comp__v2__competitions",
    "teams": "comp__v2__teams",
    "players": "comp__v2__players",
    "matches": "match__v5__matches",
    "livescore": "match__v5__livescore",
    "standings": "standings__v1__standings",
    "team_statistics": "matchstats__v1__team-statistics__2047742",
}

# short -> endpoint-level ``host:`` the YAML must carry (the spec's per-path
# ``servers``); ``None`` = the family host, which must not be repeated.
EXPECTED_HOSTS: Dict[str, Optional[str]] = {
    "competitions": None,
    "teams": None,
    "players": None,
    "matches": "https://match.uefa.com",
    "livescore": "https://match.uefa.com",
    "standings": "https://standings.uefa.com",
    "team_statistics": "https://matchstats.uefa.com",
}

_STEMS = sorted(p.stem for p in FIXTURES.glob("*.json"))
_SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
_TOKEN = re.compile(r"\{(\w+)\}")


def _load(short: str) -> Any:
    return json.loads((FIXTURES / f"{CAPTURES[short]}.json").read_text(encoding="utf-8"))


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(ENDPOINTS_YAML.read_text(encoding="utf-8"))


def _endpoint(short: str) -> Dict[str, Any]:
    return next(ep for ep in _yaml()["endpoints"] if ep["short"] == short)


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
def recorder(monkeypatch: pytest.MonkeyPatch) -> Tuple[Any, _Recorder]:
    uefa = pytest.importorskip("sportsdataverse.soccer.uefa", reason="rendered by generate.py in Task 6")
    rec = _Recorder()
    monkeypatch.setattr(uefa, "_get", rec)
    return uefa, rec


# ---------------------------------------------------------------------------
# parse_uefa -- every committed capture
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stem", _STEMS)
def test_every_fixture_parses_to_a_tidy_frame(stem: str) -> None:
    raw = json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))
    df = parse_uefa(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == len(raw), "every capture is a bare list: one row per element"
    assert df.width >= 1
    for col in df.columns:
        assert _SNAKE.match(col), f"{stem}.{col} is not snake_case"
        if is_id_name(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"
            assert not df[col].str.ends_with(".0").any(), f"{stem}.{col} has a float-origin id"


def test_page_object_is_one_row() -> None:
    """Review Focus 1. ``team_statistics`` is NOT a page object: the real body is a
    bare list of one object per team, so it parses to one row PER TEAM -- never one
    row per top-level key, never one row per statistic. The page-object branch is
    pinned on the widest real UEFA object, one ``/v5/matches`` element: nested
    ``homeTeam`` / ``awayTeam`` objects beside ONE list (``referees``) -> ONE row,
    not the referee list."""
    stats = _load("team_statistics")
    assert isinstance(stats, list) and len(stats) == 2
    df = parse_uefa(stats)
    assert df.height == 2
    assert df.schema["team_id"] == pl.String
    assert df.schema["statistics"] == pl.String
    assert json.loads(df["statistics"][0])[0]["name"]

    match = _load("matches")[0]
    assert [k for k, v in match.items() if isinstance(v, list)] == ["referees"]
    one = parse_uefa(match)
    assert one.height == 1
    assert {"id", "home_team_id", "away_team_id", "referees"} <= set(one.columns)
    assert one["id"].item() == match["id"]


def test_id_keyed_map_rows_carry_the_key_as_id() -> None:
    """A ``{id: {...}, id: {...}}`` map yields one row per value, the key in ``id``."""
    teams = _load("teams")
    raw = {row["id"]: {k: v for k, v in row.items() if k != "id"} for row in teams}
    df = parse_uefa(raw)
    assert df.height == 3
    assert df.schema["id"] == pl.String
    assert sorted(df["id"].to_list()) == sorted(row["id"] for row in teams)


def test_single_list_envelope_rows_come_from_the_list() -> None:
    df = parse_uefa({"total": 3, "data": _load("players")})
    assert df.height == 3
    assert {"total", "data"}.isdisjoint(df.columns)


def test_return_as_pandas() -> None:
    pdf = parse_uefa(_load("competitions"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3


@pytest.mark.parametrize("raw", [{}, [], None, "x", 17, [None]])
def test_empty_payload_is_zero_row_frame(raw: Any) -> None:
    df = parse_uefa(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


# ---------------------------------------------------------------------------
# generated endpoint YAML + schemas (gen_uefa.py output)
# ---------------------------------------------------------------------------


def test_yaml_lists_every_captured_route() -> None:
    doc = _yaml()
    unverified = [
        p for p in SCHEMA_DIR.glob("*.yaml") if yaml.safe_load(p.read_text(encoding="utf-8")).get("unverified")
    ]
    assert len(doc["endpoints"]) == len(_STEMS) + len(unverified)
    assert sorted(CAPTURES.values()) == _STEMS
    assert sorted(ep["short"] for ep in doc["endpoints"]) == sorted(CAPTURES)
    assert (doc["host"], doc["name_pattern"], doc["parser_module"]) == (
        FAMILY_HOST,
        "uefa_{short}",
        "soccer.uefa_parsers",
    )
    for ep in doc["endpoints"]:
        # capture.py's slug of the endpoint's example URL names its committed fixture
        path = _TOKEN.sub(lambda m: str(ep["example_args"][m.group(1)]), ep["path"])
        host = urlsplit(ep.get("host", doc["host"])).hostname or ""
        assert host.split(".")[0] + "__" + path.strip("/").replace("/", "__") == CAPTURES[ep["short"]]
        assert ep["parser"] == "parse_uefa"
        assert ep["returns_schema"] == f"native/uefa/{ep['short']}"
        schema = yaml.safe_load((SCHEMA_DIR / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
        assert schema["schema"] == ep["short"]
        assert schema["columns"], f"{ep['short']} schema has no columns"


def test_every_foreign_host_op_carries_host() -> None:
    """Every op whose spec host is not comp.uefa.com overrides ``host:`` on the endpoint."""
    for ep in _yaml()["endpoints"]:
        assert ep.get("host") == EXPECTED_HOSTS[ep["short"]], ep["short"]


def test_no_path_token_named_league_or_sport() -> None:
    """The renderer substitutes ESPN slugs for ``{league}``/``{sport}`` -- UEFA's token is ``matchId``."""
    for ep in _yaml()["endpoints"]:
        tokens = set(_TOKEN.findall(ep["path"]))
        names = {p["name"] for p in ep.get("path_params", [])}
        assert not {"league", "sport"} & (tokens | names), ep["short"]
        assert tokens == names, ep["short"]
    assert _endpoint("team_statistics")["path"] == "/v1/team-statistics/{match_id}"


def test_query_params_keep_the_wire_key() -> None:
    by_name = {p["name"]: p for p in _endpoint("teams")["extra_params"]}
    assert by_name["competition_id"]["query_key"] == "competitionId"
    assert by_name["competition_id"]["description"].startswith("Required.")
    assert by_name["season_year"]["query_key"] == "seasonYear"
    assert not by_name["season_year"]["description"].startswith("Required.")


# ---------------------------------------------------------------------------
# runtime -- the edge 403s the default python-requests User-Agent
# ---------------------------------------------------------------------------


class _Resp:
    status_code = 200
    text = "[]"

    def json(self) -> Any:
        return []


def test_runtime_sends_browser_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: Dict[str, Any] = {}

    def fake_download(url: str, params: Any = None, headers: Any = None, **kwargs: Any) -> _Resp:
        seen.update(url=url, params=params, headers=headers)
        return _Resp()

    monkeypatch.setattr(uefa_runtime, "download", fake_download)
    assert uefa_runtime._get("https://comp.uefa.com/v2/teams", {"competitionId": "1", "seasonYear": None}) == []
    assert seen["headers"]["User-Agent"].startswith("Mozilla/5.0")
    assert seen["headers"]["Origin"] == "https://www.uefa.com"
    assert seen["params"] == {"competitionId": "1"}
    uefa_runtime._get("https://comp.uefa.com/v2/teams", headers={"User-Agent": "mine"})
    assert seen["headers"]["User-Agent"] == "mine"
    assert seen["headers"]["Accept"] == "application/json"
    assert _yaml()["getter_module"] == "sportsdataverse.soccer.uefa_runtime"


# ---------------------------------------------------------------------------
# generated wrappers (skipped until generate.py has rendered the module)
# ---------------------------------------------------------------------------


def test_wrapper_uses_the_family_host(recorder: Tuple[Any, _Recorder]) -> None:
    uefa, rec = recorder
    uefa.uefa_competitions(competition_ids="1,3,2019")
    assert rec.url == "https://comp.uefa.com/v2/competitions"
    assert rec.params["competitionIds"] == "1,3,2019"


def test_wrapper_standings_host_override(recorder: Tuple[Any, _Recorder]) -> None:
    uefa, rec = recorder
    uefa.uefa_standings(competition_id="1", season_year="2026")
    assert rec.url == "https://standings.uefa.com/v1/standings"
    assert rec.params == {"competitionId": "1", "seasonYear": "2026"}


def test_wrapper_team_statistics_host_override(recorder: Tuple[Any, _Recorder]) -> None:
    uefa, rec = recorder
    uefa.uefa_team_statistics(match_id="2047742")
    assert rec.url == "https://matchstats.uefa.com/v1/team-statistics/2047742"


def test_wrapper_parses_by_default(recorder: Tuple[Any, _Recorder]) -> None:
    uefa, rec = recorder
    rec.payload = _load("teams")
    df = uefa.uefa_teams(competition_id="1")
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3


def test_curated_uefa_columns_are_described():
    import yaml

    root = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "schemas" / "native" / "uefa"
    stats = {
        c["name"]: c["description"]
        for c in yaml.safe_load((root / "team_statistics.yaml").read_text(encoding="utf-8"))["columns"]
    }
    assert "JSON" in stats["statistics"], stats["statistics"]  # the whole per-team stat list lives in this one cell
    matches = {
        c["name"]: c["description"]
        for c in yaml.safe_load((root / "matches.yaml").read_text(encoding="utf-8"))["columns"]
    }
    assert "TOURNAMENT" in matches["competition_phase"], matches["competition_phase"]
