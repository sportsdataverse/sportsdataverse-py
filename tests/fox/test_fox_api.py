"""Offline tests for the generated ``fox_api_*`` family (``api.foxsports.com``).

Fixtures in ``tests/fixtures/fox_api/`` are real captures (see its README).
No network: the generated getter is monkeypatched.
"""

from __future__ import annotations

import importlib
import json
import pkgutil
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import polars as pl
import pytest
import yaml

import sportsdataverse
from sportsdataverse.fox import fox_api as gen
from sportsdataverse.fox import fox_api_parsers as P

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures" / "fox_api"
SPEC = yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / "fox_api.yaml").read_text(encoding="utf-8"))
ENDPOINTS: List[Dict[str, Any]] = SPEC["endpoints"]


def _load(stem: str) -> Any:
    return json.loads((FIX / f"{stem}.json").read_text(encoding="utf-8"))


def test_endpoint_count_and_names() -> None:
    assert len(ENDPOINTS) == 33
    assert sorted(gen.__all__) == sorted(f"fox_api_{e['short']}" for e in ENDPOINTS)


def test_no_public_name_collision() -> None:
    """Every fox_api_* name is defined once, by the generated module; no other
    sportsdataverse module defines a ``fox_api*`` name, and the package-level
    names resolve to the generated callables (nothing shadowed)."""
    for name in gen.__all__:
        assert getattr(sportsdataverse, name) is getattr(gen, name)
    owners: Dict[str, List[str]] = {}
    for mod in pkgutil.walk_packages(sportsdataverse.__path__, "sportsdataverse."):
        if mod.name.startswith("sportsdataverse.fox"):
            continue
        try:
            m = importlib.import_module(mod.name)
        except Exception:  # optional-dependency modules
            continue
        for attr in dir(m):
            if attr.startswith("fox_api") and getattr(m, attr, None) is not getattr(gen, attr, object()):
                owners.setdefault(attr, []).append(mod.name)
    assert owners == {}


def _expected(e: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    args = e.get("example_args", {})
    url = SPEC["host"] + re.sub(r"\{(\w+)\}", lambda m: str(args[m.group(1)]), e["path"])
    params: Dict[str, Any] = {}
    for p in e["extra_params"]:
        val = args.get(p["name"], p.get("default"))
        if val is not None:
            params[p["query_key"]] = str(val).lower() if p["type"] == "bool" else val
    return url, params


@pytest.mark.parametrize("e", ENDPOINTS, ids=lambda e: e["short"])
def test_endpoint_resolves_to_expected_url_and_query(e: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    seen: Dict[str, Any] = {}

    def fake(url: str, params: Dict[str, Any] | None = None, **kw: Any) -> Dict:
        seen["url"], seen["params"] = url, {k: v for k, v in (params or {}).items() if v is not None}
        return {}

    monkeypatch.setattr(gen, "_get", fake)
    out = getattr(gen, f"fox_api_{e['short']}")(**e.get("example_args", {}), return_parsed=False)
    url, params = _expected(e)
    assert out == {}
    assert seen["url"] == url
    assert seen["params"] == params
    declares_version = any(p["query_key"] == "api-version" for p in e["extra_params"])
    assert ("api-version" in seen["params"]) == declares_version
    assert re.fullmatch(r"[A-Za-z0-9]{32}", seen["params"]["apikey"])


def test_dropped_dead_endpoints_stay_dropped() -> None:
    shorts = {e["short"] for e in ENDPOINTS}
    assert not shorts & {"fs_feed", "fs_images", "fs_layouts", "fs_videos", "explore_favorite"}


def test_scorechip_sends_no_api_version() -> None:
    sc = next(e for e in ENDPOINTS if e["short"] == "scorechip")
    assert all(p["query_key"] != "api-version" for p in sc["extra_params"])


def test_feed_tier_uses_feed_key() -> None:
    keys = {e["short"]: next(p for p in e["extra_params"] if p["name"] == "apikey")["default"] for e in ENDPOINTS}
    assert keys["trending_articles"] == keys["foxpolls"] != keys["scoreboard"]


GROUPS: Dict[str, Tuple[Any, int, str]] = {
    # fixture stem -> (parser, min_rows, a column that must exist)
    "nfl_scoreboard": (P.parse_fox_api_events, 1, "title"),
    "nfl_league_scores_segment": (P.parse_fox_api_events, 16, "game_id"),
    "nfl_league_header": (P.parse_fox_api_header, 1, "title"),
    "nfl_league_standings": (P.parse_fox_api_standings, 30, "section"),
    "cfb_league_polls": (P.parse_fox_api_polls, 25, "section"),
    "cbk_league_conferences": (P.parse_fox_api_nav, 10, "name"),
    "nfl_event_matchup": (P.parse_fox_api, 1, "entity_link_title"),
    "nfl_team_header": (P.parse_fox_api_header, 1, "title"),
    "nfl_team_roster": (P.parse_fox_api_roster, 40, "athlete_id"),
    "explore_browse_sports": (P.parse_fox_api_nav, 10, "name"),
    "search_content": (P.parse_fox_api_search, 1, "title"),
    "search_popular": (P.parse_fox_api_search, 1, "title"),
    "trending_articles": (P.parse_fox_api_trending, 2, "title"),
    "trending_videos": (P.parse_fox_api_trending, 5, "title"),
    "foxpolls": (P.parse_fox_api, 5, "poll_id"),
    "nfl_scorechip": (P.parse_fox_api_scorechip, 1, "id"),
    "topevents_segment": (P.parse_fox_api_events, 12, "game_id"),
}


@pytest.mark.parametrize("stem", GROUPS)
def test_parsers_on_real_captures(stem: str) -> None:
    parser, min_rows, col = GROUPS[stem]
    raw = _load(stem)
    df = parser(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height >= min_rows
    assert col in df.columns
    assert all(c == c.lower() for c in df.columns)
    assert parser(raw, return_as_pandas=True).shape == df.shape


def test_ids_stay_strings() -> None:
    games = P.parse_fox_api_events(_load("nfl_league_scores_segment"))
    assert games["game_id"].dtype == pl.Utf8 and "11195" in games["game_id"].to_list()
    assert not any(str(v).endswith(".0") for v in games["home_team_id"].to_list())
    roster = P.parse_fox_api_roster(_load("nfl_team_roster"))
    assert roster["athlete_id"].dtype == pl.Utf8
    assert "team_id" not in roster.columns


@pytest.mark.parametrize("parser", [getattr(P, n) for n in P.__all__])
@pytest.mark.parametrize("bad", [{}, [], None, "x", 3, {"a": [1, 2]}, {"a": [None]}])
def test_parsers_never_raise_on_empty_or_malformed(parser: Any, bad: Any) -> None:
    assert parser(bad).shape[0] == 0
    assert parser(bad, return_as_pandas=True).shape[0] == 0


def test_wrapper_end_to_end_with_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gen, "_get", lambda url, params=None, **kw: _load("nfl_league_scores_segment"))
    df = gen.fox_api_league_scores_segment(sport="nfl", segment_id="2026-3-1")
    assert df.height == 16
