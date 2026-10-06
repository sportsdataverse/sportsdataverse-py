"""Offline tests for the Sleeper fantasy API (``sleeper``) stem.

Asserts the parser against the real trimmed captures in ``tests/fixtures/sleeper/``
(never synthetic payloads), the generated endpoint YAML, and the empty/malformed
contract. No network.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

from sportsdataverse.nfl.sleeper_parsers import parse_sleeper

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sleeper"
ENDPOINTS_YAML = Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "sleeper.yaml"
STEMS = sorted(p.stem for p in FIXTURES.glob("*.json"))

# Sleeper ids are 18-digit strings: an int cast overflows a float, a float cast
# writes "123.0". Every column named like an id must stay Utf8.
_ID = re.compile(r"(^|_)(id|code)$")
_SNAKE = re.compile(r"^[a-z0-9_]+$")


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def _yaml() -> dict:
    return yaml.safe_load(ENDPOINTS_YAML.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# parser contract over every committed capture
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stem", STEMS)
def test_every_capture_parses_to_a_tidy_frame(stem: str) -> None:
    df = parse_sleeper(_load(stem))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, f"{stem}: no rows"
    for col in df.columns:
        assert _SNAKE.match(col), f"{stem}.{col} is not snake_case"
        if _ID.search(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"
            assert not df[col].str.ends_with(".0").any(), f"{stem}.{col} stringified a float"


@pytest.mark.parametrize("raw", [{}, [], None, "x", 17, [None]])
def test_empty_payload_is_zero_row_frame(raw: Any) -> None:
    df = parse_sleeper(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_return_as_pandas() -> None:
    pdf = parse_sleeper(_load("league__league_id__rosters"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3


# ---------------------------------------------------------------------------
# Review Focus: id-keyed map, page object, path tokens
# ---------------------------------------------------------------------------


def test_players_map_rows_carry_player_id() -> None:
    """``/players/nfl`` is ``{player_id: {...}}``: rows are the values, keyed by id."""
    raw = _load("players__nfl")
    df = parse_sleeper(raw)
    assert df.height == len(raw) == 3
    assert df.schema["player_id"] == pl.String
    assert sorted(df["player_id"].to_list()) == sorted(raw)
    assert {"first_name", "last_name", "position"} <= set(df.columns)


def test_page_object_is_one_row() -> None:
    """``/league/{league_id}`` is one wide object -> one row, not one row per key."""
    raw = _load("league__league_id")
    df = parse_sleeper(raw)
    assert df.height == 1
    assert df["league_id"].item() == raw["league_id"]
    assert df.schema["league_id"] == pl.String
    assert "settings_num_teams" in df.columns  # nested fixed-key objects are flattened


@pytest.mark.parametrize("stem", ["user__username", "state__nfl", "draft__draft_id"])
def test_other_page_objects_are_one_row(stem: str) -> None:
    assert parse_sleeper(_load(stem)).height == 1


def test_id_keyed_nested_maps_stay_one_json_cell() -> None:
    """``players_points`` is keyed by player id -- flattening it would make the
    column set depend on who was rostered that week."""
    df = parse_sleeper(_load("league__league_id__matchups__week"))
    assert "players_points" in df.columns
    assert not any(c.startswith("players_points_") for c in df.columns)
    assert isinstance(json.loads(df["players_points"][0]), dict)
    draft = parse_sleeper(_load("draft__draft_id"))
    assert isinstance(json.loads(draft["draft_order"].item()), dict)
    rosters = parse_sleeper(_load("league__league_id__rosters"))
    assert not any("p_nick" in c for c in rosters.columns)
    assert "p_nick_2118" in json.loads(rosters["metadata"][0])
    # fixed-key metadata elsewhere is still flattened
    assert "metadata_scoring_type" in draft.columns


def test_integer_ids_become_utf8_without_float_artifacts() -> None:
    df = parse_sleeper(_load("league__league_id__traded_picks"))
    assert df.schema["roster_id"] == pl.String
    assert df.schema["owner_id"] == pl.String
    assert df["roster_id"].to_list() == [str(r["roster_id"]) for r in _load("league__league_id__traded_picks")]


def test_no_path_token_named_league_or_sport() -> None:
    for ep in _yaml()["endpoints"]:
        tokens = set(re.findall(r"\{(\w+)\}", ep["path"]))
        assert not tokens & {"league", "sport"}, ep["path"]
        assert tokens == {p["name"] for p in ep.get("path_params", [])}, ep["path"]


def test_yaml_lists_every_captured_route() -> None:
    doc = _yaml()
    assert doc["api"] == "sleeper"
    assert doc["module"] == "sleeper"
    assert doc["name_pattern"] == "sleeper_{short}"
    assert doc["parser_module"] == "nfl.sleeper_parsers"
    assert len(doc["endpoints"]) == len(STEMS)  # every route has a committed capture; none unverified
    shorts = [ep["short"] for ep in doc["endpoints"]]
    assert len(set(shorts)) == len(shorts)
    assert {"user", "user_leagues", "league", "players", "trending_adds", "state"} <= set(shorts)
    for ep in doc["endpoints"]:
        assert ep["parser"] == "parse_sleeper"
        assert ep["returns_schema"] == f"native/sleeper/{ep['short']}"
        for prm in ep.get("path_params", []):
            assert prm["type"] == "str"
            assert isinstance(ep["example_args"][prm["name"]], str)  # 18-digit ids never travel as ints


def test_generated_schemas_match_the_parser() -> None:
    schema_dir = ENDPOINTS_YAML.parents[1] / "schemas" / "native" / "sleeper"
    for ep in _yaml()["endpoints"]:
        d = yaml.safe_load((schema_dir / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
        assert d["schema"] == ep["short"]
        assert d["columns"], ep["short"]


# ---------------------------------------------------------------------------
# generated wrappers (rendered by generate.py in the wave branch; skipped until then)
# ---------------------------------------------------------------------------


def test_wrapper_builds_the_league_url(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeper = pytest.importorskip("sportsdataverse.nfl.sleeper")
    seen: dict = {}

    def fake(url: str, params: Any = None, **kwargs: Any) -> Any:
        seen["url"] = url
        return _load("league__league_id")

    monkeypatch.setattr(sleeper, "_get", fake)
    df = sleeper.sleeper_league(league_id="289646328504385536")
    assert seen["url"] == "https://api.sleeper.app/v1/league/289646328504385536"
    assert df.height == 1


def test_package_reexports_sleeper_wrappers_and_parser():
    # the generated docstrings say ``from sportsdataverse.nfl import sleeper_draft``; pff is the precedent
    from sportsdataverse import nfl

    assert callable(nfl.sleeper_draft) and callable(nfl.parse_sleeper)


def test_descriptions_do_not_borrow_soccer_prose():
    # the shared describe() falls back to a soccer leaf dictionary; a player's full_name is not a club
    import yaml

    root = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "schemas" / "native" / "sleeper"
    for path in sorted(root.glob("*.yaml")):
        for col in yaml.safe_load(path.read_text(encoding="utf-8")).get("columns") or []:
            d = col["description"]
            assert "club" not in d.lower() and not d.startswith("Season: ") and "Opta" not in d, (
                path.name,
                col["name"],
                d,
            )
