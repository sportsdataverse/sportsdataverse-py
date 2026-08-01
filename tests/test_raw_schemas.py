"""Declared shapes for the raw provider payloads committed in the -raw repos.

Every schema in this suite was derived from real committed captures, not from
the ESPN docs or from guesswork -- an earlier draft guessed four of seven
payload shapes wrong (``game_rosters`` is a full *summary* payload, not a
roster payload; ``officials`` is the Core v2 pagination envelope).
"""

from __future__ import annotations

import pytest

from sportsdataverse.schemas import RAW_SCHEMAS, load_raw_schema, validate_payload


@pytest.mark.parametrize("name", sorted(RAW_SCHEMAS))
def test_schema_loads_and_declares_required(name):
    schema = load_raw_schema(name)
    assert schema["required"], f"{name} declares no required keys"
    assert schema["additional_properties"] is True


def test_registry_matches_the_shipped_yaml_files():
    """A schema file with no registry entry is unreachable; the reverse crashes."""
    from importlib.resources import files

    shipped = {
        p.name.removesuffix(".yaml")
        for p in files("sportsdataverse.schemas").joinpath("raw").iterdir()
        if p.name.endswith(".yaml")
    }
    assert shipped == set(RAW_SCHEMAS)


def test_validate_accepts_unknown_keys():
    """ESPN adds fields routinely; unknown keys are not a defect signal."""
    payload = {
        "header": {},
        "boxscore": {},
        "plays": [],
        "gameInfo": {},
        "someBrandNewEspnKey": 1,
    }
    assert validate_payload("espn_summary", payload) == []


def test_validate_reports_missing_required_key():
    payload = {"header": {}, "boxscore": {}, "gameInfo": {}}  # no "plays"
    problems = validate_payload("espn_summary", payload)
    assert any("plays" in p for p in problems)


def test_validate_reports_wrong_type_without_coercing():
    payload = {"header": {}, "boxscore": {}, "plays": {}, "gameInfo": {}}
    problems = validate_payload("espn_summary", payload)
    assert any("plays" in p and "list" in p for p in problems)


@pytest.mark.parametrize("value", [[], {}])
def test_union_typed_key_accepts_either_form(value):
    """winprobability is a list in most payloads and a dict in some. Both are
    real; a single-type declaration would red the daily job on real data."""
    payload = {
        "header": {},
        "boxscore": {},
        "plays": [],
        "gameInfo": {},
        "winprobability": value,
    }
    assert validate_payload("espn_summary", payload) == []


def test_union_typed_key_still_rejects_a_third_type():
    payload = {
        "header": {},
        "boxscore": {},
        "plays": [],
        "gameInfo": {},
        "winprobability": "nope",
    }
    problems = validate_payload("espn_summary", payload)
    assert any("winprobability" in p for p in problems)


def test_null_optional_is_not_a_type_error():
    payload = {"header": {}, "boxscore": {}, "plays": [], "gameInfo": {}, "odds": None}
    assert validate_payload("espn_summary", payload) == []


def test_bool_is_not_accepted_where_int_is_declared():
    """bool subclasses int in Python; a True page count is a defect."""
    payload = {"count": True, "items": [], "pageIndex": 1, "pageSize": 25, "pageCount": 1}
    problems = validate_payload("espn_officials", payload)
    assert any("count" in p and "bool" in p for p in problems)


def test_non_dict_payload_is_reported_not_raised():
    assert validate_payload("espn_summary", []) == ["espn_summary: payload is list, expected dict"]


def test_unknown_schema_name_raises():
    with pytest.raises(KeyError):
        load_raw_schema("espn_not_a_real_family")


def test_yaml_is_not_a_module_level_dependency():
    """PyYAML is a build/CI-time dep, not a runtime one. A module-level import
    would make `pip install sportsdataverse` unable to import this package."""
    from sportsdataverse import schemas

    assert not hasattr(schemas, "yaml"), "yaml must be imported lazily inside load_raw_schema, not at module level"


def test_missing_pyyaml_gives_actionable_guidance(monkeypatch):
    import sys

    from sportsdataverse import schemas

    schemas.load_raw_schema.cache_clear()
    monkeypatch.setitem(sys.modules, "yaml", None)  # makes `import yaml` raise
    try:
        with pytest.raises(ImportError, match="pip install pyyaml"):
            schemas.load_raw_schema("espn_summary")
    finally:
        schemas.load_raw_schema.cache_clear()


@pytest.mark.parametrize(
    "name,required",
    [
        ("espn_summary", {"header", "boxscore", "plays", "gameInfo"}),
        ("espn_game_rosters", {"header", "boxscore", "plays", "gameInfo"}),
        ("espn_officials", {"items", "count"}),
        ("espn_player_core", {"id", "displayName"}),
        ("espn_standings", {"id", "children"}),
        ("espn_team_stats", {"results", "team"}),
        ("espn_player_season_stats", {"categories", "teams"}),
    ],
)
def test_required_keys_match_what_was_observed(name, required):
    """Pins the observed-universal keys. Loosening one of these means a parser
    can now be handed a payload it cannot read."""
    assert required <= set(load_raw_schema(name)["required"])
