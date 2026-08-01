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


def test_non_mapping_schema_fails_at_the_source(tmp_path, monkeypatch):
    """yaml.safe_load returns whatever the document is. A schema that parsed to
    a list must fail here, not later inside a caller's .get()."""
    import sportsdataverse.schemas as schemas_mod

    class _FakePath:
        def joinpath(self, *_parts):
            return self

        def read_text(self, encoding="utf-8"):
            return "- not\n- a mapping\n"

    schemas_mod.load_raw_schema.cache_clear()
    monkeypatch.setattr(schemas_mod, "files", lambda _pkg: _FakePath())
    try:
        with pytest.raises(TypeError, match="expected a mapping"):
            schemas_mod.load_raw_schema("espn_summary")
    finally:
        schemas_mod.load_raw_schema.cache_clear()


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


# --------------------------------------------------------------------------
# stats.nba.com / stats.wnba.com families.
#
# Derived by probing the committed hoopR-nba-stats-raw tree on 2026-08-01
# (489,977 files across 64 endpoint dirs, 40 sampled per dir). The plan for
# this work assumed a single uniform {resultSets: [...]} envelope; the archive
# actually holds FIVE, and the assumed one covers a minority of the files.
# --------------------------------------------------------------------------

NBA_STATS_FAMILIES = [
    "nba_stats_result_sets",
    "nba_stats_result_set",
    "nba_stats_result_sets_grouped",
    "nba_stats_v3",
    "nba_stats_v3_period",
]

# One minimal but REAL-shaped payload per family, keyed by schema name.
NBA_STATS_SAMPLES = {
    "nba_stats_result_sets": {
        "resource": "boxscoresummaryv2",
        "parameters": {"GameID": "0029600001"},
        "resultSets": [{"name": "GameSummary", "headers": ["GAME_ID"], "rowSet": [["0029600001"]]}],
    },
    "nba_stats_result_set": {
        "resource": "leagueleaders",
        "parameters": {"Season": "1996-97", "PerMode": "PerGame"},
        # A real empty result still carries headers -- only rowSet is empty.
        "resultSet": {"name": "LeagueLeaders", "headers": ["PLAYER_ID"], "rowSet": []},
    },
    "nba_stats_result_sets_grouped": {
        "resource": "leaguedashteamshotlocations",
        "parameters": {"Season": "1996-97", "MeasureType": "Base"},
        "resultSets": {
            "name": "ShotLocations",
            # Group dicts, NOT column-name strings. This is the divergence.
            "headers": [
                {
                    "name": "SHOT_CATEGORY",
                    "columnNames": ["Restricted Area"],
                    "columnSpan": 3,
                    "columnsToSkip": 5,
                },
                {"name": "columns", "columnNames": ["TEAM_ID", "TEAM_NAME"]},
            ],
            "rowSet": [[1610612737, "Atlanta Hawks"]],
        },
    },
    "nba_stats_v3": {
        "meta": {"request": "http://...", "time": "2026-01-01", "version": 1},
        "boxScoreTraditional": {"gameId": "0029500001", "homeTeamId": 1610612737},
    },
    "nba_stats_v3_period": {
        str(period): {
            "meta": {"request": "http://...", "time": "2026-01-01", "version": 1},
            "boxScoreTraditional": {"gameId": "0029600001"},
        }
        for period in range(1, 5)
    },
}


@pytest.mark.parametrize("name", NBA_STATS_FAMILIES)
def test_nba_stats_family_accepts_its_own_real_shape(name):
    assert validate_payload(name, NBA_STATS_SAMPLES[name]) == []


@pytest.mark.parametrize("name", NBA_STATS_FAMILIES)
def test_empty_payload_fails_every_nba_stats_family(name):
    """The load-bearing assertion.

    hoopR-nba-stats-raw holds 3,347 files that are exactly ``{}`` -- ten
    endpoints are 100% empty. They persist because the scraper's write path has
    no guard and its resume check is ``path.exists()``, i.e. presence rather
    than content, so one empty write is never retried.

    ``{}`` is not a shape stats.nba.com produces: an endpoint with no rows
    still returns the full envelope with ``rowSet: []``. Every family must
    therefore reject it, which is what makes these schemas a detector for that
    corruption rather than a passive document.
    """
    assert validate_payload(name, {}), f"{name} accepted an empty payload"


@pytest.mark.parametrize("name", NBA_STATS_FAMILIES)
def test_nba_stats_families_still_tolerate_unknown_keys(name):
    payload = dict(NBA_STATS_SAMPLES[name], someNewKeyTheAPIAdded={"x": 1})
    assert validate_payload(name, payload) == []


def test_singular_and_plural_result_set_keys_are_not_interchangeable():
    """`resultSet` vs `resultSets` is a genuinely different key, not a list of
    length one. Reading a singular payload with the plural path finds nothing."""
    singular = NBA_STATS_SAMPLES["nba_stats_result_set"]
    assert validate_payload("nba_stats_result_sets", singular)
    plural = NBA_STATS_SAMPLES["nba_stats_result_sets"]
    assert validate_payload("nba_stats_result_set", plural)


def test_grouped_family_rejects_a_list_valued_result_sets():
    """The shot-locations family keys `resultSets` to a dict. A list there is
    the classic family, which carries flat string headers and a different
    column layout."""
    listy = dict(NBA_STATS_SAMPLES["nba_stats_result_sets_grouped"], resultSets=[])
    assert validate_payload("nba_stats_result_sets_grouped", listy)


def test_v3_requires_an_entity_body_not_just_meta():
    """A dropped body must not validate clean. `meta` alone is a truncated
    payload, and every v3 endpoint pairs it with exactly one entity key."""
    meta_only = {"meta": {"request": "http://...", "time": "t", "version": 1}}
    problems = validate_payload("nba_stats_v3", meta_only)
    assert problems and "expected one" in problems[0]


@pytest.mark.parametrize("entity", sorted(load_raw_schema("nba_stats_v3")["required_any"][0]))
def test_v3_accepts_each_observed_entity_key(entity):
    """Parametrized FROM the schema, so a newly-observed entity key is covered
    the moment it is declared rather than whenever someone remembers this list."""
    payload = {"meta": {"request": "r", "time": "t", "version": 1}, entity: {}}
    assert validate_payload("nba_stats_v3", payload) == []


def test_v3_period_requires_the_four_regulation_periods():
    """Overtime periods are optional; a missing regulation period is a partial
    capture, which is the whole point of this family having a schema."""
    missing_third = {k: v for k, v in NBA_STATS_SAMPLES["nba_stats_v3_period"].items() if k != "3"}
    problems = validate_payload("nba_stats_v3_period", missing_third)
    assert any("'3'" in p for p in problems)


def test_v3_period_accepts_overtime():
    ot = dict(NBA_STATS_SAMPLES["nba_stats_v3_period"])
    ot["5"] = {"meta": {"request": "r", "time": "t", "version": 1}, "boxScoreTraditional": {}}
    assert validate_payload("nba_stats_v3_period", ot) == []


def test_v3_period_rejects_a_map_of_empty_periods():
    """`values_of` -- each period is validated as a full nba_stats_v3 payload.

    Without it this passes: the four required keys are present and each holds
    *a* dict. It is a capture that lost every payload body, which is exactly
    the corruption these schemas exist to catch.
    """
    problems = validate_payload("nba_stats_v3_period", {str(p): {} for p in range(1, 5)})
    assert problems
    assert any("[1]" in p for p in problems), problems


def test_v3_period_reports_which_period_is_bad():
    payload = dict(NBA_STATS_SAMPLES["nba_stats_v3_period"])
    payload["3"] = {"meta": {"version": 1}}  # entity body dropped
    problems = validate_payload("nba_stats_v3_period", payload)
    assert any("[3]" in p and "expected one" in p for p in problems), problems


def test_v3_period_rejects_a_non_dict_period():
    payload = dict(NBA_STATS_SAMPLES["nba_stats_v3_period"])
    payload["2"] = "not-a-payload"
    assert validate_payload("nba_stats_v3_period", payload)


def test_required_any_tolerates_a_bare_string_group():
    """`required_any: [foo]` instead of `[[foo]]` is an easy YAML slip.

    Iterating the string would test the payload for single CHARACTERS and
    report nonsense, so a bare string is treated as a one-key group.
    """
    from sportsdataverse import schemas

    schema = {"required": {}, "required_any": ["meta"], "additional_properties": True}
    monkey = schemas.load_raw_schema
    try:
        schemas.load_raw_schema = lambda _n: schema
        assert schemas.validate_payload("x", {"meta": {}}) == []
        problems = schemas.validate_payload("x", {"other": 1})
        assert problems and "'meta'" in problems[0]
    finally:
        schemas.load_raw_schema = monkey
