"""A minted per-competition function documents its BASE wrapper's returns table (one source of truth)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.basketball import atrium as A
from sportsdataverse.basketball import fiba_hub as F
from sportsdataverse.basketball import genius as G
from sportsdataverse.basketball._competitions_registry import REGISTRY
from sportsdataverse.basketball.atrium import atrium_competitions_runtime as atrium_rt
from sportsdataverse.basketball.fiba_hub import fiba_competitions_runtime as fiba_rt
from sportsdataverse.basketball.genius import genius_competitions_runtime as genius_rt
from tools.codegen import generate

ROOT = Path(__file__).resolve().parents[2]
FX = ROOT / "tests" / "fixtures"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas"
HOMES = {"fiba": (fiba_rt, "fiba_hub"), "genius": (genius_rt, "genius"), "atrium": (atrium_rt, "atrium")}
BASE_PREFIX = {"fiba_hub": ("fiba_gdap_", "fiba_gamepage_"), "genius": ("genius_",), "atrium": ("atrium_",)}
_DTYPES = {
    "integer": lambda d: d.is_integer(),
    "numeric": lambda d: d.is_float(),
    "character": lambda d: d == pl.Utf8,
    "logical": lambda d: d == pl.Boolean,
}


def _minted():
    for rt, scope in HOMES.values():
        for fam in rt.FAMILIES.values():
            for name, fn in fam.items():
                yield scope, name, fn


def _native(fn):
    return yaml.safe_load((SCHEMAS / f"{fn.__sdv_schema__}.yaml").read_text(encoding="utf-8"))["columns"]


def test_every_minted_function_points_at_its_base_and_returns_a_frame_annotation():
    count = 0
    for scope, name, fn in _minted():
        count += 1
        assert fn.__sdv_base__.startswith(BASE_PREFIX[scope]), name
        assert generate._returns_a_frame(fn.__annotations__["return"]), name
    assert count == sum(len(rt.__all__) for rt, _ in HOMES.values())


def test_every_minted_function_resolves_its_base_schema_columns():
    dataframe_names = set(generate._autodoc_dataframe_names())
    for scope, name, fn in _minted():
        assert (scope, name) in dataframe_names, name
        cols = generate._autodoc_return_columns(scope, name)
        assert cols, name
        assert [c["name"] for c in cols] == [c["name"] for c in _native(fn)], name


def test_minted_tables_take_the_base_wrappers_descriptions():
    scope, name, fn = next(iter(_minted()))
    groups = generate._autodoc_groups(scope, [name])
    table = groups[0]["functions"][0]["return_columns"]
    assert len(table) == len(_native(fn))
    described = [c for c in table if c["description"].strip()]
    assert described, "the base wrapper's manual descriptions must reach the minted function's table"


def _load(path: Path):
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(path.read_text(encoding="utf-8"))


def _genius(name, parser):
    raw = _load(FX / "genius" / "full" / "data_2902690.json.gz")
    raw["_request"] = {"match_id": 2902690}
    return getattr(G, parser)(raw)


# (base short, schema dir, frame builder) -- three base wrappers per platform on committed FULL bodies.
FRAMES = {
    ("genius", "team_box"): lambda: _genius("x", "parse_genius_team_box"),
    ("genius", "player_box"): lambda: _genius("x", "parse_genius_player_box"),
    ("genius", "pbp"): lambda: _genius("x", "parse_genius_pbp"),
    ("atrium", "fixtures"): lambda: A.parse_atrium_fixtures(_load(FX / "atrium" / "full" / "12__fixtures.json")),
    ("atrium", "standings"): lambda: A.parse_atrium_standings(_load(FX / "atrium" / "full" / "12__standings.json")),
    ("atrium", "pbp"): lambda: A.parse_atrium_pbp(_load(FX / "atrium" / "full" / "14__fixture_detail__pbp.json")),
    ("fiba_gdap", "schedule"): lambda: F.parse_fiba_gdap_schedule(
        _load(FX / "fiba_gdap" / "full" / "getgdapgamesbycompetitionid__paris2024-men.json")
    ),
    ("fiba_gdap", "rounds"): lambda: F.parse_fiba_gdap_rounds(
        _load(FX / "fiba_gdap" / "full" / "getgdaproundsbycompetitionid__wc2023.json")
    ),
    ("fiba_gamepage", "pbp"): lambda: F.parse_fiba_gamepage_pbp(
        _load(FX / "fiba_gamepage" / "full" / "page__wc2023-final.json")
    ),
}


@pytest.mark.parametrize(("stem", "short"), sorted(FRAMES))
def test_declared_schema_covers_the_parsed_full_fixture(stem, short):
    frame = FRAMES[(stem, short)]()
    declared = yaml.safe_load((SCHEMAS / "native" / stem / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]
    assert frame.height > 0
    assert set(frame.columns) <= {c["name"] for c in declared}, set(frame.columns) - {c["name"] for c in declared}
    for c in declared:
        if c["name"] in frame.columns and c.get("type") in _DTYPES:
            assert _DTYPES[c["type"]](frame.schema[c["name"]]), (c["name"], frame.schema[c["name"]], c["type"])


def test_the_registry_is_fully_covered_by_the_parametrisation():
    assert {p for p in REGISTRY} == set(HOMES)
