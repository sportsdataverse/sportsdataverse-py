"""Contract tests for the ``load_mbb_ratings`` / ``load_mbb_player_value`` loaders.

Mirrors ``test_cfb_loaders_ratings.py``: pins each loader's declared
returns-schema to the producer module's schema constant so a compute schema
change cannot silently leave the published returns-table lying.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import yaml

import sportsdataverse.mbb.mbb_box_bpm  # noqa: F401
import sportsdataverse.mbb.mbb_team_ratings  # noqa: F401

_ratings_mod = sys.modules["sportsdataverse.mbb.mbb_team_ratings"]
_bpm_mod = sys.modules["sportsdataverse.mbb.mbb_box_bpm"]

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMAS = _ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml"
_RELEASES = _ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"

_POLARS_BY_NAME = {"Int64": pl.Int64, "Float64": pl.Float64, "String": pl.Utf8, "Utf8": pl.Utf8}


def _declared(fn: str) -> list[dict]:
    return yaml.safe_load(_SCHEMAS.read_text(encoding="utf-8"))[fn]


def _entry(fn: str) -> dict:
    loaders = yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]
    return next(ld for ld in loaders if ld["fn"] == fn)


def _assert_schema_matches(declared: list[dict], produced: dict) -> None:
    assert [c["name"] for c in declared] == list(produced.keys())
    for col in declared:
        assert _POLARS_BY_NAME[col["type"]] == produced[col["name"]], col["name"]


def test_ratings_declared_schema_matches_the_producer() -> None:
    _assert_schema_matches(_declared("load_mbb_ratings"), _ratings_mod._RATINGS_SCHEMA)


def test_player_value_declared_schema_matches_the_producer() -> None:
    _assert_schema_matches(_declared("load_mbb_player_value"), _bpm_mod._SCHEMA)


def test_loader_entries_point_at_the_published_tags_and_floor() -> None:
    r = _entry("load_mbb_ratings")
    assert (r["tag"], r["base"], r["min_season"]) == ("mbb_ratings", "sdv_releases", 2006)
    assert r["url"] == "mbb_ratings/mbb_ratings_{season}.parquet"

    v = _entry("load_mbb_player_value")
    assert (v["tag"], v["base"], v["min_season"]) == ("mbb_player_value", "sdv_releases", 2006)
    assert v["url"] == "mbb_player_value/mbb_player_value_{season}.parquet"


def test_loaders_are_exported() -> None:
    from sportsdataverse.mbb import load_mbb_player_value, load_mbb_ratings

    assert callable(load_mbb_ratings) and callable(load_mbb_player_value)
