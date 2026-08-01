"""Contract tests for the ``load_cfb_ratings`` dataset loader.

The codegen suite already covers URL shape and that loader modules render valid
Python. What it does NOT cover is the loader's declared returns-schema agreeing
with what the producer actually emits -- so a change to ``cfb_ratings()``'s
output would silently leave the loader's published returns-table lying. That
agreement is what these tests pin.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import yaml

# Per the NOTE in cfb_ratings.py, `sportsdataverse.cfb` re-exports the
# `cfb_ratings` *function*, shadowing the submodule -- reach the module's
# constants through a fully-qualified sys.modules lookup.
import sportsdataverse.cfb.cfb_ratings  # noqa: F401

_ratings_mod = sys.modules["sportsdataverse.cfb.cfb_ratings"]

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMAS = _ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml"
_RELEASES = _ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"

_POLARS_BY_NAME = {"Int64": pl.Int64, "Float64": pl.Float64, "String": pl.Utf8, "Utf8": pl.Utf8}


def _declared_schema() -> list[dict]:
    return yaml.safe_load(_SCHEMAS.read_text(encoding="utf-8"))["load_cfb_ratings"]


def _loader_entry() -> dict:
    loaders = yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]
    return next(ld for ld in loaders if ld["fn"] == "load_cfb_ratings")


def test_declared_schema_matches_the_producer_output_schema() -> None:
    """The loader's returns-table must match what the LOADER hands back.

    Both column ORDER and dtype: the published parquet is written straight from
    ``cfb_ratings()``, so any divergence here is a docs lie, not a formatting nit.

    One documented exception: columns listed under the loader's ``id_int64`` key
    are canonicalized to Int64 at the loader boundary (the same ESPN team id ships
    as String here and Int64 on the adv_*/box families, which makes a cross-dataset
    join silently match nothing). For those the declared type must be Int64 even
    though the producer still emits Utf8 -- the returns table documents the
    loader's output, not the raw asset.
    """
    produced = dict(_ratings_mod._RATINGS_OUTPUT_SCHEMA)
    for col in _loader_entry().get("id_int64", []):
        if col in produced:
            produced[col] = pl.Int64
    declared = _declared_schema()

    assert [c["name"] for c in declared] == list(produced.keys())
    for col in declared:
        assert _POLARS_BY_NAME[col["type"]] == produced[col["name"]], col["name"]


def test_loader_entry_points_at_the_published_tag_and_floor() -> None:
    entry = _loader_entry()

    assert entry["tag"] == "cfb_ratings"
    assert entry["base"] == "sdv_releases"
    # matches what cfb_model_publish's builder writes: cfb_ratings_{season}.parquet
    assert entry["url"] == "cfb_ratings/cfb_ratings_{season}.parquet"
    # the espn_cfb_pbp asset the ratings are computed from starts at 2004
    assert entry["min_season"] == 2004 == _loader_entry_min_season_of("load_cfb_pbp")


def _loader_entry_min_season_of(fn: str) -> int:
    loaders = yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]
    return next(ld for ld in loaders if ld["fn"] == fn)["min_season"]


def test_loader_is_exported() -> None:
    from sportsdataverse.cfb import load_cfb_ratings

    assert callable(load_cfb_ratings)
