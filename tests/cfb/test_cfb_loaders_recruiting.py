"""Contract tests for the ``load_cfb_recruiting_proj`` dataset loader.

Mirrors ``test_cfb_loaders_ratings.py``: the codegen suite covers URL shape and
loader rendering; these pin the loader's declared returns-schema to what the
producer (``cfb_recruiting_projection``) actually emits, so a producer schema
change cannot silently leave the published returns-table lying.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import yaml

# `sportsdataverse.cfb` re-exports the `cfb_recruiting_projection` *function*,
# shadowing the submodule -- reach the module's constants through a
# fully-qualified sys.modules lookup.
import sportsdataverse.cfb.cfb_recruiting_projection  # noqa: F401

_proj_mod = sys.modules["sportsdataverse.cfb.cfb_recruiting_projection"]

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMAS = _ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml"
_RELEASES = _ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"

_POLARS_BY_NAME = {"Int64": pl.Int64, "Float64": pl.Float64, "String": pl.Utf8, "Utf8": pl.Utf8}


def _declared_schema() -> list[dict]:
    return yaml.safe_load(_SCHEMAS.read_text(encoding="utf-8"))["load_cfb_recruiting_proj"]


def _loader_entry() -> dict:
    loaders = yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]
    return next(ld for ld in loaders if ld["fn"] == "load_cfb_recruiting_proj")


def test_declared_schema_matches_the_producer_output_schema() -> None:
    """The loader's returns-table must match the projection's real output.

    Both column ORDER and dtype: the published parquet is written straight from
    that frame, so any divergence here is a docs lie, not a formatting nit.
    """
    produced = _proj_mod._PROJECTION_SCHEMA
    declared = _declared_schema()

    assert [c["name"] for c in declared] == list(produced.keys())
    for col in declared:
        assert _POLARS_BY_NAME[col["type"]] == produced[col["name"]], col["name"]


def test_loader_entry_points_at_the_published_tag_and_floor() -> None:
    entry = _loader_entry()

    assert entry["tag"] == "cfb_recruiting_proj"
    assert entry["base"] == "sdv_releases"
    # matches what cfb_model_publish's builder writes: cfb_recruiting_proj_{season}.parquet
    assert entry["url"] == "cfb_recruiting_proj/cfb_recruiting_proj_{season}.parquet"
    # cfbfastR-data player_stats starts 2014; returning production lags one
    # season and the as-of ridge needs one trainable prior season -> 2016.
    assert entry["min_season"] == 2016


def test_loader_is_exported() -> None:
    from sportsdataverse.cfb import load_cfb_recruiting_proj

    assert callable(load_cfb_recruiting_proj)
