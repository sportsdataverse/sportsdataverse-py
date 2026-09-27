"""Offline contract test for ``load_mlb_park_dimensions`` (tag ``mlb_parks``).

sdv-reference-data publishes one season-less venue x season table. This pins
the manifest entry and the asset the generated loader asks for, with no network.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import polars as pl
import yaml

from sportsdataverse.mlb import mlb_loaders

SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"


def test_manifest_entry_is_season_less():
    entries = yaml.safe_load((_CODEGEN / "endpoints" / "releases.yaml").read_text(encoding="utf-8"))["loaders"]
    (e,) = [e for e in entries if e["fn"] == "load_mlb_park_dimensions"]
    assert e["league"] == "mlb" and e["tag"] == "mlb_parks" and e["notes"]
    assert e["url"] == "mlb_parks/mlb_park_dimensions.parquet" and "min_season" not in e


def test_loader_requests_the_documented_asset(monkeypatch):
    seen: list[str] = []

    def fake(url):
        seen.append(url)
        return pl.DataFrame({"league": ["mlb"], "venue_id": ["3"]})

    monkeypatch.setattr(mlb_loaders, "_read_release_parquet", fake)
    fn = mlb_loaders.load_mlb_park_dimensions
    assert list(inspect.signature(fn).parameters) == ["return_as_pandas"]
    assert fn().height == 1
    assert seen == [SDV + "mlb_parks/mlb_park_dimensions.parquet"]


def test_every_declared_column_is_described():
    schema = yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))
    desc = yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))
    cols = [c["name"] for c in schema["load_mlb_park_dimensions"]]
    assert cols[:3] == ["league", "season", "venue_id"] and len(cols) == 20
    assert set(desc["load_mlb_park_dimensions"]) == set(cols)
