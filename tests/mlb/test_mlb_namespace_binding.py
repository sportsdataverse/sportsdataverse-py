"""``sportsdataverse.mlb.mlb_schedule`` / ``mlb_teams`` name both an ESPN submodule and a Stats
API function; the package attribute is the function (B-L85)."""

from __future__ import annotations

import importlib
import types

import pytest


@pytest.mark.parametrize("name, espn_fn", [("mlb_schedule", "espn_mlb_schedule"), ("mlb_teams", "espn_mlb_teams")])
def test_package_attribute_is_the_stats_api_function(name, espn_fn):
    import sportsdataverse.mlb as mlb
    from sportsdataverse.mlb import mlb_api_extra

    assert getattr(mlb, name) is getattr(mlb_api_extra, name)
    module = importlib.import_module(f"sportsdataverse.mlb.{name}")
    assert isinstance(module, types.ModuleType)
    assert getattr(mlb, espn_fn) is getattr(module, espn_fn)
