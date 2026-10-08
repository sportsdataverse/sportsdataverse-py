"""releases.yaml ``on_missing`` must match what the sdv-py loader really does."""

from __future__ import annotations

import inspect

import pytest

import sportsdataverse._codegen_runtime as rt
import sportsdataverse.cfb as cfb
import sportsdataverse.nfl as nfl
import sportsdataverse.nfl.nfl_loaders as nl
from sportsdataverse.errors import NoDataError
from tools.codegen import generate, spec

_LOADERS = spec.load_releases(generate.ENDPOINTS / "releases.yaml").loaders
_RAISE = [ld for ld in _LOADERS if ld.on_missing == "raise"]


def _404(*_a, **_k):
    raise NoDataError("404")


def test_on_missing_values_and_scope():
    assert {ld.on_missing for ld in _LOADERS} == {"skip", "raise"}
    # Only hand-written (non-generated) leagues may declare raise; seasonless tables have no "missing season".
    assert all(ld.league not in generate._GENERATED_LOADER_LEAGUES for ld in _RAISE)
    assert all("{season" in ld.url for ld in _RAISE)
    nfl_seasonal = [ld.fn for ld in _LOADERS if ld.league == "nfl" and "{season" in ld.url]
    assert sorted(nfl_seasonal) == sorted(ld.fn for ld in _RAISE)


def test_invalid_value_rejected():
    with pytest.raises(ValueError):
        spec._on_missing({"fn": "x", "on_missing": "ignore"})


@pytest.mark.parametrize("ld", _RAISE, ids=lambda ld: ld.fn)
def test_raise_entries_raise_on_missing_season(ld, monkeypatch):
    monkeypatch.setattr(nl, "_fetch_release_parquet", _404)
    nfl.clear_cache()
    fn = getattr(nfl, ld.fn)
    assert "seasons" in inspect.signature(fn).parameters
    with pytest.raises(NoDataError):
        fn(seasons=[2024])


def test_generated_loaders_skip_missing_season(monkeypatch):
    monkeypatch.setattr(rt, "_fetch_release_parquet", _404)
    with pytest.warns(UserWarning, match=r"no data for season\(s\) \[2024\]"):
        assert getattr(cfb, "load_cfb_pbp")(seasons=[2024]).height == 0
