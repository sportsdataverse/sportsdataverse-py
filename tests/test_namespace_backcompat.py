"""Back-compat: moved leagues remain importable at the old top-level name."""

from __future__ import annotations

import importlib
import warnings

import pytest

MOVED = {
    "epl": "soccer.epl",
    "laliga": "soccer.laliga",
    "mls": "soccer.mls",
    "ucl": "soccer.ucl",
    "ufl": "football.ufl",
    "cfl": "football.cfl",
    "mch": "hockey.mch",
    "ahl": "hockey.ahl",
    "college_baseball": "baseball.college_baseball",
}


@pytest.mark.parametrize("leaf,target", list(MOVED.items()))
def test_attribute_access_still_works_with_deprecation(leaf, target):
    sdv = importlib.import_module("sportsdataverse")
    # Evict any cached attr so __getattr__ fires (caching is correct production
    # behaviour — suppress repeat warnings after first access; tests must clear).
    sdv.__dict__.pop(leaf, None)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        mod = getattr(sdv, leaf)
        nested = importlib.import_module(f"sportsdataverse.{target}")
    assert mod is nested
    assert any(issubclass(x.category, DeprecationWarning) for x in w)


@pytest.mark.parametrize("leaf,target", list(MOVED.items()))
def test_import_statement_still_works(leaf, target):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        legacy = importlib.import_module(f"sportsdataverse.{leaf}")
        nested = importlib.import_module(f"sportsdataverse.{target}")
    assert legacy is nested


def test_moved_map_matches_grouped_leagues():
    import sportsdataverse
    import tools.codegen.spec as spec
    from pathlib import Path

    cfg = spec.load_leagues(Path("tools/codegen/endpoints/leagues.yaml"))
    grouped = {lg.prefix: f"{lg.group}.{lg.prefix}" for lg in cfg.leagues if lg.group}
    hand_moved = {"ahl": "hockey.ahl", "ohl": "hockey.ohl", "qmjhl": "hockey.qmjhl", "whl": "hockey.whl"}
    expected = {**grouped, **hand_moved}
    assert sportsdataverse._MOVED == expected, (
        f"_MOVED drifted from grouped leagues.yaml: "
        f"missing={set(expected) - set(sportsdataverse._MOVED)}, "
        f"extra={set(sportsdataverse._MOVED) - set(expected)}"
    )
