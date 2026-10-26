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
    # Hand-wired HockeyTech leagues (not ESPN codegen leagues) live under hockey.*.
    hand_moved = {
        lg: f"hockey.{lg}"
        for lg in (
            "ahl",
            "ohl",
            "qmjhl",
            "whl",
            "echl",
            "sphl",
            "chl",
            "ushl",
            "bchl",
            "ajhl",
            "sjhl",
            "ojhl",
            "cchl",
            "gojhl",
            "mhl",
            "nojhl",
            "vijhl",
            "kijhl",
            "mjhl",
        )
    }
    # moved from the top level without a leagues.yaml group: a flat-API family, the cross-sport
    # providers, and the analytics modules
    hand_moved |= {
        "euroleague": "basketball.euroleague",
        **{p: f"providers.{p}" for p in ("cbs", "fox", "yahoo", "espn_content", "thesportsdb")},
        **{m: f"analytics.{m}" for m in ("rolling_windows", "metric_curves", "defense_vs_position", "paper_index")},
    }
    expected = {**grouped, **hand_moved}
    assert sportsdataverse._MOVED == expected, (
        f"_MOVED drifted from grouped leagues.yaml: "
        f"missing={set(expected) - set(sportsdataverse._MOVED)}, "
        f"extra={set(sportsdataverse._MOVED) - set(expected)}"
    )


@pytest.mark.parametrize(
    "old,new",
    [
        ("sportsdataverse.cbs.cbs_napi", "sportsdataverse.providers.cbs.cbs_napi"),
        ("sportsdataverse.euroleague.euroleague_parsers", "sportsdataverse.basketball.euroleague.euroleague_parsers"),
        ("sportsdataverse.nbl.nbl_espn_ext", "sportsdataverse.basketball.nbl.nbl_espn_ext"),
        ("sportsdataverse.mls.mls_api", "sportsdataverse.soccer.mls.mls_api"),
    ],
)
def test_a_deep_old_path_is_the_same_module_object(old, new):
    """A deep old import used to execute the module a second time, so a monkeypatch on one copy missed the other."""
    with pytest.warns(DeprecationWarning, match="will be removed in 0.4.0"):
        old_mod = importlib.import_module(old)
    assert old_mod is importlib.import_module(new)


@pytest.mark.parametrize("name", ["rolling_windows", "metric_curves", "defense_vs_position"])
def test_an_old_analytics_import_keeps_the_top_level_function(name):
    """sdv.rolling_windows is the FUNCTION; importing the old module path must not rebind it to a module."""
    import sportsdataverse

    with pytest.warns(DeprecationWarning, match=f"sportsdataverse.{name} is deprecated"):
        exec(f"from sportsdataverse.{name} import {name} as _x", {})
    assert callable(getattr(sportsdataverse, name))
