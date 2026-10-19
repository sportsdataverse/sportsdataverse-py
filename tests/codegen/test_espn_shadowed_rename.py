"""B-M31 rename: a generated ESPN wrapper whose package-level name a hand-written function holds.

In 7 leagues ``sportsdataverse.<lg>.espn_<lg>_calendar`` (and wbb/wnba standings, team roster, wnba
draft) runs a hand-written function, so the generated site-v2 wrapper of the same name was reachable
only as ``sportsdataverse.<lg>.<lg>_espn_ext.espn_<lg>_<short>``. The generator's collision rule
(``_ESPN_COLLISION_VERSIONED``, the one behind ``player_stats_v3``) now names those 12 wrappers
``*_site``; the old ext-module import still works for one release and warns.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import sys

import pytest

from tools.codegen import generate

SITE = "https://site.api.espn.com/apis/site/v2/sports"
STANDINGS = "https://site.api.espn.com/apis/v2/sports"  # the standings route has no /site segment

#: (league prefix, old name, new name, URL the wrapper requests). The URLs are the ones the old
#: names requested on the base (``*_espn_ext.py`` at 11391aba24).
RENAMED = [
    ("nba", "espn_nba_calendar", "espn_nba_calendar_site", f"{SITE}/basketball/nba/calendar"),
    ("wnba", "espn_wnba_calendar", "espn_wnba_calendar_site", f"{SITE}/basketball/wnba/calendar"),
    ("wnba", "espn_wnba_draft", "espn_wnba_draft_site", f"{SITE}/basketball/wnba/draft"),
    ("wnba", "espn_wnba_team_roster", "espn_wnba_team_roster_site", f"{SITE}/basketball/wnba/teams/1/roster"),
    ("wnba", "espn_wnba_standings", "espn_wnba_standings_site", f"{STANDINGS}/basketball/wnba/standings"),
    ("mbb", "espn_mbb_calendar", "espn_mbb_calendar_site", f"{SITE}/basketball/mens-college-basketball/calendar"),
    ("wbb", "espn_wbb_calendar", "espn_wbb_calendar_site", f"{SITE}/basketball/womens-college-basketball/calendar"),
    (
        "wbb",
        "espn_wbb_team_roster",
        "espn_wbb_team_roster_site",
        f"{SITE}/basketball/womens-college-basketball/teams/1/roster",
    ),
    (
        "wbb",
        "espn_wbb_standings",
        "espn_wbb_standings_site",
        f"{STANDINGS}/basketball/womens-college-basketball/standings",
    ),
    ("cfb", "espn_cfb_calendar", "espn_cfb_calendar_site", f"{SITE}/football/college-football/calendar"),
    ("nfl", "espn_nfl_calendar", "espn_nfl_calendar_site", f"{SITE}/football/nfl/calendar"),
    ("mlb", "espn_mlb_calendar", "espn_mlb_calendar_site", f"{SITE}/baseball/mlb/calendar"),
]
_IDS = [old for _, old, _, _ in RENAMED]


def _ext(prefix: str):
    return importlib.import_module(f"sportsdataverse.{prefix}.{prefix}_espn_ext")


def _espn_inputs():
    cfg = generate.spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    params = generate.spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    apis = [generate.spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    return cfg, apis


@pytest.mark.parametrize(("prefix", "old", "new", "url"), RENAMED, ids=_IDS)
def test_the_public_name_still_runs_the_hand_written_function(prefix, old, new, url):
    fn = getattr(importlib.import_module(f"sportsdataverse.{prefix}"), old)
    assert not fn.__module__.endswith("_espn_ext"), f"{old} should be the hand-written function"


@pytest.mark.parametrize(("prefix", "old", "new", "url"), RENAMED, ids=_IDS)
def test_the_generated_wrapper_is_public_under_its_new_name_and_requests_the_same_url(
    prefix, old, new, url, monkeypatch
):
    ext = _ext(prefix)
    fn = getattr(importlib.import_module(f"sportsdataverse.{prefix}"), new)
    assert fn is getattr(ext, new) and new in ext.__all__ and old not in ext.__all__
    seen = []
    monkeypatch.setattr(ext, "_get", lambda u, **kw: seen.append(u) or {})
    kwargs = {"team_id": 1} if "team_id" in inspect.signature(fn).parameters else {}
    fn(return_parsed=False, **kwargs)
    assert seen == [url]


@pytest.mark.parametrize(("prefix", "old", "new", "url"), RENAMED, ids=_IDS)
def test_the_old_ext_import_warns_and_returns_the_renamed_wrapper(prefix, old, new, url):
    ext = _ext(prefix)
    with pytest.warns(DeprecationWarning, match=rf"{old} is deprecated .*use {new}"):
        got = getattr(ext, old)
    assert got is getattr(ext, new)
    with pytest.warns(DeprecationWarning, match=new):
        exec(f"from sportsdataverse.{prefix}.{prefix}_espn_ext import {old} as _x", {})


def test_an_unknown_ext_attribute_still_raises_attribute_error():
    with pytest.raises(AttributeError, match="espn_cfb_nope"):
        _ext("cfb").espn_cfb_nope  # noqa: B018


def test_the_generator_renames_exactly_the_twelve_shadowed_views():
    """Only views whose package name a hand-written function holds are renamed; the 32 leagues
    whose generated calendar is the only ``espn_<lg>_calendar`` keep the bare name (33: every league
    but the seven)."""
    cfg, apis = _espn_inputs()
    renamed = {}
    kept_calendar = []
    for lg in cfg.leagues:
        for v in generate._espn_league_views(lg, apis, cfg.hosts):
            if v.renamed_from:
                renamed[v.renamed_from] = v.fn_name
            if v.fn_name == f"espn_{lg.prefix}_calendar":
                kept_calendar.append(lg.prefix)
    assert renamed == {old: new for _, old, new, _ in RENAMED}
    assert len(kept_calendar) == 33 and "nhl" in kept_calendar and "cfb" not in kept_calendar


def test_a_freshly_rendered_ext_module_carries_the_deprecation_shim(espn_ext_dir):
    """The shim comes from the template, so it is checked on a module rendered by this generator."""
    path = espn_ext_dir / "wnba_espn_ext.py"
    spec = importlib.util.spec_from_file_location("_fresh_wnba_espn_ext", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
        assert mod._RENAMED == {old: new for p, old, new, _ in RENAMED if p == "wnba"}
        with pytest.warns(DeprecationWarning, match="use espn_wnba_draft_site"):
            assert mod.espn_wnba_draft is mod.espn_wnba_draft_site
        with pytest.raises(AttributeError):
            mod.espn_wnba_nope  # noqa: B018
    finally:
        del sys.modules[spec.name]
    # a league with no shadowed view gets no shim at all
    assert "_RENAMED" not in (espn_ext_dir / "nhl_espn_ext.py").read_text(encoding="utf-8")


def test_no_ext_module_outside_the_seven_leagues_has_a_shim():
    cfg, _ = _espn_inputs()
    with_shim = set()
    for lg in cfg.leagues:
        mod = importlib.import_module(f"sportsdataverse.{generate._league_module(lg.prefix)}.{lg.prefix}_espn_ext")
        if "_RENAMED" in vars(mod):
            with_shim.add(lg.prefix)
    assert with_shim == {p for p, _, _, _ in RENAMED}
