"""The reference docs' "Valid URL" is the URL the documented example call requests.

Offline: each generated wrapper is called with its example args against a stub
``_get`` that records the URL and params instead of fetching.
"""

from __future__ import annotations

import importlib
from urllib.parse import urlencode

import pytest

from tools.codegen import generate, spec

_PARAMS = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
_CFG = spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
_LEAGUES = {lg.prefix: lg for lg in _CFG.leagues}
_FLAT = [stem for stem, _ in generate.FLAT_APIS if (generate.ENDPOINTS / f"{stem}.yaml").exists()]


def _espn_views(prefix: str):
    lg = _LEAGUES[prefix]
    apis = [spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", _PARAMS) for a in generate.ESPN_APIS]
    pkg = f"sportsdataverse.{lg.group}.{prefix}" if lg.group else f"sportsdataverse.{prefix}"
    return generate._espn_league_views(lg, apis, _CFG.hosts), f"{pkg}.{prefix}_espn_ext"


def _assert_valid_urls(monkeypatch, views, dotted: str, league: str = "") -> int:
    mod = importlib.import_module(dotted)
    seen: dict = {}

    def fake_get(url, params=None, **_kw):
        seen.update(url=url, params=params or {})
        return {}

    monkeypatch.setattr(mod, "_get", fake_get)
    checked = 0
    for v in views:
        kwargs = {"league": league} if league else {}
        kwargs.update(v.example_args)
        if v.parser:
            kwargs["return_parsed"] = False
        fn = getattr(mod, v.fn_name)
        if not v.valid_url:  # only when the example omits a required argument
            with pytest.raises(TypeError):
                fn(**kwargs)
            continue
        seen.clear()
        fn(**kwargs)
        query = {k: val for k, val in seen["params"].items() if val is not None}
        got = seen["url"] + (f"?{urlencode(query, doseq=True)}" if query else "")
        assert v.valid_url == got, v.fn_name
        assert f"{v.fn_name}(" in v.example_call and v.valid_url in v.docstring, v.fn_name
        checked += 1
    return checked


@pytest.mark.parametrize("prefix", sorted(_LEAGUES))
def test_espn_valid_url_is_what_the_example_requests(monkeypatch, prefix):
    views, dotted = _espn_views(prefix)
    lg = _LEAGUES[prefix]
    league = lg.league if lg.league_param else ""
    if league:  # the example call must carry the required ``league`` argument
        assert all(v.example_call.startswith(f"{v.fn_name}(league={league!r}") for v in views)
    assert _assert_valid_urls(monkeypatch, views, dotted, league) == len(views)


@pytest.mark.parametrize("stem", _FLAT)
def test_flat_valid_url_is_what_the_example_requests(monkeypatch, stem):
    prefix = dict(generate.FLAT_APIS)[stem]
    fa = spec.load_flat_api(generate.ENDPOINTS / f"{stem}.yaml", _PARAMS)
    dest = generate._flat_dest(prefix, fa.module).relative_to(generate.ROOT).with_suffix("")
    assert _assert_valid_urls(monkeypatch, generate._flat_views(fa, league_prefix=prefix), ".".join(dest.parts))


def test_core_child_resource_fills_every_path_token():
    views, _ = _espn_views("nba")
    by = {v.fn_name: v for v in views}
    base = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793"
    assert by["espn_nba_game_competition"].valid_url == base  # cid defaults to event_id
    assert by["espn_nba_game_odds"].valid_url == f"{base}/odds"
    assert by["espn_nba_game_team"].valid_url == f"{base}/competitors/4"


@pytest.mark.parametrize("prefix", sorted(p for p, lg in _LEAGUES.items() if "universal" in lg.scopes))
def test_summary_documents_a_dict_of_section_frames(prefix):
    views, _ = _espn_views(prefix)
    summary = [v for v in views if v.short == "summary"]
    assert summary, prefix
    for v in summary:
        assert v.parsed_doc_md == "a dict of `polars.DataFrame`s keyed by summary section", v.fn_name
        assert "keyed by summary section by default" in v.docstring, v.fn_name
