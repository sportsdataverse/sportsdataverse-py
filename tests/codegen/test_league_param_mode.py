"""Codegen league_param-mode tests (Phase 0 of ESPN soccer/cricket)."""

from __future__ import annotations

import tools.codegen.spec as spec


def test_league_defaults_league_param_false():
    lg = spec.League(prefix="nba", sport="basketball", league="nba", scopes=["universal"])
    assert lg.league_param is False


def test_load_leagues_reads_league_param_flag(tmp_path):
    yaml_text = (
        "hosts:\n"
        '  site_v2: "https://site.api.espn.com/apis/site/v2/sports"\n'
        '  site_v2_alt: "https://site.api.espn.com/apis/v2/sports"\n'
        '  web_v3: "https://site.web.api.espn.com/apis/common/v3/sports"\n'
        '  core_v2: "https://sports.core.api.espn.com/v2/sports"\n'
        "leagues:\n"
        "  - {prefix: soccer, sport: soccer, league: eng.1, league_param: true, scopes: [universal]}\n"
        "  - {prefix: epl, sport: soccer, league: eng.1, scopes: [universal]}\n"
    )
    p = tmp_path / "leagues.yaml"
    p.write_text(yaml_text, encoding="utf-8")
    cfg = spec.load_leagues(p)
    by_prefix = {lg.prefix: lg for lg in cfg.leagues}
    assert by_prefix["soccer"].league_param is True
    assert by_prefix["epl"].league_param is False  # defaults False when key absent


import tools.codegen.generate as gen


def _load_apis_and_view(prefix, sport, league, league_param, short):
    """Build one _EndpointView for the named endpoint short under a (param/fixed) league."""
    endpoints = gen.ENDPOINTS
    params = spec.load_parameters(endpoints / "parameters.yaml")
    apis = [spec.load_espn_api(endpoints / f"{a}.yaml", params) for a in gen.ESPN_APIS]
    lg = spec.League(prefix=prefix, sport=sport, league=league, scopes=["universal"], league_param=league_param)
    views = gen._espn_league_views(lg, apis, spec.load_leagues(endpoints / "leagues.yaml").hosts)
    return next(v for v in views if v.short == short)


def test_param_mode_scoreboard_url_keeps_runtime_league_token():
    v = _load_apis_and_view("soccer", "soccer", "eng.1", True, "scoreboard")
    # sport is baked, league stays a runtime f-string token, and it's an f-string literal.
    assert v.league_param is True
    assert "soccer/{_path_seg(league)}/scoreboard" in v.url_literal  # quoted at runtime (A-N2)
    assert v.url_literal.startswith('f"')


def test_fixed_mode_scoreboard_url_bakes_both_slugs():
    v = _load_apis_and_view("nba", "basketball", "nba", False, "scoreboard")
    assert v.league_param is False
    assert "basketball/nba/scoreboard" in v.url_literal
    assert "{league}" not in v.url_literal


def test_param_mode_module_source_has_leading_league_param():
    endpoints = gen.ENDPOINTS
    params = spec.load_parameters(endpoints / "parameters.yaml")
    apis = [spec.load_espn_api(endpoints / f"{a}.yaml", params) for a in gen.ESPN_APIS]
    hosts = spec.load_leagues(endpoints / "leagues.yaml").hosts
    lg = spec.League(prefix="soccer", sport="soccer", league="eng.1", scopes=["universal"], league_param=True)
    src = gen._league_module_source(lg, apis, hosts)
    # the generated scoreboard wrapper takes league as its FIRST parameter
    assert "def espn_soccer_scoreboard(" in src
    sig_start = src.index("def espn_soccer_scoreboard(")
    head = src[sig_start : sig_start + 200]
    assert "league: str," in head, head
    assert head.index("league: str,") < (head.index("dates") if "dates" in head else len(head))
    # the URL interpolates league at runtime
    assert "soccer/{_path_seg(league)}/scoreboard" in src
    # league must NOT be forwarded as a query param
    assert '"league": league' not in src


def test_soccer_module_routes_scoreboard_to_soccer_parser():
    endpoints = gen.ENDPOINTS
    params = spec.load_parameters(endpoints / "parameters.yaml")
    apis = [spec.load_espn_api(endpoints / f"{a}.yaml", params) for a in gen.ESPN_APIS]
    hosts = spec.load_leagues(endpoints / "leagues.yaml").hosts
    lg = spec.League(prefix="soccer", sport="soccer", league="eng.1", scopes=["universal"], league_param=True)
    src = gen._league_module_source(lg, apis, hosts)
    assert "return parse_soccer_scoreboard(raw" in src
    assert "return parse_soccer_summary(raw" in src


def test_nba_module_unaffected_by_soccer_override():
    endpoints = gen.ENDPOINTS
    params = spec.load_parameters(endpoints / "parameters.yaml")
    apis = [spec.load_espn_api(endpoints / f"{a}.yaml", params) for a in gen.ESPN_APIS]
    hosts = spec.load_leagues(endpoints / "leagues.yaml").hosts
    lg = spec.League(prefix="nba", sport="basketball", league="nba", scopes=["universal"])
    src = gen._league_module_source(lg, apis, hosts)
    assert "return parse_scoreboard(raw" in src
    assert "parse_soccer_scoreboard" not in src


def _real_league_views(prefix: str):
    endpoints = gen.ENDPOINTS
    params = spec.load_parameters(endpoints / "parameters.yaml")
    apis = [spec.load_espn_api(endpoints / f"{a}.yaml", params) for a in gen.ESPN_APIS]
    cfg = spec.load_leagues(endpoints / "leagues.yaml")
    lg = next(lg for lg in cfg.leagues if lg.prefix == prefix)
    return lg, apis, cfg.hosts, gen._espn_league_views(lg, apis, cfg.hosts)


def test_headline_soccer_league_documents_the_parser_it_calls():
    """A-M21: the override lands before the view, so the docstring and the returns table follow it.

    ``epl`` is one of the 12 headline soccer leagues: its prefix has no per-league schema, so before
    the fix its pages fell back to the generic cross-league tables (``boxscore_player``, ``plays``, ...).
    """
    lg, apis, hosts, views = _real_league_views("epl")
    views = {v.short: v for v in views}
    for short, parser in gen._SPORT_PARSER_OVERRIDES["soccer"].items():
        v = views[short]
        assert v.parser == parser
        assert f"parse the payload through {parser} ->" in v.docstring, short
        assert v.schema_league == "soccer"
    summary = views["summary"]
    assert summary.return_table == gen._return_table("summary", "soccer")
    assert "**lineups**" in summary.return_table and "**boxscore_player**" not in summary.return_table
    src = gen._league_module_source(lg, apis, hosts)
    assert "parse_scoreboard" not in src
    assert "return parse_soccer_scoreboard(raw" in src


def test_every_parsed_espn_wrapper_is_annotated_with_the_frame_union():
    """A-M22: a wrapper that returns a frame by default is not annotated ``-> Dict``.

    R858-4: one that returns a dict of frames (``summary``, the CDN game/pbp payloads) says so,
    as its docstring already does.
    """
    lg, apis, hosts, views = _real_league_views("nba")
    src = gen._league_module_source(lg, apis, hosts)
    assert all(v.parser for v in views)
    assert "-> Dict:" not in src
    dicts = [v for v in views if v.parsed_doc_md or v.returns_dict]
    assert "summary" in {v.short for v in dicts}
    assert src.count(") -> Union[Dict[str, pl.DataFrame], Dict[str, pd.DataFrame], Dict]:") == len(dicts)
    assert src.count(") -> Union[pl.DataFrame, pd.DataFrame, Dict]:") == len(views) - len(dicts)
    assert "\nimport polars as pl" in src and "\nimport pandas as pd" in src


def test_generated_wrapper_annotations_resolve(tmp_path):
    """R858-4: ``typing.get_type_hints`` resolves every generated wrapper's annotations.

    ``pl`` / ``pd`` used to be imported only under ``TYPE_CHECKING``, so evaluating an annotation
    (``get_type_hints``, pydantic, a docs tool) raised ``NameError: name 'pl' is not defined``.
    The ESPN module is rendered from the template here (so a template change is seen before any
    regeneration); the flat family is the committed ``cbs_napi``.
    """
    import importlib.util
    import typing

    import pandas as pd
    import polars as pl

    from sportsdataverse.cbs import cbs_napi
    from sportsdataverse.mbb import kenpom

    lg, apis, hosts, _ = _real_league_views("nba")
    path = tmp_path / "_rendered_nba_espn_ext.py"
    path.write_text(gen._league_module_source(lg, apis, hosts), encoding="utf-8")
    s = importlib.util.spec_from_file_location("_rendered_nba_espn_ext", path)
    nba = importlib.util.module_from_spec(s)
    s.loader.exec_module(nba)

    summary = typing.get_type_hints(nba.espn_nba_summary)["return"]
    assert typing.get_args(summary)[:2] == (typing.Dict[str, pl.DataFrame], typing.Dict[str, pd.DataFrame])
    roster = typing.get_type_hints(nba.espn_nba_team_roster)["return"]
    assert typing.get_args(roster)[:2] == (pl.DataFrame, pd.DataFrame)
    for mod in (nba, cbs_napi, kenpom):
        for name in mod.__all__:
            typing.get_type_hints(getattr(mod, name))

    # R891-2: the family-level ``parsed_doc`` (KenPom: one frame per HTML table) alone makes the parsed
    # return a dict of frames. ``kenpom_ratings`` also has a dict-of-frames schema; ``kenpom_win_probability``
    # has a single-table one, so only ``parsed_doc`` carries it.
    for name in ("kenpom_ratings", "kenpom_win_probability"):
        hint = typing.get_type_hints(getattr(kenpom, name))["return"]
        assert typing.get_args(hint)[:2] == (typing.Dict[str, pl.DataFrame], typing.Dict[str, pd.DataFrame]), name
    fa = spec.load_flat_api(gen.ENDPOINTS / "kenpom.yaml", spec.load_parameters(gen.ENDPOINTS / "parameters.yaml"))
    wp = next(v for v in gen._flat_views(fa, league_prefix="mbb") if v.fn_name == "kenpom_win_probability")
    assert wp.parsed_doc_md and not wp.returns_dict
    assert wp.parsed_annotation.startswith("Union[Dict[str, pl.DataFrame], Dict[str, pd.DataFrame], ")
