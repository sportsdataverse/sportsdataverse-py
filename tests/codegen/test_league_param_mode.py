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
    assert "soccer/{league}/scoreboard" in v.url_literal
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
    assert "soccer/{league}/scoreboard" in src
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
    assert "return parse_summary(raw" in src  # summary not overridden yet


def test_nba_module_unaffected_by_soccer_override():
    endpoints = gen.ENDPOINTS
    params = spec.load_parameters(endpoints / "parameters.yaml")
    apis = [spec.load_espn_api(endpoints / f"{a}.yaml", params) for a in gen.ESPN_APIS]
    hosts = spec.load_leagues(endpoints / "leagues.yaml").hosts
    lg = spec.League(prefix="nba", sport="basketball", league="nba", scopes=["universal"])
    src = gen._league_module_source(lg, apis, hosts)
    assert "return parse_scoreboard(raw" in src
    assert "parse_soccer_scoreboard" not in src
