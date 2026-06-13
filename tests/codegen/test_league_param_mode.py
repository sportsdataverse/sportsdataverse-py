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
