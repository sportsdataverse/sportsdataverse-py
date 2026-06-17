"""Codegen league-grouping (nesting) tests."""

from __future__ import annotations

import tools.codegen.spec as spec


def test_league_defaults_group_empty():
    lg = spec.League(prefix="nba", sport="basketball", league="nba", scopes=["universal"])
    assert lg.group == ""


def test_load_leagues_reads_group(tmp_path):
    yaml_text = (
        "hosts:\n"
        '  site_v2: "https://site.api.espn.com/apis/site/v2/sports"\n'
        '  site_v2_alt: "https://site.api.espn.com/apis/v2/sports"\n'
        '  web_v3: "https://site.web.api.espn.com/apis/common/v3/sports"\n'
        '  core_v2: "https://sports.core.api.espn.com/v2/sports"\n'
        "leagues:\n"
        "  - {prefix: nba, sport: basketball, league: nba, scopes: [universal]}\n"
        "  - {prefix: epl, sport: soccer, league: eng.1, scopes: [universal], group: soccer}\n"
    )
    p = tmp_path / "leagues.yaml"
    p.write_text(yaml_text, encoding="utf-8")
    by_prefix = {lg.prefix: lg for lg in spec.load_leagues(p).leagues}
    assert by_prefix["nba"].group == ""
    assert by_prefix["epl"].group == "soccer"
