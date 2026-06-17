"""Codegen league-grouping (nesting) tests."""

from __future__ import annotations

import tools.codegen.generate as gen
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


def test_build_live_writes_grouped_league_nested(tmp_path, monkeypatch):
    live = tmp_path / "sportsdataverse"
    live.mkdir()
    monkeypatch.setattr(gen, "LIVE", live)
    src = "# GENERATED\n'''stub'''\n"
    monkeypatch.setattr(gen, "_render_all", lambda: {"epl_espn_ext.py": src})
    monkeypatch.setattr(
        gen.spec,
        "load_leagues",
        lambda _p: type(
            "C",
            (),
            {
                "leagues": [
                    spec.League(prefix="epl", sport="soccer", league="eng.1", scopes=["universal"], group="soccer")
                ]
            },
        )(),
    )
    gen.build_live()
    assert (live / "soccer" / "__init__.py").exists()  # container created
    # ext lands nested (content asserted by substring — build_live runs ruff format on it)
    assert "GENERATED" in (live / "soccer" / "epl" / "epl_espn_ext.py").read_text(encoding="utf-8")
    init = (live / "soccer" / "epl" / "__init__.py").read_text(encoding="utf-8")
    assert "from sportsdataverse.soccer.epl.epl_espn_ext import *" in init


def test_build_live_generates_populated_container_init(tmp_path, monkeypatch):
    live = tmp_path / "sportsdataverse"
    live.mkdir()
    monkeypatch.setattr(gen, "LIVE", live)
    monkeypatch.setattr(gen, "_render_all", lambda: {"ufl_espn_ext.py": "# GENERATED\n"})
    monkeypatch.setattr(
        gen.spec,
        "load_leagues",
        lambda _p: type(
            "C",
            (),
            {
                "leagues": [
                    spec.League(prefix="ufl", sport="football", league="ufl", scopes=["universal"], group="football")
                ]
            },
        )(),
    )
    gen.build_live()
    body = (live / "football" / "__init__.py").read_text(encoding="utf-8")
    assert "from sportsdataverse.football import ufl" in body  # NOT empty
