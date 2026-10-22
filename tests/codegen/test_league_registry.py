"""The league registry the docs site reads (sidebar, home grid, search contexts) covers every documented league."""

from __future__ import annotations

import json

from tools.codegen import generate


def test_every_documented_league_is_listed_once_under_a_known_sport():
    registry = json.loads(generate.render_leagues_json())
    assert [s["key"] for s in registry["sports"]] == ["football", "basketball", "hockey", "baseball", "soccer", "other"]
    prefixes = [lg["prefix"] for s in registry["sports"] for lg in s["leagues"]]
    assert sorted(prefixes) == sorted(generate._doc_leagues())
    assert len(prefixes) == len(set(prefixes))


def test_registry_sports_and_labels():
    registry = json.loads(generate.render_leagues_json())
    by_sport = {s["key"]: {lg["prefix"]: lg["label"] for lg in s["leagues"]} for s in registry["sports"]}
    assert by_sport["football"]["nfl"] == "NFL"
    assert by_sport["hockey"]["pwhl"] == "PWHL"
    assert "ahl" in by_sport["hockey"]
    assert by_sport["hockey"]["sihf"] == "SIHF (Swiss hockey)"
    # The European leagues are hockey leagues, never "other"; DEL keeps its prefix (package del_).
    assert {p: by_sport["hockey"][p] for p in generate._EURO_HOCKEY_HOMES} == {
        "del": "DEL",
        "icehl": "ICE Hockey League",
        "eihl": "EIHL",
        "metal": "Metal Ligaen",
        "elh": "Czech Extraliga",
        "liiga": "Liiga",
        "shl": "SHL",
        "ha": "HockeyAllsvenskan",
        "nl": "National League",
        "magnus": "Ligue Magnus",
        "khl": "KHL",
        "vhl": "VHL",
    }
    # The international basketball homes are basketball leagues, never "other".
    assert set(generate._BASKETBALL_HOMES) <= set(by_sport["basketball"])
    assert by_sport["basketball"]["lba"] == "Lega Basket Serie A"
    assert by_sport["basketball"]["plg"] == "P. LEAGUE+"
    assert by_sport["basketball"]["fiba_hub"] == "FIBA (digital API and game pages)"
    assert by_sport["soccer"]["laliga"] == "LaLiga"
    assert by_sport["other"] == {
        "euroleague": "EuroLeague",
        "f1": "Formula 1",
        "espn_content": "ESPN content (news)",
        "thesportsdb": "TheSportsDB",
        "cricket": "Cricket",
        "odds": "Betting odds",
        "cbs": "CBS Sports",
        "yahoo": "Yahoo Sports",
        "fox": "Fox Sports",
    }


def test_the_committed_registry_is_current():
    """The sidebar, home grid and search contexts read the committed file: a stale one hides a new league everywhere."""
    committed = (generate.ROOT / "docs" / "src" / "data" / "leagues.json").read_text(encoding="utf-8")
    assert committed.rstrip("\n") == generate.render_leagues_json().rstrip("\n")


def test_nested_homes_land_under_hockey_and_del_is_importable():
    """Every European family renders into hockey/<lg>/; DEL's package dodges the ``del`` keyword."""
    import importlib

    homes = dict(generate.FLAT_APIS)
    for stem in ("del_esports", "liiga", "nationalleague", "khl_rest", "khl_webcaster", "vhl"):
        dest = generate._flat_dest(homes[stem], stem).relative_to(generate.ROOT)
        assert dest.parts[:2] == ("sportsdataverse", "hockey"), dest
    assert generate._flat_dest("del", "del_esports").parent.name == "del_"
    assert generate._home_module("del") == "sportsdataverse.hockey.del_"
    hockey = generate._container_groups({})["hockey"]
    assert "del_" in hockey and "del" not in hockey  # ``from sportsdataverse.hockey import del`` cannot parse
    for home in generate._EURO_HOCKEY_HOMES:
        importlib.import_module(generate._home_module(home))


def test_basketball_homes_nest_under_basketball_and_register_every_stem():
    """Every international basketball family renders into basketball/<home>/ and has an importable, unique home."""
    import importlib

    homes = dict(generate.FLAT_APIS)
    stems = [s for s, h in homes.items() if h in generate._BASKETBALL_HOMES]
    assert len(stems) == 28 and set(homes[s] for s in stems) == set(generate._BASKETBALL_HOMES)
    for stem in stems:
        dest = generate._flat_dest(homes[stem], stem).relative_to(generate.ROOT)
        assert dest.parts[:3] == ("sportsdataverse", "basketball", homes[stem]), dest
        assert stem in generate._FLAT_API_DOC and stem in generate._NO_R_DICT_FAMILIES
    # a prefix names one home only: no clash with an ESPN league, a hockey home or any older home
    taken = {lg.prefix for lg in generate.spec.load_leagues(generate.ENDPOINTS / "leagues.yaml").leagues}
    taken |= set(generate._EURO_HOCKEY_HOMES) | set(generate._HOCKEYTECH_MODULE_LEAGUES)
    taken |= {
        p.name for p in (generate.ROOT / "sportsdataverse").iterdir() if p.is_dir()
    }  # top-level packages (ESPN fiba, nbl, ...)
    assert not taken & set(generate._BASKETBALL_HOMES)
    assert len(generate._BASKETBALL_HOMES) == len(set(generate._BASKETBALL_HOMES))
    basketball = generate._container_groups({})["basketball"]
    assert basketball == sorted(generate._BASKETBALL_HOMES)
    init = (generate.LIVE / "basketball" / "__init__.py").read_text(encoding="utf-8")
    assert init == generate._container_init_body("basketball", basketball, False)
    pkg = importlib.import_module("sportsdataverse.basketball")
    for home in generate._BASKETBALL_HOMES:
        assert hasattr(pkg, home) and importlib.import_module(f"sportsdataverse.basketball.{home}")


def test_every_flat_api_page_has_a_label_and_every_label_a_page():
    """A-L43: nflpro had no ``_FLAT_API_DOC`` entry (its page was titled "NFL — nflpro") while a dead
    ``pff`` key survived because a test read it. The renderer looks labels up by ``fa.module``."""
    params = generate.spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    modules = {
        generate.spec.load_flat_api(generate.ENDPOINTS / f"{stem}.yaml", params).module
        for stem, _ in generate.FLAT_APIS
        if (generate.ENDPOINTS / f"{stem}.yaml").exists()
    }
    assert sorted(modules - set(generate._FLAT_API_DOC)) == []
    assert sorted(set(generate._FLAT_API_DOC) - modules) == []


def test_hockeytech_module_leagues_are_the_registry_minus_pwhl():
    """A-L45: one codegen list, and it equals ``hockeytech._leagues.LEAGUES`` minus the top-level pwhl."""
    from sportsdataverse.hockeytech._leagues import LEAGUES

    assert generate._HOCKEYTECH_MODULE_LEAGUES == [lg for lg in LEAGUES if lg != "pwhl"]
    hockey = [lg for lg, pkg in generate._LEAGUE_R_PACKAGE.items() if pkg == "fastRhockey"]
    assert set(generate._HOCKEYTECH_MODULE_LEAGUES) <= set(hockey)


def test_no_generated_page_or_example_names_a_deprecated_import_path():
    """A-L42: four renderers built ``sportsdataverse.{prefix}`` instead of the league's home, so 21
    generated Examples imported (and 146 pages named) a path that warns. Every committed generated
    docs page and module names the home (``sportsdataverse.soccer.mls``), never the alias. The
    hand-authored pages (a tutorial explaining the back-compat aliases) are not the generator's."""
    import re

    from sportsdataverse import _MOVED

    alias = re.compile(r"\bsportsdataverse\.(" + "|".join(map(re.escape, _MOVED)) + r")\b")
    roots = generate._generated_docs_roots()
    files = [p for p in sorted(generate.DOCS.rglob("*.md")) if p.relative_to(generate.DOCS).parts[0] in roots]
    files += [
        p
        for p in (generate.ROOT / "sportsdataverse").rglob("*.py")
        if "GENERATED" in p.read_text(encoding="utf-8")[:400]
    ]
    hits = [
        f"{p.relative_to(generate.ROOT)}: {m.group(0)}"
        for p in files
        for m in alias.finditer(p.read_text(encoding="utf-8"))
    ]
    assert hits == [], f"{len(hits)} deprecated paths, e.g. {hits[:5]}"
    for prefix, home in _MOVED.items():
        assert generate._home_module(prefix) == f"sportsdataverse.{home}"
