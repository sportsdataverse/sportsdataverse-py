"""Per-competition factories over FIBA, Genius LiveStats and Sportradar Atrium.

The facts pinned below are COPIED from sdv-internal-refs (fiba/README.md + captures, genius-livestats/inputs/
leagues.json, atrium-embed/SITES.md); the tests never read the refs checkout.
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.basketball import _competitions as C
from sportsdataverse.basketball._competitions_registry import REGISTRY
from sportsdataverse.basketball.atrium import atrium as atrium_api
from sportsdataverse.basketball.atrium import atrium_competitions_runtime as atrium_rt
from sportsdataverse.basketball.fiba_hub import fiba_competitions_runtime as fiba_rt
from sportsdataverse.basketball.fiba_hub import fiba_gamepage, fiba_gdap
from sportsdataverse.basketball.genius import genius as genius_api
from sportsdataverse.basketball.genius import genius_competitions_runtime as genius_rt
from sportsdataverse.errors import NoDataError

ROOT = Path(__file__).resolve().parents[2]

# ---- facts copied from the refs ------------------------------------------------------------------
# GDAP competitionMarketingNameId: 201/320/306/112/211 from fiba/README.md (id-space table); 352, 179 and 297
# from getGdapCompetitionById probes of Paris 2024 women (208723), AfroBasket 2025 (208938) and Women's
# EuroBasket 2025 (208854) on 2026-10-17.
FIBA_FAMILY_IDS = {
    "world_cup": 201, "womens_world_cup": 306, "olympics_men": 320, "olympics_women": 352,
    "bcl": 112, "euroleague_women": 211, "afrobasket": 179, "womens_eurobasket": 297,
}  # fmt: skip
FIBA_WOMEN = {"womens_world_cup", "olympics_women", "euroleague_women", "womens_eurobasket"}
# genius-livestats/inputs/leagues.json: registry key -> (ORG, gender).
GENIUS_ORGS = {
    "wbbl": ("WBBL", "women"), "slb": ("SLB", "men"), "bal": ("BAL", "men"), "nznbl": ("NZNBL", "men"),
    "esake": ("ESAKE", "men"), "lfb": ("FFBB", "women"), "lnbm": ("FRB", "men"), "lub": ("FUBB", "men"),
}  # fmt: skip
# atrium-embed/SITES.md websiteIds.
ATRIUM_SITES = {"lnb": 12, "lnbp": 14, "nbl1": 3, "lmb": 13, "lpb": 22}
# (org, season, label, comp_id) rows copied from leagues.json -> the competition the label rule must pick.
GENIUS_LABELS = {
    "wbbl": [(2018, "WBBL Championship 2017-18", 18386), (2018, "WBBL Cup 2017-18", 18387),
             (2018, "WBBL Play-Offs 2017-18", 20887), (2018, "Betty Codona WBBL Trophy 2017-18", 18393)],
    "slb": [(2026, "Championship 2025-26", 41897), (2026, "Trophy 2025-26", 42212), (2026, "Cup 2025-26", 47714),
            (2026, "Play-offs 2025-26", 48758)],
    "nznbl": [(2025, "Rapid League", 40144), (2025, "Sals NBL", 40145)],
    "esake": [(2021, "SUPER CUP", 28568), (2021, "super cup basket league", 28570), (2021, "STOIXIMAN BASKET LEAGUE", 28743),
              (2021, "Esake Woman", 29847)],
    "lfb": [(2019, "LIGUE FEMININE DE BASKET", 21849), (2019, "NATIONALE MASCULINE 1", 21850),
            (2019, "LIGUE FEMININE 2", 21851), (2019, "COUPE DE FRANCE ROBERT BUSNEL", 22217)],
    "lub": [(2026, "Liga Femenina 2025", 40794), (2026, "Liga Uruguaya de Basquetbol 2025/2026", 42104),
            (2027, "LUB 26/27", 48785), (2027, "Amistosos LUB 2026", 50123), (2027, "LFB 2026", 48456)],
}  # fmt: skip
GENIUS_PICK = {("wbbl", 2018): 18386, ("slb", 2026): 41897, ("nznbl", 2025): 40145, ("esake", 2021): 28743,
               ("lfb", 2019): 21849, ("lub", 2026): 42104, ("lub", 2027): 48785}  # fmt: skip

FIBA_SUFFIXES = {"editions", "schedule", "standings", "teams", "player_box", "team_box", "pbp", "shots"}
GENIUS_SUFFIXES = {"competitions", "schedule", "team_box", "player_box", "pbp", "shots"}
ATRIUM_SUFFIXES = {"seasons", "schedule", "standings", "teams", "player_box", "team_box", "pbp", "shots"}


def _by_key(platform):
    return {e["key"]: e for e in REGISTRY[platform]}


# ---- registry ------------------------------------------------------------------------------------


def test_registry_facts_match_the_refs():
    assert {k: e["family_id"] for k, e in _by_key("fiba").items()} == FIBA_FAMILY_IDS
    assert {k for k, e in _by_key("fiba").items() if e["gender"] == "women"} == FIBA_WOMEN
    assert {k: (e["org"], e["gender"]) for k, e in _by_key("genius").items()} == GENIUS_ORGS
    assert {k: e["website_id"] for k, e in _by_key("atrium").items()} == ATRIUM_SITES


def test_every_entry_has_the_documented_fields():
    common = {"key", "prefix", "label", "gender", "season_rule"}
    for platform, extra in (
        ("fiba", {"family_id"}),
        ("genius", {"org", "label_include", "label_exclude"}),
        ("atrium", {"website_id"}),
    ):
        for e in REGISTRY[platform]:
            assert set(e) == common | extra, e["key"]
            assert e["gender"] in {"men", "women", "both"}


def test_committed_registry_matches_the_yaml_source():
    spec = importlib.util.spec_from_file_location(
        "gen_basketball_competitions", ROOT / "tools/codegen/gen_basketball_competitions.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    assert mod.TARGET.read_text(encoding="utf-8") == mod.render(), "rerun tools/codegen/gen_basketball_competitions.py"


def test_label_rules_pick_the_headline_competition_of_real_labels():
    for key, rows in GENIUS_LABELS.items():
        e = _by_key("genius")[key]
        for season, label, comp_id in rows:
            frame = pl.DataFrame({"comp_id": [comp_id], "label": [label], "season": [season]})
            hit = frame.filter(
                pl.col("label").str.contains("(?i)" + e["label_include"])
                & ~pl.col("label").str.contains("(?i)" + e["label_exclude"])
            )
            assert (hit.height == 1) == (GENIUS_PICK[(key, season)] == comp_id), (key, label)


# ---- prefixes ------------------------------------------------------------------------------------


def _all_minted():
    return {**{n: f for e in REGISTRY["fiba"] for n, f in C.mint_fiba(e, fiba_rt.__name__).items()},
            **{n: f for e in REGISTRY["genius"] for n, f in C.mint_genius(e, genius_rt.__name__).items()},
            **{n: f for e in REGISTRY["atrium"] for n, f in C.mint_atrium(e, atrium_rt.__name__).items()}}  # fmt: skip


def test_prefixes_are_unique_and_do_not_clash_with_the_vocabulary():
    from tools.codegen import generate

    prefixes = [e["prefix"] for p in REGISTRY.values() for e in p]
    assert len(prefixes) == len(set(prefixes))
    vocabulary = set(generate._doc_leagues()) | {stem for stem, _ in generate.FLAT_APIS} | set(generate._NESTED_HOMES)
    import pkgutil

    import sportsdataverse

    vocabulary |= {m.name for m in pkgutil.iter_modules(sportsdataverse.__path__)}
    vocabulary |= {m.name for m in pkgutil.iter_modules(sportsdataverse.basketball.__path__)}
    assert not set(prefixes) & vocabulary, set(prefixes) & vocabulary


def test_minted_names_collide_with_no_existing_public_name():
    import sportsdataverse

    minted = _all_minted()
    expected = (len(REGISTRY["fiba"]) * len(FIBA_SUFFIXES) + len(REGISTRY["genius"]) * len(GENIUS_SUFFIXES)
                + len(REGISTRY["atrium"]) * len(ATRIUM_SUFFIXES))  # fmt: skip
    assert len(minted) == expected  # no two entries mint the same name
    base = set(genius_api.__all__) | set(atrium_api.__all__) | set(fiba_gdap.__all__) | set(fiba_gamepage.__all__)
    assert not set(minted) & base
    # the package namespace holds exactly one object per name (a clash would have been overwritten silently)
    for name in minted:
        assert getattr(sportsdataverse, name).__module__.endswith("_competitions_runtime"), name


# ---- minting -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("platform", "rt", "suffixes"),
    [("fiba", fiba_rt, FIBA_SUFFIXES), ("genius", genius_rt, GENIUS_SUFFIXES), ("atrium", atrium_rt, ATRIUM_SUFFIXES)],
)
def test_every_entry_mints_the_headline_set_into_the_runtime_module(platform, rt, suffixes):
    for e in REGISTRY[platform]:
        fam = rt.FAMILIES[e["key"]]
        assert set(fam) == {f"{e['prefix']}_{s}" for s in suffixes}
        for name, fn in fam.items():
            assert getattr(rt, name) is fn and name in rt.__all__
            assert fn.__name__ == fn.__qualname__ == name and fn.__module__ == rt.__name__


def test_the_homes_re_export_the_minted_names():
    import sportsdataverse.basketball.atrium as a
    import sportsdataverse.basketball.fiba_hub as f
    import sportsdataverse.basketball.genius as g

    assert f.fiba_wc_schedule is fiba_rt.fiba_wc_schedule and "fiba_wc_schedule" in f.__all__
    assert g.wbbl_schedule is genius_rt.wbbl_schedule and "wbbl_schedule" in g.__all__
    assert a.nbl1_schedule is atrium_rt.nbl1_schedule and "nbl1_schedule" in a.__all__


def test_signatures_bind_the_competition_and_keep_the_return_flags():
    sig = inspect.signature
    assert list(sig(fiba_rt.fiba_wc_schedule).parameters) == [
        "season",
        "comp",
        "return_parsed",
        "return_as_pandas",
        "kwargs",
    ]
    assert list(sig(fiba_rt.fiba_wc_pbp).parameters)[:3] == ["game_id", "season", "comp"]
    assert list(sig(genius_rt.wbbl_schedule).parameters)[:3] == ["season", "comp_id", "round_number"]
    assert list(sig(genius_rt.wbbl_pbp).parameters)[0] == "match_id"
    assert "org" not in sig(genius_rt.wbbl_schedule).parameters
    assert "website_id" not in sig(atrium_rt.lnb_schedule).parameters
    assert list(sig(atrium_rt.lnb_pbp).parameters)[:2] == ["fixture_id", "locale"]
    for fn in _all_minted().values():
        p = sig(fn).parameters
        assert p["return_parsed"].default is True and p["return_as_pandas"].default is False
        assert p["return_parsed"].kind is inspect.Parameter.KEYWORD_ONLY


def test_every_docstring_documents_args_returns_raises_example_and_names_the_competition():
    labels = {e["prefix"]: e for p in REGISTRY.values() for e in p}
    for name, fn in _all_minted().items():
        doc = inspect.getdoc(fn) or ""
        for header in ("Args:", "Returns:", "Raises:", "Example:", "Notes:"):
            assert header in doc, (name, header)
        prefix = next(p for p in sorted(labels, key=len, reverse=True) if name.startswith(p + "_"))
        entry = labels[prefix]
        assert entry["label"] in doc and entry["season_rule"] in doc, name
        assert "from sportsdataverse.basketball" in doc and f"import {name}" in doc, name
        assert ("wehoop mirror" in doc) == (entry["gender"] in {"women", "both"}), name


def test_base_family_notes_and_raises_are_inherited():
    doc = inspect.getdoc(genius_rt.wbbl_schedule) or ""
    assert "GENIUS_PROXY" in doc and "AssetFetchError" in doc
    assert "Authorized Users" in (inspect.getdoc(fiba_rt.fiba_wc_schedule) or "")
    assert "Unofficial" in (inspect.getdoc(atrium_rt.lnb_schedule) or "")


# ---- behaviour (bases monkeypatched; no network) ---------------------------------------------------


def _editions(rows):
    return pl.DataFrame({"competition_id": [r[0] for r in rows], "season": [r[1] for r in rows],
                         "start": [r[2] for r in rows]})  # fmt: skip


def test_fiba_resolves_the_edition_from_the_season(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        fiba_gdap,
        "fiba_gdap_editions",
        lambda **kw: seen.setdefault("ed", kw) and _editions([(208182, 2023, "2023-08-25")]),
    )
    monkeypatch.setattr(fiba_gdap, "fiba_gdap_schedule", lambda **kw: seen.setdefault("sched", kw) or "df")
    fiba_rt.fiba_wc_schedule(season=2023, return_as_pandas=True, timeout=5)
    assert seen["ed"] == {"family_id": 201, "season": 2023, "to_season": 2023}
    assert seen["sched"] == {"comp": 208182, "return_parsed": True, "return_as_pandas": True, "timeout": 5}


def test_fiba_comp_override_skips_the_lookup_and_game_functions_forward(monkeypatch):
    monkeypatch.setattr(fiba_gdap, "fiba_gdap_editions", lambda **kw: pytest.fail("lookup must be skipped"))
    seen = {}
    monkeypatch.setattr(fiba_gamepage, "fiba_gamepage_pbp", lambda **kw: seen.update(kw) or "pbp")
    assert fiba_rt.fiba_wc_pbp(100720, comp=208182) == "pbp"
    assert seen["game_id"] == 100720 and seen["comp"] == 208182


def test_fiba_default_season_is_the_newest_started_edition_and_ambiguity_raises(monkeypatch):
    monkeypatch.setattr(
        fiba_gdap,
        "fiba_gdap_editions",
        lambda **kw: _editions([(1, 2019, "2019-08-31"), (2, 2023, "2023-08-25"), (3, 2999, "2999-01-01")]),
    )
    assert C._fiba_comp(201, None, None, "WC") == 2
    monkeypatch.setattr(
        fiba_gdap, "fiba_gdap_editions", lambda **kw: _editions([(7, 2027, "2025-11-24"), (8, 2027, "2025-11-24")])
    )
    with pytest.raises(ValueError, match=r"\[7, 8\].*comp="):
        C._fiba_comp(201, 2027, None, "WC")
    monkeypatch.setattr(fiba_gdap, "fiba_gdap_editions", lambda **kw: _editions([]))
    with pytest.raises(NoDataError):
        C._fiba_comp(201, 1800, None, "WC")


def _genius_frame(key):
    rows = GENIUS_LABELS[key]
    return pl.DataFrame({"org": [""] * len(rows), "comp_id": [r[2] for r in rows], "label": [r[1] for r in rows],
                         "season": [r[0] for r in rows]})  # fmt: skip


@pytest.mark.parametrize(("key", "season"), sorted(GENIUS_PICK))
def test_genius_picks_the_headline_competition_from_the_org_chooser(monkeypatch, key, season):
    frame = _genius_frame(key)
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: frame)
    assert C._genius_comp(_by_key("genius")[key], season, None) == GENIUS_PICK[(key, season)]


def test_genius_schedule_binds_org_and_forwards(monkeypatch):
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: _genius_frame("wbbl"))
    seen = {}
    monkeypatch.setattr(genius_api, "genius_schedule", lambda **kw: seen.update(kw) or "df")
    assert genius_rt.wbbl_schedule(season=2018) == "df"
    assert seen["org"] == "WBBL" and seen["comp_id"] == 18386 and seen["round_number"] == -1
    seen.clear()
    genius_rt.wbbl_schedule(comp_id=1)
    assert seen["comp_id"] == 1
    seen.clear()
    monkeypatch.setattr(genius_api, "genius_pbp", lambda **kw: seen.update(kw) or "pbp")
    genius_rt.wbbl_pbp(2902690, return_parsed=False)
    assert seen == {"match_id": 2902690, "return_parsed": False, "return_as_pandas": False}


def test_genius_no_match_and_ambiguity(monkeypatch):
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: _genius_frame("wbbl"))
    with pytest.raises(NoDataError):
        genius_rt.wbbl_schedule(season=1999)
    two = pl.DataFrame({"comp_id": [1, 2], "label": ["Championship A", "Championship B"], "season": [2020, 2020]})
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: two)
    with pytest.raises(ValueError, match="comp_id="):
        genius_rt.wbbl_schedule(season=2020)


def test_atrium_binds_the_site_id(monkeypatch):
    seen = {}
    for base in ("atrium_fixtures", "atrium_standings", "atrium_teams", "atrium_seasons", "atrium_pbp"):
        monkeypatch.setattr(atrium_api, base, lambda _b=base, **kw: seen.setdefault(_b, kw) and "df")
    atrium_rt.nbl1_schedule(season=2026, competition_id="c-uuid")
    atrium_rt.lnb_standings(season_id="s-uuid", locale="fr-FR")
    atrium_rt.lnbp_seasons()
    atrium_rt.lnb_pbp("f-uuid")
    assert seen["atrium_fixtures"]["website_id"] == 3 and seen["atrium_fixtures"]["competition_id"] == "c-uuid"
    assert seen["atrium_standings"]["website_id"] == 12 and seen["atrium_standings"]["locale"] == "fr-FR"
    assert seen["atrium_seasons"]["website_id"] == 14
    assert seen["atrium_pbp"]["fixture_id"] == "f-uuid" and seen["atrium_pbp"]["website_id"] == 12
