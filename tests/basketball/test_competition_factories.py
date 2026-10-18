"""Per-competition factories over FIBA, Genius LiveStats and Sportradar Atrium.

The facts pinned below are COPIED from sdv-internal-refs (fiba/README.md + captures, genius-livestats/inputs/
leagues.json, atrium-embed/SITES.md); the tests never read the refs checkout.
"""

from __future__ import annotations

import gzip
import importlib.util
import inspect
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl
import pytest

from sportsdataverse.basketball import _competitions as C
from sportsdataverse.basketball._competitions_registry import REGISTRY
from sportsdataverse.basketball.atrium import atrium as atrium_api
from sportsdataverse.basketball.atrium import atrium_competitions_runtime as atrium_rt
from sportsdataverse.basketball.atrium import atrium_runtime as atrium_wire
from sportsdataverse.basketball.fiba_hub import fiba_competitions_runtime as fiba_rt
from sportsdataverse.basketball.fiba_hub import fiba_gamepage, fiba_gdap
from sportsdataverse.basketball.fiba_hub import fiba_runtime as fiba_wire
from sportsdataverse.basketball.genius import genius as genius_api
from sportsdataverse.basketball.genius import genius_competitions_runtime as genius_rt
from sportsdataverse.basketball.genius import genius_runtime as genius_wire
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).resolve().parents[2]

# ---- facts copied from the refs ------------------------------------------------------------------
# GDAP competitionMarketingNameId: 201/320/306/112/211 from fiba/README.md (id-space table); 352, 179 and 297
# from getGdapCompetitionById probes of Paris 2024 women (208723), AfroBasket 2025 (208938) and Women's
# EuroBasket 2025 (208854) on 2026-10-17; the rest from getGdapCompetitionById probes of the 2025 editions on
# 2026-10-18 (EuroBasket 208853, AmeriCup 208811, Asia Cup 208895, EuroCup Women 208856, BCL Americas 208898,
# Women's AmeriCup 208812, Women's Asia Cup Division A 208896, Women's AfroBasket 208937, Intercontinental Cup 208979).
FIBA_FAMILY_IDS = {
    "world_cup": 201, "womens_world_cup": 306, "olympics_men": 320, "olympics_women": 352,
    "bcl": 112, "euroleague_women": 211, "afrobasket": 179, "womens_eurobasket": 297,
    "eurobasket": 208, "americup": 184, "asia_cup": 195, "eurocup_women": 165, "bcl_americas": 110,
    "womens_americup": 303, "womens_asia_cup": 305, "womens_afrobasket": 302, "intercontinental_cup": 215,
}  # fmt: skip
FIBA_WOMEN = {"womens_world_cup", "olympics_women", "euroleague_women", "womens_eurobasket", "eurocup_women",
              "womens_americup", "womens_asia_cup", "womens_afrobasket"}  # fmt: skip
# genius-livestats/inputs/leagues.json: registry key -> (ORG, gender), and the LiveStats ORG of its /u/ shells.
GENIUS_ORGS = {
    "wbbl": ("WBBL", "women"), "slb": ("SLB", "men"), "bal": ("BAL", "men"), "nznbl": ("NZNBL", "men"),
    "esake": ("ESAKE", "men"), "lfb": ("FFBB", "women"), "lnbm": ("FRB", "men"), "lub": ("FUBB", "men"),
}  # fmt: skip
GENIUS_LIVE_ORGS = {**{k: org for k, (org, _g) in GENIUS_ORGS.items()}, "nznbl": "NZN"}
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


@pytest.fixture(autouse=True)
def _fresh_listing_cache():
    C._CACHE.clear()
    yield
    C._CACHE.clear()


def test_registry_facts_match_the_refs():
    assert {k: e["family_id"] for k, e in _by_key("fiba").items()} == FIBA_FAMILY_IDS
    assert {k for k, e in _by_key("fiba").items() if e["gender"] == "women"} == FIBA_WOMEN
    assert {k: (e["org"], e["gender"]) for k, e in _by_key("genius").items()} == GENIUS_ORGS
    assert {k: e["website_id"] for k, e in _by_key("atrium").items()} == ATRIUM_SITES


def test_every_entry_has_the_documented_fields():
    common = {"key", "prefix", "label", "gender", "season_rule"}
    for platform, extra in (
        ("fiba", {"family_id", "example_season"}),
        ("genius", {"org", "live_org", "label_include", "label_exclude"}),
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
        assert (
            "from sportsdataverse.basketball" in doc and "import " in doc.split("Example:")[1] and f"{name}(" in doc
        ), name
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
    assert seen["ed"] == {
        "family_id": 201,
        "season": 2023,
        "to_season": 2023,
        "timeout": 5,
    }  # transport kwargs forwarded
    assert seen["sched"] == {"comp": 208182, "return_parsed": True, "return_as_pandas": True, "timeout": 5}


def test_fiba_comp_override_skips_the_lookup_and_game_functions_forward(monkeypatch, gdap_competition):
    monkeypatch.setattr(fiba_gdap, "fiba_gdap_editions", lambda **kw: pytest.fail("lookup must be skipped"))
    seen = {}
    monkeypatch.setattr(fiba_gamepage, "fiba_gamepage_pbp", lambda **kw: seen.update(kw) or "pbp")
    assert fiba_rt.fiba_wc_pbp(100720, comp=208182) == "pbp"
    assert seen["game_id"] == 100720 and seen["comp"] == 208182


def test_fiba_game_function_needs_season_or_comp(monkeypatch):
    monkeypatch.setattr(fiba_gdap, "fiba_gdap_editions", lambda **kw: pytest.fail("no lookup before the check"))
    monkeypatch.setattr(fiba_gamepage, "fiba_gamepage_pbp", lambda **kw: pytest.fail("no fetch"))
    with pytest.raises(ValueError, match="season"):
        fiba_rt.fiba_wc_pbp(100720)


def test_registry_renders_python_literals_for_yaml_booleans_and_nulls(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "gen_basketball_competitions_lit", ROOT / "tools/codegen/gen_basketball_competitions.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod.yaml, "safe_load", lambda _t: {"genius": [{"org": False, "label_exclude": None, "x": 1}]})
    ns: dict = {}
    exec(mod.render(), ns)  # json.dumps would emit false / null -> NameError
    assert ns["REGISTRY"] == {"genius": [{"org": False, "label_exclude": None, "x": 1}]}


def test_every_fiba_example_season_is_a_real_edition_year():
    # the 2025 editions were listed (one per family and season) by getgdapcompetitionsbyseasonsandcompetitionmarketingname
    # on 2026-10-18, except the Women's AmeriCup and the Intercontinental Cup (getGdapCompetitionById only)
    years = {"fiba_oly": 2024, "fiba_woly": 2024, "fiba_wwc": 2022, "fiba_wc": 2023, "fiba_eurobasket": 2025,
             "fiba_americup": 2025, "fiba_asiacup": 2025, "fiba_ecw": 2025, "fiba_bcla": 2025, "fiba_wamericup": 2025,
             "fiba_wasiacup": 2025, "fiba_wafrobasket": 2025, "fiba_icup": 2025}  # fmt: skip
    got = {e["prefix"]: e["example_season"] for e in REGISTRY["fiba"]}
    assert {k: got[k] for k in years} == years


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
    shell = {"match_id": 2902690, "comp_id": 18386, "competition_name": "WBBL Championship 2017-18"}
    monkeypatch.setattr(genius_api, "genius_match_competition", lambda **kw: shell)
    genius_rt.wbbl_pbp(2902690, return_parsed=False)
    assert seen == {"match_id": 2902690, "return_parsed": False, "return_as_pandas": False}


def test_genius_no_match_and_ambiguity(monkeypatch):
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: _genius_frame("wbbl"))
    with pytest.raises(NoDataError):
        genius_rt.wbbl_schedule(season=1999)
    C._CACHE.clear()
    two = pl.DataFrame({"comp_id": [1, 2], "label": ["Championship A", "Championship B"], "season": [2020, 2020]})
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: two)
    with pytest.raises(ValueError, match="comp_id="):
        genius_rt.wbbl_schedule(season=2020)


def test_atrium_binds_the_site_id(monkeypatch):
    seen = {}
    for base in ("atrium_fixtures", "atrium_standings", "atrium_teams", "atrium_seasons"):
        monkeypatch.setattr(atrium_api, base, lambda _b=base, **kw: seen.setdefault(_b, kw) and "df")
    game = pl.DataFrame({"season_id": ["s-uuid"]})
    monkeypatch.setattr(atrium_api, "atrium_pbp", lambda **kw: seen.setdefault("atrium_pbp", kw) is not None and game)
    atrium_rt.nbl1_schedule(season=2026, competition_id="c-uuid")
    atrium_rt.lnb_standings(season_id="s-uuid", locale="fr-FR")
    atrium_rt.lnbp_seasons()
    atrium_rt.lnb_pbp("f-uuid")
    assert seen["atrium_fixtures"]["website_id"] == 3 and seen["atrium_fixtures"]["competition_id"] == "c-uuid"
    assert seen["atrium_standings"]["website_id"] == 12 and seen["atrium_standings"]["locale"] == "fr-FR"
    assert seen["atrium_seasons"]["website_id"] == 14
    assert seen["atrium_pbp"]["fixture_id"] == "f-uuid" and seen["atrium_pbp"]["website_id"] == 12


def test_listings_are_fetched_once_per_family_and_season(monkeypatch):
    calls = []
    monkeypatch.setattr(
        fiba_gdap, "fiba_gdap_editions", lambda **kw: calls.append(kw) or _editions([(208182, 2023, "2023-08-25")])
    )
    assert C._fiba_comp(201, 2023, None, "WC") == C._fiba_comp(201, 2023, None, "WC") == 208182
    assert len(calls) == 1
    frame = _genius_frame("wbbl")
    gcalls = []
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: gcalls.append(kw) or frame)
    for _ in range(2):
        C._genius_comp(_by_key("genius")["wbbl"], next(sn for (k, sn) in GENIUS_PICK if k == "wbbl"), None, timeout=7)
    assert gcalls == [{"org": "WBBL", "timeout": 7}]


def test_null_ids_are_dropped_and_an_all_null_listing_is_no_data(monkeypatch):
    monkeypatch.setattr(
        fiba_gdap,
        "fiba_gdap_editions",
        lambda **kw: pl.DataFrame({"competition_id": [None], "season": [2023], "start": ["2023-08-25"]}),
    )
    with pytest.raises(NoDataError):
        C._fiba_comp(201, 2023, None, "WC")
    frame = _genius_frame("wbbl").with_columns(comp_id=pl.lit(None, dtype=pl.Int64))
    monkeypatch.setattr(genius_api, "genius_competitions", lambda **kw: frame)
    with pytest.raises(NoDataError):
        C._genius_comp(_by_key("genius")["wbbl"], 2026, None)


# ---- a game id must belong to the bound competition (committed bodies, mocked transports) --------------

GFX = ROOT / "tests" / "fixtures" / "genius"
AFX = ROOT / "tests" / "fixtures" / "atrium"
ACCESS_DENIED = "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
U_ESAKE = "/u/ESAKE/2902690/"


class _Resp:
    def __init__(self, status, text):
        self.status_code, self.text, self.headers, self.reason, self.url = status, text, {}, "x", "https://x.invalid"


@pytest.fixture
def genius_pages(monkeypatch):
    """Serve committed Genius bodies by URL path; any other path is S3 AccessDenied (no such object)."""
    with gzip.open(GFX / "full" / "data_2902690.json.gz", "rt", encoding="utf-8") as fh:
        data = fh.read()
    pages = {"/data/2902690/data.json": data, U_ESAKE: (GFX / "html" / "u_ESAKE_2902690.html").read_text("utf-8")}
    calls = []

    def fake(url, **kw):
        path = urlsplit(url).path
        calls.append(path)
        return _Resp(200, pages[path]) if path in pages else _Resp(403, ACCESS_DENIED)

    monkeypatch.setattr(genius_wire, "download", fake)
    monkeypatch.delenv(genius_wire.PROXY_ENV, raising=False)
    return pages, calls


def test_registry_live_orgs_match_the_refs():
    assert {k: e["live_org"] for k, e in _by_key("genius").items()} == GENIUS_LIVE_ORGS


def test_genius_game_of_the_competition_passes_with_one_shell_fetch(genius_pages):
    _, calls = genius_pages
    assert genius_rt.esake_pbp(2902690).height > 0
    assert genius_rt.esake_team_box(2902690).height > 0
    assert genius_rt.esake_shots(2902690, return_parsed=False)["_request"] == {"match_id": 2902690}
    assert calls.count("/data/2902690/data.json") == 3 and calls.count(U_ESAKE) == 1  # the shell is cached


def test_genius_game_of_another_org_is_refused(genius_pages):
    with pytest.raises(AssetFetchError, match=r"not a Super League Basketball \(UK\) game.*/u/SLB/2902690/"):
        genius_rt.slb_pbp(2902690)


def test_genius_game_of_another_competition_of_the_org_is_refused(genius_pages):
    pages, _ = genius_pages
    pages[U_ESAKE] = pages[U_ESAKE].replace("Stoiximan GBL 2026-2027", "SUPER CUP 2026")
    with pytest.raises(AssetFetchError, match=r"'SUPER CUP 2026' \(comp_id 49915\).*genius_player_box"):
        genius_rt.esake_player_box(2902690)


def test_genius_unknown_match_is_no_data_before_any_shell(genius_pages):
    _, calls = genius_pages
    with pytest.raises(NoDataError):
        genius_rt.esake_pbp(1)
    assert calls == ["/data/1/data.json"]


def test_genius_shell_is_read_under_the_livestats_org(monkeypatch):
    seen = []
    shell = {"match_id": 2785478, "comp_id": 47827, "competition_name": "2026 Sal's National Basketball League"}
    monkeypatch.setattr(genius_api, "genius_pbp", lambda **kw: "pbp")
    monkeypatch.setattr(genius_api, "genius_match_competition", lambda **kw: seen.append(kw) or shell)
    assert genius_rt.nznbl_pbp(2785478) == "pbp"
    assert seen == [{"org": "NZN", "match_id": 2785478, "return_parsed": False}]


@pytest.fixture
def gdap_competition(monkeypatch):
    """getGdapCompetitionById answered from the committed World Cup 2023 body (family 201)."""
    body = json.loads((ROOT / "tests/fixtures/fiba_gdap/gdap__getGdapCompetitionById__wc2023.json").read_text("utf-8"))
    calls = []
    monkeypatch.setattr(fiba_wire, "_gdap", lambda url, params, **kw: calls.append(params) or body)
    fiba_wire._competition_family.cache_clear()
    yield calls
    fiba_wire._competition_family.cache_clear()


def test_fiba_comp_override_must_be_an_edition_of_the_family(monkeypatch, gdap_competition):
    pages = []
    monkeypatch.setattr(fiba_gamepage, "fiba_gamepage_pbp", lambda **kw: pages.append(kw) or "pbp")
    monkeypatch.setattr(fiba_gdap, "fiba_gdap_schedule", lambda **kw: pages.append(kw) or "sched")
    assert fiba_rt.fiba_wc_pbp(100720, comp=208182) == "pbp"
    with pytest.raises(AssetFetchError, match=r"208182 is an edition of family 201.*112"):
        fiba_rt.fiba_bcl_pbp(100720, comp=208182)
    with pytest.raises(AssetFetchError, match="family 201"):
        fiba_rt.fiba_bcl_schedule(comp=208182)
    assert len(pages) == 1 and gdap_competition == [{"gdapCompetitionId": 208182}]  # one cached lookup


@pytest.mark.parametrize(
    "call, said",
    [
        (lambda: fiba_rt.fiba_wc_schedule(comp=208182), "call the fiba_gdap_\\* wrapper with comp=208182"),
        (lambda: fiba_rt.fiba_wc_pbp(100720, comp=208182), "game page URL cannot be built either"),
    ],
)
def test_fiba_comp_without_a_gdap_family_names_the_real_reason_per_caller(monkeypatch, call, said):
    body = json.loads((ROOT / "tests/fixtures/fiba_gdap/gdap__getGdapCompetitionById__wc2023.json").read_text("utf-8"))
    body.pop("competitionMarketingName", None)
    monkeypatch.setattr(fiba_wire, "_gdap", lambda url, params, **kw: body)
    monkeypatch.setattr(fiba_gamepage, "fiba_gamepage_pbp", lambda **kw: pytest.fail("no page"))
    monkeypatch.setattr(fiba_gdap, "fiba_gdap_schedule", lambda **kw: pytest.fail("no schedule"))
    fiba_wire._competition_family.cache_clear()
    try:
        with pytest.raises(AssetFetchError, match=f"208182: GDAP gives it no competition family.*{said}"):
            call()
    finally:
        fiba_wire._competition_family.cache_clear()


def test_fiba_game_functions_document_season_as_required_unless_comp():
    game, sched = fiba_rt.fiba_wc_pbp.__doc__, fiba_rt.fiba_wc_schedule.__doc__
    assert "Required unless ``comp`` is given" in game and "Omitted = the newest edition" not in game
    assert "Omitted = the newest edition that has started" in sched
    with pytest.raises(ValueError, match=r"event year \(World Cup 2023 = 2023\)"):
        fiba_rt.fiba_wc_pbp(100720)


def test_genius_shell_naming_no_competition_is_its_own_error(monkeypatch):
    monkeypatch.setattr(genius_api, "genius_pbp", lambda **kw: "pbp")
    monkeypatch.setattr(genius_api, "genius_match_competition", lambda **kw: {"match_id": 1, "competition_name": None})
    with pytest.raises(AssetFetchError, match=r"names no competition.*genius_pbp\(match_id=") as err:
        genius_rt.esake_pbp(1)
    assert "None" not in str(err.value)


def test_genius_scope_check_retries_once_then_says_it_could_not_run(monkeypatch):
    shell = {"match_id": 2902690, "comp_id": 49914, "competition_name": "Stoiximan GBL 2026-2027"}
    answers = [AssetFetchError("HTTP 503"), shell]

    def flaky(**kw):
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    monkeypatch.setattr(genius_api, "genius_pbp", lambda **kw: "pbp")
    monkeypatch.setattr(genius_api, "genius_match_competition", flaky)
    assert genius_rt.esake_pbp(2902690) == "pbp"  # one transient failure is retried
    C._CACHE.clear()
    answers[:] = [AssetFetchError("HTTP 503"), AssetFetchError("HTTP 503")]
    with pytest.raises(AssetFetchError, match=r"scope check could not run.*failed twice.*not returned unchecked"):
        genius_rt.esake_pbp(2902690)


def test_the_listing_cache_is_bounded_least_recently_used_first(monkeypatch):
    monkeypatch.setattr(C, "_CACHE_MAX", 3)
    for k in range(3):
        C._cached(("k", k), lambda k=k: k)
    assert C._cached(("k", 0), lambda: pytest.fail("cached")) == 0  # a hit refreshes its place
    C._cached(("k", 3), lambda: 3)
    assert list(C._CACHE) == [("k", 2), ("k", 0), ("k", 3)]  # ("k", 1) was the least recently used


def _atrium_body(name, site=None):
    path = AFX / "full" / name if (AFX / "full" / name).exists() else AFX / name
    b = json.loads(path.read_text(encoding="utf-8"))
    if site is not None:  # what a cache in front of the API could answer for another site's fixture
        b["links"]["self"] = re.sub(r"/embed/\d+/", f"/embed/{site}/", b["links"]["self"])
    return b


@pytest.fixture
def atrium_wire_answers(monkeypatch):
    answers = {"fixtures": _atrium_body("12__fixtures.json")}
    answers["fixtures"]["links"]["self"] = f"{atrium_wire.BASE}/12/fixtures"
    calls = []

    def fake(url, params=None, **kw):
        calls.append(url.rsplit("/", 1)[1])
        return answers[url.rsplit("/", 1)[1]]

    monkeypatch.setattr(atrium_wire._bb, "_get", fake)
    atrium_wire._seasons_cache.clear()
    yield answers, calls
    atrium_wire._seasons_cache.clear()


def test_atrium_game_of_the_site_passes(atrium_wire_answers):
    answers, calls = atrium_wire_answers
    answers["fixture_detail"] = _atrium_body("12__fixture_detail__pbp.json")
    assert atrium_rt.lnb_pbp("5423072b-6715-11f0-bd8b-9ffadebc148f").height > 0
    assert atrium_rt.lnb_pbp("5423072b-6715-11f0-bd8b-9ffadebc148f", return_parsed=False)["data"]["sub"] == "pbp"
    assert calls == ["fixture_detail", "fixture_detail"]  # a game needs no season-list request


@pytest.mark.parametrize("return_parsed", [True, False])
def test_atrium_game_of_another_site_raises_through_the_runtime(atrium_wire_answers, return_parsed):
    answers, _ = atrium_wire_answers
    answers["fixture_detail"] = _atrium_body("14__fixture_detail__pbp.json")  # an LNBP body for an LNB request
    with pytest.raises(AssetFetchError, match="asked for /12/fixture_detail, the body is /14/fixture_detail"):
        atrium_rt.lnb_pbp("0dc631bd-c34c-11f1-8d16-4d5a21afb79d", return_parsed=return_parsed)
    # the live answer to a fixture the site does not serve
    answers["fixture_detail"] = _atrium_body("refusals/14__fixture_detail__pbp__other_site.json", site=12)
    with pytest.raises(NoDataError, match="site 12 has no fixture"):
        atrium_rt.lnb_pbp("5423072b-6715-11f0-bd8b-9ffadebc148f", return_parsed=return_parsed)
