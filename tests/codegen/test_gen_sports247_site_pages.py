"""Offline test for the sports247_site_pages OpenAPI -> codegen generator."""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
_SPEC = (
    Path(os.environ.get("SDV_INTERNAL_REFS_REPO", "C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-internal-refs"))
    / "247sports"
    / "site-pages.openapi.yaml"
)
pytestmark = pytest.mark.skipif(
    not _SPEC.exists(), reason="sdv-internal-refs 247 site-pages spec not present (local-only source)"
)
ENDPOINTS = "tools/codegen/endpoints/sports247_site_pages.yaml"
SCHEMA_DIR = "tools/codegen/schemas/native/sports247_site_pages"


@pytest.fixture
def gen(tmp_path, monkeypatch):
    """The generator with its output root redirected to ``tmp_path``.

    ``main()`` wipes and rewrites ``schemas/native/sports247_site_pages/``; against the
    real tree that raced every parallel test reading those schemas.
    """
    mod = importlib.import_module("tools.codegen.gen_sports247_site_pages")
    monkeypatch.setattr(mod, "ROOT", tmp_path)
    return mod


def test_generator_is_idempotent_and_emits_expected_stem(gen, tmp_path):
    endpoints_out = tmp_path / ENDPOINTS
    schema_out = tmp_path / SCHEMA_DIR
    gen.main()

    ydoc = yaml.safe_load(endpoints_out.read_text(encoding="utf-8"))
    assert ydoc["api"] == "sports247_site_pages"
    assert ydoc["host"] == "https://247sports.com"
    assert ydoc["name_pattern"] == "sports247_site_pages_{short}"
    assert ydoc["parser_module"] == "cfb.sports247_site_pages_parsers"
    assert ydoc["getter_module"] == "sportsdataverse.cfb.sports247_site_pages_runtime"

    shorts = {e["short"] for e in ydoc["endpoints"]}
    assert {"institution", "season_recruits", "playersport", "league_draft_picks"} <= shorts
    assert len(ydoc["endpoints"]) == 35
    assert ydoc["passthrough_query"] is True

    # every endpoint routes through the one generic parser + a native schema
    for e in ydoc["endpoints"]:
        assert e["parser"] == "parse_sports247_site_page"
        assert e["returns_schema"].startswith("native/sports247_site_pages/")

    # 17 distinct returns-schemas emitted
    assert len(list(schema_out.glob("*.yaml"))) == 17

    # schema names are stem-prefixed so manual_column_descriptions keys never
    # collide with another bucket's bare entity name (coach/player/event/...).
    inst = yaml.safe_load((schema_out / "sports247_site_pages_institution.yaml").read_text(encoding="utf-8"))
    assert inst["schema"] == "sports247_site_pages_institution"
    cols = {c["name"] for c in inst["columns"]}
    assert {"key", "location", "state", "latitude", "name"} <= cols

    # inlined Player object in Recruit flattens to player_* leaf columns
    rec = yaml.safe_load((schema_out / "sports247_site_pages_recruit.yaml").read_text(encoding="utf-8"))
    rec_cols = {c["name"] for c in rec["columns"]}
    assert {"key", "player_key", "player_full_name", "player_hometown_state"} <= rec_cols

    # path placeholders renamed to snake python names in the emitted path
    tl = next(e for e in ydoc["endpoints"] if e["short"] == "institution_timeline_events")
    assert "{school_slug}" in tl["path"] and "{key}" in tl["path"]
    assert tl["path"].endswith(".json")

    # idempotence: a second run is byte-identical
    first = endpoints_out.read_text(encoding="utf-8")
    gen.main()
    assert endpoints_out.read_text(encoding="utf-8") == first
    # and it matches the committed stem, so the real tree never needs rewriting
    assert first == (ROOT / ENDPOINTS).read_text(encoding="utf-8")


def test_query_and_path_params_snake_cased_with_original_query_key(gen, tmp_path):
    gen.main()
    ydoc = yaml.safe_load((tmp_path / ENDPOINTS).read_text(encoding="utf-8"))

    recruits = next(e for e in ydoc["endpoints"] if e["short"] == "season_recruits")
    # path param season is a str carrying the {year}-{Sport} note
    pp = {p["name"]: p for p in recruits["path_params"]}
    assert pp["season"]["type"] == "str"
    # query param Player.FullName -> player_full_name, original preserved
    ex = {p["name"]: p for p in recruits["extra_params"]}
    assert ex["player_full_name"]["query_key"] == "Player.FullName"
    assert ex["items"]["type"] == "int"
    assert recruits["example_args"]["season"] == "2026-Football"
