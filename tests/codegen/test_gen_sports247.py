"""Idempotency + regression guard for ``tools/codegen/gen_sports247.py``.

The generator must reproduce the committed ``endpoints/sports247.yaml`` + all 11
``schemas/native/sports247/*.yaml`` byte-for-byte (re-running is a no-op). The
existing 11 wrapper ``short`` names are the load-bearing regression contract.

The generator writes into ``tmp_path`` and the output is compared with the committed
files; it never rewrites the real tree, which other tests read in parallel.
"""

from __future__ import annotations

import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
EP = ROOT / "tools/codegen/endpoints/sports247.yaml"
SCHEMA_DIR = ROOT / "tools/codegen/schemas/native/sports247"

ORIG11 = {
    "teams",
    "institution_rankings",
    "recruits",
    "transfers",
    "coaches",
    "transfer_portal_player_feed",
    "composite_team_ranking_feed",
    "transfer_portal_team_feed",
    "target_predictions",
    "sport_years",
    "tags_autocomplete",
}


def test_regen_reproduces_committed_yaml(tmp_path, monkeypatch) -> None:
    import tools.codegen.gen_sports247 as gen

    out_ep = tmp_path / "endpoints/sports247.yaml"
    out_schemas = tmp_path / "schemas/native/sports247"
    monkeypatch.setattr(gen, "ENDPOINTS_PATH", out_ep)
    monkeypatch.setattr(gen, "SCHEMA_DIR", out_schemas)
    gen.main()
    assert out_ep.read_bytes() == EP.read_bytes(), "gen_sports247 must reproduce endpoints/sports247.yaml byte-for-byte"
    committed = {p.name: p.read_bytes() for p in sorted(SCHEMA_DIR.glob("*.yaml"))}
    generated = {p.name: p.read_bytes() for p in sorted(out_schemas.glob("*.yaml"))}
    assert generated == committed, "gen_sports247 must reproduce all schemas byte-for-byte"


def test_existing_11_shorts_preserved() -> None:
    doc = yaml.safe_load(EP.read_text(encoding="utf-8"))
    shorts = {e["short"] for e in doc["endpoints"]}
    assert ORIG11 <= shorts
    assert all("returns_schema" in e and "parser" in e for e in doc["endpoints"])
