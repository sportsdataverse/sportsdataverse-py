"""Idempotency + regression guard for ``tools/codegen/gen_sports247.py``.

The generator must reproduce the committed ``endpoints/sports247.yaml`` + all 11
``schemas/native/sports247/*.yaml`` byte-for-byte (re-running is a no-op). The
existing 11 wrapper ``short`` names are the load-bearing regression contract.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

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


def _run_generator() -> None:
    subprocess.run(
        [sys.executable, str(ROOT / "tools/codegen/gen_sports247.py")],
        cwd=ROOT,
        check=True,
    )


def test_regen_reproduces_committed_yaml() -> None:
    before_ep = EP.read_bytes()
    before_schemas = {p.name: p.read_bytes() for p in sorted(SCHEMA_DIR.glob("*.yaml"))}
    _run_generator()
    assert EP.read_bytes() == before_ep, "gen_sports247 must reproduce endpoints/sports247.yaml byte-for-byte"
    after_schemas = {p.name: p.read_bytes() for p in sorted(SCHEMA_DIR.glob("*.yaml"))}
    assert after_schemas == before_schemas, "gen_sports247 must reproduce all schemas byte-for-byte"


def test_existing_11_shorts_preserved() -> None:
    doc = yaml.safe_load(EP.read_text(encoding="utf-8"))
    shorts = {e["short"] for e in doc["endpoints"]}
    assert ORIG11 <= shorts
    assert all("returns_schema" in e and "parser" in e for e in doc["endpoints"])
