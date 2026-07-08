"""Offline tests for the On3 Recruit Database codegen generator (``gen_on3.py``).

Exercise the pure helpers (path->short, usable filter) without touching the
network or the real spec file, plus an idempotence check on ``main()``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


_GEN = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "gen_on3.py"


def _load_gen():
    spec = importlib.util.spec_from_file_location("gen_on3", _GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_short_from_path():
    gen = _load_gen()
    assert gen._short_from_path("/rdb/v1/team-ranking/{sport}-{year}/team-rankings") == "team_ranking_team_rankings"
    assert gen._short_from_path("/rdb/v2/nil-100") == "nil_100_v2"
    assert gen._short_from_path("/rdb/v1/players/industry-comparision") == "players_industry_comparision"
    assert gen._short_from_path("/rdb/v1/person-sport-rankings") == "person_sport_rankings"
    assert gen._short_from_path("/rdb/v1/player/{personKey}/profile") == "player_profile"


def test_usable_drops_non200():
    gen = _load_gen()
    # dict-form live-status (plan contract) and bare-int form (real spec) both drop 400/404
    assert gen._usable({"x-live-status": {"status": 404}}) is False
    assert gen._usable({"x-live-status": {"status": 400}}) is False
    assert gen._usable({"x-live-status": 404}) is False
    assert gen._usable({"x-live-status": 400}) is False
    # 204 (empty body) and unflagged / validated ops are kept
    assert gen._usable({"x-live-status": 204}) is True
    assert gen._usable({"x-live-validated": True}) is True
    assert gen._usable({}) is True


def test_idempotent(tmp_path, monkeypatch):
    """Two consecutive ``main()`` runs produce byte-identical endpoint YAML."""
    gen = _load_gen()
    root = tmp_path
    (root / "tools/codegen/endpoints").mkdir(parents=True)
    monkeypatch.setattr(gen, "ROOT", root)
    gen.main()
    first = (root / "tools/codegen/endpoints/on3.yaml").read_bytes()
    gen.main()
    second = (root / "tools/codegen/endpoints/on3.yaml").read_bytes()
    assert first == second
    assert b"api.on3.com/public/rdb/v1" in first
