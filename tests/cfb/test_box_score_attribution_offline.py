# tests/cfb/test_box_score_attribution_offline.py
"""Offline golden tests for create_box_score team attribution.

Each test mocks cfb_pbp.download to return a captured summary payload, so
no network is hit. Team attribution does not depend on participants, so the
participants join falls back to regex on these fixtures (the mocked download
returns the summary for any URL, which the participants parser treats as
empty and falls back). See spec section 8.
"""

from __future__ import annotations

import json
from pathlib import Path


from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _load(gid: int) -> dict:
    return json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))


def _box(monkeypatch, gid: int) -> dict:
    summary = _load(gid)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    proc.espn_cfb_pbp()
    out = proc.run_processing_pipeline()
    return out["advBoxScore"]


def _team(box_section: list[dict], team_id: int) -> dict:
    matches = [r for r in box_section if r.get("pos_team") == team_id or r.get("team_id") == team_id]
    assert matches, f"team {team_id} not found in section"
    return matches[0]


def test_fixtures_produce_box(monkeypatch):
    box = _box(monkeypatch, 401754598)
    assert set(box) >= {"turnover", "team", "defensive_players", "specialists"}


def test_attribution_cols_present(monkeypatch):
    import polars as pl

    summary = _load(401754598)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401754598)
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    for col in [
        "turnover_team",
        "is_turnover",
        "is_st_turnover",
        "fumble_recovery_team",
        "penalized_team",
        "kicking_team",
        "return_team",
    ]:
        assert col in df.columns, f"missing {col}"
