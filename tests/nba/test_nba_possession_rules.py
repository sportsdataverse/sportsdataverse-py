"""Rule-level tests for nba_possession_rules (fixture-grounded)."""

from __future__ import annotations

import json
import pathlib


from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possession_rules import (
    build_event_context,
    resolve_event_team,
)

FIX = pathlib.Path("tests/fixtures/nba_engine")
GAMES = ["0022100001", "0022200001", "0022300001"]
_ROWS_CACHE: dict = {}


def _rows(game_id: str) -> list:
    if game_id not in _ROWS_CACHE:
        payload = json.loads((FIX / game_id / "playbyplayv3.json").read_text())
        _ROWS_CACHE[game_id] = enhanced_pbp_from_payload(payload).to_dicts()
    return _ROWS_CACHE[game_id]


def test_context_co_clock_groups_same_instant_events():
    rows = _rows("0022200001")
    ctx = build_event_context(rows)
    for i, row in enumerate(rows):
        group = ctx.co_clock(i)
        assert i in group
        for j in group:
            assert rows[j]["period"] == row["period"]
            assert rows[j]["seconds_remaining"] == row["seconds_remaining"]


def test_context_empty_rows():
    ctx = build_event_context([])
    assert ctx.rows == []
    assert ctx.at_clock == {}


def test_resolve_event_team_prefers_team_id_then_location():
    assert resolve_event_team({"team_id": 42, "location": "h"}, 1, 2) == 42
    assert resolve_event_team({"team_id": 0, "location": "h"}, 1, 2) == 1
    assert resolve_event_team({"team_id": None, "location": "v"}, 1, 2) == 2
    assert resolve_event_team({"team_id": 0, "location": ""}, 1, 2) == 0
