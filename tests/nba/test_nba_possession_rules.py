"""Rule-level tests for nba_possession_rules (fixture-grounded)."""

from __future__ import annotations

import json
import pathlib

import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possession_rules import (
    _norm,
    build_event_context,
    is_no_turnover,
    is_real_rebound,
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


def test_is_no_turnover_empty_subtype_is_placeholder():
    assert is_no_turnover({"event_type": "turnover", "sub_type": ""}) is True
    assert is_no_turnover({"event_type": "turnover", "sub_type": None}) is True
    assert is_no_turnover({"event_type": "turnover", "sub_type": "Bad Pass"}) is False


@pytest.mark.parametrize("game_id", GAMES)
def test_real_rebounds_never_follow_nonfinal_ft_miss(game_id):
    """A rebound after a missed NON-final FT (e.g. 1 of 2) is a placeholder."""
    rows = _rows(game_id)
    ctx = build_event_context(rows)
    from sportsdataverse.nba.nba_possession_rules import _rebound_missed_shot_index
    from sportsdataverse.nba.nba_possessions import _is_last_ft

    seen_nonfinal_ft_rebound = 0
    for i, row in enumerate(rows):
        if (row.get("event_type") or "") != "rebound":
            continue
        j = _rebound_missed_shot_index(ctx, i)
        if (
            j >= 0
            and (rows[j].get("event_type") or "") == "free_throw"
            and not _is_last_ft(rows[j].get("sub_type") or "")
        ):
            seen_nonfinal_ft_rebound += 1
            assert is_real_rebound(ctx, i) is False, (game_id, i, rows[j]["sub_type"])
    assert seen_nonfinal_ft_rebound > 0  # fixtures contain missed FT 1-of-2 sequences


@pytest.mark.parametrize("game_id", GAMES)
def test_rebound_coincident_with_turnover_is_placeholder(game_id):
    """Rebound at the same clock as a real turnover (shot-clock/kicked-ball) is placeholder."""
    rows = _rows(game_id)
    ctx = build_event_context(rows)
    for i, row in enumerate(rows):
        if (row.get("event_type") or "") != "rebound":
            continue
        co = [j for j in ctx.co_clock(i) if j != i]
        has_real_to = any(
            (rows[j].get("event_type") or "") == "turnover"
            and not is_no_turnover(rows[j])
            and _norm(rows[j].get("sub_type")) in ("shot clock turnover", "kicked ball violation")
            for j in co
        )
        if has_real_to:
            assert is_real_rebound(ctx, i) is False, (game_id, i)
