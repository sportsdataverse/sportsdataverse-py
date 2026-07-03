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
    seen = 0
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
            seen += 1
            assert is_real_rebound(ctx, i) is False, (game_id, i)
    # Minor finding (code review): at least one of the 3 fixtures must actually
    # exercise this exclusion, or the test would vacuously pass on all of them.
    # 0022200001 (row idx 106, a team rebound co-clock with a real turnover) is
    # the qualifying case; the other two games have zero such rows.
    if game_id == "0022200001":
        assert seen > 0


# ---------------------------------------------------------------------------
# Oracle-derived fix (Critical review finding): the generic
# ``player1_id == 0`` / v3 ``team_id == 0`` signal.
#
# The reviewer's finding proposed porting pbpstats' *stats_nba*-concrete
# ``StatsRebound.is_placeholder`` (``event_action_type != 0 and
# player1_id == 0``) as a 5th, standalone exclusion. Empirically validating
# that proposal against the oracle this engine is actually built to match --
# pbpstats' ``live`` data provider on the committed cdn fixtures, via the
# file-mode harness pattern in test_nba_v3_v2_adapter.py, gated on
# SDV_PBPSTATS_ROOT -- disproved it: a bare ``team_id == 0`` exclusion flips
# 11-12 rebounds per game from correctly-real to wrongly-placeholder, because
# ``LiveRebound.is_placeholder`` (deadball qualifier / flagrant missed shot)
# does not fire for routine team rebounds. What the oracle cross-tab *did*
# confirm is that ``player1_id == 0`` is a required sub-condition of 3 of the
# existing 4 exclusions (is_turnover_placeholder / is_buzzer_beater_placeholder
# / is_buzzer_beater_rebound_at_shot_time all read
# ``... and self.player1_id == 0`` in pbpstats' own source) -- a guard the
# pre-fix predicate was missing for the buzzer-beater pair, which produced 3
# real false negatives (asserted below), plus a companion flagrant-FT bug in
# the non-final-FT exclusion caught by the same cross-tab (1 false positive).
# See nba_possession_rules.is_real_rebound's docstring for the full citation
# and the per-game confusion-matrix summary; see also
# .superpowers/sdd/phase-b/task-2-report.md ("Fix wave (Critical)").


def test_mid_quarter_team_rebound_is_real_not_placeholder():
    """A routine mid-quarter team rebound is REAL per the pbpstats-live oracle.

    This is the exact row the Critical finding cited as evidence for a
    standalone ``team_id == 0`` exclusion ("76ers Rebound", teamId=0,
    personId=1610612755 (a team id), PT10M12.00S Q1,
    tests/fixtures/nba_engine/0022200001/playbyplayv3.json actionNumber 25 ->
    enhanced-pbp row index 18). Running pbpstats' *live* provider (the oracle,
    not the stats_nba-concrete class the finding quoted) over the committed
    cdn fixture scores this row ``is_real_rebound is True`` -- confirming the
    finding's premise does not hold for the oracle this engine targets, and
    that adding the proposed standalone exclusion would have been a
    regression, not a fix (see the module docstring / task-2-report.md for
    the full cross-tab).
    """
    rows = _rows("0022200001")
    ctx = build_event_context(rows)
    i = 18
    row = rows[i]
    assert row["action_number"] == 25
    assert row["description"] == "76ers Rebound"
    assert row["team_id"] == 0
    assert is_real_rebound(ctx, i) is True


@pytest.mark.parametrize(
    "game_id,idx,action_number",
    [
        ("0022100001", 125, 163),  # "Harden REBOUND (Off:0 Def:1)", Q1 PT00M00.90S
        ("0022100001", 240, 326),  # "Harden REBOUND (Off:0 Def:4)", Q2 PT00M00.00S
        ("0022200001", 147, 193),  # "House Jr. REBOUND (Off:0 Def:1)", Q1 PT00M01.20S
    ],
)
def test_personal_buzzer_beater_rebound_is_real_not_placeholder(game_id, idx, action_number):
    """A *personal* (non-team) rebound in the closing seconds is real.

    pbpstats' ``is_buzzer_beater_placeholder`` / ``is_buzzer_beater_rebound_
    at_shot_time`` both require ``player1_id == 0`` (rebound.py:92-133) --
    the pre-fix predicate was missing that guard and wrongly excluded these
    3 rows (oracle cross-tab: pbpstats-live scores all 3 real). All 3 rows
    have a nonzero ``team_id`` (a real player crediting, not a team
    placeholder), unlike the mid-quarter team-rebound case above.
    """
    rows = _rows(game_id)
    ctx = build_event_context(rows)
    row = rows[idx]
    assert row["action_number"] == action_number
    assert row["team_id"] != 0
    assert is_real_rebound(ctx, idx) is True


def test_flagrant_final_ft_rebound_is_placeholder():
    """A rebound after a missed flagrant FT is a placeholder even when it's
    numerically the last FT of the trip.

    pbpstats' ``FreeThrow.is_end_ft`` explicitly excludes flagrant free
    throws (``... and not self.is_flagrant_ft``, free_throw.py:60-70) so
    ``is_non_live_ft_placeholder`` fires for a missed "3 of 3" flagrant FT
    the same as it would for a non-final FT. The pre-fix predicate used a
    bare ``_is_last_ft`` check and missed this (oracle: pbpstats-live scores
    this row -- "BUCKS Rebound" after a missed "Free Throw Flagrant 3 of 3",
    tests/fixtures/nba_engine/0022100001 actionNumber 267 -> row index 194 --
    as a placeholder).
    """
    rows = _rows("0022100001")
    ctx = build_event_context(rows)
    i = 194
    row = rows[i]
    assert row["action_number"] == 267
    assert row["team_id"] == 0
    assert is_real_rebound(ctx, i) is False


@pytest.mark.parametrize(
    "game_id,expected_real,expected_total",
    [
        # Provenance: pbpstats-live oracle count (real rebounds / total rebound
        # rows) measured via dev/_rebound_oracle_probe.py (gitignored,
        # SDV_PBPSTATS_ROOT-gated, not committed) against these 3 committed
        # cdn fixtures, 2026-07-03. This module's is_real_rebound now agrees
        # with the oracle on every rebound row in all 3 fixtures (0
        # disagreements) -- these counts double as that agreement check
        # without needing a live SDV_PBPSTATS_ROOT checkout in CI.
        ("0022100001", 107, 118),
        ("0022200001", 78, 86),
        ("0022300001", 86, 94),
    ],
)
def test_real_rebound_counts_match_oracle(game_id, expected_real, expected_total):
    rows = _rows(game_id)
    ctx = build_event_context(rows)
    rebound_indices = [i for i, row in enumerate(rows) if (row.get("event_type") or "") == "rebound"]
    assert len(rebound_indices) == expected_total
    n_real = sum(1 for i in rebound_indices if is_real_rebound(ctx, i))
    assert n_real == expected_real
