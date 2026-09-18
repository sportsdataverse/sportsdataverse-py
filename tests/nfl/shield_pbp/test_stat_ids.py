"""Tests for the GSIS statType decode + sum_play_stats port."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
from sportsdataverse.nfl.shield_pbp.stat_ids import (
    _FILL_ID_SLOTS,
    FILL_GROUPS,
    STAT_ID_EFFECTS,
    sum_play_stats,
)

GAME = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield" / "2024_01_BAL_KC.json.gz"


# ---------------------------------------------------------------------------
# Unit semantics
# ---------------------------------------------------------------------------


def test_completion_sets_passer_yards():
    row = sum_play_stats(
        [
            {"statType": 15, "yards": 12, "gsisPlayerId": "00-0034796", "gsisPlayerName": "L.Jackson", "teamId": "BAL"},
        ]
    )
    assert row["pass_attempt"] == 1
    assert row["complete_pass"] == 1
    assert row["passer_player_id"] == "00-0034796"
    assert row["yards_gained"] == 12
    assert row["passing_yards"] == 12


def test_air_yards_complete_vs_incomplete():
    comp = sum_play_stats([{"statType": 111, "yards": 18, "gsisPlayerId": "p", "gsisPlayerName": "n", "teamId": "T"}])
    assert comp["air_yards"] == 18 and comp["complete_pass"] == 1
    incomp = sum_play_stats([{"statType": 112, "yards": 25, "gsisPlayerId": "p", "gsisPlayerName": "n", "teamId": "T"}])
    assert incomp["air_yards"] == 25 and "complete_pass" not in incomp  # 112 must NOT set complete_pass


def test_yac_ifna_receiver_does_not_overwrite():
    # stat 21 sets the receiver; a later stat 113 (YAC) must NOT overwrite it, but DOES set yac.
    row = sum_play_stats(
        [
            {"statType": 21, "yards": 9, "gsisPlayerId": "REC1", "gsisPlayerName": "First", "teamId": "T"},
            {"statType": 113, "yards": 4, "gsisPlayerId": "REC2", "gsisPlayerName": "Second", "teamId": "T"},
        ]
    )
    assert row["receiver_player_id"] == "REC1"  # IFNA guard preserved the first receiver
    assert row["yards_after_catch"] == 4


def test_penalty_fields():
    row = sum_play_stats(
        [
            {"statType": 93, "yards": 5, "gsisPlayerId": "00-0032965", "gsisPlayerName": "R.Stanley", "teamId": "BAL"},
        ]
    )
    assert row["penalty"] == 1
    assert row["penalty_yards"] == 5
    assert row["penalty_team"] == "BAL"


def test_qb_hit_fill_two_slots_with_dedup():
    row = sum_play_stats(
        [
            {"statType": 110, "yards": 0, "gsisPlayerId": "A", "gsisPlayerName": "Aaa", "teamId": "T"},
            {
                "statType": 110,
                "yards": 0,
                "gsisPlayerId": "A",
                "gsisPlayerName": "Aaa",
                "teamId": "T",
            },  # dup -> skipped
            {"statType": 110, "yards": 0, "gsisPlayerId": "B", "gsisPlayerName": "Bbb", "teamId": "T"},
        ]
    )
    assert row["qb_hit"] == 1
    assert row["qb_hit_1_player_id"] == "A"
    assert row["qb_hit_2_player_id"] == "B"  # second distinct player took slot 2; dup A did not


def test_rush_touchdown_sets_td_fields():
    row = sum_play_stats(
        [
            {"statType": 11, "yards": 3, "gsisPlayerId": "RB", "gsisPlayerName": "R.Back", "teamId": "KC"},
        ]
    )
    assert row["rush_touchdown"] == 1 and row["touchdown"] == 1
    assert row["td_team"] == "KC" and row["td_player_id"] == "RB"
    assert row["yards_gained"] == 3


def test_empty_and_unknown_statids_are_noops():
    assert sum_play_stats([]) == {}
    assert (
        sum_play_stats([{"statType": 999, "yards": 1, "gsisPlayerId": "x", "gsisPlayerName": "y", "teamId": "z"}]) == {}
    )


def test_def_tackles_for_loss_counts_exactly():
    # statId 402 = TFL credit; calculate_stats.R def_tackles_for_loss = sum(stat_id == 402).
    # Three credits on one play must count 3 (the old 2-slot FILL capped at 2).
    row = sum_play_stats(
        [
            {"statType": 402, "yards": 2, "gsisPlayerId": "A", "gsisPlayerName": "Aaa", "teamId": "T"},
            {"statType": 402, "yards": 3, "gsisPlayerId": "B", "gsisPlayerName": "Bbb", "teamId": "T"},
            {"statType": 402, "yards": 1, "gsisPlayerId": "C", "gsisPlayerName": "Ccc", "teamId": "T"},
        ]
    )
    assert row["def_tackles_for_loss"] == 3
    assert row["def_tackles_for_loss_yards"] == 6


def test_misc_yards_sums_63_64():
    # calculate_stats.R misc_yards = sum((stat_id %in% 63:64) * yards). statId 63 is now
    # an accumulator (was a no-op); 64 also scores a TD.
    row = sum_play_stats(
        [
            {"statType": 63, "yards": 7, "gsisPlayerId": "x", "gsisPlayerName": "y", "teamId": "z"},
            {"statType": 64, "yards": 5, "gsisPlayerId": "x", "gsisPlayerName": "y", "teamId": "z"},
        ]
    )
    assert row["misc_yards"] == 12
    assert row["touchdown"] == 1  # 64 still scores


def test_lateral_yards_credited_to_team_totals():
    # rushing_yards = sum((stat_id %in% 10:13)*yards): the lateral carrier's yards (12)
    # add to the primary carrier's (10), closing the lateral-row diffs.
    rush = sum_play_stats(
        [
            {"statType": 10, "yards": 6, "gsisPlayerId": "RB", "gsisPlayerName": "R", "teamId": "T"},
            {"statType": 12, "yards": 14, "gsisPlayerId": "WR", "gsisPlayerName": "W", "teamId": "T"},
        ]
    )
    assert rush["rushing_yards"] == 20 and rush["lateral_rushing_yards"] == 14
    rec = sum_play_stats(
        [
            {"statType": 21, "yards": 8, "gsisPlayerId": "R1", "gsisPlayerName": "A", "teamId": "T"},
            {"statType": 23, "yards": 22, "gsisPlayerId": "R2", "gsisPlayerName": "B", "teamId": "T"},
        ]
    )
    assert rec["receiving_yards"] == 30 and rec["lateral_receiving_yards"] == 22


def test_fumble_recovery_lateral_yards_accumulate():
    own = sum_play_stats(
        [
            {"statType": 57, "yards": 9, "gsisPlayerId": "x", "gsisPlayerName": "y", "teamId": "T"},
        ]
    )
    assert own["fumble_recovery_own_lateral_yards"] == 9 and own["lateral_recovery"] == 1
    opp = sum_play_stats(
        [
            {"statType": 61, "yards": 4, "gsisPlayerId": "x", "gsisPlayerName": "y", "teamId": "T"},
        ]
    )
    assert opp["fumble_recovery_opp_lateral_yards"] == 4


def test_td_ids_touchdown_flag():
    # A rushing TD (statId 11) is in td_ids() -> sets td_ids_touchdown (def_tds input).
    assert (
        sum_play_stats([{"statType": 11, "yards": 1, "gsisPlayerId": "x", "gsisPlayerName": "y", "teamId": "T"}])[
            "td_ids_touchdown"
        ]
        == 1
    )
    # A fumble-recovery TD (statId 56) is NOT in td_ids() (counted in fumble_recovery_tds).
    assert "td_ids_touchdown" not in sum_play_stats(
        [{"statType": 56, "yards": 0, "gsisPlayerId": "x", "gsisPlayerName": "y", "teamId": "T"}]
    )


# ---------------------------------------------------------------------------
# Mapping integrity
# ---------------------------------------------------------------------------


def test_fill_group_references_resolve():
    # Every *_FILL_* column used in STAT_ID_EFFECTS has a FILL_GROUPS entry...
    used = {col for effects in STAT_ID_EFFECTS.values() for col, _ in effects if "_FILL_" in col}
    assert used <= set(FILL_GROUPS), f"missing FILL_GROUPS for: {used - set(FILL_GROUPS)}"
    # ...and every FILL group prefix has a de-dup id-slot mapping.
    prefixes = {c.split("_FILL_", 1)[0] for c in FILL_GROUPS}
    assert prefixes <= set(_FILL_ID_SLOTS), f"missing _FILL_ID_SLOTS for: {prefixes - set(_FILL_ID_SLOTS)}"


def test_model_critical_codes_present():
    # Lower bound + the codes the EP/WP/CP models depend on must all be mapped.
    assert len(STAT_ID_EFFECTS) >= 100
    critical = {10, 11, 14, 15, 16, 19, 20, 21, 22, 68, 70, 89, 93, 110, 111, 112, 113}
    assert critical <= set(STAT_ID_EFFECTS), f"missing critical codes: {critical - set(STAT_ID_EFFECTS)}"


# ---------------------------------------------------------------------------
# Real-game smoke test (the parity anchor)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not GAME.exists(), reason="2024_01_BAL_KC fixture not present")
def test_real_game_runs_clean_and_has_signal():
    game = json.load(gzip.open(GAME, "rt", encoding="utf-8"))
    plays = game["driveChart"]["plays"]
    rows = [sum_play_stats(p.get("stats") or []) for p in plays]

    # No exceptions, and the game produces meaningful aggregate signal.
    completions = sum(r.get("complete_pass", 0) for r in rows)
    rush_atts = sum(r.get("rush_attempt", 0) for r in rows)
    pass_atts = sum(r.get("pass_attempt", 0) for r in rows)
    air_yards_plays = sum(1 for r in rows if r.get("air_yards") is not None)
    touchdowns = sum(r.get("touchdown", 0) for r in rows)

    assert completions > 20, f"too few completions: {completions}"
    assert rush_atts > 20, f"too few rush attempts: {rush_atts}"
    assert pass_atts > completions, "pass attempts should exceed completions"
    assert air_yards_plays > 20, f"air_yards should populate on most pass plays: {air_yards_plays}"
    assert touchdowns >= 4, f"BAL@KC 2024 opener had multiple TDs: {touchdowns}"
