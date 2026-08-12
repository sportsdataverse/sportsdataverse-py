"""Era-stable canonical play types.

The mapping is validated against the full published history (2004-2025,
3,145,840 plays, 53 distinct ``type.text`` values): every observed value maps,
and the three era-synonym collapses agree with the mechanics flags
(``punt`` 99.9% ``punt_play=True``, ``kickoff`` 99.8% ``kickoff_play=True``).
These tests lock that in offline.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb import (
    PLAY_TYPE_CANONICAL,
    PLAY_TYPE_FAMILY,
    add_play_type_canonical,
    canonical_play_type_expr,
)

#: Every ``type.text`` observed in play_by_play_2004..2025. Captured 2026-08-11;
#: a value ESPN adds later must be added here AND to PLAY_TYPE_CANONICAL, which
#: is exactly the drift this list is meant to force into review.
OBSERVED_TYPES = [
    "Rush",
    "Pass Incompletion",
    "Pass Reception",
    "Pass Completion",
    "Kickoff",
    "Punt",
    "Penalty",
    "Timeout",
    "Sack",
    "Rushing Touchdown",
    "Passing Touchdown",
    "Extra Point Good",
    "Field Goal Good",
    "Interception Return",
    "Kickoff Return (Offense)",
    "Fumble Recovery (Opponent)",
    "Field Goal Missed",
    "Fumble Recovery (Own)",
    "Punt Return",
    "Extra Point Missed",
    "Punt Team Fumble Recovery",
    "Interception Return Touchdown",
    "End Period",
    "Punt Return Touchdown",
    "Kickoff Return Touchdown",
    "Safety",
    "Kickoff Team Fumble Recovery",
    "Blocked Field Goal",
    "Two-Point Conversion Missed",
    "Two-Point Conversion Good",
    "Fumble Recovery (Opponent) Touchdown",
    "Blocked Punt",
    "Fumble Return Touchdown",
    "Blocked Punt Touchdown",
    "Unknown",
    "Pass",
    "Punt (Safety)",
    "Fumble",
    "Defensive 2pt Conversion",
    "Pass Interception Return",
    "Two Point Pass",
    "Pass Interception",
    "Punt Team Fumble Recovery Touchdown",
    "Missed Field Goal Return",
    "Two Point Rush",
    "Blocked Field Goal Touchdown",
    "2pt Conversion",
    "Kickoff (Safety)",
    "Missed Field Goal Return Touchdown",
    "Fumble Recovery (Own) Touchdown",
    "End of Game",
    "End of Half",
    "Penalty (Safety)",
]


def test_every_observed_type_is_mapped():
    """Zero unmapped values across the full published history."""
    missing = [t for t in OBSERVED_TYPES if t not in PLAY_TYPE_CANONICAL]
    assert missing == [], f"unmapped play types: {missing}"


def test_every_canonical_type_has_a_family():
    orphans = sorted({v for v in PLAY_TYPE_CANONICAL.values()} - set(PLAY_TYPE_FAMILY))
    assert orphans == [], f"canonical types with no family: {orphans}"


def test_pass_completion_collapses_the_2014_vocabulary_split():
    """The headline fix: `Pass Reception` (2014+) and `Pass Completion` (2004-2013)
    are the same event. Querying the raw value silently misses half the history."""
    frame = pl.DataFrame({"type.text": ["Pass Completion", "Pass Reception"]})
    out = add_play_type_canonical(frame)
    assert out["play_type_canonical"].to_list() == ["pass_completion", "pass_completion"]


def test_punt_and_kickoff_collapse_the_2004_vocabulary():
    """2004 has no `Punt` / `Kickoff` rows at all -- it uses the *_Return spellings."""
    frame = pl.DataFrame({"type.text": ["Punt", "Punt Return", "Kickoff", "Kickoff Return (Offense)"]})
    out = add_play_type_canonical(frame)
    assert out["play_type_canonical"].to_list() == ["punt", "punt", "kickoff", "kickoff"]


def test_interception_collapses_three_spellings():
    frame = pl.DataFrame({"type.text": ["Interception Return", "Pass Interception", "Pass Interception Return"]})
    out = add_play_type_canonical(frame)
    assert set(out["play_type_canonical"].to_list()) == {"interception"}


def test_touchdown_variants_stay_distinct_from_their_base_play():
    """Collapsing era-synonyms must not collapse genuine distinctions."""
    frame = pl.DataFrame({"type.text": ["Rush", "Rushing Touchdown", "Punt", "Punt Return Touchdown"]})
    out = add_play_type_canonical(frame)
    assert out["play_type_canonical"].to_list() == [
        "rush",
        "rush_touchdown",
        "punt",
        "punt_return_touchdown",
    ]


def test_unknown_upstream_type_becomes_null_not_a_new_category():
    """Vocabulary drift must surface as a null, never pass through silently."""
    frame = pl.DataFrame({"type.text": ["Rush", "Quantum Flea Flicker", None]})
    out = add_play_type_canonical(frame)
    assert out["play_type_canonical"].to_list() == ["rush", None, None]
    assert out["play_type_family"].to_list() == ["offense", None, None]


def test_family_groups_administrative_rows_for_exclusion():
    """Timeouts / period markers are not plays and must be excludable from
    per-play denominators."""
    frame = pl.DataFrame({"type.text": ["Timeout", "End Period", "End of Half", "Rush"]})
    out = add_play_type_canonical(frame)
    assert out["play_type_family"].to_list() == [
        "administrative",
        "administrative",
        "administrative",
        "offense",
    ]


def test_missing_source_column_returns_frame_unchanged():
    """Safe on frames already projected down -- no raise, no partial column."""
    frame = pl.DataFrame({"game_id": [1, 2]})
    out = add_play_type_canonical(frame)
    assert out.equals(frame)


def test_expression_form_is_usable_standalone():
    frame = pl.DataFrame({"type.text": ["Sack"]})
    out = frame.with_columns(canonical_play_type_expr())
    assert out["play_type_canonical"][0] == "sack"


def test_custom_source_column_name():
    frame = pl.DataFrame({"play_type": ["Pass Reception"]})
    out = add_play_type_canonical(frame, source="play_type", with_family=False)
    assert out["play_type_canonical"][0] == "pass_completion"
    assert "play_type_family" not in out.columns
