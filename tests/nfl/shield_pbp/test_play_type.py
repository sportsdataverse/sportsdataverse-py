"""play_type classification: the Shield port against nflfastR's translate_play_type_nfl.

nflfastR (helper_add_nflscrapr_mutations.R, translate_play_type_nfl) lets the feed's kick play types win
outright, labels a PENALTY play no_play unless it was enforced between downs, and turns only pass/run
plays into qb_spike / qb_kneel. The two real-game cases are 2025 plays the GridironMatrix oracle check
found misclassified against nflverse.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl.shield_pbp.build import build_pbp
from sportsdataverse.nfl.shield_pbp.description import add_description_features
from sportsdataverse.nfl.shield_pbp.parse import _play_type

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield"


def _play(game_id: str, play_id: int) -> dict:
    path = FIXTURES / f"{game_id}.json.gz"
    if not path.exists():
        pytest.skip(f"{game_id} fixture not present")
    df = build_pbp(json.load(gzip.open(path, "rt", encoding="utf-8")), game_id=game_id)
    row = df.filter(pl.col("play_id") == play_id)
    assert row.height == 1
    return row.select("play_type", "qb_dropback", "shield_play_type").to_dicts()[0]


def test_blocked_field_goal_with_holder_pass_is_a_field_goal():
    # 2025_03_LA_PHI 3600: FG blocked, kicker recovers and throws incomplete, taunting enforced between downs.
    # Feed playType FIELD_GOAL; the pass stat must not turn it into a dropback.
    r = _play("2025_03_LA_PHI", 3600)
    assert (r["shield_play_type"], r["play_type"], r["qb_dropback"]) == ("FIELD_GOAL", "field_goal", 0)


def test_grounding_after_replay_reversal_stays_a_pass():
    # 2025_02_SEA_PIT 3686: sack-fumble reversed on review to an incomplete pass with intentional grounding.
    # Shield says PASS, so this is a pass and a dropback. nflverse labels it no_play only because its feed still
    # carries the pre-review FUMBLE_RECOVERED_BY_OPPONENT type: a source divergence, pinned here on purpose.
    r = _play("2025_02_SEA_PIT", 3686)
    assert (r["shield_play_type"], r["play_type"], r["qb_dropback"]) == ("PASS", "pass", 1)


@pytest.mark.parametrize(
    ("feed", "expected"),
    [
        ("FIELD_GOAL", "field_goal"),
        ("PUNT", "punt"),
        ("KICK_OFF", "kickoff"),
        ("FREE_KICK", "kickoff"),
        ("XP_KICK", "extra_point"),
    ],
)
def test_feed_kick_type_wins_over_stats(feed, expected):
    # A kick play with a pass/rush stat (fake, botched snap, blocked-kick recovery) or with no attempt stat at all
    # (nullified kickoff / PAT) keeps the feed's kick type, as nflfastR does.
    assert _play_type({"pass_attempt": 1}, feed) == expected
    assert _play_type({}, feed) == expected


@pytest.mark.parametrize(
    ("row", "desc", "expected"),
    [
        # 2025_09_IND_PIT 4470: spike, then a defensive penalty - No Play.
        (
            {"pass_attempt": 1},
            "(:25) (No Huddle) D.Jones spiked the ball to stop the clock.\r\nPENALTY on PIT, "
            "Defensive Too Many Men on Field, 5 yards, enforced at 50 - No Play.",
            "no_play",
        ),
        (
            {"pass_attempt": 1},
            "J.Doe pass short left. PENALTY on X, Unnecessary Roughness, 15 yards, enforced between downs.",
            "pass",
        ),
        ({"rush_attempt": 1}, "J.Doe up the middle. PENALTY on X, Taunting, 15 yards, enforced between downs.", "run"),
        ({"field_goal_attempt": 1}, "(Field Goal formation) PENALTY on X, False Start, 5 yards - No Play.", "no_play"),
        ({}, "PENALTY on X, Delay of Game, 5 yards - No Play.", "no_play"),
    ],
)
def test_penalty_is_no_play_unless_enforced_between_downs(row, desc, expected):
    assert _play_type(row, "PENALTY", desc) == expected


def test_spike_and_kneel_only_relabel_passes_and_runs():
    df = pl.DataFrame(
        {
            "play_type": ["no_play", "pass", "no_play", "run"],
            "desc": [
                "(:25) (No Huddle) D.Jones spiked the ball to stop the clock. PENALTY on PIT - No Play.",
                "(:30) K.Cousins spiked the ball to stop the clock.",
                "(2:31) D.Prescott kneels to LV 7 for -1 yards. PENALTY on LV - No Play.",
                "(0:40) D.Prescott kneels to LV 7 for -1 yards.",
            ],
        }
    )
    assert add_description_features(df)["play_type"].to_list() == ["no_play", "qb_spike", "no_play", "qb_kneel"]
