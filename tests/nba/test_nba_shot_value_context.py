"""Tests for the defender x shot-clock context make-prob tables + joint."""

import polars as pl

from sportsdataverse.nba.nba_shot_value import make_prob_by_context, make_prob_joint


def _ptshots():
    # stacked defender (2 buckets) + shot-clock (2 buckets); tighter defender
    # and later clock miss more
    return pl.DataFrame(
        {
            "result_set": ["ClosestDefenderShooting"] * 2 + ["ShotClockShooting"] * 2,
            "player_id": [201939, 201939, 201939, 201939],
            "bucket": [
                "0-2 Feet - Very Tight",
                "6+ Feet - Wide Open",
                "22-18 Very Early",
                "4-0 Very Late",
            ],
            "fga": [100, 100, 100, 100],
            "fgm": [40, 60, 55, 35],
            "fg_pct": [0.40, 0.60, 0.55, 0.35],
        }
    )


def test_context_defender_increases_with_openness():
    t = make_prob_by_context(_ptshots())
    d = t["defender"].sort("fg_pct")
    assert d.row(0, named=True)["bucket"] == "0-2 Feet - Very Tight"
    assert d.row(-1, named=True)["bucket"] == "6+ Feet - Wide Open"
    assert d["fg_pct"].to_list() == sorted(d["fg_pct"].to_list())


def test_context_missing_result_set_zero_rows():
    only_def = _ptshots().filter(pl.col("result_set") == "ClosestDefenderShooting")
    t = make_prob_by_context(only_def)
    assert t["shot_clock"].height == 0
    assert t["defender"].height == 2


def test_joint_probs_bounded_and_ordered():
    t = make_prob_by_context(_ptshots())
    joint = make_prob_joint(t["defender"], t["shot_clock"], overall_fg_pct=0.47)
    vals = joint["joint_fg_pct"].to_list()
    assert all(0.0 < v < 1.0 for v in vals)
    # tightest defender x latest clock is the lowest joint make-prob
    worst = joint.sort("joint_fg_pct").row(0, named=True)
    assert worst["close_def_dist_range"] == "0-2 Feet - Very Tight"
    assert worst["shot_clock_range"] == "4-0 Very Late"


def test_joint_empty_input():
    joint = make_prob_joint(pl.DataFrame(), pl.DataFrame(), 0.47)
    assert joint.height == 0 and "joint_fg_pct" in joint.columns
