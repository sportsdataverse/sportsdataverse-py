"""Tests for the pass/rush classification layer (add_pass_rush).

Reference: ``docs/superpowers/plans/2026-07-03-nflfastr-parity-reference.md`` §6
(``helper_additional_functions.R :: clean_pbp`` pass/rush derivation, lines
2571-2601 of the reference extract) + §3 (``fix_weird_pass_plays``, applied
BETWEEN the pass and rush derivations, matching nflfastR's mutate order).
"""

from __future__ import annotations

import polars as pl
from sportsdataverse.nfl.shield_pbp.description import add_pass_rush, add_qb_dropback
from polars.testing import assert_frame_equal


def _frame(**overrides: object) -> pl.DataFrame:
    """One-row frame carrying every add_pass_rush input, override per test."""
    base: dict = {
        "game_id": ["2023_01_BUF_NYJ"],
        "play_id": [100.0],
        "desc": ["(15:00) J.Allen pass short right to S.Diggs for 8 yards."],
        "qb_scramble": [0],
        "qb_kneel": [0],
        "kickoff_attempt": [0],
        "rusher_player_name": [None],
    }
    base.update({k: [v] for k, v in overrides.items()})
    return pl.DataFrame(base, schema_overrides={"rusher_player_name": pl.Utf8})


# ---------------------------------------------------------------------------
# pass — base detection: " pass " | "sacked" | "scramble" | qb_scramble == 1
# ---------------------------------------------------------------------------


def test_pass_detected_from_desc_keywords_and_scramble_flag():
    df = pl.DataFrame(
        {
            "game_id": ["2023_01_BUF_NYJ"] * 5,
            "play_id": [1.0, 2.0, 3.0, 4.0, 5.0],
            "desc": [
                "(15:00) J.Allen pass short right to S.Diggs.",
                "(14:20) J.Allen sacked at BUF 30 for -5 yards.",
                "(13:40) J.Allen scrambles right end for 6 yards.",
                "(13:00) J.Cook right guard for 3 yards.",  # qb_scramble backfilled
                "(12:30) J.Cook left tackle for 2 yards.",  # plain run
            ],
            "qb_scramble": [0, 0, 1, 1, 0],
            "qb_kneel": [0] * 5,
            "kickoff_attempt": [0] * 5,
            "rusher_player_name": [None, None, "J.Allen", "J.Cook", "J.Cook"],
        }
    )
    out = add_pass_rush(df)
    assert out["pass"].to_list() == [1, 1, 1, 1, 0]


# ---------------------------------------------------------------------------
# pass — backward/lateral-pass exclusion (only when a rusher is present)
# ---------------------------------------------------------------------------


def test_backward_pass_with_rusher_is_not_a_pass():
    out = add_pass_rush(
        _frame(
            desc="(5:00) J.Cook right end, backward pass to J.Allen for 3 yards.",
            rusher_player_name="J.Cook",
        )
    )
    assert out["pass"].to_list() == [0]
    # ...and having been reclassified, it counts as a rush.
    assert out["rush"].to_list() == [1]


def test_backward_pass_without_rusher_stays_a_pass():
    out = add_pass_rush(_frame(desc="(5:00) Backward pass caught by J.Allen, incomplete pass thrown."))
    assert out["pass"].to_list() == [1]


# ---------------------------------------------------------------------------
# pass — kickoff exclusion
# ---------------------------------------------------------------------------


def test_forward_pass_on_kickoff_is_not_a_pass():
    out = add_pass_rush(_frame(desc="(15:00) T.Bass kicks 65 yards, forward pass on the return.", kickoff_attempt=1))
    assert out["pass"].to_list() == [0]


# ---------------------------------------------------------------------------
# pass — fix_weird_pass_plays fires INSIDE the derivation (between pass and
# rush, matching nflfastR's mutate order): the 15 hardcoded false positives
# end 0, and rush is derived from the FIXED pass value.
# ---------------------------------------------------------------------------


def test_weird_pass_false_positive_zeroed_and_rush_rederived():
    df = pl.DataFrame(
        {
            "game_id": ["1999_01_ARI_PHI", "1999_01_ARI_PHI"],
            "play_id": [1611.0, 42.0],
            "desc": [
                "(2:00) garbled play description with pass keyword.",
                "(1:30) J.Plummer pass short left to R.Moore for 12 yards.",
            ],
            "qb_scramble": [0, 0],
            "qb_kneel": [0, 0],
            "kickoff_attempt": [0, 0],
            "rusher_player_name": ["A.Murrell", None],
        }
    )
    out = add_pass_rush(df)
    # row0 is on the false_positives list -> pass forced 0; with a rusher and
    # no kneel it therefore classifies as a rush (nflfastR derives rush AFTER
    # fix_weird_pass_plays). row1 is a normal pass -> untouched.
    assert out["pass"].to_list() == [0, 1]
    assert out["rush"].to_list() == [1, 0]


# ---------------------------------------------------------------------------
# rush — rusher present, no kneel, not a pass
# ---------------------------------------------------------------------------


def test_rush_requires_rusher_no_kneel_no_pass():
    df = pl.DataFrame(
        {
            "game_id": ["2023_01_BUF_NYJ"] * 4,
            "play_id": [1.0, 2.0, 3.0, 4.0],
            "desc": [
                "(10:00) J.Cook left guard for 4 yards.",  # rush
                "(9:20) J.Allen kneels to BUF 40 for -1 yards.",  # kneel -> not a rush
                "(8:40) J.Allen pass deep left to G.Davis for 32 yards.",  # pass -> not a rush
                "(8:00) five yards gained on the play.",  # no rusher -> not a rush
            ],
            "qb_scramble": [0, 0, 0, 0],
            "qb_kneel": [0, 1, 0, 0],
            "kickoff_attempt": [0] * 4,
            "rusher_player_name": ["J.Cook", "J.Allen", None, None],
        }
    )
    out = add_pass_rush(df)
    assert out["rush"].to_list() == [1, 0, 0, 0]
    assert out["pass"].to_list() == [0, 0, 1, 0]


# ---------------------------------------------------------------------------
# degenerate input — passthrough, never raise
# ---------------------------------------------------------------------------


def test_add_pass_rush_noop_without_required_columns():
    df = pl.DataFrame({"game_id": ["2023_01_BUF_NYJ"], "desc": ["some play"]})
    assert_frame_equal(add_pass_rush(df), df)


def test_add_pass_rush_noop_on_empty_frame():
    df = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "play_id": pl.Float64,
            "desc": pl.Utf8,
            "qb_scramble": pl.Int64,
            "qb_kneel": pl.Int64,
            "kickoff_attempt": pl.Int64,
            "rusher_player_name": pl.Utf8,
        }
    )
    assert_frame_equal(add_pass_rush(df), df)


# ---------------------------------------------------------------------------
# qb_dropback (reference §14): 1 when pass == 1 or qb_scramble == 1, else 0.
# ---------------------------------------------------------------------------


def test_qb_dropback_truth_table():
    df = pl.DataFrame(
        {
            "pass": [1, 0, 0],
            "qb_scramble": [0, 1, 0],
        }
    )
    out = add_qb_dropback(df)
    assert out["qb_dropback"].to_list() == [1, 1, 0]
    assert out["qb_dropback"].dtype == pl.Int32


def test_add_qb_dropback_noop_without_required_columns():
    df = pl.DataFrame({"game_id": ["2023_01_BUF_NYJ"]})
    assert_frame_equal(add_qb_dropback(df), df)


def test_add_qb_dropback_noop_on_empty_frame():
    df = pl.DataFrame(schema={"pass": pl.Int64, "qb_scramble": pl.Int64})
    assert_frame_equal(add_qb_dropback(df), df)
