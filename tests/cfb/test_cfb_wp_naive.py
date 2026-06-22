"""Tests for the spread-free (naive) win-probability surface.

Three layers:

* ``test_wp_naive_model_bundled`` -- the ``wp_naive.ubj`` artifact ships, loads,
  and exposes exactly the 12 naive features (offline).
* ``test_apply_wp_derivation_*`` -- the shared ``_apply_wp_derivation`` helper
  emits the right suffixed columns with the documented identities, for both the
  spread (``suffix=""``) and naive (``suffix="_naive"``) routes (offline, synthetic).
* ``test_live_pipeline_emits_naive_wp`` -- a real game grows ``wp_*_naive`` columns
  that are valid probabilities (live-gated).
"""

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import (
    CFBPlayProcess,
    _apply_wp_derivation,
    wp_naive_model,
)
from sportsdataverse.cfb.model_vars import kickoff_vec, wp_naive_final_names
from tests.conftest import fetch_pbp_or_skip


def test_wp_naive_model_bundled():
    """The naive booster is packaged and is the 12-feat spread-free model."""
    assert wp_naive_model.num_features() == 12
    assert list(wp_naive_model.feature_names) == wp_naive_final_names
    assert "spread_time" not in wp_naive_final_names


def _synthetic_play_df():
    """Three rows exercising the plain path + the kickoff touchback substitution,
    with every other branch of the derivation deliberately *not* firing (so the
    expected values are closed-form)."""
    return pl.DataFrame(
        {
            "type.text": ["Rush", kickoff_vec[0], "Pass"],
            "start.pos_team.id": [1, 1, 1],
            "homeTeamId": [1, 1, 1],
            "status_type_completed": [False, False, False],
            "lead_play_type": ["Rush", "Pass", "Rush"],
            "game_play_number": [1, 2, 3],
            "pos_score_diff_end": [3, 0, -3],
            "end_of_half": [False, False, False],
            "lead_pos_team": [1, 1, 1],
            "end.pos_team.id": [1, 1, 1],
            "change_of_pos_team": [False, False, False],
            "kickoff_onside": [False, False, False],
            "scoringPlay": [False, False, False],
            "start.pos_team_receives_2H_kickoff": [False, False, False],
        }
    )


def test_apply_wp_derivation_spread_identities():
    """suffix='' emits the canonical columns with wp_before/after/wpa identities,
    including the kickoff -> touchback substitution on row 1."""
    df = _synthetic_play_df()
    before = np.array([0.60, 0.50, 0.40])
    touchback = np.array([0.55, 0.55, 0.55])
    after = np.array([0.70, 0.50, 0.30])

    out = _apply_wp_derivation(df, before, touchback, after, suffix="")

    # kickoff row picks up the touchback WP; others keep their start WP.
    assert out["wp_before"].to_list() == pytest.approx([0.60, 0.55, 0.40])
    # no special end-state branch fires -> wp_after is the raw end prediction.
    assert out["wp_after"].to_list() == pytest.approx([0.70, 0.50, 0.30])
    # wpa = wp_after - wp_before (post-substitution).
    assert out["wpa"].to_list() == pytest.approx([0.10, -0.05, -0.10])
    # def = 1 - offense; home (id==homeTeamId) tracks the offense WP.
    assert out["def_wp_before"].to_list() == pytest.approx([0.40, 0.45, 0.60])
    assert out["home_wp_before"].to_list() == pytest.approx([0.60, 0.55, 0.40])


def test_apply_wp_derivation_naive_suffix_independent():
    """suffix='_naive' emits the same column family under the _naive suffix,
    driven by its own raw arrays and leaving the spread columns absent."""
    df = _synthetic_play_df()
    before_n = np.array([0.58, 0.52, 0.42])
    touchback_n = np.array([0.50, 0.50, 0.50])
    after_n = np.array([0.66, 0.52, 0.34])

    out = _apply_wp_derivation(df, before_n, touchback_n, after_n, suffix="_naive")

    for col in ("wp_before_naive", "wp_after_naive", "wpa_naive", "def_wp_before_naive", "home_wp_after_naive"):
        assert col in out.columns
    # the un-suffixed spread columns are NOT produced by the naive route.
    assert "wp_before" not in out.columns
    assert out["wp_before_naive"].to_list() == pytest.approx([0.58, 0.50, 0.42])
    assert out["wpa_naive"].to_list() == pytest.approx([0.08, 0.02, -0.08])


def test_live_pipeline_emits_naive_wp():
    """A real game grows wp_*_naive columns that are valid probabilities."""
    proc = CFBPlayProcess(gameId=401628334)
    fetch_pbp_or_skip(proc)
    proc.run_processing_pipeline()
    df = pl.DataFrame(proc.plays_json, infer_schema_length=None)

    for col in ("wp_before_naive", "wp_after_naive", "wpa_naive"):
        assert col in df.columns, f"{col} missing from processed plays"
        assert df[col].null_count() == 0, f"{col} has nulls"

    for col in ("wp_before_naive", "wp_after_naive"):
        assert df[col].min() >= 0.0 and df[col].max() <= 1.0, f"{col} out of [0,1]"

    # naive is a distinct surface from spread, not a copy.
    assert not df["wp_before"].equals(df["wp_before_naive"])
