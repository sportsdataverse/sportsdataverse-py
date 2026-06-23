"""Tests for the CFB completion-probability (cp/cpoe) + expected-pass (xpass/pass_oe) surface.

Two layers:

* ``test_cp_model_bundled`` / ``test_xpass_model_bundled`` -- the two boosters ship,
  load, and expose exactly the documented 8 / 7 features in the expected order (offline).
* ``test_live_pipeline_emits_cpoe_xpass`` -- a real game grows ``cp`` / ``cpoe`` /
  ``xpass`` / ``pass_oe`` columns; ``cp`` / ``xpass`` are valid probabilities on the
  right play subsets and null elsewhere, and ``cpoe`` / ``pass_oe`` are finite where
  defined (live-gated).
"""

import math

import polars as pl

from sportsdataverse.cfb.cfb_pbp import (
    CFBPlayProcess,
    CP_FEATURES,
    XPASS_FEATURES,
    cp_model,
    xpass_model,
)
from tests.conftest import fetch_pbp_or_skip


def test_cp_model_bundled():
    """The completion-probability booster is packaged and is the 8-feat model."""
    assert cp_model.num_features() == 8
    assert list(cp_model.feature_names) == CP_FEATURES
    assert CP_FEATURES == [
        "down",
        "distance",
        "yards_to_goal",
        "score_diff",
        "seconds_remaining",
        "is_home",
        "period",
        "passing_down",
    ]


def test_xpass_model_bundled():
    """The expected-pass booster is packaged and is the 7-feat model."""
    assert xpass_model.num_features() == 7
    assert list(xpass_model.feature_names) == XPASS_FEATURES
    assert XPASS_FEATURES == [
        "down",
        "distance",
        "yards_to_goal",
        "pos_score_diff",
        "TimeSecsRem",
        "era",
        "period",
    ]


def test_live_pipeline_emits_cpoe_xpass():
    """A real game grows cp/cpoe + xpass/pass_oe with the documented null masks."""
    proc = CFBPlayProcess(gameId=401628334)
    fetch_pbp_or_skip(proc)
    proc.run_processing_pipeline()
    df = pl.DataFrame(proc.plays_json, infer_schema_length=None)

    for col in ("cp", "cpoe", "xpass", "pass_oe"):
        assert col in df.columns, f"{col} missing from processed plays"

    # ---- cp / cpoe: defined exactly on pass plays ----
    pass_mask = pl.col("pass") == True  # noqa: E712
    cp_on_pass = df.filter(pass_mask)["cp"]
    cp_off_pass = df.filter(~pass_mask)["cp"]
    assert cp_on_pass.null_count() == 0, "cp null on some pass plays"
    assert cp_off_pass.drop_nulls().len() == 0, "cp non-null on a non-pass play"
    assert cp_on_pass.min() >= 0.0 and cp_on_pass.max() <= 1.0, "cp out of [0,1]"
    # cpoe defined exactly where cp is.
    cpoe_def = df.filter(pl.col("cp").is_not_null())["cpoe"]
    assert cpoe_def.null_count() == 0, "cpoe null where cp defined"
    assert all(math.isfinite(v) for v in cpoe_def.to_list()), "cpoe non-finite where defined"
    assert df.filter(pl.col("cp").is_null())["cpoe"].drop_nulls().len() == 0

    # ---- xpass / pass_oe: defined exactly on scrimmage rush-or-pass plays ----
    scrim_mask = (pl.col("pass") == True) | (pl.col("rush") == True)  # noqa: E712
    xp_on = df.filter(scrim_mask)["xpass"]
    xp_off = df.filter(~scrim_mask)["xpass"]
    assert xp_on.null_count() == 0, "xpass null on some scrimmage plays"
    assert xp_off.drop_nulls().len() == 0, "xpass non-null on a non-scrimmage play"
    assert xp_on.min() >= 0.0 and xp_on.max() <= 1.0, "xpass out of [0,1]"
    pass_oe_def = df.filter(pl.col("xpass").is_not_null())["pass_oe"]
    assert pass_oe_def.null_count() == 0, "pass_oe null where xpass defined"
    assert all(math.isfinite(v) for v in pass_oe_def.to_list()), "pass_oe non-finite where defined"
    assert df.filter(pl.col("xpass").is_null())["pass_oe"].drop_nulls().len() == 0
