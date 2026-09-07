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
    CP_AIR_YARDS_FEATURES,
    CP_FEATURES,
    XPASS_FEATURES,
    cp_air_yards_model,
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


def test_cp_air_yards_model_bundled():
    """The air-yards booster ships and is the 11-feat superset of CP_FEATURES."""
    assert cp_air_yards_model.num_features() == 11
    assert list(cp_air_yards_model.feature_names) == CP_AIR_YARDS_FEATURES
    # A superset, not a replacement: the game-state features are retained and
    # keep their order, so the two models stay comparable feature-for-feature.
    assert CP_AIR_YARDS_FEATURES[:8] == CP_FEATURES
    assert CP_AIR_YARDS_FEATURES[8:] == ["air_yards", "pass_is_middle", "qb_hurry"]


def _cpoe_frame(air_yards, *, direction="middle", with_air_cols=True):
    """Minimal frame carrying every column __process_cpoe reads."""
    n = len(air_yards)
    cols = {
        "start.down": [1] * n,
        "start.distance": [10] * n,
        "start.yardsToEndzone": [70] * n,
        "pos_score_diff_start": [0] * n,
        "start.TimeSecsRem": [1800] * n,
        "start.is_home": [True] * n,
        "period": [1] * n,
        "passing_down": [False] * n,
        "pass": [True] * n,
        "completion": [1] * n,
    }
    if with_air_cols:
        cols["air_yards"] = air_yards
        cols["pass_direction"] = [direction] * n
        cols["qb_hurry"] = [False] * n
    return pl.DataFrame(cols)


def _score(df):
    proc = CFBPlayProcess(gameId=1)
    return proc._CFBPlayProcess__process_cpoe(df)


def test_cpoe_routes_per_play_and_records_which_model():
    """Rows with air yards use the air booster; the rest fall back."""
    out = _score(_cpoe_frame([12, None, 4, None]))
    assert out["cp_model"].to_list() == [
        "air_yards",
        "game_state",
        "air_yards",
        "game_state",
    ]
    assert out["cp"].null_count() == 0
    assert out["cp"].min() >= 0.0 and out["cp"].max() <= 1.0


def test_cpoe_falls_back_when_air_columns_are_absent():
    """A pre-2025 frame has no air-yards columns at all and must still score."""
    out = _score(_cpoe_frame([None, None], with_air_cols=False))
    assert out["cp_model"].to_list() == ["game_state", "game_state"]
    assert out["cp"].null_count() == 0


def test_cpoe_air_arm_separates_deep_from_short_throws():
    """The air booster must actually use throw depth, not ignore it.

    A 45-yard throw and a 1-yard throw in identical game state should not get
    the same completion probability -- if they do, the feature is wired in but
    inert, which is exactly the failure the game-state model had.
    """
    out = _score(_cpoe_frame([1, 45]))
    assert out["cp_model"].to_list() == ["air_yards", "air_yards"]
    short_cp, deep_cp = out["cp"].to_list()
    assert short_cp > deep_cp, f"short {short_cp:.3f} should beat deep {deep_cp:.3f}"
    assert short_cp - deep_cp > 0.10, "throw depth barely moved cp"


def test_cp_game_state_is_the_aggregatable_series():
    """cp_game_state must be one consistent scale on EVERY pass play.

    The box score sums it into xComp. If it were the hybrid, a passer whose
    plays are partly air-yards-scored would have two different quantities added
    together, and passers would stop being comparable to each other because
    air-yards coverage varies by game.
    """
    out = _score(_cpoe_frame([12, None, 4, None]))

    # present and non-null on every pass play, whichever model won
    assert out["cp_game_state"].null_count() == 0
    assert out["cp_model"].to_list() == ["air_yards", "game_state", "air_yards", "game_state"]

    # on fallback rows the two agree; on air-yards rows cp is the better number
    for i in (1, 3):
        assert out["cp"][i] == out["cp_game_state"][i]
    assert any(out["cp"][i] != out["cp_game_state"][i] for i in (0, 2)), (
        "cp never diverged from cp_game_state -- the air-yards arm is inert"
    )
