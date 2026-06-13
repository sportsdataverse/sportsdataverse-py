"""Unit tests for play enrichment columns added to CFB and NFL PBP processors.

Covers two families of additions:
  1. Pass/rush matrix fields (pass_depth, pass_direction, rush_direction) that power
     the Game on Paper pass-target matrix and rush-direction matrix.
  2. nflfastR-compatible scoring event result columns (field_goal_result,
     extra_point_result, two_point_conv_result) derived from ESPN's structured
     scoringType and pointAfterAttempt API fields.

All logic is tested with synthetic Polars DataFrames (no network required) plus
column-presence integration tests against cached fixtures.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEPTH_RE = r"\s(short|deep)\s"
_DIR_RE = r"\s(left|middle|right)\s"


def _apply_matrix_exprs(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pass_depth=pl.when(pl.col("pass") == True).then(pl.col("text").str.extract(_DEPTH_RE, 1)).otherwise(None),
        pass_direction=pl.when(pl.col("pass") == True).then(pl.col("text").str.extract(_DIR_RE, 1)).otherwise(None),
        rush_direction=pl.when(pl.col("rush") == True).then(pl.col("text").str.extract(_DIR_RE, 1)).otherwise(None),
    )


# ---------------------------------------------------------------------------
# Pass depth + direction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,depth,direction",
    [
        ("pass complete short left to WR for 7 yards", "short", "left"),
        ("pass complete short middle to RB for 3 yards", "short", "middle"),
        ("pass complete short right for 12 yards", "short", "right"),
        ("pass complete deep left to TE for 45 yards", "deep", "left"),
        ("pass complete deep middle for 38 yards", "deep", "middle"),
        ("pass incomplete deep right intended for WR", "deep", "right"),
        # No depth keyword → null depth, direction still present
        ("pass complete to WR left for 7 yards", None, "left"),
        # No direction keyword → depth present, direction null
        ("pass complete deep to TE for 30 yards", "deep", None),
        # Sack: neither keyword
        ("sacked for a loss of 5 yards", None, None),
        # Screen: no qualifying depth/direction tokens
        ("pass complete to RB for 4 yards", None, None),
    ],
)
def test_pass_depth_direction(text: str, depth, direction):
    df = pl.DataFrame({"text": [text], "pass": [True], "rush": [False]})
    result = _apply_matrix_exprs(df)
    assert result["pass_depth"][0] == depth, f"text={text!r}"
    assert result["pass_direction"][0] == direction, f"text={text!r}"


def test_pass_fields_null_for_non_pass():
    """Rush plays must get null pass_depth and pass_direction."""
    df = pl.DataFrame({"text": ["run short left for 5 yards"], "pass": [False], "rush": [True]})
    result = _apply_matrix_exprs(df)
    assert result["pass_depth"][0] is None
    assert result["pass_direction"][0] is None


# ---------------------------------------------------------------------------
# Rush direction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,direction",
    [
        ("run right for 5 yards", "right"),
        ("run left for -1 yards", "left"),
        ("rush middle for 0 yards", "middle"),
        # QB sneak / kneel — no qualifying direction token
        ("quarterback kneel for -1 yards", None),
        ("rush for 3 yards", None),
    ],
)
def test_rush_direction(text: str, direction):
    df = pl.DataFrame({"text": [text], "pass": [False], "rush": [True]})
    result = _apply_matrix_exprs(df)
    assert result["rush_direction"][0] == direction, f"text={text!r}"


def test_rush_direction_null_for_non_rush():
    """Pass plays must get null rush_direction."""
    df = pl.DataFrame({"text": ["pass complete short left for 10 yards"], "pass": [True], "rush": [False]})
    result = _apply_matrix_exprs(df)
    assert result["rush_direction"][0] is None


# ---------------------------------------------------------------------------
# Column-presence integration test: CFB pipeline (offline fixture)
# ---------------------------------------------------------------------------

_CFB_FIX = Path(__file__).parent / "cfb" / "fixtures" / "summary_401628455.json"


@pytest.mark.skipif(not _CFB_FIX.exists(), reason="CFB fixture not present")
def test_cfb_pipeline_produces_matrix_columns(monkeypatch):
    """Full CFB pipeline adds pass_depth, pass_direction, rush_direction to plays."""
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    summary = json.loads(_CFB_FIX.read_text())

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(
        gameId=401628455,
        odds_override={"gameSpread": -7.5, "overUnder": 52.5, "homeFavorite": True, "gameSpreadAvailable": True},
    )
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()

    df = pl.DataFrame(proc.plays_json, infer_schema_length=400)
    assert "pass_depth" in df.columns
    assert "pass_direction" in df.columns
    assert "rush_direction" in df.columns

    # pass_depth / pass_direction only populated for pass plays
    pass_rows = df.filter(pl.col("pass") == True)
    if len(pass_rows) > 0:
        non_null_depth = pass_rows["pass_depth"].drop_nulls()
        if len(non_null_depth) > 0:
            assert non_null_depth.is_in(["short", "deep"]).all()
        non_null_dir = pass_rows["pass_direction"].drop_nulls()
        if len(non_null_dir) > 0:
            assert non_null_dir.is_in(["left", "middle", "right"]).all()

    # rush_direction only populated for rush plays
    rush_rows = df.filter(pl.col("rush") == True)
    if len(rush_rows) > 0:
        non_null_rush_dir = rush_rows["rush_direction"].drop_nulls()
        if len(non_null_rush_dir) > 0:
            assert non_null_rush_dir.is_in(["left", "middle", "right"]).all()

    # non-pass plays must have null pass_depth / pass_direction
    non_pass = df.filter(pl.col("pass") == False)
    assert non_pass["pass_depth"].null_count() == len(non_pass)
    assert non_pass["pass_direction"].null_count() == len(non_pass)

    # non-rush plays must have null rush_direction
    non_rush = df.filter(pl.col("rush") == False)
    assert non_rush["rush_direction"].null_count() == len(non_rush)


# ---------------------------------------------------------------------------
# Scoring event result columns
# ---------------------------------------------------------------------------


def _apply_fg_result(df: pl.DataFrame) -> pl.DataFrame:
    """Mirrors the field_goal_result logic from __add_play_category_flags."""
    return df.with_columns(
        field_goal_result=pl.when(pl.col("fg_attempt") == True)
        .then(
            pl.when(pl.col("fg_made") == True)
            .then(pl.lit("made"))
            .when(pl.col("type.text").str.contains(r"(?i)blocked"))
            .then(pl.lit("blocked"))
            .otherwise(pl.lit("missed"))
        )
        .otherwise(None)
    )


def _apply_pat_results(df: pl.DataFrame) -> pl.DataFrame:
    """Mirrors the extra_point_result / two_point_conv_result logic."""
    return df.with_columns(
        extra_point_result=pl.when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)extra point"))
        .then(
            pl.when(pl.col("pointAfterAttempt.value") == 1.0)
            .then(pl.lit("good"))
            .when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)block"))
            .then(pl.lit("blocked"))
            .otherwise(pl.lit("failed"))
        )
        .otherwise(None),
        two_point_conv_result=pl.when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)two.?point"))
        .then(pl.when(pl.col("pointAfterAttempt.value") == 2.0).then(pl.lit("success")).otherwise(pl.lit("failure")))
        .otherwise(None),
    )


@pytest.mark.parametrize(
    "fg_attempt,fg_made,type_text,expected",
    [
        (True, True, "Field Goal Good", "made"),
        (True, False, "Field Goal Missed", "missed"),
        (True, False, "Field Goal Blocked", "blocked"),
        (False, False, "Rush", None),
        (False, False, "Passing Touchdown", None),
    ],
)
def test_field_goal_result(fg_attempt: bool, fg_made: bool, type_text: str, expected):
    df = pl.DataFrame({"fg_attempt": [fg_attempt], "fg_made": [fg_made], "type.text": [type_text]})
    result = _apply_fg_result(df)
    assert result["field_goal_result"][0] == expected, f"type_text={type_text!r}"


@pytest.mark.parametrize(
    "abbrev,value,expected_ep,expected_2pt",
    [
        # Extra point made
        ("Extra Point Good", 1.0, "good", None),
        # Failed extra point (no-good)
        ("Extra Point No Good", 0.0, "failed", None),
        # Blocked extra point
        ("Extra Point Blocked", 0.0, "blocked", None),
        # Successful two-point conversion via pass
        ("Two Point Pass", 2.0, None, "success"),
        # Failed two-point conversion via pass
        ("Two Point Pass", 0.0, None, "failure"),
        # Successful two-point conversion via rush
        ("Two Point Rush", 2.0, None, "success"),
        # Non-TD play has null pointAfterAttempt
        (None, None, None, None),
    ],
)
def test_pat_results(abbrev, value, expected_ep, expected_2pt):
    df = pl.DataFrame(
        {
            "pointAfterAttempt.abbreviation": pl.Series([abbrev], dtype=pl.String),
            "pointAfterAttempt.value": pl.Series([value], dtype=pl.Float64),
        }
    )
    result = _apply_pat_results(df)
    assert result["extra_point_result"][0] == expected_ep, f"abbrev={abbrev!r} value={value}"
    assert result["two_point_conv_result"][0] == expected_2pt, f"abbrev={abbrev!r} value={value}"


@pytest.mark.skipif(not _CFB_FIX.exists(), reason="CFB fixture not present")
def test_cfb_pipeline_produces_scoring_result_columns(monkeypatch):
    """Full CFB pipeline adds field_goal_result, extra_point_result, two_point_conv_result."""
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    summary = json.loads(_CFB_FIX.read_text())

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(
        gameId=401628455,
        odds_override={"gameSpread": -7.5, "overUnder": 52.5, "homeFavorite": True, "gameSpreadAvailable": True},
    )
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()

    df = pl.DataFrame(proc.plays_json, infer_schema_length=400)

    assert "field_goal_result" in df.columns
    # Values must be in the allowed set (or null for non-FG plays)
    fg_rows = df.filter(pl.col("fg_attempt") == True)
    if len(fg_rows) > 0:
        fgr = fg_rows["field_goal_result"].drop_nulls()
        assert fgr.is_in(["made", "missed", "blocked"]).all()

    # extra_point_result and two_point_conv_result only present when ESPN provides
    # pointAfterAttempt data (modern API).
    if "extra_point_result" in df.columns:
        ep_vals = df["extra_point_result"].drop_nulls()
        if len(ep_vals) > 0:
            assert ep_vals.is_in(["good", "failed", "blocked"]).all()
    if "two_point_conv_result" in df.columns:
        tpc_vals = df["two_point_conv_result"].drop_nulls()
        if len(tpc_vals) > 0:
            assert tpc_vals.is_in(["success", "failure"]).all()
