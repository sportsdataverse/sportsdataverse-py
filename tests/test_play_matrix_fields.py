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

# The processors' own patterns (cfb_pbp.py and nfl_pbp.py both read these), so a change
# to either shows up here.
from sportsdataverse.football.espn_text import PASS_DEPTH_RE as _DEPTH_RE  # noqa: E402
from sportsdataverse.football.espn_text import PASS_DIRECTION_RE as _DIR_RE  # noqa: E402

# Rush direction is still the loose match, inline in both processors.
_RUSH_DIR_RE = r"\s(left|middle|right)\s"


def _apply_matrix_exprs(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pass_depth=pl.when(pl.col("pass") == True).then(pl.col("text").str.extract(_DEPTH_RE, 1)).otherwise(None),
        pass_direction=pl.when(pl.col("pass") == True).then(pl.col("text").str.extract(_DIR_RE, 1)).otherwise(None),
        rush_direction=pl.when(pl.col("rush") == True)
        .then(pl.col("text").str.extract(_RUSH_DIR_RE, 1))
        .otherwise(None),
    )


# ---------------------------------------------------------------------------
# Pass depth + direction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,depth,direction",
    [
        # Real ESPN play text. College, the 2025 vendor template:
        (
            "(00:24) No Huddle-Shotgun #1 T.Castellanos pass incomplete deep right to #0 D.Robinson thrown to CLE30 "
            "broken up by #23 A.Hampton",
            "deep",
            "right",
        ),
        (
            "(10:05) No Huddle-Shotgun #4 L.Fife pass complete short left to #5 D.Faupel caught at NMSU29, for 3 "
            "yards to the NMSU29 (#8 C.Hood)",
            "short",
            "left",
        ),
        # The phrase ends the text: a trailing-space match read neither word
        ("(10:29) Shotgun #8 J.Sharman pass incomplete short right", "short", "right"),
        ("(10:28) No Huddle-Shotgun #12 D.DeShields pass incomplete short", "short", None),
        # The older college wording
        (
            "(7:25) Shotgun JENNINGS, Kevin pass up the middle complete for 14 yards to PITTMAN JR., Randy to the "
            "BC0, 1ST DOWN, TOUCHDOWN SMU, clock 07:25 (Scoring play confirmed).",
            None,
            "middle",
        ),
        # NFL (2006-2025):
        (
            "(14:18) (Shotgun) K.Murray pass short left to T.McBride pushed ob at ARI 36 for 11 yards (I.Yiadom).",
            "short",
            "left",
        ),
        # An incompletion ending on its direction: the loose match lost 5,437 of these, all incompletions
        ("(15:00) (Shotgun) G.Minshew pass incomplete short right.", "short", "right"),
        ("(14:09) J.Allen pass incomplete deep right. thrown away from outside the pocket", "deep", "right"),
        # A scramble is no charted pass, whatever its text says after the run
        ("(5:29) (Shotgun) M.Stafford scrambles right end ran ob at MIN 12 for 5 yards (H.Smith).", None, None),
        (
            "(1:49) (Shotgun) G.Minshew scrambles right end ran ob at MIA 9 for 5 yards (J.Holland). The Replay "
            "Official reviewed the short of the line to gain ruling, and the play was REVERSED.",
            None,
            None,
        ),
        # A scramble then a pass in one text: the pass's own phrase, not the scramble's direction
        (
            "(13:24) J.Cutler scrambles up the middle to MIN 11 for 2 yards. J.Cutler pass incomplete short left "
            "to E.Bennett.",
            "short",
            "left",
        ),
        # 2006's free-text annotation is no charted phrase: "deep" here is the end zone
        (
            "(4:22) J.Kitna pass to M.Furrey for 13 yards, TOUCHDOWN. Pass thrown right corner 5 yards deep end zone.",
            None,
            None,
        ),
        # No depth or direction tokens (a screen, a sack)
        ("pass complete to RB for 4 yards", None, None),
        ("sacked for a loss of 5 yards", None, None),
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
