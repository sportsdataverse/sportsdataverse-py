"""Unit tests for OL/DL pressure rates + opponent adjustment (Tasks 5.1/5.2).

NOTE: sportsdataverse.nfl.__init__ re-exports the function
``nfl_line_grades`` which shadows the module attribute of the same name, so
these tests import the module via importlib.import_module.
"""

import importlib

import polars as pl

lg = importlib.import_module("sportsdataverse.nfl.nfl_line_grades")


def _pbp() -> pl.DataFrame:
    # 4 dropbacks: A allows 1 pressure on 2 dropbacks; B allows 2 on 2.
    return pl.DataFrame(
        {
            "game_id": ["G1", "G1", "G1", "G1"],
            "season": [2023] * 4,
            "posteam": ["A", "A", "B", "B"],
            "defteam": ["B", "B", "A", "A"],
            "qb_dropback": [1, 1, 1, 1],
            "sack": [1, 0, 1, 0],
            "qb_hit": [0, 0, 1, 1],
        }
    )


def test_raw_pressure_rates():
    out = lg.team_pressure_rates(_pbp()).sort("team")
    a = out.row(0, named=True)
    b = out.row(1, named=True)
    assert a["team"] == "A" and b["team"] == "B"
    assert a["dropbacks_off"] == 2 and a["pressures_allowed"] == 1
    assert abs(a["pressure_rate_allowed"] - 0.5) < 1e-9
    # A's defense generated 2 pressures on B's 2 dropbacks
    assert a["dropbacks_def"] == 2 and a["pressures_generated"] == 2
    assert abs(a["pressure_rate_generated"] - 1.0) < 1e-9


def test_opponent_adjustment_direction():
    # A allowed 0.5 but faced only elite rush D (raw gen 0.9 vs league ~0.5):
    # opponent-adjusted allowed for A must improve (drop below raw).
    pairs = pl.DataFrame(
        {
            "season": [2023] * 4,
            "off_team": ["A", "B", "C", "C"],
            "def_team": ["D", "C", "B", "D"],
            "dropbacks": [10, 10, 10, 10],
            "pressures": [5, 3, 2, 9],
        }
    )
    adj = lg.adjust_pressure_pairs(pairs)
    a = adj.filter(pl.col("team") == "A").row(0, named=True)
    assert a["adj_pressure_rate_allowed"] < a["pressure_rate_allowed"]


def test_line_grades_shrink_and_center():
    rates = pl.DataFrame(
        {
            "season": [2023] * 3,
            "team": ["A", "B", "C"],
            "dropbacks_off": [300, 300, 300],
            "pressures_allowed": [60, 90, 120],
            "pressure_rate_allowed": [0.2, 0.3, 0.4],
            "dropbacks_def": [300, 300, 10],
            "pressures_generated": [120, 90, 2],
            "pressure_rate_generated": [0.4, 0.3, 0.2],
            "adj_pressure_rate_allowed": [0.2, 0.3, 0.4],
            "adj_pressure_rate_generated": [0.4, 0.3, 0.2],
        }
    )
    out = lg._line_grades_from(rates).sort("team")
    a = out.row(0, named=True)
    c = out.row(2, named=True)
    # best pass block (lowest allowed) -> highest OL grade; best rush -> highest DL grade
    assert a["ol_pass_block_grade"] == out["ol_pass_block_grade"].max()
    assert a["dl_pass_rush_grade"] == out["dl_pass_rush_grade"].max()
    # grades centered near 50
    assert abs(out["ol_pass_block_grade"].mean() - 50.0) < 10.0
    # tiny-sample defense shrunk toward 50 relative to its unshrunk z
    assert c["dl_pass_rush_grade"] > 20.0


def test_line_grades_empty():
    out = lg._line_grades_from(lg.team_pressure_rates(_pbp().head(0)))
    assert out.height == 0
    assert "ol_pass_block_grade" in out.columns
