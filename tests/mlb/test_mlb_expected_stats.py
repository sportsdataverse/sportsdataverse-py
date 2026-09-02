"""Tests for the ① expected-outcomes (xwOBA/xBA/xSLG) engine (T6.2, Phase 1)."""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mlb.mlb_expected_stats import (
    _add_value_columns,
    build_outcome_grid,
    mlb_expected_stats,
    predict_contact_value,
)
from sportsdataverse.mlb.mlb_hitting_constants import GRID


def _bb() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "type": ["X", "X"],
            "events": ["home_run", "field_out"],
            "launch_speed": [100.0, 100.5],
            "launch_angle": [28.0, 28.5],
            "woba_value": [2.0, 0.0],
        }
    )


def test_value_columns_and_grid_cell_mean() -> None:
    g = build_outcome_grid(_add_value_columns(_bb()))
    # both balls fall in the same 2-wide EV/LA cell (100/101 -> ev_bin 40;
    # 28/28.5 -> la_bin 59 after the -90 offset)
    assert g.height == 1
    row = g.row(0, named=True)
    assert row["n"] == 2
    assert abs(row["woba"] - 1.0) < 1e-9  # mean(2.0, 0.0)
    assert abs(row["ba"] - 0.5) < 1e-9  # one hit of two
    assert abs(row["slg"] - 2.0) < 1e-9  # mean(4 bases, 0)


def test_predict_contact_value_dense_cell_gets_cell_mean_sparse_gets_marginal() -> None:
    # dense cell: min_n replicated rows at (ev=100, la=28) -> ev_bin=40, la_bin=59
    n = GRID.min_n if GRID.min_n % 2 == 0 else GRID.min_n + 1
    n_hr = n // 2
    expected_dense_woba = (n_hr * 2.0) / n
    dense = pl.DataFrame(
        {
            "type": ["X"] * n,
            "events": (["home_run"] * n_hr) + (["field_out"] * (n - n_hr)),
            "launch_speed": [100.0] * n,
            "launch_angle": [28.0] * n,
            "woba_value": ([2.0] * n_hr) + ([0.0] * (n - n_hr)),
        }
    )
    # sparse ball: a lone batted ball in an otherwise-empty cell (different LA
    # bin so it doesn't inherit the dense cell's own LA-marginal)
    sparse = pl.DataFrame(
        {
            "type": ["X"],
            "events": ["field_out"],
            "launch_speed": [60.0],
            "launch_angle": [-10.0],
            "woba_value": [0.0],
        }
    )
    bb = pl.concat([dense, sparse])
    bb = _add_value_columns(bb)
    grid = build_outcome_grid(bb)

    pred = predict_contact_value(bb, grid, value="woba")
    assert pred is not None
    assert pred.null_count() == 0
    dense_pred = pred[:n].to_list()
    assert all(abs(v - expected_dense_woba) < 1e-9 for v in dense_pred)  # dense cell = own cell mean
    sparse_pred = pred[n]
    assert sparse_pred is not None  # sparse ball falls back to LA-marginal, never null


def test_mlb_expected_stats_schema_and_pandas() -> None:
    def _fake_puller(start_dt: str, end_dt: str, *, player_type: str = "batter") -> pl.DataFrame:
        return pl.DataFrame(
            {
                "batter": [1, 1, 1, 1],
                "game_date": ["2024-06-01", "2024-06-02", "2024-06-03", "2024-06-04"],
                "type": ["X", "X", "B", "S"],
                "events": ["single", "field_out", "walk", "strikeout"],
                "launch_speed": [95.0, 90.0, None, None],
                "launch_angle": [10.0, 15.0, None, None],
                "woba_value": [0.9, 0.0, 0.7, 0.0],
                "woba_denom": [1.0, 1.0, 1.0, 1.0],
            }
        )

    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_fake_puller)
    assert out.columns == ["batter", "season", "pa", "ab", "xwoba", "xba", "xslg", "woba", "ba"]
    assert out.schema["batter"] == pl.Int64
    assert out.schema["season"] == pl.Int64
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["pa"] == 4
    assert row["season"] == 2024
    import math

    assert math.isfinite(row["xwoba"])

    pdf = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_fake_puller, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)


def test_mlb_expected_stats_empty_pull_returns_documented_schema() -> None:
    def _empty_puller(start_dt: str, end_dt: str, *, player_type: str = "batter") -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "batter": pl.Int64,
                "game_date": pl.Utf8,
                "type": pl.Utf8,
                "events": pl.Utf8,
                "launch_speed": pl.Float64,
                "launch_angle": pl.Float64,
                "woba_value": pl.Float64,
                "woba_denom": pl.Float64,
            }
        )

    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_empty_puller)
    assert out.height == 0
    assert out.columns == ["batter", "season", "pa", "ab", "xwoba", "xba", "xslg", "woba", "ba"]


def _pitchy_puller_factory(rows: dict):
    def _puller(start_dt: str, end_dt: str, *, player_type: str = "batter") -> pl.DataFrame:
        return pl.DataFrame(rows)

    return _puller


def test_pa_counts_plate_appearance_enders_not_pitches() -> None:
    """Regression (2026-09 incident): raw ball/strike pitch rows must not
    inflate pa/ab — a Statcast search pull carries EVERY pitch."""
    rows = {
        "batter": [1] * 10,
        "game_date": ["2024-06-01"] * 10,
        "type": ["B", "S", "X", "B", "S", "B", "S", "S", "B", "S"],
        "events": [None, None, "single", None, "strikeout", None, "walk", None, None, None],
        "launch_speed": [None, None, 95.0, None, None, None, None, None, None, None],
        "launch_angle": [None, None, 10.0, None, None, None, None, None, None, None],
        "woba_value": [None, None, 0.9, None, 0.0, None, 0.7, None, None, None],
        "woba_denom": [None, None, 1.0, None, 1.0, None, 1.0, None, None, None],
    }
    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_pitchy_puller_factory(rows))
    row = out.row(0, named=True)
    assert row["pa"] == 3  # single, strikeout, walk — NOT 10 pitches
    assert row["ab"] == 2  # walk is not an at-bat
    # numerator: predicted woba on the lone BIP (grid mean = 0.9) + K 0.0 + walk 0.7
    assert abs(row["xwoba"] - (0.9 + 0.0 + 0.7) / 3) < 1e-9
    assert abs(row["xba"] - 1.0 / 2) < 1e-9  # predicted ba on the BIP = cell/global mean 1.0


def test_vintage_missing_woba_denom_and_null_walk_value_stays_on_scale() -> None:
    """Regression (2026-09 incident): a cache vintage with NO woba_denom
    column and null woba_value on the walk must not corrupt the scale — the
    denominator is derived from events and the walk gets the fixed fallback."""
    rows = {
        "batter": [1, 1, 1],
        "game_date": ["2024-06-01"] * 3,
        "type": ["X", "S", "B"],
        "events": ["single", "strikeout", "walk"],
        "launch_speed": [95.0, None, None],
        "launch_angle": [10.0, None, None],
        "woba_value": [0.9, 0.0, None],  # walk value missing in this vintage
    }
    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_pitchy_puller_factory(rows))
    row = out.row(0, named=True)
    assert row["pa"] == 3
    assert abs(row["xwoba"] - (0.9 + 0.0 + 0.69) / 3) < 1e-9  # 0.69 = fixed walk fallback


def test_intent_walk_excluded_from_woba_denom_and_ab() -> None:
    rows = {
        "batter": [1, 1],
        "game_date": ["2024-06-01"] * 2,
        "type": ["X", "B"],
        "events": ["single", "intent_walk"],
        "launch_speed": [95.0, None],
        "launch_angle": [10.0, None],
        "woba_value": [0.9, 0.0],
        "woba_denom": [1.0, 0.0],
    }
    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_pitchy_puller_factory(rows))
    row = out.row(0, named=True)
    assert row["pa"] == 2
    assert row["ab"] == 1
    assert abs(row["xwoba"] - 0.9 / 1) < 1e-9  # IBB contributes neither numerator nor denominator


def test_batted_ball_row_without_events_is_ignored_everywhere() -> None:
    """Sourcery review of #421: a type=='X' row with launch data but a
    null/empty events value must not count toward pa (nor the grid) — it is a
    feed artifact, not a plate appearance."""
    rows = {
        "batter": [1, 1, 1],
        "game_date": ["2024-06-01"] * 3,
        "type": ["X", "X", "S"],
        "events": ["single", None, "strikeout"],
        "launch_speed": [95.0, 96.0, None],
        "launch_angle": [10.0, 11.0, None],
        "woba_value": [0.9, None, 0.0],
        "woba_denom": [1.0, None, 1.0],
    }
    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_pitchy_puller_factory(rows))
    row = out.row(0, named=True)
    assert row["pa"] == 2  # the events-less batted ball is not a PA
    assert row["ab"] == 2  # single + strikeout
    assert abs(row["xwoba"] - (0.9 + 0.0) / 2) < 1e-9


def test_untracked_batted_ball_gets_its_realized_outcome_in_xba_and_xslg() -> None:
    """Regression (2026-09 follow-up to #421): a PA-ending ball in play with NO
    launch data cannot be grid-predicted, but it still counts in `ab`. Before
    this fix it contributed a zero numerator, deflating league-mean xBA by
    ~(untracked share x hit rate) — measured .2026 vs an observed BA of .2556
    on the real 2015 season (19.4% untracked). xwOBA already used the realized
    value for these rows; xBA/xSLG now do the same."""
    rows = {
        "batter": [1, 1],
        "game_date": ["2024-06-01"] * 2,
        "type": ["X", "X"],
        "events": ["single", "double"],  # the double has no launch data
        "launch_speed": [95.0, None],
        "launch_angle": [10.0, None],
        "woba_value": [0.9, 1.25],
    }
    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_pitchy_puller_factory(rows))
    row = out.row(0, named=True)
    assert row["ab"] == 2
    # tracked single -> grid mean (1.0 hit, 1.0 base); untracked double -> realized 1 hit, 2 bases
    assert abs(row["xba"] - (1.0 + 1.0) / 2) < 1e-9  # NOT 0.5, which the pre-fix numerator gave
    assert abs(row["xslg"] - (1.0 + 2.0) / 2) < 1e-9


def test_observed_woba_and_ba_ship_beside_the_expected_columns() -> None:
    """The observed columns use the SAME denominators as their x-counterparts,
    so `xwoba - woba` / `xba - ba` is a luck-vs-skill delta with no second
    source."""
    rows = {
        "batter": [1] * 4,
        "game_date": ["2024-06-01"] * 4,
        "type": ["X", "X", "S", "B"],
        "events": ["single", "field_out", "strikeout", "walk"],
        "launch_speed": [95.0, 80.0, None, None],
        "launch_angle": [10.0, -5.0, None, None],
        "woba_value": [0.9, 0.0, 0.0, 0.7],
    }
    out = mlb_expected_stats("2024-06-01", "2024-06-21", puller=_pitchy_puller_factory(rows))
    row = out.row(0, named=True)
    assert row["pa"] == 4 and row["ab"] == 3
    assert abs(row["woba"] - (0.9 + 0.0 + 0.0 + 0.7) / 4) < 1e-9  # realized, all four PA
    assert abs(row["ba"] - 1.0 / 3) < 1e-9  # one real hit in three at-bats
    assert row["xwoba"] is not None and row["xba"] is not None
