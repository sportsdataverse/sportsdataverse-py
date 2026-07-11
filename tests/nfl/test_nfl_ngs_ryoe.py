"""Unit tests for nfl_ngs_ryoe (offline, synthetic loader)."""

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_ngs_tracking import _RYOE_SCHEMA, nfl_ngs_ryoe


def _fake_loader(seasons, stat_type, return_as_pandas=False):
    return pl.DataFrame(
        {
            "season": [2023] * 4,
            "week": [0] * 4,
            "player_gsis_id": [1, 2, 3, 4],
            "player_display_name": ["A", "B", "C", "D"],
            "player_position": ["RB"] * 4,
            "team_abbr": ["X"] * 4,
            "rush_attempts": [5.0, 250.0, 220.0, 240.0],
            "rush_yards": [60.0, 1100.0, 900.0, 1000.0],
            "expected_rush_yards": [30.0, 1050.0, 920.0, 980.0],
            "rush_yards_over_expected": [30.0, 50.0, -20.0, 20.0],
            "rush_yards_over_expected_per_att": [6.0, 0.2, -0.09, 0.08],
            "percent_attempts_gte_eight_defenders": [10.0, 25.0, 30.0, 20.0],
        }
    )


def test_raw_equals_ngs_field_and_shrinks_low_n():
    out = nfl_ngs_ryoe([2023], min_attempts=20, _loader=_fake_loader).sort("player_gsis_id")
    assert out.height == 4
    assert np.allclose(out["ryoe_per_att_raw"].to_numpy(), [6.0, 0.2, -0.09, 0.08])
    assert np.allclose(out["ryoe_total"].to_numpy(), [30.0, 50.0, -20.0, 20.0])
    assert np.allclose(out["pct_stacked_box"].to_numpy(), [10.0, 25.0, 30.0, 20.0])
    mu = np.average([0.2, -0.09, 0.08], weights=[250.0, 220.0, 240.0])
    raw = out["ryoe_per_att_raw"].to_numpy()
    shrunk = out["ryoe_per_att_shrunk"].to_numpy()
    assert np.all(np.abs(shrunk - mu) <= np.abs(raw - mu) + 1e-9)
    # 5-attempt extreme shrinks hardest
    rel = out["reliability"].to_numpy()
    assert int(np.argmin(rel)) == 0
    assert abs(shrunk[0] - mu) < abs(raw[0] - mu)
    assert out.schema["player_gsis_id"] == pl.Utf8 and out.schema["season"] == pl.Int64


def test_rank_only_over_qualified_rows():
    out = nfl_ngs_ryoe([2023], min_attempts=20, _loader=_fake_loader).sort("player_gsis_id")
    assert out["ryoe_rank"][0] is None  # 5 attempts: unranked, still returned
    ranked = out.filter(pl.col("ryoe_rank").is_not_null()).sort("ryoe_rank")
    assert ranked["player_display_name"].to_list() == ["B", "D", "C"]


def test_empty_input_returns_schema_frame():
    def _empty(seasons, stat_type, return_as_pandas=False):
        return pl.DataFrame()

    out = nfl_ngs_ryoe([2023], _loader=_empty)
    assert out.height == 0
    assert dict(out.schema) == _RYOE_SCHEMA
