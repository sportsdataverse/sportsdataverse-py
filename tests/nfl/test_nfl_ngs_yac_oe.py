"""Unit tests for nfl_ngs_yac_oe (offline, synthetic loader)."""

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_ngs_tracking import _YAC_SCHEMA, nfl_ngs_yac_oe


def _fake_loader(seasons, stat_type, return_as_pandas=False):
    return pl.DataFrame(
        {
            "season": [2023] * 4,
            "week": [0] * 4,
            "player_gsis_id": [1, 2, 3, 4],
            "player_display_name": ["A", "B", "C", "D"],
            "player_position": ["WR"] * 4,
            "team_abbr": ["X"] * 4,
            "receptions": [3.0, 90.0, 80.0, 85.0],
            "avg_yac": [9.0, 6.0, 5.0, 5.5],
            "avg_expected_yac": [5.0, 5.0, 5.0, 5.0],
            "avg_yac_above_expectation": [4.0, 1.0, 0.0, 0.5],
        }
    )


def test_raw_equals_ngs_field_and_shrinks_low_n():
    out = nfl_ngs_yac_oe([2023], min_receptions=10, _loader=_fake_loader).sort("player_gsis_id")
    assert out.height == 4
    # raw is the NGS field passed through, exactly
    assert np.allclose(out["yac_oe_raw"].to_numpy(), [4.0, 1.0, 0.0, 0.5])
    # prior mean over qualified (>= 10 receptions) rows
    mu = np.average([1.0, 0.0, 0.5], weights=[90.0, 80.0, 85.0])
    raw = out["yac_oe_raw"].to_numpy()
    shrunk = out["yac_oe_shrunk"].to_numpy()
    assert np.all(np.abs(shrunk - mu) <= np.abs(raw - mu) + 1e-9)
    # 3-reception extreme shrinks; 90-reception row has the top reliability
    rel = out["reliability"].to_numpy()
    assert abs(shrunk[0] - mu) < abs(raw[0] - mu)
    assert int(np.argmax(rel)) == 1
    # ids pinned Utf8, season Int64
    assert out.schema["player_gsis_id"] == pl.Utf8 and out.schema["season"] == pl.Int64


def test_rank_only_over_qualified_rows():
    out = nfl_ngs_yac_oe([2023], min_receptions=10, _loader=_fake_loader).sort("player_gsis_id")
    # 3-reception row is unranked but still returned
    assert out["yac_oe_rank"][0] is None
    ranked = out.filter(pl.col("yac_oe_rank").is_not_null()).sort("yac_oe_rank")
    # dense desc on shrunk: B (1.0) > D (0.5) > C (0.0)
    assert ranked["player_display_name"].to_list() == ["B", "D", "C"]


def test_empty_input_returns_schema_frame():
    def _empty(seasons, stat_type, return_as_pandas=False):
        return pl.DataFrame()

    out = nfl_ngs_yac_oe([2023], _loader=_empty)
    assert out.height == 0
    assert dict(out.schema) == _YAC_SCHEMA


def test_return_as_pandas():
    out = nfl_ngs_yac_oe([2023], min_receptions=10, return_as_pandas=True, _loader=_fake_loader)
    import pandas as pd

    assert isinstance(out, pd.DataFrame) and len(out) == 4
