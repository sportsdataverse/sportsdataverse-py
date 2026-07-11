"""Unit tests for nfl_ngs_separation_oe (offline, synthetic loader)."""

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_ngs_tracking import _SEP_SCHEMA, nfl_ngs_separation_oe


def _fake_loader(seasons, stat_type, return_as_pandas=False):
    cushion = np.array([4.0, 5.0, 6.0, 7.0, 8.0, 6.0])
    sep = 1.0 + 0.5 * cushion  # exact linear in cushion...
    sep[4] += 2.0  # ...except one big over-expected outlier
    return pl.DataFrame(
        {
            "season": [2023] * 6,
            "week": [0] * 6,
            "player_gsis_id": list(range(6)),
            "player_display_name": list("ABCDEF"),
            "player_position": ["WR"] * 6,
            "team_abbr": ["X"] * 6,
            "targets": [50.0] * 6,
            "avg_cushion": cushion,
            "avg_separation": sep,
            "avg_intended_air_yards": [10.0] * 6,
        }
    )


def test_expected_separation_residual_and_outlier():
    out = nfl_ngs_separation_oe([2023], min_targets=10, _loader=_fake_loader).sort("player_gsis_id")
    assert out.height == 6
    w = out["targets"].to_numpy()
    r = out["sep_oe_raw"].to_numpy()
    assert abs(float(np.average(r, weights=w))) < 1e-6  # residual mean ~ 0
    assert int(np.argmax(np.abs(r))) == 4  # the injected outlier
    assert out.schema["player_gsis_id"] == pl.Utf8 and out.schema["season"] == pl.Int64


def test_identical_features_get_identical_expectation():
    out = nfl_ngs_separation_oe([2023], min_targets=10, _loader=_fake_loader).sort("player_gsis_id")
    e = out["expected_separation"].to_numpy()
    # players C (idx 2) and F (idx 5) share cushion=6, air yards, position
    assert abs(e[2] - e[5]) < 1e-9


def test_empty_input_returns_schema_frame():
    def _empty(seasons, stat_type, return_as_pandas=False):
        return pl.DataFrame()

    out = nfl_ngs_separation_oe([2023], _loader=_empty)
    assert out.height == 0
    assert dict(out.schema) == _SEP_SCHEMA
