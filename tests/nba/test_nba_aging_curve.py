from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_aging_curve import build_aging_deltas, nba_aging_curve, nba_career_trajectory


def test_build_aging_deltas_recovers_injected_peak() -> None:
    # 3 synthetic players, each rising to a peak at age 26 then declining.
    rows = []
    shape = {22: 0.6, 23: 0.7, 24: 0.8, 25: 0.92, 26: 1.0, 27: 0.95, 28: 0.85, 29: 0.7}
    for pid in ("A", "B", "C"):
        for age, val in shape.items():
            rows.append({"player_id": pid, "age": age, "season_value": val * 10.0, "minutes": 2000.0})
    season_values = pl.DataFrame(rows)

    curve = build_aging_deltas(season_values, min_minutes=500.0)
    assert curve.schema["age"] == pl.Int64
    assert curve.schema["rel_value"] == pl.Float64
    peak_row = curve.filter(pl.col("rel_value") == pl.col("rel_value").max())
    assert peak_row["age"][0] == 26


def test_build_aging_deltas_empty_input() -> None:
    empty = pl.DataFrame(
        schema={"player_id": pl.Utf8, "age": pl.Int64, "season_value": pl.Float64, "minutes": pl.Float64}
    )
    out = build_aging_deltas(empty)
    assert out.height == 0
    assert list(out.schema.keys()) == ["age", "rel_value", "n_pairs"]


def test_nba_aging_curve_loads_bundled_artifact() -> None:
    curve = nba_aging_curve()
    assert curve.schema["age"] == pl.Int64
    assert curve.schema["rel_value"] == pl.Float64
    assert "peak_age" in curve.columns
    assert curve.height > 0


def test_nba_career_trajectory_age_adjusts() -> None:
    curve = nba_aging_curve()
    peak_age = int(curve["peak_age"][0])
    player_values = pl.DataFrame({"player_id": ["1"], "age": [peak_age], "value": [10.0]})
    out = nba_career_trajectory(player_values)
    # at peak age, rel_value == 1.0 -> no adjustment
    assert abs(out["age_adjusted_value"][0] - 10.0) < 1e-9


def test_nba_career_trajectory_empty_input_has_schema() -> None:
    empty = pl.DataFrame(schema={"player_id": pl.Utf8, "age": pl.Int64, "value": pl.Float64})
    out = nba_career_trajectory(empty)
    assert out.height == 0
    assert "age_adjusted_value" in out.columns
    assert "proj_next_value" in out.columns
