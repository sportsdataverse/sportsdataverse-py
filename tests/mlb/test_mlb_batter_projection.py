"""Tests for the ④ batter aging curve + Marcel projection (T6.2, Phase 4)."""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mlb.mlb_batter_projection import aging_curve, marcel_projection, mlb_batter_projection


def _panel() -> pl.DataFrame:
    rows = []
    for pid in range(30):
        for age in range(22, 33):
            base = 0.300 + (min(age, 27) - 22) * 0.010 - max(age - 27, 0) * 0.005
            rows.append({"batter": pid, "season": 2000 + age, "age": age, "xwoba": base, "pa": 500})
    return pl.DataFrame(rows)


def test_aging_curve_peaks_at_27() -> None:
    c = aging_curve(_panel())
    peak_age = c.sort("curve", descending=True)["age"][0]
    assert peak_age == 27


def test_aging_curve_delta_signs() -> None:
    c = aging_curve(_panel())
    by_age = {r["age"]: r["delta"] for r in c.iter_rows(named=True)}
    assert by_age[23] > 0  # still improving pre-peak
    assert by_age[29] < 0  # declining post-peak


def test_marcel_projection_regresses_toward_league_and_excludes_target_season() -> None:
    # batter 1: 3 seasons, all at the SAME raw value (0.330) so the only
    # difference from batter 2 is the amount of supporting evidence (more
    # seasons/PA -> less shrinkage needed for the same raw distance from league).
    history = pl.DataFrame(
        {
            "batter": [1, 1, 1, 2],
            "season": [2021, 2022, 2023, 2023],
            "age": [25, 26, 27, 26],
            "xwoba": [0.330, 0.330, 0.330, 0.330],
            "pa": [500, 550, 600, 600],
        }
    )
    aging = aging_curve(
        pl.DataFrame(
            {
                "batter": list(range(30)) * 2,
                "season": [2020] * 30 + [2021] * 30,
                "age": [25] * 30 + [26] * 30,
                "xwoba": [0.31] * 30 + [0.315] * 30,
                "pa": [500] * 60,
            }
        )
    )
    proj = marcel_projection(history, 2024, aging, league_xwoba=0.315)
    p1 = proj.filter(pl.col("batter") == 1).row(0, named=True)
    p2 = proj.filter(pl.col("batter") == 2).row(0, named=True)
    # regression should pull the projection toward the league mean, between
    # the raw last-season value (0.34) and league mean (0.315)
    assert 0.315 < p1["proj_xwoba"] < 0.340
    dist1 = abs(p1["proj_xwoba"] - 0.315)
    dist2 = abs(p2["proj_xwoba"] - 0.315)
    # batter 2 has only 1 prior season (less info) -> regressed harder (closer to league mean)
    assert dist2 < dist1

    # leakage assert: injecting a season==target_season row must not change the projection
    leaked_history = pl.concat(
        [
            history,
            pl.DataFrame({"batter": [1], "season": [2024], "age": [28], "xwoba": [0.99], "pa": [999]}),
        ]
    )
    proj_leaked = marcel_projection(leaked_history, 2024, aging, league_xwoba=0.315)
    p1_leaked = proj_leaked.filter(pl.col("batter") == 1).row(0, named=True)
    assert abs(p1_leaked["proj_xwoba"] - p1["proj_xwoba"]) < 1e-9


def test_mlb_batter_projection_schema_and_pandas() -> None:
    history = pl.DataFrame(
        {
            "batter": [1, 1, 1],
            "season": [2021, 2022, 2023],
            "age": [25, 26, 27],
            "xwoba": [0.300, 0.310, 0.320],
            "pa": [500, 500, 500],
        }
    )
    out = mlb_batter_projection(2024, history=history)
    assert out.columns == ["batter", "age", "proj_xwoba", "proj_pa"]
    assert out.schema["batter"] == pl.Int64
    assert out.height == 1

    pdf = mlb_batter_projection(2024, history=history, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)


def test_mlb_batter_projection_empty_history_returns_documented_schema() -> None:
    empty_history = pl.DataFrame(
        schema={"batter": pl.Int64, "season": pl.Int64, "age": pl.Int64, "xwoba": pl.Float64, "pa": pl.Int64}
    )
    out = mlb_batter_projection(2024, history=empty_history)
    assert out.height == 0
    assert out.columns == ["batter", "age", "proj_xwoba", "proj_pa"]
