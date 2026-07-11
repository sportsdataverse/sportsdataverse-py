import polars as pl

from sportsdataverse.mlb.mlb_fielding_oaa import bip_trajectory_features, catch_prob_surface, mlb_fielding_oaa


def _bip():
    return pl.DataFrame(
        {
            "hc_x": [125.42, 125.42],
            "hc_y": [100.0, 100.0],
            "hit_distance_sc": [330.0, 330.0],
            "launch_angle": [25.0, 25.0],
            "launch_speed": [95.0, 95.0],
            "hit_location": [8, 8],
            "events": ["field_out", "single"],
            "player_name": ["x", "y"],
            "fielder_8": [10, 10],
        }
    )


def test_spray_center_zero_and_surface_rate():
    f = bip_trajectory_features(_bip())
    assert abs(f["spray_angle"][0]) < 1e-6
    s = catch_prob_surface(_bip(), alpha=2.0)
    assert s.height == 1
    row = s.row(0, named=True)
    assert abs(row["p_catch"] - 0.5) < 1e-9


def test_surface_empty_input():
    s = catch_prob_surface(pl.DataFrame(schema={"hc_x": pl.Float64}))
    assert s.height == 0
    assert set(s.columns) == {"position", "dist_b", "spray_b", "la_bin", "p_catch", "n"}


def _oaa_frame():
    # position 8, one bin: fielder 10 converts all 4 into outs (above the
    # bin's shared 50% baseline); fielder 20 converts none (below it).
    n = 4
    return pl.DataFrame(
        {
            "hc_x": [125.42] * (n * 2),
            "hc_y": [100.0] * (n * 2),
            "hit_distance_sc": [330.0] * (n * 2),
            "launch_angle": [25.0] * (n * 2),
            "launch_speed": [95.0] * (n * 2),
            "hit_location": [8] * (n * 2),
            "events": (["field_out"] * n) + (["single"] * n),
            "fielder_8": ([10] * n) + ([20] * n),
        }
    )


def test_oaa_ordering_and_schema():
    out = mlb_fielding_oaa(_oaa_frame())
    assert out.schema["fielder_id"] == pl.Utf8
    assert set(out.columns) == {"fielder_id", "position", "opportunities", "oaa"}
    f10 = out.filter(pl.col("fielder_id") == "10").row(0, named=True)
    f20 = out.filter(pl.col("fielder_id") == "20").row(0, named=True)
    assert f10["opportunities"] == 4 and f20["opportunities"] == 4
    assert f10["oaa"] > 0 > f20["oaa"]


def test_oaa_empty_input_returns_schema():
    out = mlb_fielding_oaa(pl.DataFrame(schema={"hc_x": pl.Float64}))
    assert out.height == 0
    assert set(out.columns) == {"fielder_id", "position", "opportunities", "oaa"}


def test_oaa_return_as_pandas():
    import pandas as pd

    out = mlb_fielding_oaa(_oaa_frame(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
