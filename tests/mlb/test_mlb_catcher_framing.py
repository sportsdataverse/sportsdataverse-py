import polars as pl

from sportsdataverse.mlb.mlb_catcher_framing import called_strike_prob_grid, mlb_catcher_framing


def _takes():
    # two takes in the SAME (stand, px_bin, pz_bin): one strike, one ball
    return pl.DataFrame(
        {
            "plate_x": [0.02, 0.03],
            "plate_z": [2.5, 2.5],
            "sz_top": [3.5, 3.5],
            "sz_bot": [1.5, 1.5],
            "stand": ["R", "R"],
            "balls": [0, 0],
            "strikes": [0, 0],
            "description": ["called_strike", "ball"],
            "delta_run_exp": [-0.05, 0.04],
            "fielder_2": [1, 1],
        }
    )


def test_grid_laplace_rate():
    g = called_strike_prob_grid(_takes(), alpha=1.0)
    # one bin, 1 strike / 2 takes, Laplace(1): (1+1)/(2+2) = 0.5
    assert g.height == 1
    row = g.row(0, named=True)
    assert row["n"] == 2 and abs(row["p_strike"] - 0.5) < 1e-9
    assert g.schema["px_bin"] == pl.Int64 and g.schema["p_strike"] == pl.Float64


def test_grid_empty_input():
    g = called_strike_prob_grid(pl.DataFrame(schema={"plate_x": pl.Float64}))
    assert g.height == 0
    assert set(g.columns) == {"stand", "px_bin", "pz_bin", "p_strike", "n"}


def _two_catchers_frame():
    # Both catchers receive takes at the SAME borderline edge location, so the
    # fitted logistic P(strike) there is ~0.5 (in the shadow band). Catcher 1
    # gets those pitches called strikes (positive framing); catcher 2 gets them
    # called balls (negative framing). Same location -> same expected P, so the
    # only difference is the call, isolating the framing signal.
    n = 30
    rows = {
        "plate_x": [0.8] * n * 2,  # near the horizontal edge
        "plate_z": [2.5] * n * 2,  # z_norm = (2.5-1.5)/(3.5-1.5) = 0.5
        "sz_top": [3.5] * n * 2,
        "sz_bot": [1.5] * n * 2,
        "stand": ["R"] * n * 2,
        "balls": [1] * n * 2,
        "strikes": [1] * n * 2,
        "description": (["called_strike"] * n) + (["ball"] * n),
        "delta_run_exp": ([-0.05] * n) + ([0.05] * n),
        "fielder_2": ([1] * n) + ([2] * n),
    }
    return pl.DataFrame(rows)


def test_framing_runs_catcher_ordering_and_schema():
    out = mlb_catcher_framing(_two_catchers_frame())
    assert out.schema["catcher_id"] == pl.Utf8
    assert out.schema["framing_runs"] == pl.Float64
    assert set(out.columns) == {"catcher_id", "takes", "framing_runs", "strikes_gained"}
    c1 = out.filter(pl.col("catcher_id") == "1").row(0, named=True)
    c2 = out.filter(pl.col("catcher_id") == "2").row(0, named=True)
    # catcher 1 stole strikes on low pitches (framing positive);
    # catcher 2 gave up strikes in the zone (framing negative or lower than c1).
    assert c1["framing_runs"] > c2["framing_runs"]


def test_framing_empty_input_returns_schema():
    out = mlb_catcher_framing(pl.DataFrame(schema={"plate_x": pl.Float64}))
    assert out.height == 0
    assert set(out.columns) == {"catcher_id", "takes", "framing_runs", "strikes_gained"}


def test_framing_return_as_pandas():
    import pandas as pd

    out = mlb_catcher_framing(_two_catchers_frame(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
