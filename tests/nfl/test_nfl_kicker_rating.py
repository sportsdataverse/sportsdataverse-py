"""Unit tests for the environment-adjusted kicker rating (Tasks 3.1/3.3).

NOTE: sportsdataverse.nfl.__init__ re-exports the function
``nfl_kicker_rating`` which shadows the module attribute of the same name,
so these tests import the module via importlib.import_module.
"""

import importlib

import numpy as np
import polars as pl

k = importlib.import_module("sportsdataverse.nfl.nfl_kicker_rating")


def _fg_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "yardline_100": [37.0],
            "roof": ["outdoors"],
            "fg_roof": [1.0],
            "temp": [40.0],
            "wind": [12.0],
            "home_team": ["DEN"],
            "era0": [0.0],
            "era1": [0.0],
            "era2": [0.0],
            "era3": [0.0],
            "era4": [1.0],
        }
    )


def test_public_fg_wrapper_matches_private():
    from sportsdataverse.nfl.nfl_fourth_down import _fg_make_prob, fg_make_probability

    yl = np.array([37.0])
    roof = np.array([1.0])
    era = np.array([[0.0, 0.0, 0.0, 0.0, 1.0]])
    pub = fg_make_probability(yl, roof, era)
    priv = _fg_make_prob(yl, roof, era)
    assert pub is not None and priv is not None
    assert np.allclose(pub, priv)


def test_env_neutral_when_coef_zero(monkeypatch):
    monkeypatch.setattr(
        k,
        "ENVIRONMENT_FG_COEF",
        {"wind": 0.0, "temp": 0.0, "altitude_kft": 0.0, "temp_baseline": 60.0},
    )
    out = k.env_adjusted_make_prob(_fg_df())
    assert abs(out["exp_make_prob"][0] - out["base_make_prob"][0]) < 1e-9


def test_env_altitude_sign(monkeypatch):
    monkeypatch.setattr(
        k,
        "ENVIRONMENT_FG_COEF",
        {"wind": 0.0, "temp": 0.0, "altitude_kft": 0.5, "temp_baseline": 60.0},
    )
    out = k.env_adjusted_make_prob(_fg_df())
    # Denver altitude with positive beta raises the make prob
    assert out["exp_make_prob"][0] > out["base_make_prob"][0]


def test_env_dome_neutralized(monkeypatch):
    monkeypatch.setattr(
        k,
        "ENVIRONMENT_FG_COEF",
        {"wind": -0.5, "temp": 0.0, "altitude_kft": 0.0, "temp_baseline": 60.0},
    )
    df = _fg_df().with_columns(
        pl.lit("dome").alias("roof"),
        pl.lit(None, dtype=pl.Float64).alias("wind"),
        pl.lit("ATL").alias("home_team"),
    )
    out = k.env_adjusted_make_prob(df)
    # dome -> wind treated as 0 -> no shift
    assert abs(out["exp_make_prob"][0] - out["base_make_prob"][0]) < 1e-9


def _kicks(kicker: str, n: int, made_over_exp: float) -> pl.DataFrame:
    """n synthetic kicks with exp prob 0.8 and realized rate 0.8+made_over_exp."""
    made = int(round(n * (0.8 + made_over_exp)))
    return pl.DataFrame(
        {
            "season": [2023] * n,
            "week": list(range(1, n + 1)),
            "kicker_player_id": [kicker] * n,
            "kicker_player_name": [kicker] * n,
            "posteam": ["A"] * n,
            "exp_make_prob": [0.8] * n,
            "made": [1] * made + [0] * (n - made),
        }
    )


def test_fgoe_shrink_ordering():
    df = pl.concat([_kicks("BIG", 40, 0.1), _kicks("SMALL", 5, 0.2)])
    out = k._kicker_rating_from(df).sort("kicker_player_id")
    big = out.filter(pl.col("kicker_player_id") == "BIG").row(0, named=True)
    small = out.filter(pl.col("kicker_player_id") == "SMALL").row(0, named=True)
    assert big["fg_att"] == 40
    assert abs(big["fgoe"] - (big["fg_made"] - big["exp_made"])) < 1e-9
    # raw per-att: SMALL (+0.2) > BIG (+0.1); shrink pulls SMALL below BIG
    assert small["fgoe_per_att"] > big["fgoe_per_att"]
    assert small["fgoe_shrunk"] < big["fgoe_shrunk"]


def test_kicker_empty_zero_row():
    out = k._kicker_rating_from(_kicks("X", 3, 0.0).head(0))
    assert out.height == 0
    assert "fgoe_shrunk" in out.columns


def test_kicker_rating_empty_seasons():
    out = k.nfl_kicker_rating([])
    assert out.height == 0
    assert "rating" in out.columns
