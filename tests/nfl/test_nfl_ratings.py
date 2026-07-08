"""Unit tests for the NFL opponent-adjusted EPA ratings engine (Phase 1)."""

import polars as pl

from sportsdataverse.nfl.nfl_ratings import opponent_adjusted_ridge


def _mini_plays():
    # Home venue is balanced across teams: with a single fixed home team the
    # UNPENALIZED home column is collinear with that team's offense indicator,
    # and the ridge (which only penalises team coefficients) lets HFA absorb
    # the whole signal -- team coefs shrink to ~0 and ordering is undefined.
    rows = []
    for _ in range(30):
        for home in ("A", "B"):
            rows.append({"posteam": "A", "defteam": "B", "home_team": home, "epa": 0.30})
            rows.append({"posteam": "B", "defteam": "A", "home_team": home, "epa": -0.30})
    return pl.DataFrame(rows)


def test_ridge_orders_offense():
    frame, intercept, home_coef = opponent_adjusted_ridge(
        _mini_plays(),
        off_col="posteam",
        def_col="defteam",
        home_col="home_team",
        resp_col="epa",
        lam=1.0,
    )
    a = frame.filter(pl.col("team_id") == "A").row(0, named=True)
    b = frame.filter(pl.col("team_id") == "B").row(0, named=True)
    assert a["off_coef"] > b["off_coef"]
    assert frame.schema["team_id"] == pl.Utf8
    assert abs(intercept) < 0.5  # league mean EPA/play is near 0 here


def test_ridge_empty_input_returns_typed_zero_row():
    empty = pl.DataFrame(schema={"posteam": pl.Utf8, "defteam": pl.Utf8, "home_team": pl.Utf8, "epa": pl.Float64})
    frame, intercept, home_coef = opponent_adjusted_ridge(
        empty, off_col="posteam", def_col="defteam", home_col="home_team", resp_col="epa", lam=1.0
    )
    assert frame.height == 0
    assert frame.schema == {"team_id": pl.Utf8, "off_coef": pl.Float64, "def_coef": pl.Float64}
    assert intercept == 0.0 and home_coef == 0.0
