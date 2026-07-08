"""Unit tests for the NFL opponent-adjusted EPA ratings engine (Phase 1)."""

import polars as pl

from sportsdataverse.nfl.nfl_ratings import (
    efficiency_ratings,
    opponent_adjusted_ridge,
    special_teams_ratings,
)


def test_special_teams_ratings_strong_unit_positive_and_absent_zero():
    rows = []
    # A's ST unit is strong (+0.2 EPA/play) vs B; C never appears on an ST play
    # but is on the scrimmage roster and must get the documented 0.0 fill.
    for g, home in (("g1", "A"), ("g2", "B")):
        for _ in range(20):
            rows.append(
                {
                    "game_id": g,
                    "posteam": "A",
                    "defteam": "B",
                    "home_team": home,
                    "epa": 0.20,
                    "wp": 0.5,
                    "special": 1,
                    "qb_kneel": 0,
                    "qb_spike": 0,
                    "play_type": "kickoff",
                }
            )
            rows.append(
                {
                    "game_id": g,
                    "posteam": "B",
                    "defteam": "A",
                    "home_team": home,
                    "epa": -0.20,
                    "wp": 0.5,
                    "special": 1,
                    "qb_kneel": 0,
                    "qb_spike": 0,
                    "play_type": "kickoff",
                }
            )
            rows.append(
                {
                    "game_id": g,
                    "posteam": "C",
                    "defteam": "A",
                    "home_team": home,
                    "epa": 0.0,
                    "wp": 0.5,
                    "special": 0,
                    "qb_kneel": 0,
                    "qb_spike": 0,
                    "play_type": "pass",
                }
            )
    out = special_teams_ratings(pl.DataFrame(rows))
    assert out.schema == {"team_id": pl.Utf8, "adj_st_epa": pl.Float64}
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    c = out.filter(pl.col("team_id") == "C").row(0, named=True)
    assert a["adj_st_epa"] > 0.0
    assert a["adj_st_epa"] > b["adj_st_epa"]
    assert c["adj_st_epa"] == 0.0


def test_special_teams_ratings_empty_input():
    out = special_teams_ratings(_competitive_plays().head(0))
    assert out.height == 0
    assert out.schema == {"team_id": pl.Utf8, "adj_st_epa": pl.Float64}


def _competitive_plays(extra_rows=None):
    """Two-team competitive scrimmage frame: A's offense deterministically better."""
    rows = []
    for g, home in (("g1", "A"), ("g2", "B")):
        for _ in range(30):
            rows.append(
                {
                    "game_id": g,
                    "posteam": "A",
                    "defteam": "B",
                    "home_team": home,
                    "epa": 0.30,
                    "wp": 0.5,
                    "special": 0,
                    "qb_kneel": 0,
                    "qb_spike": 0,
                    "play_type": "pass",
                }
            )
            rows.append(
                {
                    "game_id": g,
                    "posteam": "B",
                    "defteam": "A",
                    "home_team": home,
                    "epa": -0.30,
                    "wp": 0.5,
                    "special": 0,
                    "qb_kneel": 0,
                    "qb_spike": 0,
                    "play_type": "run",
                }
            )
    if extra_rows:
        rows.extend(extra_rows)
    return pl.DataFrame(rows)


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


def test_efficiency_ratings_orders_and_derives_net():
    # A wp=0.99 blowout play with a huge EPA must be filtered out (competitive
    # window), so it cannot flip the ordering.
    blowout = [
        {
            "game_id": "g1",
            "posteam": "B",
            "defteam": "A",
            "home_team": "A",
            "epa": 50.0,
            "wp": 0.99,
            "special": 0,
            "qb_kneel": 0,
            "qb_spike": 0,
            "play_type": "pass",
        }
    ]
    out = efficiency_ratings(_competitive_plays(blowout))
    assert out.schema["team_id"] == pl.Utf8
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert a["adj_net"] > b["adj_net"]
    assert a["games"] == 2 and b["games"] == 2
    for r in (a, b):
        assert abs(r["adj_net"] - (r["adj_off_epa"] - r["adj_def_epa"])) < 1e-12


def test_efficiency_ratings_empty_input():
    out = efficiency_ratings(_competitive_plays().head(0))
    assert out.height == 0
    assert out.schema == {
        "team_id": pl.Utf8,
        "adj_off_epa": pl.Float64,
        "adj_def_epa": pl.Float64,
        "adj_net": pl.Float64,
        "games": pl.Int64,
    }
