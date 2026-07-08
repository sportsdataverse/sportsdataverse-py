from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_availability import availability_features, score_availability


def test_availability_features_declining_gp_by_age() -> None:
    career = pl.DataFrame(
        {
            "player_id": ["1", "1", "1", "1"],
            "season": [2016, 2017, 2018, 2019],
            "age": [22, 23, 24, 25],
            "gp": [82, 70, 50, 30],
        }
    )
    feats = availability_features(career)
    assert feats.schema["player_id"] == pl.Utf8
    assert feats.schema["season"] == pl.Int64
    assert set(["age", "prior_gp_pct", "career_gp_pct", "age_sq", "bmi"]).issubset(feats.columns)
    # first season has no strictly-prior data -> imputed with league median, not null
    row0 = feats.filter(pl.col("season") == 2016)
    assert row0["prior_gp_pct"][0] is not None
    # season 2019's prior_gp_pct should reflect 2018's low GP (50/82)
    row3 = feats.filter(pl.col("season") == 2019)
    assert abs(row3["prior_gp_pct"][0] - 50.0 / 82.0) < 1e-6


def test_availability_features_empty_input_has_schema() -> None:
    empty = pl.DataFrame(schema={"player_id": pl.Utf8, "season": pl.Int64, "age": pl.Float64, "gp": pl.Int64})
    out = availability_features(empty)
    assert out.height == 0
    for col in ["player_id", "season", "age", "prior_gp_pct", "career_gp_pct", "age_sq", "bmi"]:
        assert col in out.columns


def test_score_availability_in_zero_one_range() -> None:
    feats = pl.DataFrame(
        {
            "player_id": ["1", "2"],
            "season": [2019, 2019],
            "age": [25.0, 33.0],
            "prior_gp_pct": [0.95, 0.4],
            "career_gp_pct": [0.9, 0.5],
            "age_sq": [625.0, 1089.0],
            "bmi": [24.0, 25.0],
        }
    )
    out = score_availability(feats)
    assert out.schema["avail_pct"] == pl.Float64
    assert out["avail_pct"].min() >= 0.0
    assert out["avail_pct"].max() <= 1.0
    # player 1 (healthier recent history) should project higher availability
    p1 = out.filter(pl.col("player_id") == "1")["avail_pct"][0]
    p2 = out.filter(pl.col("player_id") == "2")["avail_pct"][0]
    assert p1 > p2


def test_score_availability_empty_input() -> None:
    from sportsdataverse.nba.nba_availability import _SCHEMA

    empty = pl.DataFrame(schema={"player_id": pl.Utf8})
    out = score_availability(empty)
    assert out.height == 0
    assert list(out.schema.keys()) == list(_SCHEMA.keys())
