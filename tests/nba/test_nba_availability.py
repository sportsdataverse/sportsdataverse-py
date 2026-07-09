from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_availability import (
    _FEATURE_COLS,
    availability_features,
    score_availability,
)
from sportsdataverse.nba.nba_draft_constants import as_of_class_split, mae

FIXTURE_DIR = "tests/fixtures/nba_draft"


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


def test_availability_holdout_beats_baseline_and_calibrates() -> None:
    """Phase 3 oracle gate.

    **Debugging record:** a first pass built ``prior_gp_pct`` via
    ``.shift(1).over("player_id")`` -- row-position based, so it would
    silently misattribute a wrong season across any real multi-season gap.
    Fixed to a ``season - 1`` self-join (matching
    ``nba_aging_curve.build_aging_deltas``'s pattern) in
    ``availability_features``. Even after the fix, raw
    ``prior_gp_pct``/``career_gp_pct`` correlations with realized GP% are
    weak (~0.01-0.02) on this corpus -- year-to-year games-played is
    evidently noisy even among players who cleared the combine-class bar.
    Despite that, the fitted model **does** beat the naive career-mean
    baseline on held-out seasons (2017+) -- the actual gate requirement --
    so the floor below is calibrated from the observed holdout MAE (0.2515),
    not an aspirational number.
    """
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    career = season_stats.with_columns(
        season_stats["season_id"].str.slice(0, 4).cast(pl.Int64).alias("season"),
        pl.col("player_age").alias("age"),
    ).filter(pl.col("season") >= 2000)

    feats = availability_features(career)
    labeled = feats.with_columns((career["gp"].cast(pl.Float64) / 82.0).clip(0.0, 1.0).alias("realized_gp_pct"))
    _, holdout = as_of_class_split(labeled, cutoff_year=2016, year_col="season")

    scored = score_availability(holdout.select("player_id", "season", *_FEATURE_COLS))
    joined = scored.join(holdout.select("player_id", "season", "realized_gp_pct"), on=["player_id", "season"])

    model_mae = mae(joined["avail_pct"].to_numpy(), joined["realized_gp_pct"].to_numpy())
    baseline_mae = mae(holdout["career_gp_pct"].to_numpy(), holdout["realized_gp_pct"].to_numpy())

    assert model_mae <= 0.27, f"availability holdout MAE {model_mae:.4f} > 0.27 -- debug feature leakage, do NOT widen"
    assert model_mae < baseline_mae, (
        f"model MAE {model_mae:.4f} must beat the career-mean baseline MAE {baseline_mae:.4f}"
    )
