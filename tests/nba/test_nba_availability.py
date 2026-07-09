from __future__ import annotations

import importlib

import polars as pl
import pytest

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

    **Leakage fix (post-review):** the null-fill imputation median was
    previously computed over the full 2000-2025 frame before the split. Fixed
    to a TRAIN-only median passed via ``availability_features(median_ref=...)``
    (features stay full-frame because the within-player prior-season lookback
    needs each player's whole time series; only the impute scalar is
    train-derived). The leak was negligible here -- post-fix holdout MAE is
    unchanged at 0.2515 (the train median 0.756 is essentially the same as the
    full-frame median, and only first-observed-season rows are ever imputed) --
    so the 0.27 floor and the beats-baseline assertion are unchanged.
    """
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    career = season_stats.with_columns(
        season_stats["season_id"].str.slice(0, 4).cast(pl.Int64).alias("season"),
        pl.col("player_age").alias("age"),
    ).filter(pl.col("season") >= 2000)

    # Impute with TRAIN-season medians only (mirrors the leak-free fit script);
    # features are built on the full frame because the within-player prior-
    # season lookback needs each player's whole time series.
    train_raw = career.filter(pl.col("season") <= 2016)
    gp_median = float((train_raw["gp"].cast(pl.Float64) / 82.0).clip(0.0, 1.0).median() or 0.75)
    feats = availability_features(career, median_ref={"gp_pct": gp_median, "bmi": 24.0})
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


def test_availability_gleague_bridge_empty_frame_no_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    """gleague_bridge=True with an empty G-League frame -> full schema, no crash.

    Fully offline: the NBA-league bulk call is faked with a small synthetic
    frame (avoids a live network dependency), the G-League bulk call is
    faked to return empty (the actual thing under test). Uses
    ``importlib.import_module`` (not ``import ... as``) -- see the same
    module/function name-shadowing note in test_nba_draft_model.py.
    """
    mod = importlib.import_module("sportsdataverse.nba.nba_availability")

    synthetic_bulk = pl.DataFrame({"player_id": [1, 2], "age": [24, 30], "gp": [70, 60]})

    def _fake(season: str, league_id: str | None = None) -> pl.DataFrame:
        if league_id == "20":
            return pl.DataFrame()
        return synthetic_bulk

    monkeypatch.setattr(mod, "nba_stats_leaguedashplayerstats", _fake)
    out = mod.nba_availability(2019, gleague_bridge=True)
    assert list(out.schema.keys()) == ["player_id", "season", "avail_pct"]
    assert out.height == 2


def test_availability_features_median_ref_uses_passed_scalar() -> None:
    """median_ref overrides the frame-computed impute median (leak-free fit path)."""
    # single-season players -> every prior_gp_pct/career_gp_pct is null (no
    # prior season), so they all get the impute scalar.
    career = pl.DataFrame({"player_id": ["1", "2"], "season": [2019, 2019], "age": [24, 30], "gp": [82, 41]})
    feats = availability_features(career, median_ref={"gp_pct": 0.33, "bmi": 24.0})
    assert feats["prior_gp_pct"].to_list() == [0.33, 0.33]
    assert feats["career_gp_pct"].to_list() == [0.33, 0.33]
    # without median_ref it falls back to the frame median (here (1.0+0.5)/2=0.75)
    feats_frame = availability_features(career)
    assert feats_frame["prior_gp_pct"][0] != 0.33


def test_availability_runtime_dedups_duplicate_player_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """A duplicated player_id in the bulk season frame is collapsed, not summed."""
    mod = importlib.import_module("sportsdataverse.nba.nba_availability")
    # two rows for player 1 in the same season (the TOT-row hazard) + one clean row
    dupe = pl.DataFrame({"player_id": [1, 1, 2], "age": [24, 24, 30], "gp": [70, 12, 60]})

    monkeypatch.setattr(mod, "nba_stats_leaguedashplayerstats", lambda season, league_id=None: dupe)
    out = mod.nba_availability(2019)
    # one row per player (player 1 deduped to a single row), not doubled
    assert out.height == 2
    assert sorted(out["player_id"].to_list()) == ["1", "2"]
