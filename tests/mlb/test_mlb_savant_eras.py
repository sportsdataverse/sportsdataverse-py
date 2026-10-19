"""Savant measurement eras in the pitch-level models.

sdv-internal-refs rules/mlb.yaml: ``mlb-2026-savant-plate-location-middle-of-plate`` and
``mlb-2026-abs-challenge-system`` (the called-strike / framing fits exclude 2026+ rows and
flag them ``abs_era``), ``mlb-2017-release-speed-statcast`` (the physics z-scores are keyed
on season as well as pitcher).

Real fixtures throughout. No 2026 Savant capture is committed yet, so the ABS-era rows are
real 2024 pitches whose ``game_year`` is relabelled 2026: the gate reads the season column,
nothing else, so that is the exact path a 2026 capture takes.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mlb._savant_eras import ABS_SEASON, abs_era_expr, drop_abs_era_rows, season_expr
from sportsdataverse.mlb.mlb_catcher_framing import mlb_catcher_framing
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_umpire_zone import fit_zone_model, mlb_umpire_bias, mlb_umpire_called_strike_prob

FIX = "tests/fixtures/mlb_pitching"
ENTRY = "mlb-2026-savant-plate-location-middle-of-plate"


@pytest.fixture(scope="module")
def pitches_2024() -> pl.DataFrame:
    return pl.read_parquet(f"{FIX}/pitches_2024-06-15.parquet")


@pytest.fixture(scope="module")
def mixed_eras(pitches_2024: pl.DataFrame) -> pl.DataFrame:
    """The 2024 capture with every other pitch relabelled as a 2026 pitch."""
    return pitches_2024.with_columns(
        pl.when(pl.arange(0, pitches_2024.height) % 2 == 0)
        .then(ABS_SEASON)
        .otherwise(pl.col("game_year"))
        .alias("game_year")
    )


def test_season_comes_from_game_year_then_game_date():
    assert season_expr(["plate_x"]) is None
    f = pl.DataFrame({"game_year": [2016, 2017], "game_date": ["2024-06-15", "2024-06-15"]})
    assert f.select(season_expr(f.columns))["game_year"].to_list() == [2016, 2017]
    f = pl.DataFrame({"game_date": ["2025-09-30", "2026-04-01"]})
    assert f.select(abs_era_expr(f.columns).alias("a"))["a"].to_list() == [False, True]
    f = pl.DataFrame({"game_date": [dt.date(2026, 4, 1)]})
    assert f.select(abs_era_expr(f.columns).alias("a"))["a"].to_list() == [True]


def test_a_frame_without_a_season_is_left_alone():
    oracle = pl.read_parquet("tests/fixtures/mlb_game_state/savant_called_pitches.parquet")
    assert season_expr(oracle.columns) is None
    assert drop_abs_era_rows(oracle, what="x").height == oracle.height  # and no warning (errors in pytest)
    assert mlb_umpire_called_strike_prob(oracle.head(500))["abs_era"].to_list() == [False] * 500


def test_pre_2026_capture_fits_without_a_warning(pitches_2024: pl.DataFrame):
    out = mlb_umpire_called_strike_prob(pitches_2024)
    assert out.schema == {"called_strike_prob": pl.Float64, "abs_era": pl.Boolean}
    assert out.height == pitches_2024.height and not out["abs_era"].any()


def test_abs_era_rows_are_excluded_from_the_zone_fit_with_a_warning(pitches_2024, mixed_eras):
    with pytest.warns(UserWarning, match=ENTRY):
        fitted = fit_zone_model(mixed_eras)
    pre = fit_zone_model(mixed_eras.filter(pl.col("game_year") < ABS_SEASON))
    assert np.allclose(fitted["coef"], pre["coef"]) and fitted["intercept"] == pytest.approx(pre["intercept"])
    pooled = fit_zone_model(pitches_2024)  # the same pitches, all pre-2026: a different fit
    assert not np.allclose(fitted["coef"], pooled["coef"])


def test_an_abs_era_only_frame_is_fit_in_its_own_frame(pitches_2024):
    # One era means nothing to mix: a 2026 season build must fit its own pitches, not drop
    # them all and score every pitch 0.5 (which turned framing into a called-strike rate).
    only_2026 = pitches_2024.with_columns(pl.lit(ABS_SEASON).alias("game_year"))
    fitted = fit_zone_model(only_2026)  # no warning: nothing is excluded
    pooled = fit_zone_model(pitches_2024)
    assert np.allclose(fitted["coef"], pooled["coef"]) and fitted["intercept"] == pytest.approx(pooled["intercept"])
    probs = mlb_umpire_called_strike_prob(only_2026)
    assert probs["abs_era"].all() and probs["called_strike_prob"].n_unique() > 1


def test_a_degenerate_zone_fit_warns(pitches_2024):
    with pytest.warns(UserWarning, match="fewer than 2 called pitches"):
        fitted = fit_zone_model(pitches_2024.head(0))
    assert fitted["coef"] == [0.0] * 7 and fitted["intercept"] == 0.0


def test_every_row_is_still_scored_and_flagged(mixed_eras):
    with pytest.warns(UserWarning, match=ENTRY):
        out = mlb_umpire_called_strike_prob(mixed_eras)
    assert out.height == mixed_eras.height
    assert out["abs_era"].to_list() == (mixed_eras["game_year"] >= ABS_SEASON).to_list()
    assert out["called_strike_prob"].drop_nans().is_between(0.0, 1.0).all()


def test_umpire_bias_splits_an_umpire_by_era(mixed_eras):
    with pytest.warns(UserWarning, match=ENTRY):
        bias = mlb_umpire_bias(mixed_eras.with_columns(pl.lit("U1").alias("umpire_id")))
    assert bias.columns == ["umpire_id", "abs_era", "n_called", "obs_strike_rate", "exp_strike_rate", "bias"]
    assert bias["abs_era"].to_list() == [False, True]
    called = mixed_eras.filter(pl.col("description").is_in(["called_strike", "ball"]))
    assert bias["n_called"].to_list() == [
        called.filter(pl.col("game_year") < ABS_SEASON).height,
        called.filter(pl.col("game_year") >= ABS_SEASON).height,
    ]
    # the fit is on the pre-2026 calls, so their residual is zero by construction
    assert abs(bias.row(0, named=True)["bias"]) < 1e-3


def test_catcher_framing_keeps_abs_era_takes_on_their_own_row(pitches_2024, mixed_eras):
    with pytest.warns(UserWarning, match=ENTRY):
        split = mlb_catcher_framing(mixed_eras)
    assert "abs_era" in split.columns and split["abs_era"].any() and not split["abs_era"].all()
    one_era = mlb_catcher_framing(pitches_2024)
    assert not one_era["abs_era"].any()
    # per catcher, the two era rows together cover every take the one-era frame counted
    assert split.group_by("catcher_id").agg(pl.col("takes").sum()).sort("catcher_id")["takes"].to_list() == (
        one_era.sort("catcher_id")["takes"].to_list()
    )


def test_velocity_is_standardized_within_pitcher_and_season():
    # Two real captures, 2023 and 2024, share two pitchers (450203, 656427). A season-keyed
    # z-score has mean 0 in each of their seasons; the pitcher-only key it replaced left
    # the smaller season off centre (450203 2024: -0.09 against the pooled mean).
    a = pl.read_parquet(f"{FIX}/pitches_2024-06-15.parquet")
    b = pl.read_parquet(f"{FIX}/pitcher_season_pitches_2023_sample.parquet").select(a.columns)
    both = pl.concat([a, b], how="vertical_relaxed").filter(pl.col("pitcher").is_in([450203, 656427]))
    assert both["game_year"].unique().sort().to_list() == [2023, 2024]
    feats = pitch_features(both)
    by_season = feats.group_by("pitcher", "game_year").agg(pl.col("velo_z").mean().alias("m"), pl.len())
    assert by_season.height == 4 and (by_season["len"] > 50).all()
    assert (by_season["m"].abs() < 1e-9).all()
    # within pitcher alone the seasons differ in level, which is exactly what the key keeps apart
    levels = both.group_by("pitcher", "game_year").agg(pl.col("release_speed").mean()).sort("pitcher", "game_year")
    assert levels.filter(pl.col("pitcher") == 450203)["release_speed"].diff().drop_nulls().abs().item() > 0.3


def test_velocity_without_a_season_column_still_standardizes_within_pitcher(pitches_2024):
    feats = pitch_features(pitches_2024.drop("game_year", "game_date"))
    assert (feats.group_by("pitcher").agg(pl.col("velo_z").mean())["velo_z"].abs() < 1e-9).all()
