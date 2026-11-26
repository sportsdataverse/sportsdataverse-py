"""The bundled NBA shot-make booster and the location model, ``score_shot_xpoints``'s NBA default.

Fixtures (``tests/fixtures/nba_shot_make``, README there): ``nba_fga_2026.parquet``, the field goals of the
first 20 regular-season games of the 2025-26 play-by-play release (the booster's holdout season), and
``nba_fga_2023_chart_games.parquet``, the play-by-play field goals of three 2022-23 games that the committed
shot chart (``tests/fixtures/nba_shot_value``) also holds. The WNBA has no booster: it failed its gates.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest

from sportsdataverse.nba.nba_shot_value import (
    SHOT_MAKE_INPUTS,
    SHOT_MAKE_MODELS,
    SHOT_MAKE_NUMERIC,
    _shot_make_design,
    _shot_make_feature_names,
    _shot_make_levels,
    add_shot_make_prob,
    load_shot_make_model,
    score_shot_xpoints,
)

_FIX = Path(__file__).resolve().parents[1] / "fixtures"


def _fga(name: str = "nba_fga_2026") -> pl.DataFrame:
    return pl.read_parquet(_FIX / "nba_shot_make" / f"{name}.parquet")


def _chart() -> tuple[pl.DataFrame, pl.DataFrame]:
    d = _FIX / "nba_shot_value"
    return pl.read_parquet(d / "shotchart_2023.parquet"), pl.read_parquet(d / "league_averages_2023.parquet")


def _ll(y: np.ndarray, p: np.ndarray) -> float:
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def test_booster_reads_the_design():
    b = load_shot_make_model("00")
    lv = _shot_make_levels(b)
    assert len(lv) >= 20 and len(set(lv)) == len(lv)
    assert b.feature_names == _shot_make_feature_names(lv)
    assert b.feature_names[: len(SHOT_MAKE_NUMERIC)] == SHOT_MAKE_NUMERIC
    assert list(SHOT_MAKE_MODELS) == ["00"]


@pytest.mark.parametrize("league_id", ["10", "20"])
def test_league_without_a_booster_raises(league_id):
    with pytest.raises(ValueError, match="no shot-make model"):
        load_shot_make_model(league_id)


def test_holdout_games_are_calibrated_and_beat_the_overall_rate():
    """Holdout-season games (3,585 shots). Observed: mean make chance 0.466 against a make rate of
    0.467, log loss 0.643 against 0.691 for the overall rate, rim 0.659 against 0.394 elsewhere."""
    fga = _fga()
    out = add_shot_make_prob(fga)
    assert out.height == fga.height == 3_585
    p = out["xfg"].to_numpy()
    y = (out["shot_result"] == "Made").cast(pl.Float64).to_numpy()
    assert np.isfinite(p).all() and ((p > 0) & (p < 1)).all()
    assert abs(p.mean() - y.mean()) < 0.01
    assert _ll(y, p) < _ll(y, np.full_like(p, y.mean())) - 0.04
    rim = np.hypot(out["x_legacy"].to_numpy(), out["y_legacy"].to_numpy()) < 40
    assert p[rim].mean() > p[~rim].mean() + 0.2


def test_only_located_field_goals_are_scored():
    fga = _fga().head(4)
    pbp = pl.concat(
        [
            fga,
            fga.head(1).with_columns(pl.lit(1, dtype=pl.Int64).alias("shot_value")),  # a free throw
            fga.head(1).with_columns(pl.lit(None, dtype=fga.schema["x_legacy"]).alias("x_legacy")),
        ],
        how="vertical_relaxed",
    )
    xfg = add_shot_make_prob(pbp)["xfg"]
    assert xfg.head(4).null_count() == 0
    assert xfg.tail(2).null_count() == 2


def test_float_inputs_and_nan_locations():
    """A frame with float columns (a pandas round trip) scores the same, and a NaN location is unscored."""
    fga = _fga().head(5)
    floated = fga.with_columns(pl.col("shot_value", "x_legacy", "y_legacy").cast(pl.Float64)).with_columns(
        pl.when(pl.int_range(pl.len()) == 4).then(float("nan")).otherwise(pl.col("x_legacy")).alias("x_legacy")
    )
    a, b = add_shot_make_prob(fga)["xfg"], add_shot_make_prob(floated)["xfg"]
    assert a.head(4).equals(b.head(4))
    assert b[4] is None


def test_pandas_output():
    out = add_shot_make_prob(_fga().head(3), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and out["xfg"].notna().all()


def test_missing_column_raises():
    with pytest.raises(ValueError, match="sub_type"):
        add_shot_make_prob(_fga().drop("sub_type"))
    assert set(SHOT_MAKE_INPUTS) <= set(_fga().columns)


def test_an_unseen_shot_type_takes_the_other_branch():
    lv = _shot_make_levels(load_shot_make_model("00"))
    f = _fga().head(3).with_columns(pl.lit("Never Seen Shot").alias("sub_type"))
    X = _shot_make_design(f, lv)
    assert X.shape == (3, len(SHOT_MAKE_NUMERIC) + len(lv))
    assert (X[:, len(SHOT_MAKE_NUMERIC) :] == 0).all()


def test_empty_input_gets_the_column():
    out = add_shot_make_prob(_fga().clear())
    assert out.height == 0 and out.schema["xfg"] == pl.Float64


def test_shot_chart_maps_onto_the_play_by_play():
    """The shot chart's ``game_event_id`` / ``loc_x`` / ``loc_y`` / ``action_type`` are the play-by-play's
    ``action_number`` / ``x_legacy`` / ``y_legacy`` / ``sub_type`` (all 173 shots of three games), so the
    location model scores a shot-chart row exactly as it scores the same shot from the play-by-play. The
    two ``shot_distance`` columns disagree (rounded against truncated), which is why the design reads
    neither."""
    chart, la = _chart()
    pbp = _fga("nba_fga_2023_chart_games")
    j = chart.with_columns(pl.col("game_id").cast(pl.Utf8)).join(
        pbp, left_on=["game_id", "game_event_id"], right_on=["game_id", "action_number"], how="inner"
    )
    in_games = chart.filter(pl.col("game_id").cast(pl.Utf8).is_in(pbp["game_id"].unique().implode()))
    assert j.height == in_games.height == 173
    assert (j["loc_x"] == j["x_legacy"]).all() and (j["loc_y"] == j["y_legacy"]).all()
    assert (j["action_type"] == j["sub_type"]).all()
    assert (j["shot_distance"] != j["shot_distance_right"]).any()
    twos = j.filter(pl.col("shot_value") == 2)
    by_chart = score_shot_xpoints(twos.select(chart.columns), la)["base_fg_pct"]
    by_pbp = add_shot_make_prob(twos.select("x_legacy", "y_legacy", "shot_value", "sub_type"))["xfg"]
    np.testing.assert_allclose(by_chart.to_numpy(), by_pbp.to_numpy())


def test_shot_chart_location_scoring_differs_from_zone_and_fits_better():
    """The NBA default is the location model on twos (not a silent fall-through to the zone table:
    mean |difference| on twos 0.073) and the zone value on threes (owner ruling 2026-11-25), and it
    fits the 2022-23 shots better than that season's own zone table (log loss 0.6400 vs 0.6610).
    2022-23 is a training season of the booster, so this checks the scoring path, not out-of-sample
    skill (the fit report holds that)."""
    shots, la = _chart()
    loc = score_shot_xpoints(shots, la)
    zone = score_shot_xpoints(shots, la, model="zone")
    assert loc.columns == zone.columns
    y = loc["shot_made_flag"].cast(pl.Float64).to_numpy()
    p_loc, p_zone = loc["base_fg_pct"].to_numpy(), zone["base_fg_pct"].to_numpy()
    three = (loc["shot_value"] == 3).to_numpy()
    assert three.sum() > 10_000
    np.testing.assert_array_equal(p_loc[three], p_zone[three])
    assert np.abs(p_loc - p_zone)[~three].mean() > 0.06
    assert _ll(y, p_loc) < _ll(y, p_zone) - 0.015
    np.testing.assert_allclose(loc["xpoints"].to_numpy(), p_loc * loc["shot_value"].to_numpy())


def test_default_is_location_for_the_nba_and_zone_elsewhere():
    shots, la = _chart()
    shots = shots.head(50)
    zone = score_shot_xpoints(shots, la, model="zone")["base_fg_pct"]
    assert score_shot_xpoints(shots, la)["base_fg_pct"].equals(
        score_shot_xpoints(shots, la, model="location")["base_fg_pct"]
    )
    assert not score_shot_xpoints(shots, la)["base_fg_pct"].equals(zone)
    for league_id in ("10", "20"):
        assert score_shot_xpoints(shots, la, league_id=league_id)["base_fg_pct"].equals(zone)


def test_an_unlocated_chart_shot_takes_the_zone_value():
    shots, la = _chart()
    shots = (
        shots.filter(pl.col("shot_type").str.starts_with("2"))
        .head(6)
        .with_columns(pl.when(pl.int_range(pl.len()) < 2).then(None).otherwise(pl.col("loc_x")).alias("loc_x"))
    )
    loc = score_shot_xpoints(shots, la, model="location")["base_fg_pct"]
    zone = score_shot_xpoints(shots, la, model="zone")["base_fg_pct"]
    assert loc.head(2).equals(zone.head(2))
    assert not loc.tail(4).equals(zone.tail(4))


@pytest.mark.parametrize("league_id", ["10", "20"])
def test_location_refused_without_a_booster(league_id):
    shots, la = _chart()
    with pytest.raises(ValueError, match="no shot-make model"):
        score_shot_xpoints(shots.head(5), la, league_id=league_id, model="location")


def test_bad_model_or_missing_location_raises():
    shots, la = _chart()
    with pytest.raises(ValueError, match="model must be"):
        score_shot_xpoints(shots.head(5), la, model="lattice")
    with pytest.raises(ValueError, match="loc_x"):
        score_shot_xpoints(shots.head(5).drop("loc_x"), la, model="location")
