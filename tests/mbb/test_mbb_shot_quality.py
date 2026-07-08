"""Tests for the empirical-Bayes zone x type shot-quality model + scorer."""

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_shot_quality import mbb_shot_quality, mbb_shot_quality_model


def _synth(zone, shot_type, n, made, pv):
    return pl.DataFrame(
        {
            "shot_zone": [zone] * n,
            "shot_type": [shot_type] * n,
            "made": [True] * made + [False] * (n - made),
            "point_value": [pv] * n,
        }
    ).with_columns(pl.col("point_value").cast(pl.Int8))


def test_model_shrinks_small_cells():
    shots = pl.concat([_synth("rim", "rim", 1000, 600, 2), _synth("mid", "jump", 4, 3, 2)])
    m = mbb_shot_quality_model(shots, league="mens")
    rim = m.filter(pl.col("shot_zone") == "rim").row(0, named=True)
    mid = m.filter(pl.col("shot_zone") == "mid").row(0, named=True)
    assert abs(rim["make_rate_shrunk"] - 0.60) < 0.02  # large n -> barely shrunk
    # mid is its zone's ONLY cell, so the parent mean equals its own rate and
    # shrinkage is a no-op; cross-cell pull is proven by the two-cell test below
    assert abs(mid["make_rate_shrunk"] - 0.75) < 1e-9
    assert abs(rim["xpoints"] - rim["make_rate_shrunk"] * 2) < 1e-9
    assert rim["n"] == 1000 and abs(rim["make_rate_raw"] - 0.60) < 1e-9


def test_model_cell_shrinks_toward_parent_zone_mean():
    # one zone, two cells: big jump cell at 0.30, tiny rim-type cell at 1.00;
    # the zone mean is dominated by the big cell, so the tiny cell lands
    # between the zone mean and its raw rate
    shots = pl.concat([_synth("paint", "jump", 900, 270, 2), _synth("paint", "rim", 10, 10, 2)])
    m = mbb_shot_quality_model(shots, league="mens")
    tiny = m.filter(pl.col("shot_type") == "rim").row(0, named=True)
    zone_mean = 280 / 910
    assert zone_mean < tiny["make_rate_shrunk"] < 1.0


def test_model_empty_input_schema():
    m = mbb_shot_quality_model(
        pl.DataFrame(schema={"shot_zone": pl.Utf8, "shot_type": pl.Utf8, "made": pl.Boolean, "point_value": pl.Int8})
    )
    assert m.columns == ["shot_zone", "shot_type", "n", "make_rate_raw", "make_rate_shrunk", "point_value", "xpoints"]
    assert m.height == 0


def test_scorer_joins_xpoints():
    shots = pl.concat([_synth("rim", "rim", 1000, 600, 2), _synth("mid", "jump", 4, 3, 2)])
    m = mbb_shot_quality_model(shots, league="mens")
    scored = mbb_shot_quality(shots, model=m, league="mens")
    assert scored.height == shots.height
    assert scored.get_column("xpoints").null_count() == 0
    rim_cell = m.filter(pl.col("shot_zone") == "rim").row(0, named=True)
    rim_scored = scored.filter(pl.col("shot_zone") == "rim").row(0, named=True)
    assert abs(rim_scored["xmake"] - rim_cell["make_rate_shrunk"]) < 1e-9


def test_scorer_builds_model_when_none():
    shots = _synth("rim", "rim", 100, 60, 2)
    scored = mbb_shot_quality(shots, league="mens")
    assert "xpoints" in scored.columns and scored.get_column("xpoints").null_count() == 0


def test_scorer_join_dtype_mismatch_raises():
    shots = _synth("rim", "rim", 10, 6, 2)
    m = mbb_shot_quality_model(shots, league="mens").with_columns(pl.col("shot_zone").cast(pl.Categorical))
    with pytest.raises(AssertionError):
        mbb_shot_quality(shots, model=m, league="mens")
