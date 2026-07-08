"""Tests for the shot-quality constants + the canonical shot adapter."""

import pytest

from sportsdataverse.mbb.mbb_shot_quality_constants import (
    PUBLISHED_ZONE_BASELINES,
    get_constants,
    three_point_radius,
)


def test_get_constants_both_leagues():
    assert get_constants("mens").rim_radius_ft == 4.0
    assert get_constants("womens").rim_radius_ft == 4.0


def test_get_constants_unknown_raises():
    with pytest.raises(ValueError):
        get_constants("nba")


def test_three_point_radius_era_split():
    # men's arc moved to 22 ft 1.75 in for 2019-20; women's for 2021-22
    assert three_point_radius("mens", 2018) == pytest.approx(20.75)
    assert three_point_radius("mens", 2020) == pytest.approx(22.15)
    assert three_point_radius("womens", 2021) == pytest.approx(20.75)
    assert three_point_radius("womens", 2022) == pytest.approx(22.15)


def test_published_baselines_present():
    assert 0.55 < PUBLISHED_ZONE_BASELINES["mens"]["rim"] < 0.70
    assert 0.34 < PUBLISHED_ZONE_BASELINES["mens"]["corner3"] < 0.42


# ---------------------------------------------------------------------------
# zone + point-value classifiers (hand-computable geometry)
# ---------------------------------------------------------------------------


def test_point_value_beyond_arc_is_three():
    from sportsdataverse.mbb.mbb_shots_adapter import classify_point_value

    # men's 2020 arc 22.15 ft: a shot 24 ft out, centered, is a 3
    assert classify_point_value(24.0, 0.0, 24.0, league="mens", season=2020) == 3
    assert classify_point_value(15.0, 0.0, 15.0, league="mens", season=2020) == 2


def test_point_value_corner_three():
    from sportsdataverse.mbb.mbb_shots_adapter import classify_point_value

    # near baseline (small y), wide (|x| beyond corner_x) -> 3 even if radial dist < arc
    assert classify_point_value(21.5, 22.0, 3.0, league="mens", season=2020) == 3


def test_zone_rim_paint_mid():
    from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_geometry

    assert classify_zone_geometry(2.0, 0.0, 2.0, league="mens", season=2020) == "rim"
    assert classify_zone_geometry(10.0, 0.0, 10.0, league="mens", season=2020) == "paint"
    assert classify_zone_geometry(18.0, 0.0, 18.0, league="mens", season=2020) == "mid"


def test_zone_threes_split_corner_vs_above_break():
    from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_geometry

    assert classify_zone_geometry(21.5, 22.0, 3.0, league="mens", season=2020) == "corner3"
    assert classify_zone_geometry(24.0, 0.0, 24.0, league="mens", season=2020) == "abovebreak3"


def test_zone_type_collapse():
    from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_type

    assert classify_zone_type("Dunk") == "rim"
    assert classify_zone_type("LayUpShot") == "rim"
    assert classify_zone_type("TipShot") == "rim"
    assert classify_zone_type("Three Point Jump Shot") == "arc3"
    assert classify_zone_type("JumpShot") == "jump"
    assert classify_zone_type(None) is None


# ---------------------------------------------------------------------------
# NCAA ShotEvent -> canonical frame
# ---------------------------------------------------------------------------


def test_shot_events_to_frame_schema_and_value():
    import datetime as dt

    import polars as pl

    from sportsdataverse.mbb.mbb_ncaa_models import ShotEvent, ShotGeo, ShotLocation
    from sportsdataverse.mbb.mbb_shots_adapter import shot_events_to_frame

    def _evt(x, y, dist, pts):
        return ShotEvent(
            player=None,
            date=dt.datetime(2024, 1, 1),
            location_type=None,
            team=None,
            opponent=None,
            is_off=True,
            lineup_id=None,
            players=[],
            score=None,
            min=5.0,
            loc=ShotLocation(x, y),
            geo=ShotGeo(0.0, 0.0),
            dist=dist,
            pts=pts,
            value=0,
            ast_by=None,
            is_ast=None,
            is_trans=None,
            raw_event=None,
        )

    df = shot_events_to_frame([_evt(0.0, 2.0, 2.0, 1), _evt(0.0, 24.0, 24.0, 1)], season=2024, league="mens")
    assert df.schema["team_id"] == pl.Utf8 and df.schema["point_value"] == pl.Int8
    assert df["point_value"].to_list() == [2, 3]
    assert df["shot_zone"].to_list() == ["rim", "abovebreak3"]
    assert df["made"].to_list() == [True, True]
    assert df["source"].unique().to_list() == ["ncaa"]
    assert df["shot_type"].unique().to_list() == ["unknown"]


def test_shot_events_to_frame_empty():
    import polars as pl

    from sportsdataverse.mbb.mbb_shots_adapter import CANONICAL_SHOT_SCHEMA, shot_events_to_frame

    df = shot_events_to_frame([], season=2024, league="mens")
    assert df.height == 0
    assert dict(df.schema) == dict(CANONICAL_SHOT_SCHEMA)
    assert df.schema["shooter_id"] == pl.Utf8
