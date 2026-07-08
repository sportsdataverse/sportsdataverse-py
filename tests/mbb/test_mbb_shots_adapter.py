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

    # ShotLocation is (x = up-court, y = lateral): a 2-ft shot at the rim
    # and a straight-on 24-footer (up-court 24, lateral 0)
    df = shot_events_to_frame([_evt(2.0, 0.0, 2.0, 1), _evt(24.0, 0.0, 24.0, 1)], season=2024, league="mens")
    assert df.schema["team_id"] == pl.Utf8 and df.schema["point_value"] == pl.Int8
    # defensive shots (is_off=False) attribute to the OPPONENT
    from sportsdataverse.mbb.mbb_ncaa_models import TeamId, TeamSeasonId, Year

    e = _evt(0.0, 2.0, 2.0, 1)
    e.team = TeamSeasonId(TeamId("A"), Year(2024))
    e.opponent = TeamSeasonId(TeamId("B"), Year(2024))
    e.is_off = False
    d2 = shot_events_to_frame([e], season=2024)
    assert d2["team_id"].to_list() == ["B"]
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


# ---------------------------------------------------------------------------
# ESPN shots -> canonical frame (fitted court scale)
# ---------------------------------------------------------------------------


def _fake_espn():
    import polars as pl

    # basket-anchored half-court grid like the real release (raw coords):
    # rim makes at (25, 2); three-point makes exactly 22.15 units away
    return pl.DataFrame(
        {
            "game_id": [1, 1, 1, 1],
            "season": [2025] * 4,
            "period_number": [1, 1, 2, 2],
            "clock_display_value": ["10:30", "5:00", "1:30", "0:45"],
            "team_id": [10, 10, 11, 11],
            "athlete_id_1": [7, 7, 8, 8],
            "type_text": ["LayUpShot", "DunkShot", "JumpShot", "JumpShot"],
            "scoring_play": [True, True, True, False],
            "score_value": [2, 2, 3, 3],
            "coordinate_x_raw": [25.0, 25.0, 25.0, 47.15],
            "coordinate_y_raw": [2.0, 2.0, 24.15, 2.0],
        }
    )


def test_fit_espn_court_scale_exact():
    from sportsdataverse.mbb.mbb_shots_adapter import fit_espn_court_scale

    ox, oy, fpu = fit_espn_court_scale(_fake_espn(), league="mens", season=2025)
    assert ox == 25.0 and oy == 2.0
    # made 3s sit exactly 22.15 units from the origin -> 1 foot per unit
    assert abs(fpu - 1.0) < 1e-9


def test_espn_shots_to_canonical():
    import polars as pl

    from sportsdataverse.mbb.mbb_shots_adapter import espn_shots_to_canonical

    out = espn_shots_to_canonical(_fake_espn(), league="mens", season=2025)
    assert out.schema["game_id"] == pl.Utf8
    assert out.schema["shooter_id"] == pl.Utf8
    assert out["source"].unique().to_list() == ["espn"]
    assert out["point_value"].to_list() == [2, 2, 3, 3]
    assert out["made"].to_list() == [True, True, True, False]
    assert out["shot_type"].to_list() == ["rim", "rim", "arc3", "arc3"]
    zones = out["shot_zone"].to_list()
    assert zones[0] == "rim" and zones[1] == "rim"
    assert zones[2] == "abovebreak3" and zones[3] == "corner3"
    assert out["sec_left"].to_list() == [630.0, 300.0, 90.0, 45.0]


def test_espn_shots_to_canonical_empty():
    import polars as pl

    from sportsdataverse.mbb.mbb_shots_adapter import CANONICAL_SHOT_SCHEMA, espn_shots_to_canonical

    out = espn_shots_to_canonical(pl.DataFrame(), league="mens", season=2025)
    assert out.height == 0
    assert dict(out.schema) == dict(CANONICAL_SHOT_SCHEMA)


def test_mbb_shot_data_espn_source(monkeypatch):

    import sportsdataverse.mbb.mbb_shots_adapter as ad

    monkeypatch.setattr("sportsdataverse.mbb.mbb_loaders.load_mbb_shots", lambda seasons: _fake_espn())
    out = ad.mbb_shot_data(2025)
    assert out.height == 4
    assert out["source"].unique().to_list() == ["espn"]
    assert dict(out.schema) == dict(ad.CANONICAL_SHOT_SCHEMA)


def test_mbb_shot_data_bad_source():
    import pytest

    from sportsdataverse.mbb.mbb_shots_adapter import mbb_shot_data

    with pytest.raises(ValueError):
        mbb_shot_data(2025, source="ncaa")


def test_committed_fixtures_round_trip():
    from pathlib import Path

    import polars as pl

    from sportsdataverse.mbb.mbb_shots_adapter import CANONICAL_SHOT_SCHEMA

    fix = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_shot_quality"
    for name in ("espn_shots_2025_train.parquet", "espn_shots_2025_holdout.parquet", "ncaa_shots_sample.parquet"):
        df = pl.read_parquet(fix / name)
        assert df.height > 0, name
        assert dict(df.schema) == dict(CANONICAL_SHOT_SCHEMA), name
        assert set(df["point_value"].unique().to_list()) <= {2, 3}, name
        assert set(df["shot_zone"].unique().to_list()) <= {"rim", "paint", "mid", "corner3", "abovebreak3"}, name


def test_ncaa_sample_orientation_pinned():
    """Canonical shot_x is LATERAL (symmetric), shot_y is up-court -- the
    NCAA source frame is the opposite and must be swapped at ingestion;
    every corner-3 must sit wide of the corner band."""
    from pathlib import Path

    import polars as pl

    fix = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_shot_quality"
    s = pl.read_parquet(fix / "ncaa_shots_sample.parquet")
    corner = s.filter(pl.col("shot_zone") == "corner3")
    assert corner.height > 0
    assert corner.filter(pl.col("shot_x").abs() < 21.0).height == 0
    # lateral axis symmetric, up-court axis one-sided
    assert float(s.get_column("shot_x").min()) < -5.0 < 5.0 < float(s.get_column("shot_x").max())
    assert float(s.get_column("shot_y").quantile(0.05)) > -3.0
