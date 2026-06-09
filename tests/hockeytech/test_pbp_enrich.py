"""Tests for pure PBP enrichment transforms: clock columns and coordinate transforms.

These are PURE frame->frame functions (no network) ported from fastRhockey's
``pwhl_pbp.R`` coordinate/clock logic.
"""

from __future__ import annotations

import polars as pl
import pytest

from tests.conftest import load_fixture

# ---------------------------------------------------------------------------
# Shared fixture helper
# ---------------------------------------------------------------------------


def _pbp():
    """Return parsed PBP for fixture game 42."""
    from sportsdataverse.hockeytech import _parsers as P

    return P.parse_pbp(load_fixture("hockeytech", "pwhl_pbp_42"), pbp_style="hockeytech_a", game_id=42)


# ---------------------------------------------------------------------------
# Clock columns -- add_clock_columns
# ---------------------------------------------------------------------------


def test_add_clock_columns_present():
    """All four clock columns must be present after enrichment."""
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    out = add_clock_columns(_pbp())
    for col in ("minute_start", "second_start", "clock", "sec_from_start"):
        assert col in out.columns, f"Missing column: {col}"


def test_add_clock_columns_sec_from_start_nonneg():
    """sec_from_start must be non-negative for all non-null values."""
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    out = add_clock_columns(_pbp())
    shots = out.filter(pl.col("event") == "shot")
    vals = shots["sec_from_start"].drop_nulls()
    assert vals.min() >= 0, f"Negative sec_from_start: {vals.min()}"


def test_add_clock_columns_sec_from_start_increases_within_period():
    """sec_from_start should be non-decreasing for shot/goal events within a period.

    Uses only shot/goal rows to avoid goalie_change events at 20:00 (end-of-period
    markers) that can appear after regular-time events.
    """
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    out = add_clock_columns(_pbp())
    for period in ("1", "2", "3"):
        p = (
            out.filter(pl.col("period_of_game") == period)
            .filter(pl.col("event").is_in(["shot", "goal", "faceoff"]))
            .filter(pl.col("sec_from_start").is_not_null())
        )
        if p.height < 2:
            continue
        vals = p["sec_from_start"].to_list()
        diffs = [vals[i + 1] - vals[i] for i in range(len(vals) - 1)]
        assert all(d >= 0 for d in diffs), f"Period {period}: sec_from_start decreased: {diffs}"


def test_add_clock_columns_synthetic_period1_elapsed():
    """Deterministic check: elapsed 3:12 in period 1.

    minute_start=3, second_start=12
    clock = "16:48"  (20:00 - 3:12 = 16:48)
    sec_from_start = 3*60 + 12 = 192  (period 1, no offset)
    """
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    df = pl.DataFrame(
        {
            "time_of_period": ["3:12"],
            "period_of_game": ["1"],
            "x_coord": [None],
            "y_coord": [None],
        },
        schema={
            "time_of_period": pl.Utf8,
            "period_of_game": pl.Utf8,
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
        },
    )
    out = add_clock_columns(df)
    assert out["minute_start"][0] == 3
    assert out["second_start"][0] == 12
    assert out["clock"][0] == "16:48"
    assert out["sec_from_start"][0] == 192


def test_add_clock_columns_synthetic_period2_offset():
    """Deterministic check: elapsed 0:00 in period 2 -> sec_from_start = 1200."""
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    df = pl.DataFrame(
        {
            "time_of_period": ["0:00"],
            "period_of_game": ["2"],
            "x_coord": [None],
            "y_coord": [None],
        },
        schema={
            "time_of_period": pl.Utf8,
            "period_of_game": pl.Utf8,
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
        },
    )
    out = add_clock_columns(df)
    assert out["minute_start"][0] == 0
    assert out["second_start"][0] == 0
    assert out["clock"][0] == "20:00"
    assert out["sec_from_start"][0] == 1200  # period 2 offset = 1200


def test_add_clock_columns_synthetic_clock_boundary():
    """Deterministic check: elapsed 20:00 in period 1.

    The R formula produces minute = 19 - 20 = -1 for this edge case
    (a known quirk in the R source that we faithfully reproduce).
    sec_from_start = 20*60 + 0 = 1200.
    """
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    df = pl.DataFrame(
        {
            "time_of_period": ["20:00"],
            "period_of_game": ["1"],
            "x_coord": [None],
            "y_coord": [None],
        },
        schema={
            "time_of_period": pl.Utf8,
            "period_of_game": pl.Utf8,
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
        },
    )
    out = add_clock_columns(df)
    assert out["minute_start"][0] == 20
    assert out["second_start"][0] == 0
    # R produces "-1:00" for elapsed=20:00 (19 - 20 = -1); faithful port
    assert out["clock"][0] == "-1:00"
    assert out["sec_from_start"][0] == 1200


def test_add_clock_columns_period3_offset():
    """Deterministic check: elapsed 1:30 in period 3 -> sec_from_start = 2490."""
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    df = pl.DataFrame(
        {
            "time_of_period": ["1:30"],
            "period_of_game": ["3"],
            "x_coord": [None],
            "y_coord": [None],
        },
        schema={
            "time_of_period": pl.Utf8,
            "period_of_game": pl.Utf8,
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
        },
    )
    out = add_clock_columns(df)
    assert out["sec_from_start"][0] == 2400 + 90  # 2490


def test_add_clock_columns_empty_frame():
    """Empty frame returns the four clock columns (zero rows)."""
    from sportsdataverse.hockeytech._analytics import add_clock_columns

    df = pl.DataFrame(
        schema={
            "time_of_period": pl.Utf8,
            "period_of_game": pl.Utf8,
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
        }
    )
    out = add_clock_columns(df)
    for col in ("minute_start", "second_start", "clock", "sec_from_start"):
        assert col in out.columns
    assert out.height == 0


# ---------------------------------------------------------------------------
# Coordinate transforms -- add_coord_transforms
# ---------------------------------------------------------------------------


def test_add_coord_transforms_present():
    """All ten coordinate-transform columns must be present."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    out = add_coord_transforms(_pbp())
    for col in (
        "x_coord_original",
        "y_coord_original",
        "x_coord_neutral",
        "y_coord_neutral",
        "x_coord_fixed",
        "y_coord_fixed",
        "x_coord_right",
        "y_coord_right",
        "x_coord_vertical",
        "y_coord_vertical",
    ):
        assert col in out.columns, f"Missing column: {col}"


def test_add_coord_transforms_original_preserves_raw():
    """x_coord_original / y_coord_original must equal the raw x_coord / y_coord."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    raw = _pbp()
    out = add_coord_transforms(raw)
    raw_x = raw["x_coord"].drop_nulls().to_list()[:10]
    raw_y = raw["y_coord"].drop_nulls().to_list()[:10]
    out_x = out["x_coord_original"].drop_nulls().to_list()[:10]
    out_y = out["y_coord_original"].drop_nulls().to_list()[:10]
    assert out_x == raw_x, f"x_coord_original mismatch: {out_x} != {raw_x}"
    assert out_y == raw_y, f"y_coord_original mismatch: {out_y} != {raw_y}"


def test_add_coord_transforms_neutral_synthetic():
    """Deterministic check: neutral coords = raw - (300, 150)."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    df = pl.DataFrame(
        {
            "x_coord": [300.0],
            "y_coord": [150.0],
            "team_id": [None],
            "home_team_id": [None],
        },
        schema={
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
        },
    )
    out = add_coord_transforms(df)
    assert out["x_coord_neutral"][0] == pytest.approx(0.0)
    assert out["y_coord_neutral"][0] == pytest.approx(0.0)


def test_add_coord_transforms_fixed_synthetic():
    """Deterministic check for x_coord_fixed and y_coord_fixed (ported from R).

    R formula (using original raw coords ox, oy):
      x_transformed = (ox / 3) - 100
      y_transformed = 42.5 - ((oy * 85 / 300) - 42.5) - 42.5

      x_coord_fixed = x_transformed / 3   [uses .data$x_coord = transformed]
      y_coord_fixed = 42.5 - ((y_transformed * 85 / 300) - 42.5)   [uses transformed y]

    With ox=300, oy=150:
      x_t = 300/3 - 100 = 0.0
      y_t = 42.5 - (150*85/300 - 42.5) - 42.5 = 42.5 - (42.5 - 42.5) - 42.5 = 0.0
      x_coord_fixed = 0.0 / 3 = 0.0
      y_coord_fixed = 42.5 - ((0.0 * 85/300) - 42.5) = 42.5 + 42.5 = 85.0
    """
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    df = pl.DataFrame(
        {
            "x_coord": [300.0],
            "y_coord": [150.0],
            "team_id": [None],
            "home_team_id": [None],
        },
        schema={
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
        },
    )
    out = add_coord_transforms(df)
    # x_t = (300/3) - 100 = 0.0
    # y_t = 42.5 - (150*85/300 - 42.5) - 42.5 = 42.5 - 0 - 42.5 = 0.0
    # x_coord_fixed = x_t / 3 = 0.0
    # y_coord_fixed = 42.5 - (y_t * 85/300 - 42.5) = 42.5 - (0 - 42.5) = 85.0
    assert out["x_coord_fixed"][0] == pytest.approx(0.0)
    assert out["y_coord_fixed"][0] == pytest.approx(85.0)


def test_add_coord_transforms_right_away_team_passthrough():
    """For away team, x_coord_right and y_coord_right equal the transformed x/y coords."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    # Use ox=300, oy=150 so x_t=0, y_t=0
    # Away team: x_coord_right = x_t = 0, y_coord_right = y_t = 0
    df = pl.DataFrame(
        {
            "x_coord": [300.0],
            "y_coord": [150.0],
            "team_id": ["3"],
            "home_team_id": ["1"],
        },
        schema={
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
        },
    )
    out = add_coord_transforms(df)
    # x_t = 0, y_t = 0; away team: right = passthrough
    assert out["x_coord_right"][0] == pytest.approx(0.0)
    assert out["y_coord_right"][0] == pytest.approx(0.0)


def test_add_coord_transforms_right_home_team_flipped():
    """For home team, x_coord_right = 100 + (100 - x_t), y_coord_right = 42.5 - (y_t - 42.5)."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    # ox=300, oy=150 -> x_t=0, y_t=0
    # Home team: x_right = 100 + (100 - 0) = 200, y_right = 42.5 - (0 - 42.5) = 85
    df = pl.DataFrame(
        {
            "x_coord": [300.0],
            "y_coord": [150.0],
            "team_id": ["1"],
            "home_team_id": ["1"],
        },
        schema={
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
        },
    )
    out = add_coord_transforms(df)
    assert out["x_coord_right"][0] == pytest.approx(200.0)
    assert out["y_coord_right"][0] == pytest.approx(85.0)


def test_add_coord_transforms_vertical_synthetic():
    """x_coord_vertical = 42.5 - (y_coord_right - 42.5), y_coord_vertical = x_coord_right."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    # Home team, ox=300, oy=150 -> x_right=200, y_right=85
    # x_vertical = 42.5 - (85 - 42.5) = 42.5 - 42.5 = 0.0
    # y_vertical = 200
    df = pl.DataFrame(
        {
            "x_coord": [300.0],
            "y_coord": [150.0],
            "team_id": ["1"],
            "home_team_id": ["1"],
        },
        schema={
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
        },
    )
    out = add_coord_transforms(df)
    assert out["x_coord_vertical"][0] == pytest.approx(0.0)
    assert out["y_coord_vertical"][0] == pytest.approx(200.0)


def test_add_coord_transforms_empty_frame():
    """Empty frame returns all ten coord columns (zero rows)."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    df = pl.DataFrame(
        schema={
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
        }
    )
    out = add_coord_transforms(df)
    for col in (
        "x_coord_original",
        "y_coord_original",
        "x_coord_neutral",
        "y_coord_neutral",
        "x_coord_fixed",
        "y_coord_fixed",
        "x_coord_right",
        "y_coord_right",
        "x_coord_vertical",
        "y_coord_vertical",
    ):
        assert col in out.columns, f"Missing column: {col}"
    assert out.height == 0


def test_add_coord_transforms_null_coords_passthrough():
    """Rows with null x_coord/y_coord produce null in all transformed coord columns."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    df = pl.DataFrame(
        {
            "x_coord": [None],
            "y_coord": [None],
            "team_id": ["1"],
            "home_team_id": ["1"],
        },
        schema={
            "x_coord": pl.Float64,
            "y_coord": pl.Float64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
        },
    )
    out = add_coord_transforms(df)
    for col in (
        "x_coord_original",
        "y_coord_original",
        "x_coord_neutral",
        "y_coord_neutral",
        "x_coord_fixed",
        "y_coord_fixed",
        "x_coord_right",
        "y_coord_right",
        "x_coord_vertical",
        "y_coord_vertical",
    ):
        assert out[col][0] is None, f"Expected null for {col} on null-coord row"


def test_add_coord_transforms_shots_populated_on_real_data():
    """Shot rows in game 42 should have non-null coord transforms (shots have coords)."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    out = add_coord_transforms(_pbp())
    shots = out.filter(pl.col("event") == "shot")
    # All shots should have x_coord_original populated
    assert shots["x_coord_original"].drop_nulls().len() == shots.height
    assert shots["y_coord_original"].drop_nulls().len() == shots.height
