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


def _one_event(ox: float, oy: float, team_id, home_team_id) -> pl.DataFrame:
    return pl.DataFrame(
        {"x_coord": [ox], "y_coord": [oy], "team_id": [team_id], "home_team_id": [home_team_id]},
        schema={"x_coord": pl.Float64, "y_coord": pl.Float64, "team_id": pl.Utf8, "home_team_id": pl.Utf8},
    )


def test_add_coord_transforms_centre_ice_is_origin_in_every_frame():
    """Canvas (300, 150) is centre ice: (0, 0) in the fixed, right and vertical frames."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    for team in ("1", "3"):  # home, then visitor
        out = add_coord_transforms(_one_event(300.0, 150.0, team, "1")).row(0, named=True)
        for col in ("x_coord_fixed", "y_coord_fixed", "x_coord_right", "y_coord_right"):
            assert out[col] == pytest.approx(0.0), (team, col)
        assert out["x_coord_vertical"] == pytest.approx(0.0) and out["y_coord_vertical"] == pytest.approx(0.0)


def test_add_coord_transforms_rotations_home_and_visitor():
    """Canvas (60, 30) is x = -80, y = 34 ft: near the net the home team attacks (sdv-internal-refs CANVAS.md).

    fixed = (-x, -y) for every event; right = home (-x, -y), visitor (x, y);
    vertical = (-y_right, x_right).
    """
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    home = add_coord_transforms(_one_event(60.0, 30.0, "1", "1")).row(0, named=True)
    away = add_coord_transforms(_one_event(60.0, 30.0, "3", "1")).row(0, named=True)
    for out in (home, away):
        assert (out["x_coord_fixed"], out["y_coord_fixed"]) == (pytest.approx(80.0), pytest.approx(-34.0))
    assert (home["x_coord_right"], home["y_coord_right"]) == (pytest.approx(80.0), pytest.approx(-34.0))
    assert (away["x_coord_right"], away["y_coord_right"]) == (pytest.approx(-80.0), pytest.approx(34.0))
    assert (home["x_coord_vertical"], home["y_coord_vertical"]) == (pytest.approx(34.0), pytest.approx(80.0))


def test_add_coord_transforms_unknown_side_is_null():
    """No team (faceoffs) or no home team id: the right and vertical frames are null, fixed is not."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms

    out = add_coord_transforms(_one_event(60.0, 30.0, None, "1")).row(0, named=True)
    assert out["x_coord_fixed"] == pytest.approx(80.0)
    assert all(out[c] is None for c in ("x_coord_right", "y_coord_right", "x_coord_vertical", "y_coord_vertical"))
    no_home = add_coord_transforms(_one_event(60.0, 30.0, "1", "1").drop("home_team_id")).row(0, named=True)
    assert no_home["x_coord_right"] is None and no_home["x_coord_fixed"] == pytest.approx(80.0)
    # enrich_pbp's home_team_id is "" when the game summary is unavailable
    empty = add_coord_transforms(_one_event(60.0, 30.0, "1", "")).row(0, named=True)
    assert empty["x_coord_right"] is None and empty["y_coord_vertical"] is None


def test_add_coord_transforms_real_game_keeps_every_shot_on_the_rink():
    """PWHL game 42: every shot and goal inside the rink in each frame; both teams attack +x when right."""
    from sportsdataverse.hockeytech import _parsers as P
    from sportsdataverse.hockeytech._analytics import enrich_pbp

    df = enrich_pbp(
        P.parse_pbp(load_fixture("hockeytech", "pwhl_pbp_42"), game_id=42),
        "pwhl",
        42,
        meta_payload=load_fixture("hockeytech", "pwhl_game_summary_42"),
        shifts_payload=load_fixture("hockeytech", "pwhl_gameshifts_42"),
    )
    shots = df.filter(pl.col("event").is_in(["shot", "goal"]) & pl.col("x_coord_original").is_not_null())
    shots = shots.with_columns(home=pl.col("team_id").cast(pl.Utf8) == pl.col("home_team_id").cast(pl.Utf8))
    assert shots["home"].sum() > 0 and (~shots["home"]).sum() > 0
    for col, lim in [("x_coord_fixed", 100), ("x_coord_right", 100), ("y_coord_vertical", 100),
                     ("y_coord_fixed", 42.5), ("y_coord_right", 42.5), ("x_coord_vertical", 42.5)]:  # fmt: skip
        assert shots[col].abs().max() <= lim, col
    med = shots.group_by("home").agg(pl.col("x_coord_right").median(), fixed=pl.col("x_coord_fixed").median())
    by = {r["home"]: r for r in med.iter_rows(named=True)}
    assert by[True]["x_coord_right"] > 0 and by[False]["x_coord_right"] > 0
    assert by[True]["fixed"] > 0 > by[False]["fixed"]
    # Faceoffs carry no team: their side is unknown, so the right frame is null but fixed is not.
    fo = df.filter((pl.col("event") == "faceoff") & pl.col("x_coord_original").is_not_null())
    assert fo.height > 0 and fo["x_coord_right"].is_null().all() and fo["x_coord_fixed"].is_not_null().all()


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
