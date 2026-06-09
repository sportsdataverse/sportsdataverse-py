from __future__ import annotations

import polars as pl
from tests.conftest import load_fixture


def test_shot_distance_angle_on_known_point():
    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle

    df = pl.DataFrame({"event": ["shot"], "x_coord": [25.0], "y_coord": [0.0]})
    out = add_shot_distance_angle(df, goal_x=89.0)
    assert "shot_distance" in out.columns and "shot_angle" in out.columns
    # straight on from x=25 -> 64 ft, angle 0
    assert abs(out["shot_distance"][0] - 64.0) < 1e-6
    assert abs(out["shot_angle"][0] - 0.0) < 1e-6


def test_shot_distance_angle_non_shot_rows_null():
    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle

    df = pl.DataFrame({"event": ["faceoff"], "x_coord": [25.0], "y_coord": [0.0]})
    out = add_shot_distance_angle(df)
    assert out["shot_distance"][0] is None


def test_shot_distance_angle_empty_frame():
    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle

    df = pl.DataFrame({"event": [], "x_coord": [], "y_coord": []})
    out = add_shot_distance_angle(df)
    assert "shot_distance" in out.columns and out.height == 0


def test_scoring_chance_flags_close_shots():
    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle, scoring_chances

    df = pl.DataFrame({"event": ["shot", "shot"], "x_coord": [80.0, 10.0], "y_coord": [2.0, 2.0]})
    out = scoring_chances(add_shot_distance_angle(df))
    assert "scoring_chance" in out.columns
    assert out["scoring_chance"][0] is True  # 9 ft from net
    assert out["scoring_chance"][1] is False  # ~79 ft from net


def test_player_toi_sums_shift_lengths():
    from sportsdataverse.hockeytech._analytics import player_toi

    shifts = pl.DataFrame(
        {
            "player_id": [1, 1, 2],
            "first_name": ["A", "A", "B"],
            "last_name": ["X", "X", "Y"],
            "period": [1, 1, 1],
            "start_s": [1200, 1100, 1200],
            "end_s": [1180, 1090, 1150],
        }
    )
    out = player_toi(shifts)
    assert "toi_seconds" in out.columns and "num_shifts" in out.columns
    a = out.filter(pl.col("player_id") == 1)
    assert a["toi_seconds"][0] == 30  # (1200-1180) + (1100-1090)
    assert a["num_shifts"][0] == 2


def test_player_toi_empty():
    from sportsdataverse.hockeytech._analytics import player_toi

    out = player_toi(
        pl.DataFrame(
            schema={
                "player_id": pl.Int64,
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "start_s": pl.Int64,
                "end_s": pl.Int64,
            }
        )
    )
    assert out.height == 0 and "toi_seconds" in out.columns


def test_build_on_ice_matches_interval_on_countdown_clock():
    from sportsdataverse.hockeytech._analytics import build_on_ice

    # period 1; event at time_s=1190 (countdown). Player 1 shift [1200..1180] covers it;
    # player 2 shift [1100..1090] does not.
    pbp = pl.DataFrame({"event": ["shot"], "period_of_game": [1], "time_s": [1190], "team_id": [10]})
    shifts = pl.DataFrame(
        {
            "player_id": [1, 2],
            "home": [1, 1],
            "period": [1, 1],
            "start_s": [1200, 1100],
            "end_s": [1180, 1090],
        }
    )
    out = build_on_ice(pbp, shifts)
    assert "on_ice_home" in out.columns and "on_ice_away" in out.columns
    assert out["on_ice_home"][0] == "1"  # only player 1 on ice
    assert out.height == pbp.height  # one row per original event, order preserved


def test_build_on_ice_separates_home_and_away():
    from sportsdataverse.hockeytech._analytics import build_on_ice

    pbp = pl.DataFrame({"event": ["shot"], "period_of_game": [2], "time_s": [600], "team_id": [10]})
    shifts = pl.DataFrame(
        {
            "player_id": [11, 12, 21],
            "home": [1, 1, 0],
            "period": [2, 2, 2],
            "start_s": [700, 700, 700],
            "end_s": [500, 500, 500],
        }
    )
    out = build_on_ice(pbp, shifts)
    assert out["on_ice_home"][0] == "11,12"
    assert out["on_ice_away"][0] == "21"


def test_build_on_ice_empty_inputs():
    from sportsdataverse.hockeytech._analytics import build_on_ice

    pbp = pl.DataFrame({"event": ["shot"], "period_of_game": [1], "time_s": [100], "team_id": [10]})
    out = build_on_ice(
        pbp,
        pl.DataFrame(
            schema={"player_id": pl.Int64, "home": pl.Int64, "period": pl.Int64, "start_s": pl.Int64, "end_s": pl.Int64}
        ),
    )
    assert out["on_ice_home"][0] is None and out.height == 1


def test_build_on_ice_real_data_multi_player_split():
    """Regression test: build_on_ice must return multiple players per side on real data.

    Historically broken because pbp already carries a ``player_id`` column,
    causing the shifts ``player_id`` to be renamed ``player_id_right`` after the
    join; the aggregation then picked up the wrong (pbp-event) column, returning
    one Float64-formatted id per side with home == away.

    Ground truth for game 42, first shot (period=1, time_s=1008):
      8 home players, 9 away players, ids integer-formatted, home != away.
    """
    from sportsdataverse.hockeytech import _analytics as A
    from sportsdataverse.hockeytech import _parsers as P

    def _mmss(s):
        if s is None:
            return None
        m, sec = str(s).split(":")
        return int(m) * 60 + int(sec)

    pbp_raw = P.parse_pbp(load_fixture("hockeytech", "pwhl_pbp_42"), pbp_style="hockeytech_a", game_id=42)
    shifts = P.parse_shifts(load_fixture("hockeytech", "pwhl_gameshifts_42"), game_id=42)

    # Convert elapsed MM:SS -> remaining seconds (countdown clock)
    plen = shifts.group_by("period").agg(plen=pl.col("start_s").max())
    pbp = (
        pbp_raw.with_columns(
            period_of_game=pl.col("period_of_game").cast(pl.Int64, strict=False),
            _elapsed=pl.col("time_of_period").map_elements(_mmss, return_dtype=pl.Int64),
        )
        .join(plen, left_on="period_of_game", right_on="period", how="left")
        .with_columns(time_s=(pl.col("plen") - pl.col("_elapsed")).cast(pl.Int64))
    )

    out = A.build_on_ice(pbp, shifts)

    shots = out.filter(pl.col("event") == "shot").filter(pl.col("on_ice_home").is_not_null())
    assert shots.height > 0, "No shots with on_ice data found"

    # --- assertion on the first shot (ground truth: 8 home, 9 away) ---
    first = shots.head(1)
    home_ids = first["on_ice_home"][0].split(",")
    away_ids = first["on_ice_away"][0].split(",")

    assert len(home_ids) >= 5, f"Expected >=5 home players, got {len(home_ids)}: {home_ids}"
    assert len(away_ids) >= 5, f"Expected >=5 away players, got {len(away_ids)}: {away_ids}"

    # ids must be integer-formatted (no '.' from Float64 cast)
    for pid in home_ids + away_ids:
        assert "." not in pid, f"Float-formatted player id found: {pid!r}"

    # home and away must differ
    assert first["on_ice_home"][0] != first["on_ice_away"][0], "on_ice_home must not equal on_ice_away"

    # --- sanity check across all shots: average >= 5 players per side ---
    home_counts = shots["on_ice_home"].map_elements(lambda s: len(s.split(",")), return_dtype=pl.Int64)
    away_counts = shots["on_ice_away"].map_elements(lambda s: len(s.split(",")), return_dtype=pl.Int64)
    assert home_counts.mean() >= 5, f"Mean home players per shot too low: {home_counts.mean()}"
    assert away_counts.mean() >= 5, f"Mean away players per shot too low: {away_counts.mean()}"
