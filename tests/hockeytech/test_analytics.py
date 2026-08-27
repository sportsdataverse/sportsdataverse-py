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


def test_add_shot_distance_angle_default_is_nhl_size_constant():
    """R7-(2): the default goal_x is the documented NHL-size-rink constant, not a magic literal."""
    from sportsdataverse.hockeytech import _analytics

    assert _analytics._NHL_SIZE_RINK_GOAL_X == 89.0
    df = pl.DataFrame({"event": ["shot"], "x_coord": [25.0], "y_coord": [0.0]})
    # default (constant) and explicit 89.0 must agree
    d_default = _analytics.add_shot_distance_angle(df)["shot_distance"][0]
    d_explicit = _analytics.add_shot_distance_angle(df, goal_x=89.0)["shot_distance"][0]
    assert d_default == d_explicit


def test_add_shot_distance_angle_rejects_scale_error_goal_x():
    """R7-(2): a mis-scaled goal_x (e.g. RAW feed value) fails loud, not silent garbage."""
    import pytest

    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle

    df = pl.DataFrame({"event": ["shot"], "x_coord": [25.0], "y_coord": [0.0]})
    for bad in (0.0, -1.0, 300.0, 600.0):
        with pytest.raises(ValueError, match="goal_x"):
            add_shot_distance_angle(df, goal_x=bad)


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


def test_build_on_ice_line_change_boundary_no_double_count():
    """Regression: at a line change the outgoing shift's end_s equals the incoming
    shift's start_s equals the event time_s. The end boundary is EXCLUSIVE, so only
    the incoming line is on ice -- a closed interval double-counted both lines and
    produced impossible ~10-skater strength states (2026-07-12 fix)."""
    from sportsdataverse.hockeytech._analytics import build_on_ice

    # event at time_s=1000; player 1 (outgoing) [1200..1000] ends exactly at 1000,
    # player 2 (incoming) [1000..800] starts exactly at 1000. Only player 2 is on ice.
    pbp = pl.DataFrame({"event": ["faceoff"], "period_of_game": [1], "time_s": [1000], "team_id": [10]})
    shifts = pl.DataFrame(
        {
            "player_id": [1, 2],
            "home": [1, 1],
            "period": [1, 1],
            "start_s": [1200, 1000],
            "end_s": [1000, 800],
        }
    )
    out = build_on_ice(pbp, shifts)
    assert out["on_ice_home"][0] == "2", (
        f"line-change instant must belong only to the incoming shift, got "
        f"{out['on_ice_home'][0]!r} (closed-interval double-count regressed)"
    )


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


def test_add_strength_state_even_pp_and_pulled_goalie():
    from sportsdataverse.hockeytech._analytics import add_strength_state

    pbp = pl.DataFrame(
        {
            "event": ["shot", "shot", "shot", "shot"],
            "on_ice_home": ["1,2,3,4,5,99", "1,2,3,4,5,99", "1,2,3,4,5,6", None],
            "on_ice_away": ["6,7,8,9,10,88", "6,7,8,9,88", "7,8,9,10,11,88", "6,7,8,9,10,88"],
        }
    )
    out = add_strength_state(pbp, goalie_ids={"99", "88"})
    assert out["strength_state"][0] == "5v5"  # even strength (goalies 99/88 stripped)
    assert out["skaters_home"][0] == 5 and out["skaters_away"][0] == 5
    assert out["strength_state_valid"][0] is True
    assert out["strength_state"][1] == "5v4"  # home power play
    assert out["strength_state"][2] == "6v5"  # home pulled goalie -> 6 skaters
    assert out["strength_state_valid"][2] is True  # a pulled goalie is a valid 6-skater state
    assert out["strength_state"][3] is None  # null on-ice


def test_add_strength_state_flags_impossible_counts():
    from sportsdataverse.hockeytech._analytics import add_strength_state

    # 7 non-goalie skaters on ice = HockeyTech shift-boundary noise -> invalid
    pbp = pl.DataFrame({"on_ice_home": ["1,2,3,4,5,6,7,99"], "on_ice_away": ["10,11,12,13,14,88"]})
    out = add_strength_state(pbp, goalie_ids={"99", "88"})
    assert out["skaters_home"][0] == 7
    assert out["strength_state_valid"][0] is False


def test_add_strength_state_without_goalie_ids_assumes_one_goalie():
    from sportsdataverse.hockeytech._analytics import add_strength_state

    pbp = pl.DataFrame({"on_ice_home": ["1,2,3,4,5,99"], "on_ice_away": ["6,7,8,9,10,88"]})
    out = add_strength_state(pbp)  # no goalie_ids -> assume 1 goalie/side
    assert out["skaters_home"][0] == 5 and out["skaters_away"][0] == 5


def test_add_strength_state_empty_frame():
    from sportsdataverse.hockeytech._analytics import add_strength_state

    out = add_strength_state(pl.DataFrame({"on_ice_home": [], "on_ice_away": []}), goalie_ids={"1"})
    assert out.height == 0
    assert {"strength_state", "skaters_home", "strength_state_valid"}.issubset(out.columns)


def test_corsi_fenwick_team_counts_and_flag():
    from sportsdataverse.hockeytech._analytics import corsi_fenwick

    pbp = pl.DataFrame(
        {
            "event": ["shot", "blocked_shot", "goal", "faceoff", "shot"],
            "team_id": [10, 10, 20, 10, 20],
        }
    )
    team = corsi_fenwick(pbp)
    # missed-shot flag present and False
    assert "corsi_includes_missed" in team.columns
    assert not team["corsi_includes_missed"].any()
    t10 = team.filter(pl.col("team_id") == 10)
    t20 = team.filter(pl.col("team_id") == 20)
    # team 10 corsi-for: 1 shot + 1 blocked = 2 (faceoff ignored). team 20: 1 goal + 1 shot = 2
    assert t10["corsi_for"][0] == 2
    assert t20["corsi_for"][0] == 2
    # corsi_against is the other team's attempts
    assert t10["corsi_against"][0] == 2
    # fenwick excludes the blocked shot: team 10 fenwick_for = 1 (shot only), team 20 = 2 (goal+shot)
    assert t10["fenwick_for"][0] == 1
    assert t20["fenwick_for"][0] == 2


def test_corsi_fenwick_empty():
    from sportsdataverse.hockeytech._analytics import corsi_fenwick

    out = corsi_fenwick(pl.DataFrame({"event": [], "team_id": []}))
    assert out.height == 0 and "corsi_for" in out.columns


def test_per60_expression():
    from sportsdataverse.hockeytech._analytics import per60

    df = pl.DataFrame({"corsi_for": [10], "toi_seconds": [1800]})
    out = df.with_columns(per60("corsi_for"))
    # 10 / 1800 * 3600 = 20.0
    assert abs(out["corsi_for_per60"][0] - 20.0) < 1e-9


def test_coord_transforms_tolerate_string_coords():
    """Regression: all-None coords infer as Utf8; add_coord_transforms / add_shot_distance_angle
    must not raise 'division with String datatypes' after the cast(Float64, strict=False) fix."""
    from sportsdataverse.hockeytech._analytics import add_coord_transforms, add_shot_distance_angle

    # all-None coords infer as Utf8; must not raise
    df = pl.DataFrame({"event": ["faceoff", "shot"], "x_coord": [None, None], "y_coord": [None, None]})
    out = add_coord_transforms(df)
    assert "x_coord_original" in out.columns
    # null coords should produce null transforms, not errors
    assert out["x_coord_original"][0] is None

    out2 = add_shot_distance_angle(df)
    assert "shot_distance" in out2.columns
    # shot row with null coords yields null distance, not an exception
    assert out2["shot_distance"][1] is None

    # numeric strings also coerce cleanly
    df2 = pl.DataFrame({"event": ["shot"], "x_coord": ["255"], "y_coord": ["150"]})
    result = add_shot_distance_angle(df2)
    assert result["shot_distance"][0] is not None


def test_backfill_power_play_sets_flags_during_penalty():
    """Shots/faceoffs during an active PP window get power_play/short_handed flags.

    Synthetic scenario:
    - Penalty at sec_from_start=100, power_play="1", penalty_length="2",
      penalized team_id="2" (home_team_id="1", away_team_id="2").
      Advantage team = home (id="1"). PP window [100, 220].
    - Shot at sec=150 by team "1" -> power_play="1", short_handed="0".
    - Faceoff at sec=160 by team "2" -> power_play="0", short_handed="1".
    - Shot at sec=250 (after PP ends) -> unchanged (power_play=None).
    - Zero-penalty safety: no crash when pens is empty.
    """
    from sportsdataverse.hockeytech._analytics import backfill_power_play

    df = pl.DataFrame(
        {
            "event": ["penalty", "shot", "faceoff", "shot"],
            "sec_from_start": [100, 150, 160, 250],
            "team_id": ["2", "1", "2", "1"],
            "home_team_id": ["1", "1", "1", "1"],
            "away_team_id": ["2", "2", "2", "2"],
            "power_play": ["1", None, None, None],
            "short_handed": [None, None, None, None],
            "penalty_length": ["2", None, None, None],
        },
        schema={
            "event": pl.Utf8,
            "sec_from_start": pl.Int64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
            "away_team_id": pl.Utf8,
            "power_play": pl.Utf8,
            "short_handed": pl.Utf8,
            "penalty_length": pl.Utf8,
        },
    )
    out = backfill_power_play(df)

    # Shot at 150 by advantage team (home "1") -> PP
    assert out["power_play"][1] == "1", f"Expected '1', got {out['power_play'][1]}"
    assert out["short_handed"][1] == "0", f"Expected '0', got {out['short_handed'][1]}"

    # Faceoff at 160 by penalized team "2" -> SH
    assert out["power_play"][2] == "0", f"Expected '0', got {out['power_play'][2]}"
    assert out["short_handed"][2] == "1", f"Expected '1', got {out['short_handed'][2]}"

    # Shot at 250 is after PP window ends -> unchanged
    assert out["power_play"][3] is None, f"Expected None, got {out['power_play'][3]}"

    # Zero-penalty case: no crash
    df_no_pen = pl.DataFrame(
        {
            "event": ["shot"],
            "sec_from_start": [100],
            "team_id": ["1"],
            "home_team_id": ["1"],
            "away_team_id": ["2"],
            "power_play": [None],
            "short_handed": [None],
            "penalty_length": [None],
        },
        schema={
            "event": pl.Utf8,
            "sec_from_start": pl.Int64,
            "team_id": pl.Utf8,
            "home_team_id": pl.Utf8,
            "away_team_id": pl.Utf8,
            "power_play": pl.Utf8,
            "short_handed": pl.Utf8,
            "penalty_length": pl.Utf8,
        },
    )
    out_no_pen = backfill_power_play(df_no_pen)
    assert out_no_pen["power_play"][0] is None  # unchanged


def _official_onice_agreement(goal_epsilon_s):
    """Count fixture goals whose official plus/minus lists match shift-derived on-ice.

    The goal payload's ``plus_players`` / ``minus_players`` are the league's own
    on-ice lists -- ground truth in-feed. Returns ``(agreeing, total)``.
    """
    from sportsdataverse.hockeytech import _parsers as P
    from sportsdataverse.hockeytech._analytics import add_clock_columns, build_on_ice

    pbp = P.parse_pbp(load_fixture("hockeytech", "pwhl_pbp_42"), pbp_style="hockeytech_a", game_id=42)
    shifts = P.parse_shifts(load_fixture("hockeytech", "pwhl_gameshifts_42"), game_id=42)
    pbp = add_clock_columns(pbp)

    period_len = {
        int(r["period"]): int(r["plen"])
        for r in shifts.group_by("period").agg(pl.col("start_s").max().alias("plen")).iter_rows(named=True)
    }
    period_int = pl.col("period_of_game").cast(pl.Int64, strict=False)
    plen_expr = pl.lit(1200, dtype=pl.Int64)
    for pv, lv in period_len.items():
        plen_expr = pl.when(period_int == pv).then(pl.lit(lv, dtype=pl.Int64)).otherwise(plen_expr)

    prepped = pbp.with_columns(
        period_of_game=period_int,
        time_s=(plen_expr - (pl.col("minute_start") * 60 + pl.col("second_start"))).cast(pl.Int64, strict=False),
    )
    out = build_on_ice(prepped, shifts, goal_epsilon_s=goal_epsilon_s)

    goals = out.filter(pl.col("event") == "goal")
    plus_cols = [c for c in goals.columns if c.startswith("plus_player_") and c.endswith("_id")]
    minus_cols = [c for c in goals.columns if c.startswith("minus_player_") and c.endswith("_id")]

    def _ids(row, cols):
        # official plus/minus ids arrive Float64 ("163.0"); on-ice ids are Utf8 ints
        return {str(int(float(row[c]))) for c in cols if row[c] is not None}

    agree = 0
    for row in goals.iter_rows(named=True):
        plus = _ids(row, plus_cols)
        minus = _ids(row, minus_cols)
        on_home = set((row["on_ice_home"] or "").split(",")) - {""}
        on_away = set((row["on_ice_away"] or "").split(",")) - {""}
        # the scoring side may be either home or away; accept whichever side covers it
        if (plus <= on_home or plus <= on_away) and (minus <= on_home or minus <= on_away):
            agree += 1
    return agree, goals.height


def test_build_on_ice_goal_epsilon_fixes_shift_boundary_disagreement():
    """Issue #369: shift boundaries are unreliable at the goal instant.

    At ``goal_epsilon_s=0`` the shift chart has already rolled to the post-goal
    deployment, so the league's own ``plus_players``/``minus_players`` on-ice
    lists match NONE of the four goals in the committed game-42 fixture. The
    epsilon convention (evaluate 2s BEFORE the goal) recovers all four.
    """
    before_agree, total = _official_onice_agreement(goal_epsilon_s=0)
    after_agree, _ = _official_onice_agreement(goal_epsilon_s=2)

    assert total == 4
    assert before_agree == 0, f"expected the eps=0 defect on this fixture, got {before_agree}/{total}"
    assert after_agree == total, f"epsilon convention should fix all goals, got {after_agree}/{total}"


def test_build_on_ice_goal_epsilon_leaves_non_goal_events_untouched():
    """The epsilon shift applies to goal rows only -- every other event is unchanged."""
    from sportsdataverse.hockeytech._analytics import build_on_ice

    pbp = pl.DataFrame(
        {"event": ["shot", "goal"], "period_of_game": [1, 1], "time_s": [1000, 1000]},
        schema={"event": pl.Utf8, "period_of_game": pl.Int64, "time_s": pl.Int64},
    )
    # player 1 is on ice only in (1002, 1001]; player 2 only in (1000, 999].
    shifts = pl.DataFrame(
        {"player_id": [1, 2], "home": [1, 1], "period": [1, 1], "start_s": [1002, 1000], "end_s": [1001, 999]},
        schema={
            "player_id": pl.Int64,
            "home": pl.Int64,
            "period": pl.Int64,
            "start_s": pl.Int64,
            "end_s": pl.Int64,
        },
    )
    out = build_on_ice(pbp, shifts, goal_epsilon_s=2)
    # shot stays at t=1000 -> player 2; goal moves to t=1002 -> player 1
    assert out.filter(pl.col("event") == "shot")["on_ice_home"][0] == "2"
    assert out.filter(pl.col("event") == "goal")["on_ice_home"][0] == "1"


def test_build_on_ice_goal_epsilon_clamped_to_period_start():
    """A goal in the opening seconds must not be pushed outside every shift."""
    from sportsdataverse.hockeytech._analytics import build_on_ice

    pbp = pl.DataFrame(
        {"event": ["goal"], "period_of_game": [1], "time_s": [1200]},
        schema={"event": pl.Utf8, "period_of_game": pl.Int64, "time_s": pl.Int64},
    )
    shifts = pl.DataFrame(
        {"player_id": [7], "home": [1], "period": [1], "start_s": [1200], "end_s": [1150]},
        schema={
            "player_id": pl.Int64,
            "home": pl.Int64,
            "period": pl.Int64,
            "start_s": pl.Int64,
            "end_s": pl.Int64,
        },
    )
    out = build_on_ice(pbp, shifts, goal_epsilon_s=2)
    # unclamped this would look up t=1202 and find nothing
    assert out["on_ice_home"][0] == "7"
