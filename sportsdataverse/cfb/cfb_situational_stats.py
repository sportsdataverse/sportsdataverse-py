"""Situational team stats computed from the enriched plays frame.

Every section aggregates the enriched frame sdv-py retains as ``plays_frame``
(sdv-py #464); nothing here re-derives play-level modeling. Points columns use
``pos_score_pts`` -- points the OFFENSE scored on its own snaps (defensive and
return touchdowns are deliberately excluded, matching how "red zone points" or
"two-minute points" read).

Derivations beyond raw flags, per the metrics note:
- pace: seconds per play from within-drive clock deltas (cross-drive and
  cross-period gaps never count);
- garbage time: the scoreboard heuristic the site's spice levels use
  (up 38+ in Q2, 28+ in Q3, 22+ in Q4) -- non-garbage EPA ships next to raw;
- depth of target: bucketed from air_yards (behind / short 0-9 / medium
  10-19 / deep 20+); ESPN's pass_depth field is empty in practice.
"""

import polars as pl

__all__ = ["create_situational_stats"]

_TRUE = lambda c: pl.col(c) == True  # noqa: E712, E731


def _num(v, digits=3):
    return None if v is None else round(float(v), digits)


def _grp(df):
    """plays / EPA per play / success rate for a slice."""
    if df.height == 0:
        return {"plays": 0, "epa_play": None, "success_rate": None}
    return {
        "plays": df.height,
        "epa_play": _num(df["EPA"].mean()),
        "success_rate": _num(df["EPA_success"].mean()),
    }


def _points(df):
    return int(df["pos_score_pts"].fill_null(0).sum()) if df.height else 0


def create_situational_stats(
    frame: pl.DataFrame,
    home_id: str | int,
    away_id: str | int,
    window_expr: pl.Expr | None = None,
) -> dict | None:
    """Build the situational team-stats block from a plays frame.

    Window-inherent sections -- ``two_minute``, ``middle_8``, ``non_garbage``,
    ``pace``, ``fourth_down_decisions`` -- are omitted from a windowed build:
    they are themselves time windows or game-level filters, and
    double-windowing them is a category error.

    Args:
        frame (pl.DataFrame): the enriched plays frame from
            :meth:`CFBPlayProcess.run_processing_pipeline` (``plays_frame``).
        home_id: ESPN home team id.
        away_id: ESPN away team id.
        window_expr: optional polars filter expression windowing the
            windowable sections to that slice (e.g.
            ``pl.col("period") == 3``). ``None`` = full game.

    Returns:
        dict: ``{"teams": {<team_id>: {<section>: ...}}}`` or ``None`` when
        the frame is unusable or the window is empty.

    Example:
        Full-game stats from a processed game::

            stats = create_situational_stats(game.plays_frame, "52", "61")
    """
    if not isinstance(frame, pl.DataFrame) or frame.height == 0:
        return None
    # fail-open contract: every column this module touches must be present
    # up front, so a partially-enriched frame returns None instead of raising
    # ColumnNotFoundError deep inside a section
    needed = {
        "EPA",
        "EPA_penalty",
        "EPA_success",
        "TFL",
        "air_yards",
        "completion",
        "cpoe",
        "distance",
        "down",
        "drive.id",
        "fg_attempt",
        "fg_made",
        "firstD_by_penalty",
        "first_down_created",
        "fourth_down_recommendation",
        "fumble_lost",
        "fumble_vec",
        "game_play_number",
        "go_boost",
        "goal_to_go",
        "havoc",
        "int",
        "is_pos_team_turnover",
        "line_yards",
        "middle_8",
        "open_field_yards",
        "opportunity_run",
        "pass",
        "pass_breakup",
        "pass_oe",
        "penalty_1st_conv",
        "penalty_declined",
        "penalty_flag",
        "penalty_team_id",
        "period",
        "pos_score_diff",
        "pos_score_pts",
        "pos_team",
        "power_rush_attempt",
        "power_rush_success",
        "punt_play",
        "rush",
        "rz_play",
        "sack",
        "scoring_opp",
        "scrimmage_play",
        "second_level_yards",
        "short_rush_attempt",
        "short_rush_success",
        "start.adj_TimeSecsRem",
        "start.yardsToEndzone.touchback",
        "statYardage",
        "stuffed_run",
        "touchdown",
        "under_2",
        "xp_attempt",
        "yards_after_catch",
    }
    if not needed.issubset(set(frame.columns)):
        return None

    if window_expr is not None:
        frame = frame.filter(window_expr)
        if frame.height == 0:
            return None

    scrim = frame.filter(_TRUE("scrimmage_play"))
    out = {}
    for tid in (str(home_id), str(away_id)):
        mine = scrim.filter(pl.col("pos_team").cast(pl.Utf8) == tid)
        opp_off = scrim.filter(pl.col("pos_team").cast(pl.Utf8) != tid)
        t = {}

        # --- clutch windows (window-inherent: full-game builds only) --------
        if window_expr is None:
            u2 = mine.filter(_TRUE("under_2"))
            t["two_minute"] = {**_grp(u2), "points": _points(u2)}
            m8 = mine.filter(_TRUE("middle_8"))
            t["middle_8"] = {**_grp(m8), "points": _points(m8)}

        # --- red zone / finishing drives (trips need drive identity; plays
        # are already attributed by pos_team, so drive.id is safe HERE) ------
        rz = mine.filter(_TRUE("rz_play"))
        rz_trips = rz["drive.id"].n_unique() if rz.height else 0
        rz_tds = rz.filter(_TRUE("touchdown"))["drive.id"].n_unique() if rz.height else 0
        rz_fgs = rz.filter(_TRUE("fg_made"))["drive.id"].n_unique() if rz.height else 0
        t["red_zone"] = {
            **_grp(rz),
            "trips": rz_trips,
            "td_trips": rz_tds,
            "fg_trips": rz_fgs,
            "points": _points(rz),
            "points_per_trip": _num(_points(rz) / rz_trips, 2) if rz_trips else None,
        }
        g2g = mine.filter(_TRUE("goal_to_go"))
        t["goal_to_go"] = {
            **_grp(g2g),
            "tds": int(g2g.filter(_TRUE("touchdown")).height),
        }
        so = mine.filter(_TRUE("scoring_opp"))
        so_trips = so["drive.id"].n_unique() if so.height else 0
        t["finishing_drives"] = {
            "trips": so_trips,
            "points": _points(so),
            "points_per_trip": _num(_points(so) / so_trips, 2) if so_trips else None,
        }

        # --- down-by-down with distance tendencies --------------------------
        downs = {}
        for down in (1, 2, 3, 4):
            dd = mine.filter(pl.col("down") == down)
            d_rush = dd.filter(_TRUE("rush"))
            d_pass = dd.filter(_TRUE("pass") & (_TRUE("sack") == False))  # noqa: E712
            entry = {
                **_grp(dd),
                "avg_distance": _num(dd["distance"].mean(), 1) if dd.height else None,
                "yards_per_play": _num(dd["statYardage"].mean(), 1) if dd.height else None,
                "rush": {
                    "att": d_rush.height,
                    "yards": int(d_rush["statYardage"].fill_null(0).sum()),
                },
                "pass": {
                    "att": d_pass.height,
                    "comp": d_pass.filter(_TRUE("completion")).height,
                    "yards": int(d_pass["statYardage"].fill_null(0).sum()),
                },
                "pass_rate": _num(dd.select(_TRUE("pass").mean()).item()) if dd.height else None,
                "pass_rate_over_expected": _num(dd["pass_oe"].mean()) if dd.height else None,
            }
            if down in (3, 4):
                conv_df = dd.filter(_TRUE("first_down_created") | _TRUE("touchdown"))
                entry["conversions"] = {"made": conv_df.height, "att": dd.height}
                # how conversions happened: through the air, on the ground,
                # or gifted by penalty (the game-book attribution)
                entry["conversions_by"] = {
                    "rush": conv_df.filter(_TRUE("rush")).height,
                    "pass": conv_df.filter(_TRUE("pass")).height,
                    "penalty": dd.filter(_TRUE("firstD_by_penalty")).height,
                }
                buckets = {}
                for name, lo, hi in (
                    ("short", 0, 3),
                    ("medium", 4, 6),
                    ("long", 7, 99),
                ):
                    bb = dd.filter(pl.col("distance").is_between(lo, hi))
                    made = bb.filter(_TRUE("first_down_created") | _TRUE("touchdown")).height
                    buckets[name] = {"made": made, "att": bb.height}
                entry["by_distance"] = buckets
            downs[f"down_{down}"] = entry
        t["downs"] = downs

        # --- rushing quality ------------------------------------------------
        rushes = mine.filter(_TRUE("rush"))
        opp_att = int(rushes.select(_TRUE("opportunity_run").sum()).item()) if rushes.height else 0
        sacks = mine.filter(_TRUE("sack"))
        sack_yds = int(sacks["statYardage"].fill_null(0).sum())
        rush_yds = int(rushes["statYardage"].fill_null(0).sum())
        t["rushing_quality"] = {
            "attempts": rushes.height,
            "yards": rush_yds,
            # sack-adjusted by construction: the rush flag never covers sacks
            "yards_per_rush": _num(rushes["statYardage"].mean(), 1),
            # official NCAA team rushing folds sacks in
            "yards_per_rush_with_sacks": _num((rush_yds + sack_yds) / (rushes.height + sacks.height), 1)
            if rushes.height + sacks.height
            else None,
            "line_yards_per_rush": _num(rushes["line_yards"].mean(), 2),
            "second_level_per_rush": _num(rushes["second_level_yards"].mean(), 2),
            "open_field_per_rush": _num(rushes["open_field_yards"].mean(), 2),
            "stuff_rate": _num(rushes.select(_TRUE("stuffed_run").mean()).item()) if rushes.height else None,
            "opportunity_rate": _num(opp_att / rushes.height) if rushes.height else None,
            "power": {
                "made": int(mine.select(_TRUE("power_rush_success").sum()).item()),
                "att": int(mine.select(_TRUE("power_rush_attempt").sum()).item()),
            },
            "short_yardage": {
                "made": int(mine.select(_TRUE("short_rush_success").sum()).item()),
                "att": int(mine.select(_TRUE("short_rush_attempt").sum()).item()),
            },
        }

        # --- passing profile (air_yards buckets; pass_depth ships empty) ----
        passes = mine.filter(_TRUE("pass") & (_TRUE("sack") == False))  # noqa: E712
        comps = passes.filter(_TRUE("completion"))
        depth = {}
        for name, lo, hi in (
            ("behind_los", -99, -1),
            ("short", 0, 9),
            ("medium", 10, 19),
            ("deep", 20, 99),
        ):
            bb = passes.filter(pl.col("air_yards").is_between(lo, hi))
            depth[name] = {
                **_grp(bb),
                "completions": bb.filter(_TRUE("completion")).height,
            }
        t["passing_profile"] = {
            "attempts": passes.height,
            "dropbacks": int(mine.select(_TRUE("pass").sum()).item()),
            "sacks_taken": {"count": sacks.height, "yards_lost": -sack_yds},
            "yards_per_attempt": _num(passes["statYardage"].mean(), 1),
            "yards_per_completion": _num(comps["statYardage"].mean(), 1),
            "air_yards_per_att": _num(passes["air_yards"].mean(), 2),
            "yac_per_completion": _num(comps["yards_after_catch"].mean(), 2),
            "cpoe": _num(passes["cpoe"].mean()),
            "by_depth": depth,
        }

        # --- big plays: pass of 15+ / rush of 10+ ---------------------------
        bp_pass = mine.filter(_TRUE("pass") & (pl.col("statYardage") >= 15))
        bp_rush = mine.filter(_TRUE("rush") & (pl.col("statYardage") >= 10))

        def _bp(df):
            return {
                "plays": df.height,
                "yards": int(df["statYardage"].fill_null(0).sum()),
                "long": int(df["statYardage"].max()) if df.height else None,
                "touchdowns": df.filter(_TRUE("touchdown")).height,
            }

        t["big_plays"] = {
            "plays": bp_pass.height + bp_rush.height,
            "yards": int(bp_pass["statYardage"].fill_null(0).sum() + bp_rush["statYardage"].fill_null(0).sum()),
            "touchdowns": bp_pass.filter(_TRUE("touchdown")).height + bp_rush.filter(_TRUE("touchdown")).height,
            "pass": _bp(bp_pass),
            "rush": _bp(bp_rush),
        }

        # --- 4th-down decision report ---------------------------------------
        fourth = mine.filter((pl.col("down") == 4) & pl.col("fourth_down_recommendation").is_not_null())
        went = fourth.filter(
            _TRUE("scrimmage_play")
            & (_TRUE("punt_play") == False)
            & (_TRUE("fg_attempt") == False)
            & (_TRUE("xp_attempt") == False)
        )  # noqa: E712
        agree = 0
        forgone = 0.0
        for r in fourth.select(["fourth_down_recommendation", "punt_play", "fg_attempt", "go_boost"]).to_dicts():
            actual = "punt" if r.get("punt_play") else "field_goal" if r.get("fg_attempt") else "go"
            if actual == r["fourth_down_recommendation"]:
                agree += 1
            elif r["fourth_down_recommendation"] == "go" and (r.get("go_boost") or 0) > 0:
                forgone += r["go_boost"]
        t["fourth_down_decisions"] = {
            "decisions": fourth.height,
            "went_for_it": went.height,
            "agreed_with_model": agree,
            "agreement_rate": _num(agree / fourth.height) if fourth.height else None,
            "go_wp_forgone": _num(forgone, 2),
        }

        # --- score-state splits ---------------------------------------------
        states = {}
        for name, expr in (
            ("leading", pl.col("pos_score_diff") > 0),
            ("tied", pl.col("pos_score_diff") == 0),
            ("trailing", pl.col("pos_score_diff") < 0),
        ):
            ss = mine.filter(expr)
            states[name] = {
                **_grp(ss),
                "pass_rate": _num(ss.select(_TRUE("pass").mean()).item()) if ss.height else None,
            }
        t["score_state"] = states

        # --- penalties, situational -----------------------------------------
        my_pens = frame.filter(
            _TRUE("penalty_flag")
            & (_TRUE("penalty_declined") == False)  # noqa: E712
            & (pl.col("penalty_team_id").cast(pl.Utf8) == tid)
        )
        t["penalties_situational"] = {
            "accepted": my_pens.height,
            "drive_extending_committed": int(
                frame.select(
                    (_TRUE("penalty_1st_conv") & (pl.col("penalty_team_id").cast(pl.Utf8) == tid)).sum()
                ).item()
            ),
            "epa_swing": _num(my_pens["EPA_penalty"].fill_null(0).sum(), 2),
            "by_quarter": {int(k): int(v) for k, v in my_pens.group_by("period").len().iter_rows() if k is not None},
        }

        # --- havoc created (defense = opponent offensive snaps) -------------
        faced = opp_off
        t["havoc_created"] = {
            "rate": _num(faced.select(_TRUE("havoc").mean()).item()) if faced.height else None,
            "front_seven": int(faced.select(_TRUE("TFL").sum()).item()),
            "secondary": int(faced.select((_TRUE("pass_breakup") | _TRUE("int")).sum()).item()),
        }

        # --- turnovers -------------------------------------------------------
        my_tos = mine.filter(_TRUE("is_pos_team_turnover"))
        my_fumbles = mine.filter(_TRUE("fumble_vec"))
        t["turnovers"] = {
            "committed": my_tos.height,
            "epa_swing": _num(my_tos["EPA"].fill_null(0).sum(), 2),
            "fumbles": my_fumbles.height,
            "fumbles_lost": int(mine.select(_TRUE("fumble_lost").sum()).item()),
        }

        # --- field-zone success ---------------------------------------------
        zones = {}
        for name, lo, hi in (
            ("backed_up", 81, 100),
            ("own_side", 51, 80),
            ("plus_territory", 21, 50),
            ("red_zone", 0, 20),
        ):
            zz = (
                mine.filter(pl.col("start.yardsToEndzone.touchback").is_between(lo, hi))
                if "start.yardsToEndzone.touchback" in mine.columns
                else mine.head(0)
            )
            zones[name] = _grp(zz)
        t["field_zones"] = zones

        # --- pace (derived): within-drive clock deltas ----------------------
        secs = []
        if mine.height > 1:
            ordered = (
                mine.sort("game_play_number")
                .select(["drive.id", "period", "start.adj_TimeSecsRem", "pos_score_diff"])
                .to_dicts()
            )
            for i in range(len(ordered) - 1):
                a, b = ordered[i], ordered[i + 1]
                if a["drive.id"] == b["drive.id"] and a["period"] == b["period"]:
                    dt = (a["start.adj_TimeSecsRem"] or 0) - (b["start.adj_TimeSecsRem"] or 0)
                    if 0 < dt <= 60:
                        secs.append((dt, a["period"], a["pos_score_diff"] or 0))

        def pace(rows):
            return _num(sum(r[0] for r in rows) / len(rows), 1) if rows else None

        t["pace"] = {
            "seconds_per_play": pace(secs),
            "first_half": pace([r for r in secs if r[1] <= 2]),
            "second_half": pace([r for r in secs if r[1] > 2]),
            "leading": pace([r for r in secs if r[2] > 0]),
            "trailing": pace([r for r in secs if r[2] < 0]),
        }

        # --- garbage-time filter (derived): the spice-level heuristic -------
        margin = pl.col("pos_score_diff").fill_null(0).abs()
        garbage = (
            ((pl.col("period") == 2) & (margin > 38))
            | ((pl.col("period") == 3) & (margin > 28))
            | ((pl.col("period") >= 4) & (margin > 22))
        )
        t["non_garbage"] = _grp(mine.filter(~garbage))

        if window_expr is not None:
            # window-inherent or sample-size-noise sections never ship on a
            # windowed build (two_minute/middle_8 were skipped above)
            for k in ("fourth_down_decisions", "pace", "non_garbage"):
                t.pop(k, None)

        out[tid] = t
    return {"teams": out}
