import polars as pl

from sportsdataverse.cfb import cfb_situational_stats as situational_stats

HOME, AWAY = "10", "20"


def _frame():
    n = 10
    base = {
        "game_play_number": list(range(1, n + 1)),
        "scrimmage_play": [True] * n,
        "pos_team": [10, 10, 10, 10, 10, 10, 20, 20, 20, 20],
        "period": [1, 1, 2, 2, 3, 4, 1, 2, 3, 4],
        "down": [1, 3, 3, 4, 1, 2, 1, 3, 2, 1],
        "distance": [10, 2, 8, 1, 10, 5, 10, 5, 3, 10],
        "EPA": [0.5, 1.0, -0.5, 2.0, 0.2, -0.1, 0.3, -0.2, 0.1, 0.4],
        "statYardage": [12, 4, 15, 2, 11, 3, 6, 7, 14, -3],
        "EPA_success": [True, True, False, True, True, False, True, False, True, True],
        "pos_score_pts": [0, 0, 7, 0, 0, 3, 0, 0, 7, 0],
        "under_2": [False, False, True, False, False, False, False, True, False, False],
        "middle_8": [False, False, True, True, False, False, False, True, False, False],
        "rz_play": [False, False, True, True, False, False, False, False, True, False],
        "goal_to_go": [
            False,
            False,
            True,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
        "scoring_opp": [
            False,
            False,
            True,
            True,
            False,
            True,
            False,
            False,
            True,
            False,
        ],
        "touchdown": [
            False,
            False,
            True,
            False,
            False,
            False,
            False,
            False,
            True,
            False,
        ],
        "fg_made": [
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
            False,
        ],
        "first_down_created": [
            True,
            True,
            False,
            True,
            False,
            False,
            True,
            False,
            True,
            False,
        ],
        "firstD_by_penalty": [False] * n,
        "rush": [True, True, False, True, True, False, True, False, True, False],
        "pass": [False, False, True, False, False, True, False, True, False, True],
        "sack": [False] * n,
        "completion": [
            False,
            False,
            True,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
        ],
        "pass_oe": [None, None, 5.0, None, None, -3.0, None, 2.0, None, 1.0],
        "line_yards": [3.0, 2.0, None, 4.5, 1.0, None, 2.0, None, 5.0, None],
        "second_level_yards": [1.0, 0.0, None, 2.0, 0.0, None, 0.0, None, 3.0, None],
        "open_field_yards": [0.0, 0.0, None, 10.0, 0.0, None, 0.0, None, 8.0, None],
        "stuffed_run": [
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
            False,
            False,
        ],
        "opportunity_run": [
            True,
            False,
            False,
            True,
            False,
            False,
            False,
            False,
            True,
            False,
        ],
        "power_rush_attempt": [
            False,
            True,
            False,
            True,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
        "power_rush_success": [
            False,
            True,
            False,
            True,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
        "short_rush_attempt": [
            False,
            True,
            False,
            True,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
        "short_rush_success": [
            False,
            True,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
        "air_yards": [None, None, 12.0, None, None, 22.0, None, 3.0, None, -2.0],
        "yards_after_catch": [None, None, 4.0, None, None, None, None, 6.0, None, None],
        "cpoe": [None, None, 10.0, None, None, -5.0, None, 3.0, None, 1.0],
        "fourth_down_recommendation": [
            None,
            None,
            None,
            "go",
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        "punt_play": [False] * n,
        "fg_attempt": [
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
            False,
        ],
        "xp_attempt": [False] * n,
        "go_boost": [None, None, None, 2.5, None, None, None, None, None, None],
        "pos_score_diff": [0, 0, 0, 7, 7, 7, 0, -7, -7, 0],
        "penalty_flag": [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            True,
        ],
        "penalty_declined": [False] * n,
        "penalty_team_id": [None, None, None, None, None, None, None, None, None, 20],
        "penalty_1st_conv": [False] * n,
        "EPA_penalty": [None, None, None, None, None, None, None, None, None, -1.5],
        "havoc": [False, False, False, False, False, False, True, True, False, False],
        "TFL": [False, False, False, False, False, False, True, False, False, False],
        "pass_breakup": [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
        ],
        "int": [False] * n,
        "is_pos_team_turnover": [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
        ],
        "fumble_vec": [False] * n,
        "fumble_lost": [False] * n,
        "start.yardsToEndzone.touchback": [75, 55, 15, 10, 60, 30, 75, 45, 12, 85],
        "drive.id": ["a", "a", "b", "b", "c", "d", "e", "f", "g", "h"],
        "start.adj_TimeSecsRem": [
            3600.0,
            3580.0,
            1900.0,
            1875.0,
            1700.0,
            800.0,
            3400.0,
            1850.0,
            900.0,
            400.0,
        ],
    }
    # the aggregate contract widened (explosive rate, unit attribution): the
    # shared fixture stays scrimmage-only, so every kick flag is off and the
    # one away turnover (row 8) is an interception the offence threw
    base.update(
        {
            "EPA_explosive": [False, True, False, True, False, False, False, False, True, False],
            "kickoff_play": [False] * n,
            "kicking_team": [None] * n,
            "int_turnover": [False, False, False, False, False, False, False, True, False, False],
            "def_fumble_lost": [False] * n,
            "is_def_pos_team_turnover": [False] * n,
            "is_st_turnover": [False] * n,
            "is_turnover": base["is_pos_team_turnover"],
            "turnover_team": [None, None, None, None, None, None, None, 20, None, None],
            "yds_penalty": [None, None, None, None, None, None, None, None, None, 15],
        }
    )
    return pl.DataFrame(base, strict=False)


def test_situational_sections_and_values():
    out = situational_stats.create_situational_stats(_frame(), HOME, AWAY)
    h = out["teams"][HOME]
    a = out["teams"][AWAY]

    # every section of the metrics note is present
    for key in (
        "big_plays",
        "two_minute",
        "middle_8",
        "red_zone",
        "goal_to_go",
        "finishing_drives",
        "downs",
        "rushing_quality",
        "passing_profile",
        "fourth_down_decisions",
        "score_state",
        "penalties_situational",
        "havoc_created",
        "turnovers",
        "field_zones",
        "pace",
        "non_garbage",
    ):
        assert key in h, key

    assert h["two_minute"] == {
        "plays": 1,
        "epa_total": -0.5,
        "epa_play": -0.5,
        "success_rate": 0.0,
        "explosive_rate": 0.0,
        "points": 7,
    }
    assert h["red_zone"]["trips"] == 1 and h["red_zone"]["td_trips"] == 1
    assert h["red_zone"]["points"] == 7
    assert h["finishing_drives"]["trips"] == 2 and h["finishing_drives"]["points"] == 10
    # play 3 is a 3rd-down TD: a TD counts as a conversion (book rule)
    assert h["downs"]["down_3"]["conversions"] == {"made": 2, "att": 2}
    assert h["downs"]["down_3"]["by_distance"]["short"] == {"made": 1, "att": 1}
    assert h["rushing_quality"]["power"] == {"made": 2, "att": 2}
    assert h["rushing_quality"]["short_yardage"] == {"made": 1, "att": 2}
    assert h["passing_profile"]["by_depth"]["medium"]["plays"] == 1
    # article-mined additions: dropbacks/sacks, per-down splits, big plays
    assert h["passing_profile"]["dropbacks"] == 2
    assert h["passing_profile"]["sacks_taken"] == {"count": 0, "yards_lost": 0}
    assert h["passing_profile"]["yards_per_completion"] == 15.0
    assert h["rushing_quality"]["yards"] == 29
    assert h["rushing_quality"]["yards_per_rush"] == 7.2
    assert h["rushing_quality"]["yards_per_rush_with_sacks"] == 7.2
    assert h["downs"]["down_3"]["avg_distance"] == 5.0
    assert h["downs"]["down_3"]["yards_per_play"] == 9.5
    assert h["downs"]["down_3"]["rush"] == {"att": 1, "yards": 4}
    assert h["downs"]["down_3"]["pass"] == {"att": 1, "comp": 1, "yards": 15}
    assert h["downs"]["down_3"]["conversions_by"] == {
        "rush": 1,
        "pass": 1,
        "penalty": 0,
    }
    # big plays: pass 15+ (idx2 TD) + rushes 10+ (idx0, idx4)
    assert h["big_plays"]["plays"] == 3 and h["big_plays"]["yards"] == 38
    assert h["big_plays"]["touchdowns"] == 1
    assert h["big_plays"]["pass"]["long"] == 15
    assert h["big_plays"]["rush"] == {
        "plays": 2,
        "epa_total": 0.7,
        "epa_play": 0.35,
        "success_rate": 1.0,
        "explosive_rate": 0.0,
        "yards": 23,
        "long": 12,
        "touchdowns": 0,
    }
    # 4th down: recommendation 'go', they went -> agreement 1/1
    assert h["fourth_down_decisions"] == {
        "decisions": 1,
        "went_for_it": 1,
        "agreed_with_model": 1,
        "agreement_rate": 1.0,
        "go_wp_forgone": 0.0,
    }
    assert h["score_state"]["leading"]["plays"] == 3
    # away's accepted penalty with EPA swing
    assert a["penalties_situational"]["accepted"] == 1
    assert a["penalties_situational"]["epa_swing"] == -1.5
    # home's defense created the away havoc plays
    assert h["havoc_created"]["front_seven"] == 1 and h["havoc_created"]["secondary"] == 1
    assert a["turnovers"]["committed"] == 1 and a["turnovers"]["epa_swing"] == -0.2
    assert h["field_zones"]["red_zone"]["plays"] == 2
    # pace: only same-drive same-period consecutive deltas count
    assert h["pace"]["seconds_per_play"] is not None


def test_situational_fails_open():
    assert situational_stats.create_situational_stats(None, HOME, AWAY) is None
    assert situational_stats.create_situational_stats(pl.DataFrame(), HOME, AWAY) is None
    assert situational_stats.create_situational_stats(pl.DataFrame({"x": [1]}), HOME, AWAY) is None


def test_windowed_build_drops_only_the_clock_named_sections():
    expr = pl.col("period").is_in([1, 2])
    out = situational_stats.create_situational_stats(_frame(), HOME, AWAY, window_expr=expr)
    h = out["teams"][HOME]
    # windowable sections present and windowed
    assert h["downs"]["down_1"]["plays"] == 1  # only the Q1 first-down play
    assert h["red_zone"]["trips"] == 1
    # two_minute and middle_8 name a clock window of their own; intersecting
    # them with another window describes neither
    for k in ("two_minute", "middle_8"):
        assert k not in h, k
    # everything else reads the already-windowed frame and ships with it
    for k in ("pace", "non_garbage", "fourth_down_decisions"):
        assert k in h, k


def test_windowed_pace_is_the_window_and_drops_the_half_split():
    h1 = situational_stats.create_situational_stats(_frame(), HOME, AWAY, window_expr=pl.col("period") == 1)["teams"][
        HOME
    ]
    # a window inside one half cannot split by half: both keys go, rather than
    # shipping a permanently-null pair that would render as two blank rows
    assert "first_half" not in h1["pace"] and "second_half" not in h1["pace"]
    assert "seconds_per_play" in h1["pace"]
    # the full-game build still splits, because there both halves have plays
    full = situational_stats.create_situational_stats(_frame(), HOME, AWAY)["teams"][HOME]
    assert "first_half" in full["pace"] and "second_half" in full["pace"]


def test_windowed_build_empty_window_is_none():
    assert situational_stats.create_situational_stats(_frame(), HOME, AWAY, window_expr=pl.col("period") > 90) is None


def test_playprocess_delegate_matches_module():
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    f = _frame().with_columns(homeTeamId=pl.lit(int(HOME)), awayTeamId=pl.lit(int(AWAY)))
    direct = situational_stats.create_situational_stats(f, HOME, AWAY)
    via_method = CFBPlayProcess.create_situational_stats(object(), f)
    assert via_method == direct


def test_delegate_fails_open_on_unusable_frame():
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    assert CFBPlayProcess.create_situational_stats(object(), pl.DataFrame()) is None
    assert CFBPlayProcess.create_situational_stats(object(), None) is None


def test_partially_enriched_frame_fails_open():
    # the five original core columns alone must NOT be enough to proceed
    f = _frame().select(["scrimmage_play", "pos_team", "EPA", "EPA_success", "pos_score_pts"])
    assert situational_stats.create_situational_stats(f, HOME, AWAY) is None


# --- the five-field aggregate shape, everywhere -----------------------------


def _grp_nodes(node, path="", out=None):
    """Every dict carrying a `plays` key, with its path."""
    out = [] if out is None else out
    if isinstance(node, dict):
        if "plays" in node:
            out.append((path, node))
        for k, v in node.items():
            _grp_nodes(v, f"{path}.{k}" if path else k, out)
    return out


def test_every_slice_reports_the_same_five_fields():
    out = situational_stats.create_situational_stats(_frame(), HOME, AWAY)
    nodes = _grp_nodes(out["teams"][HOME])
    assert len(nodes) >= 20, [p for p, _ in nodes]
    for path, g in nodes:
        for k in ("plays", "epa_total", "epa_play", "success_rate", "explosive_rate"):
            assert k in g, (path, k)
    # the three sections that used to lack it now carry it
    h = out["teams"][HOME]
    for sec in ("finishing_drives", "rushing_quality", "big_plays"):
        assert "epa_play" in h[sec] and "explosive_rate" in h[sec], sec
    # and the values are the slice's, not invented: the total is the fixture's
    # own EPA over home rushes, whatever rows those happen to be
    rq = h["rushing_quality"]
    assert rq["plays"] == rq["attempts"]
    f = _frame()
    expect = f.filter((pl.col("rush") == True) & (pl.col("pos_team") == 10))["EPA"].sum()  # noqa: E712
    assert rq["epa_total"] == round(expect, 2)


def test_explosive_rate_is_the_share_of_explosive_plays():
    h = situational_stats.create_situational_stats(_frame(), HOME, AWAY)["teams"][HOME]
    # home scrimmage plays: rows 1-6, explosive on rows 2 and 4
    assert h["downs"]["down_3"]["explosive_rate"] == 0.5  # rows 2 (T) and 3 (F)
    assert h["field_zones"]["red_zone"]["plays"] > 0


# --- unit attribution ----------------------------------------------------------


def test_scrimmage_only_game_lands_everything_on_offense_or_defense():
    out = situational_stats.create_situational_stats(_frame(), HOME, AWAY)
    a = out["teams"][AWAY]
    pu = a["penalties_situational"]["by_unit"]
    # the one accepted flag is on team 20's own snap -> offence, 15 yards
    assert pu["offense"] == {"n": 1, "yards": 15, "auto_first": 0, "epa_swing": -1.5}
    assert pu["defense"]["n"] == 0 and pu["special_teams"]["n"] == 0
    for ph in ("kickoff", "kickoff_return", "punt", "punt_return", "fg_xp"):
        assert pu["special_teams"][ph]["n"] == 0, ph
    tu = a["turnovers"]["by_unit"]
    assert tu["offense"]["n"] == 1 and tu["offense"]["interceptions"] == 1
    assert tu["defense"]["n"] == 0 and tu["special_teams"]["n"] == 0


def _st_frame():
    """Three special-teams plays, so the phase split has something to file.

    row 0: team 10 punts, team 20 muffs the return and loses it   -> 20: ST punt_return turnover
    row 1: team 10 kicks off, a hold on 10's coverage unit          -> 10: ST kickoff penalty
    row 2: team 10 punts, a block in the back on 20's return        -> 20: ST punt_return penalty
    """
    n = 3
    f = _frame()
    cols = {c: [None] * n for c in f.columns}
    cols.update(
        {
            "game_play_number": [1, 2, 3],
            "scrimmage_play": [False] * n,
            "pos_team": [10, 10, 10],
            "period": [1, 2, 3],
            "EPA": [-3.0, -0.4, -0.6],
            "EPA_explosive": [False] * n,
            "EPA_success": [False] * n,
            "punt_play": [True, False, True],
            "kickoff_play": [False, True, False],
            "kicking_team": [10, 10, 10],
            "fg_attempt": [False] * n,
            "xp_attempt": [False] * n,
            "penalty_flag": [False, True, True],
            "penalty_declined": [False] * n,
            "penalty_team_id": [None, 10, 20],
            "penalty_1st_conv": [False] * n,
            "EPA_penalty": [None, -0.4, -0.6],
            "yds_penalty": [None, 10, 10],
            "fumble_vec": [True, False, False],
            "fumble_lost": [True, False, False],
            "def_fumble_lost": [False] * n,
            "int_turnover": [False] * n,
            "int": [False] * n,
            "is_pos_team_turnover": [False] * n,
            "is_def_pos_team_turnover": [False] * n,
            "is_st_turnover": [True, False, False],
            "is_turnover": [True, False, False],
            "turnover_team": [20, None, None],
            "under_2": [False] * n,
            "middle_8": [False] * n,
            "rz_play": [False] * n,
            "goal_to_go": [False] * n,
            "scoring_opp": [False] * n,
            "pos_score_pts": [0] * n,
            "pos_score_diff": [0] * n,
            "havoc": [False] * n,
            "pass_breakup": [False] * n,
            "rush": [False] * n,
            "pass": [False] * n,
            "sack": [False] * n,
            "completion": [False] * n,
            "touchdown": [False] * n,
            "first_down_created": [False] * n,
            "firstD_by_penalty": [False] * n,
            "stuffed_run": [False] * n,
            "opportunity_run": [False] * n,
            "power_rush_attempt": [False] * n,
            "power_rush_success": [False] * n,
            "short_rush_attempt": [False] * n,
            "short_rush_success": [False] * n,
            "fg_made": [False] * n,
            "TFL": [False] * n,
            "drive.id": ["s1", "s2", "s3"],
            "start.adj_TimeSecsRem": [3500.0, 2600.0, 1700.0],
            "start.yardsToEndzone.touchback": [60, 65, 55],
            "statYardage": [0, 0, 0],
        }
    )
    return pl.DataFrame(cols, strict=False)


def test_special_teams_phases_file_by_kicking_and_return_side():
    out = situational_stats.create_situational_stats(_st_frame(), HOME, AWAY)
    h, a = out["teams"][HOME], out["teams"][AWAY]
    # team 20 lost a muffed punt: an ST turnover on the RETURN side, and the
    # offence/defence buckets stay empty because it was never a scrimmage snap
    tu = a["turnovers"]["by_unit"]
    assert tu["special_teams"]["n"] == 1 and tu["special_teams"]["punt_return"]["n"] == 1
    assert tu["special_teams"]["punt_return"]["fumbles_lost"] == 1
    assert tu["special_teams"]["punt"]["n"] == 0 and tu["offense"]["n"] == 0
    assert tu["special_teams"]["epa_swing"] == -3.0
    # a hold on the kicking team's coverage files under kickoff, not kickoff_return
    pu_h = h["penalties_situational"]["by_unit"]
    assert pu_h["special_teams"]["kickoff"] == {"n": 1, "yards": 10, "auto_first": 0, "epa_swing": -0.4}
    assert pu_h["special_teams"]["kickoff_return"]["n"] == 0
    # a block in the back on the return files under punt_return for the returning team
    pu_a = a["penalties_situational"]["by_unit"]
    assert pu_a["special_teams"]["punt_return"]["n"] == 1 and pu_a["special_teams"]["punt"]["n"] == 0
    # and the kick outranks possession: 20 had no possession on any of these
    # plays, yet nothing of theirs landed in "defense"
    assert pu_a["defense"]["n"] == 0 and pu_a["offense"]["n"] == 0
    assert a["penalties_situational"]["accepted"] == pu_a["special_teams"]["n"] == 1


def test_by_unit_keys_exist_even_when_empty():
    a = situational_stats.create_situational_stats(_frame(), HOME, AWAY)["teams"][AWAY]
    for section in ("penalties_situational", "turnovers"):
        bu = a[section]["by_unit"]
        assert set(bu) == {"offense", "defense", "special_teams"}
        assert set(bu["special_teams"]) >= {"kickoff", "kickoff_return", "punt", "punt_return", "fg_xp", "n"}
