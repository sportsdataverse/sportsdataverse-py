from sportsdataverse.cfb.cfb_pbp import _parse_recovery_abbrev, _strip_overturned_text


def test_strip_overturned_removes_original_play_clause():
    t = (
        "#11 C.Bailey sacked for loss of 2 yards to the FSU49 (#7 S.Thompson). "
        'The previous play is under automatic review - "Runner was down by contact". '
        "CALL OVERTURNED. (Original Play: (11:34) #11 C.Bailey sacked for loss of 1 yard "
        "to the FSU48, fumble by #11 C.Bailey recovered by FSU #40 A.Williams at FSU48, End Of Play)"
    )
    cleaned = _strip_overturned_text(t)
    assert "fumble by" not in cleaned
    assert "recovered by FSU" not in cleaned
    assert "C.Bailey sacked" in cleaned  # the ruled (kept) portion survives


def test_strip_overturned_noop_on_normal_text():
    t = "#4 S.White return 2 yards fumbled by #4 S.White recovered by NCSU #4 T.Thomas"
    assert _strip_overturned_text(t) == t


def test_parse_recovery_abbrev_basic():
    assert _parse_recovery_abbrev("… fumbled by #4 S.White recovered by NCSU #4 T.Thomas at FSU16") == "NCSU"


def test_parse_recovery_abbrev_muff():
    assert _parse_recovery_abbrev("punt 25 yards muffed by #24 K.Kirkland recovered by NCSU #98 C.Noonkester") == "NCSU"


def test_parse_recovery_abbrev_none_when_absent():
    assert _parse_recovery_abbrev("#22 J.Doe run for 4 yards") is None


def test_parse_recovery_abbrev_empty_string():
    assert _parse_recovery_abbrev("") is None


def test_parse_recovery_abbrev_none_input():
    assert _parse_recovery_abbrev(None) is None


def test_strip_overturned_empty_string():
    assert _strip_overturned_text("") == ""


def test_strip_overturned_none_input():
    assert _strip_overturned_text(None) is None


import polars as pl

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess


def _attr(rows: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(rows)
    proc = CFBPlayProcess(gameId=1)
    return proc._CFBPlayProcess__add_attribution_cols(df)


def test_kicking_return_team_flip():
    rows = [
        {
            "pos_team": 9,
            "def_pos_team": 252,
            "kickoff_play": True,
            "punt": False,
            "fg_attempt": False,
            "sp": True,
            "scrimmage_play": False,
            "fumble_vec": False,
            "int": False,
            "type.text": "Kickoff",
            "change_of_poss": False,
            "text": "kickoff",
            "homeTeamAbbrev": "BYU",
            "awayTeamAbbrev": "ASU",
            "homeTeamId": 252,
            "awayTeamId": 9,
            "penalty_detail": None,
            "yds_penalty": None,
            "end.pos_team.id": 9,
        },
        {
            "pos_team": 252,
            "def_pos_team": 9,
            "kickoff_play": False,
            "punt": True,
            "fg_attempt": False,
            "sp": True,
            "scrimmage_play": False,
            "fumble_vec": False,
            "int": False,
            "type.text": "Punt",
            "change_of_poss": True,
            "text": "punt",
            "homeTeamAbbrev": "BYU",
            "awayTeamAbbrev": "ASU",
            "homeTeamId": 252,
            "awayTeamId": 9,
            "penalty_detail": None,
            "yds_penalty": None,
            "end.pos_team.id": 9,
        },
    ]
    out = _attr(rows)
    assert out["kicking_team"].to_list() == [252, 252]  # kickoff->def, punt->pos
    assert out["return_team"].to_list() == [9, 9]  # kickoff->pos, punt->def


def test_muff_detected():
    rows = [
        {
            "pos_team": 252,
            "def_pos_team": 9,
            "kickoff_play": False,
            "punt": True,
            "fg_attempt": False,
            "sp": True,
            "scrimmage_play": False,
            "fumble_vec": False,
            "int": False,
            "type.text": "Punt",
            "change_of_poss": True,
            "text": "punt 25 muffed by #24 K.Kirkland recovered by ASU #1 X",
            "homeTeamAbbrev": "BYU",
            "awayTeamAbbrev": "ASU",
            "homeTeamId": 252,
            "awayTeamId": 9,
            "penalty_detail": None,
            "yds_penalty": None,
            "end.pos_team.id": 9,
        },
    ]
    out = _attr(rows)
    assert out["fumble_or_muff"].to_list() == [True]


def _base(**over):
    row = {
        "pos_team": 252,
        "def_pos_team": 9,
        "kickoff_play": False,
        "punt": False,
        "fg_attempt": False,
        "sp": False,
        "scrimmage_play": True,
        "fumble_vec": True,
        "int": False,
        "type.text": "Rush",
        "change_of_poss": False,
        "homeTeamAbbrev": "BYU",
        "awayTeamAbbrev": "ASU",
        "homeTeamId": 252,
        "awayTeamId": 9,
        "penalty_detail": None,
        "yds_penalty": None,
        "end.pos_team.id": 252,
        "text": "",
    }
    row.update(over)
    return row


def test_scrimmage_fumble_lost_to_opponent():
    # BYU (pos) fumbles, ASU recovers -> BYU turnover
    out = _attr([_base(text="#11 QB sacked fumble by #11 QB recovered by ASU #40 X")])
    r = out.to_dicts()[0]
    assert r["is_turnover"] is True
    assert r["turnover_team"] == 252  # BYU lost it
    assert r["recovery_team"] == 9


def test_own_recovery_not_turnover():
    out = _attr([_base(text="#11 QB fumble by #11 QB recovered by BYU #55 Y")])
    r = out.to_dicts()[0]
    assert r["is_turnover"] is False
    assert r["recovery_team"] == 252


def test_punt_return_fumble_lost_st():
    # punt: pos=BYU punting, def=ASU receiving; ASU returner fumbles, BYU recovers
    out = _attr(
        [
            _base(
                pos_team=252,
                def_pos_team=9,
                punt=True,
                sp=True,
                scrimmage_play=False,
                text="punt 40 #2 R return 5 fumbled by #2 R recovered by BYU #98 P",
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["is_turnover"] is True
    assert r["is_st_turnover"] is True
    assert r["turnover_team"] == 9  # ASU (returner) lost it
    assert r["recovery_team"] == 252


def test_overturned_fumble_not_turnover():
    out = _attr(
        [_base(text="#11 QB sacked. CALL OVERTURNED. (Original Play: fumble by #11 QB recovered by ASU #40 X)")],
    )
    r = out.to_dicts()[0]
    assert r["is_turnover"] is False


def test_fumble_recovery_team_is_recoverer():
    # kickoff own recovery: receiving (pos=ASU=9) recovers own; credited to 9 not def
    out = _attr(
        [
            _base(
                pos_team=9,
                def_pos_team=252,
                kickoff_play=True,
                sp=True,
                scrimmage_play=False,
                text="kickoff #2 R return fumbled by #2 R recovered by ASU #2 R",
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["fumble_recovery_team"] == 9


def test_penalized_team_defensive():
    out = _attr(
        [
            _base(
                scrimmage_play=True,
                fumble_vec=False,
                penalty_detail="Defensive Holding",
                yds_penalty="5",
                text="PENALTY ASU Defensive Holding 5 yards",
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["penalized_team"] == 9  # defensive foul -> def_pos_team
    assert r["penalty_yards_signed"] == 5


def test_penalized_team_offensive():
    out = _attr(
        [
            _base(
                scrimmage_play=True,
                fumble_vec=False,
                penalty_detail="False Start",
                yds_penalty="5",
                text="PENALTY BYU False Start 5 yards",
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["penalized_team"] == 252  # offensive foul -> pos_team


def test_penalized_team_null_on_non_penalty_play():
    out = _attr(
        [
            _base(
                scrimmage_play=True,
                fumble_vec=False,
                penalty_detail=None,
                yds_penalty=None,
                text="#22 J.Doe run for 4 yards",
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["penalized_team"] is None


def test_reclassified_punt_return_fumble_is_st_turnover():
    # ESPN reclassifies some punt-return fumbles to a "Fumble Recovery (...)" type and
    # drops the punt/sp flags, flipping pos_team to the recovering (punting) team. The
    # receiving team (def_pos_team) is the one that fumbled the return. Detection must
    # fall back to the text so the turnover is charged to the returning team and flagged ST.
    out = _attr(
        [
            _base(
                pos_team=152,
                def_pos_team=52,
                punt=False,
                sp=False,
                scrimmage_play=True,
                fumble_vec=True,
                homeTeamAbbrev="NCSU",
                awayTeamAbbrev="FSU",
                homeTeamId=152,
                awayTeamId=52,
                text="punt 42 yards to the FSU14 #4 S.White return 2 yards fumbled by #4 S.White recovered by NCSU #4 T.Thomas",
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["is_turnover"] is True
    assert r["turnover_team"] == 52  # FSU (returning/receiving team) lost it
    assert r["is_st_turnover"] is True  # detected as ST via text even though sp=False


def test_nested_double_direction_fumble_charges_both_teams():
    # Sack-strip: offense (100) fumbles, defense (200) recovers and returns, defense
    # fumbles on the return, offense (100) recovers. The ball nets back to the offense,
    # but BOTH teams lost a fumble -> a turnover is charged to each side (per-event,
    # matching the official box). The possession-chain walks both "recovered by" clauses.
    out = _attr(
        [
            _base(
                pos_team=100,
                def_pos_team=200,
                homeTeamAbbrev="OFF",
                awayTeamAbbrev="DEF",
                homeTeamId=100,
                awayTeamId=200,
                scrimmage_play=True,
                fumble_vec=True,
                text=(
                    "#1 QB sacked, fumbled by #1 QB, recovered by DEF #5 Smith, return 10 yards, "
                    "fumbled by #5 Smith, recovered by OFF #7 Jones at OFF40"
                ),
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["recovery_team_2"] == 100  # offense recovered the 2nd (defense's) fumble
    assert r["is_pos_team_turnover"] is True  # offense lost the 1st fumble (to defense)
    assert r["is_def_pos_team_turnover"] is True  # defense lost the 2nd fumble (back to offense)


def _pi_row(text):
    return _base(
        pos_team=100,
        def_pos_team=200,
        homeTeamAbbrev="OFF",
        awayTeamAbbrev="DEF",
        homeTeamId=100,
        awayTeamId=200,
        scrimmage_play=True,
        fumble_vec=False,
        penalty_detail="Pass Interference",  # generic label (would map to defense by heuristic)
        yds_penalty="15",
        text=text,
    )


def test_offensive_pass_interference_charged_to_offense():
    # OPI: penalty_detail is the generic "Pass Interference" (the heuristic would wrongly
    # charge the defense). The authoritative "PENALTY {TEAM}" text token charges the offense.
    out = _attr([_pi_row("#9 QB pass incomplete PENALTY OFF Offensive Pass Interference (#80 X) 15 yards")])
    assert out.to_dicts()[0]["penalized_team"] == 100  # offense (PENALTY OFF)


def test_defensive_pass_interference_charged_to_defense():
    out = _attr([_pi_row("#9 QB pass incomplete PENALTY DEF Defensive Pass Interference (#5 Y) 15 yards")])
    assert out.to_dicts()[0]["penalized_team"] == 200  # defense (PENALTY DEF)


# --- is_blocked_punt_turnover: standalone special-teams flag --------------------
# ESPN's official box counts only giveaways, so blocked punts stay OUT of is_turnover
# (preserving *_pbp == box reconciliation) and are surfaced via this distinct flag --
# the one possession-losing class ESPN's per-play isTurnover catches that the
# giveaway-based derivation does not.
def test_blocked_punt_defense_recovers_is_blocked_punt_turnover():
    out = _attr(
        [
            _base(
                **{
                    "type.text": "Blocked Punt",
                    "change_of_poss": True,  # defense recovered -> possession changed
                    "punt": True,
                    "sp": True,
                    "scrimmage_play": False,
                    "fumble_vec": False,
                    "text": "#5 R punt blocked by #9 X, recovered by ASU #9 X at BYU 20",
                },
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["is_blocked_punt_turnover"] is True
    assert r["is_turnover"] is False  # excluded from giveaway-based turnover (box-reconciling)
    assert r["is_st_turnover"] is False


def test_blocked_punt_touchdown_always_turnover():
    out = _attr(
        [
            _base(
                **{
                    "type.text": "Blocked Punt Touchdown",
                    "change_of_poss": False,  # defensive-score plays can flip pos_team; TD still counts
                    "punt": True,
                    "sp": True,
                    "scrimmage_play": False,
                    "fumble_vec": False,
                    "text": "#9 X 12 Yd Return of Blocked Punt",
                },
            ),
        ],
    )
    assert out.to_dicts()[0]["is_blocked_punt_turnover"] is True


def test_blocked_punt_kicking_team_recovers_not_turnover():
    out = _attr(
        [
            _base(
                **{
                    "type.text": "Blocked Punt",
                    "change_of_poss": False,  # kicking team recovered -> no possession change
                    "punt": True,
                    "sp": True,
                    "scrimmage_play": False,
                    "fumble_vec": False,
                    "text": "#5 R punt blocked, recovered by BYU #5 R",
                },
            ),
        ],
    )
    assert out.to_dicts()[0]["is_blocked_punt_turnover"] is False


def test_normal_play_not_blocked_punt_turnover():
    out = _attr([_base(text="#22 J.Doe run for 4 yards", fumble_vec=False)])
    assert out.to_dicts()[0]["is_blocked_punt_turnover"] is False
    assert out.to_dicts()[0]["is_blocked_fg_turnover"] is False


def test_blocked_fg_touchdown_is_blocked_fg_turnover():
    out = _attr(
        [
            _base(
                **{
                    "type.text": "Blocked Field Goal Touchdown",
                    "change_of_poss": False,  # defensive-score plays can flip pos_team; TD still counts
                    "fg_attempt": True,
                    "sp": True,
                    "scrimmage_play": False,
                    "fumble_vec": False,
                    "text": "#55 X 12 Yd Return of Blocked Field Goal",
                },
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["is_blocked_fg_turnover"] is True
    assert r["is_turnover"] is False  # excluded from giveaway-based turnover (box-reconciling)
    assert r["is_st_turnover"] is False


def test_blocked_fg_defense_recovers_is_blocked_fg_turnover():
    out = _attr(
        [
            _base(
                **{
                    "type.text": "Blocked Field Goal",
                    "change_of_poss": True,  # defense recovered the block
                    "fg_attempt": True,
                    "sp": True,
                    "scrimmage_play": False,
                    "fumble_vec": False,
                    "text": "#9 K 41 yd FG BLOCKED, recovered by DEF #55 X",
                },
            ),
        ],
    )
    assert out.to_dicts()[0]["is_blocked_fg_turnover"] is True


def test_blocked_fg_kicking_team_recovers_not_turnover():
    out = _attr(
        [
            _base(
                **{
                    "type.text": "Blocked Field Goal",
                    "change_of_poss": False,  # kicking team retained
                    "fg_attempt": True,
                    "sp": True,
                    "scrimmage_play": False,
                    "fumble_vec": False,
                    "text": "#9 K 41 yd FG BLOCKED, recovered by OFF #5 R",
                },
            ),
        ],
    )
    assert out.to_dicts()[0]["is_blocked_fg_turnover"] is False


# --- __add_new_play_types: strip-sack interception guard -----------------------
# The strip-sack rule reclassifies "fumble_vec & pass & change_of_poss" plays to
# "Fumble Recovery (Opponent)". An interception sets change_of_poss=1 too, so a pick
# whose returner later fumbles matched the predicate and had its interception erased.
# The int_vec guard prevents that. These tests exercise the private reclassifier on
# synthetic frames carrying every column the method reads.
def _npt_base(**over):
    row = {
        "type.text": "Rush",
        "fumble_vec": False,
        "pass": False,
        "rush": False,
        "change_of_poss": 0,
        "td_play": False,
        "td_check": False,
        "start.down": 1,
        "start.distance": 10,
        "kickoff_play": False,
        "punt_play": False,
        "statYardage": 3,
        "start.yardsToEndzone": 50,
        "text": "",
        "scoringPlay": False,
        "safety": False,
        "kickoff_safety": False,
        "punt_safety": False,
        "penalty_safety": False,
    }
    row.update(over)
    return row


def _npt(rows: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(rows)
    proc = CFBPlayProcess(gameId=1)
    return proc._CFBPlayProcess__add_new_play_types(df)


def test_strip_sack_guard_keeps_interception_return():
    # Pick whose returner fumbles: change_of_poss=1 from the INT itself would have tripped
    # the strip-sack rule. The guard keeps it an interception (normalized to canonical form).
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "Pass Interception Return",
                    "pass": True,
                    "fumble_vec": True,
                    "change_of_poss": 1,
                    "td_play": False,
                    "start.down": 2,
                    "text": "#9 QB pass intercepted by #5 DB return 5 yards fumbled recovered by OFF #7",
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Interception Return"]


def test_strip_sack_guard_keeps_interception_return_touchdown():
    # TD variant: a pick-six is an "Interception Return Touchdown", never a fumble TD.
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "Pass Interception Return Touchdown",
                    "pass": True,
                    "fumble_vec": True,
                    "change_of_poss": 1,
                    "td_play": True,
                    "td_check": True,
                    "scoringPlay": True,
                    "start.down": 2,
                    "text": "#9 QB pass intercepted by #5 DB return 40 yards fumbled recovered by DEF #5",
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Pass Interception Return Touchdown"]


def test_strip_sack_still_reclassifies_genuine_strip_sack():
    # Control: a real strip-sack (ESPN "Sack", not an interception) must still become
    # "Fumble Recovery (Opponent)" -- the int_vec guard must not block legitimate cases.
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "Sack",
                    "pass": True,
                    "fumble_vec": True,
                    "change_of_poss": 1,
                    "td_play": False,
                    "start.down": 2,
                    "text": "#1 QB sacked at OFF30 fumble recovered by DEF #5 X",
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Fumble Recovery (Opponent)"]


def test_blocked_fg_return_td_relabeled_from_extra_point_missed():
    # ESPN mislabels a blocked-FG return TD as "Extra Point Missed"; relabel it.
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "Extra Point Missed",
                    "td_play": True,
                    "text": "Seth Morgan 32 yd FG BLOCKED blocked by Cam Hardy for a TD, "
                    "Antonio Hall return for 74 yds for a TD",
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Blocked Field Goal Touchdown"]


def test_blocked_fg_no_td_relabeled_from_extra_point_missed():
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "Extra Point Missed",
                    "td_play": False,
                    "text": "John Kicker 41 yd FG BLOCKED blocked by #55 X, recovered by DEF at 50",
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Blocked Field Goal"]


def test_blocked_pat_not_relabeled_blocked_fg():
    # Control: a genuine blocked PAT ("blocked" but no FG token) stays "Extra Point Missed".
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "Extra Point Missed",
                    "td_play": False,
                    "text": "PAT attempt by #9 K blocked by #55 X",
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Extra Point Missed"]


def test_missed_pat_not_relabeled():
    # Control: an ordinary missed PAT (no "blocked", no FG) stays "Extra Point Missed".
    out = _npt([_npt_base(**{"type.text": "Extra Point Missed", "text": "#9 K extra point missed wide right"})])
    assert out["type.text"].to_list() == ["Extra Point Missed"]


# --- __add_new_play_types: pre-2014 legacy ESPN label normalization -------------
def test_2pt_conversion_good_normalized():
    # ESPN's pre-2014 successful two-point label "2pt Conversion" -> "Two-Point Conversion Good".
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "2pt Conversion",
                    "scoringPlay": True,
                    "text": "Two-point conversion attempt, QB pass to WR GOOD.",
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Two-Point Conversion Good"]


def test_2pt_conversion_missed_normalized():
    out = _npt(
        [_npt_base(**{"type.text": "2pt Conversion", "scoringPlay": False, "text": "Two-point attempt failed."})],
    )
    assert out["type.text"].to_list() == ["Two-Point Conversion Missed"]


def test_unknown_period_marker_to_end_period():
    out = _npt([_npt_base(**{"type.text": "Unknown", "text": "Start of the 2nd quarter."})])
    assert out["type.text"].to_list() == ["End Period"]


def test_unknown_end_of_game_to_end_period():
    out = _npt([_npt_base(**{"type.text": "Unknown", "text": "End of the game."})])
    assert out["type.text"].to_list() == ["End Period"]


def test_unknown_missed_field_goal_reclassified():
    out = _npt(
        [_npt_base(**{"type.text": "Unknown", "text": "35 yard field goal by Ryan Killeen (USC) is no good."})],
    )
    assert out["type.text"].to_list() == ["Field Goal Missed"]


def test_unknown_missed_extra_point_reclassified():
    out = _npt(
        [_npt_base(**{"type.text": "Unknown", "text": "Extra point by Bryan Borreson (UTAH) is no good."})],
    )
    assert out["type.text"].to_list() == ["Extra Point Missed"]


def test_kickoff_return_defense_normalized_to_kickoff():
    out = _npt(
        [_npt_base(**{"type.text": "Kickoff Return (Defense)", "text": "Onside kick recovered by Spiders."})],
    )
    assert out["type.text"].to_list() == ["Kickoff"]


def test_unrecognized_unknown_left_alone():
    # Control: an "Unknown" with no recognizable text stays "Unknown" (graceful, not guessed).
    out = _npt([_npt_base(**{"type.text": "Unknown", "text": "some unparseable legacy text"})])
    assert out["type.text"].to_list() == ["Unknown"]


def test_extra_point_row_down_sentinel_normalized():
    # Pre-2005 extra-point rows can carry a real down/distance; force the -1 sentinel.
    out = _npt(
        [
            _npt_base(
                **{
                    "type.text": "Extra Point Good",
                    "scoringPlay": True,
                    "start.down": 0,
                    "start.distance": 0,
                    "text": "Extra point by #9 K is good.",
                },
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["start.down"] == -1
    assert r["start.distance"] == -1


def test_normal_play_down_not_touched_by_pat_norm():
    # Control: a regular rush keeps its real down/distance.
    out = _npt([_npt_base(**{"type.text": "Rush", "start.down": 2, "start.distance": 7})])
    r = out.to_dicts()[0]
    assert r["start.down"] == 2
    assert r["start.distance"] == 7


# --- __refine_play_types_post_attribution: is_turnover-keyed label corrections ----
# Runs after attribution, so it can use is_turnover/recovery_team (which the step-5
# reclassifier cannot). Undoes two false relabels and recomputes the two frozen
# derived columns (downs_turnover, pos_score_diff_end) that EPA/WPA read.
def _refine_base(**over):
    row = {
        "type.text": "Rush",
        "orig_play_type": "Rush",
        "is_turnover": False,
        "fumble_vec": False,
        "td_play": False,
        "punt": False,
        "recovery_team": 0,
        "pos_team": 100,
        "statYardage": 5,
        "start.distance": 10,
        "start.down": 1,
        "penalty_1st_conv": False,
        "start.pos_team.id": 100,
        "end.pos_team.id": 100,
        "pos_score_diff": 0,
        "pos_score_pts": 0,
        "scoring_play": False,
        "change_of_pos_team": False,
        "pos_score_diff_start": 0,
    }
    row.update(over)
    return row


def _refine(rows: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(rows)
    proc = CFBPlayProcess(gameId=1)
    return proc._CFBPlayProcess__refine_play_types_post_attribution(df)


def test_refine_sack_self_recovery_to_own():
    # Sack-strip the offense recovers itself: the step-5 rule (spurious change_of_poss)
    # made it "Fumble Recovery (Opponent)"; is_turnover=False -> restore "(Own)".
    out = _refine(
        [
            _refine_base(
                **{
                    "type.text": "Fumble Recovery (Opponent)",
                    "orig_play_type": "Fumble Recovery (Own)",
                    "is_turnover": False,
                    "fumble_vec": True,
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Fumble Recovery (Own)"]


def test_refine_keeps_genuine_opponent_recovery():
    # Control: a real turnover (is_turnover=True) must stay "Fumble Recovery (Opponent)".
    out = _refine(
        [
            _refine_base(
                **{
                    "type.text": "Fumble Recovery (Opponent)",
                    "orig_play_type": "Sack",
                    "is_turnover": True,
                    "fumble_vec": True,
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Fumble Recovery (Opponent)"]


def test_refine_does_not_undo_espn_native_opponent_recovery():
    # Control: if ESPN itself labeled it "Fumble Recovery (Opponent)" (orig == final),
    # do not second-guess it even when is_turnover is False.
    out = _refine(
        [
            _refine_base(
                **{
                    "type.text": "Fumble Recovery (Opponent)",
                    "orig_play_type": "Fumble Recovery (Opponent)",
                    "is_turnover": False,
                    "fumble_vec": True,
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Fumble Recovery (Opponent)"]


def test_refine_punt_team_fumble_recovery():
    # Punt: receiving team fumbles the return, punting team (pos_team=100) recovers ->
    # a real ST turnover currently stuck as "Punt Return" -> "Punt Team Fumble Recovery".
    out = _refine(
        [
            _refine_base(
                **{
                    "type.text": "Punt Return",
                    "orig_play_type": "Punt Return",
                    "punt": True,
                    "is_turnover": True,
                    "recovery_team": 100,  # punting team (pos_team) recovered
                    "pos_team": 100,
                    "td_play": False,
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Punt Team Fumble Recovery"]


def test_refine_keeps_normal_punt_return():
    # Control: an ordinary punt return (no turnover) stays "Punt Return".
    out = _refine(
        [
            _refine_base(
                **{
                    "type.text": "Punt Return",
                    "orig_play_type": "Punt Return",
                    "punt": True,
                    "is_turnover": False,
                    "recovery_team": 0,
                    "pos_team": 100,
                },
            ),
        ],
    )
    assert out["type.text"].to_list() == ["Punt Return"]


def test_refine_recomputes_downs_turnover_and_pos_score_diff_end():
    # A self-recovered fumble on 4th-and-long short of the sticks is a turnover on downs:
    # "Fumble Recovery (Own)" joins normalplay, so downs_turnover must recompute to True
    # and pos_score_diff_end must flip sign (-pos_score_diff).
    out = _refine(
        [
            _refine_base(
                **{
                    "type.text": "Fumble Recovery (Opponent)",
                    "orig_play_type": "Fumble Recovery (Own)",
                    "is_turnover": False,
                    "fumble_vec": True,
                    "start.down": 4,
                    "statYardage": 2,
                    "start.distance": 10,
                    "pos_score_diff": 3,
                },
            ),
        ],
    )
    r = out.to_dicts()[0]
    assert r["type.text"] == "Fumble Recovery (Own)"
    assert r["downs_turnover"] is True
    assert r["pos_score_diff_end"] == -3


# --- penalized-team text resolution (binary home/away matcher) ---


def _pen_side(text, home=("TEX", "Texas", None, "Longhorns"), away=("TCU", "TCU", None, "Horned Frogs")):
    from sportsdataverse.cfb.cfb_pbp import _parse_penalty_team_side

    return _parse_penalty_team_side(
        {
            "text": text,
            "homeTeamAbbrev": home[0],
            "homeTeamName": home[1],
            "homeTeamNameAlt": home[2],
            "homeTeamMascot": home[3],
            "awayTeamAbbrev": away[0],
            "awayTeamName": away[1],
            "awayTeamNameAlt": away[2],
            "awayTeamMascot": away[3],
        },
    )


def test_penalty_side_uppercase_form_abbrev():
    assert _pen_side("pass incomplete, PENALTY TCU pass interference (Washington, A.) 10 yards") == "away"


def test_penalty_side_university_initialism_alias():
    # "UT" appears in neither payload string; the U+initial alias resolves it,
    # and TCU (an all-caps initialism name) must NOT also generate "UT".
    assert _pen_side("pass complete for 34 yards, PENALTY UT pass interference (K. Boyd)") == "home"


def test_penalty_side_leading_form():
    assert _pen_side("TCU Penalty, sideline interference (15 Yards) to the TCU 15 for a 1ST down") == "away"


def test_penalty_side_leading_form_two_words():
    side = _pen_side(
        "ARIZONA ST Penalty, Face Mask (4 yards) (Henry Hattis) to the ArzSt 5",
        home=("ASU", "Arizona State", None, "Sun Devils"),
        away=("BYU", "BYU", None, "Cougars"),
    )
    assert side == "home"


def test_penalty_side_vowel_dropped_subsequence():
    side = _pen_side(
        "WESTRN MICHIGAN Penalty, Defensive Holding (Drake Spears) to the WMich 8 for a 1ST down",
        home=("WMU", "Western Michigan", None, "Broncos"),
        away=("SYR", "Syracuse", None, "Orange"),
    )
    assert side == "home"


def test_penalty_side_longest_prefix_shrinks_to_team():
    # the capture grabs "BAYLOR Pass Interference"; word-prefix shrinking must
    # still land on BAYLOR
    side = _pen_side(
        "pass incomplete, PENALTY BAYLOR Pass Interference (R.J. Sneed) 15 yards",
        home=("BAY", "Baylor", None, "Bears"),
        away=("TCU", "TCU", None, "Horned Frogs"),
    )
    assert side == "home"


def test_penalty_side_none_when_no_token():
    assert _pen_side("Jalen Milroe run for 8 yds to the LSU 22") is None


def test_penalty_side_suffix_direction_leading_form():
    # the team is the LAST word run before "Penalty," -- junk prefix words
    # ("for a TD") must not defeat the match
    side = _pen_side(
        "Sedrick Alexander run for 2 yds for a TD Vanderbilt Penalty, (Yards) declined",
        home=("VAN", "Vanderbilt", None, "Commodores"),
        away=("AUB", "Auburn", None, "Tigers"),
    )
    assert side == "home"


def test_penalty_side_parenthesized_team_name():
    side = _pen_side(
        "Kevin Davis run for 3 yds Miami (OH) Penalty, Holding (10 Yards) to the M-OH 25",
        home=("M-OH", "Miami (OH)", None, "RedHawks"),
        away=("BGSU", "Bowling Green", None, "Falcons"),
    )
    assert side == "home"


def test_penalty_side_u_prefix_stripped():
    side = _pen_side(
        "PENALTY UMass Pass Interference (Bailey,Brennen) 15 yards from UMA35 to UMA20",
        home=("MASS", "Massachusetts", None, "Minutemen"),
        away=("URI", "Rhode Island", None, "Rams"),
    )
    assert side == "home"


def test_penalty_side_initial_plus_u_alias():
    side = _pen_side(
        "PENALTY VU Ineligible Downfield on Pass 5 yards from VU40 to VU35",
        home=("VAN", "Vanderbilt", None, "Commodores"),
        away=("BAMA", "Alabama", None, "Crimson Tide"),
    )
    assert side == "home"


def test_penalty_side_three_letter_consonant_skeleton():
    side = _pen_side(
        "PENALTY TLN Pass Interference (White,Javion) 15 yards from TLN30 to TLN15",
        home=("TULN", "Tulane", None, "Green Wave"),
        away=("MEM", "Memphis", None, "Tigers"),
    )
    assert side == "home"
