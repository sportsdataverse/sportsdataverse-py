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
