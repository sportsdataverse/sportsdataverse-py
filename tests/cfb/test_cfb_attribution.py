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
