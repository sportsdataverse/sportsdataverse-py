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
