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
