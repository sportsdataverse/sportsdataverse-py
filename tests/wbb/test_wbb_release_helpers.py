"""The release producers' R-semantics helpers.

These back the wehoop-wbb-data / wehoop-wnba-data parity ports, where being
MORE precise than R means being wrong: the R scripts round-trip payload floats
through ``as.numeric(as.character(v))``, so the published assets carry whatever
R's text form lost.
"""

import pytest

from sportsdataverse.wbb.wbb_game_rosters import _r_as_character, _rel_chr


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # Every expectation below is the literal output of R 4.5.3's
        # as.character(<double>) -- captured, not derived.
        (3.4000000000000057, "3.40000000000001"),  # 15 sig digits, not the exact repr
        (-10.200000000000003, "-10.2"),  # ...and the 15-digit form can be shorter
        (100000.0, "1e+05"),  # R (scipen=0) takes the SHORTER form
        (150000.0, "150000"),  # ...but fixed is shorter here, so fixed wins
        (1e20, "1e+20"),
        (1e-5, "1e-05"),
        (2.5, "2.5"),
        (0.0, "0"),
    ],
)
def test_r_as_character_matches_r(value, expected):
    assert _r_as_character(value) == expected


def test_r_as_character_round_trip_loses_what_r_loses():
    # The whole point: parsing our text back must land on the SAME double R
    # landed on -- which is not the double we started from.
    x = 3.4000000000000057
    assert float(_r_as_character(x)) != x
    assert float(_r_as_character(x)) == 3.40000000000001


def test_rel_chr_null_and_list_semantics():
    assert _rel_chr(None) is None
    assert _rel_chr([]) is None
    assert _rel_chr([None]) is None
    assert _rel_chr(["a", "b"]) == "a"  # R safe_chr takes the first element
    assert _rel_chr([100000.0]) == "1e+05"  # ...and still applies R's float form
    assert _rel_chr(7) == "7"  # ints are untouched (no float formatting)
    assert _rel_chr(True) == "TRUE"  # R: as.character(TRUE), not Python's "True"
    assert _rel_chr(False) == "FALSE"


def test_draft_safe_chr_is_the_same_emulation():
    # The draft port used to carry its own copy that mishandled non-integer
    # floats. It must now BE the shared one, not merely resemble it.
    from sportsdataverse.wnba.wnba_draft import _safe_chr

    assert _safe_chr(True) == "TRUE"  # the draft's pick_traded column
    assert _safe_chr(2.0) == "2"  # R: as.character(2) == "2", not "2.0"
    assert _safe_chr(3.4000000000000057) == "3.40000000000001"  # the fix it was missing
