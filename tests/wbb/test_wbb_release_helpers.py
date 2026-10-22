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


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # ?logical (R 4.x): c("T", "TRUE", "true", "True") are true, c("F", "FALSE", "false", "False")
        # false, every other string NA; numbers: 0 is FALSE, NaN NA, anything else TRUE.
        ("false", False),  # B-GB-20: bool("false") was True
        ("FALSE", False),
        ("F", False),
        ("False", False),
        ("true", True),
        ("T", True),
        ("yes", None),
        ("", None),
        (0, False),
        (2, True),
        (float("nan"), None),
        (True, True),
        (False, False),
        (None, None),
    ],
)
def test_rel_bool_follows_r_as_logical(value, expected):
    from sportsdataverse.wbb.wbb_game_rosters import _rel_bool

    assert _rel_bool(value) is expected


# --- helper_wbb_officials on ESPN's real Core v2 officials page (tests/fixtures/espn/event_officials_nba.json)


def _officials_page():
    import json
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "fixtures" / "espn" / "event_officials_nba.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _officials(payload):
    from sportsdataverse.wbb.wbb_game_officials import helper_wbb_officials

    return helper_wbb_officials(payload, season=2024, game_id=401584793)


def test_officials_null_element_is_skipped():
    page = _officials_page()
    df = _officials({**page, "items": [page["items"][0], None, *page["items"][1:]]})  # B-L8: AttributeError
    assert df["official_id"].to_list() == [int(o["id"]) for o in page["items"]]


def test_officials_empty_page_carries_the_columns():
    import polars as pl

    from sportsdataverse.wbb.wbb_game_officials import _OFFICIAL_COLS

    full = _officials(_officials_page())
    empty = _officials({**_officials_page(), "items": []})  # B-L8: was a zero-column frame
    assert empty.height == 0 and empty.schema == full.schema
    assert empty.columns == list(_OFFICIAL_COLS) and full.schema["official_id"] == pl.Int32


def test_officials_without_ids_are_not_collapsed():
    page = _officials_page()
    idless = {"position": page["items"][0]["position"]}  # every mapped field equal, no id
    df = _officials({**page, "items": [*page["items"], dict(idless), dict(idless)]})  # B-GB-18: kept one
    assert df.height == len(page["items"]) + 2
    # the same official listed twice is still one row (R distinct())
    assert _officials({**page, "items": [*page["items"], page["items"][0]]}).height == len(page["items"])
