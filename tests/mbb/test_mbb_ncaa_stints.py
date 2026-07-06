"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_stints` (Task 5b.2).

Every case below is a 1:1 transliteration of an inline ``utest`` literal from
``ExtractorUtilsTests.scala`` in the read-only cbb-explorer clone:

* ``build_player_code`` block (``ExtractorUtilsTests.scala:15-117``) -- 10
  name -> code assertions (utest ``TestUtils.inside`` destructuring = exact
  equality on ``(code, PlayerId(name), ncaa_id=None)``).
* ``parse_team_name`` block (``ExtractorUtilsTests.scala:118-156``) -- 6
  ``Right((team, opp, is_first))`` assertions.

Upstream-test-bug note (``:86-115``): the Scala loops ``team`` over
``[None, Some(TeamId("TCU"))]`` but every call passes ``None``, and no TCU
entry exists in the misspellings table -- the ``if team.nonEmpty`` guard
cases are dead. The faithful transliteration therefore expects the
UNcorrected code/name on both iterations.
"""

from __future__ import annotations

import pytest

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId, TeamId, Year
from sportsdataverse.mbb.mbb_ncaa_stints import (
    build_player_code,
    parse_team_name,
    remove_diacritics,
)

# --- build_player_code (jest.. utest :15-117) -------------------------------

BUILD_PLAYER_CODE_CASES: list[tuple[str, str, str]] = [
    # (input name, expected code, expected PlayerId name)
    (
        "Surname, F.irstname A B Iiii Iiiiaiii Jr Jr. Sr Sr. 4test the First second rAbbit Third",
        "FiRaSurname",
        "Surname, F.irstname A B Iiii Iiiiaiii Jr Jr. Sr Sr. 4test the First second rAbbit Third",
    ),
    ("Mitchell, Makhi", "MiMitchell", "Mitchell, Makhi"),  # duplicate-name -> first_last
    ("MAYER,M", "MMayer", "MAYER,M"),  # first names are never filtered
    ("Brown, Jr., Barry", "BaBrown", "Brown, Jr., Barry"),  # 3-part comma+suffix
    ("Dorka Juhász", "DoJuhasz", "Dorka Juhasz"),  # diacritics stripped in PlayerId
    ("MAMUKELASHVIL,SANDRO", "SaMamukelash", "MAMUKELASHVIL,SANDRO"),  # fragment cap
    ("BIGBY-WILLIAM,KAVELL", "KaBigby-will", "BIGBY-WILLIAM,KAVELL"),
    ("Kavell Bigby-Williams", "KaBigby-will", "Kavell Bigby-Williams"),
]


@pytest.mark.parametrize(("in_name", "code", "player_id"), BUILD_PLAYER_CODE_CASES)
def test_build_player_code_cases(in_name: str, code: str, player_id: str) -> None:
    """utest ``:15-84`` -- exact ``PlayerCodeId(code, PlayerId(name), None)``."""
    assert build_player_code(in_name, None) == PlayerCodeId(code=code, id=PlayerId(player_id))


@pytest.mark.parametrize("team", [None, TeamId("TCU")])
def test_build_player_code_misspelling_loop(team: TeamId | None) -> None:
    """utest ``:86-115`` -- the misspelling-fixer loop. The Scala always calls
    with ``team=None`` (its ``team`` loop var is unused in the call -- a latent
    upstream test bug) and no TCU misspelling entry exists, so the uncorrected
    branch is the one that fires on both iterations.
    """
    assert build_player_code("Dylan Ostekowski", None) == PlayerCodeId(
        code="DyOstekowski", id=PlayerId("Dylan Ostekowski")
    )
    assert build_player_code("Ostekowski, Dylan", None) == PlayerCodeId(
        code="DyOstekowski", id=PlayerId("Ostekowski, Dylan")
    )


def test_build_player_code_ncaa_id_is_none() -> None:
    """The Scala oracle destructures ``ncaa_id`` as ``None`` on every case."""
    assert build_player_code("Mitchell, Makhi", None).ncaa_id is None


def test_remove_diacritics() -> None:
    """Supplement (no standalone Scala oracle -- exercised via the Juhász
    case above): NFD + combining-mark strip."""
    assert remove_diacritics("Dorka Juhász") == "Dorka Juhasz"
    assert remove_diacritics("plain") == "plain"


# --- parse_team_name (utest :118-156) ---------------------------------------


def test_parse_team_name_plain_first() -> None:
    """utest ``:119-122``."""
    assert parse_team_name(["TeamA", "TeamB"], TeamId("TeamA"), Year(2018)) == (
        "TeamA",
        "TeamB",
        True,
    )


def test_parse_team_name_plain_second() -> None:
    """utest ``:123-126``."""
    assert parse_team_name(["TeamB", "TeamA"], TeamId("TeamA"), Year(2018)) == (
        "TeamA",
        "TeamB",
        False,
    )


def test_parse_team_name_seed_stripped() -> None:
    """utest ``:127-134``."""
    assert parse_team_name(["#1 TeamA", "#3 TeamB"], TeamId("TeamA"), Year(2018)) == (
        "TeamA",
        "TeamB",
        True,
    )


def test_parse_team_name_seed_and_record_stripped() -> None:
    """utest ``:135-142``."""
    assert parse_team_name(["#1 TeamA (1-1)", "TeamB (4-1)"], TeamId("TeamA"), Year(2018)) == ("TeamA", "TeamB", True)


def test_parse_team_name_alias_not_applied_other_year() -> None:
    """utest ``:143-147`` -- the NIU alias is scoped to Year(2021) only."""
    assert parse_team_name(["NIU", "TeamB"], TeamId("NIU"), Year(2018)) == (
        "NIU",
        "TeamB",
        True,
    )


def test_parse_team_name_alias_applied_2021() -> None:
    """utest ``:148-155`` -- NIU -> Northern Ill. for Year(2021), both slots."""
    assert parse_team_name(["NIU", "NIU"], TeamId("Northern Ill."), Year(2021)) == (
        "Northern Ill.",
        "Northern Ill.",
        True,
    )


def test_parse_team_name_failure_returns_parse_error() -> None:
    """Supplement (the Scala Left branch has no inline oracle case): a
    non-matching target yields a ``ParseError`` with the ``[team]`` id."""
    result = parse_team_name(["TeamB", "TeamC"], TeamId("TeamA"), Year(2018))
    assert isinstance(result, ParseError)
    assert result.id == "[team]"
