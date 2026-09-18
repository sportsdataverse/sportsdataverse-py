"""Pure-unit regression tests for ``_yardline_100`` gamebook-abbr handling.

These are intentionally NOT in ``test_parse.py``: that module's real-game class is
gated on the bundled raw-game fixture, whereas these are pure-function tests that
guard the field-position flip on their own.
"""

from __future__ import annotations

from sportsdataverse.nfl.shield_pbp.parse import _yardline_100


def test_yardline_100_gamebook_abbr_fixups():
    # The feed's ``yardLine`` string uses NFL *gamebook* club abbreviations, which
    # differ from nflverse's ``posteam`` abbreviation for four clubs. Without
    # normalizing the parsed side, ``side == posteam`` is False and the own-side
    # flip (``100 - yd``) is skipped, mis-locating the club to the opponent's red
    # zone (corrupting yardline_100 -> ep/epa/wp). Affected: LAR->LA (Rams 2016+),
    # JAC->JAX (Jaguars 1999-2019), BLT->BAL / CLV->CLE (2006).
    assert _yardline_100("LAR 1", "LA") == 99  # Rams own 1 -> 99 to score
    assert _yardline_100("LAR 32", "LA") == 68  # Rams own 32 -> 68
    assert _yardline_100("JAC 20", "JAX") == 80  # Jaguars own 20 -> 80
    assert _yardline_100("BLT 5", "BAL") == 95  # Ravens own 5 -> 95
    assert _yardline_100("CLV 5", "CLE") == 95  # Browns own 5 -> 95


def test_yardline_100_gamebook_abbr_opponent_side_unchanged():
    # When the gamebook side is the OPPONENT, the raw number is the distance to
    # score and must NOT be flipped — the fixup must not over-apply.
    assert _yardline_100("LAR 32", "SF") == 32
    assert _yardline_100("JAC 20", "HOU") == 20
    assert _yardline_100("BLT 20", "TEN") == 20
    assert _yardline_100("CLV 20", "PIT") == 20


def test_yardline_100_unaffected_clubs_still_correct():
    # Regression guard for the existing behavior on clubs whose gamebook abbr
    # already matches nflverse (no fixup): the flip must be unchanged.
    assert _yardline_100("50", "KC") == 50
    assert _yardline_100("BAL 32", "BAL") == 68
    assert _yardline_100("BAL 32", "KC") == 32
    assert _yardline_100(None, "KC") is None
