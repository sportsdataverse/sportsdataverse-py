"""Interceptor-credit routing in ``__join_participants`` (offline).

ESPN does not ship an ``interceptor`` participant type -- on an interception
play it files the defender who caught the ball as ``pass_defender``. The
pre-fix mapping sent that name to ``pass_breakup_player_name`` on every play,
which both (a) left ``interception_player_name`` null whenever the play text
did not spell the interceptor out, and (b) credited a pass breakup on plays
that were actually picks.

Empirical basis for the routing (22 published seasons): on the 313
Interception-Return-Touchdown rows carrying BOTH a text-extracted interceptor
and a ``pass_defender`` participant, the two agree on last name 93.9% of the
time; the residue is suffix/nickname variance of the same athlete
("Desmond King" vs "Desmond King II"), not a different player.

These tests drive the private ``__join_participants`` with caller-supplied
participants so no network fetch happens.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess


def _join(plays: list[dict], participants: list[dict]) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=1)
    proc.participants = pl.DataFrame(participants)
    return proc._CFBPlayProcess__join_participants(pl.DataFrame(plays))


def _play(play_id: int, *, is_int: bool, interceptor=None, pbu=None) -> dict:
    return {
        "id": play_id,
        "int": is_int,
        "interception_player_name": interceptor,
        "pass_breakup_player_name": pbu,
    }


def test_pass_defender_becomes_interceptor_on_int_play():
    """The headline fix: a pick whose text never named the defender still gets
    interceptor credit from the participants payload."""
    out = _join(
        [_play(101, is_int=True)],
        [{"play_id": 101, "pass_defender_player_name": "Xavier Watts"}],
    )
    assert out["interception_player_name"][0] == "Xavier Watts"


def test_pass_defender_does_not_credit_a_breakup_on_int_play():
    """A pick is not a pass breakup -- PBU must stay null on interception plays."""
    out = _join(
        [_play(101, is_int=True)],
        [{"play_id": 101, "pass_defender_player_name": "Xavier Watts"}],
    )
    assert out["pass_breakup_player_name"][0] is None


def test_pass_defender_still_credits_breakup_on_non_int_play():
    """Non-interception plays keep the original PBU attribution, and must not
    acquire a spurious interceptor."""
    out = _join(
        [_play(202, is_int=False)],
        [{"play_id": 202, "pass_defender_player_name": "Kyle Rogers"}],
    )
    assert out["pass_breakup_player_name"][0] == "Kyle Rogers"
    assert out["interception_player_name"][0] is None


def test_text_extracted_interceptor_survives_when_participant_absent():
    """Participants are authoritative when present, but a null participant must
    never wipe a name the play-text regex already recovered."""
    out = _join(
        [_play(303, is_int=True, interceptor="Lofa Tatupu")],
        [{"play_id": 303, "pass_defender_player_name": None}],
    )
    assert out["interception_player_name"][0] == "Lofa Tatupu"


def test_mixed_frame_routes_each_row_independently():
    out = _join(
        [
            _play(1, is_int=True),
            _play(2, is_int=False),
            _play(3, is_int=True, interceptor="Hosea Wheeler"),
        ],
        [
            {"play_id": 1, "pass_defender_player_name": "A Defender"},
            {"play_id": 2, "pass_defender_player_name": "B Defender"},
            {"play_id": 3, "pass_defender_player_name": "Hosea Wheeler"},
        ],
    )
    assert out.sort("id")["interception_player_name"].to_list() == ["A Defender", None, "Hosea Wheeler"]
    assert out.sort("id")["pass_breakup_player_name"].to_list() == [None, "B Defender", None]
