"""PWHL views whose SiteKit value WRAPS the records, run on real captures.

``modulekit`` answers ``transactions``, ``brackets`` and ``player`` (category
``gamebygame``) with a dict around the record list -- ``{"transactions": [...]}``,
``{"rounds": [{"matchups": [...]}]}``, ``{"games": [...]}`` -- not a bare list. The
generic flat parser iterated that dict's KEYS and raised ``TypeError`` on every
real payload; the old tests only fed it ``None``.
"""

from __future__ import annotations

import pytest

from sportsdataverse.pwhl import pwhl_api
from tests.conftest import load_fixture


def _serve(monkeypatch, stem: str) -> None:
    monkeypatch.setattr(pwhl_api, "hockeytech_api", lambda *a, **k: load_fixture("hockeytech", stem))


def test_transactions_one_row_per_transaction(monkeypatch):
    _serve(monkeypatch, "pwhl_transactions")
    df = pwhl_api.pwhl_transactions()
    assert df.height == 20  # the page the capture holds (num_results=171 is the league total)
    for col in ("transaction_date", "player_id", "player_name", "team_id", "team_name", "ttype_text"):
        assert col in df.columns


def test_playoff_bracket_one_row_per_series(monkeypatch):
    _serve(monkeypatch, "pwhl_brackets_9")
    df = pwhl_api.pwhl_playoff_bracket(season_id=9)
    assert df.height == 3  # 2 semifinals + the final
    for col in ("round", "round_name", "series_letter", "team1", "team2", "team1_wins", "team2_wins", "winner"):
        assert col in df.columns


def test_player_game_log_one_row_per_game(monkeypatch):
    _serve(monkeypatch, "pwhl_player_gamebygame_27_5")
    df = pwhl_api.pwhl_player_game_log(27)
    assert df.height == 5
    for col in ("id", "date_played", "home_team_code", "visiting_team_code", "goals", "assists", "points"):
        assert col in df.columns


@pytest.mark.parametrize(
    ("fn", "stem", "args"),
    [
        ("pwhl_player_game_log", "pwhl_player_gamebygame", (12,)),  # real reply with "games": []
    ],
)
def test_a_real_empty_record_list_is_a_zero_row_frame(monkeypatch, fn, stem, args):
    _serve(monkeypatch, stem)
    assert getattr(pwhl_api, fn)(*args).height == 0


def test_a_column_the_feed_mixes_int_and_str_in_becomes_strings():
    """The gamebygame view ships ``plus_minus`` as 0 in one row and "1" in the next; pyarrow
    refused the object column outright (ArrowInvalid)."""
    from sportsdataverse.hockeytech import _parsers as P

    df = P._to_frame([{"plus_minus": 0, "goals": "1"}, {"plus_minus": "1", "goals": "0"}], False)
    assert df["plus_minus"].to_list() == ["0", "1"]
    assert df["goals"].to_list() == ["1", "0"]
