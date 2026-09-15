"""Player box-score tables come out in one total order, run after run.

``group_by`` emits groups in arbitrary order, so tables sorted on volume alone (CFB)
or not at all (NFL) listed tied players in a different order on every run. Two
renders of the same final game on gameonpaper.com differed only by that shuffle.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

import polars as pl

from sportsdataverse.football.box import _ordered_rows


def _players():
    return pl.DataFrame(
        {
            "pos_team": [20, 10, 10, 20, 10, 10],
            "receiver_player_name": ["Zed", "Bell", None, "Amos", "Adams", "Cole"],
            "Tar": [1, 1, 1, 1, 5, 1],
        }
    )


def test_order_is_volume_then_team_then_name_nulls_last():
    out = _ordered_rows(_players(), "pos_team", "receiver_player_name", "Tar")
    assert out.select("pos_team", "receiver_player_name").rows() == [
        (10, "Adams"),  # 5 targets first
        (10, "Bell"),
        (10, "Cole"),
        (10, None),  # unnamed row last within its team
        (20, "Amos"),
        (20, "Zed"),
    ]


def test_any_input_order_gives_the_same_rows():
    expected = _ordered_rows(_players(), "pos_team", "receiver_player_name", "Tar").rows()
    rng = random.Random(7)
    for _ in range(20):
        idx = list(range(_players().height))
        rng.shuffle(idx)
        shuffled = _players()[idx]
        assert _ordered_rows(shuffled, "pos_team", "receiver_player_name", "Tar").rows() == expected


def test_without_volume_orders_by_team_then_name():
    out = _ordered_rows(_players().drop("Tar"), "pos_team", "receiver_player_name")
    assert out["receiver_player_name"].to_list() == ["Adams", "Bell", "Cole", None, "Amos", "Zed"]


def _box(monkeypatch, gid: int) -> dict:
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    summary = json.loads(
        (Path(__file__).resolve().parents[1] / "cfb" / "fixtures" / f"summary_{gid}.json").read_text(encoding="utf-8")
    )

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    return proc.run_processing_pipeline()["advBoxScore"]


def _is_ordered(rows, team, name, volume=None):
    def key(r):
        return (-(r.get(volume) or 0) if volume else 0, r[team], r[name] is None, r[name] or "")

    return rows == sorted(rows, key=key)


def test_processed_cfb_game_player_tables_are_ordered(monkeypatch):
    box = _box(monkeypatch, 401754598)
    assert _is_ordered(box["pass"], "pos_team", "passer_player_name", "Att")
    assert _is_ordered(box["rush"], "pos_team", "rusher_player_name", "Car")
    assert _is_ordered(box["receiver"], "pos_team", "receiver_player_name", "Tar")
    assert _is_ordered(box["defensive_players"], "def_pos_team", "player_name")
    assert _is_ordered(box["specialists"], "pos_team", "player_name")
