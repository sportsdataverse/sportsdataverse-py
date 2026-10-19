"""Player box-score tables come out in one total order, run after run.

``group_by`` emits groups in arbitrary order, so tables sorted on volume alone (CFB)
or not at all (NFL) listed tied players in a different order on every run. Two
renders of the same final game on gameonpaper.com differed only by that shuffle.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.box import _ordered_rows
from sportsdataverse.football.usage_box import SECTIONS

# Processing a default-odds capture warns DefaultOddsWarning incidentally;
# tests/cfb/test_cfb_pbp_offline.py::test_default_odds_warn asserts it.
pytestmark = pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")


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


#: committed captures whose team / situational / defensive / drives / usage / special-teams tables
#: came out in a different order in two processes before R828-4: 9 sections each, so a pre-fix run
#: matches by chance only when every one of ~36 two-way coin flips lands the same way (provenance:
#: tests/cfb/fixtures/README.md, tests/nfl/fixtures/README.md)
_TWO_PROCESS_CAPTURES = {"cfb": (243040130, 401112081, 401754598), "nfl": (241010028,)}

_DUMP = r"""
import gzip, json, sys, warnings
from pathlib import Path
warnings.simplefilter("ignore")
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess
out = {}
for league, gid in json.loads(sys.argv[1]):
    cls, meth = (CFBPlayProcess, "espn_cfb_pbp") if league == "cfb" else (NFLPlayProcess, "espn_nfl_pbp")
    proc = cls(gameId=gid, join_participants=False)
    (path,) = (Path("tests") / league / "fixtures").glob(f"summary_{gid}*.json*")
    text = gzip.decompress(path.read_bytes()).decode("utf-8") if path.suffix == ".gz" else path.read_text("utf-8")
    getattr(proc, meth)(summary=json.loads(text))
    out[f"{league}_{gid}"] = proc.run_processing_pipeline()["advBoxScore"]
print(json.dumps(out, default=str))
"""


def _boxes_in_a_fresh_process(hash_seed: str, threads: str) -> dict:
    games = [(lg, gid) for lg, gids in _TWO_PROCESS_CAPTURES.items() for gid in gids]
    env = {**os.environ, "PYTHONHASHSEED": hash_seed, "POLARS_MAX_THREADS": threads, "PYTHONIOENCODING": "utf-8"}
    root = Path(__file__).resolve().parents[2]
    done = subprocess.run(
        [sys.executable, "-c", _DUMP, json.dumps(games)],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=600,
        check=True,
    )
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_advboxscore_is_identical_in_two_processes():
    """R828-4: every section serialises to the same list in two processes (hash seed, thread count)."""
    first = _boxes_in_a_fresh_process("1", "1")
    second = _boxes_in_a_fresh_process("2", "8")
    assert first.keys() == second.keys()
    for game, box in first.items():
        # NFL: only the shared usage sections; nfl_pbp's own team tables are a separate follow-up
        sections = box.keys() | second[game].keys() if game.startswith("cfb") else set(SECTIONS)
        differ = sorted(s for s in sections if box.get(s) != second[game].get(s))
        assert not differ, f"{game}: sections in a different order across processes: {differ}"
