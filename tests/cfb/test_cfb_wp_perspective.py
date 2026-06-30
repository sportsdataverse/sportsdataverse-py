"""Game-ending win-probability perspective.

A play that flips possession on the FINAL snap must report home/away win-prob that
matches the actual winner. The bug: a game ending on a SAFETY (a possession flip
whose type is absent from end_change_vec) left pos_score_diff_end in the pos_team
perspective while home/away maps off end.pos_team.id, so the LOSING team got 1.0.
Fixed by deriving the game-ender wp_after from the end-possession team's absolute
final score (see _apply_wp_derivation), which is perspective-consistent and touches
no model feature.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.cfb.cfb_pbp as cfb_pbp_mod
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _run(monkeypatch, gid: int) -> pl.DataFrame:
    summary = json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr(cfb_pbp_mod, "download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    return pl.from_dicts(proc.plays_json, infer_schema_length=None)


def _final_play(df: pl.DataFrame) -> dict:
    return df.filter(pl.col("game_play_number") == df["game_play_number"].max()).row(0, named=True)


def test_safety_game_ender_wp_matches_actual_winner(monkeypatch):
    """401236002 ends on a safety that flips possession; home lost 31-33, so the
    home team's post-play win prob must be 0.0 (it was wrongly 1.0)."""
    r = _final_play(_run(monkeypatch, 401236002))
    assert r["type.text"] == "Safety"
    assert r["end.homeScore"] < r["end.awayScore"], "home lost this fixture"
    assert r["home_wp_after"] == 0.0
    assert r["away_wp_after"] == 1.0
    assert r["wpa"] is not None


@pytest.mark.parametrize(
    ("gid", "expected_home_wp_after"),
    [
        (401236002, 0.0),  # SAFETY game-ender, home lost 31-33 (the fixed bug)
        (401112081, 0.0),  # INT-return game-ender, home lost 23-29 (flip path, unchanged)
        (401754579, 1.0),  # INT-return game-ender, home won 48-36 (flip path, unchanged)
        (401628334, 0.0),  # no-possession-flip game-ender, home lost 20-27 (control)
        (401628455, 1.0),  # no-possession-flip game-ender, home won 52-6 (control)
    ],
)
def test_game_ender_home_wp_after_matches_winner(monkeypatch, gid, expected_home_wp_after):
    """On the final play of a completed game, home_wp_after is 1.0 iff home actually won."""
    r = _final_play(_run(monkeypatch, gid))
    assert r["end.homeScore"] != r["end.awayScore"], "fixture has a decided winner"
    assert r["home_wp_after"] == expected_home_wp_after
