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
    final = df.filter(pl.col("game_play_number") == df["game_play_number"].max())
    assert final.height == 1, "expected exactly one final play per game"
    return final.row(0, named=True)


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


def test_rekick_penalty_does_not_flip_wp_perspective(monkeypatch):
    """B6: wp_after is derived from lead_wp_before, which is stated in the NEXT
    play's possession perspective. Flipping it (1 - lead_wp_before) is a valid
    conversion only when the next play really is the other team's possession.

    A penalty on a kickoff that forces a re-kick breaks that: the re-kick row
    carries the SAME possessing team this row started with, so lead_wp_before is
    already in this row's perspective and the flip returned its complement. In
    401644749 an unsportsmanlike-conduct flag on Western Michigan published
    wp 0.020 -> 0.982 (WPA +0.962) against an EPA of -0.26.
    """
    df = _run(monkeypatch, 401644749)
    row = df.filter(
        pl.col("text").str.contains("(?i)kickoff")
        & pl.col("text").str.contains("(?i)Western Michigan Penalty, unsportsmanlike conduct \\(Jaden Nixon\\)")
    )
    assert row.height == 1, "expected exactly one WMU kickoff-penalty row in this fixture"
    r = row.row(0, named=True)

    # A re-kick follows, so B3 part (d.i) makes this a no-play at the touchback
    # yardline; the EPA is incidental here, what is under test is the perspective.
    assert abs(r["EPA"]) < 1.0, "a nullified kickoff must not carry a large EPA"
    # The flip signature: wp_after must NOT be the complement of wp_before.
    assert abs(r["wp_after"] - (1 - r["wp_before"])) > 0.5, "wp_after is still the 1-p flip"
    assert abs(r["wpa"]) < 0.1, f"near-zero EPA must not carry a huge WPA (got {r['wpa']})"


def test_kickoff_penalty_enforcement_spot_is_not_field_position(monkeypatch):
    """B3 part (d): a kickoff row carrying a penalty but describing no kick and no
    return did not move the ball. ESPN writes the penalty's ENFORCEMENT spot --
    in the KICKING team's own territory -- into end.yardsToEndzone while pos_team
    is the receiving team, so the EP model scored first-and-goal field position.

    401636889 p52 ("Jack Stone kickoff Baylor Penalty, Unsportsmanlike Conduct ...
    to the BAY 20") carried end.yardsToEndzone=20 and published EPA +4.38.
    """
    df = _run(monkeypatch, 401636889)
    row = df.filter(
        pl.col("text").str.contains("(?i)kickoff") & pl.col("text").str.contains("(?i)Baylor Penalty, Unsportsmanlike")
    )
    assert row.height == 1
    r = row.row(0, named=True)
    # A re-kick follows (p53 is a Timeout, p54 the re-kick), so this is a no-play and
    # the end state becomes the touchback yardline -- not start.yardsToEndzone, which
    # is better field position than a touchback and would leave a spurious ~+1.2 EPA.
    assert r["end.yardsToEndzone"] == 75, "post-2013 no-play kickoff ends at the touchback spot"
    assert r["EPA"] < 1.0, f"enforcement spot must not score as field position (got {r['EPA']})"


def test_kickoff_penalty_without_rekick_takes_the_next_plays_start(monkeypatch):
    """B3 part (d.ii): when no re-kick follows, the receiving team really did take
    over, and the first real play after the flag is the only record of where. The
    row itself describes no kick outcome, so its end state cannot be believed.

    401425418 p102 ("Tristan Mattson kickoff ARKANSAS ST Penalty, Unsportsmanlike
    Conduct ... to the ArkSt 20") held end.yardsToEndzone=20 -- the enforcement
    spot -- and published EPA +5.52 while the next snap starts at 76.
    """
    df = _run(monkeypatch, 401425418)
    row = df.filter(
        pl.col("text").str.contains("(?i)kickoff")
        & pl.col("text").str.contains("(?i)ARKANSAS ST Penalty, Unsportsmanlike")
    )
    assert row.height == 1
    r = row.row(0, named=True)
    assert r["end.yardsToEndzone"] > 25, "the enforcement spot must not survive as field position"
    assert abs(r["EPA"]) < 1.0, f"a flag with no kick described must not swing EP (got {r['EPA']})"
