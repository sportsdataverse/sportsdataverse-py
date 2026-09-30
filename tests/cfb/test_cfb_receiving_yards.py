"""A completion's receiving yards when its text states no "complete to ... for N" gain.

Real fixtures, offline, on stored ESPN summaries (trimmed to the keys the processor reads):

* ``summary_401752817_trimmed.json.gz`` -- Western Illinois @ Northwestern, 2025. ESPN writes
  "Preston Stone pass to Cam Porter for 4 yds to the NU 31" (no "complete"); every such
  completion came out with null ``yds_receiving``, so Stone's box line read 15 yards.
* ``summary_401643775_trimmed.json.gz`` -- North Texas @ South Alabama, 2024. ESPN writes
  "Chandler Morris pass complete to X for a 1ST down" with no yardage in the text at all.

Oracles: the yards each text states, and the change in field position.
"""

from __future__ import annotations

import gzip
import json
from functools import lru_cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"
GAMES = (401752817, 401643775)


def _summary(game_id: int) -> dict:
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


@lru_cache(maxsize=None)
def _plays(game_id: int) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id)
    proc.espn_cfb_pbp(summary=_summary(game_id))
    proc.run_processing_pipeline()
    return proc.plays_frame


def _completions(game_id: int) -> pl.DataFrame:
    return _plays(game_id).filter((pl.col("pass") == True) & (pl.col("completion") == True))  # noqa: E712


def test_pass_to_completions_read_their_stated_yards():
    """Oracle: the yards ESPN states in the text ("... pass to Cam Porter for 4 yds ...")."""
    c = _completions(401752817).with_columns(
        stated=pl.col("text").str.extract(r"pass to .+? for (-?\d+) yds?\b").cast(pl.Int32)
    )
    pass_to = c.filter(pl.col("stated").is_not_null() & ~pl.col("text").str.contains("complete to"))
    assert pass_to.height == 26
    assert (pass_to["yds_receiving"] == pass_to["stated"]).all()
    stone = c.filter(pl.col("passer_player_name") == "Preston Stone")["yds_receiving"].sum()
    # was 15; ESPN's box says 245: +20 on a reversed-text row filed under passer "TEAM" and -3 on a
    # completion lost to a fumble (typed Fumble Recovery, pass=False) are passer-attribution issues
    assert stone == 228


def test_yardless_completions_take_the_field_position_change():
    """Oracle: 2024's "pass complete to X for a 1ST down" states no yards; the spot moved."""
    # touchdown rows ("25 Yd pass from ...") state their yards and end at the next kickoff spot
    c = _completions(401643775).filter(
        pl.col("penalty_detail").is_null() & ~pl.col("text").str.contains(r"for -?\d+|Yd pass")
    )
    moved = (
        pl.when(pl.col("start.team.id") == pl.col("end.team.id"))
        .then(pl.col("start.yardsToEndzone") - pl.col("end.yardsToEndzone"))
        .otherwise(pl.col("start.yardsToEndzone") - (100 - pl.col("end.yardsToEndzone")))
    )
    assert c.height >= 45
    assert (c["yds_receiving"] == c.select(moved).to_series()).all()


@pytest.mark.parametrize("game_id", GAMES)
def test_every_completion_without_a_penalty_has_its_yards(game_id):
    c = _completions(game_id).filter(pl.col("penalty_detail").is_null())
    assert c["yds_receiving"].null_count() == 0


@pytest.mark.parametrize("game_id", GAMES)
def test_incompletions_and_sacks_gain_nothing(game_id):
    """The completion flag gates the fallback: no incompletion or sack takes statYardage."""
    other = _plays(game_id).filter((pl.col("pass") == True) & (pl.col("completion") == False))  # noqa: E712
    assert other.height > 10
    assert other["yds_receiving"].fill_null(0).abs().sum() == 0
