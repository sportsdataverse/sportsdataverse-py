"""On a lateral, the credited catcher / carrier / returner gets the box score's full name, on real 2024 games.

ESPN lists a lateral's recipient as the play's receiver, rusher or returner, so the participants join skips
ESPN's participant there and keeps the text's player (``coalesce_participants``). The id then resolves from the
box score, but the name stayed the text's abbreviation ("A.St. Brown"), unlike every other play's full name.

Fixtures (``fixtures/lateral_names/``, processed offline as the nfl-data build does: the stored summary plus the
stored core play items, names from the summary's box score):

* ``401671772`` -- SF, 2024: "B.Purdy pass short left to R.Bell ... Lateral to E.Saubert".
* ``401671642`` -- DET, 2024: "J.Goff pass short left to A.St. Brown ... Lateral to J.Williams".
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.play_participants import (
    _LATERAL_RE,
    athlete_lookup_from_summary,
    play_participants_from_items,
)
from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures" / "lateral_names"
ROLES = ("receiver", "rusher", "kickoff_return", "punt_return")


def _load(name: str):
    with gzip.open(FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


@functools.lru_cache(maxsize=None)
def _process(game_id: int, with_participants: bool = True) -> tuple[pl.DataFrame, dict[str, str]]:
    summary = _load(f"summary_{game_id}_trimmed.json.gz")
    lookup = athlete_lookup_from_summary(summary)
    parts = None
    if with_participants:
        items = _load(f"plays_{game_id}.json.gz")["items"]
        parts = play_participants_from_items(items, game_id, athlete_lookup=lookup)
    proc = NFLPlayProcess(gameId=game_id, participants=parts, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame, lookup


def _row(plays: pl.DataFrame, text: str) -> dict:
    hit = plays.filter(pl.col("text").str.contains(text, literal=True))
    assert hit.height == 1, text
    return hit.row(0, named=True)


def test_the_catcher_keeps_the_credit_and_gets_his_full_name():
    plays, _ = _process(401671772)
    row = _row(plays, "B.Purdy pass short left to R.Bell to SF 42 for 12 yards. Lateral to E.Saubert")
    assert str(row["receiver_player_id"]) == "4372063"  # R.Bell, the catcher (not the lateral's recipient)
    assert row["receiver_player_name"] == "Ronnie Bell"


def test_an_unchanged_id_reads_the_full_name_not_the_text_abbreviation():
    plays, _ = _process(401671642)
    row = _row(plays, "J.Goff pass short left to A.St. Brown to DET 38 for 2 yards. Lateral to J.Williams")
    assert str(row["receiver_player_id"]) == "4374302"
    assert row["receiver_player_name"] == "Amon-Ra St. Brown"


@pytest.mark.parametrize("game_id", [401671772, 401671642])
def test_every_lateral_role_with_a_box_score_id_reads_the_box_score_name(game_id):
    plays, lookup = _process(game_id)
    lateral = plays.filter(pl.col("text").fill_null("").str.contains(_LATERAL_RE))
    assert lateral.height > 0
    checked = 0
    for role in ROLES:
        name, pid = f"{role}_player_name", f"{role}_player_id"
        if name not in lateral.columns or pid not in lateral.columns:
            continue
        for r in lateral.select(name, pid).drop_nulls().iter_rows(named=True):
            full = lookup.get(str(r[pid]))
            if full is not None:
                assert r[name] == full, (role, r)
                checked += 1
    assert checked > 0


@pytest.mark.parametrize(
    ("game_id", "text", "name"),
    [
        (401671772, "B.Purdy pass short left to R.Bell to SF 42 for 12 yards. Lateral to E.Saubert", "R.Bell"),
        (
            401671642,
            "J.Goff pass short left to A.St. Brown to DET 38 for 2 yards. Lateral to J.Williams",
            "A.St. Brown",
        ),
    ],
)
def test_without_the_participants_join_a_lateral_keeps_the_text_name(game_id, text, name):
    # every play keeps the text's name then, so a lateral must too, or one id would carry two names in a
    # game (the box score groups by name)
    plays, _ = _process(game_id, with_participants=False)
    assert _row(plays, text)["receiver_player_name"] == name
