"""N37 -- ESPN type 80, ``"Sack Opp Fumble Recovery"``, on real 2025 ESPN data.

ESPN introduced the type in 2025 (84 rows in 2025, 17 so far in 2026; the NFL
Shield adapter emits it too). Its ``type.text`` was in none of the play-type
vectors, so the row was neither a turnover nor a possession change and the end
state stayed with the offense that had just lost the ball: 3 of the first 5 gave
that offense *positive* EPA. Fixtures are nfl-raw summaries trimmed to the keys
the processor reads (header, drives, boxscore, gameInfo, pickcenter):

* ``summary_401772636_trimmed.json.gz`` -- ATL @ IND, 2025 week 10: two type-80
  rows the *defense* recovers (play 349 read +3.71 against nflfastR's -5.48)
* ``summary_401772748_trimmed.json.gz`` -- CLE @ PIT, 2025 week 6: the one
  type-80 row in the 2025-26 corpus the *offense* recovers itself
* ``summary_401772726_trimmed.json.gz`` -- CLE @ BAL, 2025 week 2: a type-80 row
  the defense returns for a touchdown
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _process(game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = NFLPlayProcess(gameId=game_id)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame.sort("game_play_number")


@pytest.fixture(scope="module")
def atl_ind_2025() -> pl.DataFrame:
    return _process(401772636)


@pytest.fixture(scope="module")
def cle_pit_2025() -> pl.DataFrame:
    return _process(401772748)


@pytest.fixture(scope="module")
def cle_bal_2025() -> pl.DataFrame:
    return _process(401772726)


def _row(frame: pl.DataFrame, play_id: int) -> dict:
    return frame.filter(pl.col("id") == play_id).row(0, named=True)


def test_a_strip_sack_the_defense_recovers_is_the_defenses_recovery(atl_ind_2025):
    """Both type-80 rows are opponent recoveries: a turnover, and EPA against the offense.

    nflfastR ``epa`` for the same GSIS play ids is -5.48 (349) and -5.33 (2332);
    on the unfixed processor they read +3.71 and -0.97.
    """
    for play_id, gsis_epa in ((401772636349, -5.48), (4017726362332, -5.33)):
        row = _row(atl_ind_2025, play_id)
        assert row["orig_play_type"] == "Sack Opp Fumble Recovery"
        assert row["type.text"] == "Fumble Recovery (Opponent)"
        assert row["turnover_vec"] is True
        assert row["change_of_poss"] == 1
        assert row["start.pos_team.id"] != row["end.pos_team.id"]
        assert row["EPA"] == pytest.approx(gsis_epa, abs=0.15)


def test_a_strip_sack_the_offense_recovers_itself_is_not_a_turnover(cle_pit_2025):
    """ESPN types this own recovery 80 as well; possession comes from the feed, not the type.

    nflfastR ``epa`` for GSIS play 2288 is +0.77 -- CLE was sacked at CLV 20 and
    recovered at CLV 30, a net gain.
    """
    row = _row(cle_pit_2025, 4017727482288)
    assert row["orig_play_type"] == "Sack Opp Fumble Recovery"
    assert row["type.text"] == "Fumble Recovery (Own)"
    assert row["turnover_vec"] is False
    assert row["change_of_poss"] == 0
    assert row["EPA"] == pytest.approx(0.77, abs=0.15)


def test_a_strip_sack_returned_for_a_touchdown_is_a_defensive_score(cle_bal_2025):
    """ESPN replaces the gamebook text with the scoring summary, so ``td_play`` cannot see it.

    nflfastR ``epa`` for GSIS play 3773 is -8.36; the unfixed processor read -2.84.
    """
    row = _row(cle_bal_2025, 4017727263773)
    assert row["orig_play_type"] == "Sack Opp Fumble Recovery"
    assert row["type.text"] == "Fumble Return Touchdown"
    assert row["defense_score_play"] is True
    assert row["EPA"] == pytest.approx(-8.36, abs=0.15)
