"""Round-3 fixes from the processor-invariant sweep (S1-SWEEP, PR #510), on real ESPN data.

Fixtures are nfl-raw summaries trimmed to the keys the processor reads (header,
drives, boxscore, gameInfo, pickcenter), processed offline through
``espn_nfl_pbp(summary=...)``:

* ``summary_221013007_trimmed.json.gz`` -- MIA @ DEN, 2002 week 6 ("First Last (TM)" text)
* ``summary_251113015_trimmed.json.gz`` -- NE @ MIA, 2005 week 10 (score drops, "Sack" stubs)
* ``summary_290927014_trimmed.json.gz`` -- GB @ STL, 2009 week 3 (blocked FG, "Josh.Brown")
* ``summary_291018018_trimmed.json.gz`` -- NYG @ NO, 2009 week 6 (``&apos;`` in the text)
* ``summary_400791508_trimmed.json.gz`` -- WSH @ PHI, 2015 week 16 (end.team flips, "PAT failed")
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
def mia_den_2002() -> pl.DataFrame:
    return _process(221013007)


@pytest.fixture(scope="module")
def ne_mia_2005() -> pl.DataFrame:
    return _process(251113015)


@pytest.fixture(scope="module")
def gb_stl_2009() -> pl.DataFrame:
    return _process(290927014)


@pytest.fixture(scope="module")
def nyg_no_2009() -> pl.DataFrame:
    return _process(291018018)


@pytest.fixture(scope="module")
def wsh_phi_2015() -> pl.DataFrame:
    return _process(400791508)


def _row(frame: pl.DataFrame, play_id: int) -> dict:
    return frame.filter(pl.col("id") == play_id).row(0, named=True)


# ---------------------------------------------------------------------------
# N18 -- home/away WP after a play is the next play's home/away WP before
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("which", ["wsh_phi_2015", "mia_den_2002"])
def test_home_wp_after_is_never_the_complement_of_the_next_play(request, which):
    f = request.getfixturevalue(which).with_columns(pl.col("home_wp_before").shift(-1).alias("_next"))
    changed = f.filter(
        (pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
        & pl.col("home_wp_after").is_not_null()
        & pl.col("_next").is_not_null()
    )
    assert changed.height >= 10
    complemented = changed.filter(
        ((pl.col("home_wp_after") - (1 - pl.col("_next"))).abs() < 0.01)
        & ((pl.col("home_wp_after") - pl.col("_next")).abs() > 0.05)
    )
    assert complemented.select("id", "type.text").rows() == []
    assert (changed["home_wp_after"] + changed["away_wp_after"] - 1).abs().max() < 1e-6


# ---------------------------------------------------------------------------
# N19 -- an ESPN end.team flip on a play that kept the ball does not flip wp_after
# ---------------------------------------------------------------------------


def test_end_team_flip_on_a_kept_possession_keeps_the_next_plays_perspective(wsh_phi_2015):
    f = wsh_phi_2015.with_columns(
        pl.col("start.pos_team.id").shift(-1).alias("_next_pos"),
        pl.col("wp_before").shift(-1).alias("_lead_wp_before"),
    )
    flipped = f.filter(
        (pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
        & (pl.col("_next_pos") == pl.col("start.pos_team.id"))
        & (pl.col("scoringPlay") == False)  # noqa: E712
        & pl.col("type.text").is_in(["Rush", "Pass Reception", "Pass Incompletion", "Sack"])
    )
    assert flipped.height >= 5  # ESPN's end.team is wrong on eight rows of this game
    assert (flipped["wp_after"] - flipped["_lead_wp_before"]).abs().max() < 1e-6
    assert flipped["wpa"].abs().max() < 0.25
