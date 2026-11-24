"""ESPN types some passes and sacks "Rush"; the text decides (cfb_pbp.py ``__helper_cfb_pbp_features``).

Captures: ``fixtures/README.md`` (``review_laterals/`` 313160024, ``rush_typed_passes/`` 401111653).
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"
# Processing a default-odds capture warns DefaultOddsWarning incidentally; the market plays no part
# in what these tests read. tests/cfb/test_cfb_pbp_offline.py::test_default_odds_warn asserts it.
pytestmark = pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")


@functools.cache
def _plays(path: str) -> pl.DataFrame:
    summary = json.loads(gzip.decompress((FIX / path).read_bytes()))
    proc = CFBPlayProcess(gameId=int(summary["header"]["id"]), join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame


def _play(path: str, play_id: int) -> dict:
    return _plays(path).filter(pl.col("id") == play_id).row(0, named=True)


def test_a_completion_espn_typed_a_rush_is_a_catch():
    """313160024257: "Andrew Luck pass complete to Drew Terrell for 29 yards, fumbled, ..." (ESPN: Rush)."""
    play = _play("review_laterals/summary_313160024_trimmed.json.gz", 313160024257)
    assert (play["rush"], play["pass"], play["completion"]) == (False, True, True)
    assert (play["yds_receiving"], play["yds_rushed"], play["receiver_player_name"]) == (29, None, "Drew Terrell")
    assert play["turnover_vec"] == True  # noqa: E712  the fumble was still lost


def test_a_shovel_pass_espn_typed_a_rush_is_a_catch():
    """313160024035: "Darron Thomas shovel pass complete to De'Anthony Thomas for no gain" (ESPN: Rush)."""
    play = _play("review_laterals/summary_313160024_trimmed.json.gz", 313160024035)
    assert (play["type.text"], play["rush"], play["pass"], play["yds_receiving"]) == ("Pass Reception", False, True, 0)


def test_a_sack_espn_typed_a_rush_is_a_sack():
    """401111653103868001: "Tobias Oliver sacked by Xavier Thomas and Tyler Davis for a loss of 3 yards" (ESPN: Rush)."""
    play = _play("rush_typed_passes/summary_401111653_trimmed.json.gz", 401111653103868001)
    assert (play["type.text"], play["rush"], play["pass"], play["sack_vec"]) == ("Sack", False, True, True)
    assert play["yds_rushed"] is None
