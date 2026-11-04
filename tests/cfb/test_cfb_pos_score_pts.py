"""``pos_score_pts`` is the points scored on the play, signed for the possessing team (sportsdataverse-py#1089)."""

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

# incidental on these captures; asserted in test_cfb_pbp_offline.py::test_default_odds_warn and
# test_cfb_whole_game_oracle.py::test_period_monotonic_or_the_step_back_warns
pytestmark = [
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning"),
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.PlayOrderWarning"),
]

FIX = Path(__file__).parent / "fixtures"


def _plays(game_id: int) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=json.loads((FIX / f"summary_{game_id}.json").read_text(encoding="utf-8")))
    proc.run_processing_pipeline(fourth_down_probs=False, two_pt_probs=False)
    return proc.plays_frame


def _pts(df: pl.DataFrame, play: int) -> int:
    return df.filter(pl.col("game_play_number") == play)["pos_score_pts"].item()


@pytest.mark.parametrize("game_id", [401628374, 401628455])
def test_bounded_and_sums_to_the_final_margin(game_id):
    df = _plays(game_id)
    assert df["pos_score_pts"].abs().max() <= 8
    home = pl.col("pos_team") == pl.col("homeTeamId")
    net_home = df.select(pl.when(home).then(pl.col("pos_score_pts")).otherwise(-pl.col("pos_score_pts")).sum()).item()
    assert net_home == df["homeFinalScore"].max() - df["awayFinalScore"].max()


def test_a_feed_score_that_drops_to_zero_and_returns_scores_nothing():
    """401628374: ESPN's score reads 0-0 on plays 28-34 after a 14-0 lead; the running margin read 14 and 21."""
    df = _plays(401628374)
    assert _pts(df, 28) == 0  # the kickoff after the drop
    assert _pts(df, 35) == 7  # the touchdown that made it 21-0
    assert _pts(df, 80) == -2  # a safety: the possessing team gives up two


def test_a_defensive_touchdown_is_negative_for_the_offence():
    df = _plays(401628455)
    assert _pts(df, 113) == -7  # Fumble Return Touchdown
    assert _pts(df, 165) == -7  # Interception Return Touchdown
