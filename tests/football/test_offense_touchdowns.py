"""A defensive touchdown is not the offense's touchdown, in tendencies and the usage box.

Real fixture: two 2025 CFB games whose third downs include a pick-six (401752802,
Michigan State) and a fumble-return touchdown (401752689, Missouri), next to two
offensive third-down touchdowns (Kansas rush, Missouri pass) that must still
count. Both defensive scores carry ``touchdown=True`` and ``first_down_created=False``
in the released pbp.
"""

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.football.tendencies import tendencies
from sportsdataverse.football.usage_box import create_usage_box, fit_third_down_curve

FIX = Path(__file__).parent / "fixtures" / "cfb_2025_defensive_td_third_downs.parquet"
_NON_ST = ("player_usage", "position_group_usage", "team_usage", "drive_scripting")


@pytest.fixture(scope="module")
def plays() -> pl.DataFrame:
    return pl.read_parquet(FIX)


def _no_defensive_tds(p: pl.DataFrame) -> pl.DataFrame:
    return p.with_columns(touchdown=pl.col("touchdown") & ~pl.col("defense_score_play"))


def test_fixture_holds_defensive_and_offensive_third_down_tds(plays):
    third_td = plays.filter((pl.col("start.down") == 3) & pl.col("touchdown") & pl.col("scrimmage_play"))
    assert third_td.filter(pl.col("defense_score_play"))["type.text"].sort().to_list() == [
        "Fumble Return Touchdown",
        "Interception Return Touchdown",
    ]
    assert third_td.filter(~pl.col("defense_score_play")).height == 2
    assert not third_td["first_down_created"].any()


def test_tendencies_ignore_defensive_touchdowns(plays):
    assert_frame_equal(tendencies(plays, league="cfb"), tendencies(_no_defensive_tds(plays), league="cfb"))


def test_third_down_conversions_count_offensive_touchdowns_only(plays):
    t = tendencies(plays, league="cfb")
    third = plays.filter((pl.col("start.down") == 3) & pl.col("scrimmage_play") & ~pl.col("penalty_no_play"))
    want = third.group_by("pos_team").agg(
        n=(pl.col("first_down_created") | (pl.col("touchdown") & ~pl.col("defense_score_play"))).sum()
    )
    got = t.select("pos_team", "third_down_conversions").join(want, on="pos_team")
    assert got.height == 4
    assert (got["third_down_conversions"] == got["n"]).all()


def test_usage_box_ignores_defensive_touchdowns(plays):
    for gid in (401752802, 401752689):
        game = plays.filter(pl.col("game_id") == gid)
        got, want = create_usage_box(game, league="cfb"), create_usage_box(_no_defensive_tds(game), league="cfb")
        for s in _NON_ST:
            assert got[s] == want[s], (gid, s)


def test_third_down_curve_ignores_defensive_touchdowns(plays):
    assert_frame_equal(fit_third_down_curve(plays), fit_third_down_curve(_no_defensive_tds(plays)))
