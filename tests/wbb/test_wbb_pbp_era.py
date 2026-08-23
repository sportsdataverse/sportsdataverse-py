"""Era-aware clock/half regression tests for espn_wbb_pbp (wehoop#39 sibling).

NCAA women's basketball played 2x20-minute halves through 2014-15 and
10-minute quarters from 2015-16 (ESPN season year 2016).
"""

import polars as pl
import pytest

from sportsdataverse.wbb import espn_wbb_pbp
from tests.conftest import skip_if_no_live


def _plays(game_id: int) -> pl.DataFrame:
    plays = espn_wbb_pbp(game_id=game_id)["plays"]
    if not plays:
        pytest.skip(f"ESPN returned no plays for {game_id}")
    return pl.from_dicts(plays)


@skip_if_no_live
def test_wbb_pre2016_halves_model() -> None:
    df = _plays(400595044)  # 2014-15 season game
    p1 = df.filter(pl.col("period.number") == 1)
    p2 = df.filter(pl.col("period.number") == 2)
    assert p1.get_column("half").unique().to_list() == [1]
    assert p2.get_column("half").unique().to_list() == [2]
    assert p1.get_column("start.game_seconds_remaining").max() == 2400
    assert p2.get_column("start.game_seconds_remaining").max() <= 1200
    assert p1.get_column("start.half_seconds_remaining").max() <= 1200
    assert p1.get_column("start.quarter_seconds_remaining").max() <= 1200


@skip_if_no_live
def test_wbb_2016plus_quarters_model_unchanged() -> None:
    df = _plays(401596715)  # 2023-24 season game
    p2 = df.filter(pl.col("period.number") == 2)
    assert p2.get_column("half").unique().to_list() == [1]
    assert p2.get_column("start.game_seconds_remaining").max() <= 1800
