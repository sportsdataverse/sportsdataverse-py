"""Era-aware clock/half regression tests for espn_wnba_pbp (wehoop#39).

WNBA played 2x20-minute halves through 2005 and 10-minute quarters from 2006.
"""

import polars as pl
import pytest

from sportsdataverse.wnba import espn_wnba_pbp
from tests.conftest import skip_if_no_live


def _plays(game_id):
    plays = espn_wnba_pbp(game_id=game_id)["plays"]
    if not plays:
        pytest.skip(f"ESPN returned no plays for {game_id}")
    return pl.from_dicts(plays)


@skip_if_no_live
def test_wnba_pre2006_halves_model():
    df = _plays(250506017)  # 2005 regular-season game
    p1 = df.filter(pl.col("period.number") == 1)
    p2 = df.filter(pl.col("period.number") == 2)
    assert p1.get_column("half").unique().to_list() == [1]
    assert p2.get_column("half").unique().to_list() == [2]
    assert p1.get_column("start.game_seconds_remaining").max() == 2400
    assert p2.get_column("start.game_seconds_remaining").max() <= 1200
    assert p1.get_column("start.half_seconds_remaining").max() <= 1200
    assert p1.get_column("start.quarter_seconds_remaining").max() <= 1200


@skip_if_no_live
def test_wnba_2006plus_quarters_model_unchanged():
    df = _plays(401649378)  # 2024 regular-season game
    p2 = df.filter(pl.col("period.number") == 2)
    assert p2.get_column("half").unique().to_list() == [1]
    assert p2.get_column("start.game_seconds_remaining").max() <= 1800
