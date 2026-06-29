"""Test shot-zone classification."""

import json
import pathlib

import polars as pl

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_shot_zones import SHOT_ZONES, add_shot_zones

FX = pathlib.Path("tests/fixtures/nba_engine/0022200001")


def _enh() -> pl.DataFrame:
    return enhanced_pbp_from_payload(json.loads((FX / "playbyplayv3.json").read_text()))


def test_shot_zone_classification() -> None:
    """Test that shot zones are classified correctly per pbpstats rules."""
    df = add_shot_zones(_enh())

    # zone is set exactly on FG attempts, null elsewhere
    fg = df.filter(pl.col("is_field_goal") == 1)
    assert fg.height > 0
    assert fg["shot_zone"].null_count() == 0
    assert (
        df.filter(pl.col("is_field_goal") != 1)["shot_zone"].null_count()
        == df.filter(pl.col("is_field_goal") != 1).height
    )
    assert set(fg["shot_zone"].unique().to_list()).issubset(set(SHOT_ZONES))

    # every 3 is a 3-zone
    threes = fg.filter(pl.col("shot_value") == 3)
    assert threes["shot_zone"].is_in(["corner_3", "above_the_break_3"]).all()
