"""A season leaderboard keys a player on (team, player id), not on his name or position group.

Real fixture: published 2025 CFB ``adv_player_usage`` rows, in the shape the cfb-data build
aggregates (``pos_team`` is the team id). Danny Scudero (San Jose State) has 8 games with no
position group (a roster gap) and 4 as a WR, so the published season table split him into a
106-target and a 54-target row. Jackson Harris splits the same way (67 + 12); Josh Manning is
"Joshua Manning" in one game. Texas's three id-less rows are three different names and must
stay three rows.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.usage_box import aggregate_usage_box

FIX = Path(__file__).parent / "fixtures" / "cfb_2025_adv_player_usage_split_players.parquet"


@pytest.fixture(scope="module")
def season() -> pl.DataFrame:
    return aggregate_usage_box("player_usage", [pl.read_parquet(FIX)])


def _one(season: pl.DataFrame, player_id: str) -> dict:
    rows = season.filter(pl.col("player_id") == player_id)
    assert rows.height == 1, rows.select("player_name", "position_group", "targets").to_dicts()
    return rows.row(0, named=True)


def test_a_missing_position_group_does_not_split_a_player(season):
    scudero = _one(season, "5152815")
    assert (scudero["targets"], scudero["games"], scudero["position_group"]) == (160, 12, "WR")
    harris = _one(season, "5114311")
    assert (harris["targets"], harris["games"], harris["position_group"]) == (79, 10, "WR")


def test_a_name_change_does_not_split_a_player(season):
    manning = _one(season, "4921113")
    assert (manning["player_name"], manning["targets"], manning["games"]) == ("Josh Manning", 51, 12)


def test_id_less_rows_stay_one_per_name(season):
    texas = season.filter(pl.col("player_id").is_null())
    assert sorted(texas["player_name"]) == ["Aaron Butler", "Nick Townsend", "Texas"]


def test_output_columns_keep_their_order(season):
    assert season.columns[:5] == ["season", "pos_team", "player_id", "player_name", "position_group"]
