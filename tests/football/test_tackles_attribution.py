"""Tackles are credited to the tackler's own team, not to the play's defense.

Real fixture: the 2025 CFP title game (401769076, Indiana 27, Miami 21) as the
cfb-data build reads it -- the final's plays, its play participants (stringified
tackler lists) and its game roster. Built today, the box reproduces the published
``adv_tackles`` rows for this game exactly, including two Indiana players filed
under Miami (Jeff Utzinger's punt-coverage tackle and Devan Boykin's).
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.usage_box import _decode_list_cell, aggregate_usage_box, create_usage_box

FIX = Path(__file__).parent / "fixtures"
INDIANA, MIAMI = 84, 2390


@pytest.fixture(scope="module")
def game() -> dict:
    p = FIX / "cfb_401769076_"
    return {
        "plays": pl.read_parquet(f"{p}plays.parquet"),
        "participants": pl.read_parquet(f"{p}participants.parquet"),
        "roster": pl.read_parquet(f"{p}roster.parquet"),
    }


@pytest.fixture(scope="module")
def box(game) -> dict:
    return create_usage_box(game["plays"], game["participants"], league="cfb", rosters=game["roster"])


def _roster_team(game) -> dict[str, int]:
    r = game["roster"]
    return dict(zip(r["athlete_id"].cast(pl.Utf8), r["team_id"]))


def test_every_tackler_is_under_his_roster_team(game, box):
    team = _roster_team(game)
    rows = pl.from_dicts(box["tackles"])
    wrong = rows.filter(pl.col("player_id").replace_strict(team, default=None) != pl.col("def_pos_team"))
    assert wrong.height == 0, wrong.select("def_pos_team", "player_name").to_dicts()


def test_punt_coverage_tackle_stays_with_indiana(box):
    rows = pl.from_dicts(box["tackles"]).filter(pl.col("player_name") == "Jeff Utzinger")
    assert rows["def_pos_team"].to_list() == [INDIANA]


def test_raw_tackle_counts_stay_complete(game, box):
    """Every tackler / assist credit on a play lands in exactly one row."""
    ids = game["participants"].join(game["plays"].select(pl.col("id").alias("play_id")), on="play_id", how="semi")
    n = {
        kind: sum(len(_decode_list_cell(v) or []) for v in ids[col].to_list())
        for kind, col in (("tackles", "tackler_player_ids"), ("assists", "assisted_by_player_ids"))
    }
    rows = pl.from_dicts(box["tackles"])
    assert (rows["tackles"].sum(), rows["assists"].sum()) == (n["tackles"], n["assists"])


def test_a_tackler_is_one_row_per_game_and_per_season(box):
    rows = pl.from_dicts(box["tackles"])
    assert rows["player_id"].is_unique().all()
    season = aggregate_usage_box("tackles", [rows.with_columns(season=pl.lit(2025))])
    assert season["player_id"].is_unique().all()


def test_without_a_roster_the_defense_keeps_the_credit(game):
    box = create_usage_box(game["plays"], game["participants"], league="cfb")
    rows = pl.from_dicts(box["tackles"]).filter(pl.col("player_name") == "Jeff Utzinger")
    # his punt-coverage tackle is filed under the play's defense (Miami), as before
    assert set(rows["def_pos_team"]) == {INDIANA, MIAMI}
