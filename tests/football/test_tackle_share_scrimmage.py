"""Tackle share counts the defense's own standing scrimmage snaps; raw tackle counts stay complete.

Real fixture: the 2025 CFP title game (401769076, see test_tackles_attribution.py). Kickoff and
punt coverage, field goals, plays a penalty wiped out and the offense's tackles after a turnover
are credited (tackles / assists / tackle_points) but are not part of a defense's tackle share
(owner decision, 2026-09-30).
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.usage_box import _decode_list_cell, aggregate_usage_box, create_usage_box

FIX = Path(__file__).parent / "fixtures" / "cfb_401769076_"


@pytest.fixture(scope="module")
def game() -> dict:
    return {k: pl.read_parquet(f"{FIX}{k}.parquet") for k in ("plays", "participants", "roster")}


@pytest.fixture(scope="module")
def box(game) -> dict:
    return create_usage_box(game["plays"], game["participants"], league="cfb", rosters=game["roster"])


def _credits(game) -> pl.DataFrame:
    """(play, player, kind) credits joined to the play's flags and the tackler's roster team."""
    plays = game["plays"].select(
        pl.col("id").alias("play_id"), "pos_team", "def_pos_team", "scrimmage_play", "penalty_no_play"
    )
    team = dict(zip(game["roster"]["athlete_id"].cast(pl.Utf8), game["roster"]["team_id"]))
    rows = []
    for r in game["participants"].join(plays, on="play_id").iter_rows(named=True):
        for col, kind in (("tackler_player_ids", 1.0), ("assisted_by_player_ids", 0.5)):
            for pid in _decode_list_cell(r[col]) or []:
                rows.append({**r, "player_id": pid, "points": kind, "team": team.get(pid, r["def_pos_team"])})
    return pl.DataFrame(rows).with_columns(
        eligible=pl.col("scrimmage_play") & ~pl.col("penalty_no_play") & (pl.col("team") == pl.col("def_pos_team"))
    )


def test_share_is_scrimmage_points_over_the_defense_total(game, box):
    c = _credits(game)
    want = c.filter("eligible").group_by("team", "player_id").agg(pts=pl.col("points").sum())
    want = want.with_columns(share=pl.col("pts") / pl.col("pts").sum().over("team"))
    got = pl.from_dicts(box["tackles"]).join(
        want, left_on=["def_pos_team", "player_id"], right_on=["team", "player_id"]
    )
    assert got.height >= 25
    assert (got["scrimmage_tackle_points"] == got["pts"]).all()
    assert ((got["tackle_share"] - got["share"]).abs() < 1e-12).all()


def test_raw_counts_keep_special_teams_tackles(game, box):
    rows = pl.from_dicts(box["tackles"])
    c = _credits(game)
    assert rows["tackle_points"].sum() == c["points"].sum()
    assert rows["scrimmage_tackle_points"].sum() < rows["tackle_points"].sum()
    utz = rows.filter(pl.col("player_name") == "Jeff Utzinger").row(0, named=True)
    # kickoff and punt coverage only: credited, but no part of Indiana's tackle share
    assert utz["tackle_points"] > 0 and utz["scrimmage_tackle_points"] == 0 and utz["tackle_share"] == 0


def test_each_defense_shares_sum_to_one(box):
    rows = pl.from_dicts(box["tackles"])
    sums = rows.group_by("def_pos_team").agg(pl.col("tackle_share").sum())
    assert ((sums["tackle_share"] - 1).abs() < 1e-9).all()


def test_season_share_recomputes_from_scrimmage_points(box):
    game_rows = pl.from_dicts(box["tackles"]).with_columns(season=pl.lit(2025))
    season = aggregate_usage_box("tackles", [game_rows, game_rows])
    j = season.join(pl.from_dicts(box["tackles"]), on=["def_pos_team", "player_id"], suffix="_g")
    assert ((j["tackle_share"] - j["tackle_share_g"]).abs() < 1e-12).all()
    assert (j["scrimmage_tackle_points"] == 2 * j["scrimmage_tackle_points_g"]).all()


def test_position_groups_share_the_same_rule(box):
    groups = pl.from_dicts(box["position_group_tackles"])
    sums = groups.group_by("def_pos_team").agg(pl.col("scrimmage_tackle_points").sum())
    players = pl.from_dicts(box["tackles"]).filter(pl.col("position_group").is_not_null())
    want = players.group_by("def_pos_team").agg(pl.col("scrimmage_tackle_points").sum())
    assert sums.sort("def_pos_team").equals(want.sort("def_pos_team"))
