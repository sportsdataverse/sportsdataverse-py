"""GOP Q7 / Q26 / Q30: NBA / WNBA player box per game, period, garbage time and stint, on real captures.

Fixtures (none synthetic): the 16 NBA (2025-26) and 16 WNBA (2026) games of the BB-D6 corpus.
``tests/fixtures/bb_game_stints`` holds their pbp, possessions and stats.nba.com player box score
(README there); the lineups and schedule come from ``tests/fixtures/bb_game_impact``.

The oracle is stats.nba.com's own box score. The gates are the values observed on these games;
never lower one to make a change pass.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_impact import AWAY_SLOTS, HOME_SLOTS
from sportsdataverse._common.game_stints import (
    BOX_COUNTS,
    box_event_schema,
    game_player_box_splits,
    game_player_box_splits_schema,
    game_stints,
)
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_game_stints import nba_box_events, nba_game_player_box_splits, nba_stint_rows
from sportsdataverse.nba.nba_possession_flags import POSSESSION_FLAGS_SCHEMA, add_possession_flags
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_game_stints import wnba_game_player_box_splits

FIX = Path(__file__).resolve().parents[1] / "fixtures"
LEAGUES = {"nba": (2025, "00"), "wnba": (2026, "10")}
SHORT_LINEUP_GAMES = {"nba": {"0022500559", "0022500632"}, "wnba": set()}
PLAYER = pl.Int64()
KEY = ["game_id", "team_id", "player_id"]
#: stats.nba.com box column -> ours.
BOX = {
    "points": "points", "field_goals_made": "fgm", "field_goals_attempted": "fga",
    "three_pointers_made": "fg3m", "three_pointers_attempted": "fg3a", "free_throws_made": "ftm",
    "free_throws_attempted": "fta", "rebounds_offensive": "oreb", "rebounds_defensive": "dreb",
    "rebounds_total": "reb", "assists": "ast", "steals": "stl", "blocks": "blk", "turnovers": "tov",
    "fouls_personal": "pf",
}  # fmt: skip
#: Observed: player box rows, garbage-time actions and player-games with garbage minutes.
OBSERVED = {"nba": (435, 104, 16), "wnba": (391, 75, 45)}
#: Observed: events the source credits to a player his lineup has off the floor at that action
#: (2025-26 NBA lineups are mostly inferred from substitutions): four assists and a steal, on three
#: player-games.
OFF_FLOOR_EVENTS = {"nba": 5, "wnba": 0}


def _read(lg: str, name: str) -> pl.DataFrame:
    folder = "bb_game_stints" if name in ("pbp", "possessions", "player_box") else "bb_game_impact"
    return pl.read_parquet(FIX / folder / f"{lg}_{name}_{LEAGUES[lg][0]}.parquet")


def _rows(lg: str) -> pl.DataFrame:
    inputs = (_read(lg, n) for n in ("pbp", "game_lineups", "possessions", "schedule"))
    return nba_stint_rows(*inputs, league_id=LEAGUES[lg][1])


def _events(lg: str) -> pl.DataFrame:
    return nba_box_events(_read(lg, "pbp"), _read(lg, "schedule"))


def _box(lg: str) -> pl.DataFrame:
    box = _read(lg, "player_box")
    return box.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("team_id").cast(pl.Int64),
        pl.col("person_id").cast(pl.Int64).alias("player_id"),
        *[pl.col(s).cast(pl.Int64).alias(c) for s, c in BOX.items()],
    )


def _against_box(lg: str, ours: pl.DataFrame) -> pl.DataFrame:
    """Box minus ours per player-game and count (0 everywhere when they agree)."""
    box = _box(lg)
    summed = ours.group_by(KEY).agg(pl.col(list(BOX_COUNTS)).sum())
    assert summed.schema["player_id"] == box.schema["player_id"] and summed.schema["team_id"] == box.schema["team_id"]
    j = box.join(summed, on=KEY, how="full", coalesce=True, suffix="_o", validate="1:1")
    return j.select(*KEY, *[(pl.col(c).fill_null(0) - pl.col(f"{c}_o").fill_null(0)).alias(c) for c in BOX_COUNTS])


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_events_are_the_stats_nba_box(lg: str) -> None:
    events = _events(lg)
    assert events.schema == pl.Schema(box_event_schema(PLAYER))
    diff = _against_box(lg, events.with_columns((pl.col("oreb") + pl.col("dreb")).alias("reb")))
    assert diff.height == OBSERVED[lg][0]
    assert (diff.select(BOX_COUNTS).to_numpy() == 0).all()
    # every assist found its passer: the box's assists are the made shots with an AST credit
    pbp = _read(lg, "pbp")
    assert events["ast"].sum() == pbp["description"].str.contains(r"\d+ AST\)").sum()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_splits_add_up_to_the_box(lg: str) -> None:
    rows = _rows(lg)
    splits = game_player_box_splits(rows, _events(lg), player_dtype=PLAYER)
    assert splits.schema == pl.Schema(game_player_box_splits_schema(PLAYER))
    whole = splits.filter(family="all")
    # the whole game, the periods summed and the two garbage rows summed are each the box exactly
    for fam in ("all", "period", "garbage"):
        diff = _against_box(lg, splits.filter(family=fam))
        assert (diff.select(BOX_COUNTS).to_numpy() == 0).all(), fam
    # minutes: the periods and the garbage rows tile the game, which is the stints' total
    stints = game_stints(rows, player_dtype=PLAYER).group_by(KEY).agg(pl.col("seconds").sum())
    for fam in ("period", "garbage"):
        parts = splits.filter(family=fam).group_by(KEY).agg(pl.col("seconds").sum())
        j = whole.join(parts, on=KEY, how="full", coalesce=True, suffix="_p", validate="1:1")
        assert ((j["seconds"] - j["seconds_p"]).abs() < 1e-6).all(), fam
    j = whole.join(stints, on=KEY, how="left", suffix="_s", validate="1:1")
    assert ((j["seconds"] - j["seconds_s"].fill_null(0)).abs() < 1e-6).all()
    assert (splits.filter(family="garbage").group_by(KEY).len()["len"] == 2).all()
    assert splits.filter(family="garbage", split="garbage_time").filter(pl.col("seconds") > 0).height == OBSERVED[lg][2]
    period = splits.filter(family="period")
    assert (period["split"] == "p" + period["period"].cast(pl.Utf8)).all()
    assert splits.filter(pl.col("family") != "period").select("period", "on_floor_at_end").null_count().row(0) == (
        whole.height * 3,
        whole.height * 3,
    )


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_on_floor_at_period_end(lg: str) -> None:
    splits = game_player_box_splits(_rows(lg), _events(lg), player_dtype=PLAYER).filter(family="period")
    at_end = splits.group_by("game_id", "team_id", "period").agg(pl.col("on_floor_at_end").sum())
    whole = at_end.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))
    assert whole.height > 0 and (whole["on_floor_at_end"] == 5).all()
    assert (at_end["on_floor_at_end"] <= 5).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_garbage_time_is_the_possession_flag(lg: str) -> None:
    """An action inside a possession carries that possession's BB-D7 flag (the situational splits' one)."""
    rows = _rows(lg)
    assert rows["is_garbage_time"].null_count() == 0
    assert rows["is_garbage_time"].sum() == OBSERVED[lg][1]
    poss = _read(lg, "possessions")
    pbp = _read(lg, "pbp").select(
        "game_id", "order_index", "event_type", "seconds_remaining", "score_home", "score_away"
    )
    flagged = add_possession_flags(poss.drop(*POSSESSION_FLAGS_SCHEMA, strict=False), pbp)
    spans = flagged.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("start_order_index").cast(pl.Int64).alias("order"),
        pl.col("end_order_index").cast(pl.Int64).alias("_end"),
        pl.col("is_garbage_time").alias("_flag"),
    ).sort("order")
    inside = (
        rows.select("game_id", "order", "is_garbage_time")
        .sort("order")
        .join_asof(spans, on="order", by="game_id", strategy="backward")
        .filter(pl.col("order") <= pl.col("_end"))
    )
    assert inside.height > 0.9 * rows.height
    assert (inside["is_garbage_time"] == inside["_flag"]).all()
    # garbage time never ends once it starts
    late = rows.sort("game_id", "order").with_columns(
        pl.col("is_garbage_time").cast(pl.Int64).cum_max().over("game_id").alias("_seen")
    )
    assert (late["is_garbage_time"].cast(pl.Int64) == late["_seen"]).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_stint_box_is_the_box_less_off_floor_events(lg: str) -> None:
    rows, events = _rows(lg), _events(lg)
    stints = game_stints(rows, player_dtype=PLAYER, events=events)
    assert (stints["reb"] == stints["oreb"] + stints["dreb"]).all()
    # the residual, counted on its own: events at an action whose lineup does not list the player
    lineup = rows.select(
        "game_id",
        "order",
        "home_team_id",
        pl.concat_list(HOME_SLOTS).alias("_home"),
        pl.concat_list(AWAY_SLOTS).alias("_away"),
    )
    on = events.join(lineup, on=["game_id", "order"], how="inner", validate="m:1").with_columns(
        pl.when(pl.col("team_id") == pl.col("home_team_id"))
        .then(pl.col("_home").list.contains(pl.col("player_id")))
        .otherwise(pl.col("_away").list.contains(pl.col("player_id")))
        .alias("_on")
    )
    off = on.filter(~pl.col("_on"))
    assert off.height == OFF_FLOOR_EVENTS[lg]
    expected = (
        off.with_columns((pl.col("oreb") + pl.col("dreb")).alias("reb"))
        .group_by(KEY)
        .agg(pl.col(list(BOX_COUNTS)).sum())
    )
    diff = _against_box(lg, stints).filter(pl.any_horizontal([pl.col(c) != 0 for c in BOX_COUNTS]))
    j = diff.join(expected, on=KEY, how="full", coalesce=True, suffix="_e", validate="1:1")
    assert all((j[c].fill_null(0) == j[f"{c}_e"].fill_null(0)).all() for c in BOX_COUNTS)
    # without events the stints carry null counts (the earlier table, unchanged)
    assert game_stints(rows, player_dtype=PLAYER).select(BOX_COUNTS).null_count().row(0) == (stints.height,) * 15


def test_empty_and_bad_inputs() -> None:
    schema = pl.Schema(game_player_box_splits_schema(PLAYER))
    empty_rows = _rows("nba").clear()
    assert game_player_box_splits(empty_rows, _events("nba").clear(), player_dtype=PLAYER).schema == schema
    assert nba_box_events(pl.DataFrame(), pl.DataFrame()).schema == pl.Schema(box_event_schema(PLAYER))
    with pytest.raises(KeyError, match="events missing"):
        game_player_box_splits(_rows("nba"), _events("nba").drop("ast"), player_dtype=PLAYER)
    with pytest.raises(KeyError, match="events missing"):
        game_stints(_rows("nba"), player_dtype=PLAYER, events=_events("nba").drop("pf"))


def test_public_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    for module, lg, prefix in ((nba_loaders, "nba", "nba"), (wnba_loaders, "wnba", "wnba")):
        for loader, name in (("pbp", "pbp"), ("game_lineups", "game_lineups"), ("possessions", "possessions")):
            monkeypatch.setattr(module, f"load_{prefix}_stats_{loader}", lambda s, n=name, g=lg: _read(g, n))
        monkeypatch.setattr(module, f"load_{prefix}_stats_schedules", lambda s, g=lg: _read(g, "schedule"))
    for lg, fn in (("nba", nba_game_player_box_splits), ("wnba", wnba_game_player_box_splits)):
        expected = game_player_box_splits(_rows(lg), _events(lg), player_dtype=PLAYER)
        assert fn(2026).equals(expected), lg
    assert (
        len(nba_game_player_box_splits(2026, return_as_pandas=True))
        == game_player_box_splits(_rows("nba"), _events("nba"), player_dtype=PLAYER).height
    )
