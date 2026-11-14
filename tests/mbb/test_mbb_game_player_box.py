"""GOP Q7 / Q26 / Q30: college player box per game, period, garbage time and stint, on real captures.

Fixtures (none synthetic): the 13 MBB and 12 WBB contests of the BB-D6 corpus.
``tests/fixtures/bb_game_stints`` holds their pbp, possessions and the pipeline's own
``ncaa_*_player_box`` (README there).

The oracle is the stats.ncaa.org player box. The gates are the values observed on these games;
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
from sportsdataverse.mbb import mbb_loaders
from sportsdataverse.mbb.mbb_game_stints import mbb_game_player_box_splits, ncaa_box_events, ncaa_stint_rows
from sportsdataverse.mbb.mbb_situational import ncaa_team_game_counts
from sportsdataverse.wbb import wbb_loaders
from sportsdataverse.wbb.wbb_game_stints import wbb_game_player_box_splits

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_game_stints"
LEAGUE = {"mbb": "mens", "wbb": "womens"}
SHORT_LINEUP_GAMES = {"mbb": {"5722009", "5723822", "5737410", "5738251"}, "wbb": {"5722076", "5723697", "5737290"}}
PLAYER = pl.Utf8()
KEY = ["game_id", "team_id", "player_id"]
#: player box column -> ours (``reb`` is ours only: the box has ``orb`` and ``drb``).
BOX = {
    "pts": "points", "fgm": "fgm", "fga": "fga", "tpm": "fg3m", "tpa": "fg3a", "ftm": "ftm", "fta": "fta",
    "orb": "oreb", "drb": "dreb", "ast": "ast", "stl": "stl", "blk": "blk", "tov": "tov", "pf": "pf",
}  # fmt: skip
COUNTS = list(BOX.values())
#: Observed: D1 player box rows with a player id, D1 rows without one (the release could not
#: identify the player, so neither can its pbp), and player-games with garbage minutes.
OBSERVED = {"mbb": (234, 5, 35), "wbb": (243, 3, 44)}
#: Observed: events credited to a player his side's lineup does not list at that event.
OFF_FLOOR_EVENTS = {"mbb": 75, "wbb": 61}


def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"ncaa_{lg}_{name}_2025.parquet")


def _rows(lg: str) -> pl.DataFrame:
    return ncaa_stint_rows(_read(lg, "pbp"), _read(lg, "possessions"), league=LEAGUE[lg])


def _events(lg: str) -> pl.DataFrame:
    return ncaa_box_events(_read(lg, "pbp"))


def _box(lg: str) -> pl.DataFrame:
    return _read(lg, "player_box").select(
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("team_espn_team_id").cast(pl.Int64).alias("team_id"),
        "player_id",
        *[pl.col(s).cast(pl.Int64).alias(c) for s, c in BOX.items()],
    )


def _against_box(lg: str, ours: pl.DataFrame) -> pl.DataFrame:
    box = _box(lg).drop_nulls(["team_id", "player_id"])
    summed = ours.group_by(KEY).agg(pl.col(COUNTS).sum())
    assert summed.schema["player_id"] == box.schema["player_id"] and summed.schema["team_id"] == box.schema["team_id"]
    j = box.join(summed, on=KEY, how="full", coalesce=True, suffix="_o", validate="1:1")
    return j.select(*KEY, *[(pl.col(c).fill_null(0) - pl.col(f"{c}_o").fill_null(0)).alias(c) for c in COUNTS])


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_events_are_the_ncaa_box(lg: str) -> None:
    events = _events(lg)
    assert events.schema == pl.Schema(box_event_schema(PLAYER))
    diff = _against_box(lg, events)
    assert diff.height == OBSERVED[lg][0]
    assert (diff.select(COUNTS).to_numpy() == 0).all()
    # the residual: D1 box rows with no player id, whose events carry no player id either
    box = _box(lg).drop_nulls("team_id")
    assert box.filter(pl.col("player_id").is_null()).height == OBSERVED[lg][1]


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_splits_add_up_to_the_box(lg: str) -> None:
    rows = _rows(lg)
    splits = game_player_box_splits(rows, _events(lg), player_dtype=PLAYER)
    assert splits.schema == pl.Schema(game_player_box_splits_schema(PLAYER))
    for fam in ("all", "period", "garbage"):
        assert (_against_box(lg, splits.filter(family=fam)).select(COUNTS).to_numpy() == 0).all(), fam
    whole = splits.filter(family="all")
    for fam in ("period", "garbage"):
        parts = splits.filter(family=fam).group_by(KEY).agg(pl.col("seconds").sum())
        j = whole.join(parts, on=KEY, how="full", coalesce=True, suffix="_p", validate="1:1")
        assert ((j["seconds"] - j["seconds_p"]).abs() < 1e-6).all(), fam
    stints = game_stints(rows, player_dtype=PLAYER).group_by(KEY).agg(pl.col("seconds").sum())
    j = whole.join(stints, on=KEY, how="left", suffix="_s", validate="1:1")
    assert ((j["seconds"] - j["seconds_s"].fill_null(0)).abs() < 1e-6).all()
    assert splits.filter(family="garbage", split="garbage_time").filter(pl.col("seconds") > 0).height == OBSERVED[lg][2]
    periods = sorted(splits.filter(family="period")["split"].unique().to_list())
    assert periods == (["p1", "p2", "p3"] if lg == "mbb" else ["p1", "p2", "p3", "p4", "p5"])
    at_end = (
        splits.filter(family="period").group_by("game_id", "team_id", "period").agg(pl.col("on_floor_at_end").sum())
    )
    clean = at_end.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))
    assert clean.height > 0 and (clean["on_floor_at_end"] == 5).all()


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_garbage_time_is_the_situational_one(lg: str) -> None:
    """Per team, the garbage rows' shots are situational_team_games' garbage counts, less unidentified players."""
    pbp = _read(lg, "pbp")
    rows = _rows(lg)
    assert rows["is_garbage_time"].null_count() == 0
    splits = game_player_box_splits(rows, _events(lg), player_dtype=PLAYER).filter(family="garbage")
    shots = ["fga", "fgm", "fg3a", "fg3m", "fta", "ftm"]
    ours = splits.group_by("game_id", "team_id", "split").agg(pl.col(shots).sum())
    theirs = ncaa_team_game_counts(pbp).filter(family="garbage").select("game_id", "team_id", "split", *shots)
    nobody = ncaa_team_game_counts(pbp.filter(pl.col("player_1_id").is_null())).filter(family="garbage")
    j = theirs.join(ours, on=["game_id", "team_id", "split"], how="left", suffix="_o", validate="1:1").join(
        nobody.select("game_id", "team_id", "split", *shots),
        on=["game_id", "team_id", "split"],
        how="left",
        suffix="_x",
        validate="1:1",
    )
    assert j.height > 0
    for c in shots:
        assert (j[c] == j[f"{c}_o"].fill_null(0) + j[f"{c}_x"].fill_null(0)).all(), c


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_stint_box_is_the_box_less_off_floor_events(lg: str) -> None:
    rows, events = _rows(lg), _events(lg)
    stints = game_stints(rows, player_dtype=PLAYER, events=events)
    assert (stints["reb"] == stints["oreb"] + stints["dreb"]).all()
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
    expected = off.group_by(KEY).agg(pl.col(COUNTS).sum())
    diff = _against_box(lg, stints).filter(pl.any_horizontal([pl.col(c) != 0 for c in COUNTS]))
    j = diff.join(expected, on=KEY, how="full", coalesce=True, suffix="_e", validate="1:1")
    assert j.height > 0
    assert all((j[c].fill_null(0) == j[f"{c}_e"].fill_null(0)).all() for c in COUNTS)


def test_empty_inputs() -> None:
    assert ncaa_box_events(pl.DataFrame()).schema == pl.Schema(box_event_schema(PLAYER))
    empty = game_player_box_splits(_rows("mbb").clear(), _events("mbb"), player_dtype=PLAYER)
    assert empty.schema == pl.Schema(game_player_box_splits_schema(PLAYER)) and empty.height == 0
    assert set(BOX_COUNTS) - set(COUNTS) == {"reb"}


def test_public_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mbb_loaders, "load_ncaa_mbb_pbp", lambda seasons: _read("mbb", "pbp"))
    monkeypatch.setattr(mbb_loaders, "load_ncaa_mbb_possessions", lambda seasons: _read("mbb", "possessions"))
    monkeypatch.setattr(wbb_loaders, "load_ncaa_wbb_pbp", lambda seasons: _read("wbb", "pbp"))
    monkeypatch.setattr(wbb_loaders, "load_ncaa_wbb_possessions", lambda seasons: _read("wbb", "possessions"))
    for lg, fn in (("mbb", mbb_game_player_box_splits), ("wbb", wbb_game_player_box_splits)):
        assert fn(2025).equals(game_player_box_splits(_rows(lg), _events(lg), player_dtype=PLAYER)), lg
