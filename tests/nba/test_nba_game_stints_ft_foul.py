"""A free throw's box counts sit on the lineup at its foul, as its points do (real games, no edits).

``tests/fixtures/bb_stint_ft_foul`` holds two 2025-26 NBA games:

* 0022500024 (LAL at MEM): at 0:07.5 of the 4th the feed records LaRavia FOR Smart and Knecht FOR
  Doncic between Reaves' two free throws, and the lineup at his second (order 491) no longer lists
  him. Counted on that row's own lineup, the free throw fell out of his stints (20 of his 21).
* 0022400149 (2024-25): Durant's and-one at 11:33 of the 2nd, the made shot (order 151) and the free
  throw (152) one row before their shooting foul (153). The game's most recent foul before the free
  throw is at 0:26.3 of the 1st (140), which is not its: clock has run in the 2nd, so the free throw
  keeps its own row and lineup, points and box alike.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse._common.game_impact import _SLOTS, credit_free_throws_to_foul_lineup
from sportsdataverse._common.game_stints import game_stints
from sportsdataverse.nba.nba_game_impact import _actions
from sportsdataverse.nba.nba_game_stints import nba_box_events, nba_stint_rows

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_stint_ft_foul"
PLAYER = pl.Int64()
SUBS_BETWEEN, FOUL_AFTER = "0022500024", "0022400149"
SEASON = {SUBS_BETWEEN: 2025, FOUL_AFTER: 2024}  # the files are named by the loader season (start year)
REAVES = 1630559


def _read(name: str, game: str) -> pl.DataFrame:
    df = pl.read_parquet(FIX / f"nba_{name}_{SEASON[game]}.parquet")
    return df.filter(pl.col("game_id").cast(pl.Utf8) == game)


def _rows(game: str) -> pl.DataFrame:
    return nba_stint_rows(
        _read("pbp", game), _read("game_lineups", game), _read("possessions", game), _read("schedules", game)
    )


def _stints(game: str, drop_foul_order: bool = False) -> pl.DataFrame:
    rows = _rows(game)
    if drop_foul_order:  # the rows as they were before ft_foul_order existed
        rows = rows.drop("ft_foul_order")
    events = nba_box_events(_read("pbp", game), _read("schedules", game))
    return game_stints(rows, player_dtype=PLAYER, events=events)


def _per_player(stints: pl.DataFrame) -> pl.DataFrame:
    return stints.group_by("player_id").agg(pl.col("points", "ftm", "fta").sum())


def test_free_throw_counts_follow_the_points_onto_the_lineup_at_the_foul() -> None:
    box = _read("player_box", SUBS_BETWEEN).select(
        pl.col("person_id").cast(PLAYER).alias("player_id"),
        pl.col("points").cast(pl.Int64).alias("box_points"),
        pl.col("free_throws_made").cast(pl.Int64).alias("box_ftm"),
        pl.col("free_throws_attempted").cast(pl.Int64).alias("box_fta"),
    )
    stints = _stints(SUBS_BETWEEN)
    got = _per_player(stints).join(box, on="player_id", how="inner", validate="1:1")
    assert got.height == stints["player_id"].n_unique() == 19  # every player with a stint is in the box
    # every player's stints add up to stats.nba.com's box score, free throws included
    for mine, theirs in (("points", "box_points"), ("ftm", "box_ftm"), ("fta", "box_fta")):
        assert (got[mine] == got[theirs]).all(), got.filter(pl.col(mine) != pl.col(theirs))
    assert got.filter(pl.col("player_id") == REAVES).select("points", "ftm").row(0) == (21, 9)


def test_the_free_throw_after_the_substitutions_carries_its_foul_order() -> None:
    pbp, rows = _read("pbp", SUBS_BETWEEN), _rows(SUBS_BETWEEN)
    foul = rows.filter(pl.col("order") == 491)["ft_foul_order"][0]
    assert foul is not None and foul < 487  # the foul precedes the two substitutions
    assert pbp.filter(pl.col("order_index") == foul)["is_foul"][0] is True
    # only free-throw rows carry one
    is_ft = pbp.select(pl.col("order_index").alias("order"), "is_free_throw")
    marked = rows.filter(pl.col("ft_foul_order").is_not_null()).join(is_ft, on="order", how="left", validate="1:1")
    assert marked.height > 0 and (marked["is_free_throw"] == True).all()  # noqa: E712 - explicit bool mask


def test_without_the_foul_order_the_free_throw_falls_out_of_his_stints() -> None:
    """The regression this fixes: the old crediting (each free throw's box on its own row's lineup)."""
    old = _per_player(_stints(SUBS_BETWEEN, drop_foul_order=True)).filter(pl.col("player_id") == REAVES)
    assert old.select("points", "ftm").row(0) == (20, 8)


def test_a_foul_from_an_earlier_period_is_not_a_free_throws_once_clock_has_run() -> None:
    pbp, rows = _read("pbp", FOUL_AFTER), _rows(FOUL_AFTER)
    assert pbp.filter(pl.col("order_index") == 153)["is_foul"][0] is True  # its foul, one row late
    ft = rows.filter(pl.col("order") == 152)
    assert ft.select("period", "ft_foul_order").row(0) == (2, None) and ft["clock"][0] < 720
    assert ft["home_pts"][0] + ft["away_pts"][0] == 1  # its point stays on its own row


def test_the_lineup_twin_keeps_its_own_lineup_once_clock_has_run() -> None:
    games = _read("schedules", FOUL_AFTER).select(
        pl.col("game_id").cast(pl.Utf8), pl.col("home_team_id").cast(pl.Int64), pl.col("away_team_id").cast(pl.Int64)
    )
    actions = _actions(_read("pbp", FOUL_AFTER), _read("game_lineups", FOUL_AFTER), games, 720.0)
    flags = {"is_free_throw": pl.col("is_free_throw").eq(True), "is_foul": pl.col("is_foul").eq(True)}
    lineup = lambda df, o: df.filter(pl.col("order_index") == o).select(_SLOTS).row(0)  # noqa: E731
    own, at_140 = lineup(actions, 152), lineup(actions, 140)
    assert own != at_140  # the lineups differ, so the rule is observable on this game
    assert lineup(credit_free_throws_to_foul_lineup(actions, **flags, period="period"), 152) == own
    # the game-wide rule (no period) gives it the 1st quarter's lineup
    assert lineup(credit_free_throws_to_foul_lineup(actions, **flags), 152) == at_140
