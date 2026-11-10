"""BB-D32: college game stints, in-game units and on/off, on committed real captures.

Fixtures (none synthetic): the 13 MBB and 12 WBB contests of the BB-D6 corpus.
``tests/fixtures/bb_game_stints`` holds their pbp and possessions with ``poss_num`` and the shot
columns (README there); the final margins come from the BB-D6 team box and the shot counts from the
BB-D4 team box (``tests/fixtures/bb_lineup_seasons``), the pipeline's own ``ncaa_*_team_box``.

The gates are the values observed on these games; never lower one to make a change pass.
"""

from __future__ import annotations

from math import comb
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_impact import game_lineups_used, game_player_impact
from sportsdataverse._common.game_stints import game_on_off, game_stints, game_stints_schema, game_units
from sportsdataverse.mbb import mbb_loaders
from sportsdataverse.mbb.mbb_game_impact import ncaa_impact_rows
from sportsdataverse.mbb.mbb_game_stints import ncaa_stint_rows
from sportsdataverse.mbb.mbb_lineup_seasons import ncaa_lineup_count_rows
from sportsdataverse.wbb import wbb_loaders
from sportsdataverse.wbb.wbb_game_stints import wbb_game_on_off, wbb_game_stints, wbb_game_units

FIX = Path(__file__).resolve().parents[1] / "fixtures"
LEAGUE = {"mbb": "mens", "wbb": "womens"}
SHORT_LINEUP_GAMES = {"mbb": {"5722009", "5723822", "5737410", "5738251"}, "wbb": {"5722076", "5723697", "5737290"}}
#: Observed D1 team-games, and those on a game whose lineup is whole.
TEAM_GAMES = {"mbb": (25, 18), "wbb": (24, 18)}
#: The men's D1-vs-non-D1 contest: the D1 home side (ESPN 151) is its only team with rows.
NON_D1 = ("5722009", 151)
PLAYER = pl.Utf8()
KEY = ["game_id", "team_id", "player_id"]


def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / "bb_game_stints" / f"ncaa_{lg}_{name}_2025.parquet")


def _stints(lg: str) -> pl.DataFrame:
    return game_stints(
        ncaa_stint_rows(_read(lg, "pbp"), _read(lg, "possessions"), league=LEAGUE[lg]), player_dtype=PLAYER
    )


def _rows(lg: str) -> pl.DataFrame:
    return ncaa_impact_rows(_read(lg, "pbp"), _read(lg, "possessions"), league=LEAGUE[lg])


def _units(lg: str) -> pl.DataFrame:
    return game_units(_rows(lg), ncaa_lineup_count_rows(_read(lg, "pbp")), player_dtype=PLAYER)


def _team_box(lg: str, folder: str) -> pl.DataFrame:
    box = pl.read_parquet(FIX / folder / f"ncaa_{lg}_team_box_2025.parquet")
    return box.with_columns(
        pl.col("contest_id").alias("game_id"), pl.col("team_espn_team_id").cast(pl.Int64).alias("team_id")
    )


def _margins(lg: str) -> pl.DataFrame:
    box = _team_box(lg, "bb_game_impact").select("game_id", "team_id", "pts")
    opp = box.rename({"team_id": "opp_team_id", "pts": "opp_pts"})
    return (
        box.join(opp, on="game_id")
        .filter(pl.col("team_id").ne_missing(pl.col("opp_team_id")) & pl.col("team_id").is_not_null())
        .select("game_id", "team_id", (pl.col("pts") - pl.col("opp_pts")).cast(pl.Int64).alias("margin"))
    )


def _clean(frame: pl.DataFrame, lg: str) -> pl.DataFrame:
    return frame.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_stint_plus_minus_sums_to_five_times_the_margin(lg: str) -> None:
    stints = _stints(lg)
    assert stints.schema == pl.Schema(game_stints_schema(PLAYER))
    team = stints.group_by("game_id", "team_id").agg(pl.col("plus_minus", "seconds").sum())
    margins = _margins(lg)
    assert team.schema["team_id"] == margins.schema["team_id"]
    team = team.join(margins, on=["game_id", "team_id"], how="inner", validate="1:1")
    n, n_clean = TEAM_GAMES[lg]
    assert team.height == n
    clean = _clean(team, lg)
    assert clean.height == n_clean
    assert (clean["plus_minus"] == 5 * clean["margin"]).all()
    overtimes = (clean["seconds"] / 5 - 2400) / 300
    assert ((overtimes - overtimes.round(0)).abs() < 1e-6).all() and (overtimes > -1e-6).all()


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_stints_add_up_to_the_impact_box(lg: str) -> None:
    stints = _stints(lg)
    impact = game_player_impact(_rows(lg), player_dtype=PLAYER)
    summed = stints.group_by(KEY).agg(pl.col("seconds", "pts", "opp_pts", "poss", "opp_poss").sum())
    joined = impact.join(summed, on=KEY, how="full", coalesce=True, suffix="_s", validate="1:1")
    assert joined.height == impact.height == summed.height
    # A possession's lineup is the one at its first event, so even the possessions are exact.
    for c in ("pts", "opp_pts", "poss", "opp_poss"):
        assert (joined[c] == joined[f"{c}_s"]).all(), c
    assert ((joined["seconds"] - joined["seconds_s"]).abs() < 1e-6).all()
    assert (stints["clock_in"] >= stints["clock_out"]).all()
    assert ((stints["clock_in"] - stints["clock_out"] - stints["seconds"]).abs() < 1e-6).all()


def test_non_d1_opponent_gets_no_stints_but_its_points_count() -> None:
    game, d1 = NON_D1
    stints = _stints("mbb").filter(pl.col("game_id") == game)
    assert set(stints["team_id"].unique().to_list()) == {d1}
    assert stints["opp_team_id"].null_count() == stints.height
    margin = _margins("mbb").filter((pl.col("game_id") == game) & (pl.col("team_id") == d1))["margin"].item()
    assert stints["plus_minus"].sum() == 5 * margin


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_units_roll_up_and_match_the_team_box_counts(lg: str) -> None:
    rows = _rows(lg)
    units = _units(lg)
    totals = ["seconds", "poss", "opp_poss", "pts", "opp_pts", "plus_minus"]
    one = units.filter(pl.col("n_players") == 1).rename({"player_1": "player_id"})
    impact = game_player_impact(rows, player_dtype=PLAYER)
    j = impact.join(one, on=KEY, how="full", coalesce=True, suffix="_u", validate="1:1")
    assert j.height == impact.height
    assert all((j[c] - j[f"{c}_u"]).abs().max() < 1e-6 for c in totals)
    used = game_lineups_used(rows, player_dtype=PLAYER)
    five = units.filter(pl.col("n_players") == 5)
    j5 = used.join(five, on=["game_id", "team_id", "lineup_id"], how="full", coalesce=True, suffix="_u", validate="1:1")
    assert j5.height == used.height == five.height
    assert all((j5[c] - j5[f"{c}_u"]).abs().max() < 1e-6 for c in totals)
    by_k = units.group_by("game_id", "team_id", "n_players").agg(pl.col("plus_minus").sum())
    by_k = _clean(by_k.join(_margins(lg), on=["game_id", "team_id"]), lg)
    assert by_k.height == 5 * TEAM_GAMES[lg][1]
    assert (
        by_k["plus_minus"]
        == by_k["n_players"].map_elements(lambda k: comb(5, k), return_dtype=pl.Int64) * by_k["margin"]
    ).all()
    # Summed over a team's five-man units, the shot counts are the NCAA team box's.
    team = _clean(five, lg).group_by("game_id", "team_id").agg(pl.col("efg_pct_n", "fg3_pct_n", "rim_pct_n").sum())
    box = _team_box(lg, "bb_lineup_seasons").select("game_id", "team_id", "fga", "tpa", "rima")
    team = team.join(box, on=["game_id", "team_id"], how="inner", validate="1:1")
    assert team.height == TEAM_GAMES[lg][1]
    assert (team["efg_pct_n"] == team["fga"]).all()
    assert (team["fg3_pct_n"] == team["tpa"]).all()
    assert (team["rim_pct_n"] == team["rima"]).all()
    shot = units.filter(pl.col("rim_rate").is_not_null())
    assert ((shot["rim_rate"] + shot["mid_rate"] + shot["fg3_rate"] - 1).abs() < 1e-9).all()


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_on_off_splits_the_team_game(lg: str) -> None:
    onoff = game_on_off(_rows(lg), player_dtype=PLAYER)
    team = onoff.with_columns((pl.col("plus_minus_on") + pl.col("plus_minus_off")).alias("pm"))
    team = _clean(team.join(_margins(lg), on=["game_id", "team_id"]), lg)
    assert team.height > 0 and (team["pm"] == team["margin"]).all()
    total = onoff.select((pl.col("poss_on") + pl.col("poss_off")).alias("p"), "game_id", "team_id")
    assert (total.group_by("game_id", "team_id").agg(pl.col("p").n_unique())["p"] == 1).all()


def test_womens_public_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wbb_loaders, "load_ncaa_wbb_pbp", lambda seasons: _read("wbb", "pbp"))
    monkeypatch.setattr(wbb_loaders, "load_ncaa_wbb_possessions", lambda seasons: _read("wbb", "possessions"))
    monkeypatch.setattr(mbb_loaders, "load_ncaa_mbb_pbp", lambda seasons: pytest.fail("read the men's pbp"))
    assert wbb_game_stints(2025).equals(_stints("wbb"))
    assert wbb_game_units(2025).equals(_units("wbb"))
    assert wbb_game_on_off(2025).equals(game_on_off(_rows("wbb"), player_dtype=PLAYER))


def test_league_is_checked() -> None:
    with pytest.raises(ValueError, match="league"):
        ncaa_stint_rows(_read("mbb", "pbp"), _read("mbb", "possessions"), league="coed")
