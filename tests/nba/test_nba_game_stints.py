"""BB-D32: NBA / WNBA game stints, in-game units and on/off, on committed real captures.

Fixtures (none synthetic): the 16 NBA (2025-26) and 16 WNBA (2026) games of the BB-D6 corpus.
``tests/fixtures/bb_game_stints`` holds their pbp and possessions with the extra columns the stints
and shot zones read (README there); the lineups, schedule and box score come from
``tests/fixtures/bb_game_impact``.

The gates are the values observed on these games; never lower one to make a change pass. The
headline gate is the roadmap's: each team's stint +/- sums to five times its final margin, on every
game whose source lineup lists five players throughout.
"""

from __future__ import annotations

from math import comb
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_impact import game_lineups_used, game_player_impact
from sportsdataverse._common.game_stints import (
    game_on_off,
    game_on_off_schema,
    game_stints,
    game_stints_schema,
    game_units,
    game_units_schema,
)
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_game_impact import nba_impact_rows
from sportsdataverse.nba.nba_game_stints import (
    nba_box_events,
    nba_game_on_off,
    nba_game_stints,
    nba_game_units,
    nba_stint_rows,
)
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows

FIX = Path(__file__).resolve().parents[1] / "fixtures"
LEAGUES = {"nba": (2025, "00", 720.0), "wnba": (2026, "10", 600.0)}
#: The BB-D6 games whose source lineup lists fewer than five players somewhere.
SHORT_LINEUP_GAMES = {"nba": {"0022500559", "0022500632"}, "wnba": set()}
#: Observed: player-games whose stint possessions (poss, opp_poss) equal the impact box's. A
#: possession counts on the lineup at the action that starts it, which is the possession's own
#: lineup on 99.5% of these NBA possessions and on every WNBA one.
OBSERVED_POSS_EQUAL = {"nba": (312, 330), "wnba": (309, 309)}
PLAYER = pl.Int64()
KEY = ["game_id", "team_id", "player_id"]


def _read(lg: str, name: str) -> pl.DataFrame:
    folder = "bb_game_stints" if name in ("pbp", "possessions") else "bb_game_impact"
    return pl.read_parquet(FIX / folder / f"{lg}_{name}_{LEAGUES[lg][0]}.parquet")


def _inputs(lg: str) -> tuple[pl.DataFrame, ...]:
    return _read(lg, "pbp"), _read(lg, "game_lineups"), _read(lg, "possessions"), _read(lg, "schedule")


def _stints(lg: str) -> pl.DataFrame:
    pbp, _, _, schedule = inputs = _inputs(lg)
    rows = nba_stint_rows(*inputs, league_id=LEAGUES[lg][1])
    return game_stints(rows, player_dtype=PLAYER, events=nba_box_events(pbp, schedule))


def _rows(lg: str) -> pl.DataFrame:
    return nba_impact_rows(*_inputs(lg), league_id=LEAGUES[lg][1])


def _units(lg: str) -> pl.DataFrame:
    pbp, _, poss, schedule = _inputs(lg)
    return game_units(_rows(lg), nba_lineup_count_rows(poss, schedule, pbp=pbp), player_dtype=PLAYER)


def _margins(lg: str) -> pl.DataFrame:
    s = _read(lg, "schedule")
    return pl.concat(
        [
            s.select(
                "game_id",
                pl.col(f"{us}_team_id").alias("team_id"),
                (pl.col(f"{us}_pts") - pl.col(f"{them}_pts")).alias("margin"),
            )
            for us, them in (("home", "away"), ("away", "home"))
        ]
    )


def _clean(frame: pl.DataFrame, lg: str) -> pl.DataFrame:
    return frame.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_stint_plus_minus_sums_to_five_times_the_margin(lg: str) -> None:
    stints = _stints(lg)
    assert stints.schema == pl.Schema(game_stints_schema(PLAYER))
    team = stints.group_by("game_id", "team_id").agg(pl.col("plus_minus", "seconds").sum())
    margins = _margins(lg)
    assert team.schema["team_id"] == margins.schema["team_id"]
    team = team.join(margins, on=["game_id", "team_id"], how="inner", validate="1:1")
    assert team.height == 32
    clean = _clean(team, lg)
    assert clean.height == 32 - 2 * len(SHORT_LINEUP_GAMES[lg])
    assert (clean["plus_minus"] == 5 * clean["margin"]).all()
    # The stints tile each period: five players' seconds are five times the game length.
    overtimes = (clean["seconds"] / 5 - 4 * LEAGUES[lg][2]) / 300
    assert ((overtimes - overtimes.round(0)).abs() < 1e-6).all() and (overtimes > -1e-6).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_stints_add_up_to_the_impact_box(lg: str) -> None:
    stints = _stints(lg)
    impact = game_player_impact(_rows(lg), player_dtype=PLAYER)
    summed = stints.group_by(KEY).agg(pl.col("seconds", "pts", "opp_pts", "plus_minus", "poss", "opp_poss").sum())
    joined = impact.join(summed, on=KEY, how="full", coalesce=True, suffix="_s", validate="1:1")
    assert joined.height == impact.height == summed.height
    # Points, the free-throw rule and the clock are the impact box's on every player-game.
    for c in ("pts", "opp_pts", "plus_minus"):
        assert (joined[c] == joined[f"{c}_s"]).all(), c
    assert ((joined["seconds"] - joined["seconds_s"]).abs() < 1e-6).all()
    both = (joined["poss"] == joined["poss_s"]) & (joined["opp_poss"] == joined["opp_poss_s"])
    floor, n = OBSERVED_POSS_EQUAL[lg]
    assert joined.height == n and int(both.sum()) >= floor
    # Per team the possessions are exact: each one counts once, on five players.
    team = joined.group_by("game_id", "team_id").agg(pl.col("poss", "poss_s").sum())
    assert (_clean(team, lg)["poss"] == _clean(team, lg)["poss_s"]).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_stints_are_ordered_runs_within_a_period(lg: str) -> None:
    stints = _stints(lg)
    assert (stints["clock_in"] >= stints["clock_out"]).all()
    assert ((stints["clock_in"] - stints["clock_out"] - stints["seconds"]).abs() < 1e-6).all()
    numbered = stints.group_by(KEY).agg((pl.col("stint_number").sort() == pl.int_range(1, pl.len() + 1)).all())
    assert numbered["stint_number"].all()
    # Stints follow each other in time: the next one starts no earlier in the game.
    period_len = pl.when(pl.col("period") <= 4).then(LEAGUES[lg][2]).otherwise(300.0)
    elapsed = pl.col("period") * 1e4 - pl.col("clock_in") / period_len
    nxt = stints.sort([*KEY, "stint_number"]).with_columns(elapsed.alias("_t"))
    assert (nxt.select(pl.col("_t").diff().over(KEY).drop_nulls() > 0).to_series()).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_units_roll_up_to_the_impact_box_and_lineups_used(lg: str) -> None:
    rows = _rows(lg)
    units = _units(lg)
    assert units.schema == pl.Schema(game_units_schema(PLAYER))
    assert sorted(units["n_players"].unique().to_list()) == [1, 2, 3, 4, 5]
    totals = ["seconds", "poss", "opp_poss", "pts", "opp_pts", "plus_minus"]
    one = units.filter(pl.col("n_players") == 1).rename({"player_1": "player_id"})
    impact = game_player_impact(rows, player_dtype=PLAYER)
    j = impact.join(one, on=KEY, how="full", coalesce=True, suffix="_u", validate="1:1")
    assert j.height == impact.height == one.height
    assert all((j[c] - j[f"{c}_u"]).abs().max() < 1e-6 for c in totals)
    five = units.filter(pl.col("n_players") == 5)
    used = game_lineups_used(rows, player_dtype=PLAYER)
    j5 = used.join(five, on=["game_id", "team_id", "lineup_id"], how="full", coalesce=True, suffix="_u", validate="1:1")
    assert j5.height == used.height == five.height
    assert all((j5[c] - j5[f"{c}_u"]).abs().max() < 1e-6 for c in totals)
    # Each player of a five is in C(4, k - 1) of its k-man subsets: the k-man +/- of a team adds
    # up to C(5, k) times its margin.
    by_k = units.group_by("game_id", "team_id", "n_players").agg(pl.col("plus_minus").sum())
    by_k = _clean(by_k.join(_margins(lg), on=["game_id", "team_id"]), lg)
    assert (
        by_k["plus_minus"]
        == by_k["n_players"].map_elements(lambda k: comb(5, k), return_dtype=pl.Int64) * by_k["margin"]
    ).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_unit_four_factors_and_shot_zones(lg: str) -> None:
    units = _units(lg)
    poss = _read(lg, "possessions")
    five = _clean(units.filter(pl.col("n_players") == 5), lg)
    team = five.group_by("game_id", "team_id").agg(pl.col("efg_pct_n", "tov_pct_n", "fg3_pct_n").sum())
    box = poss.group_by("game_id", pl.col("offense_team_id").alias("team_id")).agg(
        (pl.col("fg2a") + pl.col("fg3a")).sum().alias("fga"), pl.len().alias("n"), pl.col("fg3a").sum()
    )
    team = team.join(box, on=["game_id", "team_id"], how="inner", validate="1:1")
    assert team.height == 32 - 2 * len(SHORT_LINEUP_GAMES[lg])
    assert (team["efg_pct_n"] == team["fga"]).all() and (team["fg3_pct_n"] == team["fg3a"]).all()
    assert (team["tov_pct_n"] == team["n"]).all()
    shot = units.filter(pl.col("efg_pct_n") > 0)
    for pre in ("", "opp_"):
        zones = shot.filter(pl.col(f"{pre}rim_rate").is_not_null())
        total = zones[f"{pre}rim_rate"] + zones[f"{pre}mid_rate"] + zones[f"{pre}fg3_rate"]
        assert zones.height > 0 and ((total - 1).abs() < 1e-9).all()
    ok = units.filter(pl.col("efg_pct").is_not_null())
    assert ((ok["efg_pct"] >= 0) & (ok["efg_pct"] <= 1.5)).all()
    assert ((units["off_rtg"] - 100 * units["pts"] / units["poss"]).abs().max() or 0) < 1e-9


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_on_off_splits_the_team_game(lg: str) -> None:
    rows = _rows(lg)
    onoff = game_on_off(rows, player_dtype=PLAYER)
    assert onoff.schema == pl.Schema(game_on_off_schema(PLAYER))
    impact = game_player_impact(rows, player_dtype=PLAYER)
    j = impact.join(onoff, on=KEY, how="inner", validate="1:1")
    assert j.height == impact.height
    assert (j["plus_minus"] == j["plus_minus_on"]).all() and (j["poss"] == j["poss_on"]).all()
    team = (j["plus_minus_on"] + j["plus_minus_off"]).to_frame("pm").with_columns(j.select("game_id", "team_id"))
    team = _clean(team.join(_margins(lg), on=["game_id", "team_id"]), lg)
    assert (team["pm"] == team["margin"]).all()
    both = onoff.filter(pl.col("net_rtg_on").is_not_null() & pl.col("net_rtg_off").is_not_null())
    assert ((both["net_rtg_diff"] - (both["net_rtg_on"] - both["net_rtg_off"])).abs() < 1e-9).all()
    thinner = onoff.select(pl.min_horizontal("net_rtg_on_n", "net_rtg_off_n")).to_series()
    assert (onoff["net_rtg_diff_n"] == thinner).all()


def test_empty_inputs_return_the_schemas() -> None:
    empty = nba_stint_rows(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
    assert game_stints(empty, player_dtype=PLAYER).schema == pl.Schema(game_stints_schema(PLAYER))
    rows = _rows("nba").clear()
    counts = nba_lineup_count_rows(pl.DataFrame(), pl.DataFrame(), pbp=pl.DataFrame())
    assert game_units(rows, counts, player_dtype=PLAYER).schema == pl.Schema(game_units_schema(PLAYER))
    assert game_on_off(rows, player_dtype=PLAYER).schema == pl.Schema(game_on_off_schema(PLAYER))
    with pytest.raises(ValueError, match="league_id"):
        nba_stint_rows(*_inputs("nba"), league_id="20")


def test_public_builders_read_the_start_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(name: str):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            assert list(seasons) == [2025], f"{name}: called with {seasons}"
            return _read("nba", name)

        return load

    for loader, name in (
        ("load_nba_stats_pbp", "pbp"),
        ("load_nba_stats_game_lineups", "game_lineups"),
        ("load_nba_stats_possessions", "possessions"),
        ("load_nba_stats_schedules", "schedule"),
    ):
        monkeypatch.setattr(nba_loaders, loader, fixture(name))
    assert nba_game_stints(2026).equals(_stints("nba"))
    assert nba_game_units(2026).equals(_units("nba"))
    assert nba_game_on_off(2026).equals(game_on_off(_rows("nba"), player_dtype=PLAYER))
    assert len(nba_game_stints(2026, return_as_pandas=True)) == _stints("nba").height
