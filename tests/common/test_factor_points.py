"""BB-D42: four-factor points on ``team_season_rates`` and ``lineup_seasons``, on real captures.

Fixtures (none synthetic; README in each directory): ``tests/fixtures/bb_team_season_rates`` (the
2025-26 NBA and 2025 WNBA possessions and schedules, and every 2024-25 game of a Big Ten team from
the NCAA possession and team-box releases) and ``tests/fixtures/bb_lineup_seasons``. The gate: on
every row the eight parts add up to ``net_rtg`` (the roadmap allows 0.1; they hold to float
precision), and the parts are the per-game accounting's prices summed over the season.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.factor_points import FACTOR_NET_POINTS, FACTOR_POINTS, FACTOR_POINTS_SCHEMA
from sportsdataverse._common.lineup_seasons import lineup_seasons
from sportsdataverse._common.team_season_rates import team_game_four_factors, team_season_rates
from sportsdataverse.mbb.mbb_game_impact import ncaa_impact_rows
from sportsdataverse.mbb.mbb_lineup_seasons import ncaa_lineup_count_rows
from sportsdataverse.nba.nba_game_impact import nba_impact_rows
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
from tests.common.test_team_game_four_factors import _inputs

LINEUPS = Path(__file__).resolve().parents[1] / "fixtures" / "bb_lineup_seasons"
LEAGUES = ("nba", "wnba", "mbb", "wbb")
OFF = [c for c in FACTOR_POINTS if c.startswith("off_")]
DEF = [c for c in FACTOR_POINTS if c.startswith("def_")]


@cache
def _rates(lg: str) -> pl.DataFrame:
    rows, counts = _inputs(lg)
    return team_season_rates(rows, minutes_per_game=48 if lg == "nba" else 40, counts=counts, level=lg)


@cache
def _units(lg: str) -> pl.DataFrame:
    def read(name: str) -> pl.DataFrame:
        prefix = "ncaa_" if lg in ("mbb", "wbb") else ""
        return pl.read_parquet(LINEUPS / f"{prefix}{lg}_{name}_2025.parquet")

    if lg in ("nba", "wnba"):
        poss, schedule = read("possessions"), read("schedule")
        rows = nba_impact_rows(
            read("pbp"), read("game_lineups"), poss, schedule, league_id="00" if lg == "nba" else "10"
        )
        return lineup_seasons(rows, nba_lineup_count_rows(poss, schedule), player_dtype=pl.Int64(), min_poss=400)
    league = "mens" if lg == "mbb" else "womens"
    pbp = read("pbp")
    rows = ncaa_impact_rows(pbp, read("possessions"), league=league)
    counts = ncaa_lineup_count_rows(pbp, league=league)
    return lineup_seasons(rows, counts, player_dtype=pl.Utf8(), min_poss=20, zones=True)


def _assert_parts_add_up(frame: pl.DataFrame) -> None:
    priced = frame.filter(pl.col("off_efg_pts").is_not_null() & pl.col("def_efg_pts").is_not_null())
    assert priced.height > 0
    gap = priced.select((pl.sum_horizontal(FACTOR_POINTS) - pl.col("net_rtg")).abs().max()).item()
    assert gap < 1e-9  # the roadmap's gate is 0.1
    nets = priced.select((pl.sum_horizontal(FACTOR_NET_POINTS) - pl.col("net_rtg")).abs().max()).item()
    assert nets < 1e-9
    off = priced.select((pl.sum_horizontal(OFF) - (pl.col("off_rtg") - pl.col("lg_rtg"))).abs().max()).item()
    dfn = priced.select((pl.sum_horizontal(DEF) - (pl.col("lg_rtg") - pl.col("def_rtg"))).abs().max()).item()
    assert off < 1e-9 and dfn < 1e-9


@pytest.mark.parametrize("lg", LEAGUES)
def test_team_parts_add_up_to_net_on_every_row(lg: str) -> None:
    rates = _rates(lg)
    assert {c: rates.schema[c] for c in FACTOR_POINTS_SCHEMA} == FACTOR_POINTS_SCHEMA
    _assert_parts_add_up(rates)
    # one league average per season, shared by every split; null parts only where counts are (college clutch)
    assert rates.group_by("season").agg(pl.col("lg_rtg").n_unique())["lg_rtg"].max() == 1
    unpriced = rates.filter(pl.col("off_efg_pts").is_null())
    assert unpriced.height == 0 if lg in ("nba", "wnba") else set(unpriced["split"]) == {"clutch"}


@pytest.mark.parametrize("lg", LEAGUES)
def test_the_league_is_its_own_average(lg: str) -> None:
    """Each part, weighted by the side's possessions, sums to zero over the league's teams."""
    full = _rates(lg).filter(split="all")
    for c in OFF:
        assert abs((full[c] * full["poss"]).sum() / full["poss"].sum()) < 1e-9, c


@pytest.mark.parametrize("lg", ["nba", "wnba"])
def test_team_parts_are_the_game_prices_summed(lg: str) -> None:
    """The pros' season parts are the per-game accounting summed, per 100 possessions."""
    rows, counts = _inputs(lg)
    books = team_game_four_factors(rows, counts=counts)
    per_team = books.group_by("season", "team_id").agg(
        pl.col("poss").sum(),
        *(pl.col(c).sum() for c in ("shooting_pts", "turnover_pts", "rebounding_pts")),
        (pl.col("free_throw_pts") + pl.col("residual_pts")).sum().alias("ft"),
    )
    joined = _rates(lg).filter(split="all").join(per_team, on=["season", "team_id"], validate="1:1", suffix="_g")
    assert joined.height == per_team.height
    for part, game in (("efg", "shooting_pts"), ("tov", "turnover_pts"), ("orb", "rebounding_pts"), ("ft", "ft")):
        gap = (joined[f"off_{part}_pts"] - 100 * joined[game] / joined["poss"]).abs().max()
        assert gap < 1e-9, part


@pytest.mark.parametrize("lg", LEAGUES)
def test_lineup_parts_add_up_to_net_on_every_row(lg: str) -> None:
    units = _units(lg)
    assert {c: units.schema[c] for c in FACTOR_POINTS_SCHEMA} == FACTOR_POINTS_SCHEMA
    _assert_parts_add_up(units)
    fives = units.filter(n_players=5)
    # The fives partition the league's possessions, so they average to zero too, but for the points
    # of units that started no possession (a free throw after a substitution), which nothing prices.
    unpriced = fives.filter(poss=0)["pts"].sum()
    for c in OFF:
        total = (fives[c] * fives["poss"]).sum() / 100
        assert abs(total + (unpriced if c == "off_ft_pts" else 0)) < 1e-6, c


@pytest.mark.parametrize("lg", LEAGUES)
def test_lineup_rebound_part_prices_the_published_orb_rate(lg: str) -> None:
    """off_orb_pts is (OREB - lg ORB% * RC) * chance_pts per 100, RC the published orb_pct_n (own OREB + their DREB)."""
    fives = _units(lg).filter(pl.col("n_players") == 5, pl.col("poss") > 0)
    oreb = fives["orb_pct"].fill_null(0) * fives["orb_pct_n"]
    lg_orb = oreb.sum() / fives["orb_pct_n"].sum()
    lg_tov = (fives["tov_pct"] * fives["poss"]).sum() / fives["poss"].sum()
    chance = fives["lg_rtg"] / 100 / (1 - lg_tov)
    expected = 100 * chance * (oreb - lg_orb * fives["orb_pct_n"]) / fives["poss"]
    assert (fives["off_orb_pts"] - expected).abs().max() < 1e-9


@pytest.mark.parametrize("lg", ["nba", "wnba"])
def test_team_defense_parts_are_the_opponents_game_prices_summed(lg: str) -> None:
    """A defensive part is minus the opponents' per-game price, summed over the season, per 100 of their possessions."""
    rows, counts = _inputs(lg)
    books = team_game_four_factors(rows, counts=counts)
    # each game row's offense, keyed by the team that defended it
    faced = books.join(books.select("game_id", pl.col("team_id").alias("defense_id")), on="game_id").filter(
        pl.col("team_id") != pl.col("defense_id")
    )
    allowed = faced.group_by("season", "defense_id").agg(
        pl.col("poss").sum().alias("opp_poss_g"),
        *(pl.col(c).sum() for c in ("shooting_pts", "turnover_pts", "rebounding_pts")),
        (pl.col("free_throw_pts") + pl.col("residual_pts")).sum().alias("ft"),
    )
    rates = _rates(lg).filter(split="all")
    assert rates.schema["team_id"] == allowed.schema["defense_id"]
    joined = rates.join(allowed, left_on=["season", "team_id"], right_on=["season", "defense_id"], validate="1:1")
    assert joined.height == rates.height
    assert (joined["opp_poss"] == joined["opp_poss_g"]).all()
    for part, game in (("efg", "shooting_pts"), ("tov", "turnover_pts"), ("orb", "rebounding_pts"), ("ft", "ft")):
        gap = (joined[f"def_{part}_pts"] + 100 * joined[game] / joined["opp_poss"]).abs().max()
        assert gap < 1e-9, part
