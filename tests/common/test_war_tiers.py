"""BB-G61's producer side: ``war_tier`` on ``{lg}_player_war`` and :func:`roster_tier_shape`.

Real 2024-25 / 2025 player_war output for all four leagues, built by the BB-D26 tests from the
committed captures (``tests/fixtures/bb_player_war``, ``tests/fixtures/{mbb,wbb}_player_value``).
"""

from __future__ import annotations

import statistics

import polars as pl
import pytest

from sportsdataverse._common.player_war import (
    TIER_MIN_POSS_SHARE,
    WAR_TIERS,
    _team_tier_counts,
    _war_tier,
    player_war,
    roster_tier_shape,
    war_tiers,
)
from tests.mbb.test_mbb_player_war import _season as college_season
from tests.nba.test_nba_player_war import SOURCES
from tests.nba.test_nba_player_war import _season as pro_season

LEAGUES = pytest.mark.parametrize("league", ["nba", "wnba", "mbb", "wbb"])
#: Each tier's share of the tier cohort: top 5%, then 15%, 30%, 30% and the rest.
SHARES = {"Star": 0.05, "Starter": 0.15, "Rotation": 0.30, "Bench": 0.30, "Fringe": 0.20}


def _war(league: str) -> pl.DataFrame:
    return (pro_season if league in ("nba", "wnba") else college_season)(league)[2]


def _floor(out: pl.DataFrame) -> float:
    return TIER_MIN_POSS_SHARE * out["poss"].sum() / 5 / out["team_id"].n_unique()


def test_tier_boundaries_are_inclusive_floors() -> None:
    pct = [95.0, 94.99, 80.0, 50.0, 20.0, 0.01, None]
    got = pl.DataFrame({"p": pct}, schema={"p": pl.Float64}).select(_war_tier(pl.col("p")))["war_tier"].to_list()
    assert got == ["Star", "Starter", "Starter", "Rotation", "Bench", "Fringe", None]


@LEAGUES
def test_only_the_possession_cohort_is_tiered(league: str) -> None:
    out = _war(league)
    floor = _floor(out)
    tiered = out["war_tier"].is_not_null()
    assert (tiered == (out["poss"] >= floor)).all()  # every war is finite on these fixtures
    # the call-ups the floor exists for: nobody with a few dozen possessions is on the ladder
    assert out.filter(pl.col("war_n") < 50)["war_tier"].null_count() == out.filter(pl.col("war_n") < 50).height
    assert 0.6 < tiered.mean() < 0.85  # observed .70 (NBA) to .79 (WNBA, WBB)


@LEAGUES
def test_tier_shares_are_the_cut_points(league: str) -> None:
    ranked = _war(league).filter(pl.col("war_tier").is_not_null())
    counts = dict(ranked.group_by("war_tier").len().iter_rows())
    n = ranked.height
    # the players at or above a floor f are floor((n + 1) * (1 - f / 100)) of the n (no ties on these
    # fixtures), which is within one player of the cut point's share; a tier, between two floors, is
    # within two (observed at most 1.1)
    cum = 0
    for tier, share in SHARES.items():
        cum += counts[tier]
        expected = n * (1 - WAR_TIERS[tier] / 100)
        assert -1.0 < cum - expected <= 1.0, (tier, cum, expected)
        assert abs(counts[tier] - share * n) < 2.0, (tier, counts[tier], share * n)
    # best first: every Star outranks every Starter, and so on down the ladder
    by_tier = ranked.group_by("war_tier").agg(pl.col("war").min().alias("lo"), pl.col("war").max().alias("hi"))
    order = dict(by_tier.select("war_tier", pl.struct("lo", "hi")).iter_rows())
    names = list(WAR_TIERS)
    for upper, lower in zip(names, names[1:]):
        assert order[upper]["lo"] >= order[lower]["hi"], (upper, lower)


@LEAGUES
def test_war_tiers_is_idempotent_on_the_output(league: str) -> None:
    out = _war(league)
    assert war_tiers(out)["war_tier"].to_list() == out["war_tier"].to_list()
    assert war_tiers(out).columns == out.columns


@LEAGUES
def test_each_team_s_tiers_add_up_to_its_tiered_players(league: str) -> None:
    out = _war(league)
    tiered = out.filter(pl.col("war_tier").is_not_null())
    counts = _team_tier_counts(out)
    teams = tiered["team_id"].n_unique()
    assert counts.height == teams * len(WAR_TIERS)  # a row for every tier of every team, zeros included
    per_team = counts.group_by("season", "team_id").agg(pl.col("players").sum())
    rostered = tiered.group_by("season", "team_id").len()
    j = per_team.join(rostered, on=["season", "team_id"], how="full", coalesce=True)
    assert j.height == teams and (j["players"] == j["len"]).all()
    assert counts["players"].sum() == tiered.height  # a traded player counts once, on his team_id


@LEAGUES
def test_the_shape_is_the_median_over_teams(league: str) -> None:
    out = _war(league)
    shape = roster_tier_shape(out)
    assert shape["war_tier"].to_list() == list(WAR_TIERS) and shape["tier_order"].to_list() == [1, 2, 3, 4, 5]
    assert shape["pct_floor"].to_list() == list(WAR_TIERS.values())
    assert shape["pct_ceiling"].to_list() == [None, 95.0, 80.0, 50.0, 20.0]
    # recount from the player rows, per (season, team): a team with nobody in a tier counts 0 there
    teams: dict[tuple[object, object], dict[str, int]] = {}
    for season, team, tier in (
        out.filter(pl.col("war_tier").is_not_null()).select("season", "team_id", "war_tier").iter_rows()
    ):
        teams.setdefault((season, team), dict.fromkeys(WAR_TIERS, 0))[tier] += 1
    for season, tier, median, mean, n in shape.select(
        "season", "war_tier", "players_median", "players_mean", "teams"
    ).iter_rows():
        col = [t[tier] for (s, _), t in teams.items() if s == season]
        assert n == len(col)
        assert median == statistics.median(col), tier
        assert mean == pytest.approx(statistics.fmean(col), abs=1e-12), tier
    # the means add up to the average tiered roster (the medians need not)
    tiered = out["war_tier"].is_not_null().sum()
    assert shape["players_mean"].sum() == pytest.approx(tiered / len(teams), abs=1e-9)


def test_under_the_cohort_floor_nobody_is_tiered() -> None:
    rated, net, _ = pro_season("wnba")
    first = rated.sort("poss", descending=True).head(8)  # eight players: under MIN_COHORT_PLAYERS
    out = player_war(first, net, league="wnba", sources=SOURCES)
    assert out.height == 8 and out["war_tier"].null_count() == 8
    assert roster_tier_shape(out).height == 0


def test_empty_input_keeps_the_schema() -> None:
    empty = pro_season("nba")[2].clear()
    assert empty.schema["war_tier"] == pl.Utf8
    assert war_tiers(empty).schema == empty.schema
    shape = roster_tier_shape(empty)
    assert shape.height == 0
    assert dict(shape.schema) == {
        "season": pl.Int64,
        "war_tier": pl.Utf8,
        "tier_order": pl.Int64,
        "pct_floor": pl.Float64,
        "pct_ceiling": pl.Float64,
        "players_median": pl.Float64,
        "players_mean": pl.Float64,
        "teams": pl.Int64,
    }
