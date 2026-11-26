"""BB-G61's producer side: ``war_tier`` on ``{lg}_player_war`` and :func:`roster_tier_shape`.

Real 2024-25 / 2025 player_war output for all four leagues, built by the BB-D26 tests from the
committed captures (``tests/fixtures/bb_player_war``, ``tests/fixtures/{mbb,wbb}_player_value``).
"""

from __future__ import annotations

import statistics

import polars as pl
import pytest

from sportsdataverse._common.player_war import WAR_TIERS, _team_tier_counts, player_war, roster_tier_shape
from tests.mbb.test_mbb_player_war import _season as college_season
from tests.nba.test_nba_player_war import SOURCES
from tests.nba.test_nba_player_war import _season as pro_season

LEAGUES = pytest.mark.parametrize("league", ["nba", "wnba", "mbb", "wbb"])
#: Each tier's share of the league-season: top 5%, then 15%, 30%, 30% and the rest.
SHARES = {"Star": 0.05, "Starter": 0.15, "Rotation": 0.30, "Bench": 0.30, "Fringe": 0.20}


def _war(league: str) -> pl.DataFrame:
    return (pro_season if league in ("nba", "wnba") else college_season)(league)[2]


@LEAGUES
def test_tier_shares_are_the_cut_points(league: str) -> None:
    out = _war(league)
    ranked = out.filter(pl.col("war_pct").is_not_null())
    assert ranked.height == out.height > 100  # every player of the season is in the cohort
    assert out["war_tier"].null_count() == 0
    counts = dict(ranked.group_by("war_tier").len().iter_rows())
    n = ranked.height
    # war_pct = 100 * (n + 1 - rank) / (n + 1): a floor falls between two ranks, so a tier's count is
    # its share of the cohort to within rounding at either end (observed at most 0.8 players)
    for tier, share in SHARES.items():
        assert abs(counts[tier] - share * n) <= 1.0, (tier, counts[tier], share * n)
    # a tier is the war_pct band of its floor and the floor above it
    floors = sorted([*WAR_TIERS.values(), 100.0 + 1e-9])
    bands = {t: (f, floors[floors.index(f) + 1]) for t, f in WAR_TIERS.items()}
    for tier, (lo, hi) in bands.items():
        pct = ranked.filter(pl.col("war_tier") == tier)["war_pct"]
        assert pct.min() >= lo and pct.max() < hi, tier
    # best first: every Star outranks every Starter, and so on down the ladder
    by_tier = ranked.group_by("war_tier").agg(pl.col("war").min().alias("lo"), pl.col("war").max().alias("hi"))
    order = dict(by_tier.select("war_tier", pl.struct("lo", "hi")).iter_rows())
    names = list(WAR_TIERS)
    for upper, lower in zip(names, names[1:]):
        assert order[upper]["lo"] >= order[lower]["hi"], (upper, lower)


@LEAGUES
def test_each_team_s_tiers_add_up_to_its_tiered_players(league: str) -> None:
    out = _war(league)
    counts = _team_tier_counts(out)
    teams = out.filter(pl.col("war_tier").is_not_null())["team_id"].n_unique()
    assert counts.height == teams * len(WAR_TIERS)  # a row for every tier of every team, zeros included
    per_team = counts.group_by("team_id").agg(pl.col("players").sum())
    rostered = out.filter(pl.col("war_tier").is_not_null()).group_by("team_id").len()
    j = per_team.join(rostered, on="team_id", how="full", coalesce=True)
    assert j.height == teams and (j["players"] == j["len"]).all()
    assert counts["players"].sum() == out.height  # a traded player counts once, on his team_id


@LEAGUES
def test_the_shape_is_the_median_over_teams(league: str) -> None:
    out = _war(league)
    shape = roster_tier_shape(out)
    assert shape["war_tier"].to_list() == list(WAR_TIERS) and shape["tier_order"].to_list() == [1, 2, 3, 4, 5]
    # recount from the player rows: a team with nobody in a tier counts 0 there
    teams: dict[object, dict[str, int]] = {}
    for team, tier in out.select("team_id", "war_tier").iter_rows():
        teams.setdefault(team, dict.fromkeys(WAR_TIERS, 0))[tier] += 1
    for tier, median, mean, n in shape.select("war_tier", "players_median", "players_mean", "teams").iter_rows():
        col = [t[tier] for t in teams.values()]
        assert n == len(teams)
        assert median == statistics.median(col), tier
        assert mean == pytest.approx(statistics.fmean(col), abs=1e-12), tier
    # the means add up to the average tiered roster (the medians need not)
    assert shape["players_mean"].sum() == pytest.approx(out.height / len(teams), abs=1e-9)


def test_under_the_cohort_floor_nobody_is_tiered() -> None:
    rated, net, _ = pro_season("wnba")
    first = rated.sort("poss", descending=True).head(8)  # eight players: under MIN_COHORT_PLAYERS
    out = player_war(first, net, league="wnba", sources=SOURCES)
    assert out.height == 8 and out["war_pct"].null_count() == 8
    assert out["war_tier"].null_count() == 8
    assert roster_tier_shape(out).height == 0


def test_empty_input_keeps_the_schema() -> None:
    empty = pro_season("nba")[2].clear()
    assert empty.schema["war_tier"] == pl.Utf8
    shape = roster_tier_shape(empty)
    assert shape.height == 0
    assert dict(shape.schema) == {
        "season": pl.Int64,
        "war_tier": pl.Utf8,
        "tier_order": pl.Int64,
        "players_median": pl.Float64,
        "players_mean": pl.Float64,
        "teams": pl.Int64,
    }
