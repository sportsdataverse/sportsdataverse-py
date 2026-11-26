"""BB-G61's producer side: ``war_tier`` / ``war_tier_conf`` on ``{lg}_player_war``, :func:`roster_tiers`
and :func:`roster_tier_shape`.

Real 2024-25 / 2025 player_war output for all four leagues, built by the BB-D26 tests from the
committed captures (``tests/fixtures/bb_player_war``, ``tests/fixtures/{mbb,wbb}_player_value``); the
college conferences come from the committed 2025 schedules (``tests/fixtures/bb_college_season``)
through ``college_teams``, as ``mbb_player_war`` reads them.
"""

from __future__ import annotations

import functools
import statistics

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS
from sportsdataverse._common.player_war import (
    TIER_MIN_POSS_SHARE,
    WAR_TIERS,
    _team_tier_counts,
    _war_tier,
    player_war,
    roster_tier_shape,
    roster_tiers,
    war_tiers,
)
from sportsdataverse.mbb.mbb_college_season import college_teams
from tests.mbb.test_mbb_player_war import _season as college_season
from tests.nba.test_nba_player_war import SOURCES
from tests.nba.test_nba_player_war import _season as pro_season

LEAGUES = pytest.mark.parametrize("league", ["nba", "wnba", "mbb", "wbb"])
#: Each tier's share of the tier cohort: top 5%, then 15%, 30%, 30% and the rest.
SHARES = {"Star": 0.05, "Starter": 0.15, "Rotation": 0.30, "Bench": 0.30, "Fringe": 0.20}


COLLEGE = pytest.mark.parametrize("league", ["mbb", "wbb"])
SCHEDULES = Path(__file__).resolve().parents[1] / "fixtures" / "bb_college_season"


@functools.cache
def _war(league: str) -> pl.DataFrame:
    if league in ("nba", "wnba"):
        return pro_season(league)[2]
    stints, net, _ = college_season(league)
    conferences = college_teams(pl.read_parquet(SCHEDULES / f"{league}_schedule_2025.parquet"))
    return player_war(stints, net, league=league, sources=["bpm"], conferences=conferences)


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
def test_a_traded_player_is_on_every_team_he_played_for(league: str) -> None:
    out = _war(league)
    rows = roster_tiers(out)
    assert rows.height == out["teams"].sum()  # one row per team he played for
    assert rows["main_team"].sum() == out.height  # exactly one main team each
    traded = out.filter(pl.col("teams") > 1)
    assert traded.height > 0  # 2024-25: NBA 81, WNBA 26, MBB 10, WBB 2 on two or more teams
    for player, team, team_ids, tier in traded.select("player_id", "team_id", "team_ids", "war_tier").iter_rows():
        mine = rows.filter(pl.col("player_id") == player)
        assert sorted(map(str, mine["team_id"])) == sorted(team_ids.split(","))
        assert team_ids.split(",")[0] == str(team)  # most possessions first: his team_id
        assert mine["war_tier"].to_list() == [tier] * mine.height  # his full-season tier on each


@LEAGUES
def test_each_team_s_tiers_add_up_to_its_tiered_players(league: str) -> None:
    rows = roster_tiers(_war(league))
    tiered = rows.filter(pl.col("war_tier").is_not_null())
    counts = _team_tier_counts(rows)
    teams = tiered["team_id"].n_unique()
    assert counts.height == teams * len(WAR_TIERS)  # a row for every tier of every team, zeros included
    per_team = counts.group_by("season", "team_id").agg(pl.col("players").sum())
    rostered = tiered.group_by("season", "team_id").len()
    j = per_team.join(rostered, on=["season", "team_id"], how="full", coalesce=True)
    assert j.height == teams and (j["players"] == j["len"]).all()
    assert counts["players"].sum() == tiered.height  # a traded player counts on each of his teams


def _recount(out: pl.DataFrame, tier: str, by_conf: bool) -> dict[tuple, dict[str, int]]:
    """Players per (season, [conference], team) and tier, from the player rows' team_ids, zeros included.

    By conference, a player on a team outside his main team's conference takes that conference's tier
    (checked on its own by the traded-player conference test); everyone else keeps his own.
    """
    conf = dict(out.select("team_id", "conference_id").drop_nulls().iter_rows()) if by_conf else {}
    elsewhere: dict[tuple, str | None] = {}
    if by_conf:
        rows = roster_tiers(out).join(out.select("player_id", pl.col("conference_id").alias("_mc")), on="player_id")
        moved = rows.filter((pl.col("main_team") == False) & pl.col("conference_id").ne_missing(pl.col("_mc")))
        elsewhere = {(p, str(t)): v for p, t, v in moved.select("player_id", "team_id", tier).iter_rows()}
    teams: dict[tuple, dict[str, int]] = {}
    for season, player, team_ids, own in out.select("season", "player_id", "team_ids", tier).iter_rows():
        for team in team_ids.split(","):
            t = elsewhere.get((player, team), own) if by_conf else own
            key = (season, conf.get(team), team) if by_conf else (season, None, team)
            if t is None or (by_conf and key[1] is None):
                continue
            teams.setdefault(key, dict.fromkeys(WAR_TIERS, 0))[t] += 1
    return teams


@LEAGUES
def test_the_shape_is_the_median_over_teams(league: str) -> None:
    out = _war(league)
    shape = roster_tier_shape(out)
    scopes = [
        ("league", "war_tier", False),
        *([("conference", "war_tier_conf", True)] if league in ("mbb", "wbb") else []),
    ]
    assert set(shape["scope"]) == {s for s, _, _ in scopes}
    for scope, tier, by_conf in scopes:
        part = shape.filter(pl.col("scope") == scope)
        assert part["pct_floor"].to_list()[:5] == list(WAR_TIERS.values())
        assert part["pct_ceiling"].to_list()[:5] == [None, 95.0, 80.0, 50.0, 20.0]
        teams = _recount(out, tier, by_conf)
        groups = {(s, c) for s, c, _ in teams}
        assert part.height == len(groups) * len(WAR_TIERS)
        for season, conf, t, median, mean, n in part.select(
            "season", "conference_id", "war_tier", "players_median", "players_mean", "teams"
        ).iter_rows():
            col = [v[t] for (s, c, _), v in teams.items() if s == season and c == conf]
            assert n == len(col)
            assert median == statistics.median(col), (scope, conf, t)
            assert mean == pytest.approx(statistics.fmean(col), abs=1e-12), (scope, conf, t)
    # league-wide, the means add up to the average roster of tiered players used (traded players twice)
    league = shape.filter(pl.col("scope") == "league")
    teams = _recount(out, "war_tier", False)
    assert league["players_mean"].sum() == pytest.approx(
        sum(map(sum, (v.values() for v in teams.values()))) / len(teams)
    )


@COLLEGE
def test_the_conference_tier_is_the_ladder_within_each_conference(league: str) -> None:
    out = _war(league)
    assert out["conference_id"].null_count() < 0.01 * out.height  # observed 15 MBB players, 0 WBB
    # the same possession floor: a player is tiered within his conference only if he is across D1
    assert (out["war_tier_conf"].is_not_null() <= out["war_tier"].is_not_null()).all()
    by_conf = out.filter(pl.col("war_tier_conf").is_not_null()).partition_by("conference_id", as_dict=True)
    assert len(by_conf) >= 30
    for part in by_conf.values():
        n, cum = part.height, 0
        counts = dict(part.group_by("war_tier_conf").len().iter_rows())
        for tier in WAR_TIERS:
            cum += counts.get(tier, 0)
            assert -1.0 < cum - n * (1 - WAR_TIERS[tier] / 100) <= 1.0, (tier, cum, n)
    # a conference's best players are D1 Stars far more often than a weak conference's
    assert out.filter(pl.col("war_tier_conf") == "Star")["war_tier"].is_in(["Star", "Starter"]).mean() > 0.5


def test_under_the_cohort_floor_nobody_is_tiered() -> None:
    rated, net, _ = pro_season("wnba")
    first = rated.sort("poss", descending=True).head(8)  # eight players: under MIN_COHORT_PLAYERS
    out = player_war(first, net, league="wnba", sources=SOURCES)
    assert out.height == 8 and out["war_tier"].null_count() == 8
    assert roster_tier_shape(out).height == 0
    assert roster_tiers(out).height == out["teams"].sum()  # untiered players still have their rows


def test_empty_input_keeps_the_schema() -> None:
    empty = pro_season("nba")[2].clear()
    assert empty.schema["war_tier"] == pl.Utf8
    assert war_tiers(empty).schema == empty.schema
    assert roster_tiers(empty).height == 0
    shape = roster_tier_shape(empty)
    assert shape.height == 0
    assert dict(shape.schema) == {
        "season": pl.Int64,
        "scope": pl.Utf8,
        "conference_id": pl.Utf8,
        "war_tier": pl.Utf8,
        "tier_order": pl.Int64,
        "pct_floor": pl.Float64,
        "pct_ceiling": pl.Float64,
        "players_median": pl.Float64,
        "players_mean": pl.Float64,
        "teams": pl.Int64,
    }


@COLLEGE
def test_a_traded_player_s_conference_tier_on_his_other_team_is_that_conference_s(league: str) -> None:
    """On a team of another conference, war_tier_conf ranks his WAR among that conference's tier cohort
    (its tiered players by main team, plus him; ties averaged), not his main conference's."""
    out = _war(league)
    rows = roster_tiers(out)
    main_conf = dict(out.select("player_id", "conference_id").iter_rows())
    away = rows.filter((pl.col("main_team") == False) & pl.col("war_tier").is_not_null()).filter(
        pl.col("conference_id").ne_missing(pl.col("player_id").replace_strict(main_conf, default=None))
    )
    assert away.height > 0  # 2024-25: MBB 10, WBB 2 rows on a team of another conference
    cohort = out.filter(pl.col("war_tier").is_not_null() & pl.col("conference_id").is_not_null())
    for season, conf, war, tier in away.select("season", "conference_id", "war", "war_tier_conf").iter_rows():
        others = cohort.filter((pl.col("season") == season) & (pl.col("conference_id") == conf))["war"].to_list()
        n = len(others) + 1
        rank = sum(w > war for w in others) + 1 + sum(w == war for w in others) / 2
        expected = (
            _war_tier(pl.lit(100 * (n + 1 - rank) / (n + 1)))
            if len(others) >= MIN_COHORT_PLAYERS
            else pl.lit(None, pl.Utf8)
        )
        assert pl.select(expected).item() == tier, (conf, war)
    # his own conference's team keeps war_tiers' value
    mains = rows.filter(pl.col("main_team")).join(out.select("player_id", "war_tier_conf"), on="player_id", suffix="_p")
    assert (mains["war_tier_conf"].eq_missing(mains["war_tier_conf_p"])).all()
