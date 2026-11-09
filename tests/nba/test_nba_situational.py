"""BB-D21: NBA / WNBA situational splits, on committed real captures.

Fixtures (none synthetic; README in ``tests/fixtures/bb_situational``):

* The 2023-24 NBA regular season, 1,230 games: BB-D7's ``bb_possession_flags/nba_possessions_2024``
  (lineups, start types, clocks) and ``nba_pbp_2024``, BB-D3's ``bb_team_season_rates/nba_possessions_2024``
  (made-shot counts) and ``nba_schedule_2024``, and ``bb_situational/nba_second_chance_2024``: the
  same release rows, column-split across the three files.
* Oracles, stats.nba.com 2023-24 Regular Season: the team clutch dashboard
  (``nba_prediction/clutch_team_2024``), the player clutch dashboard (``nba_player_clutch_2024``),
  team points by quarter (``nba_team_period_2024``) and second-chance points
  (``nba_team_misc_2024``).
* WNBA 2002, 30 games of the two-halves era (``wnba_possessions_2002`` / ``wnba_schedule_2002`` and
  BB-D7's ``wnba_pbp_2002``).

Every gate is the value observed on these files (the fixture README's last section), rounded toward
the observed value; never lower one to make a change pass. The ``bb-situational`` rows of
``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.situational_splits import (
    FAMILIES,
    situation_row_schema,
    situational_lineup_schema,
    situational_player_schema,
    situational_team,
    situational_team_schema,
)
from sportsdataverse._common.team_season_rates import team_season_rates
from sportsdataverse._common.team_strength import RIDGE_LAMBDA, possession_ridge_ratings
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_possession_flags import add_possession_flags
from sportsdataverse.nba.nba_situational import (
    nba_situation_rows,
    nba_situational_lineup,
    nba_situational_player,
    nba_situational_team,
)
from sportsdataverse.nba.nba_team_season_rates import nba_possession_rate_rows
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_situational import wnba_situational_team

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
SIT = FIXTURES / "bb_situational"
KEY = ["game_id", "period", "possession_number"]
SUMS = ["poss", "opp_poss", "pts", "opp_pts", "seconds"]
_FG = ["fg2a", "fg2m", "fg3a", "fg3m", "fta", "ftm", "oreb", "dreb", "tov"]


def _nba_2024_releases() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """The 2023-24 possessions, pbp and schedule, the possession columns joined back together."""
    flags = pl.read_parquet(FIXTURES / "bb_possession_flags" / "nba_possessions_2024.parquet")
    counts = pl.read_parquet(FIXTURES / "bb_team_season_rates" / "nba_possessions_2024.parquet").select(*KEY, *_FG)
    second = pl.read_parquet(SIT / "nba_second_chance_2024.parquet")
    poss = flags.join(counts, on=KEY, how="inner", validate="1:1").join(second, on=KEY, how="inner", validate="1:1")
    assert poss.height == flags.height == counts.height == second.height
    pbp = pl.read_parquet(FIXTURES / "bb_possession_flags" / "nba_pbp_2024.parquet")
    sched = pl.read_parquet(FIXTURES / "bb_team_season_rates" / "nba_schedule_2024.parquet")
    return poss, pbp, sched


def _patch(
    mp: pytest.MonkeyPatch, module, releases: tuple[pl.DataFrame, ...], names: tuple[str, ...], expect: list[int]
):  # type: ignore[no-untyped-def]
    for name, frame in zip(names, releases):

        def load(seasons: list[int], _frame: pl.DataFrame = frame, _name: str = name) -> pl.DataFrame:
            assert list(seasons) == expect, f"{_name}: called with {seasons}, expected {expect}"
            return _frame

        mp.setattr(module, name, load)


_NBA_LOADERS = ("load_nba_stats_possessions", "load_nba_stats_pbp", "load_nba_stats_schedules")


@pytest.fixture(scope="module")
def releases() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    return _nba_2024_releases()


@pytest.fixture(scope="module")
def tables(releases: tuple[pl.DataFrame, ...]) -> dict[str, pl.DataFrame]:
    """The three public tables for 2023-24, through the loaders (the NBA loaders take the start year)."""
    with pytest.MonkeyPatch.context() as mp:
        _patch(mp, nba_loaders, releases, _NBA_LOADERS, [2023])
        return {
            "team": nba_situational_team(2024),
            "player": nba_situational_player(2024),
            "lineup": nba_situational_lineup(2024),
        }


def test_schemas_and_families(tables: dict[str, pl.DataFrame]) -> None:
    team, player, lineup = tables["team"], tables["player"], tables["lineup"]
    assert dict(team.schema) == situational_team_schema()
    assert dict(player.schema) == situational_player_schema(pl.Int64())
    assert dict(lineup.schema) == situational_lineup_schema(pl.Int64())
    got = team.group_by("family").agg(pl.col("split").unique().sort())
    want = {f: sorted(s) for f, s in FAMILIES.items()}
    # the stats schedule has no neutral-site column; no 2023-24 regular-season game reached a 2nd overtime
    want["venue"] = ["away", "home"]
    assert {r["family"]: r["split"] for r in got.iter_rows(named=True)} == want
    assert team.filter(family="all").height == 30
    assert (team.filter(family="all")["games"] == 82).all()


@pytest.mark.parametrize(
    ("name", "entity"),
    [("team", ["team_id"]), ("player", ["team_id", "player_id"]), ("lineup", ["team_id", "lineup_id"])],
)
def test_every_family_adds_up_to_all(tables: dict[str, pl.DataFrame], name: str, entity: list[str]) -> None:
    """leading + tied + trailing = all, p1..p4 + ot = all, ... for every team, player and unit."""
    frame = tables[name]
    keys = ["season", *entity]
    parts = frame.filter(pl.col("family") != "all").group_by([*keys, "family"]).agg(pl.col(SUMS).sum())
    whole = frame.filter(family="all").select(*keys, *[pl.col(c).alias(f"{c}_all") for c in SUMS])
    j = parts.join(whole, on=keys, how="left", validate="m:1")
    assert j.height == whole.height * (len(FAMILIES) - 1)
    for c in SUMS:
        assert (j[c] - j[f"{c}_all"]).abs().max() <= 1e-6, (name, c)


def test_players_are_five_times_the_team(tables: dict[str, pl.DataFrame], releases: tuple[pl.DataFrame, ...]) -> None:
    """Five players on the floor: their sums are five times the team's, but for the sides the source lists short.

    269 offensive sides of 2023-24 name a player twice (four distinct players): each is one
    player-possession short of five, and has no five-man unit.
    """
    slots = [f"off_player_{i}" for i in range(1, 6)]
    distinct = releases[0].select(pl.concat_list(slots).list.drop_nulls().list.unique().list.len().alias("n"))["n"]
    short = int((5 - distinct).sum())
    assert short == 269
    keys = ["team_id", "family", "split"]
    players = tables["player"].group_by(keys).agg(pl.col("poss", "opp_poss", "pts", "opp_pts").sum())
    j = players.join(tables["team"], on=keys, how="inner", validate="1:1", suffix="_team")
    assert j.height == tables["team"].height
    for c in ("poss", "opp_poss", "pts", "opp_pts"):
        assert (j[c] <= 5 * j[f"{c}_team"]).all(), c
    whole = j.filter(split="all")
    assert int((5 * whole["poss_team"] - whole["poss"]).sum()) == short
    units = tables["lineup"].filter(split="all")["poss"].sum()
    assert tables["team"].filter(split="all")["poss"].sum() - units == (distinct < 5).sum()


def test_halves_are_their_quarters(tables: dict[str, pl.DataFrame]) -> None:
    """h1 is the 1st and 2nd quarters, h2 the 3rd and 4th, team by team."""
    wide = (
        tables["team"]
        .filter(pl.col("family").is_in(["period", "half"]))
        .pivot(on="split", index="team_id", values="poss", aggregate_function="first")
    )
    assert (wide["h1"] == wide["p1"] + wide["p2"]).all()
    assert (wide["h2"] == wide["p3"] + wide["p4"]).all()


def test_lineup_ids(tables: dict[str, pl.DataFrame]) -> None:
    unit = tables["lineup"].row(0, named=True)
    ids = [unit[f"player_{i}"] for i in range(1, 6)]
    assert ids == sorted(ids) and len(set(ids)) == 5
    assert unit["lineup_id"] == "-".join(str(i) for i in ids)


def test_clutch_and_venue_are_bb_d3s(tables: dict[str, pl.DataFrame], releases: tuple[pl.DataFrame, ...]) -> None:
    """The clutch, home and away splits are BB-D3's, possession for possession."""
    poss, _, sched = releases
    d3 = team_season_rates(nba_possession_rate_rows(poss, sched), minutes_per_game=48)
    team = tables["team"].filter(pl.col("split").is_in(["clutch", "home", "away"]))
    j = team.join(d3, on=["season", "team_id", "split"], how="inner", validate="1:1", suffix="_d3")
    assert j.height == 90
    for c in ("games", "poss", "opp_poss", "pts", "opp_pts"):
        assert (j[c] == j[f"{c}_d3"]).all(), c


def test_team_clutch_against_stats_nba(tables: dict[str, pl.DataFrame]) -> None:
    """stats.nba.com's team clutch dashboard (last 5:00, within 5): observed 0.996 / 0.982."""
    oracle = pl.read_parquet(FIXTURES / "nba_prediction" / "clutch_team_2024.parquet").with_columns(
        pl.col("team_id").cast(pl.Int64)
    )
    clutch = tables["team"].filter(split="clutch")
    assert clutch.schema["team_id"] == oracle.schema["team_id"]
    j = clutch.join(oracle, on="team_id", how="inner", validate="1:1")
    assert j.height == 30
    assert j.select(pl.corr("poss", "clutch_poss")).item() >= 0.99
    assert j.select(pl.corr("net_rtg", "clutch_net_rating")).item() >= 0.98
    # the level, not only the order: stats.nba.com counts a possession that starts just before 5:00
    # and acts inside it, so it has 23 more a team (14 to 29); the net ratings agree on average
    assert 20.0 <= (j["clutch_poss"] - j["poss"]).mean() <= 26.0
    assert abs((j["net_rtg"] - j["clutch_net_rating"]).mean()) <= 0.5


def test_player_clutch_against_stats_nba(tables: dict[str, pl.DataFrame]) -> None:
    """stats.nba.com's player clutch dashboard: observed 443 matched, poss r 0.965, net r 0.937 (150+ poss)."""
    oracle = pl.read_parquet(SIT / "nba_player_clutch_2024.parquet")
    clutch = tables["player"].filter(split="clutch")
    for k in ("season", "player_id", "team_id"):
        assert clutch.schema[k] == oracle.schema[k], k
    j = clutch.join(oracle, on=["season", "team_id", "player_id"], how="inner", validate="1:1")
    assert j.height >= 440
    assert j.select(pl.corr("poss", "poss_right")).item() >= 0.96
    # the oracle's min is per game
    assert j.select(pl.corr("minutes", pl.col("min") * pl.col("gp"))).item() >= 0.96
    regulars = j.filter(pl.col("poss_right") >= 150)
    assert regulars.height >= 130
    assert regulars.select(pl.corr("net_rtg", "net_rating")).item() >= 0.93
    assert (regulars["net_rtg"] - regulars["net_rating"]).abs().median() <= 3.0


def test_quarter_points_against_stats_nba(tables: dict[str, pl.DataFrame]) -> None:
    """Team points by quarter vs stats.nba.com Period=1..4: observed mean |diff| 2.79, max 8 of ~2,400."""
    oracle = pl.read_parquet(SIT / "nba_team_period_2024.parquet")
    mine = (
        tables["team"]
        .filter(family="period")
        .filter(pl.col("split") != "ot")
        .with_columns(pl.col("split").str.slice(1).cast(pl.Int64).alias("period"))
    )
    for k in ("season", "team_id", "period"):
        assert mine.schema[k] == oracle.schema[k], k
    j = mine.join(oracle, on=["season", "team_id", "period"], how="inner", validate="1:1")
    assert j.height == 120
    diff = (j["pts"] - j["pts_right"]).abs()
    assert diff.mean() <= 2.8
    assert diff.max() <= 8
    assert (j["games"] == j["gp"]).all()


def test_second_chance_against_stats_nba(tables: dict[str, pl.DataFrame]) -> None:
    """Second-chance possessions vs stats.nba.com PTS_2ND_CHANCE: observed r 0.998 / 0.997, ratio 1.022.

    The possession's points include any it scored before its offensive rebound (a made first free
    throw), so the split runs about 2% over the official second-chance points.
    """
    oracle = pl.read_parquet(SIT / "nba_team_misc_2024.parquet")
    sc = tables["team"].filter(split="second_chance")
    for k in ("season", "team_id"):
        assert sc.schema[k] == oracle.schema[k], k
    j = sc.join(oracle, on=["season", "team_id"], how="inner", validate="1:1")
    assert j.height == 30
    assert j.select(pl.corr("pts", "pts_2nd_chance")).item() >= 0.995
    assert j.select(pl.corr("opp_pts", "opp_pts_2nd_chance")).item() >= 0.995
    ratio = j["pts"].sum() / j["pts_2nd_chance"].sum()
    assert 1.0 <= ratio <= 1.03, ratio


def test_score_family_is_the_two_sides(tables: dict[str, pl.DataFrame]) -> None:
    """A possession its offense leads is one its defense trails, league-wide."""
    score = tables["team"].filter(family="score").group_by("split").agg(pl.col("poss", "opp_poss").sum())
    by = {r["split"]: r for r in score.iter_rows(named=True)}
    assert by["leading"]["poss"] == by["trailing"]["opp_poss"]
    assert by["trailing"]["poss"] == by["leading"]["opp_poss"]
    assert by["tied"]["poss"] == by["tied"]["opp_poss"]


def test_flag_families_are_bb_d7s(tables: dict[str, pl.DataFrame], releases: tuple[pl.DataFrame, ...]) -> None:
    """Garbage time, transition and second chance count exactly the flagged possessions."""
    poss, pbp, _ = releases
    flagged = add_possession_flags(poss.drop("is_transition", "is_garbage_time", strict=False), pbp)
    team = tables["team"].group_by("split").agg(pl.col("poss").sum())
    got = {r["split"]: r["poss"] for r in team.iter_rows(named=True)}
    assert got["garbage_time"] == flagged["is_garbage_time"].sum() > 0
    assert got["transition"] == flagged["is_transition"].sum() > 0
    assert got["second_chance"] == poss["is_second_chance"].sum() > 0


def test_a_release_that_carries_the_flags_is_reflagged(releases: tuple[pl.DataFrame, ...]) -> None:
    """The producers publish BB-D7's flags on the possessions release; the rows re-flag them with this
    version's window (here a release flagged at the old 10 s), never raise on the clash."""
    poss, pbp, sched = releases
    stale = add_possession_flags(poss, pbp, transition_seconds=10.0)
    assert stale["is_transition"].sum() != add_possession_flags(poss, pbp)["is_transition"].sum()
    assert nba_situation_rows(stale, pbp, sched).equals(nba_situation_rows(poss, pbp, sched))


def test_vs_top_is_the_bb_d19_top_ten(tables: dict[str, pl.DataFrame], releases: tuple[pl.DataFrame, ...]) -> None:
    """``vs_top`` games are the games against the ten best teams by BB-D19's adjusted net rating."""
    poss, _, sched = releases
    ratings = possession_ridge_ratings(nba_possession_rate_rows(poss, sched), lam=RIDGE_LAMBDA["00"])
    top = ratings.sort("adj_net_rtg", descending=True).head(10)["team_id"]
    games = pl.concat(
        [
            sched.select(pl.col(us).alias("team_id"), pl.col(them).alias("opp"))
            for us, them in (("home_team_id", "away_team_id"), ("away_team_id", "home_team_id"))
        ]
    )
    expect = games.group_by("team_id").agg(pl.col("opp").is_in(top.implode()).sum().alias("vs_top_games"))
    vs_top = tables["team"].filter(split="vs_top")
    assert vs_top.schema["team_id"] == expect.schema["team_id"]
    got = vs_top.join(expect, on="team_id", validate="1:1")
    assert got.height == 30
    assert (got["games"] == got["vs_top_games"]).all()


def test_top_n_and_errors(releases: tuple[pl.DataFrame, ...], monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: object) -> None:
        raise AssertionError("loaded before the arguments were checked")

    for name in _NBA_LOADERS:
        monkeypatch.setattr(nba_loaders, name, boom)
    with pytest.raises(ValueError, match="top_n"):
        nba_situational_team(2024, top_n=0)
    with pytest.raises(ValueError, match="league_id"):
        nba_situational_player(2024, league_id="20")
    rows = nba_situation_rows(*releases)
    with pytest.raises(KeyError, match="reg_periods"):
        situational_team(rows.drop("reg_periods"), pl.DataFrame(), top_n=10)
    empty = nba_situation_rows(releases[0].head(0), releases[1], releases[2])
    assert dict(empty.schema) == situation_row_schema(pl.Int64()) and empty.height == 0


def test_empty_season_returns_the_schemas(releases: tuple[pl.DataFrame, ...], monkeypatch: pytest.MonkeyPatch) -> None:
    """A season type with no games (a play-in before 2021) gives each table its schema, no rows."""
    _patch(monkeypatch, nba_loaders, tuple(f.head(0) for f in releases), _NBA_LOADERS, [2023])
    for fn, schema in (
        (nba_situational_team, situational_team_schema()),
        (nba_situational_player, situational_player_schema(pl.Int64())),
        (nba_situational_lineup, situational_lineup_schema(pl.Int64())),
    ):
        out = fn(2024)
        assert dict(out.schema) == schema and out.height == 0, fn.__name__


def test_wnba_two_halves_2002(monkeypatch: pytest.MonkeyPatch) -> None:
    """Through 2005 the WNBA played two 20-minute halves: periods p1, p2 and ot, clutch in the 2nd half."""
    poss = pl.read_parquet(SIT / "wnba_possessions_2002.parquet")
    pbp = pl.read_parquet(FIXTURES / "bb_possession_flags" / "wnba_pbp_2002.parquet")
    sched = pl.read_parquet(SIT / "wnba_schedule_2002.parquet")
    rows = nba_situation_rows(poss, pbp, sched, league_id="10")
    assert (rows["reg_periods"] == 2).all()
    # the clock: a half's first possession starts at 20:00, and the 2nd half is the late period
    assert rows.filter(pl.col("period") <= 2)["start_clock"].max() == 1200.0
    assert rows.filter(pl.col("late"))["period"].min() == 2
    assert rows.filter(pl.col("late") & (pl.col("period") == 2)).height > 0
    _patch(
        monkeypatch,
        wnba_loaders,
        (poss, pbp, sched),
        ("load_wnba_stats_possessions", "load_wnba_stats_pbp", "load_wnba_stats_schedules"),
        [2002],
    )
    team = wnba_situational_team(2002)
    assert set(team.filter(family="period")["split"]) <= {"p1", "p2", "ot"}
    assert set(team.filter(family="half")["split"]) <= {"h1", "h2", "ot"}
    whole = team.filter(family="all")
    # 40 minutes a game plus the overtimes: the possessions cover the game clock
    per_game = whole["minutes"].sum() / whole["games"].sum()
    assert 40.4 <= per_game <= 40.5, per_game  # observed 40.44
    assert team.filter(split="clutch")["poss"].sum() > 0


def test_only_the_requested_season_type_is_flagged() -> None:
    """A game of another season type with no play-by-play does not fail the requested one."""
    poss = pl.read_parquet(SIT / "wnba_possessions_2002.parquet")
    pbp = pl.read_parquet(FIXTURES / "bb_possession_flags" / "wnba_pbp_2002.parquet")
    sched = pl.read_parquet(SIT / "wnba_schedule_2002.parquet")
    other = sched["game_id"].sort()[0]
    # one game relabelled a playoff game and its play-by-play dropped
    sched = sched.with_columns(
        pl.when(pl.col("game_id") == other).then(pl.lit("playoffs")).otherwise("season_type").alias("season_type")
    )
    pbp = pbp.filter(pl.col("game_id") != other)
    with pytest.raises(ValueError, match="pbp has no events"):
        add_possession_flags(poss.drop("is_second_chance"), pbp)
    rows = nba_situation_rows(poss, pbp, sched, league_id="10")
    assert rows["game_id"].n_unique() == 29 and other not in rows["game_id"]
    assert nba_situation_rows(poss, pbp, sched, league_id="10", season_type="play-in").height == 0
