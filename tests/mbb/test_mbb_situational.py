"""BB-D21: college situational splits, on committed real captures.

Fixtures (none synthetic; README in ``tests/fixtures/bb_situational``): every 2024-25 game of
Michigan State and Purdue (``ncaa_mbb_possessions_2025``, 72 contests) and of UCLA and USC
(``ncaa_wbb_possessions_2025``, 69 contests, 10-minute quarters), the published 2025 ratings of
both leagues (``ncaa_ratings_2025``), and BB-D3's ESPN schedules
(``bb_team_season_rates/{mbb,wbb}_schedule_2025``) for ``neutral_site``.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.situational_splits import FAMILIES, situational_team_schema
from sportsdataverse.mbb.mbb_situational import (
    _qualified_ratings,
    mbb_situational_lineup,
    mbb_situational_player,
    mbb_situational_team,
    ncaa_situation_rows,
)
from sportsdataverse.mbb.mbb_team_season_rates import ncaa_possession_rate_rows
from sportsdataverse.wbb import wbb_loaders
from sportsdataverse.wbb.wbb_situational import wbb_situational_team

# The package re-exports the functions under the modules' names, so import the modules themselves.
mbb_team_season_rates = importlib.import_module("sportsdataverse.mbb.mbb_team_season_rates")
mbb_team_strength = importlib.import_module("sportsdataverse.mbb.mbb_team_strength")
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
SIT = FIXTURES / "bb_situational"
SUMS = ["poss", "opp_poss", "pts", "opp_pts", "seconds"]
#: league -> the two teams whose every game the fixture holds (ESPN ids).
TEAMS = {"mens": (127, 2509), "womens": (26, 30)}
_LG = {"mens": "mbb", "womens": "wbb"}


def _releases(league: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    lg = _LG[league]
    poss = pl.read_parquet(SIT / f"ncaa_{lg}_possessions_2025.parquet")
    sched = pl.read_parquet(FIXTURES / "bb_team_season_rates" / f"{lg}_schedule_2025.parquet")
    ratings = pl.read_parquet(SIT / "ncaa_ratings_2025.parquet").filter(league=league).drop("league")
    return poss, sched, ratings


def _patch(mp: pytest.MonkeyPatch, league: str) -> None:
    poss, sched, ratings = _releases(league)

    def frame(f: pl.DataFrame):  # type: ignore[no-untyped-def]
        def load(seasons: int | list[int]) -> pl.DataFrame:
            assert seasons in (2025, [2025]), seasons
            return f

        return load

    if league == "mens":
        mp.setattr(mbb_team_season_rates, "load_ncaa_mbb_possessions", frame(poss))
        mp.setattr(mbb_team_season_rates, "load_mbb_schedule", frame(sched))
        mp.setattr(mbb_team_strength, "load_mbb_ratings", frame(ratings))
    else:
        mp.setattr(wbb_loaders, "load_ncaa_wbb_possessions", frame(poss))
        mp.setattr(wbb_loaders, "load_wbb_schedule", frame(sched))
        mp.setattr(wbb_loaders, "load_wbb_ratings", frame(ratings))


@pytest.fixture(scope="module")
def mens() -> dict[str, pl.DataFrame]:
    with pytest.MonkeyPatch.context() as mp:
        _patch(mp, "mens")
        return {
            "team": mbb_situational_team(2025),
            "player": mbb_situational_player(2025),
            "lineup": mbb_situational_lineup(2025),
        }


@pytest.fixture(scope="module")
def womens() -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        _patch(mp, "womens")
        return wbb_situational_team(2025)


def _ours(team: pl.DataFrame, league: str) -> pl.DataFrame:
    return team.filter(pl.col("team_id").is_in(TEAMS[league]))


def test_families_and_periods(mens: dict[str, pl.DataFrame], womens: pl.DataFrame) -> None:
    """Men play halves, women quarters (no overtime in these 69); college has no offensive-rebound flag, so no ``chance``."""
    for team, periods in ((mens["team"], {"p1", "p2", "ot"}), (womens, {"p1", "p2", "p3", "p4"})):
        assert dict(team.schema) == situational_team_schema()
        assert set(team["family"]) == set(FAMILIES) - {"chance"}
        assert set(team.filter(family="period")["split"]) == periods
    # both teams played neutral-site games (conference tournament, November events)
    for team, league in ((mens["team"], "mens"), (womens, "womens")):
        neutral = _ours(team, league).filter(split="neutral")
        assert neutral.height == 2 and (neutral["games"] > 0).all()


@pytest.mark.parametrize(
    ("name", "entity"),
    [("team", ["team_id"]), ("player", ["team_id", "player_id"]), ("lineup", ["team_id", "lineup_id"])],
)
def test_every_family_adds_up_to_all(mens: dict[str, pl.DataFrame], name: str, entity: list[str]) -> None:
    frame = mens[name]
    keys = ["season", *entity]
    parts = frame.filter(pl.col("family") != "all").group_by([*keys, "family"]).agg(pl.col(SUMS).sum())
    whole = frame.filter(family="all").select(*keys, *[pl.col(c).alias(f"{c}_all") for c in SUMS])
    j = parts.join(whole, on=keys, how="left", validate="m:1")
    assert j.height == whole.height * (len(FAMILIES) - 2)  # no chance family
    for c in SUMS:
        assert (j[c] - j[f"{c}_all"]).abs().max() <= 1e-6, (name, c)


def test_womens_family_adds_up(womens: pl.DataFrame) -> None:
    parts = womens.filter(pl.col("family") != "all").group_by("team_id", "family").agg(pl.col(SUMS).sum())
    whole = womens.filter(family="all").select("team_id", *[pl.col(c).alias(f"{c}_all") for c in SUMS])
    j = parts.join(whole, on="team_id", validate="m:1")
    assert j.height == whole.height * (len(FAMILIES) - 2)  # no chance family
    for c in SUMS:
        assert (j[c] - j[f"{c}_all"]).abs().max() <= 1e-6, c


def test_all_is_bb_d3s_possessions(mens: dict[str, pl.DataFrame]) -> None:
    """The ``all`` row counts exactly BB-D3's possession rows, and the flag families the release's flags."""
    poss, sched, _ = _releases("mens")
    rows = ncaa_possession_rate_rows(poss, sched)
    team = _ours(mens["team"], "mens")
    for tid in TEAMS["mens"]:
        whole = team.filter(team_id=tid, split="all").row(0, named=True)
        assert whole["poss"] == rows.filter(offense_team_id=tid).height
        assert whole["pts"] == rows.filter(offense_team_id=tid)["points"].sum()
        assert whole["opp_poss"] == rows.filter(defense_team_id=tid).height
    flags = ncaa_situation_rows(poss, sched)
    league = mens["team"].group_by("split").agg(pl.col("poss").sum())
    got = {r["split"]: r["poss"] for r in league.iter_rows(named=True)}
    on = flags.filter(pl.col("offense_team_id").is_not_null())
    assert got["transition"] == on["is_transition"].sum() > 0
    assert got["garbage_time"] == on["is_garbage_time"].sum() > 0


def test_players_are_five_times_the_team(mens: dict[str, pl.DataFrame]) -> None:
    keys = ["team_id", "family", "split"]
    players = mens["player"].group_by(keys).agg(pl.col("poss", "opp_poss").sum())
    j = players.join(_ours(mens["team"], "mens"), on=keys, how="inner", validate="1:1", suffix="_team")
    assert j.height == _ours(mens["team"], "mens").height
    ratio = (j["poss"] + j["opp_poss"]).sum() / (j["poss_team"] + j["opp_poss_team"]).sum()
    assert 4.99 <= ratio <= 5.0, ratio
    assert mens["player"].schema["player_id"] == pl.Utf8
    # each side's own players: Michigan State used 13 and Purdue 16, not their opponents' hundreds
    roster = mens["player"].filter(split="all").group_by("team_id").agg(pl.col("player_id").n_unique().alias("n"))
    assert dict(roster.filter(pl.col("team_id").is_in(TEAMS["mens"])).iter_rows()) == {127: 13, 2509: 16}


def test_vs_top_is_the_qualified_top_25() -> None:
    """``vs_top`` is a published top-25 team; a team with under half the most games is never ranked."""
    poss, sched, ratings = _releases("mens")
    q = _qualified_ratings(ratings)
    assert q.height == 364 < ratings.height  # the 336 non-D1 rows (a game or two each) are out
    top = q.sort("adj_net_rtg", descending=True).head(25)["team_id"]
    rows = ncaa_situation_rows(poss, sched)
    for tid in TEAMS["mens"]:
        opp = rows.filter(offense_team_id=tid).select("game_id", "defense_team_id").unique()
        expect = opp["defense_team_id"].is_in(top.implode()).sum()
        with pytest.MonkeyPatch.context() as mp:
            _patch(mp, "mens")
            team = mbb_situational_team(2025)
        got = team.filter(team_id=tid, split="vs_top")["games"].item()
        assert got == expect > 0


def test_errors() -> None:
    with pytest.raises(ValueError, match="league"):
        mbb_situational_team(2025, league="coed")
    with pytest.raises(ValueError, match="top_n"):
        mbb_situational_player(2025, top_n=0)
