"""BB-D4: NBA / WNBA lineup seasons, on committed real captures.

Fixtures (``tests/fixtures/bb_lineup_seasons``, README there; none synthetic): every 2024-25
regular-season game of three NBA teams and 16 games of the 2025 WNBA season, from the
``*_stats_pbp``, ``*_stats_game_lineups``, ``*_stats_possessions`` and ``*_stats_schedules``
releases, plus stats.nba.com's five-man ``lineups`` dashboard (Base and Advanced totals) for the
three teams, the oracle.

The gates are the values observed on these games; never lower one to make a change pass. Points
are equal to stats.nba.com's on 81 of the 96 units over 50 possessions and minutes within 30 s on
87; the net rating differs mostly through possessions, which stats.nba.com credits to every lineup
on the floor during one (these give each possession to the lineup that started it), so our points
over its possessions give back its ORtg. The plan's acceptance ("over 200 poss, net within 2.0")
holds for 5 of the 12 units here and 64.6% of the league (fixture README). The
``bb-lineup-seasons`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.lineup_seasons import lineup_count_schema, lineup_seasons, lineup_seasons_league_averages
from sportsdataverse._common.lineup_seasons import lineup_seasons_schema
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_game_impact import nba_impact_rows
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows, nba_lineup_seasons
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_lineup_seasons import wnba_lineup_seasons

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_lineup_seasons"
LEAGUES = {"nba": "00", "wnba": "10"}
#: The three NBA teams whose whole regular season the fixture holds.
NBA_TEAMS = (1610612750, 1610612755, 1610612756)
_TOTALS = ("minutes", "poss", "opp_poss", "pts", "opp_pts")
_NS = ("efg_pct_n", "opp_efg_pct_n", "orb_pct_n", "opp_orb_pct_n")
#: The rank direction of every rate: True when the lowest value ranks 1 (written out, not imported).
LOW_FIRST = {
    "off_rtg": False, "def_rtg": True, "net_rtg": False, "efg_pct": False, "tov_pct": True,
    "orb_pct": False, "ft_rate": False, "opp_efg_pct": True, "opp_tov_pct": False,
    "opp_orb_pct": True, "opp_ft_rate": True,
}  # fmt: skip


def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"{lg}_{name}_2025.parquet")


@cache
def _built(lg: str) -> pl.DataFrame:
    poss, schedule = _read(lg, "possessions"), _read(lg, "schedule")
    rows = nba_impact_rows(_read(lg, "pbp"), _read(lg, "game_lineups"), poss, schedule, league_id=LEAGUES[lg])
    return lineup_seasons(rows, nba_lineup_count_rows(poss, schedule), player_dtype=pl.Int64(), min_poss=400)


def _oracle() -> pl.DataFrame:
    """stats.nba.com's five-man units for the three teams: Advanced poss/ratings beside Base pts/min."""
    dash = _read("nba", "lineups_dashboard").with_columns(
        pl.col("group_id")
        .str.strip_chars("-")
        .str.split("-")
        .cast(pl.List(pl.Int64))
        .list.sort()
        .cast(pl.List(pl.Utf8))
        .list.join("-")
        .alias("lineup_id")
    )
    adv = dash.filter(pl.col("measure_type") == "advanced").select(
        "team_id",
        "lineup_id",
        pl.col("poss").alias("o_poss"),
        "off_rating",
        "net_rating",
        "efg_pct",
        "tm_tov_pct",
        "oreb_pct",
    )
    base = dash.filter(pl.col("measure_type") == "base").select(
        "team_id", "lineup_id", pl.col("pts").alias("o_pts"), pl.col("min").alias("o_min")
    )
    return adv.join(base, on=["team_id", "lineup_id"], how="inner", validate="1:1")


def test_five_man_units_match_stats_nba_lineups() -> None:
    fives = _built("nba").filter(pl.col("n_players") == 5)
    oracle = _oracle()
    assert (
        oracle.schema["lineup_id"] == fives.schema["lineup_id"] and oracle.schema["team_id"] == fives.schema["team_id"]
    )
    joined = oracle.join(fives, on=["team_id", "lineup_id"], how="left", suffix="_ours", validate="1:1")
    assert joined.height == 206 and joined["poss"].null_count() == 0  # every stats.nba.com unit is here
    # Units over 50 possessions: points scored are equal on 81 of 96, minutes within 30 s on 87.
    big = joined.filter(pl.col("o_poss") >= 50)
    assert big.height == 96
    assert big.filter(pl.col("pts") == pl.col("o_pts")).height >= 81
    assert big.filter((pl.col("minutes") - pl.col("o_min")).abs() <= 0.5).height >= 87
    # Units over 200 possessions (the plan's gate population): 12 units.
    top = joined.filter(pl.col("o_poss") > 200)
    d_net = (top["net_rtg"] - top["net_rating"]).abs()
    assert top.height == 12
    # observed 4.298 / 2.127; the largest (team 1610612750, 203497-1629638-1629675-1630162-1630183) has
    # stats.nba.com's points and minutes exactly, and 197 possessions to its 204
    assert d_net.max() <= 4.3 and d_net.median() <= 2.13
    assert top.select(pl.corr("net_rtg", "net_rating")).item() >= 0.993  # observed 0.9932
    assert (top["efg_pct_ours"] - top["efg_pct"]).abs().max() <= 0.030  # observed 0.0296
    assert (top["tov_pct"] - top["tm_tov_pct"]).abs().max() <= 0.017  # observed 0.0169
    assert (top["orb_pct"] - top["oreb_pct"]).abs().max() <= 0.030  # observed 0.0296
    # stats.nba.com counts a possession for every lineup on the floor during it, so its counts run higher.
    assert 0.94 <= (top["poss"] / top["o_poss"]).median() <= 0.97  # observed 0.955
    # ... and that is the whole gap: our points over its possessions give back its ORtg.
    assert (100 * top["pts"] / top["o_poss"] - top["off_rating"]).abs().median() <= 0.05  # observed 0.043


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_two_and_three_man_units_are_ten_times_the_fives(lg: str) -> None:
    out = _built(lg)
    cols = [*_TOTALS, *_NS, "tov_pct_n"]
    per = out.group_by("team_id", "n_players").agg(pl.col(cols).sum())
    fives = per.filter(pl.col("n_players") == 5).drop("n_players")
    for k in (2, 3):
        units = per.filter(pl.col("n_players") == k).drop("n_players")
        both = fives.join(units, on="team_id", suffix=f"_{k}", validate="1:1")
        assert both.height == fives.height > 0
        for c in cols:
            ratio = both[f"{c}_{k}"] / both[c]
            assert ((ratio - 10).abs() < 1e-5).all(), f"{lg} {k}-man {c}: {ratio.to_list()}"
    assert out.filter(pl.col("n_players") == 2)["player_3"].null_count() == out.filter(pl.col("n_players") == 2).height


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_five_man_counts_add_up_to_the_possessions(lg: str) -> None:
    """Per team, the five-man rows hold every possession and count of the team's full-lineup possessions."""
    out = _built(lg).filter(pl.col("n_players") == 5)
    poss = _read(lg, "possessions").with_columns(
        (pl.col("fg2a") + pl.col("fg3a")).alias("fga"),
        *[
            pl.concat_list([f"{s}_player_{i}" for i in range(1, 6)])
            .list.drop_nulls()
            .list.n_unique()
            .eq(5)
            .alias(f"{s}_5")
            for s in ("off", "def")
        ],
    )
    offense = (
        poss.filter("off_5")
        .group_by(pl.col("offense_team_id").alias("team_id"))
        .agg(
            pl.len().alias("poss"),
            pl.col("fga").sum().alias("fga"),
            (pl.col("oreb") + pl.col("dreb")).sum().alias("chances"),
        )
    )
    defense = (
        poss.filter("def_5")
        .group_by(pl.col("defense_team_id").alias("team_id"))
        .agg(
            pl.len().alias("opp_poss"),
            pl.col("fga").sum().alias("opp_fga"),
            (pl.col("oreb") + pl.col("dreb")).sum().alias("opp_chances"),
        )
    )
    ours = out.group_by("team_id").agg(pl.col("poss", "opp_poss", *_NS).sum())
    both = ours.join(offense, on="team_id", suffix="_raw").join(defense, on="team_id", suffix="_raw")
    teams = set(_read(lg, "schedule")["home_team_id"]) | set(_read(lg, "schedule")["away_team_id"])
    assert both.height == len(teams)
    assert (both["poss"] == both["poss_raw"]).all() and (both["opp_poss"] == both["opp_poss_raw"]).all()
    assert (both["efg_pct_n"] == both["fga"]).all() and (both["opp_efg_pct_n"] == both["opp_fga"]).all()
    assert (both["orb_pct_n"] == both["chances"]).all() and (both["opp_orb_pct_n"] == both["opp_chances"]).all()


def test_ranks_follow_the_gate_and_direction() -> None:
    out = _built("nba").filter(pl.col("team_id").is_in(NBA_TEAMS))
    gate = (pl.col("poss") + pl.col("opp_poss")) >= 400
    for k in (2, 3, 5):
        units = out.filter(pl.col("n_players") == k)
        ranked = units.filter(pl.col("net_rtg_rank").is_not_null())
        assert ranked.height >= 10 and ranked.filter(~gate).height == 0
        for m, low in LOW_FIRST.items():
            rated = units.filter(pl.col(f"{m}_rank").is_not_null())
            best = rated.filter(pl.col(f"{m}_rank") == pl.col(f"{m}_rank").min())[m]
            assert (best == (rated[m].min() if low else rated[m].max())).all(), f"{k}-man {m}"
        assert (ranked["net_rtg_n"] == ranked["poss"] + ranked["opp_poss"]).all()
        assert units.filter(gate & pl.col("net_rtg_rank").is_null()).height == 0


def test_league_averages_cover_each_unit_size() -> None:
    out = _built("nba")
    avg = lineup_seasons_league_averages(out, level="nba", min_poss=400)
    five = avg.filter((pl.col("category") == "lineup_seasons_5man") & (pl.col("metric") == "net_rtg"))
    qualified = out.filter((pl.col("n_players") == 5) & (pl.col("poss") + pl.col("opp_poss") >= 400))
    assert five["n"].item() == qualified.height and five["qualifier_min"].item() == 400.0
    assert set(avg["category"]) == {"lineup_seasons_2man", "lineup_seasons_3man", "lineup_seasons_5man"}


def test_public_builder_reads_the_start_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(lg: str, name: str, want: int):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            assert list(seasons) == [want], f"{name}: called with {seasons}"
            return _read(lg, name)

        return load

    names = {"pbp": "pbp", "game_lineups": "game_lineups", "possessions": "possessions", "schedules": "schedule"}
    for loader, name in names.items():
        monkeypatch.setattr(nba_loaders, f"load_nba_stats_{loader}", fixture("nba", name, 2024))
        monkeypatch.setattr(wnba_loaders, f"load_wnba_stats_{loader}", fixture("wnba", name, 2025))
    assert nba_lineup_seasons(2025).equals(_built("nba"))
    assert nba_lineup_seasons(2025, season_type="preseason").height == 0  # the fixture has no preseason game
    wnba = wnba_lineup_seasons(2025)
    ranked = wnba.filter(pl.col("net_rtg_rank").is_not_null())
    assert ranked.filter(pl.col("poss") + pl.col("opp_poss") < 200).height == 0  # the WNBA gate is 200
    assert (
        wnba.filter(pl.col("n_players") == 5)["poss"].sum()
        == _built("wnba").filter(pl.col("n_players") == 5)["poss"].sum()
    )


def test_empty_input_bad_counts_and_bad_args() -> None:
    schema = lineup_seasons_schema(pl.Int64())
    empty_rows = nba_impact_rows(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
    empty = lineup_seasons(
        empty_rows, pl.DataFrame(schema=lineup_count_schema(pl.Int64())), player_dtype=pl.Int64(), min_poss=1
    )
    assert empty.height == 0 and empty.schema == pl.Schema(schema)
    assert nba_lineup_count_rows(pl.DataFrame(), pl.DataFrame()).schema == pl.Schema(lineup_count_schema(pl.Int64()))
    with pytest.raises(KeyError, match="counts missing columns"):
        lineup_seasons(empty_rows, pl.DataFrame({"season": [2025]}), player_dtype=pl.Int64(), min_poss=1)
    with pytest.raises(ValueError, match="league_id"):
        nba_lineup_seasons(2025, league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        nba_lineup_seasons(2025, season_type="Regular Season")
