"""BB-D30: NBA Cup group standings, wild cards, the knockout sim and its 82-game accounting, on real data.

Fixtures (none synthetic; ``tests/fixtures/bb_nba_cup/README.md``): stats.nba.com's
``scheduleleaguev2`` Cup rows for 2023-24 through 2026-27, the overtime possessions of the Group
Play games, the regular-season schedules behind the prior-season record, and stats.nba.com's own
Cup standings (``iststandings``) as the oracle.

What is checked against the real world:

* **Standings:** every team's group rank, conference rank (``istWildcardRank``) and knockout
  seed in the three completed Cups, 90 rows, equal the league's; the point differential and
  points scored apply the overtime rule (2024-25 and 2025-26 equal the oracle on every team;
  its 2023-24 figures include overtime, so the four teams of that Cup's two overtime games differ
  by exactly their overtime margin and points).
* **Knockout field:** the seeds give the real quarterfinals: seed 1 hosts seed 4 (the wild
  card), seed 2 hosts seed 3.
* **The 82:** the sim adds exactly two regular-season games per team (a quarterfinal plus a
  semifinal or a game between quarterfinal losers, or two replacement games) and never the
  final.

The ``bb-nba-cup`` rows of ``tests/mutants/registry.yaml`` go red here: the tiebreak order, the
wild card, and the final counting toward the regular season.
"""

from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1] / "fixtures"
CUP, PRO, RATES = ROOT / "bb_nba_cup", ROOT / "bb_pro_season", ROOT / "bb_team_season_rates"
cup_mod = importlib.import_module("sportsdataverse.nba.nba_cup")
sim_mod = importlib.import_module("sportsdataverse.nba.nba_season_sim")
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")

CHAMPIONS = {2024: 1610612747, 2025: 1610612749, 2026: 1610612752}  # LAL, MIL, NYK


def _schedule(season: int) -> pl.DataFrame:
    """The release schedule of a season (end year) from whichever fixture holds it."""
    path = CUP / f"nba_schedule_{season}.parquet"
    return pl.read_parquet(path if path.exists() else PRO / f"nba_schedule_{season}.parquet")


def _cup(season: int, as_of: dt.date | None = None):  # type: ignore[no-untyped-def]
    games = cup_mod.cup_games(pl.read_parquet(CUP / f"nba_cup_schedule_{season}.parquet"), as_of_date=as_of)
    ot_path = CUP / f"nba_cup_overtime_{season}.parquet"
    ot = cup_mod.overtime_points(pl.read_parquet(ot_path)) if ot_path.exists() else None
    prior = sim_mod._records(sim_mod._regular_games(_schedule(season - 1), None))
    return cup_mod.prepare_cup(games, ot_points=ot, prior_records=prior)


def _oracle(season: int) -> pl.DataFrame:
    return pl.read_parquet(CUP / f"ist_standings_{season}.parquet")


def _model(season: int, as_of: dt.date):  # type: ignore[no-untyped-def]
    """The season sim's as-of game model on the possessions fixture (no prior)."""
    sched = pl.read_parquet(RATES / f"nba_schedule_{season}.parquet")
    done = sim_mod._regular_games(_schedule(season), as_of).filter(pl.col("home_pts").is_not_null())
    rows = tsr.nba_possession_rate_rows(pl.read_parquet(RATES / f"nba_possessions_{season}.parquet"), sched)
    return sim_mod.as_of_game_model(rows.join(done.select("game_id"), on="game_id", how="semi"))


def test_cup_games_mark_groups_rounds_and_tbd_teams() -> None:
    games = cup_mod.cup_games(pl.read_parquet(CUP / "nba_cup_schedule_2027.parquet"))
    assert games.schema == pl.Schema(cup_mod.CUP_GAMES_SCHEMA)
    assert games["season"].unique().to_list() == [2027]
    rounds = dict(games.group_by("cup_round").len().iter_rows())
    assert rounds == {"group": 60, "quarterfinal": 4, "semifinal": 2, "final": 1}
    ko = games.filter(pl.col("cup_round") != "group")
    assert ko["home_team_id"].null_count() == ko.height == 7  # TBD (team id 0) is null
    assert ko.filter(pl.col("cup_round") == "semifinal")["neutral"].to_list() == [False, False]  # 2026-27: at home
    assert ko.filter(pl.col("cup_round") == "final")["neutral"].to_list() == [True]
    groups = games.filter(pl.col("cup_round") == "group").group_by("cup_group").len()
    assert sorted(groups["cup_group"].to_list()) == [f"{c} Group {g}" for c in ("East", "West") for g in "ABC"]


@pytest.mark.parametrize("season", [2024, 2025, 2026])
def test_standings_reproduce_the_league(season: int) -> None:
    st = cup_mod.cup_standings(_cup(season))
    assert st.schema == pl.Schema(cup_mod.CUP_STANDINGS_SCHEMA)
    o = _oracle(season)
    assert o.schema["teamId"] == st.schema["team_id"]
    j = st.join(o, left_on="team_id", right_on="teamId", how="inner", validate="1:1")
    assert j.height == 30
    assert (j["cup_group"] == j["istGroup"]).all()
    assert (j["group_rank"] == j["istGroupRank"]).all()
    assert (j["conference_rank"] == j["istWildcardRank"]).all()
    assert (j["knockout_seed"].fill_null(0) == j["istKnockoutRank"].fill_null(0)).all()
    assert (j["wins"] == j["wins_right"]).all() and (j["games_remaining"] == 0).all()
    off = j.filter((pl.col("point_diff") != pl.col("diff")) | (pl.col("points") != pl.col("pts")))
    # the oracle's 2023-24 figures include overtime: only that Cup's two overtime games differ
    ot_teams = pl.read_parquet(CUP / f"nba_cup_overtime_{season}.parquet")["offense_team_id"].unique().sort()
    assert off["team_id"].sort().to_list() == (ot_teams.to_list() if season == 2024 else [])
    assert j.filter(pl.col("holds_wild_card"))["group_rank"].to_list() == [2, 2]
    assert sorted(j.filter(pl.col("leads_group"))["knockout_seed"].to_list()) == [1, 1, 2, 2, 3, 3]


def test_tiebreaks_decide_real_places() -> None:
    """2024-25 and 2025-26 each had twelve places set by head-to-head and three by point differential."""
    for season in (2025, 2026):
        why = cup_mod.cup_standings(_cup(season))["tiebreak"].value_counts()
        assert dict(why.iter_rows()) == {None: 15, "head_to_head": 12, "point_diff": 3}, season


@pytest.mark.parametrize("season", [2024, 2025, 2026])
def test_seeds_give_the_real_quarterfinals(season: int) -> None:
    seed = dict(cup_mod.cup_standings(_cup(season)).select("team_id", "knockout_seed").iter_rows())
    games = cup_mod.cup_games(pl.read_parquet(CUP / f"nba_cup_schedule_{season}.parquet"))
    qf = games.filter(pl.col("cup_round") == "quarterfinal")
    assert sorted((seed[h], seed[a]) for h, a in qf.select("home_team_id", "away_team_id").iter_rows()) == [
        (1, 4),
        (1, 4),
        (2, 3),
        (2, 3),
    ]


def test_as_of_blanks_the_group_stage() -> None:
    first = cup_mod.cup_games(pl.read_parquet(CUP / "nba_cup_schedule_2026.parquet"))["game_date"].min()
    st = cup_mod.cup_standings(_cup(2026, first), as_of_date=first)
    assert (st["games_remaining"] == 4).all() and (st["wins"] == 0).all()
    assert st["as_of_date"].unique().to_list() == [first]
    # all level: last season's record orders them, a drawing where two records were equal too
    assert set(st["tiebreak"].to_list()) == {"prior_record", "random"}
    assert st.filter(pl.col("tiebreak") == "random").height == 2


@pytest.fixture(scope="module")
def mid_2026() -> tuple:
    as_of = dt.date(2025, 11, 14)
    cup = _cup(2026, as_of)
    return cup, _model(2026, as_of), as_of


def test_sim_odds_sum_and_carry_their_error(mid_2026: tuple) -> None:
    cup, model, as_of = mid_2026
    sim = cup_mod.simulate_cup(cup, model, n_sims=4_000, seed=7, as_of_date=as_of)
    assert sim.schema == pl.Schema(cup_mod.CUP_SIM_SCHEMA)
    sums = {c: round(float(sim[c].sum()), 9) for c in cup_mod._ODDS}
    assert sums == {
        "p_win_group": 6.0,
        "p_wild_card": 2.0,
        "p_quarterfinal": 8.0,
        "p_semifinal": 4.0,
        "p_final": 2.0,
        "p_champion": 1.0,
    }
    assert sim.group_by("cup_group").agg(pl.col("p_win_group").sum())["p_win_group"].round(9).to_list() == [1.0] * 6
    assert float(sim["p_quarterfinal_se"].max()) <= 0.5 / np.sqrt(4_000)
    assert (sim["p_champion"] <= sim["p_final"]).all() and (sim["p_final"] <= sim["p_semifinal"]).all()
    again = cup_mod.simulate_cup(cup, model, n_sims=4_000, seed=7, as_of_date=as_of)
    assert sim.equals(again)


def test_sim_adds_two_regular_season_games_per_team(mid_2026: tuple) -> None:
    """The knockout's 30 games give every team exactly 82; the final is not one of them."""
    from sportsdataverse.nba.nba_season_sim import _noise_sd, _team_arrays

    cup, model, _ = mid_2026
    rng = np.random.default_rng(1)
    net, sd = _team_arrays(model, cup.teams)
    margins = model.margin(net[cup.future[0]], net[cup.future[1]]) + 13 * rng.standard_normal((500, cup.future[0].size))
    draws = cup_mod.simulate_cup_draws(
        cup,
        model,
        np.broadcast_to(net, (500, net.size)).copy(),
        _noise_sd(model, sd[:, None], sd[None, :], False),
        margins,
        rng,
    )
    assert draws.kind.count("quarterfinal") == 4 and draws.kind.count("semifinal") == 2
    assert draws.kind.count("quarterfinal_loser") == 2 and draws.kind.count("replacement") == 22
    assert "final" not in draws.kind
    per_team = np.stack([np.bincount(np.r_[draws.home[s], draws.away[s]], minlength=30) for s in range(500)])
    assert (per_team == 2).all()
    homes = np.stack([np.bincount(draws.home[s], minlength=30) for s in range(500)])
    assert set(np.unique(homes)) <= {0, 1, 2}
    # the replacement games stay among the teams out of the knockout
    rep = [i for i, k in enumerate(draws.kind) if k == "replacement"]
    out = draws.seed == 0
    rows = np.arange(500)[:, None]
    assert out[rows, draws.home[:, rep]].all() and out[rows, draws.away[:, rep]].all()


@pytest.mark.parametrize("season", [2024, 2025, 2026])
def test_finished_cup_is_certain(season: int) -> None:
    """With every game played the sim returns the real field and champion with certainty."""
    cup = _cup(season)
    model = _model(2026, dt.date(2025, 12, 1))  # any model: nothing is left to simulate
    sim = cup_mod.simulate_cup(cup, model, n_sims=50, seed=1)
    st = cup_mod.cup_standings(cup)
    field = set(st.filter(pl.col("knockout_seed").is_not_null())["team_id"].to_list())
    assert set(sim.filter(pl.col("p_quarterfinal") == 1.0)["team_id"].to_list()) == field
    assert sim.filter(pl.col("p_champion") == 1.0)["team_id"].to_list() == [CHAMPIONS[season]]
    assert set(sim["p_quarterfinal"].unique().to_list()) == {0.0, 1.0}


def test_rejects_bad_inputs() -> None:
    cup = _cup(2026)
    model = _model(2026, dt.date(2025, 12, 1))
    with pytest.raises(ValueError, match="n_sims"):
        cup_mod.simulate_cup(cup, model, n_sims=0)
    games = cup_mod.cup_games(pl.read_parquet(CUP / "nba_cup_schedule_2026.parquet"))
    with pytest.raises(ValueError, match="six groups"):
        cup_mod.prepare_cup(games.filter(pl.col("cup_group") != "East Group A"))
    with pytest.raises(ValueError, match="first NBA Cup"):
        cup_mod.nba_cup_standings(2023, cup_schedule=pl.read_parquet(CUP / "nba_cup_schedule_2024.parquet"))
