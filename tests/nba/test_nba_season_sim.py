"""BB-D18: NBA / WNBA season sim, standings tiebreakers, play-in and draft lottery, on real data.

Fixtures (none synthetic): BB-D3's ``tests/fixtures/bb_team_season_rates`` possessions (NBA
2023-24 and 2025-26, WNBA 2025) and ``tests/fixtures/bb_pro_season`` schedules with dates and the
postseason (README there).

What is checked against the real world:

* **Tiebreakers:** the standings computed from the regular season give the seeds the real
  postseason shows: NBA play-in hosts are seeds 7 and 9, the 7-8 winner meets seed 2 and the
  last play-in winner seed 1, the other first-round hosts are seeds 3 and 4 against 6 and 5;
  WNBA first round 1-8, 2-7, 3-6, 4-5, the better seed at home. NBA 2023-24 holds a three-way
  tie (ORL / IND / PHI at 47-35, ORL the division leader) and two two-way ties; WNBA 2025 two
  head-to-head ties (LV / ATL at 30-14, PHX / NY at 27-17).
* **Lottery:** the exact NBA 2019 odds (slot 1: 14.0 / 13.4 / 12.7 / 12.0 / 47.9%) and the
  real 2024 NBA and 2026 WNBA lotteries: CHA and POR tied at 21-61 split 132.5 chances, and the
  WNBA's two-season records give DAL / CHI / LA / WAS / CONN 420 / 261 / 167 / 97 / 55.
* **Validation gates** (as of mid-season, 10,000 draws, the default seed; observed values from
  ``tools/validate_pro_season_sim.py``, gate = observed + 0.02 Brier / + 0.5 wins, never
  lowered): NBA 2023-24 playoff Brier 0.0585 (gate 0.0785), wins MAE 3.077 (3.577); 2025-26
  0.0829 (0.1029), 4.214 (4.714); WNBA 2025 0.0638 (0.0838), 3.501 (4.001). Each also beats the
  pace baseline's wins MAE (3.364 / 4.458 / 4.015). With the preseason prior (2025-26 and WNBA
  2025 carry it from the committed season before; 2023-24 has none): 2025-26 0.0880 / 4.271,
  WNBA 2025 0.0602 / 3.469.
* **Preseason prior:** weeks 0-6 game log loss with and without it (NBA 2025-26, WNBA 2025), the
  regressed prior and the expansion default (the 2025 Valkyries), and the in-season ridge toward
  the prior.

The ``bb-pro-season`` rows of ``tests/mutants/registry.yaml`` go red here: the tiebreaker order,
the play-in hosts, the lottery chances, the WNBA two-season order and the game model's home term.
"""

from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA
from sportsdataverse._common.team_strength import RIDGE_LAMBDA, possession_ridge_ratings
from sportsdataverse.nba.nba_season_rules import (
    NBA_DIVISIONS,
    NBA_LOTTERY_CHANCES,
    STANDINGS_SCHEMA,
    draw_picks,
    lottery_slots,
    nba_draft_lottery_odds,
    nba_season_standings,
)

ROOT = Path(__file__).resolve().parents[1] / "fixtures"
RATES, PRO = ROOT / "bb_team_season_rates", ROOT / "bb_pro_season"
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
sim_mod = importlib.import_module("sportsdataverse.nba.nba_season_sim")

DAL, CHI, LA, WAS, CONN = 1611661321, 1611661329, 1611661320, 1611661322, 1611661323
CHA, POR, DET, WAS_NBA, SAS = 1610612766, 1610612757, 1610612765, 1610612764, 1610612759
GSV = 1611661331


def _schedule(lg: str, season: int) -> pl.DataFrame:
    return pl.read_parquet(PRO / f"{lg}_schedule_{season}.parquet")


def _regular(lg: str, season: int) -> pl.DataFrame:
    return sim_mod._regular_games(_schedule(lg, season), None)


def _sim(lg: str, season: int, **kwargs) -> pl.DataFrame:  # type: ignore[no-untyped-def]
    """``nba_season_sim`` over the fixtures (the loaders patched by season).

    A season with no fixture loads as an empty frame, as a missing release does, so the sim runs
    without a preseason prior there (NBA 2023-24: 2022-23 is not committed).
    """

    def loader(kind: str):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            (s,) = seasons
            year = s + 1 if lg == "nba" else s
            name = f"{lg}_possessions_{year}.parquet" if kind == "possessions" else f"{lg}_schedule_{year}.parquet"
            path = next((d / name for d in (PRO, RATES) if (d / name).exists()), None)
            return pl.read_parquet(path) if path is not None else pl.DataFrame()

        return load

    with pytest.MonkeyPatch.context() as mp:
        if lg == "nba":
            mp.setattr(tsr, "load_nba_stats_possessions", loader("possessions"))
            mp.setattr(tsr, "load_nba_stats_schedules", loader("schedule"))
            return sim_mod.nba_season_sim(season, **kwargs)
        from sportsdataverse.wnba import wnba_loaders
        from sportsdataverse.wnba.wnba_season_sim import wnba_season_sim

        mp.setattr(wnba_loaders, "load_wnba_stats_possessions", loader("possessions"))
        mp.setattr(wnba_loaders, "load_wnba_stats_schedules", loader("schedule"))
        return wnba_season_sim(season, **kwargs)


def _postseason_seeds(schedule: pl.DataFrame) -> dict[int, int]:
    """The NBA regular-season seeds 1, 2, 7, 8, 9, 10 the real play-in and first round imply."""
    dated = schedule.with_columns(pl.col("game_date").str.to_date())
    winner = pl.when(pl.col("home_pts") > pl.col("away_pts")).then("home_team_id").otherwise("away_team_id")
    seeds: dict[int, int] = {}
    first_round = dated.filter(pl.col("season_type") == "playoffs").sort("game_date")
    for conf in ("East", "West"):
        members = [t for t, (c, _) in NBA_DIVISIONS.items() if c == conf]
        pi = dated.filter((pl.col("season_type") == "play-in") & pl.col("home_team_id").is_in(members))
        pi = pi.sort("game_date").with_columns(winner.alias("w"))
        last = pi.row(2, named=True)
        last_teams = {last["home_team_id"], last["away_team_id"]}
        game_78 = next(r for r in pi.head(2).iter_rows(named=True) if r["w"] not in last_teams)
        game_910 = next(r for r in pi.head(2).iter_rows(named=True) if r is not game_78 and r["w"] in last_teams)
        seeds |= {game_78["home_team_id"]: 7, game_78["away_team_id"]: 8}
        seeds |= {game_910["home_team_id"]: 9, game_910["away_team_id"]: 10}
        for into, top in ((game_78["w"], 2), (last["w"], 1)):
            g = first_round.filter((pl.col("home_team_id") == into) | (pl.col("away_team_id") == into)).row(
                0, named=True
            )
            seeds[g["home_team_id"] if g["away_team_id"] == into else g["away_team_id"]] = top
    return seeds


@pytest.mark.parametrize("season", [2024, 2026])
def test_nba_standings_reproduce_the_real_postseason(season: int) -> None:
    sched = _schedule("nba", season)
    st = nba_season_standings(_regular("nba", season))
    assert st.schema == pl.Schema(STANDINGS_SCHEMA)
    assert st.height == 30 and set(st["games"].to_list()) == {82}
    seed = dict(zip(st["team_id"].to_list(), st["seed"].to_list(), strict=True))
    for team, real in _postseason_seeds(sched).items():
        assert seed[team] == real, f"{team}: computed seed {seed[team]}, the postseason says {real}"
    # the other first-round hosts are seeds 3 and 4, paired with 6 and 5
    po = sched.filter(pl.col("season_type") == "playoffs").sort("game_date")
    firsts: dict[frozenset[int], tuple[int, int]] = {}
    for h, a in po.select("home_team_id", "away_team_id").iter_rows():
        if not any(h in k or a in k for k in firsts):
            firsts[frozenset((h, a))] = (h, a)
    pairs = sorted((seed[h], seed[a]) for h, a in firsts.values() if seed[h] <= 6 and seed[a] <= 6)
    assert pairs == [(3, 6), (3, 6), (4, 5), (4, 5)]
    # a division leader is first in its division
    assert st.filter(pl.col("division_leader")).get_column("division_rank").to_list() == [1] * 6


def test_nba_2024_ties_break_as_the_league_did() -> None:
    st = nba_season_standings(_regular("nba", 2024))
    row = {t: r for t, r in zip(st["team_id"].to_list(), st.iter_rows(named=True), strict=True)}
    orl, ind, phi = 1610612753, 1610612754, 1610612755
    assert {row[t]["wins"] for t in (orl, ind, phi)} == {47}
    # three-way: the division leader first, then IND over PHI head to head
    assert [row[t]["seed"] for t in (orl, ind, phi)] == [5, 6, 7]
    phx, nop = 1610612756, 1610612740
    assert row[phx]["wins"] == row[nop]["wins"] == 49 and row[phx]["point_diff"] < row[nop]["point_diff"]
    assert (row[phx]["seed"], row[nop]["seed"]) == (6, 7)  # head-to-head before point differential


def test_wnba_standings_reproduce_the_real_first_round() -> None:
    sched = _schedule("wnba", 2025)
    st = nba_season_standings(_regular("wnba", 2025), league_id="10")
    seed = dict(zip(st["team_id"].to_list(), st["seed"].to_list(), strict=True))
    po = sched.filter(pl.col("season_type") == "playoffs").sort("game_date")
    firsts: dict[frozenset[int], tuple[int, int]] = {}
    for h, a in po.select("home_team_id", "away_team_id").iter_rows():
        if not any(h in k or a in k for k in firsts):
            firsts[frozenset((h, a))] = (h, a)
    assert sorted((seed[h], seed[a]) for h, a in firsts.values()) == [(1, 8), (2, 7), (3, 6), (4, 5)]
    lv, atl, phx, ny = 1611661319, 1611661330, 1611661317, 1611661313
    assert (seed[lv], seed[atl], seed[phx], seed[ny]) == (2, 3, 4, 5)


def test_nba_lottery_odds_are_the_2019_table() -> None:
    odds = nba_draft_lottery_odds()
    p = odds.pivot(on="pick", index="lottery_slot", values="probability").sort("lottery_slot").drop("lottery_slot")
    m = p.to_numpy()
    np.testing.assert_allclose(m.sum(0), 1.0, atol=1e-12)
    np.testing.assert_allclose(m.sum(1), 1.0, atol=1e-12)
    np.testing.assert_allclose(100 * m[0, :5], [14.0, 13.4, 12.7, 12.0, 47.9], atol=0.05)
    np.testing.assert_allclose(100 * m[4, 0], 10.5, atol=0.05)
    np.testing.assert_allclose(100 * m[13, [0, 13]], [0.5, 97.6], atol=0.05)
    assert m[0, 5:].sum() == 0 and m[13, :13].sum() == pytest.approx(0.024, abs=5e-4)


def test_wnba_lottery_draws_two_picks() -> None:
    odds = nba_draft_lottery_odds(league_id="10")
    m = odds.pivot(on="pick", index="lottery_slot", values="probability").sort("lottery_slot").drop("lottery_slot")
    m = m.to_numpy()
    np.testing.assert_allclose(m[:, 0], [0.420, 0.261, 0.167, 0.097, 0.055])
    assert m[0, 3:].sum() == 0  # the worst record is guaranteed a top-3 pick (WNBA.com 2026 FAQ)


def test_batched_draws_match_the_exact_odds() -> None:
    rng = np.random.default_rng(7)
    n = 100_000
    picks = draw_picks(np.tile(np.asarray(NBA_LOTTERY_CHANCES, float), (n, 1)), 4, rng)
    exact = nba_draft_lottery_odds().filter(pl.col("pick") == 1).sort("lottery_slot")["probability"].to_numpy()
    got = (picks == 1).mean(0)
    assert np.all(np.abs(got - exact) < 4 * np.sqrt(exact * (1 - exact) / n))


def test_game_model_home_term() -> None:
    """The home edge is the ridge's home term, and it carries to the real home win rate.

    NBA 2025-26 as of mid-season: the edge is 1.92 points, and the second half's mean predicted
    home win probability is 0.558 against a real 0.551 (0.506 with the home term dropped).
    2023-24 is not used for the second check: its second-half home teams won only 50.2%.
    """
    rows = tsr.nba_possession_rate_rows(pl.read_parquet(RATES / "nba_possessions_2026.parquet"), _schedule("nba", 2026))
    games = _regular("nba", 2026)
    cut = dt.date(2026, 1, 16)
    done = games.filter(pl.col("game_date") < cut).select("game_id")
    model = sim_mod.as_of_game_model(rows.join(done, on="game_id", how="semi"))
    edge = float(model.margin(np.array([0.0]), np.array([0.0]))[0])
    assert edge == pytest.approx(2 * model.hfa * model.poss / 100)
    assert 1.5 < edge < 3.5, edge
    later = games.filter(pl.col("game_date") >= cut)
    net = dict(zip(model.ratings["team_id"].to_list(), model.ratings["adj_net_rtg"].to_list(), strict=True))
    m = model.margin(
        np.array([net[t] for t in later["home_team_id"].to_list()]),
        np.array([net[t] for t in later["away_team_id"].to_list()]),
    )
    pred = float(norm.cdf(m / model.margin_sd).mean())
    real = float((later["home_pts"] > later["away_pts"]).mean())
    assert abs(pred - real) < 0.02, (pred, real)


def _rows_2024() -> pl.DataFrame:
    return tsr.nba_possession_rate_rows(pl.read_parquet(RATES / "nba_possessions_2024.parquet"), _schedule("nba", 2024))


@pytest.fixture(scope="module")
def nba_2024_final() -> pl.DataFrame:
    """Every game played; fixed ratings, so the play-in games are independent and closed-form."""
    games = _regular("nba", 2024).select("season", "game_id", "home_team_id", "away_team_id", "home_pts", "away_pts")
    return sim_mod.simulate_pro_season(
        games, sim_mod.as_of_game_model(_rows_2024()), n_sims=20_000, seed=11, rating_uncertainty=False
    )


def test_play_in_is_simulated_with_the_higher_seed_at_home(nba_2024_final: pl.DataFrame) -> None:
    """With every game played, the play-in odds are closed-form in the game model."""
    sim = nba_2024_final
    model = sim_mod.as_of_game_model(_rows_2024())
    net = dict(zip(model.ratings["team_id"].to_list(), model.ratings["adj_net_rtg"].to_list(), strict=True))
    for conf in ("East", "West"):
        by_seed = {
            r["p_seed_7"] * 7 + r["p_seed_8"] * 8 + r["p_seed_9"] * 9 + r["p_seed_10"] * 10: r
            for r in sim.filter((pl.col("conference") == conf) & (pl.col("p_play_in") == 1.0)).iter_rows(named=True)
        }
        assert sorted(by_seed) == [7.0, 8.0, 9.0, 10.0]
        t = {k: by_seed[float(k)]["team_id"] for k in (7, 8, 9, 10)}

        def c(x: int, y: int) -> float:
            """P(seed x beats seed y at x's home)."""
            mu = float(model.margin(np.array([net[t[x]]]), np.array([net[t[y]]]))[0])
            return float(norm.cdf(mu / model.margin_sd))

        a, b = c(7, 8), c(9, 10)
        want = {
            7: a + (1 - a) * (b * c(7, 9) + (1 - b) * c(7, 10)),
            8: (1 - a) + a * (b * c(8, 9) + (1 - b) * c(8, 10)),
            9: b * (a * (1 - c(8, 9)) + (1 - a) * (1 - c(7, 9))),
            10: (1 - b) * (a * (1 - c(8, 10)) + (1 - a) * (1 - c(7, 10))),
        }
        assert sum(want.values()) == pytest.approx(2.0)
        for k, p in want.items():
            got = by_seed[float(k)]["p_playoffs"]
            assert abs(got - p) < 4 * np.sqrt(p * (1 - p) / 20_000) + 0.005, (conf, k, got, p)
    top = sim.filter(
        pl.col("p_seed_1")
        + pl.col("p_seed_2")
        + pl.col("p_seed_3")
        + pl.col("p_seed_4")
        + pl.col("p_seed_5")
        + pl.col("p_seed_6")
        == 1.0
    )
    assert top.height == 12 and top["p_playoffs"].to_list() == [1.0] * 12


def test_nba_2024_lottery_splits_tied_chances(nba_2024_final: pl.DataFrame) -> None:
    p1 = dict(zip(nba_2024_final["team_id"].to_list(), nba_2024_final["p_pick_1"].to_list(), strict=True))
    n = 20_000
    for team, want in ((DET, 0.140), (WAS_NBA, 0.140), (CHA, 0.1325), (POR, 0.1325), (SAS, 0.105)):
        assert abs(p1[team] - want) < 4 * np.sqrt(want * (1 - want) / n), (team, p1[team], want)
    assert nba_2024_final["p_lottery"].sum() == pytest.approx(14.0)
    assert nba_2024_final["p_pick_1"].sum() == pytest.approx(1.0)
    assert nba_2024_final["p_drawn_pick"].sum() == pytest.approx(4.0)


def test_wnba_lottery_uses_two_season_records() -> None:
    sim = _sim("wnba", 2025, n_sims=20_000, seed=5)
    p1 = dict(zip(sim["team_id"].to_list(), sim["p_pick_1"].to_list(), strict=True))
    n = 20_000
    for team, want in ((DAL, 0.420), (CHI, 0.261), (LA, 0.167), (WAS, 0.097), (CONN, 0.055)):
        assert abs(p1[team] - want) < 4 * np.sqrt(want * (1 - want) / n), (team, p1[team], want)
    assert sim["p_playoffs"].sum() == pytest.approx(8.0)
    assert sim["p_division"].null_count() == sim.height


GATES = {  # (league, season, as_of): (playoff Brier gate, wins MAE gate, pace-baseline MAE)
    ("nba", 2024, dt.date(2024, 1, 19)): (0.0785, 3.577, 3.364),
    ("nba", 2026, dt.date(2026, 1, 16)): (0.1029, 4.714, 4.458),
    ("wnba", 2025, dt.date(2025, 7, 16)): (0.0838, 4.001, 4.015),
}


@pytest.mark.parametrize("key", list(GATES))
def test_mid_season_sim_against_the_final_standings(key: tuple[str, int, dt.date]) -> None:
    lg, season, as_of = key
    brier_gate, mae_gate, pace_mae = GATES[key]
    sched = _schedule(lg, season)
    assert (
        sim_mod._regular_games(sched, None)["game_date"].sort()[
            sched.filter(pl.col("season_type") == "regular-season").height // 2
        ]
        == as_of
    )
    sim = _sim(lg, season, as_of_date=as_of)
    final = nba_season_standings(_regular(lg, season), league_id="00" if lg == "nba" else "10")
    playoffs = set(
        sched.filter(pl.col("season_type") == "playoffs")
        .select(pl.concat_list("home_team_id", "away_team_id"))
        .to_series()
        .explode()
        .to_list()
    )
    j = sim.join(final.select("team_id", pl.col("wins").alias("final_wins")), on="team_id", how="inner", validate="1:1")
    assert j.height == sim.height == final.height
    made = j["team_id"].is_in(list(playoffs)).cast(pl.Float64).to_numpy()
    brier = float(((j["p_playoffs"].to_numpy() - made) ** 2).mean())
    mae = float((j["proj_wins"] - j["final_wins"]).abs().mean())
    assert brier <= brier_gate, brier
    assert mae <= mae_gate, mae
    assert mae < pace_mae, (mae, pace_mae)
    # the Monte Carlo error is stated and small
    assert j["n_sims"].unique().to_list() == [10_000]
    assert float(j["p_playoffs_se"].max()) <= 0.005


def test_seeded_and_reproducible() -> None:
    a = _sim("wnba", 2025, as_of_date=dt.date(2025, 7, 16), n_sims=2_000, seed=3)
    b = _sim("wnba", 2025, as_of_date=dt.date(2025, 7, 16), n_sims=2_000, seed=3)
    c = _sim("wnba", 2025, as_of_date=dt.date(2025, 7, 16), n_sims=2_000, seed=4)
    assert a.equals(b)
    assert not a.equals(c)
    assert float((a["p_playoffs"] - c["p_playoffs"]).abs().max()) < 6 * float(a["p_playoffs_se"].max()) + 1e-9
    se = np.sqrt(a["p_playoffs"].to_numpy() * (1 - a["p_playoffs"].to_numpy()) / 2_000)
    np.testing.assert_allclose(a["p_playoffs_se"].to_numpy(), se)
    # a probability row sums to one over the seeds
    seeds = a.select([f"p_seed_{k}" for k in range(1, 16)]).sum_horizontal().to_numpy()
    np.testing.assert_allclose(seeds, 1.0)


def test_rejects_mismatched_id_dtypes() -> None:
    games = _regular("wnba", 2025).select(
        "season",
        "game_id",
        pl.col("home_team_id").cast(pl.Utf8),
        pl.col("away_team_id").cast(pl.Utf8),
        "home_pts",
        "away_pts",
    )
    model = sim_mod.as_of_game_model(pl.DataFrame(schema=RATE_ROWS_SCHEMA), league_id="10")
    with pytest.raises(TypeError, match="home_team_id"):
        sim_mod.simulate_pro_season(games, model, league_id="10", n_sims=10)
    with pytest.raises(TypeError, match="Int64"):
        nba_season_standings(games, league_id="10")


def test_tied_lottery_slots_share_their_chances() -> None:
    """NBA 2023-24: CHA and POR finished 21-61, so slots 3 and 4 split 140 + 125."""
    st = nba_season_standings(_regular("nba", 2024)).sort("win_pct")
    worst = st.head(14)
    order, chances = lottery_slots(
        worst["win_pct"].to_numpy(), np.zeros(14), NBA_LOTTERY_CHANCES, np.random.default_rng(0)
    )
    by_team = dict(zip(worst["team_id"].to_numpy()[order].tolist(), chances.tolist(), strict=True))
    assert by_team[CHA] == by_team[POR] == 132.5
    assert (by_team[DET], by_team[WAS_NBA], by_team[SAS]) == (140.0, 140.0, 105.0)
    assert sum(by_team.values()) == 1000.0


def test_rating_shift_widens_the_season_spread() -> None:
    """The per-season rating shift is applied: every team's win sd grows (2025-26 midseason: x1.28+)."""
    sched = _schedule("nba", 2026)
    rows = tsr.nba_possession_rate_rows(pl.read_parquet(RATES / "nba_possessions_2026.parquet"), sched)
    games = sim_mod._regular_games(sched, dt.date(2026, 1, 16))
    done = games.filter(pl.col("home_pts").is_not_null()).select("game_id")
    model = sim_mod.as_of_game_model(rows.join(done, on="game_id", how="semi"))
    cols = games.select("season", "game_id", "home_team_id", "away_team_id", "home_pts", "away_pts")
    shift = sim_mod.simulate_pro_season(cols, model, n_sims=2_000, seed=1)
    fixed = sim_mod.simulate_pro_season(cols, model, n_sims=2_000, seed=1, rating_uncertainty=False)
    ratio = shift.join(fixed, on="team_id", suffix="_fixed", validate="1:1").select(
        pl.col("proj_wins_sd") / pl.col("proj_wins_sd_fixed")
    )
    assert float(ratio.min().item()) > 1.15


def test_lottery_tie_shares_chances_whatever_the_tiebreak() -> None:
    """A tie on the lottery record splits the chances evenly; the next key only orders the slots.

    WNBA rule (CBS on the 2027 lottery): CONN 22-66 and TOR 11-33 tie at .250, so their
    combinations are pooled and divided evenly; the current-season record decides only the order
    (and the odd extra combination, which a mean ignores). Here the current records differ.
    """
    comb = np.array([22 / 88, 11 / 44, 26 / 88, 31 / 88])
    current = np.array([0.30, 0.25, 0.25, 0.35])
    order, chances = lottery_slots(comb, current, [442, 276, 178, 104], np.random.default_rng(0))
    assert order.tolist() == [1, 0, 2, 3]  # TOR's worse current record goes first
    assert chances.tolist() == [359.0, 359.0, 178.0, 104.0]


def test_rejects_bad_draw_counts_and_chances() -> None:
    games = _regular("wnba", 2025).select("season", "game_id", "home_team_id", "away_team_id", "home_pts", "away_pts")
    model = sim_mod.as_of_game_model(pl.DataFrame(schema=RATE_ROWS_SCHEMA), league_id="10")
    with pytest.raises(ValueError, match="n_sims"):
        sim_mod.simulate_pro_season(games, model, league_id="10", n_sims=0)
    for bad in ([420, 261, 167, 97], [420, 261, 167, 97, -55], [420, 261, 167, 97, float("nan")]):
        with pytest.raises(ValueError, match="chances"):
            sim_mod.simulate_pro_season(games, model, league_id="10", n_sims=10, chances=bad)
    for bad in ([140, 140, 140, float("inf")], [140, 140, 140, float("nan")]):
        with pytest.raises(ValueError, match="finite"):
            nba_draft_lottery_odds(chances=bad)


def test_wnba_binding_forwards_custom_chances() -> None:
    """``chances`` reaches the sim through ``wnba_season_sim`` (the 2027 lottery has no table)."""
    sim = _sim("wnba", 2025, n_sims=500, seed=2, chances=[442, 276, 178, 104, 55])
    assert sim["p_pick_1"].sum() == pytest.approx(1.0)
    assert dict(zip(sim["team_id"].to_list(), sim["p_pick_1"].to_list(), strict=True))[CONN] < 0.12


def test_tbd_cup_knockout_games_are_not_simulated() -> None:
    """2026-27 as released on 2026-11-03: the Cup knockout games list TBD teams (``team_id`` 0).

    They used to reach ``team_labels`` and fail every NBA sim until December. Each team's 80 known
    games are simulated, as the module docstring says.
    """
    sched = _schedule("nba", 2027)
    assert sched.filter(pl.col("home_team_id") == 0).height == 7  # six knockout games + the final
    games = sim_mod._regular_games(sched, dt.date(2026, 11, 3))
    assert games.height == 1200 and games.filter(pl.col("away_team_id") == 0).is_empty()
    rows = tsr.nba_possession_rate_rows(pl.read_parquet(RATES / "nba_possessions_2026.parquet"), _schedule("nba", 2026))
    cols = games.select("season", "game_id", "home_team_id", "away_team_id", "home_pts", "away_pts")
    out = sim_mod.simulate_pro_season(cols, sim_mod.as_of_game_model(rows), n_sims=200, seed=1)
    assert out.height == 30
    assert set((out["wins"] + out["losses"] + out["games_remaining"]).to_list()) == {80}


# --- the preseason prior (NBA 2025-26 and WNBA 2025, prior from the committed season before) ------


def _rows(lg: str, year: int) -> pl.DataFrame:
    """Regular-season possession rows of a committed season (end-year named)."""
    name = f"{lg}_possessions_{year}.parquet"
    path = next(d / name for d in (PRO, RATES) if (d / name).exists())
    lid = "00" if lg == "nba" else "10"
    return tsr.nba_possession_rate_rows(pl.read_parquet(path), _schedule(lg, year), league_id=lid)


def _prior(lg: str, season: int) -> sim_mod.PreseasonPrior:
    teams = sorted(set(_regular(lg, season)["home_team_id"].to_list()))
    pre = sim_mod.preseason_prior(_rows(lg, season - 1), teams, league_id="00" if lg == "nba" else "10")
    assert pre is not None
    return pre


def _early_log_loss(lg: str, season: int, prior: sim_mod.PreseasonPrior | None, first: int = 0) -> float:
    """Weeks ``first``-6: ratings as of each weekly cutoff from the first game, every later game scored."""
    lid = "00" if lg == "nba" else "10"
    games = _regular(lg, season).filter(pl.col("home_pts").is_not_null())
    rows = _rows(lg, season).join(games.select("game_id", "game_date"), on="game_id", how="inner", validate="m:1")
    start = games["game_date"].min()
    losses = []
    for week in range(first, 7):
        cut = start + dt.timedelta(days=7 * week)
        model = sim_mod.as_of_game_model(
            rows.filter(pl.col("game_date") < cut).drop("game_date"), league_id=lid, prior=prior
        )
        net = dict(zip(model.ratings["team_id"].to_list(), model.ratings["adj_net_rtg"].to_list(), strict=True))
        fut = games.filter(pl.col("game_date") >= cut)
        m = model.margin(
            np.array([net.get(t, 0.0) for t in fut["home_team_id"].to_list()]),
            np.array([net.get(t, 0.0) for t in fut["away_team_id"].to_list()]),
        )
        p = np.clip(norm.cdf(m / model.margin_sd), 1e-15, 1 - 1e-15)
        y = (fut["home_pts"] > fut["away_pts"]).to_numpy()
        losses.append(-(y * np.log(p) + (~y) * np.log(1 - p)))
    return float(np.concatenate(losses).mean())


# Observed 2026-11-03 (weeks 0-6 log loss, with / without the prior): NBA 2025-26 0.6349 / 0.6396,
# WNBA 2025 0.6557 / 0.6594. Gate: observed + 0.01, and the prior at least half the observed gain.
EARLY_GATES = {("nba", 2026): (0.6449, 0.0023), ("wnba", 2025): (0.6657, 0.0018)}


@pytest.mark.parametrize("key", list(EARLY_GATES))
def test_preseason_prior_improves_early_season_log_loss(key: tuple[str, int]) -> None:
    lg, season = key
    with_prior = _early_log_loss(lg, season, _prior(lg, season))
    without = _early_log_loss(lg, season, None)
    gate, gain = EARLY_GATES[key]
    assert with_prior <= gate, (with_prior, without)
    assert without - with_prior >= gain, (with_prior, without)


def test_preseason_model_is_the_regressed_prior() -> None:
    """Before the first game: last season's ratings x the factor, the expansion team at the quantile."""
    pre = _prior("wnba", 2025)
    reg, q = sim_mod.PRIOR_CONSTANTS["10"]
    final = possession_ridge_ratings(_rows("wnba", 2024), lam=RIDGE_LAMBDA["10"])
    new = pre.ratings.filter(pl.col("new_team"))
    assert new["team_id"].to_list() == [GSV]  # the 2025 expansion Valkyries
    want = float(final["adj_net_rtg"].quantile(q, interpolation="linear"))
    assert float((new["off_dev"] - new["def_dev"]).item()) == pytest.approx(want)
    old = pre.ratings.filter(~pl.col("new_team")).join(final, on="team_id", validate="1:1")
    np.testing.assert_allclose(
        (old["off_dev"] - old["def_dev"]).to_numpy(),
        reg * (old["adj_net_rtg"] - final["adj_net_rtg"].mean()).to_numpy(),
    )
    model = sim_mod.as_of_game_model(_rows("wnba", 2025).head(0), league_id="10", prior=pre)
    assert model.hfa == pre.hfa != 0.0 and model.poss == pre.poss
    assert model.ratings.height == 13 and model.ratings["adj_net_rtg"].std() > 1.0
    # a preseason sim is no longer every team at .500
    sim = _sim("wnba", 2025, as_of_date=dt.date(2025, 5, 1), n_sims=500, seed=1)
    assert float(sim["proj_wins"].max() - sim["proj_wins"].min()) > 8.0


def test_in_season_ridge_shrinks_toward_the_prior() -> None:
    """A week into 2025-26 the as-of ratings are the ridge toward last season, not toward average."""
    pre = _prior("nba", 2026)
    games = _regular("nba", 2026)
    cut = games["game_date"].min() + dt.timedelta(days=7)
    done = games.filter(pl.col("game_date") < cut).select("game_id")
    rows = _rows("nba", 2026).join(done, on="game_id", how="semi")
    lam = sim_mod.SIM_CONSTANTS["00"][0]
    model = sim_mod.as_of_game_model(rows, prior=pre)
    want = possession_ridge_ratings(rows, lam=lam, prior=pre.ratings)
    got = model.ratings.join(want.select("team_id", pl.col("adj_net_rtg").alias("w")), on="team_id", validate="1:1")
    assert got.height == 30
    np.testing.assert_allclose(got["adj_net_rtg"].to_numpy(), got["w"].to_numpy(), atol=1e-9)
    prior_net = pre.ratings.select("team_id", (pl.col("off_dev") - pl.col("def_dev")).alias("p"))
    flat = possession_ridge_ratings(rows, lam=lam).join(prior_net, on="team_id", validate="1:1")
    toward = got.join(prior_net, on="team_id", validate="1:1")
    assert float((toward["adj_net_rtg"] - toward["p"]).abs().mean()) < 0.7 * float(
        (flat["adj_net_rtg"] - flat["p"]).abs().mean()
    )
    # an overwhelming penalty returns the prior itself
    far = possession_ridge_ratings(rows, lam=1e9, prior=pre.ratings).join(prior_net, on="team_id", validate="1:1")
    np.testing.assert_allclose(far["adj_net_rtg"].to_numpy(), far["p"].to_numpy(), atol=1e-3)
