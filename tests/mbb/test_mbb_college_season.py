"""BB-D17 college season products on the real 2024 seasons (men's and women's), and the preseason prior.

Oracles (all committed, see tests/fixtures/bb_college_season/README.md):

* WarrenNolan's end-of-season 2024 RPI and its RPI-quadrant records, and its NET mirror.
* The actual 2024 NCAA tournament fields and seeds (``*_prediction/ncaa_tourney_2024``).

The 2025 seasons carry the preseason prior from the committed 2024 season, as the producers do;
the 2024 seasons are rated without one (2023 is not committed).

Every gate is the value observed when the products landed (2026-11-03) less a stated margin.
Raise a gate when the model improves; never lower one.
"""

from __future__ import annotations

import datetime

import numpy as np
import polars as pl
import pytest

from scipy.stats import norm

from sportsdataverse.mbb.mbb_college_season import (
    _d1_games,
    college_teams,
    game_noise_sd,
    rating_shift_sd,
    season_bracketology,
    season_inputs,
    season_prior,
    simulate_season,
    split_season,
)
from sportsdataverse.mbb.mbb_prediction_constants import PRESEASON_PRIOR, get_constants, spearman_corr
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency
from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule

FIX = "tests/fixtures"
SELECTION = datetime.date(2024, 3, 18)  # Selection Sunday was 2024-03-17: every game before the 18th
FEB1 = datetime.date(2024, 2, 1)
# Southern Indiana (ESPN 88) was reclassifying to Division I in 2023-24 and ineligible.
INELIGIBLE = {"88"}
LEAGUE = {"mbb": "mens", "wbb": "womens"}
# One 2024 men's game (401600077) ships a box score with 0 turnovers; the ratings engine drops it
# and says so. The women's 2024 box has none.
pytestmark = pytest.mark.filterwarnings("ignore:raw_game_efficiency.*0 turnovers:UserWarning")

# (observed, gate) per league; the margin is stated beside each gate.
GATES = {
    "mbb": {
        "rpi_spearman": 0.998,  # observed 0.9999
        "rpi_rank_mae": 0.8,  # observed 0.54
        "rpi_within2": 0.92,  # observed 0.950
        "quad_cells": 0.97,  # observed 0.9779 (1416 / 1448)
        "net_spearman": 0.994,  # observed 0.9960
        "field": 63,  # observed 64 / 68
        "at_large": 31,  # observed 32 / 36
        "exact_seeds": 31,  # observed 34
        "seed_mae": 0.85,  # observed 0.750
        "paymon": 270,  # observed 278 / 340
        "bid_brier_feb1": 0.090,  # observed 0.0801 with the rating noise, 0.0810 without (baseline 0.1526)
    },
    "wbb": {
        "rpi_spearman": 0.998,  # observed 0.9996
        "rpi_rank_mae": 1.5,  # observed 1.18 (18 teams' ESPN D1 records differ from the oracle's)
        "rpi_within2": 0.86,  # observed 0.892
        "quad_cells": 0.965,  # observed 0.9736 (1402 / 1440)
        "net_spearman": 0.995,  # observed 0.9975 (out of sample: the NET weights were fit on the men)
        "field": 63,  # observed 64 / 68
        "at_large": 31,  # observed 32 / 36
        "exact_seeds": 31,  # observed 34
        "seed_mae": 0.80,  # observed 0.672
        "paymon": 270,  # observed 280 / 340
        "bid_brier_feb1": 0.075,  # observed 0.0652 with the rating noise, 0.0653 without (baseline 0.1532)
    },
}


def _load(lg: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    sched = pl.read_parquet(f"{FIX}/bb_college_season/{lg}_schedule_2024.parquet")
    box = pl.read_parquet(f"{FIX}/{lg}_prediction/team_box_2024.parquet")
    return sched, box


@pytest.fixture(scope="module", params=["mbb", "wbb"])
def lg(request):
    return request.param


@pytest.fixture(scope="module")
def corpus(lg):
    sched, box = _load(lg)
    league = LEAGUE[lg]
    return {
        "lg": lg,
        "league": league,
        "sched": sched,
        "box": box,
        "wn": pl.read_parquet(f"{FIX}/bb_college_season/{lg}_warrennolan_2024.parquet"),
        "seeds": pl.read_parquet(f"{FIX}/{lg}_prediction/ncaa_tourney_2024.parquet")
        .group_by("team_id")
        .agg(pl.col("seed").min()),
        "full": season_inputs(sched, box, league=league, ineligible=INELIGIBLE),
        "selection": season_inputs(sched, box, league=league, as_of_date=SELECTION, ineligible=INELIGIBLE),
    }


def test_division_one_teams_and_conferences(corpus):
    teams = corpus["full"].teams
    expected = {"mbb": 362, "wbb": 360}[corpus["lg"]]  # WarrenNolan's 2024 Division I counts
    assert teams.height == expected
    assert teams.filter(pl.col("conference").is_not_null()).get_column("conference_id").n_unique() == 32
    if corpus["lg"] == "wbb":
        # ESPN leaves Dayton's conference id null; its A-10 conference games recover it
        assert teams.filter(pl.col("team_id") == "2168").get_column("conference").to_list() == ["A-10"]
    assert teams.schema["team_id"] == pl.Utf8


def test_rpi_matches_the_warrennolan_rpi(corpus):
    g = GATES[corpus["lg"]]
    r = corpus["full"].resume.join(corpus["wn"].select("team_id", pl.col("rpi_rank").alias("oracle")), on="team_id")
    assert r.schema["rpi_rank"] == pl.Float64
    assert r.height == corpus["full"].teams.height  # every ESPN Division I team is on the oracle
    d = (r["rpi_rank"] - r["oracle"]).abs()
    rho = spearman_corr(r["rpi_rank"].to_numpy(), r["oracle"].to_numpy())
    assert rho >= g["rpi_spearman"], rho
    assert d.mean() <= g["rpi_rank_mae"], d.mean()
    assert (d <= 2).mean() >= g["rpi_within2"], (d <= 2).mean()


def test_rpi_components_are_bounded_and_weighted(corpus):
    r = corpus["full"].resume.drop_nulls("rpi")
    for c in ("rpi_wp", "rpi_owp", "rpi_oowp", "rpi"):
        assert r[c].min() >= 0.0 and r[c].max() <= 1.0, c
    from sportsdataverse.mbb.mbb_college_season import RPI_WEIGHTS

    a, b, c = RPI_WEIGHTS  # the published components add up to the published rpi
    blend = a * r["rpi_wp"] + b * r["rpi_owp"] + c * r["rpi_oowp"]
    assert float((blend - r["rpi"]).abs().max()) < 1e-12


def test_quads_reproduce_the_oracle_quadrant_records(corpus):
    """Quads from the oracle's own RPI ranks must give its quadrant W-L, cell for cell.

    WarrenNolan's RPI quadrants use the men's NET ranges for both leagues, so both run with
    ``league="mens"`` here; the women's tables use the women's ranges.
    """
    full = corpus["full"]
    rk = full.resume.select("season", "team_id", "adj_em").join(
        corpus["wn"].select("team_id", pl.col("rpi_rank").alias("rank")), on="team_id"
    )
    assert rk.schema["team_id"] == full.played.schema["home_team_id"]
    q = strength_of_schedule(_d1_games(full.played, full.teams), rk, league="mens").join(corpus["wn"], on="team_id")
    cells = [
        x[f"q{k}"] == f"{x[f'quad{k}_w']}-{x[f'quad{k}_l']}" for x in q.iter_rows(named=True) for k in (1, 2, 3, 4)
    ]
    assert np.mean(cells) >= GATES[corpus["lg"]]["quad_cells"], np.mean(cells)


def test_net_estimate_tracks_the_net(corpus):
    r = corpus["full"].resume.join(corpus["wn"].select("team_id", "net_rank"), on="team_id")
    rho = spearman_corr(r["net_estimate_rank"].to_numpy(), r["net_rank"].to_numpy())
    assert rho >= GATES[corpus["lg"]]["net_spearman"], rho


def test_resume_schema_and_rank_grammar(corpus):
    from sportsdataverse.mbb.mbb_college_season import RESUME_SCHEMA

    r = corpus["full"].resume
    assert dict(r.schema) == RESUME_SCHEMA
    assert r.get_column("team_id").is_unique().all()
    n = r.height
    for m in ("adj_em", "sos", "wab", "rpi", "net_estimate"):
        ranked = r.drop_nulls(f"{m}_rank")
        assert ranked.height >= n - 2, m
        assert ranked[f"{m}_rank"].min() == 1.0
        assert (ranked[f"{m}_pct"] < 100).all() and (ranked[f"{m}_pct"] > 0).all()
    assert (r["rpi_n"] == r["rpi_games"]).all()
    # the women's tables use the women's quad ranges
    from sportsdataverse.mbb.mbb_prediction_constants import get_constants

    assert get_constants(corpus["league"]).quad_thresholds["home"]["q1"] == {"mens": 30, "womens": 25}[corpus["league"]]


def test_auto_bids_are_the_conference_champions(corpus):
    sel = corpus["selection"]
    bids = sel.auto_bids
    assert bids.height == 32 and bids["champion"].all()
    field = set(corpus["seeds"]["team_id"])
    assert set(bids["team_id"]) <= field
    if corpus["lg"] == "wbb":
        # Southern Indiana won the OVC final; the runner-up UT Martin took the auto-bid
        ovc = bids.filter(pl.col("conference_id") == "20")["team_id"].to_list()
        assert ovc == ["2630"]


def test_auto_bids_before_the_finals_are_alive_conference_leaders(corpus):
    sched, box = corpus["sched"], corpus["box"]
    inp = season_inputs(
        sched, box, league=corpus["league"], as_of_date=datetime.date(2024, 3, 1), ineligible=INELIGIBLE
    )
    bids = inp.auto_bids.join(inp.resume, on=["season", "team_id"])
    assert bids.height == 32 and not bids["champion"].any()
    best = inp.resume.filter(pl.col("conference").is_not_null() & pl.col("eligible")).with_columns(
        (pl.col("conf_wins") / (pl.col("conf_wins") + pl.col("conf_losses"))).alias("p")
    )
    top = best.group_by("conference_id").agg(pl.col("p").max())
    j = bids.with_columns((pl.col("conf_wins") / (pl.col("conf_wins") + pl.col("conf_losses"))).alias("p")).join(
        top, on="conference_id", suffix="_top"
    )
    assert (j["p"] == j["p_top"]).all()


def test_bracketology_against_selection_sunday(corpus):
    g = GATES[corpus["lg"]]
    sel = corpus["selection"]
    br = season_bracketology(sel.resume, sel.auto_bids, league=corpus["league"])
    field = br.filter(pl.col("bid") == True)  # noqa: E712
    assert field.height == 68
    assert field.filter(pl.col("auto_bid") == True).height == 32  # noqa: E712
    seeds = corpus["seeds"]
    assert field.schema["team_id"] == seeds.schema["team_id"]
    both = field.join(seeds, on="team_id", how="inner")
    actual_al = seeds.join(sel.auto_bids, on="team_id", how="anti")
    our_al = field.filter(pl.col("auto_bid") == False)  # noqa: E712
    d = (both["projected_seed"] - both["seed"]).abs()
    paymon = 3 * both.height + 2 * int((d == 0).sum()) + int((d == 1).sum())
    assert both.height >= g["field"], both.height
    assert our_al.join(actual_al, on="team_id").height >= g["at_large"]
    assert int((d == 0).sum()) >= g["exact_seeds"], int((d == 0).sum())
    assert d.mean() <= g["seed_mae"], d.mean()
    assert paymon >= g["paymon"], paymon


def test_first_four_seed_structure_and_bubble(corpus):
    sel = corpus["selection"]
    br = season_bracketology(sel.resume, sel.auto_bids, league=corpus["league"])
    field = br.filter(pl.col("bid") == True)  # noqa: E712
    per_line = field.group_by("projected_seed").len().sort("projected_seed")
    assert per_line["projected_seed"].to_list() == list(range(1, 17))
    # 64 slots, four per line; two First Four pairs share the 16 line and two share an at-large line
    counts = sorted(per_line["len"].to_list())
    assert counts == [4] * 14 + [6, 6]
    assert per_line.filter(pl.col("projected_seed") == 16)["len"].item() == 6
    for label, in_field in (
        ("last_four_byes", True),
        ("last_four_in", True),
        ("first_four_out", False),
        ("next_four_out", False),
    ):
        rows = br.filter(pl.col("bubble") == label)
        assert rows.height == 4, label
        assert (rows["bid"] == in_field).all(), label
    lfi = br.filter(pl.col("bubble") == "last_four_in")
    assert lfi["at_large_rank"].sort().to_list() == [33, 34, 35, 36]
    # the last four in play in the First Four: they share their seed line in pairs
    assert lfi.group_by("projected_seed").len()["len"].min() >= 2


def test_ineligible_team_gets_no_bid(corpus):
    sel = corpus["selection"]
    br = season_bracketology(sel.resume, sel.auto_bids, league=corpus["league"])
    usi = br.filter(pl.col("team_id") == "88").row(0, named=True)
    assert usi["eligible"] is False and usi["bid"] is False


@pytest.fixture(scope="module")
def feb_sim(corpus):
    inp = season_inputs(corpus["sched"], corpus["box"], league=corpus["league"], as_of_date=FEB1, ineligible=INELIGIBLE)
    sim = simulate_season(
        inp.played, inp.remaining, inp.teams, inp.resume, league=corpus["league"], n_sims=2000, seed=0
    )
    return inp, sim


def test_season_sim_bid_probabilities_are_calibrated(corpus, feb_sim):
    inp, sim = feb_sim
    assert inp.remaining.height > 1500  # a real month of games left
    y = sim.join(corpus["seeds"].select("team_id", pl.lit(1.0).alias("y")), on="team_id", how="left").with_columns(
        pl.col("y").fill_null(0.0)
    )
    brier = float(((y["bid_prob"] - y["y"]) ** 2).mean())
    base = float(((y["y"].mean() - y["y"]) ** 2).mean())
    assert brier <= GATES[corpus["lg"]]["bid_brier_feb1"], brier
    assert brier <= 0.6 * base, (brier, base)


def test_season_sim_accounting(corpus, feb_sim):
    from sportsdataverse.mbb.mbb_college_season import SEASON_SIM_SCHEMA

    inp, sim = feb_sim
    assert dict(sim.schema) == SEASON_SIM_SCHEMA
    assert sim.height == inp.teams.height
    assert abs(sim["bid_prob"].sum() - 68.0) < 1e-9
    assert abs(sim["auto_bid_prob"].sum() - 32.0) < 1e-9
    seeds = sim.select([f"seed_{k}_prob" for k in range(1, 17)])
    assert np.allclose(seeds.sum_horizontal().to_numpy(), sim["bid_prob"].to_numpy())
    line = seeds.sum().to_numpy().ravel()
    assert abs(line[15] - 6.0) < 1e-9  # the two auto-bid First Four pairs
    assert (line[:15] >= 4.0 - 1e-9).all() and (line[:15] <= 6.0 + 1e-9).all()
    assert abs(float(seeds.sum().to_numpy().sum()) - 68.0) < 1e-9
    # every remaining game is played once: projected games = played + remaining
    games = (sim["proj_wins"] + sim["proj_losses"]) - (sim["wins"] + sim["losses"])
    assert games.min() >= 0
    left = (
        pl.concat(
            [
                inp.remaining.select(pl.col("home_team_id").alias("t")),
                inp.remaining.select(pl.col("away_team_id").alias("t")),
            ]
        )
        .group_by("t")
        .len()
    )
    j = (
        sim.select("team_id", games.alias("g"))
        .join(left, left_on="team_id", right_on="t", how="left")
        .with_columns(pl.col("len").fill_null(0))
    )
    assert np.allclose(j["g"].to_numpy(), j["len"].to_numpy())
    # one regular-season champion per conference at least (co-champions share)
    per_conf = (
        sim.filter(pl.col("conference").is_not_null())
        .group_by("conference_id")
        .agg(pl.col("reg_season_title_prob").sum())
    )
    assert (per_conf["reg_season_title_prob"] >= 1.0 - 1e-9).all()
    usi = sim.filter(pl.col("team_id") == "88").row(0, named=True)
    assert usi["bid_prob"] == 0.0 and usi["auto_bid_prob"] == 0.0
    # every unfinished tournament crowns one champion per sim, ineligible winners included
    titles = (
        sim.filter(pl.col("conference").is_not_null())
        .group_by("conference_id")
        .agg(pl.col("conf_tourney_title_prob").sum())
    )
    assert np.allclose(titles["conf_tourney_title_prob"].to_numpy(), 1.0)


def test_season_sim_is_seeded_and_reproducible(corpus, feb_sim):
    inp, sim = feb_sim
    again = simulate_season(
        inp.played, inp.remaining, inp.teams, inp.resume, league=corpus["league"], n_sims=2000, seed=0
    )
    assert again.equals(sim)
    other = simulate_season(
        inp.played, inp.remaining, inp.teams, inp.resume, league=corpus["league"], n_sims=2000, seed=1
    )
    assert not other.equals(sim)
    # 2000 sims: two seeds agree on bid odds to within Monte Carlo noise
    j = sim.join(other, on="team_id", suffix="_b")
    assert float((j["bid_prob"] - j["bid_prob_b"]).abs().max()) < 0.06


def test_split_season_leaves_conference_tournaments_to_the_sim(corpus):
    played, remaining = split_season(corpus["sched"], FEB1)
    assert played["date"].max() < FEB1 <= remaining["date"].min()
    assert remaining.filter((pl.col("type_abbreviation") == "TRNMNT") & pl.col("conference_competition")).height == 0
    assert remaining.filter(pl.col("season_type") != 2).height == 0


def test_season_sim_after_selection_sunday_is_the_bracket(corpus):
    """With every game played the sim has nothing to draw: its field is the bracketology field."""
    sel = corpus["selection"]
    sim = simulate_season(
        sel.played, sel.remaining.head(0), sel.teams, sel.resume, league=corpus["league"], n_sims=50, seed=3
    )
    br = season_bracketology(sel.resume, sel.auto_bids, league=corpus["league"])
    j = sim.join(br.select("team_id", "bid", "projected_seed"), on="team_id")
    assert ((j["bid_prob"] == 1.0) == j["bid"]).all()
    if corpus["lg"] == "wbb":
        ovc = sim.filter(pl.col("team_id").is_in(["88", "2630"])).sort("team_id")
        # Southern Indiana won the OVC title; UT Martin, the runner-up, holds the auto-bid
        assert ovc.select("team_id", "conf_tourney_title_prob", "auto_bid_prob").rows() == [
            ("2630", 0.0, 1.0),
            ("88", 1.0, 0.0),
        ]
    assert j.filter(pl.col("bid") == True).select((pl.col("seed_mean") == pl.col("projected_seed")).all()).item()  # noqa: E712


def test_auto_bids_mid_conference_tournaments(corpus):
    """As of 2024-03-13 some finals are played (11 men's, 14 women's) and the rest are under way."""
    inp = season_inputs(
        corpus["sched"],
        corpus["box"],
        league=corpus["league"],
        as_of_date=datetime.date(2024, 3, 13),
        ineligible=INELIGIBLE,
    )
    bids = inp.auto_bids
    assert bids.height == 32
    assert int(bids["champion"].sum()) == {"mbb": 11, "wbb": 14}[corpus["lg"]]
    final = season_inputs(
        corpus["sched"], corpus["box"], league=corpus["league"], as_of_date=SELECTION, ineligible=INELIGIBLE
    )
    champs = final.auto_bids.select("conference_id", pl.col("team_id").alias("final_champ"))
    j = bids.join(champs, on="conference_id")
    assert (j.filter(pl.col("champion"))["team_id"] == j.filter(pl.col("champion"))["final_champ"]).all()
    # a projected auto-bid has not lost a conference-tournament game
    ct = inp.played.filter((pl.col("type_abbreviation") == "TRNMNT") & pl.col("conference_competition"))
    losers = set(
        ct.select(
            pl.when(pl.col("home_score") > pl.col("away_score"))
            .then(pl.col("away_team_id"))
            .otherwise(pl.col("home_team_id"))
        )
        .to_series()
        .to_list()
    )
    assert not (set(bids.filter(~pl.col("champion"))["team_id"]) & losers)


# --- 2025: out of sample (the NET weights and the résumé blend were set on 2024) ---------------
# The 2025 inputs carry the preseason prior from the committed 2024 season, as the producers do.

GATES_2025 = {
    "mbb": {"field": 64, "at_large": 33, "exact_seeds": 36, "seed_mae": 0.75, "paymon": 283, "bid_brier_feb1": 0.072},
    # observed: field 65 / 68, at-large 34 / 37, exact 40, seed MAE 0.585, paymon 292, Brier 0.0626 (base 0.1519);
    # without the prior and the rating noise (BB-D17): exact 39, seed MAE 0.646, paymon 291, Brier 0.0644
    "wbb": {"bid_brier_feb1": 0.060},
    # observed Brier 0.0533 (base 0.1526; 0.0532 before); the field is not gated, see the women's test below
}


@pytest.fixture(scope="module")
def corpus_2025(lg):
    sched = pl.read_parquet(f"{FIX}/bb_college_season/{lg}_schedule_2025.parquet")
    box = pl.read_parquet(f"{FIX}/bb_college_season/{lg}_team_box_2025.parquet")
    seeds = pl.read_parquet(f"{FIX}/bb_college_season/{lg}_ncaa_seeds_2025.parquet")
    return sched, box, seeds


@pytest.fixture(scope="module")
def prior_2024(lg):
    """The 2024 season as the 2025 prior season: ``(schedule, team_box)``."""
    return _load(lg)


def test_2025_selection_sunday(lg, corpus_2025, prior_2024):
    sched, box, seeds = corpus_2025
    inp = season_inputs(sched, box, league=LEAGUE[lg], as_of_date=datetime.date(2025, 3, 17), prior_season=prior_2024)
    assert inp.auto_bids.height == 31  # the Pac-12 played 2024-25 with two members and no auto-bid
    champs = inp.auto_bids.filter(pl.col("champion"))
    # every conference whose final is in the release crowns a team that was in the field
    assert champs.join(seeds, on="team_id", how="anti").height == 0
    if lg == "wbb":
        # ESPN's 2025 women's schedule stops carrying conference-tournament games after 2025-03-10
        # (184 of ~300), so 20 auto-bids fall back to the projected leader; no field gate.
        assert champs.height == 11
        return
    g = GATES_2025[lg]
    assert champs.height == 31
    br = season_bracketology(inp.resume, inp.auto_bids, league=LEAGUE[lg])
    field = br.filter(pl.col("bid") == True)  # noqa: E712
    both = field.join(seeds, on="team_id")
    d = (both["projected_seed"] - both["seed"]).abs()
    actual_al = seeds.join(inp.auto_bids, on="team_id", how="anti")
    assert both.height >= g["field"], both.height
    assert field.filter(pl.col("auto_bid") == False).join(actual_al, on="team_id").height >= g["at_large"]  # noqa: E712
    assert int((d == 0).sum()) >= g["exact_seeds"]
    assert d.mean() <= g["seed_mae"], d.mean()
    assert 3 * both.height + 2 * int((d == 0).sum()) + int((d == 1).sum()) >= g["paymon"]


def test_2025_season_sim_bid_brier(lg, corpus_2025, prior_2024):
    sched, box, seeds = corpus_2025
    inp = season_inputs(sched, box, league=LEAGUE[lg], as_of_date=FEB1.replace(year=2025), prior_season=prior_2024)
    sim = simulate_season(inp.played, inp.remaining, inp.teams, inp.resume, league=LEAGUE[lg], n_sims=2000, seed=0)
    y = sim.join(seeds.select("team_id", pl.lit(1.0).alias("y")), on="team_id", how="left").with_columns(
        pl.col("y").fill_null(0.0)
    )
    brier = float(((y["bid_prob"] - y["y"]) ** 2).mean())
    assert brier <= GATES_2025[lg]["bid_brier_feb1"], brier


def test_preseason_tables_are_empty_not_errors(corpus):
    """Before the first game the daily tables still build: every team, no résumé, no field."""
    inp = season_inputs(corpus["sched"], corpus["box"], league=corpus["league"], as_of_date=datetime.date(2023, 10, 1))
    assert inp.played.height == 0 and inp.resume.height == inp.teams.height
    br = season_bracketology(inp.resume, inp.auto_bids, league=corpus["league"])
    assert br.height == inp.teams.height and not br["bid"].any()
    sim = simulate_season(inp.played, inp.remaining, inp.teams, inp.resume, league=corpus["league"], n_sims=50, seed=0)
    assert abs(sim["bid_prob"].sum() - 68.0) < 1e-9


def test_2025_rpi_and_net_estimate_out_of_sample(lg, corpus_2025, prior_2024):
    sched, box, _seeds = corpus_2025
    wn = pl.read_parquet(f"{FIX}/bb_college_season/{lg}_warrennolan_2025.parquet")
    r = season_inputs(sched, box, league=LEAGUE[lg], prior_season=prior_2024).resume.join(
        wn.select("team_id", pl.col("rpi_rank").alias("o_rpi"), pl.col("net_rank").alias("o_net")), on="team_id"
    )
    assert r.height == wn.height
    net = spearman_corr(r["net_estimate_rank"].to_numpy(), r["o_net"].to_numpy())
    em_only = spearman_corr(r["adj_em_rank"].to_numpy(), r["o_net"].to_numpy())
    # observed with the prior: men 0.9946 (AdjEM alone 0.9908), women 0.9931 (0.9922); without it
    # (BB-D17) 0.9962 (0.9938) and 0.9935 (0.9923). The NET has no prior; four pseudo-games of one
    # out of ~34 move the end-of-season AdjEM a little away from it.
    assert net >= {"mbb": 0.994, "wbb": 0.990}[lg], net
    assert net > em_only  # the WAB share earns its place out of sample
    if lg == "wbb":
        return  # 2025 women's release: 66% of D1 records differ from the oracle's (missing games)
    d = (r["rpi_rank"] - r["o_rpi"]).abs()
    assert d.mean() <= 0.5, d.mean()  # observed 0.26
    assert (d <= 2).mean() >= 0.95  # observed 0.978


# --- the preseason prior and the rating noise (2025, prior from the committed 2024 season) ---------

# Weeks 0-6 log loss of every later Division I regular-season game, ratings as of each weekly
# cutoff from the first game, with the shipped game model. Observed 2026-11-03: men 0.5807 with
# the prior against 0.7463 without, women 0.5094 against 0.6926. Gate: observed + 0.01, and the
# prior beats no prior by at least half the observed gap.
EARLY_GATES = {"mbb": (0.5907, 0.0828), "wbb": (0.5194, 0.0916)}


def _early_log_loss(sched: pl.DataFrame, box: pl.DataFrame, lg: str, prior_season) -> float:  # type: ignore[no-untyped-def]
    c = get_constants(LEAGUE[lg])
    teams = set(college_teams(sched)["team_id"].to_list())
    done = sched.filter(pl.col("completed") & pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null())
    scored = done.filter(
        (pl.col("season_type") == 2) & pl.col("home_team_id").is_in(teams) & pl.col("away_team_id").is_in(teams)
    )
    start = done["date"].min()
    losses = []
    for week in range(7):
        cut = start + datetime.timedelta(days=7 * week)
        res = season_inputs(sched, box, league=LEAGUE[lg], as_of_date=cut, prior_season=prior_season).resume
        em = dict(zip(res["team_id"].to_list(), res["adj_em"].to_list(), strict=True))
        rated = [v for v in em.values() if v is not None]
        fill = min(rated) if rated else 0.0  # the sim's rule: an unrated team plays as the weakest
        fut = scored.filter(pl.col("date") >= cut)
        dem = np.array(
            [
                (em[h] if em[h] is not None else fill) - (em[a] if em[a] is not None else fill)
                for h, a in fut.select("home_team_id", "away_team_id").iter_rows()
            ]
        )
        p = norm.cdf((c.em_scale * dem + c.hfa * (~fut["neutral_site"].fill_null(False).to_numpy())) / c.margin_sd)
        y = (fut["home_score"] > fut["away_score"]).to_numpy()
        p = np.clip(p, 1e-15, 1 - 1e-15)
        losses.append(-(y * np.log(p) + (~y) * np.log(1 - p)))
    return float(np.concatenate(losses).mean())


def test_preseason_prior_improves_early_season_log_loss(lg, corpus_2025, prior_2024):
    sched, box, _seeds = corpus_2025
    with_prior = _early_log_loss(sched, box, lg, prior_2024)
    without = _early_log_loss(sched, box, lg, None)
    gate, gain = EARLY_GATES[lg]
    assert with_prior <= gate, (with_prior, without)
    assert without - with_prior >= gain, (with_prior, without)


def test_preseason_prior_regresses_last_season_and_defaults_new_programs(lg, corpus_2025, prior_2024):
    """Last season's D-I teams carry ``regression`` x their final rating; programs new to D-I start at the quantile."""
    sched, box, _seeds = corpus_2025
    league, pp = LEAGUE[lg], PRESEASON_PRIOR[LEAGUE[lg]]
    teams = college_teams(sched)
    prior = season_prior(*prior_2024, teams, league=league)
    assert prior.height == teams.height and prior["team_id"].n_unique() == teams.height
    prev_sched, prev_box = prior_2024
    played, _ = split_season(prev_sched)
    final = adjust_efficiency(raw_game_efficiency(played, prev_box), league=league).join(
        college_teams(prev_sched).select("team_id"), on="team_id", how="semi"
    )
    new = prior.filter(pl.col("new_team"))
    assert new.height == 2, new  # 2024-25 newcomers to Division I: Mercyhurst, West Georgia
    want = float(final["adj_em"].quantile(pp.new_team_quantile, interpolation="linear"))
    np.testing.assert_allclose(new["off_dev"].to_numpy(), want / 2)
    np.testing.assert_allclose(new["def_dev"].to_numpy(), -want / 2)
    old = prior.filter(~pl.col("new_team")).join(final, on="team_id", validate="1:1")
    avg = float(raw_game_efficiency(played, prev_box)["off_eff"].mean())
    np.testing.assert_allclose(old["off_dev"].to_numpy(), pp.regression * (old["adj_o"].to_numpy() - avg))
    np.testing.assert_allclose(old["def_dev"].to_numpy(), pp.regression * (old["adj_d"].to_numpy() - avg))
    assert (prior["prior_avg"] == avg).all()
    # before the first game the ratings are the prior; a newcomer is rated, not left null
    pre = season_inputs(sched, box, league=league, as_of_date=datetime.date(2024, 10, 1), prior_season=prior_2024)
    r = pre.resume.join(prior, on="team_id", validate="1:1")
    assert r["adj_em"].null_count() == 0 and (r["games"] == 0).all()
    np.testing.assert_allclose(r["adj_em"].to_numpy(), (r["off_dev"] - r["def_dev"]).to_numpy(), atol=1e-9)


def test_first_week_sim_is_not_a_one_game_projection(lg, corpus_2025, prior_2024):
    """A week in, the shipped sim no longer projects one-game teams to run the table (BB-D17 did)."""
    sched, box, _seeds = corpus_2025
    league = LEAGUE[lg]
    start = split_season(sched)[0]["date"].min()
    cut = start + datetime.timedelta(days=7)
    out = {}
    for name, ps, noise in (("bb_d17", None, False), ("shipped", prior_2024, True)):
        inp = season_inputs(sched, box, league=league, as_of_date=cut, prior_season=ps)
        sim = simulate_season(
            inp.played, inp.remaining, inp.teams, inp.resume, league=league, n_sims=500, seed=0, rating_noise=noise
        )
        games = sim["proj_wins"] + sim["proj_losses"]
        out[name] = {
            "top_win_pct": float((sim["proj_wins"] / games).max()),
            "locks": int((sim["bid_prob"] >= 0.995).sum()),
        }
    # observed: the top projected winning pct men 0.997 -> 0.870, women 1.000 -> 0.906; teams at
    # a 99.5% bid men 27 -> 3, women 36 -> 5
    assert out["bb_d17"]["top_win_pct"] > 0.99, out
    assert out["shipped"]["top_win_pct"] < 0.95, out
    assert out["shipped"]["locks"] <= out["bb_d17"]["locks"] / 2, out


def test_rating_noise_spreads_the_odds_and_keeps_the_marginals(corpus, feb_sim):
    """The rating shift moves bid odds off 0 / 1 without moving the projected wins."""
    inp, noisy = feb_sim
    fixed = simulate_season(
        inp.played,
        inp.remaining,
        inp.teams,
        inp.resume,
        league=corpus["league"],
        n_sims=2000,
        seed=0,
        rating_noise=False,
    )
    j = noisy.join(fixed, on="team_id", suffix="_fixed", validate="1:1")
    assert float((j["proj_wins"] - j["proj_wins_fixed"]).abs().max()) < 0.6
    sure = lambda col: int(((j[col] < 0.001) | (j[col] > 0.999)).sum())  # noqa: E731
    assert sure("bid_prob") < sure("bid_prob_fixed"), (sure("bid_prob"), sure("bid_prob_fixed"))


def test_prior_edge_cases_are_explicit(prior_2024, corpus_2025):
    """A prior with no game needs a baseline; a prior season with no rated D-I team gives no prior."""
    from sportsdataverse._common.ratings import iterative_opponent_adjust
    from sportsdataverse.mbb.mbb_team_ratings import preseason_prior

    prev_sched, prev_box = prior_2024
    eff = raw_game_efficiency(split_season(prev_sched)[0], prev_box)
    cols = dict(team_col="team_id", opp_col="opp_team_id", off_col="off_eff", def_col="def_eff")
    cols |= dict(home_col="is_home", neutral_col="neutral_site", hfa=3.0)
    with pytest.raises(ValueError, match="baseline"):
        iterative_opponent_adjust(eff.head(0), **cols, prior={"1": (1.0, -1.0)}, prior_weight=4.0)
    out = iterative_opponent_adjust(eff.head(0), **cols, baseline=100.0, prior={"1": (1.0, -1.0)}, prior_weight=4.0)
    assert out.select("adj_off", "adj_def", "games").row(0) == (101.0, 99.0, 0)
    teams = college_teams(corpus_2025[0])
    nobody = college_teams(prev_sched).with_columns(pl.lit("none").alias("team_id")).head(1)
    assert preseason_prior(eff, teams, nobody).is_empty()


def test_game_noise_keeps_each_margin_at_margin_sd(lg):
    """The shift's variance comes out of each game's own noise: margin_sd ** 2 overall, floored at a quarter."""
    league = LEAGUE[lg]
    c = get_constants(league)
    tau = rating_shift_sd(np.array([0.0, 4.0, 30.0]), league)
    assert tau[0] > tau[1] > tau[2] > 0  # more games, less rating error
    zero = game_noise_sd(np.zeros(3), np.zeros(3), league)
    np.testing.assert_allclose(zero, c.margin_sd)
    sd = game_noise_sd(tau, tau[::-1], league)
    np.testing.assert_allclose(sd**2 + c.em_scale**2 * (tau**2 + tau[::-1] ** 2), c.margin_sd**2)
    big = game_noise_sd(np.array([1e3]), np.array([1e3]), league)
    np.testing.assert_allclose(big, 0.5 * c.margin_sd)
