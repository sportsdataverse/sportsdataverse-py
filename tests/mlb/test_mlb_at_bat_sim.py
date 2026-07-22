"""Gates for the MLB at-bat sim — real statsapi fixture oracles only."""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mlb.mlb_at_bat_sim import (
    simulate_mlb_game_pbp,
    AB_OUTCOMES,
    at_bats_from_pbp,
    build_at_bat_pmf,
    simulate_mlb_ensemble,
    simulate_mlb_game,
)

FIXTURE = pathlib.Path("tests/fixtures/mlb_api/play_by_play_745282.json")


@pytest.fixture(scope="module")
def at_bats() -> pl.DataFrame:
    return at_bats_from_pbp(json.loads(FIXTURE.read_text(encoding="utf-8")))


def test_conservation_run_deltas_reconstruct_real_final(at_bats: pl.DataFrame) -> None:
    """THE oracle gate: per-play score deltas rebuild the real 6-1 final."""
    assert int(at_bats["runs_scored"].sum()) == 7
    assert int(at_bats["away_score"][-1]) == 6
    assert int(at_bats["home_score"][-1]) == 1
    # cumulative consistency: deltas resum to the cumulative columns
    assert int(at_bats["runs_scored"].cum_sum()[-1]) == int(at_bats["away_score"][-1] + at_bats["home_score"][-1])


def test_classifier_vocabulary(at_bats: pl.DataFrame) -> None:
    seen = set(at_bats["outcome"].unique().to_list())
    assert seen <= set(AB_OUTCOMES)
    # the real game covers the core classes, incl. its two real GIDPs
    assert {"so", "bb", "single", "hr", "out_inplay", "gidp"} <= seen


def test_pmf_is_distribution(at_bats: pl.DataFrame) -> None:
    pmf = build_at_bat_pmf(at_bats)
    assert sum(pmf.probs.values()) == pytest.approx(1.0)
    assert pmf.meta["n_plays"] == at_bats.height
    with pytest.raises(ValueError, match="no at-bats"):
        build_at_bat_pmf(at_bats.head(0))


def test_game_never_ties_and_extras_resolve(at_bats: pl.DataFrame) -> None:
    pmf = build_at_bat_pmf(at_bats)
    rng = np.random.default_rng(29)
    for _ in range(200):
        away, home = simulate_mlb_game(pmf, rng)
        assert away != home
        assert away >= 0 and home >= 0


def test_ensemble_deterministic_and_calibrated(at_bats: pl.DataFrame) -> None:
    pmf = build_at_bat_pmf(at_bats)
    a = simulate_mlb_ensemble(pmf, n_sim=400, seed=31)
    b = simulate_mlb_ensemble(pmf, n_sim=400, seed=31)
    assert np.array_equal(a["away"], b["away"])
    assert np.array_equal(a["home"], b["home"])
    # calibration sanity vs the real fixture game (7 total runs): the sim
    # plays BOTH sides with the same PMF, so totals sit near 2x one side's
    # expectation — assert a sane MLB-scoring window, not a point match
    assert 3.0 < a["mean_total"] < 14.0
    assert 0.0 < a["win_prob_home"] < 1.0


def test_ensemble_prices_with_odds_math(at_bats: pl.DataFrame) -> None:
    from sportsdataverse.odds.odds_math import prob_over

    pmf = build_at_bat_pmf(at_bats)
    ens = simulate_mlb_ensemble(pmf, n_sim=400, seed=37)
    p = prob_over(ens["total"], float(np.median(ens["total"])) - 0.5)
    assert 0.25 < p < 0.75


def test_full_pbp_emission(at_bats: pl.DataFrame) -> None:
    pmf = build_at_bat_pmf(at_bats)
    (away, home), pbp = simulate_mlb_game_pbp(pmf, np.random.default_rng(41))
    assert len(pbp) > 40
    assert pbp[-1]["away_score"] == away
    assert pbp[-1]["home_score"] == home
    assert sum(r["runs_on_play"] for r in pbp) == away + home
    innings = {r["inning"] for r in pbp}
    assert set(range(1, 10)) <= innings or max(innings) >= 9
    assert {r["outcome"] for r in pbp} <= set(AB_OUTCOMES)


def test_event_distribution_within_reason(at_bats: pl.DataFrame) -> None:
    pmf = build_at_bat_pmf(at_bats)
    ens = simulate_mlb_ensemble(pmf, n_sim=200, seed=43, collect_event_counts=True)
    counts = ens["event_counts"]
    assert counts is not None
    sim_ab_per_game = float(sum(counts[o].mean() for o in AB_OUTCOMES))
    # real fixture game had 73 plate events; both sim halves draw the same PMF
    assert sim_ab_per_game == pytest.approx(at_bats.height, rel=0.35)
    for outcome in AB_OUTCOMES:
        sim_share = float(counts[outcome].mean()) / sim_ab_per_game
        assert abs(sim_share - pmf.probs[outcome]) < 0.08, outcome
