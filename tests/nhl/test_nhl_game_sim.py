"""NHL event-stream sim gates — real SCF G7 fixture oracles."""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nhl.nhl_game_sim import (
    NHL_EVENTS,
    build_nhl_shelf,
    events_from_nhl_pbp,
    simulate_nhl_ensemble,
    simulate_nhl_game_pbp,
)

FIXTURE = pathlib.Path("tests/fixtures/nhl_api_web/pbp_2024_scf_g7.json")


@pytest.fixture(scope="module")
def payload() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def events(payload: dict) -> pl.DataFrame:
    return events_from_nhl_pbp(payload)


@pytest.fixture(scope="module")
def shelf(events: pl.DataFrame):
    return build_nhl_shelf(events)


def test_conservation_goals_reconstruct_real_final(events: pl.DataFrame) -> None:
    """THE oracle gate: classified goal rows rebuild the real 2-1 final."""
    goals = events.filter(pl.col("event") == "goal")
    assert goals.height == 3
    assert int(goals["goal_total"].max()) == 3
    assert int(goals["is_home"].sum()) == 2  # FLA (home) 2, EDM 1
    assert set(events["event"].unique().to_list()) <= set(NHL_EVENTS)


def test_strength_states_classified(events: pl.DataFrame) -> None:
    strengths = set(events["strength"].unique().to_list())
    assert "ev" in strengths
    assert {"pp", "sh"} & strengths  # the real PP segments classified


def test_shelf_pmfs_and_parameters(shelf) -> None:
    for key, pmf in {**shelf.event_pmfs, "all": shelf.all_pmf}.items():
        assert set(pmf) == set(NHL_EVENTS), key
        assert sum(pmf.values()) == pytest.approx(1.0), key
    assert 5.0 < shelf.seconds_per_event < 20.0
    assert all(0.0 <= v <= 1.0 for v in shelf.home_share.values())
    assert shelf.pp_goal_boost >= 1.0


def test_full_pbp_sim_structure(shelf) -> None:
    final, pbp = simulate_nhl_game_pbp(shelf, np.random.default_rng(13))
    again_final, again = simulate_nhl_game_pbp(shelf, np.random.default_rng(13))
    assert [r["event"] for r in pbp] == [r["event"] for r in again]  # deterministic
    assert len(pbp) > 200
    assert {1, 2, 3} <= {r["period"] for r in pbp}
    assert all(0 <= r["clock_seconds"] <= 1200 for r in pbp)
    totals = [r["score_home"] + r["score_away"] for r in pbp]
    assert totals == sorted(totals)  # goals only ever accumulate
    assert final.score_home != final.score_away  # OT/shootout resolves ties
    assert pbp[-1]["score_home"] == final.score_home
    assert pbp[-1]["score_away"] == final.score_away


def test_sim_distribution_within_reason(shelf, events: pl.DataFrame) -> None:
    ens = simulate_nhl_ensemble(shelf, n_sim=150, seed=17)
    b = simulate_nhl_ensemble(shelf, n_sim=150, seed=17)
    assert np.array_equal(ens["score_home"], b["score_home"])  # deterministic

    counts = ens["event_counts"]
    sim_events = sum(float(counts[e].mean()) for e in NHL_EVENTS)
    # pace: stream volume within reason of the real game's
    assert sim_events == pytest.approx(events.height, rel=0.15)
    # shots on goal near the real 42
    assert float(counts["shot_on_goal"].mean()) == pytest.approx(
        events.filter(pl.col("event") == "shot_on_goal").height, rel=0.30
    )
    # goals: near this (low-scoring) real game AND inside the NHL window
    assert ens["mean_total"] == pytest.approx(3.0, rel=0.8)
    assert 1.5 < ens["mean_total"] < 9.0
    # event-type mix tracks the real shares
    real_shares = {
        row["event"]: row["n"] / events.height for row in events.group_by("event").agg(pl.len().alias("n")).to_dicts()
    }
    for event in NHL_EVENTS:
        sim_share = float(counts[event].mean()) / sim_events
        assert abs(sim_share - real_shares.get(event, 0.0)) < 0.05, event
    # finals never tie
    assert float((ens["margin"] == 0).mean()) == 0.0


def test_ensemble_prices_with_odds_math(shelf) -> None:
    from sportsdataverse.odds.odds_math import prob_over

    ens = simulate_nhl_ensemble(shelf, n_sim=150, seed=23)
    p = prob_over(ens["total"], float(np.median(ens["total"])) - 0.5)
    assert 0.2 < p < 0.8
