"""Gates for the prop-distribution surface, on real-fixture simulations.

The prop layer composes the boxscore sample vectors (exact team
conservation by construction) with the odds math — so the gates are
distributional identities and pricing invariants, plus the availability
scenario that makes props useful: with the outcome stream held fixed,
removing a star conserves the team totals exactly and redistributes his
points to teammates.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    PlayerAttribution,
    build_shelf,
    player_game_logs_from_pbp,
    possessions_from_pbp,
    simulate_player_boxscores,
)
from sportsdataverse.nba.nba_possession_sim.expanded_nodes import aux_params_from_pbp
from sportsdataverse.nba.nba_possession_sim.props import (
    player_prop_distributions,
    price_board,
    price_prop,
)
from sportsdataverse.odds.odds_math import american_to_prob


@pytest.fixture(scope="module")
def sim():
    frames = []
    for gid in ("0022100001", "0022200001", "0022300001"):
        payload = json.loads(
            pathlib.Path(f"tests/fixtures/nba_engine/{gid}/playbyplayv3.json").read_text(encoding="utf-8")
        )
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    raw = pl.concat(frames, how="diagonal_relaxed")
    shelf = build_shelf(possessions_from_pbp(raw))
    shelf.aux = aux_params_from_pbp(raw)
    logs = player_game_logs_from_pbp(raw)
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1])
    box = simulate_player_boxscores(shelf, att, n_sim=250, seed=7)
    return shelf, att, box


def test_pmfs_are_distributions_consistent_with_the_vectors(sim) -> None:
    _shelf, _att, box = sim
    pmfs = player_prop_distributions(box)
    sums = pmfs.group_by("player_id", "stat").agg(pl.col("prob").sum().alias("total"))
    assert np.allclose(sums["total"].to_numpy(), 1.0)
    # PMF expectation equals the sample mean it was tabulated from
    means = pmfs.group_by("player_id", "stat").agg((pl.col("value") * pl.col("prob")).sum().alias("ev"))
    for row in means.iter_rows(named=True):
        assert row["ev"] == pytest.approx(float(np.mean(box[row["stat"]][row["player_id"]])))
    with pytest.raises(ValueError, match="absent"):
        player_prop_distributions(box, stats=["blk"])


def test_team_points_conserve_exactly_per_simulation(sim) -> None:
    _shelf, _att, box = sim
    total = box["score_home"] + box["score_away"]
    player_sum = np.sum([vec for vec in box["pts"].values()], axis=0)
    assert np.array_equal(player_sum, total)  # element-wise, every simulated game


def test_price_prop_invariants(sim) -> None:
    _shelf, _att, box = sim
    star = max(box["pts"], key=lambda pid: float(np.mean(box["pts"][pid])))
    samples = box["pts"][star]
    half = price_prop(samples, float(np.median(samples)) + 0.5)
    assert half.p_push == 0.0
    assert half.p_over + half.p_under == pytest.approx(1.0)
    integer = price_prop(samples, float(np.bincount(samples).argmax()))
    assert integer.p_push > 0.0
    assert integer.p_over + integer.p_under + integer.p_push == pytest.approx(1.0)
    # monotone: P(over) non-increasing in the line
    lines = np.arange(0.5, float(samples.max()) + 1.0, 1.0)
    overs = [price_prop(samples, float(line)).p_over for line in lines]
    assert all(a >= b for a, b in zip(overs, overs[1:]))
    # fair odds round-trip to the push-excluded conditional probability
    live = half.p_over + half.p_under
    assert american_to_prob(half.fair_over) == pytest.approx(half.p_over / live, abs=0.01)
    with pytest.raises(ValueError, match="empty sample"):
        price_prop(np.array([]), 10.5)


def test_price_board_tidy_and_strict(sim) -> None:
    _shelf, _att, box = sim
    players = sorted(box["pts"], key=lambda pid: -float(np.mean(box["pts"][pid])))[:3]
    board = pl.DataFrame(
        {
            "player_id": players,
            "stat": ["pts", "reb", "ast"],
            "line": [float(np.median(box[s][p])) + 0.5 for p, s in zip(players, ["pts", "reb", "ast"])],
        }
    )
    priced = price_board(box, board)
    assert priced.height == 3
    assert {"p_over", "p_under", "p_push", "fair_over", "fair_under", "mean", "median"} <= set(priced.columns)
    assert ((priced["p_over"] >= 0.0) & (priced["p_over"] <= 1.0)).all()
    with pytest.raises(ValueError, match="not in the simulation"):
        price_board(box, pl.DataFrame({"player_id": [1], "stat": ["pts"], "line": [10.5]}))


def test_star_out_conserves_team_and_redistributes_exactly(sim) -> None:
    shelf, att, box = sim
    home_ids = list(att.home.player_ids)
    star = max(home_ids, key=lambda pid: float(np.mean(box["pts"][pid])))
    scenario = simulate_player_boxscores(shelf, att.without(home_unavailable=[star]), n_sim=250, seed=7)
    assert star not in scenario["pts"]
    # attribution only credits — the outcome stream is seed-aligned, so the
    # simulated team totals are IDENTICAL and the star's points move to
    # teammates exactly
    assert np.array_equal(scenario["score_home"], box["score_home"])
    assert np.array_equal(scenario["score_away"], box["score_away"])
    remaining = [pid for pid in home_ids if pid != star]
    before = sum(float(np.mean(box["pts"][p])) for p in remaining)
    after = sum(float(np.mean(scenario["pts"][p])) for p in remaining)
    star_mean = float(np.mean(box["pts"][star]))
    assert after == pytest.approx(before + star_mean)
    # and the lift is broad, not a single-player artifact
    lifted = sum(1 for p in remaining if float(np.mean(scenario["pts"][p])) > float(np.mean(box["pts"][p])))
    assert lifted >= int(0.7 * len(remaining))
    # the prop consequence: teammates' over-probabilities move up at a fixed line
    teammate = max(remaining, key=lambda pid: float(np.mean(box["pts"][pid])))
    line = float(np.median(box["pts"][teammate])) + 0.5
    assert price_prop(scenario["pts"][teammate], line).p_over >= price_prop(box["pts"][teammate], line).p_over
