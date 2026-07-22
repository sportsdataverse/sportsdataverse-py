"""Gates for player attribution (WS4 v2) — real-fixture oracles only."""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.metrics import spearman_corr
from sportsdataverse.nba.nba_possession_sim import (
    OUTCOMES,
    PlayerAttribution,
    build_shelf,
    player_game_logs_from_pbp,
    possessions_from_pbp,
    simulate_ensemble,
)
from sportsdataverse.nba.nba_possession_sim.attribution import terminal_outcome

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAME_IDS = ("0022100001", "0022200001", "0022300001")


@pytest.fixture(scope="module")
def actions() -> pl.DataFrame:
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return pl.concat(frames, how="diagonal_relaxed")


@pytest.fixture(scope="module")
def logs(actions: pl.DataFrame) -> pl.DataFrame:
    return player_game_logs_from_pbp(actions)


@pytest.fixture(scope="module")
def team_pair(logs: pl.DataFrame) -> "tuple[int, int]":
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    assert len(teams) == 2
    return teams[0], teams[1]


def test_terminal_outcome_helper() -> None:
    vocab = set(OUTCOMES)
    assert terminal_outcome(["rim_miss", "oreb", "three_make"], vocab) == "three_make"
    assert terminal_outcome(["ft_trip_2", "ft_made_1"], vocab) == "ft_trip_2"
    assert terminal_outcome(["oreb", "dreb"], vocab) is None


def test_logs_carry_team_id(logs: pl.DataFrame) -> None:
    assert "team_id" in logs.columns
    assert logs["team_id"].null_count() == 0


def test_team_attribution_shares(logs: pl.DataFrame, team_pair) -> None:
    home, away = team_pair
    att = PlayerAttribution.from_logs(logs, home_team_id=home, away_team_id=away)
    for side in (att.home, att.away):
        for shares in (side.two_shares, side.three_shares, side.ft_shares, side.tov_shares):
            assert shares.sum() == pytest.approx(1.0)
        assert len(side.player_ids) == len(side.two_shares)


def test_ensemble_player_points_conserve_team_totals(actions, logs, team_pair) -> None:
    shelf = build_shelf(possessions_from_pbp(actions))
    home, away = team_pair
    att = PlayerAttribution.from_logs(logs, home_team_id=home, away_team_id=away)
    ens = simulate_ensemble(shelf, n_sim=150, seed=17, attribution=att)
    player_points = ens["player_points"]
    assert player_points is not None
    stacked = np.vstack([player_points[pid] for pid in sorted(player_points)])
    # conservation: attributed points reconstruct BOTH team scores, every sim
    assert np.array_equal(stacked.sum(axis=0), ens["total"])
    # determinism with attribution on
    again = simulate_ensemble(shelf, n_sim=150, seed=17, attribution=att)
    assert np.array_equal(stacked, np.vstack([again["player_points"][pid] for pid in sorted(player_points)]))


def test_sim_player_means_rank_like_real_scoring(actions, logs, team_pair) -> None:
    shelf = build_shelf(possessions_from_pbp(actions))
    home, away = team_pair
    att = PlayerAttribution.from_logs(logs, home_team_id=home, away_team_id=away)
    ens = simulate_ensemble(shelf, n_sim=300, seed=23, attribution=att)
    real = (
        logs.filter(pl.col("team_id").is_in([home, away]))
        .group_by("player_id")
        .agg(pl.col("pts").sum(), (pl.col("fga") + pl.col("fta")).sum().alias("volume"))
        .filter(pl.col("volume") >= 5)
    )
    sim_means = np.array([float(ens["player_points"][int(p)].mean()) for p in real["player_id"].to_list()])
    real_pts = real["pts"].to_numpy().astype(float)
    rho = spearman_corr(sim_means, real_pts)
    assert rho > 0.5, f"sim scoring rank diverges from real scoring (rho={rho:.3f})"
