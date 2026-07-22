"""Gates for full player boxscore attribution (pts/reb/ast + availability)."""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.metrics import spearman_corr
from sportsdataverse.nba.nba_possession_sim import (
    PlayerAttribution,
    build_shelf,
    player_game_logs_from_pbp,
    possessions_from_pbp,
)
from sportsdataverse.nba.nba_possession_sim.attribution import simulate_player_boxscores
from sportsdataverse.nba.nba_possession_sim.expanded_nodes import aux_params_from_pbp

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
def setup(actions: pl.DataFrame, logs: pl.DataFrame):
    events = possessions_from_pbp(actions)
    shelf = build_shelf(events)
    shelf.aux = aux_params_from_pbp(actions)
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1])
    return shelf, att, teams


def test_logs_extract_real_ast_reb(logs: pl.DataFrame) -> None:
    # Haliburton's real 13-assist opener is the extraction oracle
    hali = logs.filter((pl.col("game_id") == "0022300001") & (pl.col("player_id") == 1630169))
    assert int(hali["ast"][0]) == 13
    assert int(logs["ast"].sum()) > 100
    assert int(logs["reb"].sum()) > 200


def test_shares_and_p_assisted(setup) -> None:
    _, att, _ = setup
    for side in (att.home, att.away):
        assert side.reb_shares.sum() == pytest.approx(1.0)
        assert side.ast_shares.sum() == pytest.approx(1.0)
        assert 0.35 <= side.p_assisted <= 0.85


def test_assister_never_scorer(setup) -> None:
    _, att, _ = setup
    rng = np.random.default_rng(3)
    scorer = att.home.player_ids[0]
    for _ in range(200):
        assister = att.home.sample_assister(scorer, rng)
        assert assister != scorer


def test_availability_mask(setup) -> None:
    _, att, _ = setup
    star = att.home.player_ids[0]
    masked = att.without(home_unavailable=[star])
    assert star not in masked.home.player_ids
    assert masked.home.two_shares.sum() == pytest.approx(1.0)
    assert masked.away is att.away  # untouched side is shared
    with pytest.raises(ValueError, match="removed every player"):
        att.home.without(att.home.player_ids)


def test_boxscore_sim_conserves_and_ranks(setup, logs: pl.DataFrame) -> None:
    shelf, att, teams = setup
    box = simulate_player_boxscores(shelf, att, n_sim=120, seed=7)
    again = simulate_player_boxscores(shelf, att, n_sim=120, seed=7)
    stacked = np.vstack([box["pts"][p] for p in sorted(box["pts"])])
    # conservation: attributed points rebuild both team scores, every sim
    assert np.array_equal(stacked.sum(axis=0), box["score_home"] + box["score_away"])
    # determinism
    assert np.array_equal(stacked, np.vstack([again["pts"][p] for p in sorted(again["pts"])]))
    # per-game volumes in NBA-plausible windows
    reb_pg = float(np.vstack(list(box["reb"].values())).sum(axis=0).mean())
    ast_pg = float(np.vstack(list(box["ast"].values())).sum(axis=0).mean())
    assert 60 < reb_pg < 130
    assert 30 < ast_pg < 75
    # rank agreement with the real logs on volume players
    real = (
        logs.filter(pl.col("team_id").is_in(teams))
        .group_by("player_id")
        .agg(
            pl.col("reb").sum(),
            pl.col("ast").sum(),
            (pl.col("fga") + pl.col("fta")).sum().alias("vol"),
        )
        .filter(pl.col("vol") >= 5)
    )
    ids = [int(p) for p in real["player_id"].to_list()]
    sim_reb = np.array([float(box["reb"][p].mean()) for p in ids])
    sim_ast = np.array([float(box["ast"][p].mean()) for p in ids])
    assert spearman_corr(sim_reb, real["reb"].to_numpy().astype(float)) > 0.7
    assert spearman_corr(sim_ast, real["ast"].to_numpy().astype(float)) > 0.7


def test_prop_pricing_and_injury_scenario(setup) -> None:
    from sportsdataverse.odds.odds_math import prob_over

    shelf, att, _ = setup
    star = att.home.player_ids[int(np.argmax(att.home.two_shares))]
    base = simulate_player_boxscores(shelf, att, n_sim=100, seed=11)
    masked = simulate_player_boxscores(shelf, att.without(home_unavailable=[star]), n_sim=100, seed=11)
    teammates = [p for p in att.home.player_ids if p != star]
    base_share = float(np.mean([base["pts"][p].mean() for p in teammates]))
    masked_share = float(np.mean([masked["pts"][p].mean() for p in teammates]))
    # removing the top usage player redistributes scoring to teammates
    assert masked_share > base_share
    p = prob_over(base["pts"][star], 9.5)
    assert 0.0 <= p <= 1.0
