"""Gates: ESPN player logs, WNBA prop surface, rotation-minutes reweighting."""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.metrics import spearman_corr
from sportsdataverse.nba.nba_possession_sim import (
    WNBA_RULES,
    PlayerAttribution,
    build_shelf,
)
from sportsdataverse.nba.nba_possession_sim.attribution import (
    minutes_from_gamerotation,
    simulate_player_boxscores,
)
from sportsdataverse.nba.nba_possession_sim.espn_adapter import (
    espn_final_total,
    espn_summary_to_events,
    player_game_logs_from_espn,
)


@pytest.fixture(scope="module")
def summary() -> dict:
    return json.loads(pathlib.Path("tests/fixtures/espn/summary_wnba.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def logs(summary: dict) -> pl.DataFrame:
    return player_game_logs_from_espn(summary)


def test_espn_logs_conserve_player_points(summary: dict, logs: pl.DataFrame) -> None:
    """Player-level conservation: attributed points rebuild the real final."""
    assert int(logs["pts"].sum()) == espn_final_total(summary) == 129
    assert int(logs["ast"].sum()) > 20
    assert int(logs["reb"].sum()) > 50
    assert logs["team_id"].null_count() == 0


def test_wnba_player_prop_surface(summary: dict, logs: pl.DataFrame) -> None:
    teams = sorted(int(t) for t in logs["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[1], away_team_id=teams[0])
    shelf = build_shelf(espn_summary_to_events(summary))
    box = simulate_player_boxscores(shelf, att, n_sim=80, seed=7, rules=WNBA_RULES)
    stacked = np.vstack([box["pts"][p] for p in sorted(box["pts"])])
    assert np.array_equal(stacked.sum(axis=0), box["score_home"] + box["score_away"])
    real = (
        logs.group_by("player_id")
        .agg(pl.col("pts").sum(), (pl.col("fga") + pl.col("fta")).sum().alias("vol"))
        .filter(pl.col("vol") >= 5)
    )
    sim_pts = np.array([float(box["pts"][int(p)].mean()) for p in real["player_id"].to_list()])
    assert spearman_corr(sim_pts, real["pts"].to_numpy().astype(float)) > 0.5


def test_rotation_minutes_parse_exactly() -> None:
    rotation = json.loads(
        pathlib.Path("tests/fixtures/nbagl_engine/2022400003/gamerotation.json").read_text(encoding="utf-8")
    )
    minutes = minutes_from_gamerotation(rotation)
    assert len(minutes) >= 15
    # a regulation game is exactly 240 team-minutes per side
    assert sum(minutes.values()) == pytest.approx(480.0, abs=1.0)
    assert all(m > 0 for m in minutes.values())


def test_minutes_reweight(logs: pl.DataFrame) -> None:
    teams = sorted(int(t) for t in logs["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[1], away_team_id=teams[0])
    side = att.home
    zeroed, doubled = side.player_ids[0], side.player_ids[1]
    reweighted = side.reweight({zeroed: 0.0, doubled: 2.0})
    assert reweighted.two_shares[0] == 0.0
    assert reweighted.two_shares.sum() == pytest.approx(1.0)
    assert reweighted.reb_shares.sum() == pytest.approx(1.0)
    if side.two_shares[1] > 0:
        assert reweighted.two_shares[1] > side.two_shares[1]
