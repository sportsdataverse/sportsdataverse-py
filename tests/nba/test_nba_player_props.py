"""Tests for player-prop distributions + team pace projection (Phase 5, model ⑥).

Gate rule (binding): never lower a gate -- debug the model. Floors are the
observed value at gate time rounded to the safe side.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_player_props import (
    nba_player_props,
    player_rates,
    project_player_line,
    prop_distribution,
    prob_over,
    team_pace_projection,
)

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


def _mini_logs() -> pl.DataFrame:
    # player P: 2 games, 20 min each, 10 pts each -> 0.5 pts/min
    # player Q: 1 game, 10 min, 2 pts -> 0.2 pts/min
    return pl.DataFrame(
        {
            "game_id": ["G1", "G2", "G1"],
            "player_id": ["P", "P", "Q"],
            "team_id": ["A", "A", "B"],
            "minutes": [20.0, 20.0, 10.0],
            "pts": [10.0, 10.0, 2.0],
            "reb": [4.0, 6.0, 1.0],
            "ast": [2.0, 2.0, 0.0],
            "fg3m": [1.0, 3.0, 0.0],
        }
    )


def test_player_rates_per_minute() -> None:
    r = player_rates(_mini_logs())
    p = r.filter(pl.col("player_id") == "P").row(0, named=True)
    assert abs(p["pts_per_min"] - 0.5) < 1e-9
    assert abs(p["minutes_pg"] - 20.0) < 1e-9
    assert p["games"] == 2


def test_project_player_line_scales_with_minutes_and_pace() -> None:
    r = player_rates(_mini_logs())
    p = r.filter(pl.col("player_id") == "P").row(0, named=True)
    base = project_player_line(p, exp_minutes=20.0, pace_factor=1.0)
    assert abs(base["exp_pts"] - 10.0) < 1e-9  # 0.5/min * 20 min
    faster = project_player_line(p, exp_minutes=20.0, pace_factor=1.1)
    assert abs(faster["exp_pts"] - 11.0) < 1e-9  # counting stats scale with pace
    more_min = project_player_line(p, exp_minutes=30.0, pace_factor=1.0)
    assert abs(more_min["exp_pts"] - 15.0) < 1e-9


def test_prob_over_normal_at_mean_is_half() -> None:
    fam, _ = prop_distribution(20.0, "pts")
    assert fam == "normal"
    assert abs(prob_over(20.0, 20.0, "pts") - 0.5) < 0.05


def test_prob_over_monotonic_decreasing_in_line() -> None:
    lines = [5.0, 8.0, 10.0, 15.0]
    probs = [prob_over(10.0, ln, "pts") for ln in lines]
    assert probs == sorted(probs, reverse=True)


def test_prob_over_count_families_bounded() -> None:
    for stat in ("reb", "ast", "fg3m"):
        fam, _ = prop_distribution(5.0, stat)
        assert fam in ("nbinom", "poisson")
        p = prob_over(5.0, 4.5, stat)
        assert 0.0 <= p <= 1.0


def test_team_pace_projection_matches_expected_possessions() -> None:
    ratings = pl.DataFrame(
        {
            "team_id": ["A", "B"],
            "adj_off_rtg": [115.0, 110.0],
            "adj_def_rtg": [110.0, 112.0],
            "adj_net_rtg": [5.0, -2.0],
            "adj_pace": [100.0, 98.0],
        }
    )
    from sportsdataverse.nba.nba_game_predict import expected_possessions

    assert abs(team_pace_projection("A", "B", ratings) - expected_possessions(100.0, 98.0)) < 1e-9


def test_nba_player_props_realized_gate(monkeypatch) -> None:
    """Projection-accuracy gate: mean-projected lines vs realized box outcomes."""
    logs = pl.read_parquet(FIXTURE_DIR / "player_box_logs_2024.parquet").drop_nulls("minutes")

    rates = player_rates(logs)
    # descriptive projection: each player's season-mean line (rate * mean minutes, pace=1)
    proj = rates.with_columns(
        (pl.col("pts_per_min") * pl.col("minutes_pg")).alias("exp_pts"),
        (pl.col("reb_per_min") * pl.col("minutes_pg")).alias("exp_reb"),
        (pl.col("ast_per_min") * pl.col("minutes_pg")).alias("exp_ast"),
        (pl.col("fg3m_per_min") * pl.col("minutes_pg")).alias("exp_fg3m"),
    ).select("player_id", "exp_pts", "exp_reb", "exp_ast", "exp_fg3m")

    # join projection back to each realized game (rotation players only: >= 15 games, >= 15 mpg)
    rotation = rates.filter((pl.col("games") >= 15) & (pl.col("minutes_pg") >= 15.0)).select("player_id")
    joined = logs.join(rotation, on="player_id", how="inner").join(proj, on="player_id", how="inner")

    mae_pts = float((joined["pts"] - joined["exp_pts"]).abs().mean())
    mae_reb = float((joined["reb"] - joined["exp_reb"]).abs().mean())
    # Observed 2026-07-08 (321 rotation players, 21.5k player-games): mae_pts=4.94,
    # mae_reb=1.97 -- the irreducible single-game variance of a counting stat around its
    # mean projection. Floors from observed, rounded to the safe side; do not lower.
    assert mae_pts <= 5.5, f"pts projection MAE {mae_pts:.2f} above 5.5 floor"
    assert mae_reb <= 2.5, f"reb projection MAE {mae_reb:.2f} above 2.5 floor"

    # P(over) calibration: line = each player's exp_pts (integer-ish), prob_over from the
    # Normal at the projected mean should hover ~0.5 and the realized over-rate track it.
    j2 = joined.with_columns(
        pl.struct(["exp_pts"])
        .map_elements(lambda s: prob_over(s["exp_pts"], s["exp_pts"], "pts"), return_dtype=pl.Float64)
        .alias("p_over"),
        (pl.col("pts") > pl.col("exp_pts")).cast(pl.Float64).alias("actual_over"),
    )
    mean_p = float(j2["p_over"].mean())
    mean_actual = float(j2["actual_over"].mean())
    # line-at-mean over-rate should be near 0.5 for both predicted and realized (a symmetric
    # Normal predicts 0.5; counting stats are slightly right-skewed so realized is a touch under).
    assert 0.45 <= mean_p <= 0.55, f"predicted P(over) at mean {mean_p:.3f} not ~0.5"
    assert 0.40 <= mean_actual <= 0.55, f"realized over-rate at mean {mean_actual:.3f} implausible"


def test_nba_player_props_orchestrator(monkeypatch) -> None:
    logs = _mini_logs()
    ratings = pl.DataFrame(
        {
            "team_id": ["A", "B"],
            "adj_off_rtg": [115.0, 110.0],
            "adj_def_rtg": [110.0, 112.0],
            "adj_net_rtg": [5.0, -2.0],
            "adj_pace": [100.0, 98.0],
        }
    )
    mod = importlib.import_module("sportsdataverse.nba.nba_player_props")
    monkeypatch.setattr(mod, "_load_player_logs", lambda season, league_id: logs)
    monkeypatch.setattr(mod, "_load_ratings", lambda season, league_id: ratings)

    out = nba_player_props(2024, "G3", "A", "B", league_id="00")
    assert set(out.columns) >= {
        "player_id",
        "team_id",
        "stat_pts_exp",
        "stat_reb_exp",
        "stat_ast_exp",
        "stat_fg3m_exp",
        "pace_proj",
    }
    assert out.schema["player_id"] == pl.Utf8
    assert out.height >= 1
    assert (out["stat_pts_exp"] >= 0).all()
