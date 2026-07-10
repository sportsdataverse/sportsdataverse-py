"""WNBA (league_id='10') prediction-stack shim tests (Phase 6).

The WNBA models are by-reference shims over the league-agnostic NBA core; these
tests confirm (a) each shim dispatches with league_id='10', and (b) the WNBA
ratings engine clears the same oracle gate as NBA on the committed WNBA fixtures.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_prediction_constants import get_constants, mae, spearman_corr
from sportsdataverse.wnba.wnba_clutch import wnba_team_clutch
from sportsdataverse.wnba.wnba_game_predict import (
    wnba_in_game_win_prob,
    wnba_predict_games,
    wnba_predict_margin,
    wnba_win_prob_from_margin,
)
from sportsdataverse.wnba.wnba_player_props import wnba_player_props
from sportsdataverse.wnba.wnba_team_ratings import wnba_team_ratings

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


def test_wnba_constants_are_wnba() -> None:
    c = get_constants("10")
    assert c.game_minutes == 40
    assert c.in_game_wp_artifact == "wnba_in_game_wp.ubj"


def test_wnba_predict_margin_uses_wnba_hfa() -> None:
    c = get_constants("10")
    # symmetric teams, non-neutral -> margin equals the WNBA HFA (not the NBA one)
    m = wnba_predict_margin(5.0, 5.0, home_pace=c.avg_pace, away_pace=c.avg_pace, neutral=False)
    assert abs(m - c.hfa) < 1e-9
    assert abs(c.hfa - get_constants("00").hfa) > 0.5  # genuinely different from NBA


def test_wnba_win_prob_from_margin_uses_wnba_sigma() -> None:
    # same margin gives a different prob than NBA because sigma differs
    from sportsdataverse.nba.nba_game_predict import win_prob_from_margin

    assert wnba_win_prob_from_margin(5.0) != win_prob_from_margin(5.0, league_id="00")


def test_wnba_predict_games_dispatches() -> None:
    ratings = pl.DataFrame(
        {
            "team_id": ["A", "B"],
            "adj_off_rtg": [104.0, 99.0],
            "adj_def_rtg": [99.0, 103.0],
            "adj_net_rtg": [5.0, -4.0],
            "adj_pace": [95.0, 93.0],
        }
    )
    games = pl.DataFrame({"game_id": ["G1"], "home_team_id": ["A"], "away_team_id": ["B"]})
    out = wnba_predict_games(games, ratings)
    assert out.height == 1
    assert out["exp_margin"][0] is not None


def test_wnba_ratings_oracle_gate() -> None:
    """WNBA AdjNet vs stats.wnba.com NET_RATING on the 2024 fixtures."""
    results = pl.read_parquet(FIXTURE_DIR / "wnba_results_2024.parquet")
    team_box = pl.read_parquet(FIXTURE_DIR / "wnba_team_box_2024.parquet")
    oracle = pl.read_parquet(FIXTURE_DIR / "wnba_team_ratings_oracle_2024.parquet")

    wl = importlib.import_module("sportsdataverse.wnba.wnba_loaders")
    orig_s, orig_b = wl.load_wnba_schedule, wl.load_wnba_team_boxscore
    wl.load_wnba_schedule = lambda seasons: results  # type: ignore[assignment]
    wl.load_wnba_team_boxscore = lambda seasons: team_box  # type: ignore[assignment]
    try:
        mine = wnba_team_ratings(2024)
    finally:
        wl.load_wnba_schedule, wl.load_wnba_team_boxscore = orig_s, orig_b

    assert mine.schema["team_id"] == oracle.schema["team_id"]
    m = mine.join(oracle, on="team_id", how="inner")
    assert m.height == 12
    rho = spearman_corr(m["adj_net_rtg"].to_numpy(), m["net_rating"].to_numpy())
    mae_net = mae(m["adj_net_rtg"].to_numpy(), m["net_rating"].to_numpy())
    # Observed 2026-07-08: rho 0.965, mae 0.753 (same quality as NBA). Floors from observed.
    assert rho >= 0.90, f"WNBA AdjNet vs NET_RATING spearman {rho:.3f} below 0.90 floor"
    assert mae_net <= 1.0, f"WNBA AdjNet vs NET_RATING MAE {mae_net:.3f} above 1.0 floor"


def test_wnba_in_game_win_prob_scores() -> None:
    """The WNBA in-game shim loads wnba_in_game_wp.ubj and scores a large late lead high."""
    pbp = pl.DataFrame(
        {
            "start_game_seconds_remaining": [20.0],
            "home_score": [90],
            "away_score": [70],
            "team_id": ["A"],
            "home_team_id": ["A"],
        }
    )
    wp = wnba_in_game_win_prob(pbp, 0.5)
    assert wp["home_win_prob"][0] > 0.9


def test_wnba_clutch_and_props_are_partials_with_league_10() -> None:
    assert wnba_team_clutch.keywords == {"league_id": "10"}
    assert wnba_player_props.keywords == {"league_id": "10"}
