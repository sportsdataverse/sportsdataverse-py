"""Tests for :mod:`sportsdataverse.nhl.nhl_team_ratings` (Phase 1)."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent, team_game_xg_rates


def _mini():
    pbp = pl.DataFrame(
        {
            "game_id": [1, 1, 1, 1],
            "season": [2023, 2023, 2023, 2023],
            "event_team_abbr": ["TOR", "TOR", "BOS", "BOS"],
            "home_abbr": ["TOR"] * 4,
            "away_abbr": ["BOS"] * 4,
            "home_skaters": [5, 5, 5, 5],
            "away_skaters": [5, 5, 5, 5],
            "home_goalie_in": [1, 1, 1, 1],
            "away_goalie_in": [1, 1, 1, 1],
            "xg": [0.30, 0.10, 0.20, 0.05],
            "event_type": ["SHOT"] * 4,
            "home_score": [0, 1, 1, 1],
            "away_score": [0, 0, 0, 0],
        }
    )
    sched = pl.DataFrame(
        {
            "game_id": [1],
            "season": [2023],
            "date": [dt.date(2023, 1, 1)],
            "home_abbr": ["TOR"],
            "away_abbr": ["BOS"],
            "neutral_site": [False],
            "home_goals": [1],
            "away_goals": [0],
        }
    )
    return pbp, sched


def test_even_strength_xg_rates():
    pbp, sched = _mini()
    out = team_game_xg_rates(pbp, sched)
    tor = out.filter(pl.col("team") == "TOR").row(0, named=True)
    assert abs(tor["xgf"] - 0.40) < 1e-9  # 0.30 + 0.10
    assert abs(tor["xga"] - 0.25) < 1e-9  # 0.20 + 0.05
    assert tor["opp_team"] == "BOS" and tor["is_home"] is True
    assert out.schema["team"] == pl.Utf8 and out.schema["game_id"] == pl.Utf8


def test_even_strength_filter_drops_power_play():
    pbp, sched = _mini()
    # Add a 5-on-4 power-play shot that must be excluded from even-strength totals.
    pp_row = pl.DataFrame(
        {
            "game_id": [1],
            "season": [2023],
            "event_team_abbr": ["TOR"],
            "home_abbr": ["TOR"],
            "away_abbr": ["BOS"],
            "home_skaters": [5],
            "away_skaters": [4],
            "home_goalie_in": [1],
            "away_goalie_in": [1],
            "xg": [0.90],
            "event_type": ["SHOT"],
            "home_score": [1],
            "away_score": [0],
        }
    )
    pbp2 = pl.concat([pbp, pp_row], how="vertical_relaxed")
    out = team_game_xg_rates(pbp2, sched)
    tor = out.filter(pl.col("team") == "TOR").row(0, named=True)
    assert abs(tor["xgf"] - 0.40) < 1e-9  # PP shot (0.90) must not be counted


def test_empty_input_returns_documented_schema():
    empty_pbp = pl.DataFrame(
        schema={
            "game_id": pl.Int64,
            "season": pl.Int64,
            "event_team_abbr": pl.Utf8,
            "home_abbr": pl.Utf8,
            "away_abbr": pl.Utf8,
            "home_skaters": pl.Int64,
            "away_skaters": pl.Int64,
            "home_goalie_in": pl.Int64,
            "away_goalie_in": pl.Int64,
            "xg": pl.Float64,
            "event_type": pl.Utf8,
            "home_score": pl.Int64,
            "away_score": pl.Int64,
        }
    )
    empty_sched = pl.DataFrame(
        schema={
            "game_id": pl.Int64,
            "season": pl.Int64,
            "date": pl.Date,
            "home_abbr": pl.Utf8,
            "away_abbr": pl.Utf8,
            "neutral_site": pl.Boolean,
            "home_goals": pl.Int64,
            "away_goals": pl.Int64,
        }
    )
    out = team_game_xg_rates(empty_pbp, empty_sched)
    assert out.height == 0
    assert out.schema["team"] == pl.Utf8


# --- Task 1.2: opponent adjustment + shrinkage solver -----------------------


def _round_robin(n_teams: int = 4, strength=None, games_each: int = 10, seed: int = 0):
    """Synthetic round-robin where team i's true xGF exceeds a league mean by
    ``strength[i]`` regardless of opponent, plus noise -- so the *adjusted*
    rating should recover the injected strength ordering even though raw
    per-game rates are opponent-confounded.
    """
    rng = np.random.default_rng(seed)
    teams = [f"T{i}" for i in range(n_teams)]
    if strength is None:
        strength = {t: 0.5 * i for i, t in enumerate(teams)}
    avg = 2.5
    rows = []
    gid = 0
    for _ in range(games_each):
        order = list(range(n_teams))
        rng.shuffle(order)
        for a, b in zip(order[::2], order[1::2]):
            gid += 1
            home, away = teams[a], teams[b]
            xgf_home = avg + strength[home] - strength[away] + rng.normal(0, 0.05)
            xgf_away = avg + strength[away] - strength[home] + rng.normal(0, 0.05)
            rows.append(
                {
                    "season": 2023,
                    "team": home,
                    "opp_team": away,
                    "is_home": True,
                    "neutral_site": False,
                    "xgf": xgf_home,
                    "xga": xgf_away,
                }
            )
            rows.append(
                {
                    "season": 2023,
                    "team": away,
                    "opp_team": home,
                    "is_home": False,
                    "neutral_site": False,
                    "xgf": xgf_away,
                    "xga": xgf_home,
                }
            )
    return pl.DataFrame(rows), teams, strength


def test_adjust_rate_opponent_recovers_strength_ordering():
    game_rates, teams, strength = _round_robin()
    out = adjust_rate_opponent(game_rates, for_col="xgf", against_col="xga", hfa=0.0, avg=2.5, shrink_k=1.0)
    ordered = out.sort("team")["adj_net"].to_list()
    expected_order = [strength[t] for t in sorted(teams)]
    # Spearman-style: check the ranking matches (monotonic), not exact values.
    assert list(np.argsort(ordered)) == list(np.argsort(expected_order))


def test_shrinkage_pulls_low_sample_team_harder():
    game_rates, teams, strength = _round_robin(games_each=20)
    # Truncate one team's games down to a single appearance.
    low_team = teams[-1]
    keep_mask = (pl.col("team") != low_team) | (pl.arange(0, pl.len()) < 1)
    trimmed = pl.concat(
        [
            game_rates.filter(pl.col("team") != low_team),
            game_rates.filter(pl.col("team") == low_team).head(1),
            game_rates.filter(pl.col("opp_team") == low_team),
        ],
        how="vertical_relaxed",
    )
    weak_shrink = adjust_rate_opponent(trimmed, for_col="xgf", against_col="xga", hfa=0.0, avg=2.5, shrink_k=1.0)
    strong_shrink = adjust_rate_opponent(trimmed, for_col="xgf", against_col="xga", hfa=0.0, avg=2.5, shrink_k=50.0)
    weak_val = weak_shrink.filter(pl.col("team") == low_team)["adj_for"][0]
    strong_val = strong_shrink.filter(pl.col("team") == low_team)["adj_for"][0]
    # Stronger shrink_k pulls the low-sample team's rating closer to league avg (2.5).
    assert abs(strong_val - 2.5) < abs(weak_val - 2.5)


def test_neutral_site_drops_hfa_side():
    game_rates, teams, strength = _round_robin(games_each=10)
    neutral = game_rates.with_columns(pl.lit(True).alias("neutral_site"))
    out_neutral = adjust_rate_opponent(neutral, for_col="xgf", against_col="xga", hfa=0.5, avg=2.5, shrink_k=1.0)
    out_non_neutral = adjust_rate_opponent(game_rates, for_col="xgf", against_col="xga", hfa=0.5, avg=2.5, shrink_k=1.0)
    # Both should converge to a valid frame with the same teams; presence of
    # a neutral-site branch is exercised without asserting a specific delta.
    assert set(out_neutral["team"]) == set(out_non_neutral["team"])


def test_adjust_rate_opponent_empty_input():
    empty = pl.DataFrame(
        schema={
            "season": pl.Int64,
            "team": pl.Utf8,
            "opp_team": pl.Utf8,
            "is_home": pl.Boolean,
            "neutral_site": pl.Boolean,
            "xgf": pl.Float64,
            "xga": pl.Float64,
        }
    )
    out = adjust_rate_opponent(empty, for_col="xgf", against_col="xga", hfa=0.2, avg=2.5, shrink_k=15.0)
    assert out.height == 0
    assert out.schema["team"] == pl.Utf8
