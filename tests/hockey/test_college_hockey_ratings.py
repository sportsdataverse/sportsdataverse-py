"""Unit + oracle-gate tests for the NCAA hockey opponent-adjusted ratings port.

Phase-0 finding (see sportsdataverse/hockey/college_hockey_constants.py):
ESPN's college-hockey pbp carries only Goal/Penalty events -- no shots, no
coordinates, no shifts -- so there is no xG/RAPM/GSAx port; only an
opponent-adjusted goal-margin rating is feasible. These tests both pin that
capture-contract finding as a regression (real committed fixtures) and gate
the ratings math (synthetic + real committed scoreboard sample).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.hockey.college_hockey_ratings import (
    college_hockey_game_results,
    college_hockey_ratings,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "league_ports"


def _load(name: str):
    with open(FIXTURES / name, encoding="utf-8") as f:
        return json.load(f)


# --- Feasibility regression: locks in the Phase-0 capture-contract finding ---


@pytest.mark.parametrize(
    "summary_file",
    ["mch_summary.json", "wch_summary.json"],
)
def test_feasibility_locked_to_goals_and_penalties_only(summary_file):
    summary = _load(summary_file)
    plays = summary.get("plays", [])
    assert plays, f"{summary_file} fixture unexpectedly has no plays"
    types = {p.get("type", {}).get("text") for p in plays}
    assert types <= {"Goal", "Penalty"}, (
        f"{summary_file} now carries play types beyond Goal/Penalty ({types}) -- "
        "revisit the xG/RAPM downscope in college_hockey_constants.py"
    )
    for p in plays:
        assert "coordinate" not in p, f"{summary_file} now carries shot coordinates"


def test_mch_game_plays_endpoint_can_be_empty():
    # Regular-season mch event 401711791 -- Core v2 plays returns 0 items,
    # even though it is a completed game (unlike the championship game used
    # by mch_game_plays.json/mch_summary.json, which has 12 scoring/penalty
    # plays). ESPN's college-hockey PBP coverage is inconsistent game-to-game.
    payload = _load("mch_game_plays_empty_sample.json")
    assert payload.get("count") == 0
    assert payload.get("items") == []


# --- college_hockey_game_results: synthetic parsing ---


def _synthetic_event(game_id, home_id, away_id, home_score, away_score, neutral=False, completed=True):
    return {
        "id": game_id,
        "competitions": [
            {
                "id": game_id,
                "neutralSite": neutral,
                "status": {"type": {"completed": completed}},
                "competitors": [
                    {"homeAway": "home", "team": {"id": home_id}, "score": str(home_score)},
                    {"homeAway": "away", "team": {"id": away_id}, "score": str(away_score)},
                ],
            }
        ],
    }


def test_game_results_parses_two_rows_per_game():
    events = [_synthetic_event("1", "A", "B", 4, 2)]
    out = college_hockey_game_results(events, league="mch")
    assert out.height == 2
    assert out.schema["team_id"] == pl.Utf8
    assert out.schema["opp_team_id"] == pl.Utf8
    home = out.filter(pl.col("team_id") == "A").row(0, named=True)
    away = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert (home["goals_for"], home["goals_against"], home["is_home"]) == (4, 2, True)
    assert (away["goals_for"], away["goals_against"], away["is_home"]) == (2, 4, False)


def test_game_results_skips_incomplete_games():
    events = [_synthetic_event("1", "A", "B", 4, 2, completed=False)]
    out = college_hockey_game_results(events, league="mch")
    assert out.height == 0


def test_game_results_empty_input_returns_schema():
    out = college_hockey_game_results([], league="mch")
    assert out.height == 0
    assert set(out.columns) == {
        "game_id",
        "team_id",
        "opp_team_id",
        "goals_for",
        "goals_against",
        "is_home",
        "is_neutral",
    }


def test_game_results_unknown_league_raises():
    with pytest.raises(ValueError):
        college_hockey_game_results([], league="khl")


# --- college_hockey_ratings: synthetic dominance property ---


def test_ratings_dominant_team_rates_higher():
    # C beats everyone big, A and B split evenly -- C should out-rate A/B.
    events = [
        _synthetic_event("1", "A", "B", 2, 2),  # never mind exact tie score; use distinct results below
        _synthetic_event("2", "B", "A", 3, 3),
        _synthetic_event("3", "C", "A", 6, 1),
        _synthetic_event("4", "C", "B", 6, 1),
        _synthetic_event("5", "A", "C", 1, 6),
        _synthetic_event("6", "B", "C", 1, 6),
    ]
    out = college_hockey_ratings(events, league="mch")
    ranked = out.sort("adj_net", descending=True)["team_id"].to_list()
    assert ranked[0] == "C"


def test_ratings_empty_input_returns_schema():
    out = college_hockey_ratings([], league="mch")
    assert out.height == 0
    assert set(out.columns) == {
        "team_id",
        "adj_off",
        "adj_def",
        "adj_net",
        "raw_off",
        "raw_def",
        "games",
    }


def test_ratings_return_as_pandas():
    events = [_synthetic_event("1", "A", "B", 4, 2)]
    out = college_hockey_ratings(events, league="mch", return_as_pandas=True)
    import pandas as pd

    assert isinstance(out, pd.DataFrame)


# --- Oracle gate: real committed 2024-25 MCH scoreboard sample ---


def _win_pct(games: pl.DataFrame) -> pl.DataFrame:
    return (
        games.with_columns((pl.col("goals_for") > pl.col("goals_against")).cast(pl.Float64).alias("win"))
        .group_by("team_id")
        .agg(pl.col("win").mean().alias("win_pct"), pl.len().alias("n"))
    )


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    ra = pl.Series(a).rank()
    rb = pl.Series(b).rank()
    return float(np.corrcoef(ra.to_numpy(), rb.to_numpy())[0, 1])


def test_mch_ratings_internal_consistency_oracle_gate():
    events = _load("mch_scoreboard_sample.json")
    games = college_hockey_game_results(events, league="mch")
    assert games.height >= 300  # 194 games * 2 sides, real captured sample
    ratings = college_hockey_ratings(events, league="mch")
    assert ratings.height >= 60  # 64 distinct teams in the captured sample

    wp = _win_pct(games)
    joined = ratings.join(wp, on="team_id", how="inner")
    assert joined.schema["team_id"] == wp.schema["team_id"] == pl.Utf8
    corr = _spearman(joined["adj_net"].to_numpy(), joined["win_pct"].to_numpy())
    # Observed on the committed sample: ~0.672. Floor set below the observed
    # value (never raised to force a pass) -- a genuine regression in the
    # fixed-point wiring would drop this well below 0.6.
    assert corr >= 0.6, f"adj_net vs win_pct Spearman {corr:.3f} below internal-consistency floor"


# --- External sanity: 2025 NCAA tournament field overlap (real historical fact) ---

# The real 16-team field of the 2025 NCAA D-I men's ice hockey tournament
# (source: Wikipedia, "2025 NCAA Division I men's ice hockey tournament",
# fetched 2026-07-12). Hardcoded as a historical fact -- no live network call
# in the test suite (offline-fixture-test rule).
_2025_MCH_TOURNAMENT_FIELD = {
    "Boston College",
    "Providence",
    "Denver",
    "Bentley",
    "Michigan State",
    "Boston University",
    "Ohio State",
    "Cornell",
    "Maine",
    "UConn",
    "Quinnipiac",
    "Penn State",
    "Western Michigan",
    "Minnesota",
    "Massachusetts",
    "Minnesota State",
}


def test_mch_ratings_external_sanity_tournament_overlap():
    events = _load("mch_scoreboard_sample.json")
    ratings = college_hockey_ratings(events, league="mch").sort("adj_net", descending=True)

    id_to_name: dict[str, str] = {}
    for ev in events:
        for c in ev["competitions"][0]["competitors"]:
            id_to_name[str(c["team"]["id"])] = c["team"].get("displayName", "")

    top16_names = {id_to_name.get(tid, "") for tid in ratings["team_id"].to_list()[:16]}
    # UConn's ESPN displayName is "UConn Huskies"; normalize both sides on the
    # short school name via substring containment.
    matched = sum(1 for school in _2025_MCH_TOURNAMENT_FIELD if any(school in name for name in top16_names))
    overlap = matched / len(_2025_MCH_TOURNAMENT_FIELD)
    # Observed on the committed sample: 12/16 = 0.75. Floor set below the
    # observed value -- a pure adjusted-goal-margin rating won't replicate the
    # selection committee exactly (RPI/at-large rules aren't modeled here).
    assert overlap >= 0.6, f"tournament-field overlap {overlap:.2f} below external-sanity floor"


def test_wch_ratings_smoke_only_insufficient_season_coverage():
    # WCH: ESPN scoreboard coverage found was tournament-bracket-only (7 games,
    # 8 teams) -- too thin/structurally sparse (single-elim, no repeat
    # matchups) for a correlation-floor gate. Smoke-test schema + convergence
    # only; do not assert a calibration floor on this sample (see
    # sportsdataverse/hockey/wch/wch_ratings.py docstring).
    events = _load("wch_scoreboard_sample.json")
    games = college_hockey_game_results(events, league="wch")
    assert games.height == 14  # 7 games * 2 sides
    ratings = college_hockey_ratings(events, league="wch")
    assert ratings.height == 8
    assert ratings["adj_net"].is_finite().all()
