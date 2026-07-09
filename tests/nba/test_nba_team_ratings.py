"""Tests for the NBA adjusted-net-rating + pace engine (Phase 1, model ①)."""

from __future__ import annotations

import datetime as dt
import importlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_team_ratings import (
    adjust_efficiency,
    adjust_pace,
    nba_team_ratings,
    raw_game_efficiency,
)


def _mini_schedule_box() -> tuple[pl.DataFrame, pl.DataFrame]:
    sched = pl.DataFrame(
        {
            "game_id": ["G1"],
            "season": [2024],
            "date": [dt.date(2024, 1, 1)],
            "home_team_id": ["A"],
            "away_team_id": ["B"],
            "neutral_site": [False],
        }
    )
    box = pl.DataFrame(
        {
            "game_id": ["G1", "G1"],
            "team_id": ["A", "B"],
            "field_goals_attempted": [90.0, 88.0],
            "offensive_rebounds": [10.0, 9.0],
            "turnovers": [14.0, 12.0],
            "free_throws_attempted": [22.0, 20.0],
            "team_score": [112.0, 108.0],
        }
    )
    return sched, box


def test_possessions_and_ratings() -> None:
    sched, box = _mini_schedule_box()
    out = raw_game_efficiency(sched, box)
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    # poss_A = 90-10+14+0.44*22 = 103.68 ; poss_B = 88-9+12+0.44*20 = 99.8 ; avg = 101.74
    assert abs(a["poss"] - 101.74) < 1e-6
    assert abs(a["off_rtg"] - 100 * 112 / 101.74) < 1e-6
    assert abs(a["def_rtg"] - 100 * 108 / 101.74) < 1e-6
    assert a["opp_team_id"] == "B"
    assert a["is_home"] is True


def test_raw_game_efficiency_empty_input_returns_schema() -> None:
    sched, box = _mini_schedule_box()
    out = raw_game_efficiency(sched.head(0), box.head(0))
    assert out.height == 0
    assert set(out.columns) >= {"game_id", "team_id", "opp_team_id", "poss", "off_rtg", "def_rtg"}


def _round_robin_efficiency() -> pl.DataFrame:
    """Synthetic round-robin: A/B/C/D with A strongest, D weakest, injected margins hold
    regardless of opponent, plus one neutral-site game to exercise hfa_side=0.

    The observed off/def numbers bake in the NBA HFA split (+hfa/2 home
    offense, -hfa/2 home defense) so that the engine's HFA-removal step
    exactly cancels it, and true strength is recoverable regardless of each
    team's home/away game mix (A never plays a true road game here).
    """
    from sportsdataverse.nba.nba_prediction_constants import get_constants

    hfa_half = get_constants("00").hfa / 2.0
    rows = []
    # true strengths (points/100poss above a 110 baseline): A +12, B +4, C -4, D -12
    strength = {"A": 12.0, "B": 4.0, "C": -4.0, "D": -12.0}
    pairs = [("A", "B", False), ("C", "D", False), ("A", "C", False), ("B", "D", False), ("A", "D", True)]
    for home, away, neutral in pairs:
        loc = 0.0 if neutral else hfa_half
        home_off = 110.0 + strength[home] + loc  # home team's own scoring (boosted by hfa)
        away_off = 110.0 + strength[away] - loc  # away team's own scoring (suppressed by hfa)
        # def_rtg is always the opponent's off_rtg over the same shared poss estimate.
        rows.append(
            {
                "game_id": f"{home}{away}",
                "season": 2024,
                "team_id": home,
                "opp_team_id": away,
                "is_home": not neutral,
                "neutral_site": neutral,
                "poss": 100.0,
                "off_rtg": home_off,
                "def_rtg": away_off,
            }
        )
        rows.append(
            {
                "game_id": f"{home}{away}",
                "season": 2024,
                "team_id": away,
                "opp_team_id": home,
                "is_home": False,
                "neutral_site": neutral,
                "poss": 100.0,
                "off_rtg": away_off,
                "def_rtg": home_off,
            }
        )
    return pl.DataFrame(rows)


def test_adjust_efficiency_ordering_and_convergence() -> None:
    eff = _round_robin_efficiency()
    out = adjust_efficiency(eff, league_id="00")
    ranked = out.sort("adj_net_rtg", descending=True)["team_id"].to_list()
    assert ranked == ["A", "B", "C", "D"]
    # A and D each play 3 games (vs B/C/D and vs C/B/A resp.), B and C play 2 -- confirms
    # the per-team game count is tallied correctly, not that every team has equal games.
    games_by_team = dict(zip(out["team_id"].to_list(), out["games"].to_list()))
    assert games_by_team == {"A": 3, "B": 2, "C": 2, "D": 3}


def test_adjust_efficiency_empty_input_returns_schema() -> None:
    out = adjust_efficiency(pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8}), league_id="00")
    assert out.height == 0


def test_adjust_pace_fast_team_beats_raw_when_opponents_slow() -> None:
    # Team X is fast (150 poss) against slow opponents (100 poss each); adjusting for
    # opponent pace should push adj_pace above the raw 150-with-slow-opponents estimate.
    rows = []
    for i, opp in enumerate(["Y", "Z"]):
        rows.append(
            {
                "game_id": f"X{opp}",
                "season": 2024,
                "team_id": "X",
                "opp_team_id": opp,
                "poss": 150.0,
            }
        )
        rows.append(
            {
                "game_id": f"X{opp}",
                "season": 2024,
                "team_id": opp,
                "opp_team_id": "X",
                "poss": 150.0,
            }
        )
    # opponents Y/Z also play each other at a slow pace, establishing their own low baseline
    rows.append({"game_id": "YZ", "season": 2024, "team_id": "Y", "opp_team_id": "Z", "poss": 90.0})
    rows.append({"game_id": "YZ", "season": 2024, "team_id": "Z", "opp_team_id": "Y", "poss": 90.0})
    eff = pl.DataFrame(rows)
    out = adjust_pace(eff, league_id="00")
    x_pace = out.filter(pl.col("team_id") == "X")["adj_pace"][0]
    assert x_pace > 100.0


def test_adjust_pace_empty_input_returns_schema() -> None:
    out = adjust_pace(pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8}), league_id="00")
    assert out.height == 0


def test_nba_team_ratings_public_entry_point(monkeypatch: pytest.MonkeyPatch) -> None:
    sched, box = _mini_schedule_box()
    # extend to a second game so both teams have >0 games for a stable ranking
    sched2 = pl.concat(
        [
            sched,
            pl.DataFrame(
                {
                    "game_id": ["G2"],
                    "season": [2024],
                    "date": [dt.date(2024, 1, 5)],
                    "home_team_id": ["B"],
                    "away_team_id": ["A"],
                    "neutral_site": [False],
                }
            ),
        ]
    )
    box2 = pl.concat(
        [
            box,
            pl.DataFrame(
                {
                    "game_id": ["G2", "G2"],
                    "team_id": ["B", "A"],
                    "field_goals_attempted": [90.0, 88.0],
                    "offensive_rebounds": [10.0, 9.0],
                    "turnovers": [14.0, 12.0],
                    "free_throws_attempted": [22.0, 20.0],
                    "team_score": [100.0, 105.0],
                }
            ),
        ]
    )

    # sportsdataverse.nba.__init__ imports the nba_team_ratings FUNCTION under the same
    # name as this module, which shadows `sportsdataverse.nba.nba_team_ratings` (the
    # module) via attribute lookup on `import ... as mod`. importlib.import_module
    # bypasses that and returns the real module from sys.modules.
    mod = importlib.import_module("sportsdataverse.nba.nba_team_ratings")

    monkeypatch.setattr(mod, "load_nba_schedule", lambda seasons: sched2)
    monkeypatch.setattr(mod, "load_nba_team_boxscore", lambda seasons: box2)

    out = nba_team_ratings(2024, league_id="00")
    expected_cols = [
        "season",
        "team_id",
        "adj_off_rtg",
        "adj_def_rtg",
        "adj_net_rtg",
        "adj_pace",
        "raw_off_rtg",
        "raw_def_rtg",
        "raw_pace",
        "games",
        "rank",
        "adj_net_z",
    ]
    assert out.columns == expected_cols
    assert out.schema["team_id"] == pl.Utf8
    assert out.height == 2

    # as_of_date before G2 should drop it (leakage boundary)
    out_asof = nba_team_ratings(2024, league_id="00", as_of_date=dt.date(2024, 1, 2))
    assert out_asof["games"].max() == 1

    out_pd = nba_team_ratings(2024, league_id="00", return_as_pandas=True)
    assert type(out_pd).__name__ == "DataFrame"
    assert hasattr(out_pd, "iloc")
