"""Tests for the team-runs + strikeout prop closed forms and the mlb_props orchestrator."""

import polars as pl

from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_strikeouts, mlb_prop_team_runs, mlb_props, prop_over_prob


def test_runs_neutral_matchup():
    # league-average offense vs league-average defense at neutral park -> league rpg
    assert abs(mlb_prop_team_runs(4.5, 4.5, 4.5) - 4.5) < 1e-9


def test_runs_strong_offense_weak_defense():
    assert mlb_prop_team_runs(5.5, 5.0, 4.5) > 4.5


def test_over_prob_monotone_in_line():
    assert prop_over_prob(3.5, 4.5) > prop_over_prob(8.5, 4.5)


def test_strikeouts_scale():
    # team K/9=9 over 9 innings vs league-average K rate -> ~9
    assert abs(mlb_prop_strikeouts(9.0, 0.22, 0.22) - 9.0) < 1e-9


def test_mlb_props_runs_only_when_ratings_lack_k9():
    matchups = pl.DataFrame({"game_id": ["G1"], "home_team_id": ["A"], "away_team_id": ["B"]})
    ratings = pl.DataFrame({"team_id": ["A", "B"], "off_rpg": [5.0, 4.0], "def_rpg": [4.5, 4.8]})
    props = mlb_props(matchups, ratings)
    assert props.height == 1
    assert props["exp_runs_home"][0] > 0
    assert props["exp_strikeouts_home"][0] is None


def test_mlb_props_with_strikeout_columns():
    matchups = pl.DataFrame({"game_id": ["G1"], "home_team_id": ["A"], "away_team_id": ["B"]})
    ratings = pl.DataFrame(
        {
            "team_id": ["A", "B"],
            "off_rpg": [5.0, 4.0],
            "def_rpg": [4.5, 4.8],
            "k9": [8.5, 9.5],
            "k_rate": [0.22, 0.24],
        }
    )
    props = mlb_props(matchups, ratings)
    assert props["exp_strikeouts_home"][0] is not None
    assert props["exp_strikeouts_home"][0] > 0
