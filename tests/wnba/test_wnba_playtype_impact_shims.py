"""Unit tests for the WNBA play-type/impact by-reference shims (league_id="10")."""

import polars as pl

from sportsdataverse.nba.nba_expected_turnovers import nba_expected_turnovers
from sportsdataverse.nba.nba_foul_drawing import nba_foul_drawing
from sportsdataverse.nba.nba_matchup_drapm import nba_matchup_drapm
from sportsdataverse.nba.nba_playtype import nba_playtype_ratings
from sportsdataverse.wnba.wnba_playtype_impact import (
    wnba_expected_turnovers,
    wnba_foul_drawing,
    wnba_matchup_drapm,
    wnba_playtype_ratings,
)


def test_shims_bind_league_id_10_to_the_nba_core():
    assert wnba_playtype_ratings.func is nba_playtype_ratings
    assert wnba_playtype_ratings.keywords["league_id"] == "10"
    assert wnba_matchup_drapm.func is nba_matchup_drapm
    assert wnba_matchup_drapm.keywords["league_id"] == "10"
    assert wnba_foul_drawing.func is nba_foul_drawing
    assert wnba_foul_drawing.keywords["league_id"] == "10"
    assert wnba_expected_turnovers.func is nba_expected_turnovers
    assert wnba_expected_turnovers.keywords["league_id"] == "10"


def test_shims_run_offline_with_injected_frames():
    off = pl.DataFrame(
        {"team_id": [1, 2], "play_type": ["Isolation", "Isolation"], "poss": [50.0, 60.0], "pts": [45.0, 55.0]}
    )
    deff = pl.DataFrame(
        {"team_id": [1, 2], "play_type": ["Isolation", "Isolation"], "poss": [55.0, 50.0], "pts": [50.0, 44.0]}
    )
    sched = pl.DataFrame({"team_id": [1, 2], "opp_team_id": [2, 1]})
    r = wnba_playtype_ratings("2024", off_team=off, def_team=deff, schedule=sched)
    assert r.schema["team_id"] == pl.Int64

    matchups = pl.DataFrame(
        {"off_player_id": [10, 11], "def_player_id": [20, 20], "partial_poss": [30.0, 30.0], "player_pts": [30.0, 28.0]}
    )
    d = wnba_matchup_drapm("2024", matchups=matchups)
    assert set(d.columns) == {"player_id", "matchup_drapm", "matchup_poss"}


def test_shims_sparse_coverage_degrades_to_zero_row():
    assert (
        wnba_playtype_ratings("2024", off_team=pl.DataFrame(), def_team=pl.DataFrame(), schedule=pl.DataFrame()).height
        == 0
    )
    assert wnba_matchup_drapm("2024", matchups=pl.DataFrame()).height == 0
    assert wnba_foul_drawing("2024", base=pl.DataFrame(), player_mix=pl.DataFrame()).height == 0
    assert wnba_expected_turnovers("2024", base=pl.DataFrame(), player_mix=pl.DataFrame()).height == 0
