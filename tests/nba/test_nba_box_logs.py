"""Tests for nba_box_logs: per-100 box features + fetch interface."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_box_logs import box_features


def _logs():
    # 2 games, one team (id 1), one player (id 10) who plays all the team's minutes
    player = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 1],
            "player_id": [10, 10],
            "min": [240.0, 240.0],
            "pts": [20, 30],
            "fg3m": [2, 3],
            "fga": [15, 20],
            "fta": [4, 6],
            "ast": [5, 7],
            "oreb": [1, 2],
            "dreb": [4, 5],
            "stl": [1, 2],
            "blk": [0, 1],
            "tov": [3, 2],
            "pf": [2, 3],
        }
    )
    team = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 1],
            "min": [240.0, 240.0],
            "fga": [80, 85],
            "oreb": [10, 12],
            "tov": [14, 12],
            "fta": [20, 22],
        }
    )
    return player, team


def test_box_features_per100_and_totals():
    player, team = _logs()
    f = box_features(player, team)
    assert f.height == 1 and f["player_id"][0] == 10
    assert f["gp"][0] == 2 and abs(f["min"][0] - 480.0) < 1e-9
    # team_poss = (80-10+14+0.44*20)+(85-12+12+0.44*22) = 92.8 + 94.68 = 187.48
    # player plays all minutes -> player_poss = 187.48 ; pts/100 = 50/187.48*100
    assert abs(f["pts"][0] - (50 / 187.48 * 100)) < 1e-6


def test_box_features_real_parser_fg3_m_column():
    # The live ``leaguegamelog`` parser snake-cases ``FG3M`` -> ``"fg3_m"`` (underscore
    # before the trailing M), NOT ``"fg3m"``. ``box_features`` must canonicalize that at
    # the boundary or it raises ``ColumnNotFoundError`` on real data (synthetic fixtures
    # hid the bug by using ``"fg3m"`` directly). This exercises the real column name.
    player, team = _logs()
    player = player.rename({"fg3m": "fg3_m"})
    f = box_features(player, team)
    # Same team_poss as test_box_features_per100_and_totals (187.48); fg3m total = 2+3 = 5
    assert f.height == 1 and f["player_id"][0] == 10
    assert "fg3m" in f.columns and "fg3_m" not in f.columns
    assert abs(f["fg3m"][0] - (5 / 187.48 * 100)) < 1e-6
    # pts unaffected by the rename
    assert abs(f["pts"][0] - (50 / 187.48 * 100)) < 1e-6


def test_box_features_game_id_restriction():
    player, team = _logs()
    only_g1 = box_features(player, team, game_ids=["G1"])
    # G1 only: team_poss = 92.8 ; pts/100 = 20/92.8*100
    assert abs(only_g1["pts"][0] - (20 / 92.8 * 100)) < 1e-6


def test_box_features_traded_player_uses_per_game_pace():
    # player 10 plays G1 for team 1 (fast) and G2 for team 2 (slow); each game full team minutes
    player = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 2],
            "player_id": [10, 10],
            "min": [240.0, 240.0],
            "pts": [20, 20],
            "fg3m": [2, 2],
            "fga": [15, 15],
            "fta": [4, 4],
            "ast": [5, 5],
            "oreb": [1, 1],
            "dreb": [4, 4],
            "stl": [1, 1],
            "blk": [0, 0],
            "tov": [3, 3],
            "pf": [2, 2],
        }
    )
    team = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 2],
            "min": [240.0, 240.0],
            "fga": [100, 70],
            "oreb": [10, 8],
            "tov": [14, 10],
            "fta": [20, 15],
        }
    )
    f = box_features(player, team)
    # per-game player_poss: G1 team_poss=100-10+14+0.44*20=112.8 ; G2=70-8+10+0.44*15=78.6
    # player plays all minutes -> player_poss = 112.8 + 78.6 = 191.4 ; pts/100 = 40/191.4*100
    assert f.height == 1 and f["player_id"][0] == 10
    assert abs(f["pts"][0] - (40 / 191.4 * 100)) < 1e-6
