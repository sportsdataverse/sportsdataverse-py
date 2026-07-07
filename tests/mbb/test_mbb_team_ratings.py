import datetime

import polars as pl

from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency


def _mini():
    sched = pl.DataFrame(
        {
            "game_id": ["G1"],
            "season": [2024],
            "date": [datetime.date(2024, 1, 1)],
            "home_team_id": ["A"],
            "away_team_id": ["B"],
            "neutral_site": [False],
        }
    )
    box = pl.DataFrame(
        {
            "game_id": ["G1", "G1"],
            "team_id": ["A", "B"],
            "field_goals_attempted": [60.0, 55.0],
            "offensive_rebounds": [10.0, 8.0],
            "turnovers": [12.0, 10.0],
            "free_throws_attempted": [20.0, 18.0],
            "team_score": [75.0, 70.0],
        }
    )
    return sched, box


def test_possessions_and_efficiency():
    sched, box = _mini()
    out = raw_game_efficiency(sched, box)
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    # poss_A = 60-10+12+0.44*20 = 70.8 ; poss_B = 55-8+10+0.44*18 = 64.92 ; avg = 67.86
    assert abs(a["poss"] - 67.86) < 1e-6
    assert abs(a["off_eff"] - 100 * 75 / 67.86) < 1e-6
    assert abs(a["def_eff"] - 100 * 70 / 67.86) < 1e-6
    assert a["opp_team_id"] == "B"
    assert a["is_home"] is True
    assert a["neutral_site"] is False
