import datetime

import polars as pl

from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency


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


def _round_robin_eff() -> pl.DataFrame:
    """Double round-robin of 4 teams with injected net strengths + a neutral game.

    Each unordered pair plays twice (home & away) so HFA cancels; team i's
    per-game net efficiency is ``S_i - S_j``, so the recovered AdjEM must order
    the teams A > B > C > D.
    """
    strength = {"A": 20.0, "B": 7.0, "C": -7.0, "D": -20.0}
    rows: list[dict] = []
    gid = 0

    def add(i: str, j: str, neutral: bool) -> None:
        nonlocal gid
        gid += 1
        margin = (strength[i] - strength[j]) / 2.0
        base = dict(game_id=f"G{gid}", season=2024, date=datetime.date(2024, 1, 1), poss=70.0)
        rows.append(
            {
                **base,
                "team_id": i,
                "opp_team_id": j,
                "is_home": not neutral,
                "neutral_site": neutral,
                "off_eff": 100 + margin,
                "def_eff": 100 - margin,
            }
        )
        rows.append(
            {
                **base,
                "team_id": j,
                "opp_team_id": i,
                "is_home": False,
                "neutral_site": neutral,
                "off_eff": 100 - margin,
                "def_eff": 100 + margin,
            }
        )

    teams = list(strength)
    for a in teams:
        for b in teams:
            if a != b:
                add(a, b, neutral=False)  # a home, b away (both directions across the loop)
    add("A", "D", neutral=True)  # exercise the neutral (hfa_side=0) branch
    return pl.DataFrame(rows)


def test_adjust_efficiency_recovers_strength_ordering():
    game_eff = _round_robin_eff()
    ratings = adjust_efficiency(game_eff, league="mens")

    assert ratings.columns == ["season", "team_id", "adj_o", "adj_d", "adj_em", "raw_o", "raw_d", "games"]
    ordered = ratings.sort("adj_em", descending=True)["team_id"].to_list()
    assert ordered == ["A", "B", "C", "D"]

    games = dict(zip(ratings["team_id"].to_list(), ratings["games"].to_list()))
    assert games["A"] == 7  # 3 home + 3 away + 1 neutral
    assert games["B"] == 6  # 3 home + 3 away

    # all outputs finite (convergence produced sane numbers, not NaN/inf)
    for col in ("adj_o", "adj_d", "adj_em", "raw_o", "raw_d"):
        assert ratings[col].is_finite().all()
