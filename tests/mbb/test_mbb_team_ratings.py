import datetime

import polars as pl

from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, adjust_tempo, raw_game_efficiency


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


def _tempo_eff() -> pl.DataFrame:
    """FAST team (tempo 78) plays only SLOW opponents (tempo 60); league avg 67.

    Game possessions follow the additive model ``poss = tempo_i + tempo_j - avg``,
    so FAST's observed (raw) tempo is depressed by its slow opponents and the
    adjustment must push it back up.
    """
    tempo = {"FAST": 78.0, "S1": 60.0, "S2": 60.0, "S3": 60.0}
    avg = 67.0
    rows: list[dict] = []
    gid = 0

    def add(i: str, j: str) -> None:
        nonlocal gid
        gid += 1
        poss = tempo[i] + tempo[j] - avg
        base = dict(
            game_id=f"T{gid}",
            season=2024,
            date=datetime.date(2024, 1, 1),
            is_home=False,
            neutral_site=True,
            off_eff=100.0,
            def_eff=100.0,
            poss=poss,
        )
        rows.append({**base, "team_id": i, "opp_team_id": j})
        rows.append({**base, "team_id": j, "opp_team_id": i})

    add("FAST", "S1")
    add("FAST", "S2")
    add("FAST", "S3")
    add("S1", "S2")
    add("S1", "S3")
    add("S2", "S3")
    return pl.DataFrame(rows)


def test_adjust_tempo_pushes_fast_team_up():
    tempo = adjust_tempo(_tempo_eff(), league="mens")
    assert tempo.columns == ["season", "team_id", "adj_tempo"]
    row = {r["team_id"]: r["adj_tempo"] for r in tempo.iter_rows(named=True)}
    # FAST's observed game possessions all = 78+60-67 = 71; adjustment recovers ~78 > 71
    assert row["FAST"] > 71.0
    assert row["FAST"] == max(row.values())


_RATINGS_COLUMNS = [
    "season",
    "team_id",
    "adj_o",
    "adj_d",
    "adj_em",
    "adj_tempo",
    "raw_o",
    "raw_d",
    "games",
    "rank",
    "adj_em_z",
]


def test_mbb_team_ratings_public_schema(monkeypatch):
    import pandas as pd

    import importlib

    # the module and its public function share the name `mbb_team_ratings`; the
    # package `import *` rebinds the attribute to the function, so fetch the
    # module object from sys.modules to monkeypatch its loader imports.
    mod = importlib.import_module("sportsdataverse.mbb.mbb_team_ratings")

    sched, box = _mini()
    # add a second game so std/rank are well-defined over >1 team pairing
    sched2 = pl.DataFrame(
        {
            "game_id": ["G2"],
            "season": [2024],
            "date": [datetime.date(2024, 1, 2)],
            "home_team_id": ["B"],
            "away_team_id": ["A"],
            "neutral_site": [False],
        }
    )
    box2 = pl.DataFrame(
        {
            "game_id": ["G2", "G2"],
            "team_id": ["B", "A"],
            "field_goals_attempted": [58.0, 60.0],
            "offensive_rebounds": [9.0, 11.0],
            "turnovers": [11.0, 12.0],
            "free_throws_attempted": [17.0, 19.0],
            "team_score": [68.0, 78.0],
        }
    )
    full_sched = pl.concat([sched, sched2])
    full_box = pl.concat([box, box2])
    monkeypatch.setattr(mod, "load_mbb_schedule", lambda seasons: full_sched)
    monkeypatch.setattr(mod, "load_mbb_team_boxscore", lambda seasons: full_box)

    out = mod.mbb_team_ratings(2024)
    assert out.columns == _RATINGS_COLUMNS
    assert out.schema["team_id"] == pl.Utf8
    assert out.schema["rank"] == pl.Int64
    assert out.schema["adj_em_z"] == pl.Float64
    assert set(out["rank"].to_list()) == {1, 2}

    pdf = mod.mbb_team_ratings(2024, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == _RATINGS_COLUMNS


def test_mbb_team_ratings_empty_seasons(monkeypatch):
    import importlib

    # the module and its public function share the name `mbb_team_ratings`; the
    # package `import *` rebinds the attribute to the function, so fetch the
    # module object from sys.modules to monkeypatch its loader imports.
    mod = importlib.import_module("sportsdataverse.mbb.mbb_team_ratings")

    monkeypatch.setattr(
        mod,
        "load_mbb_schedule",
        lambda seasons: pl.DataFrame(
            schema={
                "game_id": pl.Utf8,
                "season": pl.Int64,
                "date": pl.Date,
                "home_team_id": pl.Utf8,
                "away_team_id": pl.Utf8,
                "neutral_site": pl.Boolean,
            }
        ),
    )
    monkeypatch.setattr(
        mod,
        "load_mbb_team_boxscore",
        lambda seasons: pl.DataFrame(
            schema={
                "game_id": pl.Utf8,
                "team_id": pl.Utf8,
                "field_goals_attempted": pl.Float64,
                "offensive_rebounds": pl.Float64,
                "turnovers": pl.Float64,
                "free_throws_attempted": pl.Float64,
                "team_score": pl.Float64,
            }
        ),
    )
    out = mod.mbb_team_ratings([2024])
    assert out.columns == _RATINGS_COLUMNS
    assert out.height == 0
