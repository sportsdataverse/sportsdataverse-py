import datetime
import warnings
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.errors import InsufficientInputError
from sportsdataverse.mbb.mbb_team_ratings import (
    _normalize_schedule,
    adjust_efficiency,
    adjust_tempo,
    raw_game_efficiency,
)


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


def test_mbb_team_ratings_missing_season_columnless_frames(monkeypatch):
    """A season with no released boxscore asset comes back COLUMN-LESS from the
    loader (it warns "no data for season(s) [...] (skipped)" and returns a frame
    with no columns at all -- e.g. WBB 2003). This crashed raw_game_efficiency
    with ColumnNotFoundError instead of honoring the documented
    empty-in/empty-out contract."""
    import importlib

    mod = importlib.import_module("sportsdataverse.mbb.mbb_team_ratings")

    sched = pl.DataFrame(
        {
            "game_id": ["G1"],
            "season": [2002],
            "date": [datetime.date(2002, 1, 1)],
            "home_team_id": ["A"],
            "away_team_id": ["B"],
            "neutral_site": [False],
        }
    )
    monkeypatch.setattr(mod, "load_mbb_schedule", lambda seasons: sched)
    monkeypatch.setattr(mod, "load_mbb_team_boxscore", lambda seasons: pl.DataFrame())

    out = mod.mbb_team_ratings([2002])
    assert out.columns == _RATINGS_COLUMNS
    assert out.height == 0


def test_raw_game_efficiency_columnless_inputs_return_typed_empty():
    import importlib

    mod = importlib.import_module("sportsdataverse.mbb.mbb_team_ratings")

    out = raw_game_efficiency(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert dict(out.schema) == dict(mod._EFF_SCHEMA)


# ---------------------------------------------------------------------------
# Regression: one all-zero boxscore shell used to take a whole season non-finite
# (published mbb_ratings_2011: 346/346 qualified teams NaN in adj_o/adj_d/adj_em
# from ESPN game 310573129; wbb_ratings_2015: 335/335 from game 400768032).
# ---------------------------------------------------------------------------


def _season_with_shell_game():
    """A 4-team round robin PLUS one game whose box is a scoreline with zero counters."""
    teams = ["A", "B", "C", "D"]
    sched_rows, box_rows = [], []
    gid = 0
    for i, home in enumerate(teams):
        for away in teams[i + 1 :]:
            gid += 1
            sched_rows.append(
                {
                    "game_id": f"G{gid}",
                    "season": 2011,
                    "date": datetime.date(2011, 1, 1),
                    "home_team_id": home,
                    "away_team_id": away,
                    "neutral_site": False,
                }
            )
            for t, pts in ((home, 75.0), (away, 70.0)):
                box_rows.append(
                    {
                        "game_id": f"G{gid}",
                        "team_id": t,
                        "field_goals_attempted": 60.0,
                        "offensive_rebounds": 10.0,
                        "turnovers": 12.0,
                        "free_throws_attempted": 20.0,
                        "team_score": pts,
                    }
                )
    # the shell: a real final score, every counter zeroed -> poss == 0
    sched_rows.append(
        {
            "game_id": "SHELL",
            "season": 2011,
            "date": datetime.date(2011, 2, 26),
            "home_team_id": "A",
            "away_team_id": "B",
            "neutral_site": False,
        }
    )
    for t, pts in (("A", 76.0), ("B", 78.0)):
        box_rows.append(
            {
                "game_id": "SHELL",
                "team_id": t,
                "field_goals_attempted": 0.0,
                "offensive_rebounds": 0.0,
                "turnovers": 0.0,
                "free_throws_attempted": 0.0,
                "team_score": pts,
            }
        )
    return pl.DataFrame(sched_rows), pl.DataFrame(box_rows)


def test_zero_possession_row_is_dropped_and_warns():
    sched, box = _season_with_shell_game()
    with pytest.warns(UserWarning, match="non-positive possession"):
        eff = raw_game_efficiency(sched, box)
    assert "SHELL" not in eff["game_id"].to_list()
    assert eff.height == 12  # 6 round-robin games x 2 team rows
    for c in ("poss", "off_eff", "def_eff"):
        assert eff[c].is_finite().all()


def test_one_shell_game_does_not_poison_the_whole_season():
    """The bug: `avg = off.mean()` is inf, so EVERY team's fixed point goes non-finite."""
    sched, box = _season_with_shell_game()
    with pytest.warns(UserWarning):
        eff = raw_game_efficiency(sched, box)
    ratings = adjust_efficiency(eff, league="mens")
    tempo = adjust_tempo(eff, league="mens")
    assert ratings.height == 4
    for c in ("adj_o", "adj_d", "adj_em", "raw_o", "raw_d"):
        assert ratings[c].is_finite().all(), f"{c} went non-finite"
    assert tempo["adj_tempo"].is_finite().all()


def test_structurally_absent_turnovers_are_refused():
    """A season whose every game has 0 turnovers (WBB 2008: a schema gap, not a value)."""
    sched, box = _mini()
    zeroed = box.with_columns(pl.lit(0.0).alias("turnovers"))
    with pytest.raises(InsufficientInputError, match="no turnovers"):
        raw_game_efficiency(sched, zeroed)


def test_one_sided_shell_is_dropped_too():
    """A shell on ONE side keeps `poss` positive but halves it -- WBB 2018 game 400998743
    scored a team at 272 efficiency (and 1452 where both boxes were partial)."""
    sched, box = _season_with_shell_game()
    # make the shell one-sided: give team B a real box in that game
    box = box.with_columns(
        pl.when((pl.col("game_id") == "SHELL") & (pl.col("team_id") == "B"))
        .then(69.0)
        .otherwise(pl.col("field_goals_attempted"))
        .alias("field_goals_attempted"),
        pl.when((pl.col("game_id") == "SHELL") & (pl.col("team_id") == "B"))
        .then(7.0)
        .otherwise(pl.col("turnovers"))
        .alias("turnovers"),
    )
    with pytest.warns(UserWarning, match="non-positive possession"):
        eff = raw_game_efficiency(sched, box)
    assert "SHELL" not in eff["game_id"].to_list()
    assert eff["off_eff"].max() < 200.0


def test_real_2011_shell_game_does_not_poison_the_season(monkeypatch):
    """Real ESPN slice, not a synthetic box: every 2011 game of New Orleans (2443) and
    Victory (3129), including shell game 310573129 (score kept, FGA/OREB/TO/FTA all 0).
    Without the drop, all 16 teams' adj_o/adj_d/adj_em/adj_em_z come out NaN -- the
    published mbb_ratings_2011 before its 2026-09-08 rebuild, still in mbb.ratings."""
    import importlib
    from pathlib import Path

    mod = importlib.import_module("sportsdataverse.mbb.mbb_team_ratings")
    fix = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_prediction"
    sched = pl.read_parquet(fix / "shell_game_schedule_2011.parquet")
    box = pl.read_parquet(fix / "shell_game_team_box_2011.parquet")
    monkeypatch.setattr(mod, "load_mbb_schedule", lambda seasons: sched)
    monkeypatch.setattr(mod, "load_mbb_team_boxscore", lambda seasons: box)

    with pytest.warns(UserWarning, match="310573129"):
        out = mod.mbb_team_ratings(2011)

    assert out.height == box["team_id"].n_unique() == 16
    for c in ("adj_o", "adj_d", "adj_em", "adj_tempo", "raw_o", "raw_d", "adj_em_z"):
        assert out[c].is_finite().all(), f"{c} went non-finite"
    games = dict(zip(out["team_id"].to_list(), out["games"].to_list()))
    assert games["2443"] == 19 and games["3129"] == 2  # the shell game is gone, the rest kept


# ---------------------------------------------------------------------------
# WBB 2009-2012: ESPN files each team's turnover total under `teamTurnovers`
# (loader column `team_turnovers`) with `turnovers`/`total_turnovers` both 0;
# from 2013 it is under `turnovers`. Real slices in
# tests/fixtures/wbb_prediction/turnover_key_*.parquet (see its README).
# ---------------------------------------------------------------------------

_WBB_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "wbb_prediction"


def _turnover_key_slice(season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    sched = pl.read_parquet(_WBB_FIX / "turnover_key_schedule.parquet").filter(pl.col("season") == season)
    box = pl.read_parquet(_WBB_FIX / "turnover_key_team_box.parquet").filter(pl.col("season") == season)
    return _normalize_schedule(sched), box


def _poss(eff: pl.DataFrame, game_id: str) -> float:
    return eff.filter(pl.col("game_id") == game_id)["poss"].unique().item()


def test_legacy_team_turnovers_key_supplies_the_turnovers():
    """2010 final (UConn 53, Stanford 47): both teams' 10 turnovers live in `team_turnovers`."""
    sched, box = _turnover_key_slice(2010)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        eff = raw_game_efficiency(sched, box)
    assert eff["game_id"].n_unique() == 64  # every STAN / CONN game kept
    # STAN 68 - 13 + 10 + 0.44*4 = 66.76 ; CONN 58 - 10 + 10 + 0.44*22 = 67.68
    assert _poss(eff, "300960041") == pytest.approx(0.5 * (66.76 + 67.68))
    # without the turnover term the slice's tempo reads ~55; real WBB tempo is ~70
    assert 65.0 < eff["poss"].mean() < 80.0


def test_turnover_key_fallback_is_per_row():
    """One season mixing both layouts must rate exactly like the all-legacy season.

    Half the 2010 games are rewritten into the 2013+ layout (the same count moved
    to `turnovers`/`total_turnovers`); a season- or frame-level switch would leave
    the other half at 0 and drop them.
    """
    sched, box = _turnover_key_slice(2010)
    modern = pl.col("game_id") % 2 == 0
    mixed = box.with_columns(
        pl.when(modern).then(pl.col("team_turnovers")).otherwise(pl.col("turnovers")).alias("turnovers"),
        pl.when(modern).then(pl.col("team_turnovers")).otherwise(pl.col("total_turnovers")).alias("total_turnovers"),
        pl.when(modern).then(0).otherwise(pl.col("team_turnovers")).alias("team_turnovers"),
    )
    assert 0 < mixed.filter(pl.col("turnovers") > 0).height < mixed.height  # both layouts present
    assert_frame_equal(raw_game_efficiency(sched, mixed), raw_game_efficiency(sched, box))


def test_modern_rows_keep_their_turnovers():
    """2014: `turnovers` > 0, and `team_turnovers` carries a different (smaller) count."""
    sched, box = _turnover_key_slice(2014)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        eff = raw_game_efficiency(sched, box)
    assert eff["game_id"].n_unique() == 40
    # CONN 89 - HART 34: CONN 60 - 10 + 8 + 0.44*15 = 64.6 ; HART 44 - 6 + 22 + 0.44*8 = 63.52
    # (team_turnovers is 2 for both -- using it would give 58.6 / 43.52)
    assert _poss(eff, "400509246") == pytest.approx(0.5 * (64.6 + 63.52))


def test_frame_without_the_extra_turnover_columns_is_unchanged():
    """The mbb path and older frames carry only `turnovers`: same output, no warning."""
    sched, box = _turnover_key_slice(2014)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        bare = raw_game_efficiency(sched, box.drop("team_turnovers", "total_turnovers"))
        only_team = raw_game_efficiency(sched, box.drop("total_turnovers"))
    assert_frame_equal(bare, raw_game_efficiency(sched, box))
    assert_frame_equal(only_team, bare)


def test_zero_turnover_game_is_dropped_with_a_warning():
    """2009 Purdue: OSU 71, PUR 60 (290252509) has 0 under every turnover key, 1 of 32 games."""
    sched, box = _turnover_key_slice(2009)
    with pytest.warns(UserWarning, match=r"dropped 2 team-game row\(s\) from 1 game\(s\).*0 turnovers.*290252509"):
        eff = raw_game_efficiency(sched, box)
    assert "290252509" not in eff["game_id"].to_list()
    assert eff["game_id"].n_unique() == 31


def _zero_game_plus(others: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The 2009 zero-turnover game plus the first ``others`` of Purdue's other games."""
    sched, box = _turnover_key_slice(2009)
    keep = ["290252509"] + sched.filter(pl.col("game_id") != 290252509)["game_id"].cast(pl.Utf8).sort().head(
        others
    ).to_list()
    return sched.filter(pl.col("game_id").cast(pl.Utf8).is_in(keep)), box


def test_more_than_ten_percent_zero_turnover_games_rejects_the_season():
    # 1 of 10 games is exactly 10%: dropped with a warning, season kept
    with pytest.warns(UserWarning, match="0 turnovers"):
        assert raw_game_efficiency(*_zero_game_plus(9))["game_id"].n_unique() == 9
    # 1 of 9 is 11.1%: refused
    with pytest.raises(InsufficientInputError, match=r"2009: 1 of 9 games"):
        raw_game_efficiency(*_zero_game_plus(8))


def test_2008_stays_rejected():
    """WBB 2008 has no turnovers under any key (10 of 1,761 games league-wide)."""
    sched, box = _turnover_key_slice(2008)
    with pytest.raises(InsufficientInputError, match=r"2008: 22 of 22 games"):
        raw_game_efficiency(sched, box)
    # a clean season alongside does not rescue it
    s10, b10 = _turnover_key_slice(2010)
    with pytest.raises(InsufficientInputError, match="2008"):
        raw_game_efficiency(pl.concat([sched, s10]), pl.concat([box, b10]))
