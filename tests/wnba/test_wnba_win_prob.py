"""Tests for the WNBA season win-probability enrichment (``wnba_win_prob``).

The helper enriches the pbp dataset **in place**: it appends two columns
(``pregame_home_prob``, ``home_win_prob``) to the ``load_wnba_pbp`` frame and
preserves every original column + dtype. ``home_win_prob`` is
:func:`wnba_in_game_win_prob`'s output unchanged, so the **reproduction gate**
below (byte parity for a game) transitively guarantees the decile-calibration
gate already proven on the shared NBA-spine artifact.
"""

from __future__ import annotations

import datetime

import pandas as pd
import polars as pl

from sportsdataverse.wnba.wnba_game_predict import wnba_in_game_win_prob, wnba_predict_margin, wnba_win_prob_from_margin
from sportsdataverse.wnba.wnba_win_prob import _WP_COLS, _compile_season_wp, build_wnba_season_wp

_MON1 = datetime.date(2024, 1, 1)  # Monday
_MON3 = datetime.date(2024, 1, 15)  # two weeks later (Monday)

_FALLBACK = wnba_win_prob_from_margin(wnba_predict_margin(0.0, 0.0, home_pace=1.0, away_pace=1.0, neutral=False))


def _schedule() -> pl.DataFrame:
    """Two week-1 games (rating source) + one week-3 game (gets an as-of prob).

    Ids are ``Int32`` to mirror the raw ``load_wnba_schedule`` dtype (the trap
    ``_pregame_probs`` must cast past before calling ``wnba_predict_games``).
    """
    return pl.DataFrame(
        {
            "game_id": [1, 2, 3],
            "season": [2024, 2024, 2024],
            "date": [_MON1, _MON1, _MON3],
            "home_team_id": [1, 3, 1],
            "away_team_id": [2, 4, 3],
            "home_score": [75, 60, 80],
            "away_score": [70, 66, 62],
            "neutral_site": [False, False, False],
        },
        schema_overrides={
            "game_id": pl.Int32,
            "season": pl.Int32,
            "home_team_id": pl.Int32,
            "away_team_id": pl.Int32,
            "home_score": pl.Int32,
            "away_score": pl.Int32,
        },
    )


def _team_box() -> pl.DataFrame:
    """Two team rows per week-1 game (efficiency source for the as-of ratings)."""
    rows = []
    for gid, (h, a) in ((1, (1, 2)), (2, (3, 4))):
        for tid, sc in ((h, 75 if gid == 1 else 60), (a, 70 if gid == 1 else 66)):
            rows.append(
                {
                    "game_id": gid,
                    "season": 2024,
                    "game_date": _MON1,
                    "team_id": tid,
                    "field_goals_attempted": 60.0,
                    "offensive_rebounds": 10.0,
                    "turnovers": 12.0,
                    "free_throws_attempted": 20.0,
                    "team_score": float(sc),
                }
            )
    return pl.DataFrame(rows, schema_overrides={"game_id": pl.Int32, "season": pl.Int32, "team_id": pl.Int32})


def _pbp_game(gid: int, home: int, away: int, hname: str, aname: str, date: datetime.date) -> pl.DataFrame:
    """A tiny 3-play pbp frame in the ``load_wnba_pbp`` schema (Int32 ids)."""
    return pl.DataFrame(
        {
            "game_id": [gid, gid, gid],
            "season": [2024, 2024, 2024],
            "game_play_number": [1, 2, 3],
            "game_date": [date, date, date],
            "home_team_name": [hname, hname, hname],
            "away_team_name": [aname, aname, aname],
            "home_score": [0, 40, 80],
            "away_score": [0, 38, 62],
            "start_game_seconds_remaining": [2400, 1200, 20],
            "team_id": [home, away, home],
            "home_team_id": [home, home, home],
        },
        schema_overrides={
            "game_id": pl.Int32,
            "season": pl.Int32,
            "game_play_number": pl.Int32,
            "home_score": pl.Int32,
            "away_score": pl.Int32,
            "start_game_seconds_remaining": pl.Int32,
            "team_id": pl.Int32,
            "home_team_id": pl.Int32,
        },
    )


def _pbp() -> pl.DataFrame:
    # game 1 (week 1, no prior -> fallback anchor), game 3 (week 3, real as-of prob)
    return pl.concat([_pbp_game(1, 1, 2, "Sky", "Mystics", _MON1), _pbp_game(3, 1, 3, "Sky", "Aces", _MON3)])


def test_enriches_pbp_in_place_preserving_columns_and_dtypes():
    src = _pbp()
    out = _compile_season_wp(src, _schedule(), _team_box())
    for col, dt in src.schema.items():
        assert out.schema[col] == dt, f"{col} dtype changed {dt} -> {out.schema[col]}"
    assert set(out.columns) - set(src.columns) == set(_WP_COLS)
    assert out.schema["pregame_home_prob"] == pl.Float64
    assert out.schema["home_win_prob"] == pl.Float64
    assert out.height == src.height  # one row per play, no drops


def test_sorted_by_game_then_play():
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    assert out.select("game_id", "game_play_number").rows() == sorted(out.select("game_id", "game_play_number").rows())


def test_reproduces_in_game_win_prob_for_a_game():
    """GATE: per-play home_win_prob is exactly wnba_in_game_win_prob's output."""
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    g = out.filter(pl.col("game_id") == 3).sort("game_play_number")
    p0 = g.get_column("pregame_home_prob")[0]
    expected = wnba_in_game_win_prob(_pbp_game(3, 1, 3, "Sky", "Aces", _MON3), p0)
    assert g.get_column("home_win_prob").to_list() == expected.get_column("home_win_prob").to_list()


def test_week3_game_gets_real_as_of_prob_not_fallback():
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    p3 = out.filter(pl.col("game_id") == 3).get_column("pregame_home_prob")[0]
    assert abs(p3 - _FALLBACK) > 1e-9, "week-3 game should have an as-of rating-based prob, not the flat anchor"


def test_opening_week_game_uses_fallback_anchor():
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    p1 = out.filter(pl.col("game_id") == 1).get_column("pregame_home_prob")[0]
    assert abs(p1 - _FALLBACK) < 1e-9, "opening-week game has no prior -> HFA-only fallback anchor"


def test_nan_pregame_anchor_coerced_to_fallback(monkeypatch):
    """A degenerate NaN as-of rating must never publish a NaN pregame_home_prob."""
    monkeypatch.setattr(
        "sportsdataverse.wnba.wnba_win_prob._pregame_probs",
        lambda schedule, team_box: pl.DataFrame({"game_id": ["3"], "pregame_home_prob": [float("nan")]}),
    )
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    p3 = out.filter(pl.col("game_id") == 3).get_column("pregame_home_prob")[0]
    assert p3 == _FALLBACK
    assert out.get_column("pregame_home_prob").is_nan().sum() == 0
    assert out.get_column("home_win_prob").is_nan().sum() == 0


def test_empty_team_box_falls_back_without_crashing():
    """A season with pbp but no boxscores (empty team_box) must not crash."""
    out = _compile_season_wp(_pbp(), _schedule(), pl.DataFrame())
    assert out.height == _pbp().height
    assert out.get_column("pregame_home_prob").unique().to_list() == [_FALLBACK]
    assert out.get_column("home_win_prob").is_nan().sum() == 0


def test_empty_pbp_returns_unchanged():
    empty = pl.DataFrame()
    out = _compile_season_wp(empty, _schedule(), _team_box())
    assert out.height == 0


def test_return_as_pandas(monkeypatch):
    monkeypatch.setattr(
        "sportsdataverse.wnba.wnba_win_prob._load_wnba_frames",
        lambda season: (_pbp(), _schedule(), _team_box()),
    )
    out = build_wnba_season_wp(2024, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert set(_WP_COLS).issubset(out.columns)


def test_return_as_polars_default(monkeypatch):
    monkeypatch.setattr(
        "sportsdataverse.wnba.wnba_win_prob._load_wnba_frames",
        lambda season: (_pbp(), _schedule(), _team_box()),
    )
    out = build_wnba_season_wp(2024)
    assert isinstance(out, pl.DataFrame)
    assert set(_WP_COLS).issubset(out.columns)


def test_real_season_slice_compiles_and_scores_against_real_outcomes(monkeypatch):
    """supp-013: build_wnba_season_wp compiles a real 2024 season slice, scored against real results.

    pbp: tests/fixtures/nba_prediction/wnba_pbp_sample_2024.parquet (every 10th play of all 264 games of
    the 2024 `load_wnba_pbp` release, with each game's final-score `home_win`); schedule + team box: the
    committed wnba_results_2024 / wnba_team_box_2024 (the box's `game_date` joined from the schedule, as
    the loader ships it). Observed 2026-10-11: Brier in-game 0.16402, pregame 0.20678, base 0.24793.
    """
    import importlib
    from pathlib import Path

    fix = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"
    pbp = pl.read_parquet(fix / "wnba_pbp_sample_2024.parquet")
    sched = pl.read_parquet(fix / "wnba_results_2024.parquet")
    box = pl.read_parquet(fix / "wnba_team_box_2024.parquet").join(
        sched.select("game_id", pl.col("date").alias("game_date")), on="game_id", how="left"
    )
    core = importlib.import_module("sportsdataverse.wnba.wnba_win_prob")
    monkeypatch.setattr(core, "_load_wnba_frames", lambda season: (pbp, core._normalize_schedule(sched), box))
    out = build_wnba_season_wp(2024)
    assert out.height == pbp.height and set(pbp.columns) <= set(out.columns)
    y = out["home_win"].cast(pl.Float64)
    brier_wp = float(((out["home_win_prob"] - y) ** 2).mean())
    brier_pre = float(((out["pregame_home_prob"] - y) ** 2).mean())
    assert brier_wp <= 0.1641 and brier_pre <= 0.2068, (brier_wp, brier_pre)
    assert brier_wp < brier_pre < float(((y.mean() - y) ** 2).mean())
    print("WNBA_SEASON_WP_REAL_OK", round(brier_wp, 5), round(brier_pre, 5))
