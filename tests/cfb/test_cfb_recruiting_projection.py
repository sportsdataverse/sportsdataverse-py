"""Tests for the recruiting-composite -> team performance projection (T2.2 Phase 3)."""

from __future__ import annotations

import polars as pl

import importlib

_mod = importlib.import_module("sportsdataverse.cfb.cfb_recruiting_projection")
from sportsdataverse.cfb.cfb_recruiting_projection import (
    _build_projection_matrix,
    cfb_recruiting_projection,
)

_FEATURES = ["talent_composite", "blue_chip_ratio", "off_returning", "def_returning", "prior_wins"]


def _synth_talent() -> pl.DataFrame:
    rows = []
    for season in range(2018, 2024):
        for team, tal, bcr in (("A", 300.0, 0.5), ("B", 150.0, 0.1)):
            rows.append(
                {
                    "season": season,
                    "team_id": team,
                    "team": team.lower(),
                    "talent_composite": tal,
                    "talent_rank": 1 if team == "A" else 2,
                    "blue_chip_ratio": bcr,
                    "n_recruits": 100,
                }
            )
    return pl.DataFrame(rows)


def _synth_returning() -> pl.DataFrame:
    rows = []
    for season in range(2018, 2024):
        for team, off in (("A", 0.8), ("B", 0.4)):
            rows.append(
                {
                    "season": season,
                    "team_id": team,
                    "off_returning": off,
                    "def_returning": off / 2,
                    "overall_returning": off,
                    "n_returning": 20,
                }
            )
    return pl.DataFrame(rows)


def _synth_results() -> pl.DataFrame:
    # team A wins 10 every season vs B's 4; margins fixed
    rows = []
    for season in range(2017, 2023):  # realized seasons only (target 2023 unplayed)
        for team, wins, margin in (("A", 10, 15.0), ("B", 4, -8.0)):
            rows.append({"season": season, "team_id": team, "wins": wins, "points_margin": margin})
    return pl.DataFrame(rows)


def test_matrix_columns_lag_and_target_nulls(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "_load_talent", lambda seasons, division: _synth_talent())
    monkeypatch.setattr(_mod, "_load_returning", lambda seasons, division: _synth_returning())
    monkeypatch.setattr(_mod, "_load_results", lambda seasons: _synth_results())
    m = _build_projection_matrix(list(range(2018, 2024)), division="fbs")
    for col in _FEATURES + ["wins", "points_margin"]:
        assert col in m.columns, f"missing {col}"
    row = m.filter((pl.col("season") == 2019) & (pl.col("team_id") == "A")).row(0, named=True)
    assert row["prior_wins"] == 10  # 2018 realized wins
    target = m.filter(pl.col("season") == 2023)
    assert target.height == 2
    assert target["wins"].null_count() == 2  # unplayed target season
    assert m.schema["team_id"] == pl.Utf8


def test_projection_respects_as_of_boundary(monkeypatch) -> None:
    # wins is an exact linear function of talent: wins = talent/30 => A: 10, B: 5
    talent = _synth_talent()
    returning = _synth_returning()
    results = (
        talent.select("season", "team_id", (pl.col("talent_composite") / 30.0).alias("wins"))
        .filter(pl.col("season") < 2023)
        .with_columns(pl.col("wins").alias("points_margin"))
    )
    monkeypatch.setattr(_mod, "_load_talent", lambda seasons, division: talent)
    monkeypatch.setattr(_mod, "_load_returning", lambda seasons, division: returning)
    monkeypatch.setattr(_mod, "_load_results", lambda seasons: results)
    out = cfb_recruiting_projection(2023, alpha=1e-6)
    assert isinstance(out, pl.DataFrame)
    assert set(out["season"].unique().to_list()) == {2023}
    pred_a = out.filter(pl.col("team_id") == "A")["pred_wins"].item()
    assert abs(pred_a - 10.0) < 0.5, f"pred_wins for A = {pred_a}"
    # rows for 2023 must never enter training: nothing to assert directly here beyond
    # the fit succeeding with target-season targets null (they'd poison the fit as NaN)


def test_projection_empty_history_returns_schema(monkeypatch) -> None:
    empty = pl.DataFrame(
        schema={
            "season": pl.Int64,
            "team_id": pl.Utf8,
            "team": pl.Utf8,
            "talent_composite": pl.Float64,
            "talent_rank": pl.Int64,
            "blue_chip_ratio": pl.Float64,
            "n_recruits": pl.Int64,
        }
    )
    monkeypatch.setattr(_mod, "_load_talent", lambda seasons, division: empty)
    monkeypatch.setattr(_mod, "_load_returning", lambda seasons, division: _synth_returning().clear())
    monkeypatch.setattr(_mod, "_load_results", lambda seasons: _synth_results().clear())
    out = cfb_recruiting_projection(2023)
    assert out.height == 0
    for col in ("season", "team_id", "pred_wins", "pred_margin"):
        assert col in out.columns


def test_crosswalk_falls_back_to_newest_available_season(monkeypatch):
    """The crosswalk asset trails the calendar (capped at 2025 while a 2026
    projection is meaningful after early signing); a missing season comes back
    COLUMN-LESS from the loader and used to crash the select. Walk back to the
    newest crosswalk that exists instead."""
    calls: list[int] = []

    def fake_xw(season):
        calls.append(season)
        if season >= 2026:
            return pl.DataFrame()  # the loader's missing-season shape
        return pl.DataFrame({"norm_key": ["georgia bulldogs"], "espn_team_id": [61]})

    monkeypatch.setattr(_mod, "load_cfb_teams_crosswalk", fake_xw)

    xw = _mod._crosswalk_names_to_espn([2026])
    assert calls == [2026, 2025]
    assert xw["espn_id"].to_list() == ["61"]


def test_crosswalk_all_seasons_missing_returns_typed_empty(monkeypatch):
    monkeypatch.setattr(_mod, "load_cfb_teams_crosswalk", lambda season: pl.DataFrame())

    xw = _mod._crosswalk_names_to_espn([2026])
    assert xw.height == 0
    assert xw.columns == ["norm_key", "espn_id"]
