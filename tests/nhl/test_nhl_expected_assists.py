from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_expected_assists import extract_goals_with_assists, nhl_expected_assists


def _goal(scorer: str, a1: str | None, a2: str | None, x: float = 80.0, y: float = 0.0) -> dict:
    return {
        "game_id": "G1",
        "season": 2024,
        "event_idx": 0,
        "period": 1,
        "time_in_period": "5:00",
        "type_desc_key": "goal",
        "event_owner_team_id": "10",
        "zone_code": "O",
        "x_coord": x,
        "y_coord": y,
        "situation_code": "1551",
        "home_team_id": "10",
        "home_team_defending_side": "left",
        "winning_player_id": None,
        "losing_player_id": None,
        "scoring_player_id": scorer,
        "assist1_player_id": a1,
        "assist2_player_id": a2,
        "shooting_player_id": scorer,
        "committed_player_id": None,
        "drawn_player_id": None,
        "penalty_type_code": None,
        "shot_type": "wrist",
    }


class _StubXG:
    """Constant xG -> every goal's relative danger normalizes to 1.0."""

    def predict(self, df: pl.DataFrame) -> pl.Series:
        return pl.Series("xg", [0.5] * df.height)


class _VaryingXG:
    """xG proportional to x_coord so relative-danger weighting is exercised."""

    def predict(self, df: pl.DataFrame) -> pl.Series:
        return (df["x_coord"] / 100.0).alias("xg")


def test_extract_goals_with_assists() -> None:
    pbp = pl.DataFrame([_goal("S1", "A", "B"), {**_goal("S2", None, None), "type_desc_key": "shot-on-goal"}])
    out = extract_goals_with_assists(pbp, xg_model=_StubXG())
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["scoring_player_id"] == "S1"
    assert row["assist1_player_id"] == "A"
    assert abs(row["goal_xg"] - 0.5) < 1e-9


def test_expected_assists_credit_and_share() -> None:
    goals = [_goal("S1", "A", "B"), _goal("S2", "A", "C")]  # A primary on both
    pbp = pl.DataFrame(goals)
    out = nhl_expected_assists(pbp, xg_model=_StubXG())
    a = out.filter(pl.col("player_id") == "A").row(0, named=True)
    assert a["primary_assists"] == 2
    # constant xG -> relative danger 1.0 per goal -> x_primary == count.
    assert abs(a["x_primary_assists"] - 2.0) < 1e-9
    assert abs(a["primary_share"] - 1.0) < 1e-9
    b = out.filter(pl.col("player_id") == "B").row(0, named=True)
    assert b["secondary_assists"] == 1
    assert abs(b["x_secondary_assists"] - 1.0) < 1e-9
    assert abs(b["primary_share"] - 0.0) < 1e-9


def test_relative_danger_weighting() -> None:
    # Two goals: one high-danger (x=90 -> xg 0.9), one low (x=50 -> xg 0.5).
    # mean xg 0.7. A primary on the high-danger goal -> relative danger
    # 0.9/0.7 ~= 1.286; C primary on the low -> 0.5/0.7 ~= 0.714.
    pbp = pl.DataFrame([_goal("S1", "A", None, x=90.0), _goal("S2", "C", None, x=50.0)])
    out = nhl_expected_assists(pbp, xg_model=_VaryingXG())
    a = out.filter(pl.col("player_id") == "A").row(0, named=True)
    c = out.filter(pl.col("player_id") == "C").row(0, named=True)
    assert a["x_primary_assists"] > 1.0 > c["x_primary_assists"]
    # unbiasedness holds exactly on this toy set: total x == total actual (2).
    assert abs(out["x_primary_assists"].sum() + out["x_secondary_assists"].sum() - 2.0) < 1e-9


def test_assists_above_expected() -> None:
    # 1 goal, A primary, constant xG -> relative danger 1.0 -> x = 1 -> aae 0.
    pbp = pl.DataFrame([_goal("S1", "A", None)])
    a = nhl_expected_assists(pbp, xg_model=_StubXG()).filter(pl.col("player_id") == "A").row(0, named=True)
    assert abs(a["assists_above_expected"] - 0.0) < 1e-9


def test_empty_pbp_returns_schema() -> None:
    empty = nhl_expected_assists(pl.DataFrame(schema={"type_desc_key": pl.Utf8}), xg_model=_StubXG())
    assert empty.height == 0
    assert "x_primary_assists" in empty.columns
