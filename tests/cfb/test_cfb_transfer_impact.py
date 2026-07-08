"""Tests for transfer-portal move extraction + win-delta impact (T2.2 Phase 4)."""

from __future__ import annotations

import polars as pl

import importlib

_mod = importlib.import_module("sportsdataverse.cfb.cfb_transfer_impact")
from sportsdataverse.cfb.cfb_transfer_impact import cfb_transfer_impact, cfb_transfer_moves


def _rosters(season: int, rows: list[tuple[str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [season] * len(rows),
            "team_id": [t for t, _ in rows],
            "player_id": [p for _, p in rows],
            "player_name": [f"Name {p}" for _, p in rows],
        }
    )


def _synth_rosters(*a, **k) -> pl.DataFrame:
    # 2022: p1+p2 on A, p3 on B; 2023: p1 moved A->B, p2 stayed, p3 stayed
    return pl.concat(
        [
            _rosters(2022, [("A", "p1"), ("A", "p2"), ("B", "p3")]),
            _rosters(2023, [("B", "p1"), ("A", "p2"), ("B", "p3")]),
        ]
    )


def test_transfer_moves_paired_in_out(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "_load_roster_keys", _synth_rosters)
    monkeypatch.setattr(
        _mod,
        "_talent_points_lookup",
        lambda seasons, division: pl.DataFrame(schema={"_name": pl.Utf8, "talent_points": pl.Float64}),
    )
    moves = cfb_transfer_moves(2023)
    assert isinstance(moves, pl.DataFrame)
    inc = moves.filter(pl.col("direction") == "in")
    out = moves.filter(pl.col("direction") == "out")
    assert inc.height == 1 and out.height == 1
    inc_row = inc.row(0, named=True)
    assert inc_row["team_id"] == "B" and inc_row["prior_team_id"] == "A"
    assert inc_row["player_id"] == "p1" and inc_row["season"] == 2023
    out_row = out.row(0, named=True)
    assert out_row["team_id"] == "A" and out_row["prior_team_id"] == "A"
    # unrated mover gets the 0-star default points
    assert inc_row["talent_points"] > 0


def test_transfer_impact_direction_and_boundary(monkeypatch) -> None:
    # synthetic history: net transfer talent linearly drives win delta (slope 0.05/pt)
    hist = []
    for season in range(2019, 2023):
        for team, net in (("A", 100.0), ("B", -100.0), ("C", 0.0)):
            hist.append({"season": season, "team_id": team, "net_transfer_talent": net})
    net_frame = pl.DataFrame(hist)
    deltas = net_frame.select("season", "team_id", (pl.col("net_transfer_talent") * 0.05).alias("win_delta"))
    target = pl.DataFrame(
        [
            {"season": 2023, "team_id": "A", "net_transfer_talent": 100.0},
            {"season": 2023, "team_id": "B", "net_transfer_talent": -100.0},
        ]
    )
    monkeypatch.setattr(_mod, "_net_transfer_talent", lambda seasons, division: pl.concat([net_frame, target]))
    monkeypatch.setattr(_mod, "_realized_win_deltas", lambda seasons: deltas)
    out = cfb_transfer_impact(2023)
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert abs(a["pred_win_delta"] - 5.0) < 0.25
    assert abs(b["pred_win_delta"] + 5.0) < 0.25
    assert a["net_transfer_talent"] == 100.0


def test_transfer_impact_empty_returns_schema(monkeypatch) -> None:
    empty_net = pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "net_transfer_talent": pl.Float64})
    monkeypatch.setattr(_mod, "_net_transfer_talent", lambda seasons, division: empty_net)
    monkeypatch.setattr(
        _mod,
        "_realized_win_deltas",
        lambda seasons: pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "win_delta": pl.Float64}),
    )
    out = cfb_transfer_impact(2023)
    assert out.height == 0
    for col in ("season", "team_id", "net_transfer_talent", "pred_win_delta"):
        assert col in out.columns
