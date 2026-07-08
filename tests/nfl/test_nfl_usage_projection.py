"""Unit + oracle tests for the target-share / air-yards / WOPR projection (Phase 3)."""

import polars as pl

from sportsdataverse.nfl.nfl_usage_projection import season_usage_shares


def _mini_weekly():
    # one team-season, two players: 60/40 target split, 200/100 air-yards split
    rows = []
    for week, (t1, t2, a1, a2) in enumerate([(30.0, 20.0, 100.0, 50.0), (30.0, 20.0, 100.0, 50.0)], start=1):
        rows.append(
            {
                "player_id": "P1",
                "season": 2023,
                "week": week,
                "position_group": "WR",
                "recent_team": "A",
                "targets": t1,
                "receiving_air_yards": a1,
            }
        )
        rows.append(
            {
                "player_id": "P2",
                "season": 2023,
                "week": week,
                "position_group": "WR",
                "recent_team": "A",
                "targets": t2,
                "receiving_air_yards": a2,
            }
        )
    return pl.DataFrame(rows)


def test_season_usage_shares_hand_computable():
    out = season_usage_shares(_mini_weekly())
    p1 = out.filter(pl.col("player_id") == "P1").row(0, named=True)
    assert abs(p1["target_share"] - 0.6) < 1e-9
    assert abs(p1["air_yards_share"] - 200.0 / 300.0) < 1e-9
    assert abs(p1["wopr"] - (1.5 * 0.6 + 0.7 * 200.0 / 300.0)) < 1e-9
    # shares within a team-season sum to 1
    sums = out.group_by("team", "season").agg(pl.col("target_share").sum(), pl.col("air_yards_share").sum())
    assert abs(sums["target_share"][0] - 1.0) < 1e-9
    assert abs(sums["air_yards_share"][0] - 1.0) < 1e-9


def _three_wr_weekly():
    rows = []
    targets = {"P1": 30.0, "P2": 20.0, "P3": 10.0}
    for season in [2021, 2022, 2023]:
        for pid, t in targets.items():
            rows.append(
                {
                    "player_id": pid,
                    "season": season,
                    "week": 1,
                    "position_group": "WR",
                    "recent_team": "A",
                    "targets": t,
                    "receiving_air_yards": t * 10.0,
                }
            )
    return pl.DataFrame(rows)


def test_usage_projection_renormalizes_within_team(monkeypatch):
    import sportsdataverse.nfl.nfl_usage_projection as mod

    weekly = _three_wr_weekly()
    assert weekly.schema["player_id"] == pl.Utf8  # join-key dtype guard
    monkeypatch.setattr(mod, "load_nfl_player_stats", lambda *a, **k: weekly)
    out = mod.nfl_usage_projection([2021, 2022, 2023], 2024)
    sums = out.group_by("proj_team").agg(pl.col("proj_target_share").sum(), pl.col("proj_air_yards_share").sum())
    assert abs(sums["proj_target_share"][0] - 1.0) < 1e-9
    assert abs(sums["proj_air_yards_share"][0] - 1.0) < 1e-9
    r = out.filter(pl.col("player_id") == "P1").row(0, named=True)
    assert abs(r["proj_wopr"] - (1.5 * r["proj_target_share"] + 0.7 * r["proj_air_yards_share"])) < 1e-9
    # proj_targets = proj share x team targets carry-forward (60 in 2023)
    assert abs(r["proj_targets"] - r["proj_target_share"] * 60.0) < 1e-9


def test_usage_projection_leakage(monkeypatch):
    import sportsdataverse.nfl.nfl_usage_projection as mod

    poisoned = pl.concat(
        [
            _three_wr_weekly(),
            pl.DataFrame(
                [
                    {
                        "player_id": "P3",
                        "season": 2024,
                        "week": 1,
                        "position_group": "WR",
                        "recent_team": "A",
                        "targets": 999.0,
                        "receiving_air_yards": 9999.0,
                    }
                ]
            ),
        ]
    )
    monkeypatch.setattr(mod, "load_nfl_player_stats", lambda *a, **k: _three_wr_weekly())
    clean = mod.nfl_usage_projection([2021, 2022, 2023, 2024], 2024)
    monkeypatch.setattr(mod, "load_nfl_player_stats", lambda *a, **k: poisoned)
    dirty = mod.nfl_usage_projection([2021, 2022, 2023, 2024], 2024)
    a = clean.filter(pl.col("player_id") == "P3")["proj_target_share"][0]
    b = dirty.filter(pl.col("player_id") == "P3")["proj_target_share"][0]
    assert abs(a - b) < 1e-12


def test_oracle_usage_shares_vs_realized_2024():
    """Usage-share oracle vs realized 2024 (offline fixtures).

    Floors from observed values 2026-07-08 (rounded down):
    RB spearman 0.7092, WR 0.6258, TE 0.7836; target-share MAE
    RB 0.0340, WR 0.0710, TE 0.0527. Team share sums are exactly 1.0.
    """
    from pathlib import Path

    import sportsdataverse.nfl.nfl_usage_projection as mod
    from sportsdataverse.nfl.nfl_projection_constants import mae, spearman_corr

    fix = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_projection"
    weekly = pl.read_parquet(fix / "player_stats_2020_2023.parquet")
    realized_w = pl.read_parquet(fix / "realized_2024.parquet")
    orig = mod.load_nfl_player_stats
    mod.load_nfl_player_stats = lambda *a, **k: weekly
    try:
        usage = mod.nfl_usage_projection([2020, 2021, 2022, 2023], 2024)
    finally:
        mod.load_nfl_player_stats = orig
    real = season_usage_shares(realized_w).filter(pl.col("games") >= 8)
    assert usage.schema["player_id"] == real.schema["player_id"]
    j = usage.join(real.select("player_id", pl.col("target_share").alias("real_ts")), on="player_id", how="inner")
    floors = {"RB": 0.70, "WR": 0.61, "TE": 0.77}
    mae_floors = {"RB": 0.04, "WR": 0.08, "TE": 0.06}
    for pos, floor in floors.items():
        s = j.filter(pl.col("position_group") == pos)
        assert s.height >= 30
        sp = spearman_corr(s["proj_target_share"].to_numpy(), s["real_ts"].to_numpy())
        m = mae(s["proj_target_share"].to_numpy(), s["real_ts"].to_numpy())
        assert sp >= floor, f"{pos}: share spearman {sp:.4f} < {floor}"
        assert m <= mae_floors[pos], f"{pos}: share MAE {m:.4f} > {mae_floors[pos]}"
    sums = usage.group_by("proj_team").agg(pl.col("proj_target_share").sum(), pl.col("proj_air_yards_share").sum())
    assert (sums["proj_target_share"] - 1.0).abs().max() < 1e-6
    assert (sums["proj_air_yards_share"] - 1.0).abs().max() < 1e-6


def test_season_usage_shares_empty_schema():
    out = season_usage_shares(_mini_weekly().head(0))
    assert out.height == 0
    assert out.schema["player_id"] == pl.Utf8
    for c in ["targets", "air_yards", "target_share", "air_yards_share", "wopr"]:
        assert out.schema[c] == pl.Float64
