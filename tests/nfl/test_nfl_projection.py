"""Unit tests for the Marcel player projection engine (Phase 1/2)."""

import polars as pl

from sportsdataverse.nfl.nfl_projection import season_player_rates


def _mini_weekly():
    return pl.DataFrame(
        {
            "player_id": ["P1", "P1"],
            "season": [2023, 2023],
            "week": [1, 2],
            "position_group": ["WR", "WR"],
            "recent_team": ["A", "A"],
            "targets": [10.0, 6.0],
            "receptions": [7.0, 4.0],
            "receiving_yards": [100.0, 40.0],
            "receiving_tds": [1.0, 0.0],
            "fantasy_points_ppr": [24.0, 12.0],
        }
    )


def _mini_rosters():
    return pl.DataFrame({"player_id": ["P1"], "season": [2023], "position": ["WR"], "age": [25.0]})


def test_season_rates_aggregate():
    out = season_player_rates(_mini_weekly(), _mini_rosters())
    r = out.filter(pl.col("player_id") == "P1").row(0, named=True)
    assert r["games"] == 2
    assert abs(r["ppg"] - 18.0) < 1e-9  # (24+12)/2
    assert abs(r["volume"] - 16.0) < 1e-9  # WR volume = total targets
    assert abs(r["receiving_yards_rate"] - 70.0) < 1e-9  # 140/2
    assert abs(r["age"] - 25.0) < 1e-9


def test_aging_curve_peaks_at_one():
    from sportsdataverse.nfl.nfl_projection import aging_curve

    rows = []
    # two players, rate ~ -(age-27)^2 shape, high volume so weights equal
    for pid in ["A", "B"]:
        for age, season in zip([24, 25, 26, 27, 28], [2019, 2020, 2021, 2022, 2023]):
            rate = 100.0 - (age - 27) ** 2
            rows.append(
                {
                    "player_id": pid,
                    "season": season,
                    "position_group": "RB",
                    "age": float(age),
                    "volume": 200.0,
                    "ppg": float(rate),
                }
            )
    df = pl.DataFrame(rows)
    curve = aging_curve(df, position_group="RB")
    assert abs(curve["aging_mult"].max() - 1.0) < 1e-9
    peak_age = curve.filter(pl.col("aging_mult") == curve["aging_mult"].max())["age"][0]
    assert peak_age == 27.0


def test_aging_curve_empty_returns_schema():
    from sportsdataverse.nfl.nfl_projection import aging_curve

    out = aging_curve(
        pl.DataFrame(
            schema={
                "player_id": pl.Utf8,
                "season": pl.Int64,
                "position_group": pl.Utf8,
                "age": pl.Float64,
                "volume": pl.Float64,
                "ppg": pl.Float64,
            }
        ),
        position_group="RB",
    )
    assert out.height == 0
    assert out.columns == ["age", "aging_mult"]


def test_season_rates_empty_input_keeps_schema():
    out = season_player_rates(_mini_weekly().head(0), _mini_rosters().head(0))
    assert out.height == 0
    for col in ["player_id", "season", "position_group", "age", "games", "volume", "ppg"]:
        assert col in out.columns
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["season"] == pl.Int64


def _synth_history(with_target_season_row=False):
    """3 WRs x seasons 2021-2023, constant per-player rates (flat aging curve)."""
    rows = []
    ppg = {"P1": 20.0, "P2": 10.0, "P3": 10.0}
    for pid, base in ppg.items():
        for i, season in enumerate([2021, 2022, 2023]):
            rows.append(
                {
                    "player_id": pid,
                    "season": season,
                    "week": 1,
                    "position_group": "WR",
                    "recent_team": "A",
                    "targets": 50.0,
                    "receptions": 5.0,
                    "receiving_yards": base * 10,
                    "receiving_tds": 0.0,
                    "fantasy_points_ppr": base,
                }
            )
    if with_target_season_row:
        rows.append(
            {
                "player_id": "P1",
                "season": 2024,
                "week": 1,
                "position_group": "WR",
                "recent_team": "A",
                "targets": 50.0,
                "receptions": 5.0,
                "receiving_yards": 9999.0,
                "receiving_tds": 99.0,
                "fantasy_points_ppr": 999.0,
            }
        )
    return pl.DataFrame(rows)


def _synth_rosters():
    rows = []
    for pid, birth in [("P1", 1997), ("P2", 1996), ("P3", 1995)]:
        for season in [2021, 2022, 2023, 2024]:
            rows.append({"player_id": pid, "season": season, "position": "WR", "age": float(season - birth)})
    return pl.DataFrame(rows)


def _patch_loaders(monkeypatch, weekly):
    import sportsdataverse.nfl.nfl_projection as mod

    monkeypatch.setattr(mod, "load_nfl_player_stats", lambda *a, **k: weekly)
    monkeypatch.setattr(mod, "load_nfl_rosters", lambda *a, **k: _synth_rosters())


def test_marcel_projection_regresses_toward_position_mean(monkeypatch):
    from sportsdataverse.nfl.nfl_projection import nfl_player_projection

    _patch_loaders(monkeypatch, _synth_history())
    out = nfl_player_projection([2021, 2022, 2023], 2024)
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["target_season"] == pl.Int64
    r = out.filter(pl.col("player_id") == "P1").row(0, named=True)
    # P1 raw weighted mean ppg = 20; volume-weighted position mean = 40/3.
    pos_mean = 40.0 / 3.0
    assert pos_mean < r["proj_ppg"] < 20.0  # regression happened
    assert r["reliability"] > 0
    assert r["target_season"] == 2024


def test_marcel_projection_pandas_flag(monkeypatch):
    import pandas as pd

    from sportsdataverse.nfl.nfl_projection import nfl_player_projection

    _patch_loaders(monkeypatch, _synth_history())
    out = nfl_player_projection([2021, 2022, 2023], 2024, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


def test_score_fantasy_ppr_hand_computable():
    from sportsdataverse.nfl.nfl_projection import score_fantasy
    from sportsdataverse.nfl.nfl_projection_constants import SCORING_PPR

    stats = pl.DataFrame({"receiving_yards": [100.0], "receptions": [5.0]})
    s = score_fantasy(stats, SCORING_PPR)
    assert abs(s[0] - 15.0) < 1e-9  # 100*0.1 + 5*1.0


def test_fantasy_projection_scores_and_ranks(monkeypatch):
    from sportsdataverse.nfl.nfl_projection import nfl_fantasy_projection

    _patch_loaders(monkeypatch, _synth_history())
    out = nfl_fantasy_projection([2021, 2022, 2023], 2024, calibrate=False)
    assert {
        "player_id",
        "target_season",
        "position_group",
        "proj_fantasy_points",
        "proj_fantasy_points_per_game",
        "position_rank",
    } <= set(out.columns)
    wrs = out.filter(pl.col("position_group") == "WR").sort("position_rank")
    # P1 (higher projected production) ranks 1 within WR
    assert wrs.row(0, named=True)["player_id"] == "P1"
    assert wrs.row(0, named=True)["position_rank"] == 1
    r = wrs.row(0, named=True)
    assert abs(r["proj_fantasy_points"] - r["proj_fantasy_points_per_game"] * 1.0) < 1e-9  # proj_games == 1


def test_fantasy_projection_calibration_applied(monkeypatch):
    import dataclasses

    from sportsdataverse.nfl import nfl_projection_constants as cm
    from sportsdataverse.nfl.nfl_projection import nfl_fantasy_projection

    _patch_loaders(monkeypatch, _synth_history())
    old = cm.POSITION_CONSTANTS["WR"]
    try:
        cm.POSITION_CONSTANTS["WR"] = dataclasses.replace(old, fp_calibration=(2.0, 0.5))
        raw = nfl_fantasy_projection([2021, 2022, 2023], 2024, calibrate=False)
        cal = nfl_fantasy_projection([2021, 2022, 2023], 2024, calibrate=True)
        r = raw.filter(pl.col("player_id") == "P1")["proj_fantasy_points"][0]
        c = cal.filter(pl.col("player_id") == "P1")["proj_fantasy_points"][0]
        assert abs(c - (2.0 + 0.5 * r)) < 1e-9
        # identity calibration leaves the projection unchanged
        cm.POSITION_CONSTANTS["WR"] = dataclasses.replace(old, fp_calibration=(0.0, 1.0))
        ident = nfl_fantasy_projection([2021, 2022, 2023], 2024, calibrate=True)
        assert abs(ident.filter(pl.col("player_id") == "P1")["proj_fantasy_points"][0] - r) < 1e-9
    finally:
        cm.POSITION_CONSTANTS["WR"] = old


def test_marcel_projection_leakage_boundary(monkeypatch):
    """A season == target_season row in the input must NOT influence the projection."""
    from sportsdataverse.nfl.nfl_projection import nfl_player_projection

    _patch_loaders(monkeypatch, _synth_history(with_target_season_row=False))
    clean = nfl_player_projection([2021, 2022, 2023, 2024], 2024)
    _patch_loaders(monkeypatch, _synth_history(with_target_season_row=True))
    poisoned = nfl_player_projection([2021, 2022, 2023, 2024], 2024)
    a = clean.filter(pl.col("player_id") == "P1")["proj_ppg"][0]
    b = poisoned.filter(pl.col("player_id") == "P1")["proj_ppg"][0]
    assert abs(a - b) < 1e-12
