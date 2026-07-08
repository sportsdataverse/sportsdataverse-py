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
