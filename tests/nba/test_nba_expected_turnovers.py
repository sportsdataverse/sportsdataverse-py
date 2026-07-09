"""Unit tests for model (4): expected-turnovers ball-security model."""

import polars as pl

from sportsdataverse.nba.nba_expected_turnovers import nba_expected_turnovers


def _mix_and_base():
    mix = pl.DataFrame(
        {
            "player_id": [1, 1, 2, 2],
            "play_type": ["Isolation", "Drives", "Isolation", "Drives"],
            "off_poss": [100.0, 100.0, 100.0, 100.0],
            "turnover_freq": [0.05, 0.05, 0.20, 0.20],  # A commits fewer TOs than mix predicts, B more
        }
    )
    base = pl.DataFrame({"player_id": [1, 2], "tov": [10.0, 40.0], "poss": [200.0, 200.0]})
    return mix, base


def test_ball_security_skill_sign_and_calibration():
    mix, base = _mix_and_base()
    out = nba_expected_turnovers("2023-24", base=base, player_mix=mix)
    sa = out.filter(pl.col("player_id") == 1)["ball_security_skill"][0]
    sb = out.filter(pl.col("player_id") == 2)["ball_security_skill"][0]
    assert sa > 0 > sb
    assert abs(out["expected_tov"].sum() - out["tov"].sum()) < 1e-6
    assert out.schema["player_id"] == pl.Int64


def test_expected_turnovers_empty_returns_schema():
    out = nba_expected_turnovers("2023-24", base=pl.DataFrame(), player_mix=pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {"player_id", "poss", "tov", "expected_tov", "ball_security_skill"}
    assert out.schema["player_id"] == pl.Int64


def test_expected_turnovers_pandas():
    mix, base = _mix_and_base()
    pdf = nba_expected_turnovers("2023-24", base=base, player_mix=mix, return_as_pandas=True)
    assert type(pdf).__name__ == "DataFrame" and hasattr(pdf, "iloc")
