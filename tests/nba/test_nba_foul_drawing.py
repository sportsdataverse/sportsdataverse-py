"""Unit tests for model (3): foul-drawing / FT-generation expected-vs-actual model."""

import polars as pl

from sportsdataverse.nba.nba_foul_drawing import nba_foul_drawing


def _mix_and_base():
    mix = pl.DataFrame(
        {
            "player_id": [1, 1, 2, 2],
            "play_type": ["Isolation", "Drives", "Isolation", "Drives"],
            "off_poss": [100.0, 100.0, 100.0, 100.0],
            "ft_freq": [0.30, 0.30, 0.05, 0.05],  # A draws more given identical mix
        }
    )
    base = pl.DataFrame({"player_id": [1, 2], "fga": [200.0, 200.0], "fta": [60.0, 10.0], "poss": [200.0, 200.0]})
    return mix, base


def test_foul_draw_skill_sign_and_calibration():
    mix, base = _mix_and_base()
    out = nba_foul_drawing("2023-24", base=base, player_mix=mix)
    sa = out.filter(pl.col("player_id") == 1)["foul_draw_skill"][0]
    sb = out.filter(pl.col("player_id") == 2)["foul_draw_skill"][0]
    assert sa > 0 > sb
    assert abs(out["expected_fta"].sum() - out["fta"].sum()) < 1e-6
    assert out.schema["player_id"] == pl.Int64
    assert "pfd" in out.columns


def test_foul_drawing_pfd_from_advanced():
    mix, base = _mix_and_base()
    adv = pl.DataFrame({"player_id": [1, 2], "pfd": [55.0, 8.0]})
    out = nba_foul_drawing("2023-24", base=base, player_mix=mix, advanced=adv)
    assert out.filter(pl.col("player_id") == 1)["pfd"][0] == 55.0


def test_foul_drawing_empty_returns_schema():
    out = nba_foul_drawing("2023-24", base=pl.DataFrame(), player_mix=pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {"player_id", "poss", "fta", "expected_fta", "foul_draw_skill", "pfd"}
    assert out.schema["player_id"] == pl.Int64


def test_foul_drawing_pandas():
    mix, base = _mix_and_base()
    pdf = nba_foul_drawing("2023-24", base=base, player_mix=mix, return_as_pandas=True)
    assert type(pdf).__name__ == "DataFrame" and hasattr(pdf, "iloc")
