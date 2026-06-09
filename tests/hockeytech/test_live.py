from __future__ import annotations

import polars as pl
import pytest

from tests.conftest import skip_unless_hockeytech


def test_pwhl_schedule_live_has_games():
    skip_unless_hockeytech()
    from sportsdataverse.pwhl import pwhl_schedule

    df = pwhl_schedule(season=2025)
    if df.height == 0:
        pytest.skip("no rows at test time")
    for col in ("game_id", "home_team", "away_team"):
        assert col in df.columns


def test_pwhl_pbp_and_corsi_live():
    skip_unless_hockeytech()
    from sportsdataverse.pwhl import pwhl_game_corsi, pwhl_pbp

    df = pwhl_pbp(game_id=42)
    if df.height == 0:
        pytest.skip("no pbp at test time")
    assert "on_ice_home" in df.columns
    corsi = pwhl_game_corsi(game_id=42)
    assert "corsi_for" in corsi.columns
    assert not corsi["corsi_includes_missed"].any()


@pytest.mark.parametrize("lg", ["ahl", "ohl", "whl", "qmjhl"])
def test_junior_schedule_live(lg):
    skip_unless_hockeytech()
    mod = __import__(f"sportsdataverse.{lg}", fromlist=["*"])
    df = getattr(mod, f"{lg}_schedule")()
    assert isinstance(df, pl.DataFrame)
