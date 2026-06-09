# tests/hockeytech/test_public_surface.py
from __future__ import annotations

import polars as pl

from tests.conftest import load_fixture


def test_pwhl_api_exports_full_parity_surface():
    import sportsdataverse.pwhl as pwhl

    expected = {
        "pwhl_schedule",
        "pwhl_scorebar",
        "pwhl_game_info",
        "pwhl_game_summary",
        "pwhl_pbp",
        "pwhl_player_box",
        "pwhl_teams",
        "pwhl_team_roster",
        "pwhl_standings",
        "pwhl_player_info",
        "pwhl_player_stats",
        "pwhl_player_game_log",
        "pwhl_player_search",
        "pwhl_stats",
        "pwhl_leaders",
        "pwhl_streaks",
        "pwhl_transactions",
        "pwhl_playoff_bracket",
        "pwhl_season_id",
        "most_recent_pwhl_season",
    }
    missing = expected - set(dir(pwhl))
    assert not missing, f"missing PWHL functions: {sorted(missing)}"


def test_pwhl_pbp_parses_via_monkeypatched_client(monkeypatch):
    import sportsdataverse.pwhl.pwhl_api as api

    monkeypatch.setattr(api, "hockeytech_api", lambda *a, **k: load_fixture("hockeytech", "pwhl_pbp_42"))
    df = api.pwhl_pbp(game_id=42)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert "game_id" in df.columns


def test_pwhl_season_id_via_monkeypatched_client(monkeypatch):
    import sportsdataverse.pwhl.pwhl_api as api

    monkeypatch.setattr(api, "hockeytech_api", lambda *a, **k: load_fixture("hockeytech", "pwhl_seasons"))
    df = api.pwhl_season_id()
    assert df.height > 0 and "season_yr" in df.columns


def test_pwhl_game_summary_dict_of_frames(monkeypatch):
    import sportsdataverse.pwhl.pwhl_api as api

    monkeypatch.setattr(api, "hockeytech_api", lambda *a, **k: load_fixture("hockeytech", "pwhl_game_summary_42"))
    out = api.pwhl_game_summary(game_id=42)
    assert isinstance(out, dict) and "goals" in out
