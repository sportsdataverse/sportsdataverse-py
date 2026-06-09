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


def test_pwhl_pbp_enriched_full_parity(monkeypatch):
    import sportsdataverse.pwhl.pwhl_api as api

    def fake_api(league, feed, view, params=None, **k):
        if view == "gameCenterPlayByPlay":
            return load_fixture("hockeytech", "pwhl_pbp_42")
        if view == "gameshifts":
            return load_fixture("hockeytech", "pwhl_gameshifts_42")
        if feed == "gc":
            return load_fixture("hockeytech", "pwhl_game_summary_42")
        return None

    monkeypatch.setattr(api, "hockeytech_api", fake_api)
    df = api.pwhl_pbp(game_id=42)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    required_cols = (
        "x_coord_original",
        "x_coord_fixed",
        "clock",
        "sec_from_start",
        "shot_distance",
        "scoring_chance",
        "on_ice_home",
        "on_ice_away",
        "home_team_id",
        "away_team_id",
        "game_id",
        "game_date",
        "game_season",
        "game_season_id",
        "home_team",
        "away_team",
    )
    for c in required_cols:
        assert c in df.columns, f"missing column: {c}"

    # On-ice populated on shots — multiple players, home != away
    shots = df.filter(pl.col("event") == "shot")
    s = shots.filter(pl.col("on_ice_home").is_not_null()).head(1)
    assert s.height == 1, "no shots have on_ice_home populated"
    assert "," in s["on_ice_home"][0], "on_ice_home should be comma-joined player ids"
    assert s["on_ice_home"][0] != s["on_ice_away"][0], "on_ice_home and on_ice_away should differ"
