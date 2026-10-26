"""nbagl_stats: the G League (LeagueID=20) wrappers over stats.nba.com, offline via an injected transport."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_stats_runtime import _latest_season
from sportsdataverse.nbagl import nbagl_stats
from tests.conftest import skip_if_no_nba_stats_live

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nbagl_stats" / "endpoints"


def _recorder(body: str):
    calls: list = []

    def transport(url, params, headers, proxy_url):
        calls.append((url, dict(params)))
        return 200, body

    return transport, calls


def test_module_surface():
    assert len(nbagl_stats.__all__) == 94  # the catalog's live, non-deprecated LeagueID=20 endpoints
    assert all(n.startswith("nbagl_stats_") for n in nbagl_stats.__all__)
    # live for the G League only (the NBA side dropped these v2 box scores)
    assert hasattr(nbagl_stats, "nbagl_stats_boxscoreadvancedv2")


def test_league_id_20_is_sent_by_default_and_parsed():
    body = (_FIXTURES / "leaguedashplayerstats.json").read_text(encoding="utf-8")
    transport, calls = _recorder(body)
    df = nbagl_stats.nbagl_stats_leaguedashplayerstats(transport=transport)
    url, params = calls[0]
    assert url == "https://stats.nba.com/stats/leaguedashplayerstats"
    assert params["LeagueID"] == "20"
    # the default season is dated for the G League (January rollover), not the NBA's November one
    assert params["Season"] == _latest_season("20", "leaguedashplayerstats", season_type=params["SeasonType"])
    assert isinstance(df, pl.DataFrame) and df.height > 0


def test_season_default_follows_the_g_league_calendar():
    # December 2025: the 2025-26 G League regular season has no rows yet; the NBA's has
    assert _latest_season("20", today=date(2025, 12, 15)) == "2024-25"
    assert _latest_season("00", today=date(2025, 12, 15)) == "2025-26"


def test_entity_defaults_are_g_league_ids():
    body = (_FIXTURES / "boxscoretraditionalv2.json").read_text(encoding="utf-8")
    transport, calls = _recorder(body)
    nbagl_stats.nbagl_stats_boxscoretraditionalv2(transport=transport)
    game_id = calls[0][1]["GameID"]
    assert game_id == "2022500137"
    assert game_id == json.loads(body)["parameters"]["GameID"]  # the game the committed capture was taken with


@skip_if_no_nba_stats_live
def test_live_leaguedashplayerstats():
    df = nbagl_stats.nbagl_stats_leaguedashplayerstats(season="2024-25")
    assert df.height > 0 and "player_id" in df.columns
