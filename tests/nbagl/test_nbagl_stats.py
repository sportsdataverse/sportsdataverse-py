"""nbagl_stats: the G League (LeagueID=20) wrappers over stats.nba.com, offline via an injected transport."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

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
    assert isinstance(df, pl.DataFrame) and df.height > 0


def test_the_wrapper_dates_its_default_season_for_the_g_league(monkeypatch):
    """On 2025-12-15 the G League (January rollover) is still in 2024-25 while the NBA (November) is in 2025-26;
    the two calendars agree from January to October, so only a frozen December date tells them apart."""
    from sportsdataverse.nba import nba_stats_runtime as rt

    class _Dec15(date):
        @classmethod
        def today(cls):
            return cls(2025, 12, 15)

    monkeypatch.setattr(rt, "date", _Dec15)
    body = (_FIXTURES / "leaguedashplayerstats.json").read_text(encoding="utf-8")
    transport, calls = _recorder(body)
    nbagl_stats.nbagl_stats_leaguedashplayerstats(transport=transport)
    assert calls[0][1]["Season"] == "2024-25"


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


@pytest.mark.parametrize(
    "slug,key,expected",
    [
        ("commonplayerinfo", "PlayerID", "1642443"),
        ("teaminfocommon", "TeamID", "1612709914"),
        ("playervsplayer", "VsPlayerID", "1631457"),
        # cumestatsteam's default game is OSC vs CPS, so its default team is OSC (they must agree)
        ("cumestatsteam", "TeamID", "1612709925"),
        ("cumestatsteam", "GameIDs", "2022500137"),
        # hoopR's 2023-03-26 is a G League off week; the sweep's own capture date has games
        ("scoreboardv3", "GameDate", "2025-01-15"),
    ],
)
def test_every_swapped_default_is_a_g_league_value(slug, key, expected):
    """hoopR's defaults name NBA games, players and teams; a G League wrapper must never send one."""
    body = (_FIXTURES / f"{slug}.json").read_text(encoding="utf-8")
    transport, calls = _recorder(body)
    getattr(nbagl_stats, f"nbagl_stats_{slug}")(transport=transport)
    assert calls[0][1][key] == expected


def test_a_param_with_no_g_league_default_is_required():
    """shotchartlineupdetail's GROUP_ID is a lineup: hoopR's is an NBA one and there is no G League one, so a call
    without it raises instead of sending no lineup and getting empty frames back."""
    with pytest.raises(TypeError, match="group_id"):
        nbagl_stats.nbagl_stats_shotchartlineupdetail()


@skip_if_no_nba_stats_live
def test_live_leaguedashplayerstats():
    df = nbagl_stats.nbagl_stats_leaguedashplayerstats(season="2024-25")
    assert df.height > 0 and "player_id" in df.columns
