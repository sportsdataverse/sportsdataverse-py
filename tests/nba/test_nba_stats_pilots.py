from pathlib import Path

import polars as pl

from sportsdataverse.nba import nba_stats
from tests.conftest import skip_if_no_live

FIX = Path(__file__).parent / "fixtures"


def _transport_for(fixture):
    text = (FIX / fixture).read_text(encoding="utf-8")
    return lambda url, params, headers, proxy_url: (200, text)


def test_pilot_leaguedashplayerstats_nba():
    df = nba_stats.nba_stats_leaguedashplayerstats(
        league_id="00", transport=_transport_for("cap_leaguedashplayerstats_nba.json")
    )
    assert isinstance(df, pl.DataFrame) and df.height > 0 and "player_id" in df.columns


def test_pilot_gleague_routes_via_league_id():
    df = nba_stats.nba_stats_leaguedashplayerstats(
        league_id="20", transport=_transport_for("cap_leaguedashplayerstats_gleague.json")
    )
    assert isinstance(df, pl.DataFrame) and df.height > 0


def test_pilot_summer_routes_via_league_id():
    df = nba_stats.nba_stats_leaguedashplayerstats(
        league_id="15", transport=_transport_for("cap_leaguedashplayerstats_summer.json")
    )
    assert isinstance(df, pl.DataFrame) and df.height > 0


def test_pilot_playercareerstats_multi_set():
    out = nba_stats.nba_stats_playercareerstats(
        player_id="2544", transport=_transport_for("cap_playercareerstats_nba.json")
    )
    assert isinstance(out, dict) and "SeasonTotalsRegularSeason" in out


def test_pilot_nba_url_routes_to_stats_nba_com():
    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        return 200, (FIX / "cap_leaguedashplayerstats_nba.json").read_text(encoding="utf-8")

    nba_stats.nba_stats_leaguedashplayerstats(league_id="00", transport=transport)
    assert captured["url"] == "https://stats.nba.com/stats/leaguedashplayerstats"


@skip_if_no_live
def test_live_leaguedashplayerstats_nba():
    df = nba_stats.nba_stats_leaguedashplayerstats(league_id="00")
    assert isinstance(df, pl.DataFrame) and df.height > 0


@skip_if_no_live
def test_live_leaguedashplayerstats_gleague():
    df = nba_stats.nba_stats_leaguedashplayerstats(league_id="20")
    assert isinstance(df, pl.DataFrame)


@skip_if_no_live
def test_live_leaguedashplayerstats_summer():
    df = nba_stats.nba_stats_leaguedashplayerstats(league_id="15")
    assert isinstance(df, pl.DataFrame)
