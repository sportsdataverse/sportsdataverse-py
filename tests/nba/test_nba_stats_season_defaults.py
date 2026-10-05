"""Season + entity defaults of the generated nba_stats / wnba_stats wrappers.

stats.nba.com answers most endpoints with an empty HTTP 500 when ``Season`` is missing, and the
runtime turns that into ``{}``. The wrappers used to default season to ``None`` (the hoopR/wehoop
call default was lost in the catalog), and NBA wrappers inherited WNBA entity ids. These tests pin
the regenerated defaults offline; the live sweep re-measures them against the API.
"""

import pytest

from sportsdataverse.nba import nba_stats
from sportsdataverse.nba.nba_stats_runtime import season_or_current as nba_season
from sportsdataverse.wnba import wnba_stats
from sportsdataverse.wnba.wnba_stats_runtime import season_or_current as wnba_season
from tests.conftest import skip_if_no_nba_stats_live


def _sent(fn, **kwargs):
    seen = {}

    def transport(url, params, headers, proxy_url):
        seen.update(params)
        return 200, '{"resultSets": []}'

    fn(return_parsed=False, transport=transport, **kwargs)
    return seen


@pytest.fixture
def frozen_seasons(monkeypatch):
    monkeypatch.setattr("sportsdataverse.nba.nba_schedule.most_recent_nba_season", lambda: 2027)
    monkeypatch.setattr("sportsdataverse.wnba.wnba_schedule.most_recent_wnba_season", lambda: 2026)


def test_season_or_current_follows_each_league(frozen_seasons):
    assert nba_season(None) == "2026-27"  # hoopR: year_to_season(most_recent_nba_season() - 1)
    assert wnba_season(None) == "2026"  # the current WNBA season, not wehoop's "- 1"
    assert nba_season("2023-24") == "2023-24" and wnba_season("") == ""


def test_season_required_wrappers_send_the_current_season(frozen_seasons):
    assert _sent(nba_stats.nba_stats_playergamelogs)["Season"] == "2026-27"
    assert _sent(nba_stats.nba_stats_commonteamroster)["Season"] == "2026-27"
    assert _sent(nba_stats.nba_stats_draftcombinestats)["SeasonYear"] == "2026-27"
    assert _sent(wnba_stats.wnba_stats_playergamelogs)["Season"] == "2026"
    assert _sent(wnba_stats.wnba_stats_playerdashptshotdefend)["Season"] == "2026"


def test_last_finished_season_where_hoopr_asks_for_it(frozen_seasons):
    # hoopR: year_to_season(most_recent_nba_season() - 2), the last finished playoffs
    assert _sent(nba_stats.nba_stats_commonplayoffseries)["Season"] == "2025-26"
    assert _sent(wnba_stats.wnba_stats_commonplayoffseries)["Season"] == "2025"


def test_all_season_endpoints_keep_the_api_default():
    # Without a Season these return every season (the full draft history, every matching game),
    # which a current-season default would silently narrow.
    assert "Season" not in _sent(nba_stats.nba_stats_drafthistory)
    assert "Season" not in _sent(nba_stats.nba_stats_leaguegamefinder)
    assert "Season" not in _sent(wnba_stats.wnba_stats_drafthistory)


def test_explicit_season_wins(frozen_seasons):
    assert _sent(nba_stats.nba_stats_playergamelogs, season_nullable="2023-24")["Season"] == "2023-24"


def test_entity_defaults_belong_to_the_wrappers_league():
    assert _sent(nba_stats.nba_stats_teaminfocommon)["TeamID"] == "1610612749"  # was a WNBA team -> 500
    assert _sent(nba_stats.nba_stats_boxscoretraditionalv3)["GameID"].startswith("002")  # NBA game
    assert _sent(wnba_stats.wnba_stats_boxscoretraditionalv3)["GameID"].startswith("102")  # WNBA game
    # wehoop sets no id for these; the other league's (an NBA game, LeBron) is never borrowed
    assert "GameID" not in _sent(wnba_stats.wnba_stats_boxscorehustlev2)
    assert "PlayerID" not in _sent(wnba_stats.wnba_stats_playerdashptshotdefend)
    assert _sent(wnba_stats.wnba_stats_playbyplayv2)["StartPeriod"] == "0"  # wehoop's constant


# One wrapper per family the 2026-10-05 sweep found broken, plus the all-seasons control.
_LIVE = [
    nba_stats.nba_stats_playergamelogs,
    nba_stats.nba_stats_commonteamroster,
    nba_stats.nba_stats_leaguedashplayerstats,
    nba_stats.nba_stats_draftcombinestats,
    nba_stats.nba_stats_teaminfocommon,
    nba_stats.nba_stats_drafthistory,
    wnba_stats.wnba_stats_playergamelogs,
    wnba_stats.wnba_stats_commonallplayers,
]


@skip_if_no_nba_stats_live
@pytest.mark.parametrize("fn", _LIVE, ids=lambda f: f.__name__)
def test_live_defaults_return_data(fn):
    raw = fn(return_parsed=False)
    assert raw.get("resultSets") or raw.get("resultSet"), f"{fn.__name__}() came back empty"
