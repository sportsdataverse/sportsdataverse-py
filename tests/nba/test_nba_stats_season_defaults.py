"""Season + entity defaults of the generated nba_stats / wnba_stats wrappers.

stats.nba.com answers most endpoints with an empty HTTP 500 when ``Season`` is missing, and the
runtime turns that into ``{}``. The wrappers used to default season to ``None`` (the hoopR/wehoop
call default was lost in the catalog), and NBA wrappers inherited WNBA entity ids. These tests pin
the regenerated defaults offline; the live sweep re-measures them against the API.
"""

from datetime import date, timedelta

import pytest

from sportsdataverse.nba import nba_stats
from sportsdataverse.nba import nba_stats_runtime as rt
from sportsdataverse.nba.nba_schedule import year_to_season
from sportsdataverse.nba.nba_stats_runtime import _latest_season
from sportsdataverse.nba.nba_stats_runtime import season_latest_with_data as nba_season
from sportsdataverse.wnba import wnba_stats
from sportsdataverse.wnba.wnba_stats_runtime import season_latest_with_data as wnba_season
from tests.conftest import skip_if_no_nba_stats_live
from tools.codegen.gen_nba_stats import _SEASON_OPTIONAL


def _sent(fn, **kwargs):
    seen = {}

    def transport(url, params, headers, proxy_url):
        seen.update(params)
        return 200, '{"resultSets": []}'

    fn(return_parsed=False, transport=transport, **kwargs)
    return seen


def _on(day):
    class Today(date):
        @classmethod
        def today(cls):
            return day

    return Today


# The first day each kind of season has rows, as (month, day, years after its first year).
# Measured on stats.nba.com 2026-10-05: the G League "Regular Season" 2025-26 ran 2025-12-19 to
# 2026-03-28; the Summer League "2026-27" ran 2026-07-09 to 07-19. The rest are the league calendars
# (NBA opener 2025-10-21, WNBA 2025-05-16, NBA playoffs 2026-04-18, WNBA playoffs 2025-09-14,
# the 2026 draft combine ended mid-May).
_FIRST_ROWS = {
    ("00", ""): (10, 21, 0),
    ("20", ""): (12, 19, 0),
    ("15", ""): (7, 9, 0),
    ("10", ""): (5, 16, 0),
    ("00", "draftcombinestats"): (5, 18, 0),
    ("00", "commonplayoffseries"): (4, 18, 1),
    ("10", "commonplayoffseries"): (9, 14, 0),
}
# The one month in which the newest season has rows but the default is still the previous one.
_LAG_MONTH = {
    ("00", ""): 10,
    ("20", ""): 12,
    ("15", ""): 7,
    ("10", ""): 5,
    ("00", "draftcombinestats"): 5,
    ("00", "commonplayoffseries"): 4,
    ("10", "commonplayoffseries"): 9,
}
# Months in which hoopR's current season (``year_to_season(most_recent_nba_season() - 1)``, rolls
# over in October) or wehoop's ``most_recent_wnba_season()`` (rolls over in May) is a different one.
_HOOPR_DIFFERS = {"00": {10}, "20": {10, 11, 12}, "15": {8, 9}, "10": {5}}


def _first_rows(kind, start):
    month, day, years = _FIRST_ROWS[kind]
    return date(start + years, month, day)


def _hoopr_current(league, day):
    if league == "10":
        return str(day.year if day.month >= 5 else day.year - 1)
    return year_to_season(day.year + (day.month >= 10) - 1)


@pytest.mark.parametrize("year", [1999, 2008, 2026])
@pytest.mark.parametrize("kind", list(_FIRST_ROWS), ids=" ".join)
def test_default_is_the_latest_season_with_rows_every_day(kind, year):
    league, endpoint = kind
    day = date(year, 1, 1)
    while day.year == year:
        label = _latest_season(league, endpoint, today=day)
        start = int(label[:4])
        assert len(label) == (4 if league == "10" else 7), (day, label)
        assert _first_rows(kind, start) <= day, f"{day}: default {label} has no rows yet"
        if _first_rows(kind, start + 1) <= day:  # a newer season has rows
            assert day.month == _LAG_MONTH[kind], f"{day}: {label} is not the latest with rows"
        if not endpoint and day.month not in _HOOPR_DIFFERS[league]:
            assert label == _hoopr_current(league, day), day
        day += timedelta(days=1)


# date -> NBA, G League, WNBA, Summer League, draft combine
_TABLE = [
    (date(2026, 1, 15), "2025-26", "2025-26", "2025", "2025-26", "2025-26"),
    (date(2026, 5, 31), "2025-26", "2025-26", "2025", "2025-26", "2025-26"),
    (date(2026, 6, 1), "2025-26", "2025-26", "2026", "2025-26", "2026-27"),
    (date(2026, 7, 31), "2025-26", "2025-26", "2026", "2025-26", "2026-27"),
    (date(2026, 8, 1), "2025-26", "2025-26", "2026", "2026-27", "2026-27"),
    (date(2026, 10, 5), "2025-26", "2025-26", "2026", "2026-27", "2026-27"),
    (date(2026, 10, 31), "2025-26", "2025-26", "2026", "2026-27", "2026-27"),
    (date(2026, 11, 1), "2026-27", "2025-26", "2026", "2026-27", "2026-27"),
    (date(2026, 12, 31), "2026-27", "2025-26", "2026", "2026-27", "2026-27"),
    (date(2027, 1, 1), "2026-27", "2026-27", "2026", "2026-27", "2026-27"),
    (date(1999, 11, 1), "1999-00", "1998-99", "1999", "1999-00", "1999-00"),
    (date(2008, 6, 1), "2007-08", "2007-08", "2008", "2007-08", "2008-09"),
]


@pytest.mark.parametrize("day,nba,gleague,wnba,summer,combine", _TABLE, ids=lambda v: str(v))
def test_wrappers_send_the_latest_season_with_rows(monkeypatch, day, nba, gleague, wnba, summer, combine):
    monkeypatch.setattr(rt, "date", _on(day))
    assert nba_season(None) == nba and wnba_season(None) == wnba
    assert _sent(nba_stats.nba_stats_playergamelogs)["Season"] == nba
    assert _sent(nba_stats.nba_stats_commonteamroster)["Season"] == nba
    assert _sent(nba_stats.nba_stats_leaguedashplayerstats, league_id="20")["Season"] == gleague
    assert _sent(nba_stats.nba_stats_leaguedashplayerstats, league_id="15")["Season"] == summer
    assert _sent(nba_stats.nba_stats_draftcombinestats)["SeasonYear"] == combine
    assert _sent(wnba_stats.wnba_stats_playergamelogs)["Season"] == wnba
    assert _sent(wnba_stats.wnba_stats_playerdashptshotdefend)["Season"] == wnba


def test_playoff_series_default_to_playoffs_that_have_been_played(monkeypatch):
    monkeypatch.setattr(rt, "date", _on(date(2027, 2, 1)))  # 2026-27 under way, its playoffs not
    assert _sent(nba_stats.nba_stats_commonplayoffseries)["Season"] == "2025-26"
    monkeypatch.setattr(rt, "date", _on(date(2026, 8, 1)))  # WNBA 2026 under way, playoffs not
    assert _sent(wnba_stats.wnba_stats_commonplayoffseries)["Season"] == "2025"


def test_explicit_season_wins(monkeypatch):
    monkeypatch.setattr(rt, "date", _on(date(2026, 10, 5)))
    assert _sent(nba_stats.nba_stats_playergamelogs, season_nullable="2023-24")["Season"] == "2023-24"
    # never re-dated, even for a league whose default differs
    assert _sent(nba_stats.nba_stats_leaguedashplayerstats, league_id="15", season="2025-26")["Season"] == "2025-26"
    assert nba_season("") == "" and wnba_season("") == ""


@pytest.mark.parametrize(
    "fn",
    [
        getattr(mod, f"{stem}_{slug}")
        for mod, stem in ((nba_stats, "nba_stats"), (wnba_stats, "wnba_stats"))
        for slug in sorted(_SEASON_OPTIONAL[stem])
    ],
    ids=lambda f: f.__name__,
)
def test_all_season_endpoints_keep_the_api_default(fn):
    # Measured to answer rows without a Season (drafthistory and the finders: every season), which a
    # season default would silently narrow.
    sent = _sent(fn)
    assert "Season" not in sent and "SeasonYear" not in sent


def test_entity_defaults_belong_to_the_wrappers_league():
    assert _sent(nba_stats.nba_stats_teaminfocommon)["TeamID"] == "1610612749"  # was a WNBA team -> 500
    assert _sent(nba_stats.nba_stats_boxscoretraditionalv3)["GameID"].startswith("002")  # NBA game
    assert _sent(wnba_stats.wnba_stats_boxscoretraditionalv3)["GameID"].startswith("102")  # WNBA game
    # wehoop sets no id for these; the other league's (an NBA game, LeBron) is never borrowed
    assert "GameID" not in _sent(wnba_stats.wnba_stats_boxscorehustlev2)
    assert "PlayerID" not in _sent(wnba_stats.wnba_stats_playerdashptshotdefend)
    assert _sent(wnba_stats.wnba_stats_playbyplayv2)["StartPeriod"] == "0"  # wehoop's constant


# One wrapper per family the 2026-10-05 sweep found broken, each league, and the all-seasons control.
_LIVE = [
    (nba_stats.nba_stats_playergamelogs, {"player_id_nullable": "2544"}),
    (nba_stats.nba_stats_commonteamroster, {}),
    (nba_stats.nba_stats_leaguedashplayerstats, {}),
    (nba_stats.nba_stats_leaguedashplayerstats, {"league_id": "20"}),
    (nba_stats.nba_stats_leaguedashplayerstats, {"league_id": "15"}),
    (nba_stats.nba_stats_draftcombinestats, {}),
    (nba_stats.nba_stats_teaminfocommon, {}),
    (nba_stats.nba_stats_drafthistory, {}),
    (wnba_stats.wnba_stats_playergamelogs, {}),
    (wnba_stats.wnba_stats_commonallplayers, {}),
]


@skip_if_no_nba_stats_live
@pytest.mark.parametrize("fn,kwargs", _LIVE, ids=lambda v: getattr(v, "__name__", None) or str(v))
def test_live_defaults_return_rows(fn, kwargs):
    raw = fn(return_parsed=False, **kwargs)
    sets = raw.get("resultSets") or raw.get("resultSet") or []
    sets = [sets] if isinstance(sets, dict) else sets
    assert sum(len(s.get("rowSet") or []) for s in sets), f"{fn.__name__}({kwargs}) returned no rows"
