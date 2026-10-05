"""Season + entity defaults of the generated nba_stats / wnba_stats wrappers.

stats.nba.com answers most endpoints with an empty HTTP 500 when ``Season`` is missing, and the
runtime turns that into ``{}``. The wrappers used to default season to ``None`` (the hoopR/wehoop
call default was lost in the catalog), and NBA wrappers inherited WNBA entity ids. These tests pin
the regenerated defaults offline; the live sweep re-measures them against the API.
"""

import time
from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from sportsdataverse.nba import nba_stats
from sportsdataverse.nba import nba_stats_runtime as rt
from sportsdataverse.nba.nba_schedule import year_to_season
from sportsdataverse.nba.nba_stats_runtime import _latest_season
from sportsdataverse.nba.nba_stats_runtime import season_latest_with_data as nba_season
from sportsdataverse.wnba import wnba_stats
from sportsdataverse.wnba.wnba_stats_runtime import season_latest_with_data as wnba_season
from tests.conftest import skip_if_no_nba_stats_live

_ENDPOINTS = Path(__file__).resolve().parents[2] / "tools/codegen/endpoints"


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
# 2026-03-28; the Summer League "2026-27" ran 2026-07-09 to 07-19; G League playoffs (leaguegamelog,
# SeasonType=Playoffs) began 2023-03-28, 2024-04-02, 2025-04-01, 2026-03-31 (the latest is used, so a
# rollover before it fails). The rest are the league calendars (NBA opener 2025-10-21, WNBA
# 2025-05-16, NBA playoffs 2026-04-18, WNBA playoffs 2025-09-14, the 2026 draft combine ended
# mid-May, the NBA draft is in late June, the WNBA draft in mid-April).
_FIRST_ROWS = {
    ("00", ""): (10, 21, 0),
    ("20", ""): (12, 19, 0),
    ("15", ""): (7, 9, 0),
    ("10", ""): (5, 16, 0),
    ("00", "draftcombinestats"): (5, 18, 0),
    ("20", "draftcombinestats"): (5, 18, 0),  # the same combine, whichever league asks
    ("00", "drafthistory"): (6, 26, 0),
    ("10", "drafthistory"): (4, 15, 0),
    ("00", "commonplayoffseries"): (4, 18, 1),
    ("20", "commonplayoffseries"): (4, 2, 1),
    ("10", "commonplayoffseries"): (9, 14, 0),
    # "SeasonType=..." is a season type on any endpoint. All-Star games (leaguegamefinder, 2026-10-05):
    # NBA 2019-02-17 .. 2024-02-18 (latest 2022-02-20; 2021's 03-07 is the pandemic outlier), WNBA
    # 2021-07-14 .. 2026-07-25. The G League has none (leaguegamelog 2024-25, 2025-26: 0 rows), so its
    # All-Star default is its regular season's.
    ("00", "SeasonType=All Star"): (2, 20, 1),
    ("10", "SeasonType=All Star"): (7, 25, 0),
    ("20", "SeasonType=All Star"): (12, 19, 0),
}
# The one month in which the newest season has rows but the default is still the previous one.
_LAG_MONTH = {
    ("00", ""): 10,
    ("20", ""): 12,
    ("15", ""): 7,
    ("10", ""): 5,
    ("00", "draftcombinestats"): 5,
    ("20", "draftcombinestats"): 5,
    ("00", "drafthistory"): 6,
    ("10", "drafthistory"): 4,
    ("00", "commonplayoffseries"): 4,
    ("20", "commonplayoffseries"): 4,
    ("10", "commonplayoffseries"): 9,
    ("00", "SeasonType=All Star"): 2,
    ("10", "SeasonType=All Star"): 7,
    ("20", "SeasonType=All Star"): 12,
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
    league, what = kind
    endpoint, _, season_type = what.partition("SeasonType=")
    day = date(year, 1, 1)
    while day.year == year:
        label = _latest_season(league, endpoint, today=day, season_type=season_type or None)
        start = int(label[:4])
        assert len(label) == (4 if league == "10" or endpoint == "drafthistory" else 7), (day, label)
        assert _first_rows(kind, start) <= day, f"{day}: default {label} has no rows yet"
        if _first_rows(kind, start + 1) <= day:  # a newer season has rows
            assert day.month == _LAG_MONTH[kind], f"{day}: {label} is not the latest with rows"
        if not what and day.month not in _HOOPR_DIFFERS[league]:
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
    assert _sent(nba_stats.nba_stats_leaguedashteamstats)["Season"] == nba  # was every season summed
    assert _sent(nba_stats.nba_stats_leaguedashplayerstats, league_id="20")["Season"] == gleague
    assert _sent(nba_stats.nba_stats_leaguedashplayerstats, league_id="15")["Season"] == summer
    assert _sent(nba_stats.nba_stats_draftcombinestats)["SeasonYear"] == combine
    assert _sent(wnba_stats.wnba_stats_playergamelogs)["Season"] == wnba
    assert _sent(wnba_stats.wnba_stats_playerdashptshotdefend)["Season"] == wnba
    assert _sent(wnba_stats.wnba_stats_leaguedashteamstats)["Season"] == wnba


# One wrapper per SeasonType argument name; every one sends the wire key ``SeasonType``.
_SEASON_TYPE_ARGS = [
    (nba_stats.nba_stats_leaguedashplayerstats, "season_type_all_star"),
    (nba_stats.nba_stats_leaguegamefinder, "season_type_nullable"),
    (nba_stats.nba_stats_leaderstiles, "season_type_playoffs"),
    (nba_stats.nba_stats_assisttracker, "season_type_all_star_nullable"),
    (nba_stats.nba_stats_playerestimatedmetrics, "season_type"),
]


@pytest.mark.parametrize("season_type", ["Playoffs", "PlayIn", "All Star"])
@pytest.mark.parametrize("fn,arg", _SEASON_TYPE_ARGS, ids=lambda v: getattr(v, "__name__", v))
def test_season_types_default_to_one_that_has_been_played(monkeypatch, fn, arg, season_type):
    monkeypatch.setattr(rt, "date", _on(date(2027, 2, 1)))  # 2026-27 under way, its All-Star and playoffs not
    assert _sent(fn)["Season"] == "2026-27"
    assert _sent(fn, **{arg: season_type})["Season"] == "2025-26"


def test_season_type_rollover_per_league(monkeypatch):
    monkeypatch.setattr(rt, "date", _on(date(2027, 2, 1)))
    assert _sent(nba_stats.nba_stats_commonplayoffseries)["Season"] == "2025-26"
    assert _sent(nba_stats.nba_stats_commonplayoffseries, league_id="20")["Season"] == "2025-26"
    gleague = {"league_id": "20", "season_type_all_star": "Playoffs"}
    assert _sent(nba_stats.nba_stats_leaguegamelog, **gleague)["Season"] == "2025-26"
    gleague["season_type_all_star"] = "All Star"  # no G League All-Star rows: its regular rule
    assert _sent(nba_stats.nba_stats_leaguegamelog, **gleague)["Season"] == "2026-27"
    monkeypatch.setattr(rt, "date", _on(date(2027, 7, 1)))  # WNBA 2027 under way, its playoffs not
    assert _sent(wnba_stats.wnba_stats_commonplayoffseries)["Season"] == "2026"
    assert _sent(wnba_stats.wnba_stats_leaguedashplayerstats)["Season"] == "2027"
    assert _sent(wnba_stats.wnba_stats_leaguedashplayerstats, season_type_all_star="Playoffs")["Season"] == "2026"
    assert _sent(wnba_stats.wnba_stats_leaguedashplayerstats, season_type_all_star="All Star")["Season"] == "2026"
    monkeypatch.setattr(rt, "date", _on(date(2027, 8, 1)))  # WNBA 2027 All-Star played
    assert _sent(wnba_stats.wnba_stats_leaguedashplayerstats, season_type_all_star="All Star")["Season"] == "2027"


def test_drafts_default_to_the_latest_draft(monkeypatch):
    monkeypatch.setattr(rt, "date", _on(date(2026, 6, 30)))  # NBA draft late June, WNBA mid-April
    assert _sent(nba_stats.nba_stats_drafthistory)["Season"] == "2025"
    assert _sent(wnba_stats.wnba_stats_drafthistory)["Season"] == "2026"
    monkeypatch.setattr(rt, "date", _on(date(2026, 7, 1)))
    assert _sent(nba_stats.nba_stats_drafthistory)["Season"] == "2026"
    # the combine rule holds for whichever league asks (it was the G League's January rule)
    assert _sent(nba_stats.nba_stats_draftcombinestats, league_id="20")["SeasonYear"] == "2026-27"


def test_explicit_season_wins(monkeypatch):
    monkeypatch.setattr(rt, "date", _on(date(2026, 10, 5)))
    assert _sent(nba_stats.nba_stats_playergamelogs, season_nullable="2023-24")["Season"] == "2023-24"
    # never re-dated, even for a league whose default differs
    assert _sent(nba_stats.nba_stats_leaguedashplayerstats, league_id="15", season="2025-26")["Season"] == "2025-26"
    assert nba_season("") == "" and wnba_season("") == ""


def _season_wrappers():
    for mod, stem in ((nba_stats, "nba_stats"), (wnba_stats, "wnba_stats")):
        doc = yaml.safe_load((_ENDPOINTS / f"{stem}.yaml").read_text(encoding="utf-8"))
        for ep in doc["endpoints"]:
            keys = sorted({p["query_key"] for p in ep["extra_params"]} & {"Season", "SeasonYear"})
            if keys:
                yield getattr(mod, f"{stem}_{ep['short']}"), "/".join(keys)


@pytest.mark.parametrize("fn,keys", list(_season_wrappers()), ids=lambda v: getattr(v, "__name__", v))
def test_every_season_endpoint_sends_a_season(fn, keys):
    # hoopR / wehoop give each one a season default (a few a literal one, paired with literal ids;
    # leaguestandings' second key, SeasonYear, defaults to "" in hoopR and stays so). Without one
    # stats.nba.com answers an empty HTTP 500 or every season summed (leaguedashteamstats:
    # SuperSonics and Bullets rows, GP up to 2,395).
    sent = _sent(fn)
    assert any(sent.get(k) for k in keys.split("/")), f"{fn.__name__} sends no {keys}"


def test_entity_defaults_belong_to_the_wrappers_league():
    assert _sent(nba_stats.nba_stats_teaminfocommon)["TeamID"] == "1610612749"  # was a WNBA team -> 500
    assert _sent(nba_stats.nba_stats_boxscoretraditionalv3)["GameID"].startswith("002")  # NBA game
    assert _sent(wnba_stats.wnba_stats_boxscoretraditionalv3)["GameID"].startswith("102")  # WNBA game
    # wehoop sets no id for these; the other league's (an NBA game, LeBron) is never borrowed
    assert "GameID" not in _sent(wnba_stats.wnba_stats_boxscorehustlev2)
    assert "PlayerID" not in _sent(wnba_stats.wnba_stats_playerdashptshotdefend)
    assert _sent(wnba_stats.wnba_stats_playbyplayv2)["StartPeriod"] == "0"  # wehoop's constant


# One wrapper per family the 2026-10-05 sweeps found broken (no season: an HTTP 500, or every season
# summed), each league.
_LIVE = [
    (nba_stats.nba_stats_playergamelogs, {"player_id_nullable": "2544"}),
    (nba_stats.nba_stats_commonteamroster, {}),
    (nba_stats.nba_stats_leaguedashplayerstats, {}),
    (nba_stats.nba_stats_leaguedashplayerstats, {"league_id": "20"}),
    (nba_stats.nba_stats_leaguedashplayerstats, {"league_id": "15"}),
    (nba_stats.nba_stats_draftcombinestats, {}),
    (nba_stats.nba_stats_teaminfocommon, {}),
    (nba_stats.nba_stats_drafthistory, {}),
    (nba_stats.nba_stats_leaguedashteamstats, {}),
    (nba_stats.nba_stats_commonplayoffseries, {"league_id": "20"}),
    (wnba_stats.wnba_stats_playergamelogs, {}),
    (wnba_stats.wnba_stats_commonallplayers, {}),
    (wnba_stats.wnba_stats_leaguedashteamstats, {}),
]


@skip_if_no_nba_stats_live
@pytest.mark.parametrize("fn,kwargs", _LIVE, ids=lambda v: getattr(v, "__name__", None) or str(v))
def test_live_defaults_return_rows(fn, kwargs):
    time.sleep(3.5)  # stats.nba.com throttles back-to-back requests
    raw = fn(return_parsed=False, **kwargs)
    sets = raw.get("resultSets") or raw.get("resultSet") or []
    sets = [sets] if isinstance(sets, dict) else sets
    assert sum(len(s.get("rowSet") or []) for s in sets), f"{fn.__name__}({kwargs}) returned no rows"
    for s in sets:  # one season, not all of them summed
        h, rows = s.get("headers") or [], s.get("rowSet") or []
        for col in {"SEASON", "SEASON_ID", "SEASON_YEAR"} & set(h if h and isinstance(h[0], str) else []):
            assert len({r[h.index(col)] for r in rows}) <= 1, f"{fn.__name__}: {s.get('name')}.{col}"
