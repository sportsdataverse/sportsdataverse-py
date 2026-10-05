"""Season names -> end year, checked on every league's real seasons feed (PY-6).

The feeds name a season "2025-26", "2025/26", "2025-2026", "2026 - 27", "26-27", "2425" or
"2026". The fixtures are the ``modulekit/seasons`` replies of all 20 leagues: 18 taken live
2026-10-05, AHL trimmed from 2026-07-12, PWHL through id 10 from 2026-06-09
(``tests/fixtures/hockeytech/README.md``). The feed's own dates are the oracle: a regular
season or playoffs ends in its end year, a one-year-named preseason or exhibition starts
the year before it.
"""

from __future__ import annotations

import importlib
import re

import polars as pl
import pytest

from sportsdataverse.hockeytech import _leagues
from sportsdataverse.hockeytech._leagues import (
    LEAGUES,
    SPECIAL_EVENT_SEASON_RE,
    most_recent_season_yr,
    resolve_season_id,
)
from sportsdataverse.hockeytech._parsers import (
    TWO_YEAR_NAME_RE,
    _derive_season_year,
    _game_type_label,
    parse_seasons,
)
from tests.conftest import load_fixture

# Rows whose name and dates disagree at the source: "CCHL Playoffs 2022" ran March-May 2023,
# and MJHL's "2015 Playoffs" runs 2015-03-04 to 2016-04-30 on the feed.
_FEED_DATE_ANOMALIES = {("cchl", "CCHL Playoffs 2022"), ("mjhl", "2015 Playoffs")}
_CAMP = ("preseason", "exhibition")


@pytest.mark.parametrize(
    "name, year",
    [
        ("2025-26 Regular Season", 2026),
        ("1999-00 Regular Season", 2000),
        ("2025/26 Regular Season", 2026),  # KIJHL: was 2025
        ("2025-2026 Regular Season", 2026),  # AJHL, SPHL, VIJHL: was 2120
        ("1999-2000 WHL Season", 2000),  # was 2020
        ("2026 - 27 Regular Season", 2027),  # WHL: was 2026
        ("2026 - 2027 Preseason", 2027),  # OJHL: was 2026
        ("OJHL 2019/2020", 2020),  # was 2019
        ("2026-27 | Regular Season", 2027),  # QMJHL
        ("26-27 Regular Season", 2027),  # OJHL: was None
        ("98-99 Regular Season", 1999),  # 2099 is not a season yet
        ("CCHL 2425 Special Events", 2025),  # was 2425
        ("CCHL 2324 ALL-STAR GAMES", 2024),  # was 2324
        ("2026 Playoffs", 2026),
        ("OJHL Playoffs - 2026", 2026),
        ("Season 2099", None),
        ("3025-26 Regular Season", None),
        ("19 Tie Break", None),
        ("Eastern Canada Cup", None),
    ],
)
def test_derive_season_year_every_name_form(name, year):
    assert _derive_season_year(name) == year


@pytest.mark.parametrize(
    "name, label",
    [
        ("2026-27 Regular Season", "regular"),
        ("2026 Memorial Cup", "regular"),
        ("2026 Pre-season", "preseason"),
        ("2025-26 Preseason Exhibition", "preseason"),  # SJHL: a preseason first
        ("2026 Calder Cup Playoffs", "playoffs"),
        ("2026 Exhibition Season", "exhibition"),  # AJHL: was regular
        ("2026-2027 Exhibition Schedule", "exhibition"),  # VIJHL: was regular
    ],
)
def test_game_type_label(name, label):
    assert _game_type_label(name) == label


def _seasons(league):
    return load_fixture("hockeytech", f"{league}_seasons")


def _patch(monkeypatch, payload):
    monkeypatch.setattr(_leagues, "_fetch_seasons_raw", lambda lg: payload)


@pytest.mark.parametrize("league", sorted(LEAGUES))
def test_season_yr_agrees_with_the_feed_dates(league):
    df = parse_seasons(_seasons(league))
    assert df.height
    assert df.schema["season_yr"] == pl.Int64
    checked = 0
    for r in df.iter_rows(named=True):
        name, yr = r["season_name"], r["season_yr"]
        m = re.search(r"(?<!\d)(\d{4}|\d{2})\s*[-/]\s*(?:\d{4}|\d{2})(?!\d)", name)
        if m:  # a two-year name ends the year after it starts
            start = int(m.group(1))
            assert yr == (start if start > 99 else 2000 + start) + 1, name
        if yr is None or not (r["end_date"] or "")[:4].isdigit() or (league, name) in _FEED_DATE_ANOMALIES:
            continue
        if r["game_type_label"] in _CAMP:
            if not m:  # a one-year name: the season the camp opens
                assert yr == int(r["start_date"][:4]) + 1, name
        elif re.search(SPECIAL_EVENT_SEASON_RE, name):
            continue
        else:
            assert yr == int(r["end_date"][:4]), name
        checked += 1
    assert checked


@pytest.mark.parametrize(
    "league, name, year, label",
    [
        ("ajhl", "2026 Exhibition Season", 2027, "exhibition"),  # starts 2026-08-30: was 2026
        ("mhl", "2026-27 MHL Exhibition Season", 2027, "exhibition"),
        ("mjhl", "2026-27 Exhibition Season", 2027, "exhibition"),
        ("vijhl", "2026-2027 Exhibition Schedule", 2027, "exhibition"),
        ("kijhl", "2025/26 Exhibition", 2026, "exhibition"),
        ("ahl", "2017-18 Exhibition", 2018, "exhibition"),  # two-year name starting 2018-02-13: not moved
        ("ohl", "2026 Pre-season", 2027, "preseason"),
        ("pwhl", "2024 Preseason", 2024, "preseason"),  # began 2023-11-01
    ],
)
def test_camp_rows_belong_to_the_season_they_open(league, name, year, label):
    df = parse_seasons(_seasons(league)).filter(pl.col("season_name") == name)
    assert df.select("season_yr", "game_type_label").row(0) == (year, label)


def test_a_two_year_preseason_name_is_never_moved_by_its_dates():
    df = parse_seasons(
        {"SiteKit": {"Seasons": [{"season_id": "1", "season_name": "2026-27 Preseason", "start_date": "2027-01-05"}]}}
    )
    assert df["season_yr"][0] == 2027


def test_season_yr_is_int64_in_pandas_too():
    pdf = parse_seasons(_seasons("cchl"), return_as_pandas=True)  # has a None year
    assert str(pdf["season_yr"].dtype) == "Int64" and pdf["season_yr"].isna().any()


@pytest.mark.parametrize("league", sorted(LEAGUES))
def test_every_resolved_season_is_that_season(monkeypatch, league):
    """Every (year, game type) the feed has resolves to a row whose dates give that year; a
    regular season is never a tournament ("2025 Mowat Cup") outside CHL, which has only those."""
    payload = _seasons(league)
    _patch(monkeypatch, payload)
    df = parse_seasons(payload)
    rows = {r["season_id"]: r for r in df.iter_rows(named=True)}
    resolved = 0
    for yr in sorted({int(y) for y in df["season_yr"].drop_nulls()}):
        for game_type in ("regular", "playoffs", *_CAMP):
            try:
                r = rows[resolve_season_id(league, season=yr, game_type=game_type)]
            except (ValueError, KeyError):  # no such season, or PWHL's fallback table
                continue
            resolved += 1
            name = r["season_name"]
            if game_type == "regular" and league != "chl":
                assert re.search(r"(?i)regular season|season|\d{2}\s*[-/]\s*\d{2}", name), (yr, name)
            if (league, name) in _FEED_DATE_ANOMALIES or not (r["end_date"] or "")[:4].isdigit():
                continue
            if game_type in _CAMP:
                if re.search(TWO_YEAR_NAME_RE, name):
                    continue
                dated = int(r["start_date"][:4]) + 1
            else:
                dated = int(r["end_date"][:4])
            assert dated == yr, (yr, game_type, name)
    assert resolved


@pytest.mark.parametrize("league", sorted(LEAGUES))
def test_most_recent_season_has_a_regular_season(monkeypatch, league):
    """most_recent_<lg>_season() then <lg>_standings(season=...) used to be 2120 -> ValueError."""
    payload = _seasons(league)
    _patch(monkeypatch, payload)
    yr = most_recent_season_yr(parse_seasons(payload), league)
    assert yr in (2026, 2027)
    assert resolve_season_id(league, season=yr) > 0


@pytest.mark.parametrize("league", sorted(LEAGUES))
def test_default_season_resolves_while_a_camp_precedes_its_regular_season(monkeypatch, league):
    """Rebuild the feed as it stood when each preseason / exhibition row was added (every row
    with a season_id up to its own) and resolve the default season. The feed lists the camp
    first ("2026 Preseason", id 77, before "2026-27 Regular Season", id 78, in ECHL), and a
    default taken from every row named a 2027 that had no regular season yet."""
    rows = _seasons(league)["SiteKit"]["Seasons"]
    camps = parse_seasons({"SiteKit": {"Seasons": rows}}).filter(pl.col("game_type_label").is_in(_CAMP))
    windows = 0
    for cut in camps["season_id"]:
        payload = {"SiteKit": {"Seasons": [r for r in rows if int(r["season_id"]) <= cut]}}
        df = parse_seasons(payload)
        regular = df.filter(
            (pl.col("game_type_label") == "regular") & ~pl.col("season_name").str.contains(SPECIAL_EVENT_SEASON_RE)
        )
        if not regular.height:  # MJHL's "2017-18 Pre-season" is id 1: nothing to default to
            continue
        _patch(monkeypatch, payload)
        resolve_season_id(league, season=most_recent_season_yr(df, league))
        windows += 1
    # the trimmed July AHL capture keeps no row older than its 2017-18 exhibition (id 58)
    assert windows or league == "ahl" or not camps.height


def test_echl_default_season_in_the_preseason_window(monkeypatch):
    """The real ECHL feed cut at "2026 Preseason" (id 77): standings() defaults to 2025-26."""
    from sportsdataverse.hockeytech import _client, _family

    rows = [r for r in _seasons("echl")["SiteKit"]["Seasons"] if int(r["season_id"]) <= 77]
    sent = []

    def fake(league, feed, view, params=None, **k):
        if view == "seasons":
            return {"SiteKit": {"Seasons": rows}}
        sent.append(dict(params or {}))
        return {}

    for mod in (_client, _family):
        monkeypatch.setattr(mod, "hockeytech_api", fake)
    echl = importlib.import_module("sportsdataverse.hockey.echl")
    assert echl.most_recent_echl_season() == 2026
    echl.echl_standings()
    regular_2025_26 = next(int(r["season_id"]) for r in rows if r["season_name"] == "2025-26 Regular Season")
    assert sent[-1]["season"] == regular_2025_26


@pytest.mark.parametrize(
    "league, season, game_type, expected",
    [
        ("ajhl", 2026, "regular", "2025-2026 Regular Season"),  # was ValueError
        ("ajhl", 2025, "playoffs", "2024-2025 Playoffs"),
        ("ajhl", 2027, "exhibition", "2026 Exhibition Season"),
        ("sphl", 2026, "regular", "2025-2026 Regular Season"),
        ("gojhl", 2026, "regular", "2025-2026 GOHL Season"),
        ("vijhl", 2024, "regular", "2023-2024 VIJHL Regular Season"),
        ("vijhl", 2026, "playoffs", "2025-2026 VIJHL Playoffs"),
        ("cchl", 2024, "regular", "CCHL 2023-2024"),
        ("cchl", 2022, "playoffs", "CCHL Playoffs 2021-22"),  # not "CCHL Playoffs 2022" (2023)
        ("ojhl", 2027, "regular", "26-27 Regular Season"),
        ("ojhl", 2025, "regular", "2024-2025 Regular Season"),  # not "2025 Cottage Cup"
        ("ojhl", 2023, "regular", "OJHL 22-23"),
        ("ojhl", 2010, "regular", "OJAHL 2009/2010"),  # not the CCHL's, listed first in OJHL's feed
        ("ojhl", 2010, "playoffs", "OJAHL Playoffs 2010"),
        ("whl", 2026, "regular", "2025 - 26 Regular Season"),  # was 2026-27
        ("kijhl", 2026, "regular", "2025/26 Regular Season"),  # was 2026/27
        ("kijhl", 2025, "regular", "2024/25 Regular Season"),  # not "2025 Mowat Cup"
        ("mjhl", 2019, "regular", "2018-19 Regular Season"),  # not "2019 ANAVET Cup"
        ("gojhl", 2027, "preseason", "2026 GOHL Pre-Season"),  # was 2026
        ("ohl", 2027, "preseason", "2026 Pre-season"),
        ("pwhl", 2024, "preseason", "2024 Preseason"),  # began 2023-11-01: stays 2024
        ("pwhl", 2024, "regular", "2024 Regular Season"),
        ("chl", 2026, "regular", "2026 Memorial Cup"),  # single-year seasons still resolve
        ("ahl", 2026, "regular", "2025-26 Regular Season"),  # not "2026 All-Star Challenge"
        # Divisions side by side, no marker between them: feed order, documented, not chosen.
        ("bchl", 2024, "regular", "2023-24 BC Regular Season"),
        ("bchl", 2024, "playoffs", "2024 AB Playoffs"),
    ],
)
def test_resolve_season_id_picks_the_season(monkeypatch, league, season, game_type, expected):
    payload = _seasons(league)
    _patch(monkeypatch, payload)
    names = {int(s["season_id"]): s["season_name"] for s in payload["SiteKit"]["Seasons"]}
    picked = names[resolve_season_id(league, season=season, game_type=game_type)]
    assert picked == expected
    assert game_type in _CAMP or not re.search(SPECIAL_EVENT_SEASON_RE, picked)


def test_a_name_that_says_regular_season_outranks_a_two_year_event(monkeypatch):
    """Constructed: no feed has this tie today, so it pins the order of the two preferences."""
    rows = [
        {"season_id": "2", "season_name": "2023-24 Hockey Canada Cup"},
        {"season_id": "1", "season_name": "2024 Regular Season"},
    ]
    _patch(monkeypatch, {"SiteKit": {"Seasons": rows}})
    assert resolve_season_id("sjhl", season=2024) == 1
