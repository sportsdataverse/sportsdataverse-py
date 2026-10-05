"""Season names -> end year, checked on every league's real seasons feed (PY-6).

The feeds name a season "2025-26", "2025/26", "2025-2026", "2026 - 27", "26-27", "2425" or
"2026"; the fixtures are the live ``modulekit/seasons`` replies of all 20 leagues
(``tests/fixtures/hockeytech/README.md``). The feed's own dates are the oracle: a regular
season or playoffs ends in its end year, a preseason starts the year before it.
"""

from __future__ import annotations

import re

import pytest

from sportsdataverse.hockeytech import _leagues
from sportsdataverse.hockeytech._leagues import (
    LEAGUES,
    SPECIAL_EVENT_SEASON_RE,
    most_recent_season_yr,
    resolve_season_id,
)
from sportsdataverse.hockeytech._parsers import _derive_season_year, parse_seasons
from tests.conftest import load_fixture

# Rows whose name and dates disagree at the source: "CCHL Playoffs 2022" ran March-May 2023,
# and MJHL's "2015 Playoffs" runs 2015-03-04 to 2016-04-30 on the feed.
_FEED_DATE_ANOMALIES = {("cchl", "CCHL Playoffs 2022"), ("mjhl", "2015 Playoffs")}


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


def _seasons(league):
    return load_fixture("hockeytech", f"{league}_seasons")


def _patch(monkeypatch, payload):
    monkeypatch.setattr(_leagues, "_fetch_seasons_raw", lambda lg: payload)


@pytest.mark.parametrize("league", sorted(LEAGUES))
def test_season_yr_agrees_with_the_feed_dates(league):
    df = parse_seasons(_seasons(league))
    assert df.height
    checked = 0
    for r in df.iter_rows(named=True):
        name, yr = r["season_name"], r["season_yr"]
        m = re.search(r"(?<!\d)(\d{4}|\d{2})\s*[-/]\s*(?:\d{4}|\d{2})(?!\d)", name)
        if m:  # a two-year name ends the year after it starts
            start = int(m.group(1))
            assert yr == (start if start > 99 else 2000 + start) + 1, name
        if yr is None or not (r["end_date"] or "")[:4].isdigit() or (league, name) in _FEED_DATE_ANOMALIES:
            continue
        if re.search(SPECIAL_EVENT_SEASON_RE, name):
            continue
        if r["game_type_label"] == "preseason":
            assert yr == int(r["start_date"][:4]) + 1, name
        else:
            assert yr == int(r["end_date"][:4]), name
        checked += 1
    assert checked


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
        for game_type in ("regular", "playoffs", "preseason"):
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
            dated = int(r["start_date"][:4]) + 1 if game_type == "preseason" else int(r["end_date"][:4])
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


@pytest.mark.parametrize(
    "league, season, game_type, expected",
    [
        ("ajhl", 2026, "regular", "2025-2026 Regular Season"),  # was ValueError
        ("ajhl", 2025, "playoffs", "2024-2025 Playoffs"),
        ("sphl", 2026, "regular", "2025-2026 Regular Season"),
        ("gojhl", 2026, "regular", "2025-2026 GOHL Season"),
        ("vijhl", 2024, "regular", "2023-2024 VIJHL Regular Season"),
        ("vijhl", 2026, "playoffs", "2025-2026 VIJHL Playoffs"),
        ("cchl", 2024, "regular", "CCHL 2023-2024"),
        ("cchl", 2022, "playoffs", "CCHL Playoffs 2021-22"),  # not "CCHL Playoffs 2022" (2023)
        ("ojhl", 2027, "regular", "26-27 Regular Season"),
        ("ojhl", 2025, "regular", "2024-2025 Regular Season"),  # not "2025 Cottage Cup"
        ("ojhl", 2023, "regular", "OJHL 22-23"),
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
    ],
)
def test_resolve_season_id_picks_the_season(monkeypatch, league, season, game_type, expected):
    payload = _seasons(league)
    _patch(monkeypatch, payload)
    names = {int(s["season_id"]): s["season_name"] for s in payload["SiteKit"]["Seasons"]}
    picked = names[resolve_season_id(league, season=season, game_type=game_type)]
    assert picked == expected
    assert game_type == "preseason" or not re.search(SPECIAL_EVENT_SEASON_RE, picked)


def test_a_name_that_says_regular_season_outranks_a_two_year_event(monkeypatch):
    """Constructed: no feed has this tie today, so it pins the order of the two preferences."""
    rows = [
        {"season_id": "2", "season_name": "2023-24 Hockey Canada Cup"},
        {"season_id": "1", "season_name": "2024 Regular Season"},
    ]
    _patch(monkeypatch, {"SiteKit": {"Seasons": rows}})
    assert resolve_season_id("sjhl", season=2024) == 1
