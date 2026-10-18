import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_schedule import (
    espn_mbb_calendar,
    espn_mbb_schedule,
    most_recent_mbb_season,
)
from sportsdataverse.errors import AssetFetchError
from tests.codegen.test_runtime_errors import _route
from tests.conftest import skip_if_no_live

ESPN = Path(__file__).resolve().parents[1] / "fixtures" / "espn"


def _capture(name: str) -> str:
    return (ESPN / name).read_text(encoding="utf-8")


def _season_route(monkeypatch, fail_day: str = "", fail=(500, "")):
    """Serve the 2023-24 season: the calendar request (``dates=2024``) gets the real season
    scoreboard (its ``leagues[0].calendar`` lists 148 game days), each captured day its real
    events, ``fail_day`` the ``fail`` answer, every other calendar day an empty slate."""
    calendar = _capture("scoreboard_mbb_dates_2024.json")
    days = json.loads(_capture("mbb_season_days_2024.json"))

    def answer(url: str):
        day = url.split("dates=")[1].split("&")[0]
        if day == fail_day:
            return (*fail, "application/json")
        if day == "2024":
            return 200, calendar, "application/json"
        return 200, json.dumps(days.get(day, {"events": []})), "application/json"

    return _route(monkeypatch, answer), json.loads(calendar), days


def test_mbb_schedule_bare_year_sweeps_the_season_calendar(monkeypatch):
    """B-M13 / R855-1: ``dates=2024`` is the 2023-24 season, opening night to the title game.

    Real captures (2026-10-13): a whole-year ``dates=2024`` request stops at 500 events on Jan 13,
    and the hoopR-style two-year sweep missed ~3,110 of 6,249 games (all 120 postseason). The
    port reads the season calendar and requests each of its 148 days in order.
    """
    calls, calendar, days = _season_route(monkeypatch)
    df = espn_mbb_schedule(dates=2024)
    requested = [c.split("dates=")[1].split("&")[0] for c in calls]
    cal_days = [d[:10].replace("-", "") for d in calendar["leagues"][0]["calendar"]]
    assert len(cal_days) == 148 and cal_days[0] == "20231106" and cal_days[-1] == "20240408"
    assert requested == ["2024", *cal_days]  # one calendar request, then one per game day, in order
    assert all("groups=50" in c for c in calls)
    want = sorted(
        {e["id"]: e for d in days.values() for e in d["events"] if e["season"]["year"] == 2024}.values(),
        key=lambda e: e["date"],
    )
    assert df["game_id"].to_list() == [int(e["id"]) for e in want]
    assert df["date"].to_list() == sorted(df["date"].to_list())
    assert df["date"].str.slice(0, 7).unique().sort().to_list() == [
        "2023-11",
        "2023-12",
        "2024-01",
        "2024-02",
        "2024-03",
        "2024-04",
    ]
    assert set(df["season"].to_list()) == {2024}
    assert df.filter(pl.col("season_type") == 3).height == 3  # NCAA tournament + title game


@pytest.mark.parametrize("fail", [(500, ""), (200, "{}")], ids=["http_500", "no_events_key"])
def test_mbb_season_sweep_failed_day_raises(monkeypatch, fail):
    """R855-1: a day that fails is an error, never a silent hole in the season."""
    _season_route(monkeypatch, fail_day="20240210", fail=fail)
    with pytest.raises(AssetFetchError):
        espn_mbb_schedule(dates=2024)


def test_mbb_calendar_season_is_an_int(monkeypatch):
    """B-L9: ``season=None`` means the current season and ``2024.0`` is requested as ``2024``."""
    calls = _route(monkeypatch, lambda url: (200, _capture("scoreboard_mbb.json"), "application/json"))
    espn_mbb_calendar(season=2024.0)
    espn_mbb_calendar()
    assert calls[0].endswith("dates=2024")
    assert calls[1].endswith(f"dates={most_recent_mbb_season()}")


@pytest.fixture()
def mbb_calendar_data():
    yield espn_mbb_calendar(season=most_recent_mbb_season(), return_as_pandas=False)


@skip_if_no_live
def test_mbb_calendar_data_check(mbb_calendar_data):
    assert isinstance(mbb_calendar_data, pl.DataFrame)
    assert len(mbb_calendar_data) > 0


@pytest.fixture()
def mbb_calendar_ondays_data():
    yield espn_mbb_calendar(season=2021, ondays=True, return_as_pandas=False)


@skip_if_no_live
def test_mbb_calendar_ondays_data_check(mbb_calendar_ondays_data):
    assert isinstance(mbb_calendar_ondays_data, pl.DataFrame)
    assert len(mbb_calendar_ondays_data) > 0


@pytest.fixture()
def mbb_schedule_data():
    yield espn_mbb_schedule(return_as_pandas=False)


@skip_if_no_live
def test_mbb_schedule_data_check(mbb_schedule_data):
    assert isinstance(mbb_schedule_data, pl.DataFrame)
    assert len(mbb_schedule_data) > 0


@pytest.fixture()
def mbb_schedule_data2():
    yield espn_mbb_schedule(dates=20221201, return_as_pandas=False)


@skip_if_no_live
def test_mbb_schedule_data_check2(mbb_schedule_data2):
    assert isinstance(mbb_schedule_data2, pl.DataFrame)
    assert len(mbb_schedule_data2) > 0


@pytest.fixture()
def january_schedule():
    yield espn_mbb_schedule(dates=20230123, return_as_pandas=False)


@skip_if_no_live
def test_january_schedule_check(january_schedule):
    assert isinstance(january_schedule, pl.DataFrame)
    assert len(january_schedule) > 0
