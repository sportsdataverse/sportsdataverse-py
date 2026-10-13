import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_schedule import (
    espn_mbb_calendar,
    espn_mbb_schedule,
    most_recent_mbb_season,
)
from tests.codegen.test_runtime_errors import _route
from tests.conftest import skip_if_no_live

ESPN = Path(__file__).resolve().parents[1] / "fixtures" / "espn"


def _capture(name: str) -> str:
    return (ESPN / name).read_text(encoding="utf-8")


def test_mbb_schedule_bare_year_sweeps_both_calendar_years(monkeypatch):
    """B-M13: ``dates=2024`` is the 2023-24 season, not calendar 2024 (hoopR ``espn_mbb_scoreboard``).

    Real captures (2026-10-13): ESPN answers ``dates=2024`` with Jan 1-13 2024 only and
    ``dates=2023`` with Jan 2023 (season 2023, filtered out); the season's November games come
    from the per-day backfill (``dates=20231106``). Other fall days answer an empty slate.
    """
    answers = {
        "dates=2024&": _capture("scoreboard_mbb_dates_2024.json"),
        "dates=2023&": _capture("scoreboard_mbb_dates_2023.json"),
        "dates=20231106&": _capture("scoreboard_mbb_20231106.json"),
    }

    def answer(url: str):
        body = next((b for k, b in answers.items() if k in url), json.dumps({"events": []}))
        return 200, body, "application/json"

    calls = _route(monkeypatch, answer)
    df = espn_mbb_schedule(dates=2024)
    dates = [c.split("dates=")[1].split("&")[0] for c in calls]
    assert dates[:2] == ["2023", "2024"]
    assert dates[2:] == [f"2023{m}{d:02d}" for m, n in (("11", 30), ("12", 31)) for d in range(1, n + 1)]
    assert all("groups=50" in c for c in calls)
    jan = [int(e["id"]) for e in json.loads(answers["dates=2024&"])["events"]]
    nov = [int(e["id"]) for e in json.loads(answers["dates=20231106&"])["events"]]
    assert df["game_id"].to_list() == jan + nov  # the 2023 (calendar) games are season 2023: dropped
    assert set(df["season"].to_list()) == {2024}
    assert df["date"].str.slice(0, 7).unique().sort().to_list() == ["2023-11", "2024-01"]


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
