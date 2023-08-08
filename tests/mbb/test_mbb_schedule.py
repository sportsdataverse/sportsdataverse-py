import polars as pl
import pytest

from sportsdataverse.mbb.mbb_schedule import (
    espn_mbb_calendar,
    espn_mbb_schedule,
    most_recent_mbb_season,
)


@pytest.fixture()
def mbb_calendar_data():
    yield espn_mbb_calendar(season=most_recent_mbb_season(), return_as_pandas=False)


def test_mbb_calendar_data_check(mbb_calendar_data):
    assert isinstance(mbb_calendar_data, pl.DataFrame)
    assert len(mbb_calendar_data) > 0


@pytest.fixture()
def mbb_calendar_ondays_data():
    yield espn_mbb_calendar(season=2021, ondays=True, return_as_pandas=False)


def test_mbb_calendar_ondays_data_check(mbb_calendar_ondays_data):
    assert isinstance(mbb_calendar_ondays_data, pl.DataFrame)
    assert len(mbb_calendar_ondays_data) > 0


@pytest.fixture()
def mbb_schedule_data():
    yield espn_mbb_schedule(return_as_pandas=False)


def test_mbb_schedule_data_check(mbb_schedule_data):
    assert isinstance(mbb_schedule_data, pl.DataFrame)
    assert len(mbb_schedule_data) > 0


@pytest.fixture()
def mbb_schedule_data2():
    yield espn_mbb_schedule(dates=20221201, return_as_pandas=False)


def test_mbb_schedule_data_check2(mbb_schedule_data2):
    assert isinstance(mbb_schedule_data2, pl.DataFrame)
    assert len(mbb_schedule_data2) > 0


@pytest.fixture()
def january_schedule():
    yield espn_mbb_schedule(dates=20230123, return_as_pandas=False)


def test_january_schedule_check(january_schedule):
    assert isinstance(january_schedule, pl.DataFrame)
    assert len(january_schedule) > 0
