import polars as pl
import pytest

from sportsdataverse.cfb.cfb_schedule import (
    espn_cfb_calendar,
    espn_cfb_schedule,
    most_recent_cfb_season,
)


@pytest.fixture()
def cfb_calendar_data():
    yield espn_cfb_calendar(season=most_recent_cfb_season(), return_as_pandas=False)


def test_cfb_calendar_data_check(cfb_calendar_data):
    assert isinstance(cfb_calendar_data, pl.DataFrame)
    assert len(cfb_calendar_data) > 0


@pytest.fixture()
def cfb_calendar_ondays_data():
    yield espn_cfb_calendar(season=2021, ondays=True, return_as_pandas=False)


def test_cfb_calendar_ondays_data_check(cfb_calendar_ondays_data):
    assert isinstance(cfb_calendar_ondays_data, pl.DataFrame)
    assert len(cfb_calendar_ondays_data) > 0


@pytest.fixture()
def cfb_schedule_data():
    yield espn_cfb_schedule(return_as_pandas=False)


def test_cfb_schedule_data_check(cfb_schedule_data):
    assert isinstance(cfb_schedule_data, pl.DataFrame)
    assert len(cfb_schedule_data) > 0


@pytest.fixture()
def cfb_schedule_data2():
    yield espn_cfb_schedule(dates=20220901, return_as_pandas=False)


def test_cfb_schedule_data_check2(cfb_schedule_data2):
    assert isinstance(cfb_schedule_data2, pl.DataFrame)
    assert len(cfb_schedule_data2) > 0


@pytest.fixture()
def week_1_cfb_schedule():
    yield espn_cfb_schedule(dates=2022, week=1, return_as_pandas=False)


def test_week_1_cfb_schedule_check(week_1_cfb_schedule):
    assert isinstance(week_1_cfb_schedule, pl.DataFrame)
    assert len(week_1_cfb_schedule) > 0
