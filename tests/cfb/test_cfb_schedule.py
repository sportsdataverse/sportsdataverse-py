import polars as pl
import pytest

from sportsdataverse.cfb.cfb_schedule import (
    espn_cfb_calendar,
    espn_cfb_schedule,
    most_recent_cfb_season,
)


@pytest.fixture()
def calendar_data():
    yield espn_cfb_calendar(season=most_recent_cfb_season(), return_as_pandas=False)


def calendar_data_check(calendar_data):
    assert isinstance(calendar_data, pl.DataFrame)
    assert len(calendar_data) > 0


@pytest.fixture()
def calendar_ondays_data():
    yield espn_cfb_calendar(season=2021, ondays=True, return_as_pandas=False)


def calendar_ondays_data_check(calendar_ondays_data):
    assert isinstance(calendar_ondays_data, pl.DataFrame)
    assert len(calendar_ondays_data) > 0


@pytest.fixture()
def schedule_data():
    yield espn_cfb_schedule(return_as_pandas=False)


def schedule_data_check(schedule_data):
    assert isinstance(schedule_data, pl.DataFrame)
    assert len(schedule_data) > 0


@pytest.fixture()
def schedule_data2():
    yield espn_cfb_schedule(dates=20220901, return_as_pandas=False)


def schedule_data_check2(schedule_data2):
    assert isinstance(schedule_data, pl.DataFrame)
    assert len(schedule_data) > 0


@pytest.fixture()
def week_1_schedule():
    yield espn_cfb_schedule(dates=2022, week=1, return_as_pandas=False)


def week_1_schedule_check(week_1_schedule):
    assert isinstance(week_1_schedule, pl.DataFrame)
    assert len(week_1_schedule) > 0
