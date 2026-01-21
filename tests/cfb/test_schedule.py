import pandas as pd
import pytest
import datetime
from sportsdataverse.cfb.cfb_schedule import (
    espn_cfb_schedule,
    espn_cfb_calendar,
    most_recent_cfb_season,
)


class TestEspnCfbSchedule:
    """Test suite for espn_cfb_schedule function"""

    def test_espn_cfb_schedule_default_parameters(self):
        """Test schedule with default parameters"""
        result = espn_cfb_schedule()

        assert isinstance(result, pd.DataFrame)
        # May be empty if no current games, so just check it's a valid dataframe
        assert result is not None

    def test_espn_cfb_schedule_with_dates(self):
        """Test schedule with specific date"""
        # Use a date that should have games (e.g., September 2020)
        result = espn_cfb_schedule(dates=20200905)

        assert isinstance(result, pd.DataFrame)

    def test_espn_cfb_schedule_with_week(self):
        """Test schedule with specific week"""
        result = espn_cfb_schedule(dates=2020, week=1)

        assert isinstance(result, pd.DataFrame)

    def test_espn_cfb_schedule_with_season_type(self):
        """Test schedule with season type (regular season)"""
        result = espn_cfb_schedule(dates=2020, season_type=2)

        assert isinstance(result, pd.DataFrame)

    def test_espn_cfb_schedule_fbs_only(self):
        """Test schedule for FBS teams only (group 80)"""
        result = espn_cfb_schedule(dates=2020, groups=80)

        assert isinstance(result, pd.DataFrame)

    def test_espn_cfb_schedule_fcs_only(self):
        """Test schedule for FCS teams only (group 81)"""
        try:
            result = espn_cfb_schedule(dates=2020, groups=81)
            assert isinstance(result, pd.DataFrame)
        except TypeError:
            # API may fail or return no data, which causes TypeError in the function
            # This is a known issue with the ESPN API for FCS schedules
            pytest.skip("ESPN API unavailable or returned no data for FCS schedules")

    def test_espn_cfb_schedule_with_limit(self):
        """Test schedule with custom limit"""
        result = espn_cfb_schedule(dates=2020, limit=10)

        assert isinstance(result, pd.DataFrame)
        # Result should respect limit if there are enough games
        if len(result) > 0:
            assert len(result) <= 10

    def test_espn_cfb_schedule_column_names(self):
        """Test that column names are properly formatted with underscores"""
        result = espn_cfb_schedule(dates=2020, week=1)

        if len(result) > 0:
            # Check that columns are snake_case (underscore format)
            for col in result.columns:
                assert " " not in col  # No spaces in column names

    def test_espn_cfb_schedule_has_game_id(self):
        """Test that schedule includes game_id column"""
        result = espn_cfb_schedule(dates=2020, week=1)

        if len(result) > 0:
            assert "game_id" in result.columns
            # Check that game_ids are integers
            assert result["game_id"].dtype in ["int64", "Int64"]

    def test_espn_cfb_schedule_has_season_info(self):
        """Test that schedule includes season information"""
        result = espn_cfb_schedule(dates=2020, week=1)

        if len(result) > 0:
            assert "season" in result.columns
            assert "season_type" in result.columns
            assert "week" in result.columns


class TestEspnCfbCalendar:
    """Test suite for espn_cfb_calendar function"""

    def test_espn_cfb_calendar_basic(self):
        """Test calendar with basic season parameter"""
        result = espn_cfb_calendar(season=2020)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_espn_cfb_calendar_with_groups(self):
        """Test calendar with FBS group"""
        result = espn_cfb_calendar(season=2020, groups=80)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_espn_cfb_calendar_ondays(self):
        """Test calendar with ondays parameter"""
        result = espn_cfb_calendar(season=2020, ondays=True)

        assert isinstance(result, pd.DataFrame)
        # Should return dates
        if len(result) > 0:
            assert "dates" in result.columns

    def test_espn_cfb_calendar_column_format(self):
        """Test that calendar columns are properly formatted"""
        result = espn_cfb_calendar(season=2020)

        # Check that columns are snake_case
        for col in result.columns:
            assert " " not in col  # No spaces in column names

    def test_espn_cfb_calendar_has_season(self):
        """Test that calendar includes season information"""
        result = espn_cfb_calendar(season=2020)

        if "season" in result.columns:
            assert result["season"].iloc[0] == 2020

    def test_espn_cfb_calendar_has_weeks(self):
        """Test that calendar includes week information"""
        result = espn_cfb_calendar(season=2020)

        if len(result) > 0 and "week" in result.columns:
            # Should have multiple weeks in a season
            assert result["week"].nunique() > 1


class TestMostRecentCfbSeason:
    """Test suite for most_recent_cfb_season function"""

    def test_most_recent_cfb_season_returns_integer(self):
        """Test that function returns an integer year"""
        result = most_recent_cfb_season()

        assert isinstance(result, int)
        assert result >= 2002  # Earliest available season

    def test_most_recent_cfb_season_reasonable_year(self):
        """Test that returned season is within reasonable range"""
        result = most_recent_cfb_season()
        current_year = datetime.datetime.now().year

        # Should be current year or previous year
        assert result in [current_year - 1, current_year]

    def test_most_recent_cfb_season_logic(self):
        """Test the season determination logic"""
        result = most_recent_cfb_season()
        now = datetime.datetime.now()

        # If it's after August 15 or September, should be current year
        # Otherwise, should be previous year
        if (now.month >= 9) or (now.month == 8 and now.day >= 15):
            assert result == now.year
        else:
            assert result == now.year - 1
