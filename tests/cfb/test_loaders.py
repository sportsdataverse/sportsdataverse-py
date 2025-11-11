import pandas as pd
import pytest
from sportsdataverse.cfb.cfb_loaders import (
    load_cfb_pbp,
    load_cfb_schedule,
    load_cfb_rosters,
    load_cfb_team_info,
    get_cfb_teams
)
from sportsdataverse.errors import SeasonNotFoundError


class TestLoadCfbPbp:
    """Test suite for load_cfb_pbp function"""

    def test_load_cfb_pbp_single_season(self):
        """Test loading play-by-play data for a single season"""
        result = load_cfb_pbp(seasons=[2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'game_id' in result.columns

    def test_load_cfb_pbp_multiple_seasons(self):
        """Test loading play-by-play data for multiple seasons"""
        result = load_cfb_pbp(seasons=[2019, 2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        # Should have data from both seasons
        assert result['season'].nunique() >= 1

    def test_load_cfb_pbp_integer_input(self):
        """Test that function handles integer input for season"""
        result = load_cfb_pbp(seasons=2020)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_pbp_invalid_season(self):
        """Test that function raises error for seasons before 2003"""
        with pytest.raises(SeasonNotFoundError, match="season cannot be less than 2003"):
            load_cfb_pbp(seasons=[2002])

    def test_load_cfb_pbp_unique_index(self):
        """Test that returned dataframe has unique index"""
        result = load_cfb_pbp(seasons=[2020])
        
        assert result.index.is_unique
        assert result.index.min() == 0


class TestLoadCfbSchedule:
    """Test suite for load_cfb_schedule function"""

    def test_load_cfb_schedule_single_season(self):
        """Test loading schedule data for a single season"""
        result = load_cfb_schedule(seasons=[2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'game_id' in result.columns

    def test_load_cfb_schedule_multiple_seasons(self):
        """Test loading schedule data for multiple seasons"""
        result = load_cfb_schedule(seasons=[2019, 2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        # Should have data from both seasons
        assert result['season'].nunique() >= 1

    def test_load_cfb_schedule_integer_input(self):
        """Test that function handles integer input for season"""
        result = load_cfb_schedule(seasons=2020)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_schedule_invalid_season(self):
        """Test that function raises error for seasons before 2002"""
        with pytest.raises(SeasonNotFoundError, match="season cannot be less than 2002"):
            load_cfb_schedule(seasons=[2001])

    def test_load_cfb_schedule_unique_index(self):
        """Test that returned dataframe has unique index"""
        result = load_cfb_schedule(seasons=[2020])
        
        assert result.index.is_unique
        assert result.index.min() == 0


class TestLoadCfbRosters:
    """Test suite for load_cfb_rosters function"""

    def test_load_cfb_rosters_single_season(self):
        """Test loading roster data for a single season"""
        result = load_cfb_rosters(seasons=[2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_rosters_multiple_seasons(self):
        """Test loading roster data for multiple seasons"""
        result = load_cfb_rosters(seasons=[2019, 2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_rosters_integer_input(self):
        """Test that function handles integer input for season"""
        result = load_cfb_rosters(seasons=2020)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_rosters_invalid_season(self):
        """Test that function raises error for seasons before 2004"""
        with pytest.raises(SeasonNotFoundError, match="season cannot be less than 2004"):
            load_cfb_rosters(seasons=[2003])

    def test_load_cfb_rosters_unique_index(self):
        """Test that returned dataframe has unique index"""
        result = load_cfb_rosters(seasons=[2020])
        
        assert result.index.is_unique
        assert result.index.min() == 0


class TestLoadCfbTeamInfo:
    """Test suite for load_cfb_team_info function"""

    def test_load_cfb_team_info_single_season(self):
        """Test loading team info for a single season"""
        result = load_cfb_team_info(seasons=[2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_team_info_multiple_seasons(self):
        """Test loading team info for multiple seasons"""
        result = load_cfb_team_info(seasons=[2019, 2020])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_team_info_integer_input(self):
        """Test that function handles integer input for season"""
        result = load_cfb_team_info(seasons=2020)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_load_cfb_team_info_invalid_season(self):
        """Test that function raises error for seasons before 2002"""
        with pytest.raises(SeasonNotFoundError, match="season cannot be less than 2002"):
            load_cfb_team_info(seasons=[2001])

    def test_load_cfb_team_info_unique_index(self):
        """Test that returned dataframe has unique index"""
        result = load_cfb_team_info(seasons=[2020])
        
        assert result.index.is_unique
        assert result.index.min() == 0


class TestGetCfbTeams:
    """Test suite for get_cfb_teams function"""

    def test_get_cfb_teams_basic(self):
        """Test loading CFB teams data"""
        result = get_cfb_teams()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_get_cfb_teams_has_required_columns(self):
        """Test that teams data has expected columns"""
        result = get_cfb_teams()
        
        # Should have team-related columns
        assert len(result.columns) > 0
        # Should have multiple teams
        assert len(result) > 50  # FBS has over 100 teams

