import pandas as pd
import pytest
from sportsdataverse.cfb.cfb_teams import espn_cfb_teams


class TestEspnCfbTeams:
    """Test suite for espn_cfb_teams function"""

    def test_espn_cfb_teams_default(self):
        """Test teams lookup with default parameters (FBS)"""
        result = espn_cfb_teams()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        # FBS should have over 100 teams
        assert len(result) > 100

    def test_espn_cfb_teams_fbs(self):
        """Test teams lookup for FBS (group 80)"""
        result = espn_cfb_teams(groups=80)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        # FBS should have over 100 teams
        assert len(result) > 100

    def test_espn_cfb_teams_fcs(self):
        """Test teams lookup for FCS (group 81)"""
        result = espn_cfb_teams(groups=81)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        # FCS should have many teams
        assert len(result) > 50

    def test_espn_cfb_teams_has_team_columns(self):
        """Test that teams dataframe has expected team-related columns"""
        result = espn_cfb_teams()
        
        # Check for expected columns (using snake_case)
        assert 'team_id' in result.columns or any('team' in col for col in result.columns)
        
    def test_espn_cfb_teams_column_format(self):
        """Test that column names are properly formatted with underscores"""
        result = espn_cfb_teams()
        
        # Check that columns are snake_case
        for col in result.columns:
            assert ' ' not in col  # No spaces in column names

    def test_espn_cfb_teams_has_data(self):
        """Test that each team has required information"""
        result = espn_cfb_teams()
        
        # Should have multiple columns of information
        assert len(result.columns) > 5
        
        # Should not have null values for key identifiers
        if 'team_id' in result.columns:
            assert result['team_id'].notna().all()

    def test_espn_cfb_teams_unique_teams(self):
        """Test that teams are unique"""
        result = espn_cfb_teams()
        
        # Find the team ID column
        id_col = None
        for col in result.columns:
            if 'id' in col.lower() and 'team' in col.lower():
                id_col = col
                break
        
        if id_col is not None:
            # Each team should appear only once
            assert result[id_col].is_unique

    def test_espn_cfb_teams_different_groups(self):
        """Test that different groups parameter works without error"""
        fbs_teams = espn_cfb_teams(groups=80)
        fcs_teams = espn_cfb_teams(groups=81)
        
        # Both should return valid dataframes
        assert isinstance(fbs_teams, pd.DataFrame)
        assert isinstance(fcs_teams, pd.DataFrame)
        assert len(fbs_teams) > 0
        assert len(fcs_teams) > 0
        # Note: ESPN API may return the same teams for both groups
