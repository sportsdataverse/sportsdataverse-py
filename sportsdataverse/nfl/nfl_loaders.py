import pyarrow.parquet as pq
import pandas as pd
import pyreadr
import tempfile
import json
from tqdm import tqdm
from pyreadr import read_r
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import NFL_BASE_URL, NFL_PLAYER_URL, NFL_ROSTER_URL,\
    NFL_WEEKLY_ROSTER_URL, NFL_TEAM_LOGO_URL, NFL_TEAM_SCHEDULE_URL,\
    NFL_PLAYER_STATS_URL, NFL_PLAYER_KICKING_STATS_URL, NFL_SNAP_COUNTS_URL,\
    NFL_PBP_PARTICIPATION_URL, NFL_CONTRACTS_URL, NFL_DRAFT_PICKS_URL,\
    NFL_COMBINE_URL, NFL_INJURIES_URL, NFL_DEPTH_CHARTS_URL, NFL_OFFICIALS_URL,\
    NFL_OTC_PLAYER_DETAILS_URL, NFL_NGS_PASSING_URL, NFL_NGS_RUSHING_URL,\
    NFL_NGS_RECEIVING_URL, NFL_PFR_SEASON_DEF_URL, NFL_PFR_WEEK_DEF_URL,\
    NFL_PFR_SEASON_PASS_URL, NFL_PFR_WEEK_PASS_URL, NFL_PFR_SEASON_REC_URL,\
    NFL_PFR_WEEK_REC_URL, NFL_PFR_SEASON_RUSH_URL, NFL_PFR_WEEK_RUSH_URL
from sportsdataverse.errors import SeasonNotFoundError, season_not_found_error
from sportsdataverse.dl_utils import download

def load_nfl_pbp(seasons: List[int]) -> pd.DataFrame:
    """Load NFL play by play data going back to 1999

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pbp(seasons=range(1999,2021))`

    Args:
        seasons (list): Used to define different seasons. 1999 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 1999.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 1999)
        i_data = pd.read_parquet(NFL_BASE_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_schedule(seasons: List[int]) -> pd.DataFrame:
    """Load NFL schedule data

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_schedule(seasons=range(1999,2021))`

    Args:
        seasons (list): Used to define different seasons. 1999 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

    Raises:
        ValueError: If `season` is less than 1999.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    with tempfile.TemporaryDirectory() as tempdirname:
        for i in tqdm(seasons):
            season_not_found_error(int(i), 1999)
            schedule_url = NFL_TEAM_SCHEDULE_URL.format(season=i)
            #i_data = pd.read_parquet(NFL_TEAM_SCHEDULE_URL.format(season = i), engine='auto', columns=None)
            i_data = pyreadr.read_r(pyreadr.download_file(schedule_url, "{}/nfl_sched_{}.rds".format(tempdirname, i)))[None]
            i_data = pd.DataFrame(i_data)
            data = pd.concat([data, i_data], ignore_index=True)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def load_nfl_player_stats(kicking = False) -> pd.DataFrame:
    """Load NFL player stats data

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_player_stats()`

    Args:
        kicking (bool): If True, load kicking stats. If False, load all other stats.

    Returns:
        pd.DataFrame: Pandas dataframe containing player stats.
    """
    data = pd.DataFrame()
    if kicking is False:
        data = pd.read_parquet(NFL_PLAYER_STATS_URL, engine='auto', columns=None)
    else:
        data = pd.read_parquet(NFL_PLAYER_KICKING_STATS_URL, engine='auto', columns=None)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_ngs_passing() -> pd.DataFrame:
    """Load NFL NextGen Stats Passing data going back to 2016

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_ngs_passing()`

    Returns:
        pd.DataFrame: Pandas dataframe containing the NextGen Stats Passing data available.

    """
    df = pd.read_parquet(NFL_NGS_PASSING_URL, engine='auto', columns=None)
    return df

def load_nfl_ngs_rushing() -> pd.DataFrame:
    """Load NFL NextGen Stats Rushing data going back to 2016

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_ngs_rushing()`

    Returns:
        pd.DataFrame: Pandas dataframe containing the NextGen Stats Rushing data available.

    """
    df = pd.read_parquet(NFL_NGS_RUSHING_URL, engine='auto', columns=None)
    return df

def load_nfl_ngs_receiving() -> pd.DataFrame:
    """Load NFL NextGen Stats Receiving data going back to 2016

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_ngs_receiving()`

    Returns:
        pd.DataFrame: Pandas dataframe containing the NextGen Stats Receiving data available.

    """
    df = pd.read_parquet(NFL_NGS_RECEIVING_URL, engine='auto', columns=None)
    return df

def load_nfl_pfr_pass() -> pd.DataFrame:
    """Load NFL Pro-Football Reference Advanced Passing data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_pass()`

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced passing stats data available.

    """
    df = pd.read_parquet(NFL_PFR_SEASON_PASS_URL, engine='auto', columns=None)
    return df

def load_nfl_pfr_weekly_pass(seasons: List[int]) -> pd.DataFrame:
    """Load NFL Pro-Football Reference Weekly Advanced Passing data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_pass(seasons=range(2018,2021))`

    Args:
        seasons (list): Used to define different seasons. 2018 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced passing stats data available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pd.read_parquet(NFL_PFR_WEEK_PASS_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_pfr_rush() -> pd.DataFrame:
    """Load NFL Pro-Football Reference Advanced Rushing data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_rush()`

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced rushing stats data available.

    """
    df = pd.read_parquet(NFL_PFR_SEASON_RUSH_URL, engine='auto', columns=None)
    return df

def load_nfl_pfr_weekly_rush(seasons: List[int]) -> pd.DataFrame:
    """Load NFL Pro-Football Reference Weekly Advanced Rushing data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_rush(seasons=range(2018,2021))`

    Args:
        seasons (list): Used to define different seasons. 2018 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced rushing stats data available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pd.read_parquet(NFL_PFR_WEEK_RUSH_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_pfr_rec() -> pd.DataFrame:
    """Load NFL Pro-Football Reference Advanced Receiving data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_rec()`

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced receiving stats data available.

    """
    df = pd.read_parquet(NFL_PFR_SEASON_REC_URL, engine='auto', columns=None)
    return df

def load_nfl_pfr_weekly_rec(seasons: List[int]) -> pd.DataFrame:
    """Load NFL Pro-Football Reference Weekly Advanced Receiving data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_rec(seasons=range(2018,2021))`

    Args:
        seasons (list): Used to define different seasons. 2018 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced receiving stats data available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pd.read_parquet(NFL_PFR_WEEK_REC_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_pfr_def() -> pd.DataFrame:
    """Load NFL Pro-Football Reference Advanced Defensive data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_def()`

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced defensive stats data available.

    """
    df = pd.read_parquet(NFL_PFR_SEASON_DEF_URL, engine='auto', columns=None)
    return df

def load_nfl_pfr_weekly_def(seasons: List[int]) -> pd.DataFrame:
    """Load NFL Pro-Football Reference Weekly Advanced Defensive data going back to 2018

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_def(seasons=range(2018,2021))`

    Args:
        seasons (list): Used to define different seasons. 2018 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing Pro-Football Reference
            advanced defensive stats data available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pd.read_parquet(NFL_PFR_WEEK_DEF_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data


def load_nfl_rosters(seasons: List[int]) -> pd.DataFrame:
    """Load NFL roster data for all seasons

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_rosters(seasons=range(1999,2021))`

    Args:
        seasons (list): Used to define different seasons. 1920 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 1920)
        i_data = pd.read_parquet(NFL_ROSTER_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_weekly_rosters(seasons: List[int]) -> pd.DataFrame:
    """Load NFL weekly roster data for selected seasons

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_weekly_rosters(seasons=range(2002,2021))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing weekly rosters available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2002)
        i_data = pd.read_parquet(NFL_WEEKLY_ROSTER_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_teams() -> pd.DataFrame:
    """Load NFL team ID information and logos

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_teams()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing teams available.
    """
    df = pd.read_csv(NFL_TEAM_LOGO_URL, low_memory=False)
    return df

def load_nfl_players() -> pd.DataFrame:
    """Load NFL Player ID information

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_players()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing players available.
    """
    df = pd.read_parquet(NFL_PLAYER_URL, engine='auto', columns=None)
    return df

def load_nfl_snap_counts(seasons: List[int]) -> pd.DataFrame:
    """Load NFL snap counts data for selected seasons

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_snap_counts(seasons=range(2012,2021))`

    Args:
        seasons (list): Used to define different seasons. 2012 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing snap counts available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2012)
        i_data = pd.read_parquet(NFL_SNAP_COUNTS_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_pbp_participation(seasons: List[int]) -> pd.DataFrame:
    """Load NFL play-by-play participation data for selected seasons

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_pbp_participation(seasons=range(2016,2021))`

    Args:
        seasons (list): Used to define different seasons. 2016 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing play-by-play participation data available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2016)
        i_data = pd.read_parquet(NFL_PBP_PARTICIPATION_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_injuries(seasons: List[int]) -> pd.DataFrame:
    """Load NFL injuries data for selected seasons

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_injuries(seasons=range(2009,2021))`

    Args:
        seasons (list): Used to define different seasons. 2009 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing injuries data available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2009)
        i_data = pd.read_parquet(NFL_INJURIES_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_depth_charts(seasons: List[int]) -> pd.DataFrame:
    """Load NFL Depth Chart data for selected seasons

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_depth_charts(seasons=range(2001,2021))`

    Args:
        seasons (list): Used to define different seasons. 2001 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing depth chart data available for the requested seasons.

    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2001)
        i_data = pd.read_parquet(NFL_DEPTH_CHARTS_URL.format(season=i), engine='auto', columns=None)
        data = pd.concat([data, i_data], ignore_index=True)

    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nfl_contracts() -> pd.DataFrame:
    """Load NFL Historical contracts information

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_contracts()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing historical contracts available.
    """
    df = pd.read_parquet(NFL_CONTRACTS_URL, engine='auto', columns=None)
    return df


def load_nfl_combine() -> pd.DataFrame:
    """Load NFL Combine information

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_combine()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing NFL combine data available.
    """
    df = pd.read_parquet(NFL_COMBINE_URL, engine='auto', columns=None)
    return df

def load_nfl_draft_picks() -> pd.DataFrame:
    """Load NFL Draft picks information

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_draft_picks()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing NFL Draft picks data available.
    """
    df = pd.read_parquet(NFL_DRAFT_PICKS_URL, engine='auto', columns=None)
    return df

def load_nfl_officials() -> pd.DataFrame:
    """Load NFL Officials information

    Example:
        `nfl_df = sportsdataverse.nfl.load_nfl_officials()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing officials available.
    """
    df = pd.read_parquet(NFL_OFFICIALS_URL, engine='auto', columns=None)
    return df
## Currently removed due to unsupported features of pyreadr's method.
## there is a list-column of nested tibbles within the data
## that is not supported by pyreadr
# def load_nfl_player_contracts_detail() -> pd.DataFrame:
#     """Load NFL Player contracts detail information

#     Example:
#         `nfl_df = sportsdataverse.nfl.load_nfl_player_contracts_detail()`

#     Args:

#     Returns:
#         pd.DataFrame: Pandas dataframe containing player contracts detail data available.
#     """
#     data = pd.DataFrame()
#     with tempfile.TemporaryDirectory() as tempdirname:
#         df = pyreadr.read_r(
#             pyreadr.download_file(NFL_OTC_PLAYER_DETAILS_URL,
#                 "{}/otc_player_details.rds".format(tempdirname)))[None]
#         df = pd.DataFrame(df)
#         data = pd.concat([data, df], ignore_index=True)
#     #Give each row a unique index
#     data.reset_index(drop=True, inplace=True)
#     return df