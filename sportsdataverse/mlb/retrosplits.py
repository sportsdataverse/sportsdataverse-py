## Script: mlb_retrosplits.py
## Author: Joseph Armstrong (armstjc)
## Purpose: Give users of the sportsdataverse-py python package the ability
##          to download stats from and/or related to the Retrosplits project,
##          and by extension, the Retrosheet project.

"""
RETROSHEET NOTICE:

    The information used here was obtained free of
    charge from and is copyrighted by Retrosheet.  Interested
    parties may contact Retrosheet at "www.retrosheet.org".
"""
import pandas as pd
from sportsdataverse.dl_utils import download
from datetime import datetime
from io import StringIO
from tqdm import tqdm
from datetime import datetime
from sportsdataverse.errors import SeasonNotFoundError

def retrosplits_game_logs_player(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives game-level player stats from the Retrosplits project.

    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing game-level player stats from historical MLB games.
    """
    #now = datetime.now()
    #current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()
    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')


    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/daybyday/teams-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([main_df,game_log_df],ignore_index=True)
    #print(game_log_df)
    return main_df


def retrosplits_game_logs_team(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives game-level team stats from the Retrosplits project.

    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing game-level team stats from historical MLB games.
    """
    #now = datetime.now()
    #current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()
 
    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')

    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/daybyday/playing-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([game_log_df,main_df],ignore_index=True)
    #print(game_log_df)
    return main_df

def retrosplits_player_batting_by_position(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives player-level, batting by position split stats from the Retrosplits project.
    The stats returned by this function are season-level stats, not game-level stats.
    
    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing batting by position split stats for MLB players.
    """
    now = datetime.now()
    current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()

    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if first_season < 1974:
        raise SeasonNotFoundError("Batting by position splits are not advalible for seasons before 1974.")
        first_season = 1974
        print("Batting by position splits are not advalible for seasons before 1974.")
    elif first_season > current_year:
        first_season = current_year
        print(f"The people behind retrosplits do not have a time machine to get stats for {first_season} at this time.")

    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')

    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/splits/batting-byposition-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([game_log_df,main_df],ignore_index=True)
    #print(game_log_df)
    return main_df


def retrosplits_player_batting_by_runners(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives player-level, batting by runners split stats from the Retrosplits project.
    The stats are batting stats, based off of how many runners are on base at the time of the at bat.
    The stats returned by this function are season-level stats, not game-level stats.
    
    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing player-level, batting by runners split stats.
    """

    now = datetime.now()
    current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()

    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if first_season < 1974:
        raise SeasonNotFoundError("Batting by runners splits are not advalible for seasons before 1974.")
        first_season = 1974
        print("Batting by runners splits are not advalible for seasons before 1974.")
    elif first_season > current_year:
        first_season = current_year
        print(f"The people behind retrosplits do not have a time machine to get stats for {first_season} at this time.")

    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')

    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/splits/batting-byrunners-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([game_log_df,main_df],ignore_index=True)
    #print(game_log_df)
    return main_df

def retrosplits_player_batting_by_platoon(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives player-level, batting by platoon (left/right hitting vs. left/right pitching) split stats from the Retrosplits project.
    The stats are batting stats, based off of the handedness of the batter vs the handedness of the pitcher.
    The stats returned by this function are season-level stats, not game-level stats.
    
    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing player-level, batting by platoon stats for batters.
    """

    now = datetime.now()
    current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()

    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if first_season < 1974:
        raise SeasonNotFoundError("Batting by platoon splits are not advalible for seasons before 1974.")
        first_season = 1974
        print("Batting by platoon splits are not advalible for seasons before 1974.")
    elif first_season > current_year:
        first_season = current_year
        print(f"The people behind retrosplits do not have a time machine to get stats for {first_season} at this time.")

    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')

    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/splits/batting-platoon-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([game_log_df,main_df],ignore_index=True)
    #print(game_log_df)
    return main_df


def retrosplits_player_head_to_head_stats(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives batter vs. pitcher stats from the Retrosplits project.
    The stats are batting stats, based off of the preformance of that specific batter agianst a specific pitcher for the durration of that specific season.
    The stats returned by this function are season-level stats, not game-level stats.
    
    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing batter vs. pitcher stats for a season, or for a range of seasons.
    """

    now = datetime.now()
    current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()

    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if first_season < 1974:
        raise SeasonNotFoundError("Batter vs. pitcher splits are not advalible for seasons before 1974.")
        first_season = 1974
        print("Batter vs. pitcher splits are not advalible for seasons before 1974.")
    elif first_season > current_year:
        first_season = current_year
        print(f"The people behind retrosplits do not have a time machine to get stats for {first_season} at this time.")

    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')

    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/splits/headtohead-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([game_log_df,main_df],ignore_index=True)
    #print(game_log_df)
    return main_df

def retrosplits_player_pitching_by_runners(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives player-level, pitching by runners split stats from the Retrosplits project.
    The stats are pitching stats, based off of how many runners are on base at the time of the at bat.
    The stats returned by this function are season-level stats, not game-level stats.
    
    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing pitching by runners split stats for a season, or for a range of seasons.
    """

    now = datetime.now()
    current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()

    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if first_season < 1974:
        raise SeasonNotFoundError("Pitching by runners splits are not advalible for seasons before 1974.")
        first_season = 1974
        print("Pitching by runners splits are not advalible for seasons before 1974.")
    elif first_season > current_year:
        first_season = current_year
        print(f"The people behind retrosplits do not have a time machine to get stats for {first_season} at this time.")

    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')

    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/splits/pitching-byrunners-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([game_log_df,main_df],ignore_index=True)
    #print(game_log_df)
    return main_df


def retrosplits_player_pitching_by_platoon(first_season:int,last_season=None) -> pd.DataFrame():
    """
    Retrives player-level, pitching by platoon (left/right pitching vs. left/right hitting) split stats from the Retrosplits project.
    The stats are pitching stats, based off of the handedness of the pitcher vs the handedness of the batter.
    The stats returned by this function are season-level stats, not game-level stats.
    
    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

    Returns:
        A pandas dataframe containing player-level, pitching by platoon stats for pitchers.
    """

    now = datetime.now()
    current_year = int(now.year)
    game_log_df = pd.DataFrame()
    main_df = pd.DataFrame()

    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    if first_season < 1974:
        raise SeasonNotFoundError("Pitching by platoon splits are not advalible for seasons before 1974.")
        first_season = 1974
        print("Pitching by platoon splits are not advalible for seasons before 1974.")
    elif first_season > current_year:
        first_season = current_year
        print(f"The people behind retrosplits do not have a time machine to get stats for {first_season} at this time.")

    if last_season == None:
        last_season = first_season
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif last_season == first_season:
        print(f'Getting all of the games for the {first_season} MLB season!')
    elif first_season > last_season: 
        last_season = first_season
        print(f'CAUGHT EXCEPTION!\nlast_season is greater than first_season!')
        print(f'Getting all of the games for the {first_season} MLB season instead.')
    elif last_season > first_season:
        print(f'Getting all MLB games between {first_season} and {last_season}!')
    else:
        print('There is something horrifically wrong with your setup.')

    for i in tqdm(range(first_season,last_season+1)):
        season = i
        print(season)
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/splits/pitching-platoon-{season}.csv"
        #print(game_log_url)
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        #season_game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df = pd.read_csv(resp_str,sep=",")
        main_df = pd.concat([game_log_df,main_df],ignore_index=True)
    #print(game_log_df)
    return main_df
