## Script: mlb_retrosheet.py
## Author: Joseph Armstrong (armstjc)
## Purpose: Give users of the sportsdataverse-py python package the ability
##          to download stats from the Retrosheet project.

"""
RETROSHEET NOTICE:

    The information used here was obtained free of
    charge from and is copyrighted by Retrosheet.  Interested
    parties may contact Retrosheet at "www.retrosheet.org".
"""
import pandas as pd
from sportsdataverse.dl_utils import download
from io import StringIO
from tqdm import tqdm
from datetime import datetime

def retrosheet_ballparks() -> pd.DataFrame():
    """
    Retrives the current TEAMABR.txt file from the Retrosheet website, and then returns the current file as a pandas dataframe. 

    Args:
        None
    
    Returns:
        A pandas Dataframe with the biographical information of notable major leauge teams.
    """
    people_url = "https://www.retrosheet.org/parkcode.txt"
    resp = download(people_url)
    resp_str = str(resp, 'UTF-8')
    resp_str = StringIO(str(resp, 'UTF-8'))
    park_df = pd.read_csv(resp_str,sep=",",)
    park_df.columns=['park_id','park_name','park_alt_name','park_city',
    'park_state','park_start_date','park_end_date','park_leauge','park_notes']
    return park_df

def retrosheet_ejections() -> pd.DataFrame():
    """
    Retrives the current Ejecdata.txt file from the  Retrosheet website, and then returns the current file as a pandas dataframe. 

    Args:
        None
    
    Returns:
        A pandas Dataframe with the biographical information of known MLB ejections.
    """
    people_url = "https://www.retrosheet.org/Ejecdata.txt"
    resp = download(people_url)
    resp_str = str(resp, 'UTF-8')
    resp_str = StringIO(str(resp, 'UTF-8'))
    ejections_df = pd.read_csv(resp_str,sep=",")
    ejections_df.columns = ['game_id','date','dh','ejectee','ejectee_name','team','job','umpire','umpire_name','inning','reason']
    return ejections_df


def retrosheet_franchises() -> pd.DataFrame():
    """
    Retrives the current TEAMABR.txt file from the  Retrosheet website, and then returns the current file as  a pandas dataframe. 

    Args:
        None
    
    Returns:
        A pandas Dataframe with the biographical information of notable major leauge teams.
    """
    people_url = "https://www.retrosheet.org/TEAMABR.TXT"
    resp = download(people_url)
    resp_str = str(resp, 'UTF-8')
    resp_str = StringIO(str(resp, 'UTF-8'))
    fran_df = pd.read_csv(resp_str,sep=",",)
    fran_df.columns = ['fran_id','leauge','city','nickname','first_year','last_year']
    fran_df.dropna()

    return fran_df

def retrosheet_people() -> pd.DataFrame():
    """
    Retrives the current BioFile.txt file from the Retrosheet website, and then returns the current file as a pandas dataframe. 

    Args:
        None
    
    Returns:
        A pandas Dataframe with the biographical information of various individuals who have played baseball.
    """
    people_url = "https://www.retrosheet.org/BIOFILE.TXT"
    resp = download(people_url)
    resp_str = StringIO(str(resp, 'UTF-8'))
    people_df = pd.read_csv(resp_str,sep=",",)
    people_df.columns = ['player_id','last_name','first_name','nickname',
    'birthdate','birth_city','birth_state','birth_country','play_debut',
    'play_last_game','mgr_debut','mgr_last_game','coach_debut',
    'coach_last_game','ump_debut','ump_last_game','death_date',
    'death_city','death_state','death_country','bats','throws',
    'height','weight','cemetery','ceme_city','ceme_state','ceme_country',
    'ceme_note','birth_name','name_chg','bat_chg','hof']

    people_df.dropna()

    return people_df

def retrosheet_schedule(first_season:int,last_season=None,original_2020_schedule=False) -> pd.DataFrame():
    """
    Retrives the scheduled games of an MLB season, or MLB seasons.

    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

        original_2020_schedule (bool):
            Retrosheet keeps a record of the orignial 2020 MLB season, before the season was delayed due to the COVID-19 pandemic.
            
            - If this is set to True, this function will return the original 2020 MLB season, before it was altered due to the COVID-19 pandemic, if the user wants this function to return the schedule for the 2020 MLB season.
        
            - If this is set to False, this function will return the altered 2020 MLB season, after it was altered due to the COVID-19 pandemic, if the user wants this function to return the schedule for the 2020 MLB season.

    Returns:
        A pandas dataframe containg historical MLB schedules.
    """
    schedule_df = pd.DataFrame()
    season_schedule_df = pd.DataFrame()
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
        if season == 2020 and original_2020_schedule == True:
            schedule_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/schedule/2020ORIG.TXT"
        elif season == 2020 and original_2020_schedule == False:
            schedule_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/schedule/2020REV.TXT"
        else:
            schedule_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/schedule/{i}SKED.TXT"
        
        resp = download(schedule_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        season_schedule_df = pd.read_csv(resp_str,sep=",",)
        season_schedule_df.columns = ['date','game_num','day_of_week',
        'road_team','road_leauge','road_team_game_num',
        'home_team','home_leauge','home_team_game_num','time_of_game',
        'postponement_indicator','makeup_date']
        schedule_df = pd.concat([schedule_df,season_schedule_df],ignore_index=True)
        del season_schedule_df
    
    return schedule_df
 
def retrosheet_game_logs_team(first_season:int,last_season=None,game_type="regular",filter_out_seasons=True) -> pd.DataFrame():
    """
    Retrives the team-level stats for MLB games in a season, or range of seasons. 
    THIS DOES NOT GET PLAYER STATS! 
    Use retrosplits_game_logs_player() for player-level game stats.

    Args:
        first_season (int):
            Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

        last_season (int):
            Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from. 

        game_type (str):
            Optional parameter. By default, this is set to "regular", or to put it in another way, this function call will return only regular season games.

            The full list of supported keywards for game_type are as follows. Case does not matter (you can set game_type to "rEgUlAr", and the function call will still work):

                - "regular": Regular season games.
            
                - "asg": All-Star games.

                - "playoffs": Playoff games.

        filter_out_seasons (bool):
            If game_type is set to either "asg" or "playoffs", and filter_out_seasons is set to true, this function will filter out seasons that do not match the inputted season and/or the range of seasons. By default, this is set to True.

    Returns:
        A pandas dataframe containing team-level stats for MLB games.
    
    """
    game_log_df = pd.DataFrame()
    season_game_log_df = pd.DataFrame()
    try:
        last_season = int(last_season)
    except:
        last_season = None
        
    columns_list = [
            ## Game Info
            'date','game_num','day_of_week',
            'away_team','away_leauge','away_team_game_num',
            'home_team','home_leauge','home_team_game_num',
            ## Scores
            'away_team_score','home_team_score',
            ## Additional Game Info
            'game_length','day_night_indicator','completion_info','forfeit_info','protest_info',
            'park_id','attendance','time_of_game','away_line_score','home_line_score',
            ## Away Batting stats
            'away_AB','away_H','away_2B','away_3B','away_HR','away_RBI','away_SH','away_SF', 
            'away_HBP','away_BB','away_IBB','away_K','away_SB','away_CS','away_GDP','away_CI',
            'away_LOB',
            ## Away Pitching
            'away_pitchers_used','away_ER','away_team_ER','away_WP','away_BK', 
            ## Away Fielding
            'away_PO','away_A','away_E','away_PB','away_DP','away_TP', 
            ## Home Batting stats
            'home_AB','home_H','home_2B','home_3B','home_HR','home_RBI','home_SH','home_SF', 
            'home_HBP','home_BB','home_IBB','home_K','home_SB','home_CS','home_GDP','home_CI',
            'home_LOB',
            ## Home Pitching
            'home_pitchers_used','home_ER','home_team_ER','home_WP','home_BK', 
            ## Home Fielding
            'away_PO','home_A','home_E','home_PB','home_DP','home_TP', 
            ## Umpires
            'home_plate_umpire_id','home_plate_umpire_name','1B_umpire_id','1B_umpire_name',
            '2B_umpire_id','2B_umpire_name','3B_umpire_id','3B_umpire_name',
            'LF_umpire_id','LF_umpire_name','RF_umpire_id','RF_umpire_name',
            ## Managers
            'away_manager_id','away_manager_name','home_manager_id','home_manager_name',
            ## Winning/Losing/Saving pitchers
            'winning_pitcher_id','winning_pitcher_name',
            'losing_pitcher_id','losing_pitcher_name',
            'saving_pitcher_id','saving_pitcher_name',
            ## Winning Hit+RBI batter
            'game_winning_hitter_id','game_winning_hitter_name',
            ## Starting Pitchers
            'away_SP_id','away_SP_name','home_SP_id','home_SP_name',
            ## Away Team Batting Lineup
            'away_batter_01_id','away_batter_01_name','away_batter_01_position',
            'away_batter_02_id','away_batter_02_name','away_batter_02_position',
            'away_batter_03_id','away_batter_03_name','away_batter_03_position',
            'away_batter_04_id','away_batter_04_name','away_batter_04_position',
            'away_batter_05_id','away_batter_05_name','away_batter_05_position',
            'away_batter_06_id','away_batter_06_name','away_batter_06_position',
            'away_batter_07_id','away_batter_07_name','away_batter_07_position',
            'away_batter_08_id','away_batter_08_name','away_batter_08_position',
            'away_batter_09_id','away_batter_09_name','away_batter_09_position',
            ## Home Team Batting Lineup
            'home_batter_01_id','home_batter_01_name','home_batter_01_position',
            'home_batter_02_id','home_batter_02_name','home_batter_02_position',
            'home_batter_03_id','home_batter_03_name','home_batter_03_position',
            'home_batter_04_id','home_batter_04_name','home_batter_04_position',
            'home_batter_05_id','home_batter_05_name','home_batter_05_position',
            'home_batter_06_id','home_batter_06_name','home_batter_06_position',
            'home_batter_07_id','home_batter_07_name','home_batter_07_position',
            'home_batter_08_id','home_batter_08_name','home_batter_08_position',
            'home_batter_09_id','home_batter_09_name','home_batter_09_position',
            'additional_info','acquisition_info']
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

    if game_type.lower() == "regular" or game_type.lower() == "reg":
        for i in tqdm(range(first_season,last_season+1)):
            season = i
            print(season)
            
            game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/gamelog/GL{i}.TXT"
            
            resp = download(game_log_url)
            resp_str = StringIO(str(resp, 'UTF-8'))
            season_game_log_df = pd.read_csv(resp_str,sep=",")
            season_game_log_df.columns = columns_list
            season_game_log_df = season_game_log_df.astype({"date":"str"})
            season_game_log_df['season'] = season_game_log_df['date'].str[0:4]
            #game_log_df.astype({'season':'str'})
            game_log_df = pd.concat([game_log_df,season_game_log_df],ignore_index=True)
            del season_game_log_df
    elif game_type.lower() == "asg" or game_type.lower() == "all star" or game_type.lower() == "all-star":
        game_log_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/gamelog/GLAS.TXT"
            
        resp = download(game_log_url)
        resp_str = StringIO(str(resp, 'UTF-8'))
        game_log_df = pd.read_csv(resp_str,sep=",")
        game_log_df.columns = columns_list

        game_log_df = game_log_df.astype({"date":"str"})
        game_log_df['season'] = game_log_df['date'].str[0:4]
        game_log_df = game_log_df.astype({'season':'int32'})
        if filter_out_seasons == True:
            game_log_df = game_log_df[game_log_df.season >= first_season]
            game_log_df = game_log_df[game_log_df.season <= last_season]
        else:
            pass
    elif game_type.lower() == "playoffs" or game_type.lower() == "october baseball" or game_type.lower() == "post" or game_type.lower() == "postseason" or game_type.lower() == "october" or game_type.lower() == "november" or game_type.lower() == "november baseball":
        wildcard_round_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/gamelog/GLWC.TXT"
        divisional_round_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/gamelog/GLDV.TXT"
        championship_round_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/gamelog/GLLC.TXT"
        world_series_round_url = f"https://raw.githubusercontent.com/chadwickbureau/retrosheet/master/gamelog/GLWS.TXT"
        URL_list = [wildcard_round_url,divisional_round_url,championship_round_url,world_series_round_url]

        for i in tqdm(URL_list):
            resp = download(i)
            resp_str = StringIO(str(resp, 'UTF-8'))
            season_game_log_df = pd.read_csv(resp_str,sep=",")
            season_game_log_df.columns = columns_list
            game_log_df = pd.concat([game_log_df,season_game_log_df],ignore_index=True)
            del season_game_log_df
        game_log_df = game_log_df.astype({"date":"str"})
        game_log_df['season'] = game_log_df['date'].str[0:4]
        game_log_df = game_log_df.astype({'season':'int32'})
        if filter_out_seasons == True:
            game_log_df = game_log_df[game_log_df.season >= first_season]
            game_log_df = game_log_df[game_log_df.season <= last_season]
        else:
            pass

    return game_log_df
