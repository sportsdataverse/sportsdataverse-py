## Script: mlb_retrosheet.py
## Author: Joseph Armstrong (armstjc)

import pandas as pd
from pandas import json_normalize
import json
from sportsdataverse.dl_utils import download
from datetime import datetime
from io import StringIO
import os

def retrosheet_people() -> pd.DataFrame():
    """
    Retrives the current BioFile.txt file from the 
    Retrosheet website, and then returns the current file as 
    a pandas dataframe. 

    Args:
        None
    
    Returns:
        A pandas Dataframe with the biographical information
        of various individuals who have played baseball.
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

def retrosheet_franchises() -> pd.DataFrame():
    """
    Retrives the current TEAMABR.txt file from the 
    Retrosheet website, and then returns the current file as 
    a pandas dataframe. 

    Args:
        None
    
    Returns:
        A pandas Dataframe with the biographical information
        of notable major leauge teams.
    """
    people_url = "https://www.retrosheet.org/TEAMABR.TXT"
    resp = download(people_url)
    resp_str = str(resp, 'UTF-8')
    resp_str = StringIO(str(resp, 'UTF-8'))
    fran_df = pd.read_csv(resp_str,sep=",",)
    fran_df.columns = ['fran_id','leauge','city','nickname','first_year','last_year']
    fran_df.dropna()

    return fran_df

def retrosheet_transactions() -> pd.DataFrame():
    """
    TODO: Add retrosheet ablilities to sportsdataverse/mlbfastr-data
    https://www.retrosheet.org/transactions/index.html
    """
    transactions_df = pd.DataFrame()
    transactions_url = "https://www.retrosheet.org/transactions/index.html"


    print('Feature needs further development. \nThis function is a placeholder.')
    return transactions_df

def retrosheet_ballparks() -> pd.DataFrame():
    """
    Retrives the current TEAMABR.txt file from the 
    Retrosheet website, and then returns the current file as 
    a pandas dataframe. 

    Args:
        None
    
    Returns:
        A pandas Dataframe with the biographical information
        of notable major leauge teams.
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
    people_url = "https://www.retrosheet.org/Ejecdata.txt"
    resp = download(people_url)
    resp_str = str(resp, 'UTF-8')
    resp_str = StringIO(str(resp, 'UTF-8'))
    ejections_df = pd.read_csv(resp_str,sep=",")
    ejections_df.columns = ['game_id','date','dh','ejectee','ejectee_name','team','job','umpire','umpire_name','inning','reason']
    return ejections_df