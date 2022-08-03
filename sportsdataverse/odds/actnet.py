import pandas as pd
import numpy as np
import requests
import os
import json
import re
from typing import List, Callable, Iterator, Union, Optional, Dict
from sportsdataverse.dl_utils import download, flatten_json_iterative, key_check
from sportsdataverse.odds.actnet_headers_gen import actnet_headers_gen

def actnet_scoreboard(league = "all", book_ids = None, date = None, headers = None, raw = False) -> Dict:
    """actnet_scoreboard()
        Args:
            league (string): League name
            book_ids (list): List of book_ids to filter by
            date (string): Date to filter by

        Returns:
            Dict: Dictionary of odds and scoreboard data with keys

        Example:
            `odds_scoreboard_df = sportsdataverse.odds.actnet_scoreboard(league = 'all', date = 20220802)`
    """
    if league is None:
        league_param = "all"
    else:
        league_param = league
    if book_ids is None:
        book_ids = "15,30,76,75,123,69,68,972,71,247,79"
    if headers is None:
        headers = actnet_headers_gen()
    pbp_txt = {}
    summary_url = f"https://api.actionnetwork.com/web/v1/scoreboard/{league_param}?bookIds={book_ids}&date={date}"
    summary_resp = requests.get(summary_url, headers = headers)
    summary = pd.json_normalize(summary_resp.json())
    if league_param == "all":
        incoming_keys_expected = ['all_games']
        dict_keys_expected = []
        array_keys_expected = ['all_games']
    else:
        incoming_keys_expected = [
            'league', 'games'
        ]
        dict_keys_expected = [
            'league'
        ]
        array_keys_expected = [
            'games'
        ]
    if raw == True:
        pbp_json = {}
        for k in incoming_keys_expected:
            if k in summary.keys():
                pbp_json[k] = summary.get(f"{k}")
            else:
                if k in dict_keys_expected:
                    pbp_json[k] = {}
                else:
                    pbp_json[k] = []
        return pbp_json

    for k in incoming_keys_expected:
        if k in summary.keys():
            pbp_txt[k] = summary.get(f"{k}")
        else:
            if k in dict_keys_expected:
                pbp_txt[k] = {}
            else:
                pbp_txt[k] = []

    pbp_json = pbp_txt
    return pbp_json