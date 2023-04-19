import json
import requests
from typing import List, Callable, Iterator, Union, Optional, Dict
from sportsdataverse.dl_utils import download, flatten_json_iterative, key_check

def nfl_token_gen():
    url = "https://api.nfl.com/v1/reroute"

    # TODO: resolve if DNT or x-domain-id are necessary.  pulled them from chrome inspector
    payload = 'grant_type=client_credentials'
    headers = {
      'DNT': '1',
      'x-domain-id': '100',
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data = payload)

    access_token = json.loads(response.content)['access_token']
    return access_token

def nfl_headers_gen():
    token = nfl_token_gen()
    NFL_HEADERS = {
        "Host": "api.nfl.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nfl.com/",
        "authorization": f"Bearer {token}",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-User": "?1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    return NFL_HEADERS

def nfl_game_details(game_id=None, headers=None, raw=False) -> Dict:
    """nfl_game_details()
        Args:
            game_id (int): Game ID
        Returns:
            Dict: Dictionary of odds and props data with keys
        Example:
            `nfl_df = nfl_game_details(
            game_id = '7ae87c4c-d24c-11ec-b23d-d15a91047884'
            )`
    """
    if headers is None:
        headers = nfl_headers_gen()
    pbp_txt = {}
    summary_url = f"https://api.nfl.com/experience/v1/gamedetails/{game_id}"
    summary_resp = requests.get(summary_url, headers=headers)
    summary = summary_resp.json()

    incoming_keys_expected = [
        'attendance',
        'distance',
        'down',
        'gameClock',
        'goalToGo',
        'homePointsOvertime',
        'homePointsQ1',
        'homePointsQ2',
        'homePointsQ3',
        'homePointsQ4',
        'homePointsTotal',
        'homeTeam',
        'homeTimeoutsRemaining',
        'homeTimeoutsUsed',
        'id',
        'offset',
        'period',
        'phase',
        'playReview',
        'possessionTeam',
        'quarter',
        'redzone',
        'scoringSummaries',
        'stadium',
        'startTime',
        'totalOffset',
        'visitorPointsOvertime',
        'visitorPointsQ1',
        'visitorPointsQ2',
        'visitorPointsQ3',
        'visitorPointsQ4',
        'visitorPointsTotal',
        'visitorTeam',
        'visitorTimeoutsRemaining',
        'visitorTimeoutsUsed',
        'weather',
        'yardLine',
        'yardsToGo',
        'drives',
        'plays'
    ]
    dict_keys_expected = [
        'homeTeam',
        'possessionTeam',
        'visitorTeam',
        'weather'
    ]
    array_keys_expected = [
        'scoringSummaries',
        'drives',
        'plays'
    ]
    if raw == True:
        return summary

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


def nfl_game_schedule(season=2021,
                      season_type="REG",
                      week=1,
                      headers=None,
                      raw=False) -> Dict:
    """nfl_game_schedule()
        Args:
            season (int): season
            season_type (str): season type - REG, POST
            week (int): week
        Returns:
            Dict: Dictionary of odds and props data with keys
        Example:
            `nfl_df = nfl_game_schedule(
                season = 2021, seasonType='REG', week=1
            )`
    """
    if headers is None:
        headers = nfl_headers_gen()
    params = {
        "season": season,
        "seasonType": season_type,
        "week": week
    }
    pbp_txt = {}
    summary_url = f"https://api.nfl.com/experience/v1/games"
    summary_resp = requests.get(summary_url,
                                headers=headers,
                               params=params)
    summary = summary_resp.json()

    incoming_keys_expected = [
        'id', 'homeTeam', 'awayTeam', 'category', 'date', 'time', 'broadcastInfo', 'neutralSite', 'venue', 'season', 'seasonType', 'status', 'week', 'weekType', 'externalIds', 'ticketUrl', 'ticketVendors', 'detail'
    ]
    dict_keys_expected = [
        'homeTeam',
        'possessionTeam',
        'visitorTeam',
        'weather'
    ]
    array_keys_expected = [
        'scoringSummaries',
        'drives',
        'plays'
    ]
    if raw == True:
        return summary

    # for k in incoming_keys_expected:
    #     if k in summary.keys():
    #         pbp_txt[k] = summary.get(f"{k}")
    #     else:
    #         if k in dict_keys_expected:
    #             pbp_txt[k] = {}
    #         else:
    #             pbp_txt[k] = []

    pbp_json = summary
    return pbp_json
