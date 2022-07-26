from typing import Dict
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os
import json
import re
from typing import List, Callable, Iterator, Union, Optional, Dict
from sportsdataverse.dl_utils import download, flatten_json_iterative, key_check

def espn_nba_pbp(game_id: int, raw = False) -> Dict:
    """espn_nba_pbp() - Pull the game by id - Data from API endpoints - `nba/playbyplay`, `nba/summary`

        Args:
            game_id (int): Unique game_id, can be obtained from nba_schedule().

        Returns:
            Dict: Dictionary of game data with keys - "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts",
             "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "timeouts", "pickcenter", "againstTheSpread",
             "odds", "predictor", "espnWP", "gameInfo", "season"

        Example:
            `nba_df = sportsdataverse.nba.espn_nba_pbp(game_id=401307514)`
    """
    # play by play
    pbp_txt = {}
    pbp_txt['timeouts'] = {}
    # summary endpoint for pickcenter array
    summary_url = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={}".format(game_id)
    summary_resp = download(summary_url)
    summary = json.loads(summary_resp)

    incoming_keys_expected = [
        'boxscore', 'format', 'gameInfo', 'leaders', 'seasonseries',
        'broadcasts', 'predictor', 'pickcenter', 'againstTheSpread',
        'odds', 'winprobability', 'header', 'plays',
        'article', 'videos', 'standings',
        'teamInfo', 'espnWP', 'season', 'timeouts'
    ]
    dict_keys_expected = [
        'boxscore','format', 'gameInfo', 'predictor',
        'article', 'header', 'season', 'standings'
    ]
    array_keys_expected = [
        'plays', 'seasonseries', 'videos', 'broadcasts', 'pickcenter',
        'againstTheSpread', 'odds', 'winprobability', 'teamInfo', 'espnWP',
        'leaders'
    ]
    if raw == True:
        # reorder keys in raw format
        pbp_json = {}
        for k in incoming_keys_expected:
            if k in summary.keys():
                pbp_json[k] = summary[k]
            else:
                if k in dict_keys_expected:
                    pbp_json[k] = {}
                else:
                    pbp_json[k] = []
        return pbp_json

    for k in incoming_keys_expected:
        if k in summary.keys():
            pbp_txt[k] = summary[k]
        else:
            if k in dict_keys_expected:
                pbp_txt[k] = {}
            else:
                pbp_txt[k] = []

    for k in ['news','shop']:
        pbp_txt.pop('{}'.format(k), None)
    pbp_json = helper_nba_pbp(game_id, pbp_txt)
    return pbp_json

def nba_pbp_disk(game_id, path_to_json):
    with open(os.path.join(path_to_json, "{}.json".format(game_id))) as json_file:
        pbp_txt = json.load(json_file)
    return pbp_txt

def helper_nba_pbp(game_id, pbp_txt):
    gameSpread, overUnder, homeFavorite, gameSpreadAvailable = helper_nba_pickcenter(pbp_txt)
    pbp_txt['timeouts'] = {}
    pbp_txt['teamInfo'] = pbp_txt['header']['competitions'][0]
    pbp_txt['season'] = pbp_txt['header']['season']
    pbp_txt['playByPlaySource'] = pbp_txt['header']['competitions'][0]['playByPlaySource']
    pbp_txt['boxscoreSource'] = pbp_txt['header']['competitions'][0]['boxscoreSource']
    pbp_txt['gameSpreadAvailable'] = gameSpreadAvailable
    pbp_txt['gameSpread'] = gameSpread
    pbp_txt["homeFavorite"] = homeFavorite
    pbp_txt["homeTeamSpread"] = np.where(
        homeFavorite == True, abs(gameSpread), -1 * abs(gameSpread)
    )
    pbp_txt["overUnder"] = float(overUnder)
    # Home and Away identification variables
    if pbp_txt['header']['competitions'][0]['competitors'][0]['homeAway']=='home':
        pbp_txt['header']['competitions'][0]['home'] = pbp_txt['header']['competitions'][0]['competitors'][0]['team']
        homeTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['id'])
        homeTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['name'])
        homeTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['location'])
        homeTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['abbreviation'])
        homeTeamNameAlt = re.sub("Stat(.+)", "St", str(homeTeamName))
        pbp_txt['header']['competitions'][0]['away'] = pbp_txt['header']['competitions'][0]['competitors'][1]['team']
        awayTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['id'])
        awayTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['name'])
        awayTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['location'])
        awayTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['abbreviation'])
        awayTeamNameAlt = re.sub("Stat(.+)", "St", str(awayTeamName))
    else:
        pbp_txt['header']['competitions'][0]['away'] = pbp_txt['header']['competitions'][0]['competitors'][0]['team']
        awayTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['id'])
        awayTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['name'])
        awayTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['location'])
        awayTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['abbreviation'])
        awayTeamNameAlt = re.sub("Stat(.+)", "St", str(awayTeamName))
        pbp_txt['header']['competitions'][0]['home'] = pbp_txt['header']['competitions'][0]['competitors'][1]['team']
        homeTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['id'])
        homeTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['name'])
        homeTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['location'])
        homeTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['abbreviation'])
        homeTeamNameAlt = re.sub("Stat(.+)", "St", str(homeTeamName))
    if (pbp_txt['playByPlaySource'] != "none") & (len(pbp_txt['plays'])>1):
        helper_nba_pbp_features(game_id, pbp_txt, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt, gameSpread, homeFavorite, gameSpreadAvailable)
    else:
        pbp_txt['plays'] = pd.DataFrame()
        pbp_txt['timeouts'] = {}
        pbp_txt['timeouts'][homeTeamId] = {"1": [], "2": []}
        pbp_txt['timeouts'][awayTeamId] = {"1": [], "2": []}
    pbp_txt['plays'] = pbp_txt['plays'].replace({np.nan: None})
    pbp_json = {
        "gameId": game_id,
        "plays" : pbp_txt['plays'].to_dict(orient='records'),
        "winprobability" : np.array(pbp_txt['winprobability']).tolist(),
        "boxscore" : pbp_txt['boxscore'],
        "header" : pbp_txt['header'],
        "format": pbp_txt['format'],
        "broadcasts" : np.array(pbp_txt['broadcasts']).tolist(),
        "videos" : np.array(pbp_txt['videos']).tolist(),
        "playByPlaySource": pbp_txt['playByPlaySource'],
        "standings" : pbp_txt['standings'],
        "article" : pbp_txt['article'],
        "leaders" : np.array(pbp_txt['leaders']).tolist(),
        "seasonseries" : np.array(pbp_txt['seasonseries']).tolist(),
        "timeouts" : pbp_txt['timeouts'],
        "pickcenter" : np.array(pbp_txt['pickcenter']).tolist(),
        "againstTheSpread" : np.array(pbp_txt['againstTheSpread']).tolist(),
        "odds" : np.array(pbp_txt['odds']).tolist(),
        "predictor" : pbp_txt['predictor'],
        "espnWP" : np.array(pbp_txt['espnWP']).tolist(),
        "gameInfo" : pbp_txt['gameInfo'],
        "teamInfo" : np.array(pbp_txt['teamInfo']).tolist(),
        "season" : np.array(pbp_txt['season']).tolist()
    }
    return pbp_json

def helper_nba_pbp_features(game_id, pbp_txt, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt, gameSpread, homeFavorite, gameSpreadAvailable):
    pbp_txt['plays_mod'] = []
    for play in pbp_txt['plays']:
        p = flatten_json_iterative(play)
        pbp_txt['plays_mod'].append(p)
    pbp_txt['plays'] = pd.json_normalize(pbp_txt,'plays_mod')
    pbp_txt['plays']['season'] = pbp_txt['season']['year']
    pbp_txt['plays']['seasonType'] = pbp_txt['season']['type']
    pbp_txt['plays']["awayTeamId"] = awayTeamId
    pbp_txt['plays']["awayTeamName"] = str(awayTeamName)
    pbp_txt['plays']["awayTeamMascot"] = str(awayTeamMascot)
    pbp_txt['plays']["awayTeamAbbrev"] = str(awayTeamAbbrev)
    pbp_txt['plays']["awayTeamNameAlt"] = str(awayTeamNameAlt)
    pbp_txt['plays']["homeTeamId"] = homeTeamId
    pbp_txt['plays']["homeTeamName"] = str(homeTeamName)
    pbp_txt['plays']["homeTeamMascot"] = str(homeTeamMascot)
    pbp_txt['plays']["homeTeamAbbrev"] = str(homeTeamAbbrev)
    pbp_txt['plays']["homeTeamNameAlt"] = str(homeTeamNameAlt)
        # Spread definition
    pbp_txt['plays']["homeTeamSpread"] = 2.5
    pbp_txt['plays']["gameSpread"] = abs(gameSpread)
    pbp_txt['plays']["homeTeamSpread"] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
    pbp_txt['homeTeamSpread'] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
    pbp_txt['plays']["homeFavorite"] = homeFavorite
    pbp_txt['plays']["gameSpread"] = gameSpread
    pbp_txt['plays']["gameSpreadAvailable"] = gameSpreadAvailable
    pbp_txt['plays'] = pbp_txt['plays'].to_dict(orient='records')
    pbp_txt['plays'] = pd.DataFrame(pbp_txt['plays'])
    pbp_txt['plays']['season'] = pbp_txt['header']['season']['year']
    pbp_txt['plays']['seasonType'] = pbp_txt['header']['season']['type']
    pbp_txt['plays']['game_id'] = int(game_id)
    pbp_txt['plays']["homeTeamId"] = homeTeamId
    pbp_txt['plays']["awayTeamId"] = awayTeamId
    pbp_txt['plays']["homeTeamName"] = str(homeTeamName)
    pbp_txt['plays']["awayTeamName"] = str(awayTeamName)
    pbp_txt['plays']["homeTeamMascot"] = str(homeTeamMascot)
    pbp_txt['plays']["awayTeamMascot"] = str(awayTeamMascot)
    pbp_txt['plays']["homeTeamAbbrev"] = str(homeTeamAbbrev)
    pbp_txt['plays']["awayTeamAbbrev"] = str(awayTeamAbbrev)
    pbp_txt['plays']["homeTeamNameAlt"] = str(homeTeamNameAlt)
    pbp_txt['plays']["awayTeamNameAlt"] = str(awayTeamNameAlt)
    pbp_txt['plays']['period.number'] = pbp_txt['plays']['period.number'].apply(lambda x: int(x))
    pbp_txt['plays']['qtr'] = pbp_txt['plays']['period.number'].apply(lambda x: int(x))


    pbp_txt['plays']["homeTeamSpread"] = 2.5
    if len(pbp_txt['pickcenter']) > 1:
        if 'spread' in pbp_txt['pickcenter'][1].keys():
            gameSpread =  pbp_txt['pickcenter'][1]['spread']
            homeFavorite = pbp_txt['pickcenter'][1]['homeTeamOdds']['favorite']
            gameSpreadAvailable = True
        else:
            gameSpread =  pbp_txt['pickcenter'][0]['spread']
            homeFavorite = pbp_txt['pickcenter'][0]['homeTeamOdds']['favorite']
            gameSpreadAvailable = True

    else:
        gameSpread = 2.5
        homeFavorite = True
        gameSpreadAvailable = False
    pbp_txt['plays']["gameSpread"] = abs(gameSpread)
    pbp_txt['plays']["gameSpreadAvailable"] = gameSpreadAvailable
    pbp_txt['plays']["homeTeamSpread"] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
    pbp_txt['homeTeamSpread'] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
    pbp_txt['plays']["homeFavorite"] = homeFavorite
    pbp_txt['plays']["gameSpread"] = gameSpread
    pbp_txt['plays']["homeFavorite"] = homeFavorite

        #----- Time ---------------

    pbp_txt['plays']['clock.displayValue'] = np.select(
            [
                pbp_txt['plays']['clock.displayValue'].str.contains(":") == False
            ],
            [
                "0:" + pbp_txt['plays']['clock.displayValue'].apply(lambda x: str(x))
            ], default = pbp_txt['plays']['clock.displayValue']
        )
    pbp_txt['plays']['time'] = pbp_txt['plays']['clock.displayValue']
    pbp_txt['plays']['clock.mm'] = pbp_txt['plays']['clock.displayValue'].str.split(pat=':')
    pbp_txt['plays'][['clock.minutes','clock.seconds']] = pbp_txt['plays']['clock.mm'].to_list()
    pbp_txt['plays']['clock.minutes'] = pbp_txt['plays']['clock.minutes'].apply(lambda x: int(x))

    pbp_txt['plays']['clock.seconds'] = pbp_txt['plays']['clock.seconds'].apply(lambda x: float(x))
        # pbp_txt['plays']['clock.mm'] = pbp_txt['plays']['clock.displayValue'].apply(lambda x: datetime.strptime(str(x),'%M:%S'))
    pbp_txt['plays']['half'] = np.where(pbp_txt['plays']['qtr'] <= 2, "1","2")
    pbp_txt['plays']['game_half'] = np.where(pbp_txt['plays']['qtr'] <= 2, "1","2")
    pbp_txt['plays']['lag_qtr'] = pbp_txt['plays']['qtr'].shift(1)
    pbp_txt['plays']['lead_qtr'] = pbp_txt['plays']['qtr'].shift(-1)
    pbp_txt['plays']['lag_game_half'] = pbp_txt['plays']['game_half'].shift(1)
    pbp_txt['plays']['lead_game_half'] = pbp_txt['plays']['game_half'].shift(-1)
    pbp_txt['plays']['start.quarter_seconds_remaining'] = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
    pbp_txt['plays']['start.half_seconds_remaining'] = np.where(
            pbp_txt['plays']['qtr'].isin([1,3]),
            600 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
            60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
        )
    pbp_txt['plays']['start.game_seconds_remaining'] = np.select(
            [
                pbp_txt['plays']['qtr'] == 1,
                pbp_txt['plays']['qtr'] == 2,
                pbp_txt['plays']['qtr'] == 3,
                pbp_txt['plays']['qtr'] == 4
            ],
            [
                1800 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                1200 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                600 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            ], default = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
        )
        # Pos Team - Start and End Id
    pbp_txt['plays']['game_play_number'] = np.arange(len(pbp_txt['plays']))+1
    pbp_txt['plays']['text'] = pbp_txt['plays']['text'].astype(str)
    pbp_txt['plays']['id'] = pbp_txt['plays']['id'].apply(lambda x: int(x))
    pbp_txt['plays']['end.quarter_seconds_remaining'] = pbp_txt['plays']['start.quarter_seconds_remaining'].shift(1)
    pbp_txt['plays']['end.half_seconds_remaining'] = pbp_txt['plays']['start.half_seconds_remaining'].shift(1)
    pbp_txt['plays']['end.game_seconds_remaining'] = pbp_txt['plays']['start.game_seconds_remaining'].shift(1)
    pbp_txt['plays']['end.quarter_seconds_remaining'] = np.select(
            [
                (pbp_txt['plays']['game_play_number'] == 1)|
                ((pbp_txt['plays']['qtr'] == 2) & (pbp_txt['plays']['lag_qtr'] == 1))|
                ((pbp_txt['plays']['qtr'] == 3) & (pbp_txt['plays']['lag_qtr'] == 2))|
                ((pbp_txt['plays']['qtr'] == 4) & (pbp_txt['plays']['lag_qtr'] == 3))
            ],
            [
                600
            ], default = pbp_txt['plays']['end.quarter_seconds_remaining']
        )
    pbp_txt['plays']['end.half_seconds_remaining'] = np.select(
            [
                (pbp_txt['plays']['game_play_number'] == 1)|
                ((pbp_txt['plays']['game_half'] == "2") & (pbp_txt['plays']['lag_game_half'] == "1"))
            ],
            [
                1200
            ], default = pbp_txt['plays']['end.half_seconds_remaining']
        )
    pbp_txt['plays']['end.game_seconds_remaining'] = np.select(
            [
                (pbp_txt['plays']['game_play_number'] == 1),
                ((pbp_txt['plays']['game_half'] == "2") & (pbp_txt['plays']['lag_game_half'] == "1"))
            ],
            [
                2400,
                1200
            ], default = pbp_txt['plays']['end.game_seconds_remaining']
        )

    pbp_txt['plays']['period'] = pbp_txt['plays']['qtr']

    del pbp_txt['plays']['clock.mm']

def helper_nba_pickcenter(pbp_txt):
    # Spread definition
    if len(pbp_txt["pickcenter"]) > 1:
        homeFavorite = pbp_txt["pickcenter"][0]["homeTeamOdds"]["favorite"]
        if "spread" in pbp_txt["pickcenter"][1].keys():
            gameSpread = pbp_txt["pickcenter"][1]["spread"]
            overUnder = pbp_txt["pickcenter"][1]["overUnder"]
            gameSpreadAvailable = True
        else:
            gameSpread = pbp_txt["pickcenter"][0]["spread"]
            overUnder = pbp_txt["pickcenter"][0]["overUnder"]
            gameSpreadAvailable = True
        # self.logger.info(f"Spread: {gameSpread}, home Favorite: {homeFavorite}, ou: {overUnder}")
    else:
        gameSpread = 2.5
        overUnder = 190.5
        homeFavorite = True
        gameSpreadAvailable = False
    return gameSpread, overUnder, homeFavorite, gameSpreadAvailable