from typing import Dict
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os
import json
import re
from typing import List, Callable, Iterator, Union, Optional, Dict
from sportsdataverse.dl_utils import download, flatten_json_iterative, key_check


def espn_nhl_pbp(game_id: int, raw = False) -> Dict:
    """espn_nhl_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary`

    Args:
        game_id (int): Unique game_id, can be obtained from nhl_schedule().

    Returns:
        Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
         "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "pickcenter", "againstTheSpread",
         "odds", "onIce", "gameInfo", "season"

    Example:
        `nhl_df = sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153)`
    """
    # play by play
    pbp_txt = {}
    # summary endpoint for pickcenter array
    summary_url = "http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event={}".format(game_id)
    summary_resp = download(summary_url)
    summary = json.loads(summary_resp)
    for k in ['plays', 'seasonseries', 'videos', 'broadcasts', 'pickcenter', 'onIce', 'againstTheSpread', 'odds', 'winprobability', 'teamInfo', 'espnWP', 'leaders']:
        pbp_txt[k]=key_check(obj=summary, key = k, replacement = np.array([]))
    for k in ['boxscore','format', 'gameInfo', 'article', 'header', 'season', 'standings']:
        pbp_txt[k] = key_check(obj=summary, key = k, replacement = {})
    for k in ['news','shop']:
        if k in pbp_txt.keys():
            del pbp_txt[k]
    incoming_keys_expected = ['boxscore', 'format', 'gameInfo', 'leaders', 'seasonseries', 'broadcasts',
                              'pickcenter', 'againstTheSpread', 'odds', 'winprobability',
                              'header', 'onIce', 'article', 'videos', 'plays', 'standings',
                              'teamInfo', 'espnWP', 'season', 'timeouts']
    if raw == True:
        # reorder keys in raw format, appending empty keys which are defined later to the end
        pbp_json = {}
        for k in incoming_keys_expected:
            if k in pbp_txt.keys():
                pbp_json[k] = pbp_txt[k]
            else:
                pbp_json[k] = {}
        return pbp_json
    pbp_json = helper_nhl_pbp(game_id, pbp_txt)
    return pbp_json

def nhl_pbp_disk(game_id, path_to_json):
    with open(os.path.join(path_to_json, "{}.json".format(game_id))) as json_file:
        pbp_txt = json.load(json_file)
    return pbp_txt

def helper_nhl_pbp(game_id, pbp_txt):

    gameSpread, homeFavorite, gameSpreadAvailable = helper_nhl_pickcenter(pbp_txt)
    pbp_txt['teamInfo'] = pbp_txt['header']['competitions'][0]
    pbp_txt['season'] = pbp_txt['header']['season']
    pbp_txt['playByPlaySource'] = pbp_txt['header']['competitions'][0]['playByPlaySource']
    # Home and Away identification variables
    homeTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['id'])
    awayTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['id'])
    homeTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['name'])
    awayTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['name'])
    homeTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['location'])
    awayTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['location'])
    homeTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['abbreviation'])
    awayTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['abbreviation'])
    homeTeamNameAlt = re.sub("Stat(.+)", "St", str(homeTeamName))
    awayTeamNameAlt = re.sub("Stat(.+)", "St", str(awayTeamName))

    if (pbp_txt['playByPlaySource'] != "none") & (len(pbp_txt['plays'])>1):
        helper_nhl_pbp_features(game_id, pbp_txt, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt, gameSpread, homeFavorite, gameSpreadAvailable)
    else:
        pbp_txt['plays'] = pd.DataFrame()
    pbp_txt['plays'] = pbp_txt['plays'].replace({np.nan: None})
    pbp_json = {
        "gameId": game_id,
        "plays" : pbp_txt['plays'].to_dict(orient='records'),
        "boxscore" : pbp_txt['boxscore'],
        "header" : pbp_txt['header'],
        "format": pbp_txt['format'],
        "broadcasts" : np.array(pbp_txt['broadcasts']).tolist(),
        "videos" : np.array(pbp_txt['videos']).tolist(),
        "playByPlaySource": pbp_txt['playByPlaySource'],
        "standings" : pbp_txt['standings'],
        "leaders" : np.array(pbp_txt['leaders']).tolist(),
        "seasonseries" : np.array(pbp_txt['seasonseries']).tolist(),
        "pickcenter" : np.array(pbp_txt['pickcenter']).tolist(),
        "againstTheSpread" : np.array(pbp_txt['againstTheSpread']).tolist(),
        "odds" : np.array(pbp_txt['odds']).tolist(),
        "onIce": np.array(pbp_txt['onIce']).tolist(),
        "gameInfo" : pbp_txt['gameInfo'],
        "teamInfo" : np.array(pbp_txt['teamInfo']).tolist(),
        "season" : np.array(pbp_txt['season']).tolist()
    }
    return pbp_json

def helper_nhl_pickcenter(pbp_txt):
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
    return gameSpread,homeFavorite,gameSpreadAvailable

def helper_nhl_pbp_features(game_id, pbp_txt, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt, gameSpread, homeFavorite, gameSpreadAvailable):
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
    pbp_txt['plays']['lag_period.number'] = pbp_txt['plays']['period.number'].shift(1)
    pbp_txt['plays']['lead_period.number'] = pbp_txt['plays']['period.number'].shift(-1)
    pbp_txt['plays']['start.period_seconds_remaining'] = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
    pbp_txt['plays']['start.game_seconds_remaining'] = np.select(
            [
                pbp_txt['plays']['period.number'] == 1,
                pbp_txt['plays']['period.number'] == 2,
                pbp_txt['plays']['period.number'] == 3
            ],
            [
                2400 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                1200 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            ], default = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
        )
        # Pos Team - Start and End Id
    pbp_txt['plays']['game_play_number'] = np.arange(len(pbp_txt['plays']))+1
    pbp_txt['plays']['text'] = pbp_txt['plays']['text'].astype(str)
    pbp_txt['plays']['id'] = pbp_txt['plays']['id'].apply(lambda x: int(x))
    pbp_txt['plays']['end.period_seconds_remaining'] = pbp_txt['plays']['start.period_seconds_remaining'].shift(1)
    pbp_txt['plays']['end.game_seconds_remaining'] = pbp_txt['plays']['start.game_seconds_remaining'].shift(1)
    pbp_txt['plays']['end.period_seconds_remaining'] = np.select(
            [
                (pbp_txt['plays']['game_play_number'] == 1)|
                ((pbp_txt['plays']['period.number'] == 2) & (pbp_txt['plays']['lag_period.number'] == 1))|
                ((pbp_txt['plays']['period.number'] == 3) & (pbp_txt['plays']['lag_period.number'] == 2))
            ],
            [
                1200
            ], default = pbp_txt['plays']['end.period_seconds_remaining']
        )
    pbp_txt['plays']['end.game_seconds_remaining'] = np.select(
            [
                (pbp_txt['plays']['game_play_number'] == 1),
                ((pbp_txt['plays']['period.number'] == 2) & (pbp_txt['plays']['lag_period.number'] == 1)),
                ((pbp_txt['plays']['period.number'] == 3) & (pbp_txt['plays']['lag_period.number'] == 2))
            ],
            [
                3600,
                2400,
                1200
            ], default = pbp_txt['plays']['end.game_seconds_remaining']
        )

    pbp_txt['plays']['period'] = pbp_txt['plays']['period.number']

    del pbp_txt['plays']['clock.mm']