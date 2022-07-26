from typing import Dict
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import re
import json
from typing import List, Callable, Iterator, Union, Optional, Dict
from sportsdataverse.dl_utils import download, flatten_json_iterative, key_check


def nhl_api_pbp(game_id: int) -> Dict:
    """nhl_api_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary`

    Args:
        game_id (int): Unique game_id, can be obtained from nhl_schedule().

    Returns:
        Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
         "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "pickcenter", "againstTheSpread",
         "odds", "onIce", "gameInfo", "season"

    Example:
        `nhl_df = sportsdataverse.nhl.nhl_api_pbp(game_id=2021020079)`
    """
    # play by play
    pbp_txt = {}
    # summary endpoint for pickcenter array
    summary_url = "https://statsapi.web.nhl.com/api/v1/game/{}/feed/live?site=en_nhl".format(game_id)
    summary_resp = download(summary_url)
    summary = json.loads(summary_resp)
    pbp_txt['datetime'] = summary.get("gameData").get("datetime")
    pbp_txt['game'] = summary.get("gameData").get("game")
    pbp_txt['players'] = summary.get("gameData").get("players")
    pbp_txt['status'] = summary.get("gameData").get("status")
    pbp_txt['teams'] = summary.get("gameData").get("teams")
    pbp_txt['venues'] = summary.get("gameData").get("venues")
    pbp_txt['gameId'] = summary.get("gameData").get("gamePk")
    pbp_txt['gameLink'] = summary.get("gameData").get("link")
    return pbp_txt


def nhl_api_schedule(start_date: str, end_date: str) -> Dict:
    """nhl_api_schedule() - Pull the game by id. Data from API endpoints - `nhl/schedule`

    Args:
        game_id (int): Unique game_id, can be obtained from nhl_schedule().

    Returns:
        Dict:

    Example:
        `nhl_sched_df = sportsdataverse.nhl.nhl_api_schedule(start_date=2021-10-23, end_date=2021-10-28)`
    """
    # play by play
    pbp_txt = {}
    # summary endpoint for pickcenter array
    summary_url = "https://statsapi.web.nhl.com/api/v1/schedule?site=en_nhl&startDate={}&endDate={}".format(start_date, end_date)
    summary_resp = download(summary_url)
    summary = json.loads(summary_resp)
    pbp_txt['dates'] = summary.get("dates")
    pbp_txt_games = pd.DataFrame()
    for date in pbp_txt['dates']:
        game = pd.json_normalize(date, record_path="games", meta=["date"])
        pbp_txt_games = pd.concat([pbp_txt_games, game], ignore_index=True)
    return pbp_txt_games