from typing import Dict

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download


def nhl_api_pbp(game_id: int, **kwargs) -> Dict:
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
    # summary endpoint for pickcenter array
    summary_url = f"https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live?site=en_nhl"
    summary_resp = download(summary_url, **kwargs)
    summary = summary_resp.json()
    pbp_txt = {"datetime": summary.get("gameData").get("datetime")}
    pbp_txt["game"] = summary.get("gameData").get("game")
    pbp_txt["players"] = summary.get("gameData").get("players")
    pbp_txt["status"] = summary.get("gameData").get("status")
    pbp_txt["teams"] = summary.get("gameData").get("teams")
    pbp_txt["venues"] = summary.get("gameData").get("venues")
    pbp_txt["gameId"] = summary.get("gameData").get("gamePk")
    pbp_txt["gameLink"] = summary.get("gameData").get("link")
    return pbp_txt


def nhl_api_schedule(start_date: str, end_date: str, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """nhl_api_schedule() - Pull the schedule by start and end date. Data from API endpoints - `nhl/schedule`

    Args:
        start_date (str): Start date to pull the NHL API schedule.
        end_date (str): End date to pull the NHL API schedule.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the schedule for the requested seasons.

    Example:
        `nhl_sched_df = sportsdataverse.nhl.nhl_api_schedule(start_date=2021-10-23, end_date=2021-10-28)`
    """
    # summary endpoint for pickcenter array
    summary_url = "https://statsapi.web.nhl.com/api/v1/schedule"
    params = {"site": "en_nhl", "startDate": start_date, "endDate": end_date}
    summary_resp = download(summary_url, params=params, **kwargs)
    summary = summary_resp.json()
    pbp_txt = {"dates": summary.get("dates")}
    pbp_txt_games = pl.DataFrame()
    for date in pbp_txt["dates"]:
        game = pl.from_pandas(pd.json_normalize(date, record_path="games", meta=["date"]))
        pbp_txt_games = pl.concat([pbp_txt_games, game], how="vertical")
    return pbp_txt_games.to_pandas() if return_as_pandas else pbp_txt_games
