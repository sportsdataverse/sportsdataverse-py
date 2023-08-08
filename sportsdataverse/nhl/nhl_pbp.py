import json
import os
import re
from typing import Dict

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, flatten_json_iterative, key_check


def espn_nhl_pbp(game_id: int, raw=False, **kwargs) -> Dict:
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
    pbp_txt = {}
    summary_url = f"http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event={game_id}"
    summary_resp = download(summary_url, **kwargs)
    summary = summary_resp.json()
    for k in [
        "plays",
        "seasonseries",
        "videos",
        "broadcasts",
        "pickcenter",
        "onIce",
        "againstTheSpread",
        "odds",
        "winprobability",
        "teamInfo",
        "espnWP",
        "leaders",
    ]:
        pbp_txt[k] = key_check(obj=summary, key=k, replacement=np.array([]))
    for k in ["boxscore", "format", "gameInfo", "article", "header", "season", "standings"]:
        pbp_txt[k] = key_check(obj=summary, key=k, replacement={})
    for k in ["news", "shop"]:
        if k in pbp_txt.keys():
            del pbp_txt[k]
    incoming_keys_expected = [
        "boxscore",
        "format",
        "gameInfo",
        "leaders",
        "seasonseries",
        "broadcasts",
        "pickcenter",
        "againstTheSpread",
        "odds",
        "winprobability",
        "header",
        "onIce",
        "article",
        "videos",
        "plays",
        "standings",
        "teamInfo",
        "espnWP",
        "season",
        "timeouts",
    ]
    if raw == True:
        # reorder keys in raw format, appending empty keys which are defined later to the end
        pbp_json = {}
        for k in incoming_keys_expected:
            if k in pbp_txt.keys():
                pbp_json[k] = pbp_txt[k]
            else:
                pbp_json[k] = {}
        return pbp_json
    return helper_nhl_pbp(game_id, pbp_txt)


def nhl_pbp_disk(game_id, path_to_json):
    with open(os.path.join(path_to_json, f"{game_id}.json")) as json_file:
        pbp_txt = json.load(json_file)
    return pbp_txt


def helper_nhl_pbp(game_id, pbp_txt):
    init = helper_nhl_pickcenter(pbp_txt)
    pbp_txt, init = helper_nhl_game_data(pbp_txt, init)

    if (pbp_txt["playByPlaySource"] != "none") & (len(pbp_txt["plays"]) > 1):
        pbp_txt = helper_nhl_pbp_features(game_id, pbp_txt, init)
    else:
        pbp_txt["plays"] = pl.DataFrame()
    return {
        "gameId": game_id,
        "plays": pbp_txt["plays"].to_dicts(),
        "boxscore": pbp_txt["boxscore"],
        "header": pbp_txt["header"],
        "format": pbp_txt["format"],
        "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
        "videos": np.array(pbp_txt["videos"]).tolist(),
        "playByPlaySource": pbp_txt["playByPlaySource"],
        "standings": pbp_txt["standings"],
        "leaders": np.array(pbp_txt["leaders"]).tolist(),
        "seasonseries": np.array(pbp_txt["seasonseries"]).tolist(),
        "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
        "againstTheSpread": np.array(pbp_txt["againstTheSpread"]).tolist(),
        "odds": np.array(pbp_txt["odds"]).tolist(),
        "onIce": np.array(pbp_txt["onIce"]).tolist(),
        "gameInfo": pbp_txt["gameInfo"],
        "teamInfo": np.array(pbp_txt["teamInfo"]).tolist(),
        "season": np.array(pbp_txt["season"]).tolist(),
    }


def helper_nhl_game_data(pbp_txt, init):
    pbp_txt["timeouts"] = {}
    pbp_txt["teamInfo"] = pbp_txt["header"]["competitions"][0]
    pbp_txt["season"] = pbp_txt["header"]["season"]
    pbp_txt["playByPlaySource"] = pbp_txt["header"]["competitions"][0]["playByPlaySource"]
    pbp_txt["boxscoreSource"] = pbp_txt["header"]["competitions"][0]["boxscoreSource"]
    pbp_txt["gameSpreadAvailable"] = init["gameSpreadAvailable"]
    pbp_txt["gameSpread"] = init["gameSpread"]
    pbp_txt["homeFavorite"] = init["homeFavorite"]
    pbp_txt["homeTeamSpread"] = np.where(
        init["homeFavorite"] == True, abs(init["gameSpread"]), -1 * abs(init["gameSpread"])
    )
    pbp_txt["overUnder"] = init["overUnder"]
    # Home and Away identification variables
    if pbp_txt["header"]["competitions"][0]["competitors"][0]["homeAway"] == "home":
        pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]
        homeTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
        homeTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["name"])
        homeTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
        homeTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
        homeTeamNameAlt = re.sub("Stat(.+)", "St", homeTeamName)
        pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]
        awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["id"])
        awayTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["name"])
        awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["location"])
        awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["abbreviation"])
        awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
    else:
        pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]
        awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
        awayTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["name"])
        awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
        awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
        awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
        pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]
        homeTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["id"])
        homeTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["name"])
        homeTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["location"])
        homeTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["abbreviation"])
        homeTeamNameAlt = re.sub("Stat(.+)", "St", homeTeamName)
    init["homeTeamId"] = homeTeamId
    init["homeTeamMascot"] = homeTeamMascot
    init["homeTeamName"] = homeTeamName
    init["homeTeamAbbrev"] = homeTeamAbbrev
    init["homeTeamNameAlt"] = homeTeamNameAlt
    init["awayTeamId"] = awayTeamId
    init["awayTeamMascot"] = awayTeamMascot
    init["awayTeamName"] = awayTeamName
    init["awayTeamAbbrev"] = awayTeamAbbrev
    init["awayTeamNameAlt"] = awayTeamNameAlt
    return pbp_txt, init


def helper_nhl_pickcenter(pbp_txt):
    # Spread definition
    if len(pbp_txt.get("pickcenter", [])) > 1:
        pickcenter = pd.json_normalize(data=pbp_txt, record_path="pickcenter")
        pickcenter = pickcenter.sort_values(by=["provider.id"])
        homeFavorite = (
            pickcenter[pickcenter["homeTeamOdds.favorite"].notnull()][["homeTeamOdds.favorite"]].values[0]
            if "homeTeamOdds.favorite" in pickcenter.columns
            else True
        )
        gameSpread = (
            pickcenter[pickcenter["spread"].notnull()][["spread"]].values[0] if "spread" in pickcenter.columns else 1.5
        )
        overUnder = (
            pickcenter[pickcenter["overUnder"].notnull()][["overUnder"]].values[0]
            if "overUnder" in pickcenter.columns
            else 5.5
        )
        gameSpreadAvailable = True
        # self.logger.info(f"Spread: {gameSpread}, home Favorite: {homeFavorite}, ou: {overUnder}")
    else:
        gameSpread = 1.5
        overUnder = 5.5
        homeFavorite = True
        gameSpreadAvailable = False

    return {
        "gameSpread": gameSpread,
        "overUnder": overUnder,
        "homeFavorite": homeFavorite,
        "gameSpreadAvailable": gameSpreadAvailable,
    }


def helper_nhl_pbp_features(game_id, pbp_txt, init):
    pbp_txt["plays_mod"] = []
    for play in pbp_txt["plays"]:
        p = flatten_json_iterative(play)
        pbp_txt["plays_mod"].append(p)
    pbp_txt["plays"] = pl.from_pandas(pd.json_normalize(pbp_txt, "plays_mod"))
    pbp_txt["plays"] = (
        pbp_txt["plays"]
        .with_columns(
            game_id=pl.lit(game_id).cast(pl.Int32),
            id=(pl.col("id").cast(pl.Int64)),
            sequenceNumber=pl.col("sequenceNumber").cast(pl.Int32),
            season=pl.lit(pbp_txt["header"]["season"]["year"]).cast(pl.Int32),
            seasonType=pl.lit(pbp_txt["header"]["season"]["type"]),
            homeTeamId=pl.lit(init["homeTeamId"]).cast(pl.Int32),
            homeTeamName=pl.lit(init["homeTeamName"]),
            homeTeamMascot=pl.lit(init["homeTeamMascot"]),
            homeTeamAbbrev=pl.lit(init["homeTeamAbbrev"]),
            homeTeamNameAlt=pl.lit(init["homeTeamNameAlt"]),
            awayTeamId=pl.lit(init["awayTeamId"]).cast(pl.Int32),
            awayTeamName=pl.lit(init["awayTeamName"]),
            awayTeamMascot=pl.lit(init["awayTeamMascot"]),
            awayTeamAbbrev=pl.lit(init["awayTeamAbbrev"]),
            awayTeamNameAlt=pl.lit(init["awayTeamNameAlt"]),
            gameSpread=pl.lit(init["gameSpread"]).abs(),
            homeFavorite=pl.lit(init["homeFavorite"]),
            gameSpreadAvailable=pl.lit(init["gameSpreadAvailable"]),
        )
        .with_columns(
            homeTeamSpread=pl.when(pl.col("homeFavorite") == True)
            .then(pl.col("gameSpread"))
            .otherwise(-1 * pl.col("gameSpread")),
        )
        .with_columns(
            pl.col("period.number").cast(pl.Int32).alias("period.number"),
            pl.col("period.number").cast(pl.Int32).alias("qtr"),
            pl.when(pl.col("clock.displayValue").str.contains(r":") == False)
            .then("0:" + pl.col("clock.displayValue"))
            .otherwise(pl.col("clock.displayValue"))
            .alias("clock.displayValue"),
        )
        .with_columns(
            pl.col("clock.displayValue").alias("time"),
            pl.col("clock.displayValue")
            .str.split(":")
            .list.to_struct(n_field_strategy="first_non_null")
            .alias("clock.mm"),
        )
        .with_columns(pl.col("clock.mm").struct.rename_fields(["clock.minutes", "clock.seconds"]))
        .unnest("clock.mm")
        .with_columns(
            pl.col("clock.minutes").cast(pl.Float32),
            pl.col("clock.seconds").cast(pl.Float32),
        )
        .with_columns(
            lag_period=pl.col("period.number").shift(1),
            lead_period=pl.col("period.number").shift(-1),
        )
        .with_columns(
            (60 * pl.col("clock.minutes") + pl.col("clock.seconds")).alias("start.period_seconds_remaining"),
            pl.when(pl.col("period.number") == 1)
            .then(2400 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .when(pl.col("period.number") == 2)
            .then(1200 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .when(pl.col("period.number") == 3)
            .then(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .alias("start.game_seconds_remaining"),
        )
        .with_columns(
            pl.col("start.period_seconds_remaining").shift(-1).alias("end.period_seconds_remaining"),
            pl.col("start.game_seconds_remaining").shift(-1).alias("end.game_seconds_remaining"),
        )
    )

    pbp_txt["plays"] = pbp_txt["plays"].with_row_count("game_play_number", 1)

    pbp_txt["plays"] = pbp_txt["plays"].with_columns(
        pl.when(pl.col("game_play_number") == 1)
        .then(1200)
        .when((pl.col("lag_period") == 1).and_(pl.col("period.number") == 2))
        .then(1200)
        .when((pl.col("lag_period") == 2).and_(pl.col("period.number") == 3))
        .then(1200)
        .when((pl.col("lag_period") == pl.col("period.number") - 1).and_(pl.col("period.number") >= 4))
        .then(1200)
        .otherwise(pl.col("end.period_seconds_remaining"))
        .alias("end.period_seconds_remaining"),
        pl.when(pl.col("game_play_number") == 1)
        .then(3600)
        .when((pl.col("lag_period") == 1).and_(pl.col("period.number") == 2))
        .then(2400)
        .when((pl.col("lag_period") == 2).and_(pl.col("period.number") == 3))
        .then(1200)
        .when((pl.col("lag_period") == pl.col("period.number") - 1).and_(pl.col("period.number") >= 4))
        .then(1200)
        .otherwise(pl.col("end.game_seconds_remaining"))
        .alias("end.game_seconds_remaining"),
    )

    return pbp_txt
