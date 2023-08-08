import json
import logging
import os
import re
import time
from functools import reduce

import numpy as np
import pandas as pd
import polars as pl
from pkg_resources import resource_filename
from xgboost import Booster, DMatrix

from sportsdataverse.cfb.model_vars import (
    defense_score_vec,
    end_change_vec,
    ep_class_to_score_mapping,
    ep_end_columns,
    ep_final_names,
    ep_start_columns,
    ep_start_touchback_columns,
    int_vec,
    kickoff_turnovers,
    kickoff_vec,
    normalplay,
    offense_score_vec,
    penalty,
    punt_vec,
    qbr_vars,
    scores_vec,
    turnover_vec,
    wp_end_columns,
    wp_final_names,
    wp_start_columns,
    wp_start_touchback_columns,
)
from sportsdataverse.dl_utils import download

ep_model_file = resource_filename("sportsdataverse", "cfb/models/ep_model.model")
wp_spread_file = resource_filename("sportsdataverse", "cfb/models/wp_spread.model")
qbr_model_file = resource_filename("sportsdataverse", "cfb/models/qbr_model.model")

ep_model = Booster({"nthread": 4})  # init model
ep_model.load_model(ep_model_file)

wp_model = Booster({"nthread": 4})  # init model
wp_model.load_model(wp_spread_file)

qbr_model = Booster({"nthread": 4})  # init model
qbr_model.load_model(qbr_model_file)

logger = logging.getLogger("sdv.cfb_pbp")
logger.addHandler(logging.NullHandler())


class CFBPlayProcess(object):
    gameId = 0
    # logger = None
    ran_pipeline = False
    ran_cleaning_pipeline = False
    raw = False
    path_to_json = "/"
    return_keys = None

    def __init__(self, gameId=0, raw=False, path_to_json="/", return_keys=None, **kwargs):
        self.gameId = int(gameId)
        # self.logger = logger
        self.ran_pipeline = False
        self.ran_cleaning_pipeline = False
        self.raw = raw
        self.path_to_json = path_to_json
        self.return_keys = return_keys

    def espn_cfb_pbp(self, **kwargs):
        """espn_cfb_pbp() - Pull the game by id. Data from API endpoints: `college-football/playbyplay`,
        `college-football/summary`

        Args:
            game_id (int): Unique game_id, can be obtained from cfb_schedule().
            raw (bool): If True, returns the raw json from the API endpoint. If False, returns a
            cleaned dictionary of datasets.

        Returns:
            Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
             "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder",
             "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP",
             "gameInfo", "season"

        Example:
            `cfb_df = sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp()`
        """
        cache_buster = int(time.time() * 1000)
        pbp_txt = {"timeouts": {}}
        # summary endpoint for pickcenter array
        summary_url = f"http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={self.gameId}&{cache_buster}"
        summary_resp = download(url=summary_url, **kwargs)
        summary = summary_resp.json()
        incoming_keys_expected = [
            "boxscore",
            "format",
            "gameInfo",
            "drives",
            "leaders",
            "broadcasts",
            "predictor",
            "pickcenter",
            "againstTheSpread",
            "odds",
            "winprobability",
            "header",
            "scoringPlays",
            "videos",
            "standings",
        ]
        dict_keys_expected = ["boxscore", "format", "gameInfo", "drives", "predictor", "header", "standings"]
        # array_keys_expected = [
        #     "leaders",
        #     "broadcasts",
        #     "pickcenter",
        #     "againstTheSpread",
        #     "odds",
        #     "winprobability",
        #     "scoringPlays",
        #     "videos",
        # ]
        if self.raw == True:
            logging.debug(f"{self.gameId}: raw cfb_pbp data requested, returning keys: {summary.keys()}")
            # reorder keys in raw format, appending empty keys which are defined later to the end
            pbp_json = {}
            for k in incoming_keys_expected:
                if k in summary.keys():
                    pbp_json[k] = summary[k]
                else:
                    pbp_json[k] = {} if k in dict_keys_expected else []
            return pbp_json

        logging.debug(f"{self.gameId}: full cfb_pbp data requested, returning keys: {summary.keys()}")
        for k in incoming_keys_expected:
            if k in summary.keys():
                pbp_txt[k] = summary[k]
            else:
                pbp_txt[k] = {} if k in dict_keys_expected else []
        for k in [
            "scoringPlays",
            "standings",
            "videos",
            "broadcasts",
            "pickcenter",
            "againstTheSpread",
            "odds",
            "predictor",
            "winprobability",
            "gameInfo",
            "leaders",
            "drives",
        ]:
            if k in summary.keys():
                pbp_txt[k] = summary[k]
            else:
                pbp_txt[k] = {} if k in dict_keys_expected else []
        for k in ["news", "shop"]:
            pbp_txt.pop(f"{k}", None)
        self.json = pbp_txt

        return self.json

    def cfb_pbp_disk(self):
        with open(os.path.join(self.path_to_json, f"{self.gameId}.json")) as json_file:
            pbp_txt = json.load(json_file)
            self.json = pbp_txt
        return self.json

    def cfb_pbp_json(self, **kwargs):
        self.json = json
        return self.json

    def __helper_cfb_pbp_drives(self, pbp_txt):
        pbp_txt, init = self.__helper_cfb_pbp(pbp_txt)

        pbp_txt["plays"] = pl.DataFrame()
        # negotiating the drive meta keys into columns after unnesting drive plays
        # concatenating the previous and current drives categories when necessary
        if (
            "drives" in pbp_txt.keys()
            and pbp_txt.get("header").get("competitions")[0].get("playByPlaySource") != "none"
        ):
            pbp_txt = self.__helper_cfb_pbp_features(pbp_txt, init)
        else:
            pbp_txt["drives"] = {}
        return pbp_txt

    def __helper_cfb_pbp_features(self, pbp_txt, init):
        pbp_txt["plays"] = pl.DataFrame()
        for key in pbp_txt.get("drives").keys():
            logging.debug(f"{self.gameId}: drives key - {key}")
            prev_drives = pd.json_normalize(
                data=pbp_txt.get("drives").get(f"{key}"),
                record_path="plays",
                meta=[
                    "id",
                    "displayResult",
                    "isScore",
                    ["team", "shortDisplayName"],
                    ["team", "displayName"],
                    ["team", "name"],
                    ["team", "abbreviation"],
                    "yards",
                    "offensivePlays",
                    "result",
                    "description",
                    "shortDisplayResult",
                    ["timeElapsed", "displayValue"],
                    ["start", "period", "number"],
                    ["start", "period", "type"],
                    ["start", "yardLine"],
                    ["start", "clock", "displayValue"],
                    ["start", "text"],
                    ["end", "period", "number"],
                    ["end", "period", "type"],
                    ["end", "yardLine"],
                    ["end", "clock", "displayValue"],
                ],
                meta_prefix="drive.",
                errors="ignore",
            )
            pbp_txt["plays"] = pl.concat([pbp_txt["plays"], pl.from_pandas(prev_drives)], how="diagonal")

        pbp_txt["timeouts"] = {
            init["homeTeamId"]: {"1": [], "2": []},
            init["awayTeamId"]: {"1": [], "2": []},
        }

        logging.debug(f"{self.gameId}: plays_df length - {len(pbp_txt['plays'])}")
        if len(pbp_txt["plays"]) == 0:
            return pbp_txt
        if (len(pbp_txt["plays"]) < 50) and (
            pbp_txt.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(f"{self.gameId}: appear to be too few plays ({len(pbp_txt['plays'])}) for a completed game")
            return pbp_txt
        if (len(pbp_txt["plays"]) > 500) and (
            pbp_txt.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(f"{self.gameId}: appear to be too many plays ({len(pbp_txt['plays'])}) for a completed game")
            return pbp_txt
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                game_id=pl.lit(int(self.gameId)),
                season=pbp_txt.get("header").get("season").get("year"),
                seasonType=pbp_txt.get("header").get("season").get("type"),
                week=pbp_txt.get("header").get("week"),
                status_type_completed=pbp_txt.get("header")
                .get("competitions")[0]
                .get("status")
                .get("type")
                .get("completed"),
                homeTeamId=pl.lit(init["homeTeamId"]),
                awayTeamId=pl.lit(init["awayTeamId"]),
                homeTeamName=pl.lit(str(init["homeTeamName"])),
                awayTeamName=pl.lit(str(init["awayTeamName"])),
                homeTeamMascot=pl.lit(str(init["homeTeamMascot"])),
                awayTeamMascot=pl.lit(str(init["awayTeamMascot"])),
                homeTeamAbbrev=pl.lit(str(init["homeTeamAbbrev"])),
                awayTeamAbbrev=pl.lit(str(init["awayTeamAbbrev"])),
                homeTeamNameAlt=pl.lit(str(init["homeTeamNameAlt"])),
                awayTeamNameAlt=pl.lit(str(init["awayTeamNameAlt"])),
                gameSpread=pl.lit(init["gameSpread"]).abs(),
                homeFavorite=pl.lit(init["homeFavorite"]),
                gameSpreadAvailable=pl.lit(init["gameSpreadAvailable"]),
                overUnder=pl.lit(float(init["overUnder"])),
            )
            .with_columns(
                homeTeamSpread=pl.when(pl.col("homeFavorite") == True)
                .then(pl.col("gameSpread"))
                .otherwise(-1 * pl.col("gameSpread")),
            )
            .with_columns(
                pl.col("period.number").cast(pl.Int32),
                pl.col("clock.displayValue")
                .str.split(":")
                .list.to_struct(n_field_strategy="max_width")
                .alias("clock.mm"),
            )
            .with_columns(pl.col("clock.mm").struct.rename_fields(["clock.minutes", "clock.seconds"]))
            .unnest("clock.mm")
            .with_columns(
                pl.col("clock.minutes").cast(pl.Int32),
                pl.col("clock.seconds").cast(pl.Int32),
                half=pl.when(pl.col("period.number") <= 2).then(1).otherwise(2),
            )
            .with_columns(lag_half=pl.col("half").shift(1), lead_half=pl.col("half").shift(-1))
            .with_columns(
                pl.when(pl.col("period.number").is_in([1, 3]))
                .then(900 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .alias("start.TimeSecsRem"),
                pl.when(pl.col("period.number") == 1)
                .then(2700 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .when(pl.col("period.number") == 2)
                .then(1800 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .when(pl.col("period.number") == 3)
                .then(900 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .alias("start.adj_TimeSecsRem"),
                pl.col("id").cast(pl.Int64),
                pl.col("sequenceNumber").cast(pl.Int32),
            )
        )
        pbp_txt["plays"] = pbp_txt["plays"].sort(by=["id", "start.adj_TimeSecsRem"])

        # drop play text dupes intelligently, even if they have different play_id values
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.col("text").cast(str),
                orig_play_type=pl.col("type.text"),
                lead_text=pl.col("text").shift(-1),
                lead_start_team=pl.col("start.team.id").shift(-1),
                lead_start_yardsToEndzone=pl.col("start.yardsToEndzone").shift(-1),
                lead_start_down=pl.col("start.down").shift(-1),
                lead_start_distance=pl.col("start.distance").shift(-1),
                lead_scoringPlay=pl.col("scoringPlay").shift(-1),
                text_dupe=pl.lit(False),
            )
            .with_columns(
                text_dupe=pl.when(
                    (pl.col("start.team.id") == pl.col("lead_start_team"))
                    .and_(pl.col("start.down") == pl.col("lead_start_down"))
                    .and_(pl.col("start.yardsToEndzone") == pl.col("lead_start_yardsToEndzone"))
                    .and_(pl.col("start.distance") == pl.col("lead_start_distance"))
                    .and_(pl.col("text") == pl.col("lead_text"))
                    .and_(pl.col("type.text") != "Timeout")
                )
                .then(pl.lit(True))
                .when(
                    (pl.col("start.team.id") == pl.col("lead_start_team"))
                    .and_(pl.col("start.down") == pl.col("lead_start_down"))
                    .and_(pl.col("start.yardsToEndzone") == pl.col("lead_start_yardsToEndzone"))
                    .and_(pl.col("start.distance") == pl.col("lead_start_distance"))
                    .and_(pl.col("text").is_in(pl.col("lead_text")))
                    .and_(pl.col("type.text") != "Timeout")
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
            )
        )
        pbp_txt["plays"] = pbp_txt["plays"].filter(pl.col("text_dupe") == False)
        pbp_txt["plays"] = pbp_txt["plays"].with_row_count("game_play_number", 1)
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.col("start.team.id").fill_null(strategy="forward").fill_null(strategy="backward").cast(pl.Int32)
            )
            .with_columns(pl.col("end.team.id").fill_null(value=pl.col("start.team.id")).cast(pl.Int32))
            .with_columns(
                pl.col("start.team.id").cast(pl.Int32),
                pl.col("end.team.id").cast(pl.Int32),
                pl.col("homeTeamId").cast(pl.Int32),
                pl.col("awayTeamId").cast(pl.Int32),
                pl.when(pl.col("type.text").is_in(kickoff_vec).and_(pl.col("start.team.id") == init["homeTeamId"]))
                .then(pl.col("awayTeamId"))
                .when(pl.col("type.text").is_in(kickoff_vec).and_(pl.col("start.team.id") == init["awayTeamId"]))
                .then(pl.col("homeTeamId"))
                .otherwise(pl.col("start.team.id"))
                .alias("start.pos_team.id"),
            )
            .with_columns(
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(init["awayTeamId"])
                .otherwise(init["homeTeamId"])
                .alias("start.def_pos_team.id"),
                pl.when(pl.col("end.team.id") == init["homeTeamId"])
                .then(init["awayTeamId"])
                .otherwise(init["homeTeamId"])
                .alias("end.def_pos_team.id"),
                pl.col("end.team.id").alias("end.pos_team.id"),
            )
            .with_columns(
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(pl.col("homeTeamName"))
                .otherwise(pl.col("awayTeamName"))
                .alias("start.pos_team.name"),
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(pl.col("awayTeamName"))
                .otherwise(pl.col("homeTeamName"))
                .alias("start.def_pos_team.name"),
                pl.when(pl.col("end.pos_team.id") == init["homeTeamId"])
                .then(pl.col("homeTeamName"))
                .otherwise(pl.col("awayTeamName"))
                .alias("end.pos_team.name"),
                pl.when(pl.col("end.pos_team.id") == init["homeTeamId"])
                .then(pl.col("awayTeamName"))
                .otherwise(pl.col("homeTeamName"))
                .alias("end.def_pos_team.name"),
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(True)
                .otherwise(False)
                .alias("start.is_home"),
                pl.when(pl.col("end.pos_team.id") == init["homeTeamId"])
                .then(True)
                .otherwise(False)
                .alias("end.is_home"),
                pl.when(
                    (pl.col("type.text") == "Timeout").and_(
                        pl.col("text")
                        .str.to_lowercase()
                        .str.contains(str(init["homeTeamAbbrev"]).lower())
                        .or_(
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamAbbrev"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamName"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamMascot"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamNameAlt"]).lower()),
                        )
                    )
                )
                .then(True)
                .otherwise(False)
                .alias("homeTimeoutCalled"),
                pl.when(
                    (pl.col("type.text") == "Timeout").and_(
                        pl.col("text")
                        .str.to_lowercase()
                        .str.contains(str(init["awayTeamAbbrev"]).lower())
                        .or_(
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamAbbrev"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamName"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamMascot"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamNameAlt"]).lower()),
                        )
                    )
                )
                .then(True)
                .otherwise(False)
                .alias("awayTimeoutCalled"),
            )
        )

        pbp_txt["timeouts"][init["homeTeamId"]]["1"] = (
            pbp_txt["plays"]
            .filter((pl.col("homeTimeoutCalled") == True).and_(pl.col("period.number") <= 2))
            .get_column("id")
            .to_list()
        )
        pbp_txt["timeouts"][init["homeTeamId"]]["2"] = (
            pbp_txt["plays"]
            .filter((pl.col("homeTimeoutCalled") == True).and_(pl.col("period.number") > 2))
            .get_column("id")
            .to_list()
        )
        pbp_txt["timeouts"][init["awayTeamId"]]["1"] = (
            pbp_txt["plays"]
            .filter((pl.col("awayTimeoutCalled") == True).and_(pl.col("period.number") <= 2))
            .get_column("id")
            .to_list()
        )
        pbp_txt["timeouts"][init["awayTeamId"]]["2"] = (
            pbp_txt["plays"]
            .filter((pl.col("awayTimeoutCalled") == True).and_(pl.col("period.number") > 2))
            .get_column("id")
            .to_list()
        )
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                (
                    3
                    - pl.struct(pl.col(["id", "period.number"])).apply(
                        lambda x: (
                            sum(
                                (i <= x["id"]) & (x["period.number"] <= 2)
                                for i in pbp_txt["timeouts"][int(init["homeTeamId"])]["1"]
                            )
                        )
                        | (
                            sum(
                                (i <= x["id"]) & (x["period.number"] > 2)
                                for i in pbp_txt["timeouts"][int(init["homeTeamId"])]["2"]
                            )
                        ),
                    )
                ).alias("end.homeTeamTimeouts"),
                (
                    3
                    - pl.struct(pl.col(["id", "period.number"])).apply(
                        lambda x: (
                            sum(
                                (i <= x["id"]) & (x["period.number"] <= 2)
                                for i in pbp_txt["timeouts"][int(init["awayTeamId"])]["1"]
                            )
                        )
                        | (
                            sum(
                                (i <= x["id"]) & (x["period.number"] > 2)
                                for i in pbp_txt["timeouts"][int(init["awayTeamId"])]["2"]
                            )
                        ),
                    )
                ).alias("end.awayTeamTimeouts"),
            )
            .with_columns(
                pl.col("end.homeTeamTimeouts").shift_and_fill(periods=1, fill_value=3).alias("start.homeTeamTimeouts"),
                pl.col("end.awayTeamTimeouts").shift_and_fill(periods=1, fill_value=3).alias("start.awayTeamTimeouts"),
                pl.col("start.TimeSecsRem").shift(periods=1).alias("end.TimeSecsRem"),
                pl.col("start.adj_TimeSecsRem").shift(periods=1).alias("end.adj_TimeSecsRem"),
            )
            .with_columns(
                pl.when(pl.col("game_play_number") == 1)
                .then(pl.lit(1800))
                .when((pl.col("half") == 2) & (pl.col("lag_half") == 1))
                .then(pl.lit(1800))
                .otherwise(pl.col("end.TimeSecsRem"))
                .alias("end.TimeSecsRem"),
                pl.when(pl.col("game_play_number") == 1)
                .then(pl.lit(3600))
                .when((pl.col("half") == 2) & (pl.col("lag_half") == 1))
                .then(pl.lit(1800))
                .otherwise(pl.col("end.adj_TimeSecsRem"))
                .alias("end.adj_TimeSecsRem"),
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.homeTeamTimeouts"))
                .otherwise(pl.col("start.awayTeamTimeouts"))
                .alias("start.posTeamTimeouts"),
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.awayTeamTimeouts"))
                .otherwise(pl.col("start.homeTeamTimeouts"))
                .alias("start.defPosTeamTimeouts"),
                pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("end.homeTeamTimeouts"))
                .otherwise(pl.col("end.awayTeamTimeouts"))
                .alias("end.posTeamTimeouts"),
                pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("end.awayTeamTimeouts"))
                .otherwise(pl.col("end.homeTeamTimeouts"))
                .alias("end.defPosTeamTimeouts"),
                pl.when(
                    (pl.col("game_play_number") == 1).and_(
                        pl.col("type.text").is_in(kickoff_vec), pl.col("start.pos_team.id") == pl.col("homeTeamId")
                    )
                )
                .then(pl.col("homeTeamId"))
                .otherwise(pl.col("awayTeamId"))
                .alias("firstHalfKickoffTeamId"),
                pl.col("period.number").alias("period"),
                pl.when(pl.col("start.team.id") == pl.col("homeTeamId"))
                .then(pl.lit(100) - pl.col("start.yardLine"))
                .otherwise(pl.col("start.yardLine"))
                .alias("start.yard"),
            )
            .with_columns(
                pl.when(pl.col("start.yardLine").is_null() == False)
                .then(pl.col("start.yardLine"))
                .otherwise(pl.col("start.yard"))
                .alias("start.yardLine"),
            )
            .with_columns(
                pl.when(pl.col("start.yardLine").is_null() == False)
                .then(pl.col("start.yardsToEndzone"))
                .otherwise(pl.col("start.yardLine"))
                .alias("start.yardsToEndzone")
            )
            .with_columns(
                pl.when(pl.col("start.yardsToEndzone") == 0)
                .then(pl.col("start.yard"))
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone"),
                pl.when(pl.col("end.team.id") == pl.col("homeTeamId"))
                .then(pl.lit(100) - pl.col("end.yardLine"))
                .otherwise(pl.col("end.yardLine"))
                .alias("end.yard"),
            )
            .with_columns(
                pl.when((pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains(r"(?i)declined")))
                .then(pl.col("start.yard"))
                .otherwise(pl.col("end.yard"))
                .alias("end.yard"),
            )
            .with_columns(
                pl.when(pl.col("end.yardLine").is_null() == False)
                .then(pl.col("end.yardsToEndzone"))
                .otherwise(pl.col("end.yard"))
                .alias("end.yardsToEndzone"),
                pl.when(
                    (pl.col("start.distance") == 0).and_(pl.col("start.downDistanceText").str.contains(r"(?i)goal"))
                )
                .then(pl.col("start.yardsToEndzone"))
                .otherwise(pl.col("start.distance"))
                .alias("start.distance"),
            )
            .with_columns(
                pl.when((pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains(r"(?i)declined")))
                .then(pl.col("start.yardsToEndzone"))
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
        )
        pbp_txt["firstHalfKickoffTeamId"] = np.where(
            (pbp_txt["plays"]["game_play_number"] == 1)
            & (pbp_txt["plays"]["type.text"].is_in(kickoff_vec))
            & (pbp_txt["plays"]["start.team.id"] == init["homeTeamId"]),
            init["homeTeamId"],
            init["awayTeamId"],
        )
        pbp_txt["firstHalfKickoffTeamId"] = pbp_txt["firstHalfKickoffTeamId"][0]

        if "scoringType.displayName" in pbp_txt["plays"].columns:
            pbp_txt["plays"] = (
                pbp_txt["plays"]
                .with_columns(
                    pl.when(pl.col("scoringType.displayName") == "Field Goal")
                    .then(pl.lit("Field Goal Good"))
                    .otherwise(pl.col("type.text"))
                    .alias("type.text")
                )
                .with_columns(
                    pl.when(pl.col("scoringType.displayName") == "Extra Point")
                    .then(pl.lit("Extra Point Good"))
                    .otherwise(pl.col("type.text"))
                    .alias("type.text")
                )
            )
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.when(pl.col("type.text").is_null())
                .then(pl.lit("Unknown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text")
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)extra point")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)no good"))
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text")
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)extra point")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)blocked"))
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text")
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)field goal")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)blocked"))
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text")
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)field goal")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)no good"))
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text")
            )
        )

        return pbp_txt

    def __helper_cfb_pbp(self, pbp_txt):
        init = self.__helper_cfb_pickcenter(pbp_txt)
        return self.__helper_cfb_game_data(pbp_txt, init)

    def __helper_cfb_pickcenter(self, pbp_txt):
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
                pickcenter[pickcenter["spread"].notnull()][["spread"]].values[0]
                if "spread" in pickcenter.columns
                else 2.5
            )
            overUnder = (
                pickcenter[pickcenter["overUnder"].notnull()][["overUnder"]].values[0]
                if "overUnder" in pickcenter.columns
                else 55.0
            )
            gameSpreadAvailable = True
            # self.logger.info(f"Spread: {gameSpread}, home Favorite: {homeFavorite}, ou: {overUnder}")
        else:
            gameSpread = 2.5
            overUnder = 55.0
            homeFavorite = True
            gameSpreadAvailable = False

        return {
            "gameSpread": gameSpread,
            "overUnder": overUnder,
            "homeFavorite": homeFavorite,
            "gameSpreadAvailable": gameSpreadAvailable,
        }

    def __helper_cfb_game_data(self, pbp_txt, init):
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
            pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][0][
                "team"
            ]
            homeTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
            homeTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["name"])
            homeTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
            homeTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
            homeTeamNameAlt = re.sub("Stat(.+)", "St", homeTeamName)
            pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][1][
                "team"
            ]
            awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["id"])
            awayTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["name"])
            awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["location"])
            awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["abbreviation"])
            awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
        else:
            pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][0][
                "team"
            ]
            awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
            awayTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["name"])
            awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
            awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
            awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
            pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][1][
                "team"
            ]
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

    def __add_downs_data(self, play_df):
        """
        Creates the following columns in play_df:
            * id, drive_id, game_id
            * down, ydstogo (distance), game_half, period
        """
        play_df = play_df.sort(by=["id", "start.adj_TimeSecsRem"])

        play_df = play_df.unique(
            subset=["text", "id", "type.text", "start.down", "sequenceNumber"], keep="last", maintain_order=True
        )
        play_df = play_df.filter(
            pl.col("type.text").str.contains("(?i)end of|(?i)coin toss|(?i)end period|(?i)wins toss") == False
        )
        play_df = (
            play_df.with_columns(
                period=pl.col("period.number"),
                half=pl.when(pl.col("period.number") <= 2).then(1).otherwise(2),
            )
            .with_columns(
                lead_half=pl.col("half").shift(-1),
                lag_scoringPlay=pl.col("scoringPlay").shift(1),
            )
            .with_columns(
                pl.when(pl.col("lead_half").is_null()).then(2).otherwise(pl.col("lead_half")).alias("lead_half"),
                end_of_half=pl.col("half") != pl.col("lead_half"),
                down_1=pl.col("start.down") == 1,
                down_2=pl.col("start.down") == 2,
                down_3=pl.col("start.down") == 3,
                down_4=pl.col("start.down") == 4,
                down_1_end=pl.col("end.down") == 1,
                down_2_end=pl.col("end.down") == 2,
                down_3_end=pl.col("end.down") == 3,
                down_4_end=pl.col("end.down") == 4,
            )
        )

        return play_df

    def __add_play_type_flags(self, play_df):
        """
        Creates the following columns in play_df:
            * Flags for fumbles, scores, kickoffs, punts, field goals
        """
        # --- Touchdown, Fumble, Special Teams flags -----------------
        play_df = (
            play_df.with_columns(
                scoring_play=pl.when(pl.col("type.text").is_in(scores_vec)).then(True).otherwise(False),
                td_play=pl.col("text").str.contains("(?i)touchdown|(?i)for a TD"),
                touchdown=pl.col("type.text").str.contains("(?i)touchdown"),
                ## Portion of touchdown check for plays where touchdown is not listed in the play_type--
                td_check=pl.col("text").str.contains("(?i)touchdown"),
                safety=pl.col("text").str.contains("(?i)safety"),
                fumble_vec=pl.when(pl.col("text").str.contains("(?i)fumble"))
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)fumble")).and_(
                        pl.col("type.text") == "Rush", pl.col("start.pos_team.id") != pl.col("end.pos_team.id")
                    )
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)fumble")).and_(
                        pl.col("type.text") == "Sack", pl.col("start.pos_team.id") != pl.col("end.pos_team.id")
                    )
                )
                .then(True)
                .otherwise(False),
                forced_fumble=pl.when(pl.col("text").str.contains("(?i)forced by")).then(True).otherwise(False),
                # --- Kicks----
                kickoff_play=pl.col("type.text").is_in(kickoff_vec),
            )
            .with_columns(
                kickoff_tb=pl.when((pl.col("text").str.contains("(?i)touchback")).and_(pl.col("kickoff_play") == True))
                .then(True)
                .when((pl.col("text").str.contains("(?i)kickoff$")).and_(pl.col("kickoff_play") == True))
                .then(True)
                .otherwise(False),
                kickoff_onside=pl.when(
                    (pl.col("text").str.contains("(?i)on-side|(?i)onside|(?i)on side")).and_(
                        pl.col("kickoff_play") == True
                    )
                )
                .then(True)
                .otherwise(False),
                kickoff_oob=pl.when(
                    (pl.col("text").str.contains("(?i)out-of-bounds|(?i)out of bounds")).and_(
                        pl.col("kickoff_play") == True
                    )
                )
                .then(True)
                .otherwise(False),
                kickoff_fair_catch=pl.when(
                    (pl.col("text").str.contains("(?i)fair catch|(?i)fair caught")).and_(
                        pl.col("kickoff_play") == True
                    )
                )
                .then(True)
                .otherwise(False),
                kickoff_downed=pl.when(
                    (pl.col("text").str.contains("(?i)downed")).and_(pl.col("kickoff_play") == True)
                )
                .then(True)
                .otherwise(False),
                kick_play=pl.col("text").str.contains("(?i)kick|(?i)kickoff"),
                kickoff_safety=pl.when(
                    (pl.col("text").str.contains("(?i)kickoff")).and_(
                        pl.col("safety") == True, pl.col("type.text").is_in(["Blocked Punt", "Penalty"]) == False
                    )
                )
                .then(True)
                .otherwise(False),
                # --- Punts----
                punt=pl.col("type.text").is_in(punt_vec),
                punt_play=pl.col("text").str.contains("(?i)punt"),
            )
            .with_columns(
                punt_tb=pl.when((pl.col("text").str.contains("(?i)touchback")).and_(pl.col("punt") == True))
                .then(True)
                .otherwise(False),
                punt_oob=pl.when(
                    (pl.col("text").str.contains("(?i)out-of-bounds|(?i)out of bounds")).and_(pl.col("punt") == True)
                )
                .then(True)
                .otherwise(False),
                punt_fair_catch=pl.when(
                    (pl.col("text").str.contains("(?i)fair catch|(?i)fair caught")).and_(pl.col("punt") == True)
                )
                .then(True)
                .otherwise(False),
                punt_downed=pl.when((pl.col("text").str.contains("(?i)downed")).and_(pl.col("punt") == True))
                .then(True)
                .otherwise(False),
                punt_safety=pl.when((pl.col("text").str.contains("(?i)punt")).and_(pl.col("safety") == True))
                .then(True)
                .otherwise(False),
                punt_blocked=pl.when((pl.col("text").str.contains("(?i)blocked")).and_(pl.col("punt") == True))
                .then(True)
                .otherwise(False),
                penalty_safety=pl.when((pl.col("type.text").is_in(["Penalty"])).and_(pl.col("safety") == True))
                .then(True)
                .otherwise(False),
            )
        )

        return play_df

    def __add_rush_pass_flags(self, play_df):
        """
        Creates the following columns in play_df:
            * Rush, Pass, Sacks
        """

        play_df = (
            play_df.with_columns(
                # --- Pass/Rush----
                pl.when(
                    (pl.col("type.text") == "Rush")
                    .or_(pl.col("type.text") == "Rushing Touchdown")
                    .or_(
                        (
                            pl.col("type.text").is_in(
                                [
                                    "Safety",
                                    "Fumble Recovery (Opponent)",
                                    "Fumble Recovery (Opponent) Touchdown",
                                    "Fumble Recovery (Own)",
                                    "Fumble Recovery (Own) Touchdown",
                                    "Fumble Return Touchdown",
                                ]
                            )
                        ).and_(pl.col("text").str.contains("run for"))
                    )
                )
                .then(True)
                .otherwise(False)
                .alias("rush"),
                pl.when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Pass Reception",
                                "Pass Completion",
                                "Passing Touchdown",
                                "Sack",
                                "Pass",
                                "Interception",
                                "Pass Interception Return",
                                "Interception Return Touchdown",
                                "Pass Incompletion",
                                "Sack Touchdown",
                                "Interception Return",
                            ]
                        )
                    )
                    .or_((pl.col("type.text") == "Safety").and_(pl.col("text").str.contains("sacked")))
                    .or_((pl.col("type.text") == "Safety").and_(pl.col("text").str.contains("pass complete")))
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own)").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted")
                        )
                    )
                    .or_((pl.col("type.text") == "Fumble Recovery (Own)").and_(pl.col("text").str.contains("sacked")))
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own) Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted")
                        )
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own) Touchdown").and_(
                            pl.col("text").str.contains("sacked")
                        )
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent)").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted")
                        )
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent)").and_(
                            pl.col("text").str.contains("sacked")
                        )
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent) Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete")
                        )
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Return Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete")
                        )
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Return Touchdown").and_(pl.col("text").str.contains("sacked"))
                    )
                )
                .then(True)
                .otherwise(False)
                .alias("pass"),
            )
            .with_columns(
                # --- Sacks----
                sack_vec=pl.when(
                    (pl.col("type.text").is_in(["Sack", "Sack Touchdown"])).or_(
                        (
                            pl.col("type.text").is_in(
                                [
                                    "Fumble Recovery (Own)",
                                    "Fumble Recovery (Own) Touchdown",
                                    "Fumble Recovery (Opponent)",
                                    "Fumble Recovery (Opponent) Touchdown",
                                    "Fumble Return Touchdown",
                                ]
                            )
                        ).and_(pl.col("text").str.contains("(?i)sacked"), pl.col("pass") == True)
                    )
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                pl.when(pl.col("sack_vec") == True).then(True).otherwise(pl.col("pass")).alias("pass"),
            )
        )

        return play_df

    def __add_team_score_variables(self, play_df):
        """
        Creates the following columns in play_df:
            * Team Score variables
            * Fix change of poss variables
        """
        play_df = (
            play_df.with_columns(
                pos_team=pl.col("start.pos_team.id"),
                def_pos_team=pl.col("start.def_pos_team.id"),
            )
            .with_columns(
                is_home=pl.col("pos_team") == pl.col("homeTeamId"),
                # --- Team Score variables ------
                lag_homeScore=pl.col("homeScore").shift(1),
                lag_awayScore=pl.col("awayScore").shift(1),
            )
            .with_columns(
                lag_HA_score_diff=pl.col("lag_homeScore") - pl.col("lag_awayScore"),
                HA_score_diff=pl.col("homeScore") - pl.col("awayScore"),
            )
            .with_columns(
                net_HA_score_pts=pl.col("HA_score_diff") - pl.col("lag_HA_score_diff"),
                H_score_diff=pl.col("homeScore") - pl.col("lag_homeScore"),
                A_score_diff=pl.col("awayScore") - pl.col("lag_awayScore"),
            )
            .with_columns(
                homeScore=pl.when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") >= 9)
                )
                .then(pl.col("lag_homeScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") < 9)
                    & (pl.col("H_score_diff") > 1)
                )
                .then(pl.col("lag_homeScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") >= -9)
                    & (pl.col("H_score_diff") < -1)
                )
                .then(pl.col("homeScore"))
                .otherwise(pl.col("homeScore")),
                awayScore=pl.when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") >= 9)
                )
                .then(pl.col("lag_awayScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") < 9)
                    & (pl.col("A_score_diff") > 1)
                )
                .then(pl.col("lag_awayScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") >= -9)
                    & (pl.col("A_score_diff") < -1)
                )
                .then(pl.col("awayScore"))
                .otherwise(pl.col("awayScore")),
            )
            .drop(["lag_homeScore", "lag_awayScore"])
            .with_columns(
                lag_homeScore=pl.col("homeScore").shift(1),
                lag_awayScore=pl.col("awayScore").shift(1),
            )
            .with_columns(
                lag_homeScore=pl.when(pl.col("lag_homeScore").is_null()).then(0).otherwise(pl.col("lag_homeScore")),
                lag_awayScore=pl.when(pl.col("lag_awayScore").is_null()).then(0).otherwise(pl.col("lag_awayScore")),
            )
            .with_columns(
                pl.when(pl.col("game_play_number") == 1)
                .then(0)
                .otherwise(pl.col("lag_homeScore"))
                .alias("start.homeScore"),
                pl.when(pl.col("game_play_number") == 1)
                .then(0)
                .otherwise(pl.col("lag_awayScore"))
                .alias("start.awayScore"),
                pl.col("homeScore").alias("end.homeScore"),
                pl.col("awayScore").alias("end.awayScore"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("homeScore"))
                .otherwise(pl.col("awayScore"))
                .alias("pos_team_score"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("awayScore"))
                .otherwise(pl.col("homeScore"))
                .alias("def_pos_team_score"),
            )
            .with_columns(
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.homeScore"))
                .otherwise(pl.col("start.awayScore"))
                .alias("start.pos_team_score"),
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.awayScore"))
                .otherwise(pl.col("start.homeScore"))
                .alias("start.def_pos_team_score"),
            )
            .with_columns(
                (pl.col("start.pos_team_score") - pl.col("start.def_pos_team_score")).alias("start.pos_score_diff"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("end.homeScore"))
                .otherwise(pl.col("end.awayScore"))
                .alias("end.pos_team_score"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("end.awayScore"))
                .otherwise(pl.col("end.homeScore"))
                .alias("end.def_pos_team_score"),
            )
            .with_columns(
                (pl.col("end.pos_team_score") - pl.col("end.def_pos_team_score")).alias("end.pos_score_diff"),
                pl.col("pos_team").shift(1).alias("lag_pos_team"),
            )
            .with_columns(
                pl.when(pl.col("lag_pos_team").is_null())
                .then(pl.col("pos_team"))
                .otherwise(pl.col("lag_pos_team"))
                .alias("lag_pos_team"),
                pl.col("pos_team").shift(-1).alias("lead_pos_team"),
                pl.col("pos_team").shift(-2).alias("lead_pos_team2"),
                (pl.col("pos_team_score") - pl.col("def_pos_team_score")).alias("pos_score_diff"),
            )
            .with_columns(
                pl.col("pos_score_diff").shift(1).alias("lag_pos_score_diff"),
            )
            .with_columns(
                pl.when(pl.col("lag_pos_score_diff").is_null())
                .then(0)
                .otherwise(pl.col("lag_pos_score_diff"))
                .alias("lag_pos_score_diff"),
            )
            .with_columns(
                pl.when(pl.col("lag_pos_team") == pl.col("pos_team"))
                .then(pl.col("pos_score_diff") - pl.col("lag_pos_score_diff"))
                .otherwise(pl.col("pos_score_diff") + pl.col("lag_pos_score_diff"))
                .alias("pos_score_pts"),
                pl.when((pl.col("kickoff_play") == True).and_(pl.col("lag_pos_team") == pl.col("pos_team")))
                .then(pl.col("lag_pos_score_diff"))
                .when((pl.col("kickoff_play") == True).or_(pl.col("lag_pos_team") != pl.col("pos_team")))
                .then(-1 * pl.col("lag_pos_score_diff"))
                .otherwise(pl.col("lag_pos_score_diff"))
                .alias("pos_score_diff_start"),
            )
            .with_columns(
                pl.when(pl.col("pos_score_diff_start").is_null() == True)
                .then(pl.col("pos_score_diff"))
                .otherwise(pl.col("pos_score_diff_start"))
                .alias("pos_score_diff_start"),
                pl.when(pl.col("start.pos_team.id") == pl.col("firstHalfKickoffTeamId"))
                .then(True)
                .otherwise(False)
                .alias("start.pos_team_receives_2H_kickoff"),
                pl.when(pl.col("end.pos_team.id") == pl.col("firstHalfKickoffTeamId"))
                .then(True)
                .otherwise(False)
                .alias("end.pos_team_receives_2H_kickoff"),
                pl.when(pl.col("start.pos_team.id") == pl.col("end.pos_team.id"))
                .then(False)
                .otherwise(True)
                .alias("change_of_poss"),
            )
            .with_columns(
                pl.when(pl.col("change_of_poss").is_null() == True)
                .then(False)
                .otherwise(pl.col("change_of_poss"))
                .alias("change_of_poss"),
            )
        )

        return play_df

    def __add_new_play_types(self, play_df):
        """
        Creates the following columns in play_df:
            * Fix play types
        """
        # --------------------------------------------------
        play_df = (
            play_df.with_columns(
                # --- Fix Strip Sacks to Fumbles ----
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == False)
                    .and_(pl.col("start.down") != 4)
                    .and_(pl.col("type.text").is_in(defense_score_vec) == False)
                )
                .then(pl.lit("Fumble Recovery (Opponent)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == True)
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Fix rushes with fumbles and a change of possession to fumbles----
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == False)
                    .and_(pl.col("start.down") != 4)
                    .and_(pl.col("type.text").is_in(defense_score_vec) == False)
                )
                .then(pl.lit("Fumble Recovery (Opponent)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == True)
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix kickoff fumble return TDs ----
                pl.when(
                    (pl.col("kickoff_play") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("td_check") == True)
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix punt return TDs ----
                pl.when((pl.col("punt_play") == True).and_(pl.col("td_play") == True).and_(pl.col("td_check") == True))
                .then(pl.lit("Punt Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix kick return TDs ----
                pl.when(
                    (pl.col("kickoff_play") == True)
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("td_check") == True)
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix rush/pass tds that aren't explicit----
                pl.when(
                    (pl.col("td_play") == True)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("td_check") == True)
                )
                .then(pl.lit("Rushing Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("td_play") == True)
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("td_check") == True)
                    .and_(pl.col("type.text").is_in(int_vec) == False)
                )
                .then(pl.lit("Passing Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("pass") == True)
                    .and_(pl.col("type.text").is_in(["Pass Reception", "Pass Completion", "Pass"]))
                    .and_(pl.col("statYardage") == pl.col("start.yardsToEndzone"))
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("type.text").is_in(int_vec) == False)
                )
                .then(pl.lit("Passing Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Blocked Field Goal"])).and_(
                        pl.col("text").str.contains("(?i)for a TD")
                    )
                )
                .then(pl.lit("Blocked Field Goal Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Blocked Punt"])).and_(pl.col("text").str.contains("(?i)for a TD"))
                )
                .then(pl.lit("Blocked Punt Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix duplicated TD play_type labels----
                pl.col("type.text")
                .str.replace(r"(?i)Touchdown Touchdown", "Touchdown")
                .alias("type.text")
            )
            .with_columns(
                # -- Fix Pass Interception Return TD play_type labels----
                pl.when(pl.col("text").str.contains("(?i)pass intercepted for a TD"))
                .then(pl.lit("Interception Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix Sack/Fumbles Touchdown play_type labels----
                pl.when(
                    (pl.col("text").str.contains("(?i)sacked"))
                    .and_(pl.col("text").str.contains("(?i)fumbled"))
                    .and_(pl.col("text").str.contains("(?i)TD"))
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix generic pass plays ----
                ##-- first one looks for complete pass
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)pass complete")))
                .then(pl.lit("Pass Completion"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- second one looks for incomplete pass
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)pass incomplete")))
                .then(pl.lit("Pass Incompletion"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- third one looks for interceptions
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)pass intercepted")))
                .then(pl.lit("Pass Interception"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- fourth one looks for sacked
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)sacked")))
                .then(pl.lit("Sack"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- fifth one play type is Passing Touchdown, but its intercepted
                pl.when(
                    (pl.col("type.text") == "Passing Touchdown").and_(
                        pl.col("text").str.contains("(?i)pass intercepted for a TD")
                    )
                )
                .then(pl.lit("Interception Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Moving non-Touchdown pass interceptions to one play_type: "Interception Return" -----
                pl.when(pl.col("type.text").is_in(["Interception", "Pass Interception", "Pass Interception Return"]))
                .then(pl.lit("Interception Return"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Moving Kickoff/Punt Touchdowns without fumbles to Kickoff/Punt Return Touchdown
                pl.when((pl.col("type.text") == "Kickoff Touchdown").and_(pl.col("fumble_vec") == False))
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Kickoff")
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("fumble_vec") == False)
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .when(
                    (pl.col("type.text") == "Kickoff")
                    .and_(pl.col("text").str.contains("(?i)for a TD"))
                    .and_(pl.col("fumble_vec") == False)
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Kickoff", "Kickoff Return (Offense)"]))
                    .and_(pl.col("fumble_vec") == True)
                    .and_(pl.col("change_of_poss") == 1)
                )
                .then(pl.lit("Kickoff Team Fumble Recovery"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Punt Touchdown")
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("change_of_poss") == 1)
                )
                .then(pl.lit("Punt Return Touchdown"))
                .when(
                    (pl.col("type.text") == "Punt")
                    .and_(pl.col("text").str.contains("(?i)for a TD"))
                    .and_(pl.col("change_of_poss") == 1)
                )
                .then(pl.lit("Punt Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Punt")
                    .and_(pl.col("fumble_vec") == True)
                    .and_(pl.col("change_of_poss") == 0)
                )
                .then(pl.lit("Punt Team Fumble Recovery"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("type.text").is_in(["Punt Touchdown"]))
                .then(pl.lit("Punt Team Fumble Recovery Touchdown"))
                .when(
                    (pl.col("scoringPlay") == True)
                    .and_(pl.col("punt_play") == True)
                    .and_(pl.col("change_of_poss") == 0)
                )
                .then(pl.lit("Punt Team Fumble Recovery Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("type.text").is_in(["Kickoff Touchdown"]))
                .then(pl.lit("Kickoff Team Fumble Recovery Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Fumble Return Touchdown"])).and_(
                        (pl.col("pass") == True).or_(pl.col("rush") == True)
                    )
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Safeties (kickoff, punt, penalty) ----
                pl.when(
                    (pl.col("type.text").is_in(["Pass Reception", "Rush", "Rushing Touchdown"]))
                    .and_((pl.col("pass") == True).or_(pl.col("rush") == True))
                    .and_(pl.col("safety") == True)
                )
                .then(pl.lit("Safety"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("kickoff_safety") == True)
                .then(pl.lit("Kickoff (Safety)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("punt_safety") == True)
                .then(pl.lit("Punt (Safety)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("penalty_safety") == True)
                .then(pl.lit("Penalty (Safety)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when((pl.col("type.text") == "Extra Point Good").and_(pl.col("text").str.contains("(?i)Two-Point")))
                .then(pl.lit("Two-Point Conversion Good"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Extra Point Missed").and_(pl.col("text").str.contains("(?i)Two-Point"))
                )
                .then(pl.lit("Two-Point Conversion Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
        )

        return play_df

    def __setup_penalty_data(self, play_df):
        """
        Creates the following columns in play_df:
            * Penalty flag
            * Penalty declined
            * Penalty no play
            * Penalty off-set
            * Penalty 1st down conversion
            * Penalty in text
            * Yds Penalty
        """
        ##-- 'Penalty' in play text ----
        play_df = (
            play_df.with_columns(
                # -- T/F flag conditions penalty_flag
                penalty_flag=pl.when(
                    (pl.col("type.text") == "Penalty").or_(pl.col("text").str.contains("(?i)penalty"))
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_declined
                penalty_declined=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)declined"))
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_no_play
                penalty_no_play=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)no play"))
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_offset
                penalty_offset=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)off-setting"))
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)penalty")).and_(pl.col("text").str.contains("(?i)off-setting"))
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_1st_conv
                penalty_1st_conv=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)1st down"))
                )
                .then(True)
                .when((pl.col("text").str.contains("(?i)penalty")).and_(pl.col("text").str.contains("(?i)1st down")))
                .then(True)
                .otherwise(False),
                # -- T/F flag for penalty text but not penalty play type --
                penalty_in_text=pl.when(
                    (pl.col("text").str.contains("(?i)penalty")).and_(
                        pl.col("type.text") != "Penalty",
                        pl.col("text").str.contains("(?i)declined") == False,
                        pl.col("text").str.contains("(?i)off-setting") == False,
                        pl.col("text").str.contains("(?i)no play") == False,
                    )
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                penalty_detail=pl.when(pl.col("penalty_offset") == 1)
                .then(pl.lit("Offsetting"))
                .when(pl.col("penalty_declined") == 1)
                .then(pl.lit("Declined"))
                .when(pl.col("text").str.contains("(?i)roughing passer"))
                .then(pl.lit("Roughing the Passer"))
                .when(pl.col("text").str.contains("(?i)offensive holding"))
                .then(pl.lit("Offensive Holding"))
                .when(pl.col("text").str.contains("(?i)pass interference"))
                .then(pl.lit("Pass Interference"))
                .when(pl.col("text").str.contains("(?i)encroachment"))
                .then(pl.lit("Encroachment"))
                .when(pl.col("text").str.contains("(?i)defensive pass interference"))
                .then(pl.lit("Defensive Pass Interference"))
                .when(pl.col("text").str.contains("(?i)offensive pass interference"))
                .then(pl.lit("Offensive Pass Interference"))
                .when(pl.col("text").str.contains("(?i)illegal procedure"))
                .then(pl.lit("Illegal Procedure"))
                .when(pl.col("text").str.contains("(?i)defensive holding"))
                .then(pl.lit("Defensive Holding"))
                .when(pl.col("text").str.contains("(?i)holding"))
                .then(pl.lit("Holding"))
                .when(pl.col("text").str.contains("(?i)offensive offside|(?i)offside offense"))
                .then(pl.lit("Offensive Offside"))
                .when(pl.col("text").str.contains("(?i)defensive offside|(?i)offside defense"))
                .then(pl.lit("Defensive Offside"))
                .when(pl.col("text").str.contains("(?i)offside"))
                .then(pl.lit("Offside"))
                .when(pl.col("text").str.contains("(?i)illegal fair catch signal"))
                .then(pl.lit("Illegal Fair Catch Signal"))
                .when(pl.col("text").str.contains("(?i)illegal batting"))
                .then(pl.lit("Illegal Batting"))
                .when(pl.col("text").str.contains("(?i)neutral zone infraction"))
                .then(pl.lit("Neutral Zone Infraction"))
                .when(pl.col("text").str.contains("(?i)ineligible downfield"))
                .then(pl.lit("Ineligible Downfield"))
                .when(pl.col("text").str.contains("(?i)illegal use of hands"))
                .then(pl.lit("Illegal Use of Hands"))
                .when(pl.col("text").str.contains("(?i)kickoff out of bounds|(?i)kickoff out-of-bounds"))
                .then(pl.lit("Kickoff Out of Bounds"))
                .when(pl.col("text").str.contains("(?i)12 men on the field"))
                .then(pl.lit("12 Men on the Field"))
                .when(pl.col("text").str.contains("(?i)illegal block"))
                .then(pl.lit("Illegal Block"))
                .when(pl.col("text").str.contains("(?i)personal foul"))
                .then(pl.lit("Personal Foul"))
                .when(pl.col("text").str.contains("(?i)false start"))
                .then(pl.lit("False Start"))
                .when(pl.col("text").str.contains("(?i)substitution infraction"))
                .then(pl.lit("Substitution Infraction"))
                .when(pl.col("text").str.contains("(?i)illegal formation"))
                .then(pl.lit("Illegal Formation"))
                .when(pl.col("text").str.contains("(?i)illegal touching"))
                .then(pl.lit("Illegal Touching"))
                .when(pl.col("text").str.contains("(?i)sideline interference"))
                .then(pl.lit("Sideline Interference"))
                .when(pl.col("text").str.contains("(?i)clipping"))
                .then(pl.lit("Clipping"))
                .when(pl.col("text").str.contains("(?i)sideline infraction"))
                .then(pl.lit("Sideline Infraction"))
                .when(pl.col("text").str.contains("(?i)crackback"))
                .then(pl.lit("Crackback"))
                .when(pl.col("text").str.contains("(?i)illegal snap"))
                .then(pl.lit("Illegal Snap"))
                .when(pl.col("text").str.contains("(?i)illegal helmet contact"))
                .then(pl.lit("Illegal Helmet Contact"))
                .when(pl.col("text").str.contains("(?i)roughing holder"))
                .then(pl.lit("Roughing the Holder"))
                .when(pl.col("text").str.contains("(?i)horse collar tackle"))
                .then(pl.lit("Horse Collar Tackle"))
                .when(pl.col("text").str.contains("(?i)illegal participation"))
                .then(pl.lit("Illegal Participation"))
                .when(pl.col("text").str.contains("(?i)tripping"))
                .then(pl.lit("Tripping"))
                .when(pl.col("text").str.contains("(?i)illegal shift"))
                .then(pl.lit("Illegal Shift"))
                .when(pl.col("text").str.contains("(?i)illegal motion"))
                .then(pl.lit("Illegal Motion"))
                .when(pl.col("text").str.contains("(?i)roughing the kicker"))
                .then(pl.lit("Roughing the Kicker"))
                .when(pl.col("text").str.contains("(?i)delay of game"))
                .then(pl.lit("Delay of Game"))
                .when(pl.col("text").str.contains("(?i)targeting"))
                .then(pl.lit("Targeting"))
                .when(pl.col("text").str.contains("(?i)face mask"))
                .then(pl.lit("Face Mask"))
                .when(pl.col("text").str.contains("(?i)illegal forward pass"))
                .then(pl.lit("Illegal Forward Pass"))
                .when(pl.col("text").str.contains("(?i)intentional grounding"))
                .then(pl.lit("Intentional Grounding"))
                .when(pl.col("text").str.contains("(?i)illegal kicking"))
                .then(pl.lit("Illegal Kicking"))
                .when(pl.col("text").str.contains("(?i)illegal conduct"))
                .then(pl.lit("Illegal Conduct"))
                .when(pl.col("text").str.contains("(?i)kick catching interference"))
                .then(pl.lit("Kick Catch Interference"))
                .when(pl.col("text").str.contains("(?i)kick catch interference"))
                .then(pl.lit("Kick Catch Interference"))
                .when(pl.col("text").str.contains("(?i)unnecessary roughness"))
                .then(pl.lit("Unnecessary Roughness"))
                .when(pl.col("text").str.contains("(?i)Penalty, UR"))
                .then(pl.lit("Unnecessary Roughness"))
                .when(pl.col("text").str.contains("(?i)roughing the snapper"))
                .then(pl.lit("Roughing the Snapper"))
                .when(pl.col("text").str.contains("(?i)illegal blindside block"))
                .then(pl.lit("Illegal Blindside Block"))
                .when(pl.col("text").str.contains("(?i)unsportsmanlike conduct"))
                .then(pl.lit("Unsportsmanlike Conduct"))
                .when(pl.col("text").str.contains("(?i)running into kicker"))
                .then(pl.lit("Running Into Kicker"))
                .when(pl.col("text").str.contains("(?i)failure to wear required equipment"))
                .then(pl.lit("Failure to Wear Required Equipment"))
                .when(pl.col("text").str.contains("(?i)player disqualification"))
                .then(pl.lit("Player Disqualification"))
                .when(pl.col("penalty_flag") == True)
                .then(pl.lit("Missing"))
            )
            .with_columns(
                penalty_text=pl.when(pl.col("penalty_flag") == True)
                .then(pl.col("text").str.extract(r"(?i)Penalty(.+)", 1))
                .otherwise(None),
            )
            .with_columns(
                yds_penalty=pl.when(pl.col("penalty_flag") == True)
                .then(
                    pl.col("penalty_text")
                    .str.extract(r"(?i)(.{0,3}) yards|(?i)yds|(?i)yd to the", 1)
                    .str.replace(" yards to the | yds to the | yd to the ", "")
                )
                .otherwise(None),
            )
            .with_columns(
                yds_penalty=pl.when(
                    (pl.col("penalty_flag") == True).and_(
                        pl.col("yds_penalty").is_null(), pl.col("text").str.contains(r"(?i)ards\)")
                    )
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(.{0,4})yards\)|Yards\)|yds\)|Yds\)", 1)
                    .str.replace("yards\\)|Yards\\)|yds\\)|Yds\\)", "")
                    .str.replace("\\(", "")
                )
                .otherwise(pl.col("yds_penalty")),
            )
        )

        return play_df

    def __add_play_category_flags(self, play_df):
        play_df = (
            play_df.with_columns(
                # --- Sacks -----
                sack=pl.when(pl.col("type.text").is_in(["Sack"]))
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ]
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked"))
                )
                .then(True)
                .when((pl.col("type.text").is_in(["Safety"])).and_(pl.col("text").str.contains("(?i)sacked")))
                .then(True)
                .otherwise(False),
                # --- Interceptions ------
                int=pl.col("type.text").is_in(["Interception Return", "Interception Return Touchdown"]),
                int_td=pl.col("type.text").is_in(["Interception Return Touchdown"]),
                # --- Pass Completions, Attempts and Targets -------
                completion=pl.when(
                    pl.col("type.text").is_in(["Pass Reception", "Pass Completion", "Passing Touchdown"])
                )
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ]
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False)
                )
                .then(True)
                .otherwise(False),
                pass_attempt=pl.when(
                    pl.col("type.text").is_in(
                        ["Pass Reception", "Pass Completion", "Passing Touchdown", "Pass Incompletion"]
                    )
                )
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ]
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False)
                )
                .then(True)
                .when((pl.col("pass") == True).and_(pl.col("text").str.contains("(?i)sacked") == False))
                .then(True)
                .otherwise(False),
                target=pl.when(
                    pl.col("type.text").is_in(
                        ["Pass Reception", "Pass Completion", "Passing Touchdown", "Pass Incompletion"]
                    )
                )
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ]
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False)
                )
                .then(True)
                .when((pl.col("pass") == True).and_(pl.col("text").str.contains("(?i)sacked") == False))
                .then(True)
                .otherwise(False),
                pass_breakup=pl.when(pl.col("text").str.contains("(?i)broken up by")).then(True).otherwise(False),
                # --- Pass/Rush TDs ------
                pass_td=pl.when(pl.col("type.text").is_in(["Passing Touchdown"]))
                .then(True)
                .when((pl.col("pass") == True).and_(pl.col("td_play") == True))
                .then(True)
                .otherwise(False),
                rush_td=pl.when(pl.col("type.text").is_in(["Rushing Touchdown"]))
                .then(True)
                .when((pl.col("rush") == True).and_(pl.col("td_play") == True))
                .then(True)
                .otherwise(False),
                # --- Change of possession via turnover
                turnover_vec=pl.col("type.text").is_in(turnover_vec),
                offense_score_play=pl.col("type.text").is_in(offense_score_vec),
                defense_score_play=pl.col("type.text").is_in(defense_score_vec),
                downs_turnover=pl.when(
                    (pl.col("type.text").is_in(normalplay))
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4)
                    .and_(pl.col("penalty_1st_conv") == False)
                )
                .then(True)
                .otherwise(False),
                # --- Touchdowns ----
                scoring_play=pl.col("type.text").is_in(scores_vec),
                yds_punted=pl.col("text").str.extract(r"(?i)(punt for \d+)").str.extract(r"(\d+)").cast(pl.Int32),
                yds_punt_gained=pl.when(pl.col("punt") == True).then(pl.col("statYardage")).otherwise(None),
                fg_attempt=pl.when(
                    (pl.col("type.text").str.contains(r"(?i)Field Goal")).or_(
                        pl.col("text").str.contains(r"(?i)Field Goal")
                    )
                )
                .then(True)
                .otherwise(False),
                fg_made=pl.col("type.text") == "Field Goal Good",
                yds_fg=pl.col("text")
                .str.extract(
                    r"(?i)(\d+)\s?Yd Field|(?i)(\d+)\s?YD FG|(?i)(\d+)\s?Yard FG|(?i)(\d+)\s?Field|(?i)(\d+)\s?Yard Field",
                    0,
                )
                .str.extract(r"(\d+)")
                .cast(pl.Int32),
            )
            .with_columns(
                pl.when(pl.col("fg_attempt") == True)
                .then(pl.col("yds_fg") - 17)
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone"),
            )
            .with_columns(
                pl.when(
                    (pl.col("start.yardsToEndzone").is_null())
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False)
                    .and_(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                )
                .then(100 - pl.col("start.yardLine").cast(pl.Int32))
                .when(
                    (pl.col("start.yardsToEndzone").is_null())
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False)
                    .and_(pl.col("start.pos_team.id") == pl.col("awayTeamId"))
                )
                .then(pl.col("start.yardLine").cast(pl.Int32))
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone"),
            )
            .with_columns(
                pos_unit=pl.when(pl.col("punt") == True)
                .then(pl.lit("Punt Offense"))
                .when(pl.col("kickoff_play") == True)
                .then(pl.lit("Kickoff Return"))
                .when(pl.col("fg_attempt") == True)
                .then(pl.lit("Field Goal Offense"))
                .when(pl.col("type.text") == "Defensive 2pt Conversion")
                .then(pl.lit("Offense"))
                .otherwise(pl.lit("Offense")),
                def_pos_unit=pl.when(pl.col("punt") == True)
                .then(pl.lit("Punt Return"))
                .when(pl.col("kickoff_play") == True)
                .then(pl.lit("Kickoff Defense"))
                .when(pl.col("fg_attempt") == True)
                .then(pl.lit("Field Goal Defense"))
                .when(pl.col("type.text") == "Defensive 2pt Conversion")
                .then(pl.lit("Defense"))
                .otherwise(pl.lit("Defense")),
                # --- Lags/Leads play type ----
                lead_play_type=pl.col("type.text").shift(-1),
                sp=pl.when(
                    (pl.col("fg_attempt") == True).or_(pl.col("punt") == True).or_(pl.col("kickoff_play") == True)
                )
                .then(True)
                .otherwise(False),
                play=pl.when(pl.col("type.text").is_in(["Timeout", "End Period", "End of Half", "Penalty"]) == False)
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                scrimmage_play=pl.when(
                    (pl.col("sp") == False).and_(
                        pl.col("type.text").is_in(
                            [
                                "Timeout",
                                "Extra Point Good",
                                "Extra Point Missed",
                                "Two-Point Pass",
                                "Two-Point Rush",
                                "Penalty",
                            ]
                        )
                        == False
                    )
                )
                .then(True)
                .otherwise(False),
                # --- Change of pos_team by lead('pos_team', 1)----
                change_of_pos_team=pl.when(
                    (pl.col("pos_team") == pl.col("lead_pos_team")).and_(
                        ((pl.col("lead_play_type").is_in(["End Period", "End of Half"])) == False).or_(
                            pl.col("lead_play_type").is_null()
                        )
                    )
                )
                .then(False)
                .when(
                    (pl.col("pos_team") == pl.col("lead_pos_team2")).and_(
                        (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).or_(
                            pl.col("lead_play_type").is_null()
                        )
                    )
                )
                .then(False)
                .otherwise(True),
            )
            .with_columns(
                change_of_pos_team=pl.when(pl.col("change_of_poss").is_null())
                .then(False)
                .otherwise(pl.col("change_of_pos_team")),
                pos_score_diff_end=pl.when(
                    (
                        (pl.col("type.text").is_in(end_change_vec)).and_(
                            pl.col("start.pos_team.id") != pl.col("end.pos_team.id")
                        )
                    ).or_(pl.col("downs_turnover") == True)
                )
                .then(-1 * pl.col("pos_score_diff"))
                .otherwise(pl.col("pos_score_diff")),
            )
            .with_columns(
                pos_score_diff_end=pl.when(
                    (pl.col("pos_score_pts").abs() >= 8)
                    .and_(pl.col("scoring_play") == False)
                    .and_(pl.col("change_of_pos_team") == False)
                )
                .then(pl.col("pos_score_diff_start"))
                .when(
                    (pl.col("pos_score_pts").abs() >= 8)
                    .and_(pl.col("scoring_play") == False)
                    .and_(pl.col("change_of_pos_team") == True)
                )
                .then(-1 * pl.col("pos_score_diff_start"))
                .otherwise(pl.col("pos_score_diff_end")),
                fumble_lost=pl.when((pl.col("fumble_vec") == True).and_(pl.col("change_of_pos_team") == True))
                .then(True)
                .otherwise(False),
                fumble_recovered=pl.when((pl.col("fumble_vec") == True).and_(pl.col("change_of_pos_team") == False))
                .then(True)
                .otherwise(False),
            )
        )

        return play_df

    def __add_yardage_cols(self, play_df):
        play_df = play_df.with_columns(
            yds_rushed=pl.when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)run for no gain")))
            .then(0)
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)for no gain")))
            .then(0)
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)run for a loss of")))
            .then(-1 * pl.col("text").str.extract(r"(?i)run for a loss of (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)rush for a loss of")))
            .then(-1 * pl.col("text").str.extract(r"(?i)rush for a loss of (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)run for")))
            .then(pl.col("text").str.extract(r"(?i)run for (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)rush for")))
            .then(pl.col("text").str.extract(r"(?i)rush for (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)Yd Run")))
            .then(pl.col("text").str.extract(r"(?i)(\d+) Yd Run").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)Yd Rush")))
            .then(pl.col("text").str.extract(r"(?i)(\d+) Yd Rush").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("text").str.contains("(?i)Yard Rush")))
            .then(pl.col("text").str.extract(r"(?i)(\d+) Yard Rush").cast(pl.Int32))
            .when(
                (pl.col("rush") == True)
                .and_(pl.col("text").str.contains("(?i)rushed"))
                .and_(pl.col("text").str.contains("(?i)touchdown") == False)
            )
            .then(pl.col("text").str.extract(r"(?i)for (\d+) yards").cast(pl.Int32))
            .when(
                (pl.col("rush") == True)
                .and_(pl.col("text").str.contains("(?i)rushed"))
                .and_(pl.col("text").str.contains("(?i)touchdown") == True)
            )
            .then(pl.col("text").str.extract(r"(?i)for a (\d+) yard").cast(pl.Int32))
            .otherwise(None),
            yds_receiving=pl.when(
                (pl.col("pass") == True)
                .and_(pl.col("text").str.contains(r"(?i)complete to"))
                .and_(pl.col("text").str.contains(r"(?i)for no gain"))
            )
            .then(0)
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("text").str.contains(r"(?i)complete to"))
                .and_(pl.col("text").str.contains(r"(?i)for a loss of"))
            )
            .then(-1 * pl.col("text").str.extract(r"(?i)for a loss of (\d+)").cast(pl.Int32))
            .when((pl.col("pass") == True).and_(pl.col("text").str.contains(r"(?i)complete to")))
            .then(pl.col("text").str.extract(r"(?i)for (\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True).and_(
                    pl.col("text").str.contains(r"(?i)incomplete|(?i) sacked|(?i)intercepted|(?i)pass defensed")
                )
            )
            .then(0)
            .when((pl.col("pass") == True).and_(pl.col("text").str.contains(r"(?i)incompletion")))
            .then(0)
            .when((pl.col("pass") == True).and_(pl.col("text").str.contains(r"(?i)Yd pass")))
            .then(pl.col("text").str.extract(r"(?i)(\d+) Yd pass").cast(pl.Int32))
            .otherwise(None),
            yds_int_return=pl.when(
                (pl.col("pass") == True)
                .and_(pl.col("int_td") == True)
                .and_(pl.col("text").str.contains(r"(?i)Yd Interception Return"))
            )
            .then(pl.col("text").str.extract(r"(?i)(.+)Yd Interception Return").str.extract(r"(\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("int") == True)
                .and_(pl.col("text").str.contains(r"(?i)for no gain"))
            )
            .then(0)
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("int") == True)
                .and_(pl.col("text").str.contains(r"(?i)for a loss of"))
            )
            .then(-1 * pl.col("text").str.extract(r"(?i)for a loss of (\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True).and_(pl.col("int") == True).and_(pl.col("text").str.contains(r"(?i)for a TD"))
            )
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("pass") == True).and_(pl.col("int") == True))
            .then(
                pl.col("text")
                .str.replace("for a 1st", "")
                .str.extract(r"(?i)for (.+)")
                .str.extract(r"(\d+)")
                .cast(pl.Int32)
            )
            .otherwise(None),
            yds_kickoff=pl.when(pl.col("kickoff_play") == True)
            .then(pl.col("text").str.extract(r"(?i)kickoff for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_kickoff_return=pl.when(
                (pl.col("kickoff_play") == True).and_(pl.col("kickoff_tb") == True).and_(pl.col("season") > 2013)
            )
            .then(25)
            .when((pl.col("kickoff_play") == True).and_(pl.col("kickoff_tb") == True).and_(pl.col("season") <= 2013))
            .then(20)
            .when(
                (pl.col("kickoff_play") == True)
                .and_(pl.col("fumble_vec") == False)
                .and_(pl.col("text").str.contains(r"(?i)for no gain|fair catch|fair caught"))
            )
            .then(0)
            .when(
                (pl.col("kickoff_play") == True)
                .and_(pl.col("fumble_vec") == False)
                .and_(pl.col("text").str.contains(r"(?i)out-of-bounds|out of bounds"))
            )
            .then(40)
            .when((pl.col("kickoff_downed") == True).or_(pl.col("kickoff_fair_catch") == True))
            .then(0)
            .when((pl.col("kickoff_play") == True).and_(pl.col("text").str.contains(r"(?i)returned by")))
            .then(pl.col("text").str.extract(r"(?i)returned by (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("kickoff_play") == True).and_(pl.col("text").str.contains(r"(?i)return for")))
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_punted=pl.when((pl.col("punt") == True).and_(pl.col("punt_blocked") == True))
            .then(0)
            .when(pl.col("punt") == True)
            .then(pl.col("text").str.extract(r"(?i)punt for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_punt_return=pl.when((pl.col("punt") == True).and_(pl.col("punt_tb") == True))
            .then(20)
            .when((pl.col("punt") == True).and_(pl.col("text").str.contains(r"(?i)fair catch|fair caught")))
            .then(0)
            .when(
                (pl.col("punt") == True).and_(
                    (pl.col("punt_downed") == True)
                    .or_(pl.col("punt_oob") == True)
                    .or_(pl.col("punt_fair_catch") == True)
                )
            )
            .then(0)
            .when((pl.col("punt") == True).and_(pl.col("text").str.contains(r"(?i)no return|no gain")))
            .then(0)
            .when((pl.col("punt") == True).and_(pl.col("text").str.contains(r"(?i)returned \d+ yards")))
            .then(pl.col("text").str.extract(r"(?i)returned (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("punt") == True).and_(pl.col("punt_blocked") == False))
            .then(pl.col("text").str.extract(r"(?i)returns for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("punt") == True).and_(pl.col("punt_blocked") == True))
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_fumble_return=pl.when((pl.col("fumble_vec") == True).and_(pl.col("kickoff_play") == False))
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_sacked=pl.when(pl.col("sack") == True)
            .then(-1 * pl.col("text").str.extract(r"(?i)sacked (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
        ).with_columns(
            yds_penalty=pl.when(pl.col("penalty_detail").is_in(["Penalty Declined", "Penalty Offset"]))
            .then(0)
            .when(pl.col("yds_penalty").is_not_null())
            .then(pl.col("yds_penalty"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("rush") == True)
            )
            .then(pl.col("statYardage") - pl.col("yds_rushed"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("int") == True)
            )
            .then(pl.col("statYardage") - pl.col("yds_int_return"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == False)
                .and_(pl.col("type.text") != "Pass Incompletion")
            )
            .then(pl.col("statYardage") - pl.col("yds_receiving"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == False)
                .and_(pl.col("type.text") == "Pass Incompletion")
            )
            .then(pl.col("statYardage"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == True)
            )
            .then(pl.col("statYardage") - pl.col("yds_sacked"))
            .when(pl.col("type.text") == "Penalty")
            .then(pl.col("statYardage"))
            .otherwise(None),
        )
        return play_df

    def __add_player_cols(self, play_df):
        play_df = (
            play_df.with_columns(
                # --- RB Names -----
                rush_player=pl.when(pl.col("rush") == True)
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i)(.{0,25} )run |(?i)(.{0,25} )\d{0,2} Yd Run|(?i)(.{0,25} )rush |(?i)(.{0,25} )rushed "
                    )
                    .str.replace(r"(?i) run |(?i) \d+ Yd Run|(?i) rush ", "")
                    .str.replace(r" \((.+)\)", "")
                )
                .otherwise(None),
                # --- QB Names -----
                pass_player=pl.when(
                    (pl.col("pass") == True)
                    .and_(pl.col("sack_vec") == False)
                    .and_(pl.col("type.text") != "Passing Touchdown")
                )
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i)(.{0,30} )pass |(?i)(.{0,30} )sacked by|(?i)(.{0,30} )sacked for|(?i)(.{0,30} )incomplete|(?i)pass from (.{0,30} ) \( "
                    )
                    .str.replace(r"(?i)pass |(?i) sacked by|(?i) sacked for|(?i) incomplete", "")
                )
                .when(
                    (pl.col("pass") == True)
                    .and_(pl.col("sack_vec") == True)
                    .and_(pl.col("type.text") != "Passing Touchdown")
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.{0,30} )sacked by|(?i)(.{0,30} )sacked for")
                    .str.replace(r"(?i)pass |(?i) sacked by|(?i) sacked for|(?i) incomplete", "")
                )
                .when((pl.col("pass") == True).and_(pl.col("type.text") == "Passing Touchdown"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)pass from(.+)")
                    .str.replace(r"pass from", "")
                    # .str.replace(r"\((.+)\)", "")
                    .str.replace(r" \,", "")
                )
                .otherwise(None),
            )
            .with_columns(
                pass_player=pl.when((pl.col("type.text") == "Passing Touchdown").and_(pl.col("pass_player").is_null()))
                .then(
                    pl.col("text")
                    .str.extract(r"(.+)pass(.+)? complete to")
                    .str.replace(r" pass complete to(.+)", "")
                    .str.replace(r" pass complete to", "")
                )
                .otherwise(pl.col("pass_player"))
            )
            .with_columns(
                pass_player=pl.when((pl.col("type.text") == "Passing Touchdown").and_(pl.col("pass_player").is_null()))
                .then(
                    pl.col("text")
                    .str.extract(r"(.+)pass,to")
                    .str.replace(r" pass,to(.+)", "")
                    .str.replace(r" pass,to", "")
                    .str.replace(r" \((.+)\)", "")
                )
                .otherwise(pl.col("pass_player"))
            )
            .with_columns(
                pass_player=pl.when(
                    (pl.col("pass") == True).and_(
                        ((pl.col("pass_player").str.strip().str.n_chars() == 0).or_(pl.col("pass_player").is_null()))
                    )
                )
                .then(pl.lit("TEAM"))
                .otherwise(pl.col("pass_player")),
                # --- WR Names -----
                receiver_player=pl.when(
                    (pl.col("pass") == True).and_(pl.col("text").str.contains(r"(?i)sacked") == False)
                )
                .then(pl.col("text").str.extract(r"(?i)to (.+)"))
                .when(pl.col("text").str.contains(r"(?i)Yd pass"))
                .then(pl.col("text").str.extract(r"(?i)(.{0,25} )\d{0,2} Yd pass"))
                .when(pl.col("text").str.contains(r"(?i)Yd TD pass"))
                .then(pl.col("text").str.extract(r"(?i)(.{0,25} )\d{0,2} Yd TD pass"))
                .otherwise(None),
            )
            .with_columns(
                receiver_player=pl.when(
                    (pl.col("type.text") == "Sack")
                    .or_(pl.col("type.text") == "Interception Return")
                    .or_(pl.col("type.text") == "Interception Return Touchdown")
                    .or_(
                        (
                            pl.col("type.text").is_in(
                                ["Fumble Recovery (Opponent) Touchdown", "Fumble Recovery (Opponent)"]
                            )
                        ).and_(pl.col("text").str.contains(r"(?i)sacked"))
                    )
                )
                .then(None)
                .otherwise(
                    pl.col("receiver_player")
                    .str.replace(r"to ", "")
                    .str.replace(r"(?i)\\,.+", "")
                    .str.replace(r"(?i)for (.+)", "")
                    .str.replace(r"(?i) (\d{1,2})", "")
                    .str.replace(r"(?i) Yd pass", "")
                    .str.replace(r"(?i) Yd TD pass", "")
                    .str.replace(r"(?i)pass complete to", "")
                    .str.replace(r"(?i)penalty", "")
                    .str.replace(r'(?i) "', "")
                )
            )
            .with_columns(
                receiver_player=pl.when(pl.col("receiver_player").str.contains(r"(?i)III") == True)
                .then(pl.col("receiver_player").str.replace(r"(?i)[A-Z]{3,}", ""))
                .otherwise(pl.col("receiver_player"))
            )
            .with_columns(
                receiver_player=pl.col("receiver_player")
                .str.replace(r"(?i) &", "")
                .str.replace(r"(?i)A&M", "")
                .str.replace(r"(?i) ST", "")
                .str.replace(r"(?i) GA", "")
                .str.replace(r"(?i) UL", "")
                .str.replace(r"(?i) FL", "")
                .str.replace(r"(?i) OH", "")
                .str.replace(r"(?i) NC", "")
                .str.replace(r'(?i) "', "")
                .str.replace(r"(?i) \\u00c9", "")
                .str.replace(r"(?i) fumbled,", "")
                .str.replace(r"(?i)the (.+)", "")
                .str.replace(r"(?i)pass incomplete to", "")
                .str.replace(r"(?i)(.+)pass incomplete", "")
                .str.replace(r"(?i)pass incomplete", "")
                .str.replace(r"(?i) \((.+)\)", ""),
                # --- Sack Names -----
                sack_players=pl.when(
                    (pl.col("sack") == True).or_((pl.col("fumble_vec") == True).and_(pl.col("pass") == True))
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)sacked by(.+)")
                    .str.replace(r"for (.+)", "")
                    .str.replace(r"(.+) by ", "")
                    .str.replace(r" at the (.+)", "")
                )
                .otherwise(None),
            )
            .with_columns(
                sack_player1=pl.col("sack_players").str.replace(r"and (.+)", ""),
                sack_player2=pl.when(pl.col("sack_players").str.contains(r"and (.+)"))
                .then(pl.col("sack_players").str.replace(r"(.+) and", ""))
                .otherwise(None),
                # --- Interception Names -----
                interception_player=pl.when(
                    (
                        (pl.col("type.text") == "Interception Return").or_(
                            pl.col("type.text") == "Interception Return Touchdown"
                        )
                    ).and_(pl.col("pass") == True)
                )
                .then(pl.col("text").str.extract(r"(?i)intercepted (.+)"))
                .when(pl.col("text").str.contains(r"Yd Interception Return"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i)(.{0,25} )\\d{0,2} Yd Interception Return|(?i)(.{0,25} )\\d{0,2} yd interception return"
                    )
                    .str.replace(r"return (.+)", "")
                    .str.replace(r"(.+) intercepted", "")
                    .str.replace(r"intercepted", "")
                    .str.replace(r"Yd Interception Return", "")
                    .str.replace(r"for a 1st down", "")
                    .str.replace(r"(\\d{1,2})", "")
                    .str.replace(r"for a TD", "")
                    .str.replace(r"at the (.+)", "")
                    .str.replace(r" by ", "")
                )
                .otherwise(None),
                # --- Pass Breakup Players ----
                pass_breakup_player=pl.when(pl.col("pass") == True)
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)broken up by (.+)")
                    .str.replace(r"(.+) broken up by", "")
                    .str.replace(r"broken up by", "")
                    .str.replace(r"Penalty(.+)", "")
                    .str.replace(r"SOUTH FLORIDA", "")
                    .str.replace(r"WEST VIRGINIA", "")
                    .str.replace(r"MISSISSIPPI ST", "")
                    .str.replace(r"CAMPBELL", "")
                    .str.replace(r"COASTL CAROLINA", "")
                )
                .otherwise(None),
                # --- Punter Names ----
                punter_player=pl.when(pl.col("type.text").str.contains("Punt"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.{0,30}) punt|(?i)Punt by (.{0,30})")
                    .str.replace(r"(?i) punt", "")
                    .str.replace(r"(?i) for(.+)", "")
                    .str.replace(r"(?i)Punt by ", "")
                    .str.replace(r"(?i)\((.+)\)", "")
                    .str.replace(r"(?i) returned \d+", "")
                    .str.replace(r"(?i) returned", "")
                    .str.replace(r"(?i) no return", "")
                )
                .otherwise(None),
                # --- Punt Returner Names ----
                punt_return_player=pl.when(pl.col("type.text").str.contains("Punt"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i), (.{0,25}) returns|(?i)fair catch by (.{0,25})|(?i), returned by (.{0,25})|(?i)yards by (.{0,30})|(?i) return by (.{0,25})"
                    )
                    .str.replace(r"(?i), ", "")
                    .str.replace(r"(?i) returns", "")
                    .str.replace(r"(?i) returned", "")
                    .str.replace(r"(?i) return", "")
                    .str.replace(r"(?i)fair catch by", "")
                    .str.replace(r"(?i) at (.+)", "")
                    .str.replace(r"(?i) for (.+)", "")
                    .str.replace(r"(?i)(.+) by ", "")
                    .str.replace(r"(?i) to (.+)", "")
                    .str.replace(r"(?i)\((.+)\)", "")
                )
                .otherwise(None),
                # --- Punt Blocker Names ----
                punt_block_player=pl.when(pl.col("type.text").str.contains("Punt"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)punt blocked by (.{0,25})|(?i)blocked by(.+)")
                    .str.replace(r"punt blocked by |for a(.+)", "")
                    .str.replace(r"blocked by(.+)", "")
                    .str.replace(r"blocked(.+)", "")
                    .str.replace(r" for(.+)", "")
                    .str.replace(r",(.+)", "")
                    .str.replace(r"punt blocked by |for a(.+)", "")
                )
                .otherwise(None),
            )
            .with_columns(
                punt_block_player=pl.when((pl.col("type.text").str.contains(r"(?i)yd return of blocked punt")))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.+) yd return of blocked")
                    .str.replace(r"(?i)blocked|(?i)Blocked", "")
                    .str.replace(r"(?i)\\d+", "")
                    .str.replace(r"(?i)yd return of", "")
                )
                .otherwise(pl.col("punt_block_player")),
                # --- Punt Block Returner Names ----
                punt_block_return_player=pl.when(
                    (pl.col("type.text").str.contains(r"Punt"))
                    .and_(pl.col("text").str.contains(r"(?i)blocked"))
                    .and_(pl.col("text").str.contains(r"(?i)return"))
                )
                .then(pl.col("text").str.extract(r"(?i)(.+) return"))
                .otherwise(None),
            )
            .with_columns(
                punt_block_return_player=pl.struct(["punt_block_player", "punt_block_return_player"]).apply(
                    lambda cols: cols["punt_block_return_player"]
                    .replace(r"(?i)(.+)blocked by", "")
                    .replace(str(pl.format(r"(?i)blocked by {}", cols["punt_block_player"])), "")
                    if cols["punt_block_return_player"] is not None
                    else None,
                    return_dtype=pl.Utf8,
                )
            )
            .with_columns(
                punt_block_return_player=pl.col("punt_block_return_player")
                .str.replace(r"(?i)return(.+)", "")
                .str.replace(r"(?i)return", "")
                .str.replace(r"for a TD(.+)|for a SAFETY(.+)", "")
                .str.replace(r"(?i)blocked by ", "")
                .str.replace(r", ", ""),
                # --- Kickoff Names ----
                kickoff_player=pl.when(pl.col("type.text").str.contains(r"(?i)kickoff"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.{0,25}) kickoff|(.{0,25}) on-side")
                    .str.replace(r"(?i) on-side| kickoff", "")
                )
                .otherwise(None),
                # --- Kickoff Returner Names ----
                kickoff_return_player=pl.when(pl.col("type.text").str.contains(r"(?i)ickoff"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i), (.{0,25}) return|(?i), (.{0,25}) fumble|(?i)returned by (.{0,25})|(?i)touchback by (.{0,25})"
                    )
                    .str.replace(r", ", "")
                    .str.replace(r"(?i) return|(?i) fumble|(?i) returned by|(?i) for |(?i)touchback by ", "")
                    .str.replace(r"\((.+)\)(.+)", "")
                )
                .otherwise(None),
                # --- Field Goal Kicker Names ----
                fg_kicker_player=pl.when(pl.col("type.text").str.contains(r"(?i)Field Goal"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i)(.{0,25} )\\d{0,2} yd field goal|(?i)(.{0,25} )\\d{0,2} yd fg|(?i)(.{0,25} )\\d{0,2} yard field goal"
                    )
                    .str.replace(r"(?i) Yd Field Goal|(?i)Yd FG |(?i)yd FG|(?i) yd FG", "")
                    .str.replace(r"(\\d{1,2})", "")
                )
                .otherwise(None),
                # --- Field Goal Blocker Names ----
                fg_block_player=pl.when(pl.col("type.text").str.contains(r"(?i)Field Goal"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)blocked by (.{0,25})")
                    .str.replace(r",(.+)", "")
                    .str.replace(r"blocked by ", "")
                    .str.replace(r"  (.)+", "")
                )
                .otherwise(None),
                # --- Field Goal Returner Names ----
                fg_return_player=pl.when(
                    (pl.col("type.text").str.contains(r"(?i)Field Goal"))
                    .and_(pl.col("text").str.contains(r"(?i)blocked by|missed"))
                    .and_(pl.col("text").str.contains(r"(?i)return"))
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)  (.+)")
                    .str.replace(r"(?i),(.+)", "")
                    .str.replace(r"(?i)return ", "")
                    .str.replace(r"(?i)returned ", "")
                    .str.replace(r"(?i) for (.+)", "")
                    .str.replace(r"(?i) for (.+)", "")
                )
                .otherwise(None),
            )
            .with_columns(
                fg_return_player=pl.when(
                    (pl.col("type.text").is_in(["Missed Field Goal Return", "Missed Field Goal Return Touchdown"]))
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.+)return")
                    .str.replace(r"(?i) return", "")
                    .str.replace(r"(?i)(.+),", "")
                )
                .otherwise(pl.col("fg_return_player")),
                # --- Fumble Recovery Names ----
                fumble_player=pl.when(pl.col("text").str.contains(r"(?i)fumble"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.{0,25} )fumble|(?i)(.{0,25} )fumble")
                    .str.replace(r"(?i) fumble(.+)", "")
                    .str.replace(r"(?i)fumble", "")
                    .str.replace(r"(?i) yds", "")
                    .str.replace(r"(?i) yd", "")
                    .str.replace(r"(?i)yardline", "")
                    .str.replace(r"(?i) yards|(?i) yard|(?i)for a TD|(?i)or a safety", "")
                    .str.replace(r"(?i) for ", "")
                    .str.replace(r"(?i) a safety", "")
                    .str.replace(r"(?i)r no gain", "")
                    .str.replace(r"(?i)(.+)(\\d{1,2})", "")
                    .str.replace(r"(?i)(\\d{1,2})", "")
                    .str.replace(r", ", "")
                )
                .otherwise(None),
            )
            .with_columns(
                fumble_player=pl.when(pl.col("type.text") == "Penalty").then(None).otherwise(pl.col("fumble_player")),
                # --- Forced Fumble Names ----
                fumble_forced_player=pl.when(
                    (pl.col("text").str.contains(r"(?i)fumble")).and_(pl.col("text").str.contains(r"(?i)forced by"))
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)forced by(.{0,25})")
                    .str.replace(r"(?i)(.+)forced by", "")
                    .str.replace(r"(?i)forced by", "")
                    .str.replace(r"(?i), recove(.+)", "")
                    .str.replace(r"(?i), re(.+)", "")
                    .str.replace(r"(?i), fo(.+)", "")
                    .str.replace(r"(?i), r", "")
                    .str.replace(r"(?i), ", "")
                )
                .otherwise(None),
            )
            .with_columns(
                fumble_forced_player=pl.when(pl.col("type.text") == "Penalty")
                .then(None)
                .otherwise(pl.col("fumble_forced_player")),
                # --- Fumble Recovered Names ----
                fumble_recovered_player=pl.when(
                    (pl.col("text").str.contains(r"(?i)fumble")).and_(pl.col("text").str.contains(r"(?i)recovered by"))
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)recovered by(.{0,30})")
                    .str.replace(r"(?i)for a 1ST down", "")
                    .str.replace(r"(?i)for a 1st down", "")
                    .str.replace(r"(?i)(.+)recovered", "")
                    .str.replace(r"(?i)(.+) by", "")
                    .str.replace(r"(?i), recove(.+)", "")
                    .str.replace(r"(?i), re(.+)", "")
                    .str.replace(r"(?i)a 1st down", "")
                    .str.replace(r"(?i) a 1st down", "")
                    .str.replace(r"(?i), for(.+)", "")
                    .str.replace(r"(?i) for a", "")
                    .str.replace(r"(?i) fo", "")
                    .str.replace(r"(?i) , r", "")
                    .str.replace(r"(?i), r", "")
                    .str.replace(r"(?i)  (.+)", "")
                    .str.replace(r"(?i) ,", "")
                    .str.replace(r"(?i)penalty(.+)", "")
                    .str.replace(r"(?i)for a 1ST down", "")
                )
                .otherwise(None),
            )
            .with_columns(
                fumble_recovered_player=pl.when(pl.col("type.text") == "Penalty")
                .then(None)
                .otherwise(pl.col("fumble_recovered_player")),
            )
            .with_columns(
                ## Extract player names
                passer_player_name=pl.col("pass_player").str.strip(),
                rusher_player_name=pl.col("rush_player").str.strip(),
                receiver_player_name=pl.col("receiver_player").str.strip(),
                sack_player_name=pl.col("sack_player1").str.strip(),
                sack_player_name2=pl.col("sack_player2").str.strip(),
                pass_breakup_player_name=pl.col("pass_breakup_player").str.strip(),
                interception_player_name=pl.col("interception_player").str.strip(),
                fg_kicker_player_name=pl.col("fg_kicker_player").str.strip(),
                fg_block_player_name=pl.col("fg_block_player").str.strip(),
                fg_return_player_name=pl.col("fg_return_player").str.strip(),
                kickoff_player_name=pl.col("kickoff_player").str.strip(),
                kickoff_return_player_name=pl.col("kickoff_return_player").str.strip(),
                punter_player_name=pl.col("punter_player").str.strip(),
                punt_block_player_name=pl.col("punt_block_player").str.strip(),
                punt_return_player_name=pl.col("punt_return_player").str.strip(),
                punt_block_return_player_name=pl.col("punt_block_return_player").str.strip(),
                fumble_player_name=pl.col("fumble_player").str.strip(),
                fumble_forced_player_name=pl.col("fumble_forced_player").str.strip(),
                fumble_recovered_player_name=pl.col("fumble_recovered_player").str.strip(),
            )
            .drop(
                [
                    "rush_player",
                    "receiver_player",
                    "pass_player",
                    "sack_player1",
                    "sack_player2",
                    "pass_breakup_player",
                    "interception_player",
                    "punter_player",
                    "fg_kicker_player",
                    "fg_block_player",
                    "fg_return_player",
                    "kickoff_player",
                    "kickoff_return_player",
                    "punt_return_player",
                    "punt_block_player",
                    "punt_block_return_player",
                    "fumble_player",
                    "fumble_forced_player",
                    "fumble_recovered_player",
                ]
            )
        )
        return play_df

    def __after_cols(self, play_df):
        play_df = (
            play_df.with_columns(
                new_down=pl.when(pl.col("type.text") == "Timeout")
                .then(pl.col("start.down"))
                .when((pl.col("type.text").is_in(penalty)).and_(pl.col("penalty_1st_conv") == True))
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == False)
                )
                .then(pl.col("start.down"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3)
                )
                .then(pl.col("start.down") + 1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4)
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance"))
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3)
                )
                .then(pl.col("start.down") + 1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4)
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance"))
                )
                .then(1)
                .otherwise(pl.col("start.down")),
                new_distance=pl.when(pl.col("type.text") == "Timeout")
                .then(pl.col("start.distance"))
                .when((pl.col("type.text").is_in(penalty)).and_(pl.col("penalty_1st_conv") == True))
                .then(10)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == False)
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3)
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4)
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance"))
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3)
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4)
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance"))
                )
                .then(pl.col("start.distance"))
                .otherwise(pl.col("start.distance")),
                middle_8=pl.when(
                    (pl.col("start.adj_TimeSecsRem") >= 1560).and_(pl.col("start.adj_TimeSecsRem") <= 2040)
                )
                .then(True)
                .otherwise(False),
                rz_play=pl.when(pl.col("start.yardLine") <= 20).then(True).otherwise(False),
                under_2=pl.when(pl.col("start.TimeSecsRem") <= 120).then(True).otherwise(False),
                goal_to_go=pl.when(pl.col("start.yardLine") <= 10).then(True).otherwise(False),
                scoring_opp=pl.when(pl.col("start.yardLine") <= 40).then(True).otherwise(False),
                stuffed_run=pl.when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") <= 0))
                .then(True)
                .otherwise(False),
                stopped_run=pl.when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") <= 2))
                .then(True)
                .otherwise(False),
                opportunity_run=pl.when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") <= 4))
                .then(True)
                .otherwise(False),
                highlight_run=pl.when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") >= 8))
                .then(True)
                .otherwise(False),
                adj_rush_yardage=pl.when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") > 8))
                .then(8)
                .when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") <= 8))
                .then(pl.col("yds_rushed"))
                .otherwise(None),
            )
            .with_columns(
                line_yards=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") < 0))
                .then(1.2 * pl.col("adj_rush_yardage"))
                .when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 0).and_(pl.col("yds_rushed") <= 3))
                .then(pl.col("adj_rush_yardage"))
                .when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 4).and_(pl.col("yds_rushed") <= 8))
                .then(3 + 0.5 * (pl.col("adj_rush_yardage") - 3))
                .when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 8))
                .then(5.5)
                .otherwise(None),
                second_level_yards=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 4))
                .then(0.5 * (pl.col("adj_rush_yardage") - 4))
                .when(pl.col("rush") == True)
                .then(0)
                .otherwise(None),
                open_field_yards=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") > 8))
                .then(pl.col("yds_rushed") - pl.col("adj_rush_yardage"))
                .when(pl.col("rush") == True)
                .then(0)
                .otherwise(None),
            )
            .with_columns(
                highlight_yards=pl.col("second_level_yards") + pl.col("open_field_yards"),
            )
            .with_columns(
                opp_highlight_yards=pl.when(pl.col("opportunity_run") == True)
                .then(pl.col("highlight_yards"))
                .when((pl.col("opportunity_run") == False).and_(pl.col("rush") == True))
                .then(0)
                .otherwise(None),
                short_rush_success=pl.when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance"))
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                )
                .then(False)
                .otherwise(None),
                short_rush_attempt=pl.when((pl.col("start.distance") < 2).and_(pl.col("rush") == True))
                .then(True)
                .when((pl.col("start.distance") >= 2).and_(pl.col("rush") == True))
                .then(False)
                .otherwise(None),
                power_rush_success=pl.when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4]))
                    .and_(pl.col("statYardage") >= pl.col("start.distance"))
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4]))
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                )
                .then(False)
                .otherwise(None),
                power_rush_attempt=pl.when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4]))
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4]))
                )
                .then(False)
                .otherwise(None),
                early_down=pl.when(
                    ((pl.col("down_1") == True).or_(pl.col("down_2") == True)).and_(pl.col("scrimmage_play") == True)
                )
                .then(True)
                .otherwise(False),
                late_down=pl.when(
                    ((pl.col("down_3") == True).or_(pl.col("down_4"))).and_(pl.col("scrimmage_play") == True)
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                early_down_pass=pl.when((pl.col("pass") == True).and_(pl.col("early_down") == True))
                .then(True)
                .otherwise(False),
                early_down_rush=pl.when((pl.col("rush") == True).and_(pl.col("early_down") == True))
                .then(True)
                .otherwise(False),
                late_down_pass=pl.when((pl.col("pass") == True).and_(pl.col("late_down") == True))
                .then(True)
                .otherwise(False),
                late_down_rush=pl.when((pl.col("rush") == True).and_(pl.col("late_down") == True))
                .then(True)
                .otherwise(False),
                standard_down=pl.when((pl.col("scrimmage_play") == True).and_(pl.col("down_1") == True))
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_2") == True)
                    .and_(pl.col("start.distance") < 8)
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_3") == True)
                    .and_(pl.col("start.distance") < 5)
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_4") == True)
                    .and_(pl.col("start.distance") < 5)
                )
                .then(True)
                .otherwise(False),
                passing_down=pl.when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_2") == True)
                    .and_(pl.col("start.distance") >= 8)
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_3") == True)
                    .and_(pl.col("start.distance") >= 5)
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_4") == True)
                    .and_(pl.col("start.distance") >= 5)
                )
                .then(True)
                .otherwise(False),
                TFL=pl.when(
                    (pl.col("type.text") != "Penalty").and_(pl.col("sp") == False).and_(pl.col("statYardage") < 0)
                )
                .then(True)
                .when(pl.col("sack_vec") == True)
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                TFL_pass=pl.when((pl.col("TFL") == True).and_(pl.col("pass") == True)).then(True).otherwise(False),
                TFL_rush=pl.when((pl.col("TFL") == True).and_(pl.col("rush") == True)).then(True).otherwise(False),
                havoc=pl.when(pl.col("pass_breakup") == True)
                .then(True)
                .when(pl.col("TFL") == True)
                .then(True)
                .when(pl.col("int") == True)
                .then(True)
                .when(pl.col("forced_fumble") == True)
                .then(True)
                .otherwise(False),
            )
        )
        return play_df

    def __add_spread_time(self, play_df):
        play_df = (
            play_df.with_columns(
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("homeTeamSpread"))
                .otherwise(-1 * pl.col("homeTeamSpread"))
                .alias("start.pos_team_spread"),
                ((3600 - pl.col("start.adj_TimeSecsRem")) / 3600).clip(0, 3600).alias("start.elapsed_share"),
            )
            .with_columns(
                (pl.col("start.pos_team_spread") * np.exp(-4 * pl.col("start.elapsed_share"))).alias(
                    "start.spread_time"
                ),
                pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("homeTeamSpread"))
                .otherwise(-1 * pl.col("homeTeamSpread"))
                .alias("end.pos_team_spread"),
                ((3600 - pl.col("end.adj_TimeSecsRem")) / 3600).clip(0, 3600).alias("end.elapsed_share"),
            )
            .with_columns(
                (pl.col("end.pos_team_spread") * np.exp(-4 * pl.col("end.elapsed_share"))).alias("end.spread_time"),
            )
        )
        return play_df

    def __calculate_ep_exp_val(self, matrix):
        return (
            matrix[:, 0] * ep_class_to_score_mapping[0]
            + matrix[:, 1] * ep_class_to_score_mapping[1]
            + matrix[:, 2] * ep_class_to_score_mapping[2]
            + matrix[:, 3] * ep_class_to_score_mapping[3]
            + matrix[:, 4] * ep_class_to_score_mapping[4]
            + matrix[:, 5] * ep_class_to_score_mapping[5]
            + matrix[:, 6] * ep_class_to_score_mapping[6]
        )

    def __process_epa(self, play_df):
        play_df = (
            play_df.with_columns(
                down=pl.when(pl.col("type.text").is_in(kickoff_vec)).then(1).otherwise(pl.col("start.down")),
                down_1=pl.when(pl.col("type.text").is_in(kickoff_vec)).then(True).otherwise(pl.col("down_1")),
                down_2=pl.when(pl.col("type.text").is_in(kickoff_vec)).then(False).otherwise(pl.col("down_2")),
                down_3=pl.when(pl.col("type.text").is_in(kickoff_vec)).then(False).otherwise(pl.col("down_3")),
                down_4=pl.when(pl.col("type.text").is_in(kickoff_vec)).then(False).otherwise(pl.col("down_4")),
                distance=pl.when(pl.col("type.text").is_in(kickoff_vec)).then(10).otherwise(pl.col("start.distance")),
            )
            .with_columns(
                pl.when(pl.col("type.text").is_in(kickoff_vec))
                .then(1)
                .otherwise(pl.col("start.down"))
                .alias("start.down"),
                pl.when(pl.col("type.text").is_in(kickoff_vec))
                .then(10)
                .otherwise(pl.col("start.distance"))
                .alias("start.distance"),
                pl.lit(99).alias("start.yardsToEndzone.touchback"),
            )
            .with_columns(
                pl.when((pl.col("type.text").is_in(kickoff_vec)).and_(pl.col("season") > 2013))
                .then(75)
                .when((pl.col("type.text").is_in(kickoff_vec)).and_(pl.col("season") <= 2013))
                .then(80)
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone.touchback"),
            )
        )

        start_touchback_data = play_df[ep_start_touchback_columns]

        start_touchback_data.columns = ep_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))

        dtest_start_touchback = DMatrix(start_touchback_data)
        EP_start_touchback_parts = ep_model.predict(dtest_start_touchback)
        EP_start_touchback = self.__calculate_ep_exp_val(EP_start_touchback_parts)

        start_data = play_df[ep_start_columns]
        start_data.columns = ep_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))

        dtest_start = DMatrix(start_data)
        EP_start_parts = ep_model.predict(dtest_start)
        EP_start = self.__calculate_ep_exp_val(EP_start_parts)

        play_df = (
            play_df.with_columns(
                pl.when(pl.col("end.TimeSecsRem") <= 0)
                .then(0)
                .otherwise(pl.col("end.TimeSecsRem"))
                .alias("end.TimeSecsRem"),
            )
            .with_columns(
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(99)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(True)
                .otherwise(pl.col("down_1_end"))
                .alias("down_1_end"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(False)
                .otherwise(pl.col("down_2_end"))
                .alias("down_2_end"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(False)
                .otherwise(pl.col("down_3_end"))
                .alias("down_3_end"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(False)
                .otherwise(pl.col("down_4_end"))
                .alias("down_4_end"),
            )
            .with_columns(
                pl.when(pl.col("end.yardsToEndzone") >= 100)
                .then(99)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                pl.when(pl.col("end.yardsToEndzone") <= 0)
                .then(99)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                pl.when(pl.col("kickoff_tb") == True)
                .then(75)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
                pl.when(pl.col("kickoff_tb") == True).then(1).otherwise(pl.col("end.down")).alias("end.down"),
                pl.when(pl.col("kickoff_tb") == True).then(10).otherwise(pl.col("end.distance")).alias("end.distance"),
            )
            .with_columns(
                pl.when(pl.col("punt_tb") == True).then(1).otherwise(pl.col("end.down")).alias("end.down"),
                pl.when(pl.col("punt_tb") == True).then(10).otherwise(pl.col("end.distance")).alias("end.distance"),
                pl.when(pl.col("punt_tb") == True)
                .then(80)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
        )

        end_data = play_df[ep_end_columns]
        end_data.columns = ep_final_names
        # self.logger.info(end_data.iloc[[36]].to_json(orient="records"))
        dtest_end = DMatrix(end_data)
        EP_end_parts = ep_model.predict(dtest_end)

        EP_end = self.__calculate_ep_exp_val(EP_end_parts)

        play_df = play_df.with_columns(
            EP_start_touchback=pl.lit(EP_start_touchback),
            EP_start=pl.lit(EP_start),
            EP_end=pl.lit(EP_end),
        )

        play_df = (
            play_df.with_columns(
                EP_start=pl.when(
                    pl.col("type.text").is_in(
                        [
                            "Extra Point Good",
                            "Extra Point Missed",
                            "Two-Point Conversion Good",
                            "Two-Point Conversion Missed",
                            "Two Point Pass",
                            "Two Point Rush",
                            "Blocked PAT",
                        ]
                    )
                )
                .then(0.92)
                .otherwise(pl.col("EP_start")),
            )
            .with_columns(
                # End of Half
                EP_end=pl.when(
                    (pl.col("type.text").str.to_lowercase().str.contains(r"end of game")).or_(
                        pl.col("type.text").str.to_lowercase().str.contains(r"end of half")
                    )
                )
                .then(0)
                # Defensive 2pt Conversion
                .when(pl.col("type.text").is_in(["Defensive 2pt Conversion"]))
                .then(-2)
                # Safeties
                .when(
                    (pl.col("type.text").is_in(defense_score_vec)).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"(?i)safety")
                    )
                )
                .then(-2)
                # Defense TD + Successful Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed") == False)
                )
                .then(-8)
                # Defense TD + Failed Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed"))
                )
                .then(-6)
                # Defense TD + Kick/PAT Missed
                .when(
                    (pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"PAT"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)missed"))
                )
                .then(-6)
                # Defense TD + Kick/PAT Good
                .when(
                    (pl.col("type.text").is_in(defense_score_vec)).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"kick\)")
                    )
                )
                .then(-7)
                # Defense TD
                .when(pl.col("type.text").is_in(defense_score_vec))
                .then(-6.92)
                # Offense TD + Failed Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed"))
                )
                .then(6)
                # Offense TD + Successful Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed") == False)
                )
                .then(8)
                # Offense Made FG
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("type.text").str.to_lowercase().str.contains(r"(?i)field goal"))
                    .and_(pl.col("type.text").str.to_lowercase().str.contains(r"(?i)good"))
                )
                .then(3)
                # Offense TD + Kick/PAT Missed
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"PAT"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)missed"))
                )
                .then(6)
                # Offense TD + Kick/PAT Good
                .when(
                    (pl.col("type.text").is_in(offense_score_vec)).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"kick\)")
                    )
                )
                .then(7)
                # Offense TD
                .when(pl.col("type.text").is_in(offense_score_vec))
                .then(6.92)
                # Extra Point Good
                .when(pl.col("type.text").is_in(["Extra Point Good"]))
                .then(1)
                # Extra Point Missed
                .when(pl.col("type.text").is_in(["Extra Point Missed"]))
                .then(0)
                # Two-Point Conversion Good
                .when(pl.col("type.text").is_in(["Two-Point Conversion Good"]))
                .then(2)
                # Two-Point Conversion Missed
                .when(pl.col("type.text").is_in(["Two-Point Conversion Missed"]))
                .then(0)
                # Two Point Pass/Rush Missed (Pre-2014 Data)
                .when(
                    (pl.col("type.text").is_in(["Two Point Pass", "Two Point Rush"])).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"(?i)no good")
                    )
                )
                .then(0)
                # Two Point Pass/Rush Good (Pre-2014 Data)
                .when(
                    (pl.col("type.text").is_in(["Two Point Pass", "Two Point Rush"])).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"(?i)no good") == False
                    )
                )
                .then(2)
                # Blocked PAT
                .when(pl.col("type.text").is_in(["Blocked PAT"]))
                .then(0)
                # Flips for Turnovers that aren't kickoffs
                .when(
                    ((pl.col("type.text").is_in(end_change_vec)).or_(pl.col("downs_turnover") == True)).and_(
                        pl.col("type.text").is_in(kickoff_vec) == False
                    )
                )
                .then(pl.col("EP_end") * -1)
                # Flips for Turnovers that are kickoffs
                .when(pl.col("type.text").is_in(kickoff_turnovers))
                .then(pl.col("EP_end") * -1)
                # Onside kicks
                .when((pl.col("kickoff_onside") == True).and_(pl.col("change_of_pos_team") == True))
                .then(pl.col("EP_end") * -1)
                .otherwise(pl.col("EP_end"))
            )
            .with_columns(
                lag_EP_end=pl.col("EP_end").shift(1),
                lag_change_of_pos_team=pl.col("change_of_pos_team").shift(1),
            )
            .with_columns(
                lag_change_of_pos_team=pl.when(pl.col("lag_change_of_pos_team").is_null())
                .then(False)
                .otherwise(pl.col("lag_change_of_pos_team")),
            )
            .with_columns(
                EP_between=pl.when(pl.col("lag_change_of_pos_team") == True)
                .then(pl.col("EP_start") + pl.col("lag_EP_end"))
                .otherwise(pl.col("EP_start") - pl.col("lag_EP_end")),
                EP_start=pl.when(
                    (pl.col("type.text").is_in(["Timeout", "End Period"])).and_(
                        pl.col("lag_change_of_pos_team") == False
                    )
                )
                .then(pl.col("lag_EP_end"))
                .otherwise(pl.col("EP_start")),
            )
            .with_columns(
                EP_start=pl.when(pl.col("type.text").is_in(kickoff_vec))
                .then(pl.col("EP_start_touchback"))
                .otherwise(pl.col("EP_start")),
            )
            .with_columns(
                EP_end=pl.when(pl.col("type.text").is_in(["Timeout"]))
                .then(pl.col("EP_start"))
                .otherwise(pl.col("EP_end")),
            )
            .with_columns(
                EPA=pl.when(pl.col("type.text").is_in(["Timeout"]))
                .then(0)
                .when((pl.col("scoring_play") == False).and_(pl.col("end_of_half") == True))
                .then(-1 * pl.col("EP_start"))
                .when((pl.col("type.text").is_in(kickoff_vec)).and_(pl.col("penalty_in_text") == True))
                .then(pl.col("EP_end") - pl.col("EP_start"))
                .when(
                    (pl.col("penalty_in_text") == True)
                    .and_(pl.col("type.text").is_in(["Penalty"]) == False)
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False)
                )
                .then(pl.col("EP_end") - pl.col("EP_start") + pl.col("EP_between"))
                .otherwise(pl.col("EP_end") - pl.col("EP_start")),
            )
            .with_columns(
                def_EPA=pl.col("EPA") * -1,
                # --- EPA Summary flags ----
                EPA_scrimmage=pl.when(pl.col("scrimmage_play") == True).then(pl.col("EPA")).otherwise(None),
                EPA_rush=pl.when((pl.col("rush") == True).and_(pl.col("penalty_in_text") == True))
                .then(pl.col("EPA"))
                .when((pl.col("rush") == True).and_(pl.col("penalty_in_text") == False))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_pass=pl.when(pl.col("pass") == True).then(pl.col("EPA")).otherwise(None),
                EPA_explosive=pl.when((pl.col("pass") == True).and_(pl.col("EPA") >= 2.4))
                .then(True)
                .when(((pl.col("rush") == True).and_(pl.col("EPA") >= 1.8)))
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                EPA_non_explosive=pl.when(pl.col("EPA_explosive") == False).then(pl.col("EPA")).otherwise(None),
                EPA_explosive_pass=pl.when((pl.col("pass") == True).and_(pl.col("EPA") >= 2.4))
                .then(True)
                .otherwise(False),
                EPA_explosive_rush=pl.when((pl.col("rush") == True).and_(pl.col("EPA") >= 1.8))
                .then(True)
                .otherwise(False),
                first_down_created=pl.when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("end.down") == 1)
                    .and_(pl.col("start.pos_team.id") == pl.col("end.pos_team.id"))
                )
                .then(True)
                .otherwise(False),
                EPA_success=pl.when(pl.col("EPA") > 0).then(True).otherwise(False),
                EPA_success_early_down=pl.when((pl.col("EPA") > 0).and_(pl.col("early_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_early_down_pass=pl.when(
                    (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("early_down") == True)
                )
                .then(True)
                .otherwise(False),
                EPA_success_early_down_rush=pl.when(
                    (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("early_down") == True)
                )
                .then(True)
                .otherwise(False),
                EPA_success_late_down=pl.when((pl.col("EPA") > 0).and_(pl.col("late_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_late_down_pass=pl.when(
                    (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("late_down") == True)
                )
                .then(True)
                .otherwise(False),
                EPA_success_late_down_rush=pl.when(
                    (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("late_down") == True)
                )
                .then(True)
                .otherwise(False),
                EPA_success_standard_down=pl.when((pl.col("EPA") > 0).and_(pl.col("standard_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_passing_down=pl.when((pl.col("EPA") > 0).and_(pl.col("passing_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_pass=pl.when((pl.col("EPA") > 0).and_(pl.col("pass") == True)).then(True).otherwise(False),
                EPA_success_rush=pl.when((pl.col("EPA") > 0).and_(pl.col("rush") == True)).then(True).otherwise(False),
                EPA_success_EPA=pl.when(pl.col("EPA") > 0).then(pl.col("EPA")).otherwise(None),
                EPA_success_standard_down_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("standard_down") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_success_passing_down_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("passing_down") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_success_pass_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("pass") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_success_rush_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("rush") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_middle_8_success=pl.when((pl.col("EPA") > 0).and_(pl.col("middle_8") == True))
                .then(True)
                .otherwise(False),
                EPA_middle_8_success_pass=pl.when(
                    (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("middle_8") == True)
                )
                .then(True)
                .otherwise(False),
                EPA_middle_8_success_rush=pl.when(
                    (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("middle_8") == True)
                )
                .then(True)
                .otherwise(False),
                EPA_penalty=pl.when(pl.col("type.text").is_in(["Penalty", "Penalty (Kickoff)"]))
                .then(pl.col("EPA"))
                .when(pl.col("penalty_in_text") == True)
                .then(pl.col("EP_end") - pl.col("EP_start"))
                .otherwise(None),
                EPA_sp=pl.when(
                    (pl.col("fg_attempt") == True).or_(pl.col("punt") == True).or_(pl.col("kickoff_play") == True)
                )
                .then(pl.col("EPA"))
                .otherwise(False),
                EPA_fg=pl.when(pl.col("fg_attempt") == True).then(pl.col("EPA")).otherwise(None),
                EPA_punt=pl.when(pl.col("punt") == True).then(pl.col("EPA")).otherwise(None),
                EPA_kickoff=pl.when(pl.col("kickoff_play") == True).then(pl.col("EPA")).otherwise(None),
            )
        )
        return play_df

    def __process_qbr(self, play_df):
        play_df = (
            play_df.with_columns(
                qbr_epa=pl.when(pl.col("EPA") < -5.0)
                .then(-5.0)
                .when(pl.col("fumble_vec") == True)
                .then(-3.5)
                .otherwise(pl.col("EPA")),
                weight=pl.when(pl.col("home_wp_before") < 0.1)
                .then(0.6)
                .when((pl.col("home_wp_before") >= 0.1).and_(pl.col("home_wp_before") < 0.2))
                .then(0.9)
                .when((pl.col("home_wp_before") >= 0.8).and_(pl.col("home_wp_before") < 0.9))
                .then(0.9)
                .when(pl.col("home_wp_before") > 0.9)
                .then(0.6)
                .otherwise(1),
                non_fumble_sack=pl.when((pl.col("sack_vec") == True).and_(pl.col("fumble_vec") == False))
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                sack_epa=pl.when(pl.col("non_fumble_sack") == True).then(pl.col("qbr_epa")).otherwise(None),
                pass_epa=pl.when(pl.col("pass") == True).then(pl.col("qbr_epa")).otherwise(None),
                rush_epa=pl.when(pl.col("rush") == True).then(pl.col("qbr_epa")).otherwise(None),
                pen_epa=pl.when(pl.col("penalty_flag") == True).then(pl.col("qbr_epa")).otherwise(None),
            )
            .with_columns(
                sack_weight=pl.when(pl.col("non_fumble_sack") == True).then(pl.col("weight")).otherwise(None),
                pass_weight=pl.when(pl.col("pass") == True).then(pl.col("weight")).otherwise(None),
                rush_weight=pl.when(pl.col("rush") == True).then(pl.col("weight")).otherwise(None),
                pen_weight=pl.when(pl.col("penalty_flag") == True).then(pl.col("weight")).otherwise(None),
            )
            .with_columns(
                action_play=pl.col("EPA") != 0,
                athlete_name=pl.when(pl.col("passer_player_name").is_not_null())
                .then(pl.col("passer_player_name"))
                .when(pl.col("rusher_player_name").is_not_null())
                .then(pl.col("rusher_player_name"))
                .otherwise(None),
            )
        )
        return play_df

    def __process_wpa(self, play_df):
        # ---- prepare variables for wp_before calculations ----
        play_df = (
            play_df.with_columns(
                pl.when(pl.col("type.text").is_in(kickoff_vec))
                .then(pl.col("pos_score_diff_start") + pl.col("EP_start_touchback"))
                .otherwise(0.000)
                .alias("start.ExpScoreDiff_touchback"),
                pl.when((pl.col("penalty_in_text") == True).and_(pl.col("type.text").is_in(["Penalty"]) == False))
                .then(pl.col("pos_score_diff_start") + pl.col("EP_start") - pl.col("EP_between"))
                .when((pl.col("type.text") == "Timeout").and_(pl.col("lag_scoringPlay") == True))
                .then(pl.col("pos_score_diff_start") + 0.92)
                .otherwise(pl.col("pos_score_diff_start") + pl.col("EP_start"))
                .alias("start.ExpScoreDiff"),
            )
            .with_columns(
                (pl.col("start.ExpScoreDiff_touchback") / (pl.col("start.adj_TimeSecsRem") + 1)).alias(
                    "start.ExpScoreDiff_Time_Ratio_touchback"
                ),
                (pl.col("start.ExpScoreDiff") / (pl.col("start.adj_TimeSecsRem") + 1)).alias(
                    "start.ExpScoreDiff_Time_Ratio"
                ),
                # ---- prepare variables for wp_after calculations ----
                pl.when(
                    ((pl.col("type.text").is_in(end_change_vec)).or_(pl.col("downs_turnover") == True))
                    .and_(pl.col("kickoff_play") == False)
                    .and_(pl.col("scoringPlay") == False)
                )
                .then(pl.col("pos_score_diff_end") - pl.col("EP_end"))
                .when(pl.col("type.text").is_in(kickoff_turnovers).and_(pl.col("scoringPlay") == False))
                .then(pl.col("pos_score_diff_end") + pl.col("EP_end"))
                .when((pl.col("scoringPlay") == False).and_(pl.col("type.text") != "Timeout"))
                .then(pl.col("pos_score_diff_end") + pl.col("EP_end"))
                .when((pl.col("scoringPlay") == False).and_(pl.col("type.text") == "Timeout"))
                .then(pl.col("pos_score_diff_end") + pl.col("EP_end"))
                .when(
                    (pl.col("scoringPlay") == True)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("season") <= 2013)
                )
                .then(pl.col("pos_score_diff_end") - 0.92)
                .when(
                    (pl.col("scoringPlay") == True)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("season") <= 2013)
                )
                .then(pl.col("pos_score_diff_end") + 0.92)
                .when(
                    (pl.col("type.text") == "Timeout")
                    .and_(pl.col("lag_scoringPlay") == True)
                    .and_(pl.col("season") <= 2013)
                )
                .then(pl.col("pos_score_diff_end") + 0.92)
                .otherwise(pl.col("pos_score_diff_end"))
                .alias("end.ExpScoreDiff"),
            )
            .with_columns(
                (pl.col("end.ExpScoreDiff") / (pl.col("end.adj_TimeSecsRem") + 1)).alias("end.ExpScoreDiff_Time_Ratio")
            )
        )

        # ---- wp_before ----
        start_touchback_data = play_df[wp_start_touchback_columns]
        start_touchback_data.columns = wp_final_names
        # self.logger.info(start_touchback_data.iloc[[36]].to_json(orient="records"))
        dtest_start_touchback = DMatrix(start_touchback_data)
        WP_start_touchback = wp_model.predict(dtest_start_touchback)
        start_data = play_df[wp_start_columns]
        start_data.columns = wp_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))
        dtest_start = DMatrix(start_data)
        WP_start = wp_model.predict(dtest_start)

        # ---- wp_after ----
        end_data = play_df[wp_end_columns]
        end_data.columns = wp_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))
        dtest_end = DMatrix(end_data)
        WP_end = wp_model.predict(dtest_end)

        play_df = (
            play_df.with_columns(
                wp_before=pl.lit(WP_start), wp_touchback=pl.lit(WP_start_touchback), wp_after=pl.lit(WP_end)
            )
            .with_columns(
                wp_before=pl.when(pl.col("type.text").is_in(kickoff_vec))
                .then(pl.col("wp_touchback"))
                .otherwise(pl.col("wp_before")),
            )
            .with_columns(
                def_wp_before=1 - pl.col("wp_before"),
            )
            .with_columns(
                home_wp_before=pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("wp_before"))
                .otherwise(pl.col("def_wp_before")),
                away_wp_before=pl.when(pl.col("start.pos_team.id") != pl.col("homeTeamId"))
                .then(pl.col("wp_before"))
                .otherwise(pl.col("def_wp_before")),
            )
            .with_columns(
                lead_wp_before=pl.col("wp_before").shift(-1),
                lead_wp_before2=pl.col("wp_before").shift(-2),
            )
            .with_columns(
                wp_after=pl.when(pl.col("type.text").is_in(["Timeout"]))
                .then(pl.col("wp_before"))
                .when(
                    (pl.col("status_type_completed") == True)
                    .and_(
                        (pl.col("lead_play_type").is_null()).or_(
                            pl.col("game_play_number") == pl.col("game_play_number").max()
                        )
                    )
                    .and_(pl.col("pos_score_diff_end") > 0)
                )
                .then(1.0)
                .when(
                    (pl.col("status_type_completed") == True)
                    .and_(
                        (pl.col("lead_play_type").is_null()).or_(
                            pl.col("game_play_number") == pl.col("game_play_number").max()
                        )
                    )
                    .and_(pl.col("pos_score_diff_end") < 0)
                )
                .then(0.0)
                .when(
                    (pl.col("end_of_half") == True)
                    .and_(pl.col("start.pos_team.id") == pl.col("lead_pos_team"))
                    .and_(pl.col("type.text") != "Timeout")
                )
                .then(pl.col("lead_wp_before"))
                .when(
                    (pl.col("end_of_half") == True)
                    .and_(pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
                    .and_(pl.col("type.text") != "Timeout")
                )
                .then(1 - pl.col("lead_wp_before"))
                .when(
                    (pl.col("end_of_half") == True)
                    .and_(pl.col("start.pos_team_receives_2H_kickoff") == False)
                    .and_(pl.col("type.text") == "Timeout")
                )
                .then(pl.col("wp_after"))
                .when(
                    (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).and_(
                        pl.col("change_of_pos_team") == False
                    )
                )
                .then(pl.col("lead_wp_before"))
                .when(
                    (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).and_(
                        pl.col("change_of_pos_team") == True
                    )
                )
                .then(1 - pl.col("lead_wp_before"))
                .when((pl.col("kickoff_onside") == True).and_(pl.col("change_of_pos_team") == True))
                .then(pl.col("wp_after"))
                .when(pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
                .then(1 - pl.col("wp_after"))
                .otherwise(pl.col("wp_after")),
            )
            .with_columns(
                def_wp_after=1 - pl.col("wp_after"),
            )
            .with_columns(
                home_wp_after=pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("wp_after"))
                .otherwise(pl.col("def_wp_after")),
                away_wp_after=pl.when(pl.col("end.pos_team.id") != pl.col("homeTeamId"))
                .then(pl.col("wp_after"))
                .otherwise(pl.col("def_wp_after")),
            )
            .with_columns(
                wpa=pl.col("wp_after") - pl.col("wp_before"),
            )
        )
        return play_df

    def __add_drive_data(self, play_df):
        play_df = (
            play_df.with_columns(
                (
                    pl.when(pl.col("drive.result").is_null())
                    .then(pl.lit("Not provided"))
                    .otherwise(pl.col("drive.result"))
                )
                .cast(pl.Utf8)
                .alias("drive.result"),
            )
            .with_columns(
                drive_start=pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(100 - pl.col("drive.start.yardLine"))
                .otherwise(pl.col("drive.start.yardLine")),
                drive_stopped=pl.when(pl.col("drive.result").is_null())
                .then(False)
                .otherwise(
                    pl.col("drive.result").str.to_lowercase().str.contains(r"(?i)punt|fumble|interception|downs")
                ),
            )
            .with_columns(
                drive_start=pl.col("drive_start").cast(pl.Float32),
            )
            .with_columns(
                drive_play_index=pl.col("scrimmage_play").cumsum().over("drive.id"),
            )
            .with_columns(
                drive_offense_plays=pl.when((pl.col("sp") == False).and_(pl.col("scrimmage_play") == True))
                .then(pl.col("play").cast(pl.Int32))
                .otherwise(0),
                prog_drive_EPA=pl.col("EPA_scrimmage").cumsum().over("drive.id"),
                prog_drive_WPA=pl.col("wpa").cumsum().over("drive.id"),
                drive_offense_yards=pl.when((pl.col("sp") == False).and_(pl.col("scrimmage_play") == True))
                .then(pl.col("statYardage"))
                .otherwise(0),
            )
            .with_columns(
                drive_total_yards=pl.col("drive_offense_yards").cumsum().over("drive.id"),
            )
        )
        return play_df

    def __cast_box_score_column(self, play_df, column, target_type):
        if column in play_df.columns:
            play_df = play_df.with_columns(pl.col(column).cast(target_type).alias(column))
        else:
            play_df = play_df.with_columns((pl.Null).alias(column))
        return play_df

    def create_box_score(self, play_df):
        # have to run the pipeline before pulling this in
        if self.ran_pipeline == False:
            self.run_processing_pipeline()

        box_score_columns = [
            "completion",
            "target",
            "yds_receiving",
            "yds_rushed",
            "rush",
            "rush_td",
            "pass",
            "pass_td",
            "EPA",
            "wpa",
            "int",
            "int_td",
            "def_EPA",
            "EPA_rush",
            "EPA_pass",
            "EPA_success",
            "EPA_success_pass",
            "EPA_success_rush",
            "EPA_success_standard_down",
            "EPA_success_passing_down",
            "middle_8",
            "rz_play",
            "scoring_opp",
            "stuffed_run",
            "stopped_run",
            "opportunity_run",
            "highlight_run",
            "short_rush_success",
            "short_rush_attempt",
            "power_rush_success",
            "power_rush_attempt",
            "EPA_explosive",
            "EPA_explosive_pass",
            "EPA_explosive_rush",
            "standard_down",
            "passing_down",
            "fumble_vec",
            "sack",
            "penalty_flag",
            "play",
            "scrimmage_play",
            "sp",
            "kickoff_play",
            "punt",
            "fg_attempt",
            "EPA_penalty",
            "EPA_sp",
            "EPA_fg",
            "EPA_punt",
            "EPA_kickoff",
            "TFL",
            "TFL_pass",
            "TFL_rush",
            "havoc",
        ]
        for item in box_score_columns:
            self.__cast_box_score_column(play_df, item, pl.Float32)

        pass_box = play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
        rush_box = play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
        # pass_box.yds_receiving.fillna(0.0, inplace=True)
        passer_box = (
            pass_box.fill_null(0.0)
            .groupby(by=["pos_team", "passer_player_name"])
            .agg(
                Comp=pl.col("completion").sum(),
                Att=pl.col("pass_attempt").sum(),
                Yds=pl.col("yds_receiving").sum(),
                Pass_TD=pl.col("pass_td").sum(),
                Int=pl.col("int").sum(),
                YPA=pl.col("yds_receiving").mean(),
                EPA=pl.col("EPA").sum(),
                EPA_per_Play=pl.col("EPA").mean(),
                WPA=pl.col("wpa").sum(),
                SR=pl.col("EPA_success").mean(),
                Sck=pl.col("sack_vec").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        # passer_box = passer_box.replace(pl.all(), pl.Null)
        qbs_list = passer_box["passer_player_name"].to_list()

        pass_qbr_box = play_df.filter(
            (pl.col("athlete_name").is_not_null() == True)
            & (pl.col("scrimmage_play") == True)
            & (pl.col("athlete_name").is_in(qbs_list))
        )
        pass_qbr = (
            pass_qbr_box.groupby(by=["pos_team", "athlete_name"])
            .agg(
                qbr_epa=(pl.col("qbr_epa") * pl.col("weight")).sum() / pl.col("weight").sum(),
                sack_epa=(pl.col("sack_epa") * pl.col("sack_weight")).sum() / pl.col("sack_weight").sum(),
                pass_epa=(pl.col("pass_epa") * pl.col("pass_weight")).sum() / pl.col("pass_weight").sum(),
                rush_epa=(pl.col("rush_epa") * pl.col("rush_weight")).sum() / pl.col("rush_weight").sum(),
                pen_epa=(pl.col("pen_epa") * pl.col("pen_weight")).sum() / pl.col("pen_weight").sum(),
                spread=(pl.col("start.pos_team_spread").first()),
            )
            .fill_null(0.0)
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        # # self.logger.info(pass_qbr)

        dtest_qbr = DMatrix(pass_qbr[qbr_vars])
        qbr_result = qbr_model.predict(dtest_qbr)
        pass_qbr = pass_qbr.with_columns(exp_qbr=pl.lit(qbr_result))
        passer_box = passer_box.join(
            pass_qbr, left_on=["passer_player_name", "pos_team"], right_on=["athlete_name", "pos_team"]
        )

        rusher_box = (
            rush_box.fill_null(0.0)
            .groupby(by=["pos_team", "rusher_player_name"])
            .agg(
                Car=pl.col("rush").sum(),
                Yds=pl.col("yds_rushed").sum(),
                Rush_TD=pl.col("rush_td").sum(),
                YPC=pl.col("yds_rushed").mean(),
                EPA=pl.col("EPA").sum(),
                EPA_per_Play=pl.col("EPA").mean(),
                WPA=pl.col("wpa").sum(),
                SR=pl.col("EPA_success").mean(),
                Fum=pl.col("fumble_vec").sum(),
                Fum_Lost=pl.col("fumble_lost").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        # rusher_box = rusher_box.replace({np.nan: None})

        receiver_box = (
            pass_box.fill_null(0.0)
            .groupby(by=["pos_team", "receiver_player_name"])
            .agg(
                Rec=pl.col("completion").sum(),
                Tar=pl.col("target").sum(),
                Yds=pl.col("yds_receiving").sum(),
                Rec_TD=pl.col("pass_td").sum(),
                YPT=pl.col("yds_receiving").mean(),
                EPA=pl.col("EPA").sum(),
                EPA_per_Play=pl.col("EPA").mean(),
                WPA=pl.col("wpa").sum(),
                SR=pl.col("EPA_success").mean(),
                Fum=pl.col("fumble_vec").sum(),
                Fum_Lost=pl.col("fumble_lost").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_base_box = (
            play_df.groupby(by=["pos_team"])
            .agg(
                EPA_plays=pl.col("play").sum(),
                total_yards=pl.col("statYardage").sum(),
                EPA_overall_total=pl.col("EPA").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_pen_box = (
            play_df.filter(pl.col("penalty_flag") == True)
            .groupby(by=["pos_team"])
            .agg(
                total_pen_yards=pl.col("statYardage").sum(),
                EPA_penalty=pl.col("EPA_penalty").sum(),
                penalty_first_downs_created=pl.col("penalty_1st_conv").sum(),
                penalty_first_downs_created_rate=pl.col("penalty_1st_conv").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_scrimmage_box = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .groupby(by=["pos_team"])
            .agg(
                scrimmage_plays=pl.col("scrimmage_play").sum(),
                EPA_overall_off=pl.col("EPA").sum(),
                EPA_overall_offense=pl.col("EPA").sum(),
                EPA_per_play=pl.col("EPA").mean(),
                EPA_non_explosive=pl.col("EPA_non_explosive").sum(),
                EPA_non_explosive_per_play=pl.col("EPA_non_explosive").mean(),
                EPA_explosive=pl.col("EPA_explosive").sum(),
                EPA_explosive_rate=pl.col("EPA_explosive").mean(),
                passes_rate=pl.col("pass").mean(),
                off_yards=pl.col("statYardage").sum(),
                total_off_yards=pl.col("statYardage").sum(),
                yards_per_play=pl.col("statYardage").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_sp_box = (
            play_df.filter(pl.col("sp") == True)
            .groupby(by=["pos_team"])
            .agg(
                special_teams_plays=pl.col("sp").sum(),
                EPA_sp=pl.col("EPA_sp").sum(),
                EPA_special_teams=pl.col("EPA_sp").sum(),
                field_goals=pl.col("fg_attempt").sum(),
                EPA_fg=pl.col("EPA_fg").sum(),
                punt_plays=pl.col("punt_play").sum(),
                EPA_punt=pl.col("EPA_punt").sum(),
                kickoff_plays=pl.col("kickoff_play").sum(),
                EPA_kickoff=pl.col("EPA_kickoff").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_scrimmage_box_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
            .fill_null(0.0)
            .groupby(by=["pos_team"])
            .agg(
                passes=pl.col("pass").sum(),
                pass_yards=pl.col("yds_receiving").sum(),
                yards_per_pass=pl.col("yds_receiving").mean(),
                passing_first_downs_created=pl.col("first_down_created").sum(),
                passing_first_downs_created_rate=pl.col("first_down_created").mean(),
                EPA_passing_overall=pl.col("EPA").sum(),
                EPA_passing_per_play=pl.col("EPA").mean(),
                EPA_explosive_passing=pl.col("EPA_explosive").sum(),
                EPA_explosive_passing_rate=pl.col("EPA_explosive").mean(),
                EPA_non_explosive_passing=pl.col("EPA_non_explosive").sum(),
                EPA_non_explosive_passing_per_play=pl.col("EPA_non_explosive").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_scrimmage_box_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .fill_null(0.0)
            .groupby(by=["pos_team"])
            .agg(
                rushes=pl.col("rush").sum(),
                rush_yards=pl.col("yds_rushed").sum(),
                yards_per_rush=pl.col("yds_rushed").mean(),
                rushing_power_rate=pl.col("power_rush_attempt").mean(),
                rushing_first_downs_created=pl.col("first_down_created").sum(),
                rushing_first_downs_created_rate=pl.col("first_down_created").mean(),
                EPA_rushing_overall=pl.col("EPA").sum(),
                EPA_rushing_per_play=pl.col("EPA").mean(),
                EPA_explosive_rushing=pl.col("EPA_explosive").sum(),
                EPA_explosive_rushing_rate=pl.col("EPA_explosive").mean(),
                EPA_non_explosive_rushing=pl.col("EPA_non_explosive").sum(),
                EPA_non_explosive_rushing_per_play=pl.col("EPA_non_explosive").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_rush_base_box = (
            play_df.filter((pl.col("scrimmage_play") == True))
            .fill_null(0.0)
            .groupby(by=["pos_team"])
            .agg(
                rushes_rate=pl.col("rush").mean(),
                first_downs_created=pl.col("first_down_created").sum(),
                first_downs_created_rate=pl.col("first_down_created").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_rush_power_box = (
            play_df.filter((pl.col("power_rush_attempt") == True) & (pl.col("scrimmage_play") == True))
            .fill_null(0.0)
            .groupby(by=["pos_team"])
            .agg(
                EPA_rushing_power=pl.col("EPA").sum(),
                EPA_rushing_power_per_play=pl.col("EPA").mean(),
                rushing_power_success=pl.col("power_rush_success").sum(),
                rushing_power_success_rate=pl.col("power_rush_success").mean(),
                rushing_power=pl.col("power_rush_attempt").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        play_df = play_df.with_columns(
            opp_highlight_yards=pl.col("opp_highlight_yards").cast(pl.Float32),
            highlight_yards=pl.col("highlight_yards").cast(pl.Float32),
            line_yards=pl.col("line_yards").cast(pl.Float32),
            second_level_yards=pl.col("second_level_yards").cast(pl.Float32),
            open_field_yards=pl.col("open_field_yards").cast(pl.Float32),
        )

        team_rush_box = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .fill_null(0.0)
            .groupby(by=["pos_team"])
            .agg(
                rushing_stuff=pl.col("stuffed_run").sum(),
                rushing_stuff_rate=pl.col("stuffed_run").mean(),
                rushing_stopped=pl.col("stopped_run").sum(),
                rushing_stopped_rate=pl.col("stopped_run").mean(),
                rushing_opportunity=pl.col("opportunity_run").sum(),
                rushing_opportunity_rate=pl.col("opportunity_run").mean(),
                rushing_highlight=pl.col("highlight_run").sum(),
                rushing_highlight_rate=pl.col("highlight_run").mean(),
                rushing_highlight_yards=pl.col("highlight_yards").sum(),
                line_yards=pl.col("line_yards").sum(),
                line_yards_per_carry=pl.col("line_yards").mean(),
                second_level_yards=pl.col("second_level_yards").sum(),
                open_field_yards=pl.col("open_field_yards").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_rush_opp_box = (
            play_df.filter(
                (pl.col("rush") == True) & (pl.col("scrimmage_play") == True) & (pl.col("opportunity_run") == True)
            )
            .fill_null(0.0)
            .groupby(by=["pos_team"])
            .agg(
                rushing_highlight_yards_per_opp=pl.col("opp_highlight_yards").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_data_frames = [
            team_rush_opp_box,
            team_pen_box,
            team_sp_box,
            team_scrimmage_box_rush,
            team_scrimmage_box_pass,
            team_scrimmage_box,
            team_base_box,
            team_rush_base_box,
            team_rush_power_box,
            team_rush_box,
        ]
        team_box = reduce(lambda left, right: left.join(right, on=["pos_team"], how="outer"), team_data_frames)

        situation_box_normal = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .groupby(by=["pos_team"])
            .agg(
                EPA_success=pl.col("EPA_success").sum(),
                EPA_success_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_rz = (
            play_df.filter((pl.col("rz_play") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_rz=pl.col("EPA_success").sum(),
                EPA_success_rate_rz=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_third = (
            play_df.filter((pl.col("start.down") == 3) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_third=pl.col("EPA_success").sum(),
                EPA_success_rate_third=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_pass=pl.col("EPA_success").sum(),
                EPA_success_pass_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_rush=pl.col("EPA_success").sum(),
                EPA_success_rush_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_middle8 = (
            play_df.filter((pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                middle_8=pl.col("middle_8").sum(),
                middle_8_pass_rate=pl.col("pass").mean(),
                middle_8_rush_rate=pl.col("rush").mean(),
                EPA_middle_8=pl.col("EPA").sum(),
                EPA_middle_8_per_play=pl.col("EPA").mean(),
                EPA_middle_8_success=pl.col("EPA_success").sum(),
                EPA_middle_8_success_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_middle8_pass = (
            play_df.filter(
                (pl.col("pass") == True) & (pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True)
            )
            .groupby(by=["pos_team"])
            .agg(
                middle_8_pass=pl.col("pass").sum(),
                EPA_middle_8_pass=pl.col("EPA").sum(),
                EPA_middle_8_pass_per_play=pl.col("EPA").mean(),
                EPA_middle_8_success_pass=pl.col("EPA_success").sum(),
                EPA_middle_8_success_pass_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_middle8_rush = (
            play_df.filter(
                (pl.col("rush") == True) & (pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True)
            )
            .groupby(by=["pos_team"])
            .agg(
                middle_8_rush=pl.col("rush").sum(),
                EPA_middle_8_rush=pl.col("EPA").sum(),
                EPA_middle_8_rush_per_play=pl.col("EPA").mean(),
                EPA_middle_8_success_rush=pl.col("EPA_success").sum(),
                EPA_middle_8_success_rush_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_early = (
            play_df.filter((pl.col("early_down") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_early_down=pl.col("EPA_success").sum(),
                EPA_success_early_down_rate=pl.col("EPA_success").mean(),
                early_downs=pl.col("early_down").sum(),
                early_down_pass_rate=pl.col("pass").mean(),
                early_down_rush_rate=pl.col("rush").mean(),
                EPA_early_down=pl.col("EPA").sum(),
                EPA_early_down_per_play=pl.col("EPA").mean(),
                early_down_first_down=pl.col("first_down_created").sum(),
                early_down_first_down_rate=pl.col("first_down_created").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_early_pass = (
            play_df.filter(
                (pl.col("pass") == True) & (pl.col("early_down") == True) & (pl.col("scrimmage_play") == True)
            )
            .groupby(by=["pos_team"])
            .agg(
                early_down_pass=pl.col("pass").sum(),
                EPA_early_down_pass=pl.col("EPA").sum(),
                EPA_early_down_pass_per_play=pl.col("EPA").mean(),
                EPA_success_early_down_pass=pl.col("EPA_success").sum(),
                EPA_success_early_down_pass_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_early_rush = (
            play_df.filter(
                (pl.col("rush") == True) & (pl.col("early_down") == True) & (pl.col("scrimmage_play") == True)
            )
            .groupby(by=["pos_team"])
            .agg(
                early_down_rush=pl.col("rush").sum(),
                EPA_early_down_rush=pl.col("EPA").sum(),
                EPA_early_down_rush_per_play=pl.col("EPA").mean(),
                EPA_success_early_down_rush=pl.col("EPA_success").sum(),
                EPA_success_early_down_rush_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_late = (
            play_df.filter((pl.col("late_down") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_late_down=pl.col("EPA_success_late_down").sum(),
                EPA_success_late_down_pass=pl.col("EPA_success_late_down_pass").sum(),
                EPA_success_late_down_rush=pl.col("EPA_success_late_down_rush").sum(),
                late_downs=pl.col("late_down").sum(),
                late_down_pass=pl.col("late_down_pass").sum(),
                late_down_rush=pl.col("late_down_rush").sum(),
                EPA_late_down=pl.col("EPA").sum(),
                EPA_late_down_per_play=pl.col("EPA").mean(),
                EPA_success_late_down_rate=pl.col("EPA_success_late_down").mean(),
                EPA_success_late_down_pass_rate=pl.col("EPA_success_late_down_pass").mean(),
                EPA_success_late_down_rush_rate=pl.col("EPA_success_late_down_rush").mean(),
                late_down_pass_rate=pl.col("late_down_pass").mean(),
                late_down_rush_rate=pl.col("late_down_rush").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_standard = (
            play_df.filter((pl.col("standard_down") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_standard_down=pl.col("EPA_success").sum(),
                EPA_success_standard_down_rate=pl.col("EPA_success").mean(),
                EPA_standard_down=pl.col("EPA").sum(),
                EPA_standard_down_per_play=pl.col("EPA").mean(),
                standard_downs=pl.col("standard_down").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_passing = (
            play_df.filter((pl.col("passing_down") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["pos_team"])
            .agg(
                EPA_success_passing_down=pl.col("EPA_success").sum(),
                EPA_success_passing_down_rate=pl.col("EPA_success").mean(),
                EPA_passing_down=pl.col("EPA").sum(),
                EPA_passing_down_per_play=pl.col("EPA").mean(),
                passing_downs=pl.col("passing_down").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_data_frames = [
            situation_box_normal,
            situation_box_pass,
            situation_box_rush,
            situation_box_rz,
            situation_box_third,
            situation_box_early,
            situation_box_early_pass,
            situation_box_early_rush,
            situation_box_middle8,
            situation_box_middle8_pass,
            situation_box_middle8_rush,
            situation_box_late,
            situation_box_standard,
            situation_box_passing,
        ]

        situation_box = reduce(
            lambda left, right: left.join(right, on=["pos_team"], how="outer"), situation_data_frames
        )

        play_df = play_df.with_columns(
            drive_stopped=pl.col("drive_stopped").cast(pl.Float32),
            drive_start=pl.col("drive_start").cast(pl.Float32),
        )

        def_base_box = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .groupby(by=["def_pos_team"])
            .agg(
                scrimmage_plays=pl.col("scrimmage_play").sum(),
                TFL=pl.col("TFL").sum(),
                TFL_pass=pl.col("TFL_pass").sum(),
                TFL_rush=pl.col("TFL_rush").sum(),
                havoc_total=pl.col("havoc").sum(),
                havoc_total_rate=pl.col("havoc").mean(),
                fumbles=pl.col("forced_fumble").sum(),
                def_int=pl.col("int").sum(),
                drive_stopped_rate=100 * pl.col("drive_stopped").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                def_pos_team=pl.col("def_pos_team").cast(pl.Int32),
            )
        )

        def_box_havoc_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["def_pos_team"])
            .agg(
                num_pass_plays=pl.col("pass").sum(),
                havoc_total_pass=pl.col("havoc").sum(),
                havoc_total_pass_rate=pl.col("havoc").mean(),
                sacks=pl.col("sack_vec").sum(),
                sacks_rate=pl.col("sack_vec").mean(),
                pass_breakups=pl.col("pass_breakup").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                def_pos_team=pl.col("def_pos_team").cast(pl.Int32),
            )
        )

        def_box_havoc_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .groupby(by=["def_pos_team"])
            .agg(
                havoc_total_rush=pl.col("havoc").sum(),
                havoc_total_rush_rate=pl.col("havoc").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                def_pos_team=pl.col("def_pos_team").cast(pl.Int32),
            )
        )

        def_data_frames = [def_base_box, def_box_havoc_pass, def_box_havoc_rush]
        def_box = reduce(lambda left, right: left.join(right, on=["def_pos_team"], how="outer"), def_data_frames)
        def_box_json = json.loads(def_box.write_json(row_oriented=True))

        turnover_box = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .groupby(by=["pos_team"])
            .agg(
                pass_breakups=pl.col("pass_breakup").sum(),
                fumbles_lost=pl.col("fumble_lost").sum(),
                fumbles_recovered=pl.col("fumble_recovered").sum(),
                total_fumbles=pl.col("fumble_vec").sum(),
                Int=pl.col("int").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        turnover_box_json = json.loads(turnover_box.write_json(row_oriented=True))
        if len(turnover_box_json) < 2:
            for i in range(len(turnover_box_json), 2):
                turnover_box_json.append({})

        turnover_box_json[0]["Int"] = int(turnover_box_json[0].get("Int", 0))
        turnover_box_json[1]["Int"] = int(turnover_box_json[1].get("Int", 0))

        away_passes_def = turnover_box_json[0].get("pass_breakups", 0)
        away_passes_int = turnover_box_json[0].get("Int", 0)
        away_fumbles = turnover_box_json[0].get("total_fumbles", 0)
        turnover_box_json[0]["expected_turnovers"] = (0.5 * away_fumbles) + (
            0.22 * (away_passes_def + away_passes_int)
        )

        home_passes_def = turnover_box_json[1].get("pass_breakups", 0)
        home_passes_int = turnover_box_json[1].get("Int", 0)
        home_fumbles = turnover_box_json[1].get("total_fumbles", 0)
        turnover_box_json[1]["expected_turnovers"] = (0.5 * home_fumbles) + (
            0.22 * (home_passes_def + home_passes_int)
        )

        turnover_box_json[0]["expected_turnover_margin"] = (
            turnover_box_json[1]["expected_turnovers"] - turnover_box_json[0]["expected_turnovers"]
        )
        turnover_box_json[1]["expected_turnover_margin"] = (
            turnover_box_json[0]["expected_turnovers"] - turnover_box_json[1]["expected_turnovers"]
        )

        away_to = turnover_box_json[0].get("fumbles_lost", 0) + turnover_box_json[0]["Int"]
        home_to = turnover_box_json[1].get("fumbles_lost", 0) + turnover_box_json[1]["Int"]

        turnover_box_json[0]["turnovers"] = away_to
        turnover_box_json[1]["turnovers"] = home_to

        turnover_box_json[0]["turnover_margin"] = home_to - away_to
        turnover_box_json[1]["turnover_margin"] = away_to - home_to

        turnover_box_json[0]["turnover_luck"] = 5.0 * (
            turnover_box_json[0]["turnover_margin"] - turnover_box_json[0]["expected_turnover_margin"]
        )
        turnover_box_json[1]["turnover_luck"] = 5.0 * (
            turnover_box_json[1]["turnover_margin"] - turnover_box_json[1]["expected_turnover_margin"]
        )

        drives_data = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .groupby(by=["pos_team"])
            .agg(
                drive_total_available_yards=pl.col("drive_start").sum(),
                drive_total_gained_yards=pl.col("drive.yards").sum(),
                avg_field_position=pl.col("drive_start").mean(),
                plays_per_drive=pl.col("drive.offensivePlays").mean(),
                yards_per_drive=pl.col("drive.yards").mean(),
                drives=pl.col("drive.id").n_unique(),
                drive_total_gained_yards_rate=100 * pl.col("drive.yards").sum() / pl.col("drive_start").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        return {
            "pass": json.loads(passer_box.write_json(row_oriented=True)),
            "rush": json.loads(rusher_box.write_json(row_oriented=True)),
            "receiver": json.loads(receiver_box.write_json(row_oriented=True)),
            "team": json.loads(team_box.write_json(row_oriented=True)),
            "situational": json.loads(situation_box.write_json(row_oriented=True)),
            "defensive": def_box_json,
            "turnover": turnover_box_json,
            "drives": json.loads(drives_data.write_json(row_oriented=True)),
        }

    def run_processing_pipeline(self):
        if self.ran_pipeline == False:
            pbp_txt = self.__helper_cfb_pbp_drives(self.json)
            self.plays_json = pbp_txt["plays"]

            pbp_json = {
                "gameId": int(self.gameId),
                "plays": self.plays_json.to_dicts(),
                "season": pbp_txt["season"],
                "week": pbp_txt["header"]["week"],
                "gameInfo": pbp_txt["gameInfo"],
                "teamInfo": pbp_txt["header"]["competitions"][0],
                "playByPlaySource": pbp_txt.get("header").get("competitions")[0].get("playByPlaySource"),
                "drives": pbp_txt["drives"],
                "boxscore": pbp_txt["boxscore"],
                "header": pbp_txt["header"],
                "standings": pbp_txt["standings"],
                "leaders": np.array(pbp_txt["leaders"]).tolist(),
                "timeouts": pbp_txt["timeouts"],
                "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                "gameSpread": pbp_txt["gameSpread"],
                "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                "overUnder": pbp_txt["overUnder"],
                "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                "videos": np.array(pbp_txt["videos"]).tolist(),
            }
            self.json = pbp_json
            self.plays_json = pbp_txt["plays"]

            confirmed_corrupt = self.corrupt_pbp_check()

            if confirmed_corrupt:
                return self.json if self.return_keys is None else {k: self.json.get(f"{k}") for k in self.return_keys}

            if (pbp_json.get("header").get("competitions")[0].get("playByPlaySource") != "none") and (
                len(pbp_txt["drives"]) > 0
            ):
                self.plays_json = (
                    self.plays_json.pipe(self.__add_downs_data)
                    .pipe(self.__add_play_type_flags)
                    .pipe(self.__add_rush_pass_flags)
                    .pipe(self.__add_team_score_variables)
                    .pipe(self.__add_new_play_types)
                    .pipe(self.__setup_penalty_data)
                    .pipe(self.__add_play_category_flags)
                    .pipe(self.__add_yardage_cols)
                    .pipe(self.__add_player_cols)
                    .pipe(self.__after_cols)
                    .pipe(self.__add_spread_time)
                    .pipe(self.__process_epa)
                    .pipe(self.__process_wpa)
                    .pipe(self.__add_drive_data)
                    .pipe(self.__process_qbr)
                )
                self.ran_pipeline = True
                advBoxScore = self.plays_json.pipe(self.create_box_score)
                self.plays_json = self.plays_json.to_dicts()
                pbp_json = {
                    "gameId": int(self.gameId),
                    "plays": self.plays_json,
                    "season": pbp_txt["season"],
                    "week": pbp_txt["header"]["week"],
                    "gameInfo": pbp_txt["gameInfo"],
                    "teamInfo": pbp_txt["header"]["competitions"][0],
                    "playByPlaySource": pbp_txt["playByPlaySource"],
                    "drives": pbp_txt["drives"],
                    "boxscore": pbp_txt["boxscore"],
                    "advBoxScore": advBoxScore,
                    "header": pbp_txt["header"],
                    "standings": pbp_txt["standings"],
                    "leaders": np.array(pbp_txt["leaders"]).tolist(),
                    "timeouts": pbp_txt["timeouts"],
                    "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                    "gameSpread": pbp_txt["gameSpread"],
                    "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                    "overUnder": pbp_txt["overUnder"],
                    "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                    "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                    "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                    "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                    "videos": np.array(pbp_txt["videos"]).tolist(),
                }
                self.json = pbp_json
            self.ran_pipeline = True
            return self.json if self.return_keys is None else {k: self.json.get(f"{k}") for k in self.return_keys}

    def run_cleaning_pipeline(self):
        if self.ran_cleaning_pipeline == False:
            pbp_txt = self.__helper_cfb_pbp_drives(self.json)
            self.plays_json = pbp_txt["plays"]

            pbp_json = {
                "gameId": int(self.gameId),
                "plays": self.plays_json.to_dicts(),
                "season": pbp_txt["season"],
                "week": pbp_txt["header"]["week"],
                "gameInfo": pbp_txt["gameInfo"],
                "teamInfo": pbp_txt["header"]["competitions"][0],
                "playByPlaySource": pbp_txt.get("header").get("competitions")[0].get("playByPlaySource"),
                "drives": pbp_txt["drives"],
                "boxscore": pbp_txt["boxscore"],
                "header": pbp_txt["header"],
                "standings": pbp_txt["standings"],
                "leaders": np.array(pbp_txt["leaders"]).tolist(),
                "timeouts": pbp_txt["timeouts"],
                "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                "gameSpread": pbp_txt["gameSpread"],
                "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                "overUnder": pbp_txt["overUnder"],
                "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                "videos": np.array(pbp_txt["videos"]).tolist(),
            }
            self.json = pbp_json
            self.plays_json = pbp_txt["plays"]

            confirmed_corrupt = self.corrupt_pbp_check()

            if confirmed_corrupt:
                return self.json if self.return_keys is None else {k: self.json.get(f"{k}") for k in self.return_keys}

            if (
                pbp_json.get("header").get("competitions")[0].get("playByPlaySource") != "none"
                and len(pbp_txt["drives"]) > 0
            ):
                self.plays_json = (
                    self.plays_json.pipe(self.__add_downs_data)
                    .pipe(self.__add_play_type_flags)
                    .pipe(self.__add_rush_pass_flags)
                    .pipe(self.__add_team_score_variables)
                    .pipe(self.__add_new_play_types)
                    .pipe(self.__setup_penalty_data)
                    .pipe(self.__add_play_category_flags)
                    .pipe(self.__add_yardage_cols)
                    .pipe(self.__add_player_cols)
                    .pipe(self.__after_cols)
                    .pipe(self.__add_spread_time)
                )
                self.plays_json = self.plays_json.to_dicts()
                pbp_json = {
                    "gameId": int(self.gameId),
                    "plays": self.plays_json,
                    "season": pbp_txt["season"],
                    "week": pbp_txt["header"]["week"],
                    "gameInfo": pbp_txt["gameInfo"],
                    "teamInfo": pbp_txt["header"]["competitions"][0],
                    "playByPlaySource": pbp_txt["playByPlaySource"],
                    "drives": pbp_txt["drives"],
                    "boxscore": pbp_txt["boxscore"],
                    "header": pbp_txt["header"],
                    "standings": pbp_txt["standings"],
                    "leaders": np.array(pbp_txt["leaders"]).tolist(),
                    "timeouts": pbp_txt["timeouts"],
                    "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                    "gameSpread": pbp_txt["gameSpread"],
                    "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                    "overUnder": pbp_txt["overUnder"],
                    "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                    "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                    "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                    "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                    "videos": np.array(pbp_txt["videos"]).tolist(),
                }
                self.json = pbp_json
            self.ran_cleaning_pipeline = True
            return self.json

    def corrupt_pbp_check(self):
        if len(self.json["plays"]) == 0:
            logging.debug(
                f"{self.gameId}: appear to be too no plays available ({len(self.json['plays'])}). run_processing_pipeline did not run"
            )
            return True
        if (len(self.json["plays"]) < 50) and (
            self.json.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(
                f"{self.gameId}: appear to be too few plays ({len(self.json['plays'])}) for a completed game. run_processing_pipeline did not run"
            )
            return True
        if (len(self.json["plays"]) > 500) and (
            self.json.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(
                f"{self.gameId}: appear to be too many plays ({len(self.json['plays'])}) for a completed game. run_processing_pipeline did not run"
            )
            return True
        return False
