import pandas as pd
import numpy as np
import re
import os
import json
import time
import pkg_resources
from xgboost import Booster, DMatrix
from numpy.core.fromnumeric import mean
from functools import reduce, partial
from sportsdataverse.dl_utils import download, key_check
from .model_vars import *

# "td" : float(p[0]),
# "opp_td" : float(p[1]),
# "fg" : float(p[2]),
# "opp_fg" : float(p[3]),
# "safety" : float(p[4]),
# "opp_safety" : float(p[5]),
# "no_score" : float(p[6])
ep_model_file = pkg_resources.resource_filename(
    "sportsdataverse", "cfb/models/ep_model.model"
)
wp_spread_file = pkg_resources.resource_filename(
    "sportsdataverse", "cfb/models/wp_spread.model"
)
qbr_model_file = pkg_resources.resource_filename(
    "sportsdataverse", "cfb/models/qbr_model.model"
)

ep_model = Booster({"nthread": 4})  # init model
ep_model.load_model(ep_model_file)

wp_model = Booster({"nthread": 4})  # init model
wp_model.load_model(wp_spread_file)

qbr_model = Booster({"nthread": 4})  # init model
qbr_model.load_model(qbr_model_file)

class CFBPlayProcess(object):

    gameId = 0
    # logger = None
    ran_pipeline = False
    ran_cleaning_pipeline = False
    raw = False
    path_to_json = '/'
    def __init__(self, gameId=0, raw=False, path_to_json='/'):
        self.gameId = int(gameId)
        # self.logger = logger
        self.ran_pipeline = False
        self.ran_cleaning_pipeline = False
        self.raw = raw
        self.path_to_json = path_to_json

    def espn_cfb_pbp(self):
        """espn_cfb_pbp() - Pull the game by id. Data from API endpoints: `college-football/playbyplay`, `college-football/summary`

        Args:
            game_id (int): Unique game_id, can be obtained from cfb_schedule().

        Returns:
            Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
             "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder",
             "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP",
             "gameInfo", "season"

        Example:
            `cfb_df = sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp()`
        """
        cache_buster = int(time.time() * 1000)
        pbp_txt = {}
        pbp_txt["timeouts"] = {}
        # summary endpoint for pickcenter array
        summary_url = f"http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={self.gameId}&{cache_buster}"
        summary_resp = download(summary_url)
        summary = json.loads(summary_resp)
        incoming_keys_expected = [
            'boxscore', 'format', 'gameInfo', 'drives', 'leaders', 'broadcasts',
            'predictor', 'pickcenter', 'againstTheSpread', 'odds', 'winprobability',
            'header', 'scoringPlays', 'videos', 'standings'
        ]
        dict_keys_expected = [
            'boxscore', 'format', 'gameInfo', 'drives', 'predictor',
            'header', 'standings'
        ]
        array_keys_expected = [
            'leaders', 'broadcasts', 'pickcenter','againstTheSpread',
            'odds', 'winprobability', 'scoringPlays', 'videos'
        ]
        if self.raw == True:
            # reorder keys in raw format, appending empty keys which are defined later to the end
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
            "espnWP",
            "gameInfo",
            "season",
            "leaders",
        ]:
            pbp_txt[k] = key_check(obj=summary, key=k)
        for k in ['news','shop']:
            pbp_txt.pop('{}'.format(k), None)
        self.json = pbp_txt

        return self.json

    def cfb_pbp_disk(self):
        with open(os.path.join(self.path_to_json, "{}.json".format(self.gameId))) as json_file:
            pbp_txt = json.load(json_file)
            self.json = pbp_txt
        return self.json

    def __helper_cfb_pbp_drives(self, pbp_txt):
        pbp_txt, gameSpread, overUnder, homeFavorite, gameSpreadAvailable, \
            homeTeamId, homeTeamMascot, homeTeamName,\
            homeTeamAbbrev, homeTeamNameAlt,\
            awayTeamId, awayTeamMascot, awayTeamName,\
            awayTeamAbbrev, awayTeamNameAlt = self.__helper_cfb_pbp(pbp_txt)

        pbp_txt["plays"] = pd.DataFrame()
        # negotiating the drive meta keys into columns after unnesting drive plays
        # concatenating the previous and current drives categories when necessary
        if "drives" in pbp_txt.keys() and pbp_txt.get('header').get('competitions')[0].get('playByPlaySource') != 'none':
            pbp_txt = self.__helper_cfb_pbp_features(pbp_txt, \
                gameSpread, gameSpreadAvailable, \
                overUnder, homeFavorite, \
                homeTeamId, homeTeamMascot, \
                homeTeamName, homeTeamAbbrev, homeTeamNameAlt, \
                awayTeamId, awayTeamMascot, awayTeamName, \
                awayTeamAbbrev, awayTeamNameAlt)
        else:
            pbp_txt["drives"] = {}
        return pbp_txt

    def __helper_cfb_sort_plays__(self, plays_df):
        plays_df = plays_df.sort_values(
            by=["id", "start.adj_TimeSecsRem"]
        )
        
        # 03-Sept-2023: ESPN changed their handling of OT, slotting every play of OT into the same quarter instead of adding new periods. 
        # Sort all of these plays separately
        pbp_ot = plays_df[
            plays_df["period.number"] >= 5
        ]

        plays_df = plays_df.drop(pbp_ot.index, axis = 0)

        pbp_ot = pbp_ot.sort_values(by = ["sequenceNumber"])
        plays_df = pd.concat([
            plays_df,
            pbp_ot
        ])
        return plays_df

    def __helper_cfb_pbp_features(self, pbp_txt,
        gameSpread, gameSpreadAvailable,
        overUnder, homeFavorite,
        homeTeamId, homeTeamMascot,
        homeTeamName, homeTeamAbbrev, homeTeamNameAlt,
        awayTeamId, awayTeamMascot, awayTeamName,
        awayTeamAbbrev, awayTeamNameAlt):
        pbp_txt["plays"] = pd.DataFrame()
        for key in pbp_txt.get("drives").keys():
            prev_drives = pd.json_normalize(
                    data=pbp_txt.get("drives").get("{}".format(key)),
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
            pbp_txt["plays"] = pd.concat(
                [pbp_txt["plays"], prev_drives], ignore_index=True
            )

        pbp_txt["plays"] = pbp_txt["plays"].to_dict(orient="records")
        pbp_txt["plays"] = pd.DataFrame(pbp_txt["plays"])
        pbp_txt["plays"]["game_id"] = int(self.gameId)
        pbp_txt["plays"]["season"] = pbp_txt.get("header").get("season").get("year")
        pbp_txt["plays"]["seasonType"] = pbp_txt.get("header").get("season").get("type")
        pbp_txt["plays"]["homeTeamId"] = homeTeamId
        pbp_txt["plays"]["awayTeamId"] = awayTeamId
        pbp_txt["plays"]["homeTeamName"] = str(homeTeamName)
        pbp_txt["plays"]["awayTeamName"] = str(awayTeamName)
        pbp_txt["plays"]["homeTeamMascot"] = str(homeTeamMascot)
        pbp_txt["plays"]["awayTeamMascot"] = str(awayTeamMascot)
        pbp_txt["plays"]["homeTeamAbbrev"] = str(homeTeamAbbrev)
        pbp_txt["plays"]["awayTeamAbbrev"] = str(awayTeamAbbrev)
        pbp_txt["plays"]["homeTeamNameAlt"] = str(homeTeamNameAlt)
        pbp_txt["plays"]["awayTeamNameAlt"] = str(awayTeamNameAlt)
        pbp_txt["plays"]["period.number"] = pbp_txt["plays"]["period.number"].apply(
                lambda x: int(x)
            )
        pbp_txt["plays"]["homeTeamSpread"] = np.where(
            homeFavorite == True, abs(gameSpread), -1 * abs(gameSpread)
        )
        pbp_txt["plays"]["gameSpread"] = gameSpread
        pbp_txt["plays"]["gameSpreadAvailable"] = gameSpreadAvailable
        pbp_txt["plays"]["overUnder"] = float(overUnder)
        pbp_txt["plays"]["homeFavorite"] = homeFavorite
        pbp_txt["homeTeamSpread"] = np.where(
            homeFavorite == True, abs(gameSpread), -1 * abs(gameSpread)
        )
        pbp_txt["gameSpread"] = gameSpread
        pbp_txt["gameSpreadAvailable"] = gameSpreadAvailable
        pbp_txt["overUnder"] = float(overUnder)
        pbp_txt["homeFavorite"] = homeFavorite

            # ----- Figuring out Timeouts ---------
        pbp_txt["timeouts"] = {}
        pbp_txt["timeouts"][homeTeamId] = {"1": [], "2": []}
        pbp_txt["timeouts"][awayTeamId] = {"1": [], "2": []}

            # ----- Time ---------------
        pbp_txt["plays"]["clock.mm"] = pbp_txt["plays"][
                "clock.displayValue"
            ].str.split(pat=":")
        pbp_txt["plays"][["clock.minutes", "clock.seconds"]] = pbp_txt["plays"][
                "clock.mm"
            ].to_list()
        pbp_txt["plays"]["half"] = np.where(
                pbp_txt["plays"]["period.number"] <= 2, "1", "2"
            )
        pbp_txt["plays"]["lag_half"] = pbp_txt["plays"]["half"].shift(1)
        pbp_txt["plays"]["lead_half"] = pbp_txt["plays"]["half"].shift(-1)
        pbp_txt["plays"]["start.TimeSecsRem"] = np.where(
                pbp_txt["plays"]["period.number"].isin([1, 3]),
                900
                + 60 * pbp_txt["plays"]["clock.minutes"].astype(int)
                + pbp_txt["plays"]["clock.seconds"].astype(int),
                60 * pbp_txt["plays"]["clock.minutes"].astype(int)
                + pbp_txt["plays"]["clock.seconds"].astype(int),
            )
        pbp_txt["plays"]["start.adj_TimeSecsRem"] = np.select(
                [
                    pbp_txt["plays"]["period.number"] == 1,
                    pbp_txt["plays"]["period.number"] == 2,
                    pbp_txt["plays"]["period.number"] == 3,
                    pbp_txt["plays"]["period.number"] == 4,
                ],
                [
                    2700
                    + 60 * pbp_txt["plays"]["clock.minutes"].astype(int)
                    + pbp_txt["plays"]["clock.seconds"].astype(int),
                    1800
                    + 60 * pbp_txt["plays"]["clock.minutes"].astype(int)
                    + pbp_txt["plays"]["clock.seconds"].astype(int),
                    900
                    + 60 * pbp_txt["plays"]["clock.minutes"].astype(int)
                    + pbp_txt["plays"]["clock.seconds"].astype(int),
                    60 * pbp_txt["plays"]["clock.minutes"].astype(int)
                    + pbp_txt["plays"]["clock.seconds"].astype(int),
                ],
                default=60 * pbp_txt["plays"]["clock.minutes"].astype(int)
                + pbp_txt["plays"]["clock.seconds"].astype(int),
            )
        # Pos Team - Start and End Id
        pbp_txt["plays"]["id"] = pbp_txt["plays"]["id"].apply(lambda x: int(x))
        pbp_txt["plays"] = self.__helper_cfb_sort_plays__(pbp_txt["plays"])

        # drop play text dupes intelligently, even if they have different play_id values
        pbp_txt["plays"]["text"] = pbp_txt["plays"]["text"].astype(str)
        pbp_txt["plays"]["lead_text"] = pbp_txt["plays"]["text"].shift(-1)
        pbp_txt["plays"]["lead_start_team"] = pbp_txt["plays"]["start.team.id"].shift(-1)
        pbp_txt["plays"]["lead_start_yardsToEndzone"] = pbp_txt["plays"]["start.yardsToEndzone"].shift(-1)
        pbp_txt["plays"]["lead_start_down"] = pbp_txt["plays"]["start.down"].shift(-1)
        pbp_txt["plays"]["lead_start_distance"] = pbp_txt["plays"]["start.distance"].shift(-1)
        pbp_txt["plays"]["lead_scoringPlay"] = pbp_txt["plays"]["scoringPlay"].shift(-1)
        pbp_txt["plays"]["text_dupe"] = False

        def play_text_dupe_checker(row):
            if (row["start.team.id"] == row["lead_start_team"]) and \
                (row["start.down"] == row["lead_start_down"]) and \
                (row["start.yardsToEndzone"] == row["lead_start_yardsToEndzone"]) and \
                (row["start.distance"] == row["lead_start_distance"]):
                if (row["text"] == row["lead_text"]):
                    return True
                if (row["text"] in row["lead_text"]) and \
                    (row["lead_scoringPlay"] == row["scoringPlay"]):
                    return True
            return False

        pbp_txt["plays"]["text_dupe"] = pbp_txt["plays"].apply(lambda x: play_text_dupe_checker(x), axis=1)

        pbp_txt["plays"] = pbp_txt["plays"][pbp_txt["plays"]["text_dupe"] == False]

        pbp_txt["plays"]["game_play_number"] = np.arange(len(pbp_txt["plays"])) + 1
        pbp_txt["plays"]["start.team.id"] = (
                pbp_txt["plays"]["start.team.id"]
                # fill downward first to make sure all playIDs are accurate
                .ffill()
                # fill upward so that any remaining NAs are covered
                .bfill()
                .apply(lambda x: int(x))
            )
        pbp_txt["plays"]["end.team.id"] = (
                pbp_txt["plays"]["end.team.id"]
                .fillna(value=pbp_txt["plays"]["start.team.id"])
                .apply(lambda x: int(x))
            )
        pbp_txt["plays"]["start.pos_team.id"] = np.select(
                [
                    (pbp_txt["plays"]["type.text"].isin(kickoff_vec))
                    & (
                        pbp_txt["plays"]["start.team.id"].astype(int)
                        == pbp_txt["plays"]["homeTeamId"].astype(int)
                    ),
                    (pbp_txt["plays"]["type.text"].isin(kickoff_vec))
                    & (
                        pbp_txt["plays"]["start.team.id"].astype(int)
                        == pbp_txt["plays"]["awayTeamId"].astype(int)
                    ),
                ],
                [
                    pbp_txt["plays"]["awayTeamId"].astype(int),
                    pbp_txt["plays"]["homeTeamId"].astype(int),
                ],
                default=pbp_txt["plays"]["start.team.id"].astype(int),
            )
        pbp_txt["plays"]["start.def_pos_team.id"] = np.where(
                pbp_txt["plays"]["start.pos_team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                pbp_txt["plays"]["awayTeamId"].astype(int),
                pbp_txt["plays"]["homeTeamId"].astype(int),
            )
        pbp_txt["plays"]["end.def_team.id"] = np.where(
                pbp_txt["plays"]["end.team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                pbp_txt["plays"]["awayTeamId"].astype(int),
                pbp_txt["plays"]["homeTeamId"].astype(int),
            )
        pbp_txt["plays"]["end.pos_team.id"] = pbp_txt["plays"]["end.team.id"].apply(
                lambda x: int(x)
            )
        pbp_txt["plays"]["end.def_pos_team.id"] = pbp_txt["plays"][
                "end.def_team.id"
            ].apply(lambda x: int(x))
        pbp_txt["plays"]["start.pos_team.name"] = np.where(
                pbp_txt["plays"]["start.pos_team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                pbp_txt["plays"]["homeTeamName"],
                pbp_txt["plays"]["awayTeamName"],
            )
        pbp_txt["plays"]["start.def_pos_team.name"] = np.where(
                pbp_txt["plays"]["start.pos_team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                pbp_txt["plays"]["awayTeamName"],
                pbp_txt["plays"]["homeTeamName"],
            )
        pbp_txt["plays"]["end.pos_team.name"] = np.where(
                pbp_txt["plays"]["end.pos_team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                pbp_txt["plays"]["homeTeamName"],
                pbp_txt["plays"]["awayTeamName"],
            )
        pbp_txt["plays"]["end.def_pos_team.name"] = np.where(
                pbp_txt["plays"]["end.pos_team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                pbp_txt["plays"]["awayTeamName"],
                pbp_txt["plays"]["homeTeamName"],
            )
        pbp_txt["plays"]["start.is_home"] = np.where(
                pbp_txt["plays"]["start.pos_team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                True,
                False,
            )
        pbp_txt["plays"]["end.is_home"] = np.where(
                pbp_txt["plays"]["end.pos_team.id"].astype(int)
                == pbp_txt["plays"]["homeTeamId"].astype(int),
                True,
                False,
            )
        pbp_txt["plays"]["homeTimeoutCalled"] = np.where(
                (pbp_txt["plays"]["type.text"] == "Timeout")
                & (
                    (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(homeTeamAbbrev), case=False)
                    )
                    | (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(homeTeamName), case=False)
                    )
                    | (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(homeTeamMascot), case=False)
                    )
                    | (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(homeTeamNameAlt), case=False)
                    )
                ),
                True,
                False,
            )
        pbp_txt["plays"]["awayTimeoutCalled"] = np.where(
                (pbp_txt["plays"]["type.text"] == "Timeout")
                & (
                    (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(awayTeamAbbrev), case=False)
                    )
                    | (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(awayTeamName), case=False)
                    )
                    | (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(awayTeamMascot), case=False)
                    )
                    | (
                        pbp_txt["plays"]["text"]
                        .str.lower()
                        .str.contains(str(awayTeamNameAlt), case=False)
                    )
                ),
                True,
                False,
            )
        pbp_txt["timeouts"][homeTeamId]["1"] = (
                pbp_txt["plays"]
                .loc[
                    (pbp_txt["plays"]["homeTimeoutCalled"] == True)
                    & (pbp_txt["plays"]["period.number"] <= 2)
                ]
                .reset_index()["id"]
            )
        pbp_txt["timeouts"][homeTeamId]["2"] = (
                pbp_txt["plays"]
                .loc[
                    (pbp_txt["plays"]["homeTimeoutCalled"] == True)
                    & (pbp_txt["plays"]["period.number"] > 2)
                ]
                .reset_index()["id"]
            )
        pbp_txt["timeouts"][awayTeamId]["1"] = (
                pbp_txt["plays"]
                .loc[
                    (pbp_txt["plays"]["awayTimeoutCalled"] == True)
                    & (pbp_txt["plays"]["period.number"] <= 2)
                ]
                .reset_index()["id"]
            )
        pbp_txt["timeouts"][awayTeamId]["2"] = (
                pbp_txt["plays"]
                .loc[
                    (pbp_txt["plays"]["awayTimeoutCalled"] == True)
                    & (pbp_txt["plays"]["period.number"] > 2)
                ]
                .reset_index()["id"]
            )

        pbp_txt["timeouts"][homeTeamId]["1"] = pbp_txt["timeouts"][homeTeamId][
                "1"
            ].apply(lambda x: int(x))
        pbp_txt["timeouts"][homeTeamId]["2"] = pbp_txt["timeouts"][homeTeamId][
                "2"
            ].apply(lambda x: int(x))
        pbp_txt["timeouts"][awayTeamId]["1"] = pbp_txt["timeouts"][awayTeamId][
                "1"
            ].apply(lambda x: int(x))
        pbp_txt["timeouts"][awayTeamId]["2"] = pbp_txt["timeouts"][awayTeamId][
                "2"
            ].apply(lambda x: int(x))
        pbp_txt["plays"]["end.homeTeamTimeouts"] = (
                3
                - pbp_txt["plays"]
                .apply(
                    lambda x: (
                        (pbp_txt["timeouts"][homeTeamId]["1"] <= x["id"])
                        & (x["period.number"] <= 2)
                    )
                    | (
                        (pbp_txt["timeouts"][homeTeamId]["2"] <= x["id"])
                        & (x["period.number"] > 2)
                    ),
                    axis=1,
                )
                .apply(lambda x: int(x.sum()), axis=1)
            )
        pbp_txt["plays"]["end.awayTeamTimeouts"] = (
                3
                - pbp_txt["plays"]
                .apply(
                    lambda x: (
                        (pbp_txt["timeouts"][awayTeamId]["1"] <= x["id"])
                        & (x["period.number"] <= 2)
                    )
                    | (
                        (pbp_txt["timeouts"][awayTeamId]["2"] <= x["id"])
                        & (x["period.number"] > 2)
                    ),
                    axis=1,
                )
                .apply(lambda x: int(x.sum()), axis=1)
            )
        pbp_txt["plays"]["start.homeTeamTimeouts"] = pbp_txt["plays"][
                "end.homeTeamTimeouts"
            ].shift(1)
        pbp_txt["plays"]["start.awayTeamTimeouts"] = pbp_txt["plays"][
                "end.awayTeamTimeouts"
            ].shift(1)
        pbp_txt["plays"]["start.homeTeamTimeouts"] = np.where(
                (pbp_txt["plays"]["game_play_number"] == 1)
                | (
                    (pbp_txt["plays"]["half"] == "2")
                    & (pbp_txt["plays"]["lag_half"] == "1")
                ),
                3,
                pbp_txt["plays"]["start.homeTeamTimeouts"],
            )
        pbp_txt["plays"]["start.awayTeamTimeouts"] = np.where(
                (pbp_txt["plays"]["game_play_number"] == 1)
                | (
                    (pbp_txt["plays"]["half"] == "2")
                    & (pbp_txt["plays"]["lag_half"] == "1")
                ),
                3,
                pbp_txt["plays"]["start.awayTeamTimeouts"],
            )
        pbp_txt["plays"]["start.homeTeamTimeouts"] = pbp_txt["plays"][
                "start.homeTeamTimeouts"
            ].apply(lambda x: int(x))
        pbp_txt["plays"]["start.awayTeamTimeouts"] = pbp_txt["plays"][
                "start.awayTeamTimeouts"
            ].apply(lambda x: int(x))
        pbp_txt["plays"]["end.TimeSecsRem"] = pbp_txt["plays"][
                "start.TimeSecsRem"
            ].shift(1)
        pbp_txt["plays"]["end.adj_TimeSecsRem"] = pbp_txt["plays"][
                "start.adj_TimeSecsRem"
            ].shift(1)
        pbp_txt["plays"]["end.TimeSecsRem"] = np.where(
                (pbp_txt["plays"]["game_play_number"] == 1)
                | (
                    (pbp_txt["plays"]["half"] == "2")
                    & (pbp_txt["plays"]["lag_half"] == "1")
                ),
                1800,
                pbp_txt["plays"]["end.TimeSecsRem"],
            )
        pbp_txt["plays"]["end.adj_TimeSecsRem"] = np.select(
                [
                    (pbp_txt["plays"]["game_play_number"] == 1),
                    (
                        (pbp_txt["plays"]["half"] == "2")
                        & (pbp_txt["plays"]["lag_half"] == "1")
                    ),
                ],
                [3600, 1800],
                default=pbp_txt["plays"]["end.adj_TimeSecsRem"],
            )
        pbp_txt["plays"]["start.posTeamTimeouts"] = np.where(
                pbp_txt["plays"]["start.pos_team.id"] == pbp_txt["plays"]["homeTeamId"],
                pbp_txt["plays"]["start.homeTeamTimeouts"],
                pbp_txt["plays"]["start.awayTeamTimeouts"],
            )
        pbp_txt["plays"]["start.defPosTeamTimeouts"] = np.where(
                pbp_txt["plays"]["start.def_pos_team.id"]
                == pbp_txt["plays"]["homeTeamId"],
                pbp_txt["plays"]["start.homeTeamTimeouts"],
                pbp_txt["plays"]["start.awayTeamTimeouts"],
            )
        pbp_txt["plays"]["end.posTeamTimeouts"] = np.where(
                pbp_txt["plays"]["end.pos_team.id"] == pbp_txt["plays"]["homeTeamId"],
                pbp_txt["plays"]["end.homeTeamTimeouts"],
                pbp_txt["plays"]["end.awayTeamTimeouts"],
            )
        pbp_txt["plays"]["end.defPosTeamTimeouts"] = np.where(
                pbp_txt["plays"]["end.def_pos_team.id"]
                == pbp_txt["plays"]["homeTeamId"],
                pbp_txt["plays"]["end.homeTeamTimeouts"],
                pbp_txt["plays"]["end.awayTeamTimeouts"],
            )
        pbp_txt["firstHalfKickoffTeamId"] = np.where(
                (pbp_txt["plays"]["game_play_number"] == 1)
                & (pbp_txt["plays"]["type.text"].isin(kickoff_vec))
                & (pbp_txt["plays"]["start.team.id"] == pbp_txt["plays"]["homeTeamId"]),
                pbp_txt["plays"]["homeTeamId"],
                pbp_txt["plays"]["awayTeamId"],
            )
        pbp_txt["plays"]["firstHalfKickoffTeamId"] = pbp_txt[
                "firstHalfKickoffTeamId"
            ]
        pbp_txt["plays"]["period"] = pbp_txt["plays"]["period.number"]
        pbp_txt["plays"]["start.yard"] = np.where(
                (pbp_txt["plays"]["start.team.id"] == homeTeamId),
                100 - pbp_txt["plays"]["start.yardLine"],
                pbp_txt["plays"]["start.yardLine"],
            )
        pbp_txt["plays"]["start.yardsToEndzone"] = np.where(
                pbp_txt["plays"]["start.yardLine"].isna() == False,
                pbp_txt["plays"]["start.yardsToEndzone"],
                pbp_txt["plays"]["start.yard"],
            )
        pbp_txt["plays"]["start.yardsToEndzone"] = np.where(
                pbp_txt["plays"]["start.yardsToEndzone"] == 0,
                pbp_txt["plays"]["start.yard"],
                pbp_txt["plays"]["start.yardsToEndzone"],
            )
        pbp_txt["plays"]["end.yard"] = np.where(
                (pbp_txt["plays"]["end.team.id"] == homeTeamId),
                100 - pbp_txt["plays"]["end.yardLine"],
                pbp_txt["plays"]["end.yardLine"],
            )
        pbp_txt["plays"]["end.yard"] = np.where(
                (pbp_txt["plays"]["type.text"] == "Penalty")
                & (
                    pbp_txt["plays"]["text"].str.contains(
                        "declined", case=False, flags=0, na=False, regex=True
                    )
                ),
                pbp_txt["plays"]["start.yard"],
                pbp_txt["plays"]["end.yard"],
            )
        pbp_txt["plays"]["end.yardsToEndzone"] = np.where(
                pbp_txt["plays"]["end.yardLine"].isna() == False,
                pbp_txt["plays"]["end.yardsToEndzone"],
                pbp_txt["plays"]["end.yard"],
            )
        pbp_txt["plays"]["end.yardsToEndzone"] = np.where(
                (pbp_txt["plays"]["type.text"] == "Penalty")
                & (
                    pbp_txt["plays"]["text"].str.contains(
                        "declined", case=False, flags=0, na=False, regex=True
                    )
                ),
                pbp_txt["plays"]["start.yardsToEndzone"],
                pbp_txt["plays"]["end.yardsToEndzone"],
            )

        pbp_txt["plays"]["start.distance"] = np.where(
                (pbp_txt["plays"]["start.distance"] == 0)
                & (
                    pbp_txt["plays"]["start.downDistanceText"]
                    .str.lower()
                    .str.contains("goal")
                ),
                pbp_txt["plays"]["start.yardsToEndzone"],
                pbp_txt["plays"]["start.distance"],
            )

        pbp_txt["timeouts"][homeTeamId]["1"] = np.array(
                pbp_txt["timeouts"][homeTeamId]["1"]
            ).tolist()
        pbp_txt["timeouts"][homeTeamId]["2"] = np.array(
                pbp_txt["timeouts"][homeTeamId]["2"]
            ).tolist()
        pbp_txt["timeouts"][awayTeamId]["1"] = np.array(
                pbp_txt["timeouts"][awayTeamId]["1"]
            ).tolist()
        pbp_txt["timeouts"][awayTeamId]["2"] = np.array(
                pbp_txt["timeouts"][awayTeamId]["2"]
            ).tolist()
        if "scoringType.displayName" in pbp_txt["plays"].keys():
            pbp_txt["plays"]["type.text"] = np.where(
                    pbp_txt["plays"]["scoringType.displayName"] == "Field Goal",
                    "Field Goal Good",
                    pbp_txt["plays"]["type.text"],
                )
            pbp_txt["plays"]["type.text"] = np.where(
                    pbp_txt["plays"]["scoringType.displayName"] == "Extra Point",
                    "Extra Point Good",
                    pbp_txt["plays"]["type.text"],
                )

        pbp_txt["plays"]["playType"] = np.where(
                pbp_txt["plays"]["type.text"].isna() == False,
                pbp_txt["plays"]["type.text"],
                "Unknown",
            )
        pbp_txt["plays"]["type.text"] = np.where(
                pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("extra point", case=False)
                & pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("no good", case=False),
                "Extra Point Missed",
                pbp_txt["plays"]["type.text"],
            )
        pbp_txt["plays"]["type.text"] = np.where(
                pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("extra point", case=False)
                & pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("blocked", case=False),
                "Extra Point Missed",
                pbp_txt["plays"]["type.text"],
            )
        pbp_txt["plays"]["type.text"] = np.where(
                pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("field goal", case=False)
                & pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("blocked", case=False),
                "Blocked Field Goal",
                pbp_txt["plays"]["type.text"],
            )
        pbp_txt["plays"]["type.text"] = np.where(
                pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("field goal", case=False)
                & pbp_txt["plays"]["text"]
                .str.lower()
                .str.contains("no good", case=False),
                "Field Goal Missed",
                pbp_txt["plays"]["type.text"],
            )
        del pbp_txt["plays"]["clock.mm"]
        pbp_txt["plays"] = pbp_txt["plays"].replace({np.nan: None})
        return pbp_txt

    def __helper_cfb_pbp(self, pbp_txt):
        gameSpread, overUnder, homeFavorite, gameSpreadAvailable = self.__helper_cfb_pickcenter(pbp_txt)
        pbp_txt['timeouts'] = {}
        pbp_txt['teamInfo'] = pbp_txt['header']['competitions'][0]
        pbp_txt['season'] = pbp_txt['header']['season']
        pbp_txt['playByPlaySource'] = pbp_txt['header']['competitions'][0]['playByPlaySource']
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
        return pbp_txt, gameSpread, overUnder, homeFavorite, gameSpreadAvailable, homeTeamId,\
            homeTeamMascot,homeTeamName,homeTeamAbbrev,homeTeamNameAlt,\
            awayTeamId,awayTeamMascot,awayTeamName,awayTeamAbbrev,awayTeamNameAlt

    def __helper_cfb_pickcenter(self, pbp_txt):
                # Spread definition
        if len(pbp_txt.get("pickcenter",[])) > 1:
            homeFavorite = pbp_txt.get("pickcenter",{})[0].get("homeTeamOdds",{}).get("favorite","")
            if "spread" in pbp_txt.get("pickcenter",{})[1].keys():
                gameSpread = pbp_txt.get("pickcenter",{})[1].get("spread","")
                overUnder = pbp_txt.get("pickcenter",{})[1].get("overUnder","")
                gameSpreadAvailable = True
            else:
                gameSpread = pbp_txt.get("pickcenter",{})[0].get("spread","")
                overUnder = pbp_txt.get("pickcenter",{})[0].get("overUnder","")
                gameSpreadAvailable = True
            # self.logger.info(f"Spread: {gameSpread}, home Favorite: {homeFavorite}, ou: {overUnder}")
        else:
            gameSpread = 2.5
            overUnder = 55.5
            homeFavorite = True
            gameSpreadAvailable = False
        return gameSpread, overUnder, homeFavorite, gameSpreadAvailable

    def __setup_penalty_data(self):
        """
        Creates the following columns in self.plays_json:
            * Penalty flag
            * Penalty declined
            * Penalty no play
            * Penalty off-set
            * Penalty 1st down conversion
            * Penalty in text
            * Yds Penalty
        """
        ##-- 'Penalty' in play text ----
        # -- T/F flag conditions penalty_flag
        self.plays_json["penalty_flag"] = False
        self.plays_json.loc[(self.plays_json["type.text"] == "Penalty"), "penalty_flag"] = True
        self.plays_json.loc[
            self.plays_json["text"].str.contains(
                "penalty", case=False, flags=0, na=False, regex=True
            ),
            "penalty_flag",
        ] = True

        # -- T/F flag conditions penalty_declined
        self.plays_json["penalty_declined"] = False
        self.plays_json.loc[
            (self.plays_json["type.text"] == "Penalty")
            & (
                self.plays_json["text"].str.contains(
                    "declined", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_declined",
        ] = True
        self.plays_json.loc[
            (
                self.plays_json["text"].str.contains(
                    "penalty", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json["text"].str.contains(
                    "declined", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_declined",
        ] = True

        # -- T/F flag conditions penalty_no_play
        self.plays_json["penalty_no_play"] = False
        self.plays_json.loc[
            (self.plays_json["type.text"] == "Penalty")
            & (
                self.plays_json["text"].str.contains(
                    "no play", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_no_play",
        ] = True
        self.plays_json.loc[
            (
                self.plays_json["text"].str.contains(
                    "penalty", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json["text"].str.contains(
                    "no play", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_no_play",
        ] = True

        # -- T/F flag conditions penalty_offset
        self.plays_json["penalty_offset"] = False
        self.plays_json.loc[
            (self.plays_json["type.text"] == "Penalty")
            & (
                self.plays_json["text"].str.contains(
                    r"off-setting", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_offset",
        ] = True
        self.plays_json.loc[
            (
                self.plays_json["text"].str.contains(
                    "penalty", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json["text"].str.contains(
                    r"off-setting", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_offset",
        ] = True

        # -- T/F flag conditions penalty_1st_conv
        self.plays_json["penalty_1st_conv"] = False
        self.plays_json.loc[
            (self.plays_json["type.text"] == "Penalty")
            & (
                self.plays_json["text"].str.contains(
                    "1st down", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_1st_conv",
        ] = True
        self.plays_json.loc[
            (
                self.plays_json["text"].str.contains(
                    "penalty", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json["text"].str.contains(
                    "1st down", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_1st_conv",
        ] = True

        # -- T/F flag for penalty text but not penalty play type --
        self.plays_json["penalty_in_text"] = False
        self.plays_json.loc[
            (
                self.plays_json["text"].str.contains(
                    "penalty", case=False, flags=0, na=False, regex=True
                )
            )
            & (~(self.plays_json["type.text"] == "Penalty"))
            & (
                ~self.plays_json["text"].str.contains(
                    "declined", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                ~self.plays_json["text"].str.contains(
                    r"off-setting", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                ~self.plays_json["text"].str.contains(
                    "no play", case=False, flags=0, na=False, regex=True
                )
            ),
            "penalty_in_text",
        ] = True

        self.plays_json["penalty_detail"] = np.select(
            [
                (self.plays_json.penalty_offset == 1),
                (self.plays_json.penalty_declined == 1),
                self.plays_json.text.str.contains(" roughing passer ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " offensive holding ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" pass interference", case=False, regex=True),
                self.plays_json.text.str.contains(" encroachment", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " defensive pass interference ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " offensive pass interference ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " illegal procedure ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " defensive holding ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" holding ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " offensive offside | offside offense", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " defensive offside | offside defense", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" offside ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " illegal fair catch signal ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" illegal batting ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " neutral zone infraction ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " ineligible downfield ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " illegal use of hands ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " kickoff out of bounds | kickoff out-of-bounds ",
                    case=False,
                    regex=True,
                ),
                self.plays_json.text.str.contains(
                    " 12 men on the field ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" illegal block ", case=False, regex=True),
                self.plays_json.text.str.contains(" personal foul ", case=False, regex=True),
                self.plays_json.text.str.contains(" false start ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " substitution infraction ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " illegal formation ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" illegal touching ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " sideline interference ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" clipping ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " sideline infraction ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" crackback ", case=False, regex=True),
                self.plays_json.text.str.contains(" illegal snap ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " illegal helmet contact ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" roughing holder ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " horse collar tackle ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " illegal participation ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" tripping ", case=False, regex=True),
                self.plays_json.text.str.contains(" illegal shift ", case=False, regex=True),
                self.plays_json.text.str.contains(" illegal motion ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " roughing the kicker ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" delay of game ", case=False, regex=True),
                self.plays_json.text.str.contains(" targeting ", case=False, regex=True),
                self.plays_json.text.str.contains(" face mask ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " illegal forward pass ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " intentional grounding ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(" illegal kicking ", case=False, regex=True),
                self.plays_json.text.str.contains(" illegal conduct ", case=False, regex=True),
                self.plays_json.text.str.contains(
                    " kick catching interference ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " unnecessary roughness ", case=False, regex=True
                ),
                self.plays_json.text.str.contains("Penalty, UR"),
                self.plays_json.text.str.contains(
                    " unsportsmanlike conduct ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " running into kicker ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " failure to wear required equipment ", case=False, regex=True
                ),
                self.plays_json.text.str.contains(
                    " player disqualification ", case=False, regex=True
                ),
                (self.plays_json.penalty_flag == True),
            ],
            [
                "Off-Setting",
                "Penalty Declined",
                "Roughing the Passer",
                "Offensive Holding",
                "Pass Interference",
                "Encroachment",
                "Defensive Pass Interference",
                "Offensive Pass Interference",
                "Illegal Procedure",
                "Defensive Holding",
                "Holding",
                "Offensive Offside",
                "Defensive Offside",
                "Offside",
                "Illegal Fair Catch Signal",
                "Illegal Batting",
                "Neutral Zone Infraction",
                "Ineligible Man Down-Field",
                "Illegal Use of Hands",
                "Kickoff Out-of-Bounds",
                "12 Men on the Field",
                "Illegal Block",
                "Personal Foul",
                "False Start",
                "Substitution Infraction",
                "Illegal Formation",
                "Illegal Touching",
                "Sideline Interference",
                "Clipping",
                "Sideline Infraction",
                "Crackback",
                "Illegal Snap",
                "Illegal Helmet contact",
                "Roughing the Holder",
                "Horse-Collar Tackle",
                "Illegal Participation",
                "Tripping",
                "Illegal Shift",
                "Illegal Motion",
                "Roughing the Kicker",
                "Delay of Game",
                "Targeting",
                "Face Mask",
                "Illegal Forward Pass",
                "Intentional Grounding",
                "Illegal Kicking",
                "Illegal Conduct",
                "Kick Catch Interference",
                "Unnecessary Roughness",
                "Unnecessary Roughness",
                "Unsportsmanlike Conduct",
                "Running Into Kicker",
                "Failure to Wear Required Equipment",
                "Player Disqualification",
                "Missing",
            ],
            default=None,
        )

        self.plays_json["penalty_text"] = np.select(
            [(self.plays_json.penalty_flag == True)],
            [self.plays_json.text.str.extract(r"Penalty(.+)", flags=re.IGNORECASE)[0]],
            default=None,
        )

        self.plays_json["yds_penalty"] = np.select(
            [(self.plays_json.penalty_flag == True)],
            [
                self.plays_json.penalty_text.str.extract(
                    "(.{0,3})yards|yds|yd to the ", flags=re.IGNORECASE
                )[0]
            ],
            default=None,
        )
        self.plays_json["yds_penalty"] = self.plays_json["yds_penalty"].str.replace(
            " yards to the | yds to the | yd to the ", "", regex = True
        )
        self.plays_json["yds_penalty"] = np.select(
            [
                (self.plays_json.penalty_flag == True)
                & (self.plays_json.text.str.contains(r"ards\)", case=False, regex=True))
                & (self.plays_json.yds_penalty.isna()),
            ],
            [
                self.plays_json.text.str.extract(
                    r"(.{0,4})yards\)|Yards\)|yds\)|Yds\)", flags=re.IGNORECASE
                )[0]
            ],
            default=self.plays_json.yds_penalty,
        )
        self.plays_json["yds_penalty"] = self.plays_json.yds_penalty.str.replace(
            "yards\\)|Yards\\)|yds\\)|Yds\\)", "", regex = True
        ).str.replace("\\(", "", regex = True)
        return self.plays_json

    def __add_downs_data(self):
        """
        Creates the following columns in self.plays_json:
            * id, drive_id, game_id
            * down, ydstogo (distance), game_half, period
        """
        self.plays_json = self.plays_json.copy(deep=True)
        self.plays_json.loc[:, "id"] = self.plays_json["id"].astype(float)
        self.plays_json = self.__helper_cfb_sort_plays__(self.plays_json)
        self.plays_json.drop_duplicates(
            subset=["text", "id", "type.text", "start.down", "sequenceNumber"], keep="last", inplace=True
        )
        self.plays_json = self.plays_json[
            (
                self.plays_json["type.text"].str.contains(
                    "end of|coin toss|end period|wins toss", case=False, regex=True
                )
                == False
            )
        ]

        self.plays_json.loc[:, "period"] = self.plays_json["period.number"].astype(int)
        self.plays_json.loc[(self.plays_json.period <= 2), "half"] = 1
        self.plays_json.loc[(self.plays_json.period > 2), "half"] = 2
        self.plays_json["lead_half"] = self.plays_json.half.shift(-1)
        self.plays_json["lag_scoringPlay"] = self.plays_json.scoringPlay.shift(1)
        self.plays_json.loc[self.plays_json.lead_half.isna() == True, "lead_half"] = 2
        self.plays_json["end_of_half"] = self.plays_json.half != self.plays_json.lead_half

        self.plays_json["down_1"] = self.plays_json["start.down"] == 1
        self.plays_json["down_2"] = self.plays_json["start.down"] == 2
        self.plays_json["down_3"] = self.plays_json["start.down"] == 3
        self.plays_json["down_4"] = self.plays_json["start.down"] == 4

        self.plays_json["down_1_end"] = self.plays_json["end.down"] == 1
        self.plays_json["down_2_end"] = self.plays_json["end.down"] == 2
        self.plays_json["down_3_end"] = self.plays_json["end.down"] == 3
        self.plays_json["down_4_end"] = self.plays_json["end.down"] == 4
        return self.plays_json

    def __add_play_type_flags(self):
        """
        Creates the following columns in self.plays_json:
            * Flags for fumbles, scores, kickoffs, punts, field goals
        """
        # --- Touchdown, Fumble, Special Teams flags -----------------
        self.plays_json.loc[:, "scoring_play"] = False
        self.plays_json.loc[self.plays_json["type.text"].isin(scores_vec), "scoring_play"] = True
        self.plays_json["td_play"] = self.plays_json.text.str.contains(
            r"touchdown|for a TD", case=False, flags=0, na=False, regex=True
        )
        self.plays_json["touchdown"] = self.plays_json["type.text"].str.contains(
            "touchdown", case=False, flags=0, na=False, regex=True
        )
        ## Portion of touchdown check for plays where touchdown is not listed in the play_type--
        self.plays_json["td_check"] = self.plays_json["text"].str.contains(
            "Touchdown", case=False, flags=0, na=False, regex=True
        )
        self.plays_json["safety"] = self.plays_json["text"].str.contains(
            "safety", case=False, flags=0, na=False, regex=True
        )
        # --- Fumbles----
        self.plays_json["fumble_vec"] = np.select(
            [
                self.plays_json["text"].str.contains("fumble", case=False, flags=0, na=False, regex=True),
                (~self.plays_json["text"].str.contains("fumble", case=False, flags=0, na=False, regex=True)) & (self.plays_json["type.text"] == "Rush") & (self.plays_json["start.pos_team.id"] != self.plays_json["end.pos_team.id"]),
                (~self.plays_json["text"].str.contains("fumble", case=False, flags=0, na=False, regex=True)) & (self.plays_json["type.text"] == "Sack") & (self.plays_json["start.pos_team.id"] != self.plays_json["end.pos_team.id"]),
            ],
            [
                True,
                True,
                True
            ],
            default=False,
        )
        self.plays_json["forced_fumble"] = self.plays_json["text"].str.contains(
            "forced by", case=False, flags=0, na=False, regex=True
        )
        # --- Kicks----
        self.plays_json["kickoff_play"] = self.plays_json["type.text"].isin(kickoff_vec)
        self.plays_json["kickoff_tb"] = np.select(
            [
                (
                    self.plays_json["text"].str.contains(
                        "touchback", case=False, flags=0, na=False, regex=True
                    )
                )
                & (self.plays_json.kickoff_play == True),
                (
                    self.plays_json["text"].str.contains(
                        "kickoff$", case=False, flags=0, na=False, regex=True
                    )
                )
                & (self.plays_json.kickoff_play == True),
            ],
            [True, True],
            default=False,
        )
        self.plays_json["kickoff_onside"] = (
            self.plays_json["text"].str.contains(
                r"on-side|onside|on side", case=False, flags=0, na=False, regex=True
            )
        ) & (self.plays_json.kickoff_play == True)
        self.plays_json["kickoff_oob"] = (
            self.plays_json["text"].str.contains(
                r"out-of-bounds|out of bounds",
                case=False,
                flags=0,
                na=False,
                regex=True,
            )
        ) & (self.plays_json.kickoff_play == True)

        self.plays_json["kickoff_fair_catch"] = (
            self.plays_json["text"].str.contains(
                r"fair catch|fair caught", case=False, flags=0, na=False, regex=True
            )
        ) & (self.plays_json.kickoff_play == True)
        self.plays_json["kickoff_downed"] = (
            self.plays_json["text"].str.contains(
                "downed", case=False, flags=0, na=False, regex=True
            )
        ) & (self.plays_json.kickoff_play == True)
        self.plays_json["kick_play"] = self.plays_json["text"].str.contains(
            r"kick|kickoff", case=False, flags=0, na=False, regex=True
        )
        self.plays_json["kickoff_safety"] = (
            (~self.plays_json["type.text"].isin(["Blocked Punt", "Penalty"]))
            & (
                self.plays_json["text"].str.contains(
                    "kickoff", case=False, flags=0, na=False, regex=True
                )
            )
            & (self.plays_json.safety == True)
        )
        # --- Punts----
        self.plays_json["punt"] = np.where(self.plays_json["type.text"].isin(punt_vec), True, False)
        self.plays_json["punt_play"] = self.plays_json["text"].str.contains(
            "punt", case=False, flags=0, na=False, regex=True
        )
        self.plays_json["punt_tb"] = np.where(
            (
                self.plays_json["text"].str.contains(
                    "touchback", case=False, flags=0, na=False, regex=True
                )
            )
            & (self.plays_json.punt == True),
            True,
            False,
        )
        self.plays_json["punt_oob"] = np.where(
            (
                self.plays_json["text"].str.contains(
                    r"out-of-bounds|out of bounds",
                    case=False,
                    flags=0,
                    na=False,
                    regex=True,
                )
            )
            & (self.plays_json.punt == True),
            True,
            False,
        )
        self.plays_json["punt_fair_catch"] = np.where(
            (
                self.plays_json["text"].str.contains(
                    r"fair catch|fair caught", case=False, flags=0, na=False, regex=True
                )
            )
            & (self.plays_json.punt == True),
            True,
            False,
        )
        self.plays_json["punt_downed"] = np.where(
            (
                self.plays_json["text"].str.contains(
                    "downed", case=False, flags=0, na=False, regex=True
                )
            )
            & (self.plays_json.punt == True),
            True,
            False,
        )
        self.plays_json["punt_safety"] = np.where(
            (self.plays_json["type.text"].isin(["Blocked Punt", "Punt"]))
            & (
                self.plays_json["text"].str.contains(
                    "punt", case=False, flags=0, na=False, regex=True
                )
            )
            & (self.plays_json.safety == True),
            True,
            False,
        )
        self.plays_json["penalty_safety"] = np.where(
            (self.plays_json["type.text"].isin(["Penalty"])) & (self.plays_json.safety == True),
            True,
            False,
        )
        self.plays_json["punt_blocked"] = np.where(
            (
                self.plays_json["text"].str.contains(
                    "blocked", case=False, flags=0, na=False, regex=True
                )
            )
            & (self.plays_json.punt == True),
            True,
            False,
        )
        return self.plays_json

    def __add_rush_pass_flags(self):
        """
        Creates the following columns in self.plays_json:
            * Rush, Pass, Sacks
        """
        # --- Pass/Rush----
        self.plays_json["rush"] = np.where(
            (
                (self.plays_json["type.text"] == "Rush")
                | (self.plays_json["type.text"] == "Rushing Touchdown")
                | (
                    self.plays_json["type.text"].isin(
                        [
                            "Safety",
                            "Fumble Recovery (Opponent)",
                            "Fumble Recovery (Opponent) Touchdown",
                            "Fumble Recovery (Own)",
                            "Fumble Recovery (Own) Touchdown",
                            "Fumble Return Touchdown",
                        ]
                    )
                    & self.plays_json["text"].str.contains("run for")
                )
            ),
            True,
            False,
        )
        self.plays_json["pass"] = np.where(
            (
                (
                    self.plays_json["type.text"].isin(
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
                | (
                    (self.plays_json["type.text"] == "Safety")
                    & (
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Safety")
                    & (
                        self.plays_json["text"].str.contains(
                            "pass complete", case=False, flags=0, na=False, regex=True
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Recovery (Own)")
                    & (
                        self.plays_json["text"].str.contains(
                            r"pass complete|pass incomplete|pass intercepted",
                            case=False,
                            flags=0,
                            na=False,
                            regex=True,
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Recovery (Own)")
                    & (
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Recovery (Own) Touchdown")
                    & (
                        self.plays_json["text"].str.contains(
                            r"pass complete|pass incomplete|pass intercepted",
                            case=False,
                            flags=0,
                            na=False,
                            regex=True,
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Recovery (Own) Touchdown")
                    & (
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Recovery (Opponent)")
                    & (
                        self.plays_json["text"].str.contains(
                            r"pass complete|pass incomplete|pass intercepted",
                            case=False,
                            flags=0,
                            na=False,
                            regex=True,
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Recovery (Opponent)")
                    & (
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Recovery (Opponent) Touchdown")
                    & (
                        self.plays_json["text"].str.contains(
                            r"pass complete|pass incomplete",
                            case=False,
                            flags=0,
                            na=False,
                            regex=True,
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Return Touchdown")
                    & (
                        self.plays_json["text"].str.contains(
                            r"pass complete|pass incomplete",
                            case=False,
                            flags=0,
                            na=False,
                            regex=True,
                        )
                    )
                )
                | (
                    (self.plays_json["type.text"] == "Fumble Return Touchdown")
                    & (
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                )
            ),
            True,
            False,
        )
        # --- Sacks----
        self.plays_json["sack_vec"] = np.where(
            (
                (self.plays_json["type.text"].isin(["Sack", "Sack Touchdown"]))
                | (
                    (
                        self.plays_json["type.text"].isin(
                            [
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Return Touchdown",
                            ]
                        )
                        & (self.plays_json["pass"] == True)
                        & (
                            self.plays_json["text"].str.contains(
                                "sacked", case=False, flags=0, na=False, regex=True
                            )
                        )
                    )
                )
            ),
            True,
            False,
        )
        self.plays_json["pass"] = np.where(self.plays_json["sack_vec"] == True, True, self.plays_json["pass"])
        return self.plays_json

    def __add_team_score_variables(self):
        """
        Creates the following columns in self.plays_json:
            * Team Score variables
            * Fix change of poss variables
        """
        # -------------------------
        self.plays_json["pos_team"] = self.plays_json["start.pos_team.id"]
        self.plays_json["def_pos_team"] = self.plays_json["start.def_pos_team.id"]
        self.plays_json["is_home"] = self.plays_json.pos_team == self.plays_json["homeTeamId"]
        # --- Team Score variables ------
        self.plays_json["lag_homeScore"] = self.plays_json["homeScore"].shift(1)
        self.plays_json["lag_awayScore"] = self.plays_json["awayScore"].shift(1)
        self.plays_json["lag_HA_score_diff"] = (
            self.plays_json["lag_homeScore"] - self.plays_json["lag_awayScore"]
        )
        self.plays_json["HA_score_diff"] = self.plays_json["homeScore"] - self.plays_json["awayScore"]
        self.plays_json["net_HA_score_pts"] = (
            self.plays_json["HA_score_diff"] - self.plays_json["lag_HA_score_diff"]
        )
        self.plays_json["H_score_diff"] = self.plays_json["homeScore"] - self.plays_json["lag_homeScore"]
        self.plays_json["A_score_diff"] = self.plays_json["awayScore"] - self.plays_json["lag_awayScore"]
        self.plays_json["homeScore"] = np.select(
            [
                (self.plays_json.scoringPlay == False)
                & (self.plays_json["game_play_number"] != 1)
                & (self.plays_json["H_score_diff"] >= 9),
                (self.plays_json.scoringPlay == False)
                & (self.plays_json["game_play_number"] != 1)
                & (self.plays_json["H_score_diff"] < 9)
                & (self.plays_json["H_score_diff"] > 1),
                (self.plays_json.scoringPlay == False)
                & (self.plays_json["game_play_number"] != 1)
                & (self.plays_json["H_score_diff"] >= -9)
                & (self.plays_json["H_score_diff"] < -1),
            ],
            [self.plays_json["lag_homeScore"], self.plays_json["lag_homeScore"], self.plays_json["homeScore"]],
            default=self.plays_json["homeScore"],
        )
        self.plays_json["awayScore"] = np.select(
            [
                (self.plays_json.scoringPlay == False)
                & (self.plays_json["game_play_number"] != 1)
                & (self.plays_json["A_score_diff"] >= 9),
                (self.plays_json.scoringPlay == False)
                & (self.plays_json["game_play_number"] != 1)
                & (self.plays_json["A_score_diff"] < 9)
                & (self.plays_json["A_score_diff"] > 1),
                (self.plays_json.scoringPlay == False)
                & (self.plays_json["game_play_number"] != 1)
                & (self.plays_json["A_score_diff"] >= -9)
                & (self.plays_json["A_score_diff"] < -1),
            ],
            [self.plays_json["lag_awayScore"], self.plays_json["lag_awayScore"], self.plays_json["awayScore"]],
            default=self.plays_json["awayScore"],
        )
        self.plays_json.drop(["lag_homeScore", "lag_awayScore"], axis=1, inplace=True)
        self.plays_json["lag_homeScore"] = self.plays_json["homeScore"].shift(1)
        self.plays_json["lag_homeScore"] = np.where(
            (self.plays_json.lag_homeScore.isna()), 0, self.plays_json["lag_homeScore"]
        )
        self.plays_json["lag_awayScore"] = self.plays_json["awayScore"].shift(1)
        self.plays_json["lag_awayScore"] = np.where(
            (self.plays_json.lag_awayScore.isna()), 0, self.plays_json["lag_awayScore"]
        )
        self.plays_json["start.homeScore"] = np.where(
            (self.plays_json["game_play_number"] == 1), 0, self.plays_json["lag_homeScore"]
        )
        self.plays_json["start.awayScore"] = np.where(
            (self.plays_json["game_play_number"] == 1), 0, self.plays_json["lag_awayScore"]
        )
        self.plays_json["end.homeScore"] = self.plays_json["homeScore"]
        self.plays_json["end.awayScore"] = self.plays_json["awayScore"]
        self.plays_json["pos_team_score"] = np.where(
            self.plays_json.pos_team == self.plays_json["homeTeamId"],
            self.plays_json.homeScore,
            self.plays_json.awayScore,
        )
        self.plays_json["def_pos_team_score"] = np.where(
            self.plays_json.pos_team == self.plays_json["homeTeamId"],
            self.plays_json.awayScore,
            self.plays_json.homeScore,
        )
        self.plays_json["start.pos_team_score"] = np.where(
            self.plays_json["start.pos_team.id"] == self.plays_json["homeTeamId"],
            self.plays_json["start.homeScore"],
            self.plays_json["start.awayScore"],
        )
        self.plays_json["start.def_pos_team_score"] = np.where(
            self.plays_json["start.pos_team.id"] == self.plays_json["homeTeamId"],
            self.plays_json["start.awayScore"],
            self.plays_json["start.homeScore"],
        )
        self.plays_json["start.pos_score_diff"] = (
            self.plays_json["start.pos_team_score"] - self.plays_json["start.def_pos_team_score"]
        )
        self.plays_json["end.pos_team_score"] = np.where(
            self.plays_json["end.pos_team.id"] == self.plays_json["homeTeamId"],
            self.plays_json["end.homeScore"],
            self.plays_json["end.awayScore"],
        )
        self.plays_json["end.def_pos_team_score"] = np.where(
            self.plays_json["end.pos_team.id"] == self.plays_json["homeTeamId"],
            self.plays_json["end.awayScore"],
            self.plays_json["end.homeScore"],
        )
        self.plays_json["end.pos_score_diff"] = (
            self.plays_json["end.pos_team_score"] - self.plays_json["end.def_pos_team_score"]
        )
        self.plays_json["lag_pos_team"] = self.plays_json["pos_team"].shift(1)
        self.plays_json.loc[
            self.plays_json.lag_pos_team.isna() == True, "lag_pos_team"
        ] = self.plays_json.pos_team
        self.plays_json["lead_pos_team"] = self.plays_json["pos_team"].shift(-1)
        self.plays_json["lead_pos_team2"] = self.plays_json["pos_team"].shift(-2)
        self.plays_json["pos_score_diff"] = self.plays_json.pos_team_score - self.plays_json.def_pos_team_score
        self.plays_json["lag_pos_score_diff"] = self.plays_json["pos_score_diff"].shift(1)
        self.plays_json.loc[self.plays_json.lag_pos_score_diff.isna(), "lag_pos_score_diff"] = 0
        self.plays_json["pos_score_pts"] = np.where(
            self.plays_json.lag_pos_team == self.plays_json.pos_team,
            self.plays_json.pos_score_diff - self.plays_json.lag_pos_score_diff,
            self.plays_json.pos_score_diff + self.plays_json.lag_pos_score_diff,
        )
        self.plays_json["pos_score_diff_start"] = np.select(
            [
                (self.plays_json.kickoff_play == True)
                & (self.plays_json.lag_pos_team == self.plays_json.pos_team),
                (self.plays_json.kickoff_play == True)
                | (self.plays_json.lag_pos_team != self.plays_json.pos_team),
            ],
            [self.plays_json.lag_pos_score_diff, -1 * self.plays_json.lag_pos_score_diff],
            default=self.plays_json.lag_pos_score_diff,
        )
        # --- Timeouts ------
        self.plays_json.loc[
            self.plays_json.pos_score_diff_start.isna() == True, "pos_score_diff_start"
        ] = self.plays_json.pos_score_diff
        self.plays_json["start.pos_team_receives_2H_kickoff"] = (
            self.plays_json["start.pos_team.id"] == self.plays_json.firstHalfKickoffTeamId
        )
        self.plays_json["end.pos_team_receives_2H_kickoff"] = (
            self.plays_json["end.pos_team.id"] == self.plays_json.firstHalfKickoffTeamId
        )
        self.plays_json["change_of_poss"] = np.where(
            self.plays_json["start.pos_team.id"] == self.plays_json["end.pos_team.id"], False, True
        )
        self.plays_json["change_of_poss"] = np.where(
            self.plays_json["change_of_poss"].isna(), 0, self.plays_json["change_of_poss"]
        )
        return self.plays_json

    def __add_new_play_types(self):
        """
        Creates the following columns in self.plays_json:
            * Fix play types
        """
        # --------------------------------------------------
        ## Fix Strip-Sacks to Fumbles----
        self.plays_json["type.text"] = np.where(
            (self.plays_json.fumble_vec == True)
            & (self.plays_json["pass"] == True)
            & (self.plays_json.change_of_poss == 1)
            & (self.plays_json.td_play == False)
            & (self.plays_json["start.down"] != 4)
            & ~(self.plays_json["type.text"].isin(defense_score_vec)),
            "Fumble Recovery (Opponent)",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json.fumble_vec == True)
            & (self.plays_json["pass"] == True)
            & (self.plays_json.change_of_poss == 1)
            & (self.plays_json.td_play == True),
            "Fumble Recovery (Opponent) Touchdown",
            self.plays_json["type.text"],
        )
        ## Fix rushes with fumbles and a change of possession to fumbles----
        self.plays_json["type.text"] = np.where(
            (self.plays_json.fumble_vec == True)
            & (self.plays_json["rush"] == True)
            & (self.plays_json.change_of_poss == 1)
            & (self.plays_json.td_play == False)
            & (self.plays_json["start.down"] != 4)
            & ~(self.plays_json["type.text"].isin(defense_score_vec)),
            "Fumble Recovery (Opponent)",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json.fumble_vec == True)
            & (self.plays_json["rush"] == True)
            & (self.plays_json.change_of_poss == 1)
            & (self.plays_json.td_play == True),
            "Fumble Recovery (Opponent) Touchdown",
            self.plays_json["type.text"],
        )

        # -- Fix kickoff fumble return TDs ----
        self.plays_json["type.text"] = np.where(
            (self.plays_json.kickoff_play == True)
            & (self.plays_json.change_of_poss == 1)
            & (self.plays_json.td_play == True)
            & (self.plays_json.td_check == True),
            "Kickoff Return Touchdown",
            self.plays_json["type.text"],
        )
        # -- Fix punt return TDs ----
        self.plays_json["type.text"] = np.where(
            (self.plays_json.punt_play == True)
            & (self.plays_json.td_play == True)
            & (self.plays_json.td_check == True),
            "Punt Return Touchdown",
            self.plays_json["type.text"],
        )
        # -- Fix kick return TDs----
        self.plays_json["type.text"] = np.where(
            (self.plays_json.kickoff_play == True)
            & (self.plays_json.fumble_vec == False)
            & (self.plays_json.td_play == True)
            & (self.plays_json.td_check == True),
            "Kickoff Return Touchdown",
            self.plays_json["type.text"],
        )
        # -- Fix rush/pass tds that aren't explicit----
        self.plays_json["type.text"] = np.where(
            (self.plays_json.td_play == True)
            & (self.plays_json.rush == True)
            & (self.plays_json.fumble_vec == False)
            & (self.plays_json.td_check == True),
            "Rushing Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json.td_play == True)
            & (self.plays_json["pass"] == True)
            & (self.plays_json.fumble_vec == False)
            & (self.plays_json.td_check == True)
            & ~(self.plays_json["type.text"].isin(int_vec)),
            "Passing Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json["pass"] == True)
            & (self.plays_json["type.text"].isin(["Pass Reception", "Pass Completion", "Pass"]))
            & (self.plays_json.statYardage == self.plays_json["start.yardsToEndzone"])
            & (self.plays_json.fumble_vec == False)
            & ~(self.plays_json["type.text"].isin(int_vec)),
            "Passing Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"].isin(["Blocked Field Goal"]))
            & (
                self.plays_json["text"].str.contains(
                    "for a TD", case=False, flags=0, na=False, regex=True
                )
            ),
            "Blocked Field Goal Touchdown",
            self.plays_json["type.text"],
        )

        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"].isin(["Blocked Punt"]))
            & (
                self.plays_json["text"].str.contains(
                    "for a TD", case=False, flags=0, na=False, regex=True
                )
            ),
            "Blocked Punt Touchdown",
            self.plays_json["type.text"],
        )
        # -- Fix duplicated TD play_type labels----
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"] == "Punt Touchdown Touchdown",
            "Punt Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"] == "Fumble Return Touchdown Touchdown",
            "Fumble Return Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"] == "Rushing Touchdown Touchdown",
            "Rushing Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"] == "Uncategorized Touchdown Touchdown",
            "Uncategorized Touchdown",
            self.plays_json["type.text"],
        )
        # -- Fix Pass Interception Return TD play_type labels----
        self.plays_json["type.text"] = np.where(
            self.plays_json["text"].str.contains(
                "pass intercepted for a TD", case=False, flags=0, na=False, regex=True
            ),
            "Interception Return Touchdown",
            self.plays_json["type.text"],
        )
        # -- Fix Sack/Fumbles Touchdown play_type labels----
        self.plays_json["type.text"] = np.where(
            (
                self.plays_json["text"].str.contains(
                    "sacked", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json["text"].str.contains(
                    "fumbled", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json["text"].str.contains(
                    "TD", case=False, flags=0, na=False, regex=True
                )
            ),
            "Fumble Recovery (Opponent) Touchdown",
            self.plays_json["type.text"],
        )
        # -- Fix generic pass plays ----
        ##-- first one looks for complete pass
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Pass")
            & (
                self.plays_json.text.str.contains(
                    "pass complete", case=False, flags=0, na=False, regex=True
                )
            ),
            "Pass Completion",
            self.plays_json["type.text"],
        )
        ##-- second one looks for incomplete pass
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Pass")
            & (
                self.plays_json.text.str.contains(
                    "pass incomplete", case=False, flags=0, na=False, regex=True
                )
            ),
            "Pass Incompletion",
            self.plays_json["type.text"],
        )
        ##-- third one looks for interceptions
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Pass")
            & (
                self.plays_json.text.str.contains(
                    "pass intercepted", case=False, flags=0, na=False, regex=True
                )
            ),
            "Pass Interception",
            self.plays_json["type.text"],
        )
        ##-- fourth one looks for sacked
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Pass")
            & (
                self.plays_json.text.str.contains(
                    "sacked", case=False, flags=0, na=False, regex=True
                )
            ),
            "Sack",
            self.plays_json["type.text"],
        )
        ##-- fifth one play type is Passing Touchdown, but its intercepted
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Passing Touchdown")
            & (
                self.plays_json.text.str.contains(
                    "pass intercepted for a TD",
                    case=False,
                    flags=0,
                    na=False,
                    regex=True,
                )
            ),
            "Interception Return Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Passing Touchdown")
            & (
                self.plays_json.text.str.contains(
                    "pass intercepted for a TD",
                    case=False,
                    flags=0,
                    na=False,
                    regex=True,
                )
            ),
            "Interception Return Touchdown",
            self.plays_json["type.text"],
        )
        # --- Moving non-Touchdown pass interceptions to one play_type: "Interception Return" -----
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"].isin(["Interception"]),
            "Interception Return",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"].isin(["Pass Interception"]),
            "Interception Return",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"].isin(["Pass Interception Return"]),
            "Interception Return",
            self.plays_json["type.text"],
        )

        # --- Moving Kickoff/Punt Touchdowns without fumbles to Kickoff/Punt Return Touchdown
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Kickoff Touchdown")
            & (self.plays_json.fumble_vec == False),
            "Kickoff Return Touchdown",
            self.plays_json["type.text"],
        )

        self.plays_json["type.text"] = np.select(
            [
                (self.plays_json["type.text"] == "Kickoff Touchdown")
                & (self.plays_json.fumble_vec == False),
                (self.plays_json["type.text"] == "Kickoff")
                & (self.plays_json["td_play"] == True)
                & (self.plays_json.fumble_vec == False),
                (self.plays_json["type.text"] == "Kickoff")
                & (
                    self.plays_json.text.str.contains(
                        "for a TD", case=False, flags=0, na=False, regex=True
                    )
                )
                & (self.plays_json.fumble_vec == False),
            ],
            [
                "Kickoff Return Touchdown",
                "Kickoff Return Touchdown",
                "Kickoff Return Touchdown",
            ],
            default=self.plays_json["type.text"],
        )

        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"].isin(["Kickoff", "Kickoff Return (Offense)"]))
            & (self.plays_json.fumble_vec == True)
            & (self.plays_json.change_of_poss == 1),
            "Kickoff Team Fumble Recovery",
            self.plays_json["type.text"],
        )

        self.plays_json["type.text"] = np.select(
            [
                (self.plays_json["type.text"] == "Punt Touchdown")
                & (self.plays_json.fumble_vec == False)
                & (self.plays_json.change_of_poss == 1),
                (self.plays_json["type.text"] == "Punt")
                & (
                    self.plays_json.text.str.contains(
                        "for a TD", case=False, flags=0, na=False, regex=True
                    )
                )
                & (self.plays_json.change_of_poss == 1),
            ],
            ["Punt Return Touchdown", "Punt Return Touchdown"],
            default=self.plays_json["type.text"],
        )

        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Punt")
            & (self.plays_json.fumble_vec == True)
            & (self.plays_json.change_of_poss == 0),
            "Punt Team Fumble Recovery",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"].isin(["Punt Touchdown"]))
            | (
                (self.plays_json["scoringPlay"] == True)
                & (self.plays_json["punt_play"] == True)
                & (self.plays_json.change_of_poss == 0)
            ),
            "Punt Team Fumble Recovery Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            self.plays_json["type.text"].isin(["Kickoff Touchdown"]),
            "Kickoff Team Fumble Recovery Touchdown",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"].isin(["Fumble Return Touchdown"]))
            & ((self.plays_json["pass"] == True) | (self.plays_json["rush"] == True)),
            "Fumble Recovery (Opponent) Touchdown",
            self.plays_json["type.text"],
        )

        # --- Safeties (kickoff, punt, penalty) ----
        self.plays_json["type.text"] = np.where(
            (
                self.plays_json["type.text"].isin(
                    ["Pass Reception", "Rush", "Rushing Touchdown"]
                )
                & ((self.plays_json["pass"] == True) | (self.plays_json["rush"] == True))
                & (self.plays_json["safety"] == True)
            ),
            "Safety",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json.kickoff_safety == True), "Kickoff (Safety)", self.plays_json["type.text"]
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json.punt_safety == True), "Punt (Safety)", self.plays_json["type.text"]
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json.penalty_safety == True), "Penalty (Safety)", self.plays_json["type.text"]
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Extra Point Good")
            & (
                self.plays_json["text"].str.contains(
                    "Two-Point", case=False, flags=0, na=False, regex=True
                )
            ),
            "Two-Point Conversion Good",
            self.plays_json["type.text"],
        )
        self.plays_json["type.text"] = np.where(
            (self.plays_json["type.text"] == "Extra Point Missed")
            & (
                self.plays_json["text"].str.contains(
                    "Two-Point", case=False, flags=0, na=False, regex=True
                )
            ),
            "Two-Point Conversion Missed",
            self.plays_json["type.text"],
        )
        return self.plays_json

    def __add_play_category_flags(self):
        # --------------------------------------------------
        # --- Sacks ----
        self.plays_json["sack"] = np.select(
            [
                self.plays_json["type.text"].isin(["Sack"]),
                (
                    self.plays_json["type.text"].isin(
                        [
                            "Fumble Recovery (Own)",
                            "Fumble Recovery (Own) Touchdown",
                            "Fumble Recovery (Opponent)",
                            "Fumble Recovery (Opponent) Touchdown",
                        ]
                    )
                )
                & (self.plays_json["pass"] == True)
                & (
                    self.plays_json["text"].str.contains(
                        "sacked", case=False, flags=0, na=False, regex=True
                    )
                ),
                (
                    (self.plays_json["type.text"].isin(["Safety"]))
                    & (
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                ),
            ],
            [True, True, True],
            default=False,
        )
        # --- Interceptions ------
        self.plays_json["int"] = self.plays_json["type.text"].isin(
            ["Interception Return", "Interception Return Touchdown"]
        )
        self.plays_json["int_td"] = self.plays_json["type.text"].isin(["Interception Return Touchdown"])

        # --- Pass Completions, Attempts and Targets -------
        self.plays_json["completion"] = np.select(
            [
                self.plays_json["type.text"].isin(
                    ["Pass Reception", "Pass Completion", "Passing Touchdown"]
                ),
                (
                    self.plays_json["type.text"].isin(
                        [
                            "Fumble Recovery (Own)",
                            "Fumble Recovery (Own) Touchdown",
                            "Fumble Recovery (Opponent)",
                            "Fumble Recovery (Opponent) Touchdown",
                        ]
                    )
                    & (self.plays_json["pass"] == True)
                    & ~(
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                ),
            ],
            [True, True],
            default=False,
        )

        self.plays_json["pass_attempt"] = np.select(
            [
                (
                    self.plays_json["type.text"].isin(
                        [
                            "Pass Reception",
                            "Pass Completion",
                            "Passing Touchdown",
                            "Pass Incompletion",
                        ]
                    )
                ),
                (
                    self.plays_json["type.text"].isin(
                        [
                            "Fumble Recovery (Own)",
                            "Fumble Recovery (Own) Touchdown",
                            "Fumble Recovery (Opponent)",
                            "Fumble Recovery (Opponent) Touchdown",
                        ]
                    )
                    & (self.plays_json["pass"] == True)
                    & ~(
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                ),
                (
                    (self.plays_json["pass"] == True)
                    & ~(
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                ),
            ],
            [True, True, True],
            default=False,
        )

        self.plays_json["target"] = np.select(
            [
                (
                    self.plays_json["type.text"].isin(
                        [
                            "Pass Reception",
                            "Pass Completion",
                            "Passing Touchdown",
                            "Pass Incompletion",
                        ]
                    )
                ),
                (
                    self.plays_json["type.text"].isin(
                        [
                            "Fumble Recovery (Own)",
                            "Fumble Recovery (Own) Touchdown",
                            "Fumble Recovery (Opponent)",
                            "Fumble Recovery (Opponent) Touchdown",
                        ]
                    )
                    & (self.plays_json["pass"] == True)
                    & ~(
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                ),
                (
                    (self.plays_json["pass"] == True)
                    & ~(
                        self.plays_json["text"].str.contains(
                            "sacked", case=False, flags=0, na=False, regex=True
                        )
                    )
                ),
            ],
            [True, True, True],
            default=False,
        )

        self.plays_json["pass_breakup"] = self.plays_json["text"].str.contains(
            "broken up by", case=False, flags=0, na=False, regex=True
        )
        # --- Pass/Rush TDs ------
        self.plays_json["pass_td"] = (self.plays_json["type.text"] == "Passing Touchdown") | (
            (self.plays_json["pass"] == True) & (self.plays_json["td_play"] == True)
        )
        self.plays_json["rush_td"] = (self.plays_json["type.text"] == "Rushing Touchdown") | (
            (self.plays_json["rush"] == True) & (self.plays_json["td_play"] == True)
        )
        # --- Change of possession via turnover
        self.plays_json["turnover_vec"] = self.plays_json["type.text"].isin(turnover_vec)
        self.plays_json["offense_score_play"] = self.plays_json["type.text"].isin(offense_score_vec)
        self.plays_json["defense_score_play"] = self.plays_json["type.text"].isin(defense_score_vec)
        self.plays_json["downs_turnover"] = np.where(
            (self.plays_json["type.text"].isin(normalplay))
            & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
            & (self.plays_json["start.down"] == 4)
            & (self.plays_json["penalty_1st_conv"] == False),
            True,
            False,
        )
        # --- Touchdowns----
        self.plays_json["scoring_play"] = self.plays_json["type.text"].isin(scores_vec)
        self.plays_json["yds_punted"] = (
            self.plays_json["text"]
            .str.extract(r"(?<= punt for)[^,]+(\d+)", flags=re.IGNORECASE)
            .astype(float)
        )
        self.plays_json["yds_punt_gained"] = np.where(
            self.plays_json.punt == True, self.plays_json["statYardage"], None
        )
        self.plays_json["fg_attempt"] = np.where(
            (
                self.plays_json["type.text"].str.contains(
                    "Field Goal", case=False, flags=0, na=False, regex=True
                )
            )
            | (
                self.plays_json["text"].str.contains(
                    "Field Goal", case=False, flags=0, na=False, regex=True
                )
            ),
            True,
            False,
        )
        self.plays_json["fg_made"] = self.plays_json["type.text"] == "Field Goal Good"
        self.plays_json["yds_fg"] = (
            self.plays_json["text"]
            .str.extract(
                r"(\d+)\s?Yd Field|(\d+)\s?YD FG|(\d+)\s?Yard FG|(\d+)\s?Field|(\d+)\s?Yard Field",
                flags=re.IGNORECASE,
            )
            .bfill(axis=1)[0]
            .astype(float)
        )
        # --------------------------------------------------
        self.plays_json["start.yardsToEndzone"] = np.where(
            self.plays_json["fg_attempt"] == True,
            self.plays_json["yds_fg"] - 17,
            self.plays_json["start.yardsToEndzone"],
        )
        self.plays_json["start.yardsToEndzone"] = np.select(
            [
                (self.plays_json["start.yardsToEndzone"].isna())
                & (~self.plays_json["type.text"].isin(kickoff_vec))
                & (self.plays_json["start.pos_team.id"] == self.plays_json["homeTeamId"]),
                (self.plays_json["start.yardsToEndzone"].isna())
                & (~self.plays_json["type.text"].isin(kickoff_vec))
                & (self.plays_json["start.pos_team.id"] == self.plays_json["awayTeamId"]),
            ],
            [
                100 - self.plays_json["start.yardLine"].astype(float),
                self.plays_json["start.yardLine"].astype(float),
            ],
            default=self.plays_json["start.yardsToEndzone"],
        )
        self.plays_json["pos_unit"] = np.select(
            [
                self.plays_json.punt == True,
                self.plays_json.kickoff_play == True,
                self.plays_json.fg_attempt == True,
                self.plays_json["type.text"] == "Defensive 2pt Conversion",
            ],
            ["Punt Offense", "Kickoff Return", "Field Goal Offense", "Offense"],
            default="Offense",
        )
        self.plays_json["def_pos_unit"] = np.select(
            [
                self.plays_json.punt == True,
                self.plays_json.kickoff_play == True,
                self.plays_json.fg_attempt == True,
                self.plays_json["type.text"] == "Defensive 2pt Conversion",
            ],
            ["Punt Return", "Kickoff Defense", "Field Goal Defense", "Defense"],
            default="Defense",
        )
        # --- Lags/Leads play type ----
        self.plays_json["lead_play_type"] = self.plays_json["type.text"].shift(-1)

        self.plays_json["sp"] = np.where(
            (self.plays_json.fg_attempt == True)
            | (self.plays_json.punt == True)
            | (self.plays_json.kickoff_play == True),
            True,
            False,
        )
        self.plays_json["play"] = np.where(
            (
                ~self.plays_json["type.text"].isin(
                    ["Timeout", "End Period", "End of Half", "Penalty"]
                )
            ),
            True,
            False,
        )
        self.plays_json["scrimmage_play"] = np.where(
            (self.plays_json.sp == False)
            & (
                ~self.plays_json["type.text"].isin(
                    [
                        "Timeout",
                        "Extra Point Good",
                        "Extra Point Missed",
                        "Two-Point Pass",
                        "Two-Point Rush",
                        "Penalty",
                    ]
                )
            ),
            True,
            False,
        )
        # --------------------------------------------------
        # --- Change of pos_team by lead('pos_team', 1)----
        self.plays_json["change_of_pos_team"] = np.where(
            (self.plays_json.pos_team == self.plays_json.lead_pos_team)
            & (
                ~(self.plays_json.lead_play_type.isin(["End Period", "End of Half"]))
                | self.plays_json.lead_play_type.isna()
                == True
            ),
            False,
            np.where(
                (self.plays_json.pos_team == self.plays_json.lead_pos_team2)
                & (
                    (self.plays_json.lead_play_type.isin(["End Period", "End of Half"]))
                    | self.plays_json.lead_play_type.isna()
                    == True
                ),
                False,
                True,
            ),
        )
        self.plays_json["change_of_pos_team"] = np.where(
            self.plays_json["change_of_poss"].isna(), False, self.plays_json["change_of_pos_team"]
        )
        self.plays_json["pos_score_diff_end"] = np.where(
            (
                (self.plays_json["type.text"].isin(end_change_vec))
                & (self.plays_json["start.pos_team.id"] != self.plays_json["end.pos_team.id"])
            )
            | (self.plays_json.downs_turnover == True)
            | ((self.plays_json.kickoff_onside == True) & (self.plays_json["start.pos_team.id"] != self.plays_json["end.pos_team.id"])),
            -1 * self.plays_json.pos_score_diff,
            self.plays_json.pos_score_diff,
        )
        self.plays_json["pos_score_diff_end"] = np.select(
            [
                (abs(self.plays_json.pos_score_pts) >= 8)
                & (self.plays_json.scoring_play == False)
                & (self.plays_json.change_of_pos_team == False),
                (abs(self.plays_json.pos_score_pts) >= 8)
                & (self.plays_json.scoring_play == False)
                & (self.plays_json.change_of_pos_team == True),
            ],
            [self.plays_json["pos_score_diff_start"], -1 * self.plays_json["pos_score_diff_start"]],
            default=self.plays_json["pos_score_diff_end"],
        )

        self.plays_json['fumble_lost'] = np.select(
            [
                (self.plays_json.fumble_vec == True) & (self.plays_json.change_of_poss == True),
                (self.plays_json.fumble_vec == True) & (self.plays_json.change_of_pos_team == True)
            ],
            [
                True,
                True
            ],
            default = False
        )

        self.plays_json['fumble_recovered'] = np.select(
            [
                (self.plays_json.fumble_vec == True) & (self.plays_json.change_of_poss == False),
                (self.plays_json.fumble_vec == True) & (self.plays_json.change_of_pos_team == False)
            ],
            [
                True,
                True
            ],
            default = False
        )

        return self.plays_json

    def __add_yardage_cols(self):
        # self.plays_json["yds_rushed"] = None
        self.plays_json["yds_rushed"] = np.select(
            [
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "run for no gain", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "for no gain", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "run for a loss of", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "rush for a loss of", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "run for", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "rush for", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "Yd Run", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "Yd Rush", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "Yard Rush", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "rushed", case=False, flags=0, na=False, regex=True
                    )
                )
                & (
                    ~self.plays_json.text.str.contains(
                        "touchdown", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.rush == True)
                & (
                    self.plays_json.text.str.contains(
                        "rushed", case=False, flags=0, na=False, regex=True
                    )
                )
                & (
                    self.plays_json.text.str.contains(
                        "touchdown", case=False, flags=0, na=False, regex=True
                    )
                ),
            ],
            [
                0.0,
                0.0,
                -1
                * self.plays_json.text.str.extract(
                    r"((?<=run for a loss of)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                -1
                * self.plays_json.text.str.extract(
                    r"((?<=rush for a loss of)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(r"((?<=run for)[^,]+)", flags=re.IGNORECASE)[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(r"((?<=rush for)[^,]+)", flags=re.IGNORECASE)[
                    0
                ]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(r"(\d+) Yd Run", flags=re.IGNORECASE)[
                    0
                ].astype(float),
                self.plays_json.text.str.extract(r"(\d+) Yd Rush", flags=re.IGNORECASE)[
                    0
                ].astype(float),
                self.plays_json.text.str.extract(r"(\d+) Yard Rush", flags=re.IGNORECASE)[
                    0
                ].astype(float),
                self.plays_json.text.str.extract(r"for (\d+) yards", flags=re.IGNORECASE)[
                    0
                ].astype(float),
                self.plays_json.text.str.extract(r"for a (\d+) yard", flags=re.IGNORECASE)[
                    0
                ].astype(float),
            ],
            default=None,
        )

        # self.plays_json["yds_receiving"] = None
        self.plays_json["yds_receiving"] = np.select(
            [
                (self.plays_json["pass"] == True)
                & (self.plays_json.text.str.contains("complete to", case=False))
                & (self.plays_json.text.str.contains(r"for no gain", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json.text.str.contains("complete to", case=False))
                & (self.plays_json.text.str.contains("for a loss", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json.text.str.contains("complete to", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json.text.str.contains("complete to", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json.text.str.contains("incomplete", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json["type.text"].str.contains("incompletion", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json.text.str.contains("Yd pass", case=False)),
            ],
            [
                0.0,
                -1
                * self.plays_json.text.str.extract(
                    r"((?<=for a loss of)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(r"((?<=for)[^,]+)", flags=re.IGNORECASE)[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(r"((?<=for)[^,]+)", flags=re.IGNORECASE)[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                0.0,
                0.0,
                self.plays_json.text.str.extract(r"(\d+)\s+Yd\s+pass", flags=re.IGNORECASE)[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
            ],
            default=None,
        )

        # self.plays_json["yds_int_return"] = None
        self.plays_json["yds_int_return"] = np.select(
            [
                (self.plays_json["pass"] == True)
                & (self.plays_json["int_td"] == True)
                & (self.plays_json.text.str.contains("Yd Interception Return", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json["int"] == True)
                & (self.plays_json.text.str.contains(r"for no gain", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json["int"] == True)
                & (self.plays_json.text.str.contains(r"for a loss of", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json["int"] == True)
                & (self.plays_json.text.str.contains(r"for a TD", case=False)),
                (self.plays_json["pass"] == True)
                & (self.plays_json["int"] == True)
                & (self.plays_json.text.str.contains(r"return for", case=False)),
                (self.plays_json["pass"] == True) & (self.plays_json["int"] == True),
            ],
            [
                self.plays_json.text.str.extract(
                    r"(.+) Interception Return", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                0.0,
                -1
                * self.plays_json.text.str.extract(
                    r"((?<= for a loss of)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(
                    r"((?<= return for)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(
                    r"((?<= return for)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.replace("for a 1st", "")
                .str.extract(r"((?<=for)[^,]+)", flags=re.IGNORECASE)[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
            ],
            default=None,
        )

        #     self.plays_json['yds_fumble_return'] = None
        #     self.plays_json['yds_penalty'] = None

        # self.plays_json["yds_kickoff"] = None
        self.plays_json["yds_kickoff"] = np.where(
            (self.plays_json["kickoff_play"] == True),
            self.plays_json.text.str.extract(r"((?<= kickoff for)[^,]+)", flags=re.IGNORECASE)[
                0
            ]
            .str.extract(r"(\d+)")[0]
            .astype(float),
            None,
        )

        # self.plays_json["yds_kickoff_return"] = None
        self.plays_json["yds_kickoff_return"] = np.select(
            [
                (self.plays_json.kickoff_play == True)
                & (self.plays_json.kickoff_tb == True)
                & (self.plays_json.season > 2013),
                (self.plays_json.kickoff_play == True)
                & (self.plays_json.kickoff_tb == True)
                & (self.plays_json.season <= 2013),
                (self.plays_json.kickoff_play == True)
                & (self.plays_json.fumble_vec == False)
                & (
                    self.plays_json.text.str.contains(
                        r"for no gain|fair catch|fair caught", regex=True, case=False
                    )
                ),
                (self.plays_json.kickoff_play == True)
                & (self.plays_json.fumble_vec == False)
                & (
                    self.plays_json.text.str.contains(
                        r"out-of-bounds|out of bounds", regex=True, case=False
                    )
                ),
                (
                    (self.plays_json.kickoff_downed == True)
                    | (self.plays_json.kickoff_fair_catch == True)
                ),
                (self.plays_json.kickoff_play == True)
                & (self.plays_json.text.str.contains(r"returned by", regex=True, case=False)),
                (self.plays_json.kickoff_play == True)
                & (self.plays_json.text.str.contains(r"return for", regex=True, case=False)),
                (self.plays_json.kickoff_play == True),
            ],
            [
                25,
                20,
                0,
                40,
                0,
                self.plays_json.text.str.extract(r"((?<= for)[^,]+)", flags=re.IGNORECASE)[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(
                    r"((?<= return for)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(
                    r"((?<= returned for)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
            ],
            default=None,
        )

        # self.plays_json["yds_punted"] = None
        self.plays_json["yds_punted"] = np.select(
            [
                (self.plays_json.punt == True) & (self.plays_json.punt_blocked == True),
                (self.plays_json.punt == True),
            ],
            [
                0,
                self.plays_json.text.str.extract(r"((?<= punt for)[^,]+)", flags=re.IGNORECASE)[
                    0
                ]
                .str.extract(r"(\d+)")[0]
                .astype(float),
            ],
            default=None,
        )

        self.plays_json["yds_punt_return"] = np.select(
            [
                (self.plays_json.punt == True) & (self.plays_json.punt_tb == 1),
                (self.plays_json.punt == True)
                & (
                    self.plays_json["text"].str.contains(
                        r"fair catch|fair caught",
                        case=False,
                        flags=0,
                        na=False,
                        regex=True,
                    )
                ),
                (self.plays_json.punt == True)
                & (
                    (self.plays_json.punt_downed == True)
                    | (self.plays_json.punt_oob == True)
                    | (self.plays_json.punt_fair_catch == True)
                ),
                (self.plays_json.punt == True)
                & (
                    self.plays_json["text"].str.contains(
                        r"no return|no gain", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.punt == True)
                & (
                    self.plays_json["text"].str.contains(
                        r"returned \d+ yards", case=False, flags=0, na=False, regex=True
                    )
                ),
                (self.plays_json.punt == True) & (self.plays_json.punt_blocked == False),
                (self.plays_json.punt == True) & (self.plays_json.punt_blocked == True),
            ],
            [
                20,
                0,
                0,
                0,
                self.plays_json.text.str.extract(r"((?<= returned)[^,]+)", flags=re.IGNORECASE)[
                    0
                ]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(
                    r"((?<= returns for)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
                self.plays_json.text.str.extract(
                    r"((?<= return for)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float),
            ],
            default=None,
        )

        self.plays_json["yds_fumble_return"] = np.select(
            [(self.plays_json.fumble_vec == True) & (self.plays_json.kickoff_play == False)],
            [
                self.plays_json.text.str.extract(
                    r"((?<= return for)[^,]+)", flags=re.IGNORECASE
                )[0]
                .str.extract(r"(\d+)")[0]
                .astype(float)
            ],
            default=None,
        )

        self.plays_json["yds_sacked"] = np.select(
            [(self.plays_json.sack == True)],
            [
                -1
                * self.plays_json.text.str.extract(r"((?<= sacked)[^,]+)", flags=re.IGNORECASE)[
                    0
                ]
                .str.extract(r"(\d+)")[0]
                .astype(float)
            ],
            default=None,
        )

        self.plays_json["yds_penalty"] = np.select(
            [(self.plays_json.penalty_detail == 1)],
            [
                -1
                * self.plays_json.text.str.extract(r"((?<= sacked)[^,]+)", flags=re.IGNORECASE)[
                    0
                ]
                .str.extract(r"(\d+)")[0]
                .astype(float)
            ],
            default=None,
        )

        self.plays_json["yds_penalty"] = np.select(
            [
                self.plays_json.penalty_detail.isin(["Penalty Declined", "Penalty Offset"]),
                self.plays_json.yds_penalty.notna(),
                (self.plays_json.penalty_detail.notna())
                & (self.plays_json.yds_penalty.isna())
                & (self.plays_json.rush == True),
                (self.plays_json.penalty_detail.notna())
                & (self.plays_json.yds_penalty.isna())
                & (self.plays_json.int == True),
                (self.plays_json.penalty_detail.notna())
                & (self.plays_json.yds_penalty.isna())
                & (self.plays_json["pass"] == 1)
                & (self.plays_json["sack"] == False)
                & (self.plays_json["type.text"] != "Pass Incompletion"),
                (self.plays_json.penalty_detail.notna())
                & (self.plays_json.yds_penalty.isna())
                & (self.plays_json["pass"] == 1)
                & (self.plays_json["sack"] == False)
                & (self.plays_json["type.text"] == "Pass Incompletion"),
                (self.plays_json.penalty_detail.notna())
                & (self.plays_json.yds_penalty.isna())
                & (self.plays_json["pass"] == 1)
                & (self.plays_json["sack"] == True),
                (self.plays_json["type.text"] == "Penalty"),
            ],
            [
                0,
                self.plays_json.yds_penalty.astype(float),
                self.plays_json.statYardage.astype(float) - self.plays_json.yds_rushed.astype(float),
                self.plays_json.statYardage.astype(float)
                - self.plays_json.yds_int_return.astype(float),
                self.plays_json.statYardage.astype(float) - self.plays_json.yds_receiving.astype(float),
                self.plays_json.statYardage.astype(float),
                self.plays_json.statYardage.astype(float) - self.plays_json.yds_sacked.astype(float),
                self.plays_json.statYardage.astype(float),
            ],
            default=None,
        )
        return self.plays_json

    def __add_player_cols(self):
        # https://stackoverflow.com/a/76344743
        init_cols = [
            "rush_player",
            "receiver_player",
            "pass_player",
            "sack_players",
            "sack_player1",
            "sack_player2",
            "interception_player",
            "pass_breakup_player",
            "fg_kicker_player",
            "fg_return_player",
            "fg_block_player",
            "punter_player",
            "punt_return_player",
            "punt_block_player",
            "punt_block_return_player",
            "kickoff_player",
            "kickoff_return_player",
            "fumble_player",
            "fumble_forced_player",
            "fumble_recovered_player",
            "rush_player_name",
            "receiver_player_name",
            "passer_player_name",
            "rusher_player_name",
            "sack_player_name",
            "sack_player_name2",
            "interception_player_name",
            "pass_breakup_player_name",
            "fg_kicker_player_name",
            "fg_return_player_name",
            "fg_block_player_name",
            "punter_player_name",
            "punt_return_player_name",
            "punt_block_player_name",
            "punt_block_return_player_name",
            "kickoff_player_name",
            "kickoff_return_player_name",
            "fumble_player_name",
            "fumble_forced_player_name",
            "fumble_recovered_player_name"
        ]

        base_player_name_matrix = [[None for x in range(len(init_cols))] for y in range(len(self.plays_json))] 
        init_player_name_df = pd.DataFrame(base_player_name_matrix, columns=init_cols, index=self.plays_json.index)

        self.plays_json = pd.concat([
            self.plays_json, 
            init_player_name_df
        ], axis=1)

        ## Extract player names
        # RB names
        self.plays_json["rush_player"] = np.where(
            (self.plays_json.rush == 1),
            self.plays_json.text.str.extract(
                r"(.{0,25} )run |(.{0,25} )\d{0,2} Yd Run|(.{0,25} )rush |(.{0,25} )rushed "
            ).bfill(axis=1)[0],
            None,
        )
        self.plays_json["rush_player"] = self.plays_json.rush_player.str.replace(
            r" run | \d+ Yd Run| rush ", "", regex=True
        )
        self.plays_json["rush_player"] = self.plays_json.rush_player.str.replace(
            r" \((.+)\)", "", regex=True
        )

        # QB names
        self.plays_json["pass_player"] = np.where(
            (self.plays_json["pass"] == 1) & (self.plays_json["type.text"] != "Passing Touchdown"),
            self.plays_json.text.str.extract(
                r"pass from (.*?) \(|(.{0,30} )pass |(.+) sacked by|(.+) sacked for|(.{0,30} )incomplete "
            ).bfill(axis=1)[0],
            self.plays_json["pass_player"],
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            r"pass | sacked by| sacked for| incomplete", "", regex=True
        )

        self.plays_json["pass_player"] = np.where(
            (self.plays_json["pass"] == 1) & (self.plays_json["type.text"] == "Passing Touchdown"),
            self.plays_json.text.str.extract("pass from(.+)")[0],
            self.plays_json["pass_player"],
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            "pass from", "", regex=True
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            r"\(.+\)", "", regex=True
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(r" \,", "", regex=True)

        self.plays_json["pass_player"] = np.where(
            (self.plays_json["type.text"] == "Passing Touchdown") & self.plays_json.pass_player.isna(),
            self.plays_json.text.str.extract("(.+)pass(.+)? complete to")[0],
            self.plays_json["pass_player"],
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            r" pass complete to(.+)", "", regex=True
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            " pass complete to", "", regex=True
        )

        self.plays_json["pass_player"] = np.where(
            (self.plays_json["type.text"] == "Passing Touchdown") & self.plays_json.pass_player.isna(),
            self.plays_json.text.str.extract("(.+)pass,to")[0],
            self.plays_json["pass_player"],
        )

        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            r" pass,to(.+)", "", regex=True
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            r" pass,to", "", regex=True
        )
        self.plays_json["pass_player"] = self.plays_json.pass_player.str.replace(
            r" \((.+)\)", "", regex=True
        )
        self.plays_json["pass_player"] = np.where(
            (self.plays_json["pass"] == 1)
            & (
                (self.plays_json.pass_player.str.strip().str.len == 0)
                | self.plays_json.pass_player.isna()
            ),
            "TEAM",
            self.plays_json.pass_player,
        )

        self.plays_json["receiver_player"] = np.where(
            (self.plays_json["pass"] == 1)
            & ~self.plays_json.text.str.contains(
                "sacked", case=False, flags=0, na=False, regex=True
            ),
            self.plays_json.text.str.extract("to (.+)")[0],
            None,
        )

        self.plays_json["receiver_player"] = np.where(
            self.plays_json.text.str.contains(
                "Yd pass", case=False, flags=0, na=False, regex=True
            ),
            self.plays_json.text.str.extract("(.{0,25} )\\d{0,2} Yd pass", flags=re.IGNORECASE)[
                0
            ],
            self.plays_json["receiver_player"],
        )

        self.plays_json["receiver_player"] = np.where(
            self.plays_json.text.str.contains("Yd TD pass", case=False),
            self.plays_json.text.str.extract(
                "(.{0,25} )\\d{0,2} Yd TD pass", flags=re.IGNORECASE
            )[0],
            self.plays_json["receiver_player"],
        )

        self.plays_json["receiver_player"] = np.where(
            (self.plays_json["type.text"] == "Sack")
            | (self.plays_json["type.text"] == "Interception Return")
            | (self.plays_json["type.text"] == "Interception Return Touchdown")
            | (
                self.plays_json["type.text"].isin(
                    [
                        "Fumble Recovery (Opponent) Touchdown",
                        "Fumble Recovery (Opponent)",
                    ]
                )
                & self.plays_json.text.str.contains("sacked", case=False)
            ),
            None,
            self.plays_json["receiver_player"],
        )

        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "to ", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "\\,.+", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "for (.+)", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            r" (\d{1,2})", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " Yd pass", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " Yd TD pass", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "pass complete to", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "penalty", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            ' "', "", case=False, regex=True
        )
        self.plays_json.receiver_player = np.where(
            ~(self.plays_json.receiver_player.str.contains("III", na=False)),
            self.plays_json.receiver_player.str.replace("[A-Z]{3,}", "", case=True, regex=True),
            self.plays_json.receiver_player,
        )

        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " &", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "A&M", "", case=True, regex=False
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " ST", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " GA", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " UL", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " FL", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " OH", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " NC", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            ' "', "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " \\u00c9", "", case=True, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            " fumbled,", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "the (.+)", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "pass incomplete to", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "(.+)pass incomplete to", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "(.+)pass incomplete", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            "pass incomplete", "", case=False, regex=True
        )
        self.plays_json.receiver_player = self.plays_json.receiver_player.str.replace(
            r" \((.+)\)", "", regex=True
        )

        self.plays_json["sack_players"] = np.where(
            (self.plays_json["sack"] == True)
            | (self.plays_json["fumble_vec"] == True) & (self.plays_json["pass"] == True),
            self.plays_json.text.str.extract("sacked by(.+)", flags=re.IGNORECASE)[0],
            self.plays_json.sack_players,
        )

        self.plays_json["sack_players"] = self.plays_json["sack_players"].str.replace(
            "for (.+)", "", case=True, regex=True
        )
        self.plays_json["sack_players"] = self.plays_json["sack_players"].str.replace(
            "(.+) by ", "", case=True, regex=True
        )
        self.plays_json["sack_players"] = self.plays_json["sack_players"].str.replace(
            " at the (.+)", "", case=True, regex=True
        )
        self.plays_json["sack_player1"] = self.plays_json["sack_players"].str.replace(
            "and (.+)", "", case=True, regex=True
        )
        self.plays_json["sack_player2"] = np.where(
            self.plays_json["sack_players"].str.contains("and .+"),
            self.plays_json["sack_players"].str.replace("(.+) and", "", case=True, regex=True),
            None,
        )

        self.plays_json["interception_player"] = np.where(
            (self.plays_json["type.text"] == "Interception Return")
            | (self.plays_json["type.text"] == "Interception Return Touchdown")
            & self.plays_json["pass"]
            == True,
            self.plays_json.text.str.extract("intercepted (.+)", flags=re.IGNORECASE)[0],
            self.plays_json.interception_player,
        )

        self.plays_json["interception_player"] = np.where(
            self.plays_json.text.str.contains("Yd Interception Return", case=True, regex=True),
            self.plays_json.text.str.extract(
                "(.{0,25} )\\d{0,2} Yd Interception Return|(.{0,25} )\\d{0,2} yd interception return",
                flags=re.IGNORECASE,
            ).bfill(axis=1)[0],
            self.plays_json.interception_player,
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "return (.+)", "", case=True, regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "(.+) intercepted", "", case=True, regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "intercepted", "", case=True, regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "Yd Interception Return", "", regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "for a 1st down", "", case=True, regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "(\\d{1,2})", "", case=True, regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "for a TD", "", case=True, regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            "at the (.+)", "", case=True, regex=True
        )
        self.plays_json["interception_player"] = self.plays_json["interception_player"].str.replace(
            " by ", "", case=True, regex=True
        )

        self.plays_json["pass_breakup_player"] = np.where(
            self.plays_json["pass"] == True,
            self.plays_json.text.str.extract("broken up by (.+)").bfill(axis=1)[0],
            self.plays_json.pass_breakup_player,
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "(.+) broken up by", "", case=True, regex=True
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "broken up by", "", case=True, regex=True
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "Penalty(.+)", "", case=True, regex=True
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "SOUTH FLORIDA", "", case=True, regex=True
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "WEST VIRGINIA", "", case=True, regex=True
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "MISSISSIPPI ST", "", case=True, regex=True
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "CAMPBELL", "", case=True, regex=True
        )
        self.plays_json["pass_breakup_player"] = self.plays_json["pass_breakup_player"].str.replace(
            "COASTL CAROLINA", "", case=True, regex=True
        )

        self.plays_json["punter_player"] = np.where(
            self.plays_json["type.text"].str.contains("Punt", regex=True),
            self.plays_json.text.str.extract(
                r"(.{0,30}) punt|Punt by (.{0,30})", flags=re.IGNORECASE
            ).bfill(axis=1)[0],
            self.plays_json.punter_player,
        )
        self.plays_json["punter_player"] = self.plays_json["punter_player"].str.replace(
            " punt", "", case=False, regex=True
        )
        self.plays_json["punter_player"] = self.plays_json["punter_player"].str.replace(
            r" for(.+)", "", case=False, regex=True
        )
        self.plays_json["punter_player"] = self.plays_json["punter_player"].str.replace(
            "Punt by ", "", case=False, regex=True
        )
        self.plays_json["punter_player"] = self.plays_json["punter_player"].str.replace(
            r"\((.+)\)", "", case=False, regex=True
        )
        self.plays_json["punter_player"] = self.plays_json["punter_player"].str.replace(
            r" returned \d+", "", case=False, regex=True
        )
        self.plays_json["punter_player"] = self.plays_json["punter_player"].str.replace(
            " returned", "", case=False, regex=True
        )
        self.plays_json["punter_player"] = self.plays_json["punter_player"].str.replace(
            " no return", "", case=False, regex=True
        )

        self.plays_json["punt_return_player"] = np.where(
            self.plays_json["type.text"].str.contains("Punt", regex=True),
            self.plays_json.text.str.extract(
                r", (.{0,25}) returns|fair catch by (.{0,25})|, returned by (.{0,25})|yards by (.{0,30})| return by (.{0,25})",
                flags=re.IGNORECASE,
            ).bfill(axis=1)[0],
            self.plays_json.punt_return_player,
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            ", ", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            " returns", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            " returned", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            " return", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            "fair catch by", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            r" at (.+)", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            r" for (.+)", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            r"(.+) by ", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            r" to (.+)", "", case=False, regex=True
        )
        self.plays_json["punt_return_player"] = self.plays_json["punt_return_player"].str.replace(
            r"\((.+)\)", "", case=False, regex=True
        )

        self.plays_json["punt_block_player"] = np.where(
            self.plays_json["type.text"].str.contains("Punt", case=True, regex=True),
            self.plays_json.text.str.extract(
                "punt blocked by (.{0,25})| blocked by(.+)", flags=re.IGNORECASE
            ).bfill(axis=1)[0],
            self.plays_json.punt_block_player,
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            r"punt blocked by |for a(.+)", "", case=True, regex=True
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            r"blocked by(.+)", "", case=True, regex=True
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            r"blocked(.+)", "", case=True, regex=True
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            r" for(.+)", "", case=True, regex=True
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            r",(.+)", "", case=True, regex=True
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            r"punt blocked by |for a(.+)", "", case=True, regex=True
        )

        self.plays_json["punt_block_player"] = np.where(
            self.plays_json["type.text"].str.contains("yd return of blocked punt"),
            self.plays_json.text.str.extract("(.+) yd return of blocked").bfill(axis=1)[0],
            self.plays_json.punt_block_player,
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            "blocked|Blocked", "", regex=True
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            r"\\d+", "", regex=True
        )
        self.plays_json["punt_block_player"] = self.plays_json["punt_block_player"].str.replace(
            "yd return of", "", regex=True
        )

        self.plays_json["punt_block_return_player"] = np.where(
            (
                self.plays_json["type.text"].str.contains(
                    "Punt", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json.text.str.contains(
                    "blocked", case=False, flags=0, na=False, regex=True
                )
                & self.plays_json.text.str.contains(
                    "return", case=False, flags=0, na=False, regex=True
                )
            ),
            self.plays_json.text.str.extract("(.+) return").bfill(axis=1)[0],
            self.plays_json.punt_block_return_player,
        )
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace("(.+)blocked by {punt_block_player}", "", regex = True)
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace("blocked by {punt_block_player}", "", regex = True)
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace("return(.+)", "", regex=True)
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace("return", "", regex=True)
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace("(.+)blocked by", "", regex=True)
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace("for a TD(.+)|for a SAFETY(.+)", "", regex=True)
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace("blocked by", "", regex=True)
        self.plays_json["punt_block_return_player"] = self.plays_json[
            "punt_block_return_player"
        ].str.replace(", ", "", regex=True)

        self.plays_json["kickoff_player"] = np.where(
            self.plays_json["type.text"].str.contains("Kickoff"),
            self.plays_json.text.str.extract("(.{0,25}) kickoff|(.{0,25}) on-side").bfill(
                axis=1
            )[0],
            self.plays_json.kickoff_player,
        )
        self.plays_json["kickoff_player"] = self.plays_json["kickoff_player"].str.replace(
            " on-side| kickoff", "", regex=True
        )

        self.plays_json["kickoff_return_player"] = np.where(
            self.plays_json["type.text"].str.contains("ickoff"),
            self.plays_json.text.str.extract(
                ", (.{0,25}) return|, (.{0,25}) fumble|returned by (.{0,25})|touchback by (.{0,25})",
                flags=re.IGNORECASE,
            ).bfill(axis=1)[0],
            self.plays_json.kickoff_return_player,
        )
        self.plays_json["kickoff_return_player"] = self.plays_json["kickoff_return_player"].str.replace(
            ", ", "", case=False, regex=True
        )
        self.plays_json["kickoff_return_player"] = self.plays_json["kickoff_return_player"].str.replace(
            " return| fumble| returned by| for |touchback by ",
            "",
            case=False,
            regex=True,
        )
        self.plays_json["kickoff_return_player"] = self.plays_json["kickoff_return_player"].str.replace(
            r"\((.+)\)(.+)", "", case=False, regex=True
        )

        self.plays_json["fg_kicker_player"] = np.where(
            self.plays_json["type.text"].str.contains("Field Goal"),
            self.plays_json.text.str.extract(
                "(.{0,25} )\\d{0,2} yd field goal|(.{0,25} )\\d{0,2} yd fg|(.{0,25} )\\d{0,2} yard field goal",
                flags=re.IGNORECASE,
            ).bfill(axis=1)[0],
            self.plays_json.fg_kicker_player,
        )
        self.plays_json["fg_kicker_player"] = self.plays_json["fg_kicker_player"].str.replace(
            " Yd Field Goal|Yd FG |yd FG| yd FG", "", case=False, regex=True
        )
        self.plays_json["fg_kicker_player"] = self.plays_json["fg_kicker_player"].str.replace(
            "(\\d{1,2})", "", case=False, regex=True
        )

        self.plays_json["fg_block_player"] = np.where(
            self.plays_json["type.text"].str.contains("Field Goal"),
            self.plays_json.text.str.extract("blocked by (.{0,25})", flags=re.IGNORECASE).bfill(axis=1)[0],
            self.plays_json.fg_block_player,
        )
        # self.plays_json["fg_block_player"] = self.plays_json["fg_block_player"].str.replace(
        #     ",(.+)", "", case=False, regex=True
        # )
        # self.plays_json["fg_block_player"] = self.plays_json["fg_block_player"].str.replace(
        #     "blocked by ", "", case=False, regex=True
        # )
        # self.plays_json["fg_block_player"] = self.plays_json["fg_block_player"].str.replace(
        #     "  (.)+", "", case=False, regex=True
        # )

        self.plays_json["fg_return_player"] = np.where(
            (self.plays_json["type.text"].str.contains("Field Goal"))
            & (self.plays_json["type.text"].str.contains("blocked by|missed"))
            & (self.plays_json["type.text"].str.contains("return")),
            self.plays_json.text.str.extract("  (.+)").bfill(axis=1)[0],
            self.plays_json.fg_return_player,
        )

        self.plays_json["fg_return_player"] = self.plays_json["fg_return_player"].str.replace(
            ",(.+)", "", case=False, regex=True
        )
        self.plays_json["fg_return_player"] = self.plays_json["fg_return_player"].str.replace(
            "return ", "", case=False, regex=True
        )
        self.plays_json["fg_return_player"] = self.plays_json["fg_return_player"].str.replace(
            "returned ", "", case=False, regex=True
        )
        self.plays_json["fg_return_player"] = self.plays_json["fg_return_player"].str.replace(
            " for (.+)", "", case=False, regex=True
        )
        self.plays_json["fg_return_player"] = self.plays_json["fg_return_player"].str.replace(
            " for (.+)", "", case=False, regex=True
        )

        self.plays_json["fg_return_player"] = np.where(
            self.plays_json["type.text"].isin(
                ["Missed Field Goal Return", "Missed Field Goal Return Touchdown"]
            ),
            self.plays_json.text.str.extract("(.+)return").bfill(axis=1)[0],
            self.plays_json.fg_return_player,
        )
        self.plays_json["fg_return_player"] = self.plays_json["fg_return_player"].str.replace(
            " return", "", case=False, regex=True
        )
        self.plays_json["fg_return_player"] = self.plays_json["fg_return_player"].str.replace(
            "(.+),", "", case=False, regex=True
        )

        self.plays_json["fumble_player"] = np.where(
            self.plays_json["text"].str.contains(
                "fumble", case=False, flags=0, na=False, regex=True
            ),
            self.plays_json["text"].str.extract("(.{0,25} )fumble").bfill(axis=1)[0],
            self.plays_json.fumble_player,
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            " fumble(.+)", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            "fumble", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            " yds", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            " yd", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            "yardline", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            " yards| yard|for a TD|or a safety", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            " for ", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            " a safety", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            "r no gain", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            "(.+)(\\d{1,2})", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            "(\\d{1,2})", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = self.plays_json["fumble_player"].str.replace(
            ", ", "", case=False, regex=True
        )
        self.plays_json["fumble_player"] = np.where(
            self.plays_json["type.text"] == "Penalty", None, self.plays_json.fumble_player
        )

        self.plays_json["fumble_forced_player"] = np.where(
            (
                self.plays_json.text.str.contains(
                    "fumble", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json.text.str.contains(
                    "forced by", case=False, flags=0, na=False, regex=True
                )
            ),
            self.plays_json.text.str.extract("forced by(.{0,25})").bfill(axis=1)[0],
            self.plays_json.fumble_forced_player,
        )

        self.plays_json["fumble_forced_player"] = self.plays_json["fumble_forced_player"].str.replace(
            "(.+)forced by", "", case=False, regex=True
        )
        self.plays_json["fumble_forced_player"] = self.plays_json["fumble_forced_player"].str.replace(
            "forced by", "", case=False, regex=True
        )
        self.plays_json["fumble_forced_player"] = self.plays_json["fumble_forced_player"].str.replace(
            ", recove(.+)", "", case=False, regex=True
        )
        self.plays_json["fumble_forced_player"] = self.plays_json["fumble_forced_player"].str.replace(
            ", re(.+)", "", case=False, regex=True
        )
        self.plays_json["fumble_forced_player"] = self.plays_json["fumble_forced_player"].str.replace(
            ", fo(.+)", "", case=False, regex=True
        )
        self.plays_json["fumble_forced_player"] = self.plays_json["fumble_forced_player"].str.replace(
            ", r", "", case=False, regex=True
        )
        self.plays_json["fumble_forced_player"] = self.plays_json["fumble_forced_player"].str.replace(
            ", ", "", case=False, regex=True
        )
        self.plays_json["fumble_forced_player"] = np.where(
            self.plays_json["type.text"] == "Penalty", None, self.plays_json.fumble_forced_player
        )

        self.plays_json["fumble_recovered_player"] = np.where(
            (
                self.plays_json.text.str.contains(
                    "fumble", case=False, flags=0, na=False, regex=True
                )
            )
            & (
                self.plays_json.text.str.contains(
                    "recovered by", case=False, flags=0, na=False, regex=True
                )
            ),
            self.plays_json.text.str.extract("recovered by(.{0,30})").bfill(axis=1)[0],
            self.plays_json.fumble_recovered_player,
        )

        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("for a 1ST down", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("for a 1st down", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("(.+)recovered", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("(.+) by", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(", recove(.+)", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(", re(.+)", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("a 1st down", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(" a 1st down", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(", for(.+)", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(" for a", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(" fo", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(" , r", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(", r", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("  (.+)", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace(" ,", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("penalty(.+)", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = self.plays_json[
            "fumble_recovered_player"
        ].str.replace("for a 1ST down", "", case=False, regex=True)
        self.plays_json["fumble_recovered_player"] = np.where(
            self.plays_json["type.text"] == "Penalty", None, self.plays_json.fumble_recovered_player
        )

        ## Extract player names
        self.plays_json["passer_player_name"] = self.plays_json["pass_player"].str.strip()
        self.plays_json["rusher_player_name"] = self.plays_json["rush_player"].str.strip()
        self.plays_json["receiver_player_name"] = self.plays_json["receiver_player"].str.strip()
        self.plays_json["sack_player_name"] = self.plays_json["sack_player1"].str.strip()
        self.plays_json["sack_player_name2"] = self.plays_json["sack_player2"].str.strip()
        self.plays_json["pass_breakup_player_name"] = self.plays_json["pass_breakup_player"].str.strip()
        self.plays_json["interception_player_name"] = self.plays_json["interception_player"].str.strip()
        self.plays_json["fg_kicker_player_name"] = self.plays_json["fg_kicker_player"].str.strip()
        self.plays_json["fg_block_player_name"] = self.plays_json["fg_block_player"].str.strip()
        self.plays_json["fg_return_player_name"] = self.plays_json["fg_return_player"].str.strip()
        self.plays_json["kickoff_player_name"] = self.plays_json["kickoff_player"].str.strip()
        self.plays_json["kickoff_return_player_name"] = self.plays_json[
            "kickoff_return_player"
        ].str.strip()
        self.plays_json["punter_player_name"] = self.plays_json["punter_player"].str.strip()
        self.plays_json["punt_block_player_name"] = self.plays_json["punt_block_player"].str.strip()
        self.plays_json["punt_return_player_name"] = self.plays_json["punt_return_player"].str.strip()
        self.plays_json["punt_block_return_player_name"] = self.plays_json[
            "punt_block_return_player"
        ].str.strip()
        self.plays_json["fumble_player_name"] = self.plays_json["fumble_player"].str.strip()
        self.plays_json["fumble_forced_player_name"] = self.plays_json[
            "fumble_forced_player"
        ].str.strip()
        self.plays_json["fumble_recovered_player_name"] = self.plays_json[
            "fumble_recovered_player"
        ].str.strip()

        self.plays_json.drop(
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
            ],
            axis=1,
            inplace=True,
        )
        return self.plays_json

    def __after_cols(self):
        self.plays_json["new_down"] = np.select(
            [
                (self.plays_json["type.text"] == "Timeout"),
                # 8 cases with three T/F penalty flags
                # 4 cases in 1
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == True),
                # offsetting penalties, no penalties declined, no 1st down by penalty (1 case)
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == False),
                # offsetting penalties, penalty declined true, no 1st down by penalty
                # seems like it would be a regular play at that point (1 case, split in three)
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] <= 3),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] == 4),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] >= self.plays_json["start.distance"]),
                # only penalty declined true, same logic as prior (1 case, split in three)
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == False)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] <= 3),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == False)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] == 4),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == False)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] >= self.plays_json["start.distance"]),
            ],
            [
                self.plays_json["start.down"],
                1,
                self.plays_json["start.down"],
                self.plays_json["start.down"] + 1,
                1,
                1,
                self.plays_json["start.down"] + 1,
                1,
                1,
            ],
            default=self.plays_json["start.down"],
        )
        self.plays_json["new_distance"] = np.select(
            [
                (self.plays_json["type.text"] == "Timeout"),
                # 8 cases with three T/F penalty flags
                # 4 cases in 1
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == True),
                # offsetting penalties, no penalties declined, no 1st down by penalty (1 case)
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == False),
                # offsetting penalties, penalty declined true, no 1st down by penalty
                # seems like it would be a regular play at that point (1 case, split in three)
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] <= 3),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] == 4),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == True)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] >= self.plays_json["start.distance"]),
                # only penalty declined true, same logic as prior (1 case, split in three)
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == False)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] <= 3),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == False)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] < self.plays_json["start.distance"])
                & (self.plays_json["start.down"] == 4),
                (self.plays_json["type.text"].isin(penalty))
                & (self.plays_json["penalty_1st_conv"] == False)
                & (self.plays_json["penalty_offset"] == False)
                & (self.plays_json["penalty_declined"] == True)
                & (self.plays_json["statYardage"] >= self.plays_json["start.distance"]),
            ],
            [
                self.plays_json["start.distance"],
                10,
                self.plays_json["start.distance"],
                self.plays_json["start.distance"] - self.plays_json["statYardage"],
                10,
                10,
                self.plays_json["start.distance"] - self.plays_json["statYardage"],
                10,
                10,
            ],
            default=self.plays_json["start.distance"],
        )

        self.plays_json["middle_8"] = np.where(
            (self.plays_json["start.adj_TimeSecsRem"] >= 1560)
            & (self.plays_json["start.adj_TimeSecsRem"] <= 2040),
            True,
            False,
        )
        self.plays_json["rz_play"] = np.where(
            self.plays_json["start.yardsToEndzone"] <= 20, True, False
        )
        self.plays_json["scoring_opp"] = np.where(
            self.plays_json["start.yardsToEndzone"] <= 40, True, False
        )
        self.plays_json["stuffed_run"] = np.where(
            (self.plays_json.rush == True) & (self.plays_json.yds_rushed <= 0), True, False
        )
        self.plays_json["stopped_run"] = np.where(
            (self.plays_json.rush == True) & (self.plays_json.yds_rushed <= 2), True, False
        )
        self.plays_json["opportunity_run"] = np.where(
            (self.plays_json.rush == True) & (self.plays_json.yds_rushed >= 4), True, False
        )
        self.plays_json["highlight_run"] = np.where(
            (self.plays_json.rush == True) & (self.plays_json.yds_rushed >= 8), True, False
        )

        self.plays_json["adj_rush_yardage"] = np.select(
            [
                (self.plays_json.rush == True) & (self.plays_json.yds_rushed > 10),
                (self.plays_json.rush == True) & (self.plays_json.yds_rushed <= 10),
            ],
            [10, self.plays_json.yds_rushed],
            default=None,
        )
        self.plays_json["line_yards"] = np.select(
            [
                (self.plays_json.rush == 1) & (self.plays_json.yds_rushed < 0),
                (self.plays_json.rush == 1)
                & (self.plays_json.yds_rushed >= 0)
                & (self.plays_json.yds_rushed <= 4),
                (self.plays_json.rush == 1)
                & (self.plays_json.yds_rushed >= 5)
                & (self.plays_json.yds_rushed <= 10),
                (self.plays_json.rush == 1) & (self.plays_json.yds_rushed >= 11),
            ],
            [
                1.2 * self.plays_json.adj_rush_yardage,
                self.plays_json.adj_rush_yardage,
                0.5 * self.plays_json.adj_rush_yardage,
                0.0,
            ],
            default=None,
        )

        self.plays_json["second_level_yards"] = np.select(
            [(self.plays_json.rush == 1) & (self.plays_json.yds_rushed >= 5), (self.plays_json.rush == 1)],
            [(0.5 * (self.plays_json.adj_rush_yardage - 5)), 0],
            default=None,
        )

        self.plays_json["open_field_yards"] = np.select(
            [(self.plays_json.rush == 1) & (self.plays_json.yds_rushed > 10), (self.plays_json.rush == 1)],
            [(self.plays_json.yds_rushed - self.plays_json.adj_rush_yardage), 0],
            default=None,
        )

        self.plays_json["highlight_yards"] = (
            self.plays_json["second_level_yards"] + self.plays_json["open_field_yards"]
        )

        self.plays_json["opp_highlight_yards"] = np.select(
            [
                (self.plays_json.opportunity_run == True),
                (self.plays_json.opportunity_run == False) & (self.plays_json.rush == 1),
            ],
            [self.plays_json["highlight_yards"], 0.0],
            default=None,
        )

        self.plays_json["short_rush_success"] = np.where(
            (self.plays_json["start.distance"] < 2)
            & (self.plays_json.rush == True)
            & (self.plays_json.statYardage >= self.plays_json["start.distance"]),
            True,
            False,
        )
        self.plays_json["short_rush_attempt"] = np.where(
            (self.plays_json["start.distance"] < 2) & (self.plays_json.rush == True), True, False
        )
        self.plays_json["power_rush_success"] = np.where(
            (self.plays_json["start.distance"] < 2)
            & (self.plays_json["start.down"].isin([3, 4]))
            & (self.plays_json.rush == True)
            & (self.plays_json.statYardage >= self.plays_json["start.distance"]),
            True,
            False,
        )
        self.plays_json["power_rush_attempt"] = np.where(
            (self.plays_json["start.distance"] < 2)
            & (self.plays_json["start.down"].isin([3, 4]))
            & (self.plays_json.rush == True),
            True,
            False,
        )
        self.plays_json["early_down"] = np.where(
            ((self.plays_json.down_1 == True) | (self.plays_json.down_2 == True))
            & (self.plays_json.scrimmage_play == True),
            True,
            False,
        )
        self.plays_json["late_down"] = np.where(
            (self.plays_json.early_down == False) & (self.plays_json.scrimmage_play == True),
            True,
            False,
        )
        self.plays_json["early_down_pass"] = np.where(
            (self.plays_json["pass"] == 1) & (self.plays_json.early_down == True), True, False
        )
        self.plays_json["early_down_rush"] = np.where(
            (self.plays_json["rush"] == 1) & (self.plays_json.early_down == True), True, False
        )
        self.plays_json["late_down_pass"] = np.where(
            (self.plays_json["pass"] == 1) & (self.plays_json.late_down == True), True, False
        )
        self.plays_json["late_down_rush"] = np.where(
            (self.plays_json["rush"] == 1) & (self.plays_json.late_down == True), True, False
        )
        self.plays_json["standard_down"] = np.select(
            [
                (self.plays_json.scrimmage_play == True) & (self.plays_json.down_1 == True),
                (self.plays_json.scrimmage_play == True)
                & (self.plays_json.down_2 == True)
                & (self.plays_json["start.distance"] < 8),
                (self.plays_json.scrimmage_play == True)
                & (self.plays_json.down_3 == True)
                & (self.plays_json["start.distance"] < 5),
                (self.plays_json.scrimmage_play == True)
                & (self.plays_json.down_4 == True)
                & (self.plays_json["start.distance"] < 5),
            ],
            [True, True, True, True],
            default=False,
        )
        self.plays_json["passing_down"] = np.select(
            [
                (self.plays_json.scrimmage_play == True)
                & (self.plays_json.down_2 == True)
                & (self.plays_json["start.distance"] >= 8),
                (self.plays_json.scrimmage_play == True)
                & (self.plays_json.down_3 == True)
                & (self.plays_json["start.distance"] >= 5),
                (self.plays_json.scrimmage_play == True)
                & (self.plays_json.down_4 == True)
                & (self.plays_json["start.distance"] >= 5),
            ],
            [True, True, True],
            default=False,
        )
        self.plays_json["TFL"] = np.select(
            [
                (self.plays_json["type.text"] != "Penalty")
                & (self.plays_json.sp == False)
                & (self.plays_json.statYardage < 0),
                (self.plays_json["sack_vec"] == True),
            ],
            [True, True],
            default=False,
        )
        self.plays_json["TFL_pass"] = np.where(
            (self.plays_json["TFL"] == True) & (self.plays_json["pass"] == True), True, False
        )
        self.plays_json["TFL_rush"] = np.where(
            (self.plays_json["TFL"] == True) & (self.plays_json["rush"] == True), True, False
        )
        self.plays_json["havoc"] = np.select(
            [
                (self.plays_json["pass_breakup"] == True),
                (self.plays_json["TFL"] == True),
                (self.plays_json["int"] == True),
                (self.plays_json["forced_fumble"] == True),
            ],
            [True, True, True, True],
            default=False,
        )
        return self.plays_json

    def __add_spread_time(self):
        self.plays_json["start.pos_team_spread"] = np.where(
            (self.plays_json["start.pos_team.id"] == self.plays_json["homeTeamId"]),
            self.plays_json["homeTeamSpread"],
            -1 * self.plays_json["homeTeamSpread"],
        )
        self.plays_json["start.elapsed_share"] = (
            (3600 - self.plays_json["start.adj_TimeSecsRem"]) / 3600
        ).clip(0, 3600)
        self.plays_json["start.spread_time"] = self.plays_json["start.pos_team_spread"] * np.exp(
            -4 * self.plays_json["start.elapsed_share"]
        )
        self.plays_json["end.pos_team_spread"] = np.where(
            (self.plays_json["end.pos_team.id"] == self.plays_json["homeTeamId"]),
            self.plays_json["homeTeamSpread"],
            -1 * self.plays_json["homeTeamSpread"],
        )
        self.plays_json["end.pos_team_spread"] = np.where(
            (self.plays_json["end.pos_team.id"] == self.plays_json["homeTeamId"]),
            self.plays_json["homeTeamSpread"],
            -1 * self.plays_json["homeTeamSpread"],
        )
        self.plays_json["end.elapsed_share"] = (
            (3600 - self.plays_json["end.adj_TimeSecsRem"]) / 3600
        ).clip(0, 3600)
        self.plays_json["end.spread_time"] = self.plays_json["end.pos_team_spread"] * np.exp(
            -4 * self.plays_json["end.elapsed_share"]
        )
        return self.plays_json

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

    def __process_epa(self):
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "down"] = 1
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "start.down"] = 1
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "down_1"] = True
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "down_2"] = False
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "down_3"] = False
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "down_4"] = False
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "distance"] = 10
        self.plays_json.loc[self.plays_json["type.text"].isin(kickoff_vec), "start.distance"] = 10
        self.plays_json["start.yardsToEndzone.touchback"] = 99
        self.plays_json.loc[
            (self.plays_json["type.text"].isin(kickoff_vec)) & (self.plays_json["season"] > 2013),
            "start.yardsToEndzone.touchback",
        ] = 75
        self.plays_json.loc[
            (self.plays_json["type.text"].isin(kickoff_vec)) & (self.plays_json["season"] <= 2013),
            "start.yardsToEndzone.touchback",
        ] = 80

        start_touchback_data = self.plays_json[ep_start_touchback_columns]
        start_touchback_data.columns = ep_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))

        dtest_start_touchback = DMatrix(start_touchback_data)
        EP_start_touchback_parts = ep_model.predict(dtest_start_touchback)
        EP_start_touchback = self.__calculate_ep_exp_val(EP_start_touchback_parts)

        start_data = self.plays_json[ep_start_columns]
        start_data.columns = ep_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))

        dtest_start = DMatrix(start_data)
        EP_start_parts = ep_model.predict(dtest_start)
        EP_start = self.__calculate_ep_exp_val(EP_start_parts)

        self.plays_json.loc[self.plays_json["end.TimeSecsRem"] <= 0, "end.TimeSecsRem"] = 0
        self.plays_json.loc[
            (self.plays_json["end.TimeSecsRem"] <= 0) & (self.plays_json.period < 5),
            "end.yardsToEndzone",
        ] = 99
        self.plays_json.loc[
            (self.plays_json["end.TimeSecsRem"] <= 0) & (self.plays_json.period < 5), "down_1_end"
        ] = True
        self.plays_json.loc[
            (self.plays_json["end.TimeSecsRem"] <= 0) & (self.plays_json.period < 5), "down_2_end"
        ] = False
        self.plays_json.loc[
            (self.plays_json["end.TimeSecsRem"] <= 0) & (self.plays_json.period < 5), "down_3_end"
        ] = False
        self.plays_json.loc[
            (self.plays_json["end.TimeSecsRem"] <= 0) & (self.plays_json.period < 5), "down_4_end"
        ] = False

        self.plays_json.loc[self.plays_json["end.yardsToEndzone"] >= 100, "end.yardsToEndzone"] = 99
        self.plays_json.loc[self.plays_json["end.yardsToEndzone"] <= 0, "end.yardsToEndzone"] = 99

        self.plays_json.loc[self.plays_json.kickoff_tb == True, "end.yardsToEndzone"] = 75
        self.plays_json.loc[self.plays_json.kickoff_tb == True, "end.down"] = 1
        self.plays_json.loc[self.plays_json.kickoff_tb == True, "end.distance"] = 10

        self.plays_json.loc[self.plays_json.punt_tb == True, "end.down"] = 1
        self.plays_json.loc[self.plays_json.punt_tb == True, "end.distance"] = 10
        self.plays_json.loc[self.plays_json.punt_tb == True, "end.yardsToEndzone"] = 80

        end_data = self.plays_json[ep_end_columns]
        end_data.columns = ep_final_names
        # self.logger.info(end_data.iloc[[36]].to_json(orient="records"))
        dtest_end = DMatrix(end_data)
        EP_end_parts = ep_model.predict(dtest_end)

        EP_end = self.__calculate_ep_exp_val(EP_end_parts)

        self.plays_json["EP_start_touchback"] = EP_start_touchback
        self.plays_json["EP_start"] = EP_start
        self.plays_json["EP_end"] = EP_end
        kick = "kick)"
        self.plays_json["EP_start"] = np.where(
            self.plays_json["type.text"].isin(
                [
                    "Extra Point Good",
                    "Extra Point Missed",
                    "Two-Point Conversion Good",
                    "Two-Point Conversion Missed",
                    "Two Point Pass",
                    "Two Point Rush",
                    "Blocked PAT",
                ]
            ),
            0.92,
            self.plays_json["EP_start"],
        )
        self.plays_json.EP_end = np.select(
            [
                # End of Half
                (
                    self.plays_json["type.text"]
                    .str.lower()
                    .str.contains(
                        "end of game", case=False, flags=0, na=False, regex=True
                    )
                )
                | (
                    self.plays_json["type.text"]
                    .str.lower()
                    .str.contains(
                        "end of game", case=False, flags=0, na=False, regex=True
                    )
                )
                | (
                    self.plays_json["type.text"]
                    .str.lower()
                    .str.contains(
                        "end of half", case=False, flags=0, na=False, regex=True
                    )
                )
                | (
                    self.plays_json["type.text"]
                    .str.lower()
                    .str.contains(
                        "end of half", case=False, flags=0, na=False, regex=True
                    )
                ),
                # Def 2pt conversion is its own play
                (self.plays_json["type.text"].isin(["Defensive 2pt Conversion"])),
                # Safeties
                (
                    (self.plays_json["type.text"].isin(defense_score_vec))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains("safety", case=False, regex=True)
                    )
                ),
                # Defense TD + Successful Two-Point Conversion
                (
                    (self.plays_json["type.text"].isin(defense_score_vec))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains("conversion", case=False, regex=False)
                    )
                    & (
                        ~self.plays_json["text"]
                        .str.lower()
                        .str.contains(r"failed\s?\)", case=False, regex=True)
                    )
                ),
                # Defense TD + Failed Two-Point Conversion
                (
                    (self.plays_json["type.text"].isin(defense_score_vec))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains("conversion", case=False, regex=False)
                    )
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains(r"failed\s?\)", case=False, regex=True)
                    )
                ),
                # Defense TD + Kick/PAT Missed
                (
                    (self.plays_json["type.text"].isin(defense_score_vec))
                    & (self.plays_json["text"].str.contains("PAT", case=True, regex=False))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains(r"missed\s?\)", case=False, regex=True)
                    )
                ),
                # Defense TD + Kick/PAT Good
                (
                    (self.plays_json["type.text"].isin(defense_score_vec))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains(kick, case=False, regex=False)
                    )
                ),
                # Defense TD
                (self.plays_json["type.text"].isin(defense_score_vec)),
                # Offense TD + Failed Two-Point Conversion
                (
                    (self.plays_json["type.text"].isin(offense_score_vec))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains("conversion", case=False, regex=False)
                    )
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains(r"failed\s?\)", case=False, regex=True)
                    )
                ),
                # Offense TD + Successful Two-Point Conversion
                (
                    (self.plays_json["type.text"].isin(offense_score_vec))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains("conversion", case=False, regex=False)
                    )
                    & (
                        ~self.plays_json["text"]
                        .str.lower()
                        .str.contains(r"failed\s?\)", case=False, regex=True)
                    )
                ),
                # Offense Made FG
                (
                    (self.plays_json["type.text"].isin(offense_score_vec))
                    & (
                        self.plays_json["type.text"]
                        .str.lower()
                        .str.contains(
                            "field goal", case=False, flags=0, na=False, regex=True
                        )
                    )
                    & (
                        self.plays_json["type.text"]
                        .str.lower()
                        .str.contains("good", case=False, flags=0, na=False, regex=True)
                    )
                ),
                # Missed FG -- Not Needed
                # (self.plays_json["type.text"].isin(offense_score_vec)) &
                # (self.plays_json["type.text"].str.lower().str.contains('field goal', case=False, flags=0, na=False, regex=True)) &
                # (~self.plays_json["type.text"].str.lower().str.contains('good', case=False, flags=0, na=False, regex=True)),
                # Offense TD + Kick/PAT Missed
                (
                    (self.plays_json["type.text"].isin(offense_score_vec))
                    & (
                        ~self.plays_json["text"]
                        .str.lower()
                        .str.contains("conversion", case=False, regex=False)
                    )
                    & ((self.plays_json["text"].str.contains("PAT", case=True, regex=False)))
                    & (
                        (
                            self.plays_json["text"]
                            .str.lower()
                            .str.contains(r"missed\s?\)", case=False, regex=True)
                        )
                    )
                ),
                # Offense TD + Kick PAT Good
                (
                    (self.plays_json["type.text"].isin(offense_score_vec))
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains(kick, case=False, regex=False)
                    )
                ),
                # Offense TD
                (self.plays_json["type.text"].isin(offense_score_vec)),
                # Extra Point Good (pre-2014 data)
                (self.plays_json["type.text"] == "Extra Point Good"),
                # Extra Point Missed (pre-2014 data)
                (self.plays_json["type.text"] == "Extra Point Missed"),
                # Extra Point Blocked (pre-2014 data)
                (self.plays_json["type.text"] == "Blocked PAT"),
                # Two-Point Good (pre-2014 data)
                (self.plays_json["type.text"] == "Two-Point Conversion Good"),
                # Two-Point Missed (pre-2014 data)
                (self.plays_json["type.text"] == "Two-Point Conversion Missed"),
                # Two-Point No Good (pre-2014 data)
                (
                    (
                        (self.plays_json["type.text"] == "Two Point Pass")
                        | (self.plays_json["type.text"] == "Two Point Rush")
                    )
                    & (
                        self.plays_json["text"]
                        .str.lower()
                        .str.contains("no good", case=False, regex=False)
                    )
                ),
                # Two-Point Good (pre-2014 data)
                (
                    (
                        (self.plays_json["type.text"] == "Two Point Pass")
                        | (self.plays_json["type.text"] == "Two Point Rush")
                    )
                    & (
                        ~self.plays_json["text"]
                        .str.lower()
                        .str.contains("no good", case=False, regex=False)
                    )
                ),
                # Flips for Turnovers that aren't kickoffs
                (
                    (
                        (self.plays_json["type.text"].isin(end_change_vec))
                        | (self.plays_json.downs_turnover == True)
                    )
                    & (self.plays_json.kickoff_play == False)
                ),
                # Flips for Turnovers that are on kickoffs
                (self.plays_json["type.text"].isin(kickoff_turnovers)),
                # onside recoveries
                (self.plays_json["kickoff_onside"] == True) & ((self.plays_json["change_of_pos_team"] == True) | (self.plays_json["change_of_poss"] == True)),
            ],
            [
                0,
                -2,
                -2,
                -6,
                -8,
                -6,
                -7,
                -6.92,
                6,
                8,
                3,
                6,
                7,
                6.92,
                1,
                0,
                0,
                2,
                0,
                0,
                2,
                (self.plays_json.EP_end * -1),
                (self.plays_json.EP_end * -1),
                (self.plays_json.EP_end * -1),
            ],
            default=self.plays_json.EP_end,
        )
        self.plays_json["lag_EP_end"] = self.plays_json["EP_end"].shift(1)
        self.plays_json["lag_change_of_pos_team"] = self.plays_json.change_of_pos_team.shift(1)
        self.plays_json["lag_change_of_pos_team"] = np.where(
            self.plays_json["lag_change_of_pos_team"].isna(),
            False,
            self.plays_json["lag_change_of_pos_team"],
        )
        self.plays_json["EP_between"] = np.where(
            self.plays_json.lag_change_of_pos_team == True,
            self.plays_json["EP_start"] + self.plays_json["lag_EP_end"],
            self.plays_json["EP_start"] - self.plays_json["lag_EP_end"],
        )
        self.plays_json["EP_start"] = np.where(
            (self.plays_json["type.text"].isin(["Timeout", "End Period"]))
            & (self.plays_json["lag_change_of_pos_team"] == False),
            self.plays_json["lag_EP_end"],
            self.plays_json["EP_start"],
        )
        self.plays_json["EP_start"] = np.where(
            (self.plays_json["type.text"].isin(kickoff_vec)),
            self.plays_json["EP_start_touchback"],
            self.plays_json["EP_start"],
        )
        self.plays_json["EP_end"] = np.where(
            (self.plays_json["type.text"] == "Timeout"), self.plays_json["EP_start"], self.plays_json["EP_end"]
        )
        self.plays_json["EPA"] = np.select(
            [
                (self.plays_json["type.text"] == "Timeout"),
                (self.plays_json["scoring_play"] == False) & (self.plays_json["end_of_half"] == True),
                (self.plays_json["type.text"].isin(kickoff_vec))
                & (self.plays_json["penalty_in_text"] == True),
                (self.plays_json["penalty_in_text"] == True)
                & (self.plays_json["type.text"] != "Penalty")
                & (~self.plays_json["type.text"].isin(kickoff_vec)),
            ],
            [
                0,
                -1 * self.plays_json["EP_start"],
                self.plays_json["EP_end"] - self.plays_json["EP_start"],
                (self.plays_json["EP_end"] - self.plays_json["EP_start"] + self.plays_json["EP_between"]),
            ],
            default=(self.plays_json["EP_end"] - self.plays_json["EP_start"]),
        )
        self.plays_json["def_EPA"] = -1 * self.plays_json["EPA"]
        # ----- EPA Summary flags ------
        self.plays_json["EPA_scrimmage"] = np.select(
            [(self.plays_json.scrimmage_play == True)], [self.plays_json.EPA], default=None
        )
        self.plays_json["EPA_rush"] = np.select(
            [
                (self.plays_json.rush == True) & (self.plays_json["penalty_in_text"] == True),
                (self.plays_json.rush == True) & (self.plays_json["penalty_in_text"] == False),
            ],
            [self.plays_json.EPA, self.plays_json.EPA],
            default=None,
        )
        self.plays_json["EPA_pass"] = np.where((self.plays_json["pass"] == True), self.plays_json.EPA, None)

        self.plays_json["EPA_explosive"] = np.where(
            ((self.plays_json["pass"] == True) & (self.plays_json["EPA"] >= 2.4))
            | (((self.plays_json["rush"] == True) & (self.plays_json["EPA"] >= 1.8))),
            True,
            False,
        )
        self.plays_json["EPA_non_explosive"] = np.where((self.plays_json["EPA_explosive"] == False), self.plays_json.EPA, None)

        self.plays_json["EPA_explosive_pass"] = np.where(
            ((self.plays_json["pass"] == True) & (self.plays_json["EPA"] >= 2.4)), True, False
        )
        self.plays_json["EPA_explosive_rush"] = np.where(
            (((self.plays_json["rush"] == True) & (self.plays_json["EPA"] >= 1.8))), True, False
        )

        self.plays_json["first_down_created"] = np.where(
            (self.plays_json.scrimmage_play == True)
            & (self.plays_json["end.down"] == 1)
            & (self.plays_json["start.pos_team.id"] == self.plays_json["end.pos_team.id"]),
            True,
            False,
        )

        self.plays_json["EPA_success"] = np.where(self.plays_json.EPA > 0, True, False)
        self.plays_json["EPA_success_early_down"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.early_down == True), True, False
        )
        self.plays_json["EPA_success_early_down_pass"] = np.where(
            (self.plays_json["pass"] == True)
            & (self.plays_json.EPA > 0)
            & (self.plays_json.early_down == True),
            True,
            False,
        )
        self.plays_json["EPA_success_early_down_rush"] = np.where(
            (self.plays_json["rush"] == True)
            & (self.plays_json.EPA > 0)
            & (self.plays_json.early_down == True),
            True,
            False,
        )
        self.plays_json["EPA_success_late_down"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.late_down == True), True, False
        )
        self.plays_json["EPA_success_late_down_pass"] = np.where(
            (self.plays_json["pass"] == True) & (self.plays_json.EPA > 0) & (self.plays_json.late_down == True),
            True,
            False,
        )
        self.plays_json["EPA_success_late_down_rush"] = np.where(
            (self.plays_json["rush"] == True) & (self.plays_json.EPA > 0) & (self.plays_json.late_down == True),
            True,
            False,
        )
        self.plays_json["EPA_success_standard_down"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.standard_down == True), True, False
        )
        self.plays_json["EPA_success_passing_down"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.passing_down == True), True, False
        )
        self.plays_json["EPA_success_pass"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json["pass"] == True), True, False
        )
        self.plays_json["EPA_success_rush"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.rush == True), True, False
        )
        self.plays_json["EPA_success_EPA"] = np.where(self.plays_json.EPA > 0, self.plays_json.EPA, None)
        self.plays_json["EPA_success_standard_down_EPA"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.standard_down == True), self.plays_json.EPA, None
        )
        self.plays_json["EPA_success_passing_down_EPA"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.passing_down == True), self.plays_json.EPA, None
        )
        self.plays_json["EPA_success_pass_EPA"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json["pass"] == True), self.plays_json.EPA, None
        )
        self.plays_json["EPA_success_rush_EPA"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json.rush == True), True, False
        )
        self.plays_json["EPA_middle_8_success"] = np.where(
            (self.plays_json.EPA > 0) & (self.plays_json["middle_8"] == True), True, False
        )
        self.plays_json["EPA_middle_8_success_pass"] = np.where(
            (self.plays_json["pass"] == True)
            & (self.plays_json.EPA > 0)
            & (self.plays_json["middle_8"] == True),
            True,
            False,
        )
        self.plays_json["EPA_middle_8_success_rush"] = np.where(
            (self.plays_json["rush"] == True)
            & (self.plays_json.EPA > 0)
            & (self.plays_json["middle_8"] == True),
            True,
            False,
        )
        self.plays_json["EPA_penalty"] = np.select(
            [
                (self.plays_json["type.text"].isin(["Penalty", "Penalty (Kickoff)"])),
                (self.plays_json["penalty_in_text"] == True),
            ],
            [self.plays_json["EPA"], self.plays_json["EP_end"] - self.plays_json["EP_start"]],
            default=None,
        )
        self.plays_json["EPA_sp"] = np.where(
            (self.plays_json.fg_attempt == True)
            | (self.plays_json.punt == True)
            | (self.plays_json.kickoff_play == True),
            self.plays_json["EPA"],
            False,
        )
        self.plays_json["EPA_fg"] = np.where((self.plays_json.fg_attempt == True), self.plays_json["EPA"], None)
        self.plays_json["EPA_punt"] = np.where((self.plays_json.punt == True), self.plays_json["EPA"], None)
        self.plays_json["EPA_kickoff"] = np.where(
            (self.plays_json.kickoff_play == True), self.plays_json["EPA"], None
        )
        return self.plays_json

    def __process_qbr(self):
        self.plays_json["qbr_epa"] = np.select(
            [
                (self.plays_json.EPA < -5.0),
                (self.plays_json.fumble_vec == True),
            ],
            [-5.0, -3.5],
            default=self.plays_json.EPA,
        )

        self.plays_json["weight"] = np.select(
            [
                (self.plays_json.home_wp_before < 0.1),
                (self.plays_json.home_wp_before >= 0.1) & (self.plays_json.home_wp_before < 0.2),
                (self.plays_json.home_wp_before >= 0.8) & (self.plays_json.home_wp_before < 0.9),
                (self.plays_json.home_wp_before > 0.9),
            ],
            [0.6, 0.9, 0.9, 0.6],
            default=1,
        )
        self.plays_json["non_fumble_sack"] = (self.plays_json["sack_vec"] == True) & (
            self.plays_json["fumble_vec"] == False
        )

        self.plays_json["sack_epa"] = np.where(
            self.plays_json["non_fumble_sack"] == True, self.plays_json["qbr_epa"], np.NaN
        )
        self.plays_json["pass_epa"] = np.where(
            self.plays_json["pass"] == True, self.plays_json["qbr_epa"], np.NaN
        )
        self.plays_json["rush_epa"] = np.where(
            self.plays_json["rush"] == True, self.plays_json["qbr_epa"], np.NaN
        )
        self.plays_json["pen_epa"] = np.where(
            self.plays_json["penalty_flag"] == True, self.plays_json["qbr_epa"], np.NaN
        )

        self.plays_json["sack_weight"] = np.where(
            self.plays_json["non_fumble_sack"] == True, self.plays_json["weight"], np.NaN
        )
        self.plays_json["pass_weight"] = np.where(
            self.plays_json["pass"] == True, self.plays_json["weight"], np.NaN
        )
        self.plays_json["rush_weight"] = np.where(
            self.plays_json["rush"] == True, self.plays_json["weight"], np.NaN
        )
        self.plays_json["pen_weight"] = np.where(
            self.plays_json["penalty_flag"] == True, self.plays_json["weight"], np.NaN
        )

        self.plays_json["action_play"] = self.plays_json.EPA != 0
        self.plays_json["athlete_name"] = np.select(
            [
                self.plays_json.passer_player_name.notna(),
                self.plays_json.rusher_player_name.notna(),
            ],
            [self.plays_json.passer_player_name, self.plays_json.rusher_player_name],
            default=None,
        )
        return self.plays_json

    def __process_wpa(self):
        # ---- prepare variables for wp_before calculations ----
        self.plays_json["start.ExpScoreDiff_touchback"] = np.select(
            [(self.plays_json["type.text"].isin(kickoff_vec))],
            [self.plays_json["pos_score_diff_start"] + self.plays_json["EP_start_touchback"]],
            default=0.000,
        )
        self.plays_json["start.ExpScoreDiff"] = np.select(
            [
                (self.plays_json["penalty_in_text"] == True)
                & (self.plays_json["type.text"] != "Penalty"),
                (self.plays_json["type.text"] == "Timeout")
                & (self.plays_json["lag_scoringPlay"] == True),
            ],
            [
                self.plays_json["pos_score_diff_start"]
                + self.plays_json["EP_start"]
                - self.plays_json["EP_between"],
                (self.plays_json["pos_score_diff_start"] + 0.92),
            ],
            default=self.plays_json["pos_score_diff_start"] + self.plays_json.EP_start,
        )
        self.plays_json["start.ExpScoreDiff_Time_Ratio_touchback"] = self.plays_json[
            "start.ExpScoreDiff_touchback"
        ] / (self.plays_json["start.adj_TimeSecsRem"] + 1)
        self.plays_json["start.ExpScoreDiff_Time_Ratio"] = self.plays_json["start.ExpScoreDiff"] / (
            self.plays_json["start.adj_TimeSecsRem"] + 1
        )

        # ---- prepare variables for wp_after calculations ----
        self.plays_json["end.ExpScoreDiff"] = np.select(
            [
                # Flips for Turnovers that aren't kickoffs
                (
                    (
                        (self.plays_json["type.text"].isin(end_change_vec))
                        | (self.plays_json.downs_turnover == True)
                    )
                    & (self.plays_json.kickoff_play == False)
                    & (self.plays_json["scoringPlay"] == False)
                ),
                # Flips for Turnovers that are on kickoffs
                (self.plays_json["type.text"].isin(kickoff_turnovers))
                & (self.plays_json["scoringPlay"] == False),
                (self.plays_json["scoringPlay"] == False) & (self.plays_json["type.text"] != "Timeout"),
                (self.plays_json["scoringPlay"] == False) & (self.plays_json["type.text"] == "Timeout"),
                (self.plays_json["scoringPlay"] == True)
                & (self.plays_json["td_play"] == True)
                & (self.plays_json["type.text"].isin(defense_score_vec))
                & (self.plays_json.season <= 2013),
                (self.plays_json["scoringPlay"] == True)
                & (self.plays_json["td_play"] == True)
                & (self.plays_json["type.text"].isin(offense_score_vec))
                & (self.plays_json.season <= 2013),
                (self.plays_json["type.text"] == "Timeout")
                & (self.plays_json["lag_scoringPlay"] == True)
                & (self.plays_json.season <= 2013),
            ],
            [
                self.plays_json["pos_score_diff_end"] - self.plays_json.EP_end,
                self.plays_json["pos_score_diff_end"] + self.plays_json.EP_end,
                self.plays_json["pos_score_diff_end"] + self.plays_json.EP_end,
                self.plays_json["pos_score_diff_end"] + self.plays_json.EP_end,
                self.plays_json["pos_score_diff_end"] + 0.92,
                self.plays_json["pos_score_diff_end"] + 0.92,
                self.plays_json["pos_score_diff_end"] + 0.92,
            ],
            default=self.plays_json["pos_score_diff_end"],
        )
        self.plays_json["end.ExpScoreDiff_Time_Ratio"] = self.plays_json["end.ExpScoreDiff"] / (
            self.plays_json["end.adj_TimeSecsRem"] + 1
        )
        # ---- wp_before ----
        start_touchback_data = self.plays_json[wp_start_touchback_columns]
        start_touchback_data.columns = wp_final_names
        # self.logger.info(start_touchback_data.iloc[[36]].to_json(orient="records"))
        dtest_start_touchback = DMatrix(start_touchback_data)
        WP_start_touchback = wp_model.predict(dtest_start_touchback)
        start_data = self.plays_json[wp_start_columns]
        start_data.columns = wp_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))
        dtest_start = DMatrix(start_data)
        WP_start = wp_model.predict(dtest_start)
        self.plays_json["wp_before"] = WP_start
        self.plays_json["wp_touchback"] = WP_start_touchback
        self.plays_json["wp_before"] = np.where(
            self.plays_json["type.text"].isin(kickoff_vec),
            self.plays_json["wp_touchback"],
            self.plays_json["wp_before"],
        )
        self.plays_json["def_wp_before"] = 1 - self.plays_json.wp_before
        self.plays_json["home_wp_before"] = np.where(
            self.plays_json["start.pos_team.id"] == self.plays_json["homeTeamId"],
            self.plays_json.wp_before,
            self.plays_json.def_wp_before,
        )
        self.plays_json["away_wp_before"] = np.where(
            self.plays_json["start.pos_team.id"] != self.plays_json["homeTeamId"],
            self.plays_json.wp_before,
            self.plays_json.def_wp_before,
        )
        # ---- wp_after ----
        end_data = self.plays_json[wp_end_columns]
        end_data.columns = wp_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))
        dtest_end = DMatrix(end_data)
        WP_end = wp_model.predict(dtest_end)

        self.plays_json["lead_wp_before"] = self.plays_json["wp_before"].shift(-1)
        self.plays_json["lead_wp_before2"] = self.plays_json["wp_before"].shift(-2)

        self.plays_json["wp_after"] = WP_end
        game_complete = self.json["teamInfo"]["status"]["type"]["completed"]
        self.plays_json["wp_after"] = np.select(
            [
                (self.plays_json["type.text"] == "Timeout"),
                game_complete
                & (
                    (self.plays_json.lead_play_type.isna())
                    | (self.plays_json.game_play_number == max(self.plays_json.game_play_number))
                )
                & (self.plays_json.pos_score_diff_end > 0),
                game_complete
                & (
                    (self.plays_json.lead_play_type.isna())
                    | (self.plays_json.game_play_number == max(self.plays_json.game_play_number))
                )
                & (self.plays_json.pos_score_diff_end < 0),
                (self.plays_json.end_of_half == 1)
                & (self.plays_json["start.pos_team.id"] == self.plays_json.lead_pos_team)
                & (self.plays_json["type.text"] != "Timeout"),
                (self.plays_json.end_of_half == 1)
                & (self.plays_json["start.pos_team.id"] != self.plays_json["end.pos_team.id"])
                & (self.plays_json["type.text"] != "Timeout"),
                (self.plays_json.end_of_half == 1)
                & (self.plays_json["start.pos_team_receives_2H_kickoff"] == False)
                & (self.plays_json["type.text"] == "Timeout"),
                (self.plays_json.lead_play_type.isin(["End Period", "End of Half"]))
                & (self.plays_json.change_of_pos_team == 0),
                (self.plays_json.lead_play_type.isin(["End Period", "End of Half"]))
                & (self.plays_json.change_of_pos_team == 1),
                (self.plays_json["kickoff_onside"] == True)
                & ((self.plays_json["change_of_pos_team"] == True) | (self.plays_json["change_of_poss"] == True)),  # onside recovery
                (self.plays_json["start.pos_team.id"] != self.plays_json["end.pos_team.id"]),
            ],
            [
                self.plays_json.wp_before,
                1.0,
                0.0,
                self.plays_json.lead_wp_before,
                (1 - self.plays_json.lead_wp_before),
                self.plays_json.wp_after,
                self.plays_json.lead_wp_before,
                (1 - self.plays_json.lead_wp_before),
                (1 - self.plays_json.lead_wp_before),
                (1 - self.plays_json.wp_after),
            ],
            default=self.plays_json.wp_after,
        )

        self.plays_json["def_wp_after"] = 1 - self.plays_json.wp_after
        self.plays_json["home_wp_after"] = np.where(
            self.plays_json["end.pos_team.id"] == self.plays_json["homeTeamId"],
            self.plays_json.wp_after,
            self.plays_json.def_wp_after,
        )
        self.plays_json["away_wp_after"] = np.where(
            self.plays_json["end.pos_team.id"] != self.plays_json["homeTeamId"],
            self.plays_json.wp_after,
            self.plays_json.def_wp_after,
        )

        self.plays_json["wpa"] = self.plays_json.wp_after - self.plays_json.wp_before
        return self.plays_json

    def __add_drive_data(self):
        base_groups = self.plays_json.groupby(["drive.id"], group_keys = False)
        self.plays_json["drive_start"] = np.where(
            self.plays_json["start.pos_team.id"] == self.plays_json["homeTeamId"],
            100 - self.plays_json["drive.start.yardLine"],
            self.plays_json["drive.start.yardLine"],
        )
        self.plays_json["drive_stopped"] = np.select([
            self.plays_json['drive.result'].isna()
        ],
        [
            False
        ],
        default = self.plays_json["drive.result"].str.lower().str.contains(
            "punt|fumble|interception|downs", regex=True, case=False
        ))
        self.plays_json["drive_start"] = self.plays_json["drive_start"].astype(float)
        self.plays_json["drive_play_index"] = base_groups["scrimmage_play"].apply(
            lambda x: x.cumsum()
        )
        self.plays_json["drive_offense_plays"] = np.where(
            (self.plays_json["sp"] == False) & (self.plays_json["scrimmage_play"] == True),
            self.plays_json["play"].astype(int),
            0,
        )
        self.plays_json["prog_drive_EPA"] = base_groups["EPA_scrimmage"].apply(
            lambda x: x.cumsum()
        )
        self.plays_json["prog_drive_WPA"] = base_groups["wpa"].apply(lambda x: x.cumsum())
        self.plays_json["drive_offense_yards"] = np.where(
            (self.plays_json["sp"] == False) & (self.plays_json["scrimmage_play"] == True),
            self.plays_json["statYardage"],
            0,
        )
        self.plays_json["drive_total_yards"] = self.plays_json.groupby(["drive.id"], group_keys = False)[
            "drive_offense_yards"
        ].apply(lambda x: x.cumsum())
        return self.plays_json

    def __cast_box_score_column(self, column, target_type):
        if (column in self.plays_json.columns):
            self.plays_json[column] = self.plays_json[column].astype(target_type)
        else:
            self.plays_json[column] = np.NaN

    def create_box_score(self):
        # have to run the pipeline before pulling this in
        if (self.ran_pipeline == False):
            self.run_processing_pipeline()

        box_score_columns = [
            'completion',
            'target',
            'yds_receiving',
            'yds_rushed',
            'rush',
            'rush_td',
            'pass',
            'pass_td',
            'EPA',
            'wpa',
            'int',
            'int_td',
            'def_EPA',
            'EPA_rush',
            'EPA_pass',
            'EPA_success',
            'EPA_success_pass',
            'EPA_success_rush',
            'EPA_success_standard_down',
            'EPA_success_passing_down',
            'middle_8',
            'rz_play',
            'scoring_opp',
            'stuffed_run',
            'stopped_run',
            'opportunity_run',
            'highlight_run',
            'short_rush_success',
            'short_rush_attempt',
            'power_rush_success',
            'power_rush_attempt',
            'EPA_explosive',
            'EPA_explosive_pass',
            'EPA_explosive_rush',
            'standard_down',
            'passing_down',
            'fumble_vec',
            'sack',
            'penalty_flag',
            'play',
            'scrimmage_play',
            'sp',
            'kickoff_play',
            'punt',
            'fg_attempt',
            'EPA_penalty',
            'EPA_sp',
            'EPA_fg',
            'EPA_punt',
            'EPA_kickoff',
            'TFL',
            'TFL_pass',
            'TFL_rush',
            'havoc',
        ]
        for item in box_score_columns:
            self.__cast_box_score_column(item, float)

        pass_box = self.plays_json[(self.plays_json["pass"] == True) & (self.plays_json.scrimmage_play == True)]
        rush_box = self.plays_json[(self.plays_json["rush"] == True) & (self.plays_json.scrimmage_play == True)]
        # pass_box.yds_receiving.fillna(0.0, inplace=True)
        passer_box = pass_box[(pass_box["pass"] == True) & (pass_box["scrimmage_play"] == True)].fillna(0.0).groupby(by=["pos_team","passer_player_name"], as_index=False, group_keys = False).agg(
            Comp = ('completion', "sum"),
            Att = ('pass_attempt',"sum"),
            Yds = ('yds_receiving',"sum"),
            Pass_TD = ('pass_td', "sum"),
            Int = ('int', "sum"),
            YPA = ('yds_receiving', "mean"),
            EPA = ('EPA', "sum"),
            EPA_per_Play = ('EPA', "mean"),
            WPA = ('wpa', "sum"),
            SR = ('EPA_success', "mean"),
            Sck = ('sack_vec', "sum")
        ).round(2)
        passer_box = passer_box.replace({np.nan: None})
        qbs_list = passer_box.passer_player_name.to_list()

        def weighted_mean(s, df, wcol):
            s = s[s.notna() == True]
            # self.logger.info(s)
            if (len(s) == 0):
                return 0
            return np.average(s, weights=df.loc[s.index, wcol])

        pass_qbr_box = self.plays_json[(self.plays_json.athlete_name.notna() == True) & (self.plays_json.scrimmage_play == True) & (self.plays_json.athlete_name.isin(qbs_list))]
        pass_qbr = pass_qbr_box.groupby(by=["pos_team","athlete_name"], as_index=False, group_keys = False).agg(
            qbr_epa = ('qbr_epa', partial(weighted_mean, df=pass_qbr_box, wcol='weight')),
            sack_epa = ('sack_epa', partial(weighted_mean, df=pass_qbr_box, wcol='sack_weight')),
            pass_epa = ('pass_epa', partial(weighted_mean, df=pass_qbr_box, wcol='pass_weight')),
            rush_epa = ('rush_epa', partial(weighted_mean, df=pass_qbr_box, wcol='rush_weight')),
            pen_epa = ('pen_epa', partial(weighted_mean, df=pass_qbr_box, wcol='pen_weight')),
            spread = ('start.pos_team_spread', lambda x: x.iloc[0])
        )
        # self.logger.info(pass_qbr)

        dtest_qbr = DMatrix(pass_qbr[qbr_vars])
        qbr_result = qbr_model.predict(dtest_qbr)
        pass_qbr["exp_qbr"] = qbr_result
        passer_box = pd.merge(passer_box, pass_qbr, left_on=["passer_player_name","pos_team"], right_on=["athlete_name","pos_team"])

        rusher_box = rush_box.fillna(0.0).groupby(by=["pos_team","rusher_player_name"], as_index=False, group_keys = False).agg(
            Car= ('rush', "sum"),
            Yds= ('yds_rushed',"sum"),
            Rush_TD = ('rush_td',"sum"),
            YPC= ('yds_rushed', "mean"),
            EPA= ('EPA', "sum"),
            EPA_per_Play= ('EPA', "mean"),
            WPA= ('wpa', "sum"),
            SR = ('EPA_success', "mean"),
            Fum = ('fumble_vec', "sum"),
            Fum_Lost = ('fumble_lost', "sum")
        ).round(2)
        rusher_box = rusher_box.replace({np.nan: None})

        receiver_box = pass_box.groupby(by=["pos_team","receiver_player_name"], as_index=False, group_keys = False).agg(
            Rec= ('completion', "sum"),
            Tar= ('target',"sum"),
            Yds= ('yds_receiving',"sum"),
            Rec_TD = ('pass_td', "sum"),
            YPT= ('yds_receiving', "mean"),
            EPA= ('EPA', "sum"),
            EPA_per_Play= ('EPA', "mean"),
            WPA= ('wpa', "sum"),
            SR = ('EPA_success', "mean"),
            Fum = ('fumble_vec', "sum"),
            Fum_Lost = ('fumble_lost', "sum")
        ).round(2)
        receiver_box = receiver_box.replace({np.nan: None})

        team_base_box = self.plays_json.groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_plays = ('play', "sum"),
            total_yards = ('statYardage', "sum"),
            EPA_overall_total = ('EPA', "sum"),
        ).round(2)

        team_pen_box = self.plays_json[(self.plays_json.penalty_flag == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            total_pen_yards = ('statYardage', "sum"),
            EPA_penalty = ('EPA_penalty', "sum"),
        ).round(2)

        team_scrimmage_box = self.plays_json[(self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            scrimmage_plays = ('scrimmage_play', "sum"),
            EPA_overall_off = ('EPA', "sum"),
            EPA_overall_offense = ('EPA', "sum"),
            EPA_per_play = ('EPA', "mean"),
            EPA_non_explosive = ('EPA_non_explosive', "sum"),
            EPA_non_explosive_per_play = ('EPA_non_explosive', "mean"),
            EPA_explosive = ('EPA_explosive', "sum"),
            EPA_explosive_rate = ('EPA_explosive', "mean"),
            passes_rate = ('pass', "mean"),
            off_yards = ('statYardage', "sum"),
            total_off_yards = ('statYardage', "sum"),
            yards_per_play = ('statYardage', "mean")
        ).round(2)

        team_sp_box = self.plays_json[(self.plays_json.sp == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            special_teams_plays = ('sp', "sum"),
            EPA_sp = ('EPA_sp', "sum"),
            EPA_special_teams = ('EPA_sp', "sum"),
            EPA_fg = ('EPA_fg', "sum"),
            EPA_punt = ('EPA_punt', "sum"),
            kickoff_plays = ('kickoff_play', "sum"),
            EPA_kickoff = ('EPA_kickoff', "sum")
        ).round(2)

        team_scrimmage_box_pass = self.plays_json[(self.plays_json["pass"] == True) & (self.plays_json["scrimmage_play"] == True)].fillna(0).groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            passes = ('pass', "sum"),
            pass_yards = ('yds_receiving', "sum"),
            yards_per_pass = ('yds_receiving', "mean"),
            EPA_passing_overall = ('EPA', "sum"),
            EPA_passing_per_play = ('EPA', "mean"),
            EPA_explosive_passing = ('EPA_explosive', "sum"),
            EPA_explosive_passing_rate = ('EPA_explosive', "mean"),
            EPA_non_explosive_passing = ('EPA_non_explosive', "sum"),
            EPA_non_explosive_passing_per_play = ('EPA_non_explosive', "mean"),
        ).round(2)

        team_scrimmage_box_rush = self.plays_json[(self.plays_json["rush"] == True) & (self.plays_json["scrimmage_play"] == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_rushing_overall = ('EPA', "sum"),
            EPA_rushing_per_play = ('EPA', "mean"),
            EPA_explosive_rushing = ('EPA_explosive', "sum"),
            EPA_explosive_rushing_rate = ('EPA_explosive', "mean"),
            EPA_non_explosive_rushing = ('EPA_non_explosive', "sum"),
            EPA_non_explosive_rushing_per_play = ('EPA_non_explosive', "mean"),
            rushes = ('rush', "sum"),
            rush_yards = ('yds_rushed', "sum"),
            yards_per_rush = ('yds_rushed', "mean"),
            rushing_power_rate = ('power_rush_attempt', "mean"),
        ).round(2)

        team_rush_base_box = self.plays_json[(self.plays_json["scrimmage_play"] == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            rushes_rate = ('rush', "mean"),
            first_downs_created = ('first_down_created', "sum"),
            first_downs_created_rate = ('first_down_created', "mean")
        )
        team_rush_power_box = self.plays_json[(self.plays_json["power_rush_attempt"] == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_rushing_power = ('EPA', "sum"),
            EPA_rushing_power_per_play = ('EPA', "mean"),
            rushing_power_success = ('power_rush_success', "sum"),
            rushing_power_success_rate = ('power_rush_success', "mean"),
            rushing_power = ('power_rush_attempt', "sum"),
        )

        self.plays_json.opp_highlight_yards = self.plays_json.opp_highlight_yards.astype(float)
        self.plays_json.highlight_yards = self.plays_json.highlight_yards.astype(float)
        self.plays_json.line_yards = self.plays_json.line_yards.astype(float)
        self.plays_json.second_level_yards = self.plays_json.second_level_yards.astype(float)
        self.plays_json.open_field_yards = self.plays_json.open_field_yards.astype(float)
        team_rush_box = self.plays_json[(self.plays_json["rush"] == True) & (self.plays_json["scrimmage_play"] == True)].fillna(0).groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            rushing_stuff = ('stuffed_run', "sum"),
            rushing_stuff_rate = ('stuffed_run', "mean"),
            rushing_stopped = ('stopped_run', "sum"),
            rushing_stopped_rate = ('stopped_run', "mean"),
            rushing_opportunity = ('opportunity_run', "sum"),
            rushing_opportunity_rate = ('opportunity_run', "mean"),
            rushing_highlight = ('highlight_run', "sum"),
            rushing_highlight_rate = ('highlight_run', "mean"),
            rushing_highlight_yards = ('highlight_yards', "sum"),
            line_yards = ('line_yards', "sum"),
            line_yards_per_carry = ('line_yards', "mean"),
            second_level_yards = ('second_level_yards', "sum"),
            open_field_yards = ('open_field_yards', "sum")
        ).round(2)

        team_rush_opp_box = self.plays_json[(self.plays_json["rush"] == True) & (self.plays_json["scrimmage_play"] == True) & (self.plays_json.opportunity_run == True)].fillna(0).groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            rushing_highlight_yards_per_opp = ('opp_highlight_yards', "mean"),
        ).round(2)

        team_data_frames = [team_rush_opp_box, team_pen_box, team_sp_box, team_scrimmage_box_rush, team_scrimmage_box_pass, team_scrimmage_box, team_base_box, team_rush_base_box, team_rush_power_box, team_rush_box]
        team_box = reduce(lambda left,right: pd.merge(left,right,on=['pos_team'], how='outer'), team_data_frames)
        team_box = team_box.replace({np.nan:None})

        situation_box_normal = self.plays_json[(self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success = ('EPA_success', "sum"),
            EPA_success_rate = ('EPA_success', "mean"),
        )

        situation_box_rz = self.plays_json[(self.plays_json.rz_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_rz = ('EPA_success', "sum"),
            EPA_success_rate_rz = ('EPA_success', "mean"),
        )

        situation_box_third = self.plays_json[(self.plays_json["start.down"] == 3)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_third = ('EPA_success', "sum"),
            EPA_success_rate_third = ('EPA_success', "mean"),
        )

        situation_box_pass = self.plays_json[(self.plays_json["pass"] == True) & (self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_pass = ('EPA_success', "sum"),
            EPA_success_pass_rate = ('EPA_success', "mean"),
        )

        situation_box_rush = self.plays_json[(self.plays_json["rush"] == True) & (self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_rush = ('EPA_success', "sum"),
            EPA_success_rush_rate = ('EPA_success', "mean"),
        )

        situation_box_middle8 = self.plays_json[(self.plays_json["middle_8"] == True) & (self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            middle_8 = ('middle_8', "sum"),
            middle_8_pass_rate = ('pass', "mean"),
            middle_8_rush_rate = ('rush', "mean"),
            EPA_middle_8 = ('EPA', "sum"),
            EPA_middle_8_per_play = ('EPA', "mean"),
            EPA_middle_8_success = ('EPA_success', "sum"),
            EPA_middle_8_success_rate = ('EPA_success', "mean"),
        )

        situation_box_middle8_pass = self.plays_json[(self.plays_json["pass"] == True) & (self.plays_json["middle_8"] == True) & (self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            middle_8_pass = ('pass', "sum"),
            EPA_middle_8_pass = ('EPA', "sum"),
            EPA_middle_8_pass_per_play = ('EPA', "mean"),
            EPA_middle_8_success_pass = ('EPA_success', "sum"),
            EPA_middle_8_success_pass_rate = ('EPA_success', "mean"),
        )

        situation_box_middle8_rush = self.plays_json[(self.plays_json["rush"] == True) & (self.plays_json["middle_8"] == True) & (self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            middle_8_rush = ('rush', "sum"),

            EPA_middle_8_rush = ('EPA', "sum"),
            EPA_middle_8_rush_per_play = ('EPA', "mean"),

            EPA_middle_8_success_rush = ('EPA_success', "sum"),
            EPA_middle_8_success_rush_rate = ('EPA_success', "mean"),
        )

        situation_box_early = self.plays_json[(self.plays_json.early_down == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_early_down = ('EPA_success', "sum"),
            EPA_success_early_down_rate = ('EPA_success', "mean"),
            early_downs = ('early_down', "sum"),
            early_down_pass_rate = ('pass', "mean"),
            early_down_rush_rate = ('rush', "mean"),
            EPA_early_down = ('EPA', "sum"),
            EPA_early_down_per_play = ('EPA', "mean"),
            early_down_first_down = ('first_down_created', "sum"),
            early_down_first_down_rate = ('first_down_created', "mean")
        )

        situation_box_early_pass = self.plays_json[(self.plays_json["pass"] == True) & (self.plays_json.early_down == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            early_down_pass = ('pass', "sum"),
            EPA_early_down_pass = ('EPA', "sum"),
            EPA_early_down_pass_per_play = ('EPA', "mean"),
            EPA_success_early_down_pass = ('EPA_success', "sum"),
            EPA_success_early_down_pass_rate = ('EPA_success', "mean"),
        )

        situation_box_early_rush = self.plays_json[(self.plays_json["rush"] == True) & (self.plays_json.early_down == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            early_down_rush = ('rush', "sum"),
            EPA_early_down_rush = ('EPA', "sum"),
            EPA_early_down_rush_per_play = ('EPA', "mean"),
            EPA_success_early_down_rush = ('EPA_success', "sum"),
            EPA_success_early_down_rush_rate = ('EPA_success', "mean"),
        )

        situation_box_late = self.plays_json[(self.plays_json.late_down == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_late_down = ('EPA_success_late_down', "sum"),
            EPA_success_late_down_pass = ('EPA_success_late_down_pass', "sum"),
            EPA_success_late_down_rush = ('EPA_success_late_down_rush', "sum"),
            late_downs = ('late_down', "sum"),
            late_down_pass = ('late_down_pass', "sum"),
            late_down_rush = ('late_down_rush', "sum"),
            EPA_late_down = ('EPA', "sum"),
            EPA_late_down_per_play = ('EPA', "mean"),
            EPA_success_late_down_rate = ('EPA_success_late_down', "mean"),
            EPA_success_late_down_pass_rate = ('EPA_success_late_down_pass', "mean"),
            EPA_success_late_down_rush_rate = ('EPA_success_late_down_rush', "mean"),
            late_down_pass_rate = ('late_down_pass', "mean"),
            late_down_rush_rate = ('late_down_rush', "mean")
        )

        situation_box_standard = self.plays_json[self.plays_json.standard_down == True].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_standard_down = ('EPA_success_standard_down', "sum"),
            EPA_success_standard_down_rate = ('EPA_success_standard_down', "mean"),
            EPA_standard_down = ('EPA_success_standard_down', "sum"),
            EPA_standard_down_per_play = ('EPA_success_standard_down', "mean")
        )
        situation_box_passing = self.plays_json[self.plays_json.passing_down == True].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            EPA_success_passing_down = ('EPA_success_passing_down', "sum"),
            EPA_success_passing_down_rate = ('EPA_success_passing_down', "mean"),
            EPA_passing_down = ('EPA_success_standard_down', "sum"),
            EPA_passing_down_per_play = ('EPA_success_standard_down', "mean")
        )
        situation_data_frames = [situation_box_normal, situation_box_pass, situation_box_rush, situation_box_rz, situation_box_third, situation_box_early, situation_box_early_pass, situation_box_early_rush, situation_box_middle8, situation_box_middle8_pass, situation_box_middle8_rush, situation_box_late, situation_box_standard, situation_box_passing]
        situation_box = reduce(lambda left,right: pd.merge(left,right,on=['pos_team'], how='outer'), situation_data_frames)
        situation_box = situation_box.replace({np.nan:None})

        self.plays_json.drive_stopped = self.plays_json.drive_stopped.astype(float)
        def_base_box = self.plays_json[(self.plays_json.scrimmage_play == True)].groupby(by=["def_pos_team"], as_index=False, group_keys = False).agg(
            scrimmage_plays = ('scrimmage_play', "sum"),
            TFL = ('TFL', "sum"),
            TFL_pass = ('TFL_pass', "sum"),
            TFL_rush = ('TFL_rush', "sum"),
            havoc_total = ('havoc', "sum"),
            havoc_total_rate = ('havoc', "mean"),
            fumbles = ('forced_fumble', "sum"),
            def_int = ('int', "sum"),
            drive_stopped_rate = ('drive_stopped', "mean")
        )
        def_base_box.drive_stopped_rate = 100 * def_base_box.drive_stopped_rate
        def_base_box = def_base_box.replace({np.nan:None})

        def_box_havoc_pass = self.plays_json[(self.plays_json.scrimmage_play == True) & (self.plays_json["pass"] == True)].groupby(by=["def_pos_team"], as_index=False, group_keys = False).agg(
            num_pass_plays = ('pass', "sum"),
            havoc_total_pass = ('havoc', "sum"),
            havoc_total_pass_rate = ('havoc', "mean"),
            sacks = ('sack_vec', "sum"),
            sacks_rate = ('sack_vec', "mean"),
            pass_breakups = ('pass_breakup', "sum")
        )
        def_box_havoc_pass = def_box_havoc_pass.replace({np.nan:None})

        def_box_havoc_rush = self.plays_json[(self.plays_json.scrimmage_play == True) & (self.plays_json["rush"] == True)].groupby(by=["def_pos_team"], as_index=False, group_keys = False).agg(
            havoc_total_rush = ('havoc', "sum"),
            havoc_total_rush_rate = ('havoc', "mean"),
        )
        def_box_havoc_rush = def_box_havoc_rush.replace({np.nan:None})

        def_data_frames = [def_base_box,def_box_havoc_pass,def_box_havoc_rush]
        def_box = reduce(lambda left,right: pd.merge(left,right,on=['def_pos_team'], how='outer'), def_data_frames)
        def_box = def_box.replace({np.nan:None})
        def_box_json = json.loads(def_box.to_json(orient="records"))

        turnover_box = self.plays_json[(self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            pass_breakups = ('pass_breakup', "sum"),
            fumbles_lost = ('fumble_lost', "sum"),
            fumbles_recovered = ('fumble_recovered', "sum"),
            total_fumbles = ('fumble_vec', "sum"),
            Int = ('int', "sum"),
        ).round(2)
        turnover_box = turnover_box.replace({np.nan:None})
        turnover_box_json = json.loads(turnover_box.to_json(orient="records"))

        if (len(turnover_box_json) < 2):
            for i in range(len(turnover_box_json), 2):
                turnover_box_json.append({})

        turnover_box_json[0]["Int"] = int(turnover_box_json[0].get("Int", 0))
        turnover_box_json[1]["Int"] = int(turnover_box_json[1].get("Int", 0))

        away_passes_def = turnover_box_json[0].get("pass_breakups", 0)
        away_passes_int = turnover_box_json[0].get("Int", 0)
        away_fumbles = turnover_box_json[0].get('total_fumbles', 0)
        turnover_box_json[0]["expected_turnovers"] = (0.5 * away_fumbles) + (0.22 * (away_passes_def + away_passes_int))

        home_passes_def = turnover_box_json[1].get("pass_breakups", 0)
        home_passes_int = turnover_box_json[1].get("Int", 0)
        home_fumbles = turnover_box_json[1].get('total_fumbles', 0)
        turnover_box_json[1]["expected_turnovers"] = (0.5 * home_fumbles) + (0.22 * (home_passes_def + home_passes_int))

        turnover_box_json[0]["expected_turnover_margin"] = turnover_box_json[1]["expected_turnovers"] - turnover_box_json[0]["expected_turnovers"]
        turnover_box_json[1]["expected_turnover_margin"] = turnover_box_json[0]["expected_turnovers"] - turnover_box_json[1]["expected_turnovers"]

        away_to = turnover_box_json[0].get("fumbles_lost", 0) + turnover_box_json[0]["Int"]
        home_to = turnover_box_json[1].get("fumbles_lost", 0) + turnover_box_json[1]["Int"]

        turnover_box_json[0]["turnovers"] = away_to
        turnover_box_json[1]["turnovers"] = home_to

        turnover_box_json[0]["turnover_margin"] = home_to - away_to
        turnover_box_json[1]["turnover_margin"] = away_to - home_to

        turnover_box_json[0]["turnover_luck"] = 5.0 * (turnover_box_json[0]["turnover_margin"] - turnover_box_json[0]["expected_turnover_margin"])
        turnover_box_json[1]["turnover_luck"] = 5.0 * (turnover_box_json[1]["turnover_margin"] - turnover_box_json[1]["expected_turnover_margin"])

        self.plays_json.drive_start = self.plays_json.drive_start.astype(float)
        drives_data = self.plays_json[(self.plays_json.scrimmage_play == True)].groupby(by=["pos_team"], as_index=False, group_keys = False).agg(
            drive_total_available_yards = ('drive_start', "sum"),
            drive_total_gained_yards = ('drive.yards', "sum"),
            avg_field_position = ('drive_start', "mean"),
            plays_per_drive = ('drive.offensivePlays', "mean"),
            yards_per_drive = ('drive.yards', "mean"),
            drives = ('drive.id', pd.Series.nunique)
        )
        drives_data['drive_total_gained_yards_rate'] = (100 * drives_data.drive_total_gained_yards / drives_data.drive_total_available_yards).round(2)

        return {
            "pass" : json.loads(passer_box.to_json(orient="records")),
            "rush" : json.loads(rusher_box.to_json(orient="records")),
            "receiver" : json.loads(receiver_box.to_json(orient="records")),
            "team" : json.loads(team_box.to_json(orient="records")),
            "situational" : json.loads(situation_box.to_json(orient="records")),
            "defensive" : def_box_json,
            "turnover" : turnover_box_json,
            "drives" : json.loads(drives_data.to_json(orient="records"))
        }

    def run_processing_pipeline(self):
        if self.ran_pipeline == False:
            pbp_txt = self.__helper_cfb_pbp_drives(self.json)
            pbp_txt['plays']['week'] = pbp_txt['header']['week']
            self.plays_json = pbp_txt['plays']

            pbp_json = {
                "gameId": self.gameId,
                "plays": np.array(self.plays_json).tolist(),
                "season": pbp_txt["season"],
                "week": pbp_txt['header']['week'],
                "gameInfo": pbp_txt["gameInfo"],
                "teamInfo": pbp_txt["header"]["competitions"][0],
                "playByPlaySource": pbp_txt.get('header').get('competitions')[0].get('playByPlaySource'),
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
            self.plays_json = pd.DataFrame(pbp_txt['plays'].to_dict(orient="records"))
            if pbp_json.get('header').get('competitions')[0].get('playByPlaySource') != 'none':
                self.__add_downs_data()
                self.__add_play_type_flags()
                self.__add_rush_pass_flags()
                self.__add_team_score_variables()
                self.__add_new_play_types()
                self.__setup_penalty_data()
                self.__add_play_category_flags()
                self.__add_yardage_cols()
                self.__add_player_cols()
                self.__after_cols()
                self.__add_spread_time()
                self.__process_epa()
                self.__process_wpa()
                self.__add_drive_data()
                self.__process_qbr()
                self.plays_json.replace({np.nan: None}, inplace = True)
                pbp_json = {
                    "gameId": self.gameId,
                    "plays": self.plays_json.to_dict(orient="records"),
                    "season": pbp_txt["season"],
                    "week": pbp_txt['header']['week'],
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
            self.ran_pipeline = True
            return pbp_json

    def run_cleaning_pipeline(self):
        if self.ran_cleaning_pipeline == False:
            pbp_txt = self.__helper_cfb_pbp_drives(self.json)
            pbp_txt['plays']['week'] = pbp_txt['header']['week']
            self.plays_json = pbp_txt['plays']

            pbp_json = {
                "gameId": self.gameId,
                "plays": np.array().tolist(),
                "season": pbp_txt["season"],
                "week": pbp_txt['header']['week'],
                "gameInfo": pbp_txt["gameInfo"],
                "teamInfo": pbp_txt["header"]["competitions"][0],
                "playByPlaySource": pbp_txt.get('header').get('competitions')[0].get('playByPlaySource'),
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
            self.plays_json = pd.DataFrame(pbp_txt['plays'].to_dict(orient="records"))
            if pbp_json.get('header').get('competitions')[0].get('playByPlaySource') != 'none':
                self.__add_downs_data()
                self.__add_play_type_flags()
                self.__add_rush_pass_flags()
                self.__add_team_score_variables()
                self.__add_new_play_types()
                self.__setup_penalty_data()
                self.__add_play_category_flags()
                self.__add_yardage_cols()
                self.__add_player_cols()
                self.__after_cols()
                self.__add_spread_time()
                self.plays_json.replace({np.nan: None}, inplace = True)
                pbp_json = {
                    "gameId": self.gameId,
                    "plays": self.plays_json.to_dict(orient="records"),
                    "season": pbp_txt["season"],
                    "week": pbp_txt['header']['week'],
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
            return pbp_json