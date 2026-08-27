from __future__ import annotations

import json
import logging
import os
import re
import time
from functools import reduce
from importlib.resources import files as _resource_files

import numpy as np
import pandas as pd
import polars as pl
from xgboost import Booster, DMatrix


def _nfl_resource_filename(package: str, resource: str) -> str:
    """Drop-in replacement for the deprecated ``pkg_resources.resource_filename``.

    Uses :func:`importlib.resources.files` (stdlib, available since Python 3.9)
    and resolves the path eagerly. Mirrors the equivalent helper in cfb_pbp.py.
    """
    return str(_resource_files(package).joinpath(resource))


def _team_mascot(team: dict) -> str:
    """Return a team's mascot, tolerating franchises ESPN ships without one.

    ESPN omits ``team.name`` for mascot-less franchises -- the 2020-2021
    Washington Football Team (id 28) is the canonical case, shipping only
    ``location`` / ``abbreviation`` / ``displayName``. Derive the mascot from
    ``displayName`` with the location prefix removed, then fall back to
    ``displayName`` and finally ``location`` so a team always has a label.
    """
    name = team.get("name")
    if name:
        return str(name)
    display = str(team.get("displayName") or "")
    location = str(team.get("location") or "")
    if display and location and display.startswith(location):
        remainder = display[len(location) :].strip()
        if remainder:
            return remainder
    return display or location


from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.ep_wp import (
    CP_FEATURES,
    EP_FEATURES,
    WP_NAIVE_FEATURES,
    WP_SPREAD_FEATURES,
    _EP_POINT_VALUES,
    _XYAC_OUT_COLS,
    _espn_cp_features,
    _espn_ep_features,
    _espn_wp_features,
    _load_model as _ep_wp_load_model,
    calculate_epa,
    calculate_wpa,
    calculate_xpass,
)
from sportsdataverse.nfl.model_vars import (
    TOUCHBACK_YARDLINE_POST_2016,
    TOUCHBACK_YARDLINE_PRE_2016,
    defense_score_vec,
    end_change_vec,
    ep_class_to_score_mapping,
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
)

# "td" : float(p[0]),
# "opp_td" : float(p[1]),
# "fg" : float(p[2]),
# "opp_fg" : float(p[3]),
# "safety" : float(p[4]),
# "opp_safety" : float(p[5]),
# "no_score" : float(p[6])
qbr_model_file = _nfl_resource_filename("sportsdataverse", "nfl/models/qbr_model.ubj")
qbr_model = Booster({"nthread": 4})
qbr_model.load_model(qbr_model_file)
# ep_model and wp_model are loaded lazily via _ep_wp_load_model() (lru_cache)
# so that import succeeds even when .ubj files are absent.

logger = logging.getLogger("sdv.nfl_pbp")
logger.addHandler(logging.NullHandler())


class NFLPlayProcess(object):
    """Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

    Wraps the ESPN ``summary`` endpoint (or a local JSON dump) and pipes the
    result through a chain of feature-engineering steps -- down/distance,
    play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced
    box score. Use ``run_processing_pipeline()`` for the full feature set
    or ``run_cleaning_pipeline()`` for a lighter clean.

    Args:
        gameId (int): ESPN ``event`` id (e.g. ``401671801``).
        raw (bool): If ``True``, ``espn_nfl_pbp()`` returns the ESPN payload
            untouched. If ``False`` (default), it normalizes keys.
        path_to_json (str): Directory containing ``{gameId}.json`` for the
            ``nfl_pbp_disk()`` flow (offline replay).
        return_keys (list[str] | None): If supplied, ``run_processing_pipeline``
            returns only the listed keys from the result dict.

    Example:
        End-to-end pipeline against the live ESPN endpoint::

            from sportsdataverse.nfl import NFLPlayProcess
            proc = NFLPlayProcess(gameId=401671801)
            proc.espn_nfl_pbp()
            result = proc.run_processing_pipeline()
            len(result["plays"])

        Offline replay from a JSON dump::

            proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
            proc.nfl_pbp_disk()
            cleaned = proc.run_cleaning_pipeline()

        Subset the return payload::

            proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
            proc.espn_nfl_pbp()
            slim = proc.run_processing_pipeline()
            sorted(slim.keys())  # ['boxscore', 'plays']

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflfastR`_ -- R sister package for NFL PBP

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflfastR: https://www.nflfastr.com
    """

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

    def espn_nfl_pbp(self, **kwargs):
        """espn_nfl_pbp() - Pull the game by id. Data from API endpoints: `nfl/playbyplay`, `nfl/summary`

        Args:
            game_id (int): Unique game_id, can be obtained from nfl_schedule().

        Returns:
            Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
             "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder",
             "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP",
             "gameInfo", "season"

        Example:
            Standard normalized payload::

                from sportsdataverse.nfl import NFLPlayProcess
                proc = NFLPlayProcess(gameId=401220403)
                payload = proc.espn_nfl_pbp()
                sorted(payload.keys())[:5]

            Raw ESPN passthrough (no key normalization)::

                proc_raw = NFLPlayProcess(gameId=401220403, raw=True)
                espn_dump = proc_raw.espn_nfl_pbp()

            Chain into the full processing pipeline::

                proc = NFLPlayProcess(gameId=401220403)
                proc.espn_nfl_pbp()
                result = proc.run_processing_pipeline()
        """
        cache_buster = int(time.time() * 1000)
        pbp_txt = {"timeouts": {}}
        # summary endpoint for pickcenter array
        summary_url = (
            f"http://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={self.gameId}&{cache_buster}"
        )
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
            logging.debug(f"{self.gameId}: raw nfl_pbp data requested, returning keys: {summary.keys()}")
            # reorder keys in raw format, appending empty keys which are defined later to the end
            pbp_json = {}
            for k in incoming_keys_expected:
                if k in summary.keys():
                    pbp_json[k] = summary[k]
                else:
                    pbp_json[k] = {} if k in dict_keys_expected else []
            return pbp_json

        logging.debug(f"{self.gameId}: full nfl_pbp data requested, returning keys: {summary.keys()}")
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

    def nfl_pbp_disk(self):
        """Load a previously-saved ESPN payload from ``{path_to_json}/{gameId}.json``.

        Use this to replay an old game offline without hitting the ESPN
        endpoint -- handy for snapshot-driven tests and reproducible
        feature engineering.

        Returns:
            Dict: The parsed JSON content; also stored on ``self.json``.

        Example:
            Replay a dump on disk::

                from sportsdataverse.nfl import NFLPlayProcess
                proc = NFLPlayProcess(gameId=401220403, path_to_json="./pbp_dump")
                proc.nfl_pbp_disk()
                result = proc.run_processing_pipeline()
        """
        with open(os.path.join(self.path_to_json, f"{self.gameId}.json")) as json_file:
            pbp_txt = json.load(json_file)
            self.json = pbp_txt
        return self.json

    def nfl_pbp_json(self, **kwargs):
        """Set ``self.json`` to the imported ``json`` module reference (legacy stub).

        Retained for API compatibility. Prefer ``espn_nfl_pbp()`` (live)
        or ``nfl_pbp_disk()`` (offline) to populate ``self.json`` with an
        actual ESPN payload.

        Returns:
            module: The Python ``json`` module reference (mirrors legacy behavior).

        Example:
            Stub usage (rarely needed -- prefer the live or disk loaders)::

                from sportsdataverse.nfl import NFLPlayProcess
                proc = NFLPlayProcess(gameId=401220403)
                proc.nfl_pbp_json()  # populates `self.json` with the json module
        """
        self.json = json
        return self.json

    def __helper_nfl_pbp_drives(self, pbp_txt):
        pbp_txt, init = self.__helper_nfl_pbp(pbp_txt)

        pbp_txt["plays"] = pl.DataFrame()
        # negotiating the drive meta keys into columns after unnesting drive plays
        # concatenating the previous and current drives categories when necessary
        if (
            "drives" in pbp_txt.keys()
            and pbp_txt.get("header").get("competitions")[0].get("playByPlaySource") != "none"
        ):
            pbp_txt = self.__helper_nfl_pbp_features(pbp_txt, init)
        else:
            pbp_txt["drives"] = {}
        return pbp_txt

    def __helper_nfl_pbp_features(self, pbp_txt, init):
        pbp_txt["plays"] = pd.DataFrame()
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
            pbp_txt["plays"] = pd.concat([pbp_txt["plays"], prev_drives], axis=0, ignore_index=True)
        pbp_txt["plays"] = pl.from_pandas(pbp_txt["plays"])
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
                # Defensive cast: ESPN sometimes returns this as a python float (no .astype()), sometimes as numpy. Same shape fix as the cfb_pbp version.
                gameSpread=pl.lit(float(np.asarray(init["gameSpread"]).reshape(-1)[0])).abs().first(),
                homeFavorite=pl.lit(bool(np.asarray(init["homeFavorite"]).reshape(-1)[0])).first(),
                gameSpreadAvailable=pl.lit(init["gameSpreadAvailable"]),
                # Defensive cast: ESPN sometimes returns this as a python float (no .astype()), sometimes as numpy. Same shape fix as the cfb_pbp version.
                overUnder=pl.lit(float(np.asarray(init["overUnder"]).reshape(-1)[0])).first(),
            )
            .with_columns(
                homeTeamSpread=pl.when(pl.col("homeFavorite") == True)
                .then(pl.col("gameSpread"))
                .otherwise(-1 * pl.col("gameSpread")),
            )
            .with_columns(
                pl.col("period.number").cast(pl.Int32),
                # Clock is always "MM:SS" → exactly 2 fields. Polars 1.x deprecated
                # `n_field_strategy` (it has no effect when `upper_bound` is set);
                # `upper_bound=2` alone is the modern, warning-free signature.
                pl.col("clock.displayValue").str.split(":").list.to_struct(upper_bound=2).alias("clock.mm"),
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
                    .and_(pl.col("type.text") != "Timeout"),
                )
                .then(pl.lit(True))
                .when(
                    (pl.col("start.team.id") == pl.col("lead_start_team"))
                    .and_(pl.col("start.down") == pl.col("lead_start_down"))
                    .and_(pl.col("start.yardsToEndzone") == pl.col("lead_start_yardsToEndzone"))
                    .and_(pl.col("start.distance") == pl.col("lead_start_distance"))
                    .and_(pl.col("text").is_in(pl.col("lead_text").implode()))
                    .and_(pl.col("type.text") != "Timeout"),
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
            )
        )
        pbp_txt["plays"] = pbp_txt["plays"].filter(pl.col("text_dupe") == False)
        pbp_txt["plays"] = pbp_txt["plays"].with_row_index("game_play_number", 1)
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.col("start.team.id").fill_null(strategy="forward").fill_null(strategy="backward").cast(pl.Int32),
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
                        ),
                    ),
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
                        ),
                    ),
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
                    - pl.struct("id", "period.number").map_elements(
                        lambda x: (
                            (
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
                            )
                        ),
                        return_dtype=pl.Int64,
                    )
                ).alias("end.homeTeamTimeouts"),
                (
                    3
                    - pl.struct("id", "period.number").map_elements(
                        lambda x: (
                            (
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
                            )
                        ),
                        return_dtype=pl.Int64,
                    )
                ).alias("end.awayTeamTimeouts"),
            )
            .with_columns(
                pl.col("end.homeTeamTimeouts").shift(n=1, fill_value=3).alias("start.homeTeamTimeouts"),
                pl.col("end.awayTeamTimeouts").shift(n=1, fill_value=3).alias("start.awayTeamTimeouts"),
                pl.col("start.TimeSecsRem").shift(n=1).alias("end.TimeSecsRem"),
                pl.col("start.adj_TimeSecsRem").shift(n=1).alias("end.adj_TimeSecsRem"),
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
                        pl.col("type.text").is_in(kickoff_vec),
                        pl.col("start.pos_team.id") == pl.col("homeTeamId"),
                    ),
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
                .alias("start.yardsToEndzone"),
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
                    (pl.col("start.distance") == 0).and_(pl.col("start.downDistanceText").str.contains(r"(?i)goal")),
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
                    .alias("type.text"),
                )
                .with_columns(
                    pl.when(pl.col("scoringType.displayName") == "Extra Point")
                    .then(pl.lit("Extra Point Good"))
                    .otherwise(pl.col("type.text"))
                    .alias("type.text"),
                )
            )
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.when(pl.col("type.text").is_null())
                .then(pl.lit("Unknown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)extra point")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)no good")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)extra point")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)blocked")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)field goal")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)blocked")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)field goal")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)no good")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
        )

        return pbp_txt

    def __helper_nfl_pbp(self, pbp_txt):
        # ESPN's summary endpoint intermittently returns a payload with no
        # `header.competitions` (transient gap / a game not yet ingested).
        # Short-circuit with a clear, catchable NoDataError *before* pickcenter
        # resolution (which would otherwise make a fallback odds network hop) and
        # before the deep `KeyError: 'competitions'` in __helper_nfl_game_data.
        # Local import so the error class doesn't leak into the package namespace
        # (it is not a public wrapper) and trip the codegen autodoc/parsed gates.
        from sportsdataverse.errors import NoDataError

        if not ((pbp_txt.get("header") or {}).get("competitions") or []):
            raise NoDataError(
                f"ESPN summary for game {self.gameId} has no header.competitions; cannot build play-by-play.",
            )
        init = self.__helper_nfl_pickcenter(pbp_txt)
        return self.__helper_nfl_game_data(pbp_txt, init)

    def __helper__espn_nfl_odds_information__(self):
        """Fetch pre-game spread/total from ESPN's modern core odds endpoint.

        Returns ``(gameSpread, overUnder, homeFavorite, gameSpreadAvailable)``.
        Mirrors the CFB equivalent — the legacy ``pickcenter`` array on the
        summary endpoint trends toward empty for recent games, so this
        restores the data path via the ``sports.core.api.espn.com`` v2 odds
        collection. Falls back to defaults ``(2.5, 55.5, True, False)`` on
        empty / error / decode failure to preserve pre-existing
        caller-visible behavior.
        """
        cache_buster = int(time.time() * 1000)
        odds_url = (
            f"https://sports.core.api.espn.com/v2/sports/football/leagues/"
            f"nfl/events/{self.gameId}/competitions/{self.gameId}/"
            f"odds?limit=100&{cache_buster}"
        )
        try:
            odds_resp = download(odds_url)
            odds = odds_resp.json()
        except Exception as e:
            logger.warning(
                "%s: odds fetch failed (%r); falling back to defaults",
                self.gameId,
                e,
            )
            return (2.5, 55.5, True, False)

        items = odds.get("items", []) if isinstance(odds, dict) else []
        if not items:
            return (2.5, 55.5, True, False)

        # Prefer ESPN BET when present; the items array is sorted by
        # provider.id, so the first index is provider-dependent. Falling
        # back to items[0] preserves the legacy ordering when no explicit
        # match is found.
        espn_bet = next(
            (
                it
                for it in items
                if isinstance(it, dict) and (it.get("provider") or {}).get("name", "").lower() == "espn bet"
            ),
            items[0],
        )

        spread_raw = espn_bet.get("spread") if isinstance(espn_bet, dict) else None
        ou_raw = espn_bet.get("overUnder") if isinstance(espn_bet, dict) else None
        home_odds = (espn_bet.get("homeTeamOdds") or {}) if isinstance(espn_bet, dict) else {}
        home_fav_raw = home_odds.get("favorite")

        gameSpreadAvailable = spread_raw is not None
        gameSpread = float(spread_raw) if spread_raw is not None else 2.5
        overUnder = float(ou_raw) if ou_raw is not None else 55.5
        homeFavorite = bool(home_fav_raw) if home_fav_raw is not None else True
        return (gameSpread, overUnder, homeFavorite, gameSpreadAvailable)

    def __helper_nfl_pickcenter(self, pbp_txt):
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
            # Cascade: legacy `pickcenter` array empty (true for many recent
            # NFL games on ESPN's summary endpoint). Try the modern core
            # odds endpoint before silently defaulting — otherwise every
            # play inherits `(2.5, 55.0, True)` and corrupts downstream
            # WPA/EP signals.
            (
                gameSpread,
                overUnder,
                homeFavorite,
                gameSpreadAvailable,
            ) = self.__helper__espn_nfl_odds_information__()
        self.gameSpread = gameSpread
        self.overUnder = overUnder
        self.homeFavorite = homeFavorite
        self.gameSpreadAvailable = gameSpreadAvailable
        return {
            "gameSpread": gameSpread,
            "overUnder": overUnder,
            "homeFavorite": homeFavorite,
            "gameSpreadAvailable": gameSpreadAvailable,
        }

    def __helper_nfl_game_data(self, pbp_txt, init):
        pbp_txt["timeouts"] = {}
        pbp_txt["teamInfo"] = pbp_txt["header"]["competitions"][0]
        pbp_txt["season"] = pbp_txt["header"]["season"]
        pbp_txt["playByPlaySource"] = pbp_txt["header"]["competitions"][0]["playByPlaySource"]
        pbp_txt["boxscoreSource"] = pbp_txt["header"]["competitions"][0]["boxscoreSource"]
        pbp_txt["gameSpreadAvailable"] = init["gameSpreadAvailable"]
        pbp_txt["gameSpread"] = init["gameSpread"]
        pbp_txt["homeFavorite"] = init["homeFavorite"]
        pbp_txt["homeTeamSpread"] = np.where(
            init["homeFavorite"] == True,
            abs(init["gameSpread"]),
            -1 * abs(init["gameSpread"]),
        )
        pbp_txt["overUnder"] = init["overUnder"]
        # Home and Away identification variables
        if pbp_txt["header"]["competitions"][0]["competitors"][0]["homeAway"] == "home":
            pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][0][
                "team"
            ]
            homeTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
            homeTeamMascot = _team_mascot(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"])
            homeTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
            homeTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
            homeTeamNameAlt = re.sub("Stat(.+)", "St", homeTeamName)
            pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][1][
                "team"
            ]
            awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["id"])
            awayTeamMascot = _team_mascot(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"])
            awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["location"])
            awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["abbreviation"])
            awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
        else:
            pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][0][
                "team"
            ]
            awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
            awayTeamMascot = _team_mascot(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"])
            awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
            awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
            awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
            pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][1][
                "team"
            ]
            homeTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["id"])
            homeTeamMascot = _team_mascot(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"])
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
        self.homeTeamId = homeTeamId
        self.homeTeamMascot = homeTeamMascot
        self.homeTeamName = homeTeamName
        self.homeTeamAbbrev = homeTeamAbbrev
        self.homeTeamNameAlt = homeTeamNameAlt
        self.awayTeamId = awayTeamId
        self.awayTeamMascot = awayTeamMascot
        self.awayTeamName = awayTeamName
        self.awayTeamAbbrev = awayTeamAbbrev
        self.awayTeamNameAlt = awayTeamNameAlt
        return pbp_txt, init

    def __add_downs_data(self, play_df):
        """
        Creates the following columns in play_df:
            * id, drive_id, game_id
            * down, ydstogo (distance), game_half, period
        """
        play_df = play_df.sort(by=["id", "start.adj_TimeSecsRem"])

        play_df = play_df.unique(
            subset=["text", "id", "type.text", "start.down", "sequenceNumber"],
            keep="last",
            maintain_order=True,
        )
        play_df = play_df.filter(
            pl.col("type.text").str.contains("(?i)end of|(?i)coin toss|(?i)end period|(?i)wins toss") == False,
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
                        pl.col("type.text") == "Rush",
                        pl.col("start.pos_team.id") != pl.col("end.pos_team.id"),
                    ),
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)fumble")).and_(
                        pl.col("type.text") == "Sack",
                        pl.col("start.pos_team.id") != pl.col("end.pos_team.id"),
                    ),
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
                        pl.col("kickoff_play") == True,
                    ),
                )
                .then(True)
                .otherwise(False),
                kickoff_oob=pl.when(
                    (pl.col("text").str.contains("(?i)out-of-bounds|(?i)out of bounds")).and_(
                        pl.col("kickoff_play") == True,
                    ),
                )
                .then(True)
                .otherwise(False),
                kickoff_fair_catch=pl.when(
                    (pl.col("text").str.contains("(?i)fair catch|(?i)fair caught")).and_(
                        pl.col("kickoff_play") == True,
                    ),
                )
                .then(True)
                .otherwise(False),
                kickoff_downed=pl.when((pl.col("text").str.contains("(?i)downed")).and_(pl.col("kickoff_play") == True))
                .then(True)
                .otherwise(False),
                kick_play=pl.col("text").str.contains("(?i)kick|(?i)kickoff"),
                kickoff_safety=pl.when(
                    (pl.col("text").str.contains("(?i)kickoff")).and_(
                        pl.col("safety") == True,
                        pl.col("type.text").is_in(["Blocked Punt", "Penalty"]) == False,
                    ),
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
                    (pl.col("text").str.contains("(?i)out-of-bounds|(?i)out of bounds")).and_(pl.col("punt") == True),
                )
                .then(True)
                .otherwise(False),
                punt_fair_catch=pl.when(
                    (pl.col("text").str.contains("(?i)fair catch|(?i)fair caught")).and_(pl.col("punt") == True),
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
                                ],
                            )
                        ).and_(pl.col("text").str.contains("run for")),
                    ),
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
                            ],
                        )
                    )
                    .or_((pl.col("type.text") == "Safety").and_(pl.col("text").str.contains("sacked")))
                    .or_((pl.col("type.text") == "Safety").and_(pl.col("text").str.contains("pass complete")))
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own)").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted"),
                        ),
                    )
                    .or_((pl.col("type.text") == "Fumble Recovery (Own)").and_(pl.col("text").str.contains("sacked")))
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own) Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own) Touchdown").and_(
                            pl.col("text").str.contains("sacked"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent)").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent)").and_(
                            pl.col("text").str.contains("sacked"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent) Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Return Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Return Touchdown").and_(pl.col("text").str.contains("sacked")),
                    ),
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
                                ],
                            )
                        ).and_(pl.col("text").str.contains("(?i)sacked"), pl.col("pass") == True),
                    ),
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
                    & (pl.col("H_score_diff") >= 9),
                )
                .then(pl.col("lag_homeScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") < 9)
                    & (pl.col("H_score_diff") > 1),
                )
                .then(pl.col("lag_homeScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") >= -9)
                    & (pl.col("H_score_diff") < -1),
                )
                .then(pl.col("homeScore"))
                .otherwise(pl.col("homeScore")),
                awayScore=pl.when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") >= 9),
                )
                .then(pl.col("lag_awayScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") < 9)
                    & (pl.col("A_score_diff") > 1),
                )
                .then(pl.col("lag_awayScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") >= -9)
                    & (pl.col("A_score_diff") < -1),
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
                    .and_(pl.col("type.text").is_in(defense_score_vec) == False),
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
                    .and_(pl.col("td_play") == True),
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
                    .and_(pl.col("type.text").is_in(defense_score_vec) == False),
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
                    .and_(pl.col("td_play") == True),
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
                    .and_(pl.col("td_check") == True),
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
                    .and_(pl.col("td_check") == True),
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
                    .and_(pl.col("td_check") == True),
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
                    .and_(pl.col("type.text").is_in(int_vec) == False),
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
                    .and_(pl.col("type.text").is_in(int_vec) == False),
                )
                .then(pl.lit("Passing Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Blocked Field Goal"])).and_(
                        pl.col("text").str.contains("(?i)for a TD"),
                    ),
                )
                .then(pl.lit("Blocked Field Goal Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when((pl.col("type.text").is_in(["Blocked Punt"])).and_(pl.col("text").str.contains("(?i)for a TD")))
                .then(pl.lit("Blocked Punt Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix duplicated TD play_type labels----
                pl.col("type.text").str.replace(r"(?i)Touchdown Touchdown", "Touchdown").alias("type.text"),
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
                    .and_(pl.col("text").str.contains("(?i)TD")),
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
                        pl.col("text").str.contains("(?i)pass intercepted for a TD"),
                    ),
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
                    .and_(pl.col("fumble_vec") == False),
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .when(
                    (pl.col("type.text") == "Kickoff")
                    .and_(pl.col("text").str.contains("(?i)for a TD"))
                    .and_(pl.col("fumble_vec") == False),
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Kickoff", "Kickoff Return (Offense)"]))
                    .and_(pl.col("fumble_vec") == True)
                    .and_(pl.col("change_of_poss") == 1),
                )
                .then(pl.lit("Kickoff Team Fumble Recovery"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Punt Touchdown")
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("change_of_poss") == 1),
                )
                .then(pl.lit("Punt Return Touchdown"))
                .when(
                    (pl.col("type.text") == "Punt")
                    .and_(pl.col("text").str.contains("(?i)for a TD"))
                    .and_(pl.col("change_of_poss") == 1),
                )
                .then(pl.lit("Punt Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Punt")
                    .and_(pl.col("fumble_vec") == True)
                    .and_(pl.col("change_of_poss") == 0),
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
                    .and_(pl.col("change_of_poss") == 0),
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
                        (pl.col("pass") == True).or_(pl.col("rush") == True),
                    ),
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
                    .and_(pl.col("safety") == True),
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
                    (pl.col("type.text") == "Extra Point Missed").and_(pl.col("text").str.contains("(?i)Two-Point")),
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
                penalty_flag=pl.when((pl.col("type.text") == "Penalty").or_(pl.col("text").str.contains("(?i)penalty")))
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_declined
                # a declined penalty usually rides a normally-labelled play
                # ("Pass Incompletion", "Rush") -- gating only on the play label
                # missed 576 of 894 declined texts in the CFB twin's 2025 sweep
                penalty_declined=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)declined")),
                )
                .then(True)
                .when((pl.col("text").str.contains("(?i)penalty")).and_(pl.col("text").str.contains("(?i)declined")))
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_no_play
                # "nullified by penalty" is the vendor's own wiped-play verdict
                penalty_no_play=pl.when(
                    (pl.col("type.text") == "Penalty").and_(
                        pl.col("text").str.contains("(?i)no play|nullified by penalty"),
                    ),
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)penalty")).and_(
                        pl.col("text").str.contains("(?i)no play|nullified by penalty"),
                    ),
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_offset
                # off-?setting: the unhyphenated spelling dominates 44:1
                penalty_offset=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)off-?setting")),
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)penalty")).and_(pl.col("text").str.contains("(?i)off-?setting")),
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_1st_conv
                penalty_1st_conv=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)1st down")),
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
                        pl.col("text").str.contains("(?i)off-?setting") == False,
                        pl.col("text").str.contains("(?i)no play") == False,
                    ),
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                # Foul-name branches FIRST: a declined/offset penalty keeps its
                # foul name (the disposition already lives in penalty_declined /
                # penalty_offset). Same fix as the CFB twin, where the 2025
                # taxonomy measured the disposition-first ordering swallowing
                # 318 foul names.
                penalty_detail=pl.when(pl.col("text").str.contains("(?i)roughing (?:the )?passer"))
                .then(pl.lit("Roughing the Passer"))
                .when(pl.col("text").str.contains("(?i)offensive holding"))
                .then(pl.lit("Offensive Holding"))
                # inter?ference: vendor texts ship a literal "Inteference" typo
                .when(pl.col("text").str.contains("(?i)pass inter?ference"))
                .then(pl.lit("Pass Interference"))
                .when(pl.col("text").str.contains("(?i)encroachment"))
                .then(pl.lit("Encroachment"))
                .when(pl.col("text").str.contains("(?i)defensive pass inter?ference"))
                .then(pl.lit("Defensive Pass Interference"))
                .when(pl.col("text").str.contains("(?i)offensive pass inter?ference"))
                .then(pl.lit("Offensive Pass Interference"))
                .when(pl.col("text").str.contains("(?i)illegal procedure"))
                .then(pl.lit("Illegal Procedure"))
                .when(pl.col("text").str.contains("(?i)defensive holding"))
                .then(pl.lit("Defensive Holding"))
                .when(pl.col("text").str.contains("(?i)holding"))
                .then(pl.lit("Holding"))
                .when(pl.col("text").str.contains("(?i)offensive off-?side|(?i)off-?side offense"))
                .then(pl.lit("Offensive Offside"))
                .when(pl.col("text").str.contains("(?i)defensive off-?side|(?i)off-?side defense"))
                .then(pl.lit("Defensive Offside"))
                # off-?side: hyphenated vendor spelling, same as the CFB twin
                .when(pl.col("text").str.contains("(?i)off-?side"))
                .then(pl.lit("Offside"))
                .when(pl.col("text").str.contains("(?i)(?:illegal|invalid) fair catch signal"))
                .then(pl.lit("Illegal Fair Catch Signal"))
                .when(pl.col("text").str.contains(r"(?i)illegal bat(?:ting)?\b"))
                .then(pl.lit("Illegal Batting"))
                .when(pl.col("text").str.contains("(?i)neutral zone infraction"))
                .then(pl.lit("Neutral Zone Infraction"))
                # inelgible: literal vendor typo
                .when(pl.col("text").str.contains("(?i)inel[ei]gible downfield|(?i)inelgible downfield"))
                .then(pl.lit("Ineligible Downfield"))
                .when(pl.col("text").str.contains("(?i)illegal use of hands"))
                .then(pl.lit("Illegal Use of Hands"))
                .when(pl.col("text").str.contains("(?i)kickoff out of bounds|(?i)kickoff out-of-bounds"))
                .then(pl.lit("Kickoff Out of Bounds"))
                .when(pl.col("text").str.contains("(?i)12 men on the field"))
                .then(pl.lit("12 Men on the Field"))
                .when(pl.col("text").str.contains("(?i)block(?:ing)? below (?:the )?waist"))
                .then(pl.lit("Block Below the Waist"))
                .when(pl.col("text").str.contains("(?i)chop block"))
                .then(pl.lit("Chop Block"))
                .when(pl.col("text").str.contains("(?i)illegal block|(?i)low block"))
                .then(pl.lit("Illegal Block"))
                .when(pl.col("text").str.contains("(?i)personal foul"))
                .then(pl.lit("Personal Foul"))
                .when(pl.col("text").str.contains("(?i)false start"))
                .then(pl.lit("False Start"))
                .when(pl.col("text").str.contains("(?i)substitution infraction|(?i)illegal substitution"))
                .then(pl.lit("Substitution Infraction"))
                .when(pl.col("text").str.contains("(?i)illegal formation"))
                .then(pl.lit("Illegal Formation"))
                # prefix covers "Illegal Touching" / "Illegal Touch Pass" / "Illegal Touch-Pass"
                .when(pl.col("text").str.contains("(?i)illegal touch"))
                .then(pl.lit("Illegal Touching"))
                .when(pl.col("text").str.contains("(?i)sideline inter?ference"))
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
                .when(pl.col("text").str.contains("(?i)roughing (?:the )?kicker"))
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
                .when(pl.col("text").str.contains("(?i)running into (?:the )?kicker"))
                .then(pl.lit("Running Into Kicker"))
                .when(pl.col("text").str.contains("(?i)failure to wear required equipment"))
                .then(pl.lit("Failure to Wear Required Equipment"))
                .when(pl.col("text").str.contains("(?i)player disqualification"))
                .then(pl.lit("Player Disqualification"))
                .when(pl.col("text").str.contains("(?i)disconcerting"))
                .then(pl.lit("Disconcerting Signals"))
                .when(pl.col("text").str.contains(r"(?i)\bleaping\b"))
                .then(pl.lit("Leaping"))
                # disposition-only labels LAST: they fire only when no foul name
                # was recognizable in the text
                .when(pl.col("penalty_offset") == 1)
                .then(pl.lit("Offsetting"))
                .when(pl.col("penalty_declined") == 1)
                .then(pl.lit("Declined"))
                .when(pl.col("penalty_flag") == True)
                .then(pl.lit("Missing")),
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
                    .str.replace(" yards to the | yds to the | yd to the ", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                yds_penalty=pl.when(
                    (pl.col("penalty_flag") == True).and_(
                        pl.col("yds_penalty").is_null(),
                        pl.col("text").str.contains(r"(?i)ards\)"),
                    ),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(.{0,4})yards\)|Yards\)|yds\)|Yds\)", 1)
                    .str.replace("yards\\)|Yards\\)|yds\\)|Yds\\)", "")
                    .str.replace("\\(", ""),
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
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked")),
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
                    pl.col("type.text").is_in(["Pass Reception", "Pass Completion", "Passing Touchdown"]),
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
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False),
                )
                .then(True)
                .otherwise(False),
                pass_attempt=pl.when(
                    pl.col("type.text").is_in(
                        ["Pass Reception", "Pass Completion", "Passing Touchdown", "Pass Incompletion"],
                    ),
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
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False),
                )
                .then(True)
                .when((pl.col("pass") == True).and_(pl.col("text").str.contains("(?i)sacked") == False))
                .then(True)
                .otherwise(False),
                target=pl.when(
                    pl.col("type.text").is_in(
                        ["Pass Reception", "Pass Completion", "Passing Touchdown", "Pass Incompletion"],
                    ),
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
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False),
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
                # --- Pass depth/direction + rush direction (Game on Paper matrix fields) ---
                # Extracted from ESPN play description text; null when the pattern is absent
                # (sacks, screens, and pre-2025 plays that omit depth/direction).
                # Depth: "short" (0-12 air yards) | "deep" (12+ air yards)
                # Direction: "left" | "middle" | "right"
                pass_depth=pl.when(pl.col("pass") == True)
                .then(pl.col("text").str.extract(r"\s(short|deep)\s", 1))
                .otherwise(None),
                pass_direction=pl.when(pl.col("pass") == True)
                .then(pl.col("text").str.extract(r"\s(left|middle|right)\s", 1))
                .otherwise(None),
                rush_direction=pl.when(pl.col("rush") == True)
                .then(pl.col("text").str.extract(r"\s(left|middle|right)\s", 1))
                .otherwise(None),
                # --- Change of possession via turnover
                turnover_vec=pl.col("type.text").is_in(turnover_vec),
                offense_score_play=pl.col("type.text").is_in(offense_score_vec),
                defense_score_play=pl.col("type.text").is_in(defense_score_vec),
                downs_turnover=pl.when(
                    (pl.col("type.text").is_in(normalplay))
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4)
                    .and_(pl.col("penalty_1st_conv") == False),
                )
                .then(True)
                .otherwise(False),
                # --- Touchdowns ----
                scoring_play=pl.col("type.text").is_in(scores_vec),
                yds_punted=pl.col("text").str.extract(r"(?i)(punt for \d+)").str.extract(r"(\d+)").cast(pl.Int32),
                yds_punt_gained=pl.when(pl.col("punt") == True).then(pl.col("statYardage")).otherwise(None),
                fg_attempt=pl.when(
                    (pl.col("type.text").str.contains(r"(?i)Field Goal")).or_(
                        pl.col("text").str.contains(r"(?i)Field Goal"),
                    ),
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
                    .and_(pl.col("start.pos_team.id") == pl.col("homeTeamId")),
                )
                .then(100 - pl.col("start.yardLine").cast(pl.Int32))
                .when(
                    (pl.col("start.yardsToEndzone").is_null())
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False)
                    .and_(pl.col("start.pos_team.id") == pl.col("awayTeamId")),
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
                    (pl.col("fg_attempt") == True).or_(pl.col("punt") == True).or_(pl.col("kickoff_play") == True),
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
                            ],
                        )
                        == False,
                    ),
                )
                .then(True)
                .otherwise(False),
                # --- Change of pos_team by lead('pos_team', 1)----
                change_of_pos_team=pl.when(
                    (pl.col("pos_team") == pl.col("lead_pos_team")).and_(
                        ((pl.col("lead_play_type").is_in(["End Period", "End of Half"])) == False).or_(
                            pl.col("lead_play_type").is_null(),
                        ),
                    ),
                )
                .then(False)
                .when(
                    (pl.col("pos_team") == pl.col("lead_pos_team2")).and_(
                        (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).or_(
                            pl.col("lead_play_type").is_null(),
                        ),
                    ),
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
                            pl.col("start.pos_team.id") != pl.col("end.pos_team.id"),
                        )
                    ).or_(pl.col("downs_turnover") == True),
                )
                .then(-1 * pl.col("pos_score_diff"))
                .otherwise(pl.col("pos_score_diff")),
            )
            .with_columns(
                pos_score_diff_end=pl.when(
                    (pl.col("pos_score_pts").abs() >= 8)
                    .and_(pl.col("scoring_play") == False)
                    .and_(pl.col("change_of_pos_team") == False),
                )
                .then(pl.col("pos_score_diff_start"))
                .when(
                    (pl.col("pos_score_pts").abs() >= 8)
                    .and_(pl.col("scoring_play") == False)
                    .and_(pl.col("change_of_pos_team") == True),
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

        # --- nflfastR-compatible scoring event result columns ---
        # field_goal_result is always derivable; extra_point_result and
        # two_point_conv_result require pointAfterAttempt.* from the ESPN API
        # (present on TD plays in modern data; absent for very old seasons).
        scoring_exprs: list = [
            pl.when(pl.col("fg_attempt") == True)
            .then(
                pl.when(pl.col("fg_made") == True)
                .then(pl.lit("made"))
                .when(pl.col("type.text").str.contains(r"(?i)blocked"))
                .then(pl.lit("blocked"))
                .otherwise(pl.lit("missed"))
            )
            .otherwise(None)
            .alias("field_goal_result"),
        ]
        if "pointAfterAttempt.abbreviation" in play_df.columns and "pointAfterAttempt.value" in play_df.columns:
            scoring_exprs += [
                # extra_point_result: "good" | "blocked" | "failed" | null (non-TD plays)
                pl.when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)extra point"))
                .then(
                    pl.when(pl.col("pointAfterAttempt.value") == 1.0)
                    .then(pl.lit("good"))
                    .when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)block"))
                    .then(pl.lit("blocked"))
                    .otherwise(pl.lit("failed"))
                )
                .otherwise(None)
                .alias("extra_point_result"),
                # two_point_conv_result: "success" | "failure" | null (non-TD plays)
                pl.when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)two.?point"))
                .then(
                    pl.when(pl.col("pointAfterAttempt.value") == 2.0)
                    .then(pl.lit("success"))
                    .otherwise(pl.lit("failure"))
                )
                .otherwise(None)
                .alias("two_point_conv_result"),
            ]
        play_df = play_df.with_columns(scoring_exprs)

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
                .and_(pl.col("text").str.contains("(?i)touchdown") == False),
            )
            .then(pl.col("text").str.extract(r"(?i)for (\d+) yards").cast(pl.Int32))
            .when(
                (pl.col("rush") == True)
                .and_(pl.col("text").str.contains("(?i)rushed"))
                .and_(pl.col("text").str.contains("(?i)touchdown") == True),
            )
            .then(pl.col("text").str.extract(r"(?i)for a (\d+) yard").cast(pl.Int32))
            .otherwise(None),
            yds_receiving=pl.when(
                (pl.col("pass") == True)
                .and_(pl.col("text").str.contains(r"(?i)complete to"))
                .and_(pl.col("text").str.contains(r"(?i)for no gain")),
            )
            .then(0)
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("text").str.contains(r"(?i)complete to"))
                .and_(pl.col("text").str.contains(r"(?i)for a loss of")),
            )
            .then(-1 * pl.col("text").str.extract(r"(?i)for a loss of (\d+)").cast(pl.Int32))
            .when((pl.col("pass") == True).and_(pl.col("text").str.contains(r"(?i)complete to")))
            .then(pl.col("text").str.extract(r"(?i)for (\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True).and_(
                    pl.col("text").str.contains(r"(?i)incomplete|(?i) sacked|(?i)intercepted|(?i)pass defensed"),
                ),
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
                .and_(pl.col("text").str.contains(r"(?i)Yd Interception Return")),
            )
            .then(pl.col("text").str.extract(r"(?i)(.+)Yd Interception Return").str.extract(r"(\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("int") == True)
                .and_(pl.col("text").str.contains(r"(?i)for no gain")),
            )
            .then(0)
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("int") == True)
                .and_(pl.col("text").str.contains(r"(?i)for a loss of")),
            )
            .then(-1 * pl.col("text").str.extract(r"(?i)for a loss of (\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True).and_(pl.col("int") == True).and_(pl.col("text").str.contains(r"(?i)for a TD")),
            )
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("pass") == True).and_(pl.col("int") == True))
            .then(
                pl.col("text")
                .str.replace("for a 1st", "")
                .str.extract(r"(?i)for (.+)")
                .str.extract(r"(\d+)")
                .cast(pl.Int32),
            )
            .otherwise(None),
            yds_kickoff=pl.when(pl.col("kickoff_play") == True)
            .then(pl.col("text").str.extract(r"(?i)kickoff for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_kickoff_return=pl.when(
                (pl.col("kickoff_play") == True).and_(pl.col("kickoff_tb") == True).and_(pl.col("season") > 2013),
            )
            .then(25)
            .when((pl.col("kickoff_play") == True).and_(pl.col("kickoff_tb") == True).and_(pl.col("season") <= 2013))
            .then(20)
            .when(
                (pl.col("kickoff_play") == True)
                .and_(pl.col("fumble_vec") == False)
                .and_(pl.col("text").str.contains(r"(?i)for no gain|fair catch|fair caught")),
            )
            .then(0)
            .when(
                (pl.col("kickoff_play") == True)
                .and_(pl.col("fumble_vec") == False)
                .and_(pl.col("text").str.contains(r"(?i)out-of-bounds|out of bounds")),
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
                    .or_(pl.col("punt_fair_catch") == True),
                ),
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
                .and_(pl.col("rush") == True),
            )
            .then(pl.col("statYardage") - pl.col("yds_rushed"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("int") == True),
            )
            .then(pl.col("statYardage") - pl.col("yds_int_return"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == False)
                .and_(pl.col("type.text") != "Pass Incompletion"),
            )
            .then(pl.col("statYardage") - pl.col("yds_receiving"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == False)
                .and_(pl.col("type.text") == "Pass Incompletion"),
            )
            .then(pl.col("statYardage"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == True),
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
                        r"(?i)(.{0,25} )run |(?i)(.{0,25} )\d{0,2} Yd Run|(?i)(.{0,25} )rush |(?i)(.{0,25} )rushed ",
                    )
                    .str.replace(r"(?i) run |(?i) \d+ Yd Run|(?i) rush ", "")
                    .str.replace(r" \((.+)\)", ""),
                )
                .otherwise(None),
                # --- QB Names -----
                pass_player=pl.when(
                    (pl.col("pass") == True)
                    .and_(pl.col("sack_vec") == False)
                    .and_(pl.col("type.text") != "Passing Touchdown"),
                )
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i)(.{0,30} )pass |(?i)(.{0,30} )sacked by|(?i)(.{0,30} )sacked for|(?i)(.{0,30} )incomplete|(?i)pass from (.{0,30} ) \( ",
                    )
                    .str.replace(r"(?i)pass |(?i) sacked by|(?i) sacked for|(?i) incomplete", ""),
                )
                .when(
                    (pl.col("pass") == True)
                    .and_(pl.col("sack_vec") == True)
                    .and_(pl.col("type.text") != "Passing Touchdown"),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.{0,30} )sacked by|(?i)(.{0,30} )sacked for")
                    .str.replace(r"(?i)pass |(?i) sacked by|(?i) sacked for|(?i) incomplete", ""),
                )
                .when((pl.col("pass") == True).and_(pl.col("type.text") == "Passing Touchdown"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)pass from(.+)")
                    .str.replace(r"pass from", "")
                    .str.replace(r" \((.+)\)", "")
                    .str.replace(r" \,", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                pass_player=pl.when((pl.col("type.text") == "Passing Touchdown").and_(pl.col("pass_player").is_null()))
                .then(
                    pl.col("text")
                    .str.extract(r"(.+)pass(.+)? complete to")
                    .str.replace(r" pass complete to(.+)", "")
                    .str.replace(r" pass complete to", ""),
                )
                .otherwise(pl.col("pass_player")),
            )
            .with_columns(
                pass_player=pl.when((pl.col("type.text") == "Passing Touchdown").and_(pl.col("pass_player").is_null()))
                .then(
                    pl.col("text")
                    .str.extract(r"(.+)pass,to")
                    .str.replace(r" pass,to(.+)", "")
                    .str.replace(r" pass,to", "")
                    .str.replace(r" \((.+)\)", ""),
                )
                .otherwise(pl.col("pass_player")),
            )
            .with_columns(
                pass_player=pl.when(
                    (pl.col("pass") == True).and_(
                        (
                            (pl.col("pass_player").str.strip_chars().str.len_chars() == 0).or_(
                                pl.col("pass_player").is_null(),
                            )
                        ),
                    ),
                )
                .then(pl.lit("TEAM"))
                .otherwise(pl.col("pass_player")),
                # --- WR Names -----
                receiver_player=pl.when(
                    (pl.col("pass") == True).and_(pl.col("text").str.contains(r"(?i)sacked") == False),
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
                                ["Fumble Recovery (Opponent) Touchdown", "Fumble Recovery (Opponent)"],
                            )
                        ).and_(pl.col("text").str.contains(r"(?i)sacked")),
                    ),
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
                    .str.replace(r'(?i) "', ""),
                ),
            )
            .with_columns(
                receiver_player=pl.when(pl.col("receiver_player").str.contains(r"(?i)III") == True)
                .then(pl.col("receiver_player").str.replace(r"(?i)[A-Z]{3,}", ""))
                .otherwise(pl.col("receiver_player")),
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
                    (pl.col("sack") == True).or_((pl.col("fumble_vec") == True).and_(pl.col("pass") == True)),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)sacked by(.+)")
                    .str.replace(r"for (.+)", "")
                    .str.replace(r"(.+) by ", "")
                    .str.replace(r" at the (.+)", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                sack_player1=pl.col("sack_players").str.replace(r"and (.+)", ""),
                sack_player2=pl.when(pl.col("sack_players").str.contains(r"and (.+)"))
                .then(pl.col("sack_players").str.replace(r"(.+) and", ""))
                .otherwise(None),
                # --- Interception Names -----
                interception_player=pl.when(pl.col("text").str.contains(r"Yd Interception Return"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i)(.{0,25} )\\d{0,2} Yd Interception Return|(?i)(.{0,25} )\\d{0,2} yd interception return",
                    )
                    .str.replace(r"return (.+)", "")
                    .str.replace(r"(.+) intercepted", "")
                    .str.replace(r"intercepted", "")
                    .str.replace(r"Yd Interception Return", "")
                    .str.replace(r"for a 1st down", "")
                    .str.replace(r"(\\d{1,2})", "")
                    .str.replace(r"for a TD", "")
                    .str.replace(r"at the (.+)", "")
                    .str.replace(r" by ", ""),
                )
                .when(
                    (
                        (pl.col("type.text") == "Interception Return").or_(
                            pl.col("type.text") == "Interception Return Touchdown",
                        )
                    ).and_(pl.col("pass") == True),
                )
                .then(pl.col("text").str.extract(r"(?i)intercepted (.+)"))
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
                    .str.replace(r"COASTL CAROLINA", ""),
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
                    .str.replace(r"(?i) no return", ""),
                )
                .otherwise(None),
                # --- Punt Returner Names ----
                punt_return_player=pl.when(pl.col("type.text").str.contains("Punt"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i), (.{0,25}) returns|(?i)fair catch by (.{0,25})|(?i), returned by (.{0,25})|(?i)yards by (.{0,30})|(?i) return by (.{0,25})",
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
                    .str.replace(r"(?i)\((.+)\)", ""),
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
                    .str.replace(r"punt blocked by |for a(.+)", ""),
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
                    .str.replace(r"(?i)yd return of", ""),
                )
                .otherwise(pl.col("punt_block_player")),
                # --- Punt Block Returner Names ----
                punt_block_return_player=pl.when(
                    (pl.col("type.text").str.contains(r"Punt"))
                    .and_(pl.col("text").str.contains(r"(?i)blocked"))
                    .and_(pl.col("text").str.contains(r"(?i)return")),
                )
                .then(pl.col("text").str.extract(r"(?i)(.+) return"))
                .otherwise(None),
            )
            .with_columns(
                punt_block_return_player=pl.struct("punt_block_player", "punt_block_return_player").map_elements(
                    lambda cols: (
                        cols["punt_block_return_player"]
                        .replace(r"(?i)(.+)blocked by", "")
                        .replace(str(pl.format(r"(?i)blocked by {}", cols["punt_block_player"])), "")
                        if cols["punt_block_return_player"] is not None
                        else None
                    ),
                    return_dtype=pl.Utf8,
                ),
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
                    .str.replace(r"(?i) on-side| kickoff", ""),
                )
                .otherwise(None),
                # --- Kickoff Returner Names ----
                kickoff_return_player=pl.when(pl.col("type.text").str.contains(r"(?i)ickoff"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i), (.{0,25}) return|(?i), (.{0,25}) fumble|(?i)returned by (.{0,25})|(?i)touchback by (.{0,25})",
                    )
                    .str.replace(r", ", "")
                    .str.replace(r"(?i) return|(?i) fumble|(?i) returned by|(?i) for |(?i)touchback by ", "")
                    .str.replace(r"\((.+)\)(.+)", ""),
                )
                .otherwise(None),
                # --- Field Goal Kicker Names ----
                fg_kicker_player=pl.when(pl.col("type.text").str.contains(r"(?i)Field Goal"))
                .then(
                    pl.col("text")
                    .str.extract(
                        r"(?i)(.{0,25} )\\d{0,2} yd field goal|(?i)(.{0,25} )\\d{0,2} yd fg|(?i)(.{0,25} )\\d{0,2} yard field goal",
                    )
                    .str.replace(r"(?i) Yd Field Goal|(?i)Yd FG |(?i)yd FG|(?i) yd FG", "")
                    .str.replace(r"(\\d{1,2})", ""),
                )
                .otherwise(None),
                # --- Field Goal Blocker Names ----
                fg_block_player=pl.when(pl.col("type.text").str.contains(r"(?i)Field Goal"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)blocked by (.{0,25})")
                    .str.replace(r",(.+)", "")
                    .str.replace(r"blocked by ", "")
                    .str.replace(r"  (.)+", ""),
                )
                .otherwise(None),
                # --- Field Goal Returner Names ----
                fg_return_player=pl.when(
                    (pl.col("type.text").str.contains(r"(?i)Field Goal"))
                    .and_(pl.col("text").str.contains(r"(?i)blocked by|missed"))
                    .and_(pl.col("text").str.contains(r"(?i)return")),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)  (.+)")
                    .str.replace(r"(?i),(.+)", "")
                    .str.replace(r"(?i)return ", "")
                    .str.replace(r"(?i)returned ", "")
                    .str.replace(r"(?i) for (.+)", "")
                    .str.replace(r"(?i) for (.+)", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                fg_return_player=pl.when(
                    (pl.col("type.text").is_in(["Missed Field Goal Return", "Missed Field Goal Return Touchdown"])),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.+)return")
                    .str.replace(r"(?i) return", "")
                    .str.replace(r"(?i)(.+),", ""),
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
                    .str.replace(r", ", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                fumble_player=pl.when(pl.col("type.text") == "Penalty").then(None).otherwise(pl.col("fumble_player")),
                # --- Forced Fumble Names ----
                fumble_forced_player=pl.when(
                    (pl.col("text").str.contains(r"(?i)fumble")).and_(pl.col("text").str.contains(r"(?i)forced by")),
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
                    .str.replace(r"(?i), ", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                fumble_forced_player=pl.when(pl.col("type.text") == "Penalty")
                .then(None)
                .otherwise(pl.col("fumble_forced_player")),
                # --- Fumble Recovered Names ----
                fumble_recovered_player=pl.when(
                    (pl.col("text").str.contains(r"(?i)fumble")).and_(pl.col("text").str.contains(r"(?i)recovered by")),
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
                    .str.replace(r"(?i)for a 1ST down", ""),
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
                passer_player_name=pl.col("pass_player").str.strip_chars(),
                rusher_player_name=pl.col("rush_player").str.strip_chars(),
                receiver_player_name=pl.col("receiver_player").str.strip_chars(),
                sack_player_name=pl.col("sack_player1").str.strip_chars(),
                sack_player_name2=pl.col("sack_player2").str.strip_chars(),
                pass_breakup_player_name=pl.col("pass_breakup_player").str.strip_chars(),
                interception_player_name=pl.col("interception_player").str.strip_chars(),
                fg_kicker_player_name=pl.col("fg_kicker_player").str.strip_chars(),
                fg_block_player_name=pl.col("fg_block_player").str.strip_chars(),
                fg_return_player_name=pl.col("fg_return_player").str.strip_chars(),
                kickoff_player_name=pl.col("kickoff_player").str.strip_chars(),
                kickoff_return_player_name=pl.col("kickoff_return_player").str.strip_chars(),
                punter_player_name=pl.col("punter_player").str.strip_chars(),
                punt_block_player_name=pl.col("punt_block_player").str.strip_chars(),
                punt_return_player_name=pl.col("punt_return_player").str.strip_chars(),
                punt_block_return_player_name=pl.col("punt_block_return_player").str.strip_chars(),
                fumble_player_name=pl.col("fumble_player").str.strip_chars(),
                fumble_forced_player_name=pl.col("fumble_forced_player").str.strip_chars(),
                fumble_recovered_player_name=pl.col("fumble_recovered_player").str.strip_chars(),
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
                ],
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
                    .and_(pl.col("penalty_declined") == False),
                )
                .then(pl.col("start.down"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.down") + 1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.down") + 1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
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
                    .and_(pl.col("penalty_declined") == False),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(pl.col("start.distance"))
                .otherwise(pl.col("start.distance")),
                middle_8=pl.when(
                    (pl.col("start.adj_TimeSecsRem") >= 1560).and_(pl.col("start.adj_TimeSecsRem") <= 2040),
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
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance")),
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
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4]))
                    .and_(pl.col("statYardage") < pl.col("start.distance")),
                )
                .then(False)
                .otherwise(None),
                power_rush_attempt=pl.when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4])),
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4])),
                )
                .then(False)
                .otherwise(None),
                early_down=pl.when(
                    ((pl.col("down_1") == True).or_(pl.col("down_2") == True)).and_(pl.col("scrimmage_play") == True),
                )
                .then(True)
                .otherwise(False),
                late_down=pl.when(
                    ((pl.col("down_3") == True).or_(pl.col("down_4"))).and_(pl.col("scrimmage_play") == True),
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
                    .and_(pl.col("start.distance") < 8),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_3") == True)
                    .and_(pl.col("start.distance") < 5),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_4") == True)
                    .and_(pl.col("start.distance") < 5),
                )
                .then(True)
                .otherwise(False),
                passing_down=pl.when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_2") == True)
                    .and_(pl.col("start.distance") >= 8),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_3") == True)
                    .and_(pl.col("start.distance") >= 5),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_4") == True)
                    .and_(pl.col("start.distance") >= 5),
                )
                .then(True)
                .otherwise(False),
                TFL=pl.when(
                    (pl.col("type.text") != "Penalty").and_(pl.col("sp") == False).and_(pl.col("statYardage") < 0),
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
                    "start.spread_time",
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
                # The 2016 rule change moved the touchback spot to the 25 (75 yards to
                # the end zone); before 2016 it was the 20 (80 yards).  nflfastR keys
                # this on the 2016 season boundary, NOT 2013.
                pl.when((pl.col("type.text").is_in(kickoff_vec)).and_(pl.col("season") >= 2016))
                .then(TOUCHBACK_YARDLINE_POST_2016)
                .when((pl.col("type.text").is_in(kickoff_vec)).and_(pl.col("season") < 2016))
                .then(TOUCHBACK_YARDLINE_PRE_2016)
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone.touchback"),
            )
        )

        _ep_model = _ep_wp_load_model("ep_model.ubj")

        X_ep_tb = _espn_ep_features(
            play_df,
            half_sec_col="start.TimeSecsRem",
            yardline_col="start.yardsToEndzone.touchback",
            home_col="start.is_home",
            ydstogo_col="distance",
            down1_col="down_1",
            down2_col="down_2",
            down3_col="down_3",
            down4_col="down_4",
            pos_timeouts_col="start.posTeamTimeouts",
            def_timeouts_col="start.defPosTeamTimeouts",
        )
        _probs_tb = _ep_model.predict(DMatrix(X_ep_tb, feature_names=EP_FEATURES)).reshape(-1, 7)
        EP_start_touchback = np.clip(_probs_tb @ _EP_POINT_VALUES, -10.0, 10.0)

        X_ep_start = _espn_ep_features(
            play_df,
            half_sec_col="start.TimeSecsRem",
            yardline_col="start.yardsToEndzone",
            home_col="start.is_home",
            ydstogo_col="start.distance",
            down1_col="down_1",
            down2_col="down_2",
            down3_col="down_3",
            down4_col="down_4",
            pos_timeouts_col="start.posTeamTimeouts",
            def_timeouts_col="start.defPosTeamTimeouts",
        )
        _probs_start = _ep_model.predict(DMatrix(X_ep_start, feature_names=EP_FEATURES)).reshape(-1, 7)
        EP_start = np.clip(_probs_start @ _EP_POINT_VALUES, -10.0, 10.0)

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

        X_ep_end = _espn_ep_features(
            play_df,
            half_sec_col="end.TimeSecsRem",
            yardline_col="end.yardsToEndzone",
            home_col="end.is_home",
            ydstogo_col="end.distance",
            down1_col="down_1_end",
            down2_col="down_2_end",
            down3_col="down_3_end",
            down4_col="down_4_end",
            pos_timeouts_col="end.posTeamTimeouts",
            def_timeouts_col="end.defPosTeamTimeouts",
        )
        _probs_end = _ep_model.predict(DMatrix(X_ep_end, feature_names=EP_FEATURES)).reshape(-1, 7)
        EP_end = np.clip(_probs_end @ _EP_POINT_VALUES, -10.0, 10.0)

        play_df = play_df.with_columns(
            pl.Series("EP_start_touchback", EP_start_touchback, dtype=pl.Float64),
            pl.Series("EP_start", EP_start, dtype=pl.Float64),
            pl.Series("EP_end", EP_end, dtype=pl.Float64),
        )

        # --- Derivation half delegated to the shared, model-free calculate_epa ---
        # The EP point estimates (EP_start / EP_end / EP_start_touchback) have been
        # scored above; calculate_epa is the verbatim lift of the scoring-overlay /
        # lead-lag / EP_between / kickoff-touchback / turnover-flip / EPA derivation
        # that used to live inline here. NFLPlayProcess always operates on a single
        # game, so calculate_epa's ``.over("game_id")`` window guards collapse to the
        # plain ``.shift`` semantics this method relied on, leaving output identical.
        # calculate_epa also emits lowercase nflverse aliases (ep / epa / ep_start /
        # ep_end) the legacy __process_epa output never carried, so drop them to keep
        # the plays schema byte-identical; the EPA_* summary flags below stay inline.
        play_df = calculate_epa(play_df).drop("ep", "epa", "ep_start", "ep_end")

        play_df = play_df.with_columns(
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
        ).with_columns(
            EPA_non_explosive=pl.when(pl.col("EPA_explosive") == False).then(pl.col("EPA")).otherwise(None),
            EPA_explosive_pass=pl.when((pl.col("pass") == True).and_(pl.col("EPA") >= 2.4)).then(True).otherwise(False),
            EPA_explosive_rush=pl.when((pl.col("rush") == True).and_(pl.col("EPA") >= 1.8)).then(True).otherwise(False),
            first_down_created=pl.when(
                (pl.col("scrimmage_play") == True)
                .and_(pl.col("end.down") == 1)
                .and_(pl.col("start.pos_team.id") == pl.col("end.pos_team.id")),
            )
            .then(True)
            .otherwise(False),
            EPA_success=pl.when(pl.col("EPA") > 0).then(True).otherwise(False),
            EPA_success_early_down=pl.when((pl.col("EPA") > 0).and_(pl.col("early_down") == True))
            .then(True)
            .otherwise(False),
            EPA_success_early_down_pass=pl.when(
                (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("early_down") == True),
            )
            .then(True)
            .otherwise(False),
            EPA_success_early_down_rush=pl.when(
                (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("early_down") == True),
            )
            .then(True)
            .otherwise(False),
            EPA_success_late_down=pl.when((pl.col("EPA") > 0).and_(pl.col("late_down") == True))
            .then(True)
            .otherwise(False),
            EPA_success_late_down_pass=pl.when(
                (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("late_down") == True),
            )
            .then(True)
            .otherwise(False),
            EPA_success_late_down_rush=pl.when(
                (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("late_down") == True),
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
                (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("middle_8") == True),
            )
            .then(True)
            .otherwise(False),
            EPA_middle_8_success_rush=pl.when(
                (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("middle_8") == True),
            )
            .then(True)
            .otherwise(False),
            EPA_penalty=pl.when(pl.col("type.text").is_in(["Penalty", "Penalty (Kickoff)"]))
            .then(pl.col("EPA"))
            .when(pl.col("penalty_in_text") == True)
            .then(pl.col("EP_end") - pl.col("EP_start"))
            .otherwise(None),
            EPA_sp=pl.when(
                (pl.col("fg_attempt") == True).or_(pl.col("punt") == True).or_(pl.col("kickoff_play") == True),
            )
            .then(pl.col("EPA"))
            .otherwise(False),
            EPA_fg=pl.when(pl.col("fg_attempt") == True).then(pl.col("EPA")).otherwise(None),
            EPA_punt=pl.when(pl.col("punt") == True).then(pl.col("EPA")).otherwise(None),
            EPA_kickoff=pl.when(pl.col("kickoff_play") == True).then(pl.col("EPA")).otherwise(None),
        )
        return play_df

    def __process_qb_epa(self, play_df):
        """Add ``qb_epa`` — EPA crediting the QB on completed-pass-then-fumble plays.

        ESPN-column port of :func:`sportsdataverse.nfl.ep_wp._derive_qb_epa`
        (nflfastR ``add_qb_epa``).  On every play ``qb_epa == EPA`` EXCEPT plays
        where the receiver caught the ball and *then* lost a fumble
        (``completion == True & fumble_vec == True``): those are RE-SPOTTED as if
        the receiver had simply been tackled at the fumble spot (no turnover),
        EP is re-scored with the bundled ``ep_model``, and
        ``qb_epa = ep_respotted - EP_start`` (negated when the re-spot is a
        turnover on downs).  The QB thereby keeps completion + YAC credit and is
        NOT penalised for the fumble turnover.

        Faithful no-op fallback: when the required columns are absent (or no
        completed-pass-fumble play exists in the game), ``qb_epa == EPA``
        exactly.  ``qb_epa`` is float64 and null wherever ``EPA`` is null.
        """
        required = ("completion", "fumble_vec", "statYardage", "EPA", "EP_start")
        if any(c not in play_df.columns for c in required):
            return play_df.with_columns(pl.col("EPA").cast(pl.Float64).alias("qb_epa"))

        play_df = play_df.with_row_index("_qbepa_idx")

        fumbles = play_df.filter(
            (pl.col("completion") == True)
            .and_(pl.col("fumble_vec") == True)
            .and_(pl.col("EPA").is_not_null())
            .and_(pl.col("start.down").is_not_null())
        )

        if fumbles.height == 0:
            return play_df.drop("_qbepa_idx").with_columns(pl.col("EPA").cast(pl.Float64).alias("qb_epa"))

        # Re-spot the play as if the receiver were tackled at the fumble spot.
        # ``statYardage`` is the ESPN gain; ``start.distance`` the yards to go and
        # ``start.yardsToEndzone`` the field position.  Mirrors _derive_qb_epa.
        respotted = (
            fumbles.with_columns(
                pl.when(pl.col("start.TimeSecsRem") <= 6)
                .then(pl.lit(0.0))
                .otherwise(pl.col("start.TimeSecsRem").cast(pl.Float64) - 6.0)
                .alias("start.TimeSecsRem"),
                pl.col("start.down").cast(pl.Float64).alias("_down"),
                pl.col("start.posTeamTimeouts").alias("_pos_to_pre"),
                pl.col("start.defPosTeamTimeouts").alias("_def_to_pre"),
                pl.col("start.is_home").alias("_is_home_pre"),
                pl.col("EP_start").alias("_ep_old"),
            )
            # New yard line from the play result.
            .with_columns((pl.col("start.yardsToEndzone") - pl.col("statYardage")).alias("_yl"))
            # New down: 1st down if the gain made the sticks, else down + 1.
            .with_columns(
                pl.when(pl.col("statYardage") >= pl.col("start.distance"))
                .then(pl.lit(1.0))
                .otherwise(pl.col("_down") + 1.0)
                .alias("_down")
            )
            # down == 5 -> turnover on downs at the fumble spot -> possession change.
            .with_columns(
                pl.when(pl.col("_down") == 5).then(pl.lit(1)).otherwise(pl.lit(0)).alias("_change"),
            )
            .with_columns(
                pl.when(pl.col("_down") == 5).then(pl.lit(1.0)).otherwise(pl.col("_down")).alias("_down"),
            )
            # ydstogo: 10 on a fresh first down, else what's left after the gain.
            .with_columns(
                pl.when(pl.col("_down") == 1)
                .then(pl.lit(10.0))
                .otherwise(pl.col("start.distance") - pl.col("statYardage"))
                .alias("_ydstogo"),
            )
            # Possession change -> 10 yards to go, flip field + timeouts + home.
            .with_columns(
                pl.when(pl.col("_change") == 1).then(pl.lit(10.0)).otherwise(pl.col("_ydstogo")).alias("_ydstogo"),
                pl.when(pl.col("_change") == 1).then(100 - pl.col("_yl")).otherwise(pl.col("_yl")).alias("_yl"),
                pl.when(pl.col("_change") == 1)
                .then(pl.col("_def_to_pre"))
                .otherwise(pl.col("_pos_to_pre"))
                .alias("_pos_to"),
                pl.when(pl.col("_change") == 1)
                .then(pl.col("_pos_to_pre"))
                .otherwise(pl.col("_def_to_pre"))
                .alias("_def_to"),
                pl.when(pl.col("_change") == 1)
                .then(~pl.col("_is_home_pre"))
                .otherwise(pl.col("_is_home_pre"))
                .alias("_is_home"),
            )
            # Goal-line clamp: can't have more yards to go than yards to the end zone.
            .with_columns(
                pl.when(pl.col("_yl") < pl.col("_ydstogo"))
                .then(pl.col("_yl"))
                .otherwise(pl.col("_ydstogo"))
                .alias("_ydstogo"),
            )
            .with_columns(pl.col("_down").cast(pl.Int64).alias("_down"))
            # Down one-hots for the re-spotted state.
            .with_columns(
                (pl.col("_down") == 1).alias("_down1"),
                (pl.col("_down") == 2).alias("_down2"),
                (pl.col("_down") == 3).alias("_down3"),
                (pl.col("_down") == 4).alias("_down4"),
            )
        )

        # Re-score EP on the re-spotted state with the bundled ep_model.
        _ep_model = _ep_wp_load_model("ep_model.ubj")
        X_ep_respot = _espn_ep_features(
            respotted,
            half_sec_col="start.TimeSecsRem",
            yardline_col="_yl",
            home_col="_is_home",
            ydstogo_col="_ydstogo",
            down1_col="_down1",
            down2_col="_down2",
            down3_col="_down3",
            down4_col="_down4",
            pos_timeouts_col="_pos_to",
            def_timeouts_col="_def_to",
        )
        _probs_respot = _ep_model.predict(DMatrix(X_ep_respot, feature_names=EP_FEATURES)).reshape(-1, 7)
        ep_respot = np.clip(_probs_respot @ _EP_POINT_VALUES, -10.0, 10.0)

        fixed = respotted.select(
            pl.col("_qbepa_idx"),
            pl.col("_change"),
            pl.col("_ep_old"),
            pl.Series("_ep_respot", ep_respot, dtype=pl.Float64),
        ).with_columns(
            (
                pl.when(pl.col("_change") == 1).then(-pl.col("_ep_respot")).otherwise(pl.col("_ep_respot"))
                - pl.col("_ep_old")
            ).alias("_fixed_epa")
        )

        play_df = play_df.join(fixed.select("_qbepa_idx", "_fixed_epa"), on="_qbepa_idx", how="left").with_columns(
            pl.coalesce(pl.col("_fixed_epa"), pl.col("EPA")).cast(pl.Float64).alias("qb_epa")
        )
        return play_df.drop([c for c in ("_qbepa_idx", "_fixed_epa") if c in play_df.columns])

    def __process_wp(self, play_df):
        """Score the start-of-play ``wp`` (naive) and ``vegas_wp`` (spread) columns.

        Emits the model ``wp`` / ``vegas_wp`` / ``def_wp`` / ``home_wp`` /
        ``away_wp`` columns consistent with
        :func:`sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` so the ESPN construction
        path agrees with the nflverse path.  ``wp`` is scored from the bundled
        ``wp_naive.ubj`` model (11-feature) and ``vegas_wp`` from
        ``wp_spread.ubj`` (12-feature), both on the START feature view (the same
        view ``__process_wpa`` already builds for ``wp_before``).  The
        possession-team -> home perspective flip mirrors
        :func:`calculate_wpa` so ``home_wp`` is the home team's pre-snap WP.

        Must run BEFORE :meth:`__process_xpass` (xpass consumes ``wp`` /
        ``vegas_wp``).  All five columns are float64.
        """
        _wp_naive_model = _ep_wp_load_model("wp_naive.ubj")
        _wp_spread_model = _ep_wp_load_model("wp_spread.ubj")

        # Naive WP — START view, 11-feature (no spread_time).
        X_wp_naive = _espn_wp_features(
            play_df,
            receive_ko_col="start.pos_team_receives_2H_kickoff",
            spread_time_col="start.spread_time",
            home_col="start.is_home",
            half_sec_col="start.TimeSecsRem",
            game_sec_col="start.adj_TimeSecsRem",
            score_diff_col="pos_score_diff_start",
            down_col="start.down",
            ydstogo_col="start.distance",
            yardline_col="start.yardsToEndzone",
            pos_timeouts_col="start.posTeamTimeouts",
            def_timeouts_col="start.defPosTeamTimeouts",
            include_spread=False,
        )
        wp_naive = _wp_naive_model.predict(DMatrix(X_wp_naive, feature_names=WP_NAIVE_FEATURES))

        # Spread WP — START view, 12-feature (with spread_time).
        X_wp_spread = _espn_wp_features(
            play_df,
            receive_ko_col="start.pos_team_receives_2H_kickoff",
            spread_time_col="start.spread_time",
            home_col="start.is_home",
            half_sec_col="start.TimeSecsRem",
            game_sec_col="start.adj_TimeSecsRem",
            score_diff_col="pos_score_diff_start",
            down_col="start.down",
            ydstogo_col="start.distance",
            yardline_col="start.yardsToEndzone",
            pos_timeouts_col="start.posTeamTimeouts",
            def_timeouts_col="start.defPosTeamTimeouts",
            include_spread=True,
        )
        wp_spread = _wp_spread_model.predict(DMatrix(X_wp_spread, feature_names=WP_SPREAD_FEATURES))

        play_df = play_df.with_columns(
            pl.Series("wp", wp_naive, dtype=pl.Float64),
            pl.Series("vegas_wp", wp_spread, dtype=pl.Float64),
        ).with_columns(
            def_wp=1.0 - pl.col("wp"),
        )
        # Possession-team -> home perspective flip (mirror calculate_wpa).
        play_df = play_df.with_columns(
            home_wp=pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
            .then(pl.col("wp"))
            .otherwise(pl.col("def_wp")),
            away_wp=pl.when(pl.col("start.pos_team.id") != pl.col("homeTeamId"))
            .then(pl.col("wp"))
            .otherwise(pl.col("def_wp")),
        )
        return play_df

    def __process_xpass(self, play_df):
        """Add ``xpass`` (expected dropback probability) and ``pass_oe``.

        Delegates to :func:`sportsdataverse.nfl.ep_wp.calculate_xpass` — the same
        faithful nflfastR ``add_xpass`` scorer the nflverse path uses.  It scores
        only the ``valid_play`` subset (``season >= 2006``, dropback-eligible
        scrimmage plays with a valid down / score state) and nulls everywhere
        else (kicks, PATs, two-point tries).  ``calculate_xpass`` keys off
        nflverse column names, so this maps the ESPN frame to the minimal
        nflverse-shape inputs first, scores, and joins ``xpass`` / ``pass_oe``
        back onto the ESPN frame by a stable row index.

        Requires ``wp`` / ``vegas_wp`` (run after :meth:`__process_wp`).  If the
        ``xpass_model.ubj`` is unavailable offline the step degrades gracefully:
        ``xpass`` / ``pass_oe`` are emitted as all-null (with a ``RuntimeWarning``).
        """
        play_df = play_df.with_row_index("_xpass_join_idx")

        # Map ESPN play type text -> nflverse play_type for the valid_play filter.
        # rush==1 -> "run", pass==1 -> "pass"; penalties -> "no_play"; everything
        # else (kicks / PATs / 2pt) -> a non-eligible label so xpass nulls.
        nfl_play_type = (
            pl.when(pl.col("type.text") == "Penalty")
            .then(pl.lit("no_play"))
            .when(pl.col("pass") == True)
            .then(pl.lit("pass"))
            .when(pl.col("rush") == True)
            .then(pl.lit("run"))
            .otherwise(pl.lit("other"))
        )

        nflverse_view = play_df.select(
            pl.col("_xpass_join_idx"),
            pl.col("season"),
            nfl_play_type.alias("play_type"),
            pl.col("start.pos_team.id").cast(pl.Utf8).alias("posteam"),
            pl.col("start.is_home").alias("home_team_flag"),
            pl.col("start.down").alias("down"),
            pl.col("start.distance").alias("ydstogo"),
            pl.col("start.yardsToEndzone").alias("yardline_100"),
            pl.col("period").alias("qtr"),
            pl.col("wp"),
            pl.col("vegas_wp"),
            pl.col("pos_score_diff_start").alias("score_differential"),
            pl.col("start.TimeSecsRem").alias("half_seconds_remaining"),
            pl.col("start.posTeamTimeouts").alias("posteam_timeouts_remaining"),
            pl.col("start.defPosTeamTimeouts").alias("defteam_timeouts_remaining"),
            pl.col("pass").cast(pl.Int8).alias("pass"),
            pl.col("rush").cast(pl.Int8).alias("rush"),
        ).with_columns(
            # _make_cp_mutations derives `home` from posteam == home_team; the
            # ESPN frame already knows home via start.is_home, so synthesize a
            # home_team string that makes `home` resolve correctly.
            home_team=pl.when(pl.col("home_team_flag") == True).then(pl.col("posteam")).otherwise(pl.lit("__away__")),
            roof=pl.lit("retractable"),
        )

        try:
            scored = calculate_xpass(nflverse_view)
        except FileNotFoundError as exc:
            import warnings

            warnings.warn(
                f"NFLPlayProcess.__process_xpass: skipping xpass step — {type(exc).__name__}: {exc}.",
                RuntimeWarning,
                stacklevel=2,
            )
            return play_df.drop("_xpass_join_idx").with_columns(
                pl.lit(None, dtype=pl.Float64).alias("xpass"),
                pl.lit(None, dtype=pl.Float64).alias("pass_oe"),
            )

        xpass_frame = scored.select("_xpass_join_idx", "xpass", "pass_oe")
        play_df = play_df.join(xpass_frame, on="_xpass_join_idx", how="left").drop("_xpass_join_idx")
        return play_df

    def __process_fourth_down(self, play_df):
        """Attach the nfl4th 4th-down decision columns (default-on).

        The decision surface
        (:func:`sportsdataverse.nfl.nfl_fourth_down.get_4th_down_probs`) keys off
        nflverse column names, so this maps the ESPN frame to the minimal
        nflverse-shape 4th-down inputs, scores only the qualifying 4th-down rows
        (``start.down == 4`` with a non-null ``start.yardsToEndzone``), and joins
        the 14 decision columns back onto the full ESPN frame by play ``id``
        (non-4th-down rows receive null decision columns).

        The 4th-down models (``fd_model`` / ``wp_model``) are download-on-demand;
        when they are unavailable offline — or an incompatible cached booster
        raises — the columns are emitted as all-null (schema-stable) with a
        ``RuntimeWarning`` rather than failing the pipeline.
        """
        import sportsdataverse.nfl.nfl_fourth_down as _fd

        decision_cols = [
            "go_wp",
            "first_down_prob",
            "wp_succeed",
            "wp_fail",
            "fg_make_prob",
            "make_fg_wp",
            "miss_fg_wp",
            "fg_wp",
            "punt_wp",
            "go_boost",
            "go_wp_diff",
            "punt_wp_diff",
            "fg_wp_diff",
            "fourth_down_recommendation",
        ]

        def _with_null_decisions(frame):
            return frame.with_columns(
                [
                    (
                        pl.lit(None, dtype=pl.Utf8).alias(c)
                        if c == "fourth_down_recommendation"
                        else pl.lit(None, dtype=pl.Float64).alias(c)
                    )
                    for c in decision_cols
                    if c not in frame.columns
                ]
            )

        required = ("id", "start.down", "start.yardsToEndzone", "start.distance", "period")
        if any(c not in play_df.columns for c in required):
            return _with_null_decisions(play_df)

        fourth = play_df.filter((pl.col("start.down") == 4).and_(pl.col("start.yardsToEndzone").is_not_null()))
        if fourth.height == 0:
            return _with_null_decisions(play_df)

        # quarter_seconds_remaining from the within-quarter clock (minutes/seconds).
        qsr = (60 * pl.col("clock.minutes") + pl.col("clock.seconds")).cast(pl.Float64)
        # home_opening_kickoff: the home team received the opening kickoff iff it
        # does NOT receive the 2H kickoff.  start.pos_team_receives_2H_kickoff is a
        # per-play flag for the *current* posteam, so resolve it to the home team.
        home_receives_2h = (
            pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
            .then(pl.col("start.pos_team_receives_2H_kickoff"))
            .otherwise(~pl.col("start.pos_team_receives_2H_kickoff"))
        )

        nflverse_view = fourth.select(
            pl.col("id").alias("play_id"),
            pl.lit(int(self.gameId)).alias("game_id"),
            pl.col("season"),
            pl.col("start.pos_team.id").cast(pl.Utf8).alias("posteam"),
            pl.col("start.def_pos_team.id").cast(pl.Utf8).alias("defteam"),
            pl.col("homeTeamId").cast(pl.Utf8).alias("home_team"),
            pl.col("awayTeamId").cast(pl.Utf8).alias("away_team"),
            pl.lit("outdoors").alias("roof"),
            pl.col("period").alias("qtr"),
            qsr.alias("quarter_seconds_remaining"),
            pl.col("start.distance").cast(pl.Float64).alias("ydstogo"),
            pl.col("start.yardsToEndzone").cast(pl.Float64).alias("yardline_100"),
            pl.col("pos_score_diff_start").cast(pl.Float64).alias("score_differential"),
            pl.col("start.posTeamTimeouts").cast(pl.Int64).alias("posteam_timeouts_remaining"),
            pl.col("start.defPosTeamTimeouts").cast(pl.Int64).alias("defteam_timeouts_remaining"),
            home_receives_2h.cast(pl.Int64).alias("home_opening_kickoff"),
            # nflverse spread_line is home-team-favored-positive; homeTeamSpread is
            # the home team's point spread (negative when favored), so flip sign.
            (-pl.col("homeTeamSpread").cast(pl.Float64)).alias("spread_line"),
            pl.col("overUnder").cast(pl.Float64).alias("total_line"),
        )

        try:
            probs_pd = _fd.get_4th_down_probs(nflverse_view)
        except Exception as exc:  # noqa: BLE001 — degrade gracefully on any scoring failure
            import warnings

            warnings.warn(
                f"NFLPlayProcess.__process_fourth_down: skipping 4th-down decision step — {type(exc).__name__}: {exc}.",
                RuntimeWarning,
                stacklevel=2,
            )
            return _with_null_decisions(play_df)

        probs = pl.from_pandas(probs_pd)
        keep = ["play_id", *[c for c in decision_cols if c in probs.columns]]
        probs = probs.select(keep).rename({"play_id": "id"})
        # Align the join-key dtype to the left frame.
        if probs.schema["id"] != play_df.schema["id"]:
            probs = probs.with_columns(pl.col("id").cast(play_df.schema["id"]))

        play_df = play_df.drop([c for c in decision_cols if c in play_df.columns])
        return play_df.join(probs, on="id", how="left")

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
                    "start.ExpScoreDiff_Time_Ratio_touchback",
                ),
                (pl.col("start.ExpScoreDiff") / (pl.col("start.adj_TimeSecsRem") + 1)).alias(
                    "start.ExpScoreDiff_Time_Ratio",
                ),
                # ---- prepare variables for wp_after calculations ----
                pl.when(
                    ((pl.col("type.text").is_in(end_change_vec)).or_(pl.col("downs_turnover") == True))
                    .and_(pl.col("kickoff_play") == False)
                    .and_(pl.col("scoringPlay") == False),
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
                    .and_(pl.col("season") <= 2013),
                )
                .then(pl.col("pos_score_diff_end") - 0.92)
                .when(
                    (pl.col("scoringPlay") == True)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("season") <= 2013),
                )
                .then(pl.col("pos_score_diff_end") + 0.92)
                .when(
                    (pl.col("type.text") == "Timeout")
                    .and_(pl.col("lag_scoringPlay") == True)
                    .and_(pl.col("season") <= 2013),
                )
                .then(pl.col("pos_score_diff_end") + 0.92)
                .otherwise(pl.col("pos_score_diff_end"))
                .alias("end.ExpScoreDiff"),
            )
            .with_columns(
                (pl.col("end.ExpScoreDiff") / (pl.col("end.adj_TimeSecsRem") + 1)).alias("end.ExpScoreDiff_Time_Ratio"),
            )
        )

        # ---- wp_before ----
        _wp_model = _ep_wp_load_model("wp_spread.ubj")

        X_wp_tb = _espn_wp_features(
            play_df,
            receive_ko_col="start.pos_team_receives_2H_kickoff",
            spread_time_col="start.spread_time",
            home_col="start.is_home",
            half_sec_col="start.TimeSecsRem",
            game_sec_col="start.adj_TimeSecsRem",
            score_diff_col="pos_score_diff_start",
            down_col="start.down",
            ydstogo_col="start.distance",
            yardline_col="start.yardsToEndzone.touchback",
            pos_timeouts_col="start.posTeamTimeouts",
            def_timeouts_col="start.defPosTeamTimeouts",
        )
        WP_start_touchback = _wp_model.predict(DMatrix(X_wp_tb, feature_names=WP_SPREAD_FEATURES))

        X_wp_start = _espn_wp_features(
            play_df,
            receive_ko_col="start.pos_team_receives_2H_kickoff",
            spread_time_col="start.spread_time",
            home_col="start.is_home",
            half_sec_col="start.TimeSecsRem",
            game_sec_col="start.adj_TimeSecsRem",
            score_diff_col="pos_score_diff_start",
            down_col="start.down",
            ydstogo_col="start.distance",
            yardline_col="start.yardsToEndzone",
            pos_timeouts_col="start.posTeamTimeouts",
            def_timeouts_col="start.defPosTeamTimeouts",
        )
        WP_start = _wp_model.predict(DMatrix(X_wp_start, feature_names=WP_SPREAD_FEATURES))

        # ---- wp_after ----
        X_wp_end = _espn_wp_features(
            play_df,
            receive_ko_col="end.pos_team_receives_2H_kickoff",
            spread_time_col="end.spread_time",
            home_col="end.is_home",
            half_sec_col="end.TimeSecsRem",
            game_sec_col="end.adj_TimeSecsRem",
            score_diff_col="end.pos_score_diff",
            down_col="end.down",
            ydstogo_col="end.distance",
            yardline_col="end.yardsToEndzone",
            pos_timeouts_col="end.posTeamTimeouts",
            def_timeouts_col="end.defPosTeamTimeouts",
        )
        WP_end = _wp_model.predict(DMatrix(X_wp_end, feature_names=WP_SPREAD_FEATURES))

        # Attach the scored WP point estimates, then delegate the derivation half to
        # the shared, model-free calculate_wpa. calculate_wpa is the verbatim lift of
        # the kickoff-touchback overlay / posteam->home flip / lead_wp / end-of-game /
        # OT derivation that used to live inline here. NFLPlayProcess always operates
        # on a single game, so calculate_wpa's ``.over("game_id")`` window guards
        # collapse to the plain ``.shift`` / ``.max()`` semantics this method relied
        # on, leaving output identical. calculate_wpa also emits lowercase nflverse
        # aliases (wp / def_wp / home_wp / away_wp) the legacy __process_wpa output
        # never carried, so drop them to keep the plays schema byte-identical.
        # NOTE: the XGBoost ``predict`` arrays are native float32 (unlike the EP
        # arrays, which are float64 after the ``np.clip(probs @ point_values)``
        # matmul). ``pl.Series`` is given the array's native float32 dtype here so
        # the downstream ``calculate_wpa`` arithmetic runs at the exact same
        # precision the prior ``pl.lit(<f32 array>)`` produced -- forcing Float64
        # would widen the inputs and shift the WP cascade by ~1e-8. This is an
        # explicitness change only, byte-identical to the prior ``pl.lit`` form.
        play_df = play_df.with_columns(
            pl.Series("wp_before", WP_start, dtype=pl.Float32),
            pl.Series("wp_touchback", WP_start_touchback, dtype=pl.Float32),
            pl.Series("wp_after", WP_end, dtype=pl.Float32),
        )
        play_df = calculate_wpa(play_df).drop("wp", "def_wp", "home_wp", "away_wp")
        return play_df

    def __add_description_features(self, play_df):
        """Derive text-only model inputs from the ESPN play ``text`` column.

        nflverse's native pipeline extracts a handful of CP / context inputs
        straight from the play description (see
        ``native_pbp.description.add_description_features``).  The ESPN path
        lacks them, so this method mirrors the canonical regexes on the
        ``text`` column:

        * ``pass_length`` — ``"short"`` / ``"deep"`` (``pass (?:incomplete )?(short|deep)``).
        * ``pass_location`` — ``"left"`` / ``"middle"`` / ``"right"``
          (``(?:short|deep) (left|middle|right)``).
        * ``pass_middle`` — ``Int8`` 1 when ``pass_location == "middle"`` else 0
          (also 0 for non-pass / null text); wired into CP scoring so the model
          sees real middle-of-field info instead of the default 0.
        * ``shotgun`` — ``Int8`` 1 when the text contains ``"Shotgun"``.
        * ``no_huddle`` — ``Int8`` 1 when the text contains ``"No Huddle"``.

        Documented gaps (NOT text-derivable on the ESPN path): ``air_yards``
        (a tracking/GSIS measure, absent here) and ``qb_hit`` (left at 0).
        """
        text = pl.col("text").fill_null("")
        play_df = play_df.with_columns(
            pass_length=text.str.extract(r"pass (?:incomplete )?(short|deep)", 1),
            pass_location=text.str.extract(r"(?:short|deep) (left|middle|right)", 1),
            shotgun=text.str.contains("Shotgun").cast(pl.Int8),
            no_huddle=text.str.contains("No Huddle").cast(pl.Int8),
        )
        return play_df.with_columns(
            pass_middle=(pl.col("pass_location") == "middle").fill_null(False).cast(pl.Int8),
        )

    def __process_cp(self, play_df):
        """Score completion probability and CPOE for pass plays.

        Requires an ``air_yards`` column (e.g. merged from NGS data).  When
        the column is absent all rows receive null ``cp`` / ``cpoe`` values so
        downstream steps always find those columns present.
        """
        if "air_yards" not in play_df.columns:
            return play_df.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("cp"),
                pl.lit(None, dtype=pl.Float64).alias("cpoe"),
            )

        _cp_model = _ep_wp_load_model("cp_model.ubj")

        play_df = play_df.with_row_index("_cp_row_idx")
        pass_df = play_df.filter(pl.col("air_yards").is_not_null())

        if len(pass_df) > 0:
            X_cp = _espn_cp_features(
                pass_df,
                air_yards_col="air_yards",
                yardline_col="start.yardsToEndzone",
                ydstogo_col="start.distance",
                down1_col="down_1",
                down2_col="down_2",
                down3_col="down_3",
                down4_col="down_4",
                pass_middle_col="pass_middle",
                home_col="start.is_home",
            )
            cp_preds = _cp_model.predict(DMatrix(X_cp, feature_names=CP_FEATURES))
            cp_frame = pass_df.select("_cp_row_idx").with_columns(pl.Series("cp", cp_preds.tolist(), dtype=pl.Float64))
        else:
            cp_frame = pl.DataFrame(
                {"_cp_row_idx": pl.Series([], dtype=pl.UInt32), "cp": pl.Series([], dtype=pl.Float64)}
            )

        play_df = play_df.join(cp_frame, on="_cp_row_idx", how="left").drop("_cp_row_idx")

        # cpoe = actual completion - predicted completion probability
        if "completion" in play_df.columns:
            play_df = play_df.with_columns((pl.col("completion").cast(pl.Float64) - pl.col("cp")).alias("cpoe"))
        else:
            play_df = play_df.with_columns(pl.lit(None, dtype=pl.Float64).alias("cpoe"))

        return play_df

    def __process_xyac(self, play_df):
        """Add the five nflfastR xYAC columns (schema-stable; null on this path).

        nflfastR's xYAC is a single ``multi:softprob`` model whose five outputs
        (``xyac_epa``/``xyac_mean_yardage``/``xyac_median_yardage``/
        ``xyac_success``/``xyac_fd``) are *derived* by re-scoring expected points
        on each of 76 YAC outcomes — see
        :func:`sportsdataverse.nfl.ep_wp.calculate_xyac`.  The primary blocker
        on the ESPN path is ``air_yards`` itself — a tracking/GSIS measure ESPN
        never ships and that is not text-derivable (see
        ``__add_description_features``) — plus the nflverse-named model inputs
        (``yardline_100`` / ``ydstogo`` / ``down`` / ``receiver_player_name``
        / the ``complete_pass``-family flags).  ``air_epa`` is no longer a
        blocker in itself: on the nflverse path it is a real column
        (``ep_wp._derive_air_yac_epa``), and ``calculate_xyac`` consumes it
        verbatim when present or derives a catch-spot fallback when absent —
        but without ``air_yards`` neither the family nor xYAC can score here,
        so this in-pipeline ESPN path emits the five columns as nulls for
        schema stability.  Run ``calculate_xyac`` on a nflverse-format frame
        (or ``enrich_nfl_pbp``) to populate them.
        """
        return play_df.with_columns([pl.lit(None, dtype=pl.Float64).alias(c) for c in _XYAC_OUT_COLS])

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
                    pl.col("drive.result").str.to_lowercase().str.contains(r"(?i)punt|fumble|interception|downs"),
                ),
            )
            .with_columns(
                drive_start=pl.col("drive_start").cast(pl.Float32),
            )
            .with_columns(
                drive_play_index=pl.col("scrimmage_play").cum_sum().over("drive.id"),
            )
            .with_columns(
                drive_offense_plays=pl.when((pl.col("sp") == False).and_(pl.col("scrimmage_play") == True))
                .then(pl.col("play").cast(pl.Int32))
                .otherwise(0),
                prog_drive_EPA=pl.col("EPA_scrimmage").cum_sum().over("drive.id"),
                prog_drive_WPA=pl.col("wpa").cum_sum().over("drive.id"),
                drive_offense_yards=pl.when((pl.col("sp") == False).and_(pl.col("scrimmage_play") == True))
                .then(pl.col("statYardage"))
                .otherwise(0),
            )
            .with_columns(
                drive_total_yards=pl.col("drive_offense_yards").cum_sum().over("drive.id"),
            )
        )
        return play_df

    def __add_fixed_drives_series(self, play_df: pl.DataFrame) -> pl.DataFrame:
        """Add nflfastR-parity ``fixed_drive`` and ``series`` columns.

        Reconstructs nflverse/nflfastR's drive- and series-numbering scheme on
        top of the ESPN ``NFLPlayProcess`` frame so the ESPN-constructed output
        reaches parity with ``load_nfl_pbp`` on these columns. Ported faithfully
        from nflfastR ``R/helper_add_fixed_drives.R`` (``add_drive_results``)
        and ``R/helper_add_series_data.R`` (``add_series_data``); dplyr/
        ``data.table`` idioms are translated to polars 1.x.

        Adds five columns -- ``fixed_drive`` (int, shared across both teams,
        increments on a real possession change with nflfastR's PAT / onside /
        safety special cases), ``fixed_drive_result`` (str), ``series`` (int,
        increments on a new set of downs / first down / possession change),
        ``series_success`` (int 0/1), and ``series_result`` (str). The method is
        strictly additive -- every pre-existing column is left untouched and the
        ``_ff_*`` working columns are dropped before returning.

        nflfastR is keyed on its own column vocabulary; the ESPN frame uses
        different names, so the inputs are mapped first: ``pos_team`` ->
        posteam, ``def_pos_team`` -> defteam, ``half`` -> game_half,
        ``start.distance`` -> ydstogo, ``statYardage`` -> yards_gained,
        ``text`` -> desc. nflfastR signals absent from the ESPN frame
        (``play_type``, ``td_team``, ``first_down_*``, ``own_kickoff_recovery``,
        ``qb_kneel``, ``interception``) are derived from ESPN columns.

        Args:
            play_df (pl.DataFrame): The plays frame after ``__add_drive_data``,
                where ``pos_team`` / ``def_pos_team`` / down / yardage / scoring
                flags / ``end_of_half`` are all populated.

        Returns:
            pl.DataFrame: The same frame plus the five drive/series columns.
        """
        cols = play_df.columns
        txt = pl.col("text") if "text" in cols else pl.col("desc")
        type_txt = pl.col("type.text") if "type.text" in cols else pl.lit("")
        eoh = pl.col("end_of_half").cast(pl.Boolean) if "end_of_half" in cols else pl.lit(False)

        # ---- map nflfastR inputs from ESPN columns + derive missing signals ----
        play_df = (
            play_df.with_columns(
                pl.col("pos_team").alias("_ff_posteam"),
                pl.col("def_pos_team").alias("_ff_defteam"),
                pl.col("half").alias("_ff_half"),
                pl.col("down").alias("_ff_down"),
                pl.col("start.distance").alias("_ff_ydstogo"),
                pl.col("statYardage").alias("_ff_ygained"),
                txt.alias("_ff_desc"),
                pl.col("touchdown").cast(pl.Int8).alias("_ff_td"),
                pl.col("safety").cast(pl.Int8).alias("_ff_safety"),
                pl.col("fumble_lost").cast(pl.Int8).alias("_ff_fumlost"),
                pl.col("field_goal_result").alias("_ff_fgr"),
                pl.col("kickoff_play").cast(pl.Int8).alias("_ff_kick_att"),
                (pl.col("punt") | pl.col("punt_play")).cast(pl.Int8).alias("_ff_punt_att"),
                pl.col("fg_attempt").cast(pl.Int8).alias("_ff_fg"),
            )
            .with_columns(
                # nflfastR play_type, collapsed from ESPN type.text / rush / pass flags
                pl.when(pl.col("_ff_kick_att") == 1)
                .then(pl.lit("kickoff"))
                .when(pl.col("_ff_punt_att") == 1)
                .then(pl.lit("punt"))
                .when(pl.col("_ff_fg") == 1)
                .then(pl.lit("field_goal"))
                .when(pl.col("pass") == True)
                .then(pl.lit("pass"))
                .when(pl.col("rush") == True)
                .then(pl.lit("run"))
                .otherwise(pl.lit("no_play"))
                .alias("_ff_play_type"),
                # own_kickoff_recovery: kicking team retains a recovered kick
                (
                    (pl.col("kickoff_play") == True)
                    & ((pl.col("kickoff_onside") == True) | (pl.col("kickoff_downed") == True))
                    & (
                        (pl.col("fumble_recovered") == True)
                        | (pl.col("end.pos_team.id") == pl.col("start.def_pos_team.id"))
                    )
                )
                .cast(pl.Int8)
                .alias("_ff_own_kick_rec"),
            )
            .with_columns(
                # td_team: defense on return / interception TDs, else offense
                pl.when(pl.col("_ff_td") == 0)
                .then(None)
                .when(type_txt.str.contains(r"(?i)return touchdown") | pl.col("int_td"))
                .then(pl.col("def_pos_team"))
                .when(
                    txt.str.contains(r"(?i)intercept(ed)?.*touchdown|fumble.*return.*touchdown|return(ed)?.*touchdown")
                )
                .then(pl.col("def_pos_team"))
                .otherwise(pl.col("pos_team"))
                .alias("_ff_tdteam"),
                (pl.col("int_td") | txt.str.contains(r"(?i)intercept")).cast(pl.Int8).alias("_ff_int"),
                (txt.str.contains(r"(?i)kneel") & (pl.col("rush") == True)).cast(pl.Int8).alias("_ff_qbkneel"),
                # standalone timeout / two-minute-warning row detector. nflfastR keys
                # the L55-82 PAT-after-defensive-TD interleave on desc matching
                # "(Timeout)|(Two-Minute Warning)"; the ESPN analog is the play-type
                # label, which carries "Timeout" / "Official Timeout" / "Two-minute
                # warning". Match type.text (and the description as a fallback).
                (
                    type_txt.str.contains(r"(?i)timeout|two-minute warning")
                    | txt.str.contains(r"(?i)timeout|two-minute warning")
                )
                .cast(pl.Int8)
                .alias("_ff_to"),
                # ESPN omits nflfastR's "END QUARTER 2/4 / END GAME" desc markers; the
                # end_of_half boolean is the 1:1 ESPN analog for "End of half".
                (eoh | txt.str.contains(r"(END QUARTER 2)|(END QUARTER 4)|(END GAME)")).cast(pl.Int8).alias("_ff_eoh"),
            )
        )

        # per-play first down: non-scoring scrimmage play that advanced the chain
        # (same possession, end down resets to 1, yards gained >= distance) or a
        # penalty that converted a first down. This strips the post-touchdown /
        # dead-ball end.down==1 bookkeeping artifacts the raw flags carry.
        fd_gain = (
            (pl.col("scrimmage_play") == True)
            & (pl.col("end.down") == 1)
            & (pl.col("start.pos_team.id") == pl.col("end.pos_team.id"))
            & (pl.col("touchdown") == False)
            & (pl.col("statYardage") >= pl.col("start.distance"))
        )
        fd_pen = (
            (pl.col("penalty_1st_conv") == True)
            & (pl.col("touchdown") == False)
            & (pl.col("start.pos_team.id") == pl.col("end.pos_team.id"))
            if "penalty_1st_conv" in cols
            else pl.lit(False)
        )
        play_df = play_df.with_columns(
            (fd_gain & (pl.col("rush") == True)).cast(pl.Int8).alias("_ff_fd_rush"),
            (fd_gain & (pl.col("pass") == True)).cast(pl.Int8).alias("_ff_fd_pass"),
            (fd_pen | (fd_gain & (pl.col("pass") == False) & (pl.col("rush") == False)))
            .cast(pl.Int8)
            .alias("_ff_fd_pen"),
        )

        # ===================== fixed_drive (add_drive_results) =====================
        # posteam swap: on a recovered kickoff the kicking team (defteam) owns the
        # drive (helper_add_fixed_drives.R L15-27).
        play_df = play_df.with_columns(
            pl.when(
                (pl.col("_ff_kick_att") == 1) & ((pl.col("_ff_own_kick_rec") == 1) | (pl.col("_ff_fumlost") == 1)),
            )
            .then(pl.col("_ff_defteam"))
            .otherwise(pl.col("_ff_posteam"))
            .alias("_ff_pt"),
        ).with_columns(
            pl.int_range(pl.len()).over(["game_id", "_ff_half"]).alias("_ff_row"),
        )
        lag_pt = pl.col("_ff_pt").shift(1).over(["game_id", "_ff_half"])
        lag_pt2 = pl.col("_ff_pt").shift(2).over(["game_id", "_ff_half"])
        lag_pt3 = pl.col("_ff_pt").shift(3).over(["game_id", "_ff_half"])
        lag_td = pl.col("_ff_td").shift(1).over(["game_id", "_ff_half"])
        lag_td2 = pl.col("_ff_td").shift(2).over(["game_id", "_ff_half"])
        lag_td3 = pl.col("_ff_td").shift(3).over(["game_id", "_ff_half"])
        lag_tdteam = pl.col("_ff_tdteam").shift(1).over(["game_id", "_ff_half"])
        lag_tdteam2 = pl.col("_ff_tdteam").shift(2).over(["game_id", "_ff_half"])
        lag_tdteam3 = pl.col("_ff_tdteam").shift(3).over(["game_id", "_ff_half"])
        lag_to = pl.col("_ff_to").shift(1).over(["game_id", "_ff_half"])
        lag_to2 = pl.col("_ff_to").shift(2).over(["game_id", "_ff_half"])
        lag_fum = pl.col("_ff_fumlost").shift(1).over(["game_id", "_ff_half"])
        lag_fum2 = pl.col("_ff_fumlost").shift(2).over(["game_id", "_ff_half"])
        lag_ptype = pl.col("_ff_play_type").shift(1).over(["game_id", "_ff_half"])
        lag_ptype2 = pl.col("_ff_play_type").shift(2).over(["game_id", "_ff_half"])
        lag_safety = pl.col("_ff_safety").shift(1).over(["game_id", "_ff_half"])
        lag_safety2 = pl.col("_ff_safety").shift(2).over(["game_id", "_ff_half"])

        play_df = (
            play_df.with_columns(
                # change in posteam, incl. the t-2 / t-3 NA-posteam variants (L32-44)
                pl.when(
                    (pl.col("_ff_pt") != lag_pt)
                    | ((pl.col("_ff_pt") != lag_pt2) & lag_pt.is_null())
                    | ((pl.col("_ff_pt") != lag_pt3) & lag_pt2.is_null() & lag_pt.is_null()),
                )
                .then(1)
                .otherwise(0)
                .alias("_ff_nd"),
            )
            .with_columns(
                # PAT after a defensive TD is not a new drive (L45-54)
                pl.when((lag_td == 1) & (lag_pt != lag_tdteam) & lag_pt.is_not_null())
                .then(0)
                .otherwise(pl.col("_ff_nd"))
                .alias("_ff_nd"),
            )
            .with_columns(
                # PAT after a defensive TD is not a new drive even if a single
                # standalone Timeout / Two-minute-warning row follows the TD
                # (L55-67). The prior row is a timeout, the TD was 2 rows back, and
                # that TD was scored by the defense. A null lag => keep _ff_nd
                # (the R `missing = new_drive` clause): the `&` chain is null when any
                # lag is out of range, and pl.when treats null as the else branch.
                pl.when(
                    (lag_to == 1) & (lag_td2 == 1) & (lag_pt2 != lag_tdteam2),
                )
                .then(0)
                .otherwise(pl.col("_ff_nd"))
                .alias("_ff_nd"),
            )
            .with_columns(
                # PAT after a defensive TD is not a new drive even if TWO standalone
                # Timeout / Two-minute-warning rows follow the TD (L68-82). The prior
                # two rows are timeouts, the TD was 3 rows back, and that TD was
                # scored by the defense. Same null-keeps-_ff_nd semantics as above.
                pl.when(
                    (lag_to == 1) & (lag_to2 == 1) & (lag_td3 == 1) & (lag_pt3 != lag_tdteam3),
                )
                .then(0)
                .otherwise(pl.col("_ff_nd"))
                .alias("_ff_nd"),
            )
            .with_columns(
                # same team retains after a lost fumble on a punt/fg/pass/run that did
                # not score: it's a new drive (L83-113).
                pl.when(
                    (pl.col("_ff_nd") != 1)
                    & (
                        (
                            (pl.col("_ff_pt") == lag_pt)
                            & (lag_fum == 1)
                            & lag_ptype.is_in(["punt", "pass", "run", "field_goal"])
                            & (lag_td == 0)
                        )
                        | (
                            lag_pt.is_null()
                            & (pl.col("_ff_pt") == lag_pt2)
                            & (lag_fum2 == 1)
                            & lag_ptype2.is_in(["punt", "pass", "run", "field_goal"])
                            & (lag_td2 == 0)
                        )
                    ),
                )
                .then(1)
                .otherwise(pl.col("_ff_nd"))
                .alias("_ff_nd"),
            )
            .with_columns(
                # first observation of a half is a new drive (L114-115)
                pl.when(pl.col("_ff_row") == 0).then(1).otherwise(pl.col("_ff_nd")).alias("_ff_nd"),
            )
            .with_columns(
                # recovered onside / muffed kick is a new drive (L117-122)
                pl.when(
                    (pl.col("_ff_play_type") == "kickoff")
                    & ((pl.col("_ff_own_kick_rec") == 1) | (pl.col("_ff_fumlost") == 1)),
                )
                .then(1)
                .otherwise(pl.col("_ff_nd"))
                .alias("_ff_nd"),
            )
            .with_columns(
                # kickoff after a safety is a new drive (L124-134)
                pl.when(
                    ((pl.col("_ff_kick_att") == 1) & (lag_safety == 1))
                    | (
                        (pl.col("_ff_kick_att") == 1)
                        & (lag_safety2 == 1)
                        & (lag_ptype.is_null() | (lag_ptype == "no_play"))
                    ),
                )
                .then(1)
                .otherwise(pl.col("_ff_nd"))
                .alias("_ff_nd"),
            )
            .with_columns(
                pl.col("_ff_nd").fill_null(0).alias("_ff_nd"),
            )
            .with_columns(
                fixed_drive=pl.col("_ff_nd").cum_sum().over("game_id"),
            )
        )

        play_df = play_df.with_columns(
            # per-play candidate drive result (L142-158)
            pl.when((pl.col("_ff_td") == 1) & (pl.col("_ff_pt") == pl.col("_ff_tdteam")))
            .then(pl.lit("Touchdown"))
            .when((pl.col("_ff_td") == 1) & (pl.col("_ff_pt") != pl.col("_ff_tdteam")))
            .then(pl.lit("Opp touchdown"))
            .when(pl.col("_ff_fgr") == "made")
            .then(pl.lit("Field goal"))
            .when(pl.col("_ff_fgr").is_in(["blocked", "missed"]))
            .then(pl.lit("Missed field goal"))
            .when(pl.col("_ff_safety") == 1)
            .then(pl.lit("Safety"))
            .when((pl.col("_ff_play_type") == "punt") | (pl.col("_ff_punt_att") == 1))
            .then(pl.lit("Punt"))
            .when((pl.col("_ff_int") == 1) | (pl.col("_ff_fumlost") == 1))
            .then(pl.lit("Turnover"))
            .when(
                (pl.col("_ff_down") == 4)
                & (pl.col("_ff_ygained") < pl.col("_ff_ydstogo"))
                & (pl.col("_ff_play_type") != "no_play"),
            )
            .then(pl.lit("Turnover on downs"))
            .when(pl.col("_ff_eoh") == 1)
            .then(pl.lit("End of half"))
            .otherwise(None)
            .alias("_ff_tmp"),
        )
        last_tmp = pl.col("_ff_tmp").drop_nulls().last().over(["game_id", "fixed_drive"])
        first_tmp = pl.col("_ff_tmp").drop_nulls().first().over(["game_id", "fixed_drive"])
        play_df = play_df.with_columns(
            # end-of-half drives take the first result seen, else the last (L162-168)
            fixed_drive_result=pl.when(last_tmp == "End of half").then(first_tmp).otherwise(last_tmp),
        )

        # ======================= series (add_series_data) ========================
        play_df = play_df.with_columns(
            pl.int_range(pl.len()).over(["game_id", "_ff_half"]).alias("_ff_srow"),
        )
        lag_fd = pl.col("fixed_drive").shift(1).over(["game_id", "_ff_half"])
        lag_fdr = pl.col("_ff_fd_rush").shift(1).over(["game_id", "_ff_half"])
        lag_fdp = pl.col("_ff_fd_pass").shift(1).over(["game_id", "_ff_half"])
        lag_fdn = pl.col("_ff_fd_pen").shift(1).over(["game_id", "_ff_half"])
        lag_td_s = pl.col("_ff_td").shift(1).over(["game_id", "_ff_half"])
        play_df = (
            play_df.with_columns(
                # new series: a new drive, a non-TD first down on the prior play, or the
                # first play of the half (L35-47)
                pl.when(
                    (pl.col("fixed_drive") != lag_fd)
                    | (((lag_fdr == 1) | (lag_fdp == 1) | (lag_fdn == 1)) & (lag_td_s == 0))
                    | (pl.col("_ff_srow") == 0),
                )
                .then(1)
                .otherwise(0)
                .alias("_ff_ns"),
            )
            .with_columns(
                pl.col("_ff_ns").fill_null(0).alias("_ff_ns"),
            )
            .with_columns(
                series=pl.col("_ff_ns").cum_sum().over("game_id"),
            )
        )
        play_df = play_df.with_columns(
            # per-play candidate series result (L54-75): note "First down" and
            # "QB kneel" precede the scoring branches, unlike the drive result.
            pl.when(
                ((pl.col("_ff_fd_pen") == 1) | (pl.col("_ff_fd_rush") == 1) | (pl.col("_ff_fd_pass") == 1))
                & (pl.col("_ff_td") == 0),
            )
            .then(pl.lit("First down"))
            .when((pl.col("_ff_td") == 1) & (pl.col("_ff_pt") == pl.col("_ff_tdteam")))
            .then(pl.lit("Touchdown"))
            .when((pl.col("_ff_td") == 1) & (pl.col("_ff_pt") != pl.col("_ff_tdteam")))
            .then(pl.lit("Opp touchdown"))
            .when(pl.col("_ff_fgr") == "made")
            .then(pl.lit("Field goal"))
            .when(pl.col("_ff_fgr").is_in(["blocked", "missed"]))
            .then(pl.lit("Missed field goal"))
            .when(pl.col("_ff_safety") == 1)
            .then(pl.lit("Safety"))
            .when((pl.col("_ff_play_type") == "punt") | (pl.col("_ff_punt_att") == 1))
            .then(pl.lit("Punt"))
            .when((pl.col("_ff_int") == 1) | (pl.col("_ff_fumlost") == 1))
            .then(pl.lit("Turnover"))
            .when(
                (pl.col("_ff_down") == 4)
                & (pl.col("_ff_ygained") < pl.col("_ff_ydstogo"))
                & (pl.col("_ff_play_type") != "no_play"),
            )
            .then(pl.lit("Turnover on downs"))
            .when(pl.col("_ff_qbkneel") == 1)
            .then(pl.lit("QB kneel"))
            .when(pl.col("_ff_eoh") == 1)
            .then(pl.lit("End of half"))
            .otherwise(None)
            .alias("_ff_stmp"),
        )
        last_stmp = pl.col("_ff_stmp").drop_nulls().last().over(["game_id", "series"])
        first_stmp = pl.col("_ff_stmp").drop_nulls().first().over(["game_id", "series"])
        play_df = play_df.with_columns(
            series_result=pl.when(last_stmp == "End of half").then(first_stmp).otherwise(last_stmp),
        ).with_columns(
            # series_success: 1 if the series ended in a TD or first down (L86-90)
            series_success=pl.when(pl.col("series_result").is_in(["Touchdown", "First down"])).then(1).otherwise(0),
        )

        # drop all working columns -- strictly additive output
        drop_cols = [c for c in play_df.columns if c.startswith("_ff_")]
        play_df = play_df.drop(drop_cols)
        return play_df

    def __cast_box_score_column(self, play_df, column, target_type):
        if column in play_df.columns:
            play_df = play_df.with_columns(pl.col(column).cast(target_type).alias(column))
        else:
            play_df = play_df.with_columns((pl.Null).alias(column))
        return play_df

    def create_box_score(self, play_df):
        """Build the advanced box score (passer / rusher / receiver / team / situational / defensive / turnover / drives)
        from a feature-engineered plays DataFrame.

        This is normally called by ``run_processing_pipeline()`` -- it
        auto-runs the pipeline first if it hasn't been triggered yet, so
        callers can also invoke it directly on a freshly-instantiated
        processor.

        Args:
            play_df (pl.DataFrame): The plays frame produced after the full
                feature-engineering chain (downs, play-type flags, EPA,
                WPA, drive aggregation).

        Returns:
            Dict[str, list]: Box score keyed by ``"pass"``, ``"rush"``,
            ``"receiver"``, ``"team"``, ``"situational"``, ``"defensive"``,
            ``"turnover"``, ``"drives"`` -- each value a list of dicts
            ready to be serialized.

        Example:
            Run the pipeline and pull out the box score::

                from sportsdataverse.nfl import NFLPlayProcess
                proc = NFLPlayProcess(gameId=401671801)
                proc.espn_nfl_pbp()
                result = proc.run_processing_pipeline()
                box = result["advBoxScore"]
                sorted(box.keys())
        """
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
            .group_by(["pos_team", "passer_player_name"])
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
            & (pl.col("athlete_name").is_in(qbs_list)),
        )
        pass_qbr = (
            pass_qbr_box.group_by(["pos_team", "athlete_name"])
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

        dtest_qbr = DMatrix(pass_qbr[qbr_vars], feature_names=list(qbr_vars))
        qbr_result = qbr_model.predict(dtest_qbr)
        pass_qbr = pass_qbr.with_columns(exp_qbr=pl.lit(qbr_result))
        passer_box = passer_box.join(
            pass_qbr,
            left_on=["passer_player_name", "pos_team"],
            right_on=["athlete_name", "pos_team"],
        )

        rusher_box = (
            rush_box.fill_null(0.0)
            .group_by(["pos_team", "rusher_player_name"])
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
            .group_by(["pos_team", "receiver_player_name"])
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
            play_df.group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
                (pl.col("rush") == True) & (pl.col("scrimmage_play") == True) & (pl.col("opportunity_run") == True),
            )
            .fill_null(0.0)
            .group_by(["pos_team"])
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
        team_box = reduce(
            lambda left, right: left.join(right, on=["pos_team"], how="full", coalesce=True),
            team_data_frames,
        )

        situation_box_normal = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["pos_team"])
            .agg(
                EPA_success=pl.col("EPA_success").sum(),
                EPA_success_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_rz = (
            play_df.filter((pl.col("rz_play") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_rz=pl.col("EPA_success").sum(),
                EPA_success_rate_rz=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_third = (
            play_df.filter((pl.col("start.down") == 3) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_third=pl.col("EPA_success").sum(),
                EPA_success_rate_third=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_pass=pl.col("EPA_success").sum(),
                EPA_success_pass_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_rush=pl.col("EPA_success").sum(),
                EPA_success_rush_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_middle8 = (
            play_df.filter((pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
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
            play_df.filter((pl.col("pass") == True) & (pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
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
            play_df.filter((pl.col("rush") == True) & (pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
                (pl.col("pass") == True) & (pl.col("early_down") == True) & (pl.col("scrimmage_play") == True),
            )
            .group_by(["pos_team"])
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
                (pl.col("rush") == True) & (pl.col("early_down") == True) & (pl.col("scrimmage_play") == True),
            )
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            .group_by(["pos_team"])
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
            lambda left, right: left.join(right, on=["pos_team"], how="full", coalesce=True),
            situation_data_frames,
        )

        play_df = play_df.with_columns(
            drive_stopped=pl.col("drive_stopped").cast(pl.Float32),
            drive_start=pl.col("drive_start").cast(pl.Float32),
        )

        def_base_box = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["def_pos_team"])
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
            .group_by(["def_pos_team"])
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
            .group_by(["def_pos_team"])
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
        def_box = reduce(
            lambda left, right: left.join(right, on=["def_pos_team"], how="full", coalesce=True),
            def_data_frames,
        )
        def_box_json = json.loads(def_box.write_json())

        turnover_box = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["pos_team"])
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

        turnover_box_json = json.loads(turnover_box.write_json())
        if len(turnover_box_json) < 2:
            for i in range(len(turnover_box_json), 2):
                turnover_box_json.append({})

        turnover_box_json[0]["Int"] = int(turnover_box_json[0].get("Int", 0))
        turnover_box_json[1]["Int"] = int(turnover_box_json[1].get("Int", 0))

        away_passes_def = turnover_box_json[0].get("pass_breakups", 0)
        away_passes_int = turnover_box_json[0].get("Int", 0)
        away_fumbles = turnover_box_json[0].get("total_fumbles", 0)
        turnover_box_json[0]["expected_turnovers"] = (0.5 * away_fumbles) + (0.22 * (away_passes_def + away_passes_int))

        home_passes_def = turnover_box_json[1].get("pass_breakups", 0)
        home_passes_int = turnover_box_json[1].get("Int", 0)
        home_fumbles = turnover_box_json[1].get("total_fumbles", 0)
        turnover_box_json[1]["expected_turnovers"] = (0.5 * home_fumbles) + (0.22 * (home_passes_def + home_passes_int))

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
            .group_by(["pos_team"])
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
            "pass": json.loads(passer_box.write_json()),
            "rush": json.loads(rusher_box.write_json()),
            "receiver": json.loads(receiver_box.write_json()),
            "team": json.loads(team_box.write_json()),
            "situational": json.loads(situation_box.write_json()),
            "defensive": def_box_json,
            "turnover": turnover_box_json,
            "drives": json.loads(drives_data.write_json()),
        }

    def run_processing_pipeline(self):
        """Run the full feature-engineering pipeline against ``self.json``.

        Pipes the plays frame through the chain of helpers: downs,
        play-type flags, rush/pass flags, team-score variables, new play
        types, penalties, play-category flags, yardage cols, player cols,
        post-play cols, spread time, EPA, WPA, drive data, and QBR --
        followed by the advanced box score build.

        Returns:
            Dict | None: The full processed game dict (or the subset
            specified by ``return_keys`` at construction). Returns the
            partial result when ``corrupt_pbp_check()`` short-circuits.

        Example:
            Standard end-to-end run::

                from sportsdataverse.nfl import NFLPlayProcess
                proc = NFLPlayProcess(gameId=401671801)
                proc.espn_nfl_pbp()
                result = proc.run_processing_pipeline()
                len(result["plays"]), len(result["drives"])

            Subset returned keys for downstream serialization::

                proc = NFLPlayProcess(
                    gameId=401671801,
                    return_keys=["plays", "advBoxScore", "winprobability"],
                )
                proc.espn_nfl_pbp()
                slim = proc.run_processing_pipeline()
                sorted(slim.keys())
        """
        if self.ran_pipeline == False:
            pbp_txt = self.__helper_nfl_pbp_drives(self.json)
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
                    .pipe(self.__add_description_features)
                    .pipe(self.__process_epa)
                    .pipe(self.__process_qb_epa)
                    .pipe(self.__process_wpa)
                    .pipe(self.__process_wp)
                    .pipe(self.__process_cp)
                    .pipe(self.__process_xpass)
                    .pipe(self.__process_xyac)
                    .pipe(self.__add_drive_data)
                    .pipe(self.__add_fixed_drives_series)
                    .pipe(self.__process_fourth_down)
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
        """Run the lighter cleaning pipeline against ``self.json``.

        Identical to ``run_processing_pipeline()`` up through the
        ``__add_spread_time`` step but stops short of EPA / WPA / QBR /
        drive aggregation and the advanced box score. Use this when you
        want clean play structure without the modeled features.

        Returns:
            Dict: The cleaned game dict (or the subset specified by
            ``return_keys`` at construction).

        Example:
            Lighter run -- drop the modeled features::

                from sportsdataverse.nfl import NFLPlayProcess
                proc = NFLPlayProcess(gameId=401671801)
                proc.espn_nfl_pbp()
                cleaned = proc.run_cleaning_pipeline()
                "plays" in cleaned and "advBoxScore" not in cleaned
        """
        if self.ran_cleaning_pipeline == False:
            pbp_txt = self.__helper_nfl_pbp_drives(self.json)
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
                    .pipe(self.__add_description_features)
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
        """Detect ESPN payloads that look corrupt or partial.

        Returns ``True`` when one of three guard conditions trips:

        * No plays at all.
        * Fewer than 50 plays for a game ESPN reports as completed.
        * More than 500 plays for a game ESPN reports as completed.

        ``run_processing_pipeline()`` and ``run_cleaning_pipeline()`` use
        this to skip feature engineering on obviously broken payloads.

        Returns:
            bool: ``True`` if the payload looks corrupt; ``False`` otherwise.

        Example:
            Verify before running expensive feature engineering::

                from sportsdataverse.nfl import NFLPlayProcess
                proc = NFLPlayProcess(gameId=401671801)
                proc.espn_nfl_pbp()
                if not proc.corrupt_pbp_check():
                    result = proc.run_processing_pipeline()
        """
        if len(self.json["plays"]) == 0:
            logging.debug(
                f"{self.gameId}: appear to be too no plays available ({len(self.json['plays'])}). run_processing_pipeline did not run",
            )
            return True
        if (len(self.json["plays"]) < 50) and (
            self.json.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(
                f"{self.gameId}: appear to be too few plays ({len(self.json['plays'])}) for a completed game. run_processing_pipeline did not run",
            )
            return True
        if (len(self.json["plays"]) > 500) and (
            self.json.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(
                f"{self.gameId}: appear to be too many plays ({len(self.json['plays'])}) for a completed game. run_processing_pipeline did not run",
            )
            return True
        return False
