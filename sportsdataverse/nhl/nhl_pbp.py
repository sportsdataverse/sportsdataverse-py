from __future__ import annotations

import json
import os
import re
from typing import Dict

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _download_json
from sportsdataverse.dl_utils import download, flatten_json_iterative, key_check


def espn_nhl_pbp(game_id: int, raw=False, **kwargs) -> Dict:
    """espn_nhl_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary`

    .. note::

        This is the **ESPN** NHL play-by-play, not the modern NHL
        api-web one. The two surfaces have different ID spaces and
        different schemas — they are NOT interchangeable:

        - ``espn_nhl_pbp(game_id)`` uses **ESPN event IDs** (e.g.
          ``401559395``). Returns a Dict of ~17 sub-frames matching
          the ESPN Site v2 summary shape (boxscore / plays / leaders /
          standings / etc.). Useful for historical alignment with the
          hoopR / wehoop R-package data stack.
        - ``nhl_web_pbp(game_id)`` + ``parse_nhl_web_pbp(payload)``
          uses **NHL native game IDs** (e.g. ``2023030417``). Returns
          the modern api-web.nhle.com PBP shape (``plays[]`` with
          ``eventId``, ``typeCode``, ``typeDescKey``,
          ``periodDescriptor``, nested ``details``). Use this for live
          games + modern NHL.com source-of-truth data.

        Pick the surface that matches your ID space + downstream join
        keys. The two cannot be cross-referenced by ``game_id``.

    Args:
        game_id (int): Unique ESPN event id (NOT the NHL native game
            id), can be obtained from nhl_schedule().

    Returns:
        Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
         "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "pickcenter", "againstTheSpread",
         "odds", "onIce", "gameInfo", "season"

    Example:
        Pull a single game's parsed feed (Stanley Cup Finals 2023 game)::

            from sportsdataverse.nhl import espn_nhl_pbp
            game = espn_nhl_pbp(game_id=401559395)
            list(game.keys())  # 'gameId', 'plays', 'boxscore', ...

        Inspect parsed plays and a quick filter on goal events::

            import polars as pl
            plays = pl.DataFrame(game["plays"])
            print(plays.shape)
            goals = plays.filter(pl.col("type.text") == "Goal")
            goals.select(["period", "time", "text"]).head()

        Pull the unparsed payload for custom downstream parsing::

            raw = espn_nhl_pbp(game_id=401559395, raw=True)
            sorted(raw.keys())[:5]

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    pbp_txt = {}
    summary_url = f"http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event={game_id}"
    summary = _download_json(download, summary_url, **kwargs)
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
    """Read a saved ESPN NHL play-by-play payload from disk.

    Args:
        game_id: The ESPN game id; the file read is ``{game_id}.json``.
        path_to_json: The directory holding the saved payloads.

    Returns:
        dict: The payload exactly as saved (the raw ESPN summary JSON), ready for
            ``helper_nhl_pbp``.

    Raises:
        FileNotFoundError: There is no ``{game_id}.json`` in ``path_to_json``.
    """
    with open(os.path.join(path_to_json, f"{game_id}.json"), encoding="utf-8") as json_file:
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
    spread = init["gameSpread"]
    pbp_txt["homeTeamSpread"] = None if spread is None else (abs(spread) if init["homeFavorite"] else -abs(spread))
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
    # One provider is a line (was `> 1`). A missing value stays None: no invented 1.5 / 5.5 / home favored.
    pickcenter = pd.json_normalize(pbp_txt.get("pickcenter") or [])
    if "provider.id" in pickcenter.columns:
        pickcenter = pickcenter.sort_values(by=["provider.id"])

    def first(col):
        vals = pickcenter[col].dropna() if col in pickcenter.columns else pd.Series(dtype=object)
        return vals.iloc[0] if len(vals) else None

    gameSpread = first("spread")
    favorite = first("homeTeamOdds.favorite")
    overUnder = first("overUnder")
    return {
        "gameSpread": None if gameSpread is None else float(gameSpread),
        "overUnder": None if overUnder is None else float(overUnder),
        # no favorite flag: the spread is the home line, so a negative one is a home favorite (none at 0)
        "homeFavorite": bool(favorite) if favorite is not None else (None if not gameSpread else bool(gameSpread < 0)),
        "gameSpreadAvailable": gameSpread is not None,
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
            # helper_nhl_pickcenter returns Python scalars or None (no line), so homeTeamSpread is null too.
            gameSpread=pl.lit(init["gameSpread"], dtype=pl.Float64).abs(),
            homeFavorite=pl.lit(init["homeFavorite"], dtype=pl.Boolean),
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
            # polars 1.x deprecated `n_field_strategy=` and now requires
            # `upper_bound=` (or `fields=`). MM:SS game clocks always
            # split into exactly 2 fields.
            .list.to_struct(fields=["clock.minutes", "clock.seconds"])
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
        # ESPN's NHL clock is ELAPSED time in the period (0:00 at the opening faceoff,
        # 20:00 at the horn), not time remaining as in basketball. A regulation period is
        # 1200 s; a playoff (season type 3) overtime is 1200 s, a regular-season overtime
        # 300 s, and the regular-season shootout (period 5, every clock 0:00) has no clock.
        # In overtime game_seconds_remaining is the OT period clock (fastRhockey nhl_game_feed.R
        # convention, no regulation offset), so it rises again at each OT period start.
        .with_columns(
            pl.when((pl.col("period.number") <= 3) | (pl.col("seasonType").cast(pl.Int64, strict=False) == 3))
            .then(pl.lit(1200.0))
            .when(pl.col("period.number") == 4)
            .then(pl.lit(300.0))
            .otherwise(pl.lit(0.0))
            .alias("_period_length"),
            pl.when(pl.col("period.number") <= 3)
            .then(1200.0 * (3 - pl.col("period.number")))
            .otherwise(pl.lit(0.0))
            .alias("_game_offset"),
        )
        .with_columns(
            (pl.col("_period_length") - 60 * pl.col("clock.minutes") - pl.col("clock.seconds")).alias(
                "start.period_seconds_remaining"
            ),
        )
        .with_columns(
            (pl.col("_game_offset") + pl.col("start.period_seconds_remaining")).alias("start.game_seconds_remaining"),
        )
        .drop("_period_length", "_game_offset")
        .with_columns(
            pl.col("start.period_seconds_remaining").shift(-1).alias("end.period_seconds_remaining"),
            pl.col("start.game_seconds_remaining").shift(-1).alias("end.game_seconds_remaining"),
        )
    )

    pbp_txt["plays"] = pbp_txt["plays"].with_row_index("game_play_number", offset=1)

    # The last row of a period ends at 0:00 of its own period, not at the next period's
    # first start (which restarts the clock); every other row ends at the next row's start.
    # Same design as basketball's period_end_seconds (#841); kept local until both live on
    # main, when the quality program's task 6.1 folds the two into one helper.
    ends_period = pl.col("period.number") != pl.col("period.number").shift(-1)
    left_in_period = pl.col("start.period_seconds_remaining")
    pbp_txt["plays"] = pbp_txt["plays"].with_columns(
        [
            pl.when(ends_period)
            .then(pl.col(f"start.{s}_seconds_remaining") - left_in_period)
            .otherwise(pl.col(f"end.{s}_seconds_remaining"))
            .alias(f"end.{s}_seconds_remaining")
            for s in ("period", "game")
        ]
    )

    return pbp_txt
