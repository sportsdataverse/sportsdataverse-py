from __future__ import annotations

import json
import os
import re

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse._espn_basketball_pbp import release_frame as _release_frame
from sportsdataverse._espn_basketball_pbp import hoopr_coordinates as _hoopr_coordinates
from sportsdataverse._espn_basketball_pbp import period_end_seconds as _period_end_seconds
from sportsdataverse._espn_basketball_pbp import pickcenter_odds as _pickcenter_odds
from sportsdataverse._espn_basketball_pbp import team_timeout_called as _team_timeout_called
from sportsdataverse._codegen_runtime import _download_json
from sportsdataverse.dl_utils import download, flatten_json_iterative
from sportsdataverse.nba.nba_play_by_play import helper_nba_play_by_play


def espn_nba_pbp(game_id: int, raw: bool = False, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs):
    """espn_nba_pbp() - Pull one game's play-by-play from ESPN's `nba/summary` endpoint.

    By default the plays come back as the frame the ``espn_nba_pbp`` release publishes (what
    :func:`~sportsdataverse.nba.load_nba_pbp` reads): snake_case columns, Int32 team / athlete /
    game ids, the Int64 play ``id``, ``game_date`` / ``game_date_time``, the hoopR shot
    coordinates, ``athlete_name_{1,2,3}``, and ``home_score`` / ``away_score`` rebuilt from the
    scoring plays when that reproduces the final (``score_source`` says which).

    Args:
        game_id (int): Unique game_id, can be obtained from nba_schedule().
        raw (bool): If True, returns the raw json from the API endpoint (wins over
            ``return_parsed``).
        return_parsed (bool): If True (default), returns the release play-by-play frame. If
            False, returns the game dictionary (the shape before 0.2.0, which
            ``helper_nba_play_by_play`` / ``helper_nba_team_box`` / ``helper_nba_player_box``
            and ``game_page_frames`` read).
        return_as_pandas (bool): If True, returns the frame as pandas instead of polars. Ignored
            with ``raw=True`` or ``return_parsed=False``, which return a dict.

    Returns:
        pl.DataFrame: One row per play, columns (in order) and dtypes as the release
        (``load_nba_pbp``) for that season; a pandas frame with ``return_as_pandas=True``.
        Zero rows, with the release's columns, when ESPN has no play-by-play for the game
        (``playByPlaySource == "none"``) or 10 or fewer plays (hoopR returns NULL).

        With ``return_parsed=False``, a dictionary of game data with keys - "gameId", "plays",
        "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource",
        "standings", "leaders", "seasonseries", "timeouts", "pickcenter", "againstTheSpread",
        "odds", "predictor", "espnWP", "gameInfo", "season".

    Example:
        Quick start::

            from sportsdataverse.nba import espn_nba_pbp
            plays = espn_nba_pbp(game_id=401585183)
            print(plays.shape)

        Pipeline next step (made threes)::

            import polars as pl
            plays.filter(pl.col("scoring_play") == True, pl.col("score_value") == 3).height

        The game dictionary (box score, header, pickcenter, ...)::

            pbp = espn_nba_pbp(game_id=401585183, return_parsed=False)
            print(list(pbp.keys()))

        Pull only the raw ESPN summary payload (skip cleaning)::

            raw_pbp = espn_nba_pbp(game_id=401585183, raw=True)

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA PBP
            * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball parallel
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    # play by play
    pbp_txt = {"timeouts": {}}
    # summary endpoint for pickcenter array
    summary_url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
    summary = _download_json(download, summary_url, **kwargs)

    incoming_keys_expected = [
        "boxscore",
        "format",
        "gameInfo",
        "leaders",
        "seasonseries",
        "broadcasts",
        "predictor",
        "pickcenter",
        "againstTheSpread",
        "odds",
        "winprobability",
        "header",
        "plays",
        "article",
        "videos",
        "standings",
        "teamInfo",
        "espnWP",
        "season",
        "timeouts",
    ]
    dict_keys_expected = ["boxscore", "format", "gameInfo", "predictor", "article", "header", "season", "standings"]
    # the expected keys in a fixed order, a missing one empty ({} for dict keys, [] for arrays)
    pbp_json = {
        k: summary[k] if k in summary else ({} if k in dict_keys_expected else []) for k in incoming_keys_expected
    }
    if raw:
        return pbp_json
    pbp_txt.update(pbp_json)

    game = helper_nba_pbp(game_id, pbp_txt)
    if not return_parsed:
        return game
    plays = _release_frame(helper_nba_play_by_play(game), game, "load_nba_pbp")
    # the release loader's conversion, so nullable ids stay integer (a plain to_pandas() makes them float)
    return plays.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else plays


def nba_pbp_disk(game_id, path_to_json):
    """Load a previously cached ESPN NBA summary JSON for a game from disk.

    Reads ``{path_to_json}/{game_id}.json``.

    Args:
        game_id (int): ESPN game / event identifier.
        path_to_json (str): Directory containing the cached JSON file.

    Returns:
        dict: Parsed JSON contents.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_pbp_disk
            pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
            print(list(pbp.keys()))
    """
    with open(os.path.join(path_to_json, f"{game_id}.json"), encoding="utf-8") as json_file:
        pbp_txt = json.load(json_file)
    return pbp_txt


def helper_nba_pbp(game_id, pbp_txt):
    """Internal helper that runs the ESPN summary payload through pickcenter,
    game-data, and feature pipelines and returns the cleaned dictionary
    consumed by :func:`espn_nba_pbp`.

    Args:
        game_id (int): ESPN game / event identifier.
        pbp_txt (dict): Trimmed ESPN summary payload (already filtered to
            the keys :func:`espn_nba_pbp` keeps).

    Returns:
        dict: Cleaned game payload with cleaned plays, boxscore, broadcasts,
        odds, etc.

    Example:
        Used internally by :func:`espn_nba_pbp`::

            from sportsdataverse.nba import espn_nba_pbp
            pbp = espn_nba_pbp(game_id=401585183, return_parsed=False)
    """
    init = helper_nba_pickcenter(pbp_txt)
    pbp_txt, init = helper_nba_game_data(pbp_txt, init)
    if "plays" in pbp_txt.keys() and pbp_txt.get("header").get("competitions")[0].get("playByPlaySource") != "none":
        pbp_txt = helper_nba_pbp_features(game_id, pbp_txt, init)
    else:
        pbp_txt["plays"] = pl.DataFrame()
        pbp_txt["timeouts"] = {
            init["homeTeamId"]: {"1": [], "2": []},
            init["awayTeamId"]: {"1": [], "2": []},
        }
    return {
        "gameId": game_id,
        "plays": pbp_txt["plays"].to_dicts(),
        "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
        "boxscore": pbp_txt["boxscore"],
        "header": pbp_txt["header"],
        "format": pbp_txt["format"],
        "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
        "videos": np.array(pbp_txt["videos"]).tolist(),
        "playByPlaySource": pbp_txt["playByPlaySource"],
        "standings": pbp_txt["standings"],
        "article": pbp_txt["article"],
        "leaders": np.array(pbp_txt["leaders"]).tolist(),
        "seasonseries": np.array(pbp_txt["seasonseries"]).tolist(),
        "timeouts": pbp_txt["timeouts"],
        "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
        "againstTheSpread": np.array(pbp_txt["againstTheSpread"]).tolist(),
        "odds": np.array(pbp_txt["odds"]).tolist(),
        "predictor": pbp_txt["predictor"],
        "espnWP": np.array(pbp_txt["espnWP"]).tolist(),
        "gameInfo": pbp_txt["gameInfo"],
        "teamInfo": np.array(pbp_txt["teamInfo"]).tolist(),
        "season": np.array(pbp_txt["season"]).tolist(),
    }


def helper_nba_game_data(pbp_txt, init):
    """Internal helper that lifts home/away team identification fields from the
    ESPN summary payload onto the cleaned ``pbp_txt`` and ``init`` dictionaries.

    Args:
        pbp_txt (dict): ESPN summary payload.
        init (dict): Pickcenter-derived spread / favorite / over-under metadata.

    Returns:
        tuple[dict, dict]: ``(pbp_txt, init)`` with team-id, mascot, location,
        abbreviation, and alt-name fields populated for both sides.

    Example:
        Used internally by :func:`espn_nba_pbp`::

            from sportsdataverse.nba import espn_nba_pbp
            pbp = espn_nba_pbp(game_id=401585183, return_parsed=False)
    """
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


def helper_nba_pbp_features(game_id, pbp_txt, init):
    """Internal helper that builds the polars play-by-play frame and timeout
    metadata from the ESPN summary payload.

    Adds clock decomposition (minutes/seconds), per-quarter and per-half
    seconds-remaining columns, half/quarter lag-lead helpers, and a per-game
    timeout map keyed by team id and half.

    Args:
        game_id (int): ESPN game / event identifier.
        pbp_txt (dict): ESPN summary payload (with ``plays`` already lifted).
        init (dict): Output of :func:`helper_nba_pickcenter` plus team-id
            metadata from :func:`helper_nba_game_data`.

    Returns:
        dict: ``pbp_txt`` mutated with ``plays`` (a polars DataFrame) and
        ``timeouts`` populated.

    Example:
        Used internally by :func:`espn_nba_pbp`::

            from sportsdataverse.nba import espn_nba_pbp
            pbp = espn_nba_pbp(game_id=401585183, return_parsed=False)
    """
    pbp_txt["plays_mod"] = []
    for play in pbp_txt["plays"]:
        p = flatten_json_iterative(play)
        pbp_txt["plays_mod"].append(p)
    pbp_txt["plays"] = pl.from_pandas(pd.json_normalize(pbp_txt, "plays_mod"))
    # hoopR / wehoop shot coordinates; ESPN's values kept as coordinate.{x,y}.raw (C-M20)
    pbp_txt["plays"] = _hoopr_coordinates(pbp_txt["plays"], init["homeTeamId"])
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
            # Defensive cast: ESPN sometimes returns these as python floats
            # / bools (no .astype()), sometimes as numpy arrays. `.first()`
            # collapses the resulting Series-of-length-1 to a scalar so
            # polars 1.x doesn't refuse to broadcast it across the
            # per-play DataFrame. Same shape fix as cfb_pbp / nfl_pbp.
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
            pl.col("clock.minutes").cast(pl.Int32),
            pl.col("clock.seconds").cast(pl.Float64),
            _team_timeout_called(pbp_txt["plays"].columns, init, "home").alias("homeTimeoutCalled"),
            _team_timeout_called(pbp_txt["plays"].columns, init, "away").alias("awayTimeoutCalled"),
        )
        .with_columns(
            half=pl.when(pl.col("qtr") <= 2).then(1).otherwise(2),
            game_half=pl.when(pl.col("qtr") <= 2).then(1).otherwise(2),
        )
        .with_columns(
            lag_qtr=pl.col("qtr").shift(1),
            lead_qtr=pl.col("qtr").shift(-1),
            lag_half=pl.col("half").shift(1),
            lead_half=pl.col("half").shift(-1),
        )
        .with_columns(
            (60 * pl.col("clock.minutes") + pl.col("clock.seconds")).alias("start.quarter_seconds_remaining"),
            pl.when(pl.col("qtr").is_in([1, 3]))
            .then(720 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .alias("start.half_seconds_remaining"),
            pl.when(pl.col("qtr") == 1)
            .then(2160 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .when(pl.col("qtr") == 2)
            .then(1440 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .when(pl.col("qtr") == 3)
            .then(720 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .alias("start.game_seconds_remaining"),
        )
        .with_columns(
            pl.col("start.quarter_seconds_remaining").shift(-1).alias("end.quarter_seconds_remaining"),
            pl.col("start.half_seconds_remaining").shift(-1).alias("end.half_seconds_remaining"),
            pl.col("start.game_seconds_remaining").shift(-1).alias("end.game_seconds_remaining"),
        )
    )

    pbp_txt["plays"] = pbp_txt["plays"].with_row_index("game_play_number", offset=1)
    pbp_txt["timeouts"] = {
        init["homeTeamId"]: {"1": [], "2": []},
        init["awayTeamId"]: {"1": [], "2": []},
    }
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
    pbp_txt["plays"] = pbp_txt["plays"].with_columns(
        *_period_end_seconds("quarter", ("half", "game")),
        pl.col("qtr").cast(pl.Int32).alias("period"),
    )
    return pbp_txt


def helper_nba_pickcenter(pbp_txt):
    """Internal helper that extracts spread / over-under / home-favorite info
    from the ESPN ``pickcenter`` array.

    A game with no line gets a null spread, over/under (the provider's own, if it has
    one) and home favorite, with ``gameSpreadAvailable`` False -- never an invented line.

    Args:
        pbp_txt (dict): ESPN summary payload.

    Returns:
        dict: ``{"gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable"}``.

    Example:
        Used internally by :func:`espn_nba_pbp`::

            from sportsdataverse.nba import espn_nba_pbp
            pbp = espn_nba_pbp(game_id=401585183, return_parsed=False)
    """
    # No line: null spread / over-under / favorite, as wehoop/hoopR have it (no invented 2.5 / 215.5 / home favored).
    return _pickcenter_odds(pbp_txt.get("pickcenter"), None, default_spread=None)
