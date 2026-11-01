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
from sportsdataverse.mbb.mbb_play_by_play import helper_mbb_play_by_play
from sportsdataverse.mbb.mbb_player_box import helper_mbb_player_box
from sportsdataverse.mbb.mbb_team_box import helper_mbb_team_box


def espn_mbb_pbp(game_id: int, raw: bool = False, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs):
    """espn_mbb_pbp() - Pull one game's play-by-play from ESPN's `mens-college-basketball/summary` endpoint.

    By default the plays come back as the frame the ``espn_mens_college_basketball_pbp`` release
    publishes (what :func:`~sportsdataverse.mbb.load_mbb_pbp` reads): snake_case columns, Int32
    team / athlete / game ids, the Int64 play ``id``, ``game_date`` / ``game_date_time``, the
    hoopR shot coordinates, ``athlete_name_{1,2,3}``, and ``home_score`` / ``away_score``
    rebuilt from the scoring plays when that reproduces the final (``score_source`` says which).
    The release's season-level ``pregame_home_prob`` / ``home_win_prob`` are not added.

    Args:
        game_id (int): Unique game_id, can be obtained from mbb_schedule().
        raw (bool): If True, returns the raw json from the API endpoint (wins over
            ``return_parsed``).
        return_parsed (bool): If True (default), returns the release play-by-play frame. If
            False, returns the game dictionary (the shape before 0.2.0, which
            ``helper_mbb_play_by_play`` / ``helper_mbb_team_box`` / ``helper_mbb_player_box``
            and ``game_page_frames`` read).
        return_as_pandas (bool): If True, returns the frame as pandas instead of polars. Ignored
            with ``raw=True`` or ``return_parsed=False``, which return a dict.

    Returns:
        pl.DataFrame: One row per play, columns (in order) and dtypes as the release
        (``load_mbb_pbp``) for that season; a pandas frame with ``return_as_pandas=True``.
        Zero rows, with the release's columns, when ESPN has no play-by-play for the game
        (``playByPlaySource == "none"``) or 10 or fewer plays (hoopR returns NULL).
        A game with no betting line has null spread, home-favorite and home-team-spread
        values on every play and the spread-available flag False (hoopR's NA), never a
        default line.

        With ``return_parsed=False``, a dictionary of game data with keys: "gameId", "plays",
        "winprobability", "boxscore", "header", "format", "broadcasts", "videos",
        "playByPlaySource", "standings", "article", "leaders", "timeouts", "pickcenter",
        "againstTheSpread", "odds", "predictor", "espnWP", "gameInfo", "teamInfo", "season".

    Example:
        Quick start (2024 NCAA Division I men's championship game)::

            from sportsdataverse.mbb import espn_mbb_pbp
            plays = espn_mbb_pbp(game_id=401638637)
            print(plays.shape)

        Filter shooting plays for a basic shot chart::

            import polars as pl
            shots = plays.filter(pl.col("shooting_play") == True)
            shots.select(
                [
                    "period_number",
                    "clock_display_value",
                    "team_id",
                    "coordinate_x",
                    "coordinate_y",
                    "score_value",
                    "text",
                ]
            ).head()

        As pandas::

            plays_pd = espn_mbb_pbp(game_id=401638637, return_as_pandas=True)
            plays_pd[plays_pd["shooting_play"] == True].head()

        The game dictionary (box score, header, pickcenter, ...)::

            game = espn_mbb_pbp(game_id=401638637, return_parsed=False)
            print(game["gameId"], len(game["plays"]))

        Raw payload (skip the cleaning pipeline) for debugging::

            raw = espn_mbb_pbp(game_id=401638637, raw=True)
            sorted(raw.keys())

        See Also:
            * `hoopR`_ - R sister package; mirrors this surface for men's basketball
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com

    """
    summary = _espn_mbb_summary_json(game_id, **kwargs)
    return _mbb_pbp_from_summary(
        game_id, summary, raw=raw, return_parsed=return_parsed, return_as_pandas=return_as_pandas
    )


def _espn_mbb_summary_json(game_id, **kwargs) -> dict:
    """The raw ESPN ``summary`` payload for one game (the one request every per-game wrapper makes)."""
    summary_url = (
        f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
    )
    return _download_json(download, summary_url, **kwargs)


def _mbb_pbp_from_summary(game_id, summary: dict, *, raw: bool, return_parsed: bool, return_as_pandas: bool):
    """``espn_mbb_pbp``'s processing of an already-fetched ``summary`` payload."""
    pbp_txt = {"timeouts": {}}
    incoming_keys_expected = [
        "boxscore",
        "format",
        "gameInfo",
        "leaders",
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
    dict_keys_expected = [
        "plays",
        "videos",
        "broadcasts",
        "pickcenter",
        "againstTheSpread",
        "odds",
        "winprobability",
        "teamInfo",
        "espnWP",
        "leaders",
    ]
    # array_keys_expected = ["boxscore", "format", "gameInfo", "predictor", "article", "header", "season", "standings"]
    if raw:
        # reorder keys in raw format, appending empty keys which are defined later to the end
        pbp_json = {}
        for k in incoming_keys_expected:
            if k in summary.keys():
                pbp_json[k] = summary[k]
            else:
                pbp_json[k] = {} if k in dict_keys_expected else []
        return pbp_json

    for k in incoming_keys_expected:
        if k in summary.keys():
            pbp_txt[k] = summary[k]
        else:
            pbp_txt[k] = {} if k in dict_keys_expected else []

    for k in ["news", "shop"]:
        pbp_txt.pop(f"{k}", None)

    game = helper_mbb_pbp(game_id, pbp_txt)
    if not return_parsed:
        return game
    plays = _release_frame(helper_mbb_play_by_play(game), game, "load_mbb_pbp")
    # the release loader's conversion, so nullable ids stay integer (a plain to_pandas() makes them float)
    return plays.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else plays


def espn_mbb_team_box(game_id: int, return_as_pandas: bool = False, **kwargs):
    """espn_mbb_team_box() - Pull one game's team box score from ESPN's `mens-college-basketball/summary` endpoint.

    The hoopR name for this request (``hoopR::espn_mbb_team_box``): one ``summary`` fetch run
    through :func:`~sportsdataverse.mbb.helper_mbb_team_box`, so the frame is the one the
    ``espn_mens_college_basketball_team_boxscores`` release publishes (what
    :func:`~sportsdataverse.mbb.load_mbb_team_boxscore` reads) for that game.

    Args:
        game_id (int): Unique game_id, can be obtained from ``espn_mbb_schedule()``.
        return_as_pandas (bool): If True, returns a pandas frame instead of polars.
        **kwargs: Forwarded to the HTTP download (``timeout``, ``proxy``, ...).

    Returns:
        pl.DataFrame: Two rows, one per team, with the release's team-box columns. A frame with
        no columns when ESPN has no box score for the game.

    Example:
        Quick start (2024 NCAA Division I men's championship game)::

            from sportsdataverse.mbb import espn_mbb_team_box
            team_box = espn_mbb_team_box(game_id=401638637)
            print(team_box.shape)

        Pipeline next step (one line)::

            team_box.select("team_display_name", "team_score", "team_winner")

    See Also:
        * :func:`~sportsdataverse.mbb.espn_mbb_game_all` -- plays and both box scores from one request.
        * `hoopR`_ -- R sister package; ``espn_mbb_team_box`` there.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    box = helper_mbb_team_box(_espn_mbb_summary_json(game_id, **kwargs))
    return box.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else box


def espn_mbb_player_box(game_id: int, return_as_pandas: bool = False, **kwargs):
    """espn_mbb_player_box() - Pull one game's player box score from ESPN's `mens-college-basketball/summary` endpoint.

    The hoopR name for this request (``hoopR::espn_mbb_player_box``): one ``summary`` fetch run
    through :func:`~sportsdataverse.mbb.helper_mbb_player_box`, so the frame is the one the
    ``espn_mens_college_basketball_player_boxscores`` release publishes (what
    :func:`~sportsdataverse.mbb.load_mbb_player_boxscore` reads) for that game.

    Args:
        game_id (int): Unique game_id, can be obtained from ``espn_mbb_schedule()``.
        return_as_pandas (bool): If True, returns a pandas frame instead of polars.
        **kwargs: Forwarded to the HTTP download (``timeout``, ``proxy``, ...).

    Returns:
        pl.DataFrame: One row per athlete (did-not-play athletes included, with null stats), away
        team first, with the release's player-box columns. A frame with no columns when ESPN has no
        box score for the game.

    Example:
        Quick start (2024 NCAA Division I men's championship game)::

            from sportsdataverse.mbb import espn_mbb_player_box
            player_box = espn_mbb_player_box(game_id=401638637)
            print(player_box.shape)

        Pipeline next step (one line)::

            player_box.filter(pl.col("did_not_play") == False).select("athlete_display_name", "points")

    See Also:
        * :func:`~sportsdataverse.mbb.espn_mbb_game_all` -- plays and both box scores from one request.
        * `hoopR`_ -- R sister package; ``espn_mbb_player_box`` there.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    box = helper_mbb_player_box(_espn_mbb_summary_json(game_id, **kwargs))
    return box.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else box


def espn_mbb_game_all(game_id: int, return_as_pandas: bool = False, **kwargs):
    """espn_mbb_game_all() - Pull one game's plays, team box and player box from one ESPN request.

    The hoopR name for this request (``hoopR::espn_mbb_game_all``): a single `mens-college-basketball/summary`
    fetch, returned as the three frames :func:`espn_mbb_pbp`, :func:`espn_mbb_team_box` and
    :func:`espn_mbb_player_box` would return one request each.

    Args:
        game_id (int): Unique game_id, can be obtained from ``espn_mbb_schedule()``.
        return_as_pandas (bool): If True, the three frames are pandas instead of polars.
        **kwargs: Forwarded to the HTTP download (``timeout``, ``proxy``, ...).

    Returns:
        dict: hoopR's list names as keys -- ``"Plays"`` (the ``load_mbb_pbp`` release frame),
        ``"Team"`` (the team box) and ``"Player"`` (the player box).

    Example:
        Quick start (2024 NCAA Division I men's championship game)::

            from sportsdataverse.mbb import espn_mbb_game_all
            game = espn_mbb_game_all(game_id=401638637)
            print(game["Plays"].shape, game["Team"].shape, game["Player"].shape)

        Pipeline next step (one line)::

            game["Player"].sort("points", descending=True).select("athlete_display_name", "points").head()

    See Also:
        * `hoopR`_ -- R sister package; ``espn_mbb_game_all`` there.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    summary = _espn_mbb_summary_json(game_id, **kwargs)
    # the box helpers first: the play-by-play processing reshapes parts of the payload in place
    team = helper_mbb_team_box(summary)
    player = helper_mbb_player_box(summary)
    plays = _mbb_pbp_from_summary(game_id, summary, raw=False, return_parsed=True, return_as_pandas=return_as_pandas)
    if return_as_pandas:
        team = team.to_pandas(use_pyarrow_extension_array=True)
        player = player.to_pandas(use_pyarrow_extension_array=True)
    return {"Plays": plays, "Team": team, "Player": player}


def mbb_pbp_disk(game_id, path_to_json):
    """Read a saved ESPN MBB play-by-play payload from disk.

    Args:
        game_id: The ESPN game id; the file read is ``{game_id}.json``.
        path_to_json: The directory holding the saved payloads.

    Returns:
        dict: The payload exactly as saved (the raw ESPN summary JSON), ready for
            ``helper_mbb_pbp``.

    Raises:
        FileNotFoundError: There is no ``{game_id}.json`` in ``path_to_json``.
    """
    with open(os.path.join(path_to_json, f"{game_id}.json"), encoding="utf-8") as json_file:
        pbp_txt = json.load(json_file)
    return pbp_txt


def helper_mbb_pbp(game_id, pbp_txt):
    init = helper_mbb_pickcenter(pbp_txt)
    pbp_txt, init = helper_mbb_game_data(pbp_txt, init)

    if "plays" in pbp_txt.keys() and pbp_txt.get("header").get("competitions")[0].get("playByPlaySource") != "none":
        pbp_txt = helper_mbb_pbp_features(game_id, pbp_txt, init)
    else:
        pbp_txt["plays"] = pl.DataFrame()
        pbp_txt["timeouts"] = {
            init["homeTeamId"]: {"1": [], "2": []},
            init["awayTeamId"]: {"1": [], "2": []},
        }
    # pbp_txt['plays'] = pbp_txt['plays'].replace({np.nan: None})
    return {
        "gameId": int(game_id),
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


def helper_mbb_game_data(pbp_txt, init):
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


def helper_mbb_pickcenter(pbp_txt):
    # No line: null spread / over-under / favorite, as hoopR has it (no invented 2.5 / 142 / home favored).
    return _pickcenter_odds(pbp_txt.get("pickcenter"), None, default_spread=None)


def helper_mbb_pbp_features(game_id, pbp_txt, init):
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
            season=pl.lit(pbp_txt["header"]["season"]["year"]),
            seasonType=pl.lit(pbp_txt["header"]["season"]["type"]),
            homeTeamId=pl.lit(init["homeTeamId"]),
            homeTeamName=pl.lit(init["homeTeamName"]),
            homeTeamMascot=pl.lit(init["homeTeamMascot"]),
            homeTeamAbbrev=pl.lit(init["homeTeamAbbrev"]),
            homeTeamNameAlt=pl.lit(init["homeTeamNameAlt"]),
            awayTeamId=pl.lit(init["awayTeamId"]),
            awayTeamName=pl.lit(init["awayTeamName"]),
            awayTeamMascot=pl.lit(init["awayTeamMascot"]),
            awayTeamAbbrev=pl.lit(init["awayTeamAbbrev"]),
            awayTeamNameAlt=pl.lit(init["awayTeamNameAlt"]),
            # A game with no line keeps these null (helper_mbb_pickcenter), so homeTeamSpread is null too.
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
            pl.col("period.number").cast(pl.Int32).alias("half"),
            pl.col("clock.displayValue").alias("time"),
            # polars 1.x deprecated `n_field_strategy=` and now requires
            # `upper_bound=` (or an explicit `fields=` sequence). MM:SS
            # game clocks always split into exactly 2 fields, so
            # `upper_bound=2` is correct + tighter than the legacy
            # heuristic.
            # A bare sub-minute clock ("23.4", as the NBA/WNBA/WBB paths accept)
            # reads as 0:23; whole-second MM:SS clocks parse exactly as before.
            pl.when(pl.col("clock.displayValue").str.contains(":"))
            .then(pl.col("clock.displayValue"))
            .otherwise("0:" + pl.col("clock.displayValue"))
            .str.split(":")
            .list.to_struct(fields=["clock.minutes", "clock.seconds"])
            .alias("clock.mm"),
        )
        .with_columns(pl.col("clock.mm").struct.rename_fields(["clock.minutes", "clock.seconds"]))
        .unnest("clock.mm")
        .with_columns(
            pl.col("clock.minutes").cast(pl.Float64).cast(pl.Int32),
            pl.col("clock.seconds").cast(pl.Float64).cast(pl.Int32),
            _team_timeout_called(pbp_txt["plays"].columns, init, "home").alias("homeTimeoutCalled"),
            _team_timeout_called(pbp_txt["plays"].columns, init, "away").alias("awayTimeoutCalled"),
        )
        .with_columns(
            lag_period=pl.col("period.number").shift(1),
            lead_period=pl.col("period.number").shift(-1),
            lag_half=pl.col("half").shift(1),
            lead_half=pl.col("half").shift(-1),
        )
        .with_columns(
            (60 * pl.col("clock.minutes") + pl.col("clock.seconds")).alias("start.period_seconds_remaining"),
            pl.when(pl.col("period.number") == 1)
            .then(1200 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
            .alias("start.game_seconds_remaining"),
        )
        .with_columns(
            pl.col("start.period_seconds_remaining").shift(-1).alias("end.period_seconds_remaining"),
            pl.col("start.game_seconds_remaining").shift(-1).alias("end.game_seconds_remaining"),
        )
    )
    pbp_txt["timeouts"] = {
        init["homeTeamId"]: {"1": [], "2": []},
        init["awayTeamId"]: {"1": [], "2": []},
    }
    pbp_txt["timeouts"][init["homeTeamId"]]["1"] = (
        pbp_txt["plays"]
        .filter((pl.col("homeTimeoutCalled") == True).and_(pl.col("period.number") <= 1))
        .get_column("id")
        .to_list()
    )
    pbp_txt["timeouts"][init["homeTeamId"]]["2"] = (
        pbp_txt["plays"]
        .filter((pl.col("homeTimeoutCalled") == True).and_(pl.col("period.number") > 1))
        .get_column("id")
        .to_list()
    )
    pbp_txt["timeouts"][init["awayTeamId"]]["1"] = (
        pbp_txt["plays"]
        .filter((pl.col("awayTimeoutCalled") == True).and_(pl.col("period.number") <= 1))
        .get_column("id")
        .to_list()
    )
    pbp_txt["timeouts"][init["awayTeamId"]]["2"] = (
        pbp_txt["plays"]
        .filter((pl.col("awayTimeoutCalled") == True).and_(pl.col("period.number") > 1))
        .get_column("id")
        .to_list()
    )

    pbp_txt["plays"] = pbp_txt["plays"].with_row_index("game_play_number", offset=1)

    pbp_txt["plays"] = pbp_txt["plays"].with_columns(_period_end_seconds("period", ("game",)))

    return pbp_txt
