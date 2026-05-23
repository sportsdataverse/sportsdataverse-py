"""sportsdataverse.nhl.nhl_api — **DEPRECATED**.

These functions target ``statsapi.web.nhl.com/api/v1/``, which the NHL
**retired in September 2023**. Calls return HTTP 404 in production.

Migration: use :mod:`sportsdataverse.nhl.nhl_api_web` instead.

| Deprecated here            | Replacement in :mod:`nhl_api_web` |
|----------------------------|------------------------------------|
| :func:`nhl_api_pbp`        | :func:`nhl_web_pbp`                |
| :func:`nhl_api_schedule`   | :func:`nhl_web_schedule`           |

The endpoint paths, return shapes, and game-id semantics all differ between
the old Stats API and the new ``api-web.nhle.com/v1/`` surface. See the
``nhl_api_web`` module docstring for the conventions.
"""

from __future__ import annotations

import warnings
from typing import Dict

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download


def _warn_deprecated_statsapi(replacement: str) -> None:
    """Emit a DeprecationWarning pointing to the modern replacement."""
    warnings.warn(
        f"sportsdataverse.nhl.nhl_api targets the deprecated "
        f"`statsapi.web.nhl.com/api/v1/` host (retired Sep 2023, returns 404 "
        f"in production). Use `sportsdataverse.nhl.{replacement}` instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def nhl_api_pbp(game_id: int, **kwargs) -> Dict:
    """nhl_api_pbp() - **DEPRECATED** — pull a game from ``statsapi.web.nhl.com``.

    .. deprecated::
       This function targets the NHL Stats API endpoint that was retired in
       September 2023. Use :func:`sportsdataverse.nhl.nhl_web_pbp` instead,
       which hits the current ``api-web.nhle.com/v1/gamecenter/{gid}/play-by-play``
       endpoint.

       Original docstring follows for archival reference.

    Args:
        game_id (int): Unique game_id, can be obtained from nhl_schedule().

    Returns:
        Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
         "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "pickcenter", "againstTheSpread",
         "odds", "onIce", "gameInfo", "season"

    Example:
        Pull a single game's metadata via the legacy NHL Stats API endpoint::

            from sportsdataverse.nhl import nhl_api_pbp
            game = nhl_api_pbp(game_id=2021020079)
            sorted(game.keys())  # ['datetime', 'game', 'gameId', 'gameLink', 'players', 'status', 'teams', 'venues']
            print(game["gameId"], game["status"]["abstractGameState"])

        Inspect the home / away team summary blocks::

            game["teams"]["home"]["name"], game["teams"]["away"]["name"]

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    _warn_deprecated_statsapi("nhl_web_pbp")
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
    """nhl_api_schedule() - **DEPRECATED** — pull the schedule from ``statsapi.web.nhl.com``.

    .. deprecated::
       This function targets the retired NHL Stats API. Use
       :func:`sportsdataverse.nhl.nhl_web_schedule` instead — which hits
       ``api-web.nhle.com/v1/schedule/{date}`` and returns a week-of-games
       payload (the modern API uses 7-day rolls rather than open ranges).

       Original docstring follows.

    Args:
        start_date (str): Start date to pull the NHL API schedule.
        end_date (str): End date to pull the NHL API schedule.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the schedule for the requested seasons.

    Example:
        Pull a one-week schedule slice::

            from sportsdataverse.nhl import nhl_api_schedule
            sched = nhl_api_schedule(start_date="2021-10-23", end_date="2021-10-28")
            print(sched.shape)
            sched.select(["gamePk", "gameDate", "teams.home.team.name", "teams.away.team.name"]).head()

        Pandas round-trip::

            sched_pd = nhl_api_schedule(
                start_date="2021-10-23", end_date="2021-10-28", return_as_pandas=True
            )
            sched_pd[["gamePk", "gameDate", "status.detailedState"]].head()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    _warn_deprecated_statsapi("nhl_web_schedule")
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
