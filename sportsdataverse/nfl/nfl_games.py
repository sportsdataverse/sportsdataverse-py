from __future__ import annotations

import json
from typing import Dict

import requests


def nfl_token_gen():
    """Mint a fresh ``api.nfl.com`` access token via the public reroute endpoint.

    Wraps the unauthenticated ``client_credentials`` grant the NFL.com web
    app uses. The returned bearer token is what ``nfl_headers_gen()`` puts
    on the ``Authorization`` header.

    Returns:
        str: The access token string.

    Example:
        Mint a token and inspect its prefix::

            from sportsdataverse.nfl.nfl_games import nfl_token_gen
            token = nfl_token_gen()
            assert isinstance(token, str)

        Pair with a downstream call (``nfl_headers_gen`` does this for you)::

            import requests
            token = nfl_token_gen()
            headers = {"Authorization": f"Bearer {token}"}
    """
    url = "https://api.nfl.com/v1/reroute"

    # TODO: resolve if DNT or x-domain-id are necessary.  pulled them from chrome inspector
    payload = "grant_type=client_credentials"
    headers = {"DNT": "1", "x-domain-id": "100", "Content-Type": "application/x-www-form-urlencoded"}

    response = requests.request("POST", url, headers=headers, data=payload)

    return json.loads(response.content)["access_token"]


def nfl_headers_gen():
    """Build the full request-header dict expected by ``api.nfl.com``.

    Mints a fresh bearer token via :func:`nfl_token_gen` and combines it
    with the browser-style headers (``Origin``, ``Referer``, ``User-Agent``,
    ``Sec-Fetch-*``, etc.) the NFL.com web app sends on every request.

    Returns:
        Dict[str, str]: Header dict ready to drop into ``requests.get``.

    Example:
        Reuse one header set across many calls::

            from sportsdataverse.nfl.nfl_games import (
                nfl_headers_gen, nfl_game_schedule,
            )
            hdrs = nfl_headers_gen()
            week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
            week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
    """
    token = nfl_token_gen()
    return {
        "Host": "api.nfl.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nfl.com/",
        "authorization": f"Bearer {token}",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-User": "?1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }


def nfl_game_details(game_id=None, headers=None, raw=False) -> Dict:
    """nfl_game_details() -- pull full ``api.nfl.com`` game details by game id.

    Args:
        game_id (str): UUID-style game id from ``api.nfl.com`` (e.g. ``'7ae87c4c-d24c-11ec-b23d-d15a91047884'``).
        headers (Dict[str, str] | None): Pre-built header dict (skip the auth roundtrip).
            Defaults to a fresh ``nfl_headers_gen()`` call.
        raw (bool): If True, return the ESPN payload untouched. If False (default),
            normalize keys to the expected schema (filling missing keys with
            empty dicts/lists).

    Returns:
        Dict: Dictionary of game details (drives, plays, scoring summaries,
        timeouts, weather, attendance, etc.).

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_games import nfl_game_details
            details = nfl_game_details(game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884")
            sorted(details.keys())[:5]

        Reuse headers across many calls (avoids re-minting tokens)::

            from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
            hdrs = nfl_headers_gen()
            details = nfl_game_details(
                game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", headers=hdrs
            )

        Raw passthrough::

            raw = nfl_game_details(
                game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", raw=True
            )
    """
    if headers is None:
        headers = nfl_headers_gen()
    pbp_txt = {}
    summary_url = f"https://api.nfl.com/experience/v1/gamedetails/{game_id}"
    summary_resp = requests.get(summary_url, headers=headers)
    summary = summary_resp.json()

    incoming_keys_expected = [
        "attendance",
        "distance",
        "down",
        "gameClock",
        "goalToGo",
        "homePointsOvertime",
        "homePointsQ1",
        "homePointsQ2",
        "homePointsQ3",
        "homePointsQ4",
        "homePointsTotal",
        "homeTeam",
        "homeTimeoutsRemaining",
        "homeTimeoutsUsed",
        "id",
        "offset",
        "period",
        "phase",
        "playReview",
        "possessionTeam",
        "quarter",
        "redzone",
        "scoringSummaries",
        "stadium",
        "startTime",
        "totalOffset",
        "visitorPointsOvertime",
        "visitorPointsQ1",
        "visitorPointsQ2",
        "visitorPointsQ3",
        "visitorPointsQ4",
        "visitorPointsTotal",
        "visitorTeam",
        "visitorTimeoutsRemaining",
        "visitorTimeoutsUsed",
        "weather",
        "yardLine",
        "yardsToGo",
        "drives",
        "plays",
    ]
    dict_keys_expected = ["homeTeam", "possessionTeam", "visitorTeam", "weather"]
    array_keys_expected = ["scoringSummaries", "drives", "plays"]
    if raw == True:
        return summary

    for k in incoming_keys_expected:
        if k in summary.keys():
            pbp_txt[k] = summary.get(f"{k}")
        else:
            pbp_txt[k] = {} if k in dict_keys_expected else []
    return pbp_txt


def nfl_game_schedule(season=2021, season_type="REG", week=1, headers=None, raw=False) -> Dict:
    """nfl_game_schedule() -- list ``api.nfl.com`` games for a season/week slice.

    Args:
        season (int): season year (e.g. ``2024``).
        season_type (str): season type. One of ``"REG"`` or ``"POST"``.
        week (int): week number (1-18 regular season, 1-4 post-season).
        headers (Dict[str, str] | None): Pre-built header dict.
            Defaults to a fresh ``nfl_headers_gen()`` call.
        raw (bool): Currently ignored -- the function always returns the
            raw NFL.com summary payload.

    Returns:
        Dict: Dictionary with the games list under ``"games"`` plus
        pagination metadata.

    Example:
        Week 1 of the 2024 regular season::

            from sportsdataverse.nfl.nfl_games import nfl_game_schedule
            week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)

        Wild Card weekend (post-season)::

            wild_card = nfl_game_schedule(season=2023, season_type="POST", week=1)

        Reuse headers across many calls::

            from sportsdataverse.nfl.nfl_games import nfl_game_schedule, nfl_headers_gen
            hdrs = nfl_headers_gen()
            for week in range(1, 19):
                summary = nfl_game_schedule(
                    season=2024, season_type="REG", week=week, headers=hdrs,
                )
    """
    if headers is None:
        headers = nfl_headers_gen()
    params = {"season": season, "seasonType": season_type, "week": week}
    pbp_txt = {}
    summary_url = "https://api.nfl.com/experience/v1/games"
    summary_resp = requests.get(summary_url, headers=headers, params=params)
    summary = summary_resp.json()

    incoming_keys_expected = [
        "id",
        "homeTeam",
        "awayTeam",
        "category",
        "date",
        "time",
        "broadcastInfo",
        "neutralSite",
        "venue",
        "season",
        "seasonType",
        "status",
        "week",
        "weekType",
        "externalIds",
        "ticketUrl",
        "ticketVendors",
        "detail",
    ]
    dict_keys_expected = ["homeTeam", "possessionTeam", "visitorTeam", "weather"]
    array_keys_expected = ["scoringSummaries", "drives", "plays"]
    return summary
