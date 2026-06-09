"""``api.nfl.com`` game schedule + play-by-play wrappers.

NFL.com retired the old ``/v1/reroute`` client-credentials token endpoint, which
broke the previous implementation. The NFL.com web app now mints a bearer token
from ``/identity/v3/token`` (the same flow the nflverse ``nflapi`` package uses) and
reads game data from the modern ``/football/v2`` + ``/experience/v1`` endpoints.

Auth uses the NFL.com **web client** credentials (the public ``WEB_DESKTOP`` app key
the site ships to every browser). They are the defaults below and can be overridden
via the ``NFL_CLIENT_KEY`` / ``NFL_CLIENT_SECRET`` environment variables (or the
function arguments) if NFL rotates them or you have your own. No login / personal
account is involved -- the minted token carries the anonymous ``free`` plan.
"""

from __future__ import annotations

import os
import uuid
from typing import Dict, Optional

import requests

API_HOST = "https://api.nfl.com"

# NFL.com web-app (WEB_DESKTOP) client credentials -- shipped publicly in the
# site's JS bundle; overridable via env vars / args. NOT a personal account.
_DEFAULT_CLIENT_KEY = "4cFUW6DmwJpzT9L7LrG3qRAcABG5s04g"
_DEFAULT_CLIENT_SECRET = "CZuvCL49d9OwfGsR"
# base64({"model":"desktop","osName":"Windows","osVersion":"10","version":"Chrome"})
_DEFAULT_DEVICE_INFO = (
    "eyJtb2RlbCI6ImRlc2t0b3AiLCJvc05hbWUiOiJXaW5kb3dzIiwib3NWZXJzaW9uIjoiMTAiLCJ2ZXJzaW9uIjoiQ2hyb21lIn0="
)
_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def nfl_token_gen(client_key: Optional[str] = None, client_secret: Optional[str] = None) -> str:
    """Mint a fresh ``api.nfl.com`` access token via ``/identity/v3/token``.

    Wraps the anonymous device-token grant the NFL.com web app uses. Credentials
    resolve in this order: explicit ``client_key``/``client_secret`` args ->
    ``NFL_CLIENT_KEY``/``NFL_CLIENT_SECRET`` env vars -> the bundled public
    ``WEB_DESKTOP`` web-app credentials.

    Args:
        client_key: Override the client key (else env var, else the web default).
        client_secret: Override the client secret (else env var, else the default).

    Returns:
        str: The bearer ``accessToken`` string.

    Example:
        Mint a token and inspect its prefix::

            from sportsdataverse.nfl.nfl_games import nfl_token_gen
            token = nfl_token_gen()
            assert isinstance(token, str) and token.startswith("ey")
    """
    key = client_key or os.environ.get("NFL_CLIENT_KEY", _DEFAULT_CLIENT_KEY)
    secret = client_secret or os.environ.get("NFL_CLIENT_SECRET", _DEFAULT_CLIENT_SECRET)
    data = {
        "clientKey": key,
        "clientSecret": secret,
        "deviceId": str(uuid.uuid4()),
        "deviceInfo": _DEFAULT_DEVICE_INFO,
        "networkType": "other",
    }
    resp = requests.post(
        API_HOST + "/identity/v3/token",
        data=data,
        headers={"User-Agent": _DEFAULT_UA, "X-Domain-Id": "100"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["accessToken"]


def nfl_headers_gen(token: Optional[str] = None) -> Dict[str, str]:
    """Build the request-header dict expected by ``api.nfl.com``.

    Mints a fresh bearer token via :func:`nfl_token_gen` (unless ``token`` is
    supplied) and combines it with the browser-style headers the NFL.com web app
    sends. Reuse the returned dict across calls to avoid re-minting tokens.

    Args:
        token: An existing access token to reuse; mints a fresh one when ``None``.

    Returns:
        Dict[str, str]: Header dict ready to drop into ``requests.get``.

    Example:
        Reuse one header set across many calls::

            from sportsdataverse.nfl.nfl_games import nfl_headers_gen, nfl_game_schedule
            hdrs = nfl_headers_gen()
            week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
            week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
    """
    token = token or nfl_token_gen()
    return {
        "User-Agent": _DEFAULT_UA,
        "Accept": "application/json",
        "Referer": "https://www.nfl.com/",
        "Origin": "https://www.nfl.com",
        "Authorization": f"Bearer {token}",
        "X-Domain-Id": "100",
    }


def nfl_game_schedule(
    season: int = 2024,
    season_type: str = "REG",
    week: int = 1,
    headers: Optional[Dict[str, str]] = None,
    raw: bool = False,
) -> Dict:
    """List ``api.nfl.com`` games for a season/week slice (``/football/v2/games``).

    Args:
        season (int): season year (e.g. ``2024``).
        season_type (str): season type. One of ``"PRE"``, ``"REG"``, ``"POST"``.
        week (int): week number (1-18 regular season, 1-4 post-season).
        headers (Dict[str, str] | None): Pre-built header dict (skip the auth
            roundtrip). Defaults to a fresh :func:`nfl_headers_gen` call.
        raw (bool): currently ignored; the function returns the parsed JSON payload.

    Returns:
        Dict: payload with the games list under ``"games"`` plus ``"pagination"``.
        Each game carries ``id`` (the uuid game id used by :func:`nfl_game_details`),
        ``homeTeam``/``awayTeam``, ``date``, ``status``, ``externalIds`` (gsis etc.).

    Example:
        Week 1 of the 2024 regular season::

            from sportsdataverse.nfl.nfl_games import nfl_game_schedule
            week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)
            first_id = week_one["games"][0]["id"]
    """
    if headers is None:
        headers = nfl_headers_gen()
    url = f"{API_HOST}/football/v2/games/season/{season}/seasonType/{season_type}/week/{week}"
    resp = requests.get(url, headers=headers, params={"withExternalIds": "true"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def nfl_game_details(
    game_id: Optional[str] = None, headers: Optional[Dict[str, str]] = None, raw: bool = False
) -> Dict:
    """Pull full ``api.nfl.com`` game details (drives + plays) by game id.

    Hits ``/experience/v1/gamedetails/{game_id}``; the payload is the shield
    ``data.viewer.gameDetail`` object (plays, drives, scoring summaries, line
    scores, possession, weather, attendance, ...).

    Args:
        game_id (str): the uuid game id from :func:`nfl_game_schedule`
            (e.g. ``'7d3e8f84-1312-11ef-afd1-646009f18b2e'``).
        headers (Dict[str, str] | None): Pre-built header dict. Defaults to a fresh
            :func:`nfl_headers_gen` call.
        raw (bool): If True, return the full envelope (``{"data": {...}}``)
            untouched. If False (default), unwrap to the ``gameDetail`` object.

    Returns:
        Dict: the ``gameDetail`` object (or the raw envelope when ``raw=True``).
        Empty ``dict`` if the game has no detail payload.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_games import nfl_game_details
            detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
            len(detail["plays"]), len(detail["drives"])

        Reuse headers across many calls (avoids re-minting tokens)::

            from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
            hdrs = nfl_headers_gen()
            detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e", headers=hdrs)
    """
    if headers is None:
        headers = nfl_headers_gen()
    resp = requests.get(f"{API_HOST}/experience/v1/gamedetails/{game_id}", headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if raw:
        return payload
    return ((payload or {}).get("data", {}) or {}).get("viewer", {}).get("gameDetail", {}) or {}


def _to_frame(records: list, return_as_pandas: bool):
    """Flatten a list of nested dicts into a polars (or pandas) DataFrame."""
    import polars as pl

    df = pl.json_normalize(records or [], separator="_", max_level=2, infer_schema_length=None)
    return df.to_pandas() if return_as_pandas else df


def nfl_week_games(
    season: int = 2024,
    season_type: str = "REG",
    week: int = 1,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` week schedule -- one row per game (polars/pandas frame).

    Tidy wrapper over :func:`nfl_game_schedule`: flattens the ``games`` list into a
    DataFrame with ``id`` (uuid game id), ``season``/``seasonType``/``week``,
    ``date``, ``status_*``, and ``homeTeam_*`` / ``awayTeam_*`` columns.

    Args:
        season (int): season year. season_type (str): ``"PRE"``/``"REG"``/``"POST"``.
        week (int): week number. headers: reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per game.

    Example:
        >>> from sportsdataverse.nfl import nfl_week_games
        >>> sched = nfl_week_games(season=2024, season_type="REG", week=1)
        >>> sched.select(["id", "homeTeam_fullName", "awayTeam_fullName"]).head()
    """
    payload = nfl_game_schedule(season=season, season_type=season_type, week=week, headers=headers)
    return _to_frame(payload.get("games", []), return_as_pandas)


def nfl_game_pbp(
    game_id: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` play-by-play -- one row per play (polars/pandas frame).

    Tidy wrapper over :func:`nfl_game_details`: flattens ``gameDetail.plays`` into a
    DataFrame (``playId``, ``quarter``, ``down``, ``yardsToGo``, ``yardLine``,
    ``playType``, ``playDescription``, ``possessionTeam_*``, ...) and prepends the
    game context (``game_id``, ``home_team``, ``visitor_team``).

    Args:
        game_id (str): uuid game id from :func:`nfl_week_games` / :func:`nfl_game_schedule`.
        headers: reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per play (empty frame if the
        game has no play-by-play yet).

    Example:
        >>> from sportsdataverse.nfl import nfl_game_pbp
        >>> pbp = nfl_game_pbp(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
        >>> pbp.select(["quarter", "down", "yardsToGo", "playType", "playDescription"]).head()
    """
    import polars as pl

    detail = nfl_game_details(game_id=game_id, headers=headers)
    df = _to_frame(detail.get("plays", []), return_as_pandas=False)
    if df.height:
        df = df.with_columns(
            pl.lit(game_id).alias("game_id"),
            pl.lit((detail.get("homeTeam") or {}).get("abbreviation")).alias("home_team"),
            pl.lit((detail.get("visitorTeam") or {}).get("abbreviation")).alias("visitor_team"),
        ).select(["game_id", "home_team", "visitor_team", *[c for c in df.columns]])
    return df.to_pandas() if return_as_pandas else df
