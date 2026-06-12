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

Token handling is fully automatic: a minted token is cached in-process and reused
until just before its JWT ``exp``, then transparently re-minted -- no setup, no
manual refresh. Everything is overridable by env var when desired (all optional):

* ``NFL_ACCESS_TOKEN`` -- use this bearer token verbatim (skips minting + caching;
  you manage its lifetime). Highest precedence.
* ``NFL_CLIENT_KEY`` / ``NFL_CLIENT_SECRET`` -- mint tokens with these credentials
  instead of the bundled public web-app pair.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
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

# In-process token cache so back-to-back wrapper calls share one minted token
# instead of POSTing to /identity/v3/token every time. Keyed by the resolving
# client key so swapping credentials never serves a stale token. Guarded by a lock
# for thread-safe minting.
_TOKEN_LOCK = threading.Lock()
_TOKEN_CACHE: Dict[str, object] = {}  # {"token": str, "key": str, "exp": float}
# Renew this many seconds before the JWT's own ``exp`` so a call never races expiry.
_TOKEN_SKEW_SECONDS = 120
# Conservative lifetime used only when a token's ``exp`` claim can't be parsed.
_TOKEN_FALLBACK_TTL = 300


def _jwt_exp(token: str) -> Optional[float]:
    """Best-effort read of a JWT's ``exp`` (unix seconds); ``None`` if unparseable.

    The token is ``header.payload.signature``; the payload segment is base64url
    decoded and its ``exp`` claim returned. No signature verification -- only the
    expiry is needed to schedule renewal.
    """
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)  # restore base64 padding
        claims = json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = claims.get("exp")
        return float(exp) if exp is not None else None
    except Exception:  # noqa: BLE001 -- any decode/parse failure -> unknown expiry
        return None


def _mint_token(key: str, secret: str) -> str:
    """POST the anonymous device-token grant and return the bearer ``accessToken``."""
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


def nfl_clear_token_cache() -> None:
    """Drop the cached ``api.nfl.com`` token (forces a fresh mint on the next call)."""
    with _TOKEN_LOCK:
        _TOKEN_CACHE.clear()


def nfl_token_gen(
    client_key: Optional[str] = None,
    client_secret: Optional[str] = None,
    force_refresh: bool = False,
) -> str:
    """Return a valid ``api.nfl.com`` bearer token, minting + caching as needed.

    The token is cached in-process and reused until ~2 min before its own JWT
    ``exp``, then transparently re-minted -- so callers never have to think about
    expiry or refresh. The first call (or any call after expiry / ``force_refresh``)
    mints a fresh token via the anonymous device-token grant at ``/identity/v3/token``.

    Resolution order (all overrides optional):

    1. ``NFL_ACCESS_TOKEN`` env var -- returned verbatim, skipping minting and
       caching (you supply + manage the token). Ignored if explicit credentials
       are passed.
    2. Credentials: explicit ``client_key``/``client_secret`` args ->
       ``NFL_CLIENT_KEY``/``NFL_CLIENT_SECRET`` env vars -> the bundled public
       ``WEB_DESKTOP`` web-app pair.

    Args:
        client_key: Override the client key (else env var, else the web default).
        client_secret: Override the client secret (else env var, else the default).
        force_refresh: Mint a new token even if a cached one is still valid.

    Returns:
        str: The bearer ``accessToken`` string.

    Example:
        Token is minted once and reused across calls::

            from sportsdataverse.nfl.nfl_games import nfl_token_gen
            token = nfl_token_gen()                # mints + caches
            assert nfl_token_gen() == token        # served from cache
            assert isinstance(token, str) and token.startswith("ey")
    """
    # 1. A user-supplied token via env wins outright (their own / paid-plan token),
    #    unless explicit credentials were passed (which mean "mint with these").
    if client_key is None and client_secret is None:
        env_token = os.environ.get("NFL_ACCESS_TOKEN")
        if env_token:
            return env_token

    # 2. Resolve credentials, then serve a cached token or mint a fresh one.
    key = client_key or os.environ.get("NFL_CLIENT_KEY", _DEFAULT_CLIENT_KEY)
    secret = client_secret or os.environ.get("NFL_CLIENT_SECRET", _DEFAULT_CLIENT_SECRET)
    now = time.time()
    with _TOKEN_LOCK:
        if (
            not force_refresh
            and _TOKEN_CACHE.get("token")
            and _TOKEN_CACHE.get("key") == key
            and float(_TOKEN_CACHE.get("exp", 0)) - _TOKEN_SKEW_SECONDS > now
        ):
            return str(_TOKEN_CACHE["token"])
        token = _mint_token(key, secret)
        exp = _jwt_exp(token)
        _TOKEN_CACHE.clear()
        _TOKEN_CACHE.update(
            {"token": token, "key": key, "exp": exp if exp is not None else now + _TOKEN_FALLBACK_TTL},
        )
        return token


def nfl_headers_gen(token: Optional[str] = None) -> Dict[str, str]:
    """Build the request-header dict expected by ``api.nfl.com``.

    Obtains a bearer token via :func:`nfl_token_gen` (which caches + auto-renews,
    or honors ``NFL_ACCESS_TOKEN``) unless ``token`` is supplied, and combines it
    with the browser-style headers the NFL.com web app sends. Token caching already
    avoids re-minting, so callers rarely need to thread ``token``/``headers`` by hand.

    Args:
        token: An existing access token to reuse; uses the cached/minted one when ``None``.

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
