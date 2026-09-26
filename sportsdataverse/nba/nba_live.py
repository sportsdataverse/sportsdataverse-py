"""NBA cdn.nba.com liveData play-by-play and boxscore wrappers.

``cdn.nba.com``/``cdn.wnba.com`` serve the same static liveData JSON that powers
NBA.com's live scoreboard: per-whistle referee ids (``officialId``, populated on
every foul since 2019-20), wall-clock timestamps (``timeActual``), and shot/foul
locations (``xLegacy``/``yLegacy``).

**Transport note (Task 5 spike, 2026-09-26):** plain HTTP
(:func:`sportsdataverse.dl_utils.download`) with hoopR's ``.nba_cdn_headers()``-
equivalent browser headers (Chrome UA, ``Origin``/``Referer: https://www.nba.com``)
returned a 403 HTML "Access Denied" body for both ``cdn.nba.com`` and
``cdn.wnba.com``. This module therefore routes through
:func:`sportsdataverse.nba.nba_stats_runtime._curl_transport` (curl_cffi, Chrome
TLS impersonation) instead, which returned 200 JSON for both hosts. See
``tests/nba/fixtures/nba_live/README.md`` / ``tests/wnba/fixtures/wnba_live/README.md``
for the capture record.

Port of hoopR's ``nba_live_pbp`` / ``nba_live_boxscore`` (R/nba_stats_pbp.R) --
the WNBA twins in :mod:`sportsdataverse.wnba.wnba_live` share this module's parser
and fetch helper, matching wehoop's ``wnba_live_pbp`` / ``wnba_live_boxscore``.
"""

from __future__ import annotations

import json as _json
import re
from typing import Any, Callable, Optional

import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba.nba_stats_runtime import _curl_transport

__all__ = [
    "parse_nba_live_pbp",
    "parse_nba_live_boxscore",
    "nba_live_pbp",
    "nba_live_boxscore",
]

Transport = Callable[[str, dict, dict, Optional[str]], tuple]

_CDN_HOSTS = {"nba": "cdn.nba.com", "wnba": "cdn.wnba.com"}
_ID_COL_RE = re.compile(r"(^(official_id|person_id|team_id)$)|(_person_id$)")


def _gid(game_id: str | int) -> str:
    """Zero-pad an NBA/WNBA game ID to 10 digits."""
    return str(int(game_id)).zfill(10)


def _is_id_col(name: str) -> bool:
    """True for join-key id columns (``official_id``/``person_id``/``team_id``/``*_person_id``)."""
    return bool(_ID_COL_RE.search(name))


def _cdn_headers(league: str = "nba") -> dict[str, str]:
    """Build the browser-mimicking headers cdn.nba.com/cdn.wnba.com require.

    Without a Chrome User-Agent plus a matching ``Origin``/``Referer``, the CDN
    returns an "Access Denied" HTML page instead of JSON -- mirrors hoopR's
    ``.nba_cdn_headers()`` / wehoop's ``.wnba_cdn_headers()``.

    Args:
        league: ``"nba"`` or ``"wnba"``. Selects the ``Origin``/``Referer`` host.

    Returns:
        A dict of HTTP request headers.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_live import _cdn_headers
            h = _cdn_headers("wnba")
            print(h["Origin"])  # "https://www.wnba.com"
    """
    is_wnba = league == "wnba"
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.wnba.com" if is_wnba else "https://www.nba.com",
        "Referer": "https://www.wnba.com/" if is_wnba else "https://www.nba.com/",
    }


def _fetch_live(
    kind: str,
    game_id: str | int,
    *,
    league: str = "nba",
    proxy: dict[str, str] | None = None,
    transport: Transport | None = None,
) -> dict[str, Any]:
    """Fetch a cdn.nba.com / cdn.wnba.com liveData payload, classifying failures.

    The one shared fetch helper behind ``nba_live_pbp``/``nba_live_boxscore`` and
    the WNBA shims in :mod:`sportsdataverse.wnba.wnba_live` -- ``league`` picks the
    host and browser-header set; everything else is identical.

    Args:
        kind: ``"playbyplay"`` or ``"boxscore"``.
        game_id: NBA/WNBA game ID (int or str). Zero-padded to 10 digits.
        league: ``"nba"`` (``cdn.nba.com``) or ``"wnba"`` (``cdn.wnba.com``).
        proxy: Optional proxy dict in the ``requests`` ``proxies=`` shape
            (``{"https": "http://host:port"}``); the ``https`` entry (falling back
            to ``http``) is forwarded to the curl_cffi transport.
        transport: Injectable ``(url, params, headers, proxy_url) -> (status, text)``
            callable, defaulting to :func:`sportsdataverse.nba.nba_stats_runtime._curl_transport`.
            Tests monkeypatch this module's ``_curl_transport`` to stay offline.

    Returns:
        The parsed JSON payload (dict).

    Raises:
        NoDataError: 404, or a 403 carrying an S3 ``AccessDenied`` body -- no
            liveData object exists for this game (too old, or not yet started).
        AssetFetchError: Any other non-200 status, including a WAF/bot-check
            block (403 HTML) or an exhausted retry budget.

    Example:
        Offline with an injected transport::

            from sportsdataverse.nba.nba_live import _fetch_live
            def fake(url, params, headers, proxy_url):
                return 200, '{"game": {"actions": []}}'
            payload = _fetch_live("playbyplay", "0022500001", transport=fake)
    """
    host = _CDN_HOSTS[league]
    url = f"https://{host}/static/json/liveData/{kind}/{kind}_{_gid(game_id)}.json"
    _transport = transport or _curl_transport
    proxy_url = (proxy or {}).get("https") or (proxy or {}).get("http")
    status, text = _transport(url, {}, _cdn_headers(league), proxy_url)
    if status == 200:
        return dict(_json.loads(text)) if text else {}
    if status == 404 or (status == 403 and "AccessDenied" in text[:500]):
        raise NoDataError(f"{host} has no liveData {kind} for game {_gid(game_id)}")
    raise AssetFetchError(f"{host} liveData {kind} fetch failed (status={status}) for game {_gid(game_id)}")


def _normalize(records: list[dict[str, Any]]) -> pl.DataFrame:
    """``pl.json_normalize`` + snake_case rename + id-column Int64 cast, or an empty frame."""
    if not records:
        return pl.DataFrame()
    df = pl.json_normalize(records, separator="_")
    df = df.rename({c: underscore(c) for c in df.columns})
    id_cols = [c for c in df.columns if _is_id_col(c)]
    if id_cols:
        df = df.with_columns([pl.col(c).cast(pl.Int64, strict=False) for c in id_cols])
    return df


def parse_nba_live_pbp(payload: dict[str, Any], *, return_as_pandas: bool = False) -> Any:
    """Parse a cdn.nba.com/cdn.wnba.com liveData play-by-play payload into a tidy frame.

    One row per ``game.actions[]`` entry. Columns are flattened
    (``pl.json_normalize(..., separator="_")``) and snake-cased via
    :func:`sportsdataverse.dl_utils.underscore`; ``official_id``, ``person_id``,
    ``team_id``, and any ``*_person_id`` column (``assist_person_id``,
    ``jump_ball_won_person_id``, etc.) are cast to ``Int64``. A ``game_id`` column
    (the payload's own ``game.gameId``, zero-padded) is added to every row.

    Args:
        payload: The JSON payload (dict) from a cdn.nba.com/cdn.wnba.com
            ``liveData/playbyplay`` endpoint.
        return_as_pandas: If True, return a pandas DataFrame instead of polars.

    Returns:
        A DataFrame with one row per action. Empty/malformed payloads (missing
        ``game`` or ``actions``) return a zero-row frame.

    Raises:
        This function does not raise. Empty or malformed payloads produce a
        zero-row DataFrame.

    Example:
        Parse a real capture::

            import json
            from sportsdataverse.nba.nba_live import parse_nba_live_pbp
            with open("playbyplay_0022500001.json") as f:
                payload = json.load(f)
            pbp = parse_nba_live_pbp(payload)
            fouls = pbp.filter(pbp["action_type"] == "foul")
            print(fouls["official_id"].null_count())  # 0 (2019-20+ contract)

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    game = (payload or {}).get("game") or {}
    actions = game.get("actions") or []
    df = _normalize(actions)
    if df.height and game.get("gameId"):
        df = df.with_columns(pl.lit(_gid(game["gameId"])).alias("game_id"))
    return df.to_pandas() if return_as_pandas else df


def parse_nba_live_boxscore(payload: dict[str, Any], *, return_as_pandas: bool = False) -> dict[str, Any]:
    """Parse a cdn.nba.com/cdn.wnba.com liveData boxscore payload into tidy frames.

    Splits the payload into six tables: game metadata, officials (crew + jersey
    numbers + ``assignment`` slot label), and one team-level + one player-level
    table per side. Columns are flattened and snake-cased the same way as
    :func:`parse_nba_live_pbp`; a ``game_id`` column is added to every table and
    ``team_id`` is added to the player tables from the enclosing team object.

    Args:
        payload: The JSON payload (dict) from a cdn.nba.com/cdn.wnba.com
            ``liveData/boxscore`` endpoint.
        return_as_pandas: If True, return pandas DataFrames instead of polars.

    Returns:
        A dict with six keys: ``"game"``, ``"officials"``, ``"home_players"``,
        ``"away_players"``, ``"home_team"``, ``"away_team"``. Missing sections
        (e.g. a game with no ``homeTeam``) yield zero-row frames for that key.

    Raises:
        This function does not raise. Empty or malformed payloads produce
        zero-row DataFrames for every key.

    Example:
        Parse a real capture::

            import json
            from sportsdataverse.nba.nba_live import parse_nba_live_boxscore
            with open("boxscore_0022500001.json") as f:
                payload = json.load(f)
            result = parse_nba_live_boxscore(payload)
            officials = result["officials"]
            print(officials.select("person_id", "assignment"))

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    g = (payload or {}).get("game") or {}
    gid = _gid(g["gameId"]) if g.get("gameId") else None

    def _with_gid(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(gid).alias("game_id")) if df.height and gid else df

    def _team_frame(team: dict[str, Any]) -> pl.DataFrame:
        flat = {k: v for k, v in team.items() if k not in ("players", "periods")}
        return _with_gid(_normalize([flat] if flat else []))

    def _players_frame(team: dict[str, Any]) -> pl.DataFrame:
        df = _normalize(team.get("players") or [])
        if df.height:
            df = df.with_columns(pl.lit(team.get("teamId")).cast(pl.Int64).alias("team_id"))
        return _with_gid(df)

    game_meta = {k: v for k, v in g.items() if k not in ("officials", "homeTeam", "awayTeam", "arena")}
    home_team = g.get("homeTeam") or {}
    away_team = g.get("awayTeam") or {}

    out = {
        "game": _normalize([game_meta] if game_meta else []),
        "officials": _with_gid(_normalize(g.get("officials") or [])),
        "home_players": _players_frame(home_team),
        "away_players": _players_frame(away_team),
        "home_team": _team_frame(home_team),
        "away_team": _team_frame(away_team),
    }
    return {k: (v.to_pandas() if return_as_pandas else v) for k, v in out.items()}


def nba_live_pbp(
    game_id: str | int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    proxy: dict[str, str] | None = None,
) -> Any:
    """Fetch and parse NBA cdn.nba.com liveData play-by-play for a game.

    Retrieves ``https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json``
    and parses it via :func:`parse_nba_live_pbp`. Unlike stats.nba.com's
    play-by-play, this feed carries per-whistle referee ids (``official_id``,
    populated on every foul since the 2019-20 season) and wall-clock timestamps
    (``time_actual``).

    Args:
        game_id: NBA game ID (int or str). Zero-padded to 10 digits.
        raw: If True, return the raw JSON payload (dict) instead of a DataFrame.
        return_as_pandas: If True, return a pandas DataFrame instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict. Otherwise, a DataFrame as documented
        in :func:`parse_nba_live_pbp`.

    Raises:
        NoDataError: The game has no liveData play-by-play object (too old, or
            not yet started).
        AssetFetchError: The fetch failed (network error, rate limit, or a
            bot-check block).

    Example:
        Fetch a game's live play-by-play::

            from sportsdataverse.nba.nba_live import nba_live_pbp
            pbp = nba_live_pbp("0022500001")
            print(pbp.filter(pbp["action_type"] == "foul").height)

        Pipeline next step (fouls with a referee id)::

            fouls = pbp.filter(pbp["action_type"] == "foul").select("official_id", "time_actual")

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    payload = _fetch_live("playbyplay", game_id, league="nba", proxy=proxy)
    return payload if raw else parse_nba_live_pbp(payload, return_as_pandas=return_as_pandas)


def nba_live_boxscore(
    game_id: str | int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    proxy: dict[str, str] | None = None,
) -> Any:
    """Fetch and parse NBA cdn.nba.com liveData boxscore for a game.

    Retrieves ``https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json``
    and parses it via :func:`parse_nba_live_boxscore` into six tables (game,
    officials, home/away players, home/away team).

    Args:
        game_id: NBA game ID (int or str). Zero-padded to 10 digits.
        raw: If True, return the raw JSON payload (dict) instead of parsed DataFrames.
        return_as_pandas: If True, return pandas DataFrames instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict. Otherwise, a dict of DataFrames as
        documented in :func:`parse_nba_live_boxscore`.

    Raises:
        NoDataError: The game has no liveData boxscore object (too old, or not
            yet started).
        AssetFetchError: The fetch failed (network error, rate limit, or a
            bot-check block).

    Example:
        Fetch a game's live boxscore::

            from sportsdataverse.nba.nba_live import nba_live_boxscore
            result = nba_live_boxscore("0022500001")
            officials = result["officials"]
            print(officials.select("person_id", "name", "assignment"))

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    payload = _fetch_live("boxscore", game_id, league="nba", proxy=proxy)
    return payload if raw else parse_nba_live_boxscore(payload, return_as_pandas=return_as_pandas)
