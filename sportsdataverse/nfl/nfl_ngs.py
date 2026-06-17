"""``nextgenstats.nfl.com`` Next Gen Stats (NGS) web-API wrappers.

The NFL Next Gen Stats site exposes a **token-free** JSON API under
``https://nextgenstats.nfl.com/api``. Unlike :mod:`sportsdataverse.nfl.nfl_games`
(which mints a bearer token against ``api.nfl.com``), the NGS API only needs a
browser-style session: a ``User-Agent`` + ``Referer`` and the cookies the site
sets on first visit. This module keeps one lazily-initialized, cookie-warmed
``requests.Session`` at module scope and routes every call through it -- the
homepage is fetched exactly once to seed cookies, not per request.

Public functions all return a tidy **polars** ``DataFrame`` by default (flattened
with :func:`polars.json_normalize`); pass ``return_as_pandas=True`` for pandas.

NGS game ids are the ``YYYYMMDDNN`` integers found in :func:`nfl_ngs_league_schedule`
(the ``gameId`` field) -- they are *not* the ``api.nfl.com`` uuid game ids. Use a
``gameId`` from the schedule for the game-scoped functions.

A family of ``/api/live/*`` endpoints (drives, scores, winProbability, playlist,
drive chart, defense splits, completion-probability, per-game passing/receiving/
rushing summaries), ``/api/participation/team/game``, ``/api/plays/highlights`` and
``/api/plays/highlight/players`` are intentionally **not** wrapped: the NGS gateway
returns an explicit-deny ``403`` for anonymous browser sessions on those paths.
See the module-level ``_DENIED_ENDPOINTS`` note and the package docs for details.
"""

from __future__ import annotations

from typing import Optional

import requests

API_HOST = "https://nextgenstats.nfl.com/api"
_HOME = "https://nextgenstats.nfl.com/"
_REFERER = "https://nextgenstats.nfl.com/stats/passing"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# Paths that return an explicit-deny 403 (or are otherwise unreachable) for an
# anonymous browser session -- documented here so callers know why they are absent.
_DENIED_ENDPOINTS = (
    "/live/game/drives",
    "/live/game/scores",
    "/live/plays/winProbability",
    "/live/plays/playlist/game",
    "/live/chart/drive",
    "/live/splits/game/defense/summary",
    "/live/stats/completionProbability/game",
    "/live/summary/game/passing",
    "/live/summary/game/receiving",
    "/live/summary/game/rushing",
    "/participation/team/game",
    "/plays/highlights",
    "/plays/highlight/players",
    "/highlights/participation/game/play",
    "/highlights/tracking/game/play/withBall/min",
)

# Module-level cached session (lazy-init + warmed once).
_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Return the shared, cookie-warmed NGS :class:`requests.Session` (lazy-init).

    On first call, creates a session with the browser ``User-Agent``/``Referer``
    and GETs the NGS homepage once so the gateway sets its session cookies. The
    same session (cookies and all) is reused for every subsequent request; the
    homepage is never re-fetched.
    """
    global _SESSION
    if _SESSION is None:
        sess = requests.Session()
        sess.headers.update({"User-Agent": _UA, "Referer": _REFERER})
        # Warm the cookie jar -- one homepage hit seeds the gateway cookies.
        try:
            sess.get(_HOME, timeout=20)
        except requests.RequestException:
            pass
        _SESSION = sess
    return _SESSION


def _ngs_get(path: str, params: Optional[dict] = None) -> dict:
    """GET ``{API_HOST}{path}`` through the shared session and return parsed JSON.

    Args:
        path: API path beginning with ``/`` (e.g. ``"/statboard/passing"``).
        params: Optional query-string params.

    Returns:
        The decoded JSON body (``dict`` or ``list``).
    """
    sess = _get_session()
    resp = sess.get(f"{API_HOST}{path}", params=params or {}, timeout=20)
    resp.raise_for_status()
    return resp.json()


def _to_frame(records, return_as_pandas: bool):
    """Flatten a list of (possibly nested) dicts into a polars/pandas DataFrame.

    Uses ``separator="_"`` and ``max_level=2`` so nested ``leader``/``play``
    blocks become prefixed columns (``leader_playerName``, ``play_playId``, ...).
    Falls back to ``strict=False`` when a deeply-nested array mixes Int/Float
    values (e.g. the per-zone breakdowns in the gamecenter passers block).
    """
    import polars as pl

    recs = records or []
    try:
        df = pl.json_normalize(recs, separator="_", max_level=2, infer_schema_length=None)
    except TypeError:
        df = pl.json_normalize(recs, separator="_", max_level=2, infer_schema_length=None, strict=False)
    return df.to_pandas() if return_as_pandas else df


# --------------------------------------------------------------------------- #
# statboard
# --------------------------------------------------------------------------- #
def nfl_ngs_statboard(
    stat_type: str = "passing",
    season: int = 2024,
    season_type: str = "REG",
    week: Optional[int] = None,
    return_as_pandas: bool = False,
):
    """NGS season/week statboard leaderboard for a stat family (one row per player).

    Wraps ``/api/statboard/{passing,receiving,rushing}``. Each record is a flat
    per-player stat line (e.g. for passing: ``completionPercentageAboveExpectation``,
    ``avgTimeToThrow``, ``aggressiveness``, ``passerRating`` ...). The player's bio
    is nested under a ``player`` object and is flattened to ``player_*`` columns.

    Args:
        stat_type (str): one of ``"passing"``, ``"receiving"``, ``"rushing"``.
            (For the cross-stat highlight board use :func:`nfl_ngs_statboard_leaders`.)
        season (int): season year, e.g. ``2024``.
        season_type (str): ``"REG"``, ``"POST"``, or ``"PRE"``.
        week (int | None): single week to filter to; ``None`` returns the
            full-season board.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per qualifying player.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_statboard
            qb = nfl_ngs_statboard(stat_type="passing", season=2024, season_type="REG")
            qb.select(["playerName", "passerRating", "completionPercentageAboveExpectation"]).head()
    """
    params = {"season": season, "seasonType": season_type}
    if week is not None:
        params["week"] = week
    payload = _ngs_get(f"/statboard/{stat_type}", params)
    return _to_frame(payload.get("stats", []), return_as_pandas)


def nfl_ngs_statboard_leaders(
    season: int = 2024,
    season_type: str = "REG",
    week: Optional[int] = None,
    return_as_pandas: bool = False,
):
    """NGS cross-stat "leaders" board, stacked long with a ``category`` column.

    Wraps ``/api/statboard/leaders``, which bundles several short top-N lists of
    mixed shape (``fastestBallCarriers``, ``fastestSacks``, ``longestCompletions``,
    ``highestSeparation``, ``rushYardsOverExpected``, ``completionPctAboveExpected``,
    ``avgYACAboveExpected``). Each list is normalized separately and concatenated
    diagonally (union of columns; missing cells become null), with a ``category``
    column recording which board each row came from.

    Args:
        season (int): season year.
        season_type (str): ``"REG"``, ``"POST"``, or ``"PRE"``.
        week (int | None): optional single-week filter.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame`` stacking every leader list, with a
        ``category`` column. Empty frame if no lists are present.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_statboard_leaders
            bd = nfl_ngs_statboard_leaders(season=2024, season_type="REG")
            bd["category"].unique().to_list()
    """
    import polars as pl

    params = {"season": season, "seasonType": season_type}
    if week is not None:
        params["week"] = week
    payload = _ngs_get("/statboard/leaders", params)

    frames = []
    for key, value in payload.items():
        if isinstance(value, list) and value:
            sub = _to_frame(value, return_as_pandas=False)
            if sub.height:
                frames.append(sub.with_columns(pl.lit(key).alias("category")))
    if not frames:
        empty = pl.DataFrame()
        return empty.to_pandas() if return_as_pandas else empty
    out = pl.concat(frames, how="diagonal_relaxed")
    cols = ["category", *[c for c in out.columns if c != "category"]]
    out = out.select(cols)
    return out.to_pandas() if return_as_pandas else out


# --------------------------------------------------------------------------- #
# leaders (single-list boards)
# --------------------------------------------------------------------------- #
# category -> (path, json key holding the records)
_LEADER_CATEGORIES = {
    "speed": ("/leaders/speed/ballCarrier", "leaders"),
    "distance_ballcarrier": ("/leaders/distance/ballCarrier", "leaders"),
    "distance_tackle": ("/leaders/distance/tackle", "leaders"),
    "time_sack": ("/leaders/time/sack", "leaders"),
    "completion_season": ("/leaders/expectation/completion/season", "completionLeaders"),
    "completion_week": ("/leaders/expectation/completion/week", "completionLeaders"),
    "ery_season": ("/leaders/expectation/ery/season", "eryLeaders"),
    "ery_week": ("/leaders/expectation/ery/week", "eryLeaders"),
    "yac_season": ("/leaders/expectation/yac/season", "yacLeaders"),
    "yac_week": ("/leaders/expectation/yac/week", "yacLeaders"),
}


def nfl_ngs_leaders(
    category: str = "speed",
    season: int = 2024,
    season_type: str = "REG",
    week: Optional[int] = None,
    return_as_pandas: bool = False,
):
    """NGS top-N "leaders" board for a single category (one row per leader play).

    One parameterized wrapper over the single-list leader endpoints. Each record
    nests a ``leader`` (player/stat) block and a ``play`` (the play that produced
    the highlight) block, flattened to ``leader_*`` / ``play_*`` columns.

    Categories (``category=`` value -> endpoint):

    * ``"speed"`` -> ``/leaders/speed/ballCarrier`` (fastest ball-carrier speeds)
    * ``"distance_ballcarrier"`` -> ``/leaders/distance/ballCarrier``
    * ``"distance_tackle"`` -> ``/leaders/distance/tackle``
    * ``"time_sack"`` -> ``/leaders/time/sack``
    * ``"completion_season"`` / ``"completion_week"`` ->
      ``/leaders/expectation/completion/{season,week}`` (most-improbable completions)
    * ``"ery_season"`` / ``"ery_week"`` -> ``/leaders/expectation/ery/{season,week}``
      (expected rush yards over expectation)
    * ``"yac_season"`` / ``"yac_week"`` -> ``/leaders/expectation/yac/{season,week}``
      (yards after catch over expectation)

    Args:
        category (str): one of the keys above. Defaults to ``"speed"``.
        season (int): season year.
        season_type (str): ``"REG"``, ``"POST"``, or ``"PRE"``.
        week (int | None): week filter -- required (and only used) by the
            ``*_week`` categories; ignored by season/non-expectation boards.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per leader entry.

    Raises:
        ValueError: if ``category`` is not a recognized key.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_leaders
            fast = nfl_ngs_leaders(category="speed", season=2024, season_type="REG")
            fast.select(["leader_playerName", "leader_maxSpeed", "play_playDescription"]).head()
    """
    if category not in _LEADER_CATEGORIES:
        valid = ", ".join(sorted(_LEADER_CATEGORIES))
        raise ValueError(f"category must be one of: {valid}")
    path, record_key = _LEADER_CATEGORIES[category]
    params = {"season": season, "seasonType": season_type}
    if week is not None:
        params["week"] = week
    payload = _ngs_get(path, params)
    return _to_frame(payload.get(record_key, []), return_as_pandas)


# --------------------------------------------------------------------------- #
# league
# --------------------------------------------------------------------------- #
def nfl_ngs_league_schedule(
    season: int = 2024,
    season_type: str = "REG",
    week: Optional[int] = None,
    return_as_pandas: bool = False,
):
    """NGS league schedule -- one row per game; source of NGS ``gameId`` values.

    Wraps ``/api/league/schedule`` (which returns a top-level list of games).
    Each row carries ``gameId`` (the ``YYYYMMDDNN`` id used by the game-scoped
    functions here), ``gameKey``, ``smartId`` (the api.nfl.com uuid), team
    abbreviations/ids/names, kickoff times, ``ngsGame`` (tracking-data flag) and
    ``season``/``seasonType``/``week``.

    Args:
        season (int): season year.
        season_type (str): ``"REG"``, ``"POST"``, or ``"PRE"``.
        week (int | None): optional single-week filter.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per scheduled game.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_league_schedule
            sched = nfl_ngs_league_schedule(season=2024, season_type="REG", week=1)
            first_game_id = sched["gameId"][0]
    """
    params = {"season": season, "seasonType": season_type}
    if week is not None:
        params["week"] = week
    payload = _ngs_get("/league/schedule", params)
    # /league/schedule returns a top-level list of game dicts.
    return _to_frame(payload, return_as_pandas)


def nfl_ngs_league_schedule_current(return_as_pandas: bool = False):
    """NGS schedule for the *current* week -- one row per game.

    Wraps ``/api/league/schedule/current``; the games are under the ``games`` key
    (alongside scalar ``season``/``seasonType``/``week`` describing the slice).

    Args:
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per game in the current week.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_league_schedule_current
            cur = nfl_ngs_league_schedule_current()
            cur.select(["gameId", "homeTeamAbbr", "visitorTeamAbbr"]).head()
    """
    payload = _ngs_get("/league/schedule/current")
    return _to_frame(payload.get("games", []), return_as_pandas)


def nfl_ngs_league_teams(return_as_pandas: bool = False):
    """NGS team directory -- one row per team.

    Wraps ``/api/league/teams`` (top-level list). Each row carries ``teamId``,
    ``abbr``, ``fullName``, ``nick``, ``conference``/``division``, ``cityState``,
    ``stadiumName``, ``smartId``, ``logo`` and site/ticket URLs.

    Args:
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per team.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_league_teams
            teams = nfl_ngs_league_teams()
            teams.select(["teamId", "abbr", "fullName", "conferenceAbbr"]).head()
    """
    payload = _ngs_get("/league/teams")
    return _to_frame(payload, return_as_pandas)


# --------------------------------------------------------------------------- #
# gamecenter
# --------------------------------------------------------------------------- #
_GAMECENTER_GROUPS = {
    "passers": "passers",
    "rushers": "rushers",
    "receivers": "receivers",
    "passRushers": "passRushers",
}


def nfl_ngs_gamecenter_overview(
    game_id,
    group: str = "passers",
    return_as_pandas: bool = False,
):
    """NGS gamecenter overview for one game -- one row per player on a side.

    Wraps ``/api/gamecenter/overview`` (keyed by NGS ``gameId``). The payload
    splits each stat ``group`` into ``home`` and ``visitor`` entries; this function
    stacks both and tags every row with ``side`` (``"home"``/``"visitor"``) plus the
    game's ``gameId``. Note ``passers`` carries a single primary QB per side (two
    rows total) while ``rushers``/``receivers``/``passRushers`` are full lists.

    Args:
        game_id: NGS ``gameId`` (e.g. ``"2024090500"``) from
            :func:`nfl_ngs_league_schedule`.
        group (str): which player group -- one of ``"passers"``, ``"rushers"``,
            ``"receivers"``, ``"passRushers"``.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per player (both teams),
        with ``side`` and ``gameId`` columns prepended.

    Raises:
        ValueError: if ``group`` is not a recognized key.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_gamecenter_overview
            ov = nfl_ngs_gamecenter_overview(game_id="2024090500", group="passers")
            ov.select(["side", "playerName", "position"]).head()
    """
    import polars as pl

    if group not in _GAMECENTER_GROUPS:
        valid = ", ".join(sorted(_GAMECENTER_GROUPS))
        raise ValueError(f"group must be one of: {valid}")
    payload = _ngs_get("/gamecenter/overview", {"gameId": game_id})
    block = payload.get(group, {}) or {}

    frames = []
    for side in ("home", "visitor"):
        recs = block.get(side)
        # ``passers`` returns a single dict per side; the others return lists.
        if isinstance(recs, dict):
            recs = [recs]
        if isinstance(recs, list) and recs:
            sub = _to_frame(recs, return_as_pandas=False)
            if sub.height:
                frames.append(sub.with_columns(pl.lit(side).alias("side")))
    if not frames:
        empty = pl.DataFrame()
        return empty.to_pandas() if return_as_pandas else empty
    out = pl.concat(frames, how="diagonal_relaxed").with_columns(pl.lit(str(game_id)).alias("gameId"))
    lead = [c for c in ("side", "gameId") if c in out.columns]
    out = out.select([*lead, *[c for c in out.columns if c not in lead]])
    return out.to_pandas() if return_as_pandas else out


# --------------------------------------------------------------------------- #
# microsite content charts
# --------------------------------------------------------------------------- #
def nfl_ngs_microsite_chart(
    season: int = 2024,
    season_type: str = "REG",
    week=None,
    chart_type=None,
    team_id=None,
    limit: int = 100,
    offset: int = 0,
    return_as_pandas: bool = False,
):
    """NGS microsite chart catalogue -- one row per rendered player chart image.

    Wraps ``/api/content/microsite/chart``; records live under ``charts`` and each
    carries the chart ``imageName``/``type`` (``qb-grid``, ``pass``, ``route``,
    ``carry``) plus the player and headline stats (``passerRating``,
    ``completions``, etc.) and image-size URLs. Supports server-side paging.

    Args:
        season (int): season year.
        season_type (str): ``"REG"``, ``"POST"``, or ``"PRE"``.
        week: optional week filter (the API accepts ``"all"`` by default).
        chart_type: optional chart-type filter (e.g. ``"qb-grid"``, ``"pass"``).
        team_id: optional team-id filter.
        limit (int): page size (passed as ``limit``).
        offset (int): page offset.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per chart in the page.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_microsite_chart
            charts = nfl_ngs_microsite_chart(season=2024, season_type="REG", limit=25)
            charts.select(["playerName", "type", "imageName"]).head()
    """
    params = {"season": season, "seasonType": season_type, "limit": limit, "offset": offset}
    if week is not None:
        params["week"] = week
    if chart_type is not None:
        params["type"] = chart_type
    if team_id is not None:
        params["teamId"] = team_id
    payload = _ngs_get("/content/microsite/chart", params)
    return _to_frame(payload.get("charts", []), return_as_pandas)


def nfl_ngs_microsite_chart_players(
    season: int = 2024,
    season_type: str = "REG",
    return_as_pandas: bool = False,
):
    """NGS microsite chart player index -- one row per player with a chart.

    Wraps ``/api/content/microsite/chart/players``; records live under ``players``
    and carry ``esbId``, ``firstName``, ``lastName`` and ``playerName``. Useful as
    the lookup list of who has charts available for a given season.

    Args:
        season (int): season year.
        season_type (str): ``"REG"``, ``"POST"``, or ``"PRE"``.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per player.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_microsite_chart_players
            who = nfl_ngs_microsite_chart_players(season=2024, season_type="REG")
            who.select(["playerName", "esbId"]).head()
    """
    payload = _ngs_get(
        "/content/microsite/chart/players",
        {"season": season, "seasonType": season_type},
    )
    return _to_frame(payload.get("players", []), return_as_pandas)


# --------------------------------------------------------------------------- #
# per-play highlight lookup
# --------------------------------------------------------------------------- #
def nfl_ngs_play_is_highlight(
    game_id,
    play_id,
    return_as_pandas: bool = False,
):
    """Look up whether a single play is an NGS highlight -- one-row frame.

    Wraps ``/api/plays/isHighlight`` (keyed by NGS ``gameId`` + ``playId``). When
    the play is a highlight, the response's nested ``highlight`` block (the play
    metadata, the ``players`` involved, season/week/team) is flattened onto the
    row alongside the top-level ``gameId``/``playId``/``isHighlight`` flag. Pull a
    real ``(gameId, playId)`` pair from :func:`nfl_ngs_leaders` -- each leader
    entry's ``play_gameId`` / ``play_playId`` is a known highlight.

    Args:
        game_id: NGS ``gameId`` (e.g. ``"2024111800"``).
        play_id: the play id within that game (e.g. ``1214``).
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A one-row polars (or pandas) ``DataFrame`` with ``gameId``, ``playId``,
        ``isHighlight`` and (when true) flattened ``highlight_*`` columns.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ngs_leaders, nfl_ngs_play_is_highlight
            lead = nfl_ngs_leaders(category="speed", season=2024, season_type="REG")
            gid, pid = lead["play_gameId"][0], lead["play_playId"][0]
            hl = nfl_ngs_play_is_highlight(game_id=gid, play_id=pid)
            hl.select(["gameId", "playId", "isHighlight"]).head()
    """
    payload = _ngs_get("/plays/isHighlight", {"gameId": game_id, "playId": play_id})
    return _to_frame([payload], return_as_pandas)


__all__ = [
    "nfl_ngs_statboard",
    "nfl_ngs_statboard_leaders",
    "nfl_ngs_leaders",
    "nfl_ngs_league_schedule",
    "nfl_ngs_league_schedule_current",
    "nfl_ngs_league_teams",
    "nfl_ngs_gamecenter_overview",
    "nfl_ngs_microsite_chart",
    "nfl_ngs_microsite_chart_players",
    "nfl_ngs_play_is_highlight",
]
