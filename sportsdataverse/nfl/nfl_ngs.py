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

from typing import TYPE_CHECKING, List, Optional

import requests

if TYPE_CHECKING:
    import polars as pl

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


# --------------------------------------------------------------------------- #
# NGS season scraper (mirrors nflverse/ngs-data's R ``load_week_ngs`` /
# ``save_ngs_type``). Produces frames column-compatible with the released
# nflverse NGS parquet that :func:`sportsdataverse.nfl.load_nfl_nextgen_stats`
# reads -- i.e. snake_cased columns with ``team_abbr`` resolved from ``team_id``.
#
# NGS statboard rows are PLAYER-WEEK AGGREGATES (``avg_intended_air_yards``,
# ``completion_percentage_above_expectation``, ...), NOT per-play rows. This is
# a season-stats source, not a per-play air-yards / completion-probability one.
# --------------------------------------------------------------------------- #

# Relocated franchises share a ``team_id`` with their current identity in the NGS
# ``/league/teams`` directory; dropping these legacy abbreviations (as nflverse
# ngs-data does) keeps the team_id -> team_abbr map one-to-one for the join.
_RELOCATED_ABBRS = ("LA", "OAK", "STL", "SD")

# Module-level cache of the team_id -> team_abbr frame (fetched at most once per
# process). ``None`` means "not yet attempted"; a fetch failure stores an empty
# frame so we never re-hit the network on every week of a season loop.
_TEAMS_CACHE: Optional["pl.DataFrame"] = None


def _team_abbr_map() -> "pl.DataFrame":
    """Return (and cache) a 2-column ``team_id`` / ``team_abbr`` polars frame.

    Sourced from :func:`nfl_ngs_league_teams` (``/api/league/teams``). The
    relocated abbreviations in :data:`_RELOCATED_ABBRS` are dropped so the
    ``team_id`` key stays unique (mirrors nflverse ngs-data). The teams frame is
    fetched at most once per process; a network failure caches an empty frame so
    a season loop never repeatedly retries.
    """
    global _TEAMS_CACHE
    import polars as pl

    if _TEAMS_CACHE is None:
        try:
            teams = nfl_ngs_league_teams()
            cols = {c.lower(): c for c in teams.columns}
            id_col = cols.get("teamid")
            abbr_col = cols.get("abbr")
            if id_col is None or abbr_col is None:
                _TEAMS_CACHE = pl.DataFrame(schema={"team_id": pl.String, "team_abbr": pl.String})
            else:
                _TEAMS_CACHE = (
                    teams.select(
                        pl.col(id_col).cast(pl.String).alias("team_id"),
                        pl.col(abbr_col).cast(pl.String).alias("team_abbr"),
                    )
                    .filter(~pl.col("team_abbr").is_in(_RELOCATED_ABBRS))
                    .unique(subset=["team_id"], keep="first")
                )
        except Exception:  # noqa: BLE001 -- never let a teams-lookup failure abort a scrape
            _TEAMS_CACHE = pl.DataFrame(schema={"team_id": pl.String, "team_abbr": pl.String})
    return _TEAMS_CACHE


def _resolve_team_abbr(df: "pl.DataFrame") -> "pl.DataFrame":
    """Left-join a ``team_abbr`` column onto ``df`` via its ``teamId``/``team_id`` key.

    The statboard frame carries the team key as ``teamId`` (camelCase, pre-rename);
    after :func:`_snake_rename` it is ``team_id``. This helper accepts either and
    casts the key to string before joining against the cached team directory
    (:func:`_team_abbr_map`). If no team key column is present the frame is
    returned unchanged.

    Args:
        df: a statboard frame (camelCase or snake_case columns).

    Returns:
        ``df`` with a ``team_abbr`` column appended (null where unmatched).
    """
    import polars as pl

    if df.height == 0:
        return df
    key = "team_id" if "team_id" in df.columns else ("teamId" if "teamId" in df.columns else None)
    if key is None or "team_abbr" in df.columns:
        return df
    out = df.with_columns(pl.col(key).cast(pl.String).alias(key))
    teams = _team_abbr_map().rename({"team_id": key})
    return out.join(teams, on=key, how="left")


def _snake_rename(df: "pl.DataFrame") -> "pl.DataFrame":
    """Rename every column of ``df`` to ``snake_case`` via :func:`dl_utils.underscore`.

    The raw NGS statboard JSON is camelCase (``completionPercentageAboveExpectation``,
    ``avgTimeToThrow``, ``player_displayName`` ...); the published nflverse NGS
    parquet is snake_case. This makes the scraped frame column-compatible with
    :func:`sportsdataverse.nfl.load_nfl_nextgen_stats`.

    Args:
        df: any polars frame.

    Returns:
        ``df`` with snake-cased column names.
    """
    from sportsdataverse.dl_utils import underscore

    return df.rename({c: underscore(c) for c in df.columns})


# Documented empty-frame schema per stat type (snake_case keys + join columns).
# Returned when the API yields no stats for a (season, week) slice so callers can
# chain without null-checks.
_NGS_KEY_COLS = ("season", "season_type", "week", "player_display_name", "player_gsis_id", "team_abbr")


def _empty_ngs_frame(stat_type: str, return_as_pandas: bool):
    """Return a zero-row frame carrying the stable NGS key columns."""
    import polars as pl

    schema = {c: pl.String for c in _NGS_KEY_COLS}
    schema["season"] = pl.Int64
    schema["week"] = pl.Int64
    empty = pl.DataFrame(schema=schema)
    return empty.to_pandas() if return_as_pandas else empty


def scrape_ngs_week(
    stat_type: str,
    season: int,
    week: int,
    season_type: str = "REG",
    *,
    return_as_pandas: bool = False,
):
    """Scrape one (season, week) NGS statboard slice, shaped like the nflverse parquet.

    Port of nflverse ngs-data's R ``load_week_ngs``: fetch a single statboard
    slice via :func:`nfl_ngs_statboard`, resolve ``team_abbr`` from the team
    directory, snake-case every column, and tag the row with the loop ``week``.
    ``week=0`` is the season-aggregate row (a ``season_type="REG"`` call with no
    ``week`` query param); weeks ``1..max_reg`` are regular-season, and the
    playoff weeks (``max_reg+1`` upward) are fetched with ``season_type="POST"``.

    NGS statboard rows are **player-week aggregates** (``avg_intended_air_yards``,
    ``completion_percentage_above_expectation``, ``avg_time_to_throw``, ...), NOT
    per-play rows -- this is a season-stats source, not a per-play air-yards /
    completion-probability source.

    Args:
        stat_type (str): one of ``"passing"``, ``"rushing"``, ``"receiving"``.
        season (int): season year (NGS coverage starts in 2016).
        week (int): NGS week. ``0`` -> season aggregate; ``1..max_reg`` -> REG;
            higher -> POST. The supplied value is what tags the returned rows.
        season_type (str): ``"REG"`` or ``"POST"``; the caller (or
            :func:`scrape_ngs_season`) selects this per week. Defaults to ``"REG"``.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame`` of player-week NGS rows with
        snake-cased columns + a resolved ``team_abbr``. An EMPTY frame carrying
        the documented key schema (not an exception) when the API yields no stats.

    Example:
        Quick start::

            from sportsdataverse.nfl import scrape_ngs_week
            wk1 = scrape_ngs_week("passing", 2023, week=1)
            wk1.select(["season", "week", "player_display_name", "team_abbr"]).head()

        Season-aggregate row (week 0)::

            tot = scrape_ngs_week("rushing", 2023, week=0)

        See Also:
            * `nflverse`_ -- the nflverse/ngs-data scraper this mirrors
            * `nflreadpy`_ -- reads the published NGS parquet directly

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    import polars as pl

    raw = nfl_ngs_statboard(
        stat_type=stat_type,
        season=season,
        season_type=season_type,
        week=(week if week else None),
        return_as_pandas=False,
    )
    if raw.height == 0:
        return _empty_ngs_frame(stat_type, return_as_pandas)

    out = _snake_rename(_resolve_team_abbr(raw))
    # Tag with the loop week + season_type (mirrors R's ``mutate(week = week)`` on
    # the info tibble) so the season-aggregate row is week 0 and POST weeks carry
    # their continuous NGS week number.
    out = out.with_columns(
        pl.lit(int(season)).alias("season"),
        pl.lit(season_type).alias("season_type"),
        pl.lit(int(week)).alias("week"),
    )
    return out.to_pandas() if return_as_pandas else out


def scrape_ngs_season(
    stat_type: str,
    season: int,
    *,
    include_season_totals: bool = True,
    return_as_pandas: bool = False,
):
    """Scrape a full season of NGS statboard data, shaped like the nflverse parquet.

    Port of nflverse ngs-data's R ``save_ngs_type``: loop the regular-season weeks
    (``1..max_reg`` where ``max_reg = 18`` for ``season >= 2021`` else ``17``) plus
    the playoff weeks (``max_reg+1 .. max_reg+5``, fetched with
    ``season_type="POST"``), stack them diagonally, and -- when
    ``include_season_totals`` -- prepend the season-aggregate rows (NGS ``week=0``,
    a ``REG`` call with no ``week`` param) tagged ``week=0``. Duplicate rows (NGS
    returned dupes for some 2022 weeks) are de-duplicated on
    ``(season, week, player_gsis_id)``.

    Output columns match the published nflverse NGS parquet read by
    :func:`sportsdataverse.nfl.load_nfl_nextgen_stats` (snake_case, ``team_abbr``
    resolved). It will not be byte-identical -- nflverse post-processes (column
    pruning / ordering) -- but the core metric columns and the
    player/team/week keys align.

    NGS statboard rows are **player-week aggregates**, NOT per-play rows.

    Args:
        stat_type (str): one of ``"passing"``, ``"rushing"``, ``"receiving"``.
        season (int): season year (NGS coverage starts in 2016).
        include_season_totals (bool): also fetch the season-aggregate (``week=0``)
            rows. Defaults to ``True`` (matches ngs-data, whose week loop starts
            at 0).
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame`` stacking every week (and, by default,
        the season totals) for the requested ``stat_type`` and ``season``. An
        EMPTY frame carrying the documented key schema if nothing is returned.

    Example:
        Quick start::

            from sportsdataverse.nfl import scrape_ngs_season
            pas = scrape_ngs_season("passing", 2023)
            pas.select(["season", "week", "player_display_name", "team_abbr"]).head()

        Regular-season weeks only (skip the week-0 totals)::

            wk = scrape_ngs_season("receiving", 2023, include_season_totals=False)

        Column-compatible with the published parquet::

            from sportsdataverse.nfl import load_nfl_nextgen_stats
            published = load_nfl_nextgen_stats(seasons=[2023], stat_type="passing")
            shared = set(pas.columns) & set(published.columns)

        See Also:
            * `nflverse`_ -- the nflverse/ngs-data scraper this mirrors
            * `nflreadpy`_ -- reads the published NGS parquet directly

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    import polars as pl

    max_reg = 18 if season >= 2021 else 17

    # Week 0 = season aggregate (REG, no week param); 1..max_reg = REG weeks;
    # max_reg+1 .. max_reg+5 = playoff weeks (POST). This mirrors ngs-data's
    # ``seq(0, max_week + 1)`` with the REG/POST split inside ``load_week_ngs``.
    weeks: List[tuple[int, str]] = []
    if include_season_totals:
        weeks.append((0, "REG"))
    weeks.extend((w, "REG") for w in range(1, max_reg + 1))
    weeks.extend((w, "POST") for w in range(max_reg + 1, max_reg + 6))

    frames = []
    for week, season_type in weeks:
        wk = scrape_ngs_week(
            stat_type=stat_type,
            season=season,
            week=week,
            season_type=season_type,
            return_as_pandas=False,
        )
        if wk.height:
            frames.append(wk)

    if not frames:
        return _empty_ngs_frame(stat_type, return_as_pandas)

    out = pl.concat(frames, how="diagonal_relaxed")
    # NGS returned duplicate rows for some 2022 weeks with slightly different
    # values; de-dupe on (season, week, player_gsis_id) like ngs-data does.
    if "player_gsis_id" in out.columns:
        out = out.unique(subset=["season", "week", "player_gsis_id"], keep="first")
    return out.to_pandas() if return_as_pandas else out


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
    "scrape_ngs_week",
    "scrape_ngs_season",
]
