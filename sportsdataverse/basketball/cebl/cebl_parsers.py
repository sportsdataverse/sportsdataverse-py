"""Parsers for the generated ``cebl`` wrappers (CEBL, the Canadian Elite Basketball League's own stats API).

The data comes from ``api.data.cebl.ca``, the JSON backend of the CEBL Stats Hub. **CEBL's own API,
owner-approved gray area**: payloads are not redistributed as release assets.

Each public ``parse_cebl_*`` takes the dict a wrapper returns with ``return_parsed=False``
(``{"rows": <API body>, "_request": {...}}`` from :mod:`cebl_runtime`) or the bare API body itself (the refs
capture shape), and follows the package parser contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_case columns, and a zero-row frame with the documented columns and dtypes,
never an exception, for an empty or unrecognised input.

**Season** is the calendar year the season is played in (May to August), which is the sdv END year; there is
no mapping. The request's season is stamped onto every frame whose body does not carry it (``season`` is null
for the All-Time table, ``season`` omitted in the request).

**Id key spaces.** Every id is ``Int64`` and CEBL's own; none is a Genius id.

* ``team_id``: the franchise identity of one season. A relocated or renamed franchise got a second id (15 + 56
  Saskatchewan Rattlers / Saskatoon Mamba, 17 + 53 Calgary Surge / Guelph Nighthawks, 19 + 55 Brampton /
  Hamilton Honey Badgers, 16 + 54 Vancouver / Fraser Valley Bandits). ``cebl_teams`` lists all 15, with
  ``is_active`` false on the former ones. A team table's league row has a null ``team_id`` and
  ``is_league_row`` true.
* ``player_id``: the person, global across seasons and teams (3, 530, 570965). Genius ``data.json`` carries
  no person id, so a Genius play-by-play joins by name, team and jersey number (unverified).
* ``game_id``: the game (568 in 2019, 8 digits from 2025). No route is keyed by it.
* ``genius_match_id``: the Genius LiveStats match id parsed from ``stats_url_en`` /
  ``cebl_stats_url_en`` / ``game_stats_url_en`` (``.../u/CEBL/{id}/``, ``.../game?id={id}``). It joins the
  ``genius`` family (organisation ``CEBL``), where play-by-play and shots live. Derived from the URL, not
  fetched: the match behind a cancelled game may have no ``data.json`` (unverified).

**Dtypes** are pinned by column name, never sampled. The API sends every aggregate as a decimal string
(``'2256.0'``, ``'12.7'``): cast to ``Float64``. Percentages are 0-100 (``'44.7'``); ``win_percentage`` is 0-1
(``'0.583'``). Minutes are decimal minutes. Times are UTC ``Datetime``.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_cebl_games",
    "parse_cebl_news",
    "parse_cebl_player",
    "parse_cebl_player_gamelog",
    "parse_cebl_player_leaders",
    "parse_cebl_player_seasons",
    "parse_cebl_player_stats",
    "parse_cebl_roster",
    "parse_cebl_seasons",
    "parse_cebl_standings",
    "parse_cebl_team_stats",
    "parse_cebl_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
#: Where the runtime stamps the request (``cebl_runtime.REQUEST_KEY``).
_REQUEST = "_request"
INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date
DT = pl.Datetime("us", "UTC")

_NUM = re.compile(r"^[+-]?\d+(\.\d+)?$")
_GENIUS = re.compile(r"/u/CEBL/(\d+)(?:/|$)|[?&]id=(\d+)(?:&|$)")


# ------------------------------------------------------------------------------------ coercion
def _s(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return "; ".join(str(x) for x in v) or None
    return str(v)


def _i(v: Any) -> Optional[int]:
    """An integer, from an int or a numeric string that is whole ('5', '5.0'); never from a bool."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, (float, str)):
        try:
            f = float(v)
        except ValueError:
            return None
        return int(f) if f == int(f) else None
    return None


def _f(v: Any) -> Optional[float]:
    """A float, from a number or a decimal string ('25.9'); never from a bool."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and _NUM.match(v.strip()):
        return float(v)
    return None


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _d(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(v) if isinstance(v, str) else None
    except ValueError:
        return None


def _dt(v: Any) -> Optional[datetime]:
    """An ISO-8601 UTC timestamp ('2025-05-11T22:00:00Z') as an aware datetime."""
    if not isinstance(v, str):
        return None
    try:
        out = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    return out.astimezone(timezone.utc) if out.tzinfo else out.replace(tzinfo=timezone.utc)


_COERCE: Dict[Any, Callable[[Any], Any]] = {INT: _i, F: _f, S: _s, B: _b, D: _d, DT: _dt}


def _year_of(v: Any) -> Optional[int]:
    stamp = _dt(v)
    return stamp.year if stamp else None


def _genius(*urls: Any) -> Optional[int]:
    for u in urls:
        m = _GENIUS.search(u) if isinstance(u, str) else None
        if m:
            return int(m.group(1) or m.group(2))
    return None


# ------------------------------------------------------------------------------------ specs
# (column, source, dtype): ``source`` is the wire key, or a callable ``(row, ctx) -> value``.
Source = Union[str, Callable[[Dict[str, Any], Dict[str, Any]], Any]]
Spec = List[Tuple[str, Source, Any]]


def _ctx(name: str) -> Source:
    return lambda _r, c: c.get(name)


def _same_cols(*pairs: str) -> Spec:
    return [(p, p, S) for p in pairs]


_TEAM_NAMES: Spec = [
    ("team_name_en", "team_name_en", S),
    ("team_short_name_en", "team_short_name_en", S),
    ("team_name_fr", "team_name_fr", S),
    ("team_short_name_fr", "team_short_name_fr", S),
]
#: The per-game or total figures of a player row (decimal strings in the wire body).
_PLAYER_FIG: Spec = [
    ("played", "played", INT),
    ("started", "started", INT),
    *[
        (c, c, F)
        for c in (
            "minutes points field_goals_made field_goals_attempted field_goal_percentage three_points_made "
            "three_points_attempted three_point_percentage free_throws_made free_throws_attempted "
            "free_throw_percentage offensive_rebounds defensive_rebounds rebounds assists turn_overs steals "
            "blocks fouls double_doubles triple_doubles target_scores"
        ).split()
    ],
]
_TEAM_FIG: Spec = [
    ("played", "played", INT),
    ("wins", "wins", INT),
    ("win_percentage", "win_percentage", F),
    ("losses", "losses", INT),
    *[
        (c, c, F)
        for c in (
            "points points_against point_difference field_goals_made field_goals_attempted field_goal_percentage "
            "three_points_made three_points_attempted three_point_percentage free_throws_made "
            "free_throws_attempted free_throw_percentage offensive_rebounds defensive_rebounds rebounds assists "
            "turn_overs steals blocks fouls fouls_drawn efficiency"
        ).split()
    ],
]
_STAT_CTX: Spec = [
    ("season", _ctx("season"), INT),
    ("mode", _ctx("mode"), S),
    ("competition", _ctx("competition"), S),
    ("segment", _ctx("segment"), S),
]

SPECS: Dict[str, Spec] = {
    "seasons": [
        ("season_id", "id", INT),
        ("season", "year", INT),
        ("is_active", "is_active", B),
        ("is_live", "is_live", B),
    ],
    "teams": [
        ("team_id", "id", INT),
        ("name_en", "name_en", S),
        ("short_name_en", "short_name_en", S),
        ("name_fr", "name_fr", S),
        ("short_name_fr", "short_name_fr", S),
        ("is_active", "is_active", B),
        ("logo_url", "logo_url", S),
        ("icon_url", "icon_url", S),
    ],
    "standings": [
        ("season", _ctx("season"), INT),
        ("team_id", "id", INT),
        ("name_en", "name_en", S),
        ("short_name_en", "short_name_en", S),
        ("name_fr", "name_fr", S),
        ("short_name_fr", "short_name_fr", S),
        ("conference", "conference", S),
        ("position", "position", INT),
        ("played", "played", INT),
        ("wins", "wins", INT),
        ("win_percentage", "win_percentage", F),
        ("home_wins", "home_wins", INT),
        ("away_wins", "away_wins", INT),
        ("losses", "losses", INT),
        ("home_losses", "home_losses", INT),
        ("away_losses", "away_losses", INT),
        ("games_behind", "games_behind", F),
        ("points_difference", "points_difference", INT),
        ("last_five", "last_five", S),
        ("streak", "streak", S),
        *_same_cols(
            "logo_url",
            "icon_url",
            "tickets_url_en",
            "tickets_url_fr",
            "website_url_en",
            "website_url_fr",
            "shop_url_en",
            "shop_url_fr",
            "stats_url_en",
            "stats_url_fr",
        ),
    ],
    "roster": [
        ("season", _ctx("season"), INT),
        ("team_id", _ctx("team_id"), INT),
        ("player_id", "id", INT),
        ("full_name", "full_name", S),
        ("nationality", "nationality", S),
        ("birth_date", "birth_date", D),
        ("position", "position", S),
        ("jersey_number", "jersey_number", S),
        ("photo_url", "photo_url", S),
    ],
    "team_stats": [
        *_STAT_CTX,
        ("vs_conference", _ctx("vs_conference"), S),
        ("vs_team_id", _ctx("vs_team_id"), INT),
        ("team_id", "id", INT),
        ("is_league_row", lambda r, _c: r.get("id") is None, B),
        ("name_en", "name_en", S),
        ("short_name_en", "short_name_en", S),
        ("name_fr", "name_fr", S),
        ("short_name_fr", "short_name_fr", S),
        *_TEAM_FIG,
        *_same_cols("logo_url", "icon_url", "stats_url_en", "stats_url_fr"),
    ],
    "games": [
        ("season", lambda r, _c: _year_of(r.get("start_time_utc")), INT),
        ("game_id", "id", INT),
        ("genius_match_id", lambda r, _c: _genius(r.get("stats_url_en"), r.get("cebl_stats_url_en")), INT),
        ("start_time_utc", "start_time_utc", DT),
        ("competition", "competition", S),
        ("status", "status", S),
        ("period", "period", INT),
        ("venue_name", "venue_name", S),
        ("home_team_id", "home_team_id", INT),
        ("home_team_name", "home_team_name", S),
        ("home_team_score", "home_team_score", INT),
        ("away_team_id", "away_team_id", INT),
        ("away_team_name", "away_team_name", S),
        ("away_team_score", "away_team_score", INT),
        *_same_cols(
            "home_team_logo_url",
            "home_team_stats_url_en",
            "home_team_stats_url_fr",
            "away_team_logo_url",
            "away_team_stats_url_en",
            "away_team_stats_url_fr",
            "stats_url_en",
            "stats_url_fr",
            "cebl_stats_url_en",
            "cebl_stats_url_fr",
            "tickets_url_en",
            "tickets_url_fr",
            "broadcast_partner_logo_urls",
        ),
    ],
    "player_stats": [
        *_STAT_CTX,
        ("player_id", "id", INT),
        ("full_name", "full_name", S),
        ("nationality", "nationality", S),
        ("team_id", "team_id", INT),
        *_TEAM_NAMES,
        ("team_logo_url", "team_logo_url", S),
        *_PLAYER_FIG,
    ],
    "player_leaders": [
        ("statistic", lambda r, _c: r.get("__statistic"), S),
        ("ranking", "ranking", INT),
        ("season", _ctx("season"), INT),
        ("mode", _ctx("mode"), S),
        ("competition", _ctx("competition"), S),
        ("player_id", "id", INT),
        ("full_name", "full_name", S),
        ("team_id", "team_id", INT),
        *_TEAM_NAMES,
        ("team_abbreviated_name", "team_abbreviated_name", S),
        ("team_logo_url", "team_logo_url", S),
        ("value", "value", F),
        ("photo_url", "photo_url", S),
    ],
    "player_seasons": [
        ("mode", _ctx("mode"), S),
        ("competition", _ctx("competition"), S),
        ("player_id", "id", INT),
        ("season", "season", INT),
        ("is_career", lambda r, _c: r.get("season") is None, B),
        ("team_id", "team_id", INT),
        *_TEAM_NAMES,
        ("team_logo_url", "team_logo_url", S),
        *_PLAYER_FIG,
    ],
    "player": [
        ("player_id", "id", INT),
        ("full_name", "full_name", S),
        ("nationality", "nationality", S),
        ("birth_date", "birth_date", D),
        ("age", "age", INT),
        ("position", "position", S),
        ("jersey_number", "jersey_number", S),
        ("team_id", "team_id", INT),
        *_TEAM_NAMES,
        *_same_cols("team_icon_url", "team_stats_url_en", "team_stats_url_fr", "photo_url"),
    ],
    "player_gamelog": [
        ("player_id", _ctx("player_id"), INT),
        ("season", lambda r, _c: _year_of(r.get("game_date")), INT),
        ("game_date", "game_date", DT),
        ("genius_match_id", lambda r, _c: _genius(r.get("game_stats_url_en")), INT),
        ("team_abbreviated_name", "team_abbreviated_name", S),
        ("matchup", "matchup", S),
        ("minutes", "minutes", F),
        ("points", "points", INT),
        ("field_goals_made", "field_goals_made", INT),
        ("field_goals_attempted", "field_goals_attempted", INT),
        ("field_goal_percentage", "field_goal_percentage", F),
        ("three_points_made", "three_points_made", INT),
        ("three_points_attempted", "three_points_attempted", INT),
        ("three_point_percentage", "three_point_percentage", F),
        ("free_throws_made", "free_throws_made", INT),
        ("free_throws_attempted", "free_throws_attempted", INT),
        ("free_throw_percentage", "free_throw_percentage", F),
        ("offensive_rebounds", "offensive_rebounds", INT),
        ("defensive_rebounds", "defensive_rebounds", INT),
        ("rebounds", "rebounds", INT),
        ("assists", "assists", INT),
        ("turn_overs", "turn_overs", INT),
        ("steals", "steals", INT),
        ("blocks", "blocks", INT),
        ("fouls", "fouls", INT),
        ("is_target_scorer", "is_target_scorer", B),
        *_same_cols("game_stats_url_en", "game_stats_url_fr", "team_stats_url_en", "team_stats_url_fr"),
    ],
    "news": [
        ("news_id", "id", INT),
        ("language", _ctx("language"), S),
        ("title", "title", S),
        ("published_at_utc", "published_at_utc", DT),
        ("image_url", "image_url", S),
        ("html_content", "html_content", S),
    ],
}
#: ``{short: {column: dtype}}``: the documented schema of each frame.
SCHEMAS: Dict[str, Dict[str, Any]] = {k: {c: d for c, _s2, d in v} for k, v in SPECS.items()}


# ------------------------------------------------------------------------------------ shaping
def _unwrap(raw: Any) -> Tuple[Any, Dict[str, Any]]:
    """``(body, request stamp)`` from a wrapper's raw record, or ``(raw, {})`` for a bare API body."""
    if isinstance(raw, dict) and _REQUEST in raw and "rows" in raw and isinstance(raw[_REQUEST], dict):
        return raw["rows"], raw[_REQUEST]
    return raw, {}


def _frame(short: str, rows: List[Dict[str, Any]], ctx: Dict[str, Any]) -> pl.DataFrame:
    spec = SPECS[short]
    data: Dict[str, List[Any]] = {col: [] for col, _src, _dt2 in spec}
    for row in rows:
        for col, src, dtype in spec:
            value = src(row, ctx) if callable(src) else row.get(src)
            data[col].append(value if callable(src) and dtype is B else _COERCE[dtype](value))
    return pl.DataFrame(data, schema=SCHEMAS[short])


def _finish(df: pl.DataFrame, return_as_pandas: bool) -> Frame:
    return df.to_pandas() if return_as_pandas else df


def _list_rows(body: Any) -> List[Dict[str, Any]]:
    return [r for r in body if isinstance(r, dict)] if isinstance(body, list) else []


def _parse_list(short: str, raw: Any, return_as_pandas: bool) -> Frame:
    body, ctx = _unwrap(raw)
    return _finish(_frame(short, _list_rows(body), ctx), return_as_pandas)


_DOC_RAW = (
    "raw: the wrapper's raw record (``return_parsed=False``: ``{'rows': <API body>, '_request': {...}}``) or the "
    "bare API body."
)


def parse_cebl_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /seasons/``: every CEBL season, with the active and live flags.

    Args:
        raw: the wrapper's raw record (``return_parsed=False``: ``{'rows': <API body>, '_request': {...}}``) or
            the bare API body (a list of season rows).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per season: ``season_id`` (CEBL's row id, no route takes it), ``season`` (calendar year = END
        year), ``is_active`` and ``is_live``. A zero-row frame with the same columns for empty or malformed input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_seasons, parse_cebl_seasons
            df = parse_cebl_seasons(cebl_seasons(return_parsed=False))
    """
    return _parse_list("seasons", raw, return_as_pandas)


def parse_cebl_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /teams/``: every franchise identity, past and present.

    Args:
        raw: the wrapper's raw record or the bare API body (a list of team rows).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per team id (15 today): ``team_id``, English and French names, ``is_active`` (false on a former
        identity such as Guelph Nighthawks) and logo URLs. A typed zero-row frame for empty or malformed input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_teams
            cebl_teams().filter(pl.col("is_active") == True)
    """
    return _parse_list("teams", raw, return_as_pandas)


def parse_cebl_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /teams/{season}/``: the standings of one season, one row per team.

    Args:
        raw: the wrapper's raw record (its ``_request`` carries the ``season``) or the bare API body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per team: ``season`` (null for a bare body, the body does not echo it), ``team_id``, names,
        ``conference`` (EAST, WEST; OVERALL in 2019), ``position``, home/away splits, ``win_percentage`` (0-1),
        ``games_behind``, ``points_difference``, ``last_five`` and ``streak``. A typed zero-row frame for empty
        input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_standings
            cebl_standings(season=2025).select("team_id", "name_en", "wins", "losses")
    """
    return _parse_list("standings", raw, return_as_pandas)


def parse_cebl_roster(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /teams/{team_id}/roster/{season}/``: one team's roster in one season.

    Args:
        raw: the wrapper's raw record (its ``_request`` carries ``team_id`` and ``season``) or the bare body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per player: ``season`` and ``team_id`` (from the request), ``player_id``, ``full_name``,
        ``nationality`` (upper-case English country name), ``birth_date`` (public bio), ``position``,
        ``jersey_number`` (a string: '0' and '00' differ; null when unknown) and ``photo_url``. A typed
        zero-row frame for empty input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_roster
            cebl_roster(team_id=20, season=2026)
    """
    return _parse_list("roster", raw, return_as_pandas)


def parse_cebl_team_stats(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /teams/statistics/aggregated/``: a team statistic table plus the league row.

    Args:
        raw: the wrapper's raw record (its ``_request`` carries the filters) or the bare API body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per team plus one league row (``is_league_row`` true, ``team_id`` and every name null: the
        league's own figures, where ``played`` counts team-games and in TOTALS ``points`` equals
        ``points_against``). The request's ``season`` / ``mode`` / ``competition`` / ``segment`` /
        ``vs_conference`` / ``vs_team_id`` are stamped on each row; every figure is ``Float64`` (TOTALS sums or
        PER_GAME averages). A typed zero-row frame for empty input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_team_stats
            cebl_team_stats(mode="PER_GAME", season=2025, competition="REGULAR")
    """
    return _parse_list("team_stats", raw, return_as_pandas)


def parse_cebl_games(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /games/{season}/``: the schedule and results of one season.

    Args:
        raw: the wrapper's raw record or the bare API body (a list of game rows).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per game: ``game_id``, ``season`` (the year of ``start_time_utc``), ``genius_match_id`` (parsed
        from ``stats_url_en``: the join to the ``genius`` family, org CEBL, where play-by-play and shots live),
        ``start_time_utc`` (UTC), ``competition`` (REGULAR, FINALS), ``status`` (COMPLETE, and the API's own
        misspelling CANELLED for a cancelled game), ``period``, venue, the two teams with their ids and scores,
        and the link columns. ``home_team_score`` / ``away_team_score`` are null on some games (a cancelled game, or
        a completed 2026 row without a score). A typed zero-row frame for empty input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_games
            cebl_games(season=2025).select("game_id", "genius_match_id", "home_team_score", "away_team_score")
    """
    return _parse_list("games", raw, return_as_pandas)


def parse_cebl_player_stats(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /players/statistics/aggregated/``: a player statistic table.

    Args:
        raw: the wrapper's raw record (its ``_request`` carries the filters) or the bare API body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per player and team: the request's ``season`` / ``mode`` / ``competition`` / ``segment``,
        ``player_id``, ``full_name``, ``nationality``, the team (``team_id`` and names, a player traded in a
        season has a row for each team), ``played`` / ``started`` and the figures as ``Float64`` (TOTALS sums or
        PER_GAME averages; percentages 0-100; minutes decimal). A typed zero-row frame for empty input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_player_stats
            cebl_player_stats(mode="PER_GAME", season=2025).sort("points", descending=True).head()
    """
    return _parse_list("player_stats", raw, return_as_pandas)


def parse_cebl_player_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /players/{player_id}/statistics/``: one player's per-season rows and career row.

    Args:
        raw: the wrapper's raw record (its ``_request`` carries ``mode`` and ``competition``) or the bare body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per season and team, plus the career row (``season`` null, ``is_career`` true; the API sends it
        with ``career_only=False`` too, and ``career_only=True`` returns it alone). ``player_id`` is the player;
        figures as in :func:`parse_cebl_player_stats`. A typed zero-row frame for empty input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_player_seasons
            cebl_player_seasons(player_id=570965, mode="PER_GAME")
    """
    return _parse_list("player_seasons", raw, return_as_pandas)


def parse_cebl_player(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /players/{player_id}/``: one player's profile and current team.

    Args:
        raw: the wrapper's raw record or the bare API body (one player object).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row: ``player_id``, ``full_name``, ``nationality``, ``birth_date``, ``age`` (computed by the API on the
        day of the request), ``position``, ``jersey_number``, ``photo_url`` and the current team. An unknown
        player answers an object whose every field is null; that parses to a typed zero-row frame, as does any
        empty or malformed input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_player
            cebl_player(player_id=570965)
    """
    body, ctx = _unwrap(raw)
    if not isinstance(body, dict) or body.get("id") is None:
        return _finish(_frame("player", [], ctx), return_as_pandas)
    return _finish(_frame("player", [body], ctx), return_as_pandas)


def parse_cebl_player_gamelog(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /players/{player_id}/gamelog/``: one player's game log.

    Args:
        raw: the wrapper's raw record (its ``_request`` carries ``player_id``) or the bare API body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per game played: ``player_id`` (from the request), ``season`` (year of ``game_date``),
        ``game_date`` (UTC), ``genius_match_id`` (parsed from ``game_stats_url_en``), ``matchup`` as the site prints it
        ('OTT 112 vs 130 VAN', '@' for an away game), counts as ``Int64`` (the API sends ``rebounds`` as a string),
        ``minutes`` and the percentages as ``Float64`` (0-100). A typed zero-row frame for empty input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_player_gamelog
            cebl_player_gamelog(player_id=570965, season=2025)
    """
    return _parse_list("player_gamelog", raw, return_as_pandas)


def parse_cebl_player_leaders(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /players/statistics/leaders/``: the top players per requested statistic.

    Args:
        raw: the wrapper's raw record (its ``_request`` carries the filters) or the bare API body (an object keyed
            by statistic name, each a top-5 list).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A long frame, one row per statistic and leader: ``statistic`` (the key, POINTS ...), ``ranking`` (competition
        ranking: ties share a rank and skip the next, 1, 1, 1, 4), ``player_id``, the team, ``value`` as ``Float64``
        in the requested mode, plus the request's ``season`` / ``mode`` / ``competition``. A typed zero-row frame
        for empty input.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_player_leaders
            cebl_player_leaders(statistics="POINTS,ASSISTS", mode="PER_GAME", season=2025)
    """
    body, ctx = _unwrap(raw)
    rows: List[Dict[str, Any]] = []
    if isinstance(body, dict):
        for statistic, items in body.items():
            rows.extend({**r, "__statistic": str(statistic)} for r in _list_rows(items))
    return _finish(_frame("player_leaders", rows, ctx), return_as_pandas)


def parse_cebl_news(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``GET /news/articles/{language}/``: news articles in one language.

    The route answered ``[]`` on every probe of 2026-10-12 (languages en, fr, EN, en-ca, fr-ca, english), so its
    columns are the upstream schema's, unobserved.

    Args:
        raw: the wrapper's raw record or the bare API body (a list of article rows).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per article: ``news_id``, ``language`` (from the request), ``title``, ``published_at_utc``,
        ``image_url`` and ``html_content``. A typed zero-row frame for empty input, which is the usual answer.

    Raises:
        None: bad input yields the typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.cebl import cebl_news
            cebl_news(language="en")
    """
    return _parse_list("news", raw, return_as_pandas)
