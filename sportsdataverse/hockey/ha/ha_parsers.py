"""Parsers for the generated ``ha`` wrappers (HockeyAllsvenskan, ``hockeyallsvenskan.se``).

Every parser follows the package contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_cased columns (``dl_utils.underscore``), and a zero-row frame
(never an exception) for an empty or malformed body. Given ``columns`` (the generated wrappers
pass the endpoint's documented returns-schema), an empty body keeps those columns and a body
missing one of them gains it as nulls.

**Row rule** (:func:`parse_ha`, the generic parser): a list is rows; a dict's rows are its first
list under ``results`` (paged stats tables), ``data`` (team table, ``/api/game``),
``standings``, ``players`` or ``games`` (the schedule page); any other non-empty dict is one
row. The stats bodies also carry ``teams`` and ``sort_by_options`` lists beside their rows;
those are not rows (``teams`` has its own parser, :func:`parse_ha_teams`). Nested objects
flatten into ``<parent>_<child>`` columns, a CMS ``logo`` object keeps only its ``url``
(``<parent>_logo_url``), and lists become JSON strings.

**Id key spaces** (the traps: the site joins three systems, and each keeps its own key).

* Teams, three spaces, kept in separately named columns, never merged:

  * ``*statnet_id`` (``team_statnet_id``, ``home_statnet_id``, ``away_statnet_id``): the
    StatNet team id, a String (``AIK``, ``MODO``, ``MIK``, ``NYB``, ``OSIK``). Play-by-play
    ``team``, the skater / goalie tables' ``team``, the leaderboard ``teamID``, the CMS game
    ``home/awayStatNetId``.
  * ``*team_code``: the display code, a String (``AIK``, ``MoDo``, ``MORA``, ``NVIF``,
    ``ÖIK``). Games-table ``HomeTeam`` / ``AwayTeam``, team-table ``team_code``, standings
    ``statNetClubLabel``, play-by-play ``team.code``, leaderboard ``teamCode``, CMS
    ``shortName``.
  * ``*team_cms_id``: the CMS (Strapi) team record id, Int64 (``81`` = AIK). CMS games and
    the player directory only.

  :func:`parse_ha_teams` (the stats bodies' ``teams`` list) is the StatNet id <-> display
  code crosswalk. Standings ``teamCode`` MIXES the first two spaces (the StatNet id ``MODO``
  for MoDo, the display code for Mora, Nybro and Östersund), so it is kept only as
  ``team_code_mixed``; join standings on ``team_code`` (from ``statNetClubLabel``).
* Players, two spaces: ``player_statnet_id`` (Int64; play-by-play ``player.statNetId``,
  leaderboard ``playerStatNetId``; the same id space as SHL's isa id, verified on one player)
  and ``player_cms_id`` (Int64) / ``player_document_id`` (String) in the CMS directory. The
  skater, goalie and games tables carry NAMES only (``player_name``), and play-by-play assists
  are names only.
* Games: ``statnet_game_number`` (Int64; ``23401`` = 2026/27 game 1), ``game_cms_id`` (Int64)
  and ``slug`` (String) in CMS records. The games table carries none of them.

**Wins and losses mean different things by table.** The team table's ``W`` / ``L`` are TOTALS
including overtime and shootout results: ``wins_total`` / ``losses_total`` (``otw``, ``sow``,
``otl``, ``sol`` are subsets of them). Standings ``wins`` / ``losses`` are REGULATION only:
``regulation_wins`` / ``regulation_losses``, and its overtime counts include shootouts:
``overtime_and_shootout_wins`` / ``_losses``.

**Season** is the END year (2026 = 2025/26). The stats tables do not say which season they
answer, so it comes from the request the runtime stamps under ``_request``; CMS game records
carry ``season_2026`` (a START year), which becomes ``season`` 2027 (``cms_season`` keeps the
raw value).

**Dtypes come from the returns-schemas** (:data:`._ha_schema.DTYPES`, written by
``tools/codegen/gen_ha.py``), then from name rules for a column no schema lists, never from a
sample, so an empty frame has the dtypes of a populated one: the site sends ``efficiency`` as
``"8.82"``, ``0`` or null in one column, and every standings value as a string. Rates
(``*perc``, ``efficiency``, ``gaa``, ``gpg``, ``gapg``, ``p_gp``, ``tppg``, arena
coordinates) are Float64 (``""`` -> null); counts sent as strings are Int64. Clock strings
(``toi_gp`` ``"18:10"``, ``mip``) and ``metric_value`` (a number or a clock, by metric) stay
String.

**Play-by-play** has no shot coordinates and no on-ice players. One row per event (Shot,
Goal, Penalty, GoalkeeperEvent, Timeout); ``time`` is seconds into the period.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64
from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.ha._ha_schema import DTYPES

__all__ = [
    "parse_ha",
    "parse_ha_games",
    "parse_ha_leaders",
    "parse_ha_pbp",
    "parse_ha_players",
    "parse_ha_schedule",
    "parse_ha_standings",
    "parse_ha_team_stats",
    "parse_ha_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``ha_runtime.REQUEST_KEY``).
_REQUEST = "_request"
_ROW_KEYS = ("results", "data", "standings", "players", "games")
#: Team StatNet ids are text (``AIK``, ``MODO``); every other ``*_id`` is a number.
_STR_ID = re.compile(r"(^|_)statnet_id$")
_INT_ID = re.compile(r"(^|_)(id|cms_id|player_statnet_id|statnet_game_number)$")
_FLOAT_COL = re.compile(r"(perc|(^|_)(efficiency|gaa|gpg|gapg|p_gp|tppg|latitude|longitude))$")
#: Counts the site sends as strings (standings, CMS games, play-by-play, the directory).
_INT_COLS = frozenset(
    {
        "rank",
        "games_played",
        "regulation_wins",
        "regulation_losses",
        "ties",
        "overtime_and_shootout_wins",
        "overtime_and_shootout_losses",
        "shootouts_wins",
        "shootouts_losses",
        "goals",
        "goals_against",
        "goal_difference",
        "total_points",
        "round",
        "attendance",
        "home_score",
        "away_score",
        "home_score_p1",
        "home_score_p2",
        "home_score_p3",
        "away_score_p1",
        "away_score_p2",
        "away_score_p3",
        "home_ot_score",
        "away_ot_score",
        "home_so_score",
        "away_so_score",
        "season",
        "period",
        "time",
        "scorer_season_goals",
        "player_jersey_number",
        "assist1_jersey_today",
        "assist1_season_assists",
        "assist2_jersey_today",
        "assist2_season_assists",
        "jersey_number",
        "height",
        "weight",
        # the sampled stats-table counts (sent as numbers; pinned so an empty frame matches a full one)
        "gp",
        "tp",
        "g",
        "a",
        "pim",
        "gwg",
        "ppg",
        "sog",
        "hits",
        "bks",
        "plus",
        "minus",
        "plus_minus",
        "svs",
        "gpi",
        "ga",
        "w",
        "t",
        "l",
        "so",
        "valid_ranking",
        "wins_total",
        "losses_total",
        "leading",
        "trailing",
        "gfg",
        "otw",
        "otl",
        "sow",
        "sol",
        "points",
        *(
            f"{side}_{c}"
            for side in ("home", "away")
            for c in ("shots_total", "shots_goal", "powerplay_goals", "powerplay_num_pp", "expulsions_in_minutes")
        ),
    }
)
#: Object-valued keys that are sometimes null (a player without headshots): no stray null column.
_OBJECT_KEYS = frozenset({"headshots", "logo", "logos", "team", "player", "Assist1", "Assist2"})
_BOOL_COLS = frozenset({"is_penalty_shot", "is_played", "is_entering", "is_completed", "is_rookie"})
_SIDES = (("home", "home_team"), ("away", "away_team"))
_SCHEMA_TYPES = {"integer": pl.Int64, "numeric": pl.Float64, "logical": pl.Boolean, "character": pl.String}
#: Separators a head count may carry (``1 386``, ``1,386``, ``1.386``); blanks become null.
_COUNT_NOISE = r"[\s,.'\u00a0\u202f]"


# --- helpers ------------------------------------------------------------------------------------


def _col(name: str) -> str:
    return underscore(name).replace("stat_net", "statnet")


def _flatten(obj: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """One flat row: nested objects as ``<parent>_<child>``, a ``logo`` as its url, lists as JSON."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        if key == _REQUEST:
            continue
        name = prefix + _col(str(key))
        if value is None and key in _OBJECT_KEYS:
            continue
        if key == "logo" and isinstance(value, dict):
            out[name + "_url"] = value.get("url")
        elif isinstance(value, list):
            out[name] = json.dumps(value, ensure_ascii=False) if value else None
        elif isinstance(value, dict):
            out.update(_flatten(value, name + "_"))
        else:
            out[name] = value
    return out


def _request(obj: Any) -> Dict[str, Any]:
    req = obj.get(_REQUEST) if isinstance(obj, dict) else None
    return dict(req) if isinstance(req, dict) else {}


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Dict[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _rows(raw: Any) -> List[Dict[str, Any]]:
    if isinstance(raw, list):
        return _list(raw)
    if not isinstance(raw, dict) or not raw:
        return []
    for key in _ROW_KEYS:
        if key in raw:  # a row key that is not a list is a malformed body, not one row
            return _list(raw[key])
    body = {k: v for k, v in raw.items() if k != _REQUEST}
    return [body] if body else []


def _rename(row: Dict[str, Any], names: Mapping[str, str]) -> Dict[str, Any]:
    return {names.get(k, k): v for k, v in row.items()}


def _stamp(row: Dict[str, Any], req: Mapping[str, Any]) -> Dict[str, Any]:
    """Request keys first, never overriding the body."""
    return {**{k: v for k, v in req.items() if k not in row}, **row}


def _dtype_for(col: str) -> Any:
    """The dtype of a column: its returns-schema type, else by name alone."""
    if col in DTYPES:
        return _SCHEMA_TYPES[DTYPES[col]]
    if _STR_ID.search(col) and not col.endswith("player_statnet_id"):
        return pl.String
    if _INT_ID.search(col) or col in _INT_COLS:
        return pl.Int64
    if _FLOAT_COL.search(col):
        return pl.Float64
    if col in _BOOL_COLS:
        return pl.Boolean
    return pl.String


def _number(expr: pl.Expr, dtype: Any) -> pl.Expr:
    return expr.cast(pl.String).str.strip_chars().cast(dtype, strict=False)


def _count(expr: pl.Expr) -> pl.Expr:
    """A head count to Int64: separators dropped, a blank or non-number to null."""
    return expr.cast(pl.String).str.replace_all(_COUNT_NOISE, "").cast(pl.Int64, strict=False)


def _typed(df: pl.DataFrame) -> pl.DataFrame:
    """Ids, counts and rates to their pinned dtypes; ``"true"`` / ``"false"`` to Boolean."""
    df = _cast_ids_int64(df, [c for c in df.columns if _dtype_for(c) == pl.Int64 and _INT_ID.search(c)])
    casts = []
    for col, dtype in df.schema.items():
        want = _dtype_for(col)
        if dtype == want:
            continue
        if dtype == pl.Null:
            casts.append(pl.col(col).cast(want))
        elif col == "attendance":
            casts.append(_count(pl.col(col)).alias(col))
        elif want == pl.Boolean and dtype == pl.String:
            casts.append(pl.col(col).str.to_lowercase().replace_strict({"true": True, "false": False}, default=None))
        elif want in (pl.Int64, pl.Float64) and not _INT_ID.search(col):
            casts.append(_number(pl.col(col), want).alias(col))
        elif want == pl.String:
            casts.append(pl.col(col).cast(pl.String))
    return df.with_columns(casts) if casts else df


def _frame(rows: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    rows = [r for r in rows if r]
    if not rows:
        return pl.DataFrame()
    try:
        df = pl.from_dicts(rows, infer_schema_length=None, strict=False)
    except Exception:  # noqa: BLE001 -- a malformed body parses to nothing, never raises
        return pl.DataFrame()
    return _typed(df)


def _finish(df: pl.DataFrame, columns: Optional[Sequence[str]], return_as_pandas: bool) -> Frame:
    """Apply the documented columns: an empty frame keeps them, a missing one is added as nulls,
    and they lead in documented order (undocumented extras after), so frames concat."""
    if columns:
        if df.width == 0:
            df = pl.DataFrame(schema={c: _dtype_for(c) for c in columns})
        else:
            missing = [pl.lit(None, dtype=_dtype_for(c)).alias(c) for c in columns if c not in df.columns]
            if missing:
                df = df.with_columns(missing)
            df = df.select([*columns, *(c for c in df.columns if c not in columns)])
    return df.to_pandas() if return_as_pandas else df


def _table(raw: Any, names: Mapping[str, str], columns: Optional[Sequence[str]], return_as_pandas: bool) -> Frame:
    req = _request(raw)
    rows = (_stamp(_rename(_flatten(r), names), req) for r in _rows(raw))
    return _finish(_frame(rows), columns, return_as_pandas)


_GENERIC = {"player": "player_name", "team": "team_statnet_id"}


# --- generic and stats tables -------------------------------------------------------------------


def parse_ha(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Generic HockeyAllsvenskan body -> frame by the module's row rule.

    Serves the skater and goalkeeper tables (``results``): ``player`` becomes ``player_name``
    (names only, no player id) and ``team`` (the StatNet team id) ``team_statnet_id``; the
    request's ``season`` (END year) leads each row.

    Args:
        raw: the decoded JSON body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per record; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha
            parse_ha({"results": [{"player": "Scott Pooley", "team": "AIK", "GP": 51}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_skater_stats` - the route this works on.
        * :func:`sportsdataverse.hockey.ha.ha_goalie_stats` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _table(raw, _GENERIC, columns, return_as_pandas)


def parse_ha_team_stats(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/api/all-time-teams-stats`` -> one row per team (``team_code`` is the display code).

    ``W`` / ``L`` are TOTALS including overtime and shootout results, so they become
    ``wins_total`` / ``losses_total`` (2025/26 IFB: 41 = 34 regulation + 3 ``otw`` + 4 ``sow``).
    Standings ``regulation_wins`` count regulation wins only: do not mix them.

    Args:
        raw: the team-table body (``{"data": [...], ...}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_team_stats
            parse_ha_team_stats({"data": [{"team_code": "IFB", "W": 41, "L": 11, "OTW": 3, "SOW": 4}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_team_stats` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _table(raw, {"w": "wins_total", "l": "losses_total"}, columns, return_as_pandas)


def parse_ha_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """A stats body's ``teams`` list -> the StatNet id <-> display code crosswalk, one row per team.

    Args:
        raw: any paged stats body (``{"results": [...], "teams": [...]}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        ``season``, ``team_statnet_id``, ``team_code``, ``name``, ``full_name`` and the logo
        urls; zero rows for a body without ``teams``.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_teams
            parse_ha_teams({"teams": [{"statNetId": "MIK", "teamCode": "MORA", "name": "Mora IK"}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_teams` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    req = _request(raw)
    teams = _list(raw.get("teams")) if isinstance(raw, dict) else []
    names = {"statnet_id": "team_statnet_id", "team_code": "team_code"}
    rows = (_stamp(_rename(_flatten(t), names), req) for t in teams)
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_ha_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/api/league-standings-all-time`` -> one row per team, every count as Int64.

    ``statNetClubLabel`` (the display code) is ``team_code``; the upstream ``teamCode`` mixes
    the StatNet id and the display code and is kept only as ``team_code_mixed``. ``wins`` /
    ``losses`` are REGULATION only (``regulation_wins`` / ``regulation_losses``) and the
    overtime counts include shootouts (``overtime_and_shootout_wins`` / ``_losses``).
    ``power_play_perc`` / ``penalty_kill_perc`` arrive as ``""`` (null).

    Args:
        raw: the standings body (``{"standings": [...]}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_standings
            parse_ha_standings({"standings": [{"teamCode": "MODO", "statNetClubLabel": "MoDo", "wins": "24"}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_standings` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    names = {
        "team_code": "team_code_mixed",
        "statnet_club_label": "team_code",
        "wins": "regulation_wins",
        "losses": "regulation_losses",
        "overtime_wins": "overtime_and_shootout_wins",
        "overtime_losses": "overtime_and_shootout_losses",
    }
    return _table(raw, names, columns, return_as_pandas)


# --- games --------------------------------------------------------------------------------------


def _games_row(game: Mapping[str, Any]) -> Dict[str, Any]:
    result = _obj(game.get("GameResult"))
    stats = _obj(game.get("GameStatistics"))
    decided = game.get("decided_in")
    row: Dict[str, Any] = {
        "date": game.get("Date"),
        "home_team_code": game.get("HomeTeam"),
        "away_team_code": game.get("AwayTeam"),
        "home_score": result.get("Home"),
        "away_score": result.get("Away"),
        "decided_in": decided,
        "is_played": None if not isinstance(decided, str) else decided != "",
        "arena": game.get("Arena"),
        "attendance": game.get("Attendance"),
    }
    for side, key in _SIDES:
        block = stats.get(key)
        row.update(_flatten(block if isinstance(block, dict) else {}, f"{side}_"))
    for key, name in (("PeriodScores", "period_scores"), ("MostPoints", "most_points")):
        value = game.get(key)
        row[name] = json.dumps(value, ensure_ascii=False) if isinstance(value, list) and value else None
    return row


def parse_ha_games(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/api/all-time-games-stats`` -> one row per game of the season, played or not.

    The season in progress lists every scheduled game. An unplayed one has ``decided_in == ""``,
    a 0-0 score and a null attendance, so ``is_played`` is ``decided_in != ""``: filter on it
    rather than on the score. ``home_team_code`` / ``away_team_code`` are display codes, while
    the ``most_points`` JSON names StatNet team ids. The table carries no game id: join CMS games
    on ``date`` + team codes. Team statistics (shots, power play, penalty minutes) are
    ``home_*`` / ``away_*``; ``*_efficiency`` is a percentage.

    Args:
        raw: the games-table body (``{"results": [...], ...}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per game; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_games
            df = parse_ha_games({"results": [{"Date": "2027-03-05", "HomeTeam": "BIK", "decided_in": ""}]})
            df["is_played"]

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_games` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    req = _request(raw)
    rows = (_stamp(_games_row(g), req) for g in _rows(raw))
    return _finish(_frame(rows), columns, return_as_pandas)


def _cms_game_row(game: Mapping[str, Any]) -> Dict[str, Any]:
    row = _flatten(game)
    names = {"id": "game_cms_id", "document_id": "game_document_id", "season": "cms_season"}
    for side, _key in _SIDES:
        names[f"{side}_team_id"] = f"{side}_team_cms_id"
        names[f"{side}_team_short_name"] = f"{side}_team_code"
    row = _rename(row, names)
    m = re.fullmatch(r"season_(\d{4})", str(row.get("cms_season") or ""))
    return {"season": int(m.group(1)) + 1 if m else None, **row}


def parse_ha_schedule(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """CMS game records (the schedule page's ``games``, ``/api/game``'s ``data``) -> one row per game.

    ``slug``, ``statnet_game_number``, ``home_statnet_id``, ``away_statnet_id`` and
    ``scheduled_date_time`` are the play-by-play request. ``game_cms_id`` and
    ``home_team_cms_id`` / ``away_team_cms_id`` are CMS ids; ``home_team_code`` /
    ``away_team_code`` are display codes. ``season`` is the END year (``cms_season``
    ``season_2026`` = 2026/27 -> 2027). Scores and attendance are sent as strings and parsed to
    Int64 (an attendance of 0 for a played game means unknown).

    Args:
        raw: the schedule-page props (``{"games": [...]}``) or an ``/api/game`` body
            (``{"data": [...]}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per game; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_schedule
            parse_ha_schedule({"data": [{"id": 6566, "slug": "20260918-aik-modo", "season": "season_2026"}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_schedule` - the route this works on.
        * :func:`sportsdataverse.hockey.ha.ha_game` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _finish(_frame(_cms_game_row(g) for g in _rows(raw)), columns, return_as_pandas)


# --- play-by-play -------------------------------------------------------------------------------


def parse_ha_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/api/play-by-play`` -> one row per event (Shot, Goal, Penalty, GoalkeeperEvent, Timeout).

    The body names no game, so ``statnet_game_number``, ``home_statnet_id`` and
    ``away_statnet_id`` come from the stamped request. ``period`` and ``time`` (seconds into
    the period) are Int64; ``team_statnet_id`` is the StatNet team id and ``team_code`` the
    display code; ``player_statnet_id`` is the StatNet player id (Int64). Goals carry
    ``running_score``, the strength in ``event_description`` (``EQ``) and the assists by NAME
    (``assist1_name``); shots carry the outcome text. No shot coordinates, no on-ice players.

    Args:
        raw: the play-by-play body (``{"game_info": {...}, "game_events": [...]}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per event in period order; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_pbp
            parse_ha_pbp({"game_events": [{"Period": "1", "Events": [{"type": "Shot", "time": "44"}]}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_pbp` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    req = _request(raw)
    rows = []
    for period in _list(raw.get("game_events") if isinstance(raw, dict) else None):
        head = {"period": period.get("Period"), "period_label": period.get("period_label")}
        for event in _list(period.get("Events")):
            row = _rename(_flatten(event), {"type": "event_type"})
            rows.append(_stamp({**head, **row}, req))
    return _finish(_frame(rows), columns, return_as_pandas)


# --- leaderboard and player directory -----------------------------------------------------------


def parse_ha_leaders(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/api/player-leaderboard`` -> one row per leader (the current season's top 25 of one metric).

    The only stats route with a player id: ``player_statnet_id`` (Int64, the StatNet player id).
    ``team_statnet_id`` (from ``teamID``) is the StatNet team id and ``team_code`` the display
    code. ``metric_value`` stays String (a count for ``TP``, a clock for ``TOI/GP``).

    Args:
        raw: the leaderboard body (``{"players": [...], "totalCount": 25, ...}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_leaders
            parse_ha_leaders({"players": [{"rank": 1, "playerStatNetId": "4204", "teamID": "LIF", "metric": "TP"}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_leaders` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _table(raw, {"player": "player_name", "team_id": "team_statnet_id"}, columns, return_as_pandas)


def parse_ha_players(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/api/all-players`` -> one row per player of the CMS directory (one team per call).

    Ids are CMS ids (``player_cms_id``, ``player_document_id``, ``team_cms_id``), not StatNet
    ids; ``team_code`` is the team's display code. ``slug`` joins the leaderboard. The headshot
    file names start with the StatNet player id (an observation, not a field).

    Args:
        raw: the directory body (``{"players": [...], "pagination": {...}}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import parse_ha_players
            parse_ha_players({"players": [{"id": 2690, "slug": "christopher-bengtsson", "team": {"id": 81}}]})

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_players` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    names = {
        "id": "player_cms_id",
        "document_id": "player_document_id",
        "team_id": "team_cms_id",
        "team_short_name": "team_code",
    }
    return _table(raw, names, columns, return_as_pandas)
