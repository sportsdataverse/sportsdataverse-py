"""Parsers for the generated ``scb`` wrappers (BBL and DBBL, Germany: the SCB API on two hosts).

Every parser follows the package contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_cased columns (``dl_utils.underscore``), and a zero-row frame (never an
exception) for an empty or malformed body. Given ``columns`` (the generated wrappers pass the endpoint's
documented returns-schema), an empty body keeps those columns with the schema's dtypes, so two empty frames
stack onto a populated one.

**Dedicated shapers, no generic row rule.** Each route has its own shaper that picks the columns worth
keeping and names them. The bodies are nested (a game row holds two team objects, a box score holds two
teams of player lines, the playoff tree holds series that hold games), so one parser per frame:

* ``/games``, ``/games/recent`` and ``/games/{gameId}`` -> :func:`parse_scb_games` (one row per game; the
  header adds ``stat_status`` / ``game_progress``);
* ``/games/{gameId}/stats`` -> :func:`parse_scb_game_players`, :func:`parse_scb_game_teams` and
  :func:`parse_scb_game_actions` (play-by-play with shot x/y);
* ``/seasons/{seasonId}`` -> :func:`parse_scb_season_players` (the id crosswalk) and
  :func:`parse_scb_season_teams`; the playoff tree -> :func:`parse_scb_playoff_series` and
  :func:`parse_scb_playoff_games`; ``/transfers/teams`` -> :func:`parse_scb_transfers_teams` and
  :func:`parse_scb_team_rosters`; ``/teams/{teamId}`` -> :func:`parse_scb_team` and
  :func:`parse_scb_team_seasons`.

**Id key spaces.** Every id column is a ``String`` except the ones the API always sends as integers
(``real_player_id``, ``season_id``, ``action_id``, ``order_id``). The API sends ids as strings in most
routes and as integers in the playoff tree and a few standings blocks; they are cast to strings here
(never through a float, so no ``"123.0"``).

* ``game_id``: a game is ``(league, game_id)``. BBL and DBBL are separate sequences on separate hosts
  that overlap numerically, so every frame carries ``league`` (``"bbl"`` / ``"dbbl"``). Ids are not
  chronological (see the refs README).
* ``season_player_id``: one player's engagement with one club (kept across seasons while he stays,
  new after a move; a player traded mid-season has two in one season). It is what box-score lines, actions,
  records, ``/season-players/{id}`` and the ``topScorer`` of a game carry.
* ``real_player_id`` (``Int64``): the person, the same across clubs and seasons. Join players across
  seasons on this one.
* The API's ``playerId`` is **dropped**: it repeats the season-player id in box scores, records and
  ``/season-players`` but is the person id in ``/seasons/{id}`` ``players[]`` and ``/transfers``, so it
  cannot be a column with one meaning. Both meanings are available under their own names.
* ``team_id`` (the club) equals ``season_team_id`` in every sample; key teams by ``team_id``. The records
  routes' ``seasonTeamId`` carries a season-player id, so it is dropped there.
* ``topScorer.id`` on DBBL is a third key space that matches nothing; ``top_scorer_season_player_id`` is
  built from ``topScorer.playerId`` (the season-player id on both hosts).

**Season.** ``season`` is the END year (2026 = 2025-26, the sdv key); ``season_id`` is the API's own
START-year ``seasonId`` (2025 for 2025-26). Bodies that do not name their season (the player and team
stat tables, the box score) take it from the request the runtime stamps under ``_request``.

**Numbers.** Percentages are 0-100 and ``Float64`` even when whole (the API sends ``50`` for 50.0); box-score
team-line counts that arrive as strings (``"12000"``) are integers here. The 2005-06 box-score totals
disagree with their own 2-point plus 3-point parts, so the box-score frames recompute
``field_goals_made`` / ``field_goals_attempted`` (and the percentage) from the parts and flag it in
``field_goals_recomputed``. Shot ``x`` / ``y`` of ``(0, 0)`` (no location) become null.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.scb.scb_dtypes import DTYPES
from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_scb_game_actions",
    "parse_scb_game_players",
    "parse_scb_game_teams",
    "parse_scb_games",
    "parse_scb_games_recent",
    "parse_scb_player_statistics",
    "parse_scb_playoff_games",
    "parse_scb_playoff_series",
    "parse_scb_records_per_game",
    "parse_scb_records_season_averages",
    "parse_scb_referees",
    "parse_scb_season_player",
    "parse_scb_season_players",
    "parse_scb_season_teams",
    "parse_scb_seasons",
    "parse_scb_standings",
    "parse_scb_team",
    "parse_scb_team_rosters",
    "parse_scb_team_seasons",
    "parse_scb_team_statistics",
    "parse_scb_transfers",
    "parse_scb_transfers_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Columns = Optional[Sequence[str]]

#: Where the runtime stamps the request (``scb_runtime.REQUEST_KEY``).
_REQUEST = "_request"
_CODES = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
#: Ids the API always sends as integers (every other ``*_id`` is a string).
_INT_IDS = frozenset({"real_player_id", "season_id", "action_id", "order_id", "all_time_order_id", "timestamp"})
_ID = re.compile(r"(^|_)id$")
_FLOAT = re.compile(r"percent|ratio|per_game|average|height|(^|_)value$")
_STR = re.compile(
    r"(^|_)(name|url|time|tlc|type|status|progress|function|position|stage|competition|bucket|side|scope|stat|"
    r"direction|round|streak|channel|address|nationalities|teams|ids|qualifiers|identifier|date|achievements|"
    r"outcomes|corrections|info|mascot|aggregation|short|reference|shirt_number)$"
)
_BOOL = re.compile(r"^(is|has)_|^overall$|_recomputed$")
_NUM = re.compile(r"^-?\d+(\.\d+)?$")
_PERIOD = re.compile(r"^(Q|OT)(\d+)$")
_CLOCK = re.compile(r"^(\d+):(\d{2}):(\d{2})$")
_OT = 9  # the API's result carries OT1..OT9 per side


# --- value helpers --------------------------------------------------------------------------------


def _i(v: Any) -> Optional[int]:
    """An integer from an int, a whole float or a digit string (``"12000"``); else ``None``."""
    if v is None or isinstance(v, bool):
        return int(v) if isinstance(v, bool) else None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == v and v.is_integer() else None
    if isinstance(v, str) and re.fullmatch(r"\s*-?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if v == v else None
    if isinstance(v, str) and _NUM.match(v.strip()):
        return float(v)
    return None


def _b(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, int) and v in (0, 1):
        return bool(v)
    return None


def _s(v: Any) -> Optional[str]:
    """A string; an integer (or whole float) id becomes its digits, never ``"123.0"``; a list or object becomes JSON."""
    if v is None:
        return None
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, str):
        return v.strip() or None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else str(v)
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def _numify(d: Any) -> Dict[str, Any]:
    """A stat block with its numeric strings made numbers (``"12000"`` -> 12000, ``""`` -> ``None``)."""
    out: Dict[str, Any] = {}
    for k, v in (d if isinstance(d, dict) else {}).items():
        if isinstance(v, str):
            t = v.strip()
            v = None if t == "" else (int(t) if re.fullmatch(r"-?\d+", t) else float(t) if _NUM.match(t) else v)
        out[k] = v
    return out


def _period(v: Any) -> Optional[int]:
    """Period number from ``4`` / ``"Q4"`` / ``"OT1"`` (-> 5); ``""`` and anything else -> ``None``."""
    if isinstance(v, int) and not isinstance(v, bool):
        return v
    m = _PERIOD.match(v.strip()) if isinstance(v, str) else None
    if not m:
        return None
    n = int(m.group(2))
    return n if m.group(1) == "Q" else 4 + n


def _seconds_left(clock: Any) -> Optional[int]:
    m = _CLOCK.match(clock) if isinstance(clock, str) else None
    return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)) if m else None


def _obj(v: Any) -> Dict[str, Any]:
    """``v`` when it is an object, else an empty one."""
    return v if isinstance(v, dict) else {}


def _dicts(v: Any) -> List[Dict[str, Any]]:
    return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []


def _request(raw: Any) -> Dict[str, Any]:
    """The request the runtime stamped on a body (or on its first row)."""
    if isinstance(raw, dict) and isinstance(raw.get(_REQUEST), dict):
        return raw[_REQUEST]
    rows = _dicts(raw)
    return _obj(rows[0].get(_REQUEST)) if rows else {}


def _head(req: Mapping[str, Any]) -> Dict[str, Any]:
    return {"league": req.get("league"), "season": req.get("season")}


def _plus1(season_id: Any) -> Optional[int]:
    n = _i(season_id)
    return None if n is None else n + 1


def _join(v: Any) -> Optional[str]:
    """A list of strings as ``"a,b"``; a string as it is."""
    if isinstance(v, list):
        return ",".join(str(x) for x in v) or None
    return _s(v)


# --- frame building -------------------------------------------------------------------------------


def _rule(name: str) -> Optional[str]:
    """The dtype code a column name implies, or ``None`` when its values decide."""
    if name in _INT_IDS or name == "season":
        return "i"
    if _ID.search(name):
        return "s"
    if _BOOL.search(name):
        return "b"
    if _STR.search(name):
        return "s"
    if _FLOAT.search(name):
        return "f"
    return None


def _infer(values: Iterable[Any]) -> str:
    seen = [v for v in values if v is not None]
    if not seen:
        return "s"
    if all(isinstance(v, bool) for v in seen):
        return "b"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in seen):
        return "i"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in seen):
        return "f"
    return "s"


def _code(parser: str, name: str, values: Iterable[Any] = ()) -> str:
    """The dtype code of a column: the parser's returns-schema (``scb_dtypes.DTYPES``), the name rules, then the values."""
    return DTYPES.get(parser, {}).get(name) or _rule(name) or _infer(values)


_COERCE = {"i": _i, "f": _f, "b": _b, "s": _s}


def _frame(parser: str, rows: List[Dict[str, Any]], columns: Columns, return_as_pandas: bool) -> Frame:
    """Rows -> a frame with pinned dtypes; an empty body keeps ``columns``, a body missing one gains it as nulls."""
    names: List[str] = list(columns or [])
    if not names and not rows:  # no body and no ``columns=``: the parser's own documented schema
        names = list(DTYPES.get(parser, {}))
    for r in rows:
        names += [k for k in r if k not in names]
    series = []
    for n in names:
        vals = [r.get(n) for r in rows]
        code = _code(parser, n, vals)
        series.append(pl.Series(n, [_COERCE[code](v) for v in vals], dtype=_CODES[code], strict=False))
    df = pl.DataFrame(series) if series else pl.DataFrame()
    return df.to_pandas() if return_as_pandas else df


def _row(row: Mapping[str, Any], rename: Mapping[str, str], drop: Iterable[str] = ()) -> Dict[str, Any]:
    """Flatten a nested row: ``rename`` (dotted wire path -> column) first and in its order, the rest as
    ``underscore(path)``; ``drop`` paths and the request stamp are skipped. A list cell is kept here and becomes compact
    JSON text in the frame (``_s``): no column is ever a polars List."""
    skip = set(drop)
    named: Dict[str, Any] = {out: None for out in rename.values()}
    rest: Dict[str, Any] = {}

    def walk(node: Mapping[str, Any], path: str) -> None:
        for k, v in node.items():
            p = f"{path}.{k}" if path else k
            if p in skip or (not path and k == _REQUEST):
                continue
            if p in rename:
                named[rename[p]] = v
            elif isinstance(v, dict):
                walk(v, p)
            else:
                rest[underscore(p.replace(".", "_"))] = v

    walk(row, "")
    return {**named, **rest}


def _fix_field_goals(row: Dict[str, Any]) -> Dict[str, Any]:
    """Recompute field goals from the 2- and 3-point parts when the totals disagree (2005-06 box scores)."""
    ints = [
        v
        for v in (
            row.get(k)
            for k in (
                "two_point_shots_made",
                "three_point_shots_made",
                "two_point_shots_attempted",
                "three_point_shots_attempted",
            )
        )
        if isinstance(v, int) and not isinstance(v, bool)
    ]
    row["field_goals_recomputed"] = False
    if len(ints) == 4:
        made, att = ints[0] + ints[1], ints[2] + ints[3]
        if (row.get("field_goals_made"), row.get("field_goals_attempted")) != (made, att):
            row.update(
                field_goals_made=made,
                field_goals_attempted=att,
                field_goals_success_percent=round(100 * made / att, 1) if att else 0.0,
                field_goals_recomputed=True,
            )
    return row


# --- /seasons ---------------------------------------------------------------------------------------


def parse_scb_seasons(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/seasons`` -> one row per season, newest first.

    ``season_id`` is the API's START year (2026 = 2026-27); ``season`` is the sdv END year (``season_id + 1``).

    Args:
        raw: the ``/seasons`` body (a list of ``{id, isActive}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per season: ``league``, ``season``, ``season_id``, ``is_active``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_seasons
            scb_seasons(league="dbbl").head()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_seasons
            parse_scb_seasons([{"id": 2025, "isActive": True}])
    """
    rows = [
        {
            "league": _request(r).get("league"),
            "season": _plus1(r.get("id")),
            "season_id": r.get("id"),
            "is_active": r.get("isActive"),
        }
        for r in _dicts(raw)
    ]
    return _frame("parse_scb_seasons", rows, columns, return_as_pandas)


def _season_body(raw: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    body = raw if isinstance(raw, dict) else {}
    head = {"league": _request(raw).get("league"), "season": _plus1(body.get("id")), "season_id": body.get("id")}
    return body, head


def parse_scb_season_players(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/seasons/{seasonId}`` -> one row per player registered in the season: the id crosswalk.

    The row joins ``season_player_id`` (one engagement with one club; box scores, actions and records use
    it) to ``real_player_id`` (the person, across clubs and seasons) and ``season_team_id``. The API's
    ``playerId`` here is the person id, equal to ``real_player_id``, so it is not repeated.

    Args:
        raw: the ``/seasons/{seasonId}`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, season_player_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_season_players
            scb_season_players(season=2027).head()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_season_players
            parse_scb_season_players({"id": 2026, "players": [{"seasonPlayerId": "56414", "realPlayerId": 37539}]})
    """
    body, head = _season_body(raw)
    rows = [
        {
            **head,
            "season_player_id": p.get("seasonPlayerId"),
            "real_player_id": p.get("realPlayerId"),
            "season_team_id": p.get("seasonTeamId"),
            "first_name": p.get("firstName"),
            "last_name": p.get("lastName"),
        }
        for p in _dicts(body.get("players"))
    ]
    return _frame("parse_scb_season_players", rows, columns, return_as_pandas)


def parse_scb_season_teams(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/seasons/{seasonId}`` -> one row per team of the season.

    Args:
        raw: the ``/seasons/{seasonId}`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, season, team_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_season_teams
            scb_season_teams(season=2027, league="bbl").head()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_season_teams
            parse_scb_season_teams({"id": 2026, "teams": [{"teamId": "413", "seasonTeamId": "413", "name": "ALBA BERLIN"}]})
    """
    body, head = _season_body(raw)
    rows = [
        {
            **head,
            "team_id": t.get("teamId"),
            "season_team_id": t.get("seasonTeamId"),
            "team_name": t.get("name"),
            "tlc": t.get("tlc"),
            "logo_url": t.get("logoUrl"),
        }
        for t in _dicts(body.get("teams"))
    ]
    return _frame("parse_scb_season_teams", rows, columns, return_as_pandas)


# --- playoff tree -----------------------------------------------------------------------------------

_ROUNDS = (("quarterFinals", "quarter_finals"), ("semiFinals", "semi_finals"), ("finals", "finals"))


def _series(body: Mapping[str, Any]) -> Iterable[Tuple[str, int, Dict[str, Any]]]:
    """``(round, 1-based series number, series)``; ``finals`` is one object, the others lists."""
    for key, name in _ROUNDS:
        node = body.get(key)
        items = [node] if isinstance(node, dict) and node else _dicts(node)
        for n, s in enumerate(items, start=1):
            yield name, n, s


def parse_scb_playoff_series(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/seasons/{seasonId}/playoff-tree`` -> one row per series (quarter-finals, semi-finals, finals).

    ``team_a`` / ``team_b`` are the two teams of the series (A is the higher seed); the playoff tree sends
    team and winner ids as integers, here they are strings like every other route.

    Args:
        raw: the playoff-tree body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(season, round, series)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_playoff_series
            scb_playoff_series(season=2026, league="bbl")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_playoff_series
            parse_scb_playoff_series({"seasonId": 2025, "finals": {"matchWinnerId": 413, "teamA": {"seasonTeamId": 413}}})
    """
    body = raw if isinstance(raw, dict) else {}
    head = {
        "league": _request(raw).get("league"),
        "season": _plus1(body.get("seasonId")),
        "season_id": body.get("seasonId"),
    }
    rows = []
    for rnd, n, s in _series(body):
        a, b = s.get("teamA") or {}, s.get("teamB") or {}
        rows.append(
            {
                **head,
                "round": rnd,
                "series": n,
                "match_winner_id": s.get("matchWinnerId"),
                "is_match_finished": s.get("isMatchFinished"),
                "team_a_id": a.get("seasonTeamId"),
                "team_a_name": a.get("name"),
                "team_a_tlc": a.get("tlc"),
                "team_a_rank": a.get("rank"),
                "team_a_victories": s.get("teamAVictories"),
                "team_b_id": b.get("seasonTeamId"),
                "team_b_name": b.get("name"),
                "team_b_tlc": b.get("tlc"),
                "team_b_rank": b.get("rank"),
                "team_b_victories": s.get("teamBVictories"),
                "games": len(_dicts(s.get("games"))),
            }
        )
    return _frame("parse_scb_playoff_series", rows, columns, return_as_pandas)


def parse_scb_playoff_games(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/seasons/{seasonId}/playoff-tree`` -> one row per playoff game, with its series.

    ``points_team_a`` / ``points_team_b`` are the points of series team A and B, **not** of the home and
    guest side (read ``home_team_id`` / ``guest_team_id`` against ``team_a_id`` / ``team_b_id``).

    Args:
        raw: the playoff-tree body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, game_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_playoff_games
            scb_playoff_games(season=2026, league="bbl")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_playoff_games
            parse_scb_playoff_games({"seasonId": 2025, "finals": {"games": [{"gameId": 2005772, "pointsTeamA": 102}]}})
    """
    body = raw if isinstance(raw, dict) else {}
    head = {
        "league": _request(raw).get("league"),
        "season": _plus1(body.get("seasonId")),
        "season_id": body.get("seasonId"),
    }
    rows = []
    for rnd, n, s in _series(body):
        a, b = s.get("teamA") or {}, s.get("teamB") or {}
        for g in _dicts(s.get("games")):
            rows.append(
                {
                    **head,
                    "round": rnd,
                    "series": n,
                    "game_id": g.get("gameId"),
                    "scheduled_time": g.get("scheduledTime"),
                    "timestamp": g.get("timestamp"),
                    "game_winner_id": g.get("gameWinnerId"),
                    "home_team_id": g.get("homeTeamId"),
                    "guest_team_id": g.get("guestTeamId"),
                    "team_a_id": a.get("seasonTeamId"),
                    "team_b_id": b.get("seasonTeamId"),
                    "points_team_a": g.get("pointsTeamA"),
                    "points_team_b": g.get("pointsTeamB"),
                }
            )
    return _frame("parse_scb_playoff_games", rows, columns, return_as_pandas)


# --- season stat tables -----------------------------------------------------------------------------

_PLAYER_STAT_RENAME = {
    "seasonPlayer.id": "season_player_id",
    "seasonPlayer.realPlayerId": "real_player_id",
    "seasonPlayer.firstName": "first_name",
    "seasonPlayer.lastName": "last_name",
    "seasonPlayer.shirtNumber": "shirt_number",
    "seasonPlayer.position": "position",
    "seasonTeam.teamId": "team_id",
    "seasonTeam.seasonTeamId": "season_team_id",
    "seasonTeam.name": "team_name",
    "seasonTeam.tlc": "team_tlc",
    "seasonTeam.seasonId": "season_id",
}
_TEAM_STAT_RENAME = {k: v for k, v in _PLAYER_STAT_RENAME.items() if k.startswith("seasonTeam.")}
_PLAYER_STAT_DROP = ("seasonPlayer.playerId", "seasonPlayer.imageUrl", "seasonPlayer.foreignLink", "seasonTeam.logoUrl")


def _stat_rows(raw: Any, rename: Mapping[str, str]) -> List[Dict[str, Any]]:
    req = _request(raw)
    rows = []
    for r in _dicts(raw):
        row = _row(r, rename, _PLAYER_STAT_DROP)
        season = req.get("season") if row.get("season_id") is None else _plus1(row["season_id"])
        rows.append({"league": req.get("league"), "season": season, "display_type": req.get("display_type"), **row})
    return rows


def parse_scb_player_statistics(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/seasons/{seasonId}/player-statistics`` -> one row per player and stat block: season totals and per-game values.

    The rows do not name their season (the path scopes the table), so ``season`` comes from the request.
    ``display_type`` echoes which games the table covers (``MAIN_ROUND``, ``PLAYOFFS``, ...). Totals and
    ``*_per_game`` values sit side by side; percentages are 0-100 and may be null with no attempts.

    Args:
        raw: the player-statistics body (a list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per player and stat block (``competition``, ``stage``, ``aggregation``).

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_player_statistics
            scb_player_statistics(season=2026, league="dbbl").sort("points", descending=True).head()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_player_statistics
            parse_scb_player_statistics([{"points": 120, "seasonPlayer": {"id": "1", "realPlayerId": 7}}])
    """
    return _frame("parse_scb_player_statistics", _stat_rows(raw, _PLAYER_STAT_RENAME), columns, return_as_pandas)


def parse_scb_team_statistics(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/seasons/{seasonId}/team-statistics`` -> one row per team and stat block: season totals and per-game values.

    Args:
        raw: the team-statistics body (a list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per team and stat block (``competition``, ``aggregation``).

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_team_statistics
            scb_team_statistics(season=2026, league="bbl")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_team_statistics
            parse_scb_team_statistics([{"points": 2500, "seasonTeam": {"teamId": "413", "seasonId": 2025}}])
    """
    return _frame("parse_scb_team_statistics", _stat_rows(raw, _TEAM_STAT_RENAME), columns, return_as_pandas)


# --- games ------------------------------------------------------------------------------------------


def _side(d: Any, p: str) -> Dict[str, Any]:
    d = d if isinstance(d, dict) else {}
    return {
        f"{p}_team_id": d.get("teamId"),
        f"{p}_season_team_id": d.get("seasonTeamId"),
        f"{p}_team_name": d.get("name"),
        f"{p}_tlc": d.get("tlc"),
        f"{p}_is_foreign": d.get("isForeign"),
        f"{p}_rank": d.get("rank"),
        f"{p}_streak": d.get("streak"),
        f"{p}_streak_count": d.get("streakCount"),
        f"{p}_last10_victories": d.get("last10Victories"),
        f"{p}_last10_losses": d.get("last10Losses"),
        f"{p}_total_games": _i(d.get("totalGames")),
    }


def _official(d: Any, p: str) -> Dict[str, Any]:
    d = d if isinstance(d, dict) else {}
    return {f"{p}_id": d.get("refId"), f"{p}_first_name": d.get("firstName"), f"{p}_last_name": d.get("lastName")}


def _game_row(g: Mapping[str, Any], req: Mapping[str, Any], bucket: Optional[str] = None) -> Dict[str, Any]:
    res = _obj(g.get("result"))
    venue = _obj(g.get("venue"))
    top = _obj(g.get("topScorer"))
    row: Dict[str, Any] = {
        "league": req.get("league"),
        "season": _plus1(g.get("seasonId")) or req.get("season"),
        "season_id": g.get("seasonId"),
        "game_id": g.get("id"),
        "competition": g.get("competition"),
        "stage": g.get("stage"),
        "status": g.get("status"),
    }
    if bucket is not None:
        row["bucket"] = bucket
    if "statstatus" in g:
        row["stat_status"] = g.get("statstatus")
    if "gameProgress" in g:
        row["game_progress"] = g.get("gameProgress")
    row.update(
        scheduled_time=g.get("scheduledTime"),
        match_day=g.get("matchDay"),
        is_tbd=g.get("isTBD"),
        is_hidden=g.get("isHidden"),
        **_side(g.get("homeTeam"), "home"),
        **_side(g.get("guestTeam"), "guest"),
        home_score=res.get("homeTeamFinalScore"),
        guest_score=res.get("guestTeamFinalScore"),
        latest_period=_period(res.get("latestPeriod")),
        spectators=res.get("spectators"),
    )
    for side, wire in (("home", "home"), ("guest", "guest")):
        for q in range(1, 5):
            row[f"{side}_q{q}_score"] = res.get(f"{wire}TeamQ{q}Score")
        for ot in range(1, _OT + 1):
            row[f"{side}_ot{ot}_score"] = res.get(f"{wire}TeamOT{ot}Score")
    for wire, p in (
        ("referee1", "referee1"),
        ("referee2", "referee2"),
        ("referee3", "referee3"),
        ("commissioner", "commissioner"),
    ):
        row.update(_official(g.get(wire), p))
    row.update(
        venue_id=venue.get("id"),
        venue_name=venue.get("name"),
        venue_address=venue.get("address"),
        venue_capacity=venue.get("capacity"),
        venue_is_main=venue.get("isMain"),
        tv_channel=_join(g.get("tvChannel")),
        tv_type=g.get("tvType"),
        is_free_tv=g.get("isFreeTV"),
        live_stream_url=g.get("linkLiveStream"),
        top_score=g.get("topScore"),
        top_scorer_season_player_id=top.get("playerId"),
        top_scorer_real_player_id=top.get("realPlayerId"),
        top_scorer_first_name=top.get("firstName"),
        top_scorer_last_name=top.get("lastName"),
    )
    return row


def parse_scb_games(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/games`` (a page), ``/games/{gameId}`` (the header) -> one row per game.

    Teams are ``home_*`` / ``guest_*`` (``guest`` is the away side). ``latest_period`` is the number of the
    last period played (``Q4`` -> 4, ``OT1`` -> 5); the quarter and overtime scores are 0 when not played, and the scores of a game not played yet are
    null (a few ``future`` rows of ``/games/recent`` send 0).
    Officials keep their id (joins ``scb_referees.referee_id``, which lists only the current officials) and
    name; ``top_scorer_*`` is the game's top scorer. ``game_time`` is dropped (``00:00:00`` in every sample).
    ``venue_id`` is a stable arena id, not the per-season ``/teams`` venue id.

    Args:
        raw: a ``/games`` page (``{items: [...]}``), a list of games, or one game header.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, game_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_games
            scb_games(season=2026, league="dbbl").filter(pl.col("status") == "OFFICIAL")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_games
            parse_scb_games({"items": [{"id": "10130", "seasonId": 2024, "competition": "DBBL"}]})
    """
    req = _request(raw)
    if isinstance(raw, dict) and isinstance(raw.get("items"), list):
        games = _dicts(raw["items"])
    elif isinstance(raw, dict) and "id" in raw:
        games = [raw]
    else:
        games = _dicts(raw)
    return _frame("parse_scb_games", [_game_row(g, req) for g in games], columns, return_as_pandas)


def parse_scb_games_recent(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/games/recent`` -> the recent, live and upcoming games, one row per game with its ``bucket``.

    ``bucket`` is ``past`` (finished), ``present`` (today / live) or ``future`` (scheduled). The body holds
    the same game shape as :func:`parse_scb_games`; ``latest_period`` is an integer here (a ``Q4`` string
    on ``/games``), normalised to the period number in both.

    Args:
        raw: the ``/games/recent`` body (``{past, present, future}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, game_id)`` with its ``bucket``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_games_recent
            scb_games_recent(league="bbl", slot_size=5)

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_games_recent
            parse_scb_games_recent({"future": [{"id": "2006930", "seasonId": 2026}]})
    """
    body = raw if isinstance(raw, dict) else {}
    req = _request(raw)
    rows = [_game_row(g, req, bucket) for bucket in ("past", "present", "future") for g in _dicts(body.get(bucket))]
    return _frame("parse_scb_games", rows, columns, return_as_pandas)


# --- box score and play-by-play ---------------------------------------------------------------------

_BOX_RENAME = {
    "seasonPlayer.id": "season_player_id",
    "seasonPlayer.realPlayerId": "real_player_id",
    "seasonPlayer.firstName": "first_name",
    "seasonPlayer.lastName": "last_name",
    "seasonPlayer.shirtNumber": "shirt_number",
    "seasonPlayer.position": "position",
    "seasonTeam.teamId": "team_id",
    "seasonTeam.seasonTeamId": "season_team_id",
    "seasonTeam.name": "team_name",
    "seasonTeam.tlc": "team_tlc",
    "seasonTeam.seasonId": "season_id",
}
_BOX_DROP = (
    "seasonPlayer.playerId",
    "seasonPlayer.imageUrl",
    "seasonPlayer.foreignLink",
    "seasonTeam.logoUrl",
    "shirtNumber",
    "gameId",
)
_TEAM_RENAME = {
    "seasonTeam.seasonTeamId": "season_team_id",
    "seasonTeam.seasonId": "season_id",
    "seasonTeam.name": "team_name",
    "seasonTeam.tlc": "team_tlc",
}
_TEAM_DROP = ("seasonTeam.teamId", "seasonTeam.logoUrl", "gameId", "scheduledTime")
_SIDES = (("home", "homeTeam"), ("guest", "guestTeam"))


def _game_head(body: Mapping[str, Any], req: Mapping[str, Any]) -> Dict[str, Any]:
    return {"league": req.get("league"), "game_id": body.get("gameId")}


def parse_scb_game_players(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/games/{gameId}/stats`` -> the box score: one row per player line of both teams.

    ``side`` is ``home`` or ``guest``. ``seconds_played`` is null for a player who did not play and
    ``plus_minus`` is null when the API sends an empty string. The 2005-06 totals disagree with their
    2-point plus 3-point parts: ``field_goals_made`` / ``field_goals_attempted`` are recomputed from the
    parts then and ``field_goals_recomputed`` says so. Percentages are 0-100.

    Args:
        raw: the ``/games/{gameId}/stats`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, game_id, season_player_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_game_players
            scb_game_players(game_id="2006920", league="bbl").sort("points", descending=True).head()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_game_players
            parse_scb_game_players({"gameId": "1", "homeTeam": {"id": "413", "playerStats": [{"points": 12}]}})
    """
    body = raw if isinstance(raw, dict) else {}
    head = _game_head(body, _request(raw))
    rows = []
    for side, wire in _SIDES:
        team = _obj(body.get(wire))
        for p in _dicts(team.get("playerStats")):
            row = _row(_numify_nested(p), _BOX_RENAME, _BOX_DROP)
            rows.append(
                _fix_field_goals(
                    {**head, "season": _plus1(row.get("season_id")), "side": side, "team_id": team.get("id"), **row}
                )
            )
    return _frame("parse_scb_game_players", rows, columns, return_as_pandas)


def _numify_nested(d: Any) -> Dict[str, Any]:
    """:func:`_numify` the stat counters of a line, leaving nested objects (the player and team) alone."""
    return {k: (v if isinstance(v, (dict, list)) else _numify({k: v})[k]) for k, v in _obj(d).items()}


def parse_scb_game_teams(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/games/{gameId}/stats`` -> the box score: one row per team (two rows), with the head coach and the team line.

    The ``team_line_*`` columns are stats the API does not credit to a player (team rebounds, team
    turnovers, bench fouls); add them to the player lines to reach the team totals. ``seconds_played`` is
    the team's total player-seconds (``12000`` for five players over 40 minutes). The 2005-06 totals are
    recomputed from their parts as in :func:`parse_scb_game_players`.

    Args:
        raw: the ``/games/{gameId}/stats`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, game_id, side)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_game_teams
            scb_game_teams(game_id="2006920", league="bbl")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_game_teams
            parse_scb_game_teams({"gameId": "1", "status": "OFFICIAL", "homeTeam": {"id": "413", "gameStat": {"points": 89}}})
    """
    body = raw if isinstance(raw, dict) else {}
    head = _game_head(body, _request(raw))
    rows = []
    for side, wire in _SIDES:
        team = _obj(body.get(wire))
        if not team:
            continue
        stat = _row(_numify_nested(team.get("gameStat")), _TEAM_RENAME, _TEAM_DROP)
        coach = _row(_numify_nested(team.get("coachStat")), {}, ("firstName", "lastName"))
        rows.append(
            _fix_field_goals(
                {
                    **head,
                    "season": _plus1(stat.get("season_id")),
                    "side": side,
                    "team_id": team.get("id"),
                    "head_coach_name": team.get("headCoachName"),
                    "status": body.get("status"),
                    "scheduled_time": body.get("scheduledTime"),
                    **stat,
                    **{f"team_line_{k}": v for k, v in coach.items()},
                }
            )
        )
    return _frame("parse_scb_game_teams", rows, columns, return_as_pandas)


def parse_scb_game_actions(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/games/{gameId}/stats`` -> the play-by-play: one row per action, in game order, with shot x/y.

    Available from 2008-09 on BBL (about 450-570 actions a game) and 2024-25 on DBBL; earlier games have none.
    ``game_time`` is the clock LEFT in the period (``00:10:00`` at the start of a quarter) and
    ``seconds_left`` its seconds. ``side`` is ``home`` / ``guest`` from the acting team. ``qualifiers`` is
    comma-joined (``NEAR_DISTANCE,LAYUP``). ``shot_x`` / ``shot_y`` are the site's half-court chart units
    (x across 0-280, rim near ``(140, 45)``; **unverified** scale and orientation) and are null for every action
    without a location, which the API sends as ``(0, 0)``. ``type`` is null on the 2008-09 to about 2010-11
    lineup markers (the five players on court at the start and end of a period). ``home_team_points`` /
    ``guest_team_points`` are the score after a scoring action.

    Args:
        raw: the ``/games/{gameId}/stats`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, action_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_game_actions
            shots = scb_game_actions(game_id="2006920").filter(pl.col("shot_x").is_not_null())

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_game_actions
            parse_scb_game_actions({"gameId": "1", "actions": [{"id": 7, "type": "TWO_POINT_THROW", "coordinates": {"x": 141, "y": 50}}]})
    """
    body = raw if isinstance(raw, dict) else {}
    head = _game_head(body, _request(raw))
    teams = {_obj(body.get(wire)).get("id"): side for side, wire in _SIDES if isinstance(body.get(wire), dict)}
    rows = []
    for a in _dicts(body.get("actions")):
        c = _obj(a.get("coordinates"))
        x, y = c.get("x"), c.get("y")
        located = not (x in (0, None) and y in (0, None))
        rows.append(
            {
                **head,
                "action_id": a.get("id"),
                "order_id": a.get("orderId"),
                "action_identifier": a.get("actionIdentifier"),
                "period": a.get("period"),
                "game_time": a.get("gameTime"),
                "seconds_left": _seconds_left(a.get("gameTime")),
                "real_time": a.get("realTime"),
                "type": a.get("type"),
                "qualifiers": _join(a.get("qualifiers")),
                "is_successful": a.get("isSuccessful"),
                "season_team_id": a.get("seasonTeamId"),
                "side": teams.get(a.get("seasonTeamId")),
                "season_player_id": a.get("seasonPlayerId"),
                "assisting_season_player_id": a.get("assistingSeasonPlayerId"),
                "shot_x": x if located else None,
                "shot_y": y if located else None,
                "home_team_points": a.get("homeTeamPoints"),
                "guest_team_points": a.get("guestTeamPoints"),
            }
        )
    return _frame("parse_scb_game_actions", rows, columns, return_as_pandas)


# --- standings --------------------------------------------------------------------------------------

_STANDINGS_RENAME = {
    "id": "standing_id",
    "seasonTeam.id": "season_team_id",
    "seasonTeam.teamId": "team_id",
    "seasonTeam.name": "team_name",
    "seasonTeam.tlc": "team_tlc",
    "seasonTeam.nameShort": "team_name_short",
    "seasonTeam.hasReachedPlayoffs": "has_reached_playoffs",
    "seasonTeam.hasReachedPlayins": "has_reached_playins",
    "seasonTeam.isRelegationCertain": "is_relegation_certain",
}
_STANDINGS_DROP = ("seasonTeam.logoUrl",)
#: The API's "Points" fields are TABLE points (2 per win); its football-named "goals" are basketball points.
_STANDINGS_TABLE = {"total_points_made": "table_points", "total_points_against": "table_points_against"}
_STANDINGS_POINTS = (
    ("goals_made", "points_for"),
    ("goals_against", "points_against"),
    ("goal_difference", "point_difference"),
)


def parse_scb_standings(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/standings`` -> one row per team and stage: the league table.

    The API names basketball points "goals" (``goals_made`` ...): they are renamed ``points_for``,
    ``points_against`` and ``point_difference`` (``home_`` / ``away_`` / ``total_``). The "points" fields
    are TABLE points (``table_points``, ``table_points_against``; 2 per win) per the provider's notes.
    BBL returns ``MAIN_ROUND`` rows only; DBBL also returns the playoff stages. ``ranking_comparison_info`` and
    ``point_corrections`` stay JSON.

    Args:
        raw: the ``/standings`` body (a list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, season, stage, team_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_standings
            scb_standings(season=2026, league="bbl").select("rank", "team_name", "total_victories")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_standings
            parse_scb_standings([{"seasonId": 2025, "rank": 1, "seasonTeam": {"teamId": "413"}}])
    """
    req = _request(raw)
    rows = []
    for r in _dicts(raw):
        row = _row(r, _STANDINGS_RENAME, _STANDINGS_DROP)
        row = {_STANDINGS_TABLE.get(k, k): v for k, v in row.items()}
        for old, new in _STANDINGS_POINTS:
            for k in [k for k in row if k.endswith(old)]:
                row[k[: -len(old)] + new] = row.pop(k)
        rows.append({"league": req.get("league"), "season": _plus1(r.get("seasonId")), **row})
    return _frame("parse_scb_standings", rows, columns, return_as_pandas)


# --- profiles ---------------------------------------------------------------------------------------


def parse_scb_season_player(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/season-players/{seasonPlayerId}`` -> one row: the player profile of that engagement.

    ``season_player_id`` is the id asked for; ``active_season_player_id`` is the player's current one.
    ``height`` is metres and ``weight`` kilograms. ``nationalities`` is comma-joined ISO codes;
    ``previous_teams`` and ``season_ids`` stay JSON lists.

    Args:
        raw: the profile body (an object).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_season_player
            scb_season_player(season_player_id="56414")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_season_player
            parse_scb_season_player({"id": "56414", "realPlayerId": 37539, "height": 1.92, "team": {"id": "413"}})
    """
    d = {k: v for k, v in raw.items() if k != _REQUEST} if isinstance(raw, dict) else {}
    if not d:
        return _frame("parse_scb_season_player", [], columns, return_as_pandas)
    team = _obj(d.get("team"))
    row = {
        "league": _request(raw).get("league"),
        "season_player_id": d.get("id"),
        "real_player_id": d.get("realPlayerId"),
        "active_season_player_id": d.get("activeSeasonPlayerId"),
        "first_name": d.get("firstName"),
        "last_name": d.get("lastName"),
        "birth_date": d.get("birthDate"),
        "position": d.get("position"),
        "shirt_number": d.get("shirtNumber"),
        "height": d.get("height"),
        "weight": d.get("weight"),
        "nationalities": _join(d.get("nationalities")),
        "is_all_star": d.get("isAllStar"),
        "is_young": d.get("isYoung"),
        "has_changed_teams": d.get("hasChangedTeams"),
        "bbl_games_played": d.get("bblGamesPlayed"),
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "previous_teams": d.get("previousTeams"),
        "season_ids": d.get("seasonIds"),
    }
    return _frame("parse_scb_season_player", [row], columns, return_as_pandas)


def parse_scb_team(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/teams/{teamId}`` -> one row: the club profile.

    The club's office contact block (address, phone, e-mail, ticket phone) and social-media links are left
    out. ``year_joined_bbl`` is null where the API sends the placeholder ``TODO_YEARJOINEDBBL``.

    Args:
        raw: the club body (an object).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_team
            scb_team(team_id="413")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_team
            parse_scb_team({"id": "413", "tlc": "BER", "nameFull": "ALBA BERLIN", "yearJoinedBBL": 1991})
    """
    d = {k: v for k, v in raw.items() if k != _REQUEST} if isinstance(raw, dict) else {}
    if not d:
        return _frame("parse_scb_team", [], columns, return_as_pandas)
    row = {
        "league": _request(raw).get("league"),
        "team_id": d.get("id"),
        "tlc": d.get("tlc"),
        "name_full": d.get("nameFull"),
        "name_short": d.get("nameShort"),
        "mascot": d.get("mascot"),
        "homepage_url": d.get("homepageUrl"),
        "ticket_url": d.get("ticketUrl"),
        "year_joined_bbl": _i(d.get("yearJoinedBBL")),
        "order_id": d.get("orderId"),
        "best_achievements": d.get("bestAchievements"),
    }
    return _frame("parse_scb_team", [row], columns, return_as_pandas)


def parse_scb_team_seasons(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/teams/{teamId}`` -> one row per season the club played: its name that season and its titles.

    ``season_id`` is the API's START year; ``team_name`` is the name that season (sponsor names change).
    The per-season venue ids are left out.

    Args:
        raw: the club body (an object).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(league, team_id, season)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_team_seasons
            scb_team_seasons(team_id="413").filter(pl.col("is_championship_winner") == True)

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_team_seasons
            parse_scb_team_seasons({"id": "413", "seasonTeams": [{"id": "413", "seasonId": 1975, "name": "DTV"}]})
    """
    d = raw if isinstance(raw, dict) else {}
    rows = [
        {
            "league": _request(raw).get("league"),
            "team_id": d.get("id"),
            "season": _plus1(s.get("seasonId")),
            "season_id": s.get("seasonId"),
            "season_team_id": s.get("id"),
            "team_name": s.get("name"),
            "is_championship_winner": s.get("isChampionshipWinner"),
            "is_cup_winner": s.get("isCupWinner"),
        }
        for s in _dicts(d.get("seasonTeams"))
    ]
    return _frame("parse_scb_team_seasons", rows, columns, return_as_pandas)


def parse_scb_referees(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/referees`` -> one row per current official (referee or commissioner).

    Only the current officials are listed, so an older game's official can be absent. Birth date,
    profession and portrait are left out.

    Args:
        raw: a ``/referees`` page (``{items: [...]}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``referee_id``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_referees
            scb_referees().sort("participations", descending=True).head()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_referees
            parse_scb_referees({"items": [{"refId": "1", "firstName": "Tamer", "officialFunction": "REFEREE"}]})
    """
    items = raw.get("items") if isinstance(raw, dict) else raw
    rows = [
        {
            "league": _request(raw).get("league"),
            "referee_id": r.get("refId"),
            "first_name": r.get("firstName"),
            "last_name": r.get("lastName"),
            "official_function": r.get("officialFunction"),
            "participations": r.get("participations"),
            "year_joined_bbl": _i(r.get("yearJoinedBBL")),
        }
        for r in _dicts(items)
    ]
    return _frame("parse_scb_referees", rows, columns, return_as_pandas)


# --- transfers --------------------------------------------------------------------------------------


def _transfer_row(t: Mapping[str, Any], req: Mapping[str, Any]) -> Dict[str, Any]:
    src = _obj(t.get("sourceTeam"))
    dst = _obj(t.get("destinationTeam"))
    p = _obj(t.get("player"))
    return {
        "league": req.get("league"),
        "transfer_date": t.get("sortingDate"),
        "season_player_id": p.get("id"),
        "real_player_id": _i(p.get("playerId")),
        "first_name": p.get("firstName"),
        "last_name": p.get("lastName"),
        "position": p.get("position"),
        "source_team_id": src.get("id"),
        "source_team_name": t.get("sourceTeamFreeText") or src.get("nameShort"),
        "destination_team_id": dst.get("id"),
        "destination_team_name": t.get("destinationTeamFreeText") or dst.get("nameShort"),
    }


def parse_scb_transfers(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/transfers`` -> the latest transfers, one row per move (BBL only).

    The API's player ``playerId`` here is the person id: it lands as ``real_player_id``; the player's
    ``id`` is the ``season_player_id`` of the new engagement. A team without an id (a club outside the league)
    keeps its free-text name.

    Args:
        raw: the ``/transfers`` body (a list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per transfer.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_transfers
            scb_transfers().head()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_transfers
            parse_scb_transfers([{"sortingDate": "2026-10-06", "player": {"id": "2005312", "playerId": "140183148"}}])
    """
    req = _request(raw)
    return _frame("parse_scb_transfers", [_transfer_row(t, req) for t in _dicts(raw)], columns, return_as_pandas)


def parse_scb_transfers_teams(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/transfers/teams`` -> per club, its joining and leaving players, one row per move with its ``direction``.

    ``team_id`` is the club the list belongs to; ``direction`` is ``joining`` or ``leaving``. The same
    move appears under the source club (``leaving``) and the destination club (``joining``). The current
    roster of the same body is :func:`parse_scb_team_rosters`.

    Args:
        raw: the ``/transfers/teams`` body (a list of clubs).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(team_id, direction, season_player_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_transfers_teams
            scb_transfers_teams().filter(pl.col("direction") == "joining")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_transfers_teams
            parse_scb_transfers_teams([{"team": {"id": "527"}, "joining": [{"sortingDate": "2026-08-13", "player": {"id": "1"}}]}])
    """
    req = _request(raw)
    rows = [
        {"team_id": _obj(c.get("team")).get("id"), "direction": direction, **_transfer_row(t, req)}
        for c in _dicts(raw)
        for direction in ("joining", "leaving")
        for t in _dicts(c.get(direction))
    ]
    return _frame("parse_scb_transfers_teams", rows, columns, return_as_pandas)


def parse_scb_team_rosters(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/transfers/teams`` -> per club, its current roster, one row per player.

    Args:
        raw: the ``/transfers/teams`` body (a list of clubs).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(team_id, season_player_id)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_team_rosters
            scb_team_rosters().group_by("team_id").len()

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_team_rosters
            parse_scb_team_rosters([{"team": {"id": "527"}, "roster": [{"id": "2004013", "realPlayerId": 11422}]}])
    """
    req = _request(raw)
    rows = []
    for c in _dicts(raw):
        for p in _dicts(c.get("roster")):
            rows.append(
                {
                    "league": req.get("league"),
                    "team_id": _obj(c.get("team")).get("id"),
                    "season": _plus1(p.get("seasonId")),
                    "season_id": p.get("seasonId"),
                    "season_team_id": p.get("seasonTeamId"),
                    "season_player_id": p.get("id"),
                    "real_player_id": p.get("realPlayerId"),
                    "first_name": p.get("firstName"),
                    "last_name": p.get("lastName"),
                    "birth_date": p.get("birthDate"),
                    "position": p.get("position"),
                    "shirt_number": p.get("shirtNumber"),
                    "height": p.get("height"),
                    "weight": p.get("weight"),
                    "nationalities": _join(p.get("nationalities")),
                    "is_all_star": p.get("isAllStar"),
                    "is_young": p.get("isYoung"),
                }
            )
    return _frame("parse_scb_team_rosters", rows, columns, return_as_pandas)


# --- records ----------------------------------------------------------------------------------------


def parse_scb_records_per_game(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/records/per-game/{seasonId}`` -> single-game records, one row per record: the season's and the all-time ones.

    ``scope`` is ``season`` or ``all_time``; ``stat`` is the record (``points``, ``rebounds``, ``assists``,
    ``steals``, ``blocks``, ``turnovers``, ``efficiency``, ``free_throws_made``, ``three_point_shots_made``,
    ``free_throws_success_percentage``). ``value`` is a number (a string in the API). The API's record
    ``seasonTeamId`` carries a season-player id and is dropped.

    Args:
        raw: the per-game records body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(scope, stat, place)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_records_per_game
            scb_records_per_game(season=2026).filter(pl.col("scope") == "season")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_records_per_game
            parse_scb_records_per_game({"seasonId": "2025", "seasonRecords": {"points": [{"value": "52", "place": 1}]}})
    """
    body = raw if isinstance(raw, dict) else {}
    season_id = _i(body.get("seasonId"))
    rows = []
    for scope, key in (("season", "seasonRecords"), ("all_time", "allTimeRecords")):
        group = _obj(body.get(key))
        for stat, items in group.items():
            for r in _dicts(items):
                rows.append(
                    {
                        "league": _request(raw).get("league"),
                        "season": _plus1(season_id),
                        "season_id": season_id,
                        "scope": scope,
                        "stat": underscore(stat),
                        "place": r.get("place"),
                        "all_time_place": r.get("allTimePlace"),
                        "order_id": r.get("orderId"),
                        "all_time_order_id": r.get("allTimeOrderId"),
                        "value": r.get("value"),
                        "ratio": r.get("ratio"),
                        "season_player_id": r.get("seasonPlayerId"),
                        "real_player_id": r.get("realPlayerId"),
                        "first_name": r.get("firstName"),
                        "last_name": r.get("lastName"),
                        "team_name": r.get("seasonTeamName"),
                        "team_tlc": r.get("teamTLC"),
                        "home_tlc": r.get("homeTeamTLC"),
                        "guest_tlc": r.get("guestTeamTLC"),
                        "game_id": r.get("gameId"),
                        "scheduled_time": r.get("scheduledTime"),
                    }
                )
    return _frame("parse_scb_records_per_game", rows, columns, return_as_pandas)


def parse_scb_records_season_averages(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/records/season-averages`` -> all-time season-average records, one row per record.

    ``stat`` is the record list (``assist_average``, ``points_average``, ...). ``value`` is a number.
    ``season_id`` is the START year of the season the record was set in. The API's record ``seasonTeamId``
    carries a season-player id and is dropped.

    Args:
        raw: the season-averages body (``{<stat>: [records]}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per ``(stat, place)``.

    Raises:
        Nothing: an empty, malformed or wrong-shaped payload parses to a zero-row frame with ``columns``.

    Example:
        Quick start::

            from sportsdataverse.basketball.scb import scb_records_season_averages
            scb_records_season_averages(league="dbbl").filter(pl.col("stat") == "points_average")

        Parse a raw body::

            from sportsdataverse.basketball.scb import parse_scb_records_season_averages
            parse_scb_records_season_averages({"assistAverage": [{"seasonId": 2021, "value": 9.67, "place": 1}]})
    """
    body = raw if isinstance(raw, dict) else {}
    rows = [
        {
            "league": _request(raw).get("league"),
            "stat": underscore(stat),
            "season": _plus1(r.get("seasonId")),
            "season_id": r.get("seasonId"),
            "place": r.get("place"),
            "order_id": r.get("orderId"),
            "value": r.get("value"),
            "ratio": r.get("ratio"),
            "season_player_id": r.get("seasonPlayerId"),
            "real_player_id": r.get("realPlayerId"),
            "first_name": r.get("firstName"),
            "last_name": r.get("lastName"),
            "team_name": r.get("seasonTeamName"),
            "team_tlc": r.get("teamTLC"),
        }
        for stat, items in body.items()
        if stat != _REQUEST
        for r in _dicts(items)
    ]
    return _frame("parse_scb_records_season_averages", rows, columns, return_as_pandas)
