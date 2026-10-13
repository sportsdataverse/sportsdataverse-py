"""Runtime for the generated TPBL wrappers (``api.tpbl.basketball``, Taiwan's Taipei Professional Basketball League).

One host, keyless, no headers: the Laravel JSON API the tpbl.basketball site calls. ``tpbl.basketball/robots.txt``
answers 200 with an empty body (no rules, no crawl-delay). Measured against the live API (sdv-internal-refs ``tpbl/``,
2026-10-12):

* **Season keys.** The public ``season`` is the END year (2026 = 2025-26). The API keys a season by its own small
  integer (1, 2, 3 = 2024-25, 2025-26, 2026-27; the ``event_id`` of ``/events/{id}/*`` is the same number) and labels
  it ``2024-2025 賽季`` (賽季 = "season"). The runtime reads ``/seasons`` once per process and maps the END year to the
  row whose label ends in it, so a new season needs no code change; a year the list lacks is ``NoDataError``.
* **Ids are not interchangeable.** TPBL ids (season, division, team, game, player) are the ones every route takes;
  ``gohoops_*`` ids belong to GoHoops, the stats platform behind the league. The box score keys its players by the
  GoHoops roster id, not by the TPBL player id.
* **Finished games only.** A game route is served only when ``/games/{id}`` says ``COMPLETED`` and not live; the box
  score fetches that detail first (one more request, cached per process) and checks the stats against it.
* **Silent wrong answers, all refused** (fail closed, type-strict compares): a game detail for another game id; a box
  score whose home / away team ids (in order), final scores, number of quarters or player points disagree with the
  game detail; a game list with a team from another season, a duplicate game id, a game whose division / event is
  inconsistent, or (``verify=True``) whose completed games per division differ from that division's standings; a
  roster of another team or event; a player game log of another player or division. The two shot lists name no team,
  player or division in the body, so they cannot be scoped: only their shape and court bounds are checked, and a
  list of foreign shots is not detectable here (the refs capture ties them to the box scores; ``verify`` does not).
* **Error vocabulary.** ``NoDataError``: HTTP 404 (an unknown id answers a Laravel ``text/html`` 404 page), a season
  the list does not hold, a legitimately empty list (no standings, roster, shots or games). ``ValueError``: a
  non-integer id or season, HTTP 400 / 422 (the player game log without ``division_id`` answers 422).
  ``AssetFetchError``: any other non-200 (429 / 5xx after three attempts, spaced by ``dl_utils.download`` and honouring
  ``Retry-After``), a dead connection, a non-JSON or error-envelope body, an unfinished game and every wrong-scope body
  above. The player game log with an UNKNOWN ``division_id`` answers HTTP 500 deterministically: that route is
  fetched without 500 in its retry set, so it fails after one request.
* **Known source defect.** Game 7 (2024-25 preseason) has a 5th quarter with no time on court, a game list that says
  4 quarters and away players whose points sum to 87 against a team score of 92: it is refused like any other
  disagreeing box score.
* **Personal data.** Birth dates (a player's, an official's) and contact fields are replaced by ``<redacted>`` by key
  name, at any depth, in every raw body before it is returned or parsed.

Every attempt, retries included, goes through the shared ``basketball_runtime.SESSION`` and so waits its turn (at
least 1 s per host).
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["REQUEST_KEY"]

API = "https://api.tpbl.basketball/api"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
#: The player game log answers a deterministic HTTP 500 for an unknown ``division_id``: never retried.
RETRY_NO_500 = RETRY_STATUSES - {500}
#: Court bounds of a shot, in millimetres (a 28 m x 15 m court).
COURT_X = 28000
COURT_Y = 15000
FINISHED = "COMPLETED"

_PERSONAL = re.compile(r"(birth|^dob$|e_?mail|phone|mobile|address|contact)", re.I)
_SEASON_LABEL = re.compile(r"^\s*(\d{4})\s*-\s*(\d{4})")
#: The season list, per process (re-read once when a requested year is missing).
_SEASONS: List[Dict[str, Any]] = []
#: Finished games' detail bodies (the context the box score is checked against), per process.
_GAMES: Dict[int, Dict[str, Any]] = {}


def _redact_personal(obj: Any) -> Any:
    """``obj`` with birth-date and contact values replaced by ``<redacted>`` by key name, any depth."""
    if isinstance(obj, dict):
        return {k: ("<redacted>" if _PERSONAL.search(str(k)) else _redact_personal(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact_personal(v) for v in obj]
    return obj


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality (``True`` is not ``1``, ``'1'`` is not ``1``); ``None`` never matches."""
    return got is not None and want is not None and type(got) is type(want) and got == want


def _at(d: Any, *path: str) -> Any:
    for p in path:
        if not isinstance(d, dict):
            return None
        d = d.get(p)
    return d


def _rows(body: Any) -> List[Dict[str, Any]]:
    return [r for r in body if isinstance(r, dict)] if isinstance(body, list) else []


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be a positive integer; got {value!r}")
    return int(value)


# ----------------------------------------------------------------------------- transport
def _fetch(url: str, params: Optional[Dict[str, Any]] = None, retry: frozenset = RETRY_STATUSES) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries the statuses in ``retry`` up to :data:`ATTEMPTS` times with a ``Retry-After``-aware
    delay and raises ``NoDataError`` on a 404. A connection failure that outlives the retries becomes an
    ``AssetFetchError`` naming the exception class only.
    """
    try:
        return download(
            url,
            params=params,
            session=SESSION,
            timeout=TIMEOUT,
            num_retries=ATTEMPTS - 1,
            retry_statuses=retry,
            cache_ttl=0,
        )
    except OSError as exc:  # requests' RequestException subclasses OSError
        raise AssetFetchError(
            f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}"
        ) from None


def _check_status(url: str, resp: Any) -> None:
    status = getattr(resp, "status_code", None)
    if status == 200:
        return
    where = _rt._where(url)
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    raise AssetFetchError(f"{where} answered HTTP {status}, not 200")


def _json(url: str, params: Optional[Dict[str, Any]] = None, retry: frozenset = RETRY_STATUSES) -> Any:
    """The parsed JSON of a route: HTTP 200, a JSON content type, a non-empty parseable body."""
    resp = _fetch(url, params, retry)
    _check_status(url, resp)
    where = _rt._where(url)
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    if "json" not in ctype:
        raise AssetFetchError(f"{where} answered {ctype or 'no content type'}, not JSON")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise AssetFetchError(f"{where} answered HTTP 200 with an empty body")
    try:
        return json.loads(text)
    except ValueError:
        raise AssetFetchError(f"{where} answered a body that is not JSON") from None


def _refuse(url: str, why: Optional[str]) -> None:
    if why:
        raise AssetFetchError(f"{_rt._where(url)}: {why}")


def _list_body(url: str, params: Optional[Dict[str, Any]] = None, retry: frozenset = RETRY_STATUSES) -> List[Any]:
    body = _json(url, params, retry)
    if not isinstance(body, list):
        raise AssetFetchError(f"{_rt._where(url)}: expected a JSON list, got {type(body).__name__} (error envelope?)")
    return body


def _stamp(body: Any, **request: Any) -> Dict[str, Any]:
    """A redacted body with the request stamped: a list becomes ``{"data": list, "_request": ...}``."""
    body = _redact_personal(body)
    out = {"data": body} if isinstance(body, list) else dict(body)
    out[REQUEST_KEY] = request
    return out


# ----------------------------------------------------------------------------- seasons
def _season_end_year(row: Dict[str, Any]) -> Optional[int]:
    m = _SEASON_LABEL.match(row.get("name") or "") if isinstance(row.get("name"), str) else None
    if m is None or int(m.group(2)) != int(m.group(1)) + 1:
        return None
    return int(m.group(2))


def _seasons_error(rows: List[Any], body: List[Any]) -> Optional[str]:
    good = _rows(rows)
    if not body or len(good) != len(body):
        return "seasons: rows are not objects"
    ids = [r.get("id") for r in good]
    if len(set(map(str, ids))) != len(ids) or not all(isinstance(i, int) and not isinstance(i, bool) for i in ids):
        return "seasons: ids are not unique integers"
    if any(_season_end_year(r) is None for r in good) or not all(isinstance(r.get("gohoops_id"), int) for r in good):
        return "seasons: a label is not 'YYYY-YYYY ...' or a row has no integer gohoops_id (layout drift)"
    return None


def _season_list(*, refresh: bool = False) -> List[Dict[str, Any]]:
    if refresh or not _SEASONS:
        url = f"{API}/seasons"
        body = _list_body(url)
        _refuse(url, _seasons_error(body, body))
        _SEASONS[:] = _rows(body)
    return _SEASONS


def _season_row(season: int) -> Dict[str, Any]:
    """The ``/seasons`` row whose label ends in ``season`` (re-reads the list once when it is missing)."""
    for refresh in (False, True):
        for row in _season_list(refresh=refresh):
            if _season_end_year(row) == season:
                return row
    raise NoDataError(f"{_rt._where(f'{API}/seasons')}: no TPBL season ends in {season} (2025 = 2024-25 is the first)")


def _seasons_route() -> Dict[str, Any]:
    url = f"{API}/seasons"
    body = _list_body(url)
    _refuse(url, _seasons_error(body, body))
    _SEASONS[:] = _rows(body)
    return _stamp(body)


# ----------------------------------------------------------------------------- season routes
def _divisions_error(body: List[Any]) -> Optional[str]:
    good = _rows(body)
    if len(good) != len(body):
        return "divisions: rows are not objects"
    if len({r.get("id") for r in good}) != len(good):
        return "divisions: duplicate division ids"
    if len({r.get("gohoops_event_id") for r in good}) > 1:
        return "divisions: rows name more than one GoHoops event"
    return None


def _divisions(season_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = f"{API}/events/{season_row['id']}/divisions"
    body = _list_body(url)
    _refuse(url, _divisions_error(body))
    return _rows(body)


def _teams_error(body: List[Any]) -> Optional[str]:
    good = _rows(body)
    if len(good) != len(body) or len({r.get("id") for r in good}) != len(good):
        return "teams: rows are not unique objects"
    return None


def _games_error(body: List[Any], season_row: Dict[str, Any], team_id: Optional[int]) -> Optional[str]:
    good = _rows(body)
    if len(good) != len(body):
        return "game list holds non-object rows"
    if len({r.get("id") for r in good}) != len(good):
        return "duplicate game ids"
    events = set()
    divisions: Dict[Any, Any] = {}
    for r in good:
        for side in ("home_team", "away_team"):
            if not _same(_at(r, side, "gohoops_season_id"), season_row["gohoops_id"]):
                return f"game {r.get('id')} {side} is not in gohoops season {season_row['gohoops_id']}"
            events.add(_at(r, side, "gohoops_event_id"))
        if not isinstance(r.get("division_id"), int) or not isinstance(r.get("gohoops_division_id"), int):
            return f"game {r.get('id')} has no integer division id"
        if divisions.setdefault(r["division_id"], r["gohoops_division_id"]) != r["gohoops_division_id"]:
            return f"division {r['division_id']} maps to two GoHoops divisions"
        if team_id is not None and team_id not in (_at(r, "home_team", "id"), _at(r, "away_team", "id")):
            return f"game {r.get('id')} does not involve team {team_id}"
    if len(events) > 1:
        return "game list teams name more than one GoHoops event"
    return None


def _standings_error(body: List[Any]) -> Optional[str]:
    good = _rows(body)
    if len(good) != len(body):
        return "standings hold non-object rows"
    ids = [_at(r, "team", "id") for r in good]
    if not all(isinstance(i, int) for i in ids) or len(set(ids)) != len(ids):
        return "standings teams are not unique integer ids"
    events = {
        _at(a, "team", "gohoops_event_id") for r in good for a in (r.get("against_result") or []) if isinstance(a, dict)
    }
    if len(events) > 1:
        return "head-to-head teams name more than one GoHoops event"
    return None


def _standings_rows(division_id: int) -> List[Dict[str, Any]]:
    url = f"{API}/divisions/{division_id}/games/standings"
    body = _list_body(url)
    _refuse(url, _standings_error(body))
    return _rows(body)


def _played(standings: List[Dict[str, Any]]) -> int:
    return sum(
        (r.get("score_won_matches") or 0) + (r.get("score_lost_matches") or 0) + (r.get("score_draw_matches") or 0)
        for r in standings
    )


def _verify_games(url: str, season_row: Dict[str, Any], games: List[Dict[str, Any]]) -> None:
    """Completeness: every division's completed games == the matches of its own standings / 2.

    A division whose standings answer an empty list (the 2024-25 playoffs) is checked against the team-filtered game
    lists instead: for every team playing in it, the team's games of that division must be the same game ids in both
    lists. One request per division, plus one per team for such a division.
    """
    divs = {d.get("id"): d for d in _divisions(season_row)}
    by_div: Dict[Any, List[Dict[str, Any]]] = {}
    for g in games:
        by_div.setdefault(g.get("division_id"), []).append(g)
    for div_id, rows in sorted(by_div.items(), key=lambda kv: str(kv[0])):
        d = divs.get(div_id)
        if d is None or not _same(d.get("gohoops_id"), rows[0].get("gohoops_division_id")):
            _refuse(url, f"games name division {div_id!r}, which is not a division of season {season_row['id']}")
        standings = _standings_rows(div_id)
        if standings:
            done = sum(1 for g in rows if g.get("status") == FINISHED)
            if _played(standings) != 2 * done:
                _refuse(
                    url,
                    f"division {div_id}: {done} completed games in the list but the standings count "
                    f"{_played(standings) / 2:g}",
                )
            continue
        teams = sorted({_at(g, s, "id") for g in rows for s in ("home_team", "away_team")})
        for team in teams:
            tid = _int_arg(team, "team id")
            tl = _list_body(f"{API}/seasons/{season_row['id']}/games", {"team_id": tid})
            theirs = {g.get("id") for g in _rows(tl) if g.get("division_id") == div_id}
            ours = {g.get("id") for g in rows if tid in (_at(g, "home_team", "id"), _at(g, "away_team", "id"))}
            if theirs != ours:
                _refuse(url, f"division {div_id}: team {tid}'s own game list differs from the season list")


def _season_route(season: int, kind: str, params: Dict[str, Any]) -> Dict[str, Any]:
    verify = params.get("verify")
    team_id = None if params.get("team_id") is None else _int_arg(params["team_id"], "team_id")
    if kind == "games":  # validated before any request, the season mapping included
        if verify not in (None, True, False):
            raise ValueError(f"verify must be a bool; got {verify!r}")
        if verify and team_id is not None:
            raise ValueError("verify=True checks the whole season: it cannot be combined with team_id")
    row = _season_row(season)
    sid = int(row["id"])
    if kind == "divisions":
        url = f"{API}/events/{sid}/divisions"
        body = _list_body(url)
        _refuse(url, _divisions_error(body))
    elif kind == "teams":
        url = f"{API}/events/{sid}/teams"
        body = _list_body(url)
        _refuse(url, _teams_error(body))
    else:
        url = f"{API}/seasons/{sid}/games"
        body = _list_body(url, {"team_id": team_id} if team_id is not None else None)
        _refuse(url, _games_error(body, row, team_id))
        if verify:
            _verify_games(url, row, _rows(body))
    if not body:
        raise NoDataError(f"{_rt._where(url)}: the API answered an empty list")
    return _stamp(body, season=season, season_id=sid, gohoops_season_id=row["gohoops_id"])


# ----------------------------------------------------------------------------- game routes
def _game_error(body: Any, game_id: int) -> Optional[str]:
    if not isinstance(body, dict):
        return f"expected a JSON object, got {type(body).__name__}"
    if not _same(body.get("id"), game_id):
        return f"game id {body.get('id')!r} is not {game_id}"
    if body.get("status") != FINISHED or body.get("is_live") is not False:
        return (
            f"game {game_id} is not finished (status {body.get('status')!r}, is_live {body.get('is_live')!r}); "
            "TPBL game wrappers serve finished games only"
        )
    for side in ("home_team", "away_team"):
        if not isinstance(_at(body, side, "id"), int) or not isinstance(_at(body, side, "won_score"), int):
            return f"{side} has no integer id or final score"
    if not isinstance(body.get("round"), int) or body["round"] < 4:
        return f"round {body.get('round')!r} is not a played game's quarter count"
    return None


def _game(game_id: int) -> Dict[str, Any]:
    """The detail of a FINISHED game (cached); anything else raises.

    Raises:
        NoDataError: HTTP 404 (an unknown game id).
        AssetFetchError: the body is another game's, or the game is not finished.
    """
    if game_id not in _GAMES:
        url = f"{API}/games/{game_id}"
        body = _json(url)
        _refuse(url, _game_error(body, game_id))
        _GAMES[game_id] = _redact_personal(body)
    return _GAMES[game_id]


def _final_score(side: Any) -> Any:
    rounds = _at(side, "teams", "rounds")
    if not isinstance(rounds, dict) or not rounds:
        return None
    try:
        last = max(rounds, key=int)
    except ValueError:
        return None
    return _at(rounds[last], "total_won_score")


def _stats_error(body: Any, game: Dict[str, Any]) -> Optional[str]:
    if not (
        isinstance(body, dict) and isinstance(body.get("home_team"), dict) and isinstance(body.get("away_team"), dict)
    ):
        return "no home_team / away_team"
    for side in ("home_team", "away_team"):
        if not _same(_at(body, side, "id"), _at(game, side, "id")):
            return f"{side}.id {_at(body, side, 'id')!r} is not the game's team {_at(game, side, 'id')!r} (home/away order)"
        final = _final_score(body[side])
        if not _same(final, _at(game, side, "won_score")):
            return f"{side} final score {final!r} differs from the game detail {_at(game, side, 'won_score')!r}"
        rounds = _at(body, side, "teams", "rounds")
        if not isinstance(rounds, dict) or len(rounds) != game["round"]:
            return f"{side} has {len(rounds) if isinstance(rounds, dict) else rounds!r} quarters, the game says {game['round']}"
        players = _at(body, side, "players", "total")
        if not isinstance(players, dict) or not players:
            return f"{side} has no whole-game player lines"
        points = sum(p.get("score") or 0 for p in players.values() if isinstance(p, dict))
        if points != final:
            return f"{side} players score {points} points, the team final score is {final!r}"
    return None


def _game_route(game_id: int, stats: bool) -> Dict[str, Any]:
    game = _game(game_id)
    if not stats:
        return _stamp(game, game_id=game_id)
    url = f"{API}/games/{game_id}/stats"
    body = _json(url)
    _refuse(url, _stats_error(body, game))
    return _stamp(
        body,
        game_id=game_id,
        home_team_id=game["home_team"]["id"],
        away_team_id=game["away_team"]["id"],
        periods=game["round"],
    )


# ----------------------------------------------------------------------------- division / player routes
def _roster_error(body: List[Any], team_id: int) -> Optional[str]:
    good = _rows(body)
    if len(good) != len(body):
        return "rows are not all objects"
    events = {r.get("gohoops_event_id") for r in good}
    if len(events) > 1:
        return "roster rows name more than one GoHoops event"
    for r in good:
        if not _same(_at(r, "team", "id"), team_id):
            return f"row {r.get('id')} is a player of team {_at(r, 'team', 'id')!r}, not {team_id}"
        if not _same(_at(r, "team", "gohoops_event_id"), r.get("gohoops_event_id")):
            return f"row {r.get('id')} team is not in the row's GoHoops event"
    return None


def _shots_error(body: List[Any]) -> Optional[str]:
    for s in body:
        if not isinstance(s, dict) or set(s) != {"is_made", "point_x", "point_y", "offense_basket"}:
            return "a shot row is not {is_made, point_x, point_y, offense_basket}"
        x, y = s["point_x"], s["point_y"]
        if (
            not isinstance(s["is_made"], bool)
            or s["offense_basket"] not in (0, 1)
            or isinstance(s["offense_basket"], bool)
        ):
            return "a shot has a non-boolean is_made or an offense_basket other than 0 / 1"
        ok = (
            all(isinstance(v, int) and not isinstance(v, bool) for v in (x, y))
            and 0 <= x <= COURT_X
            and 0 <= y <= COURT_Y
        )
        if not ok:
            return f"a shot lies outside the {COURT_X} x {COURT_Y} mm court: ({x!r}, {y!r})"
    return None


def _rounds_arg(value: Any) -> Optional[List[int]]:
    if value is None:
        return None
    items = value if isinstance(value, (list, tuple)) else [value]
    out = sorted({_int_arg(v, "rounds") for v in items})
    if not out or out[0] < 1:
        raise ValueError(f"rounds must be quarter numbers (1 = first quarter); got {value!r}")
    return out


def _player_games_error(body: List[Any], player_id: int) -> Optional[str]:
    good = _rows(body)
    if len(good) != len(body):
        return "rows are not all objects"
    divisions = set()
    for r in good:
        if not _same(_at(r, "player", "id"), player_id):
            return f"a row is player {_at(r, 'player', 'id')!r}, not {player_id}"
        divisions.add(_at(r, "player", "gohoops_division_id"))
        divisions.add(_at(r, "game", "gohoops_division_id"))
    if len(divisions) > 1 or None in divisions:
        return "rows do not all name the same GoHoops division"
    if len({_at(r, "game", "id") for r in good}) != len(good):
        return "a game appears twice in the log"
    return None


# ----------------------------------------------------------------------------- entry point
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a TPBL route, refuse every wrong answer, return the body with the request stamped.

    Args:
        url: the URL a generated wrapper built: ``api.tpbl.basketball/api/{route}``. The season-keyed routes
            (``/events/{season}/divisions``, ``/events/{season}/teams``, ``/seasons/{season}/games``) carry the
            season END year in the path; the runtime sends the API's own season id.
        params: query parameters: ``team_id`` and ``verify`` for the season game list, ``rounds`` for the team shot
            list (sent as ``rounds[]``), ``division_id`` for the player game log. ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The redacted JSON body with ``_request`` stamped. A list body is wrapped as ``{"data": [...], "_request":
        {...}}``. The stamp is ``{"season", "season_id", "gohoops_season_id"}`` for a season route,
        ``{"game_id", "home_team_id", "away_team_id", "periods"}`` for the box score, ``{"game_id"}`` for the game
        detail, ``{"division_id"}`` / ``{"division_id", "team_id"}`` / ``{"division_id", "team_id", "rounds"}`` /
        ``{"division_id", "player_id"}`` for the division routes and ``{"player_id", "division_id"}`` for the log.

    Raises:
        NoDataError: see the module docstring (HTTP 404, a season the API lacks, an empty list).
        ValueError: an id or season that is not an integer, HTTP 400 / 422 or an invalid ``verify`` / ``rounds``.
        AssetFetchError: any other failed or wrong answer, including an unfinished game.
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a TPBL request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    parts = urlsplit(url)
    base = urlsplit(API)
    if parts.netloc != base.netloc or not parts.path.startswith(base.path + "/"):
        raise ValueError(f"not a TPBL route: {_rt._where(url)}")
    segs: Tuple[str, ...] = tuple(s for s in parts.path[len(base.path) :].split("/") if s)
    n = len(segs)
    if segs == ("seasons",):
        return _seasons_route()
    if n == 3 and segs[0] in ("events", "seasons") and segs[2] in ("divisions", "teams", "games"):
        if (segs[0], segs[2]) in (("events", "divisions"), ("events", "teams"), ("seasons", "games")):
            return _season_route(_int_arg(segs[1], "season"), segs[2], q)
    if n == 2 and segs[0] == "games":
        return _game_route(_int_arg(segs[1], "game id"), stats=False)
    if n == 3 and segs[0] == "games" and segs[2] == "stats":
        return _game_route(_int_arg(segs[1], "game id"), stats=True)
    if n == 4 and segs[0] == "divisions" and segs[2:] == ("games", "standings"):
        division = _int_arg(segs[1], "division id")
        url = f"{API}/divisions/{division}/games/standings"
        rows = _standings_rows(division)
        if not rows:
            raise NoDataError(
                f"{_rt._where(url)}: the division has no standings (a playoff bracket can answer an empty list); "
                "read its games from the season list instead"
            )
        return _stamp(rows, division_id=division)
    if n == 5 and segs[0] == "divisions" and segs[2] == "teams" and segs[4] in ("players", "shooting-spots"):
        division, team = _int_arg(segs[1], "division id"), _int_arg(segs[3], "team id")
        rounds = _rounds_arg(q.get("rounds")) if segs[4] == "shooting-spots" else None
        route = f"{API}/divisions/{division}/teams/{team}/{segs[4]}"
        body = _list_body(route, {"rounds[]": rounds} if rounds else None)
        if segs[4] == "players":
            _refuse(route, _roster_error(body, team))
            stamp: Dict[str, Any] = {"division_id": division, "team_id": team}
        else:
            _refuse(route, _shots_error(body))
            stamp = {"division_id": division, "team_id": team, "rounds": rounds}
        if not body:
            raise NoDataError(f"{_rt._where(route)}: the API answered an empty list")
        return _stamp(body, **stamp)
    if n == 5 and segs[0] == "divisions" and segs[2] == "players" and segs[4] == "shooting-spots":
        division, player = _int_arg(segs[1], "division id"), _int_arg(segs[3], "player id")
        route = f"{API}/divisions/{division}/players/{player}/shooting-spots"
        body = _list_body(route)
        _refuse(route, _shots_error(body))
        if not body:
            raise NoDataError(f"{_rt._where(route)}: the API answered an empty list")
        return _stamp(body, division_id=division, player_id=player)
    if n == 4 and segs[0] == "players" and segs[2:] == ("games", "stats"):
        player = _int_arg(segs[1], "player id")
        if q.get("division_id") is None:
            raise ValueError("tpbl_player_games needs division_id (the API answers HTTP 422 without it)")
        division = _int_arg(q["division_id"], "division_id")
        route = f"{API}/players/{player}/games/stats"
        try:
            body = _list_body(route, {"division_id": division}, RETRY_NO_500)
        except AssetFetchError as exc:
            if "HTTP 500" in str(exc):
                raise AssetFetchError(
                    f"{_rt._where(route)}: HTTP 500 (an unknown division_id answers 500 deterministically; it is not retried)"
                ) from None
            raise
        _refuse(route, _player_games_error(body, player))
        if not body:
            raise NoDataError(f"{_rt._where(route)}: the player has no games in division {division}")
        return _stamp(body, player_id=player, division_id=division)
    raise ValueError(f"not a TPBL route: {_rt._where(url)}")
