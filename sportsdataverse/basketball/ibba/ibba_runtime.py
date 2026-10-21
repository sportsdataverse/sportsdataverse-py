"""Runtime for the generated ``ibba`` wrappers (``basket.co.il/ws/ws.asmx`` ASP.NET web service, keyless).

Measured against the live host (sdv-internal-refs ``ibba/``, 2026-10-12):

* **Season key.** ``cYear`` is the season END year (2026 = 2025-26): the sdv key itself, so there is no mapping.
  The wrappers' ``season`` is sent as ``cYear`` unchanged (``StatsTypes`` takes no season).
* **Every parameter is required.** A missing one is a deterministic HTTP 500 ``Missing parameter: x.``, so the
  wrappers always send the op's full list in WSDL order; ``0`` means "all" for ``board_id``, ``board_round``,
  ``team_id``, ``team_uid`` and ``game_id`` on ``Games``. Booleans go out as ``true`` / ``false``.
* **Silent wrong answers, all refused** (fail closed, type-strict compares):

  - a body that is not ``{"<key>": [rows]}`` with the op's single key (an error envelope, a stub such as the
    13-byte ``no connection`` of the CMS ops, a changed shape);
  - a row whose echo field differs from a non-zero request parameter (a game, team, player, board or season
    other than the one asked for): the host filters, but a drifted answer is wrong data, not an empty frame;
  - a box score that is not a finished two-team game (neither ``Boxscore`` nor ``Games`` carries a status
    field, so this is a heuristic: two teams, two TOTAL rows of ``200 + 25 x overtimes`` player-minutes, and
    different scores; it cannot tell a final from a box frozen at the buzzer before confirmation).

* **Error vocabulary.** ``NoDataError``: HTTP 404, a legitimate empty list. A 204 / 205 returns ``{}``. An EMPTY
  200 body is what an unknown id answers, but a failed fetch answers the same, so it is an ``AssetFetchError``
  like every blank 2xx in the package. ``ValueError``: a parameter that is not an integer, boolean or non-empty string as the op needs.
  ``AssetFetchError``: any other non-200 (429 / 502 / 503 / 504 retried three times, spaced by
  ``dl_utils.download`` and honouring ``Retry-After``; a 500 is the host's deterministic error and is not
  retried), a dead connection, a non-JSON body, an error or changed envelope, a wrong-scope row, an unfinished
  game.

Every attempt, retries included, goes through the shared ``basketball_runtime.SESSION`` and so waits its turn
(at least 1 s per host).
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["REQUEST_KEY", "OPS"]

HOST = "https://basket.co.il/ws/ws.asmx"
#: Key the request is stamped under (the parsers ignore it; tests and callers can read it).
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 502, 503, 504})  # 500 is the ASMX deterministic error: retrying only adds load

#: op -> (WSDL parameters in order, the single top-level list key of the body). Kinds: ``cYear`` and every
#: ``*_id`` / ``team_uid`` are non-negative integers, ``isAverages`` / ``isPlayOff`` booleans, ``stat_type`` text.
OPS: Dict[str, Tuple[Tuple[str, ...], str]] = {
    "Configuration": (("cYear",), "settings"),
    "Boards": (("cYear",), "boards"),
    "BoardsEx": (("team_uid", "cYear"), "boards-ex"),
    "Teams": (("board_id", "cYear"), "teams"),
    "TeamById": (("team_id", "team_uid", "cYear"), "team"),
    "Players": (("team_id", "team_uid", "cYear"), "players"),
    "PlayerById": (("player_id", "cYear"), "player"),
    "Coaches": (("team_id", "team_uid", "cYear"), "coaches"),
    "Boxscore": (("game_id", "cYear"), "boxscore"),
    "PlayerStatsById": (("player_id", "cYear", "isAverages", "isPlayOff"), "player"),
    "StatsTypes": ((), "statsTypes"),
    "LeaderByType": (("stat_type", "cYear", "team_id"), "leaders"),
    "TeamStatsById": (("team_id", "cYear", "isAverages", "isPlayOff"), "team-stats"),
    "Games": (("board_id", "board_round", "team_id", "team_uid", "game_id", "cYear"), "games"),
    "GamesEx": (("board_id", "team_uid", "cYear"), "games_ex"),
    "GamesByTeam": (("team_uid", "cYear"), "games"),
}
_BOOLS = frozenset({"isAverages", "isPlayOff"})
_TEXT = frozenset({"stat_type"})
#: op -> {parameter: the row field that echoes it as a string}. A parameter with no echo (``team_uid`` on the
#: roster ops, the booleans, ``stat_type``) cannot be checked against the body.
_ECHO: Dict[str, Dict[str, str]] = {
    "Configuration": {"cYear": "Current_Year"},
    "Boards": {"cYear": "year_id"},
    "Teams": {"board_id": "board", "cYear": "year_id"},
    "TeamById": {"team_id": "team_year_id", "team_uid": "TeamUID", "cYear": "year_id"},
    "Players": {"team_id": "team_id", "cYear": "year_id"},
    "PlayerById": {"player_id": "player_id", "cYear": "year_id"},
    "Coaches": {"team_id": "team_id", "cYear": "year_id"},
    "Boxscore": {"game_id": "game_id", "cYear": "year_id"},
    "PlayerStatsById": {"player_id": "player_id", "cYear": "year_id"},
    "TeamStatsById": {"team_id": "team_id", "cYear": "year_id"},
    "LeaderByType": {"cYear": "Year", "team_id": "team_id"},
    "Games": {"board_id": "BOARD_ID", "game_id": "game_id", "cYear": "year_id"},
    "GamesEx": {"board_id": "BOARD_ID", "cYear": "year_id"},
    "GamesByTeam": {"cYear": "year_id"},
}


def _fail(msg: str) -> AssetFetchError:
    return AssetFetchError(msg)


def _fetch(url: str, params: Dict[str, Any]) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries :data:`RETRY_STATUSES` up to :data:`ATTEMPTS` times with a
    ``Retry-After``-aware delay and raises ``NoDataError`` on a 404. A connection failure that outlives the
    retries becomes an ``AssetFetchError`` naming only the exception class.
    """
    try:
        return download(
            url,
            params=params,
            session=SESSION,
            timeout=TIMEOUT,
            num_retries=ATTEMPTS - 1,
            retry_statuses=RETRY_STATUSES,
            cache_ttl=0,
        )
    except OSError as exc:  # requests' RequestException subclasses OSError
        raise _fail(f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}") from None


def _normalise(op: str, params: Dict[str, Any]) -> Dict[str, str]:
    """The op's full parameter list as wire strings, validated (``ValueError`` on a bad or missing one)."""
    names = OPS[op][0]
    unknown = sorted(set(params) - set(names))
    if unknown:
        raise ValueError(f"{op} takes {list(names)}; got unexpected {unknown}")
    out: Dict[str, str] = {}
    for name in names:
        v = params.get(name)
        if v is None:
            raise ValueError(f"{op} needs {name}")
        if name in _BOOLS:
            if not isinstance(v, bool):
                raise ValueError(f"{name} must be a bool; got {v!r}")
            out[name] = "true" if v else "false"
        elif name in _TEXT:
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{name} must be a non-empty string; got {v!r}")
            out[name] = v.strip()
        else:
            if isinstance(v, bool) or not isinstance(v, (int, str)) or not str(v).strip().isdigit():
                raise ValueError(f"{name} must be a non-negative integer; got {v!r}")
            out[name] = str(int(v))
    return out


def _same(a: Any, b: Any) -> bool:
    """Type-strict string equality: the host sends every value as a string, so an int or null never matches."""
    return isinstance(a, str) and isinstance(b, str) and a == b


def _box_final(rows: list) -> Optional[str]:
    """Why a ``Boxscore`` body is not a finished two-team game, or ``None`` when it looks final."""
    totals = [r for r in rows if r.get("jersy") == "TOTAL"]
    teams = {r.get("team_id") for r in rows}
    if len(teams) != 2 or len(totals) != 2 or {t.get("team_id") for t in totals} != teams:
        return "not a two-team box score with two TOTAL rows"
    for t in totals:
        try:
            minutes = int(t["minutes"])
        except (KeyError, TypeError, ValueError):
            return f"TOTAL minutes {t.get('minutes')!r} is not an integer"
        if minutes < 200 or minutes % 25:
            return f"the game is not final: TOTAL minutes {minutes} is not 200 + 25 x overtimes"
    if totals[0].get("pts") == totals[1].get("pts"):
        return f"the game is not final: tied at {totals[0].get('pts')!r}"
    return None


def _scope_error(op: str, wire: Dict[str, str], rows: list) -> Optional[str]:
    for name, field in _ECHO.get(op, {}).items():
        if wire[name] == "0":  # 0 = "all" on this API
            continue
        for r in rows:
            if not _same(r.get(field), wire[name]):
                return f"out of scope: {field}={r.get(field)!r}, asked {name}={wire[name]}"
    team = wire.get("team_id", "0")
    if op in ("Games", "GamesEx", "GamesByTeam") and team != "0":
        if any(team not in (r.get("team1"), r.get("team2"), r.get("ig_team_1"), r.get("ig_team_2")) for r in rows):
            return f"out of scope: a game without team {team}"
    if op == "Boxscore":
        return _box_final(rows)
    return None


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET one ASMX op, refuse every wrong answer, return ``{"<key>": rows, "_request": {...}}``.

    Args:
        url: the URL a generated wrapper built (``basket.co.il/ws/ws.asmx/{Op}``).
        params: the op's parameters by wire name (``cYear``, ``team_uid``, ...); ``None`` values are dropped
            first, then every WSDL parameter must be present.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The parsed body ``{"<key>": [row, ...]}`` with the request stamped under ``_request``
        (``{"op": ..., "params": {...}}``); ``{}`` for a 204 / 205.

    Raises:
        NoDataError: HTTP 404, an empty list.
        ValueError: a missing, unexpected or malformed parameter, or HTTP 400 / 422.
        AssetFetchError: an empty 200 body (an unknown id, or a failed fetch), or any other failed or wrong
            answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for an IBBA request: {sorted(kwargs)}")
    parts = urlsplit(url)
    op = parts.path.rsplit("/", 1)[-1]
    if f"{parts.scheme}://{parts.netloc}{parts.path.rsplit('/', 1)[0]}" != HOST or op not in OPS:
        raise ValueError(f"not an IBBA ASMX route: {_rt._where(url)}")
    wire = _normalise(op, {k: v for k, v in (params or {}).items() if v is not None})
    where = _rt._where(url)
    resp = _fetch(url, wire)
    status = getattr(resp, "status_code", None)
    _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    if status != 200:
        raise _fail(f"{where} answered HTTP {status}, not 200")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        # a blank 2xx is a failed fetch repo-wide, even where the host also answers an unknown id so (EX-SYNC-d)
        raise _fail(f"{where}: an empty 200 body (an unknown id answers so too, but so does a failed fetch)")
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    if "json" not in ctype:
        raise _fail(f"{where} answered {ctype or 'no content type'}, not JSON")
    try:
        body = json.loads(text)
    except ValueError:
        raise _fail(f"{where} answered a body that is not JSON") from None
    key = OPS[op][1]
    if not isinstance(body, dict) or list(body) != [key]:
        raise _fail(f"{where}: not the {op} envelope (expected the single key {key!r})")
    rows = body[key]
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise _fail(f"{where}: {key!r} is not a list of objects")
    if not rows:
        raise NoDataError(f"{where}: no rows for {wire}")
    why = _scope_error(op, wire, rows)
    if why:
        raise _fail(f"{where}: {why}")
    body[REQUEST_KEY] = {"op": op, "params": wire}
    return body
