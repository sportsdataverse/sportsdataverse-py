"""Runtime for the generated BNXT League wrappers (``bnxt.sportpress.info/api/v1``, the Sportpress CMS).

The API is the JSON backend of the bnxtleague.com web app. Measured against the live host
(sdv-internal-refs ``sportpress/``, 2026-10-12):

* **Header key.** Every route needs ``X-Authorization: <key>`` (401 without it). The key is public: it
  is hard-coded in the site bundle ``bnxtleague.com/js/app.<hash>.js``. This runtime reads it from the
  bundle at run time (one read per process; accepted only if it is 36 letters / digits) and falls back
  to the documented :data:`FALLBACK_KEY` (the value of 2026-10-12) when the bundle cannot be read. On a
  401 it re-reads the bundle once, then raises ``AssetFetchError`` with a "key rotated?" hint. The key is
  NEVER printed, logged or put in an error message: every message passes through :func:`_scrub`.
* **Season key.** The END year (``/phase/season/2026`` = 2025-26), as in this package. The season routes
  (``phase``, ``schedule/club``, ``roster/season-players``, ``roster/season-nationality-codes``) take it
  as is, so the mapping is the identity and the body's ``competition.season`` is checked against the
  request. Every other route is keyed by the competition id (one per season; ``/phase/season`` maps a
  season to it), the competition-team id, the roster id or the game id.
* **Language.** ``X-Localization: en`` is always sent so the output does not depend on a default. Only
  team display names in the standings vary by language; never join on names.
* **Personal data.** Contact and social fields are dropped from every body (:func:`_scrub`); a player's
  date of birth is public on the site and stays, anyone else's (staff, referees) is dropped.
* **Silent wrong answers, all refused.** An unknown id answers HTTP 200 ``{"data": []}``, never another
  object's body, so the empty answer is ``NoDataError``. The game routes (box score, play-by-play,
  overview) carry no echo of the game id, so each one first reads ``/schedule/find/{gameId}`` (cached
  once the game is finished) and refuses a game that is not finished, a ``competitionId`` that is not the
  game's, a team that is not one of its two sides, and a play-by-play whose running score does not end on
  the final score (a truncated or foreign film). The season, per-competition and per-team routes are
  checked against the id or season they echo (see :data:`_ROUTES`); the standings, rankings, rosters,
  player averages and career routes have no echo and are checked for shape only.
* **Error vocabulary.** ``NoDataError``: HTTP 404, an empty ``data`` list, an empty ranking.
  ``ValueError``: HTTP 400 / 422, an id or season that is not an integer. ``AssetFetchError``: any other
  non-200 (429 / 5xx after three attempts, spaced by ``dl_utils.download`` and honouring ``Retry-After``),
  a dead connection, a rejected key, a non-JSON body, an error envelope, a changed envelope, an
  unfinished game and any body for another season, competition, team or game than the one asked for.

Every attempt, retries and the bundle reads included, goes through the shared
``basketball_runtime.SESSION`` and so waits its turn (at least 1 s per host).
"""

from __future__ import annotations

import json
import re
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import NOREDIRECT_SESSION as SESSION
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["FALLBACK_KEY", "REQUEST_KEY", "RANK_KEYS"]

SITE = "https://bnxtleague.com"
API = "https://bnxt.sportpress.info/api/v1"
#: Public key from the bnxtleague.com bundle, read 2026-10-12 (also in the spec's ``securitySchemes``).
FALLBACK_KEY = "BWSyE7sgg9QAurh2JX9cpjzjGc652BWLuNUS"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
#: The nine ranking categories, in the order the API lists them.
RANK_KEYS = (
    "efficiency",
    "points",
    "total_rebound",
    "block",
    "assist",
    "steal",
    "two_point_percent",
    "three_point_percent",
    "free_throw_percent",
)

_KEY_RE = re.compile(r"[A-Za-z0-9]{36}")
_APP_JS = re.compile(r'src="(/js/app\.[0-9a-f]+\.js)"')
_BUNDLE_KEY = re.compile(r'\["X-Authorization"\]\s*=\s*"([^"]{1,80})"')
_ERROR_KEYS = frozenset({"error", "errors", "message", "exception", "trace"})
#: Contact, social and date-of-birth keys (dropped); a player's ``birthdate`` is the one exception.
_PERSONAL = re.compile(
    r"(birth|^dob$|e_?mail|phone|mobile|^tel$|address|home_?page|website|passport|salary|facebook|twitter|instagram)",
    re.IGNORECASE,
)
_PLAYER_KEYS = frozenset({"player", "player2"})

_key: Dict[str, Optional[str]] = {"value": None, "source": None}
_key_lock = threading.Lock()
_finished: Dict[int, Dict[str, Any]] = {}


# --------------------------------------------------------------------------------------- secrets
def _scrub(text: Any) -> str:
    """``text`` with the key in use and the documented fallback replaced by ``<key>``."""
    out = str(text)
    for k in {_key["value"], FALLBACK_KEY}:
        if k:
            out = out.replace(k, "<key>")
    return out


def _fail(msg: str) -> AssetFetchError:
    return AssetFetchError(_scrub(msg))


def _valid_key(value: Any) -> bool:
    return isinstance(value, str) and _KEY_RE.fullmatch(value) is not None


# --------------------------------------------------------------------------------------- transport
def _fetch(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries 429 / 5xx up to :data:`ATTEMPTS` times with a ``Retry-After``-aware
    delay and raises ``NoDataError`` on a 404. A connection failure that outlives the retries becomes an
    ``AssetFetchError`` naming only the exception class (never the URL's query or any header).
    """
    try:
        return download(
            url,
            params=params,
            headers=headers,
            session=SESSION,
            timeout=TIMEOUT,
            num_retries=ATTEMPTS - 1,
            retry_statuses=RETRY_STATUSES,
            cache_ttl=0,
        )
    except OSError as exc:  # requests' RequestException subclasses OSError
        raise _fail(f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}") from None


def _read_key(*, renew: bool = False) -> str:
    """The ``X-Authorization`` key: the bundle's, else :data:`FALLBACK_KEY`. Cached per process."""
    with _key_lock:
        if _key["value"] is not None and not renew:
            return str(_key["value"])
        found: Optional[str] = None
        try:
            home = _fetch(SITE + "/")
            m = _APP_JS.search(getattr(home, "text", "") or "") if getattr(home, "status_code", None) == 200 else None
            if m:
                js = _fetch(SITE + m.group(1))
                km = (
                    _BUNDLE_KEY.search(getattr(js, "text", "") or "")
                    if getattr(js, "status_code", None) == 200
                    else None
                )
                if km and _valid_key(km.group(1)):
                    found = km.group(1)
        except (AssetFetchError, NoDataError):
            found = None  # the documented fallback answers any failure to read the bundle
        _key["value"], _key["source"] = (found, "bundle") if found else (FALLBACK_KEY, "fallback")
        return str(_key["value"])


def _call(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """GET ``API + path`` with the key; one key renewal on a 401. Returns the parsed JSON body.

    Raises:
        NoDataError: HTTP 404.
        ValueError: HTTP 400 / 422.
        AssetFetchError: a rejected key (after a renewal), any other non-200, a non-JSON or empty body.
    """
    url = API + path
    where = _rt._where(url)
    resp: Any = None
    for renew in (False, True):
        headers = {"X-Authorization": _read_key(renew=renew), "X-Localization": "en", "Accept": "application/json"}
        resp = _fetch(url, params, headers)
        if getattr(resp, "status_code", None) != 401:
            break
    status = getattr(resp, "status_code", None)
    if status == 401:
        raise _fail(
            f"{where} answered HTTP 401: the X-Authorization key was rejected (key rotated? re-read it from the "
            f"app.<hash>.js bundle linked from {SITE}/ and update FALLBACK_KEY)"
        )
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    if status != 200:
        raise _fail(f"{where} answered HTTP {status}, not 200")
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    text = getattr(resp, "text", "") or ""
    if "json" not in ctype:
        raise _fail(f"{where} answered {ctype or 'no content type'}, not JSON")
    if not text.strip():
        raise _fail(f"{where} answered HTTP 200 with an empty body")
    try:
        return json.loads(text)
    except ValueError:
        raise _fail(f"{where} answered a body that is not JSON") from None


# --------------------------------------------------------------------------------------- personal data
def _scrub_personal(obj: Any, in_player: bool = False) -> Any:
    """``obj`` without contact / social keys and without any date of birth outside a player object."""
    if isinstance(obj, dict):
        return {
            k: _scrub_personal(v, in_player or k in _PLAYER_KEYS)
            for k, v in obj.items()
            if not _PERSONAL.search(str(k)) or (in_player and k == "birthdate")
        }
    if isinstance(obj, list):
        return [_scrub_personal(v, in_player) for v in obj]
    return obj


# --------------------------------------------------------------------------------------- shape helpers
def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _get_in(d: Any, *path: str) -> Any:
    for k in path:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _same(a: Any, b: Any) -> bool:
    """Type-strict equality: ``True`` is not 1 and ``"1"`` is not 1."""
    return type(a) is type(b) and a == b


def _data_list(body: Any, where: str) -> List[Dict[str, Any]]:
    """``body["data"]`` as a non-empty list of objects.

    Raises:
        AssetFetchError: an error envelope, a body that is not ``{"data": [...]}`` or a row that is not an object.
        NoDataError: the list is empty (the API answers an unknown id this way).
    """
    if isinstance(body, dict) and "data" not in body and _ERROR_KEYS & set(body):
        raise _fail(f"{where}: the body is an error envelope")
    if not isinstance(body, dict) or set(body) != {"data"} or not isinstance(body["data"], list):
        raise _fail(f"{where}: not a Sportpress list envelope")
    rows = body["data"]
    if not rows:
        raise NoDataError(f"{where}: no rows (an unknown id or a season without data)")
    if not all(isinstance(r, dict) for r in rows):
        raise _fail(f"{where}: a row is not an object")
    return rows


def _data_obj(body: Any, where: str) -> Dict[str, Any]:
    """``body["data"]`` as a non-empty object (``NoDataError`` when it is an empty list / object)."""
    if isinstance(body, dict) and "data" not in body and _ERROR_KEYS & set(body):
        raise _fail(f"{where}: the body is an error envelope")
    if not isinstance(body, dict) or set(body) != {"data"}:
        raise _fail(f"{where}: not a Sportpress object envelope")
    d = body["data"]
    if d in ([], {}):
        raise NoDataError(f"{where}: no data (an unknown id)")
    if not isinstance(d, dict):
        raise _fail(f"{where}: data is not an object")
    return d


def _sides(game: Any) -> Dict[int, Dict[str, Any]]:
    """``{1: home competitor, 2: away competitor}`` of a game object; ``{}`` when malformed."""
    comps = game.get("competitors") if isinstance(game, dict) else None
    if not isinstance(comps, list) or len(comps) != 2 or not all(isinstance(c, dict) for c in comps):
        return {}
    out = {c.get("side"): c for c in comps}
    return out if set(out) == {1, 2} else {}


def _team_id(competitor: Any) -> Any:
    return _get_in(competitor, "competition_team", "id")


# --------------------------------------------------------------------------------------- game identity
def _ident(g: Dict[str, Any]) -> Dict[str, Any]:
    sides = _sides(g)
    return {
        "competition_id": _get_in(g, "competition", "id"),
        "home": _team_id(sides[1]),
        "away": _team_id(sides[2]),
        "score": {1: sides[1].get("finalScore"), 2: sides[2].get("finalScore")},
    }


def _game(gid: int) -> Dict[str, Any]:
    """Identity of FINISHED game ``gid`` from ``/schedule/find``; cached once finished.

    Raises:
        AssetFetchError: the game is not finished, or the body is not that game.
        NoDataError: the API has no such game.
    """
    if gid not in _finished:
        _c_game_find(_call(f"/schedule/find/{gid}"), {"gameId": gid}, {}, f"{urlsplit(API).netloc}/schedule/find/{gid}")
        if gid not in _finished:  # _c_game_find caches finished games only
            raise _fail(
                f"{urlsplit(API).netloc}/schedule/find/{gid}: game {gid} is not finished: no box score or film yet"
            )
    return _finished[gid]


def _check_game_find(g: Dict[str, Any], gid: int, where: str) -> None:
    sides = _sides(g)
    if not _same(g.get("id"), gid):
        raise _fail(f"{where}: out of scope: the body is game {g.get('id')}, asked for {gid}")
    if not sides or not all(_is_int(_team_id(c)) for c in sides.values()):
        raise _fail(f"{where}: game {gid} has no two sides")


# --------------------------------------------------------------------------------------- per-route checks
# Each check gets (body, ids, query, where), validates scope and shape, and returns nothing.
Check = Callable[[Any, Dict[str, int], Dict[str, Any], str], None]


def _c_server_config(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    langs = body.get("available_lang_array") if isinstance(body, dict) else None
    if not isinstance(langs, list) or not {"en", "fr", "nl"} <= {_get_in(x, "alpha2") for x in langs}:
        raise _fail(f"{where}: no en / fr / nl language tables")
    if not all(isinstance(x.get("resources"), dict) and x["resources"] for x in langs):
        raise _fail(f"{where}: an empty language table")


def _c_club_seasons(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    if not all(_is_int(r.get("season")) for r in _data_list(body, where)):
        raise _fail(f"{where}: a row without an integer season")


def _c_referees(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    want = q.get("title_id")
    if want is not None and any(not _same(_get_in(r, "title", "id"), want) for r in _data_list(body, where)):
        raise _fail(f"{where}: out of scope: a staff member is not of title {want}")
    _data_list(body, where)


def _c_phase_season(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    for r in _data_list(body, where):
        if not _same(_get_in(r, "competition", "season"), ids["season"]):
            raise _fail(f"{where}: out of scope: a competition is not of season {ids['season']}")
        if not isinstance(r.get("phases"), list):
            raise _fail(f"{where}: a competition without a phases list")


def _c_competition_teams(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    cid = q.get("competition_id")
    for r in _data_list(body, where):
        if not (_same(r.get("competition_id"), cid) and _same(_get_in(r, "competition", "id"), cid)):
            raise _fail(f"{where}: out of scope: team {r.get('id')} is not of competition {cid}")


def _c_standings(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    for r in _data_list(body, where):
        if not (_is_int(_get_in(r, "team", "id")) and _is_int(r.get("played"))):
            raise _fail(f"{where}: a standings row without team.id / played")


def _c_schedule_club(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    seen = set()
    for g in _data_list(body, where):
        if not _same(_get_in(g, "competition", "season"), ids["season"]):
            raise _fail(f"{where}: out of scope: game {g.get('id')} is not of season {ids['season']}")
        if q.get("phase_id") is not None and not _same(_get_in(g, "phase", "id"), q["phase_id"]):
            raise _fail(f"{where}: out of scope: game {g.get('id')} is not of phase {q['phase_id']}")
        if not _sides(g) or not _is_int(g.get("id")) or g["id"] in seen:
            raise _fail(f"{where}: a duplicate, id-less or side-less game")
        seen.add(g["id"])


def _c_game_slider(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    for g in _data_list(body, where):
        if not _same(_get_in(g, "competition", "id"), ids["competitionId"]):
            raise _fail(f"{where}: out of scope: game {g.get('id')} is not of competition {ids['competitionId']}")
        if g.get("genius_id") is not None and not _is_int(g["genius_id"]):
            raise _fail(f"{where}: genius_id is neither null nor an integer")


def _ranking(who: str) -> Check:
    def check(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
        if body == []:
            raise NoDataError(f"{where}: no ranking (a competition without statistics)")
        if isinstance(body, dict) and "data" not in body and _ERROR_KEYS & set(body):
            raise _fail(f"{where}: the body is an error envelope")
        one = q.get("type") not in (None, "all")
        keys = tuple(body) if isinstance(body, dict) else ()
        if not keys or (not one and keys != RANK_KEYS) or (one and not set(keys) <= set(RANK_KEYS)):
            raise _fail(f"{where}: the categories are not the nine ranking categories")
        for cat, rows in body.items():
            if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
                raise _fail(f"{where}: {cat} is not a list of rows")
            need = ("team", "value") if who == "team" else ("team", "value", "player", "roster_id")
            if any(not all(k in r for k in need) for r in rows):
                raise _fail(f"{where}: a {cat} row without {need}")

    return check


def _c_roster_rows(key: Optional[Tuple[str, str]]) -> Check:
    def check(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
        for r in _data_list(body, where):
            if key and not _same(r.get(key[0]), ids[key[1]]):
                raise _fail(f"{where}: out of scope: roster {r.get('id')} is not of competition team {ids[key[1]]}")
            if not (_is_int(r.get("id")) and isinstance(r.get("player"), dict)):
                raise _fail(f"{where}: a roster row without id / player")

    return check


def _c_nationality_codes(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    if body == []:
        raise NoDataError(f"{where}: no nationality codes for the season")
    if not isinstance(body, list) or not all(isinstance(x, str) for x in body):
        raise _fail(f"{where}: not a list of codes")


def _c_team_find(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    d = _data_obj(body, where)
    if not _same(d.get("id"), ids["competitionTeamId"]):
        raise _fail(f"{where}: out of scope: the body is team {d.get('id')}, asked for {ids['competitionTeamId']}")


def _c_player_averages(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    if any(not (_is_int(r.get("roster_id")) and isinstance(r.get("player"), dict)) for r in _data_list(body, where)):
        raise _fail(f"{where}: a row without roster_id / player")


def _c_team_games(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    tid = ids["competitionTeamId"]
    for g in _data_list(body, where):
        if not any(_same(_team_id(c), tid) for c in _sides(g).values()) or g.get("status") != "finished":
            raise _fail(f"{where}: out of scope: game {g.get('id')} is not a finished game of team {tid}")
        if not isinstance(g.get("total"), dict):
            raise _fail(f"{where}: game {g.get('id')} has no team total")


def _c_roster_find(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    d = _data_obj(body, where)
    if not _same(d.get("id"), ids["rosterId"]):
        raise _fail(f"{where}: out of scope: the body is roster {d.get('id')}, asked for {ids['rosterId']}")


def _c_player_career(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    if any(
        not (isinstance(r.get("competition"), dict) and isinstance(r.get("team"), dict))
        for r in _data_list(body, where)
    ):
        raise _fail(f"{where}: a career row without competition / team")


def _c_player_games(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    for r in _data_list(body, where):
        g = r.get("game")
        if not (_sides(g) and any(_same(_team_id(c), r.get("competition_team_id")) for c in _sides(g).values())):
            raise _fail(f"{where}: a line whose team did not play its game")


def _c_game_find(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    g = _data_obj(body, where)
    _check_game_find(g, ids["gameId"], where)
    if g.get("status") == "finished":
        _finished[ids["gameId"]] = _ident(g)


def _c_overview(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    ident = _game(ids["gameId"])
    d = body.get("data") if isinstance(body, dict) else None
    keys = ("team_totals", "top_performer", "game_leaders")
    if not isinstance(d, dict) or set(d) != set(keys) or not all(isinstance(d[k], list) for k in keys):
        raise _fail(f"{where}: not the overview envelope")
    if not any(d[k] for k in keys):
        raise NoDataError(f"{where}: an overview without statistics")
    want = {1: (ident["home"], True), 2: (ident["away"], False)}
    for k in keys:
        if len(d[k]) != 2:
            raise _fail(f"{where}: {k} is not two rows")
        for r in d[k]:
            c = r.get("competitor") if isinstance(r, dict) else None
            side = _get_in(c, "side")
            if side not in want or not _same(_team_id(c), want[side][0]) or not _same(r.get("is_home"), want[side][1]):
                raise _fail(f"{where}: out of scope: {k} side / team does not match game {ids['gameId']}")
            if not _same(_get_in(c, "finalScore"), ident["score"][side]):
                raise _fail(f"{where}: out of scope: {k} final score differs from game {ids['gameId']}")


def _c_box(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    ident = _game(ids["gameId"])
    if not _same(ident["competition_id"], ids["competitionId"]):
        raise _fail(f"{where}: out of scope: game {ids['gameId']} is of competition {ident['competition_id']}")
    rows = _data_list(body, where)
    if len(rows) != 2 or sorted(r.get("is_home") is True for r in rows) != [False, True]:
        raise _fail(f"{where}: not two teams, one home and one away")
    want = {True: ident["home"], False: ident["away"]}
    for r in rows:
        tid = _get_in(r, "team", "id")
        if not _same(tid, want[r["is_home"]]):
            raise _fail(f"{where}: out of scope: team {tid} is not a side of game {ids['gameId']}")
        players = r.get("players")
        if not isinstance(players, list) or not players or not isinstance(r.get("total"), dict):
            raise _fail(f"{where}: a team without player lines / total")
        if any(not _same(p.get("competition_team_id"), tid) for p in players):
            raise _fail(f"{where}: a player line is of another team")


def _c_film(body: Any, ids: Dict[str, int], q: Dict[str, Any], where: str) -> None:
    ident = _game(ids["gameId"])
    if not _same(ident["competition_id"], ids["competitionId"]):
        raise _fail(f"{where}: out of scope: game {ids['gameId']} is of competition {ident['competition_id']}")
    rows = _data_list(body, where)
    orders = [r.get("order") for r in rows]
    if not all(_is_int(o) for o in orders) or orders != sorted(set(orders)):  # type: ignore[type-var]
        raise _fail(f"{where}: event order is not strictly increasing")
    want = {True: ident["home"], False: ident["away"]}
    for r in rows:
        home = r.get("is_home")
        if not isinstance(home, bool) or not _same(_get_in(r, "team", "id"), want[home]):
            raise _fail(f"{where}: out of scope: event {r.get('order')} is of a team outside game {ids['gameId']}")
    top = (max(r.get("current_home_score") or 0 for r in rows), max(r.get("current_away_score") or 0 for r in rows))
    if top != (ident["score"][1], ident["score"][2]):
        raise _fail(
            f"{where}: the running score ends {top[0]}-{top[1]} but the game finished "
            f"{ident['score'][1]}-{ident['score'][2]}: a truncated or foreign play-by-play"
        )


# (regex over the path under API, check, ids that go into _request). First match wins.
_ROUTES: List[Tuple["re.Pattern[str]", Check, Tuple[str, ...]]] = [
    (re.compile(p), c, r)
    for p, c, r in [
        (r"^/server-config$", _c_server_config, ()),
        (r"^/competition-team/club/seasons$", _c_club_seasons, ()),
        (r"^/staff/list$", _c_referees, ()),
        (r"^/phase/season/(?P<season>[^/]+)$", _c_phase_season, ("season",)),
        (r"^/competition-team/all$", _c_competition_teams, ()),
        (
            r"^/standings/competition/(?P<competitionId>[^/]+)/phase/(?P<phaseId>[^/]+)$",
            _c_standings,
            ("competitionId", "phaseId"),
        ),
        (r"^/schedule/club/(?P<season>[^/]+)$", _c_schedule_club, ("season",)),
        (r"^/schedule/game-slider/(?P<competitionId>[^/]+)$", _c_game_slider, ("competitionId",)),
        (r"^/competition-team/average-rankings/(?P<competitionId>[^/]+)$", _ranking("team"), ("competitionId",)),
        (r"^/player/average-rankings/(?P<competitionId>[^/]+)$", _ranking("player"), ("competitionId",)),
        (r"^/roster/season-players/(?P<season>[^/]+)$", _c_roster_rows(None), ("season",)),
        (r"^/roster/season-nationality-codes/(?P<season>[^/]+)$", _c_nationality_codes, ("season",)),
        (
            r"^/roster/team-players/(?P<competitionTeamId>[^/]+)$",
            _c_roster_rows(("competition_team_id", "competitionTeamId")),
            ("competitionTeamId",),
        ),
        (r"^/competition-team/find/(?P<competitionTeamId>[^/]+)$", _c_team_find, ("competitionTeamId",)),
        (
            r"^/competition-team/player-averages/(?P<competitionTeamId>[^/]+)$",
            _c_player_averages,
            ("competitionTeamId",),
        ),
        (
            r"^/competition-team/statistics-by-game/total/(?P<competitionTeamId>[^/]+)$",
            _c_team_games,
            ("competitionTeamId",),
        ),
        (r"^/roster/find/(?P<rosterId>[^/]+)$", _c_roster_find, ("rosterId",)),
        (r"^/player/career-averages/(?P<rosterId>[^/]+)$", _c_player_career, ("rosterId",)),
        (r"^/boxscore/player/season-games/(?P<rosterId>[^/]+)$", _c_player_games, ("rosterId",)),
        (r"^/schedule/find/(?P<gameId>[^/]+)$", _c_game_find, ("gameId",)),
        (r"^/schedule/overview/(?P<gameId>[^/]+)$", _c_overview, ("gameId",)),
        (r"^/boxscore/game/(?P<competitionId>[^/]+)/(?P<gameId>[^/]+)$", _c_box, ("competitionId", "gameId")),
        (r"^/film/game/(?P<competitionId>[^/]+)/(?P<gameId>[^/]+)$", _c_film, ("competitionId", "gameId")),
    ]
]

#: Routes of a FINISHED game: refused (AssetFetchError) for a game that is not finished, before they are requested.
_GATED = frozenset({_c_overview, _c_box, _c_film})
#: Snake-case names the parsers read from ``_request`` for each path id / query key.
_REQ_NAME = {
    "season": "season",
    "competitionId": "competition_id",
    "phaseId": "phase_id",
    "competitionTeamId": "competition_team_id",
    "rosterId": "roster_id",
    "gameId": "game_id",
}
_QUERY_NAME = {"competition_id": "competition_id", "phase_id": "phase_id", "title_id": "title_id", "month": "month"}


def _int_arg(value: Any, what: str) -> int:
    if _is_int(value):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    raise ValueError(f"{what} must be an integer; got {value!r}")


# --------------------------------------------------------------------------------------- the getter
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a Sportpress route, refuse every wrong answer, return the body with the request stamped.

    Args:
        url: the URL a generated wrapper built (``bnxt.sportpress.info/api/v1/<route>``).
        params: query parameters; ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The body without contact fields: ``{"data": ...}`` (a bare list such as the nationality codes is
        wrapped as ``{"data": [...]}``), the settings object of ``/server-config``, or the ranking object
        of nine categories; plus ``_request`` = the ids and query values asked for (``season``,
        ``competition_id``, ``phase_id``, ``competition_team_id``, ``roster_id``, ``game_id``, ...).

    Raises:
        NoDataError: see the module docstring (HTTP 404, an empty list, an empty ranking).
        ValueError: an id or season that is not an integer, or HTTP 400 / 422, or not a Sportpress route.
        AssetFetchError: any other failed or wrong answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a Sportpress request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    parts = urlsplit(url)
    base = urlsplit(API)
    if parts.netloc != base.netloc or not parts.path.startswith(base.path + "/"):
        raise ValueError(f"not a Sportpress route: {_rt._where(url)}")
    path = parts.path[len(base.path) :]
    for pattern, check, req in _ROUTES:
        m = pattern.match(path)
        if m:
            break
    else:
        raise ValueError(f"not a Sportpress route: {_rt._where(url)}")
    ids = {name: _int_arg(m.group(name), name) for name in req}
    for name in ("competition_id", "phase_id", "title_id", "month"):
        if name in q:
            q[name] = _int_arg(q[name], name)
    canonical = pattern.pattern.strip("^$")
    path = re.sub(r"\(\?P<(\w+)>\[\^/\]\+\)", lambda mm: str(ids[mm.group(1)]), canonical)
    if check in _GATED:  # the gate reads the game first, so an unfinished game costs one request
        _game(ids["gameId"])
    body = _scrub_personal(_call(path, q or None))
    where = f"{base.netloc}{path}"
    check(body, ids, q, where)
    if isinstance(body, list):
        body = {"data": body}
    body[REQUEST_KEY] = {
        **{_REQ_NAME[k]: v for k, v in ids.items()},
        **{_QUERY_NAME[k]: v for k, v in q.items() if k in _QUERY_NAME},
        **({"type": q["type"]} if "type" in q else {}),
    }
    return body
