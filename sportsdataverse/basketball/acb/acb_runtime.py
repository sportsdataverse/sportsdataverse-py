"""Runtime for the generated ACB wrappers (``api2.acb.com``, the .NET API behind acb.com and live.acb.com).

One host, 33 JSON routes, measured against the live API (sdv-internal-refs ``acb/``, 2026-10-12):

* **API key.** Every request carries ``X-APIKEY``; without it the API answers 401. The key is public: it
  sits in acb.com's Next.js bundle (``sharedServicesConfigDotnet: {baseUrl, apiKey}``). The documented value is
  built in (:data:`DEFAULT_KEY`), ``$ACB_API_KEY`` overrides it, and on a 401 the runtime re-reads the key from
  the live bundle once (home page, then at most :data:`MAX_CHUNKS` script chunks, each spaced like any request)
  and retries. A key the API still rejects is an ``AssetFetchError``. The key is never put in an error message.
* **Season key.** The API keys a season by ``editionId`` = season START year - 1935 (91 = 2026-27). The public
  key is the END year: wrappers take ``season`` (2027 = 2026-27) and this runtime sends
  ``editionId = season - 1936`` (:func:`edition_id`; the first edition, 48, is 1983-84 = season 1984). A route
  that omits ``season`` serves the current edition.
* **Silent wrong answers, all refused** (fail closed, type-strict compares): a body whose echoed filters
  (``selectedFilters.season`` / ``competition`` / ``round`` / ``week``, ``matchId``, ``clubId``, the player's
  licence, the competition ids) differ from the request, whose team rows belong to another edition, or whose
  rows do not involve the asked club or search term. Routes that echo nothing of the request (most match routes
  answer by ``matchId`` alone) are checked for the envelope only.
* **Error vocabulary.** ``NoDataError``: HTTP 404 (an unknown id: ``text/plain``), HTTP 204 (a match outside a
  playoff series), and a route whose rows are legitimately empty (play-by-play and shots before 2016-17 answer
  empty ``plays`` / ``shotPoints``; a club with no matches that season). ``ValueError``: HTTP 400 / 422, a missing
  or non-integer argument (raised before any request). ``AssetFetchError``: any other non-200 (429 / 5xx after
  three attempts), a dead connection, a non-JSON or ``problem+json`` body, an error envelope, an all-empty stub,
  a wrong-scope body, a match that is not finished (``matchFinished`` false: the box score, play-by-play, shots
  and match panels of a live or scheduled game are partial and refused), and a rejected key.

Every attempt, retries included, goes through the shared ``basketball_runtime.SESSION`` and so waits its turn
(at least 1 s per host); ``Retry-After`` on 429 / 503 is honoured by ``dl_utils.download``.
"""

from __future__ import annotations

import json
import os
import re
import threading
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["DEFAULT_KEY", "REQUEST_KEY", "edition_id", "season_of"]

API = "https://api2.acb.com/api"
SITE = "https://www.acb.com/"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
#: The public key read from acb.com's bundle on 2026-10-12 (also in the refs spec); ``$ACB_API_KEY`` overrides.
DEFAULT_KEY = "0dd94928-6f57-4c08-a3bd-b1b2f092976e"
KEY_ENV = "ACB_API_KEY"
ATTEMPTS = 3
TIMEOUT = 60
MAX_CHUNKS = 60  # bundle chunks read while looking for the key (it was the 11th on 2026-10-12)
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
#: ``editionId`` = season START year - 1935 = END year - 1936.
EDITION_OFFSET = 1936
FIRST_SEASON = 1984  # editionId 48

_KEY_RE = re.compile(
    r'sharedServicesConfigDotnet[^{]{0,12}\{baseUrl:"https://api2\.acb\.com",apiKey:"([0-9A-Za-z-]{16,64})"'
)
_CHUNK_RE = re.compile(r"/_next/static/chunks/[A-Za-z0-9_\-./]+?\.js")
_ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status", "statusCode", "type", "title"})

_key: Dict[str, Optional[str]] = {"value": None}
_key_lock = threading.Lock()


def edition_id(season: int) -> int:
    """The ACB ``editionId`` of a season given by its END year (2027 -> 91).

    Args:
        season: season END year, 1984 or later (1984 = 1983-84, the first ACB season).

    Returns:
        The API's ``editionId`` (``season - 1936``).

    Raises:
        ValueError: ``season`` is not an integer, or is before 1984.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb.acb_runtime import edition_id

            edition_id(2027)  # 91
    """
    s = _int(season, "season")
    if s < FIRST_SEASON:
        raise ValueError(f"season must be {FIRST_SEASON} (1983-84, the first ACB season) or later; got {s}")
    return s - EDITION_OFFSET


def season_of(edition: int) -> int:
    """The season END year of an ACB ``editionId`` (91 -> 2027)."""
    return int(edition) + EDITION_OFFSET


def _int(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().lstrip("+").isdigit():
        raise ValueError(f"{what} must be a non-negative integer; got {value!r}")
    return int(str(value).strip())


# ----------------------------------------------------------------------------------------- routes
# route -> (required params, rows path alternatives (all empty -> NoDataError), match must be finished,
#           scope checks: (body path, request key) compared type-strictly when the request key is given)
_SCOPE_FILTERS = (
    ("selectedFilters.competition", "competitionId"),
    ("selectedFilters.season", "editionId"),
)
_ROUTES: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...], bool, Tuple[Tuple[str, str], ...]]] = {
    "/matchdata/MatchHeader/match-header": (("matchId",), ("quarterScores",), False, (("matchId", "matchId"),)),
    "/matchdata/Result/boxscores": (("matchId",), ("teamBoxscores",), True, ()),
    "/matchdata/PlayByPlay/play-by-play": (("matchId",), ("plays",), True, ()),
    "/matchdata/MatchShots/match-shots": (("matchId",), ("shotPoints",), True, ()),
    "/matchdata/Overview/lead-tracker": (("matchId",), ("leadTrackerInfo",), True, ()),
    "/matchdata/Overview/lineup": (("matchId",), ("teamLineUps",), True, ()),
    "/matchdata/Overview/match-leaders": (("matchId",), ("teamLeaders",), True, ()),
    "/matchdata/Overview/match-team-comparison": (("matchId",), ("statsPerQuarters",), True, ()),
    "/matchdata/Overview/serie": (("matchId",), ("matches",), False, ()),
    "/matchdata/Preview/quickfacts": (("matchId",), ("facts",), False, ()),
    "/matchdata/Preview/season-leaders": (("matchId",), ("teamLeaders",), False, ()),
    "/matchdata/AdvancedStats/match-advanced-stats": (("matchId",), ("homeAdvancedStats",), False, ()),
    "/matchdata/AdvancedStats/player-advanced-stats": (("matchId", "playerLicense"), ("fourFactors",), False, ()),
    "/matchdata/Menu/matchlist": (("matchId",), ("matches",), False, ()),
    "/matchdata/Menu/competition-data": ((), ("competitions",), False, ()),
    "/seasondata/Competition/matches": (
        (),
        ("matches",),
        False,
        _SCOPE_FILTERS
        + (
            ("selectedFilters.round", "roundId"),
            ("selectedFilters.week", "weekId"),
        ),
    ),
    "/seasondata/Competition/standings": (
        (),
        ("standings",),
        False,
        _SCOPE_FILTERS + (("selectedFilters.round", "roundId"),),
    ),
    "/seasondata/Competition/bracket": (
        (),
        ("data.quarterFinals.series", "data.semifinals.series", "data.final.series"),
        False,
        _SCOPE_FILTERS,
    ),
    "/seasondata/Calendar/season-calendar": ((), ("rounds",), False, _SCOPE_FILTERS),
    "/seasondata/ShotChart/league-zones": (
        (),
        ("zones",),
        False,
        (
            ("competitionId", "competitionId"),
            ("editionId", "editionId"),
        ),
    ),
    "/seasondata/TeamProfile/teams": (
        (),
        ("teams",),
        False,
        _SCOPE_FILTERS + (("teams[].selected.editionId", "editionId"),),
    ),
    "/seasondata/TeamProfile/team-matches": (
        ("clubId",),
        ("matches",),
        False,
        (("matches[].homeTeam.editionId", "editionId"),),
    ),
    "/seasondata/TeamProfile/next-matches": (("clubId",), ("matches",), False, ()),
    "/seasondata/TeamProfile/club-data": (("clubId",), ("clubData",), False, (("clubData.team.clubId", "clubId"),)),
    "/seasondata/TeamProfile/current-roster": (
        ("clubId",),
        ("players",),
        False,
        (("players[].player.editionId", "editionId"),),
    ),
    "/seasondata/TeamProfile/team-leaders": (("clubId", "numberOfPlayers"), ("leaders",), False, _SCOPE_FILTERS),
    "/seasondata/TeamProfile/team-stats": (
        ("clubId",),
        ("players",),
        False,
        (
            ("team.selected.clubId", "clubId"),
            ("team.selected.editionId", "editionId"),
        ),
    ),
    "/seasondata/TeamProfile/team-records": (("clubId",), (), False, ()),
    "/seasondata/PlayerProfile/player-data": (
        ("playerLicenceId",),
        ("playerData",),
        False,
        (("playerData.player.id", "playerLicenceId"),),
    ),
    "/seasondata/PlayerProfile/player-stats": (
        ("playerLicenceId",),
        ("statsByTeam",),
        False,
        (("playerTotals.player.id", "playerLicenceId"),),
    ),
    "/seasondata/PlayerProfile/career": (("playerLicenceId",), ("stints",), False, ()),
    "/seasondata/PlayerProfile/matches": (
        ("playerLicenceId",),
        ("matches",),
        False,
        (("matches[].homeTeam.editionId", "editionId"),),
    ),
    "/seasondata/Search/search": (("searchTerm",), ("players", "teams", "coaches"), False, ()),
}
#: Every query key a route accepts (the required ones plus these optional ones).
_OPTIONAL = {
    "/matchdata/Menu/matchlist": ("roundId", "weekId"),
    "/matchdata/Menu/competition-data": ("competitionIds",),
    "/seasondata/Competition/matches": ("competitionId", "season", "roundId", "weekId", "isRoundSelected"),
    "/seasondata/Competition/standings": ("competitionId", "season", "roundId"),
    "/seasondata/Competition/bracket": ("competitionId", "season"),
    "/seasondata/Calendar/season-calendar": ("competitionId", "season"),
    "/seasondata/ShotChart/league-zones": ("competitionId", "season"),
    "/seasondata/TeamProfile/teams": ("competitionId", "season"),
    "/seasondata/TeamProfile/team-matches": ("competitionId", "season"),
    "/seasondata/TeamProfile/next-matches": ("numberOfMatches", "competitionId", "season"),
    "/seasondata/TeamProfile/current-roster": ("competitionId", "season"),
    "/seasondata/TeamProfile/team-leaders": ("competitionId", "season"),
    "/seasondata/TeamProfile/team-stats": ("competitionId", "season"),
    "/seasondata/PlayerProfile/player-stats": ("competitionId", "season"),
    "/seasondata/PlayerProfile/matches": ("season",),
    "/seasondata/Search/search": ("limit",),
}
_INT_KEYS = frozenset(
    {
        "matchId",
        "playerLicense",
        "playerLicenceId",
        "clubId",
        "competitionId",
        "roundId",
        "weekId",
        "numberOfMatches",
        "numberOfPlayers",
        "limit",
        "season",
    }
)


# --------------------------------------------------------------------------------------- transport
def _fetch(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries 429 / 5xx up to :data:`ATTEMPTS` times with a ``Retry-After``-aware delay
    and raises ``NoDataError`` on a 404. A connection failure that outlives the retries becomes an
    ``AssetFetchError`` naming only the exception class (never the query or a header).
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
        raise AssetFetchError(
            f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}"
        ) from None


def _read_key_from_bundle() -> Optional[str]:
    """The key in the live acb.com bundle, or ``None`` when it cannot be read (any failure is the same answer)."""
    try:
        home = _fetch(SITE)
        if getattr(home, "status_code", None) != 200:
            return None
        seen: List[str] = []
        for chunk in _CHUNK_RE.findall(home.text or ""):
            if chunk not in seen:
                seen.append(chunk)
        for chunk in seen[:MAX_CHUNKS]:
            js = _fetch(SITE.rstrip("/") + chunk)
            m = _KEY_RE.search(js.text or "") if getattr(js, "status_code", None) == 200 else None
            if m:
                return m.group(1)
    except Exception:  # noqa: BLE001 - an unreadable bundle is "no key found"
        return None
    return None


def _api_key(*, renew: bool = False) -> str:
    """``$ACB_API_KEY``, else the cached key, else :data:`DEFAULT_KEY`; ``renew`` re-reads the bundle."""
    env = os.environ.get(KEY_ENV, "").strip()
    if env:
        return env
    with _key_lock:
        if renew:
            _key["value"] = _read_key_from_bundle() or _key["value"] or DEFAULT_KEY
        elif _key["value"] is None:
            _key["value"] = DEFAULT_KEY
        return str(_key["value"])


def _check_status(url: str, resp: Any) -> None:
    status = getattr(resp, "status_code", None)
    if status == 200:
        return
    where = _rt._where(url)
    if status in (204, 404):
        raise NoDataError(f"{where} answered HTTP {status}: nothing for this request")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    raise AssetFetchError(f"{where} answered HTTP {status}, not 200")


# --------------------------------------------------------------------------------------- body checks
class _Missing(Exception):
    pass


_PART = re.compile(r"^([^\[\]]*)(\[\])?$")


def _pluck(body: Any, path: str) -> List[Any]:
    """Every value at the dotted ``path`` (``[]`` = each list element). Raises :class:`_Missing`."""
    vals = [body]
    for part in path.split("."):
        m = _PART.match(part)
        if not m:
            raise ValueError(f"bad path {path!r}")
        key, each = m.group(1), m.group(2)
        nxt: List[Any] = []
        for v in vals:
            if not isinstance(v, dict) or key not in v:
                raise _Missing(path)
            x = v[key]
            if each:
                if not isinstance(x, list) or not x:
                    raise _Missing(path)
                nxt.extend(x)
            else:
                nxt.append(x)
        vals = nxt
    return vals


def _same(a: Any, b: Any) -> bool:
    """Type-strict equality: ``1`` is not ``True`` and ``"1"`` is not ``1``."""
    return type(a) is type(b) and a == b


def _all_empty(value: Any, depth: int = 0) -> bool:
    if value is None or (isinstance(value, str) and not value.strip()):
        return True
    if isinstance(value, dict):
        return depth > 50 or all(_all_empty(v, depth + 1) for v in value.values())
    if isinstance(value, list):
        return depth > 50 or all(_all_empty(v, depth + 1) for v in value)
    return False


def _fold(text: Any) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", str(text or "")) if not unicodedata.combining(c)).lower()


def _involves(row: Any, club: int, home: str, away: str) -> bool:
    def at(path: str) -> Any:
        cur = row
        for k in path.split("."):
            cur = cur.get(k) if isinstance(cur, dict) else None
        return cur

    return _same(at(home), club) or _same(at(away), club)


def _validate(path: str, body: Any, q: Dict[str, Any], where: str) -> None:
    """Refuse everything that is not this route's answer. Raises ``AssetFetchError`` / ``NoDataError``."""
    _req, rows, finished, scope = _ROUTES[path]
    if not isinstance(body, dict):
        raise AssetFetchError(f"{where}: the body is not an object")
    if body and set(body) <= _ERROR_KEYS:
        raise AssetFetchError(f"{where}: the body is an error envelope")
    if rows:
        vals = [_at(body, r) for r in rows]
        if all(v is _ABSENT for v in vals):
            raise AssetFetchError(f"{where}: not this route's shape ({' / '.join(rows)} missing)")
        if not any(v not in (_ABSENT, None, [], {}) for v in vals):
            raise NoDataError(f"{where}: the answer holds no rows ({' / '.join(rows)} is empty)")
    if _all_empty(body):
        raise AssetFetchError(f"{where}: the body is an empty stub")
    if finished and body.get("matchFinished") is False:
        raise AssetFetchError(f"{where}: the match is not finished (matchFinished is false); live data is partial")
    for bpath, key in scope:
        if q.get(key) is None:
            continue
        try:
            vals = _pluck(body, bpath)
        except _Missing:
            raise AssetFetchError(f"{where}: out of scope: {bpath} missing, asked for {key}={q[key]!r}") from None
        bad = [v for v in vals if not _same(v, q[key])]
        if bad:
            raise AssetFetchError(f"{where}: out of scope: {bpath}={bad[0]!r}, asked for {key}={q[key]!r}")
    if path == "/seasondata/TeamProfile/team-matches":
        _rows_involve(body, "matches", q, where, "homeTeam.clubId", "awayTeam.clubId")
    elif path == "/seasondata/TeamProfile/next-matches":
        _rows_involve(body, "matches", q, where, "homeClubId", "awayClubId")
    elif path == "/seasondata/TeamProfile/team-records":
        for stat, rec in body.items():
            if isinstance(rec, dict) and not _involves(rec, q["clubId"], "localTeam.clubId", "visitorTeam.clubId"):
                raise AssetFetchError(
                    f"{where}: out of scope: record {stat!r} is from a game without club {q['clubId']}"
                )
    elif path == "/seasondata/Search/search":
        term = _fold(q["searchTerm"])
        for r in body.get("players") or []:
            names = " ".join(_fold(r.get(k)) for k in ("nickname", "lastName", "firstInitialAndLastName", "firstName"))
            if isinstance(r, dict) and term not in names:
                raise AssetFetchError(f"{where}: out of scope: a player row does not match the search term")
    elif path == "/matchdata/Menu/competition-data" and q.get("competitionIds") is not None:
        ids: set = {
            int(c["id"]) for c in body.get("competitions") or [] if isinstance(c, dict) and isinstance(c.get("id"), int)
        }
        wanted = set(q["competitionIds"])
        if not wanted <= ids:
            raise AssetFetchError(f"{where}: out of scope: competitions {sorted(ids)} lack the asked {sorted(wanted)}")


_ABSENT = object()


def _at(body: Any, path: str) -> Any:
    """The value at the dotted ``path`` (no list steps), or :data:`_ABSENT` when any key is missing."""
    cur = body
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return _ABSENT
        cur = cur[k]
    return cur


def _rows_involve(body: Dict[str, Any], key: str, q: Dict[str, Any], where: str, home: str, away: str) -> None:
    for r in body.get(key) or []:
        if not (isinstance(r, dict) and _involves(r, q["clubId"], home, away)):
            raise AssetFetchError(f"{where}: out of scope: a match without club {q['clubId']}")


# --------------------------------------------------------------------------------------- the getter
def _normalise(path: str, params: Optional[dict]) -> Dict[str, Any]:
    """Validate and convert the caller's params to the wire query (``season`` becomes ``editionId``)."""
    required, _rows, _fin, _scope = _ROUTES[path]
    allowed = set(required) | set(_OPTIONAL.get(path, ()))
    given = {k: v for k, v in (params or {}).items() if v is not None}
    unknown = sorted(set(given) - allowed)
    if unknown:
        raise ValueError(f"unsupported parameter(s) for {path}: {unknown}")
    missing = [k for k in required if k not in given]
    if missing:
        raise ValueError(f"{path} needs {missing}")
    q: Dict[str, Any] = {}
    for k, v in given.items():
        if k == "season":
            q["editionId"] = edition_id(v)
        elif k in _INT_KEYS:
            q[k] = _int(v, k)
        elif k == "isRoundSelected":
            if not isinstance(v, bool):
                raise ValueError(f"isRoundSelected must be a bool; got {v!r}")
            q[k] = "true" if v else "false"
        elif k == "competitionIds":
            ids = [v] if isinstance(v, (int, str)) else list(v) if isinstance(v, (list, tuple)) and v else None
            if ids is None:
                raise ValueError(f"competitionIds must be an integer or a list of integers; got {v!r}")
            q[k] = [_int(x, "competitionIds") for x in ids]
        elif k == "searchTerm":
            if not isinstance(v, str) or not v.strip():
                raise ValueError("searchTerm must be a non-blank string")
            q[k] = v.strip()
        else:  # unreachable: _OPTIONAL and _INT_KEYS name the same keys
            raise ValueError(f"unsupported parameter {k!r}")
    return q


def _stamp(q: Dict[str, Any]) -> Dict[str, Any]:
    """The request as the parsers read it (snake_case; ``season`` is the END year)."""
    names = {
        "matchId": "match_id",
        "playerLicense": "player_id",
        "playerLicenceId": "player_id",
        "clubId": "club_id",
        "competitionId": "competition_id",
        "roundId": "round_id",
        "weekId": "week_id",
        "searchTerm": "search_term",
    }
    out: Dict[str, Any] = {names[k]: v for k, v in q.items() if k in names}
    if "editionId" in q:
        out["edition_id"] = q["editionId"]
        out["season"] = season_of(q["editionId"])
    return out


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET an ACB route, refuse every wrong answer, return the body with the request stamped.

    Args:
        url: the URL a generated wrapper built (``api2.acb.com/api/{matchdata|seasondata}/...``).
        params: the wrapper's query: ids as integers, ``season`` the END year (mapped to ``editionId``),
            ``isRoundSelected`` a bool. ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The parsed JSON body (a ``dict``) with ``_request`` = the asked ids in snake_case (``match_id``,
        ``club_id``, ``player_id``, ``competition_id``, ``round_id``, ``week_id``, ``edition_id``, ``season``).

    Raises:
        NoDataError: HTTP 204 / 404, or a route with no rows (see the module docstring).
        ValueError: a missing, unknown or non-integer argument, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for an ACB request: {sorted(kwargs)}")
    parts = urlsplit(url)
    base = urlsplit(API)
    path = (
        parts.path[len(base.path) :] if parts.netloc == base.netloc and parts.path.startswith(base.path + "/") else ""
    )
    if path not in _ROUTES:
        raise ValueError(f"not an ACB route: {_rt._where(url)}")
    q = _normalise(path, params)
    where = _rt._where(url)
    resp: Any = None
    for renew in (False, True):  # one key re-read when the API rejects the key
        resp = _fetch(url, q, {"X-APIKEY": _api_key(renew=renew), "Accept": "application/json"})
        if getattr(resp, "status_code", None) != 401 or os.environ.get(KEY_ENV, "").strip():
            break
    if getattr(resp, "status_code", None) == 401:
        raise AssetFetchError(f"{where} rejected the API key (HTTP 401) after a re-read of the bundle")
    _check_status(url, resp)
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    if "json" not in ctype or "problem+json" in ctype:
        raise AssetFetchError(f"{where} answered {ctype or 'no content type'}, not JSON")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise AssetFetchError(f"{where} answered HTTP 200 with an empty body")
    try:
        body = json.loads(text)
    except ValueError:
        raise AssetFetchError(f"{where} answered a body that is not JSON") from None
    _validate(path, body, q, where)
    body[REQUEST_KEY] = _stamp(q)
    return body
