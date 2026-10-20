"""Runtime for the generated ``vtb_infobasket`` wrappers (Infobasket, ``org.infobasket.su``).

Infobasket is the stats platform of the Russian basketball federation and serves the VTB United League's game, team and
player widgets. Every request goes through the shared :data:`sportsdataverse.basketball.basketball_runtime.SESSION`, which
waits at least one second before **every** attempt, retries included (``dl_utils.download`` honours ``Retry-After`` on
429 / 503). The host needs no key, cookie or proxy (sdv-internal-refs ``vtb/README.md``: direct from the droplet at 1
request per second, no throttling), so no proxy variable is read. ``org.infobasket.su/robots.txt`` is a 404 (no rules).

**Owner flag.** The league and the platform are run from Russia and several clubs are owned or sponsored by sanctioned
state companies (the KHL precedent). Fetching on the user's own machine is fine; a published data release, hosted mirror
or redistribution needs owner sign-off first.

What the live host does (sdv-internal-refs ``vtb/``, probed 2026-10-12):

* **Season is the END year at the public API** (2026 = 2025-26). Infobasket itself takes a competition id everywhere:
  ``/Comp/GetSeasonsForTag?tag=vtb`` maps ``SeasonYear`` (the END year) to the season-level ``CompID`` (2026 -> 50714).
  Season-level wrappers (``calendar``, ``team_stats``, ``best_players``, ``best_teams``, ``comp_issue``, the team routes with
  ``season=``) take ``season`` and ``_get`` maps it with that table (one extra request per tag, cached for the process; an
  END year the table lacks is ``ValueError`` naming the years it has). Stage-level wrappers (``standings``, ``cross_table``,
  ``playoff``) take the **stage** ``comp_id`` read from ``vtb_infobasket_comp_issue``: ``CompID`` means three things (a
  season, a stage below it, and ``Season.CompID`` in the player season stats, which is an END year).
* **Errors are HTTP 200.** An unknown id or a wrong stage type answers ``{"MessageType": n, "Header": ..., "Message": ...}``
  and an unknown game on ``GetOnline`` answers the JSON string ``"BadRequest"``. Both are ``AssetFetchError`` (an error
  envelope is a failed fetch, not "nothing here"). A route that answers an empty list (an unknown competition or tag) is
  ``NoDataError``.
* **Scope.** A body must be the one asked for, else ``AssetFetchError`` (never an empty frame), compared type-strictly: the
  game id (``Online.GameID``; ``GetOnline`` does not echo it at the top level and ``OnlineTeams[].GameID`` is another
  protocol id), the person, team or competition echoed in the body, every calendar ``GameDate`` inside the season, and a
  player game log's side against the requested team. Routes whose body echoes nothing to compare (the season list, the
  leader lists and code tables, ``GetTeamStats``, ``GetTeamSeasons``) are not scoped, and the ``tag`` / ``compId`` of the
  person and team routes cannot be checked either (``vtb/README.md``, "Scope checks").
* **Games must be final.** ``GetOnline`` and ``GamePage`` need ``GameStatus`` 1 (completed); a planned (0) or live (2) game is
  ``AssetFetchError``, and so is a ``GetOnline`` stub (fewer than two ``GameTeams``).
* **Pages.** ``GetTeamStats`` is paged (``Count``, ``Page``, ``Pages``); a body that is not the whole single page is
  ``AssetFetchError``.
* **Personal data.** :func:`_scrub` removes federation licence numbers, contact and operator-account fields for everyone,
  and birth dates, birth years and ages for coaches, referees and other officials, from every body before it is returned
  (so also from ``return_parsed=False``). Players' public bio (birth date, height, weight, nationality) is kept.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Mapping, NamedTuple, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse._codegen_runtime import _where
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

BASE = "https://org.infobasket.su"

#: Key the request is stamped under (see :func:`_get`).
REQUEST_KEY = "_request"
#: The competition tag every wrapper defaults to (``vtbyouth``, ``wbc``, ``wpremier`` and ``rfb`` also answer).
DEFAULT_TAG = "vtb"

#: Verified 2026-10-12 against ``/Comp/GetSeasonsForTag?tag=vtb`` (END year -> season CompID). The runtime reads the live
#: table; this copy is for tests and documentation.
SEASON_COMP_IDS: Dict[int, int] = {
    2010: 5052, 2011: 6076, 2012: 7050, 2013: 8187, 2014: 9113, 2015: 10050, 2016: 11046, 2017: 14009,
    2018: 17018, 2019: 30051, 2020: 32984, 2021: 34829, 2022: 37492, 2023: 39799, 2024: 42729, 2025: 46399,
    2026: 50714, 2027: 55613,
}  # fmt: skip

#: Season table per tag (``{END year: CompID}``), cached for the process; ``clear_cache()`` empties it.
_CACHE: Dict[str, Dict[int, int]] = {}


def clear_cache() -> None:
    """Forget the cached season tables."""
    _CACHE.clear()


# --- personal data ----------------------------------------------------------------------------------------------------

_PERSONAL = frozenset({"PersonBirth", "PersonBirthday", "PersonBirthYear", "Age"})
_EVERYONE = frozenset(
    {"LicenceNumber", "PersonContacts", "PersonEmail", "PersonPhone", "Email", "Phone", "SysUser", "SysMyUser"}
)
_NON_PLAYER_KEY = re.compile(r"(?i)coach|referee|official|commissioner|staff")
_PLAYER_START_TYPE = 1  # OnlineStarts.StartType: 0 team, 1 player, 3 coach, 4 official, 5 placeholder
_BETTING = frozenset({"bet"})


def _scrub(body: Any, whole_body_is_coach: bool = False) -> Any:
    """``body`` without contact data, licence numbers and betting blocks, and without coaches' / officials' birth data.

    Args:
        body: a parsed Infobasket body.
        whole_body_is_coach: ``True`` for ``/Widget/CoachPage``, whose whole body is a coach record.

    Returns:
        A copy of ``body``. Keys in ``_EVERYONE`` go wherever they sit; keys in ``_PERSONAL`` go under a coach, referee,
        official, commissioner or staff subtree, in an ``OnlineStarts`` row whose ``StartType`` is not a player's, and
        everywhere in a coach page. Players' birth dates stay.
    """

    def walk(node: Any, non_player: bool) -> Any:
        if isinstance(node, list):
            return [walk(x, non_player) for x in node]
        if not isinstance(node, dict):
            return node
        if "StartType" in node and node.get("StartType") != _PLAYER_START_TYPE:
            non_player = True
        return {
            k: walk(v, non_player or bool(_NON_PLAYER_KEY.search(k)))
            for k, v in node.items()
            if k not in _EVERYONE and k not in _BETTING and not (non_player and k in _PERSONAL)
        }

    return walk(body, whole_body_is_coach)


# --- routes -------------------------------------------------------------------------------------------------------------


class _Route(NamedTuple):
    template: str
    pattern: "re.Pattern[str]"
    token: Optional[str]  # the path token name, if any
    #: where the season goes on the wire: ``path`` (the token is an END year to map), a query key, or ``None``
    season: Optional[str]
    kind: str  # ``list`` or ``dict``: the body type


def _r(template: str, season: Optional[str] = None, kind: str = "dict") -> _Route:
    m = re.search(r"\{([a-z_]+)\}", template)
    regex = "^" + re.sub(r"\\\{[a-z_]+\\\}", r"(\\d+)", re.escape(template)).rstrip("/") + "/?$"
    return _Route(template, re.compile(regex), m.group(1) if m else None, season, kind)


_ROUTES: List[_Route] = [
    _r("/Comp/GetSeasonsForTag", kind="list"),
    _r("/Comp/GetCalendar/", season="comps", kind="list"),
    _r("/Comp/GetTeamStats/{season}", season="path"),
    _r("/Comp/GetBestPlayers/{season}", season="path", kind="list"),
    _r("/Comp/GetBestTeams/{season}", season="path", kind="list"),
    _r("/Comp/GetTeamStatsForPreview/{game_id}", season="compId", kind="list"),
    _r("/Widget/GetOnline/{game_id}"),
    _r("/Widget/GamePage/{game_id}"),
    _r("/Widget/CompIssue/{season}", season="path"),
    _r("/Widget/CompTeamResults/{comp_id}", kind="list"),
    _r("/Widget/CrossTable/{comp_id}", kind="list"),
    _r("/Widget/PlayOff/{comp_id}", kind="list"),
    _r("/Widget/GetBestPlayerParameters/", kind="list"),
    _r("/Widget/GetBestTeamParameters/", kind="list"),
    _r("/Widget/PlayerPage/{person_id}"),
    _r("/Widget/PlayerSeasonStats/{person_id}"),
    _r("/Widget/PlayerStats/{person_id}", season="compId"),
    _r("/Widget/CoachPage/{person_id}"),
    _r("/Widget/TeamPage/{team_id}"),
    _r("/Widget/GetTeamSeasons/{team_id}", kind="list"),
    _r("/Widget/GetTeamComps/{team_id}", season="season", kind="list"),
    _r("/Widget/TeamRoster/{team_id}", season="compId"),
    _r("/Widget/TeamGames/{team_id}", season="compId", kind="list"),
    _r("/Widget/TeamStats/{team_id}", season="compId"),
]


#: Routes whose wire query takes ``tag`` (the others get it only as the key of the season table).
_TAG_ROUTES = frozenset(
    {
        "/Comp/GetSeasonsForTag",
        "/Widget/PlayerPage/{person_id}",
        "/Widget/PlayerSeasonStats/{person_id}",
        "/Widget/PlayerStats/{person_id}",
        "/Widget/TeamPage/{team_id}",
        "/Widget/GetTeamSeasons/{team_id}",
        "/Widget/GetTeamComps/{team_id}",
        "/Widget/TeamRoster/{team_id}",
        "/Widget/TeamGames/{team_id}",
        "/Widget/TeamStats/{team_id}",
    }
)


def _route(url: str) -> Tuple[_Route, Dict[str, int]]:
    """The route of ``url`` and its path id; ``ValueError`` for a path the family does not wrap."""
    path = urlsplit(url).path
    for route in _ROUTES:
        m = route.pattern.match(path)
        if m:
            return route, ({route.token: int(m.group(1))} if route.token else {})
    raise ValueError(f"not a VTB Infobasket route: {path!r}")


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality (``True == 1`` in Python, so ``id: true`` must not pass for id 1)."""
    return type(got) is type(want) and got == want


def _rows(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _int_arg(value: Any, name: str) -> int:
    if isinstance(value, bool) or not str(value).strip().lstrip("-").isdigit():
        raise ValueError(f"{name} must be an integer; got {value!r}")
    return int(value)


def _year(ddmmyyyy: Any) -> Optional[int]:
    m = re.fullmatch(r"\d\d\.\d\d\.(\d{4})", ddmmyyyy) if isinstance(ddmmyyyy, str) else None
    return int(m.group(1)) if m else None


# --- fetching -----------------------------------------------------------------------------------------------------------


def _fetch(url: str, params: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
    """GET ``url`` through the polite session; an Infobasket error answer is ``AssetFetchError``."""
    kwargs.setdefault("session", _bb.SESSION)
    # a 400 / 422 stays ValueError: the request is wrong (EX-SYNC-b, the repo-wide vocabulary)
    body = _bb._fetch_json(url, dict(params or {}), **kwargs)
    where = _where(url)
    if isinstance(body, dict) and {"MessageType", "Message"} <= set(body):
        raise AssetFetchError(f"{where}: Infobasket error envelope: {str(body.get('Message'))[:80]!r}")
    if isinstance(body, str):
        raise AssetFetchError(f"{where}: answered the string {body[:40]!r}, not data")
    if not isinstance(body, (dict, list)):
        raise AssetFetchError(f"{where}: answered a {type(body).__name__}, not data")
    return body


def season_table(tag: str = DEFAULT_TAG, **kwargs: Any) -> Dict[int, int]:
    """``{END year: season CompID}`` of a competition tag, from ``/Comp/GetSeasonsForTag`` (cached for the process).

    Args:
        tag: the competition tag (``vtb``, ``vtbyouth``, ``wbc``, ``wpremier``, ``rfb``).
        **kwargs: forwarded to the transport.

    Returns:
        The season table.

    Raises:
        NoDataError: the tag answers no seasons (an unknown tag answers ``[]``).
        AssetFetchError: the fetch failed or a row is not ``{SeasonYear, CompID}``.
    """
    if tag not in _CACHE:
        url = f"{BASE}/Comp/GetSeasonsForTag"
        rows = _fetch(url, {"tag": tag, "format": "json"}, **kwargs)
        if not isinstance(rows, list):
            raise AssetFetchError(f"{_where(url)}: no season list")
        if not rows:
            raise NoDataError(f"{_where(url)}: tag {tag!r} has no seasons")
        table: Dict[int, int] = {}
        for r in _rows(rows):
            if not (isinstance(r.get("SeasonYear"), int) and isinstance(r.get("CompID"), int)):
                raise AssetFetchError(f"{_where(url)}: a season row without integer SeasonYear and CompID")
            table[r["SeasonYear"]] = r["CompID"]
        _CACHE[tag] = table
    return _CACHE[tag]


def season_comp_id(season: Any, tag: str = DEFAULT_TAG, **kwargs: Any) -> int:
    """The season-level CompID of an END year.

    Args:
        season: the sdv season, an END year (2026 = 2025-26).
        tag: the competition tag.
        **kwargs: forwarded to the transport.

    Returns:
        The CompID (50714 for 2026).

    Raises:
        ValueError: ``season`` is not an integer, or the tag has no such season.
    """
    end = _int_arg(season, "season")
    table = season_table(tag, **kwargs)
    if end not in table:
        raise ValueError(f"season must be one of {sorted(table)} for tag {tag!r} (an END year); got {end}")
    return table[end]


# --- checks -------------------------------------------------------------------------------------------------------------


def _check(
    route: _Route,
    ids: Mapping[str, int],
    params: Mapping[str, Any],
    comp: Optional[int],
    season: Optional[int],
    body: Any,
) -> None:
    """Raise ``NoDataError`` for a row-less body, ``AssetFetchError`` for a partial, non-final or out-of-scope one."""
    where = f"org.infobasket.su{route.template}"
    t = route.template
    if route.kind == "list":
        if not isinstance(body, list):
            raise AssetFetchError(f"{where}: expected a list, got {type(body).__name__}")
        if not body:
            raise NoDataError(f"{where}: the host answered no rows")
    elif not isinstance(body, dict):
        raise AssetFetchError(f"{where}: expected an object, got {type(body).__name__}")
    if t == "/Comp/GetCalendar/":
        _all(
            where,
            body,
            lambda r: _year(r.get("GameDate")) in (season - 1, season) if season else True,
            "another season",
        )
    elif t == "/Comp/GetTeamStatsForPreview/{game_id}":
        if len(body) != 2 or not _all_int(body, "TeamID"):
            raise AssetFetchError(f"{where}: expected the two teams of game {ids['game_id']}")
    elif t == "/Comp/GetTeamStats/{season}":
        stats = body.get("Stats")
        if not (isinstance(stats, list) and _same(body.get("Pages"), 1) and len(stats) == body.get("Count")):
            raise AssetFetchError(f"{where}: partial page (Pages {body.get('Pages')!r}, Count {body.get('Count')!r})")
        if not stats:
            raise NoDataError(f"{where}: the season has no team totals")
    elif t == "/Widget/GetOnline/{game_id}":
        online = body.get("Online")
        if not (isinstance(online, dict) and _same(online.get("GameID"), ids["game_id"])):
            raise AssetFetchError(f"{where}: asked for game {ids['game_id']}, the body is another game or a stub")
        if not (_same(body.get("GameStatus"), 1) and _same(body.get("IsOnline"), False)):
            raise AssetFetchError(
                f"{where}: game {ids['game_id']} is not final (GameStatus {body.get('GameStatus')!r}, "
                f"IsOnline {body.get('IsOnline')!r})"
            )
        teams = _rows(body.get("GameTeams"))
        if len(teams) != 2 or not _all_int(teams, "TeamID"):
            raise AssetFetchError(f"{where}: game {ids['game_id']} has no two-team box score (a stub body)")
    elif t == "/Widget/GamePage/{game_id}":
        if not _same(body.get("GameID"), ids["game_id"]):
            raise AssetFetchError(f"{where}: asked for game {ids['game_id']}, got {body.get('GameID')!r}")
        if not _same(body.get("GameStatus"), 1):
            raise AssetFetchError(
                f"{where}: game {ids['game_id']} is not final (GameStatus {body.get('GameStatus')!r})"
            )
    elif t == "/Widget/CompIssue/{season}":
        if not (
            _same(body.get("ParentCompID"), comp)
            and _all(
                where,
                _rows(body.get("Comps")),
                lambda r: _same(r.get("CompIDparent"), comp),
                "a stage of another competition",
            )
        ):
            raise AssetFetchError(f"{where}: the stage tree is not the one of competition {comp}")
    elif t == "/Widget/CompTeamResults/{comp_id}":
        _all(where, body, lambda r: _same(r.get("CompID"), ids["comp_id"]), "another stage")
    elif t == "/Widget/CrossTable/{comp_id}":
        _all(where, body, lambda r: isinstance(r.get("GameID"), int), "a non-game row")
    elif t == "/Widget/PlayOff/{comp_id}":
        _all(where, body, lambda r: isinstance(r.get("Scores"), list), "a non-series row")
    elif "{person_id}" in t:
        if not _same(body.get("PersonID"), ids["person_id"]):
            raise AssetFetchError(f"{where}: asked for person {ids['person_id']}, got {body.get('PersonID')!r}")
        if t == "/Widget/PlayerStats/{person_id}":
            team = params.get("team")
            if team is not None:
                _all(where, _rows(body.get("GameStats")), lambda r: _side_is(r, team), "another team")
    elif t in ("/Widget/TeamPage/{team_id}", "/Widget/TeamRoster/{team_id}", "/Widget/TeamStats/{team_id}"):
        if not _same(body.get("TeamID"), ids["team_id"]):
            raise AssetFetchError(f"{where}: asked for team {ids['team_id']}, got {body.get('TeamID')!r}")
    elif t == "/Widget/GetTeamComps/{team_id}":
        _all(
            where, body, lambda r: _same(r.get("CompID"), comp) or _same(r.get("ParentCompID"), comp), "another season"
        )
    elif t == "/Widget/TeamGames/{team_id}":
        team = ids["team_id"]
        _all(where, body, lambda r: _same(r.get("TeamAid"), team) or _same(r.get("TeamBid"), team), "another team")


def _all_int(rows: Any, key: str) -> bool:
    return all(isinstance(r, dict) and isinstance(r.get(key), int) and not isinstance(r.get(key), bool) for r in rows)


def _all(where: str, rows: Any, pred: Callable[[Mapping[str, Any]], bool], what: str) -> bool:
    rows = _rows(rows) if not isinstance(rows, list) else rows
    if not all(isinstance(r, dict) and pred(r) for r in rows):
        raise AssetFetchError(f"{where}: the body holds a row of {what}" if what else f"{where}: out-of-scope body")
    return True


def _side_is(game_row: Mapping[str, Any], team: Any) -> bool:
    teams = _rows(_rows([game_row.get("Game")])[0].get("GameTeams")) if isinstance(game_row.get("Game"), dict) else []
    want = int(team) if str(team).isdigit() else team
    return any(
        _same(t.get("GameTeamNumber"), game_row.get("TeamNumber")) and _same(t.get("TeamID"), want) for t in teams
    )


# --- the getter ---------------------------------------------------------------------------------------------------------


@_rt._no_content_is_empty
def _get(url: str, params: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Any:
    """GET one wrapped route; return its (scrubbed) body with the request stamped under ``_request``.

    Args:
        url: the full request URL built by the generated wrapper. A ``{season}`` path token holds an END year.
        params: query-string parameters. ``season`` (an END year) is mapped to the route's competition parameter
            (``comps`` on the calendar, ``compId`` elsewhere, ``season`` on the team competitions route); ``tag`` names the
            competition tag; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.basketball.basketball_runtime._get`.

    Returns:
        The body, a dict. A bare list body is wrapped as ``{"items": [...], "_request": {...}}``; the stamp holds the ids of
        the request (``game_id``, ``person_id``, ``team_id``, ``comp_id``), the END-year ``season``, its season-level
        ``season_comp_id`` and the ``tag`` where they apply.

    Raises:
        ValueError: ``url`` is not a route of this family, or ``season`` is not an END year the tag has.
        NoDataError: an empty list, a season without team totals.
        AssetFetchError: the fetch failed, the host answered an error envelope or string, a game is not final, the page is
            partial, or the body belongs to another game, person, team, competition or season than the one asked for.
    """
    route, ids = _route(url)
    p = {k: v for k, v in (params or {}).items() if v is not None}
    tag = str(p.get("tag", DEFAULT_TAG))
    season: Optional[int] = None
    comp: Optional[int] = None
    wire_url = url
    if route.season == "path":
        season = ids["season"]
        comp = season_comp_id(season, tag, **kwargs)
        wire_url = f"{BASE}{route.template.replace('{season}', str(comp))}"
    elif route.season is not None and "season" in p:
        season = _int_arg(p.pop("season"), "season")
        comp = season_comp_id(season, tag, **kwargs)
        p[route.season] = comp
    wire = {k: v for k, v in p.items() if k != "tag" or route.template in _TAG_ROUTES}
    body = _fetch(wire_url, wire, **kwargs)
    body = _scrub(body, whole_body_is_coach=route.template == "/Widget/CoachPage/{person_id}")
    _check(route, ids, p, comp, season, body)
    stamp: Dict[str, Any] = {**ids, "route": route.template}
    if season is not None:
        stamp.update(season=season, season_comp_id=comp)
    if "tag" in p:
        stamp["tag"] = tag
    if route.template.startswith("/Widget/PlayerStats") and "team" in p:
        stamp["team_id"] = _int_arg(p["team"], "team")
    if isinstance(body, list):
        return {"items": body, REQUEST_KEY: stamp}
    return {**body, REQUEST_KEY: stamp}
