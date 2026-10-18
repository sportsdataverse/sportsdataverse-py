"""Runtime for the generated FIBA wrappers (``fiba_gdap`` and ``fiba_gamepage``).

Two sources, one runtime, both measured live on 2026-10-12:

* **GDAP JSON** (``digital-api.fiba.basketball/hapi/<method>``). Every request carries the
  ``Ocp-Apim-Subscription-Key`` header. The key is PUBLIC: fiba.basketball ships it in every
  page as ``NEXT_CLIENT_APIM_SUBSCRIPTION_KEY``, so :func:`_api_key` reads it from the live page
  once per process and falls back to the documented value when the page is unreadable. It is
  never logged, quoted in an error or written anywhere. A 401 re-reads the key once.
  Errors are not uniform: a missing key is 401, an unknown method 404, a missing required
  parameter **500 with an empty body**, and an unknown id **200 with ``{}`` or ``[]``**.
* **Game pages** (``fiba.basketball/en/history/.../games/<gameId>-<A>-<B>``) requested with
  ``RSC: 1``: the answer is a ``text/x-component`` Next.js flight payload that holds the box
  score, play-by-play and shots, which GDAP does not serve. :func:`_page` resolves the URL from
  GDAP (the competition's marketing name gives ``histId`` and ``histSlug``; its schedule gives the
  two team codes), cuts the ``game`` / ``playByPlay`` / ``gameDetails`` / ``playersTeamA`` /
  ``playersTeamB`` objects out of the payload, resolves the flight tokens (any other ``$`` token
  fails closed) and returns them as one dict.

Refusals (each is a mocked-transport test):

* **NoDataError**: HTTP 404; GDAP ``{}`` / ``[]`` / ``null``; a game id GDAP does not know at
  all; a game the competition lists without its two teams (not drawn yet); a game page that carries
  only the header (pre-2006 games and games not yet played: no ``gameDetails``).
* **AssetFetchError**: 401 after a key refresh; HTTP 500 and every other non-2xx; an APIM error
  envelope (``{"statusCode", "message"}``); a ``{"Error": ...}`` object; a body for another
  game, competition, team, player, federation, family or season than asked (type-strict id
  comparison, a row without the field to check is refused, never skipped); a
  ``getgdapgamesbetweentwodates`` row outside ``[dateFrom, dateTo)`` on its local
  ``gameDateTime``; a game page asked under a competition that does not list it (the game exists
  in GDAP, so it belongs to another edition: a wrong-scope request); a game page that is not
  ``text/x-component``; the **not-found shell** (HTTP
  200, 120-150 KB, the UI's ``playByPlay`` config but no game header for the asked id); an
  unresolvable flight token; a game page of another game or competition; a game still live.

**Season key.** END year at the public API, and the same number goes to the provider: club
competitions are keyed by END year (BCL 2024-25 = 2025) and national events by event year
(World Cup 2023 = 2023), which is already the sdv key, so :func:`_season` validates and passes
it through. Qualifiers carry the TARGET tournament year, not the year they were played in
(the World Cup 2027 qualifiers played in November 2025 are season 2027); the wrappers keep that
tournament year.

**Personal data.** Officials' contact details (emails, phones, Skype handles, preferred
airport) and coaches' and officials' dates of birth are dropped from every body before it is
returned (:func:`_scrub`). Players' dates of birth are public and kept.

Every request, retries included, goes through
:data:`~sportsdataverse.basketball.basketball_runtime.SESSION`, so each attempt waits at least
one second after the host's last. ``dl_utils.download`` honours ``Retry-After`` on 429 / 503;
HTTP 500 is NOT retried (GDAP's 500 means a parameter is missing, which retrying cannot fix).
"""

from __future__ import annotations

import copy
import functools
import json
import re
import time
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION, _polite_wait
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: List[str] = []

HOST_GDAP = "digital-api.fiba.basketball"
GDAP_URL = f"https://{HOST_GDAP}/hapi"
SITE_URL = "https://www.fiba.basketball"
BCL_URL = "https://www.championsleague.basketball"
KEY_PAGE = f"{SITE_URL}/en"
KEY_HEADER = "Ocp-Apim-Subscription-Key"
#: The public key on 2026-10-12: only used when the live page cannot be read.
_FALLBACK_KEY = "898cd5e7389140028ecb42943c47eb74"
_KEY_RE = re.compile(r'NEXT_CLIENT_APIM_SUBSCRIPTION_KEY\\*"\s*:\s*\\*"([0-9a-f]{32})\\*"')
#: GDAP answers HTTP 500 for a missing parameter; do not retry it (the other statuses retry as usual).
_RETRY_STATUSES = frozenset({408, 429, 502, 503, 504})
_ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status", "statusCode"})
#: ``/en/games/<gameId>`` on fiba.basketball is the wrappers' route; the real URL is resolved at run time.
_PAGE_PATH = re.compile(r"^/en/games/(\d+)/?$")
_FIRST_SEASON = 1950


# ---------------------------------------------------------------------------
# argument transforms (the YAML ``transform:`` names)
# ---------------------------------------------------------------------------


def _int_id(value: Any) -> Optional[int]:
    """An id as ``int`` (an int or a digit string); ``None`` passes through.

    Raises:
        ValueError: ``value`` is a bool, a float (``123.0`` is never an id) or not a whole number.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"an id must be an integer; got {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise ValueError(f"an id must be an integer; got {value!r}")


def _season(season: Any) -> Optional[int]:
    """Validate a season year; FIBA's native key equals the sdv key, so nothing is mapped.

    Club competitions key a season by its END year, national-team events by the event year,
    qualifiers by the target tournament year (see the module docstring). ``None`` passes through.

    Raises:
        ValueError: ``season`` is not a four-digit year from 1950.
    """
    if season is None:
        return None
    year = _int_id(season)
    if year is None or year < _FIRST_SEASON or year > 2100:
        raise ValueError(
            f"season must be a year from {_FIRST_SEASON} (END year for clubs, event year for national teams)"
        )
    return year


def _window_date(value: Any) -> Optional[str]:
    """A ``YYYY-MM-DD`` date (or ``date`` / ``datetime`` / ISO string) as the API's ``YYYY-MM-DDT00:00:00Z``.

    GDAP compares the venue-local ``gameDateTime`` and ignores the ``Z``; ``dateTo`` is exclusive.

    Raises:
        ValueError: ``value`` is not a date.
    """
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        day = value.date() if isinstance(value, datetime) else value
        return f"{day.isoformat()}T00:00:00Z"
    if isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}", value.strip()):
        try:
            return f"{date.fromisoformat(value.strip()[:10]).isoformat()}T00:00:00Z"
        except ValueError:
            pass
    raise ValueError(f"a date must be YYYY-MM-DD; got {value!r}")


# ---------------------------------------------------------------------------
# personal data
# ---------------------------------------------------------------------------

_CONTACT_KEYS = frozenset(
    {"contactDetails", "email", "emailAddress", "phone", "mobile", "skype", "fax", "preferredAirportName"}
)
_STAFF_CONTAINERS = frozenset({"coaches", "nominations", "officials", "Officials", "staff"})
_STAFF_KEYS = frozenset({"dateOfBirth", "birthDate"})


def _scrub(obj: Any, staff: bool = False, root_staff: bool = False) -> Any:
    """``obj`` without contact details and without coaches' / officials' dates of birth.

    Contact keys go wherever they sit; ``dateOfBirth`` / ``birthDate`` go under a ``coaches``,
    ``nominations``, ``officials`` or ``staff`` container, and on every top-level row when
    ``root_staff`` (``getgdapcompetitionnominationsbyid`` returns officials). Players' dates of
    birth (rosters, awards) are public and stay.
    """
    if isinstance(obj, dict):
        return {
            k: _scrub(v, staff or k in _STAFF_CONTAINERS, False)
            for k, v in obj.items()
            if k not in _CONTACT_KEYS and not ((staff or root_staff) and k in _STAFF_KEYS)
        }
    if isinstance(obj, list):
        return [_scrub(x, staff, root_staff) for x in obj]
    return obj


# ---------------------------------------------------------------------------
# GDAP
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _api_key() -> str:
    """The subscription key from the live fiba.basketball page (cached); the documented public key otherwise."""
    try:
        with _rt._transport_errors(KEY_PAGE):
            resp = _rt.download(url=KEY_PAGE, session=SESSION, num_retries=2)
        match = _KEY_RE.search(_rt._text_body(resp, KEY_PAGE))
    except (AssetFetchError, NoDataError):
        match = None
    return match.group(1) if match else _FALLBACK_KEY


def _strict(a: Any, b: Any) -> bool:
    """Equality that also requires equal types (``True != 1``, ``"5" != 5``)."""
    return type(a) is type(b) and a == b


def _dig(obj: Any, path: Sequence[str]) -> Any:
    for key in path:
        obj = obj.get(key) if isinstance(obj, dict) else None
    return obj


#: method -> alternatives; a row passes when every ``(path, request param)`` of ONE alternative equals
#: the request's (int) value. Routes absent here carry no field to check and are unscoped:
#: rounds, team staff, national teams, medal count, ranking zones, the gateway's competition ids.
_SCOPES: Dict[str, List[List[Tuple[Tuple[str, ...], str]]]] = {
    "getgdapgamesbycompetitionid": [[(("competition", "competitionId"), "gdapCompetitionId")]],
    "getgdapgamebyid": [[(("gameId",), "gdapGameId")]],
    "getGdapCompetitionById": [[(("competitionId",), "gdapCompetitionId")]],
    "getgdapcompetitionteamsbycompetitionid": [[(("profile", "competitionId"), "gdapCompetitionId")]],
    "getgdapcompetitionfinalstandingbycompetitionid": [[(("competition", "competitionId"), "gdapCompetitionId")]],
    "getgdapcompetitionroundgamedaysbyid": [[(("competition", "competitionId"), "gdapCompetitionId")]],
    "getgdapcompetitionawardsbyid": [[(("competitionId",), "gdapCompetitionId")]],
    "getgdapcompetitionnominationsbyid": [[(("competitionLevelId",), "gdapCompetitionId")]],
    "getgdapcompetitionteamrosterbyteamid": [[(("teamId",), "gdapTeamId")]],
    "getgdapcompetitionteamlatestrosterbyteamid": [[(("teamId",), "gdapTeamId")]],
    "getgdapcompetitionteamstatisticsbyteamid": [[(("teamId",), "gdapTeamId")]],
    "getgdapgameswithleaderdetailsbyteamid": [
        [(("teamA", "teamId"), "gdapTeamId")],
        [(("teamB", "teamId"), "gdapTeamId")],
    ],
    "getgdapplayerinfobycompetitionid": [[(("playerId",), "gdapPlayerId"), (("competitionId",), "gdapCompetitionId")]],
    "getgdapplayercompetitionstatisticsbyplayerid": [
        [(("playerId",), "gdapPlayerId"), (("competitionId",), "gdapCompetitionId")]
    ],
    "getgdapplayergamestatisticsbyplayerid": [
        [(("playerId",), "gdapPlayerId"), (("competitionId",), "gdapCompetitionId")]
    ],
    "getgdapfederationgamesbyfederationid": [
        [(("teamA", "organisationId"), "gdapFederationId")],
        [(("teamB", "organisationId"), "gdapFederationId")],
    ],
    "getgdapcompetitionsbyseasonsandcompetitionmarketingname": [
        [(("competitionMarketingName", "competitionMarketingNameId"), "competitionMarketingNameId")]
    ],
}


def _method(url: str) -> str:
    return urlsplit(url).path.rsplit("/", 1)[-1]


def _scope_error(method: str, body: Any, params: Mapping[str, Any]) -> Optional[str]:
    """Why ``body`` is not about what was asked, or ``None``."""
    rows = body if isinstance(body, list) else [body]
    alts = _SCOPES.get(method)
    if alts:
        for row in rows:
            ok = any(all(_strict(_dig(row, path), params.get(name)) for path, name in alt) for alt in alts)
            if not ok:
                want = " or ".join(", ".join(f"{'.'.join(p)}={params.get(n)!r}" for p, n in alt) for alt in alts)
                return f"a row is outside the request ({want})"
    if method == "getgdapgamesbetweentwodates":
        lo, hi = str(params.get("dateFrom"))[:19], str(params.get("dateTo"))[:19]
        for row in rows:
            at = row.get("gameDateTime") if isinstance(row, dict) else None
            if not isinstance(at, str) or not lo <= at[:19] < hi:
                return f"a game starts at {at!r}, outside [{lo}, {hi})"
    if method == "getgdapcompetitionsbyseasonsandcompetitionmarketingname":
        lo_s, hi_s = params.get("fromSeason"), params.get("toSeason")
        for row in rows:
            season = row.get("season") if isinstance(row, dict) else None
            if type(season) is not int or (lo_s is not None and season < lo_s) or (hi_s is not None and season > hi_s):
                return f"an edition has season {season!r}, outside {lo_s}..{hi_s}"
    return None


def _check_gdap(url: str, body: Any, params: Mapping[str, Any]) -> Any:
    """Return ``body`` (scrubbed) when it is a complete, in-scope GDAP answer; raise otherwise."""
    where, method = _rt._where(url), _method(url)
    if body is None or body == {} or body == []:
        raise NoDataError(f"{where} answered an empty body (an unknown id or nothing there)")
    if isinstance(body, dict) and set(body) <= _ERROR_KEYS:
        raise AssetFetchError(f"{where} answered an error envelope {sorted(body)}: {_rt._excerpt(json.dumps(body))}")
    if isinstance(body, dict) and "Error" in body:
        raise AssetFetchError(f"{where} answered a GDAP Error object: {_rt._excerpt(json.dumps(body))}")
    if isinstance(body, list) and not all(isinstance(r, dict) for r in body):
        if not all(isinstance(r, (int, str)) for r in body):
            raise AssetFetchError(f"{where}: the rows are not objects")
    why = _scope_error(method, body, params)
    if why:
        raise AssetFetchError(f"{where}: {why}")
    return _scrub(body, root_staff=method == "getgdapcompetitionnominationsbyid")


def _gdap(url: str, params: Mapping[str, Any], **kwargs: Any) -> Any:
    """GET one GDAP method with the subscription key, under the module's refusal vocabulary."""
    kwargs.setdefault("session", SESSION)
    kwargs.setdefault("retry_statuses", _RETRY_STATUSES)
    caller_headers = dict(kwargs.pop("headers", None) or {})
    params = dict(params)
    if _method(url) == "getgdapcompetitionsbyseasonsandcompetitionmarketingname" and "fromSeason" in params:
        params.setdefault("toSeason", params["fromSeason"])  # one season asked: the API would run to the latest
    for attempt in range(2):
        headers = {KEY_HEADER: _api_key(), "Accept": "application/json", **caller_headers}
        with _rt._transport_errors(url):
            resp = _rt.download(url=url, params=dict(params), headers=headers, **kwargs)
        if getattr(resp, "status_code", None) == 401 and attempt == 0:
            _api_key.cache_clear()  # the key rotated: read the page again, once
            continue
        break
    return _check_gdap(url, _rt._json_body(resp, url), params)


# ---------------------------------------------------------------------------
# game pages
# ---------------------------------------------------------------------------

_PAGE_KEYS = ("game", "playByPlay", "gameDetails", "playersTeamA", "playersTeamB")
_ANCHORS = {
    "game": re.compile(r'"game":(?=\{"gameId":)'),
    "playByPlay": re.compile(r'"playByPlay":(?=\{"items":)'),
    "gameDetails": re.compile(r'"gameDetails":(?=\{)'),
    "playersTeamA": re.compile(r'"playersTeamA":(?=\[)'),
    "playersTeamB": re.compile(r'"playersTeamB":(?=\[)'),
}
_FLIGHT_REF = re.compile(r"\$([0-9a-fA-F]+):(.+)")


def _resolve(obj: Any, roots: Mapping[str, Any], depth: int = 0) -> Any:
    """Replace the React flight tokens of ``obj``; any token it does not know raises (fail closed).

    ``$undefined`` -> ``None``; ``$$x`` -> ``$x``; ``$D<iso>`` -> the ISO string;
    ``$<chunk>:props:<object>:<path>`` -> a copy of that value inside the extracted object.
    """
    if depth > 50:
        raise AssetFetchError("fiba.basketball game page: a flight reference chain is too deep")
    if isinstance(obj, dict):
        return {k: _resolve(v, roots, depth) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve(x, roots, depth) for x in obj]
    if not isinstance(obj, str) or not obj.startswith("$"):
        return obj
    if obj == "$undefined":
        return None
    if obj.startswith("$$"):
        return obj[1:]
    if obj.startswith("$D"):
        return obj[2:]
    match = _FLIGHT_REF.fullmatch(obj)
    segs = match.group(2).split(":") if match else []
    if len(segs) >= 2 and segs[0] == "props" and segs[1] in roots:
        target: Any = roots[segs[1]]
        for seg in segs[2:]:
            if isinstance(target, list) and seg.isdigit() and int(seg) < len(target):
                target = target[int(seg)]
            elif isinstance(target, dict) and seg in target:
                target = target[seg]
            else:
                raise AssetFetchError(f"fiba.basketball game page: a dangling flight reference {obj!r}")
        return _resolve(copy.deepcopy(target), roots, depth + 1)
    raise AssetFetchError(f"fiba.basketball game page: an unhandled flight token {obj[:40]!r}")


def _extract_page(text: str, game_id: int, where: str) -> Dict[str, Any]:
    """The game objects of a ``text/x-component`` payload, flight tokens resolved.

    Raises:
        AssetFetchError: the not-found shell (no ``game`` header for ``game_id``), or an
            unresolvable flight token.
    """
    decoder, raw = json.JSONDecoder(), {}
    for key, rx in _ANCHORS.items():
        for match in rx.finditer(text):
            try:
                value, _ = decoder.raw_decode(text, match.end())
            except ValueError:
                continue
            if key == "game" and not _strict(_dig(value, ("gameId",)), game_id):
                continue
            raw[key] = value
            break
    if "game" not in raw:
        raise AssetFetchError(
            f"{where}: this is the not-found shell, not game {game_id} ({len(text)} chars): "
            "a wrong slug, team-code order or competition gives HTTP 200 with no game header"
        )
    body = _resolve(raw, raw)
    return {k: body[k] for k in _PAGE_KEYS if k in body}


def _slugify(name: str) -> str:
    """fiba.basketball's history slug: lower-case, apostrophes dropped, other non-alphanumeric runs -> ``-``."""
    return re.sub(r"[^a-z0-9]+", "-", re.sub(r"['’]", "", name.lower())).strip("-")


@functools.lru_cache(maxsize=256)
def _competition_slug(comp: int) -> str:
    """``<histId>-<histSlug>`` of the competition's family (its marketing name)."""
    body = _gdap(f"{GDAP_URL}/getGdapCompetitionById", {"gdapCompetitionId": comp})
    name = _dig(body, ("competitionMarketingName", "marketingName"))
    fam = _dig(body, ("competitionMarketingName", "competitionMarketingNameId"))
    if not isinstance(name, str) or type(fam) is not int:
        raise AssetFetchError(f"competition {comp}: GDAP gives no marketing name, so the game page URL cannot be built")
    return f"{fam}-{_slugify(name)}"


#: competition -> its scheduled games -> ``(teamA code, teamB code)``, or ``None`` while the teams are not drawn.
_codes: Dict[int, Dict[int, Optional[Tuple[str, str]]]] = {}


def _team_codes(comp: int, game_id: int) -> Tuple[str, str]:
    """``(teamA code, teamB code)`` of a game from its competition's schedule (cached per competition).

    A game the schedule does not list is looked up in GDAP: an unknown id is ``NoDataError``, a
    real game of another edition is ``AssetFetchError`` (the request is outside the competition).
    """
    for fresh in (False, True):  # a game the cached schedule lacks may have been added since
        if fresh or comp not in _codes:
            rows = _gdap(f"{GDAP_URL}/getgdapgamesbycompetitionid", {"gdapCompetitionId": comp})
            _codes[comp] = {
                r["gameId"]: (str(a).upper(), str(b).upper())
                if (a := r.get("teamA", {}).get("code")) and (b := r.get("teamB", {}).get("code"))
                else None
                for r in rows
                if type(r.get("gameId")) is int
            }
        codes = _codes[comp].get(game_id)
        if codes:
            return codes
    if game_id in _codes[comp]:
        raise NoDataError(f"competition {comp} lists game {game_id} without its two team codes (not drawn yet)")
    try:
        _gdap(f"{GDAP_URL}/getgdapgamebyid", {"gdapGameId": game_id})
    except NoDataError:
        raise NoDataError(f"FIBA has no game {game_id} (competition {comp} does not list it)") from None
    raise AssetFetchError(
        f"game {game_id} is not a game of competition {comp}: it belongs to another edition (wrong scope); "
        "pass the comp that lists it"
    )


def _page_url(game_id: int, comp: int) -> str:
    code_a, code_b = _team_codes(comp, game_id)
    return f"{SITE_URL}/en/history/{_competition_slug(comp)}/{comp}/games/{game_id}-{code_a}-{code_b}"


_PAGE_ATTEMPTS = 4
_PAGE_RETRY_STATUSES = frozenset({429, 502, 503, 504})
_MAX_RETRY_AFTER = 60.0
_PAGE_TIMEOUT = 60


def _page_transport(url: str, headers: Mapping[str, str], follow: bool = True) -> Any:
    """GET ``url`` with Chrome TLS (fiba.basketball answers plain ``requests`` with an HTTP 403 page)."""
    try:
        from curl_cffi import requests as creq
    except ImportError as exc:  # pragma: no cover - exercised only on the live path
        raise ImportError(
            "Live fiba.basketball game pages require curl_cffi. Install with: pip install curl_cffi "
            "or pip install sportsdataverse[all]"
        ) from exc
    return creq.get(url, headers=dict(headers), impersonate="chrome", timeout=_PAGE_TIMEOUT, allow_redirects=follow)


def _retry_after(resp: Any) -> float:
    try:
        return min(float((getattr(resp, "headers", None) or {}).get("retry-after", 0)), _MAX_RETRY_AFTER)
    except (TypeError, ValueError):  # an HTTP-date falls back to the exponential delay
        return 0.0


def _fetch_page(url: str, headers: Mapping[str, str], follow: bool = True) -> Any:
    """The final response of a game-page request; EVERY attempt waits its turn, 429 / 50x retry.

    ``Retry-After`` is honoured on 429 and 503. A connection failure that outlives the attempts is an
    :class:`AssetFetchError`.
    """
    for attempt in range(_PAGE_ATTEMPTS):
        _polite_wait(url)
        delay = 2.0 * 2**attempt
        try:
            resp = _page_transport(url, headers, follow)
        except OSError as exc:  # curl_cffi's RequestException subclasses OSError
            if attempt == _PAGE_ATTEMPTS - 1:
                raise AssetFetchError(
                    f"{_rt._where(url)}: fetch failed after {_PAGE_ATTEMPTS} attempts: {type(exc).__name__}"
                ) from exc
        else:
            status = getattr(resp, "status_code", None)
            if status not in _PAGE_RETRY_STATUSES or attempt == _PAGE_ATTEMPTS - 1:
                return resp
            if status in (429, 503):
                delay = max(delay, _retry_after(resp))
        time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


class _NotFoundShell(AssetFetchError):
    """The HTTP 200 shell a wrong slug, team-code order or template answers (no game header)."""


_EVENT_RE = re.compile(r"/en/events/([A-Za-z0-9._~%-]+)")


def _event_slug(comp: int) -> Optional[str]:
    """The ``eventSlug`` of a competition that has an event page: its history URL answers 307 to it."""
    probe = f"{SITE_URL}/en/history/{_competition_slug(comp)}/{comp}"
    resp = _fetch_page(probe, {}, follow=False)
    if getattr(resp, "status_code", None) not in (301, 302, 307, 308):
        return None
    found = _EVENT_RE.search(str((getattr(resp, "headers", None) or {}).get("location", "")))
    return found.group(1) if found else None


def _fetch_game_page(real: str, game_id: int, headers: Mapping[str, str]) -> Dict[str, Any]:
    """One game-page URL -> its extracted objects; the not-found shell raises :class:`_NotFoundShell`."""
    where = _rt._where(real)
    resp = _fetch_page(real, headers)
    _rt._check_response(resp, real)
    ctype = str((getattr(resp, "headers", None) or {}).get("content-type", "")).lower()
    if "x-component" not in ctype:
        raise AssetFetchError(f"{where} answered {ctype or 'no content type'}, not a text/x-component flight payload")
    text = _rt._text_body(resp, real)
    try:
        return _extract_page(text, game_id, where)
    except AssetFetchError as exc:
        if "not-found shell" in str(exc):
            raise _NotFoundShell(str(exc)) from None
        raise


def _page(url: str, params: Mapping[str, Any], **kwargs: Any) -> Dict[str, Any]:
    """The checked game-page objects of ``/en/games/<game_id>`` for competition ``params['comp']``.

    The archive (history) URL is tried first; when it answers the not-found shell the competition's
    event page URL is tried (competitions that have an event page serve their games there).
    """
    game_id = int(_PAGE_PATH.match(urlsplit(url).path).group(1))  # type: ignore[union-attr]
    comp = _int_id(params.get("comp"))
    if comp is None:
        raise ValueError("comp (the GDAP competition id of the game) is required to build the game page URL")
    real = _page_url(game_id, comp)
    where = _rt._where(real)
    headers = {"RSC": "1", **dict(kwargs.get("headers") or {})}
    try:
        page = _fetch_game_page(real, game_id, headers)
    except _NotFoundShell as shell:
        code_a, code_b = _team_codes(comp, game_id)
        slug = _event_slug(comp)
        # ponytail: the Champions League host serves only its current season; unverified live (no probe left).
        alternates = [f"{SITE_URL}/en/events/{slug}/games/{game_id}-{code_a}-{code_b}"] if slug else []
        alternates.append(f"{BCL_URL}/en/games/{game_id}-{code_a}-{code_b}")
        for alt in alternates:
            try:
                page = _fetch_game_page(alt, game_id, headers)
                break
            except _NotFoundShell:
                continue
        else:
            raise shell
    got = _dig(page, ("game", "competition", "competitionId"))
    if not _strict(got, comp):
        raise AssetFetchError(f"{where}: the page is for competition {got!r}, asked {comp}")
    details = page.get("gameDetails")
    if not isinstance(details, dict):
        raise NoDataError(
            f"{where}: game {game_id} has no box score (header only: before about 2006 or not played yet)"
        )
    if not _strict(details.get("id"), game_id):
        raise AssetFetchError(f"{where}: the box score is for game {details.get('id')!r}, asked {game_id}")
    if page["game"].get("isLive") is True:
        raise AssetFetchError(f"{where}: game {game_id} is still live; its box score is not final")
    return _scrub(page)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a FIBA route: a GDAP method as checked JSON, or a game page as its extracted objects.

    Args:
        url: fully-built request URL: ``https://digital-api.fiba.basketball/hapi/<method>``, or
            ``https://www.fiba.basketball/en/games/<game_id>`` (rewritten to the real game page).
        params: query parameters; ``None`` values are dropped. A game page needs ``comp``.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The scrubbed GDAP body, or ``{"game", "playByPlay", "gameDetails", "playersTeamA",
        "playersTeamB"}`` for a game page (``playByPlay`` is absent when the page has none).

    Raises:
        NoDataError: 404, an empty GDAP answer, an unknown game id, a game whose teams are not
            drawn yet, or a game page with no box score.
        ValueError: 400 / 422, a malformed id, season or date, or a game page without ``comp``.
        AssetFetchError: any other failed fetch, an error envelope, a body outside the request,
            a game of another competition than ``comp``, the not-found shell, a live game, or an unresolvable flight token.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    parts = urlsplit(url)
    if parts.netloc == HOST_GDAP:
        return _gdap(url, clean, **kwargs)
    if _PAGE_PATH.match(parts.path):
        return _page(url, clean, **kwargs)
    raise ValueError(f"not a FIBA route: {_rt._where(url)}")
