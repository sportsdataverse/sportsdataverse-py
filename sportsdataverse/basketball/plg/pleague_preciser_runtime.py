"""Runtime for the generated ``pleague_preciser`` wrappers (``api.preciser.io``, the analytics vendor behind the
P. LEAGUE+ game page).

**Release flag: a third-party host.** ``api.preciser.io`` is the vendor's, not the league's: keyless,
``Access-Control-Allow-Origin: *``, no ``robots.txt`` (404) and no terms found. It serves the data the league's own
game page displays. The runtime keeps requests at least 1 s apart and the data release needs owner sign-off.

Four JSON routes keyed by the game ``uuid`` (the Preciser game uuid the site's game page embeds; ``uuid`` of
``pleague_game``, empty before 2022-23 so there is no vendor data for 2020-21 and 2021-22). Each body is a one-element
list. Measured against the live host (sdv-internal-refs ``pleague/``, 2026-10-12):

* **Transport.** Plain ``requests`` through the shared ``basketball_runtime.SESSION``, so every attempt, retries
  included, waits its turn; ``dl_utils.download`` retries 429 / 502 / 503 / 504 and honours ``Retry-After``. HTTP 500 is
  not retried: the host answers 500 (``text/html``) for an unknown uuid, which is indistinguishable from a server error,
  so it is an ``AssetFetchError``.
* **Clip access key.** Every video url in a body carries ``?key=<hex>``, a vendor access key. It is scrubbed
  (``key=<redacted>``) from every body before it is returned, a final scan refuses any body that still holds one, and
  no frame keeps a url.
* **Scope checks, all fail closed** (``AssetFetchError``): a body that is not a one-element list of objects; a
  ``game_id`` echo (summary, injury) that is not the uuid asked for; a shot list that is not ``post`` (finished), whose
  shots name a player the body does not list; a summary whose play log is empty. The shot list and the rotation echo
  nothing, so identity is checked by content **when the caller passes the site's** ``gid``: the runtime then fetches the
  game page and box (cached, shared with ``pleague_runtime``) and also checks that the uuid is the game page's, that
  the summary's play log ends on the final score, that the shot list's made 2- and 3-pointers equal the box's, that its
  team uuids equal the summary's, and that every rotation player is on the box roster with court time within 30 s of
  the box's minutes.
* **Error vocabulary.** ``NoDataError``: HTTP 404, a body of ``[]`` (an unknown uuid answers 200 ``[]`` on the summary),
  empty rotation team lists (an unknown or unplayed uuid). ``ValueError``: HTTP 400 / 422 or a uuid that is not a uuid.
  ``AssetFetchError``: every other failed or wrong answer.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.basketball.plg import pleague_preciser_parsers as V
from sportsdataverse.basketball.plg import pleague_runtime as SITE
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["HOST", "REQUEST_KEY"]

#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
HOST = "https://api.preciser.io"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 502, 503, 504})
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_ROUTES = {
    "/v1/chart-data/id/": "shots",
    "/post/game/summary/o/plg/gameId/": "summary",
    "/game/rotation/chart/o/plg/gameId/": "rotation",
    "/game/injury/list/o/plg/gameId/": "injury",
}


def _fetch(url: str) -> Any:
    try:
        return download(
            url,
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


def _vendor(kind: str, route: str, uuid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
    """One checked vendor body (the one game object) for ``kind``."""
    url = f"{HOST}{route}{uuid}"
    where = _rt._where(url)
    resp = _fetch(url)
    status = getattr(resp, "status_code", None)
    _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    if status != 200:
        raise AssetFetchError(f"{where} answered HTTP {status}, not 200 (an unknown uuid also answers 500)")
    final = urlsplit(str(getattr(resp, "url", "") or url)).netloc
    if final != urlsplit(HOST).netloc:
        raise AssetFetchError(f"{where}: redirected off the Preciser host to {final or 'an unknown host'}")
    if "json" not in str((getattr(resp, "headers", None) or {}).get("content-type", "")).lower():
        raise AssetFetchError(f"{where} answered a body that is not JSON")
    try:
        body = resp.json()
    except ValueError:
        raise AssetFetchError(f"{where} answered a body that is not JSON") from None
    if body == []:
        raise NoDataError(f"{where}: the vendor answered [] (an unknown uuid, or a game with no data)")
    body = V.scrub(body)
    if V.has_secret(body):
        raise AssetFetchError(f"{where}: a vendor access key survived in the body")
    if (
        kind == "rotation"
        and isinstance(body, list)
        and body
        and isinstance(body[0], dict)
        and not body[0].get("homeTeam")
        and not body[0].get("awayTeam")
    ):
        raise NoDataError(f"{where}: empty rotation lists (an unknown or unplayed uuid)")
    why = V.vendor_error(kind, body, uuid, ctx)
    if why:
        raise AssetFetchError(f"{where}: not the {kind} of game {uuid}: {why}")
    return body[0]


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET one Preciser route, refuse every wrong answer, return the game object with the request stamped.

    Args:
        url: the URL a generated wrapper built: ``/v1/chart-data/id/{uuid}``, ``/post/game/summary/o/plg/gameId/{uuid}``,
            ``/game/rotation/chart/o/plg/gameId/{uuid}`` or ``/game/injury/list/o/plg/gameId/{uuid}`` on
            ``api.preciser.io``.
        params: query parameters. ``gid`` (optional): the site's game id; when given the runtime also verifies the body
            against the site's game page and box (see the module docstring). ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        ``{"data": <the vendor's game object>, "_request": {"kind", "uuid", "verified", "gid"}}``.

    Raises:
        NoDataError: see the module docstring.
        ValueError: a uuid that is not a uuid, a ``gid`` that is not an integer, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer, including a ``gid`` whose game has another uuid.
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a Preciser request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    path = urlsplit(url).path
    route = next((r for r in _ROUTES if path.startswith(r)), None)
    if route is None:
        raise ValueError(f"not a Preciser route: {_rt._where(url)}")
    kind, uuid = _ROUTES[route], path[len(route) :].strip("/")
    if not _UUID.match(uuid):
        raise ValueError(f"uuid must be a Preciser game uuid (see the uuid column of pleague_game); got {uuid!r}")
    gid = SITE._int_arg(q["gid"], "gid") if "gid" in q else None
    ctx: Dict[str, Any] = {}
    if gid is not None:
        game = SITE.game_record(gid)
        if game.get("uuid") != uuid:
            raise AssetFetchError(f"game {gid} on the site has uuid {game.get('uuid')!r}, not {uuid!r}")
        # the site's box and (for shots / injury) the summary verify the uuid: lookups, so a 204 is unknown
        with _rt._no_content_unknown(f"game {gid} (the site box and summary checks)"):
            ctx["game"], ctx["box"] = game, SITE._box_data(gid)["data"]
            if kind in ("shots", "injury"):  # these two are compared with the summary's team uuids
                _vendor("summary", "/post/game/summary/o/plg/gameId/", uuid, ctx)
    data = _vendor(kind, route, uuid, ctx)
    return {"data": data, REQUEST_KEY: {"kind": kind, "uuid": uuid, "verified": gid is not None, "gid": gid}}
