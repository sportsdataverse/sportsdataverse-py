"""Runtime for the generated ``ibba_segev`` wrappers (Segev Stats JSON-RPC, ``stats.segevstats.com``, keyless).

Measured against the live host (sdv-internal-refs ``ibba/``, 2026-10-12):

* **One flat game-id space.** ``game_id`` is the only parameter; there is no season (the season of a game is
  its tip-off time, or the IBBA game that names it in ``ExternalID``). Ids mix every competition Segev scores
  (men's, women's, Leumit, youth, school, pre-season): the header's ``competition.name`` says which.
* **JSON served as ``text/html``.** The body is JSON whatever the content type says; the three methods are
  ``getGameInfo``, ``getBoxScore`` and ``getActions`` (no game list exists).
* **Errors are HTTP 200.** ``{"result": {"success": false}, "error": {...}}``: code ``-32000`` (``game not
  found``, what an unknown id answers) is ``NoDataError``; any other code is ``AssetFetchError``.
* **Silent wrong answers, all refused** (fail closed, type-strict compares): a body without the method's block;
  a ``gameId`` other than the one asked for; a game that is not finished (``gameFinished`` is not ``true``).

Error vocabulary. ``NoDataError``: HTTP 404, ``game not found``, a finished game with no actions.
``ValueError``: a ``game_id`` that is not a positive integer, an unknown method, HTTP 400 / 422.
``AssetFetchError``: any other non-200 (429 / 502 / 503 / 504 retried three times, spaced by
``dl_utils.download`` and honouring ``Retry-After``), a dead connection, an empty or non-JSON body, an error
or changed envelope, a wrong-scope or unfinished game.

Every attempt, retries included, goes through the shared ``basketball_runtime.SESSION`` and so waits its turn
(at least 1 s per host).
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["REQUEST_KEY", "METHODS"]

URL = "https://stats.segevstats.com/realtimestat_heb/api/"
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 502, 503, 504})
METHODS = frozenset({"getGameInfo", "getBoxScore", "getActions"})
NOT_FOUND = "-32000"  # the JSON-RPC code of "game not found"


def _fail(msg: str) -> AssetFetchError:
    return AssetFetchError(msg)


def _fetch(params: Dict[str, Any]) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response."""
    try:
        return download(
            URL,
            params=params,
            session=SESSION,
            timeout=TIMEOUT,
            num_retries=ATTEMPTS - 1,
            retry_statuses=RETRY_STATUSES,
            cache_ttl=0,
        )
    except OSError as exc:  # requests' RequestException subclasses OSError
        raise _fail(f"{_rt._where(URL)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}") from None


def _same(a: Any, b: Any) -> bool:
    """Type-strict equality: a string never equals an integer, so an echoed ``"50"`` is not ``50``."""
    return type(a) is type(b) and a == b


def _validate(method: str, game_id: int, body: Any, where: str) -> None:
    """Refuse everything that is not this method's finished-game answer for ``game_id``."""
    if not isinstance(body, dict) or "result" not in body or "error" not in body:
        raise _fail(f"{where}: not a JSON-RPC envelope")
    err = body["error"]
    if err is not None:
        code = err.get("code") if isinstance(err, dict) else None
        if _same(code, NOT_FOUND):
            raise NoDataError(f"{where}: game {game_id} not found")
        raise _fail(f"{where}: rpc error code {code!r}")
    result = body["result"]
    if not isinstance(result, dict) or result.get("success") is not True:
        raise _fail(f"{where}: result.success is not true")
    if method == "getBoxScore":
        box = result.get("boxscore")
        info = box.get("gameInfo") if isinstance(box, dict) else None
        if not isinstance(box, dict) or not all(isinstance(box.get(k), dict) for k in ("homeTeam", "awayTeam")):
            raise _fail(f"{where}: no boxscore with two teams")
    else:
        info = result.get("gameInfo")
    if not isinstance(info, dict):
        raise _fail(f"{where}: no gameInfo")
    if not _same(info.get("gameId"), str(game_id)):
        raise _fail(f"{where}: out of scope: gameId={info.get('gameId')!r}, asked {game_id}")
    if info.get("gameFinished") is not True:
        raise _fail(f"{where}: game {game_id} is not final (gameFinished is not true)")
    if method == "getActions" and not result.get("actions"):
        raise NoDataError(f"{where}: game {game_id} has no actions")


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET one Segev method, refuse every wrong answer, return the JSON-RPC envelope with the request stamped.

    Args:
        url: the URL a generated wrapper built (``stats.segevstats.com/realtimestat_heb/api/``).
        params: ``method`` (``getGameInfo`` / ``getBoxScore`` / ``getActions``) and ``game_id`` (a positive
            integer); ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The parsed envelope with ``_request`` = ``{"method": ..., "game_id": ...}``.

    Raises:
        NoDataError: HTTP 404, ``game not found``, a finished game with no actions.
        ValueError: a bad method or ``game_id``, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a Segev request: {sorted(kwargs)}")
    parts = urlsplit(url)
    if f"{parts.scheme}://{parts.netloc}{parts.path}" != URL:
        raise ValueError(f"not a Segev route: {_rt._where(url)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    method, gid = q.get("method"), q.get("game_id")
    if method not in METHODS:
        raise ValueError(f"method must be one of {sorted(METHODS)}; got {method!r}")
    if isinstance(gid, bool) or not isinstance(gid, (int, str)) or not str(gid).strip().isdigit() or int(gid) < 1:
        raise ValueError(f"game_id must be a positive integer; got {gid!r}")
    game_id = int(gid)
    where = _rt._where(URL)
    resp = _fetch({"method": method, "game_id": game_id})
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
        raise _fail(f"{where} answered HTTP 200 with an empty body")
    try:
        body = json.loads(text)  # served as text/html; the body is JSON
    except ValueError:
        raise _fail(f"{where} answered a body that is not JSON") from None
    _validate(method, game_id, body, where)
    body[REQUEST_KEY] = {"method": method, "game_id": game_id}
    return body
