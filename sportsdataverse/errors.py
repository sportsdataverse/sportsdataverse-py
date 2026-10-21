"""
Custom exceptions for sportsdataverse module
"""

from __future__ import annotations

import functools as _functools
import inspect as _inspect
import re as _re
import warnings as _warnings
from typing import TYPE_CHECKING, Any, Callable
from typing import TypeVar as _TypeVar
from typing import cast as _cast
from urllib.parse import urlsplit

if TYPE_CHECKING:
    import requests


# A credential's ``name=value`` pair: the query-string form, its URL-encoded form
# (``apiKey%3D...``, also after an escaped ``%26``), and the dict / JSON form
# (``'key': '...'``). Same names as sportsdataverse-js ``redactSecrets``. Linear in
# the input: the name is a bounded alternation behind a fixed-width lookbehind, and
# each repeat stops at a character the next part cannot start with. The leading
# lookahead (every name's first letter) only makes the scan cheaper.
_SECRET_PAIR = _re.compile(
    r"(?=[ACIJKPRSTacijkprst])(?:(?<![A-Za-z0-9])|(?<=%[0-9A-Fa-f]{2}))"
    r"((password|passwd|pass|pwd|(?:(?:access|refresh|id|auth|session)[_-]?)?token|client[_-]?secret|secret"
    r"|(?:api|app|access|subscription)[_-]?key|key|jwt|signature|sig|authorization|auth|cookie)"
    r"[\"']?\s*(?:[:=]|%3[AD])\s*[\"']?)"
    # A quoted value runs to its closing quote (it may hold spaces); an unquoted one
    # stops at the first separator.
    r"((?<=\")[^\"\n]*|(?<=')[^'\n]*|[^&\s\"'<>]+)",
    _re.IGNORECASE,
)


# A credential carried as a PATH SEGMENT rather than a query pair. TheSportsDB's route is
# ``/api/v1/json/<key>/<endpoint>.php``, so ``_SECRET_PAIR`` -- which only knows the
# ``name=value`` shapes -- never sees it, and the key would survive into every error message,
# every ``download`` retry log line and every chained traceback.
_PATH_KEY = _re.compile(r"(/api/v[12]/json/)[^/\s?#\"']+")

# A password in a URL's userinfo (``http://user:pw@proxy:8080``, the proxy shape ``download``
# documents and ``mbb_ncaa_fetch`` builds), a bearer / basic credential quoted from a header
# (``Authorization: Bearer <jwt>``, whose unquoted value ``_SECRET_PAIR`` stops at the space), and
# an unquoted ``Cookie:`` header line, every cookie on it.
_USERINFO = _re.compile(r"(://[^\s/?#@:\"']*:)[^\s/?#@\"']+@")
_BEARER = _re.compile(r"(?<![A-Za-z0-9])((?:bearer|basic)\s+)[A-Za-z0-9._~+/=-]{8,}", _re.IGNORECASE)
_COOKIE_LINE = _re.compile(r"(?<![A-Za-z0-9])(cookie\s*:\s*)[^\r\n\"']+", _re.IGNORECASE)


def _redact_pair(match: _re.Match[str]) -> str:
    prefix, name, value = match.groups()
    # An unquoted value (``{'key': 1234}``, ``(token=abc)``) runs into the
    # punctuation that closes it; keep that punctuation out of the redaction. A
    # quoted value already stopped at its quote, so all of it is the secret.
    secret = value if prefix.endswith(('"', "'")) else value.rstrip(",;)]}")
    tail = value[len(secret) :]
    # A bare ``key`` must look like a credential (16+ characters, as every
    # HockeyTech key is), so "primary key=player_id" survives. The short value is
    # still scanned: it may hold an escaped pair of its own.
    if name.lower() == "key" and len(secret) < 16:
        return prefix + _SECRET_PAIR.sub(_redact_pair, value)
    return prefix + "REDACTED" + tail


def _redact_secrets(text: str) -> str:
    """Replace the value of every credential pair in ``text`` with ``REDACTED``.

    Covers ``apiKey`` / ``api_key`` / ``apikey`` / ``appkey`` / ``access_key`` /
    ``subscription-key`` / ``key`` / ``token`` / ``access_token`` / ``password`` /
    ``secret`` / ``client_secret`` / ``sig`` / ``signature`` / ``auth`` /
    ``Authorization`` / ``pass`` / ``cookie`` (and the other names sportsdataverse-js
    redacts), case-insensitively, as ``name=value``, as ``name%3Dvalue``, and as
    ``'name': 'value'``; a key as a TheSportsDB path segment; the password of a
    ``user:pw@host`` URL; a ``Bearer`` / ``Basic`` credential; and a whole
    ``Cookie:`` header line. Everything else -- host, path,
    status, the other params -- is left as it was, so the line still says which
    request failed.
    """
    # Bearer first: ``Authorization: Bearer <jwt>`` as a pair would redact only the word "Bearer".
    text = _BEARER.sub(r"\1REDACTED", text)
    text = _COOKIE_LINE.sub(r"\1REDACTED", text)
    text = _SECRET_PAIR.sub(_redact_pair, text)
    text = _USERINFO.sub(r"\1REDACTED@", text)
    return _PATH_KEY.sub(r"\1REDACTED", text)


class SportsDataverseError(Exception):
    """Base class for every error raised by sportsdataverse.

    Catch this to handle any package-specific failure with one ``except``
    clause, while still being able to catch the narrower subclasses
    (:class:`SeasonNotFoundError`, :class:`NoDataError`) individually::

        from sportsdataverse.errors import SportsDataverseError

        try:
            df = some_loader(...)
        except SportsDataverseError as exc:
            ...  # any sportsdataverse-originated failure

    Re-parenting the existing errors under this base is backwards compatible:
    code catching ``Exception`` or the specific subclasses keeps working.

    Every message is passed through :func:`_redact_secrets`: these messages quote
    request URLs, and a URL can carry an API key in its query string.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*(_redact_secrets(a) if isinstance(a, str) else a for a in args))


class SeasonNotFoundError(SportsDataverseError):
    """Raised when a caller requests a season earlier than the loader supports.

    Each sport submodule has a per-source minimum season (e.g. CFB PBP
    starts at ``2003``, NFL PBP at ``1999``). Loaders call
    :func:`season_not_found_error` at the top of each call to fail fast
    with a clear message when the requested season is below that floor.

    Example:
        Catch the error at the call site::

            from sportsdataverse.errors import (
                SeasonNotFoundError,
                season_not_found_error,
            )

            try:
                season_not_found_error(season=1995, min_season=2003)
            except SeasonNotFoundError as exc:
                print(f"unsupported season: {exc}")
                # unsupported season: Season 1995 not found, season cannot be less than 2003
    """

    pass


def season_not_found_error(season: int, min_season: int) -> None:
    """Raise :class:`SeasonNotFoundError` when ``season`` predates ``min_season``.

    Args:
        season: Caller-supplied season (int or coercible).
        min_season: Inclusive minimum supported season for the data source.

    Raises:
        SeasonNotFoundError: When ``int(season) < int(min_season)``.

    Example:
        Guard a custom loader::

            from sportsdataverse.errors import season_not_found_error

            def load_thing(season):
                season_not_found_error(season, min_season=2003)
                # ... fetch parquet ...
    """
    if int(season) >= int(min_season):
        return
    else:
        raise SeasonNotFoundError(f"Season {season} not found, season cannot be less than {min_season}")


class RawStoreMissError(SportsDataverseError):
    """Raised when a read-only raw store has no capture for a requested payload.

    Read-only mode (``SDV_PY_NBA_RAW_JSON_READONLY`` /
    ``SDV_PY_WNBA_RAW_JSON_READONLY``, or ``raw_store_readonly=True``) means
    *offline*: the committed store is the only source. A miss therefore cannot
    be filled — silently falling back to the live API would make a build that
    looks reproducible partly live, and returning an empty payload would make
    an incomplete compile look complete. Both are the failure mode this error
    exists to prevent, mirroring
    :class:`~sportsdataverse._crosswalk_basketball_sources.CrosswalkSourceError`:
    a source that cannot produce data fails loudly.

    Per-game compile loops (:func:`~sportsdataverse.nba.nba_season_compile.compile_nba_season`)
    already catch per game, so an uncaptured game is logged and skipped rather
    than aborting the season — but the skip is now visible in the log instead
    of being papered over by a live fetch.

    Example:
        Distinguish "not captured" from a real failure::

            from sportsdataverse.errors import RawStoreMissError
            from sportsdataverse.nba import nba_possessions

            try:
                poss = nba_possessions("0022300001", raw_store_readonly=True)
            except RawStoreMissError as exc:
                print(f"not in the raw store: {exc}")
    """


class AssetFetchError(SportsDataverseError):
    """Raised when fetching a published data asset returns an unusable response.

    Covers any hosted artifact the package reads over HTTP -- a release parquet,
    a producer-repo file, a bundled JSON capture -- when the transport comes back
    with a status that is neither success nor a definitive "no data". The common
    cases are a 403 that outlived the retry budget (rate limit, or a private
    asset) and a 5xx the host never recovered from.

    Deliberately distinct from :class:`NoDataError`, which means the asset is
    genuinely absent and the caller should skip it. This one means the fetch
    FAILED and the answer is unknown, so it must never be swallowed into an empty
    frame: a rate-limited season is not an empty season.

    Example:
        Tell a missing season apart from a failed fetch::

            from sportsdataverse.errors import AssetFetchError
            from sportsdataverse.cfb import load_cfb_schedule

            try:
                sched = load_cfb_schedule([2024])
            except AssetFetchError as exc:
                print(f"fetch failed, retry later: {exc}")
    """

    #: Set True by a runtime when the failure is a dead connection or a 429/5xx, so a caller
    #: that retries can tell it from a definitive refusal (a WAF block, a wrong page).
    transient: bool = False


class NoDataError(SportsDataverseError):
    """Raised when a source has no payload for the request.

    Means the fetch SUCCEEDED and the answer is "nothing here" -- the game,
    team, season or asset does not exist upstream. Triggered by a raw HTTP 404
    from any host (an ESPN endpoint, a published release asset, a producer-repo
    file) and by the legacy ESPN convention of returning ``{"code": 404, ...}``
    with a 200 status. Callers treat all of these the same way: skip it.

    Contrast :class:`AssetFetchError`, which means the fetch FAILED and the
    answer is unknown. Keeping them apart is what stops a rate-limited season
    from being recorded as an empty one.

    Example:
        Catch missing-data responses around an ESPN call::

            from sportsdataverse.errors import NoDataError
            from sportsdataverse.cfb import espn_cfb_pbp

            try:
                pbp = espn_cfb_pbp(game_id=1)  # not a real ESPN game id
            except NoDataError as exc:
                print(f"no data: {exc}")
                pbp = None
    """

    pass


#: Back-compat alias. The error was ESPN-only when it was named, but
#: :func:`sportsdataverse.dl_utils.download` raises it for any 404 -- including
#: release assets on GitHub -- so the name misdescribes it. Existing
#: ``raise``/``except NoESPNDataError`` code keeps working unchanged.
NoESPNDataError = NoDataError


class InsufficientInputError(SportsDataverseError):
    """Raised when the inputs arrived intact but cannot support the computation.

    Distinct from both :class:`NoDataError` (nothing came back) and
    :class:`AssetFetchError` (the fetch failed): here the frame is present and
    well-formed, but a term the model needs is structurally absent, so any
    number produced from it would be wrong rather than missing. The canonical
    case is a boxscore era whose ``turnovers`` column is uniformly ``0`` --
    the possession estimate ``FGA - OREB + TO + 0.44 * FTA`` then omits ~23% of
    possessions and every efficiency/tempo rating built on it is distorted by
    each team's own (unobserved) turnover rate.

    Raise it instead of emitting a plausible-looking number; a caller building
    a published asset should record the season as unbuildable.

    Example:
        Skip a season whose inputs cannot support a rating::

            from sportsdataverse.errors import InsufficientInputError
            from sportsdataverse.wbb import wbb_team_ratings

            try:
                ratings = wbb_team_ratings(2008)
            except InsufficientInputError as exc:
                print(f"not buildable: {exc}")
    """


class EmptyResponseWarning(UserWarning):
    """Warned when a stats.nba.com / stats.wnba.com request comes back with nothing to parse.

    ``nba_stats_*`` / ``wnba_stats_*`` return ``{}`` (an empty frame when parsed) when the API
    answers a 2xx with an empty JSON object (or a 204 / 205). A failed fetch -- a non-2xx, a
    blank or non-JSON body -- raises instead (``AssetFetchError``; ``NoDataError`` for a 404,
    ``ValueError`` for a 400 / 422). This warning names the URL and status so an interactive
    empty result is not silent. The usual cause is a missing or invalid parameter, most often
    ``Season``. Silence it in a pipeline with
    ``warnings.filterwarnings("ignore", category=EmptyResponseWarning)``.

    Example:
        Quick start::

            import warnings
            from sportsdataverse.errors import EmptyResponseWarning
            warnings.filterwarnings("ignore", category=EmptyResponseWarning)
    """


class EraCoverageWarning(UserWarning):
    """Warned when NFL model features are built for a season past the validated era range.

    The nflfastR era one-hots end in an open-ended ``era4`` (``season > 2017``),
    so a season beyond :data:`sportsdataverse.nfl.model_vars.ERA_MAX_KNOWN_SEASON`
    is scored under the last era with nothing flagging that a rule change might
    warrant a new dummy. The warning is that flag. It is a ``UserWarning`` so
    scoring the in-progress season keeps working; silence it deliberately once
    the season has been reviewed, and bump ``ERA_MAX_KNOWN_SEASON`` at the next
    era-aware retrain.

    Example:
        Silence it for a producer that scores the live season::

            import warnings
            from sportsdataverse.errors import EraCoverageWarning

            warnings.filterwarnings("ignore", category=EraCoverageWarning)
    """


class SchemaDriftWarning(UserWarning):
    """Warned when a parser meets a payload whose shape it does not recognise and returns zero rows.

    The parser contract keeps a malformed payload from raising: the result is a zero-row
    frame. Without this warning that frame reads exactly like "the source has no data", so an
    upstream schema change would pass unnoticed (A-L60). The message names the parser, the
    payload path it was reading, and the exception the shape caused. Turn it into an error in a
    pipeline that must never publish a drifted parse with
    ``warnings.filterwarnings("error", category=SchemaDriftWarning)``.

    Example:
        Fail a build on drift::

            import warnings
            from sportsdataverse.errors import SchemaDriftWarning
            warnings.filterwarnings("error", category=SchemaDriftWarning)
    """


#: What flattening a payload of the wrong shape raises (a row that is not an object: ``TypeError``
#: from :func:`_object_rows`; a missing key; an unparseable value).
#: Parsers catch exactly these, warn :class:`SchemaDriftWarning` and return zero rows.
_SHAPE_ERRORS: tuple[type[Exception], ...] = (TypeError, AttributeError, ValueError, KeyError, IndexError)


def _object_rows(rows: list) -> list[dict]:
    """The row rule: drop null rows, and raise ``TypeError`` on any other row that is not an object.

    A null row (``None``, ``NaN``, ``pd.NA``) is no data, so it is dropped: ``pd.json_normalize`` would
    read it as an all-null row, and a list of nulls as an N x 0 frame (4.13). Any other row that is not
    an object is schema drift, which pandas 3 raises on itself but pandas 2 reads as an empty row (WB-1).
    Call it on the rows handed to ``pd.json_normalize``, inside the parser's ``_SHAPE_ERRORS`` catch.
    """
    kept: list[dict] = []
    for row in rows:
        if isinstance(row, dict):
            kept.append(row)
        elif not _is_null(row):
            raise TypeError(f"a row is {type(row).__name__}, not an object")
    return kept


def _is_null(row: Any) -> bool:
    """Whether a row is a null one (``None``, ``NaN``, ``pd.NA``): no data, which the row rule drops."""
    import pandas as pd  # lazy: errors.py is imported by every module, pandas only by the parsers

    return bool(pd.api.types.is_scalar(row) and pd.isna(row))


def _first_row(rows: list) -> Any:
    """The first row that is not null, or ``None``: a parser that picks a shape from a row must skip the
    null rows the row rule drops, or a leading null reshapes the frame (R953-2)."""
    return next((row for row in rows if not _is_null(row)), None)


def _warn_schema_drift(where: str, exc: BaseException) -> None:
    """Warn that ``where`` (``"<parser>: <payload path>"``) met an unrecognised shape (A-L60)."""
    _warnings.warn(
        f"{where}: unexpected payload shape ({type(exc).__name__}: {exc}); no rows read from it",
        SchemaDriftWarning,
        stacklevel=3,
    )


def _error_envelope_code(body: Any) -> int | None:
    """The status of an ESPN-style ``{"code": <4xx/5xx>, "message": ...}`` error body, else ``None``.

    Only a dict holding exactly those two keys counts, so a payload that merely carries a
    ``code`` field next to its data is never mistaken for one (A-M13), and only an HTTP-shaped
    code (400-599): a provider's own business code such as 1001 is not an HTTP status.
    """
    if isinstance(body, dict) and set(body) == {"code", "message"}:
        code = body["code"]
        if isinstance(code, int) and not isinstance(code, bool) and 400 <= code <= 599:
            return code
    return None


_Parser = _TypeVar("_Parser", bound=Callable[..., Any])


def _normalize_payload(parse: Callable[..., Any], fix: Callable[[Any], Any]) -> Callable[..., Any]:
    """``parse`` with ``fix`` applied to its payload, read under the parser's own first-parameter name.

    The payload arrives positionally or as a keyword (``parse_kbl_teams(raw=body)``): a wrapper that took it
    as ``payload`` would turn every keyword call into a ``TypeError`` (R4.1b-1).
    """
    name = next(iter(_inspect.signature(parse).parameters))

    @_functools.wraps(parse)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if args:
            args = (fix(args[0]), *args[1:])
        elif name in kwargs:
            kwargs[name] = fix(kwargs[name])
        return parse(*args, **kwargs)

    return wrapper


def _data_body(payload: Any) -> Any:
    if _error_envelope_code(payload) is not None:
        return {}
    if isinstance(payload, list):
        return [rec for rec in payload if rec != {}]
    return payload


def _data_only(parse: _Parser) -> _Parser:
    """The parser contract on a body that is not data (A-L17, EX-SYNC-g): zero rows, never a raise.

    Wraps a public provider parser so ``{"code", "message"}`` error envelopes (see
    :func:`_error_envelope_code`) read as ``{}`` and a list body loses its empty ``{}`` records, which
    carry no field, instead of becoming a one-row frame. Every other body reaches the parser unchanged.
    The result is whatever the parser already returns for ``{}`` / ``[]``.
    """
    return _cast(_Parser, _normalize_payload(parse, _data_body))


def _dict_only(parse: _Parser) -> _Parser:
    """For a parser whose payload is always an object: any non-dict body reads as ``{}`` (A-L17).

    No envelope check: every parser it wraps reads named keys, which an error envelope lacks.
    """
    return _cast(_Parser, _normalize_payload(parse, lambda payload: payload if isinstance(payload, dict) else {}))


def _object_records(records: Any, where: str, stacklevel: int = 3) -> list | None:
    """``records`` without its null rows, ready for ``pd.json_normalize``; ``None`` when it drifted (WB-providers-2).

    The rule: ``None`` is no records; a null row (``None``, ``NaN``, ``pd.NA``) is no row and is dropped;
    a ``records`` that is not a list, or any other row that is not an object, is drift. pandas 2 reads a
    scalar row as an empty one (an N x 0 frame) and pandas 3 raises, so the provider flatteners check here
    first, warn :class:`SchemaDriftWarning` naming ``where`` (``"<parser>: <payload path>"``) and return
    zero rows. ``stacklevel`` is passed to ``warnings.warn`` and should land on the parser's caller.
    ponytail: unified with #941's ``_require_objects`` (one helper, this rule) after the providers sync.
    """
    import pandas as pd  # lazy: errors.py is imported by every module, pandas only by the parsers

    if records is None:
        return []
    if not isinstance(records, list):
        shape = f"records is {type(records).__name__}, not a list"
    else:
        kept = [r for r in records if not (pd.api.types.is_scalar(r) and pd.isna(r))]  # a null row is no row
        bad = [r for r in kept if not isinstance(r, dict)]
        if not bad:
            return kept
        shape = f"a record is {type(bad[0]).__name__}, not an object"
    _warnings.warn(
        f"{where}: unexpected payload shape ({shape}); no rows read from it", SchemaDriftWarning, stacklevel=stacklevel
    )
    return None


#: Routes (``host/path`` prefixes) whose 2xx answer is a bare JSON string BY DESIGN. Every other
#: route's JSON scalar is a failed fetch (B-GB-16). OpenLigaDB's last-change timestamp answers
#: ``"2025-08-24T19:25:06.787"`` (tools/codegen/endpoints/openligadb.yaml, R880-1).
_STRING_ROUTES = ("api.openligadb.de/getlastchangedate/",)


def _string_route(url: str) -> bool:
    """Whether ``url`` is one of :data:`_STRING_ROUTES`, the routes allowed a bare string body."""
    parts = urlsplit(str(url))
    return f"{parts.netloc}{parts.path}".startswith(_STRING_ROUTES)


def _is_data_body(url: str, body: Any) -> bool:
    """A decoded 2xx body a parser can read: an object or array that is not an error envelope,
    or a string from a :data:`_STRING_ROUTES` route. Anything else is a failed fetch."""
    if isinstance(body, str):
        return _string_route(url)
    return isinstance(body, (dict, list)) and _error_envelope_code(body) is None


# Content-Type prefixes of a body that is never JSON. ponytail: a deny-list, not "JSON only" --
# plenty of hosts serve JSON as text/plain, text/html or text/javascript; add a type here when a
# binary download turns up under another name.
_BINARY_TYPES = (
    "application/octet-stream",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/pdf",
    "application/vnd.apache.parquet",
    "application/x-parquet",
    "image/",
    "audio/",
    "video/",
    "font/",
)


def _may_be_json(response: Any) -> bool:
    """False only when the response's Content-Type names a binary body, which is never decoded."""
    try:
        ctype = response.headers.get("Content-Type") or ""
    except AttributeError:
        return True
    return not (isinstance(ctype, str) and ctype.strip().lower().startswith(_BINARY_TYPES))


def no_espn_data(response: "requests.Response") -> "requests.Response":
    """Validate an ESPN response, raising :class:`NoDataError` if empty.

    Used by :func:`sportsdataverse.dl_utils.download` to normalize ESPN's
    two flavors of "no data" (HTTP 404 and 200-with-``code=404``-body)
    into a single exception type. The raised error message now includes
    a *next-action suggestion* derived from the URL (e.g. a 404 on
    ``/teams/99999/roster`` suggests ``find_team(name, league=...)``).

    Args:
        response: A ``requests.Response`` from an ESPN endpoint.

    Returns:
        The same ``response`` object unchanged when the payload is valid.

    Raises:
        NoDataError: When the response is a 404 or carries a
            ``code: 404`` JSON body.

    Example:
        Use directly when you have a hand-rolled ``requests`` call::

            import requests
            from sportsdataverse.errors import NoDataError, no_espn_data

            try:
                resp = no_espn_data(
                    requests.get("https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard")
                )
            except NoDataError:
                resp = None
    """
    if response.status_code == 404:
        raise NoDataError(_format_404(response.url))

    # ESPN's "200-but-empty" envelope is `{"code": 404, ...}`. Other
    # endpoints (e.g. jsonplaceholder.typicode.com/posts, stats.wnba.com
    # paginated payloads) return JSON arrays at the top level, where
    # ``.get("code")`` would raise AttributeError. Only probe for the
    # ESPN envelope when the body is a dict; non-dict bodies pass
    # through unchanged. A binary body (a release asset) is never decoded.
    if not _may_be_json(response):
        return response
    try:
        body = response.json()
    except ValueError:
        # Non-JSON response (e.g. raw bytes, HTML error page). Treat as
        # valid — the caller is responsible for parsing.
        return response

    # Only ``code: 404`` has been observed in a 200 envelope; other ESPN error codes
    # arrive with a matching HTTP status and are classified by the caller.
    if isinstance(body, dict) and body.get("code") == 404:
        raise NoDataError(_format_404(response.url, body=body))
    return response


# ---------------------------------------------------------------------------
# Suggestion engine — turn 404 URLs into actionable hints
# ---------------------------------------------------------------------------

_SPORT_TO_LEAGUE = {
    "basketball/nba": "nba",
    "basketball/wnba": "wnba",
    "basketball/mens-college-basketball": "mbb",
    "basketball/womens-college-basketball": "wbb",
    "football/nfl": "nfl",
    "football/college-football": "cfb",
    "baseball/mlb": "mlb",
    "hockey/nhl": "nhl",
}


def _infer_league(url: str) -> str | None:
    """Extract the league slug from an ESPN URL, if recognisable.

    Recognises both ESPN URL shapes:

    * ``site.api.espn.com/.../sports/<sport>/<league>/...`` — flat
    * ``sports.core.api.espn.com/v2/sports/<sport>/leagues/<league>/...``
      — nested under ``/leagues/``
    """
    for slug, league in _SPORT_TO_LEAGUE.items():
        sport, league_slug = slug.split("/", 1)
        # site.api shape: /sports/<sport>/<league>/
        if f"/sports/{slug}/" in url or f"/sports/{slug}?" in url:
            return league
        # core.api shape: /sports/<sport>/leagues/<league>/
        if f"/sports/{sport}/leagues/{league_slug}/" in url or f"/sports/{sport}/leagues/{league_slug}?" in url:
            return league
    return None


def suggest_next_action(url: str) -> str | None:
    """Return a human-readable next-action hint for a failed ESPN URL.

    Inspects the URL for the failing entity (team / athlete / event /
    season) and suggests the right ``find_*`` resolver call to recover.
    Returns ``None`` when no specific suggestion fits.

    Examples::

        suggest_next_action(
            "https://site.api.espn.com/.../basketball/nba/teams/99999/roster")
        "Try `find_team(name, league='nba')` to look up a valid team_id."

        suggest_next_action(
            "https://site.api.espn.com/.../baseball/mlb/athletes/0/overview")
        "Try `find_athlete(name, league='mlb', team=...)` ..."

        suggest_next_action(
            "https://site.api.espn.com/.../basketball/nba/summary?event=999")
        "Try `find_event(date, league='nba', home=..., away=...)` ..."
    """
    league = _infer_league(url) or "<league>"

    if _re.search(r"/teams/\d+/roster", url):
        return (
            f"Try `find_team(name, league={league!r})` to look up a valid team_id, then pass its `id` to the wrapper."
        )
    if _re.search(r"/teams/\d+/schedule", url):
        return (
            f"Try `find_team(name, league={league!r})` to look up a valid team_id, then pass its `id` to the wrapper."
        )
    if _re.search(r"/teams/\d+(?:/|$|\?)", url):
        return f"Try `find_team(name, league={league!r})` to verify the team_id."
    if _re.search(r"/athletes?/\d+", url):
        return f"Try `find_athlete(name, league={league!r}, team=<team>)` to look up a valid athlete_id."
    if _re.search(r"summary\?event=\d+", url) or _re.search(r"/events?/\d+", url):
        return f"Try `find_event(date, league={league!r}, home=..., away=...)` to look up a valid event_id."
    if _re.search(r"/seasons/(\d{4})", url):
        return (
            "Season ID may predate available data. Try a more recent "
            "season or check coverage on the league's docs page."
        )
    if "/scoreboard" in url:
        return (
            "Scoreboard returned no data for the given date — verify the "
            "`dates=YYYYMMDD` parameter is correct and the date had games."
        )
    return None


def _format_404(url: str, body: object | None = None) -> str:
    """Build a NoDataError message with an optional next-action hint."""
    base = f"NoDataError: No data found for {url}"
    if body is not None:
        base += f", response: {body}"
    hint = suggest_next_action(url)
    if hint:
        # Import find_* names in the message for absolute clarity. The
        # user gets the exact line to paste into a notebook.
        base += "\n\n  Suggestion: " + hint + "\n  (from `sportsdataverse import find_team, find_athlete, find_event`)"
    return base
