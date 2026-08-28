from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from itertools import chain, starmap

import numpy as np
import polars as pl
import requests
from requests.adapters import HTTPAdapter

from sportsdataverse.errors import NoDataError, no_espn_data

logger = logging.getLogger("sdv.dl_utils")
logger.addHandler(logging.NullHandler())

# Module-level pooled session: reuses TCP connections across the many small
# requests a single workflow makes (e.g. a season crosswalk fires ~50). The
# larger pool sizes support concurrent fetching. urllib3's connection pool is
# thread-safe for concurrent GETs; callers needing isolation pass their own
# ``session=``.
_SHARED_SESSION = requests.Session()
for _scheme in ("https://", "http://"):
    _SHARED_SESSION.mount(_scheme, HTTPAdapter(pool_connections=16, pool_maxsize=32))


# Hard ceiling (seconds) on an honored ``Retry-After``. RFC 7231 lets a server
# name an arbitrarily long wait; we cap it so a stray (or hostile) header can't
# park a request in ``time.sleep`` for minutes. 120s comfortably covers real
# rate-limit windows while bounding worst-case latency.
_MAX_RETRY_AFTER = 120.0

# Transient HTTP status codes worth retrying. 429 (rate limit) + 408 (request
# timeout) + the 5xx server-error family are universally transient. 403 is
# included because ESPN's Core v2 API returns it *under load* (not an auth
# failure): ``download`` is the ESPN / nflverse gateway and does NOT serve the
# auth'd endpoints — ``nfl_api`` (bearer), ``nba_stats`` / ``wnba_stats``
# (curl_cffi runtime), and PFF (Clerk cookies) each have their own ``_get`` —
# so a 403 seen here is far more likely rate-limiting than a permanent
# forbidden. Retrying a genuine 403 only costs bounded backoff latency.
_RETRYABLE_STATUS = frozenset({403, 408, 429, 500, 502, 503, 504})

# Status retries reuse the loop but get their OWN small cap so a genuinely
# permanent 403/forbidden can't spin the full ``num_retries`` (16 requests /
# ~51s) before returning — and so we don't hammer a host already signalling
# "back off." Connection-exception retries still use the full ``num_retries``.
_MAX_STATUS_RETRIES = 4


def _parse_retry_after(value: str) -> float | None:
    """Parse a ``Retry-After`` value into seconds, or ``None`` if unparseable.

    Per RFC 7231 the header is either a non-negative integer count of seconds or
    an HTTP-date. Both forms are clamped at 0 — a numeric ``-5`` or an HTTP-date
    already in the past yields ``0.0`` rather than a negative sleep (which would
    raise ``ValueError`` in ``time.sleep`` and crash the retry loop).
    """
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:  # RFC dates are GMT; tolerate a naive parse
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def _retry_delay(
    response: object,
    attempt: int,
    *,
    base: float = 0.5,
    cap: float = 4.0,
    max_retry_after: float = _MAX_RETRY_AFTER,
    jitter: bool = True,
) -> float:
    """Seconds to wait before the next retry.

    Honors a ``Retry-After`` header (429 / 503 rate limiting) when present so we
    back off exactly as the server asks -- both the numeric-seconds and
    HTTP-date forms RFC 7231 permits -- bounded by ``max_retry_after`` so an
    outsized value can't park the request in ``time.sleep`` indefinitely. When
    the header is absent or unparseable, falls back to capped exponential
    backoff (gentler than a fixed sleep on quick-recovery transients, politer
    than hammering on persistent ones). ``jitter`` (default on) spreads the
    exponential fallback over 50-100% of the computed delay so concurrent
    workers don't re-hit a recovering host in lockstep (a thundering herd); the
    server-dictated ``Retry-After`` path is left exact.
    """
    headers = getattr(response, "headers", None)
    retry_after = headers.get("Retry-After") if headers else None
    if retry_after:
        secs = _parse_retry_after(retry_after)
        if secs is not None:
            return min(max_retry_after, secs)
    delay = min(cap, base * (2**attempt))
    if jitter:
        delay *= 0.5 + random.random() * 0.5
    return delay


#: Default retry budget and per-request timeout, overridable by environment so a
#: caller that cannot pass kwargs -- CI, a batch job, a notebook -- can bound how
#: long a hostile endpoint parks the process. The defaults are unchanged: 15
#: retries at a 30s timeout is right for a scraper that must not lose a game, but
#: it is badly wrong for a test suite, where one unreachable host can burn
#: 15 x 30s plus backoff on a single call. CI sets these low.
_ENV_TIMEOUT = "SDV_PY_HTTP_TIMEOUT"
_ENV_RETRIES = "SDV_PY_HTTP_RETRIES"
_DEFAULT_TIMEOUT = 30
_DEFAULT_RETRIES = 15


class _Unset:
    """Sentinel for "argument omitted", distinct from an explicit ``None``.

    ``timeout=None`` is meaningful to ``requests``: it means *no* timeout, wait
    forever. Defaulting the parameter to ``None`` would have quietly reinterpreted
    that as "use 30s", changing behaviour for any caller who passed it deliberately.
    No caller in this repo does -- both variable call sites are typed ``int`` -- but
    ``download`` is public API, so the two cases stay distinguishable.
    """

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<unset>"


_UNSET = _Unset()


def _env_int(name: str, default: int) -> int:
    """Read a positive int from the environment, falling back on anything unusable.

    A malformed or non-positive value is IGNORED rather than raising: this runs on
    every request, and a typo in an env var must not take the whole process down.

    Args:
        name: Environment variable to read.
        default: Value to use when unset, unparseable, or not positive.

    Returns:
        The parsed override, or ``default``.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def download(
    url,
    params=None,
    headers=None,
    proxy=None,
    timeout=_UNSET,
    num_retries=_UNSET,
    session=None,
    logger=None,
    cache_ttl=None,
    retry_statuses=_RETRYABLE_STATUS,
):
    """Download a URL with retries and ESPN-aware error handling.

    Canonical HTTP gateway used by every wrapper in the package. Wraps
    :mod:`requests` with an exponential-style retry loop, raises
    :class:`~sportsdataverse.errors.NoDataError` on any 404 -- an ESPN
    200-with-``code:404`` body included, and release assets on GitHub too --
    and surfaces transient failures through the supplied ``logger`` rather
    than raising.

    Args:
        url: Target URL.
        params: Query-string parameters as a ``dict``. Forwarded to
            ``requests.Session.get``.
        headers: Extra HTTP headers as a ``dict``.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape
            (e.g. ``{"http": "http://host:port", "https": "http://host:port"}``).
        timeout: Per-request timeout in seconds. When omitted, reads
            ``SDV_PY_HTTP_TIMEOUT`` and falls back to ``30``. Pass ``None``
            explicitly for NO timeout (``requests`` waits indefinitely) -- that is
            forwarded unchanged and does NOT consult the environment.
        num_retries: Maximum retries before giving up. When omitted or ``None``,
            reads ``SDV_PY_HTTP_RETRIES`` and falls back to ``15``. Unlike
            ``timeout``, ``None`` is treated as "omitted" -- there is no
            "infinite retries" reading for it to mean.
        session: Optional ``requests.Session`` to reuse. Defaults to the
            module-level pooled ``_SHARED_SESSION`` when ``None`` — its
            connection pool *and cookie jar* are shared across all calls that
            don't pass their own session. Pass an explicit ``requests.Session``
            when you need isolation (separate cookies / auth / proxy lifecycle).
        logger: Optional ``logging.Logger``. Defaults to the package
            logger ``"sdv.dl_utils"``.
        retry_statuses: Iterable of HTTP status codes to retry (with the same
            ``Retry-After``-aware backoff as connection failures). Defaults to
            :data:`_RETRYABLE_STATUS` (403/408/429/500/502/503/504). Pass a
            narrower set (e.g. ``{429, 503}``) for an auth'd endpoint where a
            403 is a real forbidden rather than ESPN-under-load.

    Raises:
        NoDataError: When the host answers 404, or ESPN answers 200 with a
            ``{"code": 404, ...}`` body. Not retried -- it is a definitive
            "no data", and retrying only adds load against a rate-limited host.
        requests.exceptions.RequestException: When the retry budget is exhausted
            on a connection-level failure; the most recent exception is re-raised.

    Note:
        Precedence for ``timeout`` / ``num_retries`` is **explicit argument >
        environment > default**. The environment is read at CALL time, not import
        time, so a test or CI step can set it after the module is imported. A
        malformed or non-positive env value is ignored in favour of the default
        rather than raising -- this runs on every request, so a typo must not take
        the process down.

    Returns:
        The final ``requests.Response``. When the retry budget is exhausted on
        a retryable status the last response is returned unchanged (callers key
        on ``.status_code``); a connection-level failure re-raises instead.

    Example:
        Quick start::

            from sportsdataverse.dl_utils import download

            url = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams"
            resp = download(url)
            data = resp.json()

        Custom retries and timeout::

            resp = download(url, num_retries=5, timeout=30)

        Add headers (e.g. for a stats.wnba.com endpoint that needs an Origin)::

            resp = download(
                "https://stats.wnba.com/stats/leaguedashteamstats",
                headers={
                    "Origin": "https://stats.wnba.com",
                    "Referer": "https://www.wnba.com/",
                    "User-Agent": "sportsdataverse-py",
                },
            )

        Pass query-string parameters with ``params=``::

            resp = download(
                "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
                params={"groups": "80", "dates": "20240907"},
            )

        See Also:
            * `requests`_ -- underlying HTTP library.
            * `httpx`_ -- modern async alternative if a future refactor
              wants concurrent fetching.

        .. _requests: https://requests.readthedocs.io
        .. _httpx: https://www.python-httpx.org
    """
    # Resolve HERE, not in the signature: reading the env at call time means a test
    # (or a CI step) can set it after import and still be honoured. Precedence is
    # explicit argument > environment > default. Only an OMITTED argument consults
    # the environment -- an explicit ``timeout=None`` still means "no timeout" and
    # is forwarded to requests unchanged.
    if timeout is _UNSET:
        timeout = _env_int(_ENV_TIMEOUT, _DEFAULT_TIMEOUT)
    if num_retries is _UNSET or num_retries is None:
        # `None` is accepted as "omitted" here, unlike `timeout`, because it has no
        # meaningful retry semantics -- there is no "infinite retries" reading. On
        # main it reached `int(None)` and raised TypeError, so this only widens what
        # is accepted; it takes nothing away.
        num_retries = _env_int(_ENV_RETRIES, _DEFAULT_RETRIES)

    session, params, logger = init_request_settings(params, session, logger)

    # Cache lookup (mode=off → no-op; LIVE-tier URLs → no-op).
    # On hit we return a CachedResponse shim that quacks like requests.Response
    # (.json() / .status_code / .text / .url / .headers) so every wrapper
    # downstream works unchanged.
    from sportsdataverse.cache import (
        CachedResponse,
        cache_get,
        cache_set,
        get_cache_mode,
    )

    if get_cache_mode() != "off":
        # Build a hashable params dict (drop None values + ignore the
        # batters_lookup[] convention which is request-shaped)
        _cache_params = {k: v for k, v in (params or {}).items() if v is not None}
        cached = cache_get(url, _cache_params, ttl=cache_ttl)
        if cached is not None:
            return CachedResponse(cached, url=url, status_code=200, from_cache=True)
    else:
        _cache_params = None

    # Iterative retry loop. Defensive `response = None` so the exception
    # handler can log without UnboundLocalError when `session.get()` itself
    # raises (timeout, connection reset, DNS, etc.) before binding
    # `response`. We re-raise the most recent captured exception when the
    # retry budget is exhausted instead of silently returning a stale or
    # unbound `response`.
    attempts = max(int(num_retries), 0) + 1
    status_budget = min(attempts - 1, _MAX_STATUS_RETRIES)
    response = None
    last_exc: Exception | None = None
    status_retries = 0
    for attempt in range(attempts):
        try:
            response = session.get(url, params=params, proxies=proxy, headers=headers, timeout=timeout)
            response = no_espn_data(response)
            # Transient status codes (rate-limit / server errors) come back as a
            # normal Response — `requests` does not raise on them. Retry with the
            # same Retry-After-aware backoff the connection-exception path uses,
            # capped at `status_budget` (see `_MAX_STATUS_RETRIES`); once spent,
            # return the response unchanged so callers can key on `.status_code`.
            status = getattr(response, "status_code", None)
            retryable = status in retry_statuses
            # `attempt < attempts - 1` is load-bearing, not redundant with the
            # budget: when status_budget == attempts-1 (small num_retries) a
            # status retry could otherwise `continue` on the FINAL iteration,
            # exhausting the loop into the post-loop `raise last_exc` — which
            # would raise a STALE connection exception from an earlier attempt
            # instead of returning this response. Never retry on the last attempt.
            if retryable and status_retries < status_budget and attempt < attempts - 1:
                status_retries += 1
                logger.warning(
                    "retryable status %s - %s for url (%s) [status retry %d/%d]",
                    status,
                    getattr(response, "reason", "?"),
                    getattr(response, "url", url),
                    status_retries,
                    status_budget,
                )
                time.sleep(_retry_delay(response, attempt))
                continue
            if retryable:
                # Budget spent: surface a terminal line so a persistent
                # 429/403/5xx isn't invisible in logs after the last retry.
                logger.warning(
                    "retryable status %s persisted after %d retr%s; returning to caller for url (%s)",
                    status,
                    status_retries,
                    "y" if status_retries == 1 else "ies",
                    getattr(response, "url", url),
                )
            # Persist only successful (2xx) responses to the cache — never cache a
            # 429/5xx body. Catches any body-parse error so a cache write never
            # breaks the call.
            if get_cache_mode() != "off" and status is not None and 200 <= status < 300:
                try:
                    cache_set(url, _cache_params, response.json(), ttl=cache_ttl)
                except Exception:  # noqa: BLE001
                    pass
            return response
        except NoDataError:
            # A 404 (or ESPN's 200-with-`code:404` body) is a definitive "no data"
            # answer — retrying cannot change it and only amplifies load against a
            # rate-limited host. Fail fast instead of burning the retry budget.
            raise
        except Exception as e:  # noqa: BLE001
            last_exc = e
            remaining = attempts - attempt - 1

            # Surface ESPN 404 explicitly; the wrapper layer keys on this.
            if hasattr(e, "code") and getattr(e, "code") == 404:
                logger.error(f"404: {url} \nparams: {params}")

            if remaining <= 0:
                logger.error(f"Retry Limit Exceeded: {url} \nparams: {params}\n {e}")
                break

            # Status / reason / final URL are only available when the
            # exception fired AFTER a response started landing (e.g.
            # `no_espn_data` raised on a 404). Otherwise — a connect /
            # read timeout fired before any response was bound — log
            # what we have.
            if response is not None:
                logger.warning(
                    "%s: %s - %s for url (%s) [retry %d/%d]",
                    e,
                    getattr(response, "status_code", "?"),
                    getattr(response, "reason", "?"),
                    getattr(response, "url", url),
                    attempt + 1,
                    num_retries,
                )
            else:
                logger.warning(
                    "%s for url (%s) [retry %d/%d]",
                    e,
                    url,
                    attempt + 1,
                    num_retries,
                )
            time.sleep(_retry_delay(response, attempt))

    # Retry budget exhausted. Re-raise the last captured exception so
    # callers can react (and the test suite can assert on the failure
    # mode) instead of receiving a stale or unbound `response`.
    if last_exc is not None:
        raise last_exc
    return response


def init_request_settings(params, session, logger):
    if params is None:
        params = {}

    if session is None:
        session = _SHARED_SESSION

    if logger is None:
        logger = logging.getLogger("sdv.dl_utils")
        logger.addHandler(logging.NullHandler())
    return session, params, logger


def flatten_json_iterative(dictionary, sep=".", ind_start=0):
    """Flatten a nested JSON dict into a single-level dict.

    Args:
        dictionary: Nested dict to flatten.
        sep: Separator used to join nested keys. Defaults to ``"."``.
        ind_start: Starting index for list elements. Defaults to ``0``.

    Returns:
        A flat ``dict`` with composite string keys.

    Example:
        Flatten a nested ESPN-shaped payload::

            from sportsdataverse.dl_utils import flatten_json_iterative

            payload = {
                "game": {
                    "id": "401628334",
                    "competitors": [{"team": "Ohio State"}, {"team": "Marshall"}],
                }
            }
            flat = flatten_json_iterative(payload)
            # {'game.id': '401628334',
            #  'game.competitors.0.team': 'Ohio State',
            #  'game.competitors.1.team': 'Marshall'}
    """

    def unpack_one(parent_key, parent_value):
        """Unpack one level (only one) of nesting in json file"""

        # Unpacking one level

        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                t1 = parent_key + sep + key

                yield t1, value

        elif isinstance(parent_value, list):
            i = ind_start

            for value in parent_value:
                t2 = parent_key + sep + str(i)

                i += 1

                yield t2, value
        else:
            yield parent_key, parent_value

    # Continue iterating the unpack_one function until the terminating condition is satisfied

    while True:
        # Continue unpacking the json file until all values are atomic elements (aka neither a dictionary nor a list)

        dictionary = dict(chain.from_iterable(starmap(unpack_one, dictionary.items())))

        # Terminating condition: none of the values in the json file are a dictionary or a list

        if not any(isinstance(value, dict) for value in dictionary.values()) and not any(
            isinstance(value, list) for value in dictionary.values()
        ):
            break

    return dictionary


def normalize_team_roster_columns(teams_df: pl.DataFrame) -> pl.DataFrame:
    """Normalize the raw ESPN team-detail frame used by the ``*_game_rosters`` helpers.

    Every league's ``helper_<lg>_team_items`` fetches a per-team payload from the
    ESPN core API, pops the heavyweight nested keys, then flattens what remains with
    :func:`pandas.json_normalize`. Historically the resulting frame was renamed by
    *position* (``teams_df.columns = [16 hard-coded names]``), which broke the moment
    ESPN added or dropped a top-level field for a given league/game (e.g.
    ``awards_$ref`` / ``coaches_$ref`` appearing for some college payloads -> a
    width-18 frame fed a width-16 name list -> ``ShapeError``).

    This helper renames by *source key* instead, so column drift is survivable:

    * extra columns ESPN adds (any leftover ``*_$ref`` / ``*_href`` reference link,
      or unknown fields) are dropped,
    * known fields are renamed to the canonical ``team_*`` schema regardless of
      their position, and
    * ``logos`` is preserved untouched for the caller's logo-flattening step.

    Args:
        teams_df: Flattened per-team frame whose columns are the raw ESPN keys
            (camelCase, possibly containing ``$ref``), e.g. ``id``, ``guid``,
            ``displayName``, ``alternateIds_sdr``, ``logos``, ``awards_$ref``.

    Returns:
        A polars DataFrame with the canonical team columns (those present in the
        input), plus ``logos`` when supplied. Column order follows the canonical
        schema, with ``logos`` last.

    Example:
        Used internally by every ``helper_<lg>_team_items``::

            from sportsdataverse.dl_utils import normalize_team_roster_columns
            teams_df = normalize_team_roster_columns(teams_df)
    """
    # Canonicalize raw keys: $ref -> href, camelCase -> snake_case. Matches the
    # convention the sibling roster/athlete helpers already use.
    teams_df = teams_df.rename({c: underscore(c.replace("$ref", "href")) for c in teams_df.columns})

    # Source (post-underscore) -> canonical target name.
    rename_map = {
        "id": "team_id",
        "guid": "team_guid",
        "uid": "team_uid",
        "slug": "team_slug",
        "location": "team_location",
        "name": "team_name",
        "nickname": "team_nickname",
        "abbreviation": "team_abbreviation",
        "display_name": "team_display_name",
        "short_display_name": "team_short_display_name",
        "color": "team_color",
        "alternate_color": "team_alternate_color",
        "is_active": "is_active",
        "is_all_star": "is_all_star",
        "alternate_ids_sdr": "team_alternate_ids_sdr",
    }
    present = {src: dst for src, dst in rename_map.items() if src in teams_df.columns}
    teams_df = teams_df.rename(present)

    # Keep only the canonical columns we know about (+ logos for downstream
    # flattening). Anything else ESPN added (e.g. awards_href, coaches_href) is
    # dropped instead of crashing a positional rename.
    keep = [dst for dst in rename_map.values() if dst in teams_df.columns]
    if "logos" in teams_df.columns:
        keep.append("logos")
    return teams_df.select(keep)


def key_check(obj, key, replacement=np.array([])):
    """Return ``obj[key]`` when present, otherwise ``replacement``.

    Convenience helper used throughout the parsers when an upstream JSON
    payload sometimes omits a field. Defaults the fallback to an empty
    NumPy array so the result slots into vectorized downstream code
    without a branch.

    Args:
        obj: Mapping-like object to look up in.
        key: Key to fetch.
        replacement: Value to return when ``key`` is missing.

    Example:
        Default empty-array fallback::

            from sportsdataverse.dl_utils import key_check

            payload = {"score": 21}
            key_check(payload, "score")
            # 21
            key_check(payload, "missing")
            # array([], dtype=float64)

        Custom fallback::

            key_check(payload, "missing", replacement=None)
            # None
    """
    return obj[key] if key in obj.keys() else replacement


@pl.api.register_dataframe_namespace("janitor")
class ColumnJanitor:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def clean_names(self) -> pl.DataFrame:
        return self._df.rename({c: underscore(c) for c in self._df.columns})

    def to_pascal_case(self) -> pl.DataFrame:
        return self._df.rename({c: camelize(c, True) for c in self._df.columns})

    def to_camel_case(self) -> pl.DataFrame:
        return self._df.rename({c: camelize(c, False) for c in self._df.columns})

    def to_kebab_case(self) -> pl.DataFrame:
        return self._df.rename({c: kebabize(c) for c in self._df.columns})


def underscore(word):
    """Make an underscored, lowercase form from the expression in the string.

    Roughly the inverse of :func:`camelize`, though edge cases (e.g.
    consecutive capitals like ``"IOError"``) do not perfectly round-trip.

    Example:
        Basic input -> output::

            from sportsdataverse.dl_utils import underscore

            underscore("DeviceType")
            # 'device_type'

        Round-trip caveat (capital runs do not survive)::

            from sportsdataverse.dl_utils import camelize, underscore

            camelize(underscore("IOError"))
            # 'IoError'
    """

    word = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", word)

    word = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", word)

    word = word.replace("-", "_")

    return word.lower()


def kebabize(word):
    """Make a kebab-case, lowercase form from the expression in the string.

    Sister function of :func:`underscore` -- same boundary detection, but
    emits dashes instead of underscores. Useful for URL slugs and
    hyphen-friendly schema labels.

    Example:
        Basic input -> output::

            from sportsdataverse.dl_utils import kebabize

            kebabize("DeviceType")
            # 'device-type'

            kebabize("home_team_score")
            # 'home-team-score'
    """

    word = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", word)

    word = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", word)

    word = word.replace("_", "-")

    return word.lower()


def camelize(string, uppercase_first_letter=True):
    """Convert strings to CamelCase.

    Args:
        string: Input identifier in ``snake_case`` (or any underscore-
            separated form).
        uppercase_first_letter: When ``True`` (default) emit
            ``UpperCamelCase``; when ``False`` emit ``lowerCamelCase``.

    Example:
        Default UpperCamelCase::

            from sportsdataverse.dl_utils import camelize

            camelize("device_type")
            # 'DeviceType'

        lowerCamelCase form::

            camelize("device_type", False)
            # 'deviceType'

        Round-trip with :func:`underscore` is not lossless for capital
        runs::

            from sportsdataverse.dl_utils import underscore

            camelize(underscore("IOError"))
            # 'IoError'
    """
    if uppercase_first_letter:
        return re.sub(r"(?:^|_)(.)", lambda m: m.group(1).upper(), string)
    else:
        return string[0].lower() + camelize(string)[1:]


class ESPNResponse:
    def __init__(self, response, status_code, url):
        self._response = response

        self._status_code = status_code

        self._url = url

    def get_response(self):
        return self._response

    def get_dict(self):
        return json.loads(self._response)

    def get_json(self):
        return json.dumps(self.get_dict())

    def valid_json(self):
        try:
            self.get_dict()

        except ValueError:
            return False

        return True

    def get_url(self):
        return self._url


class ESPNHTTP:
    espn_response = ESPNResponse

    base_url = None

    parameters = None

    headers = None

    def clean_contents(self, contents):
        return contents

    def send_api_request(
        self,
        endpoint,
        parameters,
        referer=None,
        headers=None,
        timeout=None,
        raise_exception_on_error=False,
    ):
        if not self.base_url:
            raise Exception("Cannot use send_api_request from _HTTP class.")

        base_url = self.base_url.format(endpoint=endpoint)

        endpoint = endpoint.lower()

        self.parameters = parameters

        request_headers = self.headers if headers is None else headers
        if referer:
            request_headers["Referer"] = referer

        url = None

        status_code = None

        contents = None

        # Sort parameters by key... for some reason this matters for some requests...

        parameters = sorted(parameters.items(), key=lambda kv: kv[0])

        if not contents:
            response = requests.get(url=base_url, params=parameters, headers=request_headers, timeout=timeout)

            url = response.url

            status_code = response.status_code

            contents = response.text

        contents = self.clean_contents(contents)

        data = self.espn_response(response=contents, status_code=status_code, url=url)

        if raise_exception_on_error and not data.valid_json():
            raise Exception("InvalidResponse: Response is not in a valid JSON format.")

        return data
