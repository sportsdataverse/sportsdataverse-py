from __future__ import annotations

import json
import logging
import re
import time
from itertools import chain, starmap

import numpy as np
import polars as pl
import requests

from sportsdataverse.errors import no_espn_data

logger = logging.getLogger("sdv.dl_utils")
logger.addHandler(logging.NullHandler())


def download(
    url, params=None, headers=None, proxy=None, timeout=30, num_retries=15, session=None, logger=None, cache_ttl=None,
):
    """Download a URL with retries and ESPN-aware error handling.

    Canonical HTTP gateway used by every wrapper in the package. Wraps
    :mod:`requests` with an exponential-style retry loop, raises
    :class:`~sportsdataverse.errors.NoESPNDataError` on ESPN 404 payloads,
    and surfaces transient failures through the supplied ``logger`` rather
    than raising.

    Args:
        url: Target URL.
        params: Query-string parameters as a ``dict``. Forwarded to
            ``requests.Session.get``.
        headers: Extra HTTP headers as a ``dict``.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape
            (e.g. ``{"http": "http://host:port", "https": "http://host:port"}``).
        timeout: Per-request timeout in seconds. Defaults to ``30``.
        num_retries: Maximum retries before giving up. Defaults to ``15``.
        session: Optional ``requests.Session`` to reuse. A fresh session
            is constructed when ``None``.
        logger: Optional ``logging.Logger``. Defaults to the package
            logger ``"sdv.dl_utils"``.

    Returns:
        The final ``requests.Response``.

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
    response = None
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            response = session.get(url, params=params, proxies=proxy, headers=headers, timeout=timeout)
            response = no_espn_data(response)
            # Persist successful responses to the cache. Catches any
            # body-parse error so a cache write never breaks the call.
            if get_cache_mode() != "off":
                try:
                    cache_set(url, _cache_params, response.json(), ttl=cache_ttl)
                except Exception:  # noqa: BLE001
                    pass
            return response
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
            time.sleep(2)

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
        session = requests.Session()

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
