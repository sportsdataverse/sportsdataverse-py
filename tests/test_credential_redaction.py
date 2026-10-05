"""A credential in a query string never reaches a log line or an exception.

Synthetic keys only. Three real paths through ``dl_utils.download``: The Odds API
(``apiKey``, private and paid), HockeyTech (``key``, through ``download`` itself),
and Fox through ``_codegen_runtime._get`` (``apikey``). Each runs into a 404, a 503
that outlives the retries, and a connection failure raised by the real
requests/urllib3 stack. Every channel is checked: the message, ``str``, ``repr``,
the formatted traceback with its chained causes, and every captured log record.
"""

from __future__ import annotations

import logging
import socket
import time
import traceback

import pytest
import requests
import urllib3.util.connection

from sportsdataverse import cache as _cache
from sportsdataverse import dl_utils
from sportsdataverse.errors import AssetFetchError, NoDataError, _redact_secrets
from sportsdataverse.fox import fox_api
from sportsdataverse.odds import the_odds_api as toa

SECRET = "zzSynthKey0123456789abcdef"  # synthetic; never a real key


def _the_odds_api():
    return toa.toa_sports(api_key=SECRET, return_parsed=False, num_retries=2)


def _hockeytech():
    params = {"feed": "modulekit", "view": "seasons", "key": SECRET, "client_code": "pwhl", "fmt": "json"}
    return dl_utils.download("https://lscluster.hockeytech.com/feed/index.php", params=params, num_retries=2)


def _fox():
    return fox_api.fox_api_scoreboard("nfl", apikey=SECRET, return_parsed=False, num_retries=2)


PATHS = {
    "the_odds_api": (_the_odds_api, "api.the-odds-api.com/v4/sports"),
    "hockeytech": (_hockeytech, "lscluster.hockeytech.com/feed/index.php"),
    "fox": (_fox, "api.foxsports.com/bifrost/v1/nfl/scoreboard/main"),
}


@pytest.fixture(autouse=True)
def _offline(monkeypatch, caplog):
    monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
    monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(var, raising=False)
    caplog.set_level(logging.DEBUG)


def _respond(status):
    def get(self, url, params=None, **kwargs):
        prepared = requests.Request("GET", url, params=params).prepare()
        resp = requests.Response()
        resp.status_code = status
        resp.reason = {404: "Not Found", 503: "Service Unavailable"}[status]
        resp._content = b'{"message": "upstream says no"}'
        resp.url = prepared.url
        resp.request = prepared
        return resp

    return get


def _leaks(exc, caplog):
    """The channels that carry SECRET (empty when none does)."""
    channels = {"logs": caplog.text + "\n".join(r.getMessage() for r in caplog.records)}
    if exc is not None:
        channels.update(
            message=str(exc.args[0]) if exc.args else "",
            str=str(exc),
            repr=repr(exc),
            traceback="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        )
    return sorted(name for name, text in channels.items() if SECRET in text)


@pytest.mark.parametrize("path", PATHS)
def test_404_names_the_url_without_the_key(path, monkeypatch, caplog):
    call, where = PATHS[path]
    monkeypatch.setattr(requests.Session, "get", _respond(404))
    with pytest.raises(NoDataError) as ei:
        call()
    assert _leaks(ei.value, caplog) == []
    # The user still sees which request it was, minus the credential.
    assert where in str(ei.value) and "=REDACTED" in str(ei.value)


@pytest.mark.parametrize("path", PATHS)
def test_503_past_the_retries_logs_without_the_key(path, monkeypatch, caplog):
    call, where = PATHS[path]
    monkeypatch.setattr(requests.Session, "get", _respond(503))
    try:
        call()
        exc = None
    except AssetFetchError as err:  # The Odds API raises; the other two hand back the response
        exc = err
    warned = [r.getMessage() for r in caplog.records if "503" in r.getMessage()]
    assert warned and all(where in line for line in warned)
    assert _leaks(exc, caplog) == []


@pytest.mark.parametrize("path", PATHS)
def test_connection_failure_and_its_chain_carry_no_key(path, monkeypatch, caplog):
    call, _ = PATHS[path]
    monkeypatch.setattr(urllib3.util.connection, "create_connection", _refuse)
    with pytest.raises(requests.exceptions.ConnectionError) as ei:
        call()
    exc = ei.value
    assert "Max retries exceeded" in str(exc)  # requests quoted the request path
    assert exc.__context__ is not None  # urllib3's MaxRetryError is still chained, just clean
    assert _leaks(exc, caplog) == []

    # A wrapper that re-raises ``from`` the transport error must not carry it either.
    try:
        raise AssetFetchError(f"fetch failed: {exc}") from exc
    except AssetFetchError as wrapped:
        assert _leaks(wrapped, caplog) == []


def _refuse(*args, **kwargs):
    raise socket.gaierror(11001, "getaddrinfo failed")


def test_the_exception_attributes_carry_no_credential(monkeypatch):
    # An error reporter that serializes attributes reads err.url, err.request.url
    # and err.request.headers, not just the message.
    monkeypatch.setattr(urllib3.util.connection, "create_connection", _refuse)
    headers = {"Authorization": f"Bearer {SECRET}", "Cookie": f"session={SECRET}", "Accept": "application/json"}
    with pytest.raises(requests.exceptions.ConnectionError) as ei:
        dl_utils.download(
            "https://api.the-odds-api.com/v4/sports", params={"apiKey": SECRET}, headers=headers, num_retries=0
        )
    request = ei.value.request
    assert request.url == "https://api.the-odds-api.com/v4/sports?apiKey=REDACTED"
    assert "Authorization" not in request.headers and "Cookie" not in request.headers
    assert request.headers["Accept"] == "application/json"  # the rest of the request is kept

    chain, todo = [], [ei.value]
    while todo:
        err = todo.pop()
        if err is not None and err not in chain:
            chain.append(err)
            todo += [err.__cause__, err.__context__]
    urls = [err.url for err in chain if isinstance(getattr(err, "url", None), str)]
    assert urls == ["/v4/sports?apiKey=REDACTED"]  # urllib3's MaxRetryError.url
    for err in chain:
        assert SECRET not in repr(vars(err))


def test_a_caller_retry_adapter_logs_without_the_key(monkeypatch, caplog):
    # A session mounted with a urllib3 ``Retry`` logs "Incremented Retry for
    # (url='/v4/sports?apiKey=...')" on urllib3.util.retry.
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0)))
    monkeypatch.setattr(urllib3.util.connection, "create_connection", _refuse)
    with pytest.raises(requests.exceptions.ConnectionError) as ei:
        toa.toa_sports(api_key=SECRET, return_parsed=False, num_retries=0, session=session)
    assert [r for r in caplog.records if r.name == "urllib3.util.retry"]  # it did log the URL
    assert _leaks(ei.value, caplog) == []


def test_urllib3_request_line_is_redacted(caplog):
    logging.getLogger("urllib3.connectionpool").debug(
        '%s://%s:%s "%s %s %s" %s %s',
        "https",
        "api.the-odds-api.com",
        443,
        "GET",
        f"/v4/sports?apiKey={SECRET}&all=false",
        "HTTP/1.1",
        200,
        None,
    )
    assert SECRET not in caplog.text and "/v4/sports?apiKey=REDACTED&all=false" in caplog.text


# Listed here, not imported from dl_utils: a test reading the module's own tuple
# could not notice a logger missing from it.
@pytest.mark.parametrize(
    "name", ["urllib3.connectionpool", "urllib3.connection", "urllib3.util.retry", "urllib3.poolmanager"]
)
def test_every_urllib3_logger_that_quotes_a_url_is_redacted(name, caplog):
    with caplog.at_level(logging.DEBUG, logger=name):
        logging.getLogger(name).warning("Failed to parse headers (url=%s)", f"/v4/sports?apiKey={SECRET}")
    assert [r for r in caplog.records if r.name == name]
    assert SECRET not in caplog.text and "apiKey=REDACTED" in caplog.text


def test_every_sdv_error_message_is_redacted():
    err = NoDataError(f"No data found for https://x.test/a?apiKey={SECRET}&season=2024")
    assert SECRET not in str(err) and "season=2024" in str(err)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("/v4/sports?apiKey=abc123&all=false", "/v4/sports?apiKey=REDACTED&all=false"),
        ("?api_key=abc&x=1", "?api_key=REDACTED&x=1"),
        ("?APIKEY=abc", "?APIKEY=REDACTED"),
        ("?apikey=jE7yBJVR&api-version=1.1", "?apikey=REDACTED&api-version=1.1"),
        ("?feed=modulekit&key=f1aa699db3d81487&fmt=json", "?feed=modulekit&key=REDACTED&fmt=json"),
        ("?token=t&access_token=a&client_secret=c", "?token=REDACTED&access_token=REDACTED&client_secret=REDACTED"),
        ("?password=hunter2 secret=s3", "?password=REDACTED secret=REDACTED"),
        # The punctuation closing an unquoted value stays.
        ('{"token": 12345}', '{"token": REDACTED}'),
        ("(password=hunter2), next", "(password=REDACTED), next"),
        ("{'key': 12345678901234567890, 'a': 1}", "{'key': REDACTED, 'a': 1}"),
        ("next=%2Fv4%3Fall%3Dx%26apiKey%3Dabc123", "next=%2Fv4%3Fall%3Dx%26apiKey%3DREDACTED"),
        ("{'feed': 'modulekit', 'key': 'f1aa699db3d81487'}", "{'feed': 'modulekit', 'key': 'REDACTED'}"),
        ('{"apiKey": "abc123", "regions": "us"}', '{"apiKey": "REDACTED", "regions": "us"}'),
        # A quoted value runs to its quote: spaces and closing punctuation inside are secret.
        ("{'password': 'a b', 'x': 1}", "{'password': 'REDACTED', 'x': 1}"),
        ('token="abc)" next', 'token="REDACTED" next'),
        # Left alone: a bare ``key`` needs a credential-length value, and a name
        # only counts at the start of a word.
        ("primary key=player_id", "primary key=player_id"),
        ("monkey=banana&keyboard=qwerty&pageToken=2", "monkey=banana&keyboard=qwerty&pageToken=2"),
    ],
)
def test_redact_secrets(text, expected):
    assert _redact_secrets(text) == expected
    assert _redact_secrets(expected) == expected  # idempotent


_ADVERSARIAL = {  # ~100 KB each
    "names": "key" * 34_000,
    "pairs": "apiKey=" * 15_000,
    "long_value": "key=" + "a" * 100_000,
    "spaces_no_separator": "apiKey'" + " " * 100_000 + "x",
    "name_space": "key " * 25_000,
    "escapes": "%26" * 34_000,
    "escaped_almost_pairs": "%26key%3" * 12_500,
    "plain": "a" * 100_000,
    "unclosed_quote": 'key="' + "a" * 100_000,
    "quoted_pairs": 'key="' * 20_000,
}


@pytest.mark.parametrize("shape", _ADVERSARIAL)
def test_redact_secrets_is_linear(shape):
    text = _ADVERSARIAL[shape]
    start = time.perf_counter()
    _redact_secrets(text)
    assert time.perf_counter() - start < 0.05
