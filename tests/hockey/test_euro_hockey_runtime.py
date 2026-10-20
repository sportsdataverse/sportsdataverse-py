"""The shared European hockey runtime spaces requests to one host at least MIN_INTERVAL apart."""

from __future__ import annotations

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as rt


@pytest.fixture
def clock(monkeypatch):
    """A fake monotonic clock: ``sleep`` advances it, so the test never waits."""
    state = {"now": 1000.0, "slept": []}
    monkeypatch.setattr(rt.time, "monotonic", lambda: state["now"])

    def sleep(s):
        state["slept"].append(s)
        state["now"] += s

    monkeypatch.setattr(rt.time, "sleep", sleep)
    monkeypatch.setattr(rt, "_last", {})
    return state


def test_second_call_to_a_host_waits_the_full_interval(clock):
    rt._polite_wait("https://liiga.fi/api/v2/games")
    rt._polite_wait("https://liiga.fi/api/v2/players")
    assert clock["slept"] == [pytest.approx(rt.MIN_INTERVAL)]


def test_other_hosts_do_not_wait(clock):
    rt._polite_wait("https://liiga.fi/api/v2/games")
    rt._polite_wait("https://www.shl.se/api/x")
    assert clock["slept"] == []


def test_no_wait_once_the_interval_has_passed(clock):
    rt._polite_wait("https://liiga.fi/a")
    clock["now"] += rt.MIN_INTERVAL
    rt._polite_wait("https://liiga.fi/b")
    assert clock["slept"] == []


def test_get_delegates_to_the_shared_getter_over_the_polite_session(monkeypatch):
    seen = {}
    monkeypatch.setattr(rt._rt, "_get", lambda url, params=None, **kw: seen.update(url=url, params=params, **kw) or {})
    rt._get("https://liiga.fi/api/v2/games", {"season": 2026}, timeout=5)
    assert seen["url"] == "https://liiga.fi/api/v2/games"
    assert seen["session"] is rt.SESSION


class _Resp:
    def __init__(self, status, body=b"{}"):
        self.status_code = status
        self.content = body
        self.text = body.decode()
        self.headers = {"Content-Type": "application/json"}
        self.url = "https://liiga.fi/api/v2/games"
        self.reason = "x"

    def json(self):
        import json

        return json.loads(self.content)


def test_every_session_request_waits_its_turn(clock, monkeypatch):
    monkeypatch.setattr(rt.requests.Session, "request", lambda self, method, url, *a, **k: _Resp(200))
    rt.SESSION.get("https://liiga.fi/api/v2/games")
    rt.SESSION.get("https://liiga.fi/api/v2/players")
    assert clock["slept"] == [pytest.approx(rt.MIN_INTERVAL)]


def test_download_retries_are_throttled_too(clock, monkeypatch):
    """A retry inside ``download()`` goes through the session, so it is spaced and recorded."""
    answers = iter([_Resp(503), _Resp(200)])
    sent = []

    def fake(self, method, url, *a, **k):
        sent.append(clock["now"])
        return next(answers)

    monkeypatch.setattr(rt.requests.Session, "request", fake)
    assert rt._get("https://liiga.fi/api/v2/games") == {}
    assert len(sent) == 2
    assert sent[1] - sent[0] >= rt.MIN_INTERVAL  # the retry waited for the host's slot
    assert rt._last["liiga.fi"] == pytest.approx(sent[1])  # and is the host's last request


# --- get_text: the one text-page getter (VHL HTML, Magnus game sheet page, ELH S3 listing) ------------------


class _Page:
    """A transport answer: bytes + headers (curl_cffi / requests shape)."""

    def __init__(self, body=b"<html>ok</html>", status=200, ctype="text/html; charset=utf-8", url=None, headers=None):
        self.content = body
        self.text = body.decode("utf-8", "replace")
        self.status_code = status
        self.headers = {"content-type": ctype, **(headers or {})}
        self.url = url


@pytest.fixture
def waits(monkeypatch):
    seen = {"waited": [], "slept": []}
    monkeypatch.setattr(rt, "_polite_wait", lambda url, *a: seen["waited"].append(url))
    monkeypatch.setattr(rt.time, "sleep", seen["slept"].append)
    return seen


def _transport(*answers):
    """A transport replaying ``answers`` (an exception is raised); records the URLs it was asked for."""
    queue, asked = list(answers), []

    def call(full):
        asked.append(full)
        answer = queue.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        if answer.url is None:
            answer.url = full
        return answer

    call.asked = asked
    return call


URL = "https://example.test/page"


def test_get_text_returns_the_decoded_page_and_waits_per_attempt(waits):
    t = _transport(_Page(b"<html>ol\xc3\xa9</html>"))
    assert rt.get_text(URL, {"a": 1, "b": None}, transport=t, expect="html") == "<html>ol\u00e9</html>"
    assert t.asked == [URL + "?a=1"] and waits["waited"] == [URL]  # None dropped, one polite wait


def test_get_text_retries_a_dead_connection_429_and_5xx_then_gives_up(waits):
    t = _transport(OSError("reset"), _Page(status=429, headers={"retry-after": "30"}), _Page())
    assert rt.get_text(URL, transport=t) == "<html>ok</html>"
    assert waits["slept"] == [2.0, 30.0] and len(waits["waited"]) == 3  # Retry-After lengthens the 429 wait
    t = _transport(*[_Page(status=503)] * 3)
    with pytest.raises(AssetFetchError, match="HTTP 503"):
        rt.get_text(URL, transport=t)
    t = _transport(*[OSError("down")] * 3)
    with pytest.raises(AssetFetchError, match="fetch failed after 3 attempts: OSError"):
        rt.get_text(URL, transport=t)


@pytest.mark.parametrize(
    ("answer", "error", "match"),
    [
        (_Page(status=404), NoDataError, "HTTP 404"),
        (_Page(status=422), ValueError, "rejected the request"),
        (_Page(status=403), AssetFetchError, "HTTP 403"),
        (_Page(status=204, body=b""), NoDataError, "HTTP 204: no content"),  # EX-SYNC-a: the caller's _get answers {}
        (_Page(ctype="application/json"), AssetFetchError, "non-HTML body"),
        (_Page(ctype=""), AssetFetchError, "no content type"),
        (_Page(body=b""), AssetFetchError, "empty body"),
        (_Page(body=b"  \n"), AssetFetchError, "empty body"),
        (_Page(body=b"\xff\xfe broken"), AssetFetchError, "not valid 'utf-8'"),
        (_Page(ctype="text/html; charset=nope"), AssetFetchError, "not valid 'nope'"),
        (_Page(body="caf\u00e9".encode("latin-1"), ctype="text/html; charset=utf-8"), AssetFetchError, "utf-8"),
        (_Page(body="\ufffd".encode("utf-8")), AssetFetchError, "replacement characters"),
    ],
)
def test_get_text_refusals_use_the_error_vocabulary(waits, answer, error, match):
    with pytest.raises(error, match=match):
        rt.get_text(URL, transport=_transport(answer), expect="html")


def test_get_text_strict_charset_decodes_what_the_site_declares(waits):
    page = _Page("caf\u00e9".encode("latin-1"), ctype="text/html; charset=ISO-8859-1")
    assert rt.get_text(URL, transport=_transport(page), expect="html") == "caf\u00e9"


def test_get_text_refuses_a_redirect_and_maps_the_not_found_page(waits):
    moved = _Page(url="https://example.test/404.php")
    with pytest.raises(NoDataError, match="no such page"):
        rt.get_text(URL, transport=_transport(moved), is_missing=lambda final: final.endswith("/404.php"))
    with pytest.raises(AssetFetchError, match="redirected to example.test/login"):
        rt.get_text(URL, transport=_transport(_Page(url="https://example.test/login")))
    with pytest.raises(AssetFetchError, match="an unknown URL"):
        rt.get_text(URL, transport=_transport(_Page(url="")))
    assert rt.get_text(URL, transport=_transport(_Page(url=URL + "/"))) == "<html>ok</html>"  # trailing slash only


def test_get_text_default_path_goes_through_download_over_the_polite_session(monkeypatch):
    seen = {}

    def download(url, params=None, **kw):
        seen.update(url=url, params=params, **kw)
        return _Page(b"<ListBucketResult/>", ctype="application/xml")

    monkeypatch.setattr(rt._rt, "download", download)
    assert rt.get_text(URL, {"prefix": "p", "marker": None}, expect="xml", timeout=7) == "<ListBucketResult/>"
    assert seen == {"url": URL, "params": {"prefix": "p"}, "session": rt.SESSION, "timeout": 7}
    monkeypatch.setattr(rt._rt, "download", lambda *a, **k: _Page(status=500))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        rt.get_text(URL)
    monkeypatch.setattr(rt._rt, "download", lambda *a, **k: (_ for _ in ()).throw(OSError("boom")))
    with pytest.raises(AssetFetchError, match="fetch failed after retries"):
        rt.get_text(URL)
