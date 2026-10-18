"""Offline tests for the tiered TTL response cache."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
import requests

# ---------------------------------------------------------------------------
# Tier picker
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://api-web.nhle.com/v1/scoreboard/now", "LIVE"),
        ("https://api-web.nhle.com/v1/scoreboard/TOR/now", "LIVE"),  # nhl_scoreboard(team=...) (B-M11)
        ("https://api-web.nhle.com/v1/standings/now", "LIVE"),
        ("https://api-web.nhle.com/v1/gamecenter/2023030417/play-by-play", "IMMUTABLE"),
        ("https://api-web.nhle.com/v1/gamecenter/2023030417/boxscore", "IMMUTABLE"),
        ("https://api.nhle.com/stats/rest/en/glossary", "IMMUTABLE"),
        ("https://records.nhl.com/site/api/player", "IMMUTABLE"),
        ("https://api-web.nhle.com/v1/draft/picks/2024/1", "REFERENCE"),
        ("https://sports.core.api.espn.com/v2/.../leagues/nba/venues", "REFERENCE"),
        ("https://site.api.espn.com/apis/site/v2/sports/.../news", "FAST"),
        ("https://site.api.espn.com/apis/site/v2/sports/.../injuries", "FAST"),
        ("https://site.api.espn.com/apis/site/v2/sports/.../teams/13/roster", "SLOW"),
        ("https://api-web.nhle.com/v1/player/8478402/landing", "SLOW"),
        ("https://api.nhle.com/stats/rest/en/leaders/skaters/points", "MODERATE"),
    ],
)
def test_pick_ttl_classifies_by_url(url, expected):
    from sportsdataverse import cache

    tier_map = {
        "LIVE": cache.LIVE,
        "IMMUTABLE": cache.IMMUTABLE,
        "REFERENCE": cache.REFERENCE,
        "SLOW": cache.SLOW,
        "MODERATE": cache.MODERATE,
        "FAST": cache.FAST,
    }
    assert cache.pick_ttl(url) == tier_map[expected], f"{url} -> expected {expected}, got {cache.pick_ttl(url)}"


def test_pick_ttl_past_scoreboard_is_immutable():
    """A scoreboard URL with dates=YYYYMMDD for a past date should be
    cacheable forever (game results don't change)."""
    from sportsdataverse import cache

    # Date guaranteed in the past
    url = "https://site.api.espn.com/.../basketball/nba/scoreboard?dates=20200101"
    # Pass a fixed "today" so the test is deterministic
    today = datetime(2026, 5, 26, tzinfo=timezone.utc)
    assert cache.pick_ttl(url, today=today) == cache.IMMUTABLE


def test_pick_ttl_future_scoreboard_is_live():
    """A scoreboard URL with a future date — no point caching it, the
    games haven't been played yet."""
    from sportsdataverse import cache

    url = "https://site.api.espn.com/.../basketball/nba/scoreboard?dates=20990101"
    today = datetime(2026, 5, 26, tzinfo=timezone.utc)
    assert cache.pick_ttl(url, today=today) == cache.LIVE


@pytest.mark.parametrize(
    "dates,expected",
    [
        ("20260524-20260525", "IMMUTABLE"),
        ("20260525-20260526", "LIVE"),
        ("20260525-20260527", "LIVE"),
    ],
)
def test_pick_ttl_scoreboard_date_range(dates, expected):
    from sportsdataverse import cache

    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={dates}&limit=500"
    today = datetime(2026, 5, 26, tzinfo=timezone.utc)
    assert cache.pick_ttl(url, today=today) == getattr(cache, expected)


# ---------------------------------------------------------------------------
# Mode + read/write
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_cache_dir(monkeypatch, tmp_path):
    """Point the filesystem cache at a temp dir."""
    monkeypatch.setenv("SDV_PY_CACHE_DIR", str(tmp_path))
    yield tmp_path


@pytest.fixture
def reset_cache():
    """Reset cache state before AND after each test."""
    from sportsdataverse import cache

    original_mode = cache.get_cache_mode()
    cache._MEMORY_CACHE.clear()
    cache.set_cache_mode("off")
    yield
    cache.set_cache_mode("off")
    cache._MEMORY_CACHE.clear()
    cache.set_cache_mode(original_mode)


@pytest.fixture
def cache_clock(monkeypatch):
    from sportsdataverse import cache

    now = [datetime(2026, 5, 26, tzinfo=timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now[0]

    monkeypatch.setattr(cache, "datetime", Clock)
    return now


def test_set_cache_mode_rejects_invalid(reset_cache):
    from sportsdataverse import cache

    with pytest.raises(ValueError, match="Invalid cache mode"):
        cache.set_cache_mode("disk")


def test_memory_cache_round_trip(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set("https://x.example/a", None, {"hello": "world"})
    assert cache.cache_get("https://x.example/a") == {"hello": "world"}
    assert cache.cache_get("https://x.example/b") is None  # miss


def test_filesystem_cache_round_trip(tmp_cache_dir, reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("filesystem")
    cache.cache_set("https://x.example/a", {"q": 1}, {"data": [1, 2, 3]})
    out = cache.cache_get("https://x.example/a", {"q": 1})
    assert out == {"data": [1, 2, 3]}


def test_cache_params_are_order_insensitive(reset_cache):
    """The cache key should be the same for ?a=1&b=2 and ?b=2&a=1."""
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set("https://x.example/a", {"a": 1, "b": 2}, {"key": "ab"})
    assert cache.cache_get("https://x.example/a", {"b": 2, "a": 1}) == {"key": "ab"}


def test_cache_off_mode_short_circuits(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("off")
    cache.cache_set("https://x.example/a", None, {"value": 1})
    assert cache.cache_get("https://x.example/a") is None
    # And the memory dict stayed empty
    assert not cache._MEMORY_CACHE


def test_live_tier_bypasses_cache(reset_cache):
    """URLs that pick_ttl classifies as LIVE should never be persisted."""
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set(
        "https://api-web.nhle.com/v1/scoreboard/now",
        None,
        {"games": []},
    )
    assert (
        cache.cache_get(
            "https://api-web.nhle.com/v1/scoreboard/now",
        )
        is None
    )


def test_per_call_ttl_override_wins(reset_cache):
    """A caller-supplied ttl= overrides the tier picker."""
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    # This URL would normally be LIVE (no cache); force it to 1h
    cache.cache_set(
        "https://api-web.nhle.com/v1/scoreboard/now",
        None,
        {"forced": True},
        ttl=timedelta(hours=1),
    )
    out = cache.cache_get(
        "https://api-web.nhle.com/v1/scoreboard/now",
        ttl=timedelta(hours=1),
    )
    assert out == {"forced": True}


def test_clear_cache_by_url(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set("https://x.example/a", None, {"a": 1})
    cache.cache_set("https://x.example/b", None, {"b": 2})
    n = cache.clear_cache(url="https://x.example/a")
    assert n == 1
    assert cache.cache_get("https://x.example/a") is None
    assert cache.cache_get("https://x.example/b") == {"b": 2}


def test_clear_cache_all(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    for i in range(5):
        cache.cache_set(f"https://x.example/{i}", None, {"i": i})
    n = cache.clear_cache()
    assert n == 5
    assert cache.cache_stats()["entries"] == 0


def test_cache_stats_reflects_mode(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    stats = cache.cache_stats()
    assert stats["mode"] == "memory"
    assert stats["entries"] == 0
    cache.cache_set("https://x.example/a", None, {"v": 1})
    assert cache.cache_stats()["entries"] == 1


# ---------------------------------------------------------------------------
# download() integration: cache hit returns CachedResponse
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["memory", "filesystem"])
@pytest.mark.parametrize("dates", [20260526, "20260527", "20260525-20260526", "20260525-20260527"])
def test_scoreboard_query_dates_refresh(mode, dates, monkeypatch, tmp_cache_dir, reset_cache, cache_clock):
    from sportsdataverse import cache
    from sportsdataverse.nba.nba_espn_ext import espn_nba_scoreboard

    cache.set_cache_mode(mode)
    responses = []
    for event_id in (1, 2):
        response = requests.Response()
        response.status_code = 200
        response._content = f'{{"events": [{{"id": "{event_id}"}}]}}'.encode()
        responses.append(response)
    get = Mock(side_effect=responses)
    monkeypatch.setattr("requests.Session.get", get)

    first = espn_nba_scoreboard(dates=dates, return_parsed=False)
    second = espn_nba_scoreboard(dates=dates, return_parsed=False)

    assert first == {"events": [{"id": "1"}]}
    assert second == {"events": [{"id": "2"}]}
    assert get.call_count == 2
    assert get.call_args.kwargs["params"]["dates"] == dates
    assert cache.cache_stats()["entries"] == 0


@pytest.mark.parametrize("mode", ["memory", "filesystem"])
@pytest.mark.parametrize("dates", ["20260526", "20260525-20260527"])
def test_scoreboard_query_dates_bypass_existing_entry(mode, dates, tmp_cache_dir, reset_cache, cache_clock):
    from sportsdataverse import cache

    cache.set_cache_mode(mode)
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?limit=500"
    params = {"dates": dates}
    cache.cache_set(url, params, {"stale": True}, ttl=timedelta(hours=1))
    assert cache.cache_get(url, params) is None
    assert cache.cache_get(url, params, ttl=timedelta(hours=1)) == {"stale": True}
    assert params == {"dates": dates}


@pytest.mark.parametrize("mode", ["memory", "filesystem"])
@pytest.mark.parametrize("dates", [20260525, "20260524-20260525"])
def test_scoreboard_query_dates_keep_historical_ttl(mode, dates, tmp_cache_dir, reset_cache, cache_clock):
    from sportsdataverse import cache

    cache.set_cache_mode(mode)
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
    params = {"dates": dates, "groups": None}
    cache.cache_set(url, params, {"historical": True})
    cache_clock[0] += timedelta(hours=2)
    assert cache.cache_get(url, params) == {"historical": True}
    assert params == {"dates": dates, "groups": None}


def test_download_returns_cached_response_on_hit(reset_cache):
    """When the cache has an entry for the URL, download() must return a
    CachedResponse shim WITHOUT making a network call."""
    from sportsdataverse import cache
    from sportsdataverse.dl_utils import download

    cache.set_cache_mode("memory")
    cache.cache_set(
        "https://api.example.com/teams",
        None,
        {"teams": [{"id": 1}]},
    )
    resp = download("https://api.example.com/teams")
    assert isinstance(resp, cache.CachedResponse)
    assert resp.from_cache is True
    assert resp.json() == {"teams": [{"id": 1}]}
    assert resp.status_code == 200


def test_download_offmode_does_not_consult_cache(monkeypatch):
    """With mode=off, the cache layer must never short-circuit."""
    from sportsdataverse import cache

    cache.set_cache_mode("off")
    cache._MEMORY_CACHE.clear()
    cache._MEMORY_CACHE["fake-key"] = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "url": "https://api.example.com/teams",
        "body": {"DO_NOT_RETURN": True},
    }
    # If the off-mode short-circuit fails, we'd hit the network — but with
    # the network mocked to raise, we'd see the exception
    import sportsdataverse.dl_utils as dl_utils

    called = {"hit": False}

    def fake_get(self, url, **kw):
        called["hit"] = True
        raise RuntimeError("would have hit network")

    monkeypatch.setattr("requests.Session.get", fake_get)
    with pytest.raises(RuntimeError, match="would have hit network"):
        dl_utils.download("https://api.example.com/teams", num_retries=0)
    assert called["hit"], "download should hit network when mode=off"


# ---------------------------------------------------------------------------
# Credentials are scrubbed before the entry is stored
# ---------------------------------------------------------------------------

_KEY = "abcd1234efgh5678"


@pytest.mark.parametrize("mode", ["memory", "filesystem"])
def test_cache_set_scrubs_credentials_before_storing(mode, tmp_cache_dir, reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode(mode)
    url = f"https://x.example/feed?apiKey={_KEY}&season=2025"
    body = {
        "self": f"https://x.example/feed?apiKey={_KEY}",
        "echo": {"note": f"called with {_KEY}"},
        "rows": [{"key": "regular-season-rank", "n": 3}],
    }
    cache.cache_set(url, None, body)
    stored = cache._MEMORY_CACHE if mode == "memory" else {p.name: p.read_text() for p in tmp_cache_dir.glob("*.json")}
    assert _KEY not in repr(stored)
    out = cache.cache_get(url)  # read-through still keyed on the real URL
    assert out["self"] == "https://x.example/feed?apiKey=REDACTED"
    assert out["echo"]["note"] == "called with REDACTED"
    assert out["rows"] == [{"key": "regular-season-rank", "n": 3}]  # ordinary data untouched
    assert body["self"].endswith(_KEY)  # the live body is not mutated


def test_cache_set_scrubs_path_key_url(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set(f"https://x.example/api/v1/json/{_KEY}/teams.php", None, {"ok": 1})
    assert _KEY not in repr(cache._MEMORY_CACHE)


def test_cache_set_clean_body_unchanged(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    body = {"events": [{"id": "1", "name": "A at B", "link": "https://x.example/e?id=1&k=v"}], "n": 2, "x": None}
    cache.cache_set("https://x.example/clean", {"dates": 2025}, body)
    assert cache.cache_get("https://x.example/clean", {"dates": 2025}) == body
    (entry,) = cache._MEMORY_CACHE.values()
    assert entry["url"] == "https://x.example/clean"


def test_cache_set_scrubs_a_key_sent_as_a_param(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    params = {"apiKey": _KEY, "regions": "us"}
    cache.cache_set("https://x.example/odds", params, {"echo": {"apiKey": _KEY}, "regions": "us"})
    assert _KEY not in repr(cache._MEMORY_CACHE)
    assert cache.cache_get("https://x.example/odds", params) == {"echo": {"apiKey": "REDACTED"}, "regions": "us"}


def test_cache_hit_equals_live_body_for_credential_like_text(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    body = {"note": "key: 1234 token=abc", "title": "password=reset day"}
    cache.cache_set("https://x.example/text", None, body)
    assert cache.cache_get("https://x.example/text") == body  # only URL-like strings are pattern-redacted


# ---------------------------------------------------------------------------
# Task 4.7: a keyed URL never reaches the cache dir; an authenticated request is never cached
# (every credential below is a fake)
# ---------------------------------------------------------------------------

_FAKE_SECRET = "FAKE-not-a-real-credential-0001"


def _count_gets(monkeypatch, body):
    """Stub ``requests.Session.get`` with a 200 JSON answer; returns the list of calls."""
    import json as _json

    calls = []

    def fake_get(self, url, params=None, headers=None, **kwargs):
        calls.append((url, dict(headers or {})))
        resp = requests.Response()
        resp.status_code = 200
        resp._content = _json.dumps(body).encode("utf-8")
        resp.headers["Content-Type"] = "application/json"
        resp.url = url
        return resp

    monkeypatch.setattr("requests.Session.get", fake_get)
    return calls


def test_redact_keyed_thesportsdb_entry_never_reaches_disk(monkeypatch, tmp_cache_dir, reset_cache):
    """A-N1 / A-L66: the premium key rides in the URL path; the filesystem entry holds none of it."""
    from sportsdataverse import cache
    from sportsdataverse.thesportsdb import thesportsdb_runtime as rt

    cache.set_cache_mode("filesystem")
    monkeypatch.setenv("THESPORTSDB_API_KEY", _FAKE_SECRET)
    body = {"sports": [{"idSport": "102", "strSport": "Soccer"}]}
    calls = _count_gets(monkeypatch, body)
    url = "https://www.thesportsdb.com/api/v1/json/{key}/all_sports.php"
    assert rt._get(url) == body
    assert _FAKE_SECRET in calls[0][0]  # the key was sent ...
    on_disk = "".join(p.read_text(encoding="utf-8") for p in tmp_cache_dir.glob("*.json"))
    assert on_disk and _FAKE_SECRET not in on_disk  # ... and never stored
    assert rt._get(url) == body and len(calls) == 1  # still a read-through hit


_AUTH_REQUESTS = {
    "authorization": {"headers": {"Authorization": f"Bearer {_FAKE_SECRET}"}},
    "cookie": {"headers": {"Cookie": f"_premium_key={_FAKE_SECRET}"}},
    "api-key-header": {"headers": {"X-Api-Key": _FAKE_SECRET}},
}


@pytest.mark.parametrize("mode", ["memory", "filesystem"])
@pytest.mark.parametrize("how", [*_AUTH_REQUESTS, "session-cookie"])
def test_no_cache_authenticated_request(how, mode, monkeypatch, tmp_cache_dir, reset_cache):
    """A-M8: the cache key holds no credential, so an authenticated body would be served to a
    second credential; such a request is fetched every time and nothing is stored."""
    from sportsdataverse import cache
    from sportsdataverse.dl_utils import download

    cache.set_cache_mode(mode)
    calls = _count_gets(monkeypatch, {"entitled": True})
    if how == "session-cookie":
        sess = requests.Session()
        sess.cookies.set("sessionid", _FAKE_SECRET)
        kwargs = {"session": sess}
    else:
        kwargs = dict(_AUTH_REQUESTS[how])
    for _ in range(2):
        assert download("https://api.example.com/premium", **kwargs).json() == {"entitled": True}
    assert len(calls) == 2
    assert not cache._MEMORY_CACHE and not list(tmp_cache_dir.glob("*.json"))


def test_no_cache_authenticated_leaves_plain_requests_cached(monkeypatch, reset_cache):
    from sportsdataverse import cache
    from sportsdataverse.dl_utils import download

    cache.set_cache_mode("memory")
    calls = _count_gets(monkeypatch, {"ok": 1})
    for _ in range(2):
        download("https://api.example.com/public", headers={"User-Agent": "x", "Accept": "application/json"})
    assert len(calls) == 1


def test_no_cache_authenticated_runtimes(monkeypatch, reset_cache):
    """The legacy cookie PFF transport and the bearer api.nfl.com getter go through download()
    with their credential in a header: neither body is cached."""
    from sportsdataverse import cache
    from sportsdataverse.nfl import nfl_api_runtime, pff_runtime
    from sportsdataverse.nfl.nfl_games import nfl_headers_gen

    cache.set_cache_mode("memory")
    calls = _count_gets(monkeypatch, {"data": []})
    for _ in range(2):
        pff_runtime._default_transport("https://premium.pff.com/api/v1/x", {}, {}, {"_premium_key": _FAKE_SECRET})
        nfl_api_runtime._get("https://api.nfl.com/football/v2/x", headers=nfl_headers_gen(token=_FAKE_SECRET))
    assert len(calls) == 4
    assert not cache._MEMORY_CACHE


def test_no_cache_authenticated_runtimes_spell_it_cache_ttl_0(monkeypatch, reset_cache):
    """The basketball runtimes pass ``cache_ttl=0`` (an int): with a cache mode on that reached
    ``.total_seconds()`` and raised AttributeError before any request was made."""
    from sportsdataverse import cache
    from sportsdataverse.dl_utils import download

    cache.set_cache_mode("memory")
    calls = _count_gets(monkeypatch, {"ok": 1})
    for _ in range(2):
        assert download("https://api.example.com/x", cache_ttl=0).json() == {"ok": 1}
    assert len(calls) == 2 and not cache._MEMORY_CACHE
    download("https://api.example.com/y", cache_ttl=3600)  # seconds work too
    download("https://api.example.com/y", cache_ttl=3600)
    assert len(calls) == 3
