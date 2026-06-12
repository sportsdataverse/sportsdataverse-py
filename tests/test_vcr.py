"""Tests for the VCR-style record/replay harness (``tests/_vcr.py``).

Two layers, both offline:

* **Harness unit tests** -- scrubbing, match-key folding, the record->replay
  round trip (driven by a fake ``Session.request``, no network), cassette
  misses, and the secret-never-touches-disk invariant.
* **Demo replay tests** -- replay the two committed cassettes through the
  *real* call path (``download()`` -> ``no_espn_data()``), proving the harness
  exercises genuine package code with zero network.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import NoESPNDataError
from tests._vcr import (
    _REDACTED,
    RECORDING,
    CassetteMiss,
    _key,
    _scrub_url,
    use_cassette,
)
from tests.conftest import skip_if_no_live

# The live-recording smoke test needs BOTH a network (live) AND record mode --
# otherwise an SDV_PY_LIVE_TESTS=1 run would fire an extra live ESPN call and
# cassette write on every CI pass, defeating the offline-by-default design.
skip_if_no_record = pytest.mark.skipif(
    not RECORDING,
    reason="Set SDV_PY_RECORD=1 to run the live cassette-recording smoke test",
)

# ===========================================================================
# Scrubbing + match-key (pure)
# ===========================================================================


def test_scrub_url_redacts_secret_query_params() -> None:
    url = "https://api.the-odds-api.com/v4/sports/?apiKey=DEADBEEF&regions=us"
    out = _scrub_url(url)
    assert "DEADBEEF" not in out
    assert f"apiKey={_REDACTED}" in out
    assert "regions=us" in out  # non-secret params survive


def test_scrub_url_noop_without_query() -> None:
    url = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/194"
    assert _scrub_url(url) == url


def test_key_folds_url_query_and_params_and_is_order_invariant() -> None:
    # URL-embedded query + explicit params kwarg collapse to one canonical map,
    # and param ordering doesn't change the key.
    k1 = _key("get", "https://x.test/path?a=1", {"b": 2})
    k2 = _key("GET", "https://x.test/path?b=2", {"a": 1})
    assert k1 == k2
    assert k1[0] == "GET"  # method upper-cased
    assert k1[1] == "https://x.test/path"  # query stripped off base


def test_key_redacts_secret_and_drops_none() -> None:
    _, _, params_json = _key("GET", "https://x.test/?apiKey=SECRET", {"empty": None, "region": "us"})
    params = json.loads(params_json)
    assert params["apiKey"] == _REDACTED
    assert "empty" not in params  # None dropped
    assert params["region"] == "us"


# ===========================================================================
# Record -> replay round trip (fake Session.request, no network)
# ===========================================================================


def _fake_response(url: str, body: str, status: int = 200) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status
    resp._content = body.encode("utf-8")
    resp.url = url
    resp.encoding = "utf-8"
    resp.headers["Content-Type"] = "application/json"
    return resp


def test_record_then_replay_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cassette = tmp_path / "round_trip.json"
    url = "https://x.test/v4/data"
    payload = '{"value": 42}'

    # The "real" network is a fake that returns a canned body. use_cassette
    # captures requests.sessions.Session.request *at entry*, so patching it here
    # makes our fake the thing that gets recorded.
    def fake_request(self, method, url, **kwargs):  # noqa: ANN001, ANN201
        return _fake_response(url, payload)

    monkeypatch.setattr(requests.sessions.Session, "request", fake_request, raising=True)

    # Record.
    with use_cassette(cassette, record=True):
        resp = requests.get(url, params={"region": "us"})
        assert resp.json() == {"value": 42}
    assert cassette.exists()  # written on exit

    # Replay -- and make the fake explode so a cassette miss would be obvious.
    def exploding_request(self, method, url, **kwargs):  # noqa: ANN001, ANN201
        raise AssertionError("replay must not hit the (fake) network")

    monkeypatch.setattr(requests.sessions.Session, "request", exploding_request, raising=True)
    with use_cassette(cassette, record=False):
        resp = requests.get(url, params={"region": "us"})
        assert resp.json() == {"value": 42}
        assert resp.status_code == 200


def test_replay_miss_raises_cassettemiss(tmp_path: Path) -> None:
    empty = tmp_path / "empty.json"
    empty.write_text('{"version": 1, "interactions": []}', encoding="utf-8")
    with use_cassette(empty, record=False):
        with pytest.raises(CassetteMiss, match="No recorded interaction"):
            requests.get("https://x.test/never-recorded")


def test_secret_is_never_written_to_disk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cassette = tmp_path / "secret.json"
    # Deliberately a fake, non-credential literal -- never a real API key (real
    # keys must not appear in tracked files, even in a leak-prevention test).
    secret = "NOT_A_REAL_KEY_0123456789abcdef"  # noqa: S105
    url = "https://api.the-odds-api.com/v4/sports"

    def fake_request(self, method, url, **kwargs):  # noqa: ANN001, ANN201
        # Echo the secret back in response.url too, the way a real GET does.
        full = f"{url}?apiKey={kwargs['params']['apiKey']}"
        return _fake_response(full, '{"ok": true}')

    monkeypatch.setattr(requests.sessions.Session, "request", fake_request, raising=True)
    with use_cassette(cassette, record=True):
        requests.get(url, params={"apiKey": secret})

    raw = cassette.read_text(encoding="utf-8")
    assert secret not in raw, "API key leaked into a committed cassette!"
    assert _REDACTED in raw


# ===========================================================================
# Demo replay tests -- the REAL call path against committed cassettes (offline)
# ===========================================================================

_TEAM_BASE = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams"


def test_replay_download_happy_path() -> None:
    with use_cassette("espn_cfb_team_basic"):
        resp = download(f"{_TEAM_BASE}/194")
    assert resp.status_code == 200
    assert resp.json()["team"]["displayName"] == "Ohio State Buckeyes"


def test_replay_download_404_raises_no_espn_data() -> None:
    # Proves the live error path (download -> no_espn_data -> NoESPNDataError)
    # runs end-to-end offline. num_retries=0 so a replayed 404 doesn't spin the
    # backoff loop re-fetching the same recorded miss.
    with use_cassette("espn_cfb_team_missing"):
        with pytest.raises(NoESPNDataError):
            download(f"{_TEAM_BASE}/99999999", num_retries=0)


# ===========================================================================
# Live recording path (documentation + smoke; gated by BOTH live + record)
# ===========================================================================


@skip_if_no_live
@skip_if_no_record
def test_record_live_espn_team(tmp_path: Path) -> None:
    """Smoke-tests the record path against live ESPN, then replays its own capture.

    Gated by BOTH ``SDV_PY_LIVE_TESTS=1`` (it hits the network) AND
    ``SDV_PY_RECORD=1`` (it records) -- a plain live run stays offline-by-default
    and never fires this extra ESPN call. It records to a throwaway ``tmp_path``
    cassette -- it does NOT overwrite the committed demo cassettes -- then
    re-opens that capture in replay mode to confirm the round trip holds against
    a genuine payload.
    """
    cassette = tmp_path / "live_capture.json"
    with use_cassette(cassette, record=True):
        live = download(f"{_TEAM_BASE}/194").json()
    assert cassette.exists()
    with use_cassette(cassette, record=False):
        replayed = download(f"{_TEAM_BASE}/194").json()
    assert replayed["team"]["id"] == live["team"]["id"]
