"""Offline tests for the The Odds API wrappers + parsers.

All network is mocked (``the_odds_api.download`` is monkeypatched), so nothing
here hits ``api.the-odds-api.com``. Covers: API-key resolution (arg / env /
missing), the long-format odds flattening, ``return_parsed`` / ``return_as_pandas``
toggles, usage-header capture, historical snapshot unwrapping, and the empty /
malformed payload paths.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.odds import the_odds_api as toa
from sportsdataverse.odds import the_odds_api_parsers as toap

_ODDS_EVENT = {
    "id": "e1",
    "sport_key": "americanfootball_nfl",
    "sport_title": "NFL",
    "commence_time": "2024-09-08T17:00:00Z",
    "home_team": "Houston Texans",
    "away_team": "Kansas City Chiefs",
    "bookmakers": [
        {
            "key": "draftkings",
            "title": "DraftKings",
            "last_update": "2024-09-08T16:00:00Z",
            "markets": [
                {
                    "key": "h2h",
                    "last_update": "2024-09-08T16:00:00Z",
                    "outcomes": [
                        {"name": "Houston Texans", "price": 2.23},
                        {"name": "Kansas City Chiefs", "price": 1.45},
                    ],
                },
            ],
        },
    ],
}


class _FakeResp:
    def __init__(self, payload, headers=None):
        self._payload = payload
        self.headers = headers or {"x-requests-remaining": "491", "x-requests-used": "9", "x-requests-last": "1"}

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Default to a known key; reset the usage cache between tests."""
    monkeypatch.setenv("ODDS_API_KEY", "test-key")
    toa._USAGE.update({"requests_remaining": None, "requests_used": None, "last_cost": None})
    yield


def _patch_download(monkeypatch, payload, headers=None, capture=None):
    def fake_download(url, params=None, **kwargs):
        if capture is not None:
            capture["url"] = url
            capture["params"] = params
        return _FakeResp(payload, headers)

    monkeypatch.setattr(toa, "download", fake_download)


def test_key_missing_raises(monkeypatch):
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    with pytest.raises(ValueError, match="Odds API key"):
        toa.toa_sports()


def test_key_from_arg_overrides_env(monkeypatch):
    cap = {}
    _patch_download(monkeypatch, [], capture=cap)
    toa.toa_sports(api_key="explicit-key", return_parsed=False)
    assert cap["params"]["apiKey"] == "explicit-key"


def test_key_from_env(monkeypatch):
    cap = {}
    _patch_download(monkeypatch, [], capture=cap)
    toa.toa_sports(return_parsed=False)
    assert cap["params"]["apiKey"] == "test-key"


def test_none_params_stripped(monkeypatch):
    cap = {}
    _patch_download(monkeypatch, [_ODDS_EVENT], capture=cap)
    toa.toa_sports_odds(sport="americanfootball_nfl", regions="us", return_parsed=False)
    # bookmakers/eventIds left None -> must not appear on the wire
    assert "bookmakers" not in cap["params"]
    assert "eventIds" not in cap["params"]
    assert cap["params"]["regions"] == "us"


def test_odds_long_format(monkeypatch):
    _patch_download(monkeypatch, [_ODDS_EVENT])
    df = toa.toa_sports_odds(sport="americanfootball_nfl", regions="us")
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2  # one row per outcome
    for col in ("event_id", "bookmaker_key", "market_key", "outcome_name", "outcome_price"):
        assert col in df.columns
    assert sorted(df["outcome_price"].to_list()) == [1.45, 2.23]


def test_return_parsed_false_returns_raw(monkeypatch):
    _patch_download(monkeypatch, [_ODDS_EVENT])
    raw = toa.toa_sports_odds(return_parsed=False)
    assert isinstance(raw, list)
    assert raw[0]["id"] == "e1"


def test_return_as_pandas(monkeypatch):
    _patch_download(monkeypatch, [_ODDS_EVENT])
    df = toa.toa_sports_odds(return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)


def test_usage_headers_captured(monkeypatch):
    _patch_download(monkeypatch, [_ODDS_EVENT])
    toa.toa_sports_odds()
    usage = toa.toa_usage()
    assert usage["requests_remaining"].to_list() == [491]
    assert usage["requests_used"].to_list() == [9]


def test_event_odds_single_object(monkeypatch):
    _patch_download(monkeypatch, _ODDS_EVENT)  # single event dict, not a list
    df = toa.toa_event_odds(sport="americanfootball_nfl", event_id="e1", regions="us")
    assert df.height == 2
    assert df["event_id"].unique().to_list() == ["e1"]


def test_event_markets_parsing(monkeypatch):
    _patch_download(monkeypatch, _ODDS_EVENT)
    df = toa.toa_event_markets(sport="americanfootball_nfl", event_id="e1", regions="us")
    assert df.height == 1  # one bookmaker x one market
    assert df["market_key"].to_list() == ["h2h"]


def test_historical_snapshot_unwrapped_and_stamped(monkeypatch):
    snapshot = {
        "timestamp": "2023-11-29T22:45:00Z",
        "previous_timestamp": "2023-11-29T22:40:00Z",
        "next_timestamp": "2023-11-29T22:50:00Z",
        "data": [_ODDS_EVENT],
    }
    _patch_download(monkeypatch, snapshot)
    df = toa.toa_sports_odds_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z", regions="us")
    assert df.height == 2
    assert df["snapshot_timestamp"].unique().to_list() == ["2023-11-29T22:45:00Z"]


def test_parse_empty_payloads():
    assert toap.parse_toa_odds([]).shape == (0, 0)
    assert toap.parse_toa_sports([]).shape == (0, 0)
    assert toap.parse_toa_odds(None).shape == (0, 0)


def test_flatten_odds_keeps_eventless_bookmakers():
    # An event with no bookmakers still yields one bare row (not dropped).
    rows = toap._flatten_odds([{"id": "x", "home_team": "A", "away_team": "B"}])
    assert len(rows) == 1
    assert rows[0]["event_id"] == "x"


def test_bool_str():
    assert toa._bool_str(True) == "true"
    assert toa._bool_str(False) == "false"
    assert toa._bool_str(None) is None
