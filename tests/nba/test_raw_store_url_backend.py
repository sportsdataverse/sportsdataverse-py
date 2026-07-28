"""Offline tests for the raw-store URL backend + the period one-file read fix.

No network: the URL backend's HTTP getter is monkeypatched, filesystem cases use
tmp_path. Covers the clone-free CI read path (compile off a committed raw tree
served over http(s):// or a local dir) and the boxscoretraditionalv3_period
layout reconciliation (one file per game, period-keyed).
"""

from __future__ import annotations

import json

import polars as pl

from sportsdataverse.nba import nba_possessions as npo
from sportsdataverse.nba import nba_season_compile as nsc


def _boom():
    raise AssertionError("live fetch must not be called on a store hit / URL miss")


# --- path helpers -----------------------------------------------------------


def test_season_dir_end_year_and_wnba():
    assert npo._season_dir_for_game("0022300001") == "2024"  # 2023-24 -> end year
    assert npo._season_dir_for_game("0029600001") == "1997"  # 1996-97
    assert npo._season_dir_for_game("1022400001") == "2024"  # WNBA single-year, no shift


def test_relpath_matches_committed_layout():
    assert npo._raw_store_relpath("playbyplayv3", "0022300001") == "playbyplayv3/2024/0022300001.json"
    assert (
        npo._raw_store_relpath("boxscoretraditionalv3_period", "0022300001")
        == "boxscoretraditionalv3_period/2024/0022300001.json"
    )


def test_url_root_detection_and_no_filesystem_path():
    assert npo._is_url_root("https://raw.githubusercontent.com/x/y/main/nba_stats/json")
    assert npo._is_url_root("http://cdn.example/x")
    assert not npo._is_url_root("/local/checkout/nba_stats/json")
    # URL roots carry no Path (existence checks get None; the store serves over HTTP)
    assert npo._raw_store_path("playbyplayv3", "0022300001", root="https://cdn/x") is None


# --- URL backend ------------------------------------------------------------


def test_through_store_url_hit_and_miss(monkeypatch):
    served = {"https://cdn/x/playbyplayv3/2024/0022300001.json": {"game": {"actions": [1]}}}
    monkeypatch.setattr(npo, "_http_get_json", lambda url, **k: served.get(url))
    hit = npo._through_raw_store("playbyplayv3", "0022300001", _boom, store_dir="https://cdn/x")
    assert hit == {"game": {"actions": [1]}}
    # a URL miss returns empty WITHOUT a live fetch (offline-authoritative)
    miss = npo._through_raw_store("playbyplayv3", "0022399999", _boom, store_dir="https://cdn/x")
    assert miss == {}


def test_through_store_filesystem_hit(tmp_path):
    p = tmp_path / "playbyplayv3" / "2024" / "0022300001.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps({"game": {"actions": [1, 2]}}), encoding="utf-8")
    got = npo._through_raw_store("playbyplayv3", "0022300001", _boom, store_dir=str(tmp_path))
    assert got == {"game": {"actions": [1, 2]}}


def test_http_get_json_logs_faults_but_not_plain_misses(monkeypatch, caplog):
    """A 404 is an ordinary miss and stays quiet; a transport error or a non-404
    status is logged, so an unreachable host / typo'd base is distinguishable
    from "this capture doesn't exist" instead of looking like empty data."""
    import requests

    class Resp:
        def __init__(self, code):
            self.status_code = code

        def json(self):
            return {"ok": 1}

    monkeypatch.setattr(requests, "get", lambda *a, **k: Resp(404))
    with caplog.at_level("WARNING"):
        assert npo._http_get_json("https://cdn/x/a.json") is None
    assert caplog.records == []

    caplog.clear()
    monkeypatch.setattr(requests, "get", lambda *a, **k: Resp(500))
    with caplog.at_level("WARNING"):
        assert npo._http_get_json("https://cdn/x/a.json") is None
    assert any("500" in r.getMessage() for r in caplog.records)

    caplog.clear()

    def _boom_get(*a, **k):
        raise requests.RequestException("unreachable")

    monkeypatch.setattr(requests, "get", _boom_get)
    with caplog.at_level("WARNING"):
        assert npo._http_get_json("https://cdn/x/a.json") is None
    assert caplog.records, "a transport failure must not be silent"


# --- period-box one-file reconciliation -------------------------------------


def test_fetch_box_periods_reads_committed_one_file(tmp_path, monkeypatch):
    # committed shape: ONE file per game, period-keyed (what the -raw scraper writes)
    p = tmp_path / "boxscoretraditionalv3_period" / "2024" / "0022300001.json"
    p.parent.mkdir(parents=True)
    p.write_text(
        json.dumps({"1": {"a": 1}, "2": {"a": 2}, "3": {"a": 3}, "4": {"a": 4}}),
        encoding="utf-8",
    )
    import sportsdataverse.nba.nba_stats as ns

    monkeypatch.setattr(ns, "nba_stats_boxscoretraditionalv3", lambda **k: _boom(), raising=True)
    out = npo._fetch_box_periods("0022300001", 4, raw_store_dir=str(tmp_path), raw_store_readonly=True)
    assert out == {1: {"a": 1}, 2: {"a": 2}, 3: {"a": 3}, 4: {"a": 4}}


def test_fetch_box_periods_url(monkeypatch):
    served = {
        "https://cdn/x/boxscoretraditionalv3_period/2024/0022300001.json": {
            "1": {"a": 1},
            "2": {"a": 2},
        }
    }
    monkeypatch.setattr(npo, "_http_get_json", lambda url, **k: served.get(url))
    out = npo._fetch_box_periods("0022300001", 2, raw_store_dir="https://cdn/x")
    assert out == {1: {"a": 1}, 2: {"a": 2}}


# --- season discovery from the committed leaguegamelog -----------------------


def _leaguegamelog_raw() -> dict:
    return {
        "resultSets": [
            {
                "name": "LeagueGameLog",
                "headers": ["GAME_ID", "GAME_DATE"],
                "rowSet": [
                    ["0022300001", "2023-10-24"],
                    ["0022300001", "2023-10-24"],  # team-level dup -> collapses
                    ["0022300002", "2023-10-25"],
                ],
            }
        ]
    }


def test_season_index_from_store_filesystem(tmp_path):
    p = tmp_path / "leaguegamelog" / "2024" / "regular-season.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps(_leaguegamelog_raw()), encoding="utf-8")
    idx = nsc._season_index_from_store(2024, "Regular Season", str(tmp_path))
    assert idx is not None
    assert idx["game_id"].to_list() == ["0022300001", "0022300002"]
    assert idx.schema["game_date"] == pl.Date


def test_season_index_from_store_url(monkeypatch):
    served = {"https://cdn/x/leaguegamelog/2024/playoffs.json": _leaguegamelog_raw()}
    monkeypatch.setattr(npo, "_http_get_json", lambda url, **k: served.get(url))
    idx = nsc._season_index_from_store(2024, "Playoffs", "https://cdn/x")
    assert idx is not None and idx.height == 2


def test_season_index_from_store_absent_returns_none(tmp_path):
    assert nsc._season_index_from_store(2024, "Regular Season", str(tmp_path)) is None
    assert nsc._season_index_from_store(2024, "Regular Season", None) is None


# --- public season-level store reader ---------------------------------------


def test_season_frame_parameterized_and_bare_layouts(tmp_path):
    """Both committed shapes: {endpoint}/{season}/{variant}.json (swept params)
    and {endpoint}/{season}.json (unparameterized, e.g. playerindex)."""
    par = tmp_path / "leaguedashplayerbiostats" / "2024" / "regular-season_totals.json"
    par.parent.mkdir(parents=True)
    par.write_text(
        json.dumps({"resultSets": [{"name": "X", "headers": ["PLAYER_ID", "AGE"], "rowSet": [[1, 25.0]]}]}),
        encoding="utf-8",
    )
    got = npo.nba_raw_store_season_frame(
        "leaguedashplayerbiostats", 2024, "regular-season_totals", raw_store_dir=str(tmp_path)
    )
    assert got is not None and got["player_id"].to_list() == [1]

    bare = tmp_path / "playerindex" / "2024.json"
    bare.parent.mkdir(parents=True)
    bare.write_text(
        json.dumps({"resultSets": [{"name": "P", "headers": ["PERSON_ID", "POSITION"], "rowSet": [[7, "PG"]]}]}),
        encoding="utf-8",
    )
    got = npo.nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=str(tmp_path))
    assert got is not None and got["person_id"].to_list() == [7]


def test_season_frame_url_and_absent(monkeypatch, tmp_path):
    served = {
        "https://cdn/x/playerindex/2024.json": {
            "resultSets": [{"name": "P", "headers": ["PERSON_ID"], "rowSet": [[7]]}]
        }
    }
    monkeypatch.setattr(npo, "_http_get_json", lambda url, **k: served.get(url))
    assert npo.nba_raw_store_season_frame("playerindex", 2024, raw_store_dir="https://cdn/x") is not None
    # absent capture / disabled store -> None so the caller falls back to live
    assert npo.nba_raw_store_season_frame("playerindex", 1999, raw_store_dir="https://cdn/x") is None
    assert npo.nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=str(tmp_path)) is None
    assert npo.nba_raw_store_season_frame("playerindex", 2024, raw_store_dir="") is None


def test_season_frame_empty_rowset_returns_none(tmp_path):
    """An empty capture must read as a miss, not an empty frame that would
    silently replace a live fetch with zero rows."""
    p = tmp_path / "playerindex" / "2024.json"
    p.parent.mkdir(parents=True)
    p.write_text(
        json.dumps({"resultSets": [{"name": "P", "headers": ["PERSON_ID"], "rowSet": []}]}),
        encoding="utf-8",
    )
    assert npo.nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=str(tmp_path)) is None
