"""Offline tests for the read-through raw JSON store in nba_possessions.

The store is enabled solely by ``SDV_PY_NBA_RAW_JSON_DIR``; these tests
exercise the three behaviors that matter: disabled passthrough, miss ->
fetch + persist -> subsequent hit without network, and corrupt-file refetch.
"""

import json

import pytest

from sportsdataverse.nba.nba_possessions import (
    _fetch_pbp,
    _raw_store_path,
    _through_raw_store,
)

PAYLOAD = {"meta": {"code": 200}, "game": {"gameId": "0022300001"}}


def _fetch_ok() -> dict:
    return dict(PAYLOAD)


def _fetch_boom() -> dict:
    raise AssertionError("network fetch should not have been called")


def test_disabled_is_passthrough(monkeypatch, tmp_path):
    monkeypatch.delenv("SDV_PY_NBA_RAW_JSON_DIR", raising=False)
    assert _raw_store_path("playbyplayv3", "0022300001") is None
    assert _through_raw_store("playbyplayv3", "0022300001", _fetch_ok) == PAYLOAD
    assert list(tmp_path.iterdir()) == []


def test_miss_persists_then_hits_without_network(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_NBA_RAW_JSON_DIR", str(tmp_path))
    out = _through_raw_store("playbyplayv3", "0022300001", _fetch_ok)
    assert out == PAYLOAD
    stored = tmp_path / "playbyplayv3" / "2023" / "0022300001.json"
    assert stored.exists()
    assert json.loads(stored.read_text(encoding="utf-8")) == PAYLOAD
    # Hit path: a fetch that raises proves the network is not touched.
    assert _through_raw_store("playbyplayv3", "0022300001", _fetch_boom) == PAYLOAD


def test_readonly_reads_but_never_writes(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_NBA_RAW_JSON_DIR", str(tmp_path))
    monkeypatch.setenv("SDV_PY_NBA_RAW_JSON_READONLY", "1")
    # Miss: fetches, but persists nothing (compile jobs are pure consumers).
    assert _through_raw_store("playbyplayv3", "0022300001", _fetch_ok) == PAYLOAD
    assert not (tmp_path / "playbyplayv3").exists()
    # Hit: still served from the store without a fetch.
    stored = tmp_path / "playbyplayv3" / "2023" / "0022300001.json"
    stored.parent.mkdir(parents=True)
    stored.write_text(json.dumps(PAYLOAD), encoding="utf-8")
    assert _through_raw_store("playbyplayv3", "0022300001", _fetch_boom) == PAYLOAD


def test_corrupt_file_refetches_and_rewrites(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_NBA_RAW_JSON_DIR", str(tmp_path))
    stored = tmp_path / "playbyplayv3" / "2023" / "0022300001.json"
    stored.parent.mkdir(parents=True)
    stored.write_text("{torn", encoding="utf-8")
    assert _through_raw_store("playbyplayv3", "0022300001", _fetch_ok) == PAYLOAD
    assert json.loads(stored.read_text(encoding="utf-8")) == PAYLOAD


@pytest.mark.parametrize(
    ("game_id", "season"),
    [("0029600001", "1996"), ("0022300001", "2023"), ("0024600001", "1946")],
)
def test_season_decoding(monkeypatch, tmp_path, game_id, season):
    monkeypatch.setenv("SDV_PY_NBA_RAW_JSON_DIR", str(tmp_path))
    path = _raw_store_path("gamerotation", game_id, "_p2")
    assert path == tmp_path / "gamerotation" / season / f"{game_id}_p2.json"


def test_fetch_pbp_routes_through_store(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_NBA_RAW_JSON_DIR", str(tmp_path))
    import sportsdataverse.nba.nba_stats as nba_stats

    calls = {"n": 0}

    def fake_pbp(**kwargs):
        calls["n"] += 1
        return dict(PAYLOAD)

    monkeypatch.setattr(nba_stats, "nba_stats_playbyplayv3", fake_pbp)
    assert _fetch_pbp("0022300001") == PAYLOAD
    assert calls["n"] == 1
    # Second call is served from the store; the transport is not re-invoked.
    assert _fetch_pbp("0022300001") == PAYLOAD
    assert calls["n"] == 1
