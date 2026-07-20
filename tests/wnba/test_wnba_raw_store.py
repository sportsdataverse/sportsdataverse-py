"""Read-through raw store wired into wnba_engine's fetchers.

WNBA reuses the league-agnostic store in nba_possessions but with its own env
namespace (SDV_PY_WNBA_RAW_JSON_DIR / _READONLY). These tests exercise the
consumer contract and — critically — that the WNBA namespace never bleeds
into the NBA one.
"""

import json

import sportsdataverse.wnba.wnba_engine as W
import sportsdataverse.wnba.wnba_stats as wnba_stats

PAYLOAD = {"meta": {"code": 200}, "game": {"gameId": "1022600071"}}
GID = "1022600071"  # WNBA 2026 -> calendar-year dir, no shift


def _patch_pbp(monkeypatch):
    calls = {"n": 0}

    def fake(**kwargs):
        calls["n"] += 1
        return dict(PAYLOAD)

    monkeypatch.setattr(wnba_stats, "wnba_stats_playbyplayv3", fake)
    return calls


def test_disabled_when_env_unset(monkeypatch, tmp_path):
    monkeypatch.delenv("SDV_PY_WNBA_RAW_JSON_DIR", raising=False)
    calls = _patch_pbp(monkeypatch)
    assert W._fetch_pbp(GID) == PAYLOAD
    assert calls["n"] == 1
    assert list(tmp_path.iterdir()) == []


def test_miss_persists_then_hits_without_network(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_WNBA_RAW_JSON_DIR", str(tmp_path))
    monkeypatch.delenv("SDV_PY_WNBA_RAW_JSON_READONLY", raising=False)
    calls = _patch_pbp(monkeypatch)
    assert W._fetch_pbp(GID) == PAYLOAD
    stored = tmp_path / "playbyplayv3" / "2026" / f"{GID}.json"  # calendar year, no +1
    assert stored.exists()
    assert json.loads(stored.read_text(encoding="utf-8")) == PAYLOAD
    assert W._fetch_pbp(GID) == PAYLOAD  # served from disk
    assert calls["n"] == 1  # transport hit exactly once


def test_readonly_reads_but_never_writes(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_WNBA_RAW_JSON_DIR", str(tmp_path))
    monkeypatch.setenv("SDV_PY_WNBA_RAW_JSON_READONLY", "1")
    calls = _patch_pbp(monkeypatch)
    assert W._fetch_pbp(GID) == PAYLOAD  # miss -> fetch, no persist
    assert calls["n"] == 1
    assert not (tmp_path / "playbyplayv3").exists()


def test_no_bleed_from_nba_env(monkeypatch, tmp_path):
    # NBA env set, WNBA env unset -> the WNBA store must stay OFF, and nothing
    # may be written under the NBA root by a WNBA fetch.
    nba_root = tmp_path / "nba"
    monkeypatch.setenv("SDV_PY_NBA_RAW_JSON_DIR", str(nba_root))
    monkeypatch.delenv("SDV_PY_WNBA_RAW_JSON_DIR", raising=False)
    calls = _patch_pbp(monkeypatch)
    assert W._fetch_pbp(GID) == PAYLOAD
    assert calls["n"] == 1
    assert not nba_root.exists()


def test_rotation_and_box_route_through_store(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_WNBA_RAW_JSON_DIR", str(tmp_path))
    monkeypatch.delenv("SDV_PY_WNBA_RAW_JSON_READONLY", raising=False)
    monkeypatch.setattr(wnba_stats, "wnba_stats_gamerotation", lambda **k: dict(PAYLOAD))
    monkeypatch.setattr(wnba_stats, "wnba_stats_boxscoretraditionalv3", lambda **k: dict(PAYLOAD))
    W._fetch_rotation(GID)
    W._fetch_box(GID)
    assert (tmp_path / "gamerotation" / "2026" / f"{GID}.json").exists()
    assert (tmp_path / "boxscoretraditionalv3" / "2026" / f"{GID}.json").exists()
