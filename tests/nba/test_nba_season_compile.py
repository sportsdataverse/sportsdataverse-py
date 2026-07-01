"""Offline + gated-live tests for nba_season_compile."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba import nba_season_compile as C

_OFF = [f"off_player_{i}" for i in range(1, 6)]
_DEF = [f"def_player_{i}" for i in range(1, 6)]


def _poss(gid: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [gid, gid],
            "offense_team_id": [100, 200],
            "points": [2, 0],
            **{c: [1, 2] for c in _OFF},
            **{c: [3, 4] for c in _DEF},
        }
    )


def test_compile_dedups_gameids_and_tags_season(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "_game_ids_for_season", lambda s, st: ["001", "001", "002"])
    calls = []

    def fake_fetch(gid, league_id):
        calls.append(gid)
        return _poss(gid)

    monkeypatch.setattr(C, "_fetch_possessions", fake_fetch)
    out = C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert sorted(calls) == ["001", "002"]  # deduped
    assert set(out["game_id"].unique().to_list()) == {"001", "002"}
    assert out["season"].unique().to_list() == [2023]


def test_compile_resume_skips_cached(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "_game_ids_for_season", lambda s, st: ["001", "002"])
    monkeypatch.setattr(C, "_fetch_possessions", lambda gid, lid: _poss(gid))
    C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)  # warm cache
    calls = []
    monkeypatch.setattr(
        C,
        "_fetch_possessions",
        lambda gid, lid: (calls.append(gid), _poss(gid))[1],
    )
    C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)  # all cached
    assert calls == []  # nothing re-fetched


def test_compile_best_effort_skips_failing_game(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "_game_ids_for_season", lambda s, st: ["ok1", "bad", "ok2"])

    def fetch(gid, lid):
        if gid == "bad":
            raise RuntimeError("api boom")
        return _poss(gid)

    monkeypatch.setattr(C, "_fetch_possessions", fetch)
    out = C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert set(out["game_id"].unique().to_list()) == {"ok1", "ok2"}  # bad skipped, no raise


def test_compile_never_raises_on_no_games(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "_game_ids_for_season", lambda s, st: [])
    out = C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert out.is_empty() and "season" in out.columns


# ---------------------------------------------------------------------------
# Gated live slice — only runs when SDV_PY_NBA_STATS_LIVE=1
# ---------------------------------------------------------------------------
from tests.conftest import skip_if_no_nba_stats_live  # noqa: E402


@skip_if_no_nba_stats_live
def test_compile_live_small_slice(tmp_path, monkeypatch):
    # only the first 3 real regular-season game ids, real fetch, real cache
    real_ids = C._game_ids_for_season(2023, "Regular Season")[:3]
    monkeypatch.setattr(C, "_game_ids_for_season", lambda s, st: real_ids)
    out = C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=1.0)
    assert not out.is_empty() and "off_player_1" in out.columns
