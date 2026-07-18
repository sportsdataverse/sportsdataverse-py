"""Offline + gated-live tests for nba_season_compile."""

from __future__ import annotations

import datetime
import json
import pathlib

import polars as pl
import pytest

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


def _idx(ids: list, dates: list) -> pl.DataFrame:
    return pl.DataFrame(
        {"game_id": ids, "game_date": dates},
        schema={"game_id": pl.Utf8, "game_date": pl.Date},
    )


def test_compile_dedups_gameids_and_tags_season(tmp_path, monkeypatch):
    monkeypatch.setattr(
        C,
        "_season_game_index",
        lambda s, st, *, proxy_url=None: _idx(
            ["001", "002"], [datetime.date(2023, 10, 24), datetime.date(2023, 10, 25)]
        ),
    )
    calls = []

    def fake_fetch(gid, league_id, *, lineup_source: str = "auto", proxy_url=None, **_kwargs):
        calls.append(gid)
        return _poss(gid)

    monkeypatch.setattr(C, "_fetch_possessions", fake_fetch)
    out = C.compile_nba_season(2024, cache_dir=str(tmp_path), delay_s=0.0)
    assert sorted(calls) == ["001", "002"]  # deduped
    assert set(out["game_id"].unique().to_list()) == {"001", "002"}
    assert out["season"].unique().to_list() == [2024]


def test_season_game_index_converts_end_year_to_stats_nba_season_string(monkeypatch):
    """``_season_game_index`` takes the season END year but stats.nba.com's
    ``leaguegamelog`` wants the START-year hyphenated label -- the ``- 1``
    inside ``_season_game_index`` is what bridges the two conventions. This
    exercises that conversion directly (unlike the other tests here, which
    monkeypatch ``_season_game_index`` wholesale and so never touch it).
    """
    from sportsdataverse.nba import nba_stats

    seen: dict = {}

    def fake_leaguegamelog(**kwargs):
        seen.update(kwargs)
        return pl.DataFrame()

    monkeypatch.setattr(nba_stats, "nba_stats_leaguegamelog", fake_leaguegamelog)
    out = C._season_game_index(2024, "Regular Season")
    assert seen["season"] == "2023-24"
    assert out.is_empty()


def test_compile_resume_skips_cached(tmp_path, monkeypatch):
    monkeypatch.setattr(
        C,
        "_season_game_index",
        lambda s, st, *, proxy_url=None: _idx(
            ["001", "002"], [datetime.date(2023, 10, 24), datetime.date(2023, 10, 25)]
        ),
    )
    monkeypatch.setattr(
        C, "_fetch_possessions", lambda gid, lid, *, lineup_source="auto", proxy_url=None, **_kwargs: _poss(gid)
    )
    C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)  # warm cache
    calls = []
    monkeypatch.setattr(
        C,
        "_fetch_possessions",
        lambda gid, lid, *, lineup_source="auto", proxy_url=None, **_kwargs: (calls.append(gid), _poss(gid))[1],
    )
    C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)  # all cached
    assert calls == []  # nothing re-fetched


def test_compile_best_effort_skips_failing_game(tmp_path, monkeypatch):
    monkeypatch.setattr(
        C,
        "_season_game_index",
        lambda s, st, *, proxy_url=None: _idx(
            ["ok1", "bad", "ok2"],
            [datetime.date(2023, 10, 24), datetime.date(2023, 10, 25), datetime.date(2023, 10, 26)],
        ),
    )

    def fetch(gid, lid, *, lineup_source: str = "auto", proxy_url=None, **_kwargs):
        if gid == "bad":
            raise RuntimeError("api boom")
        return _poss(gid)

    monkeypatch.setattr(C, "_fetch_possessions", fetch)
    out = C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert set(out["game_id"].unique().to_list()) == {"ok1", "ok2"}  # bad skipped, no raise


def test_compile_never_raises_on_no_games(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "_season_game_index", lambda s, st, *, proxy_url=None: _idx([], []))
    out = C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert out.is_empty() and "season" in out.columns


# ---------------------------------------------------------------------------
# proxy_provider: stats.nba.com rejects/hangs on datacenter IPs, so an unattended
# host must proxy. The provider is called once for discovery then PER GAME (N+1),
# so a pool rotates the exit IP across a season instead of burning one address.
# ---------------------------------------------------------------------------


def test_compile_rotates_proxy_once_per_game(tmp_path, monkeypatch):
    monkeypatch.setattr(
        C,
        "_season_game_index",
        lambda s, st, *, proxy_url=None: _idx(
            ["001", "002", "003"],
            [datetime.date(2023, 10, 24), datetime.date(2023, 10, 25), datetime.date(2023, 10, 26)],
        ),
    )
    # p0 is consumed by game discovery (test_compile_proxies_game_discovery) --
    # a season costs N+1 provider calls, not N.
    pool = iter(["http://p0:0", "http://p1:1", "http://p2:2", "http://p3:3"])
    seen: list[str | None] = []

    def fetch(gid, lid, *, lineup_source="auto", proxy_url=None, **_kwargs):
        seen.append(proxy_url)  # the URL must actually REACH the fetcher
        return _poss(gid)

    monkeypatch.setattr(C, "_fetch_possessions", fetch)
    C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0, proxy_provider=lambda: next(pool))
    # one distinct proxy per game -- not one proxy reused for the whole season
    assert seen == ["http://p1:1", "http://p2:2", "http://p3:3"]


def test_compile_without_proxy_provider_passes_none(tmp_path, monkeypatch):
    """Default (no provider) must stay a plain unproxied fetch -- back-compat."""
    monkeypatch.setattr(
        C, "_season_game_index", lambda s, st, *, proxy_url=None: _idx(["001"], [datetime.date(2023, 10, 24)])
    )
    seen: list[str | None] = []

    def fetch(gid, lid, *, lineup_source="auto", proxy_url=None, **_kwargs):
        seen.append(proxy_url)
        return _poss(gid)

    monkeypatch.setattr(C, "_fetch_possessions", fetch)
    C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert seen == [None]


def test_compile_proxies_game_discovery(tmp_path, monkeypatch):
    """Discovery is a stats.nba.com call too -- it MUST be proxied.

    Unproxied, the leaguegamelog discovery call returns {} on a datacenter host,
    which _season_game_index turns into an empty index (empty-in/empty-out). The
    season then compiles to zero rows and exits 0 -- a silent no-op backfill that
    looks like success. The per-game fetch being proxied is not enough.
    """
    seen: list[str | None] = []

    def idx(s, st, *, proxy_url=None):
        seen.append(proxy_url)
        return _idx(["001"], [datetime.date(2023, 10, 24)])

    monkeypatch.setattr(C, "_season_game_index", idx)
    monkeypatch.setattr(
        C, "_fetch_possessions", lambda gid, lid, *, lineup_source="auto", proxy_url=None, **_kwargs: _poss(gid)
    )
    C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0, proxy_provider=lambda: "http://p1:1")
    assert seen == ["http://p1:1"]


def test_nba_possessions_threads_proxy_to_every_fetcher(monkeypatch):
    """proxy_url must reach ALL the network calls -- a missed one silently deanonymizes."""
    from sportsdataverse.nba import nba_possessions as P

    got: dict[str, str | None] = {}

    def _pbp(game_id, league_id="00", *, proxy_url=None, **_kwargs):
        got["pbp"] = proxy_url
        return json.loads(pathlib.Path("tests/fixtures/nba_engine/0022300001/playbyplayv3.json").read_text())

    def _box(game_id, league_id="00", *, proxy_url=None, **_kwargs):
        got["box"] = proxy_url
        return json.loads(pathlib.Path("tests/fixtures/nba_engine/0022300001/boxscoretraditionalv3.json").read_text())

    monkeypatch.setattr(P, "_fetch_pbp", _pbp)
    monkeypatch.setattr(P, "_fetch_box", _box)
    P.nba_possessions("0022300001", lineup_source="pbp", proxy_url="http://proxy:9")
    assert got == {"pbp": "http://proxy:9", "box": "http://proxy:9"}


# ---------------------------------------------------------------------------
# WP1 Task 3: game_date attachment
# ---------------------------------------------------------------------------


def _fake_index() -> pl.DataFrame:
    return pl.DataFrame(
        {"game_id": ["0022300001"], "game_date": [datetime.date(2023, 10, 24)]},
        schema={"game_id": pl.Utf8, "game_date": pl.Date},
    )


def _fixture_poss() -> pl.DataFrame:
    payload = json.loads(pathlib.Path("tests/fixtures/nba_engine/0022300001/playbyplayv3.json").read_text())
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
    from sportsdataverse.nba.nba_possessions import build_possessions

    return build_possessions(enhanced_pbp_from_payload(payload))


def test_compile_attaches_game_date(monkeypatch, tmp_path):
    from sportsdataverse.nba import nba_season_compile as mod

    monkeypatch.setattr(mod, "_season_game_index", lambda s, st, *, proxy_url=None: _fake_index())
    monkeypatch.setattr(
        mod, "_fetch_possessions", lambda gid, lid, lineup_source="auto", proxy_url=None, **_kwargs: _fixture_poss()
    )
    out = mod.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert out.schema["game_date"] == pl.Date
    assert out["game_date"].null_count() == 0
    assert out["game_date"][0] == datetime.date(2023, 10, 24)


def test_compile_game_date_covers_cached_parquet(monkeypatch, tmp_path):
    """Upgrade-on-read: a pre-existing cache parquet WITHOUT game_date still gets one."""
    from sportsdataverse.nba import nba_season_compile as mod

    cache = tmp_path / mod._game_cache_key("0022300001")
    _fixture_poss().write_parquet(cache)  # simulates a cache written before this change
    monkeypatch.setattr(mod, "_season_game_index", lambda s, st, *, proxy_url=None: _fake_index())
    monkeypatch.setattr(
        mod,
        "_fetch_possessions",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not fetch — cache hit expected")),
    )
    out = mod.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)
    assert out["game_date"].null_count() == 0


def test_compile_missing_game_date_raises(monkeypatch, tmp_path):
    """A game id absent from the season index is an explicit error, not silent nulls."""
    from sportsdataverse.nba import nba_season_compile as mod

    # index knows the id (so it is compiled) but its game_date is null
    bad_index = pl.DataFrame(
        {"game_id": ["0022300001"], "game_date": [None]},
        schema={"game_id": pl.Utf8, "game_date": pl.Date},
    )
    monkeypatch.setattr(mod, "_season_game_index", lambda s, st, *, proxy_url=None: bad_index)
    monkeypatch.setattr(
        mod, "_fetch_possessions", lambda gid, lid, lineup_source="auto", proxy_url=None, **_kwargs: _fixture_poss()
    )
    with pytest.raises(ValueError, match="game_date"):
        mod.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=0.0)


# ---------------------------------------------------------------------------
# Gated live slice — only runs when SDV_PY_NBA_STATS_LIVE=1
# ---------------------------------------------------------------------------
from tests.conftest import skip_if_no_nba_stats_live  # noqa: E402


@skip_if_no_nba_stats_live
def test_compile_live_small_slice(tmp_path, monkeypatch):
    # only the first 3 real regular-season game ids, real fetch, real cache
    real_index = C._season_game_index(2023, "Regular Season").head(3)
    monkeypatch.setattr(C, "_season_game_index", lambda s, st, *, proxy_url=None: real_index)
    out = C.compile_nba_season(2023, cache_dir=str(tmp_path), delay_s=1.0)
    assert not out.is_empty() and "off_player_1" in out.columns
