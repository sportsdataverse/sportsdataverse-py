"""Offline + live tests for the hand-written ``*_stats_leaguedash`` cube loaders.

The offline tests monkeypatch ``_read_release_parquet`` so the family/season
URL construction, the unknown-family guard, the season floor, and the
missing-season skip are all exercised without touching the network. The live
tests (gated by ``SDV_PY_LIVE_TESTS=1``) assert the loader actually reads the
published release.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse._codegen_runtime import SeasonNotFoundError
from sportsdataverse.nba import nba_loaders_extra as nba_extra
from sportsdataverse.wnba import wnba_loaders_extra as wnba_extra
from tests.conftest import skip_if_no_live


@pytest.fixture
def urls(monkeypatch):
    """Record every release URL the loader asks for; serve a 1-row frame."""
    seen: list[str] = []

    def fake(url, *args, **kwargs):
        seen.append(url)
        return pl.DataFrame({"player_id": [1], "season": [2024]})

    monkeypatch.setattr(nba_extra, "_read_release_parquet", fake)
    return seen


def test_family_and_season_are_both_in_the_url(urls):
    nba_extra.load_nba_stats_leaguedash("player_stats_advanced", [2023, 2024])
    assert urls == [
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
        f"nba_stats_leaguedash/player_stats_advanced_{s}.parquet"
        for s in (2023, 2024)
    ]


def test_wnba_twin_reads_its_own_tag(urls):
    wnba_extra.load_wnba_stats_leaguedash("team_stats_base", 2025)
    assert urls == [
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
        "wnba_stats_leaguedash/team_stats_base_2025.parquet"
    ]


def test_unknown_family_raises_and_names_the_valid_ones(urls):
    with pytest.raises(ValueError, match="unknown family 'player_stats_adv'"):
        nba_extra.load_nba_stats_leaguedash("player_stats_adv", 2024)
    assert urls == [], "a bad family must not reach the network"


def test_wnba_rejects_a_tracking_family_the_tag_does_not_publish(urls):
    # stats.wnba.com ships no tracking dashboards; the NBA-only families must
    # not silently 404 their way to an empty frame.
    with pytest.raises(ValueError):
        wnba_extra.load_wnba_stats_leaguedash("player_tracking_drives", 2025)
    assert urls == []


@pytest.mark.parametrize(
    ("fn", "season"),
    [(nba_extra.load_nba_stats_leaguedash, 1995), (wnba_extra.load_wnba_stats_leaguedash, 1996)],
)
def test_season_below_the_tag_floor_raises(fn, season, urls):
    with pytest.raises(SeasonNotFoundError):
        fn("standings", season)


def test_missing_season_is_skipped_not_fatal(monkeypatch):
    # player_tracking_catchshoot really does have a hole at 2002 on the live tag.
    def fake(url, *args, **kwargs):
        return None if "_2002." in url else pl.DataFrame({"player_id": [1]})

    monkeypatch.setattr(nba_extra, "_read_release_parquet", fake)
    with pytest.warns(UserWarning, match=r"player_tracking_catchshoot data for season\(s\) \[2002\]"):
        out = nba_extra.load_nba_stats_leaguedash("player_tracking_catchshoot", [2001, 2002, 2003])
    assert out.height == 2


def test_no_published_season_returns_an_empty_frame(monkeypatch):
    monkeypatch.setattr(nba_extra, "_read_release_parquet", lambda url, *a, **k: None)
    out = nba_extra.load_nba_stats_leaguedash("standings", [1996, 1997])
    assert isinstance(out, pl.DataFrame) and out.height == 0


def test_seasons_with_drifting_columns_union_rather_than_raise(monkeypatch):
    def fake(url, *args, **kwargs):
        if url.endswith("_2023.parquet"):
            return pl.DataFrame({"player_id": [1]})
        return pl.DataFrame({"player_id": [2], "new_measure": [0.5]})

    monkeypatch.setattr(nba_extra, "_read_release_parquet", fake)
    out = nba_extra.load_nba_stats_leaguedash("player_stats_base", [2023, 2024])
    assert out.columns == ["player_id", "new_measure"]
    assert out["new_measure"].to_list() == [None, 0.5]


def test_return_as_pandas_round_trips(monkeypatch):
    monkeypatch.setattr(nba_extra, "_read_release_parquet", lambda url, *a, **k: pl.DataFrame({"player_id": [1]}))
    out = nba_extra.load_nba_stats_leaguedash("player_master", 2024, return_as_pandas=True)
    assert list(out.columns) == ["player_id"]
    assert not isinstance(out, pl.DataFrame)


def test_family_tuples_match_the_documented_inventory():
    # Verified against the live release listing on 2026-09-02: 833 NBA assets /
    # 36 families, 720 WNBA parquet assets / 24 families, WNBA == NBA minus
    # every player_tracking_* family.
    assert len(nba_extra.NBA_STATS_LEAGUEDASH_FAMILIES) == 36
    assert len(wnba_extra.WNBA_STATS_LEAGUEDASH_FAMILIES) == 24
    assert set(wnba_extra.WNBA_STATS_LEAGUEDASH_FAMILIES) == {
        f for f in nba_extra.NBA_STATS_LEAGUEDASH_FAMILIES if not f.startswith("player_tracking_")
    }
    for families in (nba_extra.NBA_STATS_LEAGUEDASH_FAMILIES, wnba_extra.WNBA_STATS_LEAGUEDASH_FAMILIES):
        assert list(families) == sorted(families), "keep the tuples sorted for reviewable diffs"


def test_loaders_are_exported_from_the_league_packages():
    import sportsdataverse.nba as nba
    import sportsdataverse.wnba as wnba

    assert nba.load_nba_stats_leaguedash is nba_extra.load_nba_stats_leaguedash
    assert nba.NBA_STATS_LEAGUEDASH_FAMILIES is nba_extra.NBA_STATS_LEAGUEDASH_FAMILIES
    assert wnba.load_wnba_stats_leaguedash is wnba_extra.load_wnba_stats_leaguedash
    assert wnba.WNBA_STATS_LEAGUEDASH_FAMILIES is wnba_extra.WNBA_STATS_LEAGUEDASH_FAMILIES


@skip_if_no_live
def test_live_nba_leaguedash_reads_the_release():
    df = nba_extra.load_nba_stats_leaguedash("player_stats_advanced", 2024)
    assert df.height > 400, df.height
    # ID dtype discipline: these are join keys, pinned Int64 by the producer in
    # every family and season (probed across 6 families / 2 seasons, 2026-09-02).
    assert df.schema["player_id"] == pl.Int64
    assert df.schema["team_id"] == pl.Int64


@skip_if_no_live
def test_live_wnba_leaguedash_reads_the_release():
    df = wnba_extra.load_wnba_stats_leaguedash("team_stats_base", 2025)
    assert df.height > 0
    assert df.schema["team_id"] == pl.Int64
