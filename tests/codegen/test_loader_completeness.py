"""Loader completeness: a pbp loader returns every row and game of the asset it reads (W-H6, live).

``load_cfb_pbp([2023])`` came back with 903 games. The cause was the asset, not the loader
(critic R-008), so the fix is a republish (REPROCESS.md run #2). What stays here is the check
that would catch the loader half -- a season filter on the wrong column, a cache hit on an old
asset, a dropped row -- for one season of each pbp loader family: the loader's frame against
the release parquet it read, fetched again on its own and counted from its footer.

Live and slow (a full pbp season per family), so gated by ``SDV_PY_LIVE_TESTS=1`` and run from
the owner's machine; the counts it prints go in the gate ledger's EVIDENCE line.
"""

import importlib
import io

import pyarrow.parquet as pq
import pytest

from sportsdataverse import cache
from sportsdataverse.dl_utils import download
from tests.conftest import skip_if_no_live

pytestmark = skip_if_no_live

# (module, loader, season, reader the loader calls). One season per pbp loader family;
# cfb 2023 is the W-H6 season.
_FAMILIES = [
    pytest.param("sportsdataverse.cfb.cfb_loaders", "load_cfb_pbp", 2023, "_read_release_parquet", id="cfb_2023"),
    pytest.param("sportsdataverse.mbb.mbb_loaders", "load_mbb_pbp", 2024, "_read_release_parquet", id="mbb_2024"),
    pytest.param("sportsdataverse.wbb.wbb_loaders", "load_wbb_pbp", 2024, "_read_release_parquet", id="wbb_2024"),
    pytest.param("sportsdataverse.nba.nba_loaders", "load_nba_pbp", 2024, "_read_release_parquet", id="nba_2024"),
    pytest.param("sportsdataverse.wnba.wnba_loaders", "load_wnba_pbp", 2024, "_read_release_parquet", id="wnba_2024"),
    pytest.param("sportsdataverse.nhl.nhl_loaders", "load_nhl_pbp", 2024, "_read_release_parquet", id="nhl_2024"),
    pytest.param("sportsdataverse.pwhl.pwhl_loaders", "load_pwhl_pbp", 2024, "_read_release_parquet", id="pwhl_2024"),
    pytest.param("sportsdataverse.nfl.nfl_loaders", "load_nfl_pbp", 2024, "_fetch_release_parquet", id="nfl_2024"),
]


@pytest.mark.parametrize(("module", "loader", "season", "reader"), _FAMILIES)
def test_pbp_loader_returns_the_whole_asset(module, loader, season, reader, monkeypatch, capsys):
    mod = importlib.import_module(module)
    urls: list = []
    real = getattr(mod, reader)

    def spy(url):
        urls.append(url)
        return real(url)

    monkeypatch.setattr(mod, reader, spy)
    from sportsdataverse.nfl import clear_cache, reset_config, update_config

    mode = cache.get_cache_mode()
    update_config(cache_mode="off")  # the nfl loaders' own @cached_loader
    clear_cache()
    cache.set_cache_mode("off")  # the HTTP response cache
    try:
        frame = getattr(mod, loader)([season])
    finally:
        cache.set_cache_mode(mode)
        reset_config()

    assert len(urls) == 1, urls
    asset = io.BytesIO(download(urls[0]).content)
    footer_rows = pq.read_metadata(asset).num_rows
    asset_games = len(pq.read_table(asset, columns=["game_id"]).column("game_id").unique())
    loader_games = frame["game_id"].n_unique()
    with capsys.disabled():
        print(
            f"\n{loader}({season}): asset {urls[0]} footer rows {footer_rows}, asset games {asset_games}; "
            f"loader rows {frame.height}, loader games {loader_games}"
        )
    assert frame.height == footer_rows
    assert loader_games == asset_games
