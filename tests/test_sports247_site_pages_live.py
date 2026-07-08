"""Live smoke tests for the sports247_site_pages stem (247sports.com page models).

Gated behind ``SDV_PY_247_LIVE=1`` (the shared 247-family gate) — NO CI workflow
sets it, so these run only when a contributor enables them from a residential IP
(``www.247sports.com`` may hang rather than fail-fast on datacenter/CI IPs, and the
live path needs ``curl_cffi``). Tests tolerate upstream flakiness/pagination.
"""

from __future__ import annotations

import polars as pl

from tests.conftest import skip_if_no_247_live

pytestmark = skip_if_no_247_live


def test_institution_detail_live():
    from sportsdataverse.cfb import sports247_site_pages_institution

    df = sports247_site_pages_institution(key=24099)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 1
    assert df.schema["key"].is_integer()


def test_season_recruits_live():
    from sportsdataverse.cfb import sports247_site_pages_season_recruits

    df = sports247_site_pages_season_recruits(season="2026-Football", items=15, page=1)
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 0  # tolerate pagination/empty; column contract holds when populated
    if df.height:
        assert "key" in df.columns


def test_player_detail_live():
    from sportsdataverse.cfb import sports247_site_pages_player

    df = sports247_site_pages_player(key=46051367)
    assert isinstance(df, pl.DataFrame)
    assert df.height <= 1  # detail route -> zero or one row
