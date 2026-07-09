"""Residential-gated live tests for the guest-usable sports247 RDB routes added
beyond the original 11 (Track 3).

Gated by ``skip_if_no_247_live`` (env ``SDV_PY_247_LIVE=1``), which NO workflow
sets: ``ipa.247sports.com`` sits behind a Fastly edge that hangs (not fails fast)
on datacenter/CI IPs — same class as stats.nba.com — so these run only from a
residential IP with ``curl_cffi`` installed. Kept resilient to upstream
flakiness (tolerate zero rows).
"""

from __future__ import annotations

import polars as pl

from tests.conftest import skip_if_no_247_live

pytestmark = skip_if_no_247_live


def test_positions_live_football():
    from sportsdataverse.cfb import sports247_positions

    df = sports247_positions(sport_key=1)
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 0
    if df.height > 0:
        assert {"group", "group_key", "label", "value"}.issubset(set(df.columns))


def test_positions_live_raw_is_list():
    from sportsdataverse.cfb import sports247_positions

    raw = sports247_positions(sport_key=1, return_parsed=False)
    # RDB positions is a bare JSON array; empty dict only on a transport miss.
    assert isinstance(raw, (list, dict))
