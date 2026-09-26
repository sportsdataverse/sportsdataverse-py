"""Live-gated smoke tests for the PFF Developer API wrappers (``pff_api_*``).

Gated by ``@skip_if_no_pff_live`` (env ``SDV_PY_PFF_LIVE=1``) -- never set in CI: the API needs
a PFF Pro key (``PFF_API_KEY``) and every call spends the account's per-minute budget.
"""

import polars as pl
import pytest

from sportsdataverse.nfl import pff_api
from sportsdataverse.nfl.pff_api_runtime import _resolve_api_key
from tests.conftest import skip_if_no_pff_live

pytestmark = [
    skip_if_no_pff_live,
    pytest.mark.skipif(not _resolve_api_key(), reason="PFF_API_KEY / SDV_PY_PFF_API_KEY not set"),
]


def test_live_whoami_is_entitled():
    me = pff_api.pff_api_whoami()
    assert me["entitled"] is True and me["credential"] == "api_key"


def test_live_facet_team_filter_is_honoured():
    # the snake_case filter narrows to ONE team; camelCase would return the whole leaderboard
    df = pff_api.pff_api_facet_passing_summary(league="nfl", season="2022", week="1", franchise_id=7)
    assert isinstance(df, pl.DataFrame) and df.height >= 1
    assert df["franchise_id"].unique().to_list() == [7]


def test_live_v2_team_stats():
    df = pff_api.pff_api_team_stats(league="nfl", season=2022, category="offense-passing")
    assert df.height == 32 and df.schema["team_id"] == pl.Int64
