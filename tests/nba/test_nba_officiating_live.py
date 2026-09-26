"""Live smoke tests for official.nba.com (L2M + referee assignments) and
cdn.nba.com liveData play-by-play.

Gated by ``SDV_PY_LIVE_TESTS=1`` (:data:`tests.conftest.skip_if_no_live`) --
these hit real external hosts. official.nba.com is behind Akamai Bot Manager
and cdn.nba.com is behind a TLS/JA3 fingerprint check (see the module
docstrings in ``sportsdataverse/nba/nba_officiating.py`` /
``sportsdataverse/nba/nba_live.py``); a blocked fetch surfaces as
:class:`~sportsdataverse.errors.AssetFetchError`, which these tests turn into
a ``skip`` (not a pass, not a failure) so a CI-runner IP block doesn't read as
a regression.
"""

from __future__ import annotations

import pytest

from sportsdataverse.errors import AssetFetchError
from sportsdataverse.nba.nba_live import nba_live_pbp
from sportsdataverse.nba.nba_officiating import nba_l2m, nba_l2m_games, nba_referee_assignments
from tests.conftest import skip_if_no_live

pytestmark = skip_if_no_live


def test_nba_l2m_live_playoff_game():
    """A known 2025 playoff game (0042500405) has 21+ tracked L2M calls."""
    try:
        result = nba_l2m("0042500405")
    except AssetFetchError:
        pytest.skip("official.nba.com blocked from this IP")
        return
    assert result["calls"].height >= 21


def test_nba_l2m_games_live_season_listing():
    """The 2025-26 season L2M listing has at least 415 games with reports."""
    try:
        df = nba_l2m_games(2026)
    except AssetFetchError:
        pytest.skip("official.nba.com blocked from this IP")
        return
    assert df.height >= 415


def test_nba_referee_assignments_live_date():
    """2026-06-13 (NBA Finals window) has at least 4 official crew-slot rows."""
    try:
        result = nba_referee_assignments("2026-06-13")
    except AssetFetchError:
        pytest.skip("official.nba.com blocked from this IP")
        return
    assert result["officials"].height >= 4


def test_nba_live_pbp_live_game():
    """A known 2025-26 season-opener game has a non-empty liveData play-by-play."""
    try:
        df = nba_live_pbp("0022500001")
    except AssetFetchError:
        pytest.skip("cdn.nba.com blocked from this IP")
        return
    assert df.height > 0
