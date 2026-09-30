"""Live smoke tests for official.nba.com (L2M + referee assignments) and
cdn.nba.com liveData play-by-play.

Gated by ``SDV_PY_LIVE_TESTS=1`` (:data:`tests.conftest.skip_if_no_live`) --
these hit real external hosts. official.nba.com is behind Akamai Bot Manager
and cdn.nba.com refuses a plain client's default headers with a 403 page (see
the module docstrings in ``sportsdataverse/nba/nba_officiating.py`` /
``sportsdataverse/nba/nba_live.py``). A blocked or failed fetch -- a transport
error or a non-200 status, whose :class:`~sportsdataverse.errors.AssetFetchError`
says "fetch failed" -- becomes a ``skip`` (not a pass, not a failure), so a
CI-runner IP block doesn't read as a regression. Every other ``AssetFetchError``
is schema drift (malformed referee tables, a listing page without its marker or
with unreadable report links, a body without its ``game`` row/object) and fails
the run, so the weekly drift cron sees it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from sportsdataverse.errors import AssetFetchError
from sportsdataverse.nba.nba_live import nba_live_pbp
from sportsdataverse.nba.nba_officiating import nba_l2m, nba_l2m_games, nba_referee_assignments
from tests.conftest import skip_if_no_live

pytestmark = skip_if_no_live


@contextmanager
def _skip_if_blocked(host: str) -> Iterator[None]:
    """Skip on a failed fetch (the fetchers' "fetch failed" errors); re-raise drift."""
    try:
        yield
    except AssetFetchError as exc:
        if "fetch failed" not in str(exc):
            raise
        pytest.skip(f"{host} blocked from this IP: {exc}")


def test_nba_l2m_live_playoff_game():
    """A known 2025 playoff game (0042500405) has 21+ tracked L2M calls."""
    with _skip_if_blocked("official.nba.com"):
        result = nba_l2m("0042500405")
    assert result["calls"].height >= 21


def test_nba_l2m_games_live_season_listing():
    """The 2025-26 season L2M listing has at least 415 games with reports."""
    with _skip_if_blocked("official.nba.com"):
        df = nba_l2m_games(2026)
    assert df.height >= 415


def test_nba_referee_assignments_live_date():
    """2026-06-13 (NBA Finals window) has at least 4 official crew-slot rows."""
    with _skip_if_blocked("official.nba.com"):
        result = nba_referee_assignments("2026-06-13")
    assert result["officials"].height >= 4


def test_nba_live_pbp_live_game():
    """A known 2025-26 season-opener game has a non-empty liveData play-by-play."""
    with _skip_if_blocked("cdn.nba.com"):
        df = nba_live_pbp("0022500001")
    assert df.height > 0
