"""Shared pytest fixtures and skip helpers for the sportsdataverse test suite.

Live-API gating
---------------
Tests that hit external services (ESPN, NHL api-web, MLB Stats API,
Baseball Savant) are gated behind the ``SDV_PY_LIVE_TESTS`` environment
variable so CI doesn't flake on upstream downtime and so contributors
don't accidentally hit live endpoints during local development.

Set ``SDV_PY_LIVE_TESTS=1`` to enable them::

    SDV_PY_LIVE_TESTS=1 pytest tests/test_espn_live.py -v
    SDV_PY_LIVE_TESTS=1 pytest tests/wbb/

The gating is OPT-IN — without the env var, gated tests are skipped, not
failed.

Two usage patterns
~~~~~~~~~~~~~~~~~~

**Per-test decorator**: apply ``@skip_if_no_live`` to a single test::

    from tests.conftest import skip_if_no_live

    @skip_if_no_live
    def test_my_thing():
        ...

**Module-level marker**: apply to every test in a file via
``pytestmark`` (used by :mod:`tests.test_espn_live` and any future
``test_*_live.py``)::

    from tests.conftest import skip_if_no_live
    pytestmark = skip_if_no_live

Both forms read the env var at collection time so unsetting it inside a
test won't suddenly unskip the rest of the module.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

LIVE: bool = os.environ.get("SDV_PY_LIVE_TESTS") == "1"

skip_if_no_live = pytest.mark.skipif(
    not LIVE,
    reason="Set SDV_PY_LIVE_TESTS=1 to run tests that hit live external APIs",
)


# ---------------------------------------------------------------------------
# Captured-fixture loader
# ---------------------------------------------------------------------------

FIXTURES_ROOT = Path(__file__).parent / "fixtures"


def load_fixture(category: str, stem: str) -> dict:
    """Load a JSON fixture from ``tests/fixtures/{category}/{stem}.json``.

    Single shared helper used by every ``test_*_parsers.py`` file so the
    five parser test modules don't each carry their own copy of the
    same ``json.loads((FIXTURE_DIR / f'{stem}.json').read_text(...))``
    boilerplate.

    Args:
        category: Fixture subdirectory under ``tests/fixtures/``
            (``"espn"``, ``"mlb_api"``, ``"nhl_api_web"``, ``"nhl_edge"``,
            ``"nhl_stats_rest"``, ``"nhl_records"``).
        stem: Filename without the ``.json`` extension
            (e.g. ``"summary_nba"``, ``"team_roster_nfl"``).

    Returns:
        Parsed JSON payload as a Python ``dict`` (or list — whatever
        the fixture contains at the top level).

    Raises:
        FileNotFoundError: If the fixture doesn't exist. The error
            message points at the expected path so missing-fixture bugs
            are easy to locate.
    """
    path = FIXTURES_ROOT / category / f"{stem}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Fixture not found: {path}. Expected category={category!r}, stem={stem!r}.",
        )
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_pbp_or_skip(proc):
    """Run a CFB/NFL ``*PlayProcess`` live ESPN fetch, gating + skipping cleanly.

    This is the single live-fetch chokepoint for the PBP test modules, so it owns
    the live gate: it ``skip``s unless ``SDV_PY_LIVE_TESTS=1`` (same contract as
    :data:`skip_if_no_live`) -- meaning every fixture / test that fetches through
    it is gated, with no per-test decorator to forget.

    When live, ESPN's summary endpoint intermittently returns a body with no
    ``header.competitions`` (offseason / a game not yet ingested). The PBP
    processors raise :class:`~sportsdataverse.errors.NoESPNDataError` for that case
    instead of a bare ``KeyError``; we treat it as a transient gap and ``skip``
    rather than fail. The guard fires inside ``run_processing_pipeline()`` (not the
    fetch), so this helper runs **both** the fetch and the pipeline under one
    ``try`` -- ``run_processing_pipeline`` is idempotent, so a test/fixture calling
    it again afterward just gets the cached result. ``proc`` is an ``NFLPlayProcess``
    / ``CFBPlayProcess`` (the league ``espn_*_pbp`` method is auto-detected).
    Returns ``proc`` for chaining.
    """
    if not LIVE:
        pytest.skip("Live ESPN PBP fetch — set SDV_PY_LIVE_TESTS=1 to run.")

    from sportsdataverse.errors import NoESPNDataError

    fetch = getattr(proc, "espn_cfb_pbp", None) or getattr(proc, "espn_nfl_pbp", None)
    try:
        fetch()
        proc.run_processing_pipeline()
    except NoESPNDataError as exc:
        pytest.skip(f"ESPN returned incomplete data for game {getattr(proc, 'gameId', '?')}: {exc}")
    return proc
