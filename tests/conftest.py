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

# stats.nba.com / stats.wnba.com hang on datacenter / cloud IPs: the TLS/JA3
# fingerprint block compounds with IP reputation, so even with curl_cffi browser
# impersonation the request silently stalls (not a fast failure) from CI runners.
# These live tests therefore need a SEPARATE opt-in that NO CI workflow sets
# (tests.yml + live-tests-cron.yml only ever set SDV_PY_LIVE_TESTS), so they run
# only when a contributor explicitly enables them from a residential IP.
NBA_STATS_LIVE: bool = os.environ.get("SDV_PY_NBA_STATS_LIVE") == "1"

skip_if_no_nba_stats_live = pytest.mark.skipif(
    not NBA_STATS_LIVE,
    reason=(
        "stats.nba.com/stats.wnba.com hang on datacenter/cloud IPs; set "
        "SDV_PY_NBA_STATS_LIVE=1 to run these live tests from a residential IP"
    ),
)

# premium.pff.com is paywalled (a PFF+ session cookie) and best exercised from a
# residential IP; like the nba-stats gate, NO CI workflow sets this — it runs
# only when a contributor explicitly enables it.
PFF_LIVE: bool = os.environ.get("SDV_PY_PFF_LIVE") == "1"

skip_if_no_pff_live = pytest.mark.skipif(
    not PFF_LIVE,
    reason=(
        "PFF Premium is paywalled (needs a PFF+ session); set SDV_PY_PFF_LIVE=1 "
        "to run these live tests from a residential IP"
    ),
)

# ipa/www.247sports.com sit behind a Fastly edge that may hang (not fail fast) on
# datacenter/CI IPs the way stats.nba.com does; both 247 tracks (RDB + site-pages)
# share this ONE gate, which NO CI workflow sets.
SPORTS247_LIVE: bool = os.environ.get("SDV_PY_247_LIVE") == "1"

skip_if_no_247_live = pytest.mark.skipif(
    not SPORTS247_LIVE,
    reason=(
        "ipa/www.247sports.com may hang on datacenter/CI IPs; set SDV_PY_247_LIVE=1 "
        "to run these live tests from a residential IP"
    ),
)

# Concurrent-validity tests correlate a computed metric (e.g. nba_la_rapm,
# nba_decay_rapm) against a real published oracle (Ryan Davis RAPM CSVs). The
# oracle files aren't bundled with the repo, so these tests skip cleanly
# unless a contributor points SDV_PY_NBA_ORACLE_DIR at a local checkout.
skip_if_no_nba_oracle = pytest.mark.skipif(
    not os.environ.get("SDV_PY_NBA_ORACLE_DIR"),
    reason="SDV_PY_NBA_ORACLE_DIR not set (Ryan Davis oracle CSVs unavailable)",
)


def _rscript_available() -> bool:
    try:
        from tools.validation.lint.leakage_r import rscript_path
    except ImportError:
        return False
    return rscript_path() is not None


skip_if_no_rscript = pytest.mark.skipif(
    not _rscript_available(),
    reason="Rscript not found — install R or set SDV_RSCRIPT to run the R-lint live tests",
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
