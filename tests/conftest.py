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

import os

import pytest

LIVE: bool = os.environ.get("SDV_PY_LIVE_TESTS") == "1"

skip_if_no_live = pytest.mark.skipif(
    not LIVE,
    reason="Set SDV_PY_LIVE_TESTS=1 to run tests that hit live external APIs",
)
