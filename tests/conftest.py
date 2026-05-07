"""Shared pytest fixtures and skip helpers for the sportsdataverse test suite.

Live-API gating
---------------
Tests that hit external services (ESPN, etc.) are gated behind the
SDV_PY_LIVE_TESTS environment variable so CI doesn't flake on upstream
downtime and so contributors don't accidentally hit live endpoints during
local development.

Set ``SDV_PY_LIVE_TESTS=1`` to enable them::

    SDV_PY_LIVE_TESTS=1 pytest tests/wbb/

The gating is OPT-IN — without the env var, gated tests are skipped, not
failed.
"""

from __future__ import annotations

import os

import pytest

LIVE: bool = os.environ.get("SDV_PY_LIVE_TESTS") == "1"

skip_if_no_live = pytest.mark.skipif(
    not LIVE,
    reason="Set SDV_PY_LIVE_TESTS=1 to run tests that hit live external APIs",
)
