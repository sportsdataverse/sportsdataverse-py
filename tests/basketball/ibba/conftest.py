"""Fixtures shared by the IBBA ASMX and Segev tests."""

from __future__ import annotations

from typing import List

import pytest

from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    return type("W", (), {"urls": seen, "slept": slept})
