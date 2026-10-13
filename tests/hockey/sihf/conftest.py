"""The SIHF runtime spaces requests 1 s apart; offline tests skip the wait, the ``test_live_*`` ones keep it."""

from __future__ import annotations

import pytest

from sportsdataverse.hockey import euro_hockey_runtime


@pytest.fixture(autouse=True)
def _no_polite_wait(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if not request.node.name.startswith("test_live"):
        monkeypatch.setattr(euro_hockey_runtime, "_polite_wait", lambda url, *a, **kw: None)
