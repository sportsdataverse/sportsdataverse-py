"""``mlb_api_extra`` sends list-like params through the shared ``_csv`` (B-L86). Offline."""

from __future__ import annotations

import numpy as np
import pytest

from sportsdataverse.mlb import mlb_api_extra


@pytest.mark.parametrize(
    ("league_id", "wire"),
    [({104, 103}, "103,104"), ([103, 104], "103,104"), (np.array([103, 104]), "103,104"), (103.0, "103")],
    ids=["set", "list", "numpy", "float"],
)
def test_league_id_reaches_the_wire_as_csv(league_id, wire, monkeypatch):
    """The private copy sent a set as the text ``"{103, 104}"`` and a float as ``"103.0"``."""
    sent = {}
    monkeypatch.setattr(mlb_api_extra, "_get", lambda path, params=None, **kw: sent.update(params) or {})
    mlb_api_extra.mlb_schedule(date="2024-07-01", league_id=league_id)
    assert sent["leagueId"] == wire
