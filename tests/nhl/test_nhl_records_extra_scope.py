"""``nhl_records_comeback_wins(scope=)`` validates like its siblings (B-M27). Offline."""

from __future__ import annotations

import pytest

from sportsdataverse.nhl import nhl_records_extra


@pytest.mark.parametrize("scope", ["Franchise", "team", ""])
def test_unknown_scope_raises_instead_of_returning_the_league_table(scope, monkeypatch):
    monkeypatch.setattr(nhl_records_extra, "_fetch", lambda *a, **k: pytest.fail("no request for a bad scope"))
    with pytest.raises(ValueError, match="scope must be one of"):
        nhl_records_extra.nhl_records_comeback_wins(scope=scope)


@pytest.mark.parametrize(
    ("scope", "path"), [("league", "/comeback-league-wins"), ("franchise", "/comeback-franchise-wins")]
)
def test_valid_scopes_route_to_their_endpoint(scope, path, monkeypatch):
    seen = []
    monkeypatch.setattr(nhl_records_extra, "_fetch", lambda p, **k: seen.append(p) or {})
    nhl_records_extra.nhl_records_comeback_wins(scope=scope)
    assert seen == [path]
