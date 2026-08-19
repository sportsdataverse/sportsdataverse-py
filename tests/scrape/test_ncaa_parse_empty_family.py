"""The parse stage must not drop a team that parses cleanly and yields nothing.

There are THREE ways a team's lineups can vanish, and only two were covered:

1. `get_box_lineup` returns `list[ParseError]`   -> logged by `_log_family_skip`
2. something raises                              -> logged by `parse_bundle`
3. everything succeeds, every stint is bad,
   `good == []`                                  -> WAS SILENT

Path 3 is the sibling-code signature: the roster parses, the play-by-play
parses, every stint is flagged `player_count_error`, and the team contributes
nothing. It is how Kansas came out of a corpus re-parse at 1/36 (2010) and
0/38 (2011) games while the run reported EXIT=0, `failed=0` on every shard,
and zero skip warnings.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import polars as pl
import pytest

import sportsdataverse.scrape.ncaa.parse as parse_mod


@pytest.fixture
def pbp_df() -> pl.DataFrame:
    return pl.DataFrame({"home": ["Kansas"], "away": ["Hofstra"]})


def _box(n_players: int = 5):
    players = [SimpleNamespace(code=f"P{i}", id=SimpleNamespace(name=f"Player {i}")) for i in range(n_players)]
    return SimpleNamespace(players=players)


def test_empty_but_successful_parse_is_logged(monkeypatch, caplog, pbp_df) -> None:
    """good == [] with no error must still name the team in the log."""
    monkeypatch.setattr(parse_mod, "get_box_lineup", lambda *a, **k: _box())
    # Succeeds -- returns a tuple, not a ParseError -- but every stint is bad.
    monkeypatch.setattr(parse_mod, "create_lineup_data", lambda *a, **k: ([], ["bad1", "bad2", "bad3"]))

    with caplog.at_level(logging.WARNING, logger=parse_mod.logger.name):
        out = parse_mod._parse_lineups("999", pbp_df, "<pbp/>", "<box/>")

    assert out == []
    msgs = [r.getMessage() for r in caplog.records]
    assert any("NO good stints" in m for m in msgs), msgs
    assert any("team=Kansas" in m for m in msgs), msgs
    assert any("team=Hofstra" in m for m in msgs), msgs
    assert any("3 bad" in m for m in msgs), msgs


def test_a_healthy_team_logs_nothing(monkeypatch, caplog, pbp_df) -> None:
    """No false positives -- a team with usable stints must stay quiet.

    Without this, the guard could 'pass' by warning on everything.
    """
    monkeypatch.setattr(parse_mod, "get_box_lineup", lambda *a, **k: _box())
    monkeypatch.setattr(parse_mod, "_jsonable", lambda ev: {"ev": ev})
    monkeypatch.setattr(parse_mod, "create_lineup_data", lambda *a, **k: (["good1", "good2"], []))

    with caplog.at_level(logging.WARNING, logger=parse_mod.logger.name):
        out = parse_mod._parse_lineups("999", pbp_df, "<pbp/>", "<box/>")

    assert len(out) == 4  # two teams x two stints
    assert not [r for r in caplog.records if "NO good stints" in r.getMessage()]
