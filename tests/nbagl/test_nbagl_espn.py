"""Offline tests for the generated ESPN G League family (``espn_nbagl_*``, ``basketball/nba-development``).

Each wrapper is driven through its real codegen body + parser with the ``_get`` HTTP
chokepoint mocked by a real captured G League payload (``tests/fixtures/espn/*_nbagl.json``).
"""

from __future__ import annotations

import polars as pl
import pytest

import sportsdataverse as sdv
from sportsdataverse.nbagl import nbagl_espn_ext as ext
from tests.conftest import load_fixture

CASES = [
    # (wrapper, fixture stem, kwargs, columns the parsed frame must carry, min rows)
    ("espn_nbagl_standings", "standings_nbagl", {"season": 2026}, {"group_name", "team_id", "wins", "losses"}, 25),
    ("espn_nbagl_teams_site", "teams_nbagl", {}, {"team_id", "team_abbreviation", "team_display_name"}, 25),
    ("espn_nbagl_scoreboard", "scoreboard_nbagl", {"dates": "20260115"}, {"game_id", "home_id", "away_id"}, 1),
]


@pytest.mark.parametrize(("name", "stem", "kwargs", "cols", "min_rows"), CASES)
def test_espn_nbagl_wrapper_parses_real_payload(name, stem, kwargs, cols, min_rows, monkeypatch):
    fixture = load_fixture("espn", stem)
    urls: list[str] = []

    def fake_get(url, *a, **k):
        urls.append(url)
        return fixture

    monkeypatch.setattr(ext, "_get", fake_get)
    fn = getattr(ext, name)

    df = fn(**kwargs)
    assert isinstance(df, pl.DataFrame)
    assert df.height >= min_rows
    assert cols <= set(df.columns), f"{name}: missing {cols - set(df.columns)}"
    assert "/basketball/nba-development/" in urls[0]

    assert fn(**kwargs, return_parsed=False) is fixture


def test_espn_nbagl_standings_covers_both_conferences(monkeypatch):
    monkeypatch.setattr(ext, "_get", lambda *a, **k: load_fixture("espn", "standings_nbagl"))
    df = ext.espn_nbagl_standings(season=2026)
    assert set(df["group_name"].unique()) == {"Eastern Conference", "Western Conference"}
    assert df["team_id"].n_unique() == df.height  # one row per team


def test_espn_nbagl_is_exported_top_level_and_from_package():
    from sportsdataverse.nbagl import espn_nbagl_scoreboard, espn_nbagl_standings, espn_nbagl_teams_site

    assert sdv.espn_nbagl_standings is espn_nbagl_standings
    assert sdv.espn_nbagl_teams_site is espn_nbagl_teams_site
    assert sdv.espn_nbagl_scoreboard is espn_nbagl_scoreboard
    assert len([n for n in ext.__all__ if n.startswith("espn_nbagl_")]) >= 100
