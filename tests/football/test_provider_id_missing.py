"""#930: a provider game id missing from a pandas-built id-map row is no id, never the string "nan".

NaN is truthy and ``str(nan)`` is ``"nan"``; ``pandas.NA`` raises on truthiness. Each resolver reads
its stored id through ``_id_text``, so all four missing forms fall through to the next leg.
"""

from __future__ import annotations

import types

import pandas as pd
import pytest

from sportsdataverse.football.sources.dispatch import SourceUnavailable

MISSING = [float("nan"), pd.NA, None, ""]
_TEAMS = {"home_espn_team_id": 2, "away_espn_team_id": 8}


@pytest.mark.parametrize("missing", MISSING, ids=["nan", "pd.NA", "None", "empty"])
def test_cfb_fox_falls_through(monkeypatch, missing):
    from sportsdataverse.cfb.fox_pbp import to_espn_summary as m

    monkeypatch.setattr(m, "_crosswalk_fox_id", lambda espn_id, seasons: None)
    assert m._resolve_fox_event_id(401, {"fox_event_id": missing, "season": 2024}) == (None, "unresolved")
    assert m._resolve_fox_event_id(401, {"fox_event_id": 12345.0, "season": 2024}) == ("12345", "idmap")


@pytest.mark.parametrize("missing", MISSING, ids=["nan", "pd.NA", "None", "empty"])
def test_nfl_fox_falls_through(monkeypatch, missing):
    from sportsdataverse.nfl.fox_pbp import to_espn_summary as m

    searched = []
    monkeypatch.setattr(m, "_resolve_fox_game_id", lambda *a, **k: searched.append(a) or (None, "unresolved"))
    assert m._resolve_fox_event_id({"fox_event_id": missing, **_TEAMS})[0] is None
    assert searched, "a missing stored id must fall through to the search"


@pytest.mark.parametrize("missing", MISSING, ids=["nan", "pd.NA", "None", "empty"])
def test_cfb_ncaa_falls_through(missing):
    from sportsdataverse.cfb.ncaa_pbp.fetch import _resolve_contest_id

    assert _resolve_contest_id({"ncaa_game_id": missing}) == (None, "unresolved")
    assert _resolve_contest_id({"ncaa_game_id": 6305900.0}) == ("6305900", "idmap")


@pytest.mark.parametrize("missing", MISSING, ids=["nan", "pd.NA", "None", "empty"])
def test_cfb_yahoo_falls_through(monkeypatch, missing):
    from sportsdataverse.cfb.yahoo_pbp import fetch as m

    monkeypatch.setattr(m, "_crosswalk_yahoo_id", lambda espn_id, seasons: None)
    row = {"yahoo_game_id": missing, "kickoff_utc": None, "home_team": {"yahoo_team_id": missing}, **_TEAMS}
    game_id, resolved_by = m._resolve_game_id(401, row, (2024,))
    assert game_id is None or "nan" not in game_id
    assert resolved_by != "idmap"
    # a missing home_team cell (NaN in a pandas-built row) is no team, not a crash
    assert m._resolve_game_id(401, {**row, "home_team": missing}, (2024,))[1] != "idmap"


@pytest.mark.parametrize("missing", MISSING, ids=["nan", "pd.NA", "None", "empty"])
def test_nfl_yahoo_falls_through(monkeypatch, missing):
    from sportsdataverse.nfl.yahoo_pbp import fetch as m

    monkeypatch.setattr(m, "_row_from_nflverse_schedule", lambda espn_id: None)
    _, game_id, provenance = m._resolve_row(401, {"yahoo_game_id": missing, "kickoff_utc": None, **_TEAMS})
    assert game_id is None
    assert provenance["yahoo_id_resolved_by"] == "unresolved"


@pytest.mark.parametrize("league", ["cfb", "nfl"])
@pytest.mark.parametrize("missing", MISSING, ids=["nan", "pd.NA", "None", "empty"])
def test_cbs_missing_id_looks_it_up_instead(league, missing):
    """With no stored id and no season/week to search with, the adapter hands over, not fetches "nan"."""
    if league == "cfb":
        from sportsdataverse.cfb.cbs_pbp.to_espn_summary import _cbs_adapter
    else:
        from sportsdataverse.nfl.cbs_pbp.to_espn_summary import _cbs_adapter

    ctx = types.SimpleNamespace(payload=None, idmap_row={"cbs_game_id": missing, "season": 2024, **_TEAMS})
    with pytest.raises(SourceUnavailable, match="no season/week"):
        _cbs_adapter(league, 401, ctx)
