"""Smoke tests for sportsdataverse.wnba.wnba_team_roster."""

from __future__ import annotations

import polars as pl

from sportsdataverse.wnba import espn_wnba_team_roster
from tests.conftest import skip_if_no_live

CORE_COLUMNS: set[str] = {
    "athlete_id",
    "full_name",
    "jersey",
    "position_abbreviation",
    "team_id",
    "season",
}


@skip_if_no_live
def test_espn_wnba_team_roster_returns_polars_with_core_columns():
    df = espn_wnba_team_roster(team_id=3, season=2024)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"
    assert df["season"].unique().to_list() == [2024]
    assert df["team_id"].unique().to_list() == [3]


@skip_if_no_live
def test_espn_wnba_team_roster_raw_returns_dict():
    raw = espn_wnba_team_roster(team_id=3, season=2024, raw=True)
    assert isinstance(raw, dict)
    # ESPN sometimes nests differently, so accept either top-level shape.
    assert "athletes" in raw or "team" in raw


@skip_if_no_live
def test_espn_wnba_team_roster_return_as_pandas_returns_pandas():
    import pandas as pd

    df = espn_wnba_team_roster(team_id=3, season=2024, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_return_parsed_false_returns_the_payload_and_is_not_sent_to_download(monkeypatch):
    """find_athlete asks every roster for ``return_parsed=False``; WNBA/WBB forwarded it to download() (TypeError)."""
    from sportsdataverse.wbb import wbb_team_roster

    sent: dict = {}
    payload = {"athletes": [], "team": {"id": "3"}}

    def fake_download_json(transport, url, **kwargs):
        sent.update(kwargs)
        return payload

    monkeypatch.setattr(wbb_team_roster, "_download_json", fake_download_json)
    assert espn_wnba_team_roster(team_id=3, return_parsed=False) is payload
    assert "return_parsed" not in sent
