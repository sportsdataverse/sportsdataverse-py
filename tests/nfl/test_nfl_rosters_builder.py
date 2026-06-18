"""Tests for the public-Shield NFL roster builder (``build_nfl_rosters``).

The offline test monkeypatches the Shield wrapper to a synthetic payload and
asserts the SDV-native schema + season-aware team relocations. The live test
(gated by ``SDV_PY_LIVE_TESTS=1``) hits the real Shield API and asserts a
non-empty frame with a dense ``gsis_id`` and the core columns present.
"""

from __future__ import annotations

import polars as pl

import sportsdataverse.nfl.nfl_roster_builder as rosters_mod
from sportsdataverse.nfl import build_nfl_rosters
from tests.conftest import skip_if_no_live

# Minimal columns every roster frame must carry.
_CORE_COLS = {
    "season",
    "team",
    "position",
    "jersey_number",
    "status",
    "full_name",
    "gsis_id",
    "years_exp",
    "headshot_url",
}


def _fake_payload(season: int):
    """A 2-team Shield ``/football/v2/rosters`` payload for *season*."""
    return {
        "rosters": [
            {
                "season": season,
                "seasonType": "REG",
                "team": {"abbreviation": "OAK"},
                "persons": [
                    {
                        "displayName": "Test Quarterback",
                        "firstName": "Test",
                        "lastName": "Quarterback",
                        "commonFirstName": "Test",
                        "position": "QB",
                        "positionGroup": "QB",
                        "jerseyNumber": 7,
                        "status": "ACT",
                        "birthDate": "1990-01-01",
                        "height": 75,
                        "weight": 220,
                        "collegeNames": ["State U"],
                        "gsisId": "00-0011111",
                        "esbId": "ESB011111",
                        "nflExperience": 3,
                        "headshot": "https://example.test/qb.png",
                    }
                ],
            },
            {
                "season": season,
                "seasonType": "REG",
                "team": {"abbreviation": "JAC"},
                "persons": [
                    {
                        "displayName": "Test Receiver",
                        "firstName": "Test",
                        "lastName": "Receiver",
                        "commonFirstName": "Test",
                        "position": "WR",
                        "positionGroup": "WR",
                        "jerseyNumber": None,
                        "status": "CUT",
                        "birthDate": "1995-05-05",
                        "height": 70,
                        "weight": 190,
                        "collegeNames": [],
                        "gsisId": "00-0022222",
                        "esbId": "ESB022222",
                        "nflExperience": 0,
                        "headshot": None,
                    }
                ],
            },
        ],
        "pagination": {},
    }


def test_build_nfl_rosters_parses_schema(monkeypatch):
    # Disable the players-table enrichment so the unit test is fully offline.
    monkeypatch.setattr(rosters_mod, "_enrich_cross_ids", lambda frame: frame)
    monkeypatch.setattr(rosters_mod, "nfl_rosters", lambda season, return_parsed=False: _fake_payload(season))

    df = build_nfl_rosters([2023])
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    # Full documented schema, in order.
    assert list(df.columns) == list(rosters_mod._SCHEMA.keys())
    assert _CORE_COLS.issubset(set(df.columns))
    # gsis_id densely populated by Shield.
    assert df["gsis_id"].null_count() == 0
    # Field mapping spot-checks.
    qb = df.filter(pl.col("gsis_id") == "00-0011111").row(0, named=True)
    assert qb["full_name"] == "Test Quarterback"
    assert qb["depth_chart_position"] == "QB"  # positionGroup
    assert qb["college"] == "State U"  # collegeNames[0]
    assert qb["esb_id"] == "ESB011111"
    assert qb["entry_year"] == 2023 - 3  # season - nflExperience


def test_build_nfl_rosters_relocations(monkeypatch):
    monkeypatch.setattr(rosters_mod, "_enrich_cross_ids", lambda frame: frame)

    # 2019: OAK predates the 2020 LV move -> stays OAK; JAC always -> JAX.
    monkeypatch.setattr(rosters_mod, "nfl_rosters", lambda season, return_parsed=False: _fake_payload(season))
    df_2019 = build_nfl_rosters([2019])
    assert set(df_2019["team"].to_list()) == {"OAK", "JAX"}

    # 2020+: OAK -> LV.
    df_2021 = build_nfl_rosters([2021])
    assert set(df_2021["team"].to_list()) == {"LV", "JAX"}


def test_build_nfl_rosters_empty_season(monkeypatch):
    monkeypatch.setattr(rosters_mod, "_enrich_cross_ids", lambda frame: frame)
    monkeypatch.setattr(rosters_mod, "nfl_rosters", lambda season, return_parsed=False: {"rosters": []})
    df = build_nfl_rosters([1899])
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    # Empty frame still carries the full documented schema.
    assert list(df.columns) == list(rosters_mod._SCHEMA.keys())


def test_build_nfl_rosters_pandas(monkeypatch):
    import pandas as pd

    monkeypatch.setattr(rosters_mod, "_enrich_cross_ids", lambda frame: frame)
    monkeypatch.setattr(rosters_mod, "nfl_rosters", lambda season, return_parsed=False: _fake_payload(season))
    df = build_nfl_rosters([2023], return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


@skip_if_no_live
def test_build_nfl_rosters_live_2023():
    df = build_nfl_rosters([2023])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert _CORE_COLS.issubset(set(df.columns))
    # Shield supplies gsis_id densely.
    dense = 1.0 - (df["gsis_id"].null_count() / df.height)
    assert dense > 0.95, f"gsis_id only {dense:.1%} populated"
    # 2023 is post-relocation; nflverse-standard abbreviations only.
    teams = set(df["team"].to_list())
    assert "OAK" not in teams and "JAC" not in teams
