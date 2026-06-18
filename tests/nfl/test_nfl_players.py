"""Tests for the public-ESPN NFL players builder (``build_nfl_players``) and
the pure-consumer ID crosswalk (``nfl_players_crosswalk``).

The offline tests monkeypatch the ESPN athlete fetch to a synthetic payload and
assert the SDV-native schema + the highest-espn_id dedup rule. The live tests
(gated by ``SDV_PY_LIVE_TESTS=1``) hit the real public ESPN / nflverse surfaces
and assert non-empty frames with the documented invariants.
"""

from __future__ import annotations

import polars as pl

import sportsdataverse.nfl.nfl_players as players_mod
from sportsdataverse.nfl import build_nfl_players, nfl_players_crosswalk
from tests.conftest import skip_if_no_live

# Minimal columns the players frame must carry.
_CORE_COLS = {
    "espn_id",
    "full_name",
    "first_name",
    "last_name",
    "position",
    "jersey",
    "height",
    "weight",
    "birth_date",
    "status",
    "headshot_url",
    "gsis_id",
}


def _athlete(athlete_id, full_name, dob, position="QB"):
    """One ESPN core-v2 athlete-detail payload (shape mirrors the live API)."""
    first, _, last = full_name.partition(" ")
    return {
        "id": str(athlete_id),
        "firstName": first,
        "lastName": last or first,
        "fullName": full_name,
        "displayName": full_name,
        "weight": 225.0,
        "height": 74.0,
        "dateOfBirth": dob,
        "jersey": "15",
        "status": {"id": "1", "name": "Active", "type": "active"},
        "headshot": {"href": f"https://a.espncdn.com/i/headshots/nfl/players/full/{athlete_id}.png"},
        "position": {"id": "8", "name": "Quarterback", "abbreviation": position},
        "team": {"$ref": "http://sports.core.api.espn.com/.../teams/12?lang=en"},
    }


def _fake_athletes():
    """Two distinct players, plus a duplicate-id pair for the same player.

    "Old Player" appears with both a legacy 4-digit espn_id (1234) and a modern
    7-digit espn_id (3139477) for the SAME (full_name, birth_date) — the dedup
    rule must keep the higher (modern) id.
    """
    return [
        _athlete(1234, "Old Player", "1990-01-01T07:00Z"),
        _athlete(3139477, "Old Player", "1990-01-01T07:00Z"),
        _athlete(4242, "Solo Receiver", "1995-05-05T07:00Z", position="WR"),
    ]


def test_build_nfl_players_parses_schema(monkeypatch):
    # Disable the players-table enrichment so the unit test is fully offline.
    monkeypatch.setattr(players_mod, "_enrich_cross_ids", lambda frame: frame)
    monkeypatch.setattr(players_mod, "_fetch_athletes", lambda limit=None: _fake_athletes())

    df = build_nfl_players()
    assert isinstance(df, pl.DataFrame)
    # Two distinct players after the duplicate "Old Player" rows collapse.
    assert df.height == 2
    # Full documented schema, in order.
    assert list(df.columns) == list(players_mod._SCHEMA.keys())
    assert _CORE_COLS.issubset(set(df.columns))
    # espn_id present + unique per player.
    assert df["espn_id"].null_count() == 0
    assert df["espn_id"].n_unique() == df.height
    # Field-mapping spot-checks.
    wr = df.filter(pl.col("full_name") == "Solo Receiver").row(0, named=True)
    assert wr["position"] == "WR"
    assert wr["first_name"] == "Solo"
    assert wr["status"] == "Active"
    assert wr["headshot_url"].endswith("4242.png")


def test_build_nfl_players_espn_id_dedup_keeps_highest(monkeypatch):
    monkeypatch.setattr(players_mod, "_enrich_cross_ids", lambda frame: frame)
    monkeypatch.setattr(players_mod, "_fetch_athletes", lambda limit=None: _fake_athletes())

    df = build_nfl_players()
    old = df.filter(pl.col("full_name") == "Old Player")
    # Exactly one row survives and it carries the HIGHER (modern) espn_id.
    assert old.height == 1
    assert old.row(0, named=True)["espn_id"] == "3139477"


def test_build_nfl_players_empty_fetch(monkeypatch):
    monkeypatch.setattr(players_mod, "_enrich_cross_ids", lambda frame: frame)
    monkeypatch.setattr(players_mod, "_fetch_athletes", lambda limit=None: [])

    df = build_nfl_players()
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    # Empty frame still carries the full documented schema.
    assert list(df.columns) == list(players_mod._SCHEMA.keys())


def test_build_nfl_players_pandas(monkeypatch):
    import pandas as pd

    monkeypatch.setattr(players_mod, "_enrich_cross_ids", lambda frame: frame)
    monkeypatch.setattr(players_mod, "_fetch_athletes", lambda limit=None: _fake_athletes())

    df = build_nfl_players(return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


def test_build_nfl_players_enrichment(monkeypatch):
    """The cross-id enrichment fills gsis_id from the players table by espn_id."""
    monkeypatch.setattr(players_mod, "_fetch_athletes", lambda limit=None: _fake_athletes())

    fake_players = pl.DataFrame(
        {
            "espn_id": ["3139477"],
            "display_name": ["Old Player"],
            "birth_date": ["1990-01-01"],
            "gsis_id": ["00-0033333"],
            "college_name": ["State U"],
        }
    )
    monkeypatch.setattr(
        "sportsdataverse.nfl.nfl_loaders.load_nfl_players",
        lambda *a, **k: fake_players,
    )

    df = build_nfl_players()
    old = df.filter(pl.col("full_name") == "Old Player").row(0, named=True)
    assert old["gsis_id"] == "00-0033333"
    assert old["college"] == "State U"


def test_nfl_players_crosswalk_empty(monkeypatch):
    monkeypatch.setattr(
        "sportsdataverse.nfl.nfl_loaders.load_nfl_players",
        lambda *a, **k: pl.DataFrame(),
    )
    xwalk = nfl_players_crosswalk()
    assert isinstance(xwalk, pl.DataFrame)
    assert xwalk.height == 0
    assert "gsis_id" in xwalk.columns and "espn_id" in xwalk.columns


def test_nfl_players_crosswalk_slices_and_dedups(monkeypatch):
    fake_players = pl.DataFrame(
        {
            "gsis_id": ["00-0011111", "00-0011111", "00-0022222"],
            "display_name": ["A Player", "A Player", "B Player"],
            "position": ["QB", "QB", "WR"],
            "espn_id": ["111", "111", "222"],
            "pfr_id": ["Aaaa01", "Aaaa01", "Bbbb02"],
            "height": [74, 74, 70],  # non-crosswalk column should be dropped
        }
    )
    monkeypatch.setattr(
        "sportsdataverse.nfl.nfl_loaders.load_nfl_players",
        lambda *a, **k: fake_players,
    )
    xwalk = nfl_players_crosswalk()
    assert xwalk.height == 2  # deduped on gsis_id
    assert "full_name" in xwalk.columns and "position" in xwalk.columns
    assert "height" not in xwalk.columns  # non-crosswalk column dropped
    assert set(xwalk["gsis_id"].to_list()) == {"00-0011111", "00-0022222"}


@skip_if_no_live
def test_build_nfl_players_live():
    df = build_nfl_players()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert _CORE_COLS.issubset(set(df.columns))
    # espn_id present + unique per player (the dedup invariant).
    assert df["espn_id"].null_count() == 0
    assert df["espn_id"].n_unique() == df.height


@skip_if_no_live
def test_nfl_players_crosswalk_live():
    xwalk = nfl_players_crosswalk()
    assert isinstance(xwalk, pl.DataFrame)
    assert xwalk.height > 0
    assert "gsis_id" in xwalk.columns and "espn_id" in xwalk.columns
    # gsis_id is the dedup key — densely populated.
    assert xwalk["gsis_id"].null_count() == 0
