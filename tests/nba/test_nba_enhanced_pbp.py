"""Tests for NBA enhanced PBP engine."""

import json
import pathlib

import polars as pl
from sportsdataverse.nba import nba_pbp_constants as C


def test_constants_shape():
    """Verify constants module shape and basic functionality."""
    assert C.ACTION_TYPE_EVENT["Substitution"] == "substitution"
    assert C.ACTION_TYPE_EVENT["Made Shot"] == "made_shot"
    assert "is_substitution" in C.EVENT_FLAG_COLUMNS
    assert C.ENHANCED_PBP_SCHEMA["game_id"] == pl.Utf8
    assert C.ENHANCED_PBP_SCHEMA["person_id"] == pl.Int64
    assert C.LINEUPS_SCHEMA["home_player_1"] == pl.Int64
    df = pl.DataFrame({"clock": ["PT08M24.00S", "PT12M00.00S"]})
    secs = df.select(C.iso_clock_to_seconds(pl.col("clock")).alias("s"))["s"].to_list()
    assert abs(secs[0] - 504.0) < 1e-6 and abs(secs[1] - 720.0) < 1e-6


def _payload() -> dict:
    """Load fixture payload."""
    fx = pathlib.Path("tests/fixtures/nba_engine/0022200001")
    return json.loads((fx / "playbyplayv3.json").read_text())


def test_ingest_normalizes_v3():
    """Test enhanced_pbp_from_payload ingests and normalizes v3 actions."""
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload

    df = enhanced_pbp_from_payload(_payload())
    assert df.schema["game_id"] == pl.Utf8 and df.schema["person_id"] == pl.Int64
    assert df["game_id"][0] == "0022200001"
    row = df.filter(pl.col("clock") == "PT08M24.00S").head(1)
    assert abs(row["seconds_remaining"][0] - 504.0) < 1e-6
    assert df.height == 468


FX = pathlib.Path("tests/fixtures/nba_engine/0022200001")


def test_event_flags_and_order_match_fixture():
    """Event type, is_substitution, and order_index must exactly match the committed fixture."""
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload

    exp = pl.read_parquet(FX / "enhanced_pbp_expected.parquet")
    df = enhanced_pbp_from_payload(_payload())
    got = df.select(["action_number", "event_type", "is_substitution", "order_index"]).sort("order_index")
    e = exp.select(["action_number", "event_type", "is_substitution", "order_index"]).sort("order_index")
    assert got.equals(e)
    assert df.filter(pl.col("event_type") == "substitution").height == 44
    assert df["order_index"].n_unique() == df.height


# ---------------------------------------------------------------------------
# Offline tests for the public nba_enhanced_pbp() fetcher (Task 6)
# ---------------------------------------------------------------------------


def test_nba_enhanced_pbp_offline(monkeypatch) -> None:
    """nba_enhanced_pbp() with monkeypatched _fetch_pbp returns the same frame as the pure function."""
    import sportsdataverse.nba.nba_enhanced_pbp as mod
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload, nba_enhanced_pbp

    fixture_payload = _payload()
    monkeypatch.setattr(mod, "_fetch_pbp", lambda game_id, league_id="00": fixture_payload)

    df = nba_enhanced_pbp("0022200001")
    expected = enhanced_pbp_from_payload(fixture_payload)

    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.equals(expected), "nba_enhanced_pbp() must return the same frame as enhanced_pbp_from_payload()"
    # home_player_* columns are not part of enhanced_pbp — verify the key schema fields
    assert "home_player_1" not in df.columns
    assert df.schema["game_id"] == pl.Utf8
    assert df.schema["person_id"] == pl.Int64


def test_nba_enhanced_pbp_return_as_pandas(monkeypatch) -> None:
    """nba_enhanced_pbp(return_as_pandas=True) returns a pandas DataFrame."""
    import pandas as pd

    import sportsdataverse.nba.nba_enhanced_pbp as mod
    from sportsdataverse.nba.nba_enhanced_pbp import nba_enhanced_pbp

    fixture_payload = _payload()
    monkeypatch.setattr(mod, "_fetch_pbp", lambda game_id, league_id="00": fixture_payload)

    df_pd = nba_enhanced_pbp("0022200001", return_as_pandas=True)
    assert isinstance(df_pd, pd.DataFrame)
    assert len(df_pd) > 0
    assert "game_id" in df_pd.columns
