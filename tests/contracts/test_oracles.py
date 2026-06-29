from __future__ import annotations

import polars as pl
from tools.validation.oracles import ORACLES, CfbSelfOracle, NflfastrOracle


def test_cfb_oracle_has_no_external_reference():
    keys = pl.DataFrame({"game_id": [1]})
    assert CfbSelfOracle().reference_frame("espn_cfb_pbp", keys) is None


def test_nfl_oracle_without_source_returns_none():
    keys = pl.DataFrame({"game_id": [1]})
    assert NflfastrOracle().reference_frame("nfl_pbp", keys) is None


def test_registry_keys():
    assert set(ORACLES) == {"cfb", "nfl"}
    assert ORACLES["nfl"].thresholds["ep"] == 0.99


def test_reference_frame_casts_float_key_to_producer_int(monkeypatch):
    ref_data = pl.DataFrame(
        {
            "game_id": pl.Series(["2024_01_KC_BAL"], dtype=pl.Utf8),
            "play_id": pl.Series([100.0], dtype=pl.Float64),  # R serialises ints as Float64
            "ep": pl.Series([2.5], dtype=pl.Float64),
        }
    )
    monkeypatch.setattr(pl, "scan_parquet", lambda path: ref_data.lazy())
    oracle = NflfastrOracle(source_glob="/fake/*.parquet")
    keys = pl.DataFrame(
        {
            "game_id": pl.Series(["2024_01_KC_BAL"], dtype=pl.Utf8),
            "play_id": pl.Series([100], dtype=pl.Int64),  # producer dtype
        }
    )
    result = oracle.reference_frame("nfl_model_pbp", keys)
    assert result is not None
    assert result.schema["play_id"] == pl.Int64  # cast aligned to producer
    assert result.height == 1  # join is non-vacuous


def test_reference_frame_none_when_no_shared_keys(monkeypatch):
    ref_data = pl.DataFrame({"foo": pl.Series([1], dtype=pl.Int64)})
    monkeypatch.setattr(pl, "scan_parquet", lambda path: ref_data.lazy())
    oracle = NflfastrOracle(source_glob="/fake/*.parquet")
    keys = pl.DataFrame({"game_id": pl.Series(["x"], dtype=pl.Utf8)})
    assert oracle.reference_frame("nfl_model_pbp", keys) is None


def test_reference_frame_none_when_unwired():
    assert NflfastrOracle(source_glob=None).reference_frame("nfl_model_pbp", pl.DataFrame({"game_id": ["x"]})) is None
