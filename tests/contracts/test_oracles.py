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
