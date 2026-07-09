"""Phase 0 -- MEASURE_SPECS/oracle helpers + fetch/role-bucket/engine tests."""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nba.nba_tracking_value import _attach_role_bucket, _over_expected, _pin_ids, _season_str
from sportsdataverse.nba.nba_tracking_value_constants import (
    ELITE_ORACLE,
    LEAGUE_IDS,
    MEASURE_SPECS,
    residual_sums_to_zero,
    top_k_ids,
)


def test_league_ids():
    assert LEAGUE_IDS["nba"] == "00" and LEAGUE_IDS["wnba"] == "10" and LEAGUE_IDS["gleague"] == "20"


def test_measure_specs_cover_six_models():
    assert set(MEASURE_SPECS) >= {"reb", "ast", "drive", "cs", "pu", "touch", "rim"}


def test_residual_sums_to_zero_true_and_false():
    ok = pl.DataFrame({"b": ["g", "g", "w"], "oe": [1.0, -1.0, 0.0]})
    bad = pl.DataFrame({"b": ["g", "g"], "oe": [1.0, 0.5]})
    assert residual_sums_to_zero(ok, "oe", ["b"]) is True
    assert residual_sums_to_zero(bad, "oe", ["b"]) is False


def test_top_k_ids_orders_desc():
    df = pl.DataFrame({"player_id": ["a", "b", "c"], "v": [3.0, 1.0, 2.0]})
    assert top_k_ids(df, "v", k=2) == ["a", "c"]


def test_season_str():
    assert _season_str(2024) == "2023-24"
    assert _season_str("2022-23") == "2022-23"


def test_over_expected_math_and_sum_to_zero():
    # one bucket "g": actual=[10,4], denom=[8,8]; rate=14/16=0.875
    # expected=[7.0,7.0]; oe=[+3.0,-3.0] -> sums to 0 in the bucket
    df = pl.DataFrame(
        {"player_id": ["a", "b"], "position_bucket": ["g", "g"], "reb": [10.0, 4.0], "reb_chances": [8.0, 8.0]}
    )
    out = _over_expected(df, actual="reb", denom="reb_chances", group_cols=["position_bucket"], out_prefix="reb")
    rows = {r["player_id"]: r for r in out.iter_rows(named=True)}
    assert abs(rows["a"]["reb_baseline_rate"] - 0.875) < 1e-9
    assert abs(rows["a"]["reb_expected"] - 7.0) < 1e-9
    assert abs(rows["a"]["reb_oe"] - 3.0) < 1e-9
    assert residual_sums_to_zero(out, "reb_oe", ["position_bucket"]) is True


def test_over_expected_missing_cols_returns_null_oe_not_raise():
    df = pl.DataFrame({"player_id": ["a"], "position_bucket": ["g"]})
    out = _over_expected(df, actual="reb", denom="reb_chances", group_cols=["position_bucket"], out_prefix="reb")
    assert out["reb_oe"].null_count() == 1  # graceful, no raise


def test_pin_ids_casts_int_and_float_like_to_utf8():
    df = pl.DataFrame({"player_id": [201939, 2544], "team_id": [1610612744, 1610612747]})
    out = _pin_ids(df)
    assert out.schema["player_id"] == pl.Utf8 and out.schema["team_id"] == pl.Utf8
    assert out["player_id"].to_list() == ["201939", "2544"]


def test_attach_role_bucket_missing_positions_falls_back_to_all():
    df = pl.DataFrame({"player_id": ["1"], "reb": [1.0]})
    out = _attach_role_bucket(df, 2024, positions=None)
    assert out["position_bucket"].to_list() == ["all"]


def test_elite_oracle_allowlists_are_frozen_utf8_ids():
    categories = {"reb", "ast", "drive", "shot", "touch", "rim"}
    season = ELITE_ORACLE["2023-24"]
    assert set(season) == categories
    for cat in categories:
        ids = season[cat]
        assert len(ids) >= 8
        assert all(isinstance(i, str) for i in ids)


def test_attach_role_bucket_joins_and_fills_missing_with_all():
    df = pl.DataFrame({"player_id": ["1", "2"], "reb": [1.0, 2.0]})
    positions = pl.DataFrame({"player_id": ["1"], "position_bucket": ["guard"]})
    out = _attach_role_bucket(df, 2024, positions=positions)
    rows = {r["player_id"]: r["position_bucket"] for r in out.iter_rows(named=True)}
    assert rows["1"] == "guard"
    assert rows["2"] == "all"


def test_attach_role_bucket_non_overlapping_id_space_trips_match_floor():
    # dtype AGREES (both Utf8) but the id spaces are disjoint -> every row would
    # silently fill "all", collapsing the by-position baseline. At league scale
    # (>=50 rows) the match-rate floor must catch this.
    df = pl.DataFrame({"player_id": [str(i) for i in range(100)], "reb": [1.0] * 100})
    positions = pl.DataFrame({"player_id": [str(i) for i in range(1000, 1100)], "position_bucket": ["guard"] * 100})
    with pytest.raises(AssertionError, match="id-space mismatch"):
        _attach_role_bucket(df, 2024, positions=positions)
