"""Rank-sanity + Sigma=0 oracle gates on the committed 2023-24 fixtures.

Each phase appends its own rank-sanity gate here, reusing the Phase-0
Sigma=0 template: fixture-backed ``_get_fn`` -> the public model function ->
``residual_sums_to_zero`` + ``ELITE_ORACLE`` top-K membership. No network.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_tracking_value import _attach_role_bucket, _fetch_leaguedash_tracking, _over_expected
from sportsdataverse.nba.nba_tracking_value_constants import residual_sums_to_zero

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_stats" / "tracking"


def _load_fixture(measure_file: str) -> dict:
    return json.loads((FIXTURE_DIR / measure_file).read_text(encoding="utf-8"))


def _load_positions() -> pl.DataFrame:
    return pl.read_parquet(FIXTURE_DIR / "player_positions_2324.parquet")


def test_engine_sum_to_zero_on_rebounding_fixture():
    raw = _load_fixture("leaguedashptstats_rebounding_2324.json")
    df = _fetch_leaguedash_tracking(2024, "Rebounding", _get_fn=lambda **kw: raw)
    assert df.height > 0

    positions = _load_positions()
    bucketed = _attach_role_bucket(df, 2024, positions=positions)
    out = _over_expected(bucketed, actual="reb", denom="reb_chances", group_cols=["position_bucket"], out_prefix="reb")
    assert residual_sums_to_zero(out, "reb_oe", ["position_bucket"]) is True
