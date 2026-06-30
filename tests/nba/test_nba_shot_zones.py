"""Test shot-zone classification."""

from __future__ import annotations

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_shot_zones import SHOT_ZONES, add_shot_zones
from tests.conftest import skip_if_no_nba_stats_live

FX = pathlib.Path("tests/fixtures/nba_engine/0022200001")
FXROOT = pathlib.Path("tests/fixtures/nba_engine")


def _enh() -> pl.DataFrame:
    return enhanced_pbp_from_payload(json.loads((FX / "playbyplayv3.json").read_text()))


def test_shot_zone_classification() -> None:
    """Test that shot zones are classified correctly per pbpstats rules."""
    df = add_shot_zones(_enh())

    # zone is set exactly on FG attempts, null elsewhere
    fg = df.filter(pl.col("is_field_goal") == 1)
    assert fg.height > 0
    assert fg["shot_zone"].null_count() == 0
    assert (
        df.filter(pl.col("is_field_goal") != 1)["shot_zone"].null_count()
        == df.filter(pl.col("is_field_goal") != 1).height
    )
    assert set(fg["shot_zone"].unique().to_list()).issubset(set(SHOT_ZONES))

    # every 3 is a 3-zone
    threes = fg.filter(pl.col("shot_value") == 3)
    assert threes["shot_zone"].is_in(["corner_3", "above_the_break_3"]).all()


# ---------------------------------------------------------------------------
# Task 4: offline monkeypatch tests for the public nba_shot_zones() fetcher
# ---------------------------------------------------------------------------


def test_nba_shot_zones_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nba_shot_zones() offline: monkeypatch _fetch_pbp to committed fixture.

    Asserts:
    - Returns non-empty polars frame filtered to FG rows.
    - shot_zone column is present and non-null on every row.
    - return_as_pandas=True returns a pandas DataFrame.
    """
    import sportsdataverse.nba.nba_shot_zones as SZ

    g = "0022200001"
    monkeypatch.setattr(SZ, "_fetch_pbp", lambda gid, lg: json.loads((FXROOT / g / "playbyplayv3.json").read_text()))

    df = SZ.nba_shot_zones(g)

    # Must be a polars DataFrame
    assert isinstance(df, pl.DataFrame)

    # Non-empty — the fixture game has field-goal attempts
    assert df.height > 0, "nba_shot_zones() returned empty frame for fixture game"

    # shot_zone column present
    assert "shot_zone" in df.columns, "shot_zone column missing from result"

    # Every row is a field-goal attempt (the function filters to is_field_goal == 1)
    assert (df["is_field_goal"] == 1).all(), "Non-FG rows found in nba_shot_zones() output"

    # No null shot zones on FG rows
    assert df["shot_zone"].null_count() == 0, "Null shot_zone values on FG rows"

    # All zones are valid
    assert set(df["shot_zone"].unique().to_list()).issubset(set(SHOT_ZONES))

    # return_as_pandas=True
    df_pd = SZ.nba_shot_zones(g, return_as_pandas=True)
    assert isinstance(df_pd, pd.DataFrame), f"Expected pd.DataFrame, got {type(df_pd)}"
    assert len(df_pd) > 0


# ---------------------------------------------------------------------------
# Task 4: gated live smoke test for nba_shot_zones()
# ---------------------------------------------------------------------------


@skip_if_no_nba_stats_live
def test_nba_shot_zones_live() -> None:
    """Live smoke: nba_shot_zones() returns non-empty frame with valid zones.

    Gated behind SDV_PY_NBA_STATS_LIVE=1 — stats.nba.com hangs on datacenter
    IPs; run only from a residential IP.
    """
    from sportsdataverse.nba.nba_shot_zones import nba_shot_zones

    g = "0022200001"
    df = nba_shot_zones(g)

    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, "Live nba_shot_zones() returned empty frame"
    assert "shot_zone" in df.columns
    assert (df["is_field_goal"] == 1).all(), "Non-FG rows in live nba_shot_zones() output"
    assert df["shot_zone"].null_count() == 0
    assert set(df["shot_zone"].unique().to_list()).issubset(set(SHOT_ZONES))
