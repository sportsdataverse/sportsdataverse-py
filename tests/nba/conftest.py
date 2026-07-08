"""Shared fixtures for the NBA test suite."""

from pathlib import Path

import polars as pl
import pytest

_SHOT_VALUE_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nba_shot_value"


@pytest.fixture(scope="session")
def shot_value_corpus() -> "dict[str, pl.DataFrame]":
    """The committed 2022-23 shot-value oracle corpus (see the fixtures README).

    Returns a dict of the four frames: ``shots`` (per-shot Shot_Chart_Detail),
    ``league_avgs`` (LeagueAverages zone table), ``ptshots`` (stacked
    defender/shot-clock buckets), ``ptdefend`` (shot-defend rows).
    """
    return {
        "shots": pl.read_parquet(_SHOT_VALUE_FIX / "shotchart_2023.parquet"),
        "league_avgs": pl.read_parquet(_SHOT_VALUE_FIX / "league_averages_2023.parquet"),
        "ptshots": pl.read_parquet(_SHOT_VALUE_FIX / "playerdashptshots_sample.parquet"),
        "ptdefend": pl.read_parquet(_SHOT_VALUE_FIX / "playerdashptshotdefend_sample.parquet"),
    }
