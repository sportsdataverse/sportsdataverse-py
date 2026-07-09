from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "nhl_microstat"


@pytest.fixture(scope="session")
def oracle_pbp() -> pl.DataFrame:
    """The committed 2023-24 pbp slice (Task 0.1) used by every oracle gate."""
    return pl.read_parquet(FIXTURES_DIR / "pbp_2024_slice.parquet")


@pytest.fixture(scope="session")
def oracle_edge_skaters() -> pl.DataFrame:
    """The committed EDGE skater detail sample (Task 0.1)."""
    return pl.read_parquet(FIXTURES_DIR / "edge_skater_detail_sample.parquet")
