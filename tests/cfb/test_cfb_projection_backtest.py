"""Backtest harness + shared oracle-corpus fixture for the CFB projection spine (T2.2).

Task 0.3 provides the ``oracle_corpus`` fixture and asserts the committed corpus is
present, non-empty, and id-typed. Per-model predictive-accuracy asserts are added by
the later phases (roster talent → returning production → recruiting projection →
transfer impact → draft projection), all reading this fixture. The draft parquet is
captured in Phase 5, so it is loaded lazily / optionally here.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_projection"


@pytest.fixture(scope="session")
def oracle_corpus() -> dict[str, pl.DataFrame]:
    """Load the committed projection oracle parquets into a dict of frames.

    Keys: ``results`` (game scores 2016-2023) and ``talent`` (247 composite, 2023).
    ``draft`` is added when Phase 5 lands its fixture.
    """
    corpus = {
        "results": pl.read_parquet(_FIX / "results_2016_2023.parquet"),
        "talent": pl.read_parquet(_FIX / "talent_247_2023.parquet"),
    }
    draft = _FIX / "draft_2017_2024.parquet"
    if draft.exists():
        corpus["draft"] = pl.read_parquet(draft)
    return corpus


def test_corpus_non_empty(oracle_corpus: dict[str, pl.DataFrame]) -> None:
    """Every committed oracle frame has rows."""
    assert oracle_corpus["results"].height > 10_000
    assert oracle_corpus["talent"].height > 100


def test_corpus_ids_are_utf8(oracle_corpus: dict[str, pl.DataFrame]) -> None:
    """Join keys are all Utf8 (the pinned id dtype)."""
    results = oracle_corpus["results"]
    assert results.schema["home_team_id"] == pl.Utf8
    assert results.schema["away_team_id"] == pl.Utf8
    assert oracle_corpus["talent"].schema["team_id"] == pl.Utf8


def test_results_span_validation_seasons(oracle_corpus: dict[str, pl.DataFrame]) -> None:
    """Results cover 2016-2023 with completed scores."""
    seasons = set(oracle_corpus["results"]["season"].unique().to_list())
    assert {2016, 2019, 2023} <= seasons
    assert oracle_corpus["results"]["home_score"].null_count() == 0


def test_talent_ranks_are_dense_from_one(oracle_corpus: dict[str, pl.DataFrame]) -> None:
    """The 247 talent snapshot is a clean ranked list (top team = rank 1, no unranked 0s)."""
    talent = oracle_corpus["talent"]
    assert talent["talent_rank"].min() == 1
    assert (talent["talent_rank"] > 0).all()
    top = talent.sort("talent_rank").row(0, named=True)
    assert top["talent_247"] == talent["talent_247"].max()  # rank 1 has the highest rating
