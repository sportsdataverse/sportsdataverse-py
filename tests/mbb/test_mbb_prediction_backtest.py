"""Backtest harness for the MBB prediction stack.

Task 0.4: the as-of-date leakage split + the shared ``oracle_corpus`` fixture.
Per-model backtest assertions are added by later phases (2, 3, 4, 6).
"""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_prediction_constants import as_of_ratings_split

FIX_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_prediction"

_CORE_FIXTURES = ("results_2024", "team_box_2024", "torvik_2024")
_OPTIONAL_FIXTURES = ("espn_predictor_sample", "espn_odds_sample", "espn_bpi_2024")


@pytest.fixture
def oracle_corpus() -> dict[str, pl.DataFrame]:
    """Load the committed Task-0.1 oracle fixtures into a dict keyed by name.

    The three core fixtures (results / team_box / torvik) are always present;
    the ESPN per-game samples are loaded when present (captured in Phase 2/4).
    """
    corpus: dict[str, pl.DataFrame] = {}
    for name in (*_CORE_FIXTURES, *_OPTIONAL_FIXTURES):
        path = FIX_DIR / f"{name}.parquet"
        if path.exists():
            corpus[name] = pl.read_parquet(path)
    return corpus


def test_as_of_ratings_split_excludes_same_and_future():
    df = pl.DataFrame(
        {
            "game_id": ["a", "b", "c"],
            "date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 10),
                datetime.date(2024, 1, 20),
            ],
        }
    )
    out = as_of_ratings_split(df, datetime.date(2024, 1, 10))
    # strictly before cutoff: the 2024-01-10 game (same day) and later are excluded
    assert out["game_id"].to_list() == ["a"]


def test_oracle_corpus_core_fixtures_nonempty(oracle_corpus):
    for name in _CORE_FIXTURES:
        assert name in oracle_corpus, f"missing core fixture {name}"
        assert oracle_corpus[name].height > 0, f"empty core fixture {name}"


def test_oracle_corpus_id_columns_are_utf8(oracle_corpus):
    results = oracle_corpus["results_2024"]
    for col in ("game_id", "home_team_id", "away_team_id"):
        assert results.schema[col] == pl.Utf8
    assert oracle_corpus["torvik_2024"].schema["team_id"] == pl.Utf8
