"""Backtest driver fixture over the committed 2023 NHL oracle corpus.

Per-model asserts are added in later phases (Task 2.3, 3.4, 4.2); this
module owns the shared ``oracle_corpus`` fixture + basic shape/dtype
sanity so every later phase can rely on a validated load.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nhl_prediction"


@pytest.fixture(scope="module")
def oracle_corpus() -> dict[str, pl.DataFrame]:
    return {
        "results": pl.read_parquet(FIXTURES_DIR / "results_2023.parquet"),
        "moneypuck_teams": pl.read_parquet(FIXTURES_DIR / "moneypuck_teams_2023.parquet"),
        "espn_power": pl.read_parquet(FIXTURES_DIR / "espn_power_2023.parquet"),
        "espn_predictor": pl.read_parquet(FIXTURES_DIR / "espn_predictor_sample.parquet"),
        "espn_odds": pl.read_parquet(FIXTURES_DIR / "espn_odds_sample.parquet"),
        "espn_propbets": pl.read_parquet(FIXTURES_DIR / "espn_propbets_sample.parquet"),
        "pbp_sample": pl.read_parquet(FIXTURES_DIR / "pbp_sample_2023.parquet"),
    }


def test_results_nonempty_and_typed(oracle_corpus):
    results = oracle_corpus["results"]
    assert results.height > 0
    assert results.schema["game_id"] == pl.Utf8
    assert results.schema["home_team"] == pl.Utf8
    assert results.schema["away_team"] == pl.Utf8


def test_moneypuck_teams_is_32_rows(oracle_corpus):
    mp = oracle_corpus["moneypuck_teams"]
    assert mp.height == 32
    assert mp.schema["team"] == pl.Utf8


def test_pbp_sample_nonempty(oracle_corpus):
    assert oracle_corpus["pbp_sample"].height > 0


def test_documented_empty_oracles_have_expected_schema(oracle_corpus):
    # ESPN power-index + predictor are genuinely unavailable for NHL at the
    # API level (see tests/fixtures/nhl_prediction/README.md); the fixtures
    # are committed empty with the documented schema, not fabricated.
    assert oracle_corpus["espn_power"].columns == ["team", "power_index", "rank"]
    assert oracle_corpus["espn_predictor"].columns == [
        "game_id",
        "home_team",
        "away_team",
        "home_win_prob",
    ]
