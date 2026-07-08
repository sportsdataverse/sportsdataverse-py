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


_RETURNING = _FIX / "returning_2017_2023.parquet"


@pytest.mark.skipif(not _RETURNING.exists(), reason="returning-production fixture not captured")
def test_returning_production_retention_gate(oracle_corpus: dict[str, pl.DataFrame]) -> None:
    """Phase-2 gate: returning production predicts YoY scoring-margin change.

    Observed on the 2026-07-08 capture (FBS 2018-2023, >=6 games both seasons,
    n=794): spearman(overall_returning, margin_delta) = 0.229 with the fitted
    unit weights (offense-only; see fit_returning_weights.py). Floor set one
    notch below at 0.20 -- never lower it to pass.
    """
    from sportsdataverse.cfb.cfb_projection_constants import get_constants, spearman_corr

    rp = pl.read_parquet(_RETURNING)
    res = oracle_corpus["results"]
    home = res.select(
        pl.col("season"),
        pl.col("home_team_id").alias("team_id"),
        (pl.col("home_score") - pl.col("away_score")).alias("m"),
    )
    away = res.select(
        pl.col("season"),
        pl.col("away_team_id").alias("team_id"),
        (pl.col("away_score") - pl.col("home_score")).alias("m"),
    )
    margins = (
        pl.concat([home, away])
        .group_by("season", "team_id")
        .agg(pl.col("m").mean().alias("avg_margin"), pl.len().alias("g"))
    )
    delta = (
        margins.join(
            margins.with_columns((pl.col("season") + 1).alias("season")).rename(
                {"avg_margin": "prior_margin", "g": "prior_g"}
            ),
            on=["season", "team_id"],
            how="inner",
        )
        .filter((pl.col("g") >= 6) & (pl.col("prior_g") >= 6))
        .with_columns((pl.col("avg_margin") - pl.col("prior_margin")).alias("margin_delta"))
    )
    # recombine overall from the unit columns with the CURRENT fitted weights so the
    # committed fixture stays valid across weight refits
    w = get_constants("fbs").returning_prod_weights
    fbs = rp.filter(pl.col("classification") == "fbs").drop_nulls(["team_id", "off_returning", "def_returning"])
    denom = w["offense"] + w["defense"]
    fbs = fbs.with_columns(
        ((pl.col("off_returning") * w["offense"] + pl.col("def_returning") * w["defense"]) / denom).alias("overall_w")
    )
    assert fbs.schema["team_id"] == delta.schema["team_id"] == pl.Utf8
    j = fbs.join(delta, on=["season", "team_id"], how="inner")
    assert j.height >= 700, f"expected ~794 FBS team-season rows, got {j.height}"
    rho = spearman_corr(j["overall_w"].to_numpy(), j["margin_delta"].to_numpy())
    assert rho >= 0.20, f"spearman(overall_returning, margin_delta) = {rho:.4f} < 0.20"
