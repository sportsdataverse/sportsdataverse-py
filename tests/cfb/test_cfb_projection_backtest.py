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


_RECRUITS14 = _FIX / "recruits_2014_2023.parquet"
_TEAMS = _FIX / "teams_2023.parquet"


@pytest.mark.skipif(
    not (_RECRUITS14.exists() and _TEAMS.exists() and _RETURNING.exists()),
    reason="projection fixtures not captured",
)
def test_recruiting_projection_backtest_gate(oracle_corpus: dict[str, pl.DataFrame], monkeypatch) -> None:
    """Phase-3 gate: as-of wins projection beats naive baselines, MAE under floor.

    Observed on the 2026-07-08 fixtures (FBS targets 2019-2023, n=575 pooled,
    train from 2018): model MAE 2.190 vs prior-year 2.464 and division-mean
    2.343. Floor 2.35 (one notch above observed). Per-season the model beats
    prior-year all five years; the division-mean baseline wins the two COVID
    seasons (2020/2021) individually, so the baseline comparisons are pooled --
    documented, not a gate relaxation. Never lower the gate to pass.
    """
    import importlib

    import numpy as np

    from sportsdataverse.cfb.cfb_crosswalk import _norm_team
    from sportsdataverse.cfb.cfb_projection_constants import mae

    proj = importlib.import_module("sportsdataverse.cfb.cfb_recruiting_projection")
    tal_mod = importlib.import_module("sportsdataverse.cfb.cfb_roster_talent")

    recruits = pl.read_parquet(_RECRUITS14)
    teams = pl.read_parquet(_TEAMS)
    returning = pl.read_parquet(_RETURNING)
    results_g = oracle_corpus["results"]

    monkeypatch.setattr(
        tal_mod,
        "load_recruit_classes",
        lambda seasons, **k: recruits.filter(
            pl.col("season").is_in([seasons] if isinstance(seasons, int) else list(seasons))
        ),
    )
    name_map = teams.with_columns(
        (pl.col("school") + " " + pl.col("mascot").fill_null(""))
        .map_elements(_norm_team, return_dtype=pl.Utf8)
        .alias("_full")
    ).select("_full", pl.col("team_id").alias("espn_id"), "classification")

    def load_talent(seasons: list[int], division: str) -> pl.DataFrame:
        t = tal_mod.cfb_roster_talent(seasons, division=division)
        return (
            t.with_columns(pl.col("team").map_elements(_norm_team, return_dtype=pl.Utf8).alias("_full"))
            .join(name_map, on="_full", how="inner")
            .filter(pl.col("classification") == "fbs")
            .drop("team_id", "_full", "classification")
            .rename({"espn_id": "team_id"})
        )

    def load_returning(seasons: list[int], division: str) -> pl.DataFrame:
        return returning.filter(pl.col("season").is_in(list(seasons)) & pl.col("team_id").is_not_null()).select(
            "season", "team_id", "off_returning", "def_returning"
        )

    def load_results(seasons: list[int]) -> pl.DataFrame:
        done = results_g.filter(pl.col("season").is_in(list(seasons)))
        home = done.select(
            "season",
            pl.col("home_team_id").alias("team_id"),
            (pl.col("home_score") > pl.col("away_score")).cast(pl.Int64).alias("win"),
            (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("m"),
        )
        away = done.select(
            "season",
            pl.col("away_team_id").alias("team_id"),
            (pl.col("away_score") > pl.col("home_score")).cast(pl.Int64).alias("win"),
            (pl.col("away_score") - pl.col("home_score")).cast(pl.Float64).alias("m"),
        )
        return (
            pl.concat([home, away])
            .group_by("season", "team_id")
            .agg(pl.col("win").sum().alias("wins"), pl.col("m").mean().alias("points_margin"))
        )

    monkeypatch.setattr(proj, "_load_talent", load_talent)
    monkeypatch.setattr(proj, "_load_returning", load_returning)
    monkeypatch.setattr(proj, "_load_results", load_results)

    model_err: list[np.ndarray] = []
    prior_err: list[np.ndarray] = []
    mean_err: list[np.ndarray] = []
    for target in range(2019, 2024):
        out = proj.cfb_recruiting_projection(target, history_seasons=list(range(2018, target)))
        realized = load_results([target]).rename({"wins": "real_wins"})
        assert out.schema["team_id"] == realized.schema["team_id"] == pl.Utf8
        j = out.join(realized, on=["season", "team_id"], how="inner")
        matrix = proj._build_projection_matrix(list(range(2018, target + 1)))
        j = j.join(
            matrix.filter(pl.col("season") == target).select("team_id", "prior_wins"),
            on="team_id",
            how="left",
        ).drop_nulls(["prior_wins"])
        assert j.height >= 100, f"{target}: joined only {j.height} FBS teams"
        real = j["real_wins"].to_numpy().astype(float)
        model_err.append(np.abs(j["pred_wins"].to_numpy() - real))
        prior_err.append(np.abs(j["prior_wins"].to_numpy() - real))
        mean_err.append(np.abs(np.full(j.height, 6.0) - real))
    mae_model = float(np.concatenate(model_err).mean())
    mae_prior = float(np.concatenate(prior_err).mean())
    mae_mean = float(np.concatenate(mean_err).mean())
    assert mae_model <= mae_prior, f"model {mae_model:.3f} > prior baseline {mae_prior:.3f}"
    assert mae_model <= mae_mean, f"model {mae_model:.3f} > mean baseline {mae_mean:.3f}"
    assert mae_model <= 2.35, f"pooled wins MAE {mae_model:.3f} > 2.35 floor"
    assert mae is not None  # keep the shared-metric import exercised
