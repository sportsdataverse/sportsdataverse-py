"""Holdout gates for the NBA pick curves (``nba_draft_model`` ①, ``nba_rookie_projection`` ④).

Corpus: ``tests/fixtures/nba_draft/pick_corpus.parquet``, every drafted player
of classes 2003-2019, with career / rookie / second-season box value and rookie
minutes (``tools/models/nba_draft/capture_pick_corpus.py``). The curves are fit
on 2003-2015 (``tools/models/nba_draft/fit_pick_curve.py``). 2016-2019 is the
holdout. Classes 2020+ are not in the corpus on purpose. 2020-2021 (career) and
2020-2023 (rookie) were spent as the rebuild's test windows. 2024-2025 is the
unscored rookie reserve.

Why pick curves: the combine-measurement models these replace lost to the pick
on their own holdout. Among the 177 combine attendees with a known pick, career
value scored Spearman 0.048 vs 0.455 for the pick, and rookie value 0.097 vs
0.400. Two pre-registered walk-forward rebuilds did not beat the pick by a
detectable margin. The second added college BPM / RAPM / seasons and
EuroLeague production, and scored +0.024 on 2020-21 with a 95% class-clustered
interval of [-0.030, +0.075].

A curve that is monotone in the pick ranks a class exactly as the pick does.
The gates assert that the shipped scoring path does that: it ties the
baseline and never ranks below it. They also assert that the bundled
constants are what the fitting script produces. On the old artifacts the
scoring path read combine features that a pick frame does not carry, so every
prediction collapsed to one value. The tie gate fails there.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_draft_constants import spearman_corr
from sportsdataverse.nba.nba_draft_model import _load_artifact
from sportsdataverse.nba.nba_draft_model import _score as score_draft
from sportsdataverse.nba.nba_rookie_projection import _load_rookie_artifact
from sportsdataverse.nba.nba_rookie_projection import _score as score_rookie

CORPUS = "tests/fixtures/nba_draft/pick_corpus.parquet"
TIE_TOL = 0.005


@pytest.fixture(scope="module")
def corpus() -> pl.DataFrame:
    return pl.read_parquet(CORPUS)


@pytest.fixture(scope="module")
def holdout(corpus: pl.DataFrame) -> pl.DataFrame:
    return corpus.filter(pl.col("draft_year").is_between(2016, 2019))


def test_corpus_holds_no_reserve_class(corpus: pl.DataFrame) -> None:
    assert corpus.schema["player_id"] == pl.Utf8
    assert corpus["draft_year"].min() == 2003 and corpus["draft_year"].max() == 2019
    assert corpus.group_by("draft_year").len()["len"].min() >= 57  # a full class (forfeited picks aside)
    # one row per drafted player and per slot: a duplicate would double-weight a pick in the fit
    assert corpus["player_id"].n_unique() == corpus.height
    assert corpus.select("draft_year", "overall_pick").is_duplicated().sum() == 0


def test_refit_reproduces_bundled_curves(corpus: pl.DataFrame) -> None:
    """Lineage: the bundled constants are exactly what fit_pick_curve.py fits on the corpus."""
    from tools.models.nba_draft.fit_pick_curve import fit_all

    for art, targets in (
        (_load_artifact(), ["career_value"]),
        (_load_rookie_artifact(), ["rookie_value", "soph_value", "rookie_min"]),
    ):
        assert art["kind"] == "pick_curve" and art["training_script"] == "tools/models/nba_draft/fit_pick_curve.py"
        assert art["train_classes"] == [2003, 2015] and art["holdout_classes"] == [2016, 2019]
        curves, _ = fit_all(corpus, targets)
        for t in targets:
            assert curves[t]["intercept"] == pytest.approx(art["curves"][t]["intercept"], abs=1e-9)
            assert curves[t]["coef"] == pytest.approx(art["curves"][t]["coef"], abs=1e-9)


@pytest.mark.parametrize("target", ["career_value", "rookie_value", "soph_value", "rookie_min"])
def test_curves_fall_with_the_pick(target: str) -> None:
    art = _load_artifact() if target == "career_value" else _load_rookie_artifact()
    from tools.models.nba_draft.fit_pick_curve import apply_curve

    grid = apply_curve(art["curves"][target], np.arange(1, 61))
    assert np.all(np.diff(grid) < 0), f"{target} curve ranks a later pick above an earlier one"


def _pick_baseline(frame: pl.DataFrame, target: str) -> float:
    return spearman_corr(-frame["overall_pick"].to_numpy(), frame[target].to_numpy())


def test_draft_value_ties_pick_on_holdout(holdout: pl.DataFrame) -> None:
    pred = score_draft(holdout.with_columns(pl.lit("x").alias("organization")), _load_artifact())
    joined = pred.join(holdout.select("player_id", "career_value"), on="player_id")
    assert joined.height == holdout.height == 240
    model = spearman_corr(joined["proj_career_value"].to_numpy(), joined["career_value"].to_numpy())
    base = _pick_baseline(holdout, "career_value")
    assert base == pytest.approx(0.5709, abs=1e-3), "pick baseline moved: re-check the corpus capture"
    assert abs(model - base) <= TIE_TOL, f"curve {model:.4f} vs pick {base:.4f}: a pick curve must tie the pick"
    # projected_pick is the pick order within a class
    rank = pred.select(pl.col("overall_pick").rank("ordinal").over("draft_year").cast(pl.Int64))["overall_pick"]
    assert pred["projected_pick"].equals(rank)


@pytest.mark.parametrize(
    "target,col,base_expected",
    [
        ("rookie_value", "proj_rookie_value", 0.4768),
        ("soph_value", "proj_soph_value", 0.5079),
        ("rookie_min", "proj_rookie_min", 0.6634),
    ],
)
def test_rookie_curves_tie_pick_on_holdout(holdout: pl.DataFrame, target: str, col: str, base_expected: float) -> None:
    pred = score_rookie(holdout.select("player_id", "draft_year", "overall_pick"), _load_rookie_artifact())
    model = spearman_corr(pred[col].to_numpy(), holdout[target].to_numpy())
    base = _pick_baseline(holdout, target)
    assert base == pytest.approx(base_expected, abs=1e-3)
    assert abs(model - base) <= TIE_TOL, f"{col} {model:.4f} vs pick {base:.4f}"


def test_bundled_holdout_metrics_match_recomputed(holdout: pl.DataFrame) -> None:
    """The artifact's recorded holdout numbers are the ones the gates compute (no hand-typed numbers)."""
    for art in (_load_artifact(), _load_rookie_artifact()):
        for t, m in art["holdout"].items():
            assert m["holdout_n"] == holdout.height
            assert m["holdout_spearman_pick"] == pytest.approx(_pick_baseline(holdout, t), abs=1e-4)
            assert m["holdout_spearman_curve"] == pytest.approx(m["holdout_spearman_pick"], abs=TIE_TOL)


def test_artifacts_are_json_with_no_combine_heads() -> None:
    for name in ("nba_draft_value.json", "nba_rookie_projection.json"):
        art = json.loads(Path("sportsdataverse/nba/models", name).read_text(encoding="utf-8"))
        assert "value_coef" not in art and "rookie_fraction" not in art and art["basis"] == ["log_pick", "pick_over_60"]


def test_shipped_scoring_equals_the_fitting_evaluator(holdout: pl.DataFrame) -> None:
    """The scoring path applies exactly the fitted curve.

    The rank gates cannot see a wrong scale, because any decreasing curve ties
    the pick. This test does see it.
    """
    from tools.models.nba_draft.fit_pick_curve import apply_curve

    pick = holdout["overall_pick"].to_numpy()
    d = score_draft(holdout.with_columns(pl.lit("x").alias("organization")), _load_artifact())
    assert np.allclose(
        d["proj_career_value"].to_numpy(), apply_curve(_load_artifact()["curves"]["career_value"], pick), atol=1e-9
    )
    r = score_rookie(holdout.select("player_id", "draft_year", "overall_pick"), _load_rookie_artifact())
    for t, col in (
        ("rookie_value", "proj_rookie_value"),
        ("soph_value", "proj_soph_value"),
        ("rookie_min", "proj_rookie_min"),
    ):
        assert np.allclose(r[col].to_numpy(), apply_curve(_load_rookie_artifact()["curves"][t], pick), atol=1e-9), col
