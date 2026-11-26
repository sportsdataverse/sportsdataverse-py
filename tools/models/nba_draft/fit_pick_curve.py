"""Fit the NBA pick curves behind ``nba_draft_model`` and ``nba_rookie_projection``.

Each curve is ordinary least squares of a realized value on the pick:
``value = intercept + b1*log(pick) + b2*(pick/60)``, or the log term alone when the
two-term fit is not decreasing over picks 1-60 (a curve must never rank a later pick higher). It is fit on draft
classes 2003-2015 of ``tests/fixtures/nba_draft/pick_corpus.parquet`` (see
``capture_pick_corpus.py``), with 2016-2019 held out.

The curves replace the combine-measurement models. On their own holdout
those ranked career value at Spearman 0.048 among drafted players, against
0.455 for the pick alone. Two walk-forward rebuilds with college production,
college RAPM, college seasons and EuroLeague production did not beat the pick
by a detectable margin either. A curve is monotone in the pick, so it ranks
exactly as the pick does: it ties the pick baseline by construction and adds
a value scale.

Writes ``sportsdataverse/nba/models/nba_draft_value.json`` (``career_value``)
and ``nba_rookie_projection.json`` (``rookie_value``, ``soph_value``,
``rookie_min``). Prints the holdout Spearman of each curve next to the pick's.

Run: ``uv run python tools/models/nba_draft/fit_pick_curve.py`` (offline).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_draft_constants import spearman_corr

CORPUS = Path("tests/fixtures/nba_draft/pick_corpus.parquet")
MODELS = Path("sportsdataverse/nba/models")
TRAIN = (2003, 2015)
HOLDOUT = (2016, 2019)
BASIS = ["log_pick", "pick_over_60"]


def basis(pick: np.ndarray, k: int = 2) -> np.ndarray:
    p = np.asarray(pick, dtype=float)
    return np.column_stack([np.log(p), p / 60.0][:k])


def fit_curve(pick: np.ndarray, y: np.ndarray) -> dict:
    """Two-term fit; when it is not decreasing over picks 1-60, the log-only fit (monotone iff b1 < 0)."""
    for k in (2, 1):
        A = np.column_stack([np.ones(len(pick)), basis(pick, k)])
        beta, *_ = np.linalg.lstsq(A, np.asarray(y, dtype=float), rcond=None)
        curve = {"intercept": float(beta[0]), "coef": [float(b) for b in beta[1:]]}
        if np.all(np.diff(apply_curve(curve, np.arange(1, 61))) < 0):
            return curve
    raise AssertionError("no decreasing pick curve: the label does not fall with the pick")


def apply_curve(curve: dict, pick: np.ndarray) -> np.ndarray:
    coef = np.asarray(curve["coef"], dtype=float)
    return curve["intercept"] + basis(pick, len(coef)) @ coef


def fit_all(corpus: pl.DataFrame, targets: list[str]) -> tuple[dict, dict]:
    train = corpus.filter(pl.col("draft_year").is_between(*TRAIN))
    hold = corpus.filter(pl.col("draft_year").is_between(*HOLDOUT))
    curves, metrics = {}, {}
    for t in targets:
        c = fit_curve(train["overall_pick"].to_numpy(), train[t].to_numpy())
        curves[t] = c
        pred = apply_curve(c, hold["overall_pick"].to_numpy())
        metrics[t] = {
            "holdout_spearman_curve": round(spearman_corr(pred, hold[t].to_numpy()), 4),
            "holdout_spearman_pick": round(spearman_corr(-hold["overall_pick"].to_numpy(), hold[t].to_numpy()), 4),
            "holdout_n": hold.height,
        }
    return curves, metrics


def artifact(name: str, curves: dict, metrics: dict) -> dict:
    return {
        "league": "nba",
        "kind": "pick_curve",
        "model": name,
        "basis": BASIS,
        "curves": curves,
        "train_classes": list(TRAIN),
        "holdout_classes": list(HOLDOUT),
        "baseline": "pick rank (Spearman of -overall_pick)",
        "holdout": metrics,
        "training_script": "tools/models/nba_draft/fit_pick_curve.py",
        "corpus": str(CORPUS).replace("\\", "/"),
    }


def main() -> None:
    corpus = pl.read_parquet(CORPUS)
    for name, file, targets in (
        ("draft_value", "nba_draft_value.json", ["career_value"]),
        ("rookie_projection", "nba_rookie_projection.json", ["rookie_value", "soph_value", "rookie_min"]),
    ):
        curves, metrics = fit_all(corpus, targets)
        (MODELS / file).write_text(
            json.dumps(artifact(name, curves, metrics), indent=2) + "\n", encoding="utf-8", newline="\n"
        )
        for t, m in metrics.items():
            print(
                f"{name}.{t}: holdout Spearman curve {m['holdout_spearman_curve']} vs pick {m['holdout_spearman_pick']} (n={m['holdout_n']})"
            )
        print(f"wrote {MODELS / file}")


if __name__ == "__main__":
    main()
