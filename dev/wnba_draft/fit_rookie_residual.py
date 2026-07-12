"""Fit the WNBA rookie/sophomore residual artifact on real WNBA data.

Genuine re-fit -- the shipped T3.4 artifact was an all-zero placeholder
(``rookie_fraction=0.1``, ``residual={...: 0.0}``). Reads the committed Task-5.1 fixtures
(offline), scores the training-era draft classes through the *freshly-fit* WNBA draft artifact
(``fit_draft_model.py``) + WNBA aging curve (``fit_aging_curve.py``) using
:func:`sportsdataverse.wnba.wnba_draft_model._score` -- the same module-level scoring function
``wnba_draft_model`` calls at runtime, imported directly (not duplicated) so this offline fit
can never drift from the runtime's math -- composes the same way ``wnba_rookie_projection``
does at runtime, and fits ``residual[pro_tier] = mean(realized_rookie_value - composed_value)``
on the training classes only (holdout classes reserved for the oracle gate). Writes
``sportsdataverse/nba/models/wnba_rookie_projection.json``.

Run: ``uv run python dev/wnba_draft/fit_rookie_residual.py`` (offline).
"""

from __future__ import annotations

import json

import polars as pl

from sportsdataverse.nba.nba_aging_curve import nba_aging_curve
from sportsdataverse.nba.nba_draft_constants import as_of_class_split
from sportsdataverse.wnba.wnba_draft_model import _score

FIXTURE_DIR = "tests/fixtures/wnba_draft"
ARTIFACT_PATH = "sportsdataverse/nba/models/wnba_rookie_projection.json"
DRAFT_ARTIFACT_PATH = "sportsdataverse/nba/models/wnba_draft_value.json"
CUTOFF_YEAR = 2018  # matches fit_draft_model.py's CUTOFF_YEAR
ROOKIE_AGE = 22.0  # WNBA draftees are typically 4-year college seniors


def main() -> None:
    draft_history = pl.read_parquet(f"{FIXTURE_DIR}/draft_history.parquet").select(
        "player_id", "draft_year", "overall_pick", "round_number"
    )
    rookie = pl.read_parquet(f"{FIXTURE_DIR}/rookie_values.parquet")
    career = pl.read_parquet(f"{FIXTURE_DIR}/career_values.parquet")

    with open(DRAFT_ARTIFACT_PATH, encoding="utf-8") as f:
        art = json.load(f)

    scored = _score(
        draft_history.with_columns(
            pl.col("overall_pick").cast(pl.Float64, strict=False), pl.col("round_number").cast(pl.Float64, strict=False)
        ),
        art,
    )
    train_scored, _ = as_of_class_split(scored, cutoff_year=CUTOFF_YEAR)

    # nba_aging_curve is a POPULATION-level fit (fit over all observed ages in
    # dev/wnba_draft/fit_aging_curve.py, standard for a delta-method curve) -- not a per-draft-
    # class predictor, so using it here for every holdout class is not an as-of leak.
    curve = nba_aging_curve(league="wnba").select("age", "rel_value")
    rel_rookie = float(curve.filter(pl.col("age") == int(ROOKIE_AGE))["rel_value"][0])
    rel_peak = 1.0
    # Strict as-of hygiene: derive rookie_fraction on TRAIN classes (<=CUTOFF_YEAR) only, never
    # the full corpus. It's a rank-neutral global scalar (doesn't reorder the gate), but a
    # holdout-inclusive median is still an as-of-boundary smell -- restrict it to train_scored's
    # player_ids to keep every fitted quantity train-derived.
    rookie_fraction_candidates = (
        career.join(rookie.select("player_id", "rookie_value"), on="player_id", how="inner")
        .join(train_scored.select("player_id"), on="player_id", how="inner")
        .filter(pl.col("career_value") > 0)
    )
    rookie_fraction = float(
        (rookie_fraction_candidates["rookie_value"] / rookie_fraction_candidates["career_value"]).median()
    )
    rookie_fraction = max(0.02, min(rookie_fraction, 0.5))
    print(f"rookie_fraction (median rookie_value/career_value): {rookie_fraction:.4f}")

    composed = train_scored.with_columns(
        (pl.col("proj_career_value") * rookie_fraction * (rel_rookie / rel_peak)).alias("composed_value")
    )
    composed = composed.join(rookie.select("player_id", "rookie_value"), on="player_id", how="left").with_columns(
        pl.col("rookie_value").fill_null(0.0)
    )
    residual_by_tier = (
        composed.group_by("pro_tier")
        .agg((pl.col("rookie_value") - pl.col("composed_value")).mean().alias("residual"), pl.len().alias("n"))
        .sort("pro_tier")
    )
    print(residual_by_tier)

    artifact = {
        "league": "wnba",
        "rookie_fraction": rookie_fraction,
        "residual": dict(zip(residual_by_tier["pro_tier"].to_list(), residual_by_tier["residual"].to_list())),
    }
    with open(ARTIFACT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
    print(f"wrote {ARTIFACT_PATH}")


if __name__ == "__main__":
    main()
