"""Swing/take run-value formulation check (T6.2, Phase 2). Not shipped --
provenance for the ``swing_take_runs`` definition in ``mlb_swing_decision.py``.

Run (from repo root, no network -- reads the committed fixture):

    uv run python dev/mlb_hitting/fit_swing_take.py

Finding: an initial ``swing_take_runs = sum(rv_chosen)`` (the league-average
run value of the batter's decision at that zone x count, looked up from the
surface) only reached Spearman 0.227 vs Savant's ``runs_all`` on the committed
sample, and 0.298 on a live FULL-season pull -- i.e. it did NOT clear the
>= 0.90 design-doc gate even like-for-like. Root cause: the surface lookup
replaces every pitch with the league-average RV for its cell, averaging away
exactly the batter-specific outcome signal Savant credits. Savant's swing/take
run value is the ACTUAL per-pitch run value (``delta_run_exp``) credited to the
batter's swing/take decision, summed. Switching ``swing_take_runs`` to
``sum(delta_run_exp over decision pitches)`` roughly doubles the partial-sample
correlation (0.498) and is the formulation Savant itself uses. ``selective_agg``
keeps the surface-baseline formulation on purpose -- it is a distinct,
outcome-independent decision-quality metric (SEAGER analog), not a Savant
concurrent-validity target.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mlb.mlb_hitting_constants import SWING_DESCRIPTIONS, TAKE_DESCRIPTIONS, spearman_corr

FIX = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "mlb_hitting"


def main() -> None:
    s = pl.read_parquet(FIX / "statcast_sample_2024.parquet")
    lb = pl.read_parquet(FIX / "swing_take_2024.parquet")

    dec = (
        pl.when(pl.col("description").is_in(list(SWING_DESCRIPTIONS)))
        .then(pl.lit("swing"))
        .when(pl.col("description").is_in(list(TAKE_DESCRIPTIONS)))
        .then(pl.lit("take"))
        .otherwise(None)
    )
    s = s.with_columns(dec.alias("decision")).filter(pl.col("delta_run_exp").is_not_null())

    # Chosen formulation: sum of actual per-pitch delta_run_exp over decisions.
    chosen = (
        s.filter(pl.col("decision").is_not_null())
        .group_by("batter")
        .agg(pl.col("delta_run_exp").sum().alias("swing_take_runs"))
    )

    # Rejected formulation: surface-average lookup (mean delta per zone x count x decision).
    sd = s.filter(pl.col("decision").is_not_null() & pl.col("zone").is_not_null()).with_columns(
        (pl.col("balls").cast(pl.Utf8) + "-" + pl.col("strikes").cast(pl.Utf8)).alias("count")
    )
    surf = sd.group_by("zone", "count", "decision").agg(pl.col("delta_run_exp").mean().alias("rv"))
    rejected = (
        sd.join(surf, on=["zone", "count", "decision"], how="left")
        .group_by("batter")
        .agg(pl.col("rv").sum().alias("swing_take_runs"))
    )

    for name, frame in [("CHOSEN sum(delta_run_exp)", chosen), ("REJECTED sum(surface rv_chosen)", rejected)]:
        j = frame.join(lb, left_on="batter", right_on="player_id", how="inner")
        sp = spearman_corr(j["swing_take_runs"].to_numpy(), j["runs_all"].to_numpy())
        print(f"{name:34s} spearman vs runs_all = {sp:.4f}  (n={j.height})")


if __name__ == "__main__":
    main()
