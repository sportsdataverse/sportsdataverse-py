"""Phase-1 oracle gate for the shot-quality spine (offline, fixture-driven).

Fits the empirical-Bayes model on the TEMPORAL-train fixture and scores the
holdout (later games), so the calibration measurement respects the leakage
boundary. Hard external anchors are the OBSERVED Barttorvik national 2P/3P
make rates (``BART_NATIONAL_SPLITS``, captured 2026-07-08 from 5,000+ player
rows); the per-zone literature estimates (``PUBLISHED_ZONE_BASELINES``) are
plan-authored ESTIMATES, asserted only as a wide sanity band. Observed at
gate authorship: calibration ratio ~1.00, holdout 2P .511 vs bart .5082,
3P .344 vs .3376. NEVER loosen these to pass -- debug the model.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mbb.mbb_shot_quality import mbb_shot_quality, mbb_shot_quality_model
from sportsdataverse.mbb.mbb_shot_quality_constants import (
    BART_NATIONAL_SPLITS,
    PUBLISHED_ZONE_BASELINES,
)

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_shot_quality"

CAL_LO, CAL_HI = 0.98, 1.02
ANCHOR_TOL = 0.02
ZONE_ESTIMATE_TOL = 0.07


def _train() -> pl.DataFrame:
    return pl.read_parquet(_FIX / "espn_shots_2025_train.parquet")


def _holdout() -> pl.DataFrame:
    return pl.read_parquet(_FIX / "espn_shots_2025_holdout.parquet")


def test_holdout_calibration_gate():
    model = mbb_shot_quality_model(_train(), league="mens")
    scored = mbb_shot_quality(_holdout(), model=model, league="mens")
    scored = scored.filter(pl.col("xpoints").is_not_null())
    assert scored.height > 80_000, f"holdout join collapsed: n={scored.height}"
    actual = scored.select((pl.col("made").cast(pl.Float64) * pl.col("point_value").cast(pl.Float64)).sum()).item()
    expected = scored.get_column("xpoints").sum()
    ratio = expected / actual
    assert CAL_LO <= ratio <= CAL_HI, f"holdout xpoints calibration {ratio:.4f} outside [{CAL_LO}, {CAL_HI}]"


def test_external_bart_anchors():
    """Blended 2P/3P make rates of the holdout match the OBSERVED national
    Barttorvik aggregates (the value classifier + make rates jointly)."""
    h = _holdout()
    for pv, key in ((2, "fg2_pct"), (3, "fg3_pct")):
        sub = h.filter(pl.col("point_value") == pv)
        rate = sub.get_column("made").cast(pl.Float64).mean()
        anchor = BART_NATIONAL_SPLITS["mens"][key]
        assert abs(rate - anchor) <= ANCHOR_TOL, f"{key}: fixture {rate:.4f} vs bart {anchor:.4f}"


def test_zone_structure_and_estimate_band():
    model = mbb_shot_quality_model(_train(), league="mens")
    zone_rates = dict(_train().group_by("shot_zone").agg(pl.col("made").cast(pl.Float64).mean().alias("r")).iter_rows())
    # basketball-invariant structure
    assert max(zone_rates, key=zone_rates.get) == "rim"
    assert all(0.25 < r < 0.75 for r in zone_rates.values()), zone_rates
    assert zone_rates["rim"] - zone_rates["abovebreak3"] >= 0.15
    # literature-estimate sanity band (estimates, not captures -- wide on purpose)
    for zone, est in PUBLISHED_ZONE_BASELINES["mens"].items():
        assert abs(zone_rates[zone] - est) <= ZONE_ESTIMATE_TOL, (zone, zone_rates[zone], est)
    # every model cell is a sane probability
    assert model.filter((pl.col("make_rate_shrunk") < 0.0) | (pl.col("make_rate_shrunk") > 1.0)).height == 0


def test_ncaa_sample_scores_through_model():
    """The NCAA-source frame (shot_type='unknown') still gets zone-level
    scores when the model is built league-wide from ESPN + its own cells."""
    ncaa = pl.read_parquet(_FIX / "ncaa_shots_sample.parquet")
    scored = mbb_shot_quality(ncaa, league="mens")
    assert scored.get_column("xpoints").null_count() == 0


def test_shot_selection_zero_sum_on_real_fixture():
    """Phase-2 gate: attempt-weighted selection_value sums to ~0 across the
    league, and the most rim-heavy high-volume team rates positive."""
    from sportsdataverse.mbb.mbb_shot_selection import mbb_shot_selection

    model = mbb_shot_quality_model(_train(), league="mens")
    scored = mbb_shot_quality(_holdout(), model=model, league="mens").filter(pl.col("xpoints").is_not_null())
    sel = mbb_shot_selection(scored, group="team_id")
    total = float((sel.get_column("selection_value") * sel.get_column("n_shots")).sum())
    assert abs(total) < 1e-6 * scored.height, f"selection value not zero-sum: {total}"
    # the highest rim-share team with real volume should have positive value
    rim_share = (
        scored.group_by("team_id")
        .agg(
            (pl.col("shot_zone") == "rim").cast(pl.Float64).mean().alias("rim_share"),
            pl.len().alias("n"),
        )
        .filter(pl.col("n") >= 200)
        .sort("rim_share", descending=True)
    )
    top_rim_team = rim_share.row(0, named=True)["team_id"]
    top_val = sel.filter(pl.col("team_id") == top_rim_team).row(0, named=True)["selection_value"]
    assert top_val > 0, f"most rim-heavy team {top_rim_team} has selection_value {top_val:.4f}"
