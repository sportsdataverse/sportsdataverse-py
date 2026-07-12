"""Phase 5 oracle gate: WNBA draft/aging/availability/rookie holdout backtest.

Mirrors ``tests/nba/test_nba_draft_backtest.py`` (+ the availability/aging-curve gates split
out into their own NBA test files) but against the real WNBA corpus captured live from
``stats.wnba.com`` 2026-07-11 (``dev/wnba_draft/capture_corpus.py``) -- see
``.superpowers/sdd/wnba-draft-refit/progress.md`` for the full capture/fit debugging record.
Floors below are calibrated from the OBSERVED holdout numbers on this corpus (never an
aspirational target, never lowered to pass) -- see each test's docstring for the number and the
run that produced it.

**Structural difference from the NBA gate:** there is no ``draft_prob`` AUC gate here.
``wnba_stats_drafthistory`` has no undrafted/invitee negative class (``draftcombinestats``
returns 0 rows for every WNBA season), so ``draft_prob`` is a documented constant, not a fitted
classifier (see ``dev/wnba_draft/fit_draft_model.py``'s module docstring) -- there is nothing to
gate. `test_wnba_draft_prob_is_a_documented_constant` locks in that the artifact stays an
honest constant instead of silently regressing to a fake-looking fit.
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_aging_curve import nba_aging_curve
from sportsdataverse.nba.nba_draft_constants import as_of_class_split, mae, spearman_corr
from sportsdataverse.nba.nba_availability import _FEATURE_COLS, availability_features, score_availability
from sportsdataverse.wnba.wnba_draft_model import _load_artifact, _score

FIXTURE_DIR = "tests/fixtures/wnba_draft"
DRAFT_CUTOFF_YEAR = 2018  # must match dev/wnba_draft/fit_draft_model.py's CUTOFF_YEAR
AVAIL_CUTOFF_SEASON = 2018  # must match dev/wnba_draft/fit_availability.py's CUTOFF_SEASON


def _load_scored_draft_holdout() -> tuple[pl.DataFrame, pl.DataFrame]:
    draft_history = pl.read_parquet(f"{FIXTURE_DIR}/draft_history.parquet").select(
        "player_id", "draft_year", "overall_pick", "round_number"
    )
    career = pl.read_parquet(f"{FIXTURE_DIR}/career_values.parquet")
    assert draft_history.schema["player_id"] == career.schema["player_id"] == pl.Utf8

    df = (
        draft_history.with_columns(
            pl.col("overall_pick").cast(pl.Float64, strict=False), pl.col("round_number").cast(pl.Float64, strict=False)
        )
        .join(career.select("player_id", "career_value"), on="player_id", how="left")
        .with_columns(pl.col("career_value").fill_null(0.0))
    )

    art = _load_artifact()
    art_median = art["feature_median"]
    _, holdout = as_of_class_split(df, cutoff_year=DRAFT_CUTOFF_YEAR)
    holdout = holdout.with_columns([pl.col(c).fill_null(art_median.get(c, 0.0)) for c in art["features"]])

    pred = _score(holdout, art).select("player_id", "draft_year", "proj_career_value", "draft_prob")
    real = holdout.select("player_id", "career_value")
    return pred, real


def test_wnba_draft_holdout_ranks_realized_value() -> None:
    """Train=1997-2018 (942 prospects), holdout=2019-2025 (271 scored, 259 draft-board rows).

    Observed holdout Spearman(``proj_career_value``, realized ``career_value``): **0.228**
    (deterministic -- see the ``maintain_order=True`` dedup fix in the test/fit scripts).
    Floor set with margin below that, not an aspirational number.
    """
    pred, real = _load_scored_draft_holdout()
    joined = pred.join(real, on="player_id", how="inner")
    # oracle-join-hygiene: a shrunken join must not pass vacuously (observed n=271).
    assert joined.height >= 250, f"draft-gate join shrank to {joined.height} rows (<250) -- fixture/join drifted"
    s = spearman_corr(joined["proj_career_value"].to_numpy(), joined["career_value"].to_numpy())
    assert s >= 0.15, f"WNBA draft-value holdout Spearman {s:.3f} < 0.15 -- debug features/leakage, do NOT lower gate"


def test_wnba_draft_prob_is_a_documented_constant() -> None:
    """`draft_prob` must stay an honest constant, not silently regress to a fake classifier fit.

    See ``dev/wnba_draft/fit_draft_model.py``'s module docstring: ``wnba_stats_drafthistory``
    has no undrafted/invitee negative class, so there is no honest way to fit a real
    drafted/undrafted classifier from this corpus. `prob_coef` must stay all-zero (no
    per-feature signal) and the constant probability must stay high (reflecting the true 100%
    base rate of "this player IS in the draft-history corpus").
    """
    art = _load_artifact()
    assert art["prob_coef"] == [0.0, 0.0], "prob_coef drifted from the documented all-zero constant"
    prob = 1.0 / (1.0 + np.exp(-float(art["prob_intercept"])))
    assert prob > 0.9, f"draft_prob constant {prob:.3f} should reflect the corpus's 100% drafted base rate"


def test_wnba_aging_curve_fitted_from_real_data() -> None:
    """Phase 5 oracle gate -- WNBA aging curve, fitted on real per-player-season box/age data.

    Unlike the NBA gate there is no independent published-WNBA-curve fixture to correlate
    against (out of scope to hand-transcribe one for this task) -- the gate instead checks the
    structural properties every consumer (`nba_career_trajectory`, `wnba_rookie_projection`)
    relies on: a single interior peak (unimodal), a peak age inside a basketball-plausible
    range, and that the curve is no longer the byte-identical NBA copy it shipped as.
    """
    cur = nba_aging_curve(league="wnba")
    nba_cur = nba_aging_curve(league="nba")
    assert cur.height > 0
    peak = cur.filter(pl.col("rel_value") == pl.col("rel_value").max())["age"][0]
    assert 24 <= peak <= 33, f"WNBA peak_age {peak} outside a basketball-plausible [24,33] range"
    d = np.diff(cur.sort("age")["rel_value"].to_numpy())
    assert (np.diff(np.sign(d)) != 0).sum() <= 1, "WNBA aging curve is not unimodal"
    # the T3.4 seed shipped literally byte-identical to the NBA curve -- assert that regression
    # can never silently reappear.
    assert cur["rel_value"].to_list() != nba_cur["rel_value"].to_list(), (
        "wnba_aging_curve.json is byte-identical to nba_aging_curve.json again -- re-fit regressed to the old seed"
    )


def test_wnba_availability_holdout_beats_baseline() -> None:
    """Phase 5 oracle gate -- WNBA availability holdout MAE vs the naive career-mean baseline.

    Train=1997-2018 seasons (2713 rows), holdout=2019-2025 seasons (1126 rows). Observed
    (deterministic, post ``maintain_order=True`` dedup fix): model holdout MAE **0.2315** vs
    career-mean baseline **0.2485** -- beats the naive baseline, the actual gate requirement.
    Floor set with margin above that observed value, not an aspirational number.
    """
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    # maintain_order=True: polars' default sort is unstable under threads, so ties in "min"
    # would otherwise pick a non-deterministic survivor across runs (must match the fit
    # script's dedup exactly for the gate to be reproducible).
    season_stats = season_stats.sort("min", descending=True, maintain_order=True).unique(
        subset=["player_id", "season_id"], keep="first", maintain_order=True
    )
    career = season_stats.with_columns(
        season_stats["season_id"].str.slice(0, 4).cast(pl.Int64).alias("season"),
        pl.col("player_age").alias("age"),
    ).filter(pl.col("season") >= 1997)

    train_raw = career.filter(pl.col("season") <= AVAIL_CUTOFF_SEASON)
    # 40.0: the modern WNBA regular-season game count. Intentional flat approximation -- WNBA
    # seasons were 28-34 games pre-2003 (28 in '97-'98, 32 in '99-2002), so GP% for those early
    # rows is slightly understated. Left flat because it matches the fit script's denominator
    # exactly and the holdout is 2019-2025 (all 36-40-game seasons, unaffected).
    gp_median = float((train_raw["gp"].cast(pl.Float64) / 40.0).clip(0.0, 1.0).median() or 0.75)
    feats = availability_features(career, league="wnba", median_ref={"gp_pct": gp_median, "bmi": 24.0})
    labeled = feats.with_columns((career["gp"].cast(pl.Float64) / 40.0).clip(0.0, 1.0).alias("realized_gp_pct"))
    _, holdout = as_of_class_split(labeled, cutoff_year=AVAIL_CUTOFF_SEASON, year_col="season")

    scored = score_availability(holdout.select("player_id", "season", *_FEATURE_COLS), league="wnba")
    joined = scored.join(holdout.select("player_id", "season", "realized_gp_pct"), on=["player_id", "season"])
    # oracle-join-hygiene: a shrunken join must not pass vacuously (observed n=1126).
    assert joined.height >= 1000, f"availability-gate join shrank to {joined.height} rows (<1000) -- fixture drifted"

    model_mae = mae(joined["avail_pct"].to_numpy(), joined["realized_gp_pct"].to_numpy())
    baseline_mae = mae(holdout["career_gp_pct"].to_numpy(), holdout["realized_gp_pct"].to_numpy())

    assert model_mae <= 0.25, f"WNBA availability holdout MAE {model_mae:.4f} > 0.25 -- debug feature leakage"
    assert model_mae < baseline_mae, (
        f"model MAE {model_mae:.4f} must beat the career-mean baseline MAE {baseline_mae:.4f}"
    )


def test_wnba_rookie_projection_holdout_ranks_realized_value() -> None:
    """Phase 5 oracle gate -- WNBA rookie/sophomore projection holdout backtest.

    Same 2019-2025 holdout classes as the draft-value gate (n=259). Observed holdout
    Spearman(``proj_rookie_value``, realized ``rookie_value``): **0.127** (deterministic; with
    the train-only ``rookie_fraction`` -- see fit_rookie_residual.py). Floor set with margin
    below that, not an aspirational number. The 0.11 floor sits close to the noise threshold for
    n=259 -- a deliberate consequence of the small WNBA rookie corpus (a shorter, shallower
    league than the NBA), not a weak model; it is the honest achievable bar on the data
    available, kept just below the observed 0.127.
    """
    draft_history = pl.read_parquet(f"{FIXTURE_DIR}/draft_history.parquet").select(
        "player_id", "draft_year", "overall_pick", "round_number"
    )
    rookie = pl.read_parquet(f"{FIXTURE_DIR}/rookie_values.parquet")

    art = _load_artifact()
    scored = _score(
        draft_history.with_columns(
            pl.col("overall_pick").cast(pl.Float64, strict=False), pl.col("round_number").cast(pl.Float64, strict=False)
        ),
        art,
    )
    _, holdout_scored = as_of_class_split(scored, cutoff_year=DRAFT_CUTOFF_YEAR)

    curve = nba_aging_curve(league="wnba").select("age", "rel_value")
    rel_rookie = float(curve.filter(pl.col("age") == 22)["rel_value"][0])

    rr_art = json.loads(open("sportsdataverse/nba/models/wnba_rookie_projection.json", encoding="utf-8").read())
    rookie_fraction = rr_art["rookie_fraction"]
    residual = rr_art["residual"]

    composed = holdout_scored.with_columns(
        (pl.col("proj_career_value") * rookie_fraction * rel_rookie).alias("composed")
    )
    residual_expr = pl.lit(0.0)
    for tier, val in residual.items():
        residual_expr = pl.when(pl.col("pro_tier") == tier).then(pl.lit(val)).otherwise(residual_expr)
    composed = composed.with_columns((pl.col("composed") + residual_expr).alias("proj_rookie_value"))
    composed = composed.join(rookie.select("player_id", "rookie_value"), on="player_id", how="left").with_columns(
        pl.col("rookie_value").fill_null(0.0)
    )
    # oracle-join-hygiene: a shrunken holdout must not pass vacuously (observed n=259).
    assert composed.height >= 250, f"rookie-gate holdout shrank to {composed.height} rows (<250) -- fixture drifted"

    s = spearman_corr(composed["proj_rookie_value"].to_numpy(), composed["rookie_value"].to_numpy())
    assert s >= 0.11, f"WNBA rookie-projection holdout Spearman {s:.3f} < 0.11 -- debug the aging-curve ratio"


def test_wnba_rookie_projection_schema_separates_availability() -> None:
    """`proj_avail_pct` must be a separate column, never folded into value."""
    import importlib

    mod = importlib.import_module("sportsdataverse.wnba.wnba_rookie_projection")
    assert {"proj_rookie_value", "proj_soph_value", "proj_avail_pct", "proj_rookie_min"}.issubset(
        set(mod._SCHEMA.keys())
    )
