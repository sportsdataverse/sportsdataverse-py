"""WBB shot-quality oracle gates (offline, fixture-driven; same thresholds
as the mens gates in ``tests/mbb/test_mbb_shot_quality_oracle.py``).

Season 2026 (the wbb shots-release floor). Hard external anchors are the
OBSERVED women's Barttorvik 2026 national 2P/3P rates; the women's zone
baselines are literature estimates (sanity band). Observed at gate
authorship: calibration ~1.00, fitted talent k=92.4 (split MSE 0.0105 vs
0.0163 unshrunk). NEVER loosen to pass -- debug the model.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mbb.mbb_shot_quality import mbb_shot_quality, mbb_shot_quality_model
from sportsdataverse.mbb.mbb_shot_quality_constants import (
    BART_NATIONAL_SPLITS,
    PUBLISHED_ZONE_BASELINES,
    get_constants,
)

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "wbb_shot_quality"

CAL_LO, CAL_HI = 0.98, 1.02
ANCHOR_TOL = 0.02
ZONE_ESTIMATE_TOL = 0.07


def _train() -> pl.DataFrame:
    return pl.read_parquet(_FIX / "espn_shots_2026_train.parquet")


def _holdout() -> pl.DataFrame:
    return pl.read_parquet(_FIX / "espn_shots_2026_holdout.parquet")


def test_wbb_train_holdout_games_disjoint():
    assert set(_train()["game_id"].to_list()).isdisjoint(_holdout()["game_id"].to_list())


def test_wbb_holdout_calibration_gate():
    model = mbb_shot_quality_model(_train(), league="womens")
    scored = mbb_shot_quality(_holdout(), model=model, league="womens").filter(pl.col("xpoints").is_not_null())
    assert scored.height > 300_000, f"holdout join collapsed: n={scored.height}"
    actual = scored.select((pl.col("made").cast(pl.Float64) * pl.col("point_value").cast(pl.Float64)).sum()).item()
    ratio = scored.get_column("xpoints").sum() / actual
    assert CAL_LO <= ratio <= CAL_HI, f"wbb holdout calibration {ratio:.4f} outside [{CAL_LO}, {CAL_HI}]"
    # per-zone band (observed max 0.0118, abovebreak3) -- derived ceiling 0.03
    per_zone = scored.group_by("shot_zone").agg(
        (pl.col("xmake").mean() - pl.col("made").cast(pl.Float64).mean()).abs().alias("gap")
    )
    assert float(per_zone.get_column("gap").max()) <= 0.03


def test_wbb_external_bart_anchors():
    h = _holdout()
    for pv, key in ((2, "fg2_pct"), (3, "fg3_pct")):
        rate = h.filter(pl.col("point_value") == pv).get_column("made").cast(pl.Float64).mean()
        anchor = BART_NATIONAL_SPLITS["womens"][key]
        assert abs(rate - anchor) <= ANCHOR_TOL, f"{key}: fixture {rate:.4f} vs bart {anchor:.4f}"


def test_wbb_zone_structure_and_estimate_band():
    zone_rates = dict(_train().group_by("shot_zone").agg(pl.col("made").cast(pl.Float64).mean().alias("r")).iter_rows())
    assert max(zone_rates, key=zone_rates.get) == "rim"
    assert all(0.20 < r < 0.75 for r in zone_rates.values()), zone_rates
    assert zone_rates["rim"] - zone_rates["abovebreak3"] >= 0.15
    for zone, est in PUBLISHED_ZONE_BASELINES["womens"].items():
        assert abs(zone_rates[zone] - est) <= ZONE_ESTIMATE_TOL, (zone, zone_rates[zone], est)


def test_wbb_shooter_talent_split_half_reliability_gate():
    from sportsdataverse.mbb.mbb_shooter_talent import talent_split_mse

    model = mbb_shot_quality_model(_train(), league="womens")
    scored = mbb_shot_quality(_train(), model=model, league="womens")
    k = get_constants("womens").shrink_k_talent
    assert k > 0, "womens shrink_k_talent not fitted"
    # off-fit-seed evaluation (k fitted at seed=0)
    for seed in (1, 2):
        assert talent_split_mse(scored, k=k, seed=seed) < talent_split_mse(scored, k=1e-9, seed=seed)


def test_wbb_shot_selection_zero_sum():
    from sportsdataverse.mbb.mbb_shot_selection import mbb_shot_selection

    model = mbb_shot_quality_model(_train(), league="womens")
    scored = mbb_shot_quality(_holdout(), model=model, league="womens").filter(pl.col("xpoints").is_not_null())
    sel = mbb_shot_selection(scored, group="team_id")
    total = float((sel.get_column("selection_value") * sel.get_column("n_shots")).sum())
    assert abs(total) < 1e-6 * scored.height
    # structural half (mirrors the mens gate): the most rim-heavy high-volume
    # team rates positive
    rim_share = (
        scored.group_by("team_id")
        .agg((pl.col("shot_zone") == "rim").cast(pl.Float64).mean().alias("rim_share"), pl.len().alias("n"))
        .filter(pl.col("n") >= 200)
        .sort("rim_share", descending=True)
    )
    top_team = rim_share.row(0, named=True)["team_id"]
    assert sel.filter(pl.col("team_id") == top_team).row(0, named=True)["selection_value"] > 0
