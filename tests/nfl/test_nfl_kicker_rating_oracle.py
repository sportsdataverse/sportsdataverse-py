"""Phase-3 oracle gates: FG calibration + kicker rank sanity.

Fixture provenance: tests/fixtures/nfl_scheme/README.md.  Calibration is
gated on the 2014-2023 corpus (n=10481; ~1048 attempts per decile) so the
binomial noise floor sits below the 0.03 gate; the 2019-2023 sub-window
shows one ~2-sigma decile dip (0.040 at n=532) that is sampling noise
(its mean_actual is non-monotone vs neighboring deciles, which no
calibrated monotone model can reproduce).
"""

from pathlib import Path

import importlib

import numpy as np
import polars as pl
import pytest

k = importlib.import_module("sportsdataverse.nfl.nfl_kicker_rating")

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_scheme"


def _scored(name: str) -> pl.DataFrame:
    fg = pl.read_parquet(FIXTURES / name)
    return k.env_adjusted_make_prob(fg).with_columns(
        (pl.col("field_goal_result") == "made").cast(pl.Int64).alias("made")
    )


@pytest.fixture(scope="module")
def fg14() -> pl.DataFrame:
    return _scored("fg_attempts_2014_2023.parquet")


@pytest.fixture(scope="module")
def fg19() -> pl.DataFrame:
    return _scored("fg_attempts_2019_2023.parquet")


def test_env_adjusted_calibration_deciles(fg14):
    """Gate: |mean_pred - mean_actual| <= 0.03 per exp_make_prob decile.

    Observed at gate time (2026-07-08, 2014-2023 fixture, n=10481): max
    decile gap 0.0272.  Hypotheses tried before the fix (never widening the
    gate): offset-only refit, free intercept+slope recalibration, quadratic
    distance recalibration — the residual under-prediction of long kicks
    traced to nfl4th's 0.9 decision clamp (selection bias on attempted 56+
    yarders), corrected by the fitted ``long_kick`` term.
    """
    p = fg14["exp_make_prob"].to_numpy()
    made = fg14["made"].to_numpy()
    order = np.argsort(p)
    dec = np.arange(len(p))[np.argsort(order)] * 10 // len(p)
    gaps = [abs(p[dec == i].mean() - made[dec == i].mean()) for i in range(10)]
    assert max(gaps) <= 0.03, f"decile gaps {gaps}"


def test_known_elite_kicker_top_decile(fg19):
    """Gate: Justin Tucker sits in the top FGOE-per-att decile, 2019-2023 pooled.

    Observed at gate time: rank 1 of 36 kickers with 50+ attempts.
    """
    pooled = (
        fg19.filter(pl.col("kicker_player_id").is_not_null())
        .group_by("kicker_player_id")
        .agg(
            pl.col("kicker_player_name").drop_nulls().first().alias("name"),
            pl.len().alias("att"),
            ((pl.col("made") - pl.col("exp_make_prob")).sum() / pl.len()).alias("fgoe_pa"),
        )
        .filter(pl.col("att") >= 50)
        .sort("fgoe_pa", descending=True)
    )
    names = [str(n) for n in pooled["name"].to_list()]
    tucker_rank = next(i + 1 for i, n in enumerate(names) if "Tucker" in n)
    assert tucker_rank <= max(1, pooled.height // 10), f"Tucker rank {tucker_rank}"
