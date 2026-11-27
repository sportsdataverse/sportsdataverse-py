"""cfb_adjusted_breakdowns._PUBLISHED is the fitter's output on its committed split-half table.

``tests/fixtures/cfb_adjusted_breakdowns/split_half_by_season.csv`` is ``tools/fit_cfb_adjusted_breakdowns.py``'s
per-season split-half table, 2014-2025 (provenance in that folder's README). These tests re-derive the tiers and the
shrinkage constants from it, so a hand edit of a tier or a ``k`` fails here.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_adjusted_breakdowns import _PUBLISHED

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "fit_cfb_adjusted_breakdowns", _ROOT / "tools" / "fit_cfb_adjusted_breakdowns.py"
)
assert _spec is not None and _spec.loader is not None
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)

TABLE = _ROOT / "tests" / "fixtures" / "cfb_adjusted_breakdowns" / "split_half_by_season.csv"


def _published(tiers: pl.DataFrame) -> dict[tuple[str, str], tuple[str, float]]:
    rows = tiers.filter(pl.col("tier") != "none").iter_rows(named=True)
    return {(r["metric"], r["side"]): (r["tier"], round(r["k"], 1)) for r in rows}


def test_published_constants_are_the_fitters_output() -> None:
    derived = _published(fit.tiers(pl.read_csv(TABLE)))
    for key, value in _PUBLISHED.items():
        assert derived.get(key) == value, key


def test_holdout_seasons_do_not_move_any_tier_or_k() -> None:
    table = pl.read_csv(TABLE)
    assert set(table["season"].unique().to_list()) == set(fit.TRAIN) | set(fit.HOLDOUT)
    full = fit.tiers(table).select("metric", "side", "tier", "k")
    train_only = fit.tiers(table.filter(pl.col("season").is_in(fit.TRAIN))).select("metric", "side", "tier", "k")
    assert full.equals(train_only)


def test_rule_thresholds_are_the_preregistered_ones() -> None:
    assert (fit.SB_ADJ_MIN, fit.SB_GAIN_MIN, fit.SB_RAW_MIN) == (0.50, 0.03, 0.40)
    assert fit.TRAIN == tuple(range(2014, 2024))
    assert fit.HOLDOUT == (2024, 2025)
