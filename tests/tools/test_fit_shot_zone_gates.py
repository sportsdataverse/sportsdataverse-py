"""BB-D35: the shot-zone rank gates re-fit offline from the committed reliability curves.

``tests/fixtures/bb_shot_zone_gates/{lg}_fits.csv`` are the split-half curves
``tools/fit_shot_zone_gates.py --write`` saved from the real shot releases (README in
``tests/fixtures/bb_shot_zones``). The ``M`` fit is re-run on each TRAIN curve and must land on
the frozen :data:`~sportsdataverse._common.shot_zone_profiles.PLAYER_MIN_N` / ``TEAM_MIN_N`` gate;
a rate whose train ``M`` is not finite must have no gate (never ranked).

Holdout reliability at the frozen gate (``g / (g + M_holdout)``) over the rates with a finite
holdout ``M``, the median and the worst (observed, rounded down at the second decimal; never
loosen a gate to pass -- refit): players NBA 0.48 / 0.43, WNBA 0.48 / 0.43, MBB 0.51 / 0.49,
WBB 0.51 / 0.49; teams NBA 0.47 / 0.34, WNBA 0.48 / 0.38, MBB 0.52 / 0.43, WBB 0.51 / 0.41. A train
gate the holdout puts under a third signal is withheld by the freeze rule (``frozen_gates``): MBB
player ``corner_3_ast_pct``, NBA team ``opp_rim_ast_pct``, WNBA team ``mid_ast_pct`` / ``mid_freq`` /
``opp_above_break_3_freq``.
"""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.shot_zone_profiles import PLAYER_MIN_N, TEAM_MIN_N, zone_columns

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_shot_zone_gates", _ROOT / "tools" / "fit_shot_zone_gates.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)
_rate_spec = importlib.util.spec_from_file_location("fit_rate_gates", _ROOT / "tools" / "fit_rate_gates.py")
rate_fit = importlib.util.module_from_spec(_rate_spec)
_rate_spec.loader.exec_module(rate_fit)

GATES = _ROOT / "tests" / "fixtures" / "bb_shot_zone_gates"
FROZEN = {"player": PLAYER_MIN_N, "team": TEAM_MIN_N}
#: (league, kind) -> (median, worst) holdout reliability at the frozen gate, observed, rounded down.
HOLDOUT = {
    ("nba", "player"): (0.48, 0.43), ("wnba", "player"): (0.48, 0.43), ("mbb", "player"): (0.51, 0.49),
    ("wbb", "player"): (0.51, 0.49), ("nba", "team"): (0.47, 0.34), ("wnba", "team"): (0.48, 0.38),
    ("mbb", "team"): (0.52, 0.43), ("wbb", "team"): (0.51, 0.41),
}  # fmt: skip
LEAGUES = ("nba", "wnba", "mbb", "wbb")


def _fits(league: str) -> pl.DataFrame:
    return pl.read_csv(GATES / f"{league}_fits.csv")


@pytest.mark.parametrize("league", LEAGUES)
def test_frozen_gates_refit_from_the_curves(league: str) -> None:
    """Re-run every ``M`` fit from the curve, then the freeze rule, and land on the module's gates."""
    fits = _fits(league)
    refit = fits.with_columns(
        pl.Series("m", [fit.fit_m(rate_fit.parse_curve(c or "")) for c in fits["curve"]], dtype=pl.Float64)
    )
    for kind, table in FROZEN.items():
        rates = zone_columns() + ([f"opp_{m}" for m in zone_columns()] if kind == "team" else [])
        for span in ("train", "holdout"):
            got = refit.filter((pl.col("kind") == kind) & (pl.col("span") == span))["rate"].to_list()
            assert sorted(got) == sorted(rates)
        assert fit.frozen_gates(refit, kind) == table[league], kind


@pytest.mark.parametrize("league", LEAGUES)
def test_holdout_reliability_at_the_frozen_gate(league: str) -> None:
    hold = _fits(league).filter(pl.col("span") == "holdout")
    for kind, table in FROZEN.items():
        rel = []
        for r in hold.filter(pl.col("kind") == kind).iter_rows(named=True):
            g = table[league].get(r["rate"])
            mh = fit.fit_m(rate_fit.parse_curve(r["curve"] or ""))
            if g is not None and math.isfinite(mh):
                rel.append(g / (g + mh))
        median, worst = HOLDOUT[(league, kind)]
        assert np.median(rel) >= median and min(rel) >= worst >= fit.MIN_HOLDOUT_R, (kind, np.median(rel), min(rel))


def test_gate_rounds_up() -> None:
    assert fit.gate(69.96) == 70 and fit.gate(1222.3) == 1300
