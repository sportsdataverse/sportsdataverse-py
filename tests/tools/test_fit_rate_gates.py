"""Lane 10: the rank sample gates re-fit offline from the committed reliability curves.

``tests/fixtures/bb_rate_gates/{lg}_fits.csv`` are the split-half curves ``tools/fit_rate_gates.py
--write`` saved from the real ESPN team box scores and the BB-D6 game impact box (README there).
The ``M`` fit (``r(n) = n / (n + M)``) is re-run here on each TRAIN curve and must land on the
frozen :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N` / ``ON_OFF_MIN_N`` gate (``M`` rounded
up to two significant figures); the HOLDOUT seasons' own ``M`` then scores the frozen gate.

Holdout reliability at the frozen gate (``g / (g + M_holdout)``), the median and the worst rate of
each league (gates = observed, rounded down at the second decimal; never loosen a gate to pass --
refit and re-derive it): team rates NBA 0.479 / 0.39 (opp_orb_pct), WNBA 0.489 / 0.33 (pace), MBB
0.492 / 0.43 (opp_ft_rate), WBB 0.503 / 0.41 (opp_efg_pct); on/off NBA 0.555 / 0.49, WNBA
0.477 / 0.26 (off_rtg_on), MBB 0.513 / 0.50, WBB 0.542 / 0.53. A gate fitted on the train seasons
is about half signal on seasons it never saw; single rates scatter (the WNBA has 36 holdout teams).
"""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.rate_gates import LEVELS, ON_OFF_MIN_N, TEAM_MIN_N, on_off_gates, team_gates

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_rate_gates", _ROOT / "tools" / "fit_rate_gates.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)

GATES = _ROOT / "tests" / "fixtures" / "bb_rate_gates"
FROZEN = {"team": TEAM_MIN_N, "on_off": ON_OFF_MIN_N}
#: (league, kind) -> (median, worst) holdout reliability at the frozen gate, observed, rounded down.
HOLDOUT = {
    ("nba", "team"): (0.47, 0.39), ("wnba", "team"): (0.48, 0.33), ("mbb", "team"): (0.49, 0.43),
    ("wbb", "team"): (0.50, 0.41), ("nba", "on_off"): (0.55, 0.49), ("wnba", "on_off"): (0.47, 0.26),
    ("mbb", "on_off"): (0.51, 0.50), ("wbb", "on_off"): (0.54, 0.53),
}  # fmt: skip


def _fits(league: str) -> pl.DataFrame:
    return pl.read_csv(GATES / f"{league}_fits.csv")


@pytest.mark.parametrize("league", LEVELS)
def test_refit_lands_on_the_frozen_gates(league: str) -> None:
    fits = _fits(league)
    for kind, table in FROZEN.items():
        train = fits.filter(kind=kind, span="train")
        assert sorted(train["rate"]) == sorted(table[league]), f"{league} {kind}: a gate without a fit, or the reverse"
        train_span, hold_span = fit.SPANS[league][kind]
        hold = fits.filter(kind=kind, span="holdout")
        # the committed curves are the tool's current spans: a moved span without a refit goes red
        assert set(train["seasons"]) == {f"{train_span.start}-{train_span.stop - 1}"}
        assert set(hold["seasons"]) == {f"{hold_span.start}-{hold_span.stop - 1}"}
        assert not set(train_span) & set(hold_span) and max(train_span) < min(hold_span)
        for r in train.iter_rows(named=True):
            m = fit.fit_m(fit.parse_curve(r["curve"]))
            # the grid search is exact up to the platform's float summation (Windows vs Linux numpy)
            assert m == pytest.approx(r["m"], rel=1e-9) or (math.isinf(m) and math.isinf(r["m"])), (
                f"{league} {r['rate']}"
            )
            frozen = table[league][r["rate"]]
            assert frozen == (fit.gate(m) if math.isfinite(m) else math.inf), f"{league} {kind} {r['rate']}"
            if math.isfinite(m):
                assert m <= frozen < 1.11 * m + 1  # rounded up, never down: every ranked row has r >= 0.5


@pytest.mark.parametrize(("league", "kind"), sorted(HOLDOUT))
def test_frozen_gates_hold_out(league: str, kind: str) -> None:
    fits = _fits(league).filter(kind=kind)
    held = {r["rate"]: r["m"] for r in fits.filter(span="holdout").iter_rows(named=True)}
    rel = [g / (g + held[m]) for m, g in FROZEN[kind][league].items() if math.isfinite(g) and math.isfinite(held[m])]
    assert len(rel) >= 3
    median, worst = HOLDOUT[(league, kind)]
    assert float(np.median(rel)) >= median and min(rel) >= worst


def test_gate_rounds_up_to_two_significant_figures() -> None:
    assert [fit.gate(x) for x in (1253.0, 1805.0, 24.65, 4.8, 130.0, 999.1)] == [1300, 1900, 25, 5, 130, 1000]
    with pytest.raises(ValueError, match="no gate"):
        fit.gate(math.inf)


def test_gate_lookups() -> None:
    assert team_gates(None, ["off_rtg"]) == {}
    assert team_gates("nba", ["off_rtg", "rim_pct"]) == {"off_rtg": TEAM_MIN_N["nba"]["off_rtg"]}  # rim: no gate
    assert on_off_gates("mbb", ["net_rtg_diff"]) == {"net_rtg_diff": math.inf}  # never half signal in college
    assert fit.fit_m([(10.0, 0.05, 50), (20.0, 0.08, 50), (40.0, 0.1, 50)]) == math.inf  # no extrapolated gate
    with pytest.raises(KeyError, match="no sample gates"):
        team_gates("nbagl", ["off_rtg"])
