"""The college pregame fitter's paired-comparison SE (tools/models/mbb_prediction/fit_pregame.py)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pytest

_PATH = Path(__file__).resolve().parents[2] / "tools" / "models" / "mbb_prediction" / "fit_pregame.py"
_spec = importlib.util.spec_from_file_location("fit_pregame", _PATH)
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)


def test_cr1_se_matches_the_clustered_formula() -> None:
    d = np.array([1.0, 2.0, 3.0, 10.0, 0.0])
    g = np.array(["a", "a", "b", "b", "c"])
    # cluster sums of d - mean (mean 3.2): a -3.4, b 6.6, c -3.2; CR1 = sqrt(3/2 * sum^2) / 5
    expected = np.sqrt(1.5 * (3.4**2 + 6.6**2 + 3.2**2)) / 5
    assert fit._cr1_se(d, g) == pytest.approx(expected, rel=1e-12)
    # one observation per cluster: the usual SE of the mean with the G / (G - 1) correction
    x = np.array([0.5, -1.0, 2.0, 0.25])
    assert fit._cr1_se(x, np.arange(4)) == pytest.approx(x.std(ddof=1) / 2, rel=1e-12)
