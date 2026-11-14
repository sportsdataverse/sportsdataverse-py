"""Offline check of the pooling in ``tools/fit_nhl_special_teams_rates.py`` (the fit itself needs the network)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import polars as pl
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_st", _ROOT / "tools" / "fit_nhl_special_teams_rates.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)


def test_pp_rate_pools_only_special_teams_stints_from_the_pp_side():
    stints = pl.DataFrame(
        {
            "strength_state": ["5v4", "4v5", "5v5", "5v4", None],
            "duration": [600, 1200, 3600, 0, 60],
            "xgf_home": [1.0, 0.1, 9.0, 5.0, 5.0],  # 4v5: the AWAY side is on the power play
            "xgf_away": [0.2, 2.0, 9.0, 5.0, 5.0],
        }
    )
    rate, secs = fit.pp_rate(stints)
    assert secs == 1800  # the 5v5, zero-length and unparseable stints are dropped
    assert rate == pytest.approx((1.0 + 2.0) * 3600.0 / 1800)
