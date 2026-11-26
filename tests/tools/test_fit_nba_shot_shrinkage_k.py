"""``tools/fit_nba_shot_shrinkage_k.py`` reruns to the frozen shrinkage constants on the committed fixture."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from sportsdataverse.nba.nba_shot_value_constants import TALENT_SHRINKAGE_K

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_k", _ROOT / "tools" / "fit_nba_shot_shrinkage_k.py")
fit_k = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit_k)


def test_nba_k_is_the_location_model_fit():
    assert fit_k.main("location") == pytest.approx(TALENT_SHRINKAGE_K["00"], abs=0.05)


def test_g_league_k_is_the_zone_table_fit():
    """The G League is scored from the zone table and reuses the NBA fixture's zone fit."""
    assert fit_k.main("zone") == pytest.approx(TALENT_SHRINKAGE_K["20"], abs=0.05)
