"""``tools/fit_nba_shot_shrinkage_k.py`` reruns to the frozen zone-table k on the committed fixture."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from sportsdataverse.nba.nba_shot_value_constants import TALENT_SHRINKAGE_K

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_k", _ROOT / "tools" / "fit_nba_shot_shrinkage_k.py")
fit_k = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit_k)


def test_zone_k_is_the_frozen_constant():
    assert fit_k.main("zone") == pytest.approx(TALENT_SHRINKAGE_K["00"], abs=0.05)
    assert TALENT_SHRINKAGE_K["20"] == TALENT_SHRINKAGE_K["00"]  # the G League reuses the NBA fit


def test_location_k_is_the_one_the_docs_quote():
    """Not frozen while the location model is opt-in (``score_shot_xpoints`` and the lineage page quote it)."""
    assert fit_k.main("location") == pytest.approx(60.7, abs=0.05)
