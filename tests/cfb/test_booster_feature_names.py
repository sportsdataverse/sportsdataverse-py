"""Every bundled CFB booster names its features, so a permuted DMatrix raises (gap-C-083 twin, task 2.7).

The NFL `qbr_model.ubj` shipped unnamed and was scored on permuted inputs; this keeps the CFB bundle from
ever taking an unnamed artifact, and pins the CFB QBR booster's names to `cfb.model_vars.qbr_vars`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from xgboost import Booster

from sportsdataverse.cfb.model_vars import qbr_vars

MODELS = Path(__file__).resolve().parents[2] / "sportsdataverse" / "cfb" / "models"


def _load(path: Path) -> Booster:
    b = Booster()
    b.load_model(str(path))
    return b


@pytest.mark.parametrize("path", sorted(MODELS.glob("*.ubj")), ids=lambda p: p.name)
def test_every_bundled_booster_names_its_features(path):
    b = _load(path)
    assert b.feature_names is not None and len(b.feature_names) == b.num_features(), (path.name, b.feature_names)


def test_cfb_qbr_model_names_are_qbr_vars():
    assert _load(MODELS / "qbr_model.ubj").feature_names == list(qbr_vars)
