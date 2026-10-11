"""Every bundled NFL booster names its features, so a permuted DMatrix raises (gap-C-083 = W-C3, task 2.7).

An unnamed booster accepts any column order: the bundled `qbr_model.ubj` shipped unnamed and was fed
`[qbr_epa, sack_epa, pass_epa, ...]` against its trainer's `[qbr_epa, pass_epa, rush_epa, sack_epa, ...]`
(`cfbfastR-dev/cfb_qbr/qbr_xgb.R:233`, cfb_qbr @ fb56ff3). Task 2.7 re-saved the same trees with the
trainer's names; these tests keep every bundled artifact named and the QBR names equal to `qbr_vars`.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pytest
from xgboost import Booster, DMatrix

from sportsdataverse.nfl.model_vars import qbr_vars

MODELS = Path(__file__).resolve().parents[2] / "sportsdataverse" / "nfl" / "models"
TRAINER_ORDER = ["qbr_epa", "pass_epa", "rush_epa", "sack_epa", "pen_epa", "spread"]
# sha256 of the trees (`learner.gradient_booster`, sorted JSON) of the unnamed artifact at devel-nfl ece03537a6:
# the named re-save changed only `learner.feature_names`, never a split.
QBR_TREES_SHA256 = "fd62057ae3c5381a71d1d9c86972262448f1a1d3980ed90a08e16caa98b667df"


def _load(path: Path) -> Booster:
    b = Booster()
    b.load_model(str(path))
    return b


@pytest.mark.parametrize("path", sorted(MODELS.glob("*.ubj")), ids=lambda p: p.name)
def test_every_bundled_booster_names_its_features(path):
    b = _load(path)
    assert b.feature_names is not None and len(b.feature_names) == b.num_features(), (path.name, b.feature_names)


def test_qbr_model_names_are_the_trainer_order_and_qbr_vars():
    assert _load(MODELS / "qbr_model.ubj").feature_names == TRAINER_ORDER
    assert list(qbr_vars) == TRAINER_ORDER


def test_qbr_rename_kept_every_tree():
    trees = json.loads(_load(MODELS / "qbr_model.ubj").save_raw("json"))["learner"]["gradient_booster"]
    assert hashlib.sha256(json.dumps(trees, sort_keys=True).encode()).hexdigest() == QBR_TREES_SHA256


def test_the_old_permuted_order_now_raises():
    old = ["qbr_epa", "sack_epa", "pass_epa", "rush_epa", "pen_epa", "spread"]
    with pytest.raises(ValueError, match="feature_names mismatch"):
        _load(MODELS / "qbr_model.ubj").predict(DMatrix(np.zeros((1, 6)), feature_names=old))


def test_the_processor_loaded_the_named_qbr_model():
    from sportsdataverse.nfl import nfl_pbp

    assert nfl_pbp.qbr_model.feature_names == list(qbr_vars)
