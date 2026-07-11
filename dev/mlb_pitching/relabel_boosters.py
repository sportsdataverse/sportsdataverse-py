"""Scratch (gitignored) -- bakes canonical feature names into the committed
Stuff+/Command+ .ubj artifacts so xgboost validates train/score column
alignment at predict time (instead of the silent positional-only alignment
that feature_names=None gives -- the same bug class the _CATEGORICAL_CODE_MAPS
fix closed).

Offline, no retrain: the trainers already `df.select(<FEATURES>)` in exactly
the canonical order, so the existing boosters' f0..fN map 1:1 onto the feature
lists below. Relabeling is pure metadata -- predictions are bit-identical
(asserted below), so the "+"-scale centering constants are unaffected.

Run: uv run python dev/mlb_pitching/relabel_boosters.py
"""

from __future__ import annotations

import numpy as np
from xgboost import Booster, DMatrix

from sportsdataverse.mlb.mlb_command_plus import _model_feature_names
from sportsdataverse.mlb.mlb_stuff_plus import STUFF_FEATURES

_ARTIFACTS = {
    "sportsdataverse/mlb/models/mlb_stuff_plus.ubj": STUFF_FEATURES,
    "sportsdataverse/mlb/models/mlb_command_plus.ubj": _model_feature_names(),
}


def main() -> None:
    rng = np.random.default_rng(0)
    for path, names in _ARTIFACTS.items():
        b = Booster()
        b.load_model(path)
        probe = rng.normal(size=(64, len(names)))
        before = b.predict(DMatrix(probe))  # positional
        b.feature_names = names
        after = b.predict(DMatrix(probe, feature_names=names))  # named
        assert np.allclose(before, after), f"relabel changed predictions for {path}"
        b.save_model(path)
        print(f"relabeled {path} -> feature_names={names}")


if __name__ == "__main__":
    main()
