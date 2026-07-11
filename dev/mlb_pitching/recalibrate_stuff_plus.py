"""Scratch (gitignored) — recomputes Stuff+'s "+"-scale centering constants
against a genuinely held-out population instead of the training corpus.

**Why this exists**: ``fit_stuff_plus.py``'s 30-pitcher 2023 training corpus is
workhorse-selected (top-N by pitch volume in a single probe week), which is
not representative of the general league. Centering the "+"-scale on that
corpus's own mean predicted run value shifted a genuinely held-out 2024
population's average ``stuff_plus`` to ~97.6 -- outside the ±0.5 internal-
calibration target. This script re-scores the ALREADY-TRAINED (unchanged)
booster over the committed, non-training
``tests/fixtures/mlb_pitching/pitcher_holdout_season_2024.parquet`` fixture
and prints the mean/sd to paste into ``mlb_pitching_constants.py``.

Offline (no network) -- run with: uv run python dev/mlb_pitching/recalibrate_stuff_plus.py
"""

from __future__ import annotations

import numpy as np
import polars as pl
from xgboost import DMatrix

from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_stuff_plus import STUFF_FEATURES, _load_stuff_booster


def main() -> None:
    holdout = pl.read_parquet("tests/fixtures/mlb_pitching/pitcher_holdout_season_2024.parquet")
    feats = pitch_features(holdout)
    have = [c for c in STUFF_FEATURES if c in feats.columns]
    scored = feats.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in have]))
    booster = _load_stuff_booster()
    x = scored.select(have).to_numpy()
    dmat = DMatrix(x, feature_names=have)
    rv_hat = booster.predict(dmat)
    print("n pitches:", len(rv_hat))
    print("STUFF_LEAGUE_MEAN_RV =", float(np.mean(rv_hat)))
    print("STUFF_LEAGUE_SD_RV =", float(np.std(rv_hat)))


if __name__ == "__main__":
    main()
