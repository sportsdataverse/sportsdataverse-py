"""Train + bundle the play-call classifier (Task 1.2; network required).

Trains ``multi:softprob`` on 2016-2021 (seasons strictly before the 2022-2023
evaluation window — the as-of leakage boundary) and writes
``sportsdataverse/nfl/models/nfl_playcall.ubj``.
"""

from pathlib import Path

import numpy as np
import polars as pl
import xgboost as xgb

from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp, load_nfl_pbp_participation
from sportsdataverse.nfl.nfl_playcall import (
    FAMILIES,
    PLAYCALL_FEATURE_ORDER,
    playcall_features,
)

TRAIN_SEASONS = list(range(2016, 2022))


def main() -> None:
    print("loading pbp", TRAIN_SEASONS, flush=True)
    pbp = calculate_xpass(load_nfl_pbp(TRAIN_SEASONS))
    print("loading participation", flush=True)
    # per-season + diagonal_relaxed: multi-season loader call crashes on schema drift
    part = pl.concat(
        [load_nfl_pbp_participation([s]) for s in TRAIN_SEASONS],
        how="diagonal_relaxed",
    )
    feat = playcall_features(pbp, part)
    feat = feat.filter(pl.col("family").is_not_null())
    print("training rows:", feat.height, flush=True)

    x = feat.select(PLAYCALL_FEATURE_ORDER).to_numpy().astype(np.float32)
    y = np.array([FAMILIES.index(f) for f in feat["family"].to_list()], dtype=np.int32)
    dtrain = xgb.DMatrix(x, label=y, feature_names=PLAYCALL_FEATURE_ORDER)
    booster = xgb.train(
        {
            "objective": "multi:softprob",
            "num_class": len(FAMILIES),
            "max_depth": 6,
            "eta": 0.1,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "tree_method": "hist",
            "eval_metric": "mlogloss",
        },
        dtrain,
        num_boost_round=300,
    )
    out = Path(__file__).resolve().parents[2] / "sportsdataverse" / "nfl" / "models" / "nfl_playcall.ubj"
    booster.save_model(str(out))
    print("saved", out, flush=True)
    probs = booster.predict(dtrain)
    in_sample = float(-np.mean(np.log(np.clip(probs[np.arange(y.size), y], 1e-15, 1.0))))
    print("in-sample mlogloss:", in_sample, flush=True)


if __name__ == "__main__":
    main()
