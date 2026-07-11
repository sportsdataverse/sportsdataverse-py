"""Scratch (gitignored) — reproducible trainer for the bundled Command+/Location+ xgboost model.

Run with (network + a few minutes):

    SDV_PY_LIVE_TESTS=1 uv run python dev/mlb_pitching/fit_command_plus.py

Reuses the same 30-pitcher, full-2023-season corpus as
``dev/mlb_pitching/fit_stuff_plus.py`` (see that module's docstring for the
disclosed corpus-size deviation from the plan's "2021-2023" full-league pull)
so the two bundled models are trained on an identical population.

Writes ``sportsdataverse/mlb/models/mlb_command_plus.ubj`` and prints
``command_mean_rv`` / ``command_sd_rv`` to paste into
``mlb_pitching_constants.py`` (``COMMAND_LEAGUE_MEAN_RV`` / ``COMMAND_LEAGUE_SD_RV``).
"""

from __future__ import annotations

import numpy as np
import xgboost as xgb

from sportsdataverse.mlb.mlb_command_plus import _encode_categoricals, _model_feature_names
from sportsdataverse.mlb.mlb_stuff_plus import STUFF_FEATURES  # noqa: F401 -- documents feature-set contrast

try:
    from fit_stuff_plus import load_corpus
except ImportError:  # running as `python dev/mlb_pitching/fit_command_plus.py` (no package context)
    import sys

    sys.path.insert(0, "dev/mlb_pitching")
    from fit_stuff_plus import load_corpus  # type: ignore[no-redef]

ARTIFACT = "sportsdataverse/mlb/models/mlb_command_plus.ubj"


def main() -> None:
    df = _encode_categoricals(load_corpus())
    feature_names = _model_feature_names()
    have = [c for c in feature_names if c in df.columns]
    df = df.drop_nulls(subset=have + ["run_value"])
    print("corpus rows after dropna:", df.height)
    x = df.select(have).to_numpy()
    y = df["run_value"].to_numpy()
    model = xgb.XGBRegressor(n_estimators=400, max_depth=4, learning_rate=0.05, subsample=0.8, n_jobs=-1)
    model.fit(x, y)
    model.get_booster().save_model(ARTIFACT)
    rv_hat = model.predict(x)
    print("command_mean_rv =", float(np.mean(rv_hat)))
    print("command_sd_rv =", float(np.std(rv_hat)))


if __name__ == "__main__":
    main()
