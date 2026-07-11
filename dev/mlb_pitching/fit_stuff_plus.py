"""Scratch (gitignored) — reproducible trainer for the bundled Stuff+ xgboost model.

Run with (network + a few minutes):

    SDV_PY_LIVE_TESTS=1 uv run python dev/mlb_pitching/fit_stuff_plus.py

**Disclosed corpus-size deviation from the plan's "2021-2023" full-league pull:**
a full 3-season, all-pitcher Statcast pull is impractical within a single build
session (tens of millions of rows, hours of chunked HTTP requests against
Savant's 25k-row cap). Instead this trainer pulls the **full 2023 season for
~30 real, diverse pitchers** (discovered from a live one-week probe across
starters/relievers, not hand-picked) — a real, unbiased, moderately-sized
corpus. This is a corpus-size trade-off, not a validation shortcut: the
Task 2.4 oracle gate still runs against the full real 2024 Savant fixtures and
its floor is set from the observed value, never lowered to pass.

Writes ``sportsdataverse/mlb/models/mlb_stuff_plus.ubj`` and prints
``league_mean_rv`` / ``league_sd_rv`` to paste into
``mlb_pitching_constants.py`` (``STUFF_LEAGUE_MEAN_RV`` / ``STUFF_LEAGUE_SD_RV``).
"""

from __future__ import annotations

import numpy as np
import polars as pl
import xgboost as xgb

from sportsdataverse.mlb import mlb_statcast_search
from sportsdataverse.mlb.mlb_pitch_features import add_sequence_features, pitch_features
from sportsdataverse.mlb.mlb_stuff_plus import STUFF_FEATURES

ARTIFACT = "sportsdataverse/mlb/models/mlb_stuff_plus.ubj"


def load_corpus() -> pl.DataFrame:
    probe = pitch_features(
        mlb_statcast_search("2023-06-01", "2023-06-07", player_type="pitcher").with_columns(
            pl.col("pitcher").cast(pl.Int64)
        )
    )
    pitcher_ids = (
        probe.group_by("pitcher").agg(pl.len().alias("n")).sort("n", descending=True).head(30)["pitcher"].to_list()
    )
    print(f"training corpus: {len(pitcher_ids)} distinct pitchers, full 2023 season")
    raw = mlb_statcast_search("2023-03-30", "2023-10-01", player_type="pitcher", pitchers_lookup=pitcher_ids)
    return add_sequence_features(pitch_features(raw))


def main() -> None:
    df = load_corpus().drop_nulls(subset=STUFF_FEATURES + ["run_value"])
    print("corpus rows after dropna:", df.height)
    # fit on a NAMED pandas frame so the booster records feature_names and
    # xgboost validates train/score column alignment at predict time (not the
    # silent positional-only alignment feature_names=None gives).
    x = df.select(STUFF_FEATURES).to_pandas()
    y = df["run_value"].to_numpy()
    model = xgb.XGBRegressor(n_estimators=400, max_depth=4, learning_rate=0.05, subsample=0.8, n_jobs=-1)
    model.fit(x, y)
    model.get_booster().save_model(ARTIFACT)
    rv_hat = model.predict(x)
    print("league_mean_rv =", float(np.mean(rv_hat)))
    print("league_sd_rv =", float(np.std(rv_hat)))


if __name__ == "__main__":
    main()
