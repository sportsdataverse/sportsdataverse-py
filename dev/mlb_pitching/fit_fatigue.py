"""Scratch (gitignored) -- fits the TTO/fatigue OLS coefficients from the
committed real 2023 pitcher-season sample fixture (offline, no network).

Run with: uv run python dev/mlb_pitching/fit_fatigue.py

Fits ``run_value ~ C(times_through_order) + cum_pitches_game +
velo_drop_from_start`` via OLS (``numpy.linalg.lstsq``), TTO=1 as the
reference level. Prints ``tto_penalty`` to paste into
``mlb_pitching_constants.py``'s ``LEAGUE_BASELINES``.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_pitch_fatigue import add_velo_drop_from_start
from sportsdataverse.mlb.mlb_pitch_features import add_sequence_features, pitch_features


def main() -> None:
    raw = pl.read_parquet("tests/fixtures/mlb_pitching/pitcher_season_pitches_2023_sample.parquet")
    feats = add_velo_drop_from_start(add_sequence_features(pitch_features(raw)))
    feats = feats.drop_nulls(subset=["run_value", "times_through_order", "cum_pitches_game", "velo_drop_from_start"])
    print("rows:", feats.height)

    max_tto = int(feats["times_through_order"].max())
    n = feats.height
    # design matrix: [intercept, tto2_dummy, tto3_dummy(if present), cum_pitches_game, velo_drop_from_start]
    tto = feats["times_through_order"].to_numpy()
    cols = [np.ones(n)]
    for level in range(2, max_tto + 1):
        cols.append((tto == level).astype(float))
    cols.append(feats["cum_pitches_game"].to_numpy().astype(float))
    cols.append(feats["velo_drop_from_start"].to_numpy().astype(float))
    x = np.column_stack(cols)
    y = feats["run_value"].to_numpy().astype(float)

    coef, *_ = np.linalg.lstsq(x, y, rcond=None)
    tto_penalty = [0.0] + [float(coef[i]) for i in range(1, max_tto)]
    print("tto_penalty =", tto_penalty)
    print("cum_pitches_game coef =", float(coef[max_tto]))
    print("velo_drop_from_start coef =", float(coef[max_tto + 1]))


if __name__ == "__main__":
    main()
