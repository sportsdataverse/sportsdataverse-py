"""Record / replay the two download-on-demand nfl4th boosters, so the fourth-down oracle runs in CI.

`fd_model.ubj` (~73 MB) and `wp_model.ubj` (~7.6 MB) are not bundled and CI never downloads them. Their
raw outputs on the committed oracle subsample are recorded once, locally, by
`tests/nfl/fixtures/record_nfl4th_replay.py`, keyed by the exact float32 bytes of each feature row the
booster saw. In CI a replay booster serves those outputs through `nfl_fourth_down._try_load`, so every
line of sdv-py's own fourth-down code downstream of the two boosters (go / fg / punt assembly, the 2-pt
branch, the clamps, the spread sign, the recommendation) is checked against nfl4th's published values.

`xgboost.DMatrix` is swapped for a subclass that keeps the array it was built from (the booster call
sites import `DMatrix` at call time); it is otherwise the real class, so the bundled boosters (EP / WP /
2-pt / FG) keep scoring normally. A row the recording never saw fails the test: a change that alters
the boosters' inputs cannot pass silently. A float32 key can differ by an ulp across platforms (the WP
features go through `np.exp`), so a miss falls back to the nearest recorded row of the same width
within 1e-4 per feature, and fails beyond that.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import xgboost

REPLAY = Path(__file__).resolve().parent / "fixtures" / "nfl4th_booster_replay.parquet"
MODELS = ("fd_model.ubj", "wp_model.ubj")
_REAL_DMATRIX = xgboost.DMatrix


class KeyedDMatrix(_REAL_DMATRIX):
    """The real `DMatrix`, plus the float32 array it was built from (`_sdv_x`)."""

    def __init__(self, data, *args, **kwargs):
        super().__init__(data, *args, **kwargs)
        x = np.asarray(data, dtype=np.float32)
        self._sdv_x = np.where(np.isnan(x), np.float32(np.nan), x).astype(np.float32)


def _keys(x: np.ndarray) -> list[bytes]:
    return [row.tobytes() for row in np.ascontiguousarray(x, dtype=np.float32)]


class RecordingBooster:
    """Wraps a real booster and records (model, feature row, prediction) for every row it scores."""

    def __init__(self, name: str, booster, sink: list[dict]):
        self.name, self.booster, self.sink = name, booster, sink

    def predict(self, dm, *args, **kwargs):
        pred = np.asarray(self.booster.predict(dm, *args, **kwargs), dtype=np.float32)
        x = dm._sdv_x
        per_row = pred.reshape(x.shape[0], -1)
        for key, row, p in zip(_keys(x), x, per_row):
            self.sink.append({"model": self.name, "key": key, "x": row.tolist(), "pred": p.tolist()})
        return pred


class ReplayBooster:
    """Serves recorded predictions by feature-row key; a row it never saw raises."""

    def __init__(self, name: str, table: pl.DataFrame):
        sub = table.filter(pl.col("model") == name)
        self.name = name
        self.lookup = dict(zip(sub["key"].to_list(), sub["pred"].to_list()))
        self.x = np.array(sub["x"].to_list(), dtype=np.float64)
        self.preds = sub["pred"].to_list()
        self.misses_served = 0

    def _nearest(self, row: np.ndarray):
        a, b = self.x, row.astype(np.float64)[None, :]
        both_nan = np.isnan(a) & np.isnan(b)
        diff = np.where(both_nan, 0.0, np.abs(a - b))
        diff = np.where(np.isnan(diff), np.inf, diff).max(axis=1)
        i = int(np.argmin(diff))
        if diff[i] > 1e-4:
            raise AssertionError(f"{self.name}: feature row {row.tolist()} was never recorded (nearest {diff[i]:.3g})")
        self.misses_served += 1
        return self.preds[i]

    def predict(self, dm, *args, **kwargs):
        x = dm._sdv_x
        out = [self.lookup[k] if k in self.lookup else self._nearest(r) for k, r in zip(_keys(x), x)]
        arr = np.asarray(out, dtype=np.float32)
        return arr[:, 0] if arr.shape[1] == 1 else arr.reshape(-1)


def write_replay(rows: list[dict], path: Path = REPLAY) -> pl.DataFrame:
    table = (
        pl.DataFrame(
            rows, schema={"model": pl.Utf8, "key": pl.Binary, "x": pl.List(pl.Float32), "pred": pl.List(pl.Float32)}
        )
        .unique(["model", "key"], keep="first", maintain_order=True)
        .sort("model", "key")
    )
    table.write_parquet(path, compression="zstd", compression_level=19)
    return table
