"""Record the nfl4th boosters' outputs on the oracle subsample (run locally, once; never in CI).

Needs `fd_model.ubj` and `wp_model.ubj` already in the sdv-py model cache (`<cache_dir>/models`); it
never downloads them. From the sdv-py repository root:

    PYTHONPATH=. uv run --frozen --all-extras python tests/nfl/fixtures/record_nfl4th_replay.py

Writes `tests/nfl/fixtures/nfl4th_booster_replay.parquet` and prints the two boosters' sha256 for the
README. The subsample is every 7th play of `nfl4th_2022_slice.parquet` (rows 0, 7, 14, ...).
"""

from __future__ import annotations

import hashlib

import polars as pl
import xgboost

import sportsdataverse
import sportsdataverse.nfl.nfl_fourth_down as fd
from sportsdataverse.nfl.ep_wp import _load_model, _model_cache_dir
from tests.nfl._booster_replay import MODELS, KeyedDMatrix, RecordingBooster, write_replay

SLICE = "tests/nfl/fixtures/nfl4th_2022_slice.parquet"
STRIDE = 7


def main() -> None:
    print("sportsdataverse from", sportsdataverse.__file__)
    for name in MODELS:
        path = _model_cache_dir() / name
        print(name, path, hashlib.sha256(path.read_bytes()).hexdigest())
    sub = pl.read_parquet(SLICE).gather_every(STRIDE)
    rows: list[dict] = []
    real = {name: _load_model(name) for name in MODELS}
    xgboost.DMatrix = KeyedDMatrix
    fd._try_load = lambda name: RecordingBooster(name, real[name], rows) if name in real else None
    out = fd.get_4th_down_probs(sub)
    table = write_replay(rows)
    print("plays", sub.height, "scored", len(out), table.group_by("model").len().sort("model").rows())


if __name__ == "__main__":
    main()
