<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Football fixtures](#football-fixtures)
  - [`nflverse_third_down_2022_2023.csv`](#nflverse_third_down_2022_2023csv)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Football fixtures

The `cfb_*` / `nfl_2025_*` parquets are described in the docstrings of the tests that read them
(`grep -rn <file name> tests/football`).

## `nflverse_third_down_2022_2023.csv`

Read by `tests/football/test_usage_box.py::test_nfl_third_down_curve_level_vs_nflverse` (C-L38): the
external level oracle for the bundled `sportsdataverse/nfl/models/nfl_third_down_conversion.parquet`.

- **Source.** nflverse-data release `pbp`, assets
  `https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2022.parquet` and
  `.../play_by_play_2023.parquet`, read with `sportsdataverse.nfl.load_nfl_pbp([2022, 2023])` on 2026-10-11.
- **Aggregation** (50 rows: `season, distance, attempts, converted`):

```python
import polars as pl
from sportsdataverse.nfl import load_nfl_pbp
t = load_nfl_pbp([2022, 2023]).filter(
    (pl.col("down") == 3) & pl.col("play_type").is_in(["pass", "run"]) & (pl.col("ydstogo") >= 1)
).with_columns(pl.col("ydstogo").clip(1, 25).cast(pl.Int64).alias("distance"))
(t.group_by("season", "distance")
  .agg(pl.len().alias("attempts"), pl.col("third_down_converted").sum().cast(pl.Int64).alias("converted"))
  .sort("season", "distance").write_csv("tests/football/fixtures/nflverse_third_down_2022_2023.csv"))
```
