---
title: F1 — additional Python functions
sidebar_label: Additional functions
description: "F1 — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# F1 — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.f1`
not covered by the generated API-endpoint reference above.

## Other

### f1_laps {#f1_laps}

`f1_laps(season: 'Union[int, str]', round: 'Union[int, str]', *, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame]'`

Lap times of one race: one row per driver per lap (1996+), all pages.

Endpoint: `GET https://api.jolpi.ca/ergast/f1/{season}/{round}/laps.json`,
requested with `limit=100` and `offset` advanced page by page until
`MRData.total` timings have been read.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Union[int, str]` |  | Four-digit season year, or `current`. |
| `round` | `Union[int, str]` |  | Round number within the season, or `last` / `next`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame with the `laps_page` columns (`season`, `round`, `race_*`, `lap_number`, `driver_id`, `position`, `time`), concatenated across pages; zero rows when the race has no lap data (before 1996, or a round not yet run).

**Example**

```python
from sportsdataverse.f1 import f1_laps

df = f1_laps(2024, 1)
print(df.shape)  # (1129, 17) -- 12 requests

# Pipeline next step (one line)

import polars as pl

df.filter(pl.col("driver_id") == "max_verstappen").select("lap_number", "position", "time")
```
