---
title: MCH — additional Python functions
sidebar_label: Additional functions
description: "MCH — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# MCH — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mch`
not covered by the generated API-endpoint reference above.

## Models and calculators

### mch_ratings {#mch_ratings}

`mch_ratings(dates: 'list[str]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

MCH opponent-adjusted goal-margin ratings over a set of scoreboard dates.

Fetches `espn_mch_scoreboard` for each date in `dates`, concatenates
the completed games, and adjusts with
`sportsdataverse.hockey.college_hockey_ratings.college_hockey_ratings`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `list[str]` |  | `YYYYMMDD` date strings to fetch (ESPN has no single "whole season" scoreboard endpoint; the caller supplies the date sweep -- see `dev/league_ports/capture_wch_and_scoreboards.py` for the sweep used to build the committed oracle fixture). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

One row per team: `team_id, adj_off, adj_def, adj_net, raw_off, raw_def, games`.

**Example**

```python
from sportsdataverse.hockey.mch import mch_ratings
ratings = mch_ratings(["20250118", "20250201"])
ratings.sort("adj_net", descending=True).head()
```
