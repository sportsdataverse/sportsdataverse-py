---
title: IBBA — additional Python functions
sidebar_label: Additional functions
description: "IBBA — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# IBBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.basketball.ibba`
not covered by the generated API-endpoint reference above.

## IDs and crosswalks

### ibba_segev_game_crosswalk {#ibba_segev_game_crosswalk}

`ibba_segev_game_crosswalk(df: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'Any'`

IBBA `game_id` <-> Segev `game_id` from `ibba_games` (the `segev_game_id` column).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `polars.DataFrame` |  | an `ibba_games` frame (2025-26 on; earlier rows and external-team news links carry no Segev id). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: `game_id` (IBBA) and `segev_game_id` (the `game_id` every `ibba_segev_*` wrapper takes), both `Int64`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | IBBA game id (the game_id of the ibba_games frame). |
| `segev_game_id` | integer | Segev game id of the same game (the game_id every ibba_segev_* wrapper takes). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | a missing column, or a key that maps to two ids. |
| `TypeError` | `df` is not polars, or an id column has the wrong dtype. |

**Example**

```python
from sportsdataverse.basketball.basketball_crosswalk import ibba_segev_game_crosswalk
from sportsdataverse.basketball.ibba import ibba_games

xw = ibba_segev_game_crosswalk(ibba_games(season=2026))
```
