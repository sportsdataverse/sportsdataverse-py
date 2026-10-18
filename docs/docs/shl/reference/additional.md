---
title: SHL — additional Python functions
sidebar_label: Additional functions
description: "SHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# SHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.hockey.shl` that are not
generated API-endpoint wrappers (those have their own reference pages).

## SHL site API

### shl_player_crosswalk {#shl_player_crosswalk}

`shl_player_crosswalk(season: 'int', game_type: 'str' = 'regular', *, return_as_pandas: 'bool' = False) -> 'Frame'`

isa player id -> athlete uuid for every player of an SHL season, from one stats-info call.

The SHL has three player id systems: the **isa** (StatNet) id that play-by-play, box scores
and stats tables key on, the platform **athlete uuid** that rosters and athlete routes key
on, and the old `ramses_legacy` CMS id (athlete-details only). The `players_summary`
table's `players{}` map is keyed by isa and carries each player's uuid, skaters and
goalies alike.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | season END year (2026 = 2025/26). |
| `game_type` | `str` | `'regular'` | regular, playoff, qualdown or playin. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per player: `player_id` (isa, `Int64`), `athlete_uuid` (null where SHL has none, common before about 2005), names, birth date, nationality, position, jersey number, height and weight.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | Layout-info lists no such ssgt, or the table has no rows. |
| `AssetFetchError` | The fetch failed or the table came back cut. |

**Example**

```python
from sportsdataverse.hockey.shl import shl_player_crosswalk
xw = shl_player_crosswalk(2026)
xw.filter(xw["player_id"] == 2427)
```

### shl_ssgt_uuid {#shl_ssgt_uuid}

`shl_ssgt_uuid(season: 'int', game_type: 'str' = 'regular') -> 'str'`

The SHL ssgt uuid of a season (END year) and game type, from `/statistics-v2/layout-info`.

Use it for the routes that take an ssgt uuid directly (`shl_standings`, `shl_sites`,
`shl_ssgt_provider`, `shl_post_game_team_stats`, `shl_player_h2h`). The directory is
fetched once per process.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | season END year (2026 = 2025/26). |
| `game_type` | `str` | `'regular'` | regular, playoff, qualdown or playin. |

**Returns**

the ssgt uuid (e.g. `"iuzqg7dqk9"` for the 2025/26 regular season).

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | Layout-info lists no ssgt for that season and game type. |
| `ValueError` | `season` is not a year. |
| `AssetFetchError` | Layout-info could not be fetched or read. |

**Example**

```python
from sportsdataverse.hockey.shl import shl_ssgt_uuid
shl_ssgt_uuid(2026)              # 2025/26 regular season
shl_ssgt_uuid(2025, "playoff")
```
