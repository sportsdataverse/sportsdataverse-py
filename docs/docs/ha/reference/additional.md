---
title: HA — additional Python functions
sidebar_label: Additional functions
description: "HA — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# HA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.hockey.ha` that are not
generated API-endpoint wrappers (those have their own reference pages).

## HockeyAllsvenskan site API

### ha_player_crosswalk {#ha_player_crosswalk}

`ha_player_crosswalk(players: 'Union[pl.DataFrame, Iterable[Any]]', season: 'Optional[int]' = None, game_type: 'str' = 'regular', *, shl_crosswalk: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Frame'`

HockeyAllsvenskan `player_statnet_id` -> the SHL isa id and athlete uuid of the same player.

The HA and the SHL share the StatNet player id, so `shl_player_id` always equals
`player_statnet_id` where the SHL has the player; the lookup adds the SHL athlete uuid and
confirms the player appears in that SHL season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `Union[DataFrame, Iterable[Any]]` |  | an HA frame with a `player_statnet_id` column (leaderboard, play-by-play, player directory), or an iterable of StatNet player ids. |
| `season` | `Optional[int]` | `None` | SHL season END year (2026 = 2025/26) to look the players up in; required unless `shl_crosswalk` is given. |
| `game_type` | `str` | `'regular'` | SHL game type: regular, playoff, qualdown or playin. |
| `shl_crosswalk` | `Optional[DataFrame]` | `None` | an already fetched `~sportsdataverse.hockey.shl.shl_player_crosswalk` frame, to skip the request. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per distinct player id: `player_statnet_id` and `shl_player_id` (`Int64`; null when the SHL season has no such player), `athlete_uuid`, `full_name` and `date_of_birth` from the SHL.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | Neither `season` nor `shl_crosswalk` was given, a frame has no `player_statnet_id` column, or an id is not an integer. |
| `sportsdataverse.errors.NoDataError` | The SHL has no such season (from the SHL lookup). |
| `sportsdataverse.errors.AssetFetchError` | The SHL fetch failed. |

**Example**

```python
from sportsdataverse.hockey.ha import ha_leaders, ha_player_crosswalk
ha_player_crosswalk(ha_leaders(), season=2026)
```
