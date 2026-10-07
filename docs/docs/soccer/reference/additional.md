---
title: SOCCER — additional Python functions
sidebar_label: Additional functions
description: "SOCCER — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# SOCCER — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.soccer`
not covered by the generated API-endpoint reference above.

## kloppy open event data

### soccer_events_to_frame {#soccer_events_to_frame}

`soccer_events_to_frame(dataset: 'Any', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Turn a kloppy dataset (any provider, any file) into a tidy frame.

The one place the package's column convention is applied to kloppy output: one row per
event, columns snake-cased (kloppy's own names -- `event_id`, `event_type`,
`period_id`, `timestamp`, `team_id`, `player_id`, `coordinates_x`,
`coordinates_y`, ... -- already are, so this is a no-op guard for any extra column).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dataset` | `Any` |  | A kloppy `EventDataset` (or any dataset with `to_df`), e.g. from `kloppy.statsbomb.load(event_data=..., lineup_data=...)` or `kloppy.opta.load(...)`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A polars DataFrame (pandas with `return_as_pandas=True`), one row per event. Coordinates are in the dataset's coordinate system -- kloppy's default is a 0-1 normalized pitch; pass `coordinates="statsbomb"` (etc.) to kloppy's loader to keep the provider's units.

**Example**

```python
from kloppy import statsbomb
from sportsdataverse.soccer import soccer_events_to_frame
ds = statsbomb.load(event_data="8658.json", lineup_data="lineups_8658.json")
df = soccer_events_to_frame(ds)
print(df.shape)

# Useful parameter combination

df_pd = soccer_events_to_frame(ds, return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("event_type") == "SHOT").select("player_id", "coordinates_x", "coordinates_y")
```

### soccer_open_events {#soccer_open_events}

`soccer_open_events(provider: 'str', match_id: 'Union[int, str]', *, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame]'`

Load one match of a provider's free open event data as a tidy frame.

`provider="statsbomb"` reads StatsBomb open data (https://github.com/statsbomb/open-data)
through `kloppy.statsbomb.load_open_data(match_id=...)`. That data is free for research and
non-commercial use only, under StatsBomb's open-data license -- read it before publishing
anything built on it. Other kloppy open samples (Metrica, SkillCorner) follow the same
shape and are added on request.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `provider` | `str` |  | Open-data provider key; currently `"statsbomb"`. |
| `match_id` | `Union[int, str]` |  | The provider's match id (StatsBomb: e.g. `8658` -- France v Croatia, 2018 World Cup final). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A polars DataFrame (pandas with `return_as_pandas=True`), one row per event; see `soccer_events_to_frame` for the columns.

**Example**

```python
from sportsdataverse.soccer import soccer_open_events
df = soccer_open_events("statsbomb", 8658)
print(df.shape)

# Useful parameter combination

df_pd = soccer_open_events("statsbomb", 8658, coordinates="statsbomb", return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("event_type") == "SHOT").group_by("team_id").len()
```
