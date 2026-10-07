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

### soccer_open_dataset {#soccer_open_dataset}

`soccer_open_dataset(provider: 'str', match_id: 'Union[int, str]', **kwargs: 'Any') -> 'Any'`

Load one match of a provider's free open event data as a kloppy `EventDataset`.

`provider="statsbomb"` reads StatsBomb open data (https://github.com/statsbomb/open-data)
through `kloppy.statsbomb.load_open_data(match_id=...)`. That data is free for research and
non-commercial use only, under StatsBomb's open-data license -- read it before publishing
anything built on it. This is the input `soccer_spadl` expects;
`soccer_open_events` is the same load flattened to a frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `provider` | `str` |  | Open-data provider key; currently `"statsbomb"`. |
| `match_id` | `Union[int, str]` |  | The provider's match id (StatsBomb: e.g. `8658` -- France v Croatia, 2018 World Cup final). |

**Returns**

A kloppy `EventDataset`.

**Example**

```python
from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl
dataset = soccer_open_dataset("statsbomb", 8658)
actions = soccer_spadl(dataset)

# Pipeline next step (one line)

frame = dataset.to_df(engine="polars")
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

### soccer_spadl {#soccer_spadl}

`soccer_spadl(dataset: 'Any', *, game_id: 'Optional[Union[int, str]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Convert a kloppy event dataset to SPADL actions on the 105 x 68 pitch.

Every action attacks left to right (kloppy `ACTION_EXECUTING_TEAM` orientation), so a
frame from any provider kloppy reads is comparable. StatsBomb is the tested path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dataset` | `Any` |  | A kloppy `EventDataset` (e.g. from `soccer_open_dataset`). |
| `game_id` | `Optional[Union[int, str]]` | `None` | Game identifier when the dataset's metadata carries none. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per on-ball action with the SPADL columns (`type_name`, `result_name`, `bodypart_name`, start/end coordinates in meters, `time_seconds` from the period's kick-off). Empty dataset -> zero-row frame with the same schema.

**Example**

```python
from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl
actions = soccer_spadl(soccer_open_dataset("statsbomb", 8658))
print(actions.shape)

# Pipeline next step (one line)

actions.filter(pl.col("type_name") == "shot").group_by("team_id").len()
```
