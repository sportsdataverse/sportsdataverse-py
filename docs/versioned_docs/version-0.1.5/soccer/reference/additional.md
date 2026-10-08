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

### NotFittedError {#NotFittedError}

`NotFittedError(...)`

The grid is all zeros: call `fit` or load a model first.

### XThreat {#XThreat}

`XThreat(grid: 'Optional[np.ndarray]' = None, *, l: 'int' = 16, w: 'int' = 12, eps: 'float' = 1e-05, max_iter: 'int' = 1000, meta: 'Optional[dict[str, Any]]' = None) -> 'None'`

A fitted Expected Threat grid.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `grid` | `Optional[ndarray]` | `None` | An existing `(w, l)` array (row 0 = the top of the pitch); `None` for an unfitted model. |
| `l` | `int` | `16` | Cells along the pitch length. |
| `w` | `int` | `12` | Cells across the pitch width. |
| `eps` | `float` | `1e-05` | Convergence tolerance on the absolute change of every cell. |
| `max_iter` | `int` | `1000` | Iteration cap; exceeding it raises `RuntimeError`. |
| `meta` | `Optional[dict[str, Any]]` | `None` | Free-form provenance stored in the JSON. |

**Example**

```python
from sportsdataverse.soccer import XThreat, soccer_open_dataset, soccer_spadl
actions = soccer_spadl(soccer_open_dataset("statsbomb", 8658))
model = XThreat().fit(actions)
actions = actions.with_columns(model.rate(actions))
```

**Methods**

#### XThreat.fit

`XThreat.fit(actions: 'pl.DataFrame') -> 'XThreat'`

Fit the grid on SPADL actions by value iteration.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `actions` | `DataFrame` |  | SPADL actions with `type_name`, `result_name` and start/end coordinates. |

**Returns**

This model, fitted in place.

**Example**

```python
from sportsdataverse.soccer import XThreat, soccer_open_dataset, soccer_spadl
model = XThreat().fit(soccer_spadl(soccer_open_dataset("statsbomb", 8658)))
print(model.iterations)
```

#### XThreat.from_json

`XThreat.from_json(path: 'Union[str, Path]') -> 'XThreat'`

Read this module's format or socceraction's bare nested list.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `Union[str, Path]` |  | A JSON file written by `to_json` or socceraction's `save_model`. |

**Returns**

The loaded `XThreat`.

**Example**

```python
from sportsdataverse.soccer import XThreat
model = XThreat.from_json("xthreat.json")
```

#### XThreat.rate

`XThreat.rate(actions: 'pl.DataFrame') -> 'pl.Series'`

Rate each action: end-cell minus start-cell value for successful passes, dribbles and crosses.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `actions` | `DataFrame` |  | SPADL actions with `type_name`, `result_name` and start/end coordinates. |

**Returns**

A `Float64` series named `xt_value`; null for actions xT does not value.

**Example**

```python
from sportsdataverse.soccer import load_xthreat_model
actions = actions.with_columns(load_xthreat_model().rate(actions))
```

#### XThreat.to_json

`XThreat.to_json(path: 'Union[str, Path]') -> 'None'`

Write `{"xT": grid, "w": .., "l": .., "meta": {..}}` (readable by `from_json`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `Union[str, Path]` |  | Destination file. |

**Example**

```python
model.to_json("xthreat.json")
```

### load_xthreat_model {#load_xthreat_model}

`load_xthreat_model() -> 'XThreat'`

The bundled grid fit on StatsBomb open data (see `meta` for competitions, counts and license).

**Returns**

The fitted `XThreat` shipped with the package.

**Example**

```python
from sportsdataverse.soccer import load_xthreat_model
model = load_xthreat_model()
print(model.xT.shape, model.meta["matches"])
```

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

| col_name | type | description |
|---|---|---|
| `event_id` | character |  |
| `event_type` | character |  |
| `period_id` | integer |  |
| `timestamp` | character |  |
| `end_timestamp` | character |  |
| `ball_state` | character |  |
| `ball_owning_team` | character |  |
| `team_id` | character |  |
| `player_id` | character |  |
| `coordinates_x` | double |  |
| `coordinates_y` | double |  |
| `result` | character |  |
| `success` | logical |  |
| `end_coordinates_x` | double |  |
| `end_coordinates_y` | double |  |
| `receiver_player_id` | character |  |
| `set_piece_type` | character |  |
| `body_part_type` | character |  |
| `is_under_pressure` | logical |  |
| `pass_type` | character |  |
| `duel_type` | character |  |
| `goalkeeper_type` | character |  |
| `is_counter_attack` | logical |  |
| `card_type` | character |  |

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

| col_name | type | description |
|---|---|---|
| `event_id` | character |  |
| `event_type` | character |  |
| `period_id` | integer |  |
| `timestamp` | character |  |
| `end_timestamp` | character |  |
| `ball_state` | character |  |
| `ball_owning_team` | character |  |
| `team_id` | character |  |
| `player_id` | character |  |
| `coordinates_x` | double |  |
| `coordinates_y` | double |  |
| `result` | character |  |
| `success` | logical |  |
| `end_coordinates_x` | double |  |
| `end_coordinates_y` | double |  |
| `receiver_player_id` | character |  |
| `set_piece_type` | character |  |
| `body_part_type` | character |  |
| `is_under_pressure` | logical |  |
| `pass_type` | character |  |
| `duel_type` | character |  |
| `goalkeeper_type` | character |  |
| `is_counter_attack` | logical |  |
| `card_type` | character |  |

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

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `original_event_id` | character |  |
| `action_id` | integer |  |
| `period_id` | integer |  |
| `time_seconds` | double |  |
| `team_id` | character |  |
| `player_id` | character |  |
| `start_x` | double |  |
| `start_y` | double |  |
| `end_x` | double |  |
| `end_y` | double |  |
| `bodypart_id` | integer |  |
| `bodypart_name` | character |  |
| `type_id` | integer |  |
| `type_name` | character |  |
| `result_id` | integer |  |
| `result_name` | character |  |

**Example**

```python
from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl
actions = soccer_spadl(soccer_open_dataset("statsbomb", 8658))
print(actions.shape)

# Pipeline next step (one line)

actions.filter(pl.col("type_name") == "shot").group_by("team_id").len()
```

### soccer_xthreat_rate {#soccer_xthreat_rate}

`soccer_xthreat_rate(actions: 'pl.DataFrame', model: 'Optional[XThreat]' = None, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Append `xt_value` (Expected Threat added by each successful pass, dribble or cross) to a SPADL frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `actions` | `DataFrame` |  | SPADL actions from `soccer_spadl` (needs `type_name`, `result_name`, start/end coordinates). |
| `model` | `Optional[XThreat]` | `None` | A fitted `XThreat`; `None` uses the bundled grid. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`actions` with a `Float64` `xt_value` column (null for actions xT does not value).

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `original_event_id` | character |  |
| `action_id` | integer |  |
| `period_id` | integer |  |
| `time_seconds` | double |  |
| `team_id` | character |  |
| `player_id` | character |  |
| `start_x` | double |  |
| `start_y` | double |  |
| `end_x` | double |  |
| `end_y` | double |  |
| `bodypart_id` | integer |  |
| `bodypart_name` | character |  |
| `type_id` | integer |  |
| `type_name` | character |  |
| `result_id` | integer |  |
| `result_name` | character |  |
| `xt_value` | double |  |

**Example**

```python
import polars as pl
from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl, soccer_xthreat_rate
actions = soccer_xthreat_rate(soccer_spadl(soccer_open_dataset("statsbomb", 8658)))
print(actions.group_by("player_id").agg(pl.col("xt_value").sum()).sort("xt_value", descending=True).head())
```
