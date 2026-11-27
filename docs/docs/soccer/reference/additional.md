---
title: Soccer (all) — additional Python functions
sidebar_label: Additional functions
description: "Soccer (all) — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# Soccer (all) — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.soccer` that are not
generated API-endpoint wrappers (those have their own reference pages).

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

**Raises**

| Exception | Description |
|---|---|
| `RuntimeError` | If the grid has not converged within `max_iter` iterations. |

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

**Raises**

| Exception | Description |
|---|---|
| `NotFittedError` | If the grid is all zeros. |

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
| `event_id` | String |  |
| `event_type` | String |  |
| `period_id` | Int64 |  |
| `timestamp` | String |  |
| `end_timestamp` | String |  |
| `ball_state` | String |  |
| `ball_owning_team` | String |  |
| `team_id` | String |  |
| `player_id` | String |  |
| `coordinates_x` | Float64 |  |
| `coordinates_y` | Float64 |  |
| `result` | String |  |
| `success` | Boolean |  |
| `end_coordinates_x` | Float64 |  |
| `end_coordinates_y` | Float64 |  |
| `receiver_player_id` | String |  |
| `set_piece_type` | String |  |
| `body_part_type` | String |  |
| `is_under_pressure` | Boolean |  |
| `pass_type` | String |  |
| `duel_type` | String |  |
| `goalkeeper_type` | String |  |
| `is_counter_attack` | Boolean |  |
| `card_type` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `AttributeError` | `dataset` has no `to_df` (it is not a kloppy dataset). |

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

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | Kloppy is not installed -- `pip install "sportsdataverse[soccer]"`. |
| `ValueError` | `provider` is not a supported open-data key. |
| `AssetFetchError` | The events file arrived truncated or corrupt twice (the retry evicts kloppy's cached copy first). |

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
| `event_id` | String |  |
| `event_type` | String |  |
| `period_id` | Int64 |  |
| `timestamp` | String |  |
| `end_timestamp` | String |  |
| `ball_state` | String |  |
| `ball_owning_team` | String |  |
| `team_id` | String |  |
| `player_id` | String |  |
| `coordinates_x` | Float64 |  |
| `coordinates_y` | Float64 |  |
| `result` | String |  |
| `success` | Boolean |  |
| `end_coordinates_x` | Float64 |  |
| `end_coordinates_y` | Float64 |  |
| `receiver_player_id` | String |  |
| `set_piece_type` | String |  |
| `body_part_type` | String |  |
| `is_under_pressure` | Boolean |  |
| `pass_type` | String |  |
| `duel_type` | String |  |
| `goalkeeper_type` | String |  |
| `is_counter_attack` | Boolean |  |
| `card_type` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | Kloppy is not installed -- `pip install "sportsdataverse[soccer]"`. |
| `ValueError` | `provider` is not a supported open-data key. |
| `AssetFetchError` | The events file arrived truncated or corrupt twice (the retry evicts kloppy's cached copy first). |

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
| `game_id` | String |  |
| `original_event_id` | String |  |
| `action_id` | Int64 |  |
| `period_id` | Int64 |  |
| `time_seconds` | Float64 |  |
| `team_id` | String |  |
| `player_id` | String |  |
| `start_x` | Float64 |  |
| `start_y` | Float64 |  |
| `end_x` | Float64 |  |
| `end_y` | Float64 |  |
| `bodypart_id` | Int64 |  |
| `bodypart_name` | String |  |
| `type_id` | Int64 |  |
| `type_name` | String |  |
| `result_id` | Int64 |  |
| `result_name` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | Kloppy is missing (`pip install "sportsdataverse[soccer]"`). |
| `ValueError` | Neither the dataset nor `game_id` names the game; or the dataset's provider has no usable coordinate system (unknown / `OTHER`) or its pitch dimensions are not fixed. |

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
| `game_id` | String |  |
| `original_event_id` | String |  |
| `action_id` | Int64 |  |
| `period_id` | Int64 |  |
| `time_seconds` | Float64 |  |
| `team_id` | String |  |
| `player_id` | String |  |
| `start_x` | Float64 |  |
| `start_y` | Float64 |  |
| `end_x` | Float64 |  |
| `end_y` | Float64 |  |
| `bodypart_id` | Int64 |  |
| `bodypart_name` | String |  |
| `type_id` | Int64 |  |
| `type_name` | String |  |
| `result_id` | Int64 |  |
| `result_name` | String |  |
| `xt_value` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `NotFittedError` | If `model` is an unfitted grid. |

**Example**

```python
import polars as pl
from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl, soccer_xthreat_rate
actions = soccer_xthreat_rate(soccer_spadl(soccer_open_dataset("statsbomb", 8658)))
print(actions.group_by("player_id").agg(pl.col("xt_value").sum()).sort("xt_value", descending=True).head())
```

## reep football id register

### soccer_espn_reep_team_crosswalk {#soccer_espn_reep_team_crosswalk}

`soccer_espn_reep_team_crosswalk(league: 'str | None' = None, *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

ESPN soccer team id -> reep team id, with reep's ids for the same club at about 20 providers.

One row per ESPN team in the leagues the table covers: `eng.1`, `eng.2`, `esp.1`, `ger.1`,
`ita.1`, `fra.1`, `usa.1` and `mex.1` (the clubs ESPN listed when the table was built,
2026-11-26). The table ships with sdv-py; no request is made. reep data is CC0 1.0 (release
2026.25). NWSL is not covered: reep has no row for 5 of its 16 clubs, below the 75% per-league floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str \| None` | `None` | An ESPN league slug from the list above, to keep that league's rows; `None` (default) returns every league. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per `espn_team_id` (unique). Every column is text (Utf8): `espn_team_id` joins `espn_soccer_teams_site().team_id` and the ESPN soccer scoreboard and summary team ids as it is. The reep columns are null when no reep row is that club (`match_method == "unmatched"`).

| col_name | type | description |
|---|---|---|
| `espn_team_id` | String | ESPN soccer team id, digits as text (Utf8, the dtype of team_id in espn_soccer_teams_site and the ESPN soccer scoreboard and summary frames); unique in the table. |
| `espn_league` | String | ESPN league slug the team was listed under when the table was built (eng.1, eng.2, esp.1, ger.1, ita.1, fra.1, usa.1, mex.1). |
| `espn_display_name` | String | ESPN display name of the team at build time (e.g. 'Manchester United'). |
| `espn_abbreviation` | String | ESPN team abbreviation at build time (e.g. 'MAN' for Manchester United). |
| `reep_id` | String | reep team id (reep_t + 8 hex characters); null when no reep row is this club. No reep_id appears on two ESPN teams. |
| `reep_name` | String | Club name as reep records it (e.g. 'Manchester United F.C.'). |
| `reep_country` | String | Country reep records for the club ('United Kingdom' for English clubs, 'Wales' for some Welsh ones). |
| `key_wikidata` | String | Wikidata entity id of the club (e.g. 'Q18656'). |
| `key_transfermarkt` | String | Transfermarkt club id, digits as text (e.g. '985'). |
| `key_fbref` | String | FBref squad id (8 hex characters). |
| `key_soccerway` | String | Soccerway's id for the club as reep records it (text); null when reep has none. |
| `key_opta` | String | Opta team UUID (25 alphanumeric characters). |
| `key_opta_numeric` | String | Opta numeric team id. |
| `key_kicker` | String | kicker.de club slug. |
| `key_flashscore` | String | Flashscore's id for the club as reep records it (text); null when reep has none. |
| `key_sofascore` | String | Sofascore's id for the club as reep records it (text); null when reep has none. |
| `key_soccerbase` | String | Soccerbase's id for the club as reep records it (text); null when reep has none. |
| `key_uefa` | String | UEFA's id for the club as reep records it (text); null when reep has none. |
| `key_footballdatabase_eu` | String | footballdatabase.eu club id. |
| `key_worldfootball` | String | worldfootball.net club slug. |
| `key_playmakerstats` | String | playmakerstats.com team id. |
| `key_clubelo` | String | ClubElo club name (the key its ratings use, e.g. 'ManUnited'). |
| `key_sportmonks` | String | Sportmonks's id for the club as reep records it (text); null when reep has none. |
| `key_api_football` | String | API-Football team id. |
| `key_sofifa` | String | SoFIFA's id for the club as reep records it (text); null when reep has none. |
| `key_fotmob` | String | FotMob's id for the club as reep records it (text); null when reep has none. |
| `key_thesportsdb` | String | TheSportsDB's id for the club as reep records it (text); null when reep has none. |
| `key_understat` | String | Understat's id for the club as reep records it (text); null when reep has none. |
| `key_capology` | String | Capology club slug. |
| `match_method` | String | How the ESPN team was tied to reep. 'key_espn' (reep already carries this ESPN id), 'name' (exactly one reep row in the league's country shares a normalized name or alias), 'review' (settled by hand in tools/soccer_espn_reep_overrides.yaml), 'unmatched' (reviewed; no reep row is this club, reep columns null). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If *league* is not one of the covered league slugs. |

**Example**

```python
from sportsdataverse.soccer import soccer_espn_reep_team_crosswalk
xw = soccer_espn_reep_team_crosswalk()
print(xw.shape)

# One league, as pandas

epl = soccer_espn_reep_team_crosswalk(league="eng.1", return_as_pandas=True)

# Pipeline next step (one line)

xw.select("espn_team_id", "key_transfermarkt", "key_fbref").drop_nulls("key_transfermarkt")
```

## Transfermarkt dataset (CC0)

### load_soccer_transfermarkt_clubs {#load_soccer_transfermarkt_clubs}

`load_soccer_transfermarkt_clubs(*, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load the CC0 Transfermarkt clubs table: one row per club of the first-tier leagues the dataset covers.

About 800 clubs, each with its league, squad profile and stadium as of its last season in the dataset.
A snapshot current to 2026-07-06 (not refreshed), read on every call from the release mirror.
Licence: CC0 1.0.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per `club_id` (Int64), the id `soccer_espn_transfermarkt_team_crosswalk` maps ESPN teams to. A column holding a value of another type comes back as text with a `SchemaDriftWarning`.

| col_name | type | description |
|---|---|---|
| `club_id` | Int64 | Transfermarkt club id (Int64); the club_id of soccer_espn_transfermarkt_team_crosswalk. |
| `club_code` | String | Transfermarkt URL slug of the club (e.g. 'fc-barcelona'). |
| `name` | String | Club name as Transfermarkt writes it (e.g. 'FC Barcelona'). |
| `domestic_competition_id` | String | Transfermarkt id of the club's domestic league (e.g. 'ES1'); joins competition_id of load_soccer_transfermarkt_competitions. |
| `squad_size` | Int64 | Players in the club's squad in its last season in the dataset. |
| `average_age` | Float64 | Average age of that squad, in years. |
| `foreigners_number` | Int64 | Players in that squad without the club's nationality. |
| `foreigners_percentage` | Float64 | foreigners_number as a percentage of squad_size; null when Transfermarkt shows none. |
| `national_team_players` | Int64 | Players in that squad capped by a senior national team. |
| `stadium_name` | String | Name of the club's home stadium. |
| `stadium_seats` | Int64 | Seating capacity of that stadium. |
| `net_transfer_record` | String | Net transfer balance as Transfermarkt displays it, text with currency and unit (e.g. '+€5.90m', '€-25.00m'). |
| `coach_name` | String | Head coach Transfermarkt lists; null for about half the clubs. |
| `last_season` | Int64 | Start year of the club's last season in a covered competition (2025 = 2025-26). |
| `url` | String | Transfermarkt page of the club. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The original source answered 400 / 422. |
| `NoDataError` | The original source answered 404 (the file moved). |
| `AssetFetchError` | The original source, read when the release mirror cannot be (with a warning), failed (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or lacks a column this loader types. |

**Example**

```python
from sportsdataverse.soccer import load_soccer_transfermarkt_clubs
clubs = load_soccer_transfermarkt_clubs()

# ESPN Premier League teams with their Transfermarkt club rows

from sportsdataverse.soccer import soccer_espn_transfermarkt_team_crosswalk
soccer_espn_transfermarkt_team_crosswalk(league="eng.1").join(clubs, on="club_id", how="left")
```

### load_soccer_transfermarkt_competitions {#load_soccer_transfermarkt_competitions}

`load_soccer_transfermarkt_competitions(*, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load the CC0 Transfermarkt competitions table: the leagues and cups the dataset covers.

65 competitions: first-tier domestic leagues, domestic cups and international club competitions, read
on every call from the release mirror. Licence: CC0 1.0.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per `competition_id` (text, e.g. `"GB1"`), the id the clubs and players tables carry as `domestic_competition_id` / `current_club_domestic_competition_id`.

| col_name | type | description |
|---|---|---|
| `competition_id` | String | Transfermarkt competition id (e.g. 'GB1', 'CL'). |
| `competition_code` | String | Transfermarkt URL slug of the competition (e.g. 'premier-league'). |
| `name` | String | Competition name as its slug (e.g. 'premier-league'). |
| `sub_type` | String | Kind of competition (e.g. 'first_tier', 'domestic_cup', 'uefa_champions_league', 'world_cup'). |
| `type` | String | Competition group - 'domestic_league', 'domestic_cup', 'international_cup', 'national_team_competition' or 'other'. |
| `country_id` | Int64 | Transfermarkt country id (Int64); -1 for international competitions. |
| `country_name` | String | Country name; null for international competitions. |
| `domestic_league_code` | String | Id of the country's first-tier league; null for international competitions. |
| `confederation` | String | Confederation as Transfermarkt names it ('europa', 'amerika', 'asien', 'afrika', 'fifa'). |
| `total_clubs` | Int64 | Clubs in the competition; null for some cups and national-team competitions. |
| `url` | String | Transfermarkt page of the competition. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The original source answered 400 / 422. |
| `NoDataError` | The original source answered 404 (the file moved). |
| `AssetFetchError` | The original source, read when the release mirror cannot be (with a warning), failed (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or lacks a column this loader types. |

**Example**

```python
from sportsdataverse.soccer import load_soccer_transfermarkt_competitions
comps = load_soccer_transfermarkt_competitions()

# First-tier leagues only

import polars as pl
comps.filter(pl.col("sub_type") == "first_tier")
```

### load_soccer_transfermarkt_player_valuations {#load_soccer_transfermarkt_player_valuations}

`load_soccer_transfermarkt_player_valuations(*, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load the CC0 Transfermarkt market-value history: one row per player and valuation date.

About 656,000 valuations, from 2000 to 2026-06-12 (the snapshot is not refreshed), read on every call from
the release mirror. Licence: CC0 1.0.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per (`player_id`, `date`); `player_id` and `current_club_id` are Int64 and join the players and clubs tables. A column holding a value of another type comes back as text with a `SchemaDriftWarning`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Transfermarkt player id (Int64); joins load_soccer_transfermarkt_players. |
| `date` | Date | Date of the valuation (2000-01-20 to 2026-06-12 in the snapshot). |
| `market_value_in_eur` | Int64 | Market value Transfermarkt assigned on that date, in euros. |
| `current_club_name` | String | Club the player was at on that date ('Unknown' when Transfermarkt does not say). |
| `current_club_id` | Int64 | Transfermarkt id of that club (Int64). |
| `player_club_domestic_competition_id` | String | Domestic league id of that club (e.g. 'BE1'); null for clubs outside the covered leagues. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The original source answered 400 / 422. |
| `NoDataError` | The original source answered 404 (the file moved). |
| `AssetFetchError` | The original source, read when the release mirror cannot be (with a warning), failed (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or lacks a column this loader types. |

**Example**

```python
from sportsdataverse.soccer import load_soccer_transfermarkt_player_valuations
values = load_soccer_transfermarkt_player_valuations()

# One player's value curve

import polars as pl
values.filter(pl.col("player_id") == 418560).sort("date")
```

### load_soccer_transfermarkt_players {#load_soccer_transfermarkt_players}

`load_soccer_transfermarkt_players(*, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load the CC0 Transfermarkt players table: one row per player, with current club and market value.

About 50,000 players: everyone who appeared for a club of the first-tier leagues the dataset covers
(Premier League, LaLiga, Bundesliga, Serie A, Ligue 1, MLS, Liga MX and about 20 more). A snapshot
current to 2026-07-06 (not refreshed), read on every call from the release mirror. Licence: CC0 1.0.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per `player_id` (Int64). `current_club_id` (Int64) joins `club_id` of `load_soccer_transfermarkt_clubs` and of `soccer_espn_transfermarkt_team_crosswalk`. A column holding a value of another type comes back as text with a `SchemaDriftWarning`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Transfermarkt player id (Int64), unique in the table. |
| `first_name` | String | Given name; null when Transfermarkt shows one name only. |
| `last_name` | String | Family name (or the single name). |
| `name` | String | Full name as Transfermarkt shows it. |
| `last_season` | Int64 | Start year of the last season the player appeared in a covered competition (2025 = 2025-26). |
| `current_club_id` | Int64 | Transfermarkt id of the player's club at the snapshot (Int64); joins club_id. |
| `player_code` | String | Transfermarkt URL slug of the player. |
| `country_of_birth` | String | Country of birth. |
| `city_of_birth` | String | City of birth, as Transfermarkt writes it (may carry trailing non-breaking spaces). |
| `country_of_citizenship` | String | First citizenship Transfermarkt lists. |
| `date_of_birth` | Date | The player's date of birth as Transfermarkt records it. |
| `sub_position` | String | Detailed position (e.g. 'Centre-Forward', 'Left-Back'). |
| `position` | String | Position group - 'Goalkeeper', 'Defender', 'Midfield', 'Attack' or 'Missing'. |
| `foot` | String | Preferred foot - 'right', 'left' or 'both'; null when unknown. |
| `height_in_cm` | Int64 | Height in centimetres. |
| `contract_expiration_date` | Date | Date the player's current contract ends; null when unknown. |
| `agent_name` | String | Player's agency as Transfermarkt lists it. |
| `image_url` | String | URL of the player's Transfermarkt portrait. |
| `international_caps` | Int64 | Senior caps for current_national_team_id; null when uncapped. |
| `international_goals` | Int64 | Senior international goals; null when uncapped. |
| `current_national_team_id` | Int64 | Transfermarkt id of the national team the player represents (Int64); null when none. |
| `url` | String | Transfermarkt page of the player. |
| `current_club_domestic_competition_id` | String | Domestic league id of current_club_id (e.g. 'GB1'). |
| `current_club_name` | String | Name of current_club_id as Transfermarkt writes it (often the legal name, e.g. 'Società Sportiva Lazio S.p.A.'). |
| `market_value_in_eur` | Int64 | Latest Transfermarkt market value, in euros; null when none is listed. |
| `highest_market_value_in_eur` | Int64 | Highest market value Transfermarkt has recorded for the player, in euros. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The original source answered 400 / 422. |
| `NoDataError` | The original source answered 404 (the file moved). |
| `AssetFetchError` | The original source, read when the release mirror cannot be (with a warning), failed (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or lacks a column this loader types. |

**Example**

```python
from sportsdataverse.soccer import load_soccer_transfermarkt_players
players = load_soccer_transfermarkt_players()
print(players.shape)

# Most valuable players

players.sort("market_value_in_eur", descending=True, nulls_last=True).head(10)
```

### load_soccer_transfermarkt_transfers {#load_soccer_transfermarkt_transfers}

`load_soccer_transfermarkt_transfers(*, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load the CC0 Transfermarkt transfers table: one row per player move between clubs, with its fee.

About 175,000 transfers involving the clubs the dataset covers, to the 2025-26 season (the snapshot is
current to 2026-07-06 and not refreshed), read on every call from the release mirror. Licence: CC0 1.0.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per transfer. `player_id`, `from_club_id` and `to_club_id` are Int64 Transfermarkt ids. A column holding a value of another type comes back as text with a `SchemaDriftWarning`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Transfermarkt player id (Int64). |
| `transfer_date` | Date | Date the move takes effect; a pre-agreed future move can be dated years ahead. |
| `transfer_season` | String | Season of the move as Transfermarkt writes it (e.g. '25/26'). |
| `from_club_id` | Int64 | Transfermarkt id of the selling club (Int64). |
| `to_club_id` | Int64 | Transfermarkt id of the buying club (Int64). |
| `from_club_name` | String | Name of the selling club. |
| `to_club_name` | String | Name of the buying club. |
| `transfer_fee` | Float64 | Fee in euros; 0 for a free transfer, null when Transfermarkt shows no fee (undisclosed or unknown). |
| `market_value_in_eur` | Float64 | Player's Transfermarkt market value at the time of the move, in euros; Float64 because the file writes it with decimals, e.g. '700000.000'). |
| `player_name` | String | Name of the player. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The original source answered 400 / 422. |
| `NoDataError` | The original source answered 404 (the file moved). |
| `AssetFetchError` | The original source, read when the release mirror cannot be (with a warning), failed (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or lacks a column this loader types. |

**Example**

```python
from sportsdataverse.soccer import load_soccer_transfermarkt_transfers
moves = load_soccer_transfermarkt_transfers()

# The largest fees of one season

import polars as pl
moves.filter(pl.col("transfer_season") == "25/26").sort("transfer_fee", descending=True, nulls_last=True)
```

### soccer_espn_transfermarkt_player_crosswalk {#soccer_espn_transfermarkt_player_crosswalk}

`soccer_espn_transfermarkt_player_crosswalk(league: 'str | None' = None, *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

ESPN soccer athlete id -> Transfermarkt `player_id`, for the first-tier leagues' current ESPN rosters.

One row per athlete on the ESPN rosters (fetched 2026-11-27) of the eng.1, esp.1, ger.1, ita.1, fra.1, usa.1 and
mex.1 teams whose Transfermarkt club is in the dataset: 4,346 athletes, 3,661 resolved (84%). The table ships
with sdv-py; no request is made. An athlete is resolved by birth date and name against every Transfermarkt
player: exactly one player with the same birth date and a shared name token, or, among several, the one with the
athlete's citizenship. A 1 January birth date is too common a stand-in to count alone, so such an athlete is
resolved only when exactly one same-date, same-name player also plays for the team's Transfermarkt club (or
moved to it from 2025-07-01) or shares the citizenship. No player is matched to two athletes, and where the reep
register carries both ids they agree on 99% of 2,478 pairs. The Transfermarkt data ends in July 2026, so a
player who joined later can resolve with his previous club.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str \| None` | `None` | One of the seven ESPN league slugs above, to keep that league's rows; `None` (default) returns all. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per `espn_athlete_id` (Utf8, unique). `player_id` (Int64) joins `load_soccer_transfermarkt_players().player_id` and `load_soccer_transfermarkt_player_valuations()`; it is null, with `match_method == "unmatched"`, when no rule resolved the athlete.

| col_name | type | description |
|---|---|---|
| `espn_athlete_id` | String | ESPN soccer athlete id, digits as text (Utf8, as the ESPN soccer roster frames carry it); unique in the table. |
| `espn_team_id` | String | ESPN team whose roster listed the athlete on 2026-11-27 (Utf8). |
| `espn_league` | String | ESPN league slug of that team (eng.1, esp.1, ger.1, ita.1, fra.1, usa.1, mex.1). |
| `espn_date_of_birth` | Date | Birth date ESPN gives for the athlete; null when ESPN gives none (the athlete is then unmatched). |
| `player_id` | Int64 | Transfermarkt player id (Int64); joins player_id of load_soccer_transfermarkt_players and load_soccer_transfermarkt_player_valuations. Null when unmatched; never on two athletes. |
| `tm_name` | String | Transfermarkt name of the matched player; null when unmatched. |
| `match_method` | String | How the athlete was resolved. 'name_dob' (the only Transfermarkt player with the same birth date and a shared name token), 'name_dob_nationality' (several such players, one with the athlete's citizenship), 'name_jan1_club' / 'name_jan1_nationality' (a 1 January birth date, resolved only because exactly one such player is also at the team's Transfermarkt club or shares the citizenship), or 'unmatched'. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If *league* is not one of the covered league slugs. |

**Example**

```python
from sportsdataverse.soccer import soccer_espn_transfermarkt_player_crosswalk
xw = soccer_espn_transfermarkt_player_crosswalk()
print(xw.shape)

# One league, as pandas

epl = soccer_espn_transfermarkt_player_crosswalk(league="eng.1", return_as_pandas=True)

# Market values for ESPN athletes

from sportsdataverse.soccer import load_soccer_transfermarkt_players
xw = soccer_espn_transfermarkt_player_crosswalk()
xw.join(load_soccer_transfermarkt_players(), on="player_id").select("espn_athlete_id", "market_value_in_eur")
```

### soccer_espn_transfermarkt_team_crosswalk {#soccer_espn_transfermarkt_team_crosswalk}

`soccer_espn_transfermarkt_team_crosswalk(league: 'str | None' = None, *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

ESPN soccer team id -> Transfermarkt `club_id`, for the leagues of the ESPN <-> reep crosswalk.

Read from the bundled ESPN <-> reep table (`soccer_espn_reep_team_crosswalk`), whose reep rows carry
the Transfermarkt id, plus a reviewed id for a club whose reep row has none. No request is made. A
newly promoted club can carry a `club_id` the Transfermarkt clubs table does not list: that snapshot
ends in July 2026 and covers first tiers only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str \| None` | `None` | An ESPN league slug of the reep crosswalk (`eng.1`, `eng.2`, `esp.1`, `ger.1`, `ita.1`, `fra.1`, `usa.1`, `mex.1`); `None` (default) returns every league. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per `espn_team_id` (Utf8, unique; joins the ESPN soccer frames' team ids). `club_id` is Int64, null when no Transfermarkt id is known, and never on two ESPN teams.

| col_name | type | description |
|---|---|---|
| `espn_team_id` | String | ESPN soccer team id, digits as text (Utf8, as the ESPN soccer frames carry team ids); unique. |
| `espn_league` | String | ESPN league slug the team was listed under when the ESPN <-> reep crosswalk was built. |
| `espn_display_name` | String | ESPN display name of the team at that build. |
| `club_id` | Int64 | Transfermarkt club id (Int64); joins club_id of load_soccer_transfermarkt_clubs and current_club_id of load_soccer_transfermarkt_players. Null when no Transfermarkt id is known; never on two ESPN teams. |
| `match_method` | String | Where club_id comes from - 'reep' (the reep row's Transfermarkt id) or 'review' (reviewed by hand because the reep row has none); null with club_id. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If *league* is not one of the covered league slugs. |

**Example**

```python
from sportsdataverse.soccer import soccer_espn_transfermarkt_team_crosswalk
tm = soccer_espn_transfermarkt_team_crosswalk(league="eng.1")

# Current Transfermarkt squads of the ESPN Premier League teams

from sportsdataverse.soccer import load_soccer_transfermarkt_players
tm.join(load_soccer_transfermarkt_players(), left_on="club_id", right_on="current_club_id")
```
