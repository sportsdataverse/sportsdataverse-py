---
title: "NBA — additional Python functions — ESPN"
sidebar_label: "ESPN"
sidebar_position: 2
description: "NBA — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — ESPN

### espn_nba_game_rosters {#espn_nba_game_rosters}

`espn_nba_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nba_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'logo_href', 'logo_dark_href', 'game_id'

**Example**

```python
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
print(rosters.shape)

# Pandas round-trip

rosters_pd = espn_nba_game_rosters(game_id=401585183, return_as_pandas=True)
rosters_pd.head()

# Pipeline next step (filter to game starters)

import polars as pl
starters = espn_nba_game_rosters(game_id=401585183).filter(
    pl.col("starter") == True
)
```

### espn_nba_pbp {#espn_nba_pbp}

`espn_nba_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_nba_pbp() - Pull the game by id - Data from API endpoints - `nba/playbyplay`, `nba/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from nba_schedule(). |
| `raw` | `bool` | `False` | If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets. |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
print(list(pbp.keys()))

# Pull only the raw ESPN summary payload (skip cleaning)

raw_pbp = espn_nba_pbp(game_id=401585183, raw=True)

# Pipeline next step (load plays into a polars DataFrame)

import polars as pl
pbp = espn_nba_pbp(game_id=401585183)
plays_df = pl.from_dicts(pbp["plays"])
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN NBA scoreboard event dict into a

shape suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN NBA scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.nba import espn_nba_schedule
sched = espn_nba_schedule(dates=20230102)
```
