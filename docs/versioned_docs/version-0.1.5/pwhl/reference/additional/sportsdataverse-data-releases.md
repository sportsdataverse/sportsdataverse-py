---
title: "PWHL — additional Python functions — sportsdataverse-data releases"
sidebar_label: "sportsdataverse-data releases"
sidebar_position: 1
description: "PWHL — additional Python functions — sportsdataverse-data releases — function reference in sdv-py, the SportsDataverse Python package."
---
# PWHL — additional Python functions — sportsdataverse-data releases

### load_pwhl_games {#load_pwhl_games}

`load_pwhl_games(return_as_pandas: 'bool' = False)`

Load the PWHL games-in-data-repo manifest (no `seasons` argument).

Mirrors fastRhockey (R) `load_pwhl_games()` which reads a manifest of every
PWHL game that has processed data in the data repository.

Tries the sportsdataverse-data release asset first; falls back to the raw
fastRhockey-data GitHub path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of all games in the data repository.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_date` | character | Game date. |
| `game_status` | character | Game status text. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | Home team identifier. |
| `away_team` | character | Away team name. |
| `away_team_id` | character | Away team identifier. |
| `home_score` | character | Home team final score. |
| `away_score` | character | Away team final score. |
| `winner` | character | Whether this competitor won the game. |
| `venue` | character | Venue where the game was played. |
| `venue_url` | character | URL for the venue. |
| `game_type` | character | Game type the row belongs to. |
| `game_json` | logical | Whether processed game JSON is available. |
| `game_json_url` | character | URL to the processed game JSON. |
| `PBP` | logical | Whether play-by-play data is available. |
| `player_box` | logical | Whether player box score data is available. |
| `skater_box` | logical | Whether skater box data is available. |
| `goalie_box` | logical | Whether goalie box data is available. |
| `team_box` | logical | Whether team box score data is available. |
| `game_info` | logical | Whether game info data is available. |
| `game_rosters` | logical | Whether game rosters data is available. |
| `scoring_summary` | logical | Whether scoring summary data is available. |
| `penalty_summary` | logical | Whether penalty summary data is available. |
| `three_stars` | logical | Whether three stars data is available. |
| `officials` | logical | Whether officials data is available. |
| `shots_by_period` | logical | Whether shots-by-period data is available. |
| `shootout` | logical | Whether shootout data is available. |

**Example**

```python
load_pwhl_games()
```

### load_pwhl_goalie_box {#load_pwhl_goalie_box}

`load_pwhl_goalie_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_goalie_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_pwhl_goalie_boxscores` returns -- one row per goalie per game, the requested seasons stacked; see its Returns table for the columns. A season with no published asset is skipped with a warning. A pandas DataFrame when `return_as_pandas` is True.

### load_pwhl_player_box {#load_pwhl_player_box}

`load_pwhl_player_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_player_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_pwhl_player_boxscores` returns -- one row per player per game, the requested seasons stacked; see its Returns table for the columns. A season with no published asset is skipped with a warning. A pandas DataFrame when `return_as_pandas` is True.

### load_pwhl_schedule {#load_pwhl_schedule}

`load_pwhl_schedule(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_schedules() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_pwhl_schedules` returns -- one row per game, the requested seasons stacked; see its Returns table for the columns. A season with no published asset is skipped with a warning. A pandas DataFrame when `return_as_pandas` is True.

### load_pwhl_skater_box {#load_pwhl_skater_box}

`load_pwhl_skater_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_skater_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_pwhl_skater_boxscores` returns -- one row per skater per game, the requested seasons stacked; see its Returns table for the columns. A season with no published asset is skipped with a warning. A pandas DataFrame when `return_as_pandas` is True.

### load_pwhl_team_box {#load_pwhl_team_box}

`load_pwhl_team_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_team_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_pwhl_team_boxscores` returns -- one row per team per game, the requested seasons stacked; see its Returns table for the columns. A season with no published asset is skipped with a warning. A pandas DataFrame when `return_as_pandas` is True.
