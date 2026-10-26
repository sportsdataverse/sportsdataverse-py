---
title: "NHL — additional Python functions — sportsdataverse-data releases"
sidebar_label: "sportsdataverse-data releases"
sidebar_position: 2
description: "NHL — additional Python functions — sportsdataverse-data releases — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — sportsdataverse-data releases

### load_nhl_games {#load_nhl_games}

`load_nhl_games(return_as_pandas: 'bool' = False)`

Load the NHL games-in-data-repo manifest (no `seasons` argument).

Mirrors fastRhockey (R) `load_nhl_games()` which reads a manifest of every
NHL game that has processed data in the data repository.

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
| `game_id` | Int64 | Unique game identifier. |
| `season_full` | String | Full season label (e.g. 20212022). |
| `game_type` | String | Game type the row belongs to. |
| `game_date` | String | Game date. |
| `game_time` | String | Scheduled start time of the game. |
| `home_team_abbr` | String | Home team abbreviation. |
| `away_team_abbr` | String | Away team abbreviation. |
| `home_team_name` | String | Home team name. |
| `away_team_name` | String | Away team name. |
| `home_score` | Int64 | Home team final score. |
| `away_score` | Int64 | Away team final score. |
| `game_state` | String | Game state (e.g., FINAL, LIVE). |
| `venue` | String | Venue where the game was played. |
| `series_letter` | String | Single-letter identifier for the playoff series to which this game belongs, used to group games within the same bracket matchup in the NHL games dataset. |
| `playoff_round` | Int64 | Playoff round identifier. |
| `series_game_number` | Int64 |  |
| `season` | Int64 | Season year (echoed from arg). |
| `game_json` | Boolean | Whether processed game JSON is available. |
| `game_json_url` | String | URL to the processed game JSON. |
| `PBP` | Boolean | Whether play-by-play data is available. |
| `team_box` | Boolean | Whether team box score data is available. |
| `player_box` | Boolean | Whether player box score data is available. |
| `skater_box` | Boolean | Whether skater box data is available. |
| `goalie_box` | Boolean | Whether goalie box data is available. |
| `game_info` | Boolean | Whether game info data is available. |
| `game_rosters` | Boolean | Whether game rosters data is available. |
| `scoring` | Boolean |  |
| `penalties` | Boolean | Penalty count. |
| `scratches` | Boolean | Logical flag indicating whether a scratches list (players healthy-scratched and not dressing) is available for this game in the NHL games loader output. |
| `linescore` | Boolean | Logical flag indicating whether linescore data (period-by-period scoring breakdown) is available for this game in the NHL games loader output. |
| `three_stars` | Boolean | Whether three stars data is available. |
| `shifts` | Boolean | Number of shifts. |
| `officials` | Boolean | Whether officials data is available. |
| `shots_by_period` | Boolean | Whether shots-by-period data is available. |
| `shootout` | Boolean | Whether shootout data is available. |

**Example**

```python
load_nhl_games()
```

### load_nhl_goalie_box {#load_nhl_goalie_box}

`load_nhl_goalie_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_goalie_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_nhl_goalie_boxscores` returns -- one row per goalie per game, the requested seasons stacked; see its documented columns. A pandas DataFrame when `return_as_pandas` is True.

### load_nhl_player_box {#load_nhl_player_box}

`load_nhl_player_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_player_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_nhl_player_boxscore` returns -- one row per player per game, the requested seasons stacked; see its documented columns. A pandas DataFrame when `return_as_pandas` is True.

### load_nhl_skater_box {#load_nhl_skater_box}

`load_nhl_skater_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_skater_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_nhl_skater_boxscores` returns -- one row per skater per game, the requested seasons stacked; see its documented columns. A pandas DataFrame when `return_as_pandas` is True.

### load_nhl_team_box {#load_nhl_team_box}

`load_nhl_team_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_team_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | A season or list of seasons, as END years (2026 = the 2025-26 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Exactly what `load_nhl_team_boxscore` returns -- one row per team per game, the requested seasons stacked; see its documented columns. A pandas DataFrame when `return_as_pandas` is True.
