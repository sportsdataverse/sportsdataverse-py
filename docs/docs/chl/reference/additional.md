---
title: CHL — additional Python functions
sidebar_label: Additional functions
description: "CHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# CHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.chl`
not covered by the generated API-endpoint reference above.

## Utilities & helpers

### `most_recent_chl_season() -> 'int'` {#most_recent_chl_season}

Most-recent CHL season as an end-year integer (max `season_yr`), or 2026.

## Other

### `build_family(league: 'str') -> 'dict[str, Any]'` {#build_family}

Return a dict of public callables for *league*.

All callables are fully independent closures over the single `league`
string; none share mutable state.  The dict is ready to be spread into
a module namespace via `globals().update(...)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | HockeyTech league code: `"ahl"`, `"ohl"`, `"whl"`, or `"qmjhl"`. |

**Returns**

Keys are the public function names (e.g. `"ahl_schedule"`).

### `chl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#chl_game_corsi}

Player-level on-ice Corsi and Fenwick for a single CHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#chl_game_shifts}

Parsed shift stints for a single CHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_game_summary(game_id: 'int') -> 'dict'` {#chl_game_summary}

CHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |

### `chl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#chl_leaders}

CHL statistical leaders for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#chl_pbp}

CHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#chl_player_stats}

CHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#chl_player_toi}

Per-player time-on-ice totals for a single CHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#chl_schedule}

CHL schedule — one row per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#chl_season_id}

All CHL seasons with end-year + game-type labels.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

### `chl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#chl_standings}

CHL standings — one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#chl_team_roster}

CHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `chl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#chl_teams}

CHL teams for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |
