---
title: MJHL — additional Python functions
sidebar_label: Additional functions
description: "MJHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# MJHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mjhl`
not covered by the generated API-endpoint reference above.

## Utilities & helpers

### `most_recent_mjhl_season() -> 'int'` {#most_recent_mjhl_season}

Most-recent MJHL season as an end-year integer (max `season_yr`), or 2026.

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

### `mjhl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_game_corsi}

Player-level on-ice Corsi and Fenwick for a single MJHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `mjhl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_game_shifts}

Parsed shift stints for a single MJHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `mjhl_game_summary(game_id: 'int') -> 'dict'` {#mjhl_game_summary}

MJHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |

### `mjhl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_leaders}

MJHL statistical leaders for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `player_id` | character | Unique player identifier. |
| `jersey_number` | character | Jersey number. |
| `name` | character | Display name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_code` | character | Internal team code. |
| `team_logo` | character | Team logo image URL. |
| `team_logo_small` | character | URL of the logo of the leader's team; the host and path vary by league and team (an lscluster.hockeytech.com download.php link or an assets.leaguestat.com logos path), and it can be the same URL as team_logo. |
| `stat_formatted` | character | The leader's value in the type_formatted category as a display string; for the Points and Goals leaderboards requested here it is a whole-number count (e.g. '29'), and '0' on every row when the season has no games yet. |
| `type_formatted` | character | Leaderboard the row belongs to, 'Points' or 'Goals': the function requests those two skater stat types and stacks both leaderboards into one frame. |
| `photo` | character | URL to the player photo. |
| `photo_small` | character | URL of the player's 60x60 headshot on assets.leaguestat.com, keyed by league and player_id (e.g. 'https://assets.leaguestat.com/<league>/60x60/<player_id>.jpg'), or the generic placeholder 'https://lscluster.hockeytech.com/img/nophoto.png'. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `division` | character | Team division. |

### `mjhl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_pbp}

MJHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `mjhl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_player_stats}

MJHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `mjhl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_player_toi}

Per-player time-on-ice totals for a single MJHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `mjhl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_schedule}

MJHL schedule — one row per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_status` | character | Game status label. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | Unique identifier for the home team. |
| `home_score` | character | Home team score at the time of the play. |
| `away_team` | character | Away team name. |
| `away_team_id` | character | Unique identifier for the away team. |
| `away_score` | character | Away team score at the time of the play. |
| `venue` | character | Venue name. |
| `season_id` | character | Unique season identifier. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |

### `mjhl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_season_id}

All MJHL seasons with end-year + game-type labels.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season_id` | integer | Unique season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `season_short` | character | Short season name. |
| `career` | character | Whether this is a career-stats season. |
| `playoff` | character | Whether the row is playoff statistics. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `end_date` | character | End date (YYYY-MM-DD). |
| `season_yr` | integer | Year derived from the season name (concluding year). |
| `game_type_label` | character | Game type: "preseason", "regular", or "playoffs". |

### `mjhl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_standings}

MJHL standings — one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `team_code` | character | Internal team code. |
| `wins` | character | Total wins. |
| `losses` | character | Total losses. |
| `ot_losses` | character | Overtime losses. |
| `ot_wins` | character | Overtime wins. |
| `shootout_losses` | character | Shootout losses. |
| `points` | integer | Points scored. |
| `penalty_minutes` | character | Penalty minutes. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `goals_diff` | character | Goal differential, goals_for minus goals_against (feed header 'Goal Differential'), as a signed integer string such as '14' or '-10'. |
| `percentage` | character | Points percentage (feed header 'Percentage'): points earned divided by the maximum points available from games played, as a three-decimal string, e.g. '0.833' for 5 of 6 possible points; '0.000' before a team has played. |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `team` | character | Team-side label or team identifier. |

### `mjhl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_team_roster}

MJHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `mjhl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#mjhl_teams}

MJHL teams for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | character | Unique team identifier. |
| `team_code` | character | Internal team code. |
| `team_nickname` | character | Team nickname. |
| `team_label` | character | Short city label. |
| `division` | character | Team division. |
| `team_logo` | character | Team logo image URL. |
