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

### `chl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#chl_season_id}

All CHL seasons with end-year + game-type labels.

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

### `chl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#chl_standings}

CHL standings — one row per team.

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
| `nickname` | character | Team or athlete nickname. |
| `division` | character | Team division. |
| `wins` | character | Total wins. |
| `losses` | character | Total losses. |
| `ties` | character | Number of ties in the series. |
| `ot_losses` | character | Overtime losses. |
| `ot_wins` | character | Overtime wins. |
| `shootout_wins` | character | Shootout wins. |
| `shootout_losses` | character | Shootout losses. |
| `regulation_wins` | character | Wins in regulation. |
| `row` | character | Row index within the game grouping (sequencing helper). |
| `points` | integer | Points scored. |
| `penalty_minutes` | character | Penalty minutes. |
| `streak` | character | Current streak (e.g. 'W3' for three-game win streak). |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `goals_diff` | character | Goal differential, goals_for minus goals_against (feed header 'Goal Differential'), as a signed integer string such as '14' or '-10'. |
| `in_div` | character | Team record in games within its division (feed header 'In-Division') as a hyphenated three-part string; '0-0-0' in every sampled row, so the order of its parts is unverified. |
| `games_remaining` | character | Games remaining in the season. |
| `gpct` | character | Share of all goals in the team's games scored by the team, goals_for / (goals_for + goals_against), as a three-decimal string (e.g. '0.769' for 20 GF and 6 GA); the feed titles it 'Goal Differential Percentage' and ships '0' before any games. |
| `percentage` | character | Points percentage (feed header 'Percentage'): points earned divided by the maximum points available from games played, as a three-decimal string, e.g. '0.833' for 5 of 6 possible points; '0.000' before a team has played. |
| `overall_rank` | character | Overall recruit ranking (top recruits only; may be `NA`). |
| `games_played` | character | Games played. |
| `div_percentage` | character | Division percentage as a three-decimal string (feed header label 'PCT'); '0.000' in every sampled row while in_div was '0-0-0', so its formula could not be verified. |
| `team_rank` | integer | Team rank in the standings. |
| `past_10` | character | Record over the team's most recent 10 games (feed header 'Past 10 Games') as a hyphenated string: W-L-OTL-SOL when four parts are shipped (e.g. '2-0-0-1'), W-L-OTL when three are (e.g. '0-2-1'). |
| `team` | character | Team-side label or team identifier. |

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
