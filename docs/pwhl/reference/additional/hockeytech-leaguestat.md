# PWHL — additional Python functions — HockeyTech / LeagueStat: game_corsi–team_roster

> PWHL — additional Python functions — HockeyTech / LeagueStat: game_corsi–team_roster — function reference in sdv-py, the SportsDataverse Python package.

### pwhl_game_corsi {#pwhl_game_corsi}

`pwhl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Player-level on-ice Corsi and Fenwick for a single PWHL game.

Computes shot-attempt counts for every player found on ice during a
shot/blocked_shot/goal event, then joins their time-on-ice so per-60
rates are available.

**Corsi/Fenwick note**: the HockeyTech feed has no missed-shot event,
so both metrics are proxies that count only shot + blocked_shot + goal.
Every output row carries `corsi_includes_missed = False`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

One row per on-ice player with columns: - `player_id` (Int64) - `corsi_for`, `corsi_against` (Int64) - `corsi_for_pct` (Float64) - `fenwick_for`, `fenwick_against` (Int64) - `fenwick_for_pct` (Float64) - `toi_seconds` (Int64, from shifts; null if player not in shift data) - `corsi_for_per60` (Float64) - `corsi_includes_missed` (Boolean, always False)

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `corsi_for` | Int64 | Total shot attempts (on goal, missed, and blocked) directed toward the opponent by this team while at the tracked strength in the PWHL game. |
| `corsi_against` | Int64 | Total shot attempts (on goal, missed, and blocked) directed against this team while at the tracked strength in the PWHL game. |
| `corsi_for_pct` | Float64 | Share of all tracked shot attempts belonging to this team, expressed as a decimal (Corsi For percentage). |
| `fenwick_for` | Int64 | Unblocked shot attempts (on goal and missed only) directed toward the opponent by this team at the tracked strength in the PWHL game. |
| `fenwick_against` | Int64 | Unblocked shot attempts (on goal and missed only) directed against this team at the tracked strength in the PWHL game. |
| `fenwick_for_pct` | Float64 | Share of all unblocked shot attempts belonging to this team, expressed as a decimal (Fenwick For percentage). |
| `corsi_includes_missed` | Boolean | Boolean flag indicating whether missed shots are included in the Corsi totals for this record. |
| `toi_seconds` | Float64 | Total time on ice in seconds for this team at the tracked strength during the PWHL game. |
| `corsi_for_per60` | Float64 | This team's Corsi For rate projected to a full 60 minutes of ice time. |

### pwhl_game_info {#pwhl_game_info}

`pwhl_game_info(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL single-game metadata from the `statviewfeed` `gameSummary` view.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row: `game_id`, `game_number`, `game_date` / `game_date_iso`, `start_time` / `end_time` / `game_duration`, `game_venue`, `attendance`, `game_status`, `game_season_id`, `started` / `final` flags, `home_team_id` / `home_team` / `home_team_abbr` / `home_score`, the same `away_*` columns, `has_shootout` and the official report / text box score URLs (ids, scores and flags Int64): the published `load_pwhl_game_info` schema. A zero-row frame of that schema when the reply carries no game. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | HockeyTech game id (the game_id pwhl_game_info was called with). |
| `game_number` | String | Game number within the season as the feed labels it, as text (e.g. '72'). |
| `game_date` | String | Game date as a long display string (e.g. 'Wednesday, March 19, 2025'). |
| `game_date_iso` | String | Scheduled start as an ISO 8601 date-time with UTC offset (e.g. '2025-03-19T19:00:00-04:00'). |
| `start_time` | String | Actual puck-drop time as a local display string with time zone (e.g. '7:08 pm EDT'). |
| `end_time` | String | Time the game ended as a local display string with time zone (e.g. '9:47 pm EDT'). |
| `game_duration` | String | Elapsed real time from start to end as 'h:mm' (e.g. '2:39'). |
| `game_venue` | String | Arena and city as one display string (e.g. 'Coca-Cola Coliseum \| Toronto'). |
| `attendance` | Int64 | Announced attendance. |
| `game_status` | String | Game status label (e.g. 'Final'). |
| `game_season_id` | Int64 | HockeyTech season id the game belongs to (resolve to a season with pwhl_season_id). |
| `started` | Int64 | 1 when the game has started, 0 otherwise. |
| `final` | Int64 | 1 when the game is final, 0 otherwise. |
| `home_team_id` | Int64 | HockeyTech team id of the home team. |
| `home_team` | String | Full name of the home team (e.g. 'Toronto Sceptres'). |
| `home_team_abbr` | String | Abbreviation of the home team (e.g. 'TOR'). |
| `home_score` | Int64 | Goals scored by the home team. |
| `away_team_id` | Int64 | HockeyTech team id of the visiting team. |
| `away_team` | String | Full name of the visiting team (e.g. 'New York Sirens'). |
| `away_team_abbr` | String | Abbreviation of the visiting team (e.g. 'NY'). |
| `away_score` | Int64 | Goals scored by the visiting team. |
| `has_shootout` | Int64 | 1 when the game went to a shootout, 0 otherwise. |
| `game_report_url` | String | URL of the official HockeyTech game report. |
| `boxscore_url` | String | URL of the HockeyTech text box score. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_game_info
df = pwhl_game_info(game_id=164)
print(df.select("game_id", "home_team", "home_score", "away_team", "away_score"))

# As pandas

pdf = pwhl_game_info(game_id=164, return_as_pandas=True)
```

### pwhl_game_shifts {#pwhl_game_shifts}

`pwhl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Parsed shift stints for a single PWHL game.

Calls the HockeyTech `modulekit/gameshifts` endpoint and returns one
row per player-shift stint via `~sportsdataverse.hockeytech._parsers.parse_shifts`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

Columns include `player_id`, `first_name`, `last_name`, `home`, `period`, `start_time`, `end_time`, `start_s`, `end_s`, `goal_on_shift`, `penalty_on_shift`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Unique game identifier. |
| `player_id` | Int64 | Unique player identifier. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `jersey_number` | String | Jersey number. |
| `home` | Int64 | Whether the player's team was home. |
| `period` | Int64 | Period number. |
| `start_time` | String | Shift start time (MM:SS countdown clock). |
| `end_time` | String | Shift end time (MM:SS countdown clock). |
| `length` | String | Length of the streak in games. |
| `start_s` | Int64 | Shift start time in seconds elapsed from the start of the period. |
| `end_s` | Int64 | Shift end time in seconds elapsed from the start of the period. |
| `goal_on_shift` | Int64 | Number of goals scored while this shift was active (0 or 1 in most cases). |
| `penalty_on_shift` | Int64 | Number of penalties called while this shift was active. |

### pwhl_game_summary {#pwhl_game_summary}

`pwhl_game_summary(game_id: 'int') -> 'dict'`

PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |

**Returns**

`game` (one row: `game_id`, `date`, `status`, `venue`, `attendance`, both teams and scores), `goals` (one row per goal with the scorer, both assists and the plus / minus skaters), `penalties` (one row per penalty), `shots_by_period` (`side`, `period`, `shots`) and `three_stars`.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_game_summary
out = pwhl_game_summary(game_id=164)

# Next step

out["goals"].head()
```

### pwhl_leaders {#pwhl_leaders}

`pwhl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL statistical leaders for a given season.

NOTE: the `leadersExtended` endpoint uses `season_id` (integer) to filter
by season, not `season` (name string). The resolved integer is passed as the
`season_id` param so historical-season requests return results.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per ranked skater (points and goals leaders): `rank` (Int64), `player_id`, `name`, `jersey_number`, `position`, `team_id` / `team_name` / `team_code`, `stat_formatted` and `type_formatted` (`player_id` / `team_id` Int64, the rest String), plus photo and logo URLs. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the streak. |
| `player_id` | Int64 | Unique player identifier. |
| `jersey_number` | String | Jersey number. |
| `name` | String | Player full name (e.g. 'Hilary Knight'). |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `team_code` | String | Team abbreviation. |
| `team_logo` | String | URL to the team logo image. |
| `team_logo_small` | String | URL of the small-format team logo image for the PWHL leader-board entry. |
| `stat_formatted` | String | The leader's value in the type_formatted category as a display string; for the Points and Goals leaderboards requested here it is a whole-number count (e.g. '29'), and '0' on every row when the season has no games yet. |
| `type_formatted` | String | Human-readable label for the statistical category driving the leader-board ranking (e.g., "Goals", "Save Percentage"). |
| `photo` | String | URL to the player photo. |
| `photo_small` | String | URL of the small-format headshot image for the PWHL leader-board player. |
| `position` | String | Player position. |
| `division` | String | Division identifier. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_leaders
df = pwhl_leaders(season=2025)

# Next step

import polars as pl
df.filter(pl.col("type_formatted") == "Goals").head()
```

### pwhl_pbp {#pwhl_pbp}

`pwhl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL play-by-play — one row per event, fully enriched.

Matches fastRhockey `pwhl_pbp` column parity, adding:

- Coordinate transforms (`*_original`, `*_neutral`, `*_fixed`,
  `*_right`, `*_vertical`).
- Clock columns (`minute_start`, `second_start`, `clock`,
  `sec_from_start`).
- Shot geometry (`shot_distance`, `shot_angle`, `scoring_chance`).
- Game-meta join (`game_date`, `game_season`, `game_season_id`,
  `home_team`, `home_team_id`, `away_team`, `away_team_id`).
- On-ice player strings (`on_ice_home`, `on_ice_away`) derived from
  shift data.

Goal double-rowing: the HockeyTech feed emits both a `goal` row and a
twin `shot` row for (nearly) every goal. The twin shot row is flagged
`is_goal_twin = True` — shot rows (twins included) match the official
boxscore shots-on-goal totals, while dropping flagged rows yields a
deduplicated event stream. See `sportsdataverse.hockeytech._parsers.parse_pbp`.

The three network fetches (PBP payload, game summary meta, and shift data)
all go through the module-level `hockeytech_api` reference so tests can
monkeypatch `sportsdataverse.pwhl.pwhl_api.hockeytech_api` to intercept
all calls without touching the shared core.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per event (shot, goal, penalty, faceoff, hit, goalie change ...): `game_id`, `event`, `team_id`, `period_of_game`, `time_of_period`, rink `x_coord` / `y_coord` (Float64, feet, centre ice at (0, 0), as in fastRhockey and the `load_pwhl_pbp` release; the raw 600×300 feed canvas values are `x_coord_original` / `y_coord_original`) and the transforms above (`x_coord_fixed` / `x_coord_right` and their `y_` pairs, also feet), the primary / second / third player and goalie ids (Int64) and names, the plus / minus skaters on a goal, `is_goal_twin`, and the clock, geometry, game-meta and on-ice columns listed above. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Unique game identifier. |
| `event` | String | Event description label. |
| `team_id` | Int64 | Unique team identifier. |
| `period_of_game` | String | Period in which the event occurred. |
| `time_of_period` | String | Elapsed time within the period (MM:SS). |
| `x_coord` | Float64 | Transformed x-coordinate of the event (feet scale). |
| `y_coord` | Float64 | Transformed y-coordinate of the event (feet scale). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name_first` | String | Primary player first name. |
| `player_name_last` | String | Primary player last name. |
| `player_position` | String | Primary player position. |
| `goal` | Boolean | Flag for whether the event was a goal. |
| `is_goal_twin` | Boolean |  |
| `goalie_id` | Int64 | Goalie identifier on the play. |
| `goalie_first` | String | Goalie first name. |
| `goalie_last` | String | Goalie last name. |
| `home_win` | String | Whether the home player won the faceoff. |
| `player_team_id` | Int64 | Unique team identifier of the primary player. |
| `event_type` | String | Standardized event type code. |
| `shot_quality` | String | Shot quality descriptor. |
| `empty_net` | String | Whether the net was empty. |
| `game_winner` | String | Whether the goal was the game-winning goal. |
| `penalty_shot` | String | Whether the goal came on a penalty shot. |
| `insurance` | String | Whether the goal was an insurance goal. |
| `short_handed` | String | Whether the event occurred while short-handed. |
| `power_play` | String | Whether the event occurred on a power play. |
| `player_two_id` | Int64 | Second player's unique identifier. |
| `player_two_name_first` | String | Second player first name. |
| `player_two_name_last` | String | Second player last name. |
| `player_two_position` | String | Second player position. |
| `plus_player_one_id` | Int64 | On-ice plus player one unique identifier. |
| `plus_player_one_first` | String | On-ice plus player one first name. |
| `plus_player_one_last` | String | On-ice plus player one last name. |
| `plus_player_one_position` | String | On-ice plus player one position. |
| `plus_player_two_id` | Int64 | On-ice plus player two unique identifier. |
| `plus_player_two_first` | String | On-ice plus player two first name. |
| `plus_player_two_last` | String | On-ice plus player two last name. |
| `plus_player_two_position` | String | On-ice plus player two position. |
| `plus_player_three_id` | Int64 | On-ice plus player three unique identifier. |
| `plus_player_three_first` | String | On-ice plus player three first name. |
| `plus_player_three_last` | String | On-ice plus player three last name. |
| `plus_player_three_position` | String | On-ice plus player three position. |
| `plus_player_four_id` | Int64 | On-ice plus player four unique identifier. |
| `plus_player_four_first` | String | On-ice plus player four first name. |
| `plus_player_four_last` | String | On-ice plus player four last name. |
| `plus_player_four_position` | String | On-ice plus player four position. |
| `plus_player_five_id` | Int64 | On-ice plus player five unique identifier. |
| `plus_player_five_first` | String | On-ice plus player five first name. |
| `plus_player_five_last` | String | On-ice plus player five last name. |
| `plus_player_five_position` | String | On-ice plus player five position. |
| `minus_player_one_id` | Int64 | On-ice minus player one unique identifier. |
| `minus_player_one_first` | String | On-ice minus player one first name. |
| `minus_player_one_last` | String | On-ice minus player one last name. |
| `minus_player_one_position` | String | On-ice minus player one position. |
| `minus_player_two_id` | Int64 | On-ice minus player two unique identifier. |
| `minus_player_two_first` | String | On-ice minus player two first name. |
| `minus_player_two_last` | String | On-ice minus player two last name. |
| `minus_player_two_position` | String | On-ice minus player two position. |
| `minus_player_three_id` | Int64 | On-ice minus player three unique identifier. |
| `minus_player_three_first` | String | On-ice minus player three first name. |
| `minus_player_three_last` | String | On-ice minus player three last name. |
| `minus_player_three_position` | String | On-ice minus player three position. |
| `minus_player_four_id` | Int64 | On-ice minus player four unique identifier. |
| `minus_player_four_first` | String | On-ice minus player four first name. |
| `minus_player_four_last` | String | On-ice minus player four last name. |
| `minus_player_four_position` | String | On-ice minus player four position. |
| `minus_player_five_id` | Int64 | On-ice minus player five unique identifier. |
| `minus_player_five_first` | String | On-ice minus player five first name. |
| `minus_player_five_last` | String | On-ice minus player five last name. |
| `minus_player_five_position` | String | On-ice minus player five position. |
| `penalty_length` | String | Penalty length in minutes. |
| `player_three_id` | Int64 | Third player's unique identifier. |
| `player_three_name_first` | String | Third player first name. |
| `player_three_name_last` | String | Third player last name. |
| `player_three_position` | String | Third player position. |
| `game_date` | String | Game date. |
| `game_season` | Int64 | Season (concluding year, YYYY). |
| `game_season_id` | Int64 | HockeyTech season identifier. |
| `home_team` | String | Home team name. |
| `home_team_id` | Int64 | Home team identifier. |
| `away_team` | String | Away team name. |
| `away_team_id` | Int64 | Away team identifier. |
| `x_coord_original` | Float64 | Original raw x-coordinate from the feed. |
| `y_coord_original` | Float64 | Original raw y-coordinate from the feed. |
| `x_coord_neutral` | Float64 | Neutral-zone-centered x-coordinate. |
| `y_coord_neutral` | Float64 | Neutral-zone-centered y-coordinate. |
| `x_coord_fixed` | Float64 | Fixed-orientation x-coordinate. |
| `y_coord_fixed` | Float64 | Fixed-orientation y-coordinate. |
| `x_coord_right` | Float64 | Right-orientation x-coordinate. |
| `y_coord_right` | Float64 | Right-orientation y-coordinate. |
| `x_coord_vertical` | Float64 | Vertical-orientation x-coordinate. |
| `y_coord_vertical` | Float64 | Vertical-orientation y-coordinate. |
| `minute_start` | Int64 | Minute mark of the period when the event started. |
| `second_start` | Int64 | Second mark of the period when the event started. |
| `clock` | String | Game clock time remaining (MM:SS). |
| `sec_from_start` | Int64 | Seconds elapsed since the start of the game. |
| `shot_distance` | Float64 | Distance of the shot from the net. |
| `shot_angle` | Float64 | Angle of the shot relative to the net. |
| `scoring_chance` | Boolean | TRUE when event is a shot-type within 25 ft of the net. |
| `on_ice_home` | String | Comma-joined sorted player_ids on ice for the home team. |
| `on_ice_away` | String | Comma-joined sorted player_ids on ice for the away team. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_pbp
df = pwhl_pbp(game_id=164)

# Next step

import polars as pl
df.filter(pl.col("event") == "goal").head()
```

### pwhl_player_box {#pwhl_player_box}

`pwhl_player_box(game_id: 'int', return_as_pandas: 'bool' = False) -> 'dict[str, Any]'`

PWHL player box score for a single game, skaters and goalies (fastRhockey `pwhl_player_box`).

Reads the `statviewfeed` `gameSummary` view. The return is a dict, as in fastRhockey and as
the `fastRhockey-pwhl-raw` producer reads it (`box["skaters"]` / `box["goalies"]`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

`skaters` -- one row per dressed skater, home team first: `player_id`, `first_name`, `last_name`, `position`, `team_id`, `game_id`, `league`, `toi` ("mm:ss"), `time_on_ice` (Float64 minutes), `goals`, `assists`, `points`, `shots`, `hits`, `blocked_shots`, `penalty_minutes`, `plus_minus`, `faceoff_attempts` / `faceoff_wins` / `faceoff_losses`, `faceoff_pct` (Float64, null on no attempts) and `starting` (1 = starting lineup). `goalies` -- one row per dressed goalie with the same id / name / time columns, `saves`, `goals_against`, `shots_against`, the scoring, penalty and faceoff columns and `starting`; a goalie who did not play has null `toi` / `time_on_ice`. Ids and counts are Int64. Both frames are zero-row with these columns when the reply carries no players. pandas DataFrames when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_player_box
box = pwhl_player_box(game_id=164)
print(box["skaters"].shape, box["goalies"].shape)

# Goalies who played

import polars as pl
box["goalies"].filter(pl.col("time_on_ice").is_not_null())
```

### pwhl_player_game_log {#pwhl_player_game_log}

`pwhl_player_game_log(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL player game-by-game log.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game played: `id` (the game id), `date_played`, `home_team_code` / `visiting_team_code` and names, `player_team`, `goals`, `assists`, `points`, `plus_minus`, `shots`, `hits`, `penalty_minutes`, `ice_time_minutes_seconds`, faceoff, power-play, short-handed and shootout counts. Counts arrive as strings except `points` and the percentages (Int64). A pandas DataFrame when `return_as_pandas` is True; a zero-row frame for a player with no games.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_player_game_log
df = pwhl_player_game_log(player_id=27)

# Next step

print(df.shape)
```

### pwhl_player_info {#pwhl_player_info}

`pwhl_player_info(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL player biographical info from the `statviewfeed` `player` view.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row: `player_id` and `person_id` (Int64), `first_name`, `last_name`, `name`, `jersey_number`, `position`, `shoots`, `catches`, `height`, `weight`, `birthdate`, `birth_place`, `nationality`, `player_type` ("skater" or "goalie"), `team_name` / `team_nickname` (most recent team), `image_url` and `current_season_id` (Int64). A zero-row frame of these columns when the reply carries no player. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | HockeyTech player id (the player_id pwhl_player_info was called with). |
| `person_id` | Int64 | HockeyTech person id; one person keeps it across player and staff records. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `name` | String | First and last name joined by a space. |
| `jersey_number` | String | Jersey number as text, on the player's most recent team. |
| `position` | String | Position code (e.g. 'LD', 'C', 'G'). |
| `shoots` | String | Shooting hand, 'L' or 'R'. |
| `catches` | String | Catching hand, 'L' or 'R' (the feed fills it for skaters too). |
| `height` | String | Height as feet'inches text (e.g. "5'11"). |
| `weight` | String | Weight in pounds as text; '0' when the feed has none. |
| `birthdate` | String | Date of birth as YYYY-MM-DD. |
| `birth_place` | String | Birthplace as 'city, region, country' text. |
| `nationality` | String | Nationality label (e.g. 'American'). |
| `player_type` | String | Player kind, 'skater' or 'goalie'. |
| `team_name` | String | Full name of the player's most recent team (e.g. 'Boston Fleet'). |
| `team_nickname` | String | Nickname of the player's most recent team (e.g. 'Fleet'). |
| `image_url` | String | URL of the player's 240x240 profile image. |
| `current_season_id` | Int64 | HockeyTech season id the feed treats as current when answering (not a season the player played). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_player_info
df = pwhl_player_info(player_id=12)
print(df.select("player_id", "name", "position", "team_name"))

# As pandas

pdf = pwhl_player_info(player_id=12, return_as_pandas=True)
```

### pwhl_player_search {#pwhl_player_search}

`pwhl_player_search(name: 'str', return_as_pandas: 'bool' = False) -> 'Any'`

Search for PWHL players by name.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The search text, matched against player names. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per matching player (the `*_id` columns Int64, the rest String): `person_id`, `player_id`, `first_name` / `last_name`, `position`, `shoots` / `catches`, `height` / `weight`, `birthdate` and birthplace, `last_team_name` / `last_team_code`, `role_name`, `active`, `last_active_date` and the match `score`. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique person identifier. |
| `player_id` | Int64 | Unique player identifier. |
| `active` | String | Whether athlete is currently active. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `phonetic_name` | String | Phonetic spelling of the player name. |
| `shoots` | String | Shooting hand. |
| `catches` | String | Catching hand (goalies). |
| `height` | String | Player height in inches. |
| `weight` | String | Player weight in pounds. |
| `rawbirthdate` | String | Player's birth date as a raw string in the format returned by the PWHL HockeyTech API, typically YYYY-MM-DD. |
| `birthdate` | String | Date of birth. |
| `birthtown` | String | Player birth town. |
| `birthprov` | String | Player birth province/state. |
| `birthcntry` | String | Player birth country. |
| `team_id` | Int64 | Unique team identifier. |
| `jersey_number` | String | Jersey number. |
| `role_id` | Int64 | Id of the person's role in the HockeyTech person record ('1' = 'Player' in the captures): player or staff, not a playing position (see position). |
| `season_id` | Int64 | Season identifier. |
| `role_name` | String | Label of the person's role in the HockeyTech person record (e.g. 'Player'), not a playing position (see position). |
| `all_roles` | String | Pipe- or comma-delimited string listing every positional or roster role associated with the player in the PWHL HockeyTech system. |
| `last_team_name` | String | Full name of the PWHL team on which the player most recently appeared. |
| `last_team_code` | String | Short abbreviation code for the PWHL team on which the player most recently appeared. |
| `division` | String | Division identifier. |
| `position` | String | Player position. |
| `profile_image` | String | File name of the player's profile photo, not a URL (e.g. '7dea378a91cedc9d5d14b1222802322c.jpg'). |
| `score` | String | Search relevance score of the match as a decimal string (e.g. '5.911529541015625'), not a game score. |
| `last_active_date` | String | Date (YYYY-MM-DD) the feed reports the person as last active; for the active players captured it equals the capture date, so it is not the date of the player's last game. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_player_search
df = pwhl_player_search(name="Poulin")

# Next step

df.select("player_id", "first_name", "last_name")
```

### pwhl_player_stats {#pwhl_player_stats}

`pwhl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per season (and team) the player played: `season_id`, `season_name`, `playoff`, `team_id` / `team_name` / `team_code`, `games_played`, `goals`, `assists`, `points`, `plus_minus`, `penalty_minutes`, power-play / short-handed / shootout splits, `shots`, `faceoff_wins` / `faceoff_attempts`, `ice_time` and `stat_type`. `season_id` and `team_id` are Int64; the rest are String, as the feed ships them. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Season identifier. |
| `season_name` | String | Full season name (e.g., "2024-25 Regular Season"). |
| `shortname` | String | Player short name. |
| `playoff` | String | Whether the row is playoff statistics. |
| `career` | String | Whether this is a career-stats season. |
| `sopt_track_faceoffs` | String | Flag indicating whether faceoff tracking is enabled for this player's statistical record in the HockeyTech system. |
| `max_start_date` | String | Latest game start date for the season. |
| `veteran_status` | String | Player veteran status. |
| `veteran` | String | Whether the player is a veteran. |
| `jersey_number` | String | Jersey number. |
| `goals` | String | Goals scored. |
| `games_played` | String | Games played. |
| `assists` | String | Assists. |
| `points` | String | Total points (goals + assists). |
| `plus_minus` | String | Plus/minus rating. |
| `penalty_minutes` | String | Penalty minutes. |
| `power_play_goals` | String | Power-play goals. |
| `power_play_assists` | String | Power-play assists. |
| `shots` | String | Shots on goal. |
| `shootout_attempts` | String | Shootout attempts. |
| `shootout_goals` | String | Shootout goals. |
| `shootout_percentage` | String | Shootout scoring percentage. |
| `shooting_percentage` | String | Shooting percentage. |
| `shootout_winning_goals` | String | Shootout game-winning goals. |
| `points_per_game` | String | Points per game. |
| `short_handed_goals` | String | Short-handed goals. |
| `short_handed_assists` | String | Short-handed assists. |
| `game_winning_goals` | String | Game-winning goals. |
| `game_tieing_goals` | String | Game-tying goals. |
| `faceoff_wins` | String | Faceoff wins. |
| `faceoff_attempts` | String | Faceoff attempts. |
| `faceoff_pct` | String | Faceoff win percentage. |
| `hits` | String | Hits. |
| `team_name` | String | Team name. |
| `team_code` | String | Team abbreviation. |
| `team_city` | String | Team city. |
| `team_nickname` | String | Team nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `active` | String | Whether athlete is currently active. |
| `first_goals` | String | First goals of a game. |
| `insurance_goals` | String | Insurance goals. |
| `overtime_goals` | String | Overtime goals. |
| `unassisted_goals` | String | Unassisted goals. |
| `empty_net_goals` | String | Empty-net goals. |
| `penalty_minutes_per_game` | String | Penalty minutes per game. |
| `division` | String | Division identifier. |
| `ice_time` | String | Total ice time. |
| `ice_time_minutes_seconds` | String | Ice time in minutes and seconds. |
| `shots_blocked_by_player` | String | Shots blocked by the player. |
| `stat_type` | String | Statistic type ("regular"/"playoff"). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_player_stats
df = pwhl_player_stats(player_id=27)

# Next step

df.select("season_name", "games_played", "points")
```

### pwhl_player_toi {#pwhl_player_toi}

`pwhl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player time-on-ice totals for a single PWHL game.

Fetches shifts via `pwhl_game_shifts` then aggregates via
`~sportsdataverse.hockeytech._analytics.player_toi`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

One row per player with `player_id`, `first_name`, `last_name`, `toi_seconds`, `num_shifts`, `avg_shift_s`, sorted by `toi_seconds` descending.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `toi_seconds` | Float64 | Total time on ice for the PWHL player during the game or reporting period, recorded in seconds. |
| `num_shifts` | Int64 | Total number of shifts the player took during the game or reporting period. |
| `avg_shift_s` | Float64 | Average duration of a single shift for the player during the game or reporting period, measured in seconds. |

### pwhl_playoff_bracket {#pwhl_playoff_bracket}

`pwhl_playoff_bracket(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL playoff bracket for a given season.

With neither `season` nor `season_id`, the newest season that has playoffs:
the newest season overall is usually still before its playoffs, with no bracket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). |
| `season_id` | `Optional[int]` | `None` | The HockeyTech playoff season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per playoff series: `round` / `round_name` / `round_type_name`, `series_letter` / `series_name`, `team1` / `team2` (team ids), `team1_wins` / `team2_wins` (Int64), `winner` (the winning team id, but the feed often leaves it empty even after a series ends, so read the result from the win counts), `feeder_series1` / `feeder_series2`, and `games` (a list of structs, one per game: ids, both teams, goal counts, status, date). A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The seasons feed lists no playoff season, or the feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_playoff_bracket
df = pwhl_playoff_bracket(season=2025)

# Next step

print(df.shape)
```

### pwhl_schedule {#pwhl_schedule}

`pwhl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL schedule — one row per game of one season (matches fastRhockey `pwhl_schedule`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game: `game_id`, `game_date`, `game_status`, `home_team` / `home_team_id` / `home_score`, `away_team` / `away_team_id` / `away_score`, `venue`, `season_id` and `game_type`; the `*_id` columns are Int64, the rest String. Only the requested season's games: a regular season, its playoffs and its preseason are separate season ids, so pass `season_id` (from `pwhl_season_id()`) for the playoffs. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Unique game identifier. |
| `game_date` | String | Game date. |
| `game_status` | String | Game status text. |
| `home_team` | String | Home team name. |
| `home_team_id` | Int64 | Home team identifier. |
| `home_score` | String | Home team final score. |
| `away_team` | String | Away team name. |
| `away_team_id` | Int64 | Away team identifier. |
| `away_score` | String | Away team final score. |
| `venue` | String | Venue where the game was played. |
| `season_id` | Int64 | Season identifier. |
| `game_type` | String | Game type the row belongs to. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404, or, with no season given, the seasons feed lists no regular season. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |
| `ValueError` | No season matches `season`. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_schedule
df = pwhl_schedule(season=2025)

# Next step

import polars as pl
df.filter(pl.col("game_status") == "Final").head()
```

### pwhl_scorebar {#pwhl_scorebar}

`pwhl_scorebar(return_as_pandas: 'bool' = False) -> 'Any'`

PWHL live scorebar (today ± 3 days).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game in the window, as the feed ships it (`id` and the `*_id` columns Int64, the rest String): `id`, `season_id`, `game_date` / `game_date_iso8601`, `scheduled_time`, `home_id` / `home_code` / `home_long_name` / `home_goals`, the same `visitor_*` columns, `period`, `game_clock`, `game_status` / `game_status_string`, `venue_name`, both teams' records, and broadcast URLs. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | HockeyTech game id, the game_id the single-game functions (pwhl_pbp, pwhl_game_summary) take. |
| `season_id` | Int64 | Season identifier. |
| `league_id` | Int64 | HockeyTech league id of the game within the feed (e.g. '1'; AHL '4', SJHL '3'); not always the league_id the standings call sends. |
| `game_number` | String | Game number within the schedule. |
| `game_letter` | String | Playoff-series letter (A, B, C, ... one per series), set on every playoff game and empty on regular-season games. |
| `game_type` | String | Game type the row belongs to. |
| `quick_score` | String | Unused by the feed: '0' in every captured row (final and scheduled games, 17 leagues), never a score. |
| `date` | String | Game date as YYYY-MM-DD (date only, no time). |
| `flo_core_event_id` | Int64 | FloSports core event identifier linking this PWHL game to its FloSports broadcast event record. |
| `flo_live_event_id` | Int64 | FloSports live-stream event identifier for this PWHL game. |
| `game_date` | String | Game date. |
| `game_date_iso8601` | String | Scheduled start as an ISO 8601 date-time with UTC offset (e.g. '2023-12-04T13:00:00-05:00'). |
| `scheduled_time` | String | Raw scheduled start time for the game as returned by the HockeyTech feed, typically in HH:MM:SS format. |
| `scheduled_formatted_time` | String | Human-readable local game start time string formatted for display (e.g., "7:00 PM ET"). |
| `timezone` | String | Time zone of the scheduled start as a tz database name (e.g. 'Canada/Eastern'). |
| `ticket_url` | String | URL to the official ticketing page where fans can purchase tickets for this game. |
| `home_id` | Int64 | HockeyTech team id of the home team (the team_id of pwhl_teams, e.g. '1' = Boston), not an ESPN id. |
| `home_code` | String | Short team code (abbreviation) for the home team (e.g., "BOS", "MIN"). |
| `home_city` | String | City name of the home team (e.g. 'Boston', 'Minnesota'). |
| `home_nickname` | String | Franchise nickname for the home team (e.g., "Fleet", "Frost"). |
| `home_long_name` | String | Full name including city and franchise for the home team (e.g., "Boston Fleet"). |
| `home_division` | String | Home team division. |
| `home_goals` | String | Goals scored by the home team in the game so far; the final score once game_status is '4'. |
| `home_audio_url` | String | URL of the home-team radio or audio broadcast stream for this game. |
| `home_video_url` | String | URL of the home-team video broadcast stream for this game. |
| `home_webcast_url` | String | URL of the home-team webcast for online viewing of this game. |
| `visitor_id` | Int64 | HockeyTech team identifier for the visiting team in this game. |
| `visitor_code` | String | Short team code (abbreviation) for the visiting team (e.g., "NYR", "OTT"). |
| `visitor_city` | String | City name of the visiting team (e.g., "New York", "Ottawa"). |
| `visitor_nickname` | String | Franchise nickname for the visiting team (e.g., "Charge", "Sceptres"). |
| `visitor_long_name` | String | Full name including city and franchise for the visiting team (e.g., "Ottawa Charge"). |
| `visiting_division` | String | Visiting team division. |
| `visitor_goals` | String | Number of goals scored by the visiting team at the current point in the game. |
| `visitor_audio_url` | String | URL of the visiting-team radio or audio broadcast stream for this game. |
| `visitor_video_url` | String | URL of the visiting-team video broadcast stream for this game. |
| `visitor_webcast_url` | String | URL of the visiting-team webcast for online viewing of this game. |
| `period` | String | Period number. |
| `period_name_short` | String | Abbreviated name of the current or final game period (e.g., "3rd", "OT"). |
| `period_name_long` | String | Verbose name of the current or final game period (e.g., "Third Period", "Overtime"). |
| `game_clock` | String |  |
| `game_summary_url` | String | Game-summary link target, not a full URL: the bare game id in some leagues (e.g. PWHL '74') and a site-relative path in others (e.g. '/game-center/?game_id=4896'). |
| `home_wins` | String | Home team's wins in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `home_regulation_losses` | String | Home team's regulation losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `home_ot_losses` | String | Home team's overtime losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `home_shootout_losses` | String | Home team's shootout losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_wins` | String | Visiting team's wins in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_regulation_losses` | String | Visiting team's regulation losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_ot_losses` | String | Visiting team's overtime losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_shootout_losses` | String | Visiting team's shootout losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `game_status` | String | Numeric game-status code as text: '1' for a scheduled game, '4' for a final. The label is in game_status_string ('Final', or the start time of a scheduled game). |
| `intermission` | String | Flag or string indicating whether the game is currently in an intermission period. |
| `game_status_string` | String | Short status label for the game's current state (e.g., "Final", "In Progress", "Scheduled"). |
| `game_status_string_long` | String | Verbose status description for the game's current state, including period or overtime context. |
| `ord` | String | Ordinal sort key used by the HockeyTech scorebar feed to order games within a day. |
| `venue_name` | String | Name of the venue. |
| `venue_location` | String | City and/or arena name indicating the physical location where the game is played. |
| `league_name` | String | League name. |
| `league_code` | String | Short code identifying the league for this scorebar record (e.g., "PWHL"). |
| `time_tbd` | String | Scorebar flag string named for a to-be-determined start time; '0' in every sampled row, so the non-zero case was not observed. |
| `date_tbd` | String | Scorebar flag string named for a to-be-determined game date; '0' in every sampled row, so the non-zero case was not observed. |
| `timezone_short` | String | Abbreviated timezone label for the game's scheduled start time (e.g., "ET", "CT"). |
| `home_logo` | String | Home team logo URL. |
| `visitor_logo` | String | URL of the logo image for the visiting team. |
| `flo_hockey_url` | String | URL to the FloHockey streaming page for this PWHL game. |
| `combined_client_code` | String | Combined league-and-client identifier string used by the HockeyTech feed to distinguish multi-tenant deployments. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_scorebar
df = pwhl_scorebar()

# Next step

print(df.shape)
```

### pwhl_skater_rapm {#pwhl_skater_rapm}

`pwhl_skater_rapm(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, xg_model: 'Any' = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

② PWHL skater xG RAPM -- shim over `nhl_skater_rapm` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring); returns a documented
empty frame + `cli_warn` rather than fitting a degenerate ridge when `shifts` is
too thin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_pwhl_pbp` frame (adapted, see the module docstring) or a frame already shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | the matching `load_pwhl_shifts` frame, or one shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory for NHL-shaped input (the borrowed NHL xG boosters). |
| `xg_model` | `Any` | `None` | a fitted `PwhlCoordXGModel` scoring HockeyTech input; `None` fits one on `pbp` (see `sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg`). That default is an in-sample fit: the goals in `pbp` shape the xG the ratings are fit to. For a backtest, pass a model fit on prior seasons only. |

**Returns**

Same schema as `nhl_skater_rapm`: `player_id:Int64, xg_rapm_off:Float64, xg_rapm_def:Float64, xg_rapm:Float64, toi_minutes:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: since #846 it runs on the published PWHL pbp and shift releases (tested on five real 2025 games) and returns the same schema as its NHL engine, which has no capture of its own.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | A HockeyTech pbp/shift column the NHL contract needs is missing. |

**Example**

```python
from sportsdataverse.pwhl import load_pwhl_pbp, load_pwhl_shifts
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_skater_rapm
rapm = pwhl_skater_rapm(load_pwhl_pbp(2025), load_pwhl_shifts(2025))
```

### pwhl_skater_war {#pwhl_skater_war}

`pwhl_skater_war(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, xg_model: 'Any' = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

③ PWHL GAR/WAR composite -- shim over `nhl_skater_war` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_pwhl_pbp` frame (adapted, see the module docstring) or a frame already shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | the matching `load_pwhl_shifts` frame, or one shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory for NHL-shaped input (the borrowed NHL xG boosters). |
| `xg_model` | `Any` | `None` | a fitted `PwhlCoordXGModel` scoring HockeyTech input; `None` fits one on `pbp` (see `sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg`). That default is an in-sample fit: the goals in `pbp` shape the xG the ratings are fit to. For a backtest, pass a model fit on prior seasons only. |

**Returns**

Same schema as `nhl_skater_war`: `player_id:Int64, ev_off:Float64, ev_def:Float64, pp:Float64, pk:Float64, pens:Float64, faceoffs:Float64, gar:Float64, war:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: since #846 it runs on the published PWHL pbp and shift releases (tested on five real 2025 games) and returns the same schema as its NHL engine, which has no capture of its own.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | A HockeyTech pbp/shift column the NHL contract needs is missing. |

**Example**

```python
from sportsdataverse.pwhl import load_pwhl_pbp, load_pwhl_shifts
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_skater_war
war = pwhl_skater_war(load_pwhl_pbp(2025), load_pwhl_shifts(2025))
```

### pwhl_special_teams_value {#pwhl_special_teams_value}

`pwhl_special_teams_value(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, xg_model: 'Any' = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

⑥ PWHL special-teams value -- shim over `nhl_special_teams_value` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_pwhl_pbp` frame (adapted, see the module docstring) or a frame already shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | the matching `load_pwhl_shifts` frame, or one shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory for NHL-shaped input (the borrowed NHL xG boosters). |
| `xg_model` | `Any` | `None` | a fitted `PwhlCoordXGModel` scoring HockeyTech input; `None` fits one on `pbp` (see `sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg`). That default is an in-sample fit: the goals in `pbp` shape the xG the ratings are fit to. For a backtest, pass a model fit on prior seasons only. |

**Returns**

Same schema as `nhl_special_teams_value`: `player_id:Int64, pp_toi_minutes:Float64, pk_toi_minutes:Float64, pp_value:Float64, pk_value:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: since #846 it runs on the published PWHL pbp and shift releases (tested on five real 2025 games) and returns the same schema as its NHL engine, which has no capture of its own.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | A HockeyTech pbp/shift column the NHL contract needs is missing. |

**Example**

```python
from sportsdataverse.pwhl import load_pwhl_pbp, load_pwhl_shifts
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_special_teams_value
st = pwhl_special_teams_value(load_pwhl_pbp(2025), load_pwhl_shifts(2025))
```

### pwhl_standings {#pwhl_standings}

`pwhl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL standings — one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team`, `team_code`, `team_rank` and `wins` (Int64), and `games_played`, `losses`, `regulation_wins`, `non_reg_wins`, `non_reg_losses`, `points`, `goals_for`, `goals_against`, `games_remaining`, `percentage` and `overall_rank` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `team_code` | String | Team abbreviation. |
| `losses` | String | Losses. |
| `regulation_wins` | String | Wins in regulation. |
| `points` | Int64 | Total points (goals + assists). |
| `streak_wl` | String | Present but unpopulated in the sampled PWHL standings (empty string in every row); the name suggests a win/loss streak, but no value was observed to confirm a format. |
| `goals_for` | String | Goals for. |
| `goals_against` | String | Goals against. |
| `non_reg_wins` | String | Non-regulation wins. |
| `non_reg_losses` | String | Non-regulation losses. |
| `games_remaining` | String | Games remaining in the season. |
| `percentage` | String | Points percentage earned by the PWHL team (points divided by maximum possible points), expressed as a decimal between 0 and 1. |
| `overall_rank` | String |  |
| `games_played` | String | Games played. |
| `team_rank` | Int64 | Team rank in the standings. |
| `team` | String | Team name. |
| `wins` | Int64 | Wins. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_standings
df = pwhl_standings(season=2025)

# Next step

df.sort("team_rank").head()
```

### pwhl_stats {#pwhl_stats}

`pwhl_stats(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, position: 'str' = 'skaters', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL aggregate stats by season and position.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `position` | `str` | `'skaters'` | `"skaters"` (default) or `"goalies"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player for that season and position, as the feed ships it (~90 columns; the `*_id` columns Int64, the others typed as the returns table lists): `player_id`, `name`, `team_id` / `team_name` / `team_code`, `position`, `games_played`, and the counting, per-game and ice-time stats of that position (skaters: `goals`, `assists`, `points`, `plus_minus`, `shots`, `hits`, `penalty_minutes` ...). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `shortname` | String | Player short name. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `name` | String | Player full name (e.g. 'Jessie Eldridge'). |
| `phonetic_name` | String | Phonetic spelling of the player name. |
| `active` | String | Whether athlete is currently active. |
| `height` | String | Player height as feet-and-inches text (e.g. 5'11), not a number of inches. |
| `weight` | String | Player weight in pounds. |
| `last_years_club` | String | Player's club in the previous season. |
| `age` | String | Player age. |
| `shoots` | String | Shooting hand. |
| `position` | String | Player position. |
| `suspension_games_remaining` | String | Suspension games remaining. |
| `suspension_indefinite` | String | Whether the suspension is indefinite. |
| `rookie` | String | Whether the player is a rookie. |
| `veteran` | String | HockeyTech veteran-status code as text ('1' or '2' in the captured rows; the feed does not document the codes), not a boolean. |
| `draft_eligible` | String | Whether the player is draft eligible. |
| `jersey_number` | String | Jersey number. |
| `team_name` | String | Team name. |
| `team_code` | String | Team abbreviation. |
| `team_id` | Int64 | Unique team identifier. |
| `division` | String | Division name as text (e.g. 'PWHL'), not an identifier. |
| `birthdate` | String | Date of birth. |
| `birthdate_year` | String | Player birth year. |
| `hometown` | String | Player hometown as city and province, state or country abbreviation (e.g. 'Barrie, ON'). |
| `homeprov` | String | Player home province/state. |
| `homecntry` | String | Player home country. |
| `birthtown` | String | Player birth town. |
| `birthprov` | String | Player birth province/state. |
| `birthcntry` | String | Player birth country. |
| `hometownprov` | String | Player hometown and province/state. |
| `homeplace` | String | Player home place description. |
| `games_played` | String | Games played. |
| `game_winning_goals` | String | Game-winning goals. |
| `game_tieing_goals` | String | Game-tying goals. |
| `first_goals` | String | First goals of a game. |
| `insurance_goals` | String | Insurance goals. |
| `unassisted_goals` | String | Unassisted goals. |
| `empty_net_goals` | String | Empty-net goals. |
| `overtime_goals` | String | Overtime goals. |
| `ice_time` | String | Total ice time. |
| `ice_time_avg` | String | Average ice time. |
| `goals` | String | Goals scored. |
| `shots` | String | Shots on goal. |
| `loose_ball_recoveries` | String | Loose ball recoveries. |
| `caused_turnovers` | String | Turnovers caused. |
| `turnovers` | String | Turnovers committed. |
| `hits` | String | Hits. |
| `shots_blocked_by_player` | String | Shots blocked by the player. |
| `ice_time_minutes_seconds` | String | Ice time in minutes and seconds. |
| `shooting_percentage` | String | Shooting percentage. |
| `assists` | String | Assists. |
| `points` | String | Total points (goals + assists). |
| `points_per_game` | String | Points per game. |
| `plus_minus` | String | Plus/minus rating. |
| `penalty_minutes` | String | Penalty minutes. |
| `penalty_minutes_per_game` | String | Penalty minutes per game. |
| `ice_time_per_game_avg` | String | Average ice time per game. |
| `hits_per_game_avg` | String | Average hits per game. |
| `minor_penalties` | String | Minor penalties. |
| `major_penalties` | String | Major penalties. |
| `power_play_goals` | String | Power-play goals. |
| `power_play_assists` | String | Power-play assists. |
| `power_play_points` | String | Power play points. |
| `short_handed_goals` | String | Short-handed goals. |
| `short_handed_assists` | String | Short-handed assists. |
| `short_handed_points` | String | Short-handed points. |
| `shootout_goals` | String | Shootout goals. |
| `shootout_attempts` | String | Shootout attempts. |
| `shootout_winning_goals` | String | Shootout game-winning goals. |
| `shootout_games_played` | String | Games played that went to a shootout. |
| `faceoff_attempts` | String | Faceoff attempts. |
| `faceoff_wins` | String | Faceoff wins. |
| `faceoff_pct` | String | Faceoff win percentage. |
| `faceoff_wa` | String | Faceoff wins-to-attempts metric. |
| `shots_on` | String | Shots on goal count. |
| `shootout_percentage` | String | Shootout scoring percentage. |
| `latest_team_id` | Int64 | Most recent team identifier. |
| `num_teams` | String | Number of teams the player has played for. |
| `logo` | String | URL to the team logo. |
| `rank` | Int64 | 1-based position of the player in the feed's sorted stat table. |
| `player_page_link` | String | URL to the player page. |
| `player_image` | String | URL to the player's headshot image as served by the HockeyTech/PWHL data feed. |
| `namelink` | String | Player name as plain text (the text of the feed's player link; equals name in the captures), not HTML. |
| `teamlink` | String | HTML link for the team. |
| `team_breakdown` | Int64 | Per-team statistical breakdown. |
| `is_total` | Float64 | Whether the row is a season total. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_stats
df = pwhl_stats(season=2025)

# Next step

print(df.shape)
```

### pwhl_streaks {#pwhl_streaks}

`pwhl_streaks(return_as_pandas: 'bool' = False) -> 'Any'`

Current PWHL player/team streaks — **non-functional: no such upstream view**.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True` return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

An empty frame (the upstream view does not exist).

**Example**

```python
from sportsdataverse.pwhl import pwhl_streaks
df = pwhl_streaks()

# Next step

print(df.shape)
```

### pwhl_team_roster {#pwhl_team_roster}

`pwhl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | The HockeyTech team id. |
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image`. The `id` / `*_id` columns are Int64; the rest are String, as the feed ships them. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `person_id` | Int64 | Unique person identifier. |
| `active` | String | Whether athlete is currently active. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `phonetic_name` | String | Phonetic spelling of the player name. |
| `display_name` | String | Player display name. |
| `shoots` | String | Shooting hand. |
| `hometown` | String | Prospect hometown. |
| `homeprov` | String | Player home province/state. |
| `homecntry` | String | Player home country. |
| `homeplace` | String | Player home place description. |
| `birthtown` | String | Player birth town. |
| `birthprov` | String | Player birth province/state. |
| `birthcntry` | String | Player birth country. |
| `birthplace` | String | City, province/state, or country where the player was born, as recorded in the PWHL HockeyTech roster. |
| `height` | String | Player height in inches. |
| `weight` | String | Player weight in pounds. |
| `height_hyphenated` | String | Player's height formatted as feet-inches with a hyphen separator (e.g., '5-9'), as supplied by the PWHL HockeyTech API. |
| `hidden` | String | Flag indicating whether the player's roster entry is suppressed from public-facing displays in the PWHL HockeyTech system. |
| `current_team` | String | Name or identifier of the PWHL team to which the player is currently rostered. |
| `player_id` | Int64 | Unique player identifier. |
| `status` | String | Status string (e.g. captain markers). |
| `birthdate` | String | Date of birth. |
| `birthdate_year` | String | Player birth year. |
| `rawbirthdate` | String | Player's birth date as a raw string in the format returned by the PWHL HockeyTech API, typically YYYY-MM-DD. |
| `latest_team_id` | Int64 | Most recent team identifier. |
| `veteran_status` | String | Player veteran status. |
| `veteran_description` | String | Text label or descriptor indicating the player's veteran status or experience classification in the PWHL. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `division` | String | Division identifier. |
| `tp_jersey_number` | String | Jersey number assigned to the player on the current PWHL team roster, as provided by the HockeyTech feed. |
| `rookie` | String | Whether the player is a rookie. |
| `position_id` | Int64 | Official position identifier. |
| `position` | String | Player position. |
| `nhlteam` | String | Name or identifier of the NHL organization that holds the player's NHL rights, if applicable. |
| `player_id_1` | String | Alternate or secondary HockeyTech player identifier, distinct from the primary person_id and player_id fields. |
| `is_rookie` | String | Whether the player is a rookie. |
| `h` | String |  |
| `w` | String |  |
| `draft_status` | String | Text description of the player's draft history or eligibility status (e.g., undrafted, drafted year and round). |
| `name` | String | Player full name, first then last (e.g. 'Megan Keller'). |
| `player_image` | String | URL of the player's official roster photograph from the PWHL HockeyTech feed. |
| `catches` | String | Catching hand (goalies). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_team_roster
df = pwhl_team_roster(team_id=1, season=2025)

# Next step

import polars as pl
df.filter(pl.col("position") == "G")
```
