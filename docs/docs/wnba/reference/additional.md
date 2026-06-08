---
title: WNBA — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# WNBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wnba`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_wnba_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull the officials assigned to a WNBA game.

See `sportsdataverse.wbb.espn_wbb_game_officials` for full
documentation of the column set, the empty-frame fallback when ESPN
ships no officials, and the `raw` / `return_as_pandas` flag
semantics.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN WNBA event identifier (e.g. `401620238` for Game 1 of the 2024 WNBA Finals). |
| `season` | `int \| None` | `None` | Season year (recorded as output column only). |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

Polars (or pandas) DataFrame with the same columns documented in `sportsdataverse.wbb.espn_wbb_game_officials`. If `raw=True`, returns the raw response dict.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `official_id` | character | Unique official / referee identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `full_name` | character | Player's full name. |
| `display_name` | character | Display name. |
| `position_id` | character | Unique position identifier. |
| `position_name` | character | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | character | Position display name. |
| `order` | integer | Display order within the result set. |

**Example**

```python
from sportsdataverse.wnba import espn_wnba_game_officials
refs = espn_wnba_game_officials(game_id=401620238, season=2024)
print(refs.shape)
refs.select(["full_name", "position_name", "order"]).head()

# Pandas round-trip

refs_pd = espn_wnba_game_officials(
    game_id=401620238, season=2024, return_as_pandas=True
)
refs_pd[["full_name", "position_name"]].head()

# Inspect the raw ESPN payload (e.g. for fields not flattened)

payload = espn_wnba_game_officials(game_id=401620238, season=2024, raw=True)
list(payload.keys())[:8]
```

### `espn_wnba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a WNBA athlete's ESPN **season** stat line.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}`
stat columns, the athlete / team metadata blocks, and the
`season_type` / `total` parameters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN WNBA athlete identifier (e.g. `3149391` for A'ja Wilson). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`. See `sportsdataverse.wbb.espn_wbb_player_stats` for the column layout.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `total` | logical | Total. |
| `athlete_id` | integer | Unique athlete identifier (ESPN). |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `full_name` | character | Player's full name. |
| `display_name` | character | Display name. |
| `short_name` | character | Short display name. |
| `weight` | double | Player weight in pounds. |
| `display_weight` | character | Player weight in display format (e.g. '180 lbs'). |
| `height` | double | Player height (string e.g. '6-2' or inches). |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `jersey` | character | Jersey number worn by the player. |
| `slug` | character | URL-safe identifier. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `position_id` | integer | Unique position identifier. |
| `position_name` | character | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation ('G' / 'F' / 'C'). |
| `college_name` | character | College name. |
| `status_id` | integer | Status identifier. |
| `status_name` | character | Status label. |
| `defensive_blocks` | double | Short for blocked shot, number of times when a defensive player legally deflects a field goal attempt from an offensive player. |
| `defensive_defensive_rebounds` | double | The number of times when the defense obtains the possession of the ball after a missed shot by the offense. |
| `defensive_steals` | double | The number of times a defensive player forced a turnover by intercepting or deflecting a pass or a dribble of an offensive player. |
| `defensive_avg_defensive_rebounds` | double | The average defensive rebounds per game. |
| `defensive_avg_blocks` | double | The average blocks per game. |
| `defensive_avg_steals` | double | The average steals per game. |
| `defensive_avg48_defensive_rebounds` | double |  |
| `defensive_avg48_blocks` | double |  |
| `defensive_avg48_steals` | double |  |
| `general_disqualifications` | double | The number of times a player reached the foul limit. |
| `general_flagrant_fouls` | double | The number of fouls that the officials thought were unnecessary or excessive. |
| `general_fouls` | double | The number of times a player had illegal contact with the opponent. |
| `general_ejections` | double | The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_technical_fouls` | double | The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_rebounds` | double | The total number of rebounds (offensive and defensive). |
| `general_vorp` | double | Value Over Replacement Player. |
| `general_minutes` | double | The total number of minutes played. |
| `general_avg_minutes` | double | The average number of minutes per game. |
| `general_fantasy_rating` | double | The Fantasy Rating of a player. |
| `general_nba_rating` | double | General nba rating. |
| `general_plus_minus` | double | A player's estimated on-court impact on team performance measured in point differential per 100 possessions. |
| `general_avg_rebounds` | double | The average rebounds per game. |
| `general_avg_fouls` | double | The average fouls committed per game. |
| `general_avg_flagrant_fouls` | double | The average number of flagrant fouls per game. |
| `general_avg_technical_fouls` | double | The average number of technical fouls per game. |
| `general_avg_ejections` | double | The average ejections per game. |
| `general_avg_disqualifications` | double | The average number of disqualifications per game. |
| `general_assist_turnover_ratio` | double | The average number of assists a player or team records per turnover. |
| `general_steal_foul_ratio` | double | The average number of steals a player or team records per foul committed. |
| `general_block_foul_ratio` | double | The average number of blocks a player or record per foul committed. |
| `general_avg_team_rebounds` | double | The average number of rebounds for a team per game. |
| `general_total_rebounds` | double | The total number of rebounds for a team or player. |
| `general_total_technical_fouls` | double | The total number of technical fouls for a team or player. |
| `general_team_assist_turnover_ratio` | double | The number of assists per turnover for a team. |
| `general_steal_turnover_ratio` | double | The number of steals per turnover. |
| `general_avg48_rebounds` | double |  |
| `general_avg48_fouls` | double |  |
| `general_avg48_flagrant_fouls` | double |  |
| `general_avg48_technical_fouls` | double |  |
| `general_avg48_ejections` | double |  |
| `general_avg48_disqualifications` | double |  |
| `general_games_played` | double | Games Played. |
| `general_games_started` | double | The number of games started by an athlete. |
| `general_double_double` | double | The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | double | The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `offensive_assists` | double | The number of times a player who passes the ball to a teammate in a way that leads to a score by field goal, meaning that he or she was "assisting" in the basket. There is some judgment involved in deciding whether a passer should be credited with an assist. |
| `offensive_field_goals` | double | Field Goal makes and attempts. |
| `offensive_field_goals_attempted` | double | The number of times a 2pt field goal was attempted. |
| `offensive_field_goals_made` | double | The number of times a 2pt field goal was made. |
| `offensive_field_goal_pct` | double | The ratio of field goals made to field goals attempted: FGM / FGA. |
| `offensive_free_throws` | double | Free Throw makes and attempts. |
| `offensive_free_throw_pct` | double | The ratio of free throws made to free throws attempted: FTM / FTA. |
| `offensive_free_throws_attempted` | double | The number of times a free throw was attempted. |
| `offensive_free_throws_made` | double | The number of times a free throw was made. |
| `offensive_offensive_rebounds` | double | The number of times when the offense obtains the possession of the ball after a missed shot. |
| `offensive_points` | double | The number of points scored. |
| `offensive_turnovers` | double | The number of times a player loses possession to the other team. |
| `offensive_three_point_pct` | double | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_three_point_field_goals_attempted` | double | The number of times a 3pt field goal was attempted. |
| `offensive_three_point_field_goals_made` | double | The number of times a 3pt field goal was made. |
| `offensive_total_turnovers` | double | The number of turnovers plus team turnovers for the team. |
| `offensive_points_in_paint` | double | The amount of points scored in the area known as "the Paint"(the rectangle between the foul line and the baseline). |
| `offensive_brick_index` | double | How many points a player costs his team with his shooting compared with the league average on a per-40-minute basis. ((52.8 - TS%) x (FGA + (FTA x 0.44))) / (Min/40) . |
| `offensive_avg_field_goals_made` | double | The average field goals made per game. |
| `offensive_avg_field_goals_attempted` | double | The average field goals attempted per game. |
| `offensive_avg_three_point_field_goals_made` | double | The average three point field goals made per game. |
| `offensive_avg_three_point_field_goals_attempted` | double | The average three point field goals attempted per game. |
| `offensive_avg_free_throws_made` | double | The average free throw shots made per game. |
| `offensive_avg_free_throws_attempted` | double | The average free throw shots attempted per game. |
| `offensive_avg_points` | double | The average number of points scored per game. |
| `offensive_avg_offensive_rebounds` | double | The average offensive rebounds per game. |
| `offensive_avg_assists` | double | The average assists per game. |
| `offensive_avg_turnovers` | double | The average turnovers committed per game. |
| `offensive_offensive_rebound_pct` | double | The percentage of the number of times they obtain the possession of the ball after a missed shot. |
| `offensive_estimated_possessions` | double | An estimation of the number of possessions for a team or player. |
| `offensive_avg_estimated_possessions` | double | The average number of estimated possessions per game for a team or player. |
| `offensive_points_per_estimated_possessions` | double | The number of points per estimated possession for a team or player. |
| `offensive_avg_team_turnovers` | double | The average number of turnovers for a team per game. |
| `offensive_avg_total_turnovers` | double | The average number of total turnovers for a team per game. |
| `offensive_three_point_field_goal_pct` | double | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_two_point_field_goals_made` | double | The number of 2-point field goals made for a team or player. |
| `offensive_two_point_field_goals_attempted` | double | The number of 2-point field goals attempted for a team or player. |
| `offensive_avg_two_point_field_goals_made` | double | The number of 2-point field goals made per game for a team or player. |
| `offensive_avg_two_point_field_goals_attempted` | double | The number of 2-point field goals attempted per game for a team or player. |
| `offensive_two_point_field_goal_pct` | double | The percentage of 2-points fields goals made by a team or player. |
| `offensive_shooting_efficiency` | double | The efficiency with which a team or player shoots the basketball. |
| `offensive_scoring_efficiency` | double | The efficiency with which a team or player scores the basketball. |
| `offensive_avg48_field_goals_made` | double |  |
| `offensive_avg48_field_goals_attempted` | double |  |
| `offensive_avg48_three_point_field_goals_made` | double |  |
| `offensive_avg48_three_point_field_goals_attempted` | double |  |
| `offensive_avg48_free_throws_made` | double |  |
| `offensive_avg48_free_throws_attempted` | double |  |
| `offensive_avg48_points` | double |  |
| `offensive_avg48_offensive_rebounds` | double |  |
| `offensive_avg48_assists` | double |  |
| `offensive_avg48_turnovers` | double |  |
| `team_id` | integer | Unique team identifier. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_guid` | character | ESPN team GUID. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | character | Team city or location string. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_color` | character | Team primary color (hex without leading '#'). |
| `team_alternate_color` | character | Team alternate color (hex without leading '#'). |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_logo_href` | character | Default team logo URL; `team_detail = TRUE` only. |

**Example**

```python
from sportsdataverse.wnba import espn_wnba_player_stats
df = espn_wnba_player_stats(athlete_id=3149391, season=2024)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### `espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wnba_schedule - look up the WNBA schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` |  | `False` |  |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character | Unique play identifcation number |
| `uid` | character | ESPN UID string. |
| `date` | character | Date in YYYY-MM-DD format. |
| `attendance` | integer | Reported attendance. |
| `time_valid` | logical | Time valid. |
| `neutral_site` | logical | Neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | TRUE if play-by-play is available. |
| `recent` | logical | Recent. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `broadcast` | character | Broadcast information string. |
| `highlights` | integer | Game highlight urls. |
| `notes_type` | character | Notes type. |
| `notes_headline` | character | Notes headline. |
| `broadcast_market` | character | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | character | Broadcast name. |
| `type_id` | character | Type identifier (numeric). |
| `type_abbreviation` | character | Play type abbreviation |
| `venue_id` | character | Unique venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `status_clock` | double | Status clock. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Status period. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status type state. |
| `status_type_completed` | logical | Status type completed. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `format_regulation_periods` | integer | Format regulation periods. |
| `home_id` | character | Unique identifier for home. |
| `home_uid` | character | Home team's uid. |
| `home_location` | character | Home team's location. |
| `home_name` | character | Home name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home display name. |
| `home_short_display_name` | character | Home short display name. |
| `home_color` | character | Color code (hex) for home. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_is_active` | logical | Home team's is active. |
| `home_venue_id` | character | Unique identifier for home venue. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team score at the time of the play. |
| `home_winner` | logical | Home team's winner. |
| `home_linescores` | integer |  |
| `home_records` | character |  |
| `away_id` | character | Unique identifier for away. |
| `away_uid` | character | Away team's uid. |
| `away_location` | character | Away team's location. |
| `away_name` | character | Away name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away display name. |
| `away_short_display_name` | character | Away short display name. |
| `away_color` | character | Color code (hex) for away. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_is_active` | logical | Away team's is active. |
| `away_venue_id` | character | Unique identifier for away venue. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team score at the time of the play. |
| `away_winner` | logical | Away team's winner. |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer | Unique game identifier. |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | integer | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

**Example**

```python
from sportsdataverse.wnba import espn_wnba_schedule
sched = espn_wnba_schedule(dates=20241011)  # 2024 WNBA Finals Game 1
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

# Pull a full regular season's worth of games

reg = espn_wnba_schedule(dates=2024, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)

# Pandas round-trip for a single date

espn_wnba_schedule(dates=20241011, return_as_pandas=True).head()
```

### `espn_wnba_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'`

Pull ESPN team season stats for a WNBA team.

See `sportsdataverse.wbb.espn_wbb_team_stats` for full
documentation of the return shape, the canonical three category keys
(`"Averages"`, `"Totals"`, `"Misc"`), the per-category column
set, and the `"Other"` fallback bucket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | ESPN WNBA team identifier (e.g. `17` for the Las Vegas Aces). |
| `season` | `int` |  | Season year, forwarded to ESPN as `?season=YYYY`. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a dict of pandas DataFrames; otherwise polars. |

**Returns**

Dict with one DataFrame per stat category — see `sportsdataverse.wbb.espn_wbb_team_stats` for the full column / key documentation. If `raw=True`, returns the raw response dict.

**Example**

```python
from sportsdataverse.wnba import espn_wnba_team_stats
frames = espn_wnba_team_stats(team_id=17, season=2024)
sorted(frames.keys())  # 'Averages', 'Totals', 'Misc' (plus optional 'Other')
frames["Averages"].head()

# Compare per-game and totals at a glance

avgs = frames["Averages"]
totals = frames["Totals"]
print(avgs.shape, totals.shape)
avgs.select(["games_played", "points_per_game", "rebounds_per_game"])

# Pandas round-trip

frames_pd = espn_wnba_team_stats(team_id=17, season=2024, return_as_pandas=True)
frames_pd["Misc"].head()
```

## Utilities & helpers

### `most_recent_wnba_season()`

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

Returns the current calendar year if it's May or later (the WNBA regular
season has tipped off), otherwise the previous calendar year.

**Returns**

Year (e.g. `2024`) suitable for passing as a `season` argument to schedule / loader functions.

**Example**

```python
from sportsdataverse.wnba import most_recent_wnba_season, espn_wnba_calendar
season = most_recent_wnba_season()
cal = espn_wnba_calendar(season=season)
print(season, cal.height)
```

## Other

### `espn_wnba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wnba_teams - look up WNBA teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.wnba.espn_wnba_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_alternate_color` | character | Team alternate color (hex without leading '#'). |
| `team_color` | character | Team primary color (hex without leading '#'). |
| `team_display_name` | character | Full team display name. |
| `team_id` | character | Unique team identifier. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_is_all_star` | logical | TRUE if the row represents an All-Star team. |
| `team_location` | character | Team city or location string. |
| `team_logos` | integer |  |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Example**

```python
from sportsdataverse.wnba import espn_wnba_teams
teams = espn_wnba_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Las Vegas Aces (team_id 17)

teams.filter(__import__("polars").col("team_id") == "17").to_dicts()

# Refresh the cache (the call is ``lru_cache``'d)

espn_wnba_teams.cache_clear()  # cached at function-level
teams_pd = espn_wnba_teams(return_as_pandas=True)
```

### `scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### `wnba_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |
