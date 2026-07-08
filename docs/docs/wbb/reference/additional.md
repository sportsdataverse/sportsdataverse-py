---
title: WBB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# WBB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wbb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_wbb_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_wbb_game_officials}

Pull the officials assigned to a women's-college-basketball game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN event identifier (e.g. `401637613` for the 2024 NCAA Division I women's championship game). |
| `season` | `int \| None` | `None` | Season year. Recorded as the `season` column on the output; does NOT alter the request URL because ESPN's officials endpoint keys on event ID alone. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

Polars (or pandas) DataFrame with one row per official: `game_id`, `season`, `official_id`, `first_name`, `last_name`, `full_name`, `display_name`, `position_id`, `position_name`, `position_display_name`, `order`. When ESPN ships no officials for the game (often for unscheduled or future events), an empty frame with the documented schema is returned so callers see a stable column set. If `raw=True`, returns the raw response dict.

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
from sportsdataverse.wbb import espn_wbb_game_officials
officials = espn_wbb_game_officials(game_id=401587902, season=2024)
print(officials.shape)
officials.select(["full_name", "position_display_name", "order"]).head()

# Pandas round-trip

officials_pd = espn_wbb_game_officials(
    game_id=401587902, season=2024, return_as_pandas=True
)
officials_pd.head()

# Raw payload (skip the cleaning pipeline)

raw = espn_wbb_game_officials(
    game_id=401587902, season=2024, raw=True
)
sorted(raw.keys())
```

### `espn_wbb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_wbb_game_rosters}

espn_wbb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from wbb_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer | Unique athlete identifier (ESPN). |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `full_name` | character | Player's full name. |
| `athlete_display_name` | character | Athlete display name (full). |
| `short_name` | character | Short display name. |
| `height` | double | Player height (string e.g. '6-2' or inches). |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `slug` | character | URL-safe identifier. |
| `jersey` | character | Jersey number worn by the player. |
| `linked` | logical | TRUE if the record is linked to a related entity. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_country_alternate_id` | character | Alternate identifier for the athlete's country of birth used in ESPN's country-flag system. |
| `birth_country_abbreviation` | character | Birth country abbreviation. |
| `headshot_href` | character | Headshot image URL. |
| `headshot_alt` | character | Alternative-text label for the headshot. |
| `flag_href` | character | URL of the SVG or PNG flag image representing the athlete's country of birth. |
| `flag_alt` | character | Alt-text string for the athlete's country-of-birth flag image, typically the country name. |
| `flag_rel` | character | Relationship descriptor for the athlete's country-of-birth flag link (e.g., "flag"). |
| `experience_years` | integer | Experience years. |
| `experience_display_value` | character | Experience display value. |
| `experience_abbreviation` | character | Experience abbreviation. |
| `status_id` | character | Status identifier. |
| `status_name` | character | Status label. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `weight` | double | Player weight in pounds. |
| `display_weight` | character | Player weight in display format (e.g. '180 lbs'). |
| `starter` | logical | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `jersey_right` | character | Jersey number displayed on the right side of the roster card, used for alternate or secondary number representations. |
| `valid` | logical | Valid. |
| `did_not_play` | logical | TRUE if the player did not appear in the game. |
| `display_name` | character | Display name. |
| `ejected` | logical | TRUE if the player was ejected from the game. |
| `athlete_href` | character | ESPN API resource URL for the athlete's full profile endpoint. |
| `position_href` | character | ESPN API resource URL for the athlete's position resource. |
| `statistics_href` | character | ESPN API resource URL pointing to the athlete's statistics endpoint. |
| `team_id` | integer | Unique team identifier. |
| `order` | integer | Display order within the result set. |
| `home_away` | character | Game venue label ('home' or 'away'). |
| `winner` | logical | Winner. |
| `team_guid` | character | ESPN team GUID. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | character | Team city or location string. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | character | Team nickname. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_color` | character | Team primary color (hex without leading '#'). |
| `team_alternate_color` | character | Team alternate color (hex without leading '#'). |
| `is_active` | logical | Whether the team was active in this season. |
| `is_all_star` | logical | Is all star. |
| `team_alternate_ids_sdr` | character | Alternate team identifier from ESPN's SDR (Sports Data Reference) system for the athlete's team. |
| `logo_href` | character | Team or league logo URL. |
| `logo_dark_href` | character | Logo URL for dark backgrounds. |
| `game_id` | integer | Unique game identifier. |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_game_rosters
roster = espn_wbb_game_rosters(game_id=401587902)
print(roster.shape)

# Identify starters

import polars as pl
starters = roster.filter(pl.col("starter") == True).select(
    ["full_name", "jersey", "team_display_name"]
)

# Pandas round-trip

roster_pd = espn_wbb_game_rosters(game_id=401587902, return_as_pandas=True)
roster_pd.head()
```

### `espn_wbb_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'` {#espn_wbb_pbp}

espn_wbb_pbp() - Pull the game by id. Data from API endpoints - `womens-college-basketball/playbyplay`,

`womens-college-basketball/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from wbb_schedule(). |
| `raw` | `bool` | `False` | If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets. |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor","espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.wbb import espn_wbb_pbp
game = espn_wbb_pbp(game_id=401587902)
print(game["gameId"])
print(len(game["plays"]))

# Convert plays to a DataFrame and filter shooting plays

import polars as pl
plays = pl.DataFrame(game["plays"])
shots = plays.filter(pl.col("scoring_play") | pl.col("shooting_play"))
shots.select(["period_number", "clock_display_value", "team_id", "coordinate_x", "coordinate_y", "score_value", "text"]).head()

# Convert to pandas for downstream analysis

import pandas as pd
shots_pd = pd.DataFrame(game["plays"])
shots_pd[shots_pd["shooting_play"] == True].head()

# Raw payload (skip the cleaning pipeline) for debugging

raw = espn_wbb_pbp(game_id=401587902, raw=True)
sorted(raw.keys())
```

### `espn_wbb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_wbb_player_stats}

Pull a women's-college-basketball athlete's ESPN **season** stat line.

Returns **one wide row** combining athlete identity, the season stat
line pivoted as `{category}_{stat}` columns, and team identity. For
the richer multi-category web-v3 payload use
`espn_wbb_player_stats_v3` instead.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN athlete identifier (e.g. `4433985`). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). Columns: identity / echo (`season`, `season_type`, `total`), athlete metadata (`athlete_id`, `full_name`, `position_*`, ...), the season stat line as `{category}_{stat}` numeric columns (e.g. `offensive_points`, `defensive_blocks`), and team metadata (`team_id`, `team_display_name`, ...). When `raw=True` returns the raw statistics JSON `dict`.

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
| `weight` | character | Player weight in pounds. |
| `display_weight` | character | Player weight in display format (e.g. '180 lbs'). |
| `height` | double | Player height (string e.g. '6-2' or inches). |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `age` | character | Player age (in years). |
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
| `defensive_turnover_points` | double | The amount of points resulting from the possession following a turnover. |
| `defensive_avg_defensive_rebounds` | double | The average defensive rebounds per game. |
| `defensive_avg_blocks` | double | The average blocks per game. |
| `defensive_avg_steals` | double | The average steals per game. |
| `general_disqualifications` | double | The number of times a player reached the foul limit. |
| `general_flagrant_fouls` | double | The number of fouls that the officials thought were unnecessary or excessive. |
| `general_fouls` | double | The number of times a player had illegal contact with the opponent. |
| `general_per` | double | A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_ejections` | double | The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_technical_fouls` | double | The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_rebounds` | double | The total number of rebounds (offensive and defensive). |
| `general_minutes` | double | The total number of minutes played. |
| `general_avg_minutes` | double | The average number of minutes per game. |
| `general_fantasy_rating` | double | The Fantasy Rating of a player. |
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
| `general_steal_turnover_ratio` | double | The number of steals per turnover. |
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
| `offensive_three_point_field_goals_attempted` | double | The number of times a 3pt field goal was attempted. |
| `offensive_three_point_field_goals_made` | double | The number of times a 3pt field goal was made. |
| `offensive_total_turnovers` | double | The number of turnovers plus team turnovers for the team. |
| `offensive_points_in_paint` | double | The amount of points scored in the area known as "the Paint"(the rectangle between the foul line and the baseline). |
| `offensive_second_chance_points` | double | Points scored by the player on offensive-rebound put-back opportunities during the season. |
| `offensive_fast_break_points` | double | The number of points scored on fast breaks. |
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
from sportsdataverse.wbb import espn_wbb_player_stats
df = espn_wbb_player_stats(athlete_id=4433985, season=2025)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### `espn_wbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_wbb_schedule}

espn_wbb_schedule - look up the women's college basketball schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `groups` | `int` | `50` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character | Unique play identifcation number |
| `uid` | character | ESPN UID string. |
| `date` | character | Date in YYYY-MM-DD format. |
| `attendance` | integer | Reported attendance. |
| `time_valid` | logical | Whether the start time is confirmed. |
| `neutral_site` | logical | Neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Whether the game is recent. |
| `tournament_id` | integer | ESPN tournament identifier. |
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
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current period. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `format_regulation_periods` | integer | Format regulation periods. |
| `home_id` | character | Unique identifier for home. |
| `home_uid` | character | Home team's uid. |
| `home_location` | character | Home team's location. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_short_display_name` | character | Home short display name. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_is_active` | logical | Home team's is active. |
| `home_venue_id` | character | Unique identifier for home venue. |
| `home_logo` | character | Home team logo URL. |
| `home_conference_id` | character | Unique identifier for home conference. |
| `home_score` | character | Home team score at the time of the play. |
| `home_winner` | logical | Whether the home team won. |
| `home_current_rank` | integer | Current AP/coaches poll ranking of the home team at the time of the game. |
| `home_linescores` | list | Points scored by the home team in each period or half of the game. |
| `home_records` | character | Win-loss record string for the home team at the time of the game. |
| `away_id` | character | Unique identifier for away. |
| `away_uid` | character | Away team's uid. |
| `away_location` | character | Away team's location. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_short_display_name` | character | Away short display name. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_is_active` | logical | Away team's is active. |
| `away_venue_id` | character | Unique identifier for away venue. |
| `away_logo` | character | Away team logo URL. |
| `away_conference_id` | character | Unique identifier for away conference. |
| `away_score` | character | Away team score at the time of the play. |
| `away_winner` | logical | Whether the away team won. |
| `away_current_rank` | integer | Current AP/coaches poll ranking of the away team at the time of the game. |
| `away_linescores` | list | Points scored by the away team in each period or half of the game. |
| `away_records` | character | Win-loss record string for the away team at the time of the game. |
| `game_id` | integer | Unique game identifier. |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | integer | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_schedule
day = espn_wbb_schedule(dates=20240407)
print(day.shape)

# Season-level pull (2024 season)

season = espn_wbb_schedule(dates=2024, limit=1500)
print(season.shape)

# Filter to a specific team (UConn ``team_id=2509``)

import polars as pl
uconn = season.filter(
    (pl.col("home_id") == "2509") | (pl.col("away_id") == "2509")
)

# Pandas round-trip

season_pd = espn_wbb_schedule(dates=2024, return_as_pandas=True)
season_pd.head()
```

### `espn_wbb_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'` {#espn_wbb_team_stats}

Pull ESPN team season stats for a women's-college-basketball team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | ESPN team identifier (e.g. `2509` for UConn). |
| `season` | `int` |  | Season year, forwarded to ESPN as `?season=YYYY`. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a dict of pandas DataFrames; otherwise polars. |

**Returns**

Dict with one DataFrame per stat category. The canonical keys `"Averages"`, `"Totals"`, `"Misc"` are ALWAYS present; missing categories come back as empty frames carrying the documented schema. Any ESPN-shipped category whose name does not match one of the three canonical keys is collected under an additional `"Other"` key (only added if non-empty). Per-category column set (one row per stat): * `stat_name` (Utf8) * `abbreviation` (Utf8) * `display_value` (Utf8) * `value` (Float64) * `description` (Utf8) * `category` (Utf8, constant per frame) * `team_id` (Int64, constant) * `season` (Int32, constant) If `raw=True`, returns the raw response dict.

**Example**

```python
from sportsdataverse.wbb import espn_wbb_team_stats
frames = espn_wbb_team_stats(team_id=2509, season=2025)
print(sorted(frames.keys()))

# Index into a specific table

averages = frames["Averages"]
print(averages.shape)
averages.select(["stat_name", "display_value", "value"]).head()

# Iterate the canonical categories

for cat in ("Averages", "Totals", "Misc"):
    print(cat, frames[cat].shape)

# ``Other`` fallback bucket (only present when ESPN ships a category that does not map onto one of the three canonical keys)

if "Other" in frames:
    frames["Other"].select(["category", "stat_name", "value"])

# Pandas round-trip

frames_pd = espn_wbb_team_stats(
    team_id=2579, season=2025, return_as_pandas=True
)  # team_id 2579 = South Carolina
frames_pd["Averages"].head()
```

## Dataset loaders

### `load_artifact(name: 'str') -> 'dict'` {#load_artifact}

Read a bundled player-value artifact (`mbb/models/<name>.json`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Artifact stem, e.g. `"mbb_box_bpm"`. |

**Returns**

The parsed JSON dict.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import load_artifact
art = load_artifact("mbb_box_bpm")
```

### `load_proxybonanza_pool(api_key: 'str', pkg: 'str', *, transport: 'Optional[PoolTransport]' = None) -> "'list[str]'"` {#load_proxybonanza_pool}

Resolve a ProxyBonanza package into a list of `http://login:pass@ip:port` URLs.

Graduated from `dev/ncaa_proxy.py`'s `load_proxy_pool` -- same
endpoint shape, minus the `.Renviron` reader (creds are now explicit
params, per the creds-hygiene directive).

Endpoint: `GET https://api.proxybonanza.com/v1/userpackages/{pkg}.json`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `api_key` | `str` |  | ProxyBonanza API key. |
| `pkg` | `str` |  | ProxyBonanza package id. |
| `transport` | `Optional[PoolTransport]` | `None` | Injectable `(url, headers) -> (status, text)` callable for offline testing. Defaults to a curl_cffi GET. |

**Returns**

One `http://login:password@ip:port` URL per IP in the package.

**Example**

```python
def fake(url, headers):
    return 200, '{"data": {"login": "u", "password": "p", "ippacks": []}}'
pool = load_proxybonanza_pool("key", "pkg", transport=fake)
```

## Utilities & helpers

### `most_recent_wbb_season()` {#most_recent_wbb_season}

Return the most recent women's college basketball season year.

The women's college basketball season spans late October through early
April; for any month October-December the "current season" is the
following calendar year (e.g. October 2025 returns `2026`).

**Returns**

The most recent / current season year.

**Example**

```python
from sportsdataverse.wbb import most_recent_wbb_season, espn_wbb_schedule
season = most_recent_wbb_season()
sched = espn_wbb_schedule(dates=season)
```

## Other

### `AssistEvent(player_code: 'str', count: 'ShotClockStats' = <factory>) -> None` {#AssistEvent}

One assist relationship's counts (`LineupEventStats.AssistEvent`,

`LineupEventStats.scala:64-67`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_code` | `str` |  | The other player in the assist event (by code). |
| `count` | `ShotClockStats` | `<factory>` | The assist counts, by shot-clock segment. |

### `AssistInfo(counts: 'ShotClockStats' = <factory>, target: 'Optional[list[AssistEvent]]' = None, source: 'Optional[list[AssistEvent]]' = None) -> None` {#AssistInfo}

Detailed assist info, split into given/received

(`LineupEventStats.AssistInfo`, `LineupEventStats.scala:87-91`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `counts` | `ShotClockStats` | `<factory>` | Raw assist statistics. |
| `target` | `Optional[list[AssistEvent]]` | `None` | Players "I" assisted, if tracked. |
| `source` | `Optional[list[AssistEvent]]` | `None` | Players who assisted "me", if tracked. |

### `BadLineupClump(evs: 'list[LineupEvent]', next_good: 'Optional[LineupEvent]' = None) -> None` {#BadLineupClump}

A run of consecutive bad :class:`~sportsdataverse.mbb.mbb_ncaa_models

.LineupEvent`\ s that were merged together, plus the first following
good event (`LineupErrorAnalysisUtils.BadLineupClump`, `:223-226`).

The Scala case class is `protected` (module-private), but this port
exports it: the Task 5d.3 fixers and the (not-yet-ported) Task 5e
orchestrator both consume `BadLineupClump` instances directly, so
keeping it private here would just force every caller to reach past a
leading underscore.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `list[LineupEvent]` |  | The clumped lineup events, in chronological order. |
| `next_good` | `Optional[LineupEvent]` | `None` | The first known-good lineup event following the clump, if any -- used by the Task 5d.3 fixers to reason about a player who should have subbed back in. |

### `ConcurrentClump(evs: 'list[RawGameEvent]' = <factory>, lineups: 'list[LineupEvent]' = <factory>) -> None` {#ConcurrentClump}

A clump of concurrent raw events, together with the lineups that end

in that clump (`Concurrency.ConcurrentClump`, `PossessionUtils.scala
:64-69`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `list[RawGameEvent]` | `<factory>` | The raw game events in this clump, in chronological order. |
| `lineups` | `list[LineupEvent]` | `<factory>` | The lineups (if any) whose `end_min` falls in this clump. |

### `ConferenceId(name: 'str') -> None` {#ConferenceId}

CBB conference identifier (`ConferenceId`, ``models/ConferenceId

.scala:7`, `AnyVal`). **Scope addition, Task 5e.4** -- the first model
consumed by `mbb_ncaa_team_parsers.py` (`TeamIdParser.get_team_triples`
/ `build_lineup_cli_array` / `build_available_team_list``). Appended
here (not inserted among the 5a-reviewed classes above) to keep this an
additive-only change, matching `RosterEntry`'s precedent.

**`ConferenceId.is_high_major` (the companion object's other member,
`models/ConferenceId.scala:11-16`) is NOT ported.** It has no call site
anywhere in `TeamIdParser`/`TeamScheduleParser` (verified: the only
other `ConferenceId` construction sites in the upstream tree are
`kenpom/TeamParser.scala` and `BuildIngestPipeline.scala`, neither of
which is in this port's scope, and neither calls `is_high_major`
either) -- nothing in Phase 5e would exercise it. Noted here rather than
silently dropped, matching this module's precedent for other
unreferenced companion-object members (see the module docstring's
`Year.until` / `Game.Score.by_winner` notes).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The unique name of the conference. |

### `CutdownShotEvent(loc: 'Optional[ShotLocation]', geo: 'Optional[ShotGeo]', dist: 'Optional[float]', pts: 'int', value: 'int', is_ast: 'Optional[bool]', is_trans: 'Optional[bool]', is_orb: 'Optional[bool]') -> None` {#CutdownShotEvent}

A narrowed `ShotEvent`, keeping only the fields needed once a

shot has been matched to a player/lineup event (`CutdownShotEvent`,
`models/ncaa/ShotEvent.scala:31-40`). **Scope addition, Task 5e.5** --
ported for shape fidelity even though **it is dead code in the ENTIRE
upstream tree**: grepping shows it appears only in its own definition
(`ShotEvent.scala`) and as the never-populated `shot_info:
Option[CutdownShotEvent]` field on `PlayerEvent.scala` -- it is never
constructed anywhere. (`PlayByPlayUtils.shot_value` is an UNRELATED
`event_str -> int` point-value classifier that merely shares a similar
name -- Task 5e.6 will NOT produce this type either.) Appended here (not
inserted among the 5a-reviewed classes above) to keep this an
additive-only change, matching `RosterEntry`/`ConferenceId`'s
precedent.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `loc` | `Optional[ShotLocation]` |  | The shot's court location, in feet, if known. |
| `geo` | `Optional[ShotGeo]` |  | The shot's synthetic lat/lon, if known. |
| `dist` | `Optional[float]` |  | The shot's distance from the basket, in feet, if known. |
| `pts` | `int` |  | The point value if made (`2`/`3`), else `0`. |
| `value` | `int` |  | The shot's attempt value (`2`/`3`), regardless of make/miss. |
| `is_ast` | `Optional[bool]` |  | Whether the shot was assisted, if known. |
| `is_trans` | `Optional[bool]` |  | Whether the shot was in transition, if known. |
| `is_orb` | `Optional[bool]` |  | Whether the shot followed an offensive rebound, if known. |

### `Direction(*values)` {#Direction}

Which team is in possession (`RawGameEvent.Direction`, `:119-121`).

### `FieldAverage(league_off: 'float', league_def: 'float', hca_off: 'float', hca_def: 'float') -> None` {#FieldAverage}

League average + estimated HCA for one stat field (`ts:620-625`).

`league_off`/`league_def` are the possession-weighted league means of
the per-game raw rate; `hca_off`/`hca_def` are the residual-derived
home-court advantages the solver converged on.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_off` | `float` |  |  |
| `league_def` | `float` |  |  |
| `hca_off` | `float` |  |  |
| `hca_def` | `float` |  |  |

### `FieldGoalStats(attempts: 'ShotClockStats' = <factory>, made: 'ShotClockStats' = <factory>, ast: 'Optional[ShotClockStats]' = None) -> None` {#FieldGoalStats}

Field-goal counting stats (`LineupEventStats.FieldGoalStats`,

`LineupEventStats.scala:75-79`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `attempts` | `ShotClockStats` | `<factory>` | Shot attempts, successful or not. |
| `made` | `ShotClockStats` | `<factory>` | Successful shot attempts. |
| `ast` | `Optional[ShotClockStats]` | `None` | Successful shot attempts that were assisted, if tracked. |

### `FuzzyMatchError(message: 'str') -> None` {#FuzzyMatchError}

A failed `fuzzy_box_match` resolution (Scala's `Left[String]`

half of `Either[String, String]` -- Python has no `Either`, so the
error is returned directly; check `isinstance(result, FuzzyMatchError)`,
matching the `parse_team_name` / `~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError`
convention already used in this port).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `message` | `str` |  | Human-readable description of why no name won. |

### `GameBreakEvent(min: 'float', score: 'Score') -> None` {#GameBreakEvent}

A break in play (timeout, end of period, etc.) short of the end of

the game (`Model.GameBreakEvent`, `ExtractorUtils.scala:874-877`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |

**Methods**

#### `GameBreakEvent.with_min(new_min: 'float') -> "'GameBreakEvent'"`

Return a copy with `min` replaced (`:876`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### `GameEndEvent(min: 'float', score: 'Score') -> None` {#GameEndEvent}

The end of the game (`Model.GameEndEvent`, `ExtractorUtils.scala:878-881`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |

**Methods**

#### `GameEndEvent.with_min(new_min: 'float') -> "'GameEndEvent'"`

Return a copy with `min` replaced (`:880`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### `IterationResult(adj_values: ForwardRef('AdjValues'), hca_per_field: ForwardRef('HcaPerField'))` {#IterationResult}

Return of `run_iterative_adjustment_with_hca` (`ts:314-317`).

`adj_values` maps `team_name -> field -> {"off","def"}` (the converged
strength-of-schedule adjustment); `hca_per_field` maps `field ->
{"hca_off","hca_def"}`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_values` | `ForwardRef('AdjValues')` |  |  |
| `hca_per_field` | `ForwardRef('HcaPerField')` |  |  |

### `LeagueConstants(hfa: 'float', margin_sd: 'float', em_scale: 'float', avg_tempo: 'float', avg_efficiency: 'float', quad_thresholds: 'dict[str, dict[str, int]]', bubble_adj_em: 'float', in_game_wp_artifact: 'str') -> None` {#LeagueConstants}

Per-league fitted constants for the prediction & tournament stack.

Algorithms in the stack are league-agnostic; every men's/women's-specific
number lives here so a WBB caller is a by-reference shim plus this table
(the same pattern `wbb_rapm` / `wbb_ratings` already use).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hfa` | `float` |  | Home-court advantage in points (fitted on the 2024 backtest). |
| `margin_sd` | `float` |  | Std. dev. of the game-margin residual (fitted on the 2024 backtest; the Brier-minimizing sigma agrees to within 0.04). |
| `em_scale` | `float` |  | Slope applied to the AdjEM difference when predicting a game margin. AdjEM is per-100-possessions, so a game margin scales by ~tempo/100 (~0.67); the fitted value is lower still because the as-of AdjEM estimate is noisy and the optimal predictive slope is attenuated (regression dilution). Fitted jointly with `hfa`. |
| `avg_tempo` | `float` |  | League baseline possessions per game (adjusted-tempo anchor). |
| `avg_efficiency` | `float` |  | League baseline points per 100 possessions. |
| `quad_thresholds` | `dict[str, dict[str, int]]` |  | NET-style quadrant opponent-rank upper bounds, keyed by venue (`home` / `neutral` / `away`) then `q1` / `q2` / `q3` (Quad 4 is any opponent ranked worse than `q3`). |
| `bubble_adj_em` | `float` |  | AdjEM of a bubble-quality team on THIS engine's scale (mean of engine ranks 40-50 on the fit season) -- the WAB baseline. |
| `in_game_wp_artifact` | `str` |  | Filename of the bundled in-game-WP coefficients under `sportsdataverse/mbb/models` (fitted + committed in Phase 3). |

### `LineupBuildingState(curr: 'LineupEvent', tidy_ctx: "'TidyPlayerContext'", prev: 'list[LineupEvent]' = <factory>, old_format: 'Optional[bool]' = None) -> None` {#LineupBuildingState}

State for building raw lineup data across a fold over play-by-play

events (`Model.LineupBuildingState`, `ExtractorUtils.scala:735-819`).

See the module docstring's "`with_*` methods return NEW instances"
note -- every mutator below returns a fresh `LineupBuildingState`
rather than mutating `self`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr` | `LineupEvent` |  | The lineup event currently being built. |
| `tidy_ctx` | `TidyPlayerContext` |  | Name-resolution context for the current game (see `~sportsdataverse.mbb.mbb_ncaa_names.TidyPlayerContext`); threaded through `build_partial_lineup_list`, which calls `~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` with it on every sub event. |
| `prev` | `list[LineupEvent]` | `<factory>` | Completed lineup events, most-recently-completed first (i.e. the reverse of `build`'s output order). |
| `old_format` | `Optional[bool]` | `None` | `True` once latched onto the legacy (pre-2018-ish) NCAA play-by-play format, `None` until the first sub is seen. |

**Methods**

#### `LineupBuildingState.build() -> 'list[LineupEvent]'`

The full chronological lineup-event list (`:742-744`:

`(curr :: prev).reverse`).

#### `LineupBuildingState.is_active(min: 'float') -> 'bool'`

Whether the current lineup has non-sub activity, or has simply

been on the floor long enough to trust (`:760-766`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute to check against. |

**Returns**

`True` if any raw event on `curr` isn't an opponent sub, or if `min` is more than `SUB_SAFETY_DELTA_MINS` past `curr`'s `end_min`.

#### `LineupBuildingState.is_sub(raw: 'RawGameEvent') -> 'bool'`

Whether `raw` is an *opponent*-side substitution line

(`:749-758`).

Only `~sportsdataverse.mbb.mbb_ncaa_models.RawGameEvent.opponent`
is inspected -- per the Scala scaladoc, "opposition subs are
currently treated as game events but shouldn't result in new
lineups"; the team's own subs never reach here as raw events in the
first place (they route through the fold's dedicated `Sub*Event`
branches, not `with_team_event`), so this check only ever
needs to look at the opponent side. Ported verbatim, quirk and all.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `raw` | `RawGameEvent` |  | The raw event to classify. |

**Returns**

`True` if `raw.opponent` ends with one of the four substitution phrases (case/whitespace-insensitive), else `False` (including when `raw.opponent` is `None`).

#### `LineupBuildingState.with_latest_score(score: 'Score') -> "'LineupBuildingState'"`

Update `curr`'s running end-of-event score (`:788-796`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `score` | `Score` |  |  |

#### `LineupBuildingState.with_opponent_event(min: 'float', event_string: 'str') -> "'LineupBuildingState'"`

Append an opponent-side raw event and bump `end_min` (`:808-818`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  |  |
| `event_string` | `str` |  |  |

#### `LineupBuildingState.with_player_in(player_name: 'str') -> "'LineupBuildingState'"`

Prepend a new "subbed in" player code onto `curr` (`:770-778`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_name` | `str` |  |  |

#### `LineupBuildingState.with_player_out(player_name: 'str') -> "'LineupBuildingState'"`

Prepend a new "subbed out" player code onto `curr` (`:779-787`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_name` | `str` |  |  |

#### `LineupBuildingState.with_team_event(min: 'float', event_string: 'str') -> "'LineupBuildingState'"`

Append a team-side raw event and bump `end_min` (`:797-807`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  |  |
| `event_string` | `str` |  |  |

### `LineupEvent(date: 'datetime', location_type: 'LocationType', start_min: 'float', end_min: 'float', duration_mins: 'float', score_info: 'ScoreInfo', team: 'TeamSeasonId', opponent: 'TeamSeasonId', lineup_id: 'LineupId', players: 'list[PlayerCodeId]', players_in: 'list[PlayerCodeId]', players_out: 'list[PlayerCodeId]', raw_game_events: 'list[RawGameEvent]', team_stats: 'LineupEventStats', opponent_stats: 'LineupEventStats', player_count_error: 'Optional[int]' = None) -> None` {#LineupEvent}

A portion of a game during which a given lineup was on the floor

(`LineupEvent`, `LineupEvent.scala:41-58`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `datetime` |  | The date of the game. |
| `location_type` | `LocationType` |  | Home/away/neutral (etc.) for this game. |
| `start_min` | `float` |  | The point in the game at which the lineup entered. |
| `end_min` | `float` |  | The point in the game at which the lineup changed. |
| `duration_mins` | `float` |  | The duration of the lineup. |
| `score_info` | `ScoreInfo` |  | The score differential context for this event. |
| `team` | `TeamSeasonId` |  | The team under analysis. |
| `opponent` | `TeamSeasonId` |  | The opposing team. |
| `lineup_id` | `LineupId` |  | A string that defines the set of players on the floor. |
| `players` | `list[PlayerCodeId]` |  | Mapping from player code to full identity, for this lineup. |
| `players_in` | `list[PlayerCodeId]` |  | Players who subbed in for this event. |
| `players_out` | `list[PlayerCodeId]` |  | Players who subbed out for this event. |
| `raw_game_events` | `list[RawGameEvent]` |  | The raw NCAA event strings for both teams. |
| `team_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (team side). |
| `opponent_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (opponent side). |
| `player_count_error` | `Optional[int]` | `None` | If the lineup is "impossible", the number of players actually seen (for analysis purposes). |

### `LineupEventStats(num_events: 'int' = 0, num_possessions: 'int' = 0, fg: 'FieldGoalStats' = <factory>, fg_rim: 'FieldGoalStats' = <factory>, fg_mid: 'FieldGoalStats' = <factory>, fg_2p: 'FieldGoalStats' = <factory>, fg_3p: 'FieldGoalStats' = <factory>, ft: 'FieldGoalStats' = <factory>, orb: 'Optional[ShotClockStats]' = None, drb: 'Optional[ShotClockStats]' = None, to: 'ShotClockStats' = <factory>, stl: 'Optional[ShotClockStats]' = None, blk: 'Optional[ShotClockStats]' = None, assist: 'Optional[ShotClockStats]' = None, ast_rim: 'Optional[AssistInfo]' = None, ast_mid: 'Optional[AssistInfo]' = None, ast_3p: 'Optional[AssistInfo]' = None, foul: 'Optional[ShotClockStats]' = None, player_shot_info: 'Optional[PlayerShotInfo]' = None, pts: 'int' = 0, plus_minus: 'int' = 0) -> None` {#LineupEventStats}

A lineup event's full counting-stat tree (`LineupEventStats`,

`LineupEventStats.scala:7-38`).

Only `num_events`/`num_possessions`/`pts`/`plus_minus` are
exercised by Phase 5a -- see the module docstring's scope note.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `num_events` | `int` | `0` | Number of raw events folded into this lineup event. |
| `num_possessions` | `int` | `0` | Number of possessions attributed to this lineup event. |
| `fg` | `FieldGoalStats` | `<factory>` | Overall field-goal stats. |
| `fg_rim` | `FieldGoalStats` | `<factory>` | Rim field-goal stats. |
| `fg_mid` | `FieldGoalStats` | `<factory>` | Mid-range field-goal stats. |
| `fg_2p` | `FieldGoalStats` | `<factory>` | 2pt field-goal stats. |
| `fg_3p` | `FieldGoalStats` | `<factory>` | 3pt field-goal stats. |
| `ft` | `FieldGoalStats` | `<factory>` | Free-throw stats. |
| `orb` | `Optional[ShotClockStats]` | `None` | Offensive-rebound stats, if tracked. |
| `drb` | `Optional[ShotClockStats]` | `None` | Defensive-rebound stats, if tracked. |
| `to` | `ShotClockStats` | `<factory>` | Turnover stats. |
| `stl` | `Optional[ShotClockStats]` | `None` | Steal stats, if tracked. |
| `blk` | `Optional[ShotClockStats]` | `None` | Block stats, if tracked. |
| `assist` | `Optional[ShotClockStats]` | `None` | Assist stats, if tracked. |
| `ast_rim` | `Optional[AssistInfo]` | `None` | Rim-shot assist info, if tracked. |
| `ast_mid` | `Optional[AssistInfo]` | `None` | Mid-range-shot assist info, if tracked. |
| `ast_3p` | `Optional[AssistInfo]` | `None` | 3pt-shot assist info, if tracked. |
| `foul` | `Optional[ShotClockStats]` | `None` | Foul stats, if tracked. |
| `player_shot_info` | `Optional[PlayerShotInfo]` | `None` | Per-player shot-quality info, if tracked. |
| `pts` | `int` | `0` | Points scored. |
| `plus_minus` | `int` | `0` | Point differential while this lineup was on the floor. |

### `LineupId(value: 'str') -> None` {#LineupId}

The set of players on the floor, as an opaque id string

(`LineupEvent.LineupId`, `LineupEvent.scala:172`, `AnyVal`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `value` | `str` |  | The opaque lineup identifier. |

### `LocationType(*values)` {#LocationType}

Game location (`Game.LocationType`, `Game.scala:36-38`).

### `NcaaFetchConfig(cache_dir: 'Optional[Path]' = None, proxy_url: 'Optional[str]' = None, proxybonanza_key: 'Optional[str]' = None, proxybonanza_pkg: 'Optional[str]' = None, timeout: 'int' = 45, impersonate: 'str' = 'chrome', max_retries: 'int' = 2, transport: 'Optional[FetchTransport]' = None) -> None` {#NcaaFetchConfig}

Runtime configuration for the stats.ncaa.org fetch layer.

Exactly one proxy source should be configured: either a single explicit
`proxy_url` (`http://login:password@ip:port`), or a ProxyBonanza pool
via `proxybonanza_key` + `proxybonanza_pkg` (resolved lazily by

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache_dir` | `Optional[Path]` | `None` |  |
| `proxy_url` | `Optional[str]` | `None` |  |
| `proxybonanza_key` | `Optional[str]` | `None` |  |
| `proxybonanza_pkg` | `Optional[str]` | `None` |  |
| `timeout` | `int` | `45` |  |
| `impersonate` | `str` | `'chrome'` |  |
| `max_retries` | `int` | `2` |  |
| `transport` | `Optional[FetchTransport]` | `None` |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import get_config
cfg = get_config()
cfg.cache_dir     # ~/.sportsdataverse/ncaa_cache
cfg.impersonate   # "chrome"

# Configure a single proxy explicitly (rarely needed -- prefer ``update_config`` or the env vars)

from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetchConfig
cfg = NcaaFetchConfig(proxy_url="http://user:pass@1.2.3.4:8080")
```

### `NcaaFetcher(config: 'Optional[NcaaFetchConfig]' = None, *, proxy_pool: "Optional['list[str]']" = None) -> 'None'` {#NcaaFetcher}

Cache-first stats.ncaa.org fetcher, proxy-bound per the binding directive.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config` | `Optional[NcaaFetchConfig]` | `None` |  |
| `proxy_pool` | `Optional['list[str]']` | `None` |  |

**Example**

```python
# Scrape game-detail data (the **suggested** path -- browser transport clears the Akamai bm-verify wall; see :meth:`with_browser`)

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
    with NcaaFetcher.with_browser() as fetcher:
        pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
        box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML

# Un-challenged pages (landing / team) via the curl_cffi proxy path

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher, update_config
    update_config(proxy_url="http://user:pass@1.2.3.4:8080")
    fetcher = NcaaFetcher()
    html = fetcher.fetch_team_schedule("391")  # cached after this call

# Offline (injected transport + explicit pool, no network/env needed)

    def fake(url, proxies, headers):
        return 200, "<html>...</html>"
    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetchConfig
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=fake)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1"])
```

**Methods**

#### `NcaaFetcher.fetch_game_box(contest_id: 'object', period: 'int' = 1, *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a game's box-score *landing* page for *period* (1-indexed).

Note: on current (2026) stats.ncaa.org this page is the team-stats /
game-leaders view -- the per-player box the box-score parser consumes
split out into `fetch_game_individual_stats`. Kept for the
team-stats surface and the legacy layout.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `contest_id` | `object` |  |  |
| `period` | `int` | `1` |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### `NcaaFetcher.fetch_game_individual_stats(contest_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a game's per-player box (the `individual_stats` tab).

This is the page `~sportsdataverse.mbb.mbb_ncaa_boxscore_parser
.get_box_lineup` parses on current markup (`format_version=1`): two
`table.dataTable.small_font#competitor_*` per-team player tables.
The server ignores `?period_no` here (returns the full-game box), so
no period arg -- see `dev/phase5f-live-proof.md`.

ponytail: the modern box split out of `box_score` into this tab; the
legacy (pre-2018) layout has no separate individual-stats page, so
`legacy=True` falls back to the legacy `box_score` path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `contest_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### `NcaaFetcher.fetch_game_pbp(contest_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a game's play-by-play page.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `contest_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### `NcaaFetcher.fetch_html(path: 'str', *, force: 'bool' = False) -> 'str'`

Fetch *path* (bare path or full stats.ncaa.org URL), cache-first.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | e.g. `"contests/4690813/play_by_play"` or a full `https://stats.ncaa.org/...` URL. |
| `force` | `bool` | `False` | Bypass the cache and re-fetch, overwriting the cache file. |

**Returns**

The response HTML, decoded as UTF-8.

#### `NcaaFetcher.fetch_team_roster(team_id: 'object', year_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a team's roster page for *year_id*.

ponytail: URL shape by analogy to the confirmed team-id scheme, not
independently live-confirmed -- see module docstring; fix in Task
5f.2 if the real path differs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `object` |  |  |
| `year_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### `NcaaFetcher.fetch_team_schedule(team_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a team's game-by-game schedule page.

Modern shape (`teams/{id}/game_by_game`) is confirmed by
`dev/phase5-ncaa-proxy-proof.md`; the legacy shape is by analogy
(see `fetch_team_roster`'s note).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

### `NoSurnameMatch(box_name: 'str', exact_first_name: 'Optional[str]', near_first_name: 'Optional[str]', err: 'str') -> None` {#NoSurnameMatch}

No candidate surname fragment scored well enough

(`NameFixer.NoSurnameMatch`, `:638-643`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `exact_first_name` | `Optional[str]` |  | A first-name fragment shared verbatim between candidate and box name, if any. |
| `near_first_name` | `Optional[str]` |  | A first-name fragment fuzzy-matching the box name's first name, if any (only computed when `exact_first_name` is absent). |
| `err` | `str` |  | Human-readable diagnostic (debug-only; see the module docstring's fuzzy-match-parity note for why its embedded score may not byte-match the upstream Java oracle). |

### `OtherOpponentEvent(min: 'float', score: 'Score', event_string: 'str') -> None` {#OtherOpponentEvent}

A non-sub event belonging to the opponent (`Model.OtherOpponentEvent`,

`ExtractorUtils.scala:866-873`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `event_string` | `str` |  | The raw play-by-play event string. |

**Methods**

#### `OtherOpponentEvent.with_min(new_min: 'float') -> "'OtherOpponentEvent'"`

Return a copy with `min` replaced (`:871`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### `OtherTeamEvent(min: 'float', score: 'Score', event_string: 'str') -> None` {#OtherTeamEvent}

A non-sub event belonging to the team under analysis

(`Model.OtherTeamEvent`, `ExtractorUtils.scala:858-865`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `event_string` | `str` |  | The raw play-by-play event string. |

**Methods**

#### `OtherTeamEvent.with_min(new_min: 'float') -> "'OtherTeamEvent'"`

Return a copy with `min` replaced (`:863`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### `ParseError(location: 'str', id: 'str', messages: 'list[str]') -> None` {#ParseError}

A parse-time error (`ParseError`, `ParseError.scala:9`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `location` | `str` |  | The module in which the error occurred. |
| `id` | `str` |  | The module-specific id for which the error occurred. |
| `messages` | `list[str]` |  | Human-readable description(s) of the error. |

### `PbpBuilders(team_finder: 'Callable[[BeautifulSoup], list[str]]', event_finder: 'Callable[[BeautifulSoup], list[Tag]]', event_time_finder: 'Callable[[Tag], Optional[str]]', event_score_finder: 'Callable[[Tag], Optional[str]]', game_event_finder: 'Callable[[Tag], Optional[str]]', event_team_finder: 'Callable[[Tag, bool], Optional[str]]', event_opponent_finder: 'Callable[[Tag, bool], Optional[str]]') -> None` {#PbpBuilders}

One version-era's HTML finder functions (``PlayByPlayParser

.base_builders`, `PlayByPlayParser.scala:37-51``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_finder` | `Callable[[BeautifulSoup], list[str]]` |  |  |
| `event_finder` | `Callable[[BeautifulSoup], list[Tag]]` |  |  |
| `event_time_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `event_score_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `game_event_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `event_team_finder` | `Callable[[Tag, bool], Optional[str]]` |  |  |
| `event_opponent_finder` | `Callable[[Tag, bool], Optional[str]]` |  |  |

### `PeekableIterator(iterable: 'Iterable[_T]') -> 'None'` {#PeekableIterator}

A stateful iterator with one element of look-ahead, the Python

stand-in for Scala's `scala.collection.Iterator`.

Reproduces the three `Iterator` operations `find_pbp_clump` /

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `iterable` | `Iterable[_T]` |  | Any iterable to wrap. |

**Methods**

#### `PeekableIterator.find(pred: 'Callable[[_T], bool]') -> 'Optional[_T]'`

First element satisfying `pred`, consuming up to and including

it (or exhausting the iterator and returning `None`) -- Scala
`Iterator.find`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pred` | `Callable[[_T], bool]` |  |  |

#### `PeekableIterator.has_next() -> 'bool'`

Whether another element is available, without consuming it

(Scala `Iterator.hasNext`).

#### `PeekableIterator.to_list() -> 'list[_T]'`

Drain the remaining elements into a list (Scala `Iterator.toList`).

### `PlayerCodeId(code: 'str', id: 'PlayerId', ncaa_id: 'Optional[str]' = None) -> None` {#PlayerCodeId}

A player's within-team-season code paired with their full identity

(`LineupEvent.PlayerCodeId`, `LineupEvent.scala:185-189`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `code` | `str` |  | The player code, unique within the team/season only. |
| `id` | `PlayerId` |  | The player's globally-unique identity. |
| `ncaa_id` | `Optional[str]` | `None` | The player's NCAA-issued id, if known. |

### `PlayerEvent(player: 'PlayerCodeId', player_stats: 'LineupEventStats', date: 'datetime', location_type: 'LocationType', start_min: 'float', end_min: 'float', duration_mins: 'float', score_info: 'ScoreInfo', team: 'TeamSeasonId', opponent: 'TeamSeasonId', lineup_id: 'LineupId', players: 'list[PlayerCodeId]', players_in: 'list[PlayerCodeId]', players_out: 'list[PlayerCodeId]', raw_game_events: 'list[RawGameEvent]', team_stats: 'LineupEventStats', opponent_stats: 'LineupEventStats', player_count_error: 'Optional[int]' = None) -> None` {#PlayerEvent}

A lineup event's stats, narrowed to one player (`PlayerEvent`,

`models/ncaa/PlayerEvent.scala:48-70`). **Scope addition, Task 5c.4**
-- deferred by 5a since only `~sportsdataverse.mbb
.mbb_ncaa_lineup_enrich.create_player_events` (5c.4) returns it. Appended
here (not inserted among the 5a-reviewed classes above) to keep this an
additive-only change.

Same field shape as `LineupEvent` with two fields prepended
(`player`, `player_stats`) -- the Scala builds this via a
`shapeless.LabelledGeneric` HList splice of `PlayerEvent`'s own
`player`/`player_stats` onto every field of a `LineupEvent`
instance; this port has no generic-programming machinery, so
`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.create_player_events`
constructs the dataclass directly instead.

**`SingleEventMeta` / `event_meta` / `game_id` are NOT ported.**
`PlayerEvent.scala`'s companion object nests a `SingleEventMeta` case
class, but the two fields that would carry it (`event_meta`,
`game_id`) are commented out in the Scala source itself
(`PlayerEvent.scala:67-69`) -- never part of the live case class, and
`create_player_events` never constructs a `SingleEventMeta`. Nothing
to defer; there is no live field to port.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player` | `PlayerCodeId` |  | The player this narrowed event describes. |
| `player_stats` | `LineupEventStats` |  | The player's own numerical stats for this lineup event. |
| `date` | `datetime` |  | The date of the game. |
| `location_type` | `LocationType` |  | Home/away/neutral (etc.) for this game. |
| `start_min` | `float` |  | The point in the game at which the lineup entered. |
| `end_min` | `float` |  | The point in the game at which the lineup changed. |
| `duration_mins` | `float` |  | The duration of the lineup. |
| `score_info` | `ScoreInfo` |  | The score differential context for this event. |
| `team` | `TeamSeasonId` |  | The team under analysis. |
| `opponent` | `TeamSeasonId` |  | The opposing team. |
| `lineup_id` | `LineupId` |  | A string that defines the set of players on the floor. |
| `players` | `list[PlayerCodeId]` |  | Mapping from player code to full identity, for this lineup. |
| `players_in` | `list[PlayerCodeId]` |  | Players who subbed in for this event. |
| `players_out` | `list[PlayerCodeId]` |  | Players who subbed out for this event. |
| `raw_game_events` | `list[RawGameEvent]` |  | The raw NCAA event strings for both teams. |
| `team_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (team side). |
| `opponent_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (opponent side). |
| `player_count_error` | `Optional[int]` | `None` | If the lineup is "impossible", the number of players actually seen (for analysis purposes). |

### `PlayerId(name: 'str') -> None` {#PlayerId}

CBB player identifier (`PlayerId`, `PlayerId.scala`, `AnyVal`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The unique player name. |

### `PlayerShotInfo(unknown_3pm: 'Optional[tuple[int, int, int, int, int]]' = None, early_3pa: 'Optional[tuple[int, int, int, int, int]]' = None, unast_3pm: 'Optional[tuple[int, int, int, int, int]]' = None, ast_3pm: 'Optional[tuple[int, int, int, int, int]]' = None) -> None` {#PlayerShotInfo}

Per-player shot-quality info, keyed by lineup slot

(`LineupEventStats.PlayerShotInfo`, `LineupEventStats.scala:98-103`).
Each tuple is a fixed-arity 5-slot (one per lineup spot), mirroring the
Scala `PlayerTuple[Int] = Tuple5[Int, Int, Int, Int, Int]` alias.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `unknown_3pm` | `Optional[tuple[int, int, int, int, int]]` | `None` | 3pt makes of unknown assist status, per slot. |
| `early_3pa` | `Optional[tuple[int, int, int, int, int]]` | `None` | Early-shot-clock 3pt attempts, per slot. |
| `unast_3pm` | `Optional[tuple[int, int, int, int, int]]` | `None` | Unassisted 3pt makes, per slot. |
| `ast_3pm` | `Optional[tuple[int, int, int, int, int]]` | `None` | Assisted 3pt makes, per slot. |

### `PlayerValueConstants(pace_baseline: 'float', bubble_recruit_rank: 'int', bundle_prefix: 'str') -> None` {#PlayerValueConstants}

Per-league constants for the player-value spine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pace_baseline` | `float` |  | League baseline possessions per game (per-100 scaling). |
| `bubble_recruit_rank` | `int` |  | National recruit rank of a "bubble" high-major rotation player (recruiting-model reference point). |
| `bundle_prefix` | `str` |  | Artifact filename prefix under `mbb/models` (`"mbb"` / `"wbb"`). |

### `PossCalcFragment(shots_made_or_missed: 'int' = 0, liveball_orbs: 'int' = 0, actual_deadball_orbs: 'int' = 0, ft_events: 'int' = 0, ignored_and_ones: 'int' = 0, bad_fouls: 'int' = 0, offsetting_bad_fouls: 'int' = 0, turnovers: 'int' = 0) -> None` {#PossCalcFragment}

Running stats needed to calculate possessions for one lineup event,

one direction at a time (`PossessionUtils.PossCalcFragment`,
`PossessionUtils.scala:124-144`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots_made_or_missed` | `int` | `0` | Count of shot attempts (made or missed). |
| `liveball_orbs` | `int` | `0` | Count of live-ball offensive rebounds. |
| `actual_deadball_orbs` | `int` | `0` | Count of dead-ball offensive rebounds. |
| `ft_events` | `int` | `0` | Count of free-throw *sets* (capped-at-1 flag per set). |
| `ignored_and_ones` | `int` | `0` | Count of and-one free throws ignored for possession purposes (capped-at-1 flag). |
| `bad_fouls` | `int` | `0` | Count of technical/flagrant fouls counted against the defending side (capped-at-1 flag). |
| `offsetting_bad_fouls` | `int` | `0` | Count of technical/flagrant fouls that offset (net zero) rather than counting against either side (capped-at-1 flag). |
| `turnovers` | `int` | `0` | Count of turnovers. |

### `PossState(team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', prev_clump: 'ConcurrentClump') -> None` {#PossState}

Running state threaded through `calculate_possessions_by_event`

(`PossessionUtils.PossState`, `PossessionUtils.scala:39-49`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_stats` | `PossCalcFragment` |  | Accumulated fragment for the team since the last lineup boundary. |
| `opponent_stats` | `PossCalcFragment` |  | Accumulated fragment for the opponent since the last lineup boundary. |
| `prev_clump` | `ConcurrentClump` |  | The previously-processed merged clump (used by `calculate_stats`'s and-one / deadball-rebound heuristics). |

### `PossessionEvent(dir: 'Direction') -> None` {#PossessionEvent}

Decomposes `RawGameEvent`\ s into attacking/defending sides

(`RawGameEvent.PossessionEvent`, `LineupEvent.scala:126-149`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dir` | `Direction` |  | Which team (`Direction.TEAM` / `Direction.OPPONENT`) is currently in possession. |

**Methods**

#### `PossessionEvent.attacking_team(ev: 'RawGameEvent') -> 'Optional[str]'`

The event string for the team in possession, or `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `RawGameEvent` |  | The raw game event to inspect. |

**Returns**

`ev.team` if `dir` is `Direction.TEAM`, `ev.opponent` if `Direction.OPPONENT`, else `None`.

#### `PossessionEvent.defending_team(ev: 'RawGameEvent') -> 'Optional[str]'`

The event string for the team NOT in possession, or `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `RawGameEvent` |  | The raw game event to inspect. |

**Returns**

`ev.team` if `dir` is `Direction.OPPONENT`, `ev.opponent` if `Direction.TEAM`, else `None`.

### `PossessionSplits(home_off_poss: 'float', away_off_poss: 'float', neutral_off_poss: 'float', total_off_poss: 'float', home_def_poss: 'float', away_def_poss: 'float', neutral_def_poss: 'float', total_def_poss: 'float') -> None` {#PossessionSplits}

Home/away/neutral possession totals for one team (`ts:143-152`).

Off and def possessions are bucketed by the game's `location_type`
(missing -> `"Neutral"`). The HCA residual step reads the off/def
imbalance `(home - away) / total` off these totals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off_poss` | `float` |  |  |
| `away_off_poss` | `float` |  |  |
| `neutral_off_poss` | `float` |  |  |
| `total_off_poss` | `float` |  |  |
| `home_def_poss` | `float` |  |  |
| `away_def_poss` | `float` |  |  |
| `neutral_def_poss` | `float` |  |  |
| `total_def_poss` | `float` |  |  |

### `RapmConfig(...)` {#RapmConfig}

Port of `RapmConfig` (`RapmUtils.ts:175-179`).

### `RapmPlayerContext(...)` {#RapmPlayerContext}

Port of `RapmPlayerContext` (`RapmUtils.ts:147-173`).

See the module docstring for why `filtered_lineups` is a Python
callable rather than a materialized dict.

### `RapmPreProcDiagnostics(...)` {#RapmPreProcDiagnostics}

Port of `RapmPreProcDiagnostics` (`RapmUtils.ts:187-194`) -- the

multi-collinearity diagnostic `calc_collinearity_diag` returns.

### `RapmPriorInfo(...)` {#RapmPriorInfo}

Port of `RapmPriorInfo` (`RapmUtils.ts:124-133`).

### `RapmProcessingInputs(...)` {#RapmProcessingInputs}

Port of `RapmProcessingInputs` (`RapmUtils.ts:196-203`).

See the module docstring's "Task 3.5 notes" for why `soln_matrix` and
`sd_rapm` are plain nested `list`s rather than `NDArray`s, and why
`sd_rapm` exists at all (a Python-only addition beyond upstream's own
return shape).

### `RawGameEvent(min: 'float', team: 'Optional[str]' = None, opponent: 'Optional[str]' = None) -> None` {#RawGameEvent}

A single NCAA play-by-play event line (`LineupEvent.RawGameEvent`,

`LineupEvent.scala:65-105`).

Exactly one of `team` / `opponent` is populated per event -- the raw
string is the literal `"date,time,event"` line from the NCAA website.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The game-clock minute (fractional) this event occurred at. |
| `team` | `Optional[str]` | `None` | The raw event string, if this event belongs to the team under analysis. |
| `opponent` | `Optional[str]` | `None` | The raw event string, if this event belongs to the opponent. |

### `RosterEntry(player_code_id: 'PlayerCodeId', number: 'str', pos: 'str', height: 'str', height_in: 'Optional[int]', year_class: 'str', gp: 'int', origin: 'Optional[str]', role: 'Optional[str]') -> None` {#RosterEntry}

An entry in an NCAA team roster (`RosterEntry`, ``models/ncaa

/RosterEntry.scala:11-21`). **Scope addition, Task 5e.1** -- the first
model consumed by the HTML-parser layer (`mbb_ncaa_roster_parser.py``).
Appended here (not inserted among the 5a-reviewed classes above) to keep
this an additive-only change, matching `PlayerEvent`'s precedent.

The trailing `role` field (present in the Scala case class shape but
never populated by `RosterParser.parse_roster` -- every construction
site there, both the real-player and coach__` branches, passes the
literal `None` for it) is presumably set by a later phase's box-score
parser (`BoxscoreParser`, out of this task's scope); it is carried
here for shape fidelity even though Task 5e.1's only producer never
populates it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_code_id` | `PlayerCodeId` |  | The player's code + full identity (the roster-row equivalent of a box-score/PbP player reference). |
| `number` | `str` |  | The jersey number, as printed (may be non-numeric text). |
| `pos` | `str` |  | The listed position. |
| `height` | `str` |  | The listed height, in `"FT-IN"` text form (e.g. `"6-3"`). |
| `height_in` | `Optional[int]` |  | `height` parsed to total inches, if it matched `height_regex`. |
| `year_class` | `str` |  | The listed academic year (`"Fr"`/`"So"`/`"Jr"`/ `"Sr"`/etc.). |
| `gp` | `int` |  | Games played. |
| `origin` | `Optional[str]` |  | The player's hometown/prior-school text, if the source table has that column (v1 rosters only). |
| `role` | `Optional[str]` |  | Reserved for a later phase; always `None` from `parse_roster` (see above). |

### `ScheduleBuilders(team_name_finder: 'Callable[[BeautifulSoup], Optional[str]]', neutral_game_finder: 'Callable[[BeautifulSoup], list[str]]') -> None` {#ScheduleBuilders}

One version-era's HTML finder functions (``TeamScheduleParser

.base_builders`, `TeamScheduleParser.scala:36-39``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_name_finder` | `Callable[[BeautifulSoup], Optional[str]]` |  |  |
| `neutral_game_finder` | `Callable[[BeautifulSoup], list[str]]` |  |  |

### `ScoreInfo(start: 'Score', end: 'Score', start_diff: 'int', end_diff: 'int') -> None` {#ScoreInfo}

Score context at the start/end of a lineup event

(`LineupEvent.ScoreInfo`, `LineupEvent.scala:153-158`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start` | `Score` |  | Score at the start of the event. |
| `end` | `Score` |  | Score at the end of the event. |
| `start_diff` | `int` |  | Score differential (team - opponent) at the start. |
| `end_diff` | `int` |  | Score differential (team - opponent) at the end. |

### `ShotClockStats(total: 'int' = 0, early: 'Optional[int]' = None, mid: 'Optional[int]' = None, late: 'Optional[int]' = None, orb: 'Optional[int]' = None) -> None` {#ShotClockStats}

Counting stats broken down by shot-clock segment

(`LineupEventStats.ShotClockStats`, `LineupEventStats.scala:51-57`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `total` | `int` | `0` | Count across the entire shot clock. |
| `early` | `Optional[int]` | `None` | Count in the first 10s, if tracked. |
| `mid` | `Optional[int]` | `None` | Count in the middle 10s, if tracked. |
| `late` | `Optional[int]` | `None` | Count in the last 10s, if tracked. |
| `orb` | `Optional[int]` | `None` | Count in the first 10s following an offensive rebound, if tracked (else folded into `mid`/`late` as normal). |

### `ShotEvent(player: 'Optional[PlayerCodeId]', date: 'datetime', location_type: 'LocationType', team: 'TeamSeasonId', opponent: 'TeamSeasonId', is_off: 'bool', lineup_id: 'Optional[LineupId]', players: 'list[PlayerCodeId]', score: 'Score', min: 'float', loc: 'ShotLocation', geo: 'ShotGeo', dist: 'float', pts: 'int', value: 'int', ast_by: 'Optional[PlayerCodeId]', is_ast: 'Optional[bool]', is_trans: 'Optional[bool]', raw_event: 'Optional[str]') -> None` {#ShotEvent}

Info about one shot taken during a game, all distances in feet

(`ShotEvent`, `models/ncaa/ShotEvent.scala:9-29`). **Scope addition,
Task 5e.5** -- the model produced by
`~sportsdataverse.mbb.mbb_ncaa_shot_parser.create_shot_event_data`.

**Fields `mbb_ncaa_shot_parser.create_shot_event_data` (this task)
actually populates:** `player` (best-effort -- tidy-resolved + coded
for the team under analysis, name-coded only for the opponent),
`date`/`location_type`/`team`/`opponent` (copied from the
box-score lineup), `is_off`, `score` (re-oriented for home/away/
neutral perspective), `min` (ascending game-clock time, after
`phase1_shot_event_enrichment`), `loc`/`dist` (transformed court
coordinates + Euclidean distance from the basket, after the
self-correcting flip pass), `geo` (synthetic lat/lon), `raw_event`
(the SVG `<title>` text, for debugging).

**Fields left as placeholders for a LATER phase (Task 5e.6,
`PlayByPlayUtils`/`ShotEnrichmentUtils`):** `lineup_id` (always
`None` here -- "discard if bad lineup" per the Scala comment, filled in
once the shot is matched against an actual on-floor lineup), `players`
(always `[]` here -- filled in from the matched lineup), `pts`
(**not the real point value** -- this task only sets it to `1`/`0`
for made/missed, matching the Scala's own `// (enrich in final phase)`
comment; the real 2pt/3pt value comes from `PlayByPlayUtils.shot_value`
in Task 5e.6), `value` (always `0` here, same "final phase" note),
`ast_by`/`is_ast`/`is_trans` (always `None` here -- assist/
transition attribution needs the play-by-play cross-reference Task 5e.6
builds).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player` | `Optional[PlayerCodeId]` |  | The shooting player's code + identity, if resolved (`None` is never actually produced by this task's parser, but the type allows for it per the Scala `Option`). |
| `date` | `datetime` |  | The date of the game. |
| `location_type` | `LocationType` |  | Home/away/neutral (etc.) for this game. |
| `team` | `TeamSeasonId` |  | The team under analysis. |
| `opponent` | `TeamSeasonId` |  | The opposing team. |
| `is_off` | `bool` |  | Whether the team under analysis is the one shooting. |
| `lineup_id` | `Optional[LineupId]` |  | The on-floor lineup id, if/when matched (see above). |
| `players` | `list[PlayerCodeId]` |  | The on-floor lineup's players, if/when matched (see above). |
| `score` | `Score` |  | The score at the time of the shot, team-oriented. |
| `min` | `float` |  | The ascending game-clock time (minutes) of the shot. |
| `loc` | `ShotLocation` |  | The shot's transformed court location, in feet. |
| `geo` | `ShotGeo` |  | The shot's synthetic lat/lon. |
| `dist` | `float` |  | The shot's distance from the basket, in feet. |
| `pts` | `int` |  | Made(`1`)/missed(`0`) flag from this task -- NOT the real point value (see above). |
| `value` | `int` |  | Always `0` from this task (see above). |
| `ast_by` | `Optional[PlayerCodeId]` |  | The assisting player, if/when matched (see above). |
| `is_ast` | `Optional[bool]` |  | Whether the shot was assisted, if/when matched (see above). |
| `is_trans` | `Optional[bool]` |  | Whether the shot was in transition, if/when matched (see above). |
| `raw_event` | `Optional[str]` |  | The raw SVG `<title>` text this shot was parsed from, for debugging (discarded before writing to disk upstream). |

### `ShotEventBuilders(team_finder: 'Callable[[BeautifulSoup], list[str]]', shot_event_finder: 'Callable[[BeautifulSoup], list[Tag]]', script_extractor: 'Callable[[str], Optional[str]]', title_extractor: 'Callable[[Tag], Optional[str]]', event_period_finder: 'Callable[[Tag], Optional[int]]', event_time_finder: 'Callable[[Tag], Optional[float]]', event_player_finder: 'Callable[[Tag], Optional[str]]', shot_location_finder: 'Callable[[Tag], Optional[tuple[float, float]]]', event_score_finder: 'Callable[[Tag], Optional[Score]]', shot_result_finder: 'Callable[[Tag], Optional[bool]]', shot_taking_team_finder: 'Callable[[Tag], Optional[str]]') -> None` {#ShotEventBuilders}

The HTML finder-function table (`ShotEventParser.base_builders`,

`:37-50`). v1-only -- SVG shot maps did not exist in the v0 page
format, so there is exactly one instance (`v1_builders`), unlike
the v0/v1 pairs in every other 5e parser.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_finder` | `Callable[[BeautifulSoup], list[str]]` |  |  |
| `shot_event_finder` | `Callable[[BeautifulSoup], list[Tag]]` |  |  |
| `script_extractor` | `Callable[[str], Optional[str]]` |  |  |
| `title_extractor` | `Callable[[Tag], Optional[str]]` |  |  |
| `event_period_finder` | `Callable[[Tag], Optional[int]]` |  |  |
| `event_time_finder` | `Callable[[Tag], Optional[float]]` |  |  |
| `event_player_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `shot_location_finder` | `Callable[[Tag], Optional[tuple[float, float]]]` |  |  |
| `event_score_finder` | `Callable[[Tag], Optional[Score]]` |  |  |
| `shot_result_finder` | `Callable[[Tag], Optional[bool]]` |  |  |
| `shot_taking_team_finder` | `Callable[[Tag], Optional[str]]` |  |  |

### `ShotGeo(lat: 'float', lon: 'float') -> None` {#ShotGeo}

A shot's synthetic lat/lon, for geo-aware visualization tooling

(`ShotEvent.ShotGeo`, `models/ncaa/ShotEvent.scala:45`). **Scope
addition, Task 5e.5** -- flattened per `ShotLocation`'s note.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lat` | `float` |  | Synthetic latitude (feet-to-meters converted, offset from an arbitrary base point -- not a real-world location). |
| `lon` | `float` |  | Synthetic longitude, same convention as `lat`. |

### `ShotLocation(x: 'float', y: 'float') -> None` {#ShotLocation}

A shot's court-relative coordinates, in feet (`ShotEvent.ShotLocation`,

`models/ncaa/ShotEvent.scala:48`). **Scope addition, Task 5e.5** --
flattened out of the Scala `ShotEvent` companion object per this
module's established nested-object-flattening precedent (see the module
docstring's "Scala idiom decisions": `ScoreInfo`/`PlayerCodeId` were
already flattened out of `LineupEvent`'s companion the same way).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `x` | `float` |  | Feet from the basket; positive is to the right of the basket (facing the goal), negative is to the left. |
| `y` | `float` |  | Feet from the basket along the baseline-perpendicular axis. |

### `ShotMapDimensions()` {#ShotMapDimensions}

SVG shot-map pixel<->feet conversion constants, taken from the

`svg#court` element (`ShotEventParser.ShotMapDimensions`,
`:568-582`). A plain class (not a dataclass) used purely as a
namespace, mirroring the Scala `object`'s "static singleton" role --
field names are kept snake_case to match the Scala vals verbatim,
letting the ported oracle tests reference e.g.
`ShotMapDimensions.court_length_x_px` 1:1.

### `StrengthAdjustedResult(averages: 'dict[str, FieldAverage]', teams: 'list[TeamStrengthAdjusted]') -> None` {#StrengthAdjustedResult}

The compute output of `build_strength_adjusted_stats`.

Mirrors `main()`'s `{ averages, teams }` object (`ts:656-662`) minus
the `lastUpdated`/`gender`/`year` serialization wrapper.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `averages` | `dict[str, FieldAverage]` |  |  |
| `teams` | `list[TeamStrengthAdjusted]` |  |  |

### `StrongSurnameMatch(box_name: 'str', score: 'int') -> None` {#StrongSurnameMatch}

A surname fragment matched and the whole-name score cleared

`MIN_OVERALL_SCORE` (`NameFixer.StrongSurnameMatch`, `:646-647`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `score` | `int` |  | The whole-name similarity score. |

### `SubInEvent(min: 'float', score: 'Score', player_name: 'str') -> None` {#SubInEvent}

A player subs into the game (`Model.SubInEvent`, `ExtractorUtils.scala:850-853`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `player_name` | `str` |  | The raw or processed name of the player subbing in. |

**Methods**

#### `SubInEvent.with_min(new_min: 'float') -> "'SubInEvent'"`

Return a copy with `min` replaced (`:852`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### `SubOutEvent(min: 'float', score: 'Score', player_name: 'str') -> None` {#SubOutEvent}

A player subs out of the game (`Model.SubOutEvent`, `ExtractorUtils.scala:854-857`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `player_name` | `str` |  | The raw or processed name of the player subbing out. |

**Methods**

#### `SubOutEvent.with_min(new_min: 'float') -> "'SubOutEvent'"`

Return a copy with `min` replaced (`:856`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### `TeamId(name: 'str') -> None` {#TeamId}

CBB team identifier (`TeamId`, `TeamId.scala`, `AnyVal`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The unique team name. |

### `TeamSeasonId(team: 'TeamId', year: 'Year') -> None` {#TeamSeasonId}

A team's season identifier (`TeamSeasonId`, `TeamSeasonId.scala`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamId` |  | The team playing the season. |
| `year` | `Year` |  | The year the season ends. |

### `TeamStrengthAdjusted(team_name: 'str', conf: 'str', raw: 'FieldSideMap', adj: 'FieldSideMap', adj_hca: 'FieldSideMap') -> None` {#TeamStrengthAdjusted}

One team's raw / adjusted / HCA-adjusted rates (`ts:642-648`).

Each of `raw` / `adj` / `adj_hca` maps a stat field
(`efg`/`3p`/`2pmid`/`2prim`) to a `{"off": float, "def": float}`
dict. `adj` is the strength-of-schedule-adjusted value; `adj_hca` adds
the home-court term (`off + hca_off`, `def - hca_def`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_name` | `str` |  |  |
| `conf` | `str` |  |  |
| `raw` | `FieldSideMap` |  |  |
| `adj` | `FieldSideMap` |  |  |
| `adj_hca` | `FieldSideMap` |  |  |

### `TidyPlayerContext(box_lineup: 'LineupEvent', all_players_map: 'dict[str, str]', alt_all_players_map: 'dict[str, list[str]]', resolution_cache: 'dict[str, str]' = <factory>) -> None` {#TidyPlayerContext}

Precomputed box-score lookup tables + resolution cache for

`tidy_player` (`LineupErrorAnalysisUtils.TidyPlayerContext`,
`:31-36`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The box-score lineup event this context resolves names against. |
| `all_players_map` | `dict[str, str]` |  | Player code -> full name, for every player in `box_lineup.players`. |
| `alt_all_players_map` | `dict[str, list[str]]` |  | Truncated player code (see truncate_code_1` / truncate_code_2`) -> the list of full names sharing that truncation -- used when the exact code doesn't match but a unique truncated one does. |
| `resolution_cache` | `dict[str, str]` | `<factory>` | Memoizes prior `tidy_player` resolutions. See the module docstring's "Behavioral quirk" note -- this is read by the raw input name but written by the corrected name, faithfully reproducing the upstream asymmetry. |

### `ValidationError(*values)` {#ValidationError}

The 3 ways a lineup can be declared invalid, in Scala declaration

(ordinal) order (`LineupErrorAnalysisUtils.ValidationError`, `:18-20`).
Member order is load-bearing -- see the module docstring's "Return
shape" note.

### `WeakSurnameMatch(box_name: 'str', score: 'int', info: 'str') -> None` {#WeakSurnameMatch}

A surname fragment matched, but the whole-name score fell short of

`MIN_OVERALL_SCORE` (`NameFixer.WeakSurnameMatch`, `:644-645`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `score` | `int` |  | The whole-name similarity score. |
| `info` | `str` |  | Human-readable diagnostic (debug-only; see the fuzzy-match- parity note). |

### `Year(value: 'int') -> None` {#Year}

CBB season, named by the year it ends (`Year`, `Year.scala`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `value` | `int` |  | The ending year of the season. |

### `add_missing_players(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'` {#add_missing_players}

Back-fills a clump whose lineups carry TOO FEW players

(`LineupErrorAnalysisUtils.add_missing_players`, `:315-401`).

Fires only when the clump's first event has `<= 4` on-floor players
(`:324-325`: `players_in.size > 4` is a no-op). The candidate pool is
every box-score player NOT already on the first event's floor
(`:328`), **seeded** with a heuristic (`:352-357`): the `next_good`
lineup's sub-outs minus anyone appearing anywhere in the clump -- a good
lineup that opens by subbing out a player who was never actually on the
floor is a strong signal that player belongs to this under-filled clump.

Walking the clump chronologically (`:359-385`): a candidate who subs IN
is dropped from the pool (they're accounted for), and any remaining
candidate named in a team-side raw play (same `parse_any_play` ->
`tidy_player` -> `build_player_code` chain as
`find_missing_subs`) is collected into `players_to_add`. If
anything was collected, it is appended to **every** event's `players`
(raw list concat, no dedup -- `:388-391`, a verbatim port; an over-add
that pushes an event past 5 players lands it in the still-to-fix bucket
for the second `find_missing_subs` pass in
`analyze_and_fix_clumps` to trim back). The augmented events are
partitioned by `validate_lineup`. If nothing was collected, the
original clump is returned unchanged.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- the now-valid augmented events and a clump of the still-invalid ones (carrying the input's `next_good`); or `([], clump)` on a no-op / nothing-to-add outcome.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    add_missing_players,
)
fixed, still = add_missing_players(clump, box_lineup, valid_codes)
```

### `add_stats_to_lineups(lineup: 'LineupEvent') -> 'LineupEvent'` {#add_stats_to_lineups}

Enrich a lineup with play-by-play stats for both team and opponent

(`add_stats_to_lineups`, `LineupUtils.scala:1441-1451`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup event to enrich (not mutated). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` with `team_stats`/`opponent_stats` populated.

### `adjust_efficiency(game_eff: 'pl.DataFrame', *, league: 'str' = 'mens', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'` {#adjust_efficiency}

Iterative opponent-adjusted efficiency -> AdjO / AdjD / AdjEM per team-season.

KenPom-style fixed point: initialise `adj_o = raw_o` / `adj_d = raw_d`,
then repeatedly recompute each team's rating from its games with the
opponent's *current* adjusted rating and a home-court adjustment removed,
until the largest change is below `tol`. Ratings are computed independently
per season (a team's opponent pool is within-season).

The per-game offensive update is
`off_eff - (adj_d_opp - avg) - loc_o` where `loc_o` is `+hfa/2` at
home, `-hfa/2` away, `0` neutral (defense is symmetric with the opposite
sign); `avg` is the league mean efficiency and `hfa` comes from
`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the HFA constant. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest rating change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_o, adj_d, adj_em, raw_o, raw_d, games`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency
ratings = adjust_efficiency(raw_game_efficiency(sched, box))
```

### `adjust_off_rating_stats(pts_correction_factor: 'float', poss_correction_factor: 'float', mutable_o_rtg: 'ORtgDiagnostics', maybe_raw_o_rtg: 'float | None') -> 'tuple[float, float] | None'` {#adjust_off_rating_stats}

Apply a missing-possession correction factor to an `ORtgDiagnostics` dict in place.

Faithful port of `RatingUtils.adjustOffRatingStats` (`RatingUtils.ts:993-1033`).
Genuinely public upstream (called from `LineupTableUtils.ts` after a
lineup-level pts/poss reconciliation), so this port is public too.
Recomputes the productivity fields via `build_productivity`
(reused, not re-derived).

**Landmine 4** (see module docstring): the `o_adj = avgEff / defSos or
1` recomputation here is unguarded against `defSos == 0` -- same
reachability analysis as landmine 3 (only reachable if the diagnostics
dict's original `build_o_rtg` call used `avg_efficiency == 0`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pts_correction_factor` | `float` |  | Points correction factor (e.g. team pts / sum of player pts, capped to `[0.95, 1.05]` by callers). |
| `poss_correction_factor` | `float` |  | Possession correction factor, same shape. |
| `mutable_o_rtg` | `ORtgDiagnostics` |  | The `ORtgDiagnostics` dict to mutate in place (`oRtg`, `Usage`, `adjORtg`, `adjORtgPlus`, `Usage_Bonus`, `SoS_Bonus`, `adjPtsFactor`, `adjPossFactor`, and (conditionally) `Raw_Usage` are all updated). |
| `maybe_raw_o_rtg` | `float \| None` |  | The un-overridden raw `oRtg` value (`rawORtg`'s `.value`, or `None` when no override was in play), used to compute the raw-side return. |

**Returns**

`(new_raw_o_rtg, raw_adj_o_rtg_plus)` when both `mutable_o_rtg["Raw_Usage"]` and `maybe_raw_o_rtg` are not `None`; otherwise `None` (.isNil` semantics -- an explicit `0` does NOT count as nil).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_o_rtg, adjust_off_rating_stats

_, _, raw_o_rtg, _, o_diags = build_o_rtg(player, {}, {}, 100.0, True, False)
maybe_raw = raw_o_rtg["value"] if raw_o_rtg else None
adjust_off_rating_stats(1.1, 0.9, o_diags, maybe_raw)
print(o_diags["oRtg"], o_diags["adjORtgPlus"])
```

### `adjust_tempo(game_eff: 'pl.DataFrame', *, league: 'str' = 'mens', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'` {#adjust_tempo}

Opponent-adjusted tempo (possessions/40) per team-season.

Same fixed point as `adjust_efficiency`, applied to game possessions
under the additive model `poss = tempo_i + tempo_j - avg`: a team's tempo
is recovered by removing its opponents' current adjusted tempo. `avg` is
the league baseline tempo from
`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the tempo baseline. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest tempo change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_tempo`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import adjust_tempo, raw_game_efficiency
tempo = adjust_tempo(raw_game_efficiency(sched, box))
```

### `aggregate_player_seasons(seasons: "'list[int]'", *, league: 'str' = 'mens') -> 'pl.DataFrame'` {#aggregate_player_seasons}

Canonical per-player-season counting frame from the boxscore release.

Sums the per-game player boxscores into one row per (player_id, season,
team_id) with the counting columns `player_per100_features` expects.
Shot-location splits come from the shots release (2025+): free throws
(`MadeFreeThrow`) are excluded, layup/dunk/tip = rim, and jump shots
split three vs mid by `score_value` (the release's `type_text` carries
no three-point marker; `score_value` is populated on misses too). For
seasons without shots data, three-point attempts come from the box and
all remaining attempts fold into `fga_mid`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to aggregate. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, season, team_id:Utf8, player, minutes` + the counting columns + `fga_rim, fga_mid, fga_three`. Empty input returns zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import (
    aggregate_player_seasons, player_per100_features,
)
feats = player_per100_features(aggregate_player_seasons([2025]))
```

### `alias_combos(first: 'str', last: 'str', to_name: 'str') -> 'dict[str, str]'` {#alias_combos}

Pair each of `combos`' three name variants with a shared alias

target (`DataQualityIssues.alias_combos`, `DataQualityIssues.scala:351-356`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first (mis-recorded) first name. |
| `last` | `str` |  | The player's (mis-recorded) last name. |
| `to_name` | `str` |  | The canonical `"Lastname, Firstname"` this player should resolve to. |

**Returns**

A dict mapping each of the three name variants to `to_name`.

### `analyze_and_fix_clumps(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'` {#analyze_and_fix_clumps}

Runs the full self-healing fixer pipeline over one bad-lineup clump

(`LineupErrorAnalysisUtils.analyze_and_fix_clumps`, `:556-610`).

The strict, order-dependent sequence (each stage threads
`(fixed_so_far + newly_fixed, still_to_fix)`):

1. `handle_common_sub_bug`,
2. `find_missing_subs`,
3. `add_missing_players`,
4. `find_missing_subs` **again** -- the Scala's own comment
   (`:587-588`) explains: "Try this again since add_missing_players can
   go too far". Step 3 back-fills onto every event and can push some past
   5 players; the second trim pass removes the over-add.

Finally every accumulated `fixed` lineup gets a fresh `lineup_id` via
`~sportsdataverse.mbb.mbb_ncaa_stints.build_lineup_id` (`:597-605`)
-- the fixers changed the on-floor `players`, so the id computed during
stint construction is stale. The Scala's `debug`-gated
`analyze_unfixed_clumps` call (`:593-596`) is dropped (see the module
docstring's "Debug-only" note); it only prints.

The Scala wraps the whole pipeline in `Some(clump).map { ... }
.getOrElse((Nil, clump))`, but `Some(_)` is never empty so the
`getOrElse` is dead -- omitted here.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- every repaired lineup (with a recomputed `lineup_id`) and whatever clump the pipeline could not fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    analyze_and_fix_clumps,
)
fixed, still = analyze_and_fix_clumps(clump, box_lineup, valid_codes)
for lineup in fixed:
    print(lineup.lineup_id.value)
```

### `apply_relative_positional_overrides(results: 'list[dict[str, str]]', team_season: 'str', recurse_count: 'int' = 0) -> 'list[dict[str, str]]'` {#apply_relative_positional_overrides}

Recursively re-shuffle an ordered lineup per `RELATIVE_POSITION_FIXES`.

Faithful port of the private `PositionUtils.applyRelativePositionalOverrides`
(`PositionUtils.ts:657-693`). Finds the first rule (in table order) whose
`key` slots all match the current `results` codes (a `None` key slot
matches anything), applies that rule's `rule` slots (`None` = leave
unchanged, `int` = 1-based back-reference into the *pre-rule* results,
`dict` = literal replacement) to produce a new ordering, then recurses on
the new ordering -- since one swap can expose a second rule to match (e.g.
the Maryland 2019/20 Morsell/Wiggins swap can cascade into the Lindo/Smith
swap). Recursion is bounded by `recurse_count < len(rules)` (ported
verbatim from the TS bound), so it always terminates even if two rules
somehow ping-ponged each other.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `list[dict[str, str]]` |  | The current 5-slot `{"code": ..., "id": ...}` ordering (PG/SG/SF/PF/C, index 0-4). |
| `team_season` | `str` |  | Key into `RELATIVE_POSITION_FIXES`. A team/season absent from the table (or the recursion exhausting that team/season's rule count) returns `results` unchanged. |
| `recurse_count` | `int` | `0` | Internal recursion depth counter -- callers should not pass this explicitly (mirrors the TS default parameter). |

**Returns**

The (possibly re-shuffled) 5-slot ordering.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import apply_relative_positional_overrides
results = [
    {"code": "AnCowan", "id": "Cowan, Anthony"},
    {"code": "ErAyala", "id": "Ayala, Eric"},
    {"code": "DaMorsell", "id": "Morsell, Darryl"},
    {"code": "AaWiggins", "id": "Wiggins, Aaron"},
    {"code": "JaSmith", "id": "Smith, Jalen"},
]
apply_relative_positional_overrides(results, "Men_Maryland_2019/20")
```

### `apply_weak_priors(field: 'str', player_poss_pcts: 'list[float]', prior_info: 'RapmPriorInfo', debug_mode: 'bool' = False) -> 'Callable[[float, list[float]], list[float]]'` {#apply_weak_priors}

Build a closure that nudges ridge-regressed RAPM back towards its weak prior.

Faithful port of `RapmUtils.applyWeakPriors` (`RapmUtils.ts:921-995`).
Ridge regression depresses estimates towards `0`; this "fills" the
team-total error (see `pick_ridge_regression`'s
`[IMPORTANT-EQUATION-01]` team-total reconciliation) back in using each
player's weak (KenPom-derived) prior as the fallback signal, capped so no
more than half the team-total error gets attributed via this path
(`max_multiplier = -0.5`) -- an alternate flat-translation path
(`use_alt_rating`) kicks in for `off_adj_ppp`/`def_adj_ppp` fields
when the capped path can't fully explain the error.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | The prior key to read off each `prior_info["players_weak"]` entry, e.g. `"off_adj_ppp"`. |
| `player_poss_pcts` | `list[float]` |  | Per-player possession-share weights (index-aligned with `prior_info["players_weak"]`), e.g. `pick_ridge_regression`'s own `pct_by_player[off_or_def]`. |
| `prior_info` | `RapmPriorInfo` |  | A `RapmPriorInfo` (only `["players_weak"]` is read). |
| `debug_mode` | `bool` | `False` | Kept for TS signature parity -- upstream gates a `console.log` behind this flag (`RapmUtils.ts:979-984`), which this port deliberately does not reproduce: every production call site pins it `False` (`offDefDebugMode.off`/`.def` are hardcoded `False` constants inside `pickRidgeRegression`), so it is dead in every current caller and would only ever emit console noise, not test-observable behavior. |

**Returns**

A closure `(error, base_results) -> adjusted_results` -- call it with the team-total efficiency error and the pre-adjustment RAPM vector to get the weak-prior-nudged result.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import apply_weak_priors

nudge = apply_weak_priors("off_adj_ppp", pct_by_player, ctx["prior_info"])
adjusted = nudge(adj_eff_err_pre_prior, results_pre_prior)
```

### `as_of_ratings_split(results: 'pl.DataFrame', cutoff_date: 'datetime.date') -> 'pl.DataFrame'` {#as_of_ratings_split}

Return only games strictly before `cutoff_date` (the leakage boundary).

Predictive backtests must rate a game using only games that finished before
it — this split enforces that as-of-date rule so no future information leaks
into a game's own prediction.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | A frame with a `date` column of dtype `pl.Date`. |
| `cutoff_date` | `date` |  | The date of the game being predicted; games on or after it are dropped. |

**Returns**

The subset of `results` with `date < cutoff_date`.

**Example**

```python
from sportsdataverse.mbb.mbb_prediction_constants import as_of_ratings_split
prior = as_of_ratings_split(results, some_game_date)
```

### `as_of_season_split(df: 'pl.DataFrame', target_season: 'int') -> 'pl.DataFrame'` {#as_of_season_split}

Rows strictly before `target_season` -- the leakage boundary.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame with an integer `season` column. |
| `target_season` | `int` |  | The season being predicted; its rows (and later) drop. |

**Returns**

The subset with `season < target_season`.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import as_of_season_split
prior = as_of_season_split(df, 2026)
```

### `assign_to_right_lineup(state: 'PossState', team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', clump: 'ConcurrentClump', prev_clump: 'ConcurrentClump') -> 'list[LineupEvent]'` {#assign_to_right_lineup}

Assign a clump's possessions to the lineup(s) ending in it

(`PossessionUtils.assign_to_right_lineup`, `PossessionUtils.scala
:418-518`).

Applies the running `state` total (accumulated since the last lineup
boundary) to the *first* ending lineup only, then hands off to
`lineup_balancer` (this clump's own fragment, split across
candidates if there's more than one) and finally `lineup_fixer`
(the negative-possession clamp).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state` | `PossState` |  | The running possession state since the last lineup boundary. |
| `team_stats` | `PossCalcFragment` |  | This clump's team-direction fragment. |
| `opponent_stats` | `PossCalcFragment` |  | This clump's opponent-direction fragment. |
| `clump` | `ConcurrentClump` |  | The merged clump ending one or more lineups. |
| `prev_clump` | `ConcurrentClump` |  | The previous merged clump. |

**Returns**

The lineup(s) ending in this clump, enriched with possession counts. Empty if `clump.lineups` is empty (see the module docstring's landmine-index note -- unreachable via `calculate_possessions_by_event`).

### `attr_regex_filter(tags: 'list[Tag]', attr: 'str', regex: 'str') -> 'list[Tag]'` {#attr_regex_filter}

JSoup `[attr~=regex]`: candidates whose `attr` value matches

`regex`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tags` | `list[Tag]` |  | Candidate tags to filter (typically the result of an earlier `.select()`/`.find_all()` call). |
| `attr` | `str` |  | The attribute name to test. |
| `regex` | `str` |  | The pattern the attribute value must `re.search`-match. |

**Returns**

The subset of `tags` that have `attr` set and whose value matches `regex`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, parse_html
soup = parse_html('<div width="45%"></div><div width="10%"></div>')
attr_regex_filter(soup.find_all("div"), "width", r"^\[?4?5")
```

### `bootstrap_ari(fit_fn: "'Callable[[np.ndarray], tuple[np.ndarray, np.ndarray]]'", X: 'np.ndarray', n_boot: 'int' = 20, seed: 'int' = 0) -> 'float'` {#bootstrap_ari}

Cluster stability: mean ARI between the full fit and bootstrap refits.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fit_fn` | `Callable[[ndarray], tuple[ndarray, ndarray]]` |  | `X -> (centers, labels)` (e.g. a seeded `kmeans_fit` partial). |
| `X` | `ndarray` |  | Feature matrix. |
| `n_boot` | `int` | `20` | Bootstrap resamples. |
| `seed` | `int` | `0` | RNG seed. |

**Returns**

Mean adjusted Rand index of the resample fits' assignments (of the FULL sample, via nearest refit center) vs the full-fit labels.

**Example**

```python
from functools import partial
score = bootstrap_ari(lambda Z: kmeans_fit(Z, 8, seed=0), Z, n_boot=20, seed=0)
```

### `box_aware_compare(candidate_in: 'str', box_name_in: 'str') -> 'MatchResult'` {#box_aware_compare}

Score how well a single play-by-play candidate name fits a single

box-score name (`NameFixer.box_aware_compare`, `:658-766`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `candidate_in` | `str` |  | The raw play-by-play name fragment. |
| `box_name_in` | `str` |  | One box-score player's full name (`"Surname, First [Middle]"` format). |

**Returns**

A `StrongSurnameMatch` / `WeakSurnameMatch` / `NoSurnameMatch`, per the surname- and whole-name-score thresholds.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import box_aware_compare
box_aware_compare("Tuitele, Peanut", "Tuitele, Peanut")
# StrongSurnameMatch(box_name='Tuitele, Peanut', score=100)
```

### `brier_score(y_true: 'np.ndarray', p_pred: 'np.ndarray') -> 'float'` {#brier_score}

Mean squared error between binary outcomes and predicted probabilities.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of realized binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in `[0, 1]`. |

**Returns**

The Brier score (lower is better; 0.0 is perfect).

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_prediction_constants import brier_score
brier_score(np.array([1, 0]), np.array([1.0, 0.0]))
```

### `build_3p_shot_info(p: 'LineupStatSet') -> 'OffLuckShotInfo3P'` {#build_3p_shot_info}

3P-only shot-decomposition wrapper.

Public port of `build3PShotInfo` (`LuckUtils.ts:741-759`) --
remaps build_shot_info`'s generic keys to the 3pm`/
3pa`/3p` suffixes used throughout the luck-adjustment engine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p` | `LineupStatSet` |  | The player's `LineupStatSet`/`IndivStatSet`-shaped dict. |

**Returns**

`{"shot_info_ast_3pm", "shot_info_early_3pa", "shot_info_scramble_3pa", "shot_info_unast_3pm", "shot_info_unknown_3pM", "shot_info_total_3p"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import build_3p_shot_info

info = build_3p_shot_info(player)
print(info["shot_info_total_3p"])
```

### `build_adjusted_3p(p: 'LineupStatSet', info: 'OffLuckShotInfo3P') -> 'OffLuckAdj3P'` {#build_adjusted_3p}

3P-only approx-unassisted/assisted-FG% wrapper.

Public port of `buildAdjusted3P` (`LuckUtils.ts:812-835`, "retained
for bwc [backwards compat]" per the upstream comment) -- a thin remap of
build_adjusted_fg` called with `shot_type="3p"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p` | `LineupStatSet` |  | The (typically base-period) player dict driving `off_3p`/ `off_3p_ast`. |
| `info` | `OffLuckShotInfo3P` |  | An `build_3p_shot_info`-shaped dict (the "biggest sample available" per the upstream comment -- normally the base period, not the sample being luck-adjusted). |

**Returns**

`{"base3P", "unassisted3P", "assisted3P", "baseAssistPct"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import build_3p_shot_info, build_adjusted_3p

base_info = build_3p_shot_info(base_player)
adj = build_adjusted_3p(base_player, base_info)
print(adj["assisted3P"], adj["unassisted3P"])
```

### `build_available_team_list(in_by_year: 'dict[str, list[tuple[TeamId, str, ConferenceId]]]') -> 'dict[ConferenceId, Callable[[str], str]]'` {#build_available_team_list}

Builds a per-conference team-index JSON fragment for

`cbb-on-off-analyzer` (`TeamIdParser.build_available_team_list`,
`TeamIdParser.scala:105-124`) -- the caller inserts the app-specific
index key to get the final JSON string.

See `build_lineup_cli_array`'s docstring for why this port doesn't
attempt to reproduce Scala's hash-map iteration order (both the
conference-level and, here, the team-level grouping) -- the upstream
oracle covering this ordering is itself permanently disabled.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_by_year` | `dict[str, list[tuple[TeamId, str, ConferenceId]]]` |  | Season-key (e.g. `"2018/9"`) -> that season's `(team, ncaa_id, conference)` triples, e.g. from repeated `get_team_triples` calls. |

**Returns**

Conference -> a function `index_key -> JSON-fragment string`, one `' "team": [ ... ],'` block per team in that conference (each block listing every season that team appeared in, in encounter order).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import build_available_team_list
from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId

by_year = {"2018/9": [(TeamId("Kentucky"), "450591", ConferenceId("SEC"))]}
build_available_team_list(by_year)[ConferenceId("SEC")]("test")
```

### `build_base_event(box_lineup: 'LineupEvent') -> 'ShotEvent'` {#build_base_event}

Fills in the fields a shot event can borrow straight from the

box-score lineup, leaving the shot-specific fields as overridable
placeholders (`ShotEventParser.build_base_event`, `:379-410`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event. |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.ShotEvent` with `date`/`location_type`/`team`/`opponent` populated and every other field at its Scala-literal placeholder default (`player=None`, `is_off=True`, `lineup_id=None`, `players=[]`, `score=Score(0, 0)`, `min=0.0`, `loc=ShotLocation(0.0, 0.0)`, `geo=ShotGeo(0.0, 0.0)`, `dist=0.0`, `pts=0`, `value=0`, `ast_by=None`, `is_ast=None`, `is_trans=None`, `raw_event=None`) -- every caller immediately overrides the placeholders it cares about via `dataclasses.replace`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import build_base_event
base = build_base_event(box_lineup)
```

### `build_d_rtg(stat_set: 'LineupStatSet | None', avg_efficiency: 'float', calc_diags: 'bool', override_adjusted: 'bool') -> 'tuple[dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, DRtgDiagnostics | None]'` {#build_d_rtg}

Individual defensive rating (Dean-Oliver DRtg) + diagnostics.

Faithful port of `RatingUtils.buildDRtg` (`RatingUtils.ts:1252-1485`).
Mirrors `build_o_rtg`'s structure (`stat_get` closure,
`calc_diags`/`override_adjusted` flag pair, recursive
un-overridden raw-value pass) over the simpler
`(stat_set, avg_efficiency, calc_diags, override_adjusted)` 4-arg
signature (no roster/extra-team-stat args, confirmed against the TS).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_set` | `LineupStatSet \| None` |  | The player's stat dict. `None` returns an all-`None` 5-tuple (`RatingUtils.ts:1264-1265`'s `if (!statSet)` -- null/undefined only). Unlike `build_o_rtg`, an **empty dict computes cleanly** -- every division in `buildDRtg` is guard-ternary'd (see the module docstring's "Contrast" note), so `{}` does not raise `ZeroDivisionError`. |
| `avg_efficiency` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `calc_diags` | `bool` |  | When `True`, populate the 5th tuple slot (`DRtgDiagnostics`); otherwise it is `None`. |
| `override_adjusted` | `bool` |  | When `True`, apply build_def_overrides` to the raw opponent-FGM/points fields before computing, and additionally recurse once (with `calc_diags=False, override_adjusted=False`) to compute the un-overridden "raw" values for the 3rd/4th tuple slots. |

**Returns**

A 5-tuple `(d_rtg, adj_d_rtg, raw_d_rtg, raw_adj_d_rtg, d_rtg_diags)`: - `d_rtg`: `{"value": DRtg}` when `Opponent_Possessions_Box > 0`, else `None`. - `adj_d_rtg`: `{"value": Adj_DRtgPlus}` under the same guard. - `raw_d_rtg` / `raw_adj_d_rtg`: the un-overridden values from the recursive call when `override_adjusted=True`; `None` otherwise (unlike `build_o_rtg`, there is no internal-usage special case here -- the TS destructures only the first 2 slots of the recursive 5-tuple). - `d_rtg_diags`: the full `DRtgDiagnostics` dict (`None` unless `calc_diags=True`).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_d_rtg

d_rtg, adj_d_rtg, _, _, diags = build_d_rtg(player, 100.0, True, False)
print(d_rtg["value"], diags["dRtg"])

# Override-adjusted (manual 3P-defense-% override applied)

d_rtg2, adj_d_rtg2, raw_d_rtg2, raw_adj_d_rtg2, _ = build_d_rtg(
    player, 100.0, False, True,
)
```

### `build_efficiency_margins(mutable_stat_set: 'LineupStatSet', key_override: 'str | None' = None) -> 'None'` {#build_efficiency_margins}

Derive `off_net` / `off_raw_net` on a stat set, in place.

Faithful port of `LineupUtils.buildEfficiencyMargins` (`LineupUtils.ts:145`).
`off_net` is `off_adj_ppp - def_adj_ppp` (adjusted efficiency margin);
`off_raw_net` is `off_ppp - def_ppp` (raw/unadjusted margin). Both are
only written when their two source fields are both present on
`mutable_stat_set`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_stat_set` | `LineupStatSet` |  | The `LineupStatSet` (or team-report equivalent) to mutate in place. |
| `key_override` | `str \| None` | `None` | `"value"` or `"old_value"` -- which sub-key to read from the source fields and write into `off_net` / `off_raw_net`. When `None` (the default), the upstream `nonLuckKey` fallback applies: use `"old_value"` if `mutable_stat_set["off_ppp"]["old_value"]` is present, otherwise `"value"`. When given explicitly, the written field is merged onto any existing `off_net` / `off_raw_net` dict (so a second call with the other key preserves the first call's key) rather than replacing it outright. |

**Returns**

None. `mutable_stat_set` is mutated in place.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import build_efficiency_margins

build_efficiency_margins(team_info, "value")
off_ppp = team_info.get("off_ppp")
if isinstance(off_ppp, dict) and off_ppp.get("old_value") is not None:
    build_efficiency_margins(team_info, "old_value")
print(team_info["off_net"]["value"])
```

### `build_exp_3p(info: 'OffLuckShotTypeAndAdj3P') -> 'float'` {#build_exp_3p}

Expected made-3P count given a player's shot-type mix + shooting %s.

Public port of `buildExp3P` (`LuckUtils.ts:838-847`): `(assisted
3PM * assisted3P%) + (unassisted 3PM * unassisted3P%) +
(early/scramble/unknown 3PA * base3P%)`. Pure weighted sum -- no
division, so this introduces no landmine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `info` | `OffLuckShotTypeAndAdj3P` |  | A dict carrying both `build_3p_shot_info`'s `shot_info_*` keys and `build_adjusted_3p`'s `*3P` keys (i.e. an `OffLuckShotTypeAndAdj3P`). |

**Returns**

The expected number of made 3-pointers (`3P% * total 3P`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import (
    build_3p_shot_info, build_adjusted_3p, build_exp_3p,
)

base_info = build_3p_shot_info(base_player)
info = {**build_3p_shot_info(player), **build_adjusted_3p(base_player, base_info)}
expected_makes = build_exp_3p(info)
```

### `build_lineup_cli_array(in_triples: 'list[tuple[TeamId, str, ConferenceId]]') -> 'dict[ConferenceId, str]'` {#build_lineup_cli_array}

Builds the per-conference team array for `lineups-cli.sh` files

(`TeamIdParser.build_lineup_cli_array`, `TeamIdParser.scala:94-100`).

**Iteration-order note (upstream-DISABLED context).** Scala's
`List.groupBy` returns an immutable `Map` whose iteration order is
hash-bucket-dependent, not insertion order -- the disabled oracle's
expected `Map.toList` ordering (`SEC` before `B1G`) reflects that
JVM-specific hashing, not a documented contract. This port uses a plain
`dict` (Python 3.7+ preserves insertion order), the natural pythonic
choice; since the upstream test asserting a specific cross-conference
order is itself permanently disabled (see the module docstring), there
is no live oracle to match here regardless of dict vs hash-map ordering.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_triples` | `list[tuple[TeamId, str, ConferenceId]]` |  | `(team, ncaa_id, conference)` triples, e.g. from `get_team_triples`. |

**Returns**

Conference -> newline-joined `" 'ncaa_id::URL-encoded team name'"` lines, one per team in that conference (in encounter order).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import build_lineup_cli_array
from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId

triples = [(TeamId("Penn St."), "1", ConferenceId("B1G"))]
build_lineup_cli_array(triples)[ConferenceId("B1G")]
# "   '1::Penn+St.'"
```

### `build_lineup_id(players: 'list[PlayerCodeId]') -> 'LineupId'` {#build_lineup_id}

Builds a lineup id from a list of players (`ExtractorUtils.scala:602-606`):

every player's `code`, sorted, joined with `"_"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerCodeId]` |  | The players on the floor for this lineup. |

**Returns**

The opaque `~sportsdataverse.mbb.mbb_ncaa_models.LineupId`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId
from sportsdataverse.mbb.mbb_ncaa_stints import build_lineup_id
build_lineup_id([PlayerCodeId("BbBob", PlayerId("Bob")), PlayerCodeId("AaAl", PlayerId("Al"))])
# LineupId("AaAl_BbBob")
```

### `build_net_points(player_rapm_and_poss_pct: 'LineupStatSet', ortg: 'ORtgDiagnostics', drtg: 'DRtgDiagnostics', avg_eff: 'float', scale_type: "Literal['T%', 'P%', '/G']", num_games: 'float' = 1, missing_game_adjustment: 'float' = 1) -> 'NetPoints'` {#build_net_points}

Decompose ORtg/DRtg + RAPM into a Net-Points-like breakdown.

Faithful port of `RatingUtils.buildNetPoints` (`RatingUtils.ts:1036-1234`).
Genuinely public upstream (called from `buildLeaderboards.ts`,
`PlayerImpactBreakdownTable.tsx`, and `ImpactBreakdownUtils.ts`), so
this port is public too.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_rapm_and_poss_pct` | `LineupStatSet` |  | The player's stat dict -- reads `off_team_poss_pct`/`def_team_poss_pct` (nullish-coalesced to `0.0`, see nullish`) and, when present, `off_adj_rapm`/`def_adj_rapm` (each a `{"value": float}` "Statistic"-shaped field) for the RAPM "with-or-without-you" (WOWY) deltas. |
| `ortg` | `ORtgDiagnostics` |  | An `ORtgDiagnostics` dict from `build_o_rtg` (`calc_diags=True`), typically with `adjPtsFactor`/ `adjPossFactor` overridden from their `1` default by a missing-possession correction. |
| `drtg` | `DRtgDiagnostics` |  | A `DRtgDiagnostics` dict from `build_d_rtg` (`calc_diags=True`). If it carries an `onBallDiags` key (this port's `build_d_rtg` never sets one -- see the module docstring's deferred-work note), the on-ball-adjusted branch is used instead of the base `dRtg`/`adjDRtgPlus`. |
| `avg_eff` | `float` |  | League/context average efficiency. |
| `scale_type` | `Literal['T%', 'P%', '/G']` |  | `"T%"` (scale by on-floor team-possession share, `avgEff`-adjusted possession count), `"P%"` (scale to 100 possessions), or `"/G"` (scale to per-game). |
| `num_games` | `float` | `1` | Divisor for the `"/G"` scale type. Default `1`. |
| `missing_game_adjustment` | `float` | `1` | Multiplier folded into the `"T%"` scale factor for imputed-missing-games correction. Default `1`. |

**Returns**

A `NetPoints` dict -- 20 keys, plus an optional `defNetPtsIndiv` 21st key present only when `drtg["onBallDiags"]` is set (TS-verbatim key names throughout).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_o_rtg, build_d_rtg, build_net_points

_, _, _, _, o_diags = build_o_rtg(player, {}, {}, 100.0, True, False)
_, _, _, _, d_diags = build_d_rtg(player, 100.0, True, False)
net_pts = build_net_points(player, o_diags, d_diags, 100.0, "T%")
print(net_pts["offNetPts"], net_pts["defNetPts"])
```

### `build_new_player_list(curr: 'LineupEvent', prev: 'LineupEvent') -> 'list[PlayerCodeId]'` {#build_new_player_list}

Builds a player list from the previous (or current, if pre-initialized)

lineup and the current lineup's in/out subs (`ExtractorUtils.scala:654-693`).

Three candidate reconciliations are computed --

* `poss1`: `prev.players` minus everyone in `curr.players_out`,
  plus everyone in `curr.players_in` (subs-out removed first, then
  subs-in merged on top).
* `poss2`: `prev.players` plus `curr.players_in`, minus everyone in
  `curr.players_out` (subs-in merged first, then subs-out removed).
* `poss3`: just `curr.players_in`.

-- and whichever has exactly 5 players wins (checked in `poss3`,
`poss1`, `poss2` order); if none does, a common play-by-play error
(a player appearing in both the in- and out- lists for the same sub
event) is corrected by dropping the common players from both sides
before reconciling via the `poss1` recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr` | `LineupEvent` |  | The lineup event whose `players_in`/`players_out` describe the subs to apply. |
| `prev` | `LineupEvent` |  | The lineup event whose `players` is the starting roster (complete_lineup` passes `curr` for both arguments -- see its docstring). |

**Returns**

The reconciled player list, sorted by `code`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import build_new_player_list
build_new_player_list(curr_lineup_event, prev_lineup_event)
```

### `build_o_rtg(stat_set: 'LineupStatSet | None', roster_stats_by_code: 'dict[str, LineupStatSet] | None', extra_team_stat_info: 'LineupStatSet', avg_efficiency: 'float', calc_diags: 'bool', override_adjusted: 'bool') -> 'tuple[dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, ORtgDiagnostics | None]'` {#build_o_rtg}

Individual offensive rating (Dean-Oliver ORtg) + diagnostics.

Faithful port of `RatingUtils.buildORtg` (`RatingUtils.ts:398-960`).
See the module docstring for the signature-vs-brief note (this mirrors
the TS 6-positional-arg / 5-tuple contract verbatim, snake_cased) and
the diagnostics-dict key-naming convention (TS-verbatim, not
snake_cased).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_set` | `LineupStatSet \| None` |  | The player's `LineupStatSet` (ES-aggregation-shaped per-player doc, "IndivStatSet" upstream). `None` returns an all-`None` 5-tuple (`RatingUtils.ts:412-413`'s `if (!statSet)` -- null/undefined only). An **empty dict does NOT short-circuit** (`{}` is truthy in JS and falls through to compute upstream); in this port it falls into unguarded-division landmine 1 and raises `ZeroDivisionError` where the TS degrades to a NaN-laced degenerate result -- see the module docstring's landmine list. |
| `roster_stats_by_code` | `dict[str, LineupStatSet] \| None` |  | `{player_code: LineupStatSet}` for every player on the roster -- used for the approximate team-ORB apportionment and the per-shot-location assisted-eFG fallback. `None` is treated as `{}` (every vendored jest call passes a literal `{}`). |
| `extra_team_stat_info` | `LineupStatSet` |  | `{"total_off_to": {...}, "sum_total_off_to": {...}}` -- team-level TOV bookkeeping used to compute "unblamed" team turnovers apportioned by `off_team_poss_pct`. |
| `avg_efficiency` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `calc_diags` | `bool` |  | When `True`, populate the 5th tuple slot (`ORtgDiagnostics`); otherwise it is `None`. |
| `override_adjusted` | `bool` |  | When `True`, apply build_off_overrides` to the raw made/attempt/turnover fields before computing, and additionally recurse once (with `calc_diags=False, override_adjusted=False`) to compute the un-overridden "raw" values for the 3rd/4th tuple slots. |

**Returns**

A 5-tuple `(o_rtg, adj_o_rtg, raw_o_rtg, raw_adj_o_rtg, o_rtg_diags)`: - `o_rtg`: `{"value": ORtg}` when `TotPoss > 0`, else `None`. - `adj_o_rtg`: `{"value": Adj_ORtgPlus}` when `TotPoss > 0`, else `None`. - `raw_o_rtg`: when `calc_diags or override_adjusted`, the un-overridden `ORtg` (`None` if `override_adjusted=False`, since no un-overridden pass was computed); otherwise a special internal-recursion value `{"value": usage}` (`RatingUtils.ts:835`'s "if called internally return usage here" case). - `raw_adj_o_rtg`: the un-overridden `adj_o_rtg` (`None` when `override_adjusted=False`). - `o_rtg_diags`: the full `ORtgDiagnostics` dict (`None` unless `calc_diags=True`).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_o_rtg

o_rtg, adj_o_rtg, _, _, diags = build_o_rtg(
    player, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
    100.0, True, False,
)
print(o_rtg["value"], diags["oRtg"])

# Override-adjusted (manual shooting-% overrides applied)

o_rtg2, adj_o_rtg2, raw_o_rtg2, raw_adj_o_rtg2, _ = build_o_rtg(
    player, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
    100.0, False, True,
)
```

### `build_partial_lineup_list(reversed_partial_events: 'Iterable[PlayByPlayEvent]', box_lineup: 'LineupEvent') -> 'list[LineupEvent]'` {#build_partial_lineup_list}

Converts a stream of partially parsed events into a list of lineup

events (`ExtractorUtils.scala:118-227`).

`box_lineup` is expected to carry every player on the team's roster,
with the top 5 (by whatever order the caller supplies) being the
starters. The events are first reordered into forward-chronological
order via `reorder_and_reverse`, then folded through a
`LineupBuildingState`:

* A `SubIn`/`SubOut` event either **opens a new stint** (if the
  current lineup `~LineupBuildingState.is_active`: the just-built
  lineup is completed via complete_lineup` and appended, and a
  fresh lineup is started via new_lineup_event`) or **keeps
  accumulating** onto the current (not-yet-active) lineup via
  `~LineupBuildingState.with_player_in`/`with_player_out`. A
  `SubIn` event naming literally `"team"` (case-insensitive) is
  always a no-op, win or lose the active check; `SubOut` has no such
  exemption. Every sub name is resolved through
  `~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` first.
* The **old/new play-by-play format** is latched (once, forever) the
  first time a sub name is seen: an all-caps name (no lowercase letters
  at all) means the old (pre-2018-ish) format; this only ever updates on
  a sub-event branch, never on a `GameBreakEvent`.
* `OtherTeamEvent`/`OtherOpponentEvent` accumulate onto the current
  lineup via `~LineupBuildingState.with_team_event`/
  `with_opponent_event` plus `~LineupBuildingState.with_latest_score`.
* `GameBreakEvent` (half/quarter/OT boundary short of the game's end)
  completes the current lineup and starts a fresh one -- but whether
  that fresh lineup **resets to the starting 5** or **carries over** the
  just-completed lineup's players depends on the (possibly still
  unlatched) `old_format` flag: old format resets to
  `starters_only`, new format (2018+, the default once
  `box_lineup.team.year.value >= 2018` if never latched) carries over.
* `GameEndEvent` only completes the current lineup (no new one is
  started, and it is not appended to `prev` here -- `LineupBuildingState.build`
  folds it in as the trailing entry).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `reversed_partial_events` | `Iterable[PlayByPlayEvent]` |  | The full play-by-play event stream for one team's box-score lineup, in reverse-chronological order. |
| `box_lineup` | `LineupEvent` |  | The team's roster lineup event (`players` is the full roster; the first 5 are the starters). |

**Returns**

The chronological list of lineup (stint) events.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import build_partial_lineup_list
stints = build_partial_lineup_list(reversed(events), box_lineup)
print(len(stints))
```

### `build_player_code(in_name: 'str', team: 'Optional[TeamId]') -> 'PlayerCodeId'` {#build_player_code}

Build a short player code from a name, in any of the NCAA formats

(`ExtractorUtils.scala:290-391`).

The code is a compact `FirstInitials + [Middle] + Lastname` string
(e.g. `"Mitchell, Makhi"` -> `"MiMitchell"`) unique within a team +
season, used to join play-by-play name fragments to box-score rosters.
Supported input shapes: `"First [Middle...] Last"` (no comma),
`"Last, First [Middle...]"`, and `"Last, Suffix, First"`.

The full name is first corrected via the team-scoped misspelling table
and diacritic-stripped -- that corrected string becomes the returned
`~sportsdataverse.mbb.mbb_ncaa_models.PlayerId`. Each fragment is
lowercased, de-dotted, individually misspelling-corrected, and truncated
to `PLAYER_CODE_MAX_FRAGMENT_LENGTH`. Junk fragments (jr/sr/roman
numerals/ordinals/digit-leading) are dropped -- except the first-name
fragment, which is never dropped for being short.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_name` | `str` |  | The raw player name as it appears in the source HTML. |
| `team` | `Optional[TeamId]` |  | The team, for team-scoped misspelling corrections; `None` uses only the generic corrections. |

**Returns**

A `PlayerCodeId` with the derived `code` and the corrected full name as `id` (`ncaa_id` is always `None` here).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code
pc = build_player_code("Mitchell, Makhi", None)
print(pc.code)  # "MiMitchell"

# Play-by-play (all-caps, truncated) form

build_player_code("BIGBY-WILLIAM,KAVELL", None).code  # "KaBigby-will"
```

### `build_player_context(players: 'list[PlayerOnOffStats]', lineups: 'list[LineupStatSet]', players_baseline: 'dict[PlayerId, IndivStatSet]', stats_averages: 'PureStatSet', avg_efficiency: 'float', agg_value_key: 'ValueKey' = 'value', config: 'RapmConfig' = {'prior_mode': -1, 'removal_pct': 0.06, 'fixed_regression': -1}) -> 'RapmPlayerContext'` {#build_player_context}

Build the context object the RAPM matrix-solve layer consumes.

Faithful port of `RapmUtils.buildPlayerContext` (`RapmUtils.ts:427-541`).
Removes low-possession players (`config["removal_pct"]` of total
on+off possessions), flags fully-removed lineups (mutating `lineups`
in place -- see the module docstring's landmine 5), builds the
player-to-column index, and folds `build_priors` into
`prior_info`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerOnOffStats]` |  | The per-player on/off splits (`PlayerOnOffStats`), e.g. `mbb_lineup_stats.lineup_to_team_report(...)["players"]`. |
| `lineups` | `list[LineupStatSet]` |  | The per-lineup `LineupStatSet` docs feeding this team's aggregate (**mutated in place** -- see landmine 5). |
| `players_baseline` | `dict[PlayerId, IndivStatSet]` |  | `{player_id: IndivStatSet}` -- forwarded to `build_priors` unchanged. |
| `stats_averages` | `PureStatSet` |  | League/context average stat set -- forwarded to `build_priors` unchanged. |
| `avg_efficiency` | `float` |  | League/context average efficiency. |
| `agg_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- forwarded to `build_priors` as its `value_key` (only affects prior calculations, not the lineup-filtering/aggregation above it). |
| `config` | `RapmConfig` | `{'prior_mode': -1, 'removal_pct': 0.06, 'fixed_regression': -1}` | Removal-percent / prior-mode / regression config. Defaults to `DEFAULT_RAPM_CONFIG`; never mutated by this function (only `config["removal_pct"]`/`config["prior_mode"]` are read), matching the TS default parameter's own read-only usage. |

**Returns**

A `RapmPlayerContext`.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report
from sportsdataverse.mbb.mbb_rapm import build_player_context, DEFAULT_RAPM_CONFIG

report = lineup_to_team_report({"lineups": buckets, "error_code": None})
ctx = build_player_context(
    report["players"], buckets, {}, {}, 100.0, "value", DEFAULT_RAPM_CONFIG
)
print(ctx["num_players"], ctx["team_info"]["off_poss"]["value"])

# Filtering lineups by side (the ``filtered_lineups`` closure)

off_lineups = ctx["filtered_lineups"]("off")
def_lineups = ctx["filtered_lineups"]("def")
```

### `build_position(confs: 'dict[str, float]', confs_no_height: 'dict[str, float] | None', player: 'dict[str, Any]', team_season: 'str') -> 'tuple[str, str]'` {#build_position}

Classify a player into a position label + diagnostic trace string.

Faithful port of `PositionUtils.buildPosition` (`PositionUtils.ts:401-580`)
-- the PG / s-PG / CG / WG / WF / S-PF / PF/C / C decision tree. A
`ABSOLUTE_POSITION_FIXES` manual override short-circuits the whole
tree (recursing once, with `team_season=""`, purely to compute the
diagnostic "what would this have been" string); otherwise the function
walks the confidence-threshold / assist-rate / 3PT-rate branch cascade,
applies the "too few effective possessions" (< 25) fallback, and
reconciles the result against roster metadata via `using_roster_pos`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `confs` | `dict[str, float]` |  | The 5-way positional confidence dict (`TRAD_POS_LIST` keys), typically the height-adjusted output of `build_position_confidences`. |
| `confs_no_height` | `dict[str, float] \| None` |  | The pre-height-adjustment confidences, or `None` when the caller has no height data. When present, a PG <-> s-PG flip caused solely by the height adjustment is reverted (the `maybeIgnoreHeight` closure, `ts:433-457`). The check is `is not None` (JS object-truthiness: an empty dict is still a truthy JS object), NOT a Python-falsy `if confs_no_height`. |
| `player` | `dict[str, Any]` |  | The player stat dict. Reads `key` (override lookup), `off_assist` / `off_3pr` / `off_usage` / `off_team_poss` (each `{"value": N}`-wrapped), and `roster` (a plain `{"pos": ..., "role": ...}` dict of un-wrapped strings). |
| `team_season` | `str` |  | `"{sport}_{team}_{season}"` key into `ABSOLUTE_POSITION_FIXES`. Pass `""` to disable override lookup for a given call (the recursive diagnostic call inside the override branch does exactly this). |

**Returns**

A `(position, diagnostic)` tuple. `position` is one of `ID_TO_POSITION`'s keys; `diagnostic` is a human-readable trace of which rule fired, byte-identical to the TS's template strings (including `.toFixed(1)`-style percentage formatting).

**Example**

```python
from sportsdataverse.mbb.mbb_positions import build_position, TRAD_POS_LIST
confs = dict(zip(TRAD_POS_LIST, [0.9, 0.1, 0, 0, 0]))
player = {"off_assist": {"value": 0.10}, "off_3pr": {"value": 0.20},
          "off_team_poss": {"value": 1000}, "off_usage": {"value": 0.20}}
build_position(confs, None, player, "Men_Boston College_2019/20")

# A manual-override short-circuit

build_position(confs, None, {"key": "Popovic, Nik",
    "off_usage": {"value": 1}, "off_team_poss": {"value": 200},
    "off_assist": {"value": 0.10}}, "Men_Boston College_2019/20")
```

### `build_position_confidences(player: 'dict[str, Any]', height_in: 'float | None' = None) -> 'tuple[dict[str, float], dict[str, Any]]'` {#build_position_confidences}

Build the 5-way positional confidence vector for a player.

Faithful port of `PositionUtils.buildPositionConfidences`
(`PositionUtils.ts:263-338`). Derives the six `calc_*` ratios from the
player's box-score fields, dot-products the resulting 17-feature vector
against `POSITION_FEATURE_WEIGHTS` (each field regressed via
`regress_shot_quality` and multiplied by its per-feature `scale`)
plus the `POSITION_FEATURE_INIT` intercepts, applies a softmax over
the five raw scores, and -- when `height_in` is supplied -- reweights the
confidences via `incorporate_height`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player` | `dict[str, Any]` |  | The player stat dict (ES-aggregation bucket shape); each stat field is `{"value": N}`. Reads `total_off_assist`, `total_off_to`, `off_3p`, `off_efg`, `off_2pmid`, `off_2prim`, `total_off_fga`, `total_off_fta`, `total_off_ftm` (for the `calc_*` ratios) plus every non-`calc_` field in `POSITION_FEATURE_WEIGHTS`. |
| `height_in` | `float \| None` | `None` | Optional player height in inches. When truthy, the returned confidences are height-adjusted; when `None` / `0`, the raw softmax confidences are returned. (JS `height_in ? ... : ...` falsy check, ts:324 -- a `0` height is treated as "no height".) |

**Returns**

A `(confidences, diagnostics)` tuple. `confidences` maps each `TRAD_POS_LIST` key (in order) to its final confidence. `diagnostics` carries `"scores"` (raw scores x `0.1`, keyed by position), `"confsNoHeight"` (the pre-height confidences, present only when `height_in` is truthy, else `None`), and `"calculated"` (the six derived `calc_*` ratios). The upstream diag object has exactly these three fields -- no UI-only fields are dropped.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import build_position_confidences
confs, diags = build_position_confidences(player_bucket)
print(confs["pos_pg"], diags["calculated"]["calc_ast_tov"])

# Height-adjusted confidences

confs_h, diags_h = build_position_confidences(player_bucket, 78.0)
```

### `build_positional_aware_filter(filter_str: 'str') -> 'tuple[list[dict[str, Any]], list[dict[str, Any]], bool]'` {#build_positional_aware_filter}

Decompose a search-filter string into positionally-aware +ve/-ve fragments.

Faithful port of `PositionUtils.buildPositionalAwareFilter`
(`PositionUtils.ts:764-828`). Picks a fragment separator by scanning
`[";", "/", ","]` in priority order for the first one present anywhere
in `filter_str` (a fragment separator of `"!!!"` -- never itself
present -- is the "no separator found" fallback, which leaves the whole
string as a single fragment). Splits on that separator, trims whitespace,
drops empty fragments and `[`-prefixed ones (reserved for aggregation-key
filters elsewhere in the app), then routes each fragment to the positive
or negative bucket by a leading `-`, and parses each fragment's optional
`=<tokens>` position spec via decomp_positional_filter_fragment`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filter_str` | `str` |  | A raw filter string, e.g. `"test1=pg / -test2=Pf+C / test3"`. |

**Returns**

A `(positive_fragments, negative_fragments, has_position)` triple. Each fragment is `{"filter": <lowercased name>, "pos": [indices]}`. `has_position` is `True` iff any fragment (either side) carried at least one recognized position token.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import build_positional_aware_filter
build_positional_aware_filter("test1=pg / -test2=Pf+C / test3")
```

### `build_priors(players_baseline: 'dict[PlayerId, IndivStatSet]', stats_averages: 'PureStatSet', avg_efficiency: 'float', col_to_player: 'list[str]', prior_mode: 'float', value_key: 'ValueKey' = 'value') -> 'RapmPriorInfo'` {#build_priors}

Build strong/weak per-player RAPM priors for every column.

Faithful port of `RapmUtils.buildPriors` (`RapmUtils.ts:237-407`).
See the module docstring's landmine list, item 1, for the critical
Python-vs-JS `{}`-truthiness gotcha this function's implementation
deliberately avoids.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players_baseline` | `dict[PlayerId, IndivStatSet]` |  | `{player_id: IndivStatSet}` -- the most-general per-player baseline info (in production, sourced from `mbb_ratings.build_productivity`'s output; see the module docstring's "RAPM prior source" note). |
| `stats_averages` | `PureStatSet` |  | League/context average stat set, used by the (currently dead-code, see landmine 4) `get_prior_basis` fallback and by `with_avg_or_undef`'s nil-check gate. |
| `avg_efficiency` | `float` |  | League/context average efficiency. |
| `col_to_player` | `list[str]` |  | The player ids, in column order -- `playersStrong`/ `playersWeak` are index-aligned with this list. |
| `prior_mode` | `float` |  | `-1` for adaptive mode, `-2` (or lower) for no prior, `0`-`1` for a fixed strong-prior weight. |
| `value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- allows priors to be built from luck-adjusted parameters. |

**Returns**

A `RapmPriorInfo`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import build_priors

priors = build_priors({}, {}, 100.0, ["Wiggins, Aaron"], -1)
print(priors["players_weak"][0])
```

### `build_productivity(o_rtg: 'float', o_adj: 'float', usage: 'float', avg_efficiency: 'float') -> 'dict[str, float]'` {#build_productivity}

Public port of `RatingUtils.buildProductivity` (`RatingUtils.ts:963-990`).

Promoted to public in Task 2.3 -- see the module docstring's "Ported
behavior" section for the promotion rationale (Phase-3 RAPM needs to
import this across module boundaries).

Converts `ORtg` and a few other numbers into "productivity" using Dean
Oliver's PUE ("Player Usage Efficiency") formulation, SoS-adjusted via
`o_adj = avgEfficiency / Def_SOS`. **RAPM prior source (Phase 3):**
`Adj_ORtgPlus` is the value RAPM uses as an individual-offense prior --
see `PLAN-phase2.md`'s self-review notes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `o_rtg` | `float` |  | The player's (possibly override-adjusted) `ORtg`. |
| `o_adj` | `float` |  | `avg_efficiency / Def_SOS` -- the strength-of-schedule adjustment factor. |
| `usage` | `float` |  | `100 * TotPoss / (Team_Poss or 1)` -- the player's possession-usage percentage. |
| `avg_efficiency` | `float` |  | The league/context average efficiency (`100` in every vendored jest call). |

**Returns**

float, "Adj_ORtgPlus": float, "Usage_Bonus": float, "SoS_Bonus": float}`` -- keys kept TS-verbatim (see module docstring's naming-convention note).

### `build_strength_adjusted_stats(teams: 'Sequence[TeamDetail]', *, max_iterations: 'int' = 100, tolerance: 'float' = 1e-06) -> 'StrengthAdjustedResult'` {#build_strength_adjusted_stats}

Run the full strength-adjustment compute over a team list.

Ports the COMPUTE half of the CLI `main()` (`ts:594-662`): dedupe
teams by name (first-wins, as `main` does across its tier files),
compute possession splits + league averages, run
`run_iterative_adjustment_with_hca`, then assemble each team's
`raw` / `adj` / `adj_hca` field maps. The file/CLI glue
(`fs`/`argv`/`dataLastUpdated`/serialization) is intentionally not
ported -- pass an already-loaded `team_details` list.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | The `team_details` team dicts (each `{team_name, conf, opponents: [...]}`). Duplicate `team_name`s keep the first occurrence. |
| `max_iterations` | `int` | `100` | Solver iteration cap (default `MAX_ITERATIONS`). |
| `tolerance` | `float` | `1e-06` | Solver convergence tolerance (default `TOLERANCE`). |

**Returns**

A `StrengthAdjustedResult` (`averages` per field + per-team `raw`/`adj`/`adj_hca`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import build_strength_adjusted_stats

result = build_strength_adjusted_stats(team_details)
print(result.averages["3p"].league_off)
print(result.teams[0].adj["3p"])  # {"off": ..., "def": ...}
```

### `build_sub_error(*subids: 'str', error: 'str') -> 'ParseError'` {#build_sub_error}

Build a location-less `ParseError` from id fragments

(`ParseUtils.build_sub_error`, `ParseUtils.scala:83-85`, delegating
through `build_error`/`build_errors`/`build_error_id` with
`location=""`/`base_id=""`; the `shapeless`-based
`sequence_kv_results` accumulation machinery in the same file is out
of scope).

Scala's call shape is curried -- `build_sub_error("team")("message")`
(two argument groups: varargs `subids`, then a single `error`
string). Python has no currying sugar for that shape, so `subids` is
a plain `*args` tuple and `error` is a required keyword-only
argument at the same call site.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `error` | `str` |  | The single human-readable error message. |

**Returns**

A `ParseError` with `location=""` and `messages=[error]`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import build_sub_error

err = build_sub_error("team", error="Could not match team names")
err.id  # '[team]'
```

### `build_tidy_player_context(box_lineup: 'LineupEvent') -> 'TidyPlayerContext'` {#build_tidy_player_context}

Build the alternative player-code lookup maps for a box-score lineup

(`LineupErrorAnalysisUtils.build_tidy_player_context`, `:59-73`).

Sometimes the play-by-play uses `SURNAME,INITIAL` instead of
`SURNAME,NAME`, or `SURNAME,NAME1` instead of `SURNAME,NAME1
NAME2` -- both collapse to the same *truncated* code, so grouping by
truncated code (and only keeping groups with exactly one distinct name)
lets `tidy_player` recover the box-score name unambiguously.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The box-score lineup event to index. |

**Returns**

A fresh `TidyPlayerContext` (empty `resolution_cache`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context
ctx = build_tidy_player_context(box_lineup)
```

### `build_weak_prior_from_rapm(rapm_results: 'list[float]', off_or_def: 'str') -> 'list[dict[str, float]]'` {#build_weak_prior_from_rapm}

Wrap a flat RAPM-estimate vector into `playersWeak`-shaped dicts.

Faithful port of `RapmUtils.buildWeakPriorFromRapm` (`RapmUtils.ts:410-419`),
used only by `pick_ridge_regression`'s `use_recursive_weak_prior`
branch to substitute the just-computed (pre-strong-prior) RAPM values as
the *weak* prior for a follow-up `apply_weak_priors` call -- "the
recursive prior" per the upstream `/** For "recursive" prior */` comment.

**Uncovered by the oracle** -- `semiRealRapmResults.testContext.priorInfo
.useRecursiveWeakPrior` is `false`, so `RapmUtils.test.ts`'s
`"pickRidgeRegression"` test never calls this function. Ported
faithfully from TS regardless (per "TS governs"); flagged as a documented
gap rather than backed by a synthetic test, matching this module's
existing convention for other upstream-untested branches (e.g. the
"Task 3.3 coverage gap" note above).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rapm_results` | `list[float]` |  | A flat per-player RAPM estimate vector, e.g. `pick_ridge_regression`'s own `results_pre_prior`. |
| `off_or_def` | `str` |  | `"off"` or `"def"` -- selects the output key, `f"{off_or_def}_adj_ppp"`. |

**Returns**

One `{f"{off_or_def}_adj_ppp": rapm}` dict per input element, index-aligned with `rapm_results`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import build_weak_prior_from_rapm

weak_prior = build_weak_prior_from_rapm([5.0, 4.5], "off")
print(weak_prior[0])  # {"off_adj_ppp": 5.0}
```

### `cached_path(path: 'str', *, cache_dir: 'Optional[Path]' = None) -> 'Path'` {#cached_path}

Return the on-disk cache file path for *path*, without touching it.

Layout: `{cache_dir}/stats.ncaa.org/{dirs...}/{last}.html`, where the
URL path's `/`-separated segments become nested directories and a
query string is folded into the final filename as {safe_query}.html`
(unsafe characters replaced with `). Two different query strings for
the same base path therefore always produce two distinct cache files.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  |  |
| `cache_dir` | `Optional[Path]` | `None` |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import cached_path
cached_path("contests/4690813/play_by_play")
# .../stats.ncaa.org/contests/4690813/play_by_play.html
cached_path("contests/4690813/box_score?period_no=2")
# .../stats.ncaa.org/contests/4690813/box_score__period_no=2.html
```

### `calc_collinearity_diag(weight_matrix: 'NDArray[np.float64]', ctx: 'RapmPlayerContext') -> 'RapmPreProcDiagnostics'` {#calc_collinearity_diag}

Multi-collinearity diagnostic between the players in an off/def design matrix.

Faithful port of `RapmUtils.calcCollinearityDiag` (`RapmUtils.ts:1629-1760`).
Runs an SVD of `weight_matrix`, builds condition indices ("lineup
combos") from the ratio of the largest to each singular value, and a
variance-decomposition-proportions ("VDP") matrix identifying which
players load onto which collinear combo -- the classic Belsley-Kuh-Welsch
collinearity-diagnostics recipe (see the upstream comment's
[colldiag.m](https://github.com/brian-lau/colldiag/blob/master/colldiag.m)
citation). Also builds a plain Pearson player/player correlation matrix
(calc_player_correlations`) and folds it into a possession
-weighted `adaptive_correl_weights` summary per player.

**`numpy.linalg.svd(weight_matrix, full_matrices=False)` replaces
`svd-js`'s `SVD(weightMatrix, false)`.** Both are the standard
Golub-Kahan-Reinsch decomposition (`A = U @ diag(S) @ Vᵀ`); numpy's
`Vh` return value already *is* `Vᵀ` (what the TS code separately
computes via `transpose(matrix(v))`), so this port skips that
transpose. The TS code (and this port) never reads `u`/the first SVD
return -- only `q`/`S` (singular values) and `v`/`Vᵀ`. Singular
-vector **sign is immaterial here**: every place `V` is used
(`phiMatrix`/`phi_matrix`) squares each entry (`val * val`), and a
per-singular-value sign flip on `U`/`V` together is a valid SVD
regardless -- so any `U`/`V` sign convention difference between
`svd-js` and LAPACK (numpy's backend) cannot change this function's
output. **Singular-value ordering is likewise immaterial**: both this
port and the TS source explicitly re-sort `q` (ascending, carrying the
original index along) before using it, so whichever order either SVD
implementation returns values in, the final result only depends on the
*values themselves* (up to the explicit resort), not on numpy's native
descending convention vs whatever order `svd-js` happens to return.

**`correl_matrix`/`poss_correl_matrix` stay `numpy.ndarray`** (see
the module docstring's "Task 3.6 notes" for why this doesn't hit the
Task 3.5 "`ndarray` breaks deep `==`" concern).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `weight_matrix` | `NDArray[float64]` |  | An off/def design matrix, shape `(num_lineups, ctx["num_players"])` (e.g. `calc_player_weights`'s first return value, or a hand-built matrix for isolated testing). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. `ctx["num_players"]` sizes every per-player structure; `ctx["col_to_player"]` keys `player_combos`. |

**Returns**

A `RapmPreProcDiagnostics`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_collinearity_diag, calc_player_weights

off_weights, _ = calc_player_weights(ctx)
diag = calc_collinearity_diag(off_weights, ctx)
print(diag["lineup_combos"][0])  # the worst-conditioned combo
```

### `calc_def_player_luck_adj(sample: 'LineupStatSet', base: 'LineupStatSet', avg_eff: 'float') -> 'DefLuckAdjustmentDiags'` {#calc_def_player_luck_adj}

Defensive 3P-luck adjustment for a single player.

Faithful port of `LuckUtils.calcDefPlayerLuckAdj` (`LuckUtils.ts:402-426`).
Unlike `calc_off_player_luck_adj`, this is **not** a pure
delegation -- see the module docstring's `calc_def_player_luck_adj`
note for the `translate()` remap this wraps around
`calc_def_team_luck_adj`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample` | `LineupStatSet` |  | The player's stat dict for the period being luck-adjusted (must carry `oppo_total_def_3p_made`/`oppo_total_def_3p_attempts` -- there is no player-level `def_3p` field upstream, hence the remap). |
| `base` | `LineupStatSet` |  | The player's stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |

**Returns**

Same shape as `calc_def_team_luck_adj`, computed against the translated (`oppo_*` -> `def_*`) player stat dicts.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_def_player_luck_adj

diags = calc_def_player_luck_adj(sample_player, base_player, 100.0)
print(diags["deltaDefAdjEff"])
```

### `calc_def_team_luck_adj(sample: 'LineupStatSet', base: 'LineupStatSet', avg_eff: 'float', sample_def_3pa_override: 'float | None' = None) -> 'DefLuckAdjustmentDiags'` {#calc_def_team_luck_adj}

Defensive 3P-luck adjustment for a team (or lineup).

Faithful port of `LuckUtils.calcDefTeamLuckAdj` (`LuckUtils.ts:429-531`).
See the module docstring for the SoS-vs-luck-split formula (`LUCK_PCT`)
and the shared unguarded-division landmine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample` | `LineupStatSet` |  | The team/lineup/player stat dict for the period being luck-adjusted (e.g. an on/off split or a single lineup). |
| `base` | `LineupStatSet` |  | The team/lineup/player stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `sample_def_3pa_override` | `float \| None` | `None` | When given, used as `sampleDef3PA` instead of `sample["total_def_3p_attempts"]` -- see `calc_off_team_luck_adj`'s `sample_3pa_override` docstring for the shared "lineup regression" rationale (`LuckUtils.ts:433-434`, verbatim comment). |

**Returns**

A `DefLuckAdjustmentDiags` dict -- TS-verbatim keys (`avgEff`, `luckPct`, `baseDef3P`, `baseDef3PSos`, `baseDef3PA`, `basePoss`, `base3PSosAdj`, `sampleDef3P`, `sampleDef3PSos`, `sampleDef3PA`, `samplePoss`, `sample3PSosAdj`, `sampleDefEfg`, `sampleDefPpp`, `sampleOffSos`, `sampleDef3PRate`, `sampleDefFGA`, `sampleDefOrb`, `avg3PSosAdj`, `adjDef3P`, `delta3P`, `deltaDefEfg`, `deltaDefPppNoOrb`, `deltaMissesPct`, `deltaDefOrbFactor`, `deltaPtsOffMisses`, `deltaDefPpp`, `deltaDefAdjEff`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_def_team_luck_adj

diags = calc_def_team_luck_adj(sample_team_off, base_team, 100.0)
print(diags["deltaDefAdjEff"])
```

### `calc_lineup_outputs(field: 'str', off_offset: 'float', def_offset: 'float', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None' = None, use_old_val_if_possible: 'tuple[bool, bool]' = (False, False)) -> 'list[NDArray[np.float64]]'` {#calc_lineup_outputs}

Build the off/def target vectors the RAPM design matrices are fit against.

Faithful port of `RapmUtils.calcLineupOutputs` (`RapmUtils.ts:598-751`).
For each filtered lineup, computes a possession-weighted residual: the
lineup's own stat value, plus any global luck adjustment, minus the
accumulated "prior offset" contributed by every player on the lineup
(a strong-prior blend for kept players -- see get_strong_weight`
-- or a fixed baseline contribution for removed players).

Upstream keeps this as a plain `Array<Array<number>>` (*not* a mathjs
`Matrix`, unlike `calc_player_weights`'s `offWeights`/
`defWeights` -- `RapmUtils.test.ts`'s own `tidyResults` helper for
this function has a visibly different shape, see the classification map
in `tests/fixtures/hoop_explorer/README.md`). This port still
materializes both output vectors as `numpy.ndarray` for consistency
with `calc_player_weights` at the same dict -> array boundary --
Task 3.4's ridge-regression solve consumes both as arrays regardless of
the upstream distinction.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | The stat suffix to read off each lineup, e.g. `"adj_ppp"` (read as `{prefix}_{field}`, e.g. `"off_adj_ppp"`). |
| `off_offset` | `float` |  | The D1-average offensive value for `field` (the regression's starting/baseline value on the RHS). |
| `def_offset` | `float` |  | The D1-average defensive value for `field`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`, e.g. from `build_player_context`. |
| `adaptive_correl_weights` | `list[float] \| None` | `None` | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`), used as the strong-prior blend fallback when `ctx["prior_info"] ["strong_weight"] < 0` -- see get_strong_weight`. |
| `use_old_val_if_possible` | `tuple[bool, bool]` | `(False, False)` | `(use_old_val_for_off, use_old_val_for_def)` -- whether to prefer each lineup/team stat's luck-adjusted `old_value` over its raw `value` when present. This is the luck-adjustment hook Task 3.1's classification map flags as an **inherited coverage gap**: the vendored oracle fixture has `old_value == value` on every field (via `insertOldValues`), so neither jest nor this port's replay test ever observes this flag change the resulting numbers -- only that passing it doesn't crash. See the module docstring's "Task 3.3 coverage gap" note. |

**Returns**

`[off_outputs, def_outputs]` -- two 1-D `numpy.ndarray` target vectors, index-aligned with `ctx["filtered_lineups"]("off"/"def")` (plus one extra element each when `ctx["unbias_weight"] > 0`, an "unbiasing observation" target -- always unreached in production, same as `calc_player_weights`'s extra row).

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_lineup_outputs

off_outputs, def_outputs = calc_lineup_outputs(
    "adj_ppp", 100.0, 100.0, ctx
)
print(off_outputs.shape)  # (num_off_lineups,)

# Luck-adjusted variant (reads ``old_value`` where present)

off_luck, def_luck = calc_lineup_outputs(
    "adj_ppp", 100.0, 100.0, ctx, use_old_val_if_possible=(True, True)
)
```

### `calc_off_player_luck_adj(sample_player: 'LineupStatSet', base_player: 'LineupStatSet', avg_eff: 'float') -> 'OffLuckAdjustmentDiags'` {#calc_off_player_luck_adj}

Offensive 3P-luck adjustment for a single player.

Faithful port of `LuckUtils.calcOffPlayerLuckAdj` (`LuckUtils.ts:174-187`).
Per Task 2.1's surprise #4, this is a literal 1-player-team delegation
to `calc_off_team_luck_adj` -- ORB effects are ignored for an
individual player (the upstream comment: "the team calc basically
works fine here, apart from ORBs, which we'll ignore").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample_player` | `LineupStatSet` |  | The player's stat dict for the period being luck-adjusted. |
| `base_player` | `LineupStatSet` |  | The player's stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |

**Returns**

Same shape as `calc_off_team_luck_adj` -- identical to calling that function with `sample_players=[sample_player]`, `base_players_map={base_player["key"]: base_player}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_off_player_luck_adj

diags = calc_off_player_luck_adj(sample_player, base_player, 100.0)
print(diags["deltaOffAdjEff"])
```

### `calc_off_team_luck_adj(sample_team: 'LineupStatSet', sample_players: 'list[LineupStatSet]', base_team: 'LineupStatSet', base_players_map: 'dict[str, LineupStatSet]', avg_eff: 'float', sample_3pa_override: 'float | None' = None, manual_overrides: 'list[ManualOverride] | None' = None) -> 'OffLuckAdjustmentDiags'` {#calc_off_team_luck_adj}

Offensive 3P-luck adjustment for a team (or lineup).

Faithful port of `LuckUtils.calcOffTeamLuckAdj` (`LuckUtils.ts:190-399`).
See the module docstring for the Bayesian-shrink formula, the JS-array-
truthiness / object-selection landmines, and the one unguarded-division
landmine this function carries.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample_team` | `LineupStatSet` |  | The team/lineup stat dict for the period being luck-adjusted (e.g. an on/off split or a single lineup). |
| `sample_players` | `list[LineupStatSet]` |  | The roster of per-player stat dicts backing `sample_team` (`samplePlayers == players.map(on/off/baseline)` per the upstream comment). |
| `base_team` | `LineupStatSet` |  | The team stat dict for the baseline/reference period (typically full-season). |
| `base_players_map` | `dict[str, LineupStatSet]` |  | `{player_key: base_period_player_stat_dict}`. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `sample_3pa_override` | `float \| None` | `None` | When given, used as `sample3PA` instead of `sample_team["total_off_3p_attempts"]`. Per the upstream comment (`LuckUtils.ts:196-198`, shared verbatim with `calc_def_team_luck_adj`'s `sample_def_3pa_override`): "when calc'ing luck on lineups, each lineup gets the total sample as its regression so its average is right over the set" -- i.e. this lets every lineup in a sweep share one common 3PA denominator (the team's) for its regression target, rather than each lineup regressing against its own much smaller, noisier 3PA count. Note that `calc_off_player_luck_adj` itself does *not* pass this (its delegation call omits it entirely) -- the jest oracle's own "3P override" cross-check (`LuckUtils.test.ts:100-115`) instead calls `calc_off_team_luck_adj` directly with the player's own 3PA as this override, purely to demonstrate the parameter's effect in isolation. |
| `manual_overrides` | `list[ManualOverride] \| None` | `None` | Per-player 3P%-expectation overrides from the UI. **A non-`None` empty list still activates the team-level override-delta branch** (JS array truthiness) -- see the module docstring's landmine note. `None` (the default) is the "no overrides at all" case. |

**Returns**

An `OffLuckAdjustmentDiags` dict -- TS-verbatim keys (`avgEff`, `samplePoss`, `sample3P`, `sample3PA`, `base3PA`, `player3PInfo` (per-player detail, sorted by descending `shot_info_total_3p`), `sampleBase3P`, `regress3P`, `sampleOff3PRate`, `sampleOffFGA`, `sampleOffOrb`, `sampleOffEfg`, `sampleOffPpp`, `sampleDefSos`, `delta3P`, `deltaOffEfg`, `deltaMissesPct`, `deltaOffPppNoOrb`, `deltaOffOrbFactor`, `deltaPtsOffMisses`, `deltaOffPpp`, `deltaOffAdjEff`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_off_team_luck_adj

diags = calc_off_team_luck_adj(
    sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
)
print(diags["deltaOffAdjEff"])

# With per-player manual 3P% overrides

diags = calc_off_team_luck_adj(
    sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
    manual_overrides=[
        {"rowId": "Cowan, Anthony", "statName": "off_3p", "newVal": 0.5, "use": True},
    ],
)
```

### `calc_player_weights(ctx: 'RapmPlayerContext') -> 'list[NDArray[np.float64]]'` {#calc_player_weights}

Build the off/def player-weight (design) matrices for the RAPM solve.

Faithful port of `RapmUtils.calcPlayerWeights` (`RapmUtils.ts:544-595`).
One row per (filtered) lineup, one column per remaining player; each
filled cell is `sqrt(lineup_possessions / total_side_possessions)` --
the possession-weighted design-matrix entry the ridge regression (Task
3.4) solves against. This is the first function in the module where a
`dict`-shaped `RapmPlayerContext` gets materialized into a
`numpy.ndarray` -- see the module docstring's "dict -> `numpy.ndarray`
boundary" note.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`, e.g. from `build_player_context`. |

**Returns**

`[off_weights, def_weights]` -- two `numpy.ndarray` matrices of shape `(num_{off,def}_lineups [+1 if ctx["unbias_weight"] > 0], ctx["num_players"])`. The optional extra row (only emitted when `ctx["unbias_weight"] > 0` -- always `0.0` in production per `build_player_context`'s hardcoded local, but settable directly on the returned context dict, as the oracle test does) holds each column's `unbias_weight`-scaled sum-of-squares, an "unbiasing observation" row (`RapmUtils.ts:578-593`).

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_player_weights

off_weights, def_weights = calc_player_weights(ctx)
print(off_weights.shape)  # (num_off_lineups, num_players)
```

### `calc_slow_pseudo_inverse(player_weight_matrix: 'NDArray[np.float64]', ridge_lambda: 'float', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'` {#calc_slow_pseudo_inverse}

Per-parameter variance terms for the ridge-regression standard errors.

Faithful port of the private `RapmUtils.calcSlowPseudoInverse`
(`RapmUtils.ts:1544-1557`): the same `(XᵀX + ridge_lambda·I)⁻¹` as
`slow_regression`'s `bottomInv`, but this function returns the
square root of its diagonal instead of the full solver matrix -- the
`paramErrs` term consumed by the standard-error formula (see
`calculate_sd_rapm`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, same shape as `slow_regression`'s. |
| `ridge_lambda` | `float` |  | The Tikhonov regularization strength (must match the `ridge_lambda` used to build the corresponding `slow_regression` solver, for the SEs to be meaningful). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` -- only `ctx["num_players"]` is read. |

**Returns**

A length-`num_players` array, `sqrt(diag((XᵀX + λI)⁻¹))`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_slow_pseudo_inverse

param_errs = calc_slow_pseudo_inverse(x, 1.0, ctx)
```

### `calculate_aggregated_lineup_stats(lineups: 'list[LineupStatSet] | None') -> 'LineupStatSet'` {#calculate_aggregated_lineup_stats}

Combine all lineups into a single team stat set.

Faithful port of `LineupUtils.calculateAggregatedLineupStats`
(`LineupUtils.ts:106`). Seeds an accumulator from
`StatModels.emptyLineup()` (`{"key": "empty", "doc_count": 0}`) plus
an `all_lineups` sub-accumulator of the same shape, then merges every
lineup via `weighted_avg`: lineups without a truthy `rapmRemove`
key merge into the main accumulator, while `rapmRemove` lineups merge
into `all_lineups` instead (their contribution is folded back in
afterward). Calls `complete_weighted_avg` to turn the main
accumulator's weighted sums into weighted averages, then -- because
`StatModels.emptyLineup()` always carries `key`/`doc_count` and so
is never considered "empty" by the upstream `lodash.isEmpty` check --
unconditionally re-merges the (now-averaged) team totals into
`all_lineups` and finishes that sub-accumulator too. Finally rebuilds
`off_net` / `off_raw_net` via `build_efficiency_margins`
(value-key always; old-value-key too when the team is in luck-adjusted
mode, i.e. `off_ppp.old_value` is present) -- but only on the top-level
result, matching upstream's "don't bother for all_lineups" comment.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupStatSet] \| None` |  | The per-lineup `LineupStatSet` docs to fold together (e.g. the ES aggregation buckets under `responses[0].aggregations.lineups.buckets`). `None` or an empty list yields an all-zero/empty team stat set (mirrors the upstream `lineups \|\| []` guard). |

**Returns**

The aggregated team-total `LineupStatSet`, including a nested `all_lineups` key holding the `rapmRemove`-lineups-plus-team-total composite sub-aggregate.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import calculate_aggregated_lineup_stats

buckets = raw_response["responses"][0]["aggregations"]["lineups"]["buckets"]
team_info = calculate_aggregated_lineup_stats(buckets)
print(team_info["off_ppp"]["value"], team_info["off_poss"]["value"])

# RAPM-exclusion flag

buckets[1]["rapmRemove"] = True  # divert into all_lineups instead
team_info = calculate_aggregated_lineup_stats(buckets)
```

### `calculate_possessions(lineup_events: 'Iterable[LineupEvent]') -> 'list[LineupEvent]'` {#calculate_possessions}

Top-level entry point: calculate team/opponent possessions for a

sequence of lineup events (`PossessionUtils.calculate_possessions`,
`PossessionUtils.scala:371-379`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `Iterable[LineupEvent]` |  | The lineups to enrich, in chronological order. |

**Returns**

The lineups, each enriched with possession counts.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_possessions import calculate_possessions

enriched = calculate_possessions(lineups)
enriched[0].team_stats.num_possessions
```

### `calculate_possessions_by_event(raw_events_as_clumps: 'Iterable[ConcurrentClump]') -> 'list[LineupEvent]'` {#calculate_possessions_by_event}

Drive the batch loop + per-clump scoring over an already-flattened

clump stream (`PossessionUtils.calculate_possessions_by_event`,
`PossessionUtils.scala:521-573`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `raw_events_as_clumps` | `Iterable[ConcurrentClump]` |  | The unbatched clump stream, e.g. from flat-mapping `lineup_as_raw_clumps` over several lineups. |

**Returns**

The lineups, each enriched with possession counts, in original order.

### `calculate_predicted_out(player_weight_matrix: 'NDArray[np.float64]', regressed_players: 'list[float]', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'` {#calculate_predicted_out}

Predict per-lineup outputs from fitted per-player RAPM values.

Faithful port of `RapmUtils.calculatePredictedOut` (`RapmUtils.ts:1559-1567`).
`ctx` is accepted for signature parity with the TS source but unused in
the body (ported verbatim -- upstream's own `ctx` param is likewise
dead in this function).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, shape `(num_lineups, num_players)`. |
| `regressed_players` | `list[float]` |  | The fitted per-player values (e.g. the final, strong-prior-blended RAPM from Task 3.5's `pickRidgeRegression`, or a raw `calculate_rapm` output), length `num_players`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (unused). |

**Returns**

The predicted per-lineup value, length `num_lineups` -- feed into `calculate_residual_error` alongside the actual lineup outputs.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_predicted_out

predicted = calculate_predicted_out(x, [0.875, 1.375], ctx)
```

### `calculate_rapm(regression_matrix: 'NDArray[np.float64]', player_outputs: 'list[float]') -> 'NDArray[np.float64]'` {#calculate_rapm}

Apply a regression solver matrix to a target-outputs vector.

Faithful port of `RapmUtils.calculateRapm` (`RapmUtils.ts:772-775`).
Note the TS signature carries no `ctx` parameter (unlike its solve-layer
siblings) -- ported verbatim, param-for-param.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `regression_matrix` | `NDArray[float64]` |  | The `(num_players, num_lineups)` solver from `slow_regression`. |
| `player_outputs` | `list[float]` |  | The per-lineup target vector, length `num_lineups` (e.g. `calc_lineup_outputs`'s `off_outputs`/`def_outputs`). |

**Returns**

The per-player RAPM estimate, length `num_players`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_rapm

rapm = calculate_rapm(solver, [1.0, 2.0, 3.0])
print(rapm.shape)  # (num_players,)
```

### `calculate_residual_error(player_outs: 'list[float]', regressed_outs: 'list[float]', ctx: 'RapmPlayerContext') -> 'float'` {#calculate_residual_error}

Sum of squared residuals between actual and predicted lineup outputs.

Faithful port of `RapmUtils.calculateResidualError` (`RapmUtils.ts:1569-1579`).
`ctx` is accepted for signature parity but unused in the body (dead
upstream too).

**NaN/shape regime (landmine 7):** TS zips the two arrays via lodash
.zip` (pads the shorter side with `undefined`, so a length
mismatch silently contributes `NaN` to the running sum via
`undefined - number`) then reduces with plain `+`. This port instead
subtracts the two as `numpy` arrays: a length mismatch **raises**
`ValueError` (numpy broadcast rules), rather than the TS silent-NaN
behavior -- not reachable via either language's own call sites (both
arguments are always index-aligned to the same lineup count in
production), so this is a divergence in dead territory, not a fixed bug.
A `NaN` *value already present* inside either input (as opposed to a
length mismatch) propagates through the `numpy` subtraction/sum
exactly as it would through the JS arithmetic (both regimes:
numpy-propagate).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_outs` | `list[float]` |  | The actual per-lineup target values (e.g. `calc_lineup_outputs`'s output). |
| `regressed_outs` | `list[float]` |  | The predicted per-lineup values (e.g. `calculate_predicted_out`'s output). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (unused). |

**Returns**

`sum((player_outs[i] - regressed_outs[i]) ** 2)` -- the `errSq` term consumed by `calculate_sd_rapm`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_residual_error

err_sq = calculate_residual_error([1.0, 2.0, 3.0], [0.875, 1.375, 2.25], ctx)
```

### `calculate_sd_rapm(param_errs: 'NDArray[np.float64]', err_sq: 'float', num_lineups: 'int', num_players: 'int') -> 'NDArray[np.float64]'` {#calculate_sd_rapm}

Per-player RAPM standard errors.

Faithful port of the inline `sdRapm` computation in
`RapmUtils.pickRidgeRegression` (`RapmUtils.ts:1373-1390`, not itself
a named TS function -- promoted to a standalone, independently testable
helper here since Task 3.4's brief calls out the formula explicitly).
Cites [arXiv:1509.09169](https://arxiv.org/pdf/1509.09169.pdf).

**Two NaN/error regimes (landmines 8-9):**

8. `dof_inv = 1.0 / (num_lineups - num_players)` -- if
   `num_lineups == num_players` exactly, JS silently produces
   `Infinity` (float division by zero); this port instead **raises**
   `ZeroDivisionError` (Python float division by zero), matching this
   module's already-established landmine-2 convention (unguarded
   division, Python-raises vs JS-Infinity/NaN). Not reachable via the
   oracle fixtures (`num_off_lineups`/`num_def_lineups` always
   comfortably exceed `num_players` there).
9. `sqrt(sqrt(param_errs) * err_sq * dof_inv)` -- a negative
   `param_errs` entry (only possible if `XᵀX + λI` isn't actually
   positive-definite, e.g. `ridge_lambda < 0`) silently
   **numpy-propagates** to `NaN` (matching JS `Math.sqrt(negative)
   -> NaN`, with a `RuntimeWarning` rather than a raise) -- both
   language regimes agree here, unlike landmine 8.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `param_errs` | `NDArray[float64]` |  | Per-player variance terms from `calc_slow_pseudo_inverse`, length `num_players`. |
| `err_sq` | `float` |  | The residual sum of squares from `calculate_residual_error`. |
| `num_lineups` | `int` |  | `ctx["num_off_lineups"]` or `ctx["num_def_lineups"]` (whichever side `param_errs`/`err_sq` were computed for). |
| `num_players` | `int` |  | `ctx["num_players"]`. |

**Returns**

A length-`num_players` array of per-player RAPM standard errors.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_sd_rapm

sd_rapm = calculate_sd_rapm(param_errs, err_sq, num_lineups=3, num_players=2)
```

### `calculate_stats(clump: 'ConcurrentClump', prev: 'ConcurrentClump', dir: 'Direction') -> 'PossCalcFragment'` {#calculate_stats}

Calculate one direction's possession-fragment for one merged clump

(`PossessionUtils.calculate_stats`, `PossessionUtils.scala:170-369`).

See the upstream source's inline worked examples (and-one detection,
technical/flagrant offsetting, the deadball-rebound heuristic) for the
hand-annotated NCAA play-by-play snippets that motivate each step; this
port reproduces every step in the same order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `ConcurrentClump` |  | The merged clump to score. |
| `prev` | `ConcurrentClump` |  | The previously-processed merged clump (feeds the and-one and deadball-rebound heuristics -- see below). |
| `dir` | `Direction` |  | Which side (`Direction.TEAM`/`Direction.OPPONENT`) is "attacking" for this calculation. Named to match the Scala (shadows the `dir` builtin -- consistent with this port's existing precedent of naming params after their Scala originals, e.g. `RawGameEvent.for_team`'s `min`). |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.PossCalcFragment` for this clump/direction.

### `calibration_table(y_true: 'np.ndarray', p_pred: 'np.ndarray', n_bins: 'int' = 10) -> 'pl.DataFrame'` {#calibration_table}

Bin predicted probabilities and compare mean-predicted vs mean-actual.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of realized binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in `[0, 1]`. |
| `n_bins` | `int` | `10` | Number of equal-width probability bins. |

**Returns**

A `polars.DataFrame` with columns `bin_mid`, `mean_pred`, `mean_actual`, `n` (one row per non-empty bin, sorted ascending).

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_prediction_constants import calibration_table
y = np.random.default_rng(0).integers(0, 2, 200)
p = np.random.default_rng(1).random(200)
calibration_table(y, p, n_bins=10)
```

### `categorize_bad_lineups(lineup_events: 'list[LineupEvent]') -> 'dict[int, tuple[int, int]]'` {#categorize_bad_lineups}

Aggregates bad lineup events for display, by clump-leader player count

(`LineupErrorAnalysisUtils.categorize_bad_lineups`, `:617-633`,
display-only -- the Scala doc comment says "can live without tests").

Re-clumps `lineup_events` (each paired with `next_good=None` --
`clump_bad_lineups`'s grouping predicate never inspects
`next_good`, so this re-clumping is faithful to the Scala's own
`lineup_events.map(e => (e, None))`), then groups the resulting clumps
by `len(clump.evs[0].players)` (the FIRST event's player count -- `5`
means a lineup with a bad *player*, not a bad *count*).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `list[LineupEvent]` |  | The bad lineup events to categorize, in chronological order. |

**Returns**

Player count -> `(num_clumps, total_possessions)`, where `total_possessions` sums `team_stats.num_possessions` across every event in every clump in that group.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import categorize_bad_lineups
categorize_bad_lineups([bad_ev])  # {5: (1, bad_ev.team_stats.num_possessions)}
```

### `clump_bad_lineups(lineup_events: 'list[tuple[LineupEvent, Optional[LineupEvent]]]') -> 'list[BadLineupClump]'` {#clump_bad_lineups}

Groups consecutive bad lineup events into `BadLineupClump`\ s

(`LineupErrorAnalysisUtils.clump_bad_lineups`, `:229-263`).

The Scala original is a bespoke `foldLeft` (NOT the generic
`Clumper` utility used elsewhere in the codebase) that prepends onto
two nested lists -- the per-clump `evs` and the top-level clump list
-- and reverses both at the end. This port walks the input once and
appends directly (to the current clump's `evs`, or a new clump to the
result list), which produces the identical chronological order as the
Scala's prepend-then-double-reverse without needing an explicit reverse
step: mirroring a "prepend to the front, reverse at the end" fold as a
plain "append to the back" loop is behavior-preserving precisely because
reversing a prepend-built list restores insertion order.

The current clump extends to cover the next `(lineup, next_good)` pair
iff ALL 5 conditions hold, compared against the clump's LAST-ADDED event
(`last`, not its first event) (`:242-249`):

1. `lineup.team == last.team`
2. `lineup.opponent == last.opponent`
3. `lineup.start_min == last.end_min` (no time gap)
4. `len(lineup.players) == len(last.players)`
5. `len(lineup.players_in) == len(lineup.players_out)` -- this checks
   the INCOMING lineup's own in/out balance, not a comparison against
   `last` (an unbalanced sub is a bad sign in isolation, per the
   Scala's own comment at `:247`).

`TeamSeasonId` (`lineup.team` / `.opponent`) is a plain (non-frozen)
dataclass, so `==` is a field-wise value comparison out of the box --
no `PlayerCodeId`-unhashability workaround is needed here, since this
predicate only compares team identities and player-list lengths, never a
set of `PlayerCodeId`.

Each time a clump is extended, `next_good` is REPLACED with the
incoming pair's own second element (`:251`) -- the final clump's
`next_good` is always the LAST-extended event's `next`, discarding
whatever `next_good` an earlier extension set.

Starting a new clump uses the incoming pair's own `next` too (`:234`,
`:253`) -- a fresh clump's `next_good` is never inherited from the
clump before it.

The Scala's third `foldLeft` case (`:255-259`, matching a head clump
whose `evs` is empty) is dead code in practice -- every
`BadLineupClump` this function ever constructs starts with exactly one
event and is only ever appended to, so `evs` can never be empty. Omitted
here with this comment in place of an unreachable branch.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `list[tuple[LineupEvent, Optional[LineupEvent]]]` |  | `(lineup_event, next_good_or_None)` pairs, in chronological order. |

**Returns**

The clumps, in chronological order, each with `evs` in chronological order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import clump_bad_lineups
clumps = clump_bad_lineups([(bad_ev, good_ev)])
clumps[0].evs  # [bad_ev]
```

### `combos(first: 'str', last: 'str') -> 'list[str]'` {#combos}

Generate the three name-string variants NCAA sources use for one

player (`DataQualityIssues.combos`, `DataQualityIssues.scala:330-337`).

The Scala signature takes a single `(String, String)` tuple, but every
call site (including the `fix_combos`/`alias_combos` helpers below
and the upstream `DataQualityIssuesTests` oracle) invokes it with two
positional arguments via Scala's tuple auto-conversion -- ported here as
a plain two-argument function since Python has no such conversion.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first name. |
| `last` | `str` |  | The player's last name. |

**Returns**

`[f"{last}, {first}", f"{first} {last}", f"{last.upper()},{first.upper()}"]` -- new-box, new-PbP, and old-box/legacy-PbP formats respectively.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import combos

combos("Makhi", "Mitchell")
# ['Mitchell, Makhi', 'Makhi Mitchell', 'MITCHELL,MAKHI']
```

### `complete_weighted_avg(mutable_acc: 'LineupStatSet', harmonic_weighting: 'bool' = False, regress_diffs: 'float' = 0.0) -> 'None'` {#complete_weighted_avg}

Finish a `weighted_avg` accumulator into true weighted averages.

Faithful port of `LineupUtils.completeWeightedAvg` (`LineupUtils.ts:752`).
Mutates `mutable_acc` in place and returns `None`, mirroring the
upstream `void` + mutable-arg contract. Recomputes the per-field weight
tables from `mutable_acc` itself (`getSimpleWeights(mutableAcc, 1,
regressDiffs)` -- note the `default_val=1`, unlike `weighted_avg`'s
`default_val=0`), then, unless `harmonic_weighting` is set, calls
recalculate_play_type_poss` to fix up the transition/scramble
possession fields that `weighted_avg` skipped. Finally divides every
non-ignored field's accumulated weighted sum by its matching weight
total (shot-type / `ppp_totals` / `orb_totals` / `fta_totals` /
`ast_totals` / generic FGA fallback); `total_*` and `SUM_FIELDS`
fields are left untouched (they are already true totals, not sums to be
averaged). `off_ftr` / `def_ftr` get a special non-`harmonic_weighting`
recompute straight from the accumulated `total_{off|def}_fta` rather
than dividing their own weighted sum.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_acc` | `LineupStatSet` |  | The `weighted_avg`-accumulated `LineupStatSet` to finish in place. Every field with a non-`total_`/`SUM_FIELDS` key is converted from a weighted sum to a weighted average. |
| `harmonic_weighting` | `bool` | `False` | When `True`, skips the recalculate_play_type_poss` fixup and uses a harmonic-style division for `off_ftr`/`def_ftr` instead of the totals-based recompute. Matches the upstream default (`False`) used by `calculate_aggregated_lineup_stats`. |
| `regress_diffs` | `float` | `0.0` | Forwarded to get_simple_weights` -- regression toward ~1000 possessions for on/off diff calculations. Defaults to `0.0` (no regression), matching `calculate_aggregated_lineup_stats`'s call site. |

**Returns**

None. `mutable_acc` is mutated in place.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg, complete_weighted_avg

acc: dict = {}
for lineup in lineups:
    weighted_avg(acc, lineup)
complete_weighted_avg(acc)
print(acc["off_ppp"]["value"])  # now a true weighted average
```

### `compute_league_averages_from_per_game(teams: 'Sequence[TeamDetail]', fields: 'Sequence[str]' = ('efg', '3p', '2pmid', '2prim')) -> 'LeagueAverages'` {#compute_league_averages_from_per_game}

Possession-weighted league means per field (`computeLeagueAveragesFromPerGame`, `ts:189-221`).

For each field, the weighted mean of every team's per-game raw rate over
all their games; only games with a non-`None` raw and a positive weight
contribute. An empty accumulator yields `0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | All teams. |
| `fields` | `Sequence[str]` | `('efg', '3p', '2pmid', '2prim')` | The stat fields to average (default `STRENGTH_ADJUSTED_FIELDS`). |

**Returns**

{"league_off": float, "league_def": float}}``.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_league_averages_from_per_game

teams = [{"team_name": "A", "opponents": [{"off_3p_made": 5, "off_3p_attempts": 10}]}]
print(compute_league_averages_from_per_game(teams, ["3p"])["3p"]["league_off"])  # 0.5
```

### `compute_opponent_strengths(team: 'TeamDetail', team_by_name: 'dict[str, TeamDetail]', fields: 'Sequence[str]', adj_values: 'AdjValues') -> 'dict[str, SideValues]'` {#compute_opponent_strengths}

Schedule-weighted opponent strength per field (`computeOpponentStrengths`, `ts:253-299`).

**Cross-named on purpose:** `avg_opp_def` is weighted by the *offensive*
game weights and reads each opponent's `def` adjustment; `avg_opp_off`
is weighted by *defensive* weights and reads the opponent's `off`. Each
opponent value is its current adjusted value, falling back to its raw
per-game value when no adjustment exists yet. Games whose opponent is not
in `team_by_name` (or whose off+def weights are both `<= 0`) are
skipped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | The team whose schedule is being summarized. |
| `team_by_name` | `dict[str, TeamDetail]` |  | `{team_name: team_detail}` for opponent lookup. |
| `fields` | `Sequence[str]` |  | The stat fields to compute. |
| `adj_values` | `AdjValues` |  | Current `{team_name: field: {"off","def"}}` adjustments. |

**Returns**

{"avg_opp_def": float, "avg_opp_off": float}}``.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_opponent_strengths

team = {"team_name": "A", "opponents": [{"oppo_name": "B", "off_3p_attempts": 10}]}
by_name = {"A": team, "B": {"team_name": "B"}}
adj = {"B": {"3p": {"off": 0.5, "def": 0.3}}}
print(compute_opponent_strengths(team, by_name, ["3p"], adj)["3p"]["avg_opp_def"])  # 0.3
```

### `compute_possession_splits(team: 'TeamDetail') -> 'PossessionSplits'` {#compute_possession_splits}

Home/away/neutral possession totals for a team (`computePossessionSplits`, `ts:154-186`).

Each opponent game's `off_poss`/`def_poss` (missing -> 0) is bucketed
by `location_type` (missing or any non `"Home"`/`"Away"` value ->
the neutral bucket).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | A `team_details` team dict. |

**Returns**

A `PossessionSplits`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_possession_splits

team = {"opponents": [{"off_poss": 70, "def_poss": 68, "location_type": "Home"}]}
print(compute_possession_splits(team).home_off_poss)  # 70.0
```

### `concurrent_event_handler(clumps: 'Iterable[ConcurrentClump]') -> 'list[ConcurrentClump]'` {#concurrent_event_handler}

Batch a stream of singleton/boundary clumps into merged

concurrent-event clumps (`Concurrency.concurrent_event_handler` +
`StateUtils.foldLeft`'s clumping machinery, `PossessionUtils.scala
:71-111` -- see the module docstring for the full batching-predicate
breakdown and the post-game-break singleton port trap).

# ponytail: manual accumulate-and-flush loop replacing the generic
# Clumper/StateUtils.foldLeft abstraction -- this is the ONE clumper
# instantiation in the port, so a reusable abstraction buys nothing.
# Lift this back into a small clumper type if a second concurrent-event
# family needs the same batching later.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clumps` | `Iterable[ConcurrentClump]` |  | An ordered stream of `ConcurrentClump`\ s, each either a singleton raw event (`evs=[ev]`) or a lineup-boundary marker (`evs=[]`, `lineups=[lineup]`), e.g. from `lineup_as_raw_clumps`. |

**Returns**

The merged clumps, each an in-order concatenation of one batch's `evs`/`lineups`.

### `convert_from_digits(name: 'str', player_numbers: 'list[PlayerCodeId]') -> 'Optional[str]'` {#convert_from_digits}

Resolve a jersey-number-only name to its box-score player

(`LineupErrorAnalysisUtils.convert_from_digits`, `:166-175`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate name; only matches if every character is a digit (an empty string is vacuously all-digit, matching Scala's `forall` on an empty `String`). |
| `player_numbers` | `list[PlayerCodeId]` |  | Candidate `(code, id)` pairs -- typically `box_lineup.players_out`, since a number-only PbP mention almost always refers to a player who just left the game. |

**Returns**

The matching player's full name, or `None` if `name` isn't all-digit or no code matches.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId
from sportsdataverse.mbb.mbb_ncaa_names import convert_from_digits
codes = [PlayerCodeId(code="1000", id=PlayerId("name1"))]
convert_from_digits("1000", codes)  # "name1"
```

### `convert_from_initials(name: 'str', codes_to_names: 'dict[str, str]') -> 'Optional[str]'` {#convert_from_initials}

Resolve a 2-initial name (`"A B"` / `"B, A"`) to the single

box-score player whose code starts with those initials
(`LineupErrorAnalysisUtils.convert_from_initials`, `:147-164`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate initials string. |
| `codes_to_names` | `dict[str, str]` |  | Player code -> full name (e.g. `TidyPlayerContext.all_players_map`). |

**Returns**

The single matching full name, or `None` if `name` isn't an initials shorthand, or if zero or multiple codes match.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import convert_from_initials
convert_from_initials("A B", {"AoBo": "name1"})  # "name1"
```

### `count_matching(evs: 'Iterable[RawGameEvent]', side: 'DirFn', *parsers: 'Parser') -> 'int'` {#count_matching}

Count events on one side matching any of the given parsers.

Ports the pervasive `clump.evs.collect { case side(ParseX(_)) => () }
.size` idiom (and its multi-arm `case side(ParseX(_)) => ();
case side(ParseY(_)) => ()` union form, when more than one parser is
passed -- e.g. the and-one free-throw count, which matches *either* a
made or a missed free throw on the same event).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `Iterable[RawGameEvent]` |  | The events to scan. |
| `side` | `DirFn` |  | `~sportsdataverse.mbb.mbb_ncaa_models.PossessionEvent .attacking_team` or `.defending_team`, selecting which raw string (if any) to test per event. |

**Returns**

The count of matching events.

### `create_lineup_data(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent', format_version: 'int') -> 'Union[tuple[list[LineupEvent], list[LineupEvent]], list[ParseError]]'` {#create_lineup_data}

Combines the different methods to build a set of lineup events

(`PlayByPlayParser.create_lineup_data`, `:153-217`) -- the
orchestrator that chains the ENTIRE Phase 5a-5d surface:

1. `parse_game_events` -- HTML -> reversed
   `~sportsdataverse.mbb.mbb_ncaa_stints.PlayByPlayEvent`\ s.
2. `~sportsdataverse.mbb.mbb_ncaa_stints.build_partial_lineup_list`
   -- events -> chronological lineup stints.
3. `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.fix_possible_score_swap_bug`
   -- undoes a rare NCAA score-transposition bug.
4. `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.enrich_lineup`
   (mapped over every stint) -- populates `pts`/`plus_minus`/stat
   trees.
5. `~sportsdataverse.mbb.mbb_ncaa_possessions.calculate_possessions`
   -- per-stint possession counts.
6. Zip each stint with its successor (`None` for the last), then
   `~sportsdataverse.mbb.mbb_ncaa_stint_validation.validate_lineup`
   partitions the `(stint, next)` pairs into good (empty error list)
   and bad.
7. `~sportsdataverse.mbb.mbb_ncaa_stint_validation.clump_bad_lineups`
   groups consecutive bad stints, then
   `~sportsdataverse.mbb.mbb_ncaa_stint_validation.analyze_and_fix_clumps`
   tries to self-heal each clump.
8. Concatenate: good stints + every clump's fixed stints -> `good`;
   every clump's still-unfixed stints -> `bad`, each stamped with
   `player_count_error=len(players)` as the VERY LAST step.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw play-by-play-page HTML. |
| `box_lineup` | `LineupEvent` |  | The team's validated box-score lineup (`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.get_box_lineup`'s result) -- supplies the full roster (for validation), the team/year (for parsing), and the trusted final score (for the swap-bug fix). |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout. |

**Returns**

`(good_lineups, bad_lineups)` on success, or a `list[ParseError]` if `parse_game_events` failed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    box_html = f.read()
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
    pbp_html = f.read()
result = create_lineup_data("test.html", pbp_html, box_lineup, format_version=0)

# Pipeline next step (one line)

    good, bad = result
    sum(ev.duration_mins for ev in good + bad)
```

### `create_player_events(lineup_event_maybe_bad: 'LineupEvent', box_lineup: 'LineupEvent') -> 'list[PlayerEvent]'` {#create_player_events}

Split a lineup event into one :class:`~sportsdataverse.mbb

.mbb_ncaa_models.PlayerEvent` per player on the floor
(`create_player_events`, `LineupUtils.scala:1454-1529`).

First re-tidies `lineup_event_maybe_bad`'s `players`/`players_in`/
`players_out` against `box_lineup` (via player_tidier`),
dropping any player who doesn't actually resolve to a box-score player --
this recovers from "impossible" lineups. Then, for each surviving player
(in lineup-slot order, 0-4), builds their own `enrich_stats` call
with a per-player `player_filter_coder` + that player's slot index (the
only caller in this module that ever passes a non-default
`player_index`, wiring increment_player_3p_shot_info`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_event_maybe_bad` | `LineupEvent` |  | The lineup event to split (its player lists may reference names not actually in `box_lineup`). |
| `box_lineup` | `LineupEvent` |  | The trusted box-score lineup for this game (name resolution + team-scoping context). |

**Returns**

One `~sportsdataverse.mbb.mbb_ncaa_models.PlayerEvent` per (tidied) player in `lineup_event_maybe_bad.players`, same order. Kept even if a player has zero matching raw events -- needed downstream for usage/possession math.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import create_player_events

player_events = create_player_events(lineup, box_lineup)
player_events[0].player_stats.fg_3p.made.total
```

### `create_shot_event_data(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent') -> 'Union[list[ShotEvent], list[ParseError]]'` {#create_shot_event_data}

Parses a game page's SVG shot map into a list of :class:`~sportsdataverse

.mbb.mbb_ncaa_models.ShotEvent` (`ShotEventParser.create_shot_event_data`,
`:175-259`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw game-page HTML (containing the SVG shot map, either baked in as `circle.shot` elements or built client-side via an `addShot(...)` JS call -- see `shot_js_to_html`). |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (supplies `team`/ `year`/`location_type` and the tidy-name lookup context). |

**Returns**

Every shot found, sorted chronologically and court-geometry enriched, or a `list[ParseError]` if the HTML couldn't be parsed, the team names couldn't be matched, no shot events were found (even after the JS fallback), or any one circle failed to parse (the first such failure's error(s) only -- Scala's `.sequence` over `List[Either[...]]` is fail-fast, not accumulating).

**Example**

```python
from pathlib import Path
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_shot_parser import create_shot_event_data

box_html = Path("tests/fixtures/ncaa/test_lineup.html").read_text(encoding="utf-8")
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=1)
shots = create_shot_event_data("test_p1.html", box_html, box_lineup)
```

### `duration_from_period(period: 'int', is_women_game: 'bool') -> 'float'` {#duration_from_period}

The game duration (minutes elapsed) once `period` has completed

(`ExtractorUtils.scala:286-287`: `start_time_from_period(period + 1,
...)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `period` | `int` |  | The 1-indexed period number. |
| `is_women_game` | `bool` |  | Whether to use the women's or men's period schedule. |

**Returns**

The game-clock minute at the end of `period`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import duration_from_period
duration_from_period(2, is_women_game=False)  # 40.0 (end of men's regulation)
duration_from_period(4, is_women_game=True)  # 40.0 (end of women's regulation)
```

### `enrich_and_reverse_game_events(in_events: 'list[PlayByPlayEvent]') -> 'list[PlayByPlayEvent]'` {#enrich_and_reverse_game_events}

Inserts game-break events and turns descending per-row times into

ascending game-clock minutes, returning the whole list latest-to-earliest
(`PlayByPlayParser.enrich_and_reverse_game_events`, `:297-370`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_events` | `list[PlayByPlayEvent]` |  | The raw parsed events, earliest to latest, with each `.min` still a per-period DESCENDING clock reading. |

**Returns**

`in_events` with `~sportsdataverse.mbb.mbb_ncaa_stints .GameBreakEvent`\ s inserted at every period boundary, every `.min` converted to an ASCENDING whole-game reading, and a trailing (once reversed, LEADING) `~sportsdataverse.mbb .mbb_ncaa_stints.GameEndEvent` -- the whole list in LATEST-TO-EARLIEST order (the caller is expected to `reversed(...)` it back when chronological order is wanted, exactly like `get_sorted_pbp_events` does).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import Score
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import enrich_and_reverse_game_events
from sportsdataverse.mbb.mbb_ncaa_stints import OtherTeamEvent

events = [OtherTeamEvent(18.0, Score(1, 1), "tipoff")]
reversed_enriched = enrich_and_reverse_game_events(events)
reversed_enriched[0].__class__.__name__  # 'GameEndEvent'
```

### `enrich_lineup(lineup: 'LineupEvent') -> 'LineupEvent'` {#enrich_lineup}

Populate `pts`/`plus_minus` from the score delta, then run the

full stat-tree enrichment (`enrich_lineup`, `LineupUtils.scala:29-46`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup event to enrich (not mutated -- see the module docstring's "Scala idiom decisions"). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` with `team_stats`/`opponent_stats` fully populated.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import enrich_lineup

enriched = enrich_lineup(lineup)
enriched.team_stats.pts
```

### `enrich_shot_events_with_pbp(sorted_shot_events: 'list[ShotEvent]', sorted_pbp_events: 'list[PlayByPlayEvent]', lineup_events: 'list[LineupEvent]', bad_lineup_events: 'list[LineupEvent]', box_lineup: 'LineupEvent') -> 'list[ShotEvent]'` {#enrich_shot_events_with_pbp}

Enrich each shot with its play-by-play event + on-floor lineup

(`PlayByPlayUtils.enrich_shot_events_with_pbp`,
`PlayByPlayUtils.scala:28-278`).

Folds over the (time-sorted) shots, threading two iterators (play-by-play
and lineup) and a small amount of carry-over state. For each shot it:

1. gathers the play-by-play events at the shot's time (`find_pbp_clump`),
   keeping only the ones on the shot's side (team if `is_off`);
2. picks the matching shot event via a strict -> loose -> first-of-N
   cascade (`right_kind_of_shot` then `matching_player`);
3. locates the on-floor lineup (`find_lineup`), falling back to
   `bad_lineup_events` if the good lineups yield nothing (a bad-lineup
   match is used for `players` but its id is suppressed);
4. attributes an assist (a same-time non-self assist event) and transition
   flag (`"fastbreak"` in the event string), and fills in `lineup_id` /
   `players` / `pts` / `value` / `ast_by` / `is_ast` / `is_trans`
   -- exactly the fields Task 5e.5's parser left as placeholders.

Shots with no matching play-by-play clump, no matching shot event, or no
matching lineup are dropped (the Scala logs a `WARN` and discards; the
logging is dropped per the module note, the discard preserved).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_shot_events` | `list[ShotEvent]` |  | Shots in ascending game-clock order. |
| `sorted_pbp_events` | `list[PlayByPlayEvent]` |  | The full play-by-play event stream, ascending. |
| `lineup_events` | `list[LineupEvent]` |  | The good (validation-passing) stint events. |
| `bad_lineup_events` | `list[LineupEvent]` |  | The validation-flagged stint events, used only as a last resort (their ids are never attributed). |
| `box_lineup` | `LineupEvent` |  | The roster lineup event (drives name resolution). |

**Returns**

The enriched, still-time-sorted list of shots (a subset of the input -- unmatchable shots are dropped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import enrich_shot_events_with_pbp
enriched = enrich_shot_events_with_pbp(
    shots, pbp, good_lineups, bad_lineups, box_lineup
)
```

### `enrich_stats(lineup: 'LineupEvent', event_parser: 'PossessionEvent', stats: 'LineupEventStats', player_filter_coder: 'Optional[PlayerFilterCoder]' = None, player_index: 'int' = -1) -> 'LineupEventStats'` {#enrich_stats}

Fold a lineup's raw events into a counting-stat tree (``protected def

enrich_stats`, `LineupUtils.scala:115-162``). Reuses the Task 5a.3
concurrent-clump batching (`~sportsdataverse.mbb.mbb_ncaa_possessions
.lineup_as_raw_clumps` + `~sportsdataverse.mbb.mbb_ncaa_possessions
.concurrent_event_handler`) rather than duplicating it -- both were
already public/exported from Task 5a.3.

`stats` is deep-copied once up front (see the module docstring's
"Scala idiom decisions"), so this function never mutates the caller's
`stats` argument -- safe to call repeatedly against the same starting
literal (e.g. a shared "empty stats" fixture).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup whose `raw_game_events` to fold over. |
| `event_parser` | `PossessionEvent` |  | Selects which side (team/opponent) is "attacking". |
| `stats` | `LineupEventStats` |  | The starting stat tree (not mutated -- see above). |
| `player_filter_coder` | `Optional[PlayerFilterCoder]` | `None` | Optional `name -> (is_this_player, code)` predicate/coder, for per-player scoping (Task 5c.4). |
| `player_index` | `int` | `-1` | Lineup-slot index for `~sportsdataverse.mbb .mbb_ncaa_models.PlayerShotInfo` tuples (Task 5c.4; `-1` for team-level calls, the only value exercised before then). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats` with every matching event folded in.

### `ensure_ev_uniqueness(clump: 'ConcurrentClump') -> 'ConcurrentClump'` {#ensure_ev_uniqueness}

Nudge each event's `min` by a tiny per-index delta so truly

concurrent (identical-`min`) events within a clump don't collapse
under `==` (`ensure_ev_uniqueness`, `LineupUtils.scala:105-111`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `ConcurrentClump` |  | The clump whose events to nudge. |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_possessions.ConcurrentClump` with each event's `min` incremented by `1e-6 * index`.

### `espn_wbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_wbb_teams}

espn_wbb_teams - look up the women's college basketball teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.wbb.espn_wbb_teams.clear_cache().

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
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_teams
teams = espn_wbb_teams()
print(teams.shape)
print(teams.columns[:8])

# Walk every team-id (handy for batched scrapes)

team_ids = teams["team_id"].to_list()
print(len(team_ids), "D1 teams")

# Pandas round-trip + Division II/III

d2_d3 = espn_wbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

### `extract_player_from_ev(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', tidy_ctx: 'TidyPlayerContext') -> 'Optional[PlayerCodeId]'` {#extract_player_from_ev}

Resolve the player named in `pbp_event` to a

`~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId`
(`ShotEnrichmentUtils.extract_player_from_ev`,
`PlayByPlayUtils.scala:613-635`).

For a shot by the team under analysis (`shot.is_off`) the name is
tidied against the box score before coding (so a mis-spelled play-by-play
name resolves to the roster identity); for an opponent shot it is coded
verbatim with no team context.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched (only `is_off` is read). |
| `pbp_event` | `MiscGameEvent` |  | The play-by-play event naming the player. |
| `tidy_ctx` | `TidyPlayerContext` |  | The name-resolution context for this game. |

**Returns**

The resolved `PlayerCodeId`, or `None` if the event string names no player (`~sportsdataverse.mbb.mbb_ncaa_events.parse_any_play` found nothing).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import extract_player_from_ev
pc = extract_player_from_ev(shot, pbp_event, tidy_ctx)
```

### `field_keys(field: 'str') -> 'dict[str, str]'` {#field_keys}

Off/def stat-key names for a field (`fieldKeys`, `ts:77-79`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | A stat field (`"efg"` / `"3p"` / `"2pmid"` / `"2prim"`). |

**Returns**

f"off_{field}", "def": f"def_{field}"}``.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import field_keys

keys = field_keys("3p")
print(keys["off"], keys["def"])  # off_3p def_3p
```

### `filter_matching_own(tags: 'list[Tag]', regex: 'str') -> 'list[Tag]'` {#filter_matching_own}

JSoup `:matchesOwn(regex)` applied to an already-computed candidate

list, rather than a fresh `root.select(selector)` call (Task 5e.2
addition; see the module docstring's note on composing this with
`attr_regex_filter`).

Same own-text-only semantics as `select_matching_own` -- JSoup's
`Element.ownText()` walks only the element's direct `TextNode`
children, not text nested inside child elements.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tags` | `list[Tag]` |  | Candidate tags to filter (typically the result of an earlier `.select()`/`attr_regex_filter` call). |
| `regex` | `str` |  | The pattern each candidate's own (whitespace-collapsed) text must `re.search`-match. |

**Returns**

The subset of `tags` whose own text contains a `regex` match, in the input list's order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, filter_matching_own, parse_html
soup = parse_html('<td style="font-size:36px">92</td><td style="color:red">x</td>')
candidates = attr_regex_filter(soup.find_all("td"), "style", r"font-size:36px")
filter_matching_own(candidates, r"[0-9]+")  # [<td style="font-size:36px">92</td>]
```

### `find_lineup(shot: 'ShotEvent', curr_pbp: 'Optional[MiscGameEvent]', curr_lineups: 'list[LineupEvent]', lineup_it: "'PeekableIterator[LineupEvent]'") -> 'tuple[Optional[LineupEvent], list[LineupEvent]]'` {#find_lineup}

Find the lineup (stint) event on the floor for `shot`

(`ShotEnrichmentUtils.find_lineup`, `PlayByPlayUtils.scala:352-517`).

A recursive state machine over three lists: `curr_lineup` (the current
candidate), `fallback_lineups` (time-matching lineups whose raw events
did not contain `curr_pbp` -- kept as fallbacks), and `stashed_lineups`
(lineups pulled from the iterator but not yet stepped into, available for
future shots). The branch cases (labelled 2.1-2.4 in the Scala):

* **2.1** -- no time-matching lineup left: return the fallbacks.
* **2.2** -- the next lineup starts *after* the shot: no match, stash it.
* **2.3** -- strictly inside a lineup with no prior fallbacks: take it.
* **2.4** -- shot is exactly at a lineup boundary (or we are already
  choosing among multiple fallbacks): take this lineup iff its raw game
  events contain `curr_pbp`'s event string (`curr_pbp is None` takes
  it unconditionally); otherwise keep it as a fallback and recurse.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot to place (only `min` / `is_off` are read). |
| `curr_pbp` | `Optional[MiscGameEvent]` |  | The already-matched play-by-play event for this shot, used to disambiguate boundary lineups; `None` disables that check. |
| `curr_lineups` | `list[LineupEvent]` |  | Lineups pulled from the iterator on a previous call and still available (the current one first). |
| `lineup_it` | `PeekableIterator[LineupEvent]` |  | The shared lineup iterator (consumed in place). |

**Returns**

`(matched_lineup_or_None, lineups_to_retry_next_time)` -- the second element always includes the matched lineup (so out-of-order shots sharing it still resolve) plus any leftover stash.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
    PeekableIterator,
    find_lineup,
)
matched, retry = find_lineup(shot, None, [lineup], PeekableIterator([]))
```

### `find_missing_subs(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'` {#find_missing_subs}

Trims a clump whose lineups carry TOO MANY players by identifying the

"ghost" player(s) a missing sub-out left behind
(`LineupErrorAnalysisUtils.find_missing_subs`, `:406-514`).

Fires only when the clump's first event has `>= 6` on-floor players
(`:415-416`: `candidates.size < 6` is a no-op). `expected_size_diff`
(`:419`) is `first_event_player_count - 5` -- the number of ghosts the
trim should end up removing.

**Phase 1 -- shrink the candidate pool** (`:437-478`). Starting from the
first event's players, walk the clump chronologically. At each event a
candidate is *confirmed present* (and dropped from the pool) if it subs
out (`ev.players_out`, **skipped for the first event** -- `:445`,
literal port of `clump.evs.headOption.contains(ev)` as value equality
`ev == clump.evs[0]`; for a well-formed clump of distinct events this is
exactly `index == 0`) or is named in one of the event's team-side raw
plays (`parse_any_play` -> `~sportsdataverse.mbb.mbb_ncaa_names
.tidy_player` -> `~sportsdataverse.mbb.mbb_ncaa_stints
.build_player_code`, `:448-456`; unlike `validate_lineup` this
does NOT skip the literal `"team"` token -- ported verbatim).
`matching_index` is the **FIRST** event index at which the pool size
first equals `expected_size_diff` (`:475`); once set it freezes -- all
later events are skipped in phase 1 (`:439-441`).

**Accept gate** (`:479-480`): the final pool must be non-empty and no
larger than `expected_size_diff`. If `matching_index` never fired
(the pool jumped past `expected_size_diff` in a single step, or never
shrank to it), the gate still accepts iff the residual pool is a non-empty
subset of size `<= expected_size_diff` -- in which case phase 3 routes
**every** event through the "before match" branch (`index > None` is
always false). On failure the **original** clump is returned unchanged.

**Phase 3 -- rebuild the events** (`:482-503`, a `scanLeft` ported as
a manual accumulate loop that drops the seed). For events at/before
`matching_index` the ghost pool is simply removed from `players`
(`filterNot`). For events strictly **after** `matching_index`
(`index > matching_index` -- the matched event itself is "before")
`players` is rebuilt from the previous *tidied* event via
`~sportsdataverse.mbb.mbb_ncaa_stints.build_new_player_list` (the
`scanLeft` threads the previously-emitted event; its seed is `None`,
but the first event can never be an "after match" event, so the
`getOrElse(ev)` fallback is only ever a formality -- ported faithfully
all the same). The rebuilt events are partitioned by
`validate_lineup`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- the now-valid rebuilt events and a clump of the still-invalid ones (carrying the input's `next_good`); or `([], clump)` on a no-op / rejected fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    find_missing_subs,
)
fixed, still = find_missing_subs(clump, box_lineup, valid_codes)
```

### `find_pbp_clump(shot_time: 'float', pbp_it: "'PeekableIterator[PlayByPlayEvent]'", curr_pbp_clump: 'list[MiscGameEvent]', maybe_next_pbp_event: 'Optional[MiscGameEvent]') -> 'tuple[list[MiscGameEvent], Optional[MiscGameEvent]]'` {#find_pbp_clump}

Gather every play-by-play shot/assist event sharing `shot_time`

(`ShotEnrichmentUtils.find_pbp_clump`, `PlayByPlayUtils.scala:556-608`).

If `curr_pbp_clump` (carried over from the previous shot) already holds
events at `shot_time` they are returned as-is; otherwise the iterator is
walked forward, discarding earlier events, accumulating the equal-time
ones, and stopping (returning it as `maybe_next_pbp_event`) at the first
later event.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot_time` | `float` |  | The shot's game-clock minute to gather events for. |
| `pbp_it` | `PeekableIterator[PlayByPlayEvent]` |  | The shared play-by-play iterator (consumed in place). |
| `curr_pbp_clump` | `list[MiscGameEvent]` |  | Events left over from the previous shot's clump. |
| `maybe_next_pbp_event` | `Optional[MiscGameEvent]` |  | The look-ahead event stashed by the previous call, if any. |

**Returns**

`(clump, maybe_next)` -- the equal-time events, plus the first strictly-later event (or `None` at end of stream).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
    PeekableIterator,
    find_pbp_clump,
)
clump, nxt = find_pbp_clump(5.0, PeekableIterator([]), [], None)
# ([], None)
```

### `fix_combos(first: 'str', last: 'str', code_start: 'Optional[str]' = None) -> 'list[tuple[str, Optional[str]]]'` {#fix_combos}

Pair each of `combos`' three name variants with a shared

player-code override (`DataQualityIssues.fix_combos`,
`DataQualityIssues.scala:340-346`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first name. |
| `last` | `str` |  | The player's last name. |
| `code_start` | `Optional[str]` | `None` | The forced player-code prefix for every variant, or `None` to leave the default `build_player_code` truncation behavior in place. |

**Returns**

Three `(name_variant, code_start)` pairs.

### `fix_possible_score_swap_bug(lineup: 'list[LineupEvent]', box_lineup: 'LineupEvent') -> 'list[LineupEvent]'` {#fix_possible_score_swap_bug}

Undo a rare NCAA data bug where the scores get transposed

(`fix_possible_score_swap_bug`, `LineupUtils.scala:51-90`).

If the last lineup's ending score is the exact transpose of the box
score's ending score, every lineup's `score_info` is un-transposed and
`pts`/`plus_minus` are swapped/negated between `team_stats` and
`opponent_stats` -- nothing else in the stat trees changes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `list[LineupEvent]` |  | The lineups to (maybe) fix, in chronological order. |
| `box_lineup` | `LineupEvent` |  | The trusted box-score lineup to compare the final score against. |

**Returns**

`lineup` unchanged if the scores aren't transposed (or `lineup` is empty); otherwise a new list with every entry's score/pts/ plus_minus corrected.

### `fuzzy_box_match(candidate: 'str', unassigned_box_names: 'list[str]', team_context: 'str') -> 'Union[str, FuzzyMatchError]'` {#fuzzy_box_match}

Pick the single unassigned box-score name a mis-spelled play-by-play

name most likely refers to (`NameFixer.fuzzy_box_match`, `:774-905`).

Resolution order: a single strong match wins outright; multiple strong
matches only resolve if there's a clear (>10-point) winner; failing
that, a single weak match wins; failing that, a first-name-only match
only wins if there are no other first-name matches (exact or fuzzy)
among the un-matched box names.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `candidate` | `str` |  | The raw play-by-play name. |
| `unassigned_box_names` | `list[str]` |  | Box-score full names not yet claimed by another resolution. |
| `team_context` | `str` |  | Debug-only context string (Scala used it to de-duplicate diagnostic prints; this port has no logging surface to de-duplicate, so the value is otherwise unused). |

**Returns**

The winning box-score name, or a `FuzzyMatchError` describing why no single name won.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import fuzzy_box_match
fuzzy_box_match(
    "sirena tuitele",
    ["Suitele, Sirena", "Tuitele, Peanut", "Guity, Amaya"],
    "team_context",
)
# "Suitele, Sirena"
```

### `get_ascending_time(event: 'ShotEvent', period: 'int', is_women_game: 'bool') -> 'float'` {#get_ascending_time}

Converts the descending in-period clock time to an ascending

game-elapsed time (`ShotEventParser.get_ascending_time`, `:531-537`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `ShotEvent` |  | The shot event (only `~sportsdataverse.mbb .mbb_ncaa_models.ShotEvent.min`, the raw descending clock minute, is read). |
| `period` | `int` |  | The 1-indexed period the shot was taken in. |
| `is_women_game` | `bool` |  | Whether to use women's-quarters (10min) or men's- halves (20min, then 5min OTs) period lengths. |

**Returns**

The ascending game-elapsed time, in minutes.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import get_ascending_time
get_ascending_time(shot_with_min_4, period=1, is_women_game=False)  # 16.0
```

### `get_box_lineup(filename: 'str', in_html: 'str', team_id: 'TeamId', format_version: 'int', external_roster: 'tuple[list[str], list[RosterEntry]]' = ([], []), neutral_game_dates: 'AbstractSet[str]' = frozenset()) -> 'Union[LineupEvent, list[ParseError]]'` {#get_box_lineup}

Gets the boxscore lineup from the HTML page (``BoxscoreParser

.get_box_lineup`, `:122-222``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name -- used both for error reporting and to extract the period via `parse_period_from_filename` (e.g. `"test_p2.html"`). |
| `in_html` | `str` |  | The raw box-score-page HTML. |
| `team_id` | `TeamId` |  | The team this box score is being parsed for. |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout (see the module docstring's selector-translation notes). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` | `([], [])` | `(other_players, roster_players)` -- either just names, or a full roster, to validate/fuzzy-correct box names against (see `inject_validated_players`). Also seeds `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent.players_out` on the interim lineup (`roster_players`, each's `code` replaced by its jersey `number`). |
| `neutral_game_dates` | `AbstractSet[str]` | `frozenset()` | Date strings (the first whitespace-separated token of the raw date-cell text) known to be neutral-site games -- overrides the default home/away inference. |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` whose `players` is the validated box-score lineup (natural HTML order -- see the module docstring's "not sorted" note), or a `list[ParseError]` if any parsing step failed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    html = f.read()
result = get_box_lineup("test_p1.html", html, TeamId("TeamA"), format_version=0)
```

### `get_config() -> 'NcaaFetchConfig'` {#get_config}

Return the live `NcaaFetchConfig` singleton.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import get_config
cfg = get_config()
print(cfg.cache_dir, cfg.timeout)
```

### `get_constants(league: 'str') -> 'LeagueConstants'` {#get_constants}

Return the `LeagueConstants` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | Either `"mens"` or `"womens"`. |

**Returns**

The league's `LeagueConstants`.

**Example**

```python
from sportsdataverse.mbb.mbb_prediction_constants import get_constants
get_constants("mens").hfa
```

### `get_game_weight(opp: 'OpponentGame', field: 'str', side: 'str') -> 'float'` {#get_game_weight}

Weight for one game/field/side (`getGameWeight`, `ts:119-140`).

The field-specific shot volume (FGA for `efg`, 3PA for `3p`,
`2pmid_attempts` / `2prim_attempts` for the mid/rim fields); when that
is `0` (no shots of that type), **falls back to** `off_poss` /
`def_poss` so the game still carries weight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `opp` | `OpponentGame` |  | One opponent game dict. |
| `field` | `str` |  | A stat field. |
| `side` | `str` |  | `"off"` or `"def"`. |

**Returns**

The (non-negative) game weight.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_game_weight

game = {"off_3p_attempts": 0, "off_poss": 70}
print(get_game_weight(game, "3p", "off"))  # 70.0 (poss fallback)
```

### `get_neutral_games(filename: 'str', in_html: 'str', format_version: 'int') -> 'Union[tuple[TeamId, set[str]], list[ParseError]]'` {#get_neutral_games}

Extracts the set of neutral/away-marked game dates from a saved NCAA

team-schedule page (`TeamScheduleParser.get_neutral_games`,
`TeamScheduleParser.scala:63-94`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw team-schedule-page HTML. |
| `format_version` | `int` |  | `0` for the legacy `fieldset`/`legend` layout, `1` for the 2018+ `div.card-header`/`div.card-body` layout. |

**Returns**

`(team, neutral_game_dates)` -- the team parsed from the page's image `alt` attribute, and every `"MM/DD/YYYY"` date string found on an `"@Opponent"`-marked row -- or a single-element `list[ParseError]` if the HTML fails to parse, or the team name can't be located.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_neutral_games

with open("tests/fixtures/ncaa/test_schedule.html", encoding="utf-8") as f:
    html = f.read()
result = get_neutral_games("test_schedule.html", html, format_version=0)
if isinstance(result, list):
    raise RuntimeError(result)  # list[ParseError]
team, neutral_dates = result
```

### `get_per_game_raw(opp: 'OpponentGame', field: 'str', side: 'str') -> 'Optional[float]'` {#get_per_game_raw}

Per-game raw shooting rate from one opponent row (`getPerGameRaw`, `ts:82-116`).

`efg` is `(2pmid_made + 2prim_made + 1.5 * 3p_made) / (2pmid_att +
2prim_att + 3p_att)`; `3p` / `2pmid` / `2prim` are `made /
attempts`. Every counter read is nullish (missing -> 0); the sole guard
is on total attempts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `opp` | `OpponentGame` |  | One opponent game dict. |
| `field` | `str` |  | A stat field; an unknown field returns `None`. |
| `side` | `str` |  | `"off"` or `"def"` (selects the `off_`/`def_` prefix). |

**Returns**

The rate as a float, or `None` when the relevant attempts total is `<= 0` (game skipped by the weighted means -- **not** a 0-rate).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_per_game_raw

game = {"off_3p_made": 4, "off_3p_attempts": 10}
print(get_per_game_raw(game, "3p", "off"))  # 0.4
```

### `get_player_value_constants(league: 'str') -> 'PlayerValueConstants'` {#get_player_value_constants}

Return the `PlayerValueConstants` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"mens"` or `"womens"`. |

**Returns**

The league's `PlayerValueConstants`.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import get_player_value_constants
get_player_value_constants("mens").bundle_prefix
```

### `get_sorted_pbp_events(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent', format_version: 'int') -> 'Union[list[PlayByPlayEvent], list[ParseError]]'` {#get_sorted_pbp_events}

Handy util to return the play-by-play events in chronological order,

used in a few other places (`PlayByPlayParser.get_sorted_pbp_events`,
`:221-239`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw play-by-play-page HTML. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup (supplies `team`/`year`). |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout. |

**Returns**

The play-by-play events in chronological (earliest-to-latest) order, or a `list[ParseError]` on failure. `enrich=True` is used internally to get the correct ascending timestamps, and its reversal is undone here (`.reverse`) to restore chronological order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import get_sorted_pbp_events

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    box_html = f.read()
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
    pbp_html = f.read()
events = get_sorted_pbp_events("test.html", pbp_html, box_lineup, format_version=0)
```

### `get_stats_diff(stat_set1: 'LineupStatSet', stat_set2: 'LineupStatSet', off_title: 'str', def_title: 'str | None' = None) -> 'LineupStatSet'` {#get_stats_diff}

Straight (unweighted) field-by-field diff of two team stat sets.

Faithful port of `LineupUtils.getStatsDiff` (`LineupUtils.ts:185`).
For every field on `stat_set1`, subtracts the matching field's
`value` (and, when both sides carry one, `old_value`) from
`stat_set2`. No possession weighting or regression -- this is a raw
subtraction, unlike `weighted_avg` / `complete_weighted_avg`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_set1` | `LineupStatSet` |  | The "from" team stat set (e.g. this team). |
| `stat_set2` | `LineupStatSet` |  | The "to subtract" team stat set (e.g. the opponent, or a prior period). |
| `off_title` | `str` |  | Written into the result's `off_title` field verbatim. |
| `def_title` | `str \| None` | `None` | Written into the result's `def_title` field verbatim (`None` when omitted, mirroring the upstream optional arg). |

**Returns**

A new `LineupStatSet`: one `{"value": ..., "old_value": ..., "override": ...}` dict per field present on `stat_set1`, plus `off_title` / `def_title`. A field becomes `None` (the JS `undefined` analog) instead of a diff dict when either side is missing a `value` -- e.g. because that field was never populated for one of the two stat sets.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import get_stats_diff

diff = get_stats_diff(team_a, team_b, "Team A", "Team B")
print(diff["off_ppp"]["value"])  # team_a.off_ppp - team_b.off_ppp
```

### `get_team_raw_from_per_game(team: 'TeamDetail', field: 'str') -> 'SideValues'` {#get_team_raw_from_per_game}

A team's field rate as the weighted mean of its per-game raws (`getTeamRawFromPerGame`, `ts:224-250`).

Same accumulation as `compute_league_averages_from_per_game` but
scoped to one team's games; empty -> `0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | A `team_details` team dict. |
| `field` | `str` |  | A stat field. |

**Returns**

float, "def": float}``.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_team_raw_from_per_game

team = {"opponents": [{"off_3p_made": 4, "off_3p_attempts": 10}]}
print(get_team_raw_from_per_game(team, "3p")["off"])  # 0.4
```

### `get_team_triples(filename: 'str', in_html: 'str', old_format: 'bool' = False) -> 'Union[list[tuple[TeamId, str, ConferenceId]], list[ParseError]]'` {#get_team_triples}

Extracts `(team, NCAA id, conference)` triples from a saved NCAA

team-list/attendance page (`TeamIdParser.get_team_triples`,
`TeamIdParser.scala:69-91`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw team-list-page HTML. |
| `old_format` | `bool` | `False` | `True` for pages where the team name and conference are in separate `<td>`s; `False` (default) for pages where the conference is embedded in the team-name cell as `"Team (Conf)"`. |

**Returns**

One `(TeamId, ncaa_id, ConferenceId)` triple per row that has both a resolvable id and name/conference (rows missing either are silently skipped, matching the Scala's `case _ => Nil`), or a single-element `list[ParseError]` if the HTML itself fails to parse.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_team_triples

with open("tests/fixtures/ncaa/test_attendance_list.html", encoding="utf-8") as f:
    html = f.read()
result = get_team_triples("test_attendance_list.html", html, old_format=True)
```

### `get_unified_ncaa_id(filename: 'str', in_html: 'str') -> 'Union[Optional[str], list[ParseError]]'` {#get_unified_ncaa_id}

Gets a player's lowest cross-season NCAA id from a saved player page

(`RosterParser.get_unified_ncaa_id`, `RosterParser.scala:136-152`).

Always uses the v1 selector table -- this bonus lookup only exists on
2018+-era pages.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw player-page HTML. |

**Returns**

The numerically-lowest NCAA id found, `None` if the page has no `tr[id^=player_season_]` rows, or a single-element `list[ParseError]` if the HTML couldn't be parsed at all.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_roster_parser import get_unified_ncaa_id
get_unified_ncaa_id("player.html", player_page_html)
```

### `handle_common_sub_bug(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'` {#handle_common_sub_bug}

Fixes the "2-in-1-out then a compensating 1-out" substitution bug

(`LineupErrorAnalysisUtils.handle_common_sub_bug`, `:269-298`).

Handles a **single-event** bad clump whose next known-good lineup carries
a lone sub-out that the clump's event should have applied but didn't (e.g.
`IN: X, Y, Z; OUT: A, B` in the bad event, then `OUT: C` in the good
one). Fires only when all three guard conditions hold
(`:275-278`):

* the bad event has more players subbing IN than OUT
  (`len(players_in) > len(players_out)`),
* the good event has **no** sub-ins (`len(good.players_in) == 0` --
  otherwise there's no way to tell which of its sub-outs to borrow), and
* the good event has at least one sub-out (`len(good.players_out) > 0`).

The fix (`:279-283`) removes the good event's sub-outs from the bad
event's on-floor `players` (value-equality `not in` -- the Scala's
`filterNot(good.players_out.toSet)`) and appends them to the bad
event's `players_out` (order-preserving distinct` -- the Scala's
`(bad.players_out ++ good.players_out).distinct`). The fix is **accepted
only if** the result then passes `validate_lineup` (`:284-294`):
on success the fixed event is returned as the sole `fixed` lineup and
the still-to-fix clump is emptied; on failure the *fixed* event (not the
original) is returned as the still-to-fix clump, keeping the same
`next_good` so a later pass can try again.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- `fixed_lineups` is `[fixed]` on an accepted fix else `[]`; `still_to_fix` is an empty clump on accept, the (unchanged) input clump on a guard miss, or the single-event *fixed*-but-still-invalid clump on a rejected fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    handle_common_sub_bug,
)
fixed, still = handle_common_sub_bug(clump, box_lineup, valid_codes)
```

### `in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'` {#in_game_features}

Per-play in-game win-probability features from a `load_mbb_pbp` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame with `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id` (event team) and `home_team_id` (the `load_mbb_pbp` schema). |
| `pregame_home_prob` | `float` |  | The pregame home win probability (e.g. from `win_prob_from_margin`), encoded as a constant logit column. Clipped to `[1e-6, 1 - 1e-6]` so a saturated CDF (exact 0/1) cannot crash the logit. |

**Returns**

One row per input play: `score_diff` (home - away), `sec_left` (clipped at 0 -- overtime plays count as 0 seconds left), `sqrt_sec_left`, `pregame_logit`, `home_has_ball` (`Int8`; dead-ball / unknown-team plays are 0).

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import in_game_features
from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
feats = in_game_features(pbp, 0.62)
```

### `incorporate_height(height_in: 'float', confs: 'list[float]') -> 'list[float]'` {#incorporate_height}

Reweight positional confidences by height (Bayesian-ish height prior).

Faithful port of `PositionUtils.incorporateHeight`
(`PositionUtils.ts:346-368`; see `build_height_adj_probs` in the
linked hoop-explorer blog post). For each position `i` it computes a
height-plausibility mass `cdf(height + 1) - cdf(height - 1)` under
`N(mean_i, sqrt2 * std_i)` (the `sqrt2` "height dampening" widens the
variance so the effect is not too aggressive), multiplies it into the
prior confidence, and renormalizes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `height_in` | `float` |  | Player height in inches. |
| `confs` | `list[float]` |  | The five raw (pre-height) confidences, in `TRAD_POS_LIST` order. |

**Returns**

The five height-adjusted confidences, renormalized to sum to 1 (the `sum_product or 1` guard makes a degenerate all-zero product a no-op rather than a divide-by-zero -- see module landmine index item 1).

**Example**

```python
from sportsdataverse.mbb.mbb_positions import incorporate_height
incorporate_height(81, [0.03, 0.19, 0.49, 0.09, 0.18])
```

### `inject_luck(mutable_stats: 'LineupStatSet', off_luck: 'OffLuckAdjustmentDiags | None', def_luck: 'DefLuckAdjustmentDiags | None') -> 'None'` {#inject_luck}

Reversibly mutate a stat set in place with luck-adjustment deltas.

Faithful port of `LuckUtils.injectLuck` (`LuckUtils.ts:534-650`).
Works on a team, lineup, or player stat dict -- only the fields already
present on `mutable_stats` are touched (see
override_mutable_val`'s object-presence gate), so calling this
on a stat set that doesn't carry a given field (e.g. a bare
`{"key": ..., "doc_count": 0}` placeholder) is a safe no-op for that
field. Passing `off_luck=None, def_luck=None` resets every field this
function has ever touched back to its pre-luck value (see the module
docstring's landmine list for the exact mechanics, including the
absolute-vs-delta distinction on `def_3p`/`oppo_def_3p`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_stats` | `LineupStatSet` |  | The stat-set dict to mutate in place. May be a team/lineup stat set (carries `off_net`/`off_raw_net`/no `oppo_total_def_3p_made`) or a player stat set (carries `oppo_total_def_3p_made`, gating the extra `oppo_def_3p` recompute -- see the module docstring's landmine #2). |
| `off_luck` | `OffLuckAdjustmentDiags \| None` |  | The output of `calc_off_team_luck_adj` / `calc_off_player_luck_adj`, or `None` to omit/reset the offensive-side fields. |
| `def_luck` | `DefLuckAdjustmentDiags \| None` |  | The output of `calc_def_team_luck_adj` / `calc_def_player_luck_adj`, or `None` to omit/reset the defensive-side fields. |

**Returns**

`None` -- this function mutates `mutable_stats` in place (TS `injectLuck` likewise returns nothing).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import (
    calc_off_team_luck_adj, calc_def_team_luck_adj, inject_luck,
)

off_luck = calc_off_team_luck_adj(sample_team_on, sample_players_on, base_team, base_players_map, 100.0)
def_luck = calc_def_team_luck_adj(sample_team_off, base_team, 100.0)
inject_luck(sample_team_on, off_luck, def_luck)
print(sample_team_on["off_3p"])

# Reset back to the pre-luck values

inject_luck(sample_team_on, None, None)
```

### `inject_rapm_into_players(players: 'list[PlayerOnOffStats]', off_rapm_input: 'RapmProcessingInputs', def_rapm_input: 'RapmProcessingInputs', stats_averages: 'PureStatSet', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', read_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value'), write_value_key: 'ValueKey' = 'value') -> 'None'` {#inject_rapm_into_players}

Write `pick_ridge_regression`'s RAPM predictions back onto each player.

Faithful port of `RapmUtils.injectRapmIntoPlayers` (`RapmUtils.ts:781-916`).
For every `onOffReportReplacement` field (minus the possession/title/
separator/`adj_opp` housekeeping keys -- see landmine 11 for the exact,
faithfully-ported omit-key quirk), re-derives that field's off/def target
vectors via `calc_lineup_outputs`, applies each side's
`calculate_rapm` solver, blends in the strong prior (mirroring
`pick_ridge_regression`'s own blend, except for `adj_ppp` which
reuses `off_rapm_input["rapm_adj_ppp"]`/`def_rapm_input["rapm_adj_ppp"]`
directly rather than recomputing), then writes `{playerId}.rapm[field]
= {write_value_key: result, "override": ...}` onto every player not in
`ctx["removed_players"]`.

**NOTE (upstream comment, verbatim): when `write_value_key ==
"old_value"`, this must be called *after* an initial `write_value_key
== "value"` call on the same `players` list** -- the `old_value`
pass .merge`s (lodash_merge`) its results into each player's
*existing* `rapm` dict rather than replacing it, so a player's
`rapm["field"]` ends up carrying both a `value` (from the first
call) and an `old_value` (from the second) side by side.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerOnOffStats]` |  | The players to write RAPM results onto (mutated in place -- each qualifying player gets a `"rapm"` key set/merged). |
| `off_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s offensive output. |
| `def_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s defensive output. |
| `stats_averages` | `PureStatSet` |  | League/context average stat set -- consulted for each field's off/def offset before `ctx["team_info"]`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (the same one `pick_ridge_regression` was called with). |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights, forwarded to `calc_lineup_outputs` / get_strong_weight` exactly as `pick_ridge_regression` does. |
| `read_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- which key (`"value"`/`"old_value"`) to prefer when reading `stats_averages`/`ctx["team_info"]` offsets and when calling `calc_lineup_outputs` (forwarded as its `use_old_val_if_possible` flag). |
| `write_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key each written field carries its result under. |

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import inject_rapm_into_players

inject_rapm_into_players(players, off_results, def_results, {}, ctx, None)
print(players[0]["rapm"]["off_adj_ppp"])  # {"value": ..., "override": None}

# Luck-adjusted two-call sequence (``"value"`` first, THEN ``"old_value"``)

inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("value", "old_value"), "value"
)
inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("old_value", "old_value"), "old_value"
)
```

### `inject_starting_lineup_into_box(sorted_pbp_events: 'list[PlayByPlayEvent]', box_lineup: 'LineupEvent', external_roster: 'tuple[list[str], list[RosterEntry]]', format_version: 'int') -> 'LineupEvent'` {#inject_starting_lineup_into_box}

Infer the starting five and reorder the box-score roster so they lead

(`PlayByPlayUtils.inject_starting_lineup_into_box`,
`PlayByPlayUtils.scala:684-845`).

The v1 (2018+) NCAA box score dropped the ordered list of starters, so we
reconstruct it from the play-by-play sub sequencing. A player is a starter
if, walking the events forward, they are seen *before* their first sub-in
-- either subbed *out* (before ever being subbed in), or *named in a
team-side play* that isn't concurrent with a sub. Anyone subbed *in* before
ever being seen is excluded. The reconstruction stops once five starters
are found.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_pbp_events` | `list[PlayByPlayEvent]` |  | The full play-by-play event stream, ascending time. |
| `box_lineup` | `LineupEvent` |  | The box-score lineup event (its `players` is the full roster to reorder). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` |  | Unused here -- carried for signature parity with the Scala (its pipeline caller passes it). See the module note. |
| `format_version` | `int` |  | Unused here -- carried for signature parity. See the module note. |

**Returns**

A copy of `box_lineup` with `players` reordered so the inferred starters lead. If fewer than five starters could be inferred (a "40-trillion" player who was never subbed nor mentioned), the roster is ordered starters -> possible-starters -> definitely-not-starters as the best available guess.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import inject_starting_lineup_into_box
fixed = inject_starting_lineup_into_box(pbp_events, box_lineup, ([], []), 1)
```

### `inject_validated_players(ordered_lineup_from_box: 'list[str]', box_minus_players: 'LineupEvent', external_roster: 'tuple[list[str], list[RosterEntry]]') -> 'list[str]'` {#inject_validated_players}

Validates box players against the roster (if available) and any

other available box scores (`BoxscoreParser.inject_validated_players`,
`:233-279`).

See the module docstring's "un-threaded `tidy_ctx`" note -- every
fuzzy-resolution call inside the loop uses the SAME original context,
never the updated one a call returns (ported verbatim, including this
apparent Scala oversight).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ordered_lineup_from_box` | `list[str]` |  | The raw player-name strings scraped straight off the box-score page (already v0-normalized if the source was v1, by `get_box_lineup`'s caller). |
| `box_minus_players` | `LineupEvent` |  | The in-progress `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` (used only for its `team` field, both to scope the fuzzy-match context and to key `~sportsdataverse.mbb.mbb_ncaa_data_quality.players_missing_from_boxscore`). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` |  | `(other_players, roster_players)` -- extra known player names, and a full team roster (if available) to validate against / fuzzy-correct box names onto. |

**Returns**

`ordered_lineup_from_box` with any name not found in `roster_players` fuzzy-corrected onto the closest roster name (if a roster was supplied at all), followed by any roster/other/ known-missing players not already present in that corrected list (see the module docstring's "Extra-players Set ordering" note for this trailing group's order).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import inject_validated_players
inject_validated_players(["Player One"], box_lineup, ([], []))
```

### `is_cached(path: 'str', *, cache_dir: 'Optional[Path]' = None) -> 'bool'` {#is_cached}

Return whether *path* already has a cache file on disk.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  |  |
| `cache_dir` | `Optional[Path]` | `None` |  |

### `is_end_of_game_fouling_vs_fastbreak(curr_clump: 'ConcurrentClump', event_parser: 'PossessionEvent') -> 'bool'` {#is_end_of_game_fouling_vs_fastbreak}

Check for intentional fouling to prolong the game, specifically so it

can be excluded from being counted as a fast break
(`is_end_of_game_fouling_vs_fastbreak`, `LineupUtils.scala:603-656`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr_clump` | `ConcurrentClump` |  | The clump to classify. |
| `event_parser` | `PossessionEvent` |  | Selects which side of each event is "attacking". |

**Returns**

`True` iff the FIRST attacking-side FT-made/FT-missed event in `curr_clump.evs` is both near the end of a period AND has the attacking team ahead by `(0, 10]` points; `False` if no such event exists.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_end_of_game_fouling_vs_fastbreak

is_end_of_game_fouling_vs_fastbreak(curr_clump, event_parser)
```

### `is_gen2(ev: 'RawGameEvent') -> 'bool'` {#is_gen2}

Detect the new/"gen2" NCAA event format (`EventUtils.is_gen2`,

`EventUtils.scala:12-14`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `RawGameEvent` |  | The raw game event to inspect. |

**Returns**

`True` if `ev.info` contains a comma-space (`", "`), the gen2 format's field separator; `False` for the old/legacy format.

### `is_scramble(curr_clump: 'ConcurrentClump', prev_clumps: 'list[ConcurrentClump]', event_parser: 'PossessionEvent', player_version: 'bool') -> 'tuple[Callable[[RawGameEvent], bool], str]'` {#is_scramble}

Figure out if (each event of) the current clump is part of a

"scramble scenario" following an ORB (`is_scramble`, `LineupUtils
.scala:222-597`).

Returns a `(predicate, debug_tag)` tuple -- **the tuple shape is
load-bearing**: the oracle asserts the debug tag string directly
(`"N/A"`/`"0a"`/`"1aa"`/`"1ab"`/`"1b"`/`"2aa"`/`"2ab"`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr_clump` | `ConcurrentClump` |  | The clump to classify. |
| `prev_clumps` | `list[ConcurrentClump]` |  | Prior merged clumps, most-recent-first. |
| `event_parser` | `PossessionEvent` |  | Selects which side of each event is "attacking". |
| `player_version` | `bool` |  | Unused -- see the module docstring's `is_scramble` port notes (the Scala's debug-print gate this flag controls is permanently `false` regardless of its value). |

**Returns**

`(predicate, debug_tag)` where `predicate(ev)` reports whether `ev` is part of a scramble.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_scramble

predicate, tag = is_scramble(curr_clump, prev_clumps, event_parser, player_version=False)
[predicate(ev) for ev in curr_clump.evs]
```

### `is_team_shooting_left_to_start(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]') -> 'tuple[bool, int]'` {#is_team_shooting_left_to_start}

Infers which side of the SVG court the team under analysis shoots

towards in the first period, from its own made/missed shot locations
(`ShotEventParser.is_team_shooting_left_to_start`, `:540-555`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs, pre-geometry-transform. |

**Returns**

`(team_shooting_left_in_first_period, first_period)` -- the first element of `sorted_very_raw_events`, if any, determines `first_period`; the majority side (by count) of the team's own (`is_off`) shots within that period determines the direction.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_team_shooting_left_to_start
is_team_shooting_left_to_start([(1, shot_a), (1, shot_b)])
```

### `is_transition(curr_clump: 'ConcurrentClump', prev_clumps: 'list[ConcurrentClump]', event_parser: 'PossessionEvent', player_version: 'bool') -> 'tuple[Callable[[RawGameEvent, bool], bool], str]'` {#is_transition}

Figure out if the current clump is part of a transition offense

following opponent offense (or a marked-fastbreak play) (`is_transition`,
`LineupUtils.scala:668-927`).

Returns a `(predicate, debug_tag)` tuple mirroring `is_scramble`
-- the oracle asserts the debug tag directly (`"N/A"`/`"0a.X"`/
`"1a.a"`/`"1a.b"`/`"1b.a"`/`"1b.b"`/`"1b.X"`/`"NOT"`).
Unlike `is_scramble`'s predicate, this one takes a *second*
argument -- `is_scramble` -- so **scramble always wins**: an event
already classified as a scramble is never additionally tagged
transition (`!is_scramble && is_transition_event`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr_clump` | `ConcurrentClump` |  | The clump to classify. |
| `prev_clumps` | `list[ConcurrentClump]` |  | Prior merged clumps, most-recent-first. |
| `event_parser` | `PossessionEvent` |  | Selects which side of each event is "attacking" (and, for this heuristic, "defending"). |
| `player_version` | `bool` |  | Unused -- see `is_scramble`'s port notes in the module docstring (the Scala's debug-print gate this flag controls is permanently `false` regardless of its value). |

**Returns**

`(predicate, debug_tag)` where `predicate(ev, is_scramble)` reports whether `ev` is part of a transition play, given whether it was already classified as a scramble.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_transition

predicate, tag = is_transition(curr_clump, prev_clumps, event_parser, player_version=False)
[predicate(ev, is_scramble=False) for ev in curr_clump.evs]
```

### `is_women_game(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]') -> 'bool'` {#is_women_game}

Infers men's vs. women's game from timing evidence

(`ShotEventParser.is_women_game`, `:558-566`). **Shot-parser-specific
variant** -- distinct from the play-by-play parser's own
`is_women_game` (Task 5e.3), which uses PbP event timing instead of
shot timing; the plan's recon flags both as "its OWN is_women_game
variant" per module.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs. |

**Returns**

`True` if at least 4 periods were seen AND no shot was taken with more than 10 minutes showing on the (descending) clock in the very first event (women's quarters are 10 minutes; a shot at >10:00 remaining could only happen in a longer men's period).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_women_game
is_women_game([(1, shot), (2, shot), (3, shot), (4, shot)])  # True
```

### `jsoup_text(el: 'Optional[Tag]') -> 'str'` {#jsoup_text}

JSoup `Element.text()`: all descendant text, whitespace-collapsed.

JSoup's `.text()` joins every text node under `el` (including
descendants) and collapses runs of whitespace (spaces, tabs, newlines)
into single spaces, trimming the ends. bs4's `.get_text()` does the
joining but not the collapsing, so captured HTML's indentation/newlines
would otherwise leak into every extracted value.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `el` | `Optional[Tag]` |  | The element to extract text from, or `None`. |

**Returns**

The whitespace-collapsed text, or `""` if `el` is `None`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html
soup = parse_html("<td>\n  Akin,\tDaniel  </td>")
jsoup_text(soup.find("td"))  # "Akin, Daniel"
```

### `kmeans_fit(X: 'np.ndarray', k: 'int', seed: 'int', n_init: 'int' = 10, max_iter: 'int' = 100) -> "'tuple[np.ndarray, np.ndarray]'"` {#kmeans_fit}

Seeded Lloyd's KMeans, best-of-`n_init` by inertia.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)` (standardize first). |
| `k` | `int` |  | Number of clusters. |
| `seed` | `int` |  | RNG seed (deterministic output). |
| `n_init` | `int` | `10` | Independent restarts. |
| `max_iter` | `int` | `100` | Lloyd iterations per restart. |

**Returns**

`(centers[k, d], labels[n])`.

**Example**

```python
centers, labels = kmeans_fit(Z, k=8, seed=0)
```

### `lineup_as_raw_clumps(lineup: 'LineupEvent') -> 'Iterator[ConcurrentClump]'` {#lineup_as_raw_clumps}

Turn one lineup's raw events into unprocessed singleton clumps, plus a

trailing lineup-boundary marker (`Concurrency.lineup_as_raw_clumps`,
`PossessionUtils.scala:114-120`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup event to expand. |

**Returns**

One `ConcurrentClump([ev])` per raw event (in order), then a final `ConcurrentClump([], [lineup])` boundary marker.

### `lineup_balancer(lineups: 'list[LineupEvent]', team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', clump: 'ConcurrentClump', prev_clump: 'ConcurrentClump') -> 'list[LineupEvent]'` {#lineup_balancer}

Attribute this clump's possessions to the candidate lineup(s)

(`PossessionUtils.assign_to_right_lineup.lineup_balancer`,
`PossessionUtils.scala:429-471`).

A single candidate just receives the whole clump's possessions. Multiple
candidates (a lineup change landing mid-clump) are split via a greedy
round-robin: for each direction, rank lineups by an "approximate" possession
count computed from just that lineup's own raw events at the clump's
minute, then hand out possessions one at a time to whichever lineup
currently has the highest remaining approximate share.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupEvent]` |  | The candidate lineups (already updated with any running state total from `assign_to_right_lineup`). |
| `team_stats` | `PossCalcFragment` |  | This clump's team-direction fragment. |
| `opponent_stats` | `PossCalcFragment` |  | This clump's opponent-direction fragment. |
| `clump` | `ConcurrentClump` |  | The merged clump being assigned. |
| `prev_clump` | `ConcurrentClump` |  | The previous merged clump (only used for the first candidate's approximate stats -- see below). |

**Returns**

New lineup copies with `num_possessions` incremented.

### `lineup_fixer(lineups: 'list[LineupEvent]') -> 'list[LineupEvent]'` {#lineup_fixer}

Clamp obviously-broken possession counts (``PossessionUtils

.assign_to_right_lineup.lineup_fixer`, `PossessionUtils.scala:490-507`).

For both `team_stats` and `opponent_stats` independently: a lineup
that scored (`pts > 0``) but was attributed zero-or-fewer possessions
is clamped to exactly 1 (you can't score on zero possessions); any
still-negative possession count is clamped to 0.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupEvent]` |  | The lineups to fix (already balanced). |

**Returns**

New lineup copies with clamped `num_possessions`.

### `lineup_to_team_report(lineup_report: 'LineupStatSet', inc_replacement: 'bool' = False, regress_diffs: 'float' = 0.0, rep_on_off_diag_mode: 'int' = 0) -> 'LineupStatSet'` {#lineup_to_team_report}

Build per-player on/off splits out of a team's lineups.

Faithful port of `LineupUtils.lineupToTeamReport` (`LineupUtils.ts:277`).
For every distinct player across `lineup_report["lineups"]`, partitions
the team's lineups into ON (the player was on the floor) and OFF (they
weren't) buckets, merging each bucket via `weighted_avg` /
`complete_weighted_avg`. Also builds a `teammates` map of
possession overlap with every other player, and -- when
`inc_replacement=True` -- a "replacement" on-minus-off composite via
combine_replacement_on_off`.

Lineups whose `key` is the empty string are skipped in the
on/off-partition loop (workaround for an upstream data issue, tracked
as upstream issue #53) but still contribute to the player roster.
Every lineup's `rapmRemove` key (if present, e.g. left over from a
prior `calculate_aggregated_lineup_stats` call sharing the same
input list) is deleted as a side effect while building the roster --
`lineup_to_team_report` itself never consults `rapmRemove`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_report` | `LineupStatSet` |  | `{"lineups": [...], "avgOff": ..., "error_code": ...}` -- the per-team lineup list plus metadata (mirrors upstream's `LineupStatsModel`). Only `lineups` and `error_code` are consumed here. |
| `inc_replacement` | `bool` | `False` | When `True`, additionally builds each player's `replacement` on-minus-off composite (more expensive -- scans every OFF lineup against every ON lineup for a 4-of-5-shared- players complement match). |
| `regress_diffs` | `float` | `0.0` | Forwarded to combine_replacement_on_off`'s final `complete_weighted_avg` call -- regression toward ~1000 possessions for the replacement diff (only meaningful when `inc_replacement=True`). |
| `rep_on_off_diag_mode` | `int` | `0` | When `> 0`, retains diagnostic detail (`myLineups` on each player's replacement entry, plus `lineupUsage` bookkeeping) instead of discarding it after use. |

**Returns**

{code: id}, "players": [...], "error_code": ...}`. Each entry in `players` is `{"playerId", "playerCode", "teammates", "on", "off", "replacement"}` -- `on`/`off` are finished `LineupStatSet` averages (or, for a player who's always ON, an all-zero `off`); `replacement` is `None` unless `inc_replacement=True``.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report

report = lineup_to_team_report({"lineups": buckets, "error_code": None})
for player in report["players"]:
    print(player["playerId"], player["on"]["off_poss"]["value"])

# With replacement (on-minus-off) splits

report = lineup_to_team_report(
    {"lineups": buckets, "error_code": None},
    inc_replacement=True,
    regress_diffs=-500,
)
```

### `log_loss_score(y_true: 'np.ndarray', p_pred: 'np.ndarray', eps: 'float' = 1e-15) -> 'float'` {#log_loss_score}

Binary cross-entropy (log loss) between outcomes and probabilities.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of realized binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in `[0, 1]`. |
| `eps` | `float` | `1e-15` | Clipping bound to keep the log finite at 0/1. |

**Returns**

The mean log loss (lower is better).

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_prediction_constants import log_loss_score
log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### `logistic_fit(X: 'np.ndarray', y: 'np.ndarray', lam: 'float' = 1.0) -> 'np.ndarray'` {#logistic_fit}

L2-penalized logistic regression via L-BFGS (intercept unpenalized).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)`. |
| `y` | `ndarray` |  | Binary outcomes (0/1). |
| `lam` | `float` | `1.0` | L2 penalty on the non-intercept coefficients. |

**Returns**

Coefficient vector of length `d + 1` (intercept first).

**Example**

```python
coef = logistic_fit(X, drafted, lam=1.0)
```

### `mae(a: 'np.ndarray', b: 'np.ndarray') -> 'float'` {#mae}

Mean absolute error between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array. |
| `b` | `ndarray` |  | Second array (same length as `a`). |

**Returns**

The mean absolute error.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_prediction_constants import mae
mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
```

### `matching_player(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', tidy_ctx: 'TidyPlayerContext', code_match: 'bool') -> 'bool'` {#matching_player}

Whether the player in `pbp_event` matches `shot`'s shooter

(`ShotEnrichmentUtils.matching_player`, `PlayByPlayUtils.scala:638-652`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched. |
| `pbp_event` | `MiscGameEvent` |  | The candidate play-by-play event. |
| `tidy_ctx` | `TidyPlayerContext` |  | The name-resolution context. |
| `code_match` | `bool` |  | If `True`, compare on player *code* only (looser -- lets a name that resolves to the wrong identity but the right code match); if `False`, require full `~sportsdataverse.mbb .mbb_ncaa_models.PlayerCodeId` equality. |

**Returns**

`True` if the resolved player matches `shot.player` under the selected comparison, else `False`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import matching_player
matching_player(shot, pbp_event, tidy_ctx, code_match=False)
```

### `misspellings(team: 'Optional[TeamId]') -> 'dict[str, str]'` {#misspellings}

Team-scoped misspelling map, falling back to the generic map

(`DataQualityIssues.misspellings`, `DataQualityIssues.scala:165-322`
-- see the module docstring's "fallback semantics" note for why this is
a precomputed merge, not a runtime two-level lookup).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `Optional[TeamId]` |  | The team to look up team-specific corrections for. `None` (like any team absent from the table) falls back to `generic_misspellings`. |

**Returns**

A fresh dict -- the team's misspelling map merged with `generic_misspellings`, or a copy of `generic_misspellings` if `team` has no specific entries.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import misspellings
from sportsdataverse.mbb.mbb_ncaa_models import TeamId

misspellings(TeamId("NJIT"))["Lewal, Levi"]  # 'Lawal, Levi'
misspellings(TeamId("Some Unlisted Team"))  # {} (generic fallback)
misspellings(None)  # {} (generic fallback)
```

### `order_lineup(player_codes_and_ids: 'list[dict[str, str]]', players_by_id: 'dict[str, dict[str, Any]]', team_season: 'str') -> 'list[dict[str, str]]'` {#order_lineup}

Order a 5-man lineup `X1_X2_X3_X4_X5` into PG/SG/SF/PF/C slot order.

Faithful port of `PositionUtils.orderLineup` (`PositionUtils.ts:696-761`).
Greedily fits each player (in input order) to their best-scoring slot via
fit_player` (dominated by `pos_class_to_score` on the
player's `posClass`, tie-broken by their raw `posConfidences`),
evicting and recursively re-fitting any player displaced along the way,
then applies `apply_relative_positional_overrides` (keyed on
`team_season`) as a final hand-tuned correction pass.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_codes_and_ids` | `list[dict[str, str]]` |  | The lineup membership, each a `{"code": ..., "id": ...}` dict. Order does not affect the final result (the slot-fitting algorithm is order-invariant by construction -- displaced players are always re-fit). |
| `players_by_id` | `dict[str, dict[str, Any]]` |  | Per-player positional info keyed by `id`, each a `{"posConfidences": [pg, sg, sf, pf, c], "posClass": "..."}` dict (the tradPosList-ordered raw confidence scores plus the classifier's `ID_TO_POSITION`-keyed class label). |
| `team_season` | `str` |  | Key into `RELATIVE_POSITION_FIXES` for the final override pass. |

**Returns**

A 5-element list of `{"code": ..., "id": ...}` dicts in PG/SG/SF/PF/C order.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import order_lineup
players_by_id = {
    "Cowan, Anthony": {"posConfidences": [60, 40, 10, 0, 0], "posClass": "s-PG"},
    "Ayala, Eric": {"posConfidences": [40, 60, 10, 0, 0], "posClass": "CG"},
}
order_lineup(
    [{"code": "AnCowan", "id": "Cowan, Anthony"},
     {"code": "ErAyala", "id": "Ayala, Eric"}],
    players_by_id, "",
)
```

### `phase1_shot_event_enrichment(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]', second_half_override: 'Optional[set[int]]' = None) -> 'list[ShotEvent]'` {#phase1_shot_event_enrichment}

The court-geometry enrichment pass: ascending time, coordinate

transform + geo synthesis, and the self-correcting side-flip re-run
(`ShotEventParser.phase1_shot_event_enrichment`, `:415-528`).

For each shot: compute the ascending game time, decide (from
`is_team_shooting_left_to_start` + which half the period falls in)
whether the shot's side needs flipping, run `transform_shot_location`
to get both the believed-correct and alternative (mirrored) locations,
keep whichever is closer to the basket (a >1.2x distance advantage for
the "alternative" wins, or ANY shot taken with <0.1 min left on the
clock always keeps the original -- a half-court heave near the buzzer
is plausible, so the tie-break favors trusting the raw geometry there),
then synthesize a lat/lon.

After all shots are processed, if any period had >=6 shots AND more than
75% of them came back implausibly long-distance (>50ft), the whole pass
re-runs ONCE with those periods' orientation flipped (the self-correcting
part) -- `second_half_override` is `None` on the initial call and a
non-`None` set on the one allowed retry, preventing infinite recursion.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs from `parse_shot_html`, pre-geometry-transform. |
| `second_half_override` | `Optional[set[int]]` | `None` | The set of periods whose `second_half_switch` orientation should be inverted (the self-correction re-run's input); `None` on the first call. |

**Returns**

The fully court-geometry-enriched shots, in the same order as `sorted_very_raw_events`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import phase1_shot_event_enrichment
shots = phase1_shot_event_enrichment([(1, very_raw_shot)])
```

### `pick_ridge_regression(off_weights: 'NDArray[np.float64]', def_weights: 'NDArray[np.float64]', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', diag_mode: 'bool', agg_value_key: 'ValueKey' = 'value', lineup_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value')) -> 'tuple[RapmProcessingInputs, RapmProcessingInputs]'` {#pick_ridge_regression}

Adaptively pick a ridge-regression lambda and blend in the RAPM priors.

Faithful port of `RapmUtils.pickRidgeRegression` (`RapmUtils.ts:1001-1540`)
-- the top-level driver that, per off/def side: scales a dimensionless
`lambda_range` by the design matrix's mean singular value
(`avg_eigen_val`) into an actual ridge strength, solves via
`slow_regression`/`calculate_rapm`, blends in each player's
strong prior (get_strong_weight`), reconciles the possession
-weighted team total against the actual team efficiency
(`[IMPORTANT-EQUATION-01]`, see below), nudges the result back towards
the weak priors on any remaining error (`apply_weak_priors`), and
decides whether to keep sweeping `lambda` upward, roll back to the
previous step, or stop.

**`[IMPORTANT-EQUATION-01]`** (`RapmUtils.ts:1306-1314`/`:1325-1333`):
`combined_adj_eff = sum(pct_by_player[i] * rapm[i] for i) +
add_low_volume_adj_rtg`, compared against `actual_eff[off_or_def]`
(the team's actual, prior-basis-adjusted efficiency, including
bench/removed-player possessions) to derive `adj_eff_err` -- the error
signal both the weak-prior nudge and the stopping rule react to.

**Stopping rule** (checked once per `lambda` step, in order): (1) once a
*second* step has run (`not_first_step`) and, unless in `diag_mode`,
the current step is past `lambda_range_to_use[3]`, roll back to the
*previous* step's `soln_matrix`/`ridge_lambda` (but **not**
`rapm_adj_ppp`/`rapm_raw_adj_ppp`/`sd_rapm`, which stay at the
current, over-threshold step's values -- a faithful, non-obvious TS
asymmetry, `RapmUtils.ts:1443-1448` vs `:1483-1484`) when
`adj_eff_err >= error_exit_thresh` (`1.35` for the low-possession
-count offense special case, else `1.05`) **and** the error is still
increasing (`>= last_error`); else (2) stop in place once
`mean_diff` (the mean per-player RAPM change since the previous step)
drops below `pick_ridge_thresh` (`0.061` off / `0.091` def --
"more confident in offensive priors"); else (3) keep sweeping.

**Adaptive-weight / prior asymmetry** (the deep-equality oracle's load
-bearing behavior): the per-player strong-prior blend
(get_strong_weight(ctx["prior_info"], adaptive_correl_weights[i])`)
only consults `adaptive_correl_weights` when
`ctx["prior_info"]["strong_weight"] < 0` (adaptive mode) -- a fixed,
non-negative `strong_weight` always wins. A fixture whose
`players_strong` entries carry no `def_adj_ppp` key makes the blend's
`stat.get(f"{off_or_def}_adj_ppp") or 0.0` term (and, transitively,
`calc_lineup_outputs`'s own `strong_val` term) contribute exactly
`0` on the def side regardless of `strong_weight` or
`adaptive_correl_weights` -- see the oracle test's `def_results1`/
`def_results2` invariance assertions.

**`svd` is `numpy.linalg.svd(..., compute_uv=False)`, singular values
only.** Upstream's `SVD(weights[side].valueOf())` (`svd-js`) also
computes `u`/`v`, but only `svd.q` (the singular values, via
`mean(svd.off.q)`/`mean(svd.def.q)` at `avg_eigen_val`,
`RapmUtils.ts:1077`) is ever read -- `u`/`v` are dead. Skipping them
is an efficiency-only deviation with an identical result (singular
values are unique to a matrix regardless of the underlying SVD
implementation).

**Dead-debug computation promoted to a real output (Python-side
addition, not upstream's own shape):** upstream also computes
`residuals`/`errSq`/`paramErrs`/`sdRapm` at this point
(`RapmUtils.ts:1363-1394`) purely to feed a `console.log` gated
behind the same hardcoded-`False` `debugMode` as
`apply_weak_priors` -- none of the four is ever stored on
`acc.output` upstream (`RapmProcessingInputs` has no `sdRapm`
field there either). Since Task 3.4 built
`calculate_predicted_out`/`calculate_residual_error`/
`calc_slow_pseudo_inverse`/`calculate_sd_rapm` specifically
so this task could surface real standard errors, this port keeps
calling all four (matching TS's actual computation, which reuses the
exact same `XᵀX + ridge_lambda·I` inverse `slow_regression`
already computed -- so no *new* failure mode is introduced by keeping
this) and additionally stores the result on `sd_rapm` -- a superset
of, not a divergence from, the upstream return shape.

**`soln_matrix`/`sd_rapm` are nested Python `list`s, not
`NDArray`s.** Every field on the returned `RapmProcessingInputs`
is a plain (possibly nested) Python `list`/`float` specifically so
the whole dict stays comparable via plain `==` -- the oracle's deep
-equality assertions (e.g. `off_results1 == off_results`) would
otherwise raise `ValueError: truth value of an array with more than one
element is ambiguous` the moment Python's dict/list equality machinery
tried to `bool()` a multi-element `ndarray` comparison.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `off_weights` | `NDArray[float64]` |  | The offensive design matrix (e.g. `calc_player_weights`'s first return value). |
| `def_weights` | `NDArray[float64]` |  | The defensive design matrix. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`) -- see the "adaptive-weight / prior asymmetry" note above. |
| `diag_mode` | `bool` |  | If `True`, keeps sweeping every remaining `lambda` step (collecting `prev_attempts` diagnostics for all of them) even after a stopping condition has already fired, and relaxes the rollback/pick eligibility guards for the first few (`< lambda_range_to_use[3]`) diagnostic-only steps. **Not exercised by this task's oracle** (always called with `False`) -- ported faithfully from TS, uncovered by test. |
| `agg_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key team/aggregate-level reads (`actual_eff`, the low-volume player adjustment) prefer when present. |
| `lineup_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- forwarded to `calc_lineup_outputs` as its `use_old_val_if_possible` flag (translated: `key == "old_value"`). |

**Returns**

`(off_results, def_results)` -- two `RapmProcessingInputs`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import pick_ridge_regression

off_results, def_results = pick_ridge_regression(
    off_weights, def_weights, ctx, None, False
)
print(off_results["ridge_lambda"], off_results["rapm_adj_ppp"][:3])
```

### `player_per100_features(season_stats: 'pl.DataFrame') -> 'pl.DataFrame'` {#player_per100_features}

Per-100 / rate features for every (player_id, season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season_stats` | `DataFrame` |  | One row per player-season with the canonical counting columns (`minutes, field_goals_made, field_goals_attempted, three_point_field_goals_made, free_throws_attempted, turnovers, points, fga_rim, fga_mid, fga_three, offensive_rebounds, defensive_rebounds, assists, blocks, steals`) -- built from the player-boxscore aggregation (see the Phase-0 fitters). |

**Returns**

One row per (player_id, season): ids as `Utf8` plus the 17 rate / per-100 features. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import player_per100_features
feats = player_per100_features(season_stats)
```

### `playwright_transport(*, headless_new: 'bool' = True, challenge_wait_ms: 'int' = 8000, nav_timeout_ms: 'int' = 30000, user_agent: 'Optional[str]' = None) -> "'_PlaywrightTransport'"` {#playwright_transport}

Build the **suggested** stats.ncaa.org game-detail scraping transport.

Drives a real Chromium via Playwright in Chrome's new-headless mode
(`--headless=new`) to clear the Akamai `bm-verify` challenge that
`curl_cffi` cannot, then serves raw server HTML for the 5a-5e parsers.
Playwright is a **lazy optional import** (not a hard dependency); a clear
`ImportError` fires on first use if it is missing.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headless_new` | `bool` | `True` | Use `--headless=new` (real-GPU render, no window) -- the default and the proven-working mode. `False` runs old headless (`headless_shell`), which Akamai flags -- avoid. |
| `challenge_wait_ms` | `int` | `8000` | Milliseconds to let the bm-verify sensor run after the first navigation. |
| `nav_timeout_ms` | `int` | `30000` | Per-navigation timeout. |
| `user_agent` | `Optional[str]` | `None` | Override the Chrome UA string. |

**Returns**

A stateful, callable `FetchTransport` reusing one browser for the session. Close it when done (it is a context manager, has `close()`, and registers an `atexit` safety net).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
with NcaaFetcher.with_browser() as fetcher:
    pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
    box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML
# -> feed to get_box_lineup / create_lineup_data (mbb_ncaa_*_parser)
```

### `pos_class_to_score(pos_class: 'str') -> 'int'` {#pos_class_to_score}

Ordinal "positional weight" for a position class, PG=1000..C=8000.

Faithful port of `PositionUtils.posClassToScore` (`PositionUtils.ts:629-654`,
a literal `switch`). Unmapped classes default to `4000` (the TS
default-case comment notes "won't happen").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pos_class` | `str` |  | A position-class code (e.g. `"PG"`, `"WF"`, `"C"`). |

**Returns**

The class's ordinal score.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import pos_class_to_score
pos_class_to_score("WF")
```

### `poss_calc_fragment_sum(a: 'PossCalcFragment', b: 'PossCalcFragment') -> 'PossCalcFragment'` {#poss_calc_fragment_sum}

Field-wise add two `PossCalcFragment`\ s

(`PossCalcFragment.sum`, `PossessionUtils.scala:146-153`).

The Scala original uses `shapeless.Generic` to zip the two case
classes' fields and sum pairwise; since every field is a plain `Int`,
a plain `zip` over `dataclasses.astuple` reproduces the same
behavior without the generic-programming machinery.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `PossCalcFragment` |  | The left-hand fragment. |
| `b` | `PossCalcFragment` |  | The right-hand fragment. |

**Returns**

A new `PossCalcFragment` with each field summed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import (
    PossCalcFragment,
    poss_calc_fragment_sum,
)

frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
frag2 = PossCalcFragment(1, 3, 5, 7, 9, 11, 13, 15)
poss_calc_fragment_sum(frag1, frag2)
# PossCalcFragment(2, 5, 8, 11, 14, 17, 20, 23)
```

### `predict_margin(home_adj_em: 'float', away_adj_em: 'float', neutral: 'bool' = False) -> 'float'` {#predict_margin}

Women's expected margin.

Delegates to `sportsdataverse.mbb.mbb_game_predict.predict_margin` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_em` | `float` |  | Home team's adjusted efficiency margin. |
| `away_adj_em` | `float` |  | Away team's adjusted efficiency margin. |
| `neutral` | `bool` | `False` | True for a neutral-site game. |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.wbb.wbb_game_predict import predict_margin
predict_margin(20.0, 10.0)
```

### `predict_total(home_adj_o: 'float', home_adj_d: 'float', away_adj_o: 'float', away_adj_d: 'float', home_tempo: 'float', away_tempo: 'float') -> 'float'` {#predict_total}

Women's expected total points.

Delegates to `sportsdataverse.mbb.mbb_game_predict.predict_total` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_o` | `float` |  | Home adjusted offensive efficiency (points / 100 poss). |
| `home_adj_d` | `float` |  | Home adjusted defensive efficiency. |
| `away_adj_o` | `float` |  | Away adjusted offensive efficiency. |
| `away_adj_d` | `float` |  | Away adjusted defensive efficiency. |
| `home_tempo` | `float` |  | Home adjusted tempo (possessions / game). |
| `away_tempo` | `float` |  | Away adjusted tempo. |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.wbb.wbb_game_predict import predict_total
predict_total(100.0, 88.0, 96.0, 92.0, 72.0, 70.0)
```

### `project_bracket(resume: 'pl.DataFrame', auto_bids: 'set[str]', *, league: 'str' = 'mens', field_size: 'int' = 68) -> 'pl.DataFrame'` {#project_bracket}

Select and seed a tournament field from a per-team résumé frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `resume` | `DataFrame` |  | One row per (season, team_id) with `adj_em_z, sos, wab, quad1_w` (the ratings + strength-of-schedule outputs joined). |
| `auto_bids` | `set[str]` |  | `team_id` set of conference auto-bid winners (see conference_auto_bids`); always in the field. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (kept for shim parity; the blend is league-agnostic). |
| `field_size` | `int` | `68` | Tournament field size (68). |

**Returns**

One row per input team: `season, team_id, resume_score, projected_seed` (1-16, capped for the First Four; null outside the field), `at_large_prob` (logistic in `resume_score` centred on the selection cutoff -- every selected at-large clears 0.5), `auto_bid`, `bid` (exactly `field_size` true).

**Example**

```python
from sportsdataverse.mbb.mbb_bracketology import project_bracket
field = project_bracket(resume, auto_bids)
```

### `raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'` {#raw_game_efficiency}

Per-team, per-game possessions + raw offensive/defensive efficiency.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids as strings or ints; cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team boxscore with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_eff, def_eff`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_mbb_schedule([2024]), load_mbb_team_boxscore([2024]))
```

### `regress_shot_quality(stat: 'float', pos: 'int', feat: 'str', player: 'dict[str, Any]') -> 'float'` {#regress_shot_quality}

Shrink a small-sample shot-quality stat toward its positional average.

Faithful port of `PositionUtils.regressShotQuality`
(`PositionUtils.ts:216-258`). Only the three relative shot-quality
features (`calc_three_relative` / `calc_rim_relative` /
`calc_mid_relative`) are regressed; any other `feat` passes `stat`
through unchanged. A player is regressed toward the positional average
whenever the relevant shot volume is below `max(0.25 * total_fga, 15)`
(i.e. under 25% of their attempts come from that zone, floored at 15
attempts). A `center` (`pos == 4`) who took 0-2 threes and made none is
left at `0` to avoid widespread changes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat` | `float` |  | The raw (unregressed) feature value. |
| `pos` | `int` |  | Position index (`0=pg` ... `4=c`). |
| `feat` | `str` |  | Feature field name (only the three relative shot-quality keys trigger regression; anything else is a passthrough). |
| `player` | `dict[str, Any]` |  | The player stat dict; reads `total_off_fga` and the per-feature volume field (`total_off_{3p,2pmid,2prim}_attempts`), each shaped `{"value": N}`. |

**Returns**

The regressed feature value (or `stat` unchanged when the feature is not regressed, volume is sufficient, or the center-3s carve-out fires).

**Example**

```python
from sportsdataverse.mbb.mbb_positions import regress_shot_quality
player = {"total_off_fga": {"value": 25},
          "total_off_3p_attempts": {"value": 1}}
regress_shot_quality(-15.5, 2, "misc_feature", player)

# Low-volume shrink toward the positional average

regress_shot_quality(100, 3, "calc_rim_relative",
    {"total_off_fga": {"value": 25},
     "total_off_2prim_attempts": {"value": 8}})
```

### `remove_diacritics(fragment: 'str') -> 'str'` {#remove_diacritics}

Strip diacritical marks, e.g. `"Juhász"` -> `"Juhasz"`

(`ExtractorUtils.scala:38-43`: NFD normalization then removal of the
combining-diacritical-marks block).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fragment` | `str` |  | Any string (a full player name or a name fragment). |

**Returns**

The string with combining marks removed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import remove_diacritics
print(remove_diacritics("Dorka Juhász"))  # "Dorka Juhasz"
```

### `reorder_and_reverse(reversed_partial_events: 'Iterable[PlayByPlayEvent]') -> 'list[PlayByPlayEvent]'` {#reorder_and_reverse}

Orders same-minute play-by-play events so subs never enclose the plays

they logically precede/follow (`ExtractorUtils.scala:435-599`).

Groups consecutive events sharing the same `min` into a block (the
input arrives in descending/reverse-chronological order, so blocks are
discovered and internally accumulated in reverse too), then -- for any
block containing a sub -- reorders it via `inner_sort`: events
referencing a subbed-OUT player (or scoring no higher than the sub) land
in a pre-sub group, the subs themselves come next (in ascending-score
order), and events referencing a subbed-IN player (or scoring higher
than the sub) land in a trailing post-sub group. Free-throw attempts
sharing the sub's inferred "direction" (team vs. opponent, inferred from
the nearest preceding shot/FT/foul) are pulled into the pre-sub group
unless the shooter is one of the players being subbed in. Blocks with no
sub are returned unchanged apart from the initial score-based sort.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `reversed_partial_events` | `Iterable[PlayByPlayEvent]` |  | Events for one lineup event, in reverse-chronological (descending-time) order -- the natural order encountered walking play-by-play text bottom-up. |

**Returns**

The same events, forward-chronological (ascending time), with each same-minute block internally reordered so no sub encloses a play it logically shouldn't.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import Score
from sportsdataverse.mbb.mbb_ncaa_stints import (
    OtherTeamEvent,
    SubInEvent,
    reorder_and_reverse,
)
events = [
    SubInEvent(0.4, Score(0, 0), "player1"),
    OtherTeamEvent(0.4, Score(0, 0), "rebound"),
]
reorder_and_reverse(events)
# [OtherTeamEvent(...), SubInEvent(...)]
```

### `reset_config() -> 'NcaaFetchConfig'` {#reset_config}

Reset the active config to its env-var-derived defaults.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import update_config, reset_config
update_config(timeout=5)
reset_config()
```

### `ridge_cv_lambda(X: 'np.ndarray', y: 'np.ndarray', groups: 'np.ndarray', lams: "'list[float]'") -> 'float'` {#ridge_cv_lambda}

Pick lambda by leave-one-group-out CV (groups = seasons/classes).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix. |
| `y` | `ndarray` |  | Targets. |
| `groups` | `ndarray` |  | Group label per row (e.g. season); each held out once. |
| `lams` | `list[float]` |  | Candidate penalties. |

**Returns**

The candidate with the lowest mean held-out MSE.

**Example**

```python
lam = ridge_cv_lambda(X, y, seasons, [0.1, 1, 10, 100])
```

### `ridge_fit(X: 'np.ndarray', y: 'np.ndarray', lam: 'float') -> 'np.ndarray'` {#ridge_fit}

Closed-form ridge with an unpenalized intercept (coefficient 0).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)`. |
| `y` | `ndarray` |  | Targets `(n,)`. |
| `lam` | `float` |  | L2 penalty on the non-intercept coefficients. |

**Returns**

Coefficient vector of length `d + 1` (intercept first).

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_player_value_constants import ridge_fit
beta = ridge_fit(np.random.rand(50, 3), np.random.rand(50), lam=1.0)
```

### `right_kind_of_shot(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', strict: 'bool') -> 'bool'` {#right_kind_of_shot}

Whether `pbp_event`'s shot type is compatible with `shot`'s

distance and make/miss (`ShotEnrichmentUtils.right_kind_of_shot`,
`PlayByPlayUtils.scala:659-679`).

The distance-in-the-data is approximate, so exact 2-vs-3 discrimination is
impossible; this only rules out the *obvious* mismatches (a clearly-short
shot matched to a 3, or vice versa) and always requires make/miss
agreement.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched (`pts`/`dist` read). |
| `pbp_event` | `MiscGameEvent` |  | The candidate play-by-play event. |
| `strict` | `bool` |  | If `True`, also apply the distance gate; if `False`, only the make/miss agreement is required. |

**Returns**

`True` if the event could plausibly be this shot.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import right_kind_of_shot
right_kind_of_shot(shot, pbp_event, strict=True)
```

### `roc_auc(y_true: 'np.ndarray', score: 'np.ndarray') -> 'float'` {#roc_auc}

Area under the ROC curve via the rank-sum (Mann-Whitney) identity.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Binary outcomes (0/1). |
| `score` | `ndarray` |  | Predicted scores (any monotone scale). |

**Returns**

AUC in `[0, 1]`; `nan` when only one class is present.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_player_value_constants import roc_auc
roc_auc(np.array([0, 1]), np.array([0.2, 0.9]))
```

### `run_iterative_adjustment_with_hca(teams: 'Sequence[TeamDetail]', team_by_name: 'dict[str, TeamDetail]', fields: 'Sequence[str]', league_averages: 'LeagueAverages', poss_splits: 'dict[str, PossessionSplits]', *, max_iterations: 'int' = 100, tolerance: 'float' = 1e-06) -> 'IterationResult'` {#run_iterative_adjustment_with_hca}

KenPom-style SoS + HCA fixed-point solver (`runIterativeAdjustmentWithHCA`, `ts:306-527`).

Each iteration (Jacobi -- all teams read the *previous* iteration's
adjustments, then commit together):

1. Per team/field, adjust every game
   `adj_game = raw_game * (league / (opp_adj +/- hca))` and take the
   weighted mean; a field with no valid games keeps its current value.
2. Re-estimate per-field HCA from home/away possession-imbalance residuals
   `hca = sum((raw - pred) * |imbalance|) / sum(|imbalance|)` over teams
   with `|imbalance| >= IMBALANCE_MIN`.

Stops when the max per-team/field change drops below `tolerance` or after
`max_iterations` sweeps (the HCA re-estimate still runs on the final
sweep). The cross-guard on the per-game branch, the asymmetric residual
prediction, and the cross-named opponent strengths are all preserved -- see
the module docstring's landmine list.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | The teams to solve over. |
| `team_by_name` | `dict[str, TeamDetail]` |  | `{team_name: team_detail}` for opponent lookup. |
| `fields` | `Sequence[str]` |  | The stat fields to solve. |
| `league_averages` | `LeagueAverages` |  | Output of `compute_league_averages_from_per_game`. |
| `poss_splits` | `dict[str, PossessionSplits]` |  | `{team_name:` `PossessionSplits` `}`. |
| `max_iterations` | `int` | `100` | Iteration cap (default `MAX_ITERATIONS`; pin to `1` to inspect a single sweep). |
| `tolerance` | `float` | `1e-06` | Convergence tolerance (default `TOLERANCE`). |

**Returns**

An `IterationResult` (`adj_values`, `hca_per_field`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import (
    STRENGTH_ADJUSTED_FIELDS,
    compute_league_averages_from_per_game,
    compute_possession_splits,
    run_iterative_adjustment_with_hca,
)

by_name = {t["team_name"]: t for t in teams}
league = compute_league_averages_from_per_game(teams)
splits = {t["team_name"]: compute_possession_splits(t) for t in teams}
result = run_iterative_adjustment_with_hca(
    teams, by_name, STRENGTH_ADJUSTED_FIELDS, league, splits,
)
print(result.hca_per_field["3p"]["hca_off"])
```

### `save_artifact(name: 'str', obj: 'dict') -> 'None'` {#save_artifact}

Write a bundled artifact (dev/fitter use -- writes into the source tree).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Artifact stem, e.g. `"mbb_box_bpm"`. |
| `obj` | `dict` |  | JSON-serializable artifact payload. |

**Example**

```python
save_artifact("mbb_box_bpm", {"league": "mens", "coef": [0.1]})
```

### `score_to_tuple(s: 'str') -> 'tuple[int, int]'` {#score_to_tuple}

Parse a `"scored-allowed"` score string (`ExtractorUtils.score_to_tuple`,

`ExtractorUtils.scala:107-113`).

Scala's `str match { case regex(s1, s2) => ... }` on a compiled
`Regex` requires the ENTIRE string to match (`Regex.unapplySeq` calls
`Matcher.matches()`, not `find()`) -- ported here as
`re.fullmatch`, not `re.match`/`re.search`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `s` | `str` |  | The raw score string, e.g. `"55-68"`. |

**Returns**

`(scored, allowed)` as a tuple of ints, or `(0, 0)` if `s` doesn't fully match `([0-9]+)-([0-9]+)`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import score_to_tuple

score_to_tuple("55-68")   # (55, 68)
score_to_tuple("garbage")  # (0, 0)
```

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### `select_contains(root: 'Tag', selector: 'str', text: 'str') -> 'list[Tag]'` {#select_contains}

JSoup `root.select(sel + ":contains(text)")`: candidates whose full

text (own + every descendant's) case-insensitively CONTAINS `text` as
a plain substring -- **not** a regex (Task 5e.2 addition; see the module
docstring's "Critical divergence" note).

JSoup's `:contains()` is documented case-insensitive substring
containment; soupsieve's `:-soup-contains()` (the non-deprecated
spelling of its `:contains()`) is case-SENSITIVE, with no
case-insensitive variant of its own. Reproducing JSoup's actual
semantics therefore needs this helper rather than `:-soup-contains()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector for the structural part of the match (everything before `:contains`). |
| `text` | `str` |  | The plain substring each candidate's collapsed text must case-insensitively contain. |

**Returns**

Every `selector` match whose `jsoup_text` case-insensitively contains `text`, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_contains
soup = parse_html("<td>game date:</td><td>Location:</td>")
select_contains(soup, "td", "Game Date:")  # [<td>game date:</td>]
```

### `select_matching(root: 'Tag', selector: 'str', regex: 'str') -> 'list[Tag]'` {#select_matching}

JSoup `root.select(sel + ":matches(regex)")`: candidates whose full

text (own + every descendant's) matches `regex`.

Soupsieve has no `:matches()` pseudo-class equivalent, so this runs the
plain structural `selector` first, then filters by `re.search`
over each candidate's `jsoup_text` (own text plus descendants',
matching JSoup's `:matches()` semantics -- as opposed to
`select_matching_own`'s own-text-only `:matchesOwn()`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector. |
| `regex` | `str` |  | The pattern each candidate's collapsed text must `re.search`-match. |

**Returns**

Every `selector` match whose `jsoup_text` contains a `regex` match, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching
soup = parse_html("<div><p>Home Team</p><p>Away Team</p></div>")
select_matching(soup, "p", r"^Home")  # [<p>Home Team</p>]
```

### `select_matching_own(root: 'Tag', selector: 'str', regex: 'str') -> 'list[Tag]'` {#select_matching_own}

JSoup `root.select(sel + ":matchesOwn(regex)")`: candidates whose

OWN text only (excluding descendant elements' text) matches `regex`.

JSoup's `Element.ownText()` walks only the element's direct
`TextNode` children, not text nested inside child elements -- the
same distinction bs4 draws between a tag's direct
`bs4.NavigableString` children and its full `.get_text()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector. |
| `regex` | `str` |  | The pattern each candidate's own (whitespace-collapsed) text must `re.search`-match. |

**Returns**

Every `selector` match whose own text contains a `regex` match, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching_own
soup = parse_html('<div class="card-header">Coach <b>Info</b></div>')
select_matching_own(soup, "div.card-header", r"^Coach")
# [<div class="card-header">Coach <b>Info</b></div>]
```

### `shot_js_to_html(js: 'str') -> 'list[Tag]'` {#shot_js_to_html}

Converts client-side `addShot(...)` JS calls into parseable

`circle.shot` HTML, for pages where the shot map is built on the fly
rather than baked into the initial HTML (`ShotEventParser
.shot_js_to_html`, `:266-283`). See the module docstring's "Scala
idiom decision" note -- the Scala's `builders`/`browser` parameters
are dropped here since the Scala body never actually uses them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `js` | `str` |  | The concatenated `<script>` text containing one or more `addShot(x, y, ..., 'title', ...)` calls, one per line. |

**Returns**

The `circle.shot` elements reconstructed from every matching line (non-matching lines, e.g. the `addShot` function definition line itself, are silently skipped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import shot_js_to_html
js = "addShot(27.0, 77.0, 392, false, 1, 'title text', 'class', false);"
circles = shot_js_to_html(js)
```

### `shot_value(event_str: 'str') -> 'int'` {#shot_value}

Classify a play-by-play event string as a 3, a 2, or an assist

(`ShotEnrichmentUtils.shot_value`, `PlayByPlayUtils.scala:534-542`).

Ported as an ordered first-match cascade, exactly mirroring the Scala
`match` arm order (assist is tested first, so an assist string never
falls through to a shot classifier).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event_str` | `str` |  | The raw play-by-play event string. |

**Returns**

`0` for an assist, `3` for any 3-pointer (made or missed), `2` for any 2-pointer (made or missed), or `-1` for anything else (rebounds, turnovers, unparseable, ...).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import shot_value
shot_value("18:28:00,0-0,Eric Ayala, 3pt jumpshot made")   # 3
shot_value("18:28:00,0-0,Kyle Guy, assist")                # 0
shot_value("04:28:0,52-59,Team, rebound deadballdeadball")  # -1
```

### `simulate_game(home_em: 'float', away_em: 'float', neutral: 'bool', rng: 'np.random.Generator') -> 'bool'` {#simulate_game}

Sample one women's game outcome (women's sigma/HFA/em_scale).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_em` | `float` |  |  |
| `away_em` | `float` |  |  |
| `neutral` | `bool` |  |  |
| `rng` | `Generator` |  |  |

**Example**

```python
import numpy as np
from sportsdataverse.wbb.wbb_season_sim import simulate_game
simulate_game(20.0, 5.0, False, np.random.default_rng(0))
```

### `slow_regression(player_weight_matrix: 'NDArray[np.float64]', ridge_lambda: 'float', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'` {#slow_regression}

Build the Tikhonov (ridge) regression solver matrix.

Faithful port of the private `RapmUtils.slowRegression`
(`RapmUtils.ts:756-769`): `(XᵀX + ridge_lambda·I)⁻¹Xᵀ`, where `X`
is `player_weight_matrix` (one row per lineup, one column per player --
see `calc_player_weights`). See the section banner above for why
this is a plain matrix inverse (`numpy.linalg.inv`), not an SVD.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, shape `(num_lineups, ctx["num_players"])`. |
| `ridge_lambda` | `float` |  | The Tikhonov regularization strength. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` -- only `ctx["num_players"]` is read (sizes the identity matrix). |

**Returns**

The `(num_players, num_lineups)` solver matrix; apply it to a target vector via `calculate_rapm`.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_rapm import slow_regression, calculate_rapm

x = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
solver = slow_regression(x, 1.0, ctx)  # ctx["num_players"] == 2
rapm = calculate_rapm(solver, [1.0, 2.0, 3.0])
```

### `spearman_corr(a: 'np.ndarray', b: 'np.ndarray') -> 'float'` {#spearman_corr}

Spearman rank correlation between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array. |
| `b` | `ndarray` |  | Second array (same length as `a`). |

**Returns**

The Spearman rank-correlation coefficient in `[-1, 1]`.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_prediction_constants import spearman_corr
spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
```

### `start_time_from_period(period: 'int', is_women_game: 'bool') -> 'float'` {#start_time_from_period}

The game-clock time (minutes elapsed) a period starts at

(`ExtractorUtils.scala:272-281`).

Women's games play four 10-minute quarters then 5-minute overtimes; men's
games play two 20-minute halves then 5-minute overtimes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `period` | `int` |  | The 1-indexed period number (1/2 = halves for men, 1-4 = quarters for women, 5+ = overtimes for both). |
| `is_women_game` | `bool` |  | Whether to use the women's (quarters) or men's (halves) period schedule. |

**Returns**

The game-clock minute the period begins at.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import start_time_from_period
start_time_from_period(2, is_women_game=False)  # 20.0 (men's 2nd half)
start_time_from_period(1, is_women_game=True)  # 0.0 (women's 1st quarter)
start_time_from_period(6, is_women_game=False)  # 45.0 (men's 2nd OT)
```

### `strength_of_schedule(results: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'` {#strength_of_schedule}

Per-team SoS + Quad 1-4 record + WAB from completed games and ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Completed games with `game_id, season, home_team_id, away_team_id, home_score, away_score, neutral_site`. |
| `ratings` | `DataFrame` |  | One row per team with `season, team_id, adj_em, rank` (the `mbb_team_ratings` output). Team-id dtype must match `results`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (quad thresholds, HFA, bubble EM). |

**Returns**

One row per (season, team_id): `season, team_id, sos, sos_rank, wab, quad1_w .. quad4_l, quality_wins`. `sos` is the mean opponent `adj_em` (rank 1 = hardest schedule); quads follow the NET venue-adjusted opponent-rank thresholds; `quality_wins` is Quad-1 + Quad-2 wins; `wab` is actual wins minus a bubble-quality team's expected wins against the same schedule. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule
resume = strength_of_schedule(results, ratings)
```

### `sum_event_stats(lhs: 'LineupEventStats', rhs: 'LineupEventStats') -> 'LineupEventStats'` {#sum_event_stats}

Field-wise add two :class:`~sportsdataverse.mbb.mbb_ncaa_models

.LineupEventStats` (`protected def sum_event_stats`, `LineupUtils.scala
:1534-1622`, debug-only -- the Scala's own docstring says "just used for
debug"). The Scala builds this via `shapeless.Generic` field-zipping;
this port is an explicit field-by-field call since Python has no
equivalent generic-programming machinery.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lhs` | `LineupEventStats` |  | The left-hand stat tree. |
| `rhs` | `LineupEventStats` |  | The right-hand stat tree. |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats` with every field summed (see the module's private sum_*` helpers for the `Optional`/nested-field summing rules).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_event_stats
from sportsdataverse.mbb.mbb_ncaa_models import LineupEventStats

sum_event_stats(LineupEventStats.empty(), LineupEventStats.empty()).num_events
```

### `sum_shot_infos(shot_infos: 'list[PlayerShotInfo]') -> 'Optional[PlayerShotInfo]'` {#sum_shot_infos}

Field-wise sum a list of :class:`~sportsdataverse.mbb.mbb_ncaa_models

.PlayerShotInfo`\ s (`sum_shot_infos`, `LineupUtils.scala:1625-1655`,
debug-only).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot_infos` | `list[PlayerShotInfo]` |  | The list to combine, in order. |

**Returns**

`None` if `shot_infos` is empty; the single element if there's exactly one; otherwise a left-fold of pairwise field-wise sums (`reduceOption`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_shot_infos
from sportsdataverse.mbb.mbb_ncaa_models import PlayerShotInfo

sum_shot_infos([PlayerShotInfo(ast_3pm=(1, 0, 0, 0, 0)), PlayerShotInfo(ast_3pm=(0, 1, 0, 0, 0))])
```

### `td_at(row: 'Tag', n: 'int') -> 'Optional[Tag]'` {#td_at}

JSoup `row >?> element("td:eq(n)")`: the `n`-th `<td>` child.

Soupsieve has no `:eq()` positional pseudo-class (unlike JSoup), so
this is a plain 0-indexed lookup into `row.find_all("td")`, guarded
against an out-of-range index (JSoup's `>?>` returns `None` rather
than raising when the selector matches nothing).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `row` | `Tag` |  | The row (or other container) element to search. |
| `n` | `int` |  | The 0-indexed `<td>` position. |

**Returns**

The `n`-th `<td>` descendant, or `None` if `row` has fewer than `n + 1` of them.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, td_at
soup = parse_html("<tr><td>A</td><td>B</td></tr>")
row = soup.find("tr")
td_at(row, 1).get_text()  # "B"
td_at(row, 5)  # None
```

### `test_positional_aware_filter(sorted_to_test: 'list[dict[str, str]]', pve_frags: 'list[dict[str, Any]]', nve_frags: 'list[dict[str, Any]]') -> 'bool'` {#test_positional_aware_filter}

Check a positional-aware filter (from `build_positional_aware_filter`)

against a sorted (`order_lineup`-ordered) lineup array.

Faithful port of `PositionUtils.testPositionalAwareFilter`
(`PositionUtils.ts:831-858`). A fragment matches if any of its
position-restricted slots (or, when `pos` is empty, any slot at all)
has a `code`/`id` containing the fragment's filter text
(case-insensitive substring match). Every positive fragment must match
(vacuously true if there are none); no negative fragment may match
(vacuously true if there are none).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_to_test` | `list[dict[str, str]]` |  | The ordered lineup, each a `{"id": ..., "code": ...}` dict (as returned by `order_lineup`). |
| `pve_frags` | `list[dict[str, Any]]` |  | Positive-filter fragments (must ALL match). |
| `nve_frags` | `list[dict[str, Any]]` |  | Negative-filter fragments (NONE may match). |

**Returns**

Whether the lineup satisfies both the positive and negative filters.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import test_positional_aware_filter
lineup = [{"code": "AnCowan", "id": "Cowan, Anthony"}]
test_positional_aware_filter(lineup, [{"filter": "cowan", "pos": []}], [])
```

### `tidy_player(p_in: 'str', ctx: 'TidyPlayerContext') -> 'tuple[str, TidyPlayerContext]'` {#tidy_player}

Resolve a raw play-by-play name to its box-score full name, via an

ordered fallback chain (`LineupErrorAnalysisUtils.tidy_player`,
`:76-144`). Order is semantic -- ported as ordered first-non-`None`:

1. Cache hit (see the module docstring's asymmetric-cache note).
2. Exact box-score code match.
3. Unique truncated-code match (`TidyPlayerContext.alt_all_players_map`).
4. Double-barrel-surname strip retry (`"Smith-Jones"` -> `"Jones"`),
   recursing into this same function.
5. Initials (`convert_from_initials`).
6. Jersey number (`convert_from_digits`, against
   `ctx.box_lineup.players_out`).
7. Truncated-code + inserted-"j"-for-"junior" retry.
8. Fuzzy match (`fuzzy_box_match`) -- skipped (and normalized to
   `"Team"`) for the four team-stat-row sentinels.
9. Identity fallthrough (the input is returned unresolved; a later,
   out-of-scope validation pass is expected to reject it).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p_in` | `str` |  | The raw play-by-play name. |
| `ctx` | `TidyPlayerContext` |  | The lookup context (see `build_tidy_player_context`). |

**Returns**

`(resolved_name, updated_ctx)` -- `updated_ctx` carries the new cache entry.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context, tidy_player
ctx = build_tidy_player_context(box_lineup)
resolved_name, ctx = tidy_player("MITCHELL,M", ctx)
```

### `transfer_cohort(rosters: 'pl.DataFrame') -> 'pl.DataFrame'` {#transfer_cohort}

One row per transfer: same `player_id`, different `team_id` in

consecutive seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `DataFrame` |  | Frame with `player_id`, `team_id`, `season` (extra columns ignored; one row per player-season-team). |

**Returns**

Utf8, from_team_id:Utf8, to_team_id:Utf8, from_season:Int64, to_season:Int64`` -- a player transferring twice appears twice.

**Example**

```python
from sportsdataverse.mbb import mbb_box_bpm, transfer_cohort
bpm = mbb_box_bpm([2025, 2026]).filter(pl.col("min") >= 150)
moves = transfer_cohort(bpm.select("player_id", "team_id", "season"))
```

### `transform_shot_location(x: 'float', y: 'float', second_half_switch: 'bool', team_shooting_left_in_first_period: 'bool', is_offensive: 'bool') -> 'tuple[float, float, float, float]'` {#transform_shot_location}

Transforms a raw SVG pixel location into feet from the basket, always

oriented as if shooting towards the left goal (`ShotEventParser
.transform_shot_location`, `:588-620`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `x` | `float` |  | Raw SVG `cx` pixel coordinate. |
| `y` | `float` |  | Raw SVG `cy` pixel coordinate. |
| `second_half_switch` | `bool` |  | Whether this shot is in the "other" half of the game from `team_shooting_left_in_first_period` (each `False` factor below flips which side is treated as "left"). |
| `team_shooting_left_in_first_period` | `bool` |  | Whether the team under analysis shot towards the left goal in the first period (see `is_team_shooting_left_to_start`). |
| `is_offensive` | `bool` |  | Whether the team under analysis is shooting (an opponent shot flips the expected side again). |

**Returns**

`(x, y, alt_x, alt_y)` in feet -- the believed-correct location, then the alternative (mirror-image) location, both relative to the goal the shot is (believed to be) attacking.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import transform_shot_location
transform_shot_location(310.2, 235, False, False, True)
```

### `update_config(**kwargs: 'object') -> 'NcaaFetchConfig'` {#update_config}

Update the active config in place.

**Returns**

The (mutated) global config object.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import update_config
update_config(proxy_url="http://user:pass@1.2.3.4:8080")
```

### `using_roster_pos(pos_class: 'str', roster_pos: 'str | None') -> 'tuple[str, str | None]'` {#using_roster_pos}

Reconcile a stats-derived position class against roster metadata.

Faithful port of `PositionUtils.usingRosterPos` (`PositionUtils.ts:583-626`).
When the classifier landed on an "unsure" bucket (`"G?"`/`"F/C?"`),
roster info narrows it (a roster `"C"` always wins outright); otherwise
an obviously-wrong stats classification is compromised toward the
roster-implied side, gated by `pos_class_to_score` thresholds.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pos_class` | `str` |  | The stats-derived position class. |
| `roster_pos` | `str \| None` |  | The roster-reported position (`"G"`/`"F"`/`"C"`), or `None`/`""` when unknown. `if (rosterPos)` (ts:587) is a plain JS truthiness check on a string -- `""` and `None` behave identically (both mean "no correction"), so `if not roster_pos` is the faithful Python mirror, not an `is None` landmine. |

**Returns**

A `(position, info)` tuple. `info` is `None` when no correction/explanation applies (matches the TS `undefined`), else a human-readable note on why the position was adjusted.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import using_roster_pos
using_roster_pos("G?", "C")
```

### `validate_box_score(team: 'TeamId', lineup: 'list[str]') -> 'Union[list[PlayerCodeId], ParseError]'` {#validate_box_score}

Checks there are no duplicates in the lineup (``BoxscoreParser

.validate_box_score`, `:388-404``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamId` |  | The team the lineup belongs to (feeds `~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code`'s team-scoped misspelling corrections). |
| `lineup` | `list[str]` |  | The raw player-name strings, in whatever order they were assembled by `inject_validated_players`. |

**Returns**

`lineup`, mapped to `~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId` (same order, no sort -- see the module docstring's "not sorted" note), or a `~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` if two DIFFERENT names collide on the same player code.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import validate_box_score
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
validate_box_score(TeamId("Team"), ["Player One", "Player Two"])
```

### `validate_lineup(lineup_event: 'LineupEvent', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'list[ValidationError]'` {#validate_lineup}

Flags a lineup stint as internally inconsistent, via 3 independent

checks (`LineupErrorAnalysisUtils.validate_lineup`, `:181-218`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_event` | `LineupEvent` |  | The lineup stint to validate. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (`players` is the full roster) -- used both to build the name-resolution context (see `~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context`) and, indirectly, as the source of `players_out` for jersey-number resolution inside `~sportsdataverse.mbb .mbb_ncaa_names.tidy_player`. |
| `valid_player_codes` | `set[str]` |  | Every player code that's actually on the box score / roster for this team-season. |

**Returns**

The failing `ValidationError`\ s, in declaration order (see the module docstring's "Return shape" note) -- empty if `lineup_event` is clean. * `ValidationError.WRONG_NUMBER_OF_PLAYERS` -- `lineup_event` doesn't have exactly 5 players on the floor. * `ValidationError.UNKNOWN_PLAYERS` -- some player on the floor isn't in `valid_player_codes`. * `ValidationError.INACTIVE_PLAYERS` -- some player mentioned in `lineup_event`'s own (team-side) raw game events resolves to a code not in `valid_player_codes` (i.e. isn't on the floor, per the lineup being validated).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import validate_lineup
errors = validate_lineup(lineup_event, box_lineup, {"MiMitchell", "BbBob"})
assert not errors  # a clean lineup returns []
```

### `wbb_bracket_sim(seeded_field: 'pl.DataFrame', ratings: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wbb_bracket_sim}

Women's single-elimination bracket Monte Carlo.

Delegates to `sportsdataverse.mbb.mbb_season_sim.mbb_bracket_sim` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seeded_field` | `DataFrame` |  | Bracket-ordered rows with `team_id` (adjacent rows meet in round 1). |
| `ratings` | `DataFrame` |  | One row per team (`team_id, adj_em`). |
| `n_sims` | `int` | `10000` | Number of simulated brackets. |
| `seed` | `int` | `0` | RNG seed (deterministic output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per field team: `team_id, seed?, reach_r32 .. champion` probabilities -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_bracket_sim
odds = wbb_bracket_sim(field_64, ratings, n_sims=20000, seed=42)
```

### `wbb_bracketology(season: 'int', *, as_of_date: 'Union[datetime.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wbb_bracketology}

Women's projected tournament field for a season.

Delegates to `sportsdataverse.mbb.mbb_bracketology.mbb_bracketology` with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season to project (e.g. `2024`). |
| `as_of_date` | `Union[date, None]` | `None` | Only use games strictly before this date; `None` uses every completed game. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, resume_score, projected_seed, at_large_prob, auto_bid, bid` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_bracketology
field = wbb_bracketology(2024)
```

### `wbb_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wbb_in_game_win_prob}

Women's per-play in-game win probability (bundled `wbb_in_game_wp.ubj`).

Delegates to `sportsdataverse.mbb.mbb_game_predict.mbb_in_game_win_prob` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game's plays in the `load_wbb_pbp` schema. |
| `pregame_home_prob` | `float` |  | Pregame home win probability. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the five feature columns plus `home_win_prob` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_in_game_win_prob
wp = wbb_in_game_win_prob(pbp, 0.62)
```

### `wbb_pbp_disk(game_id, path_to_json)` {#wbb_pbp_disk}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### `wbb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wbb_predict_games}

Women's vectorized pregame predictions over a schedule.

Delegates to `sportsdataverse.mbb.mbb_game_predict.mbb_predict_games` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game (`game_id, home_team_id, away_team_id` and optionally `neutral_site`). |
| `ratings` | `DataFrame` |  | One row per team (the `wbb_team_ratings` output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_predict_games, wbb_team_ratings
preds = wbb_predict_games(games, wbb_team_ratings(2024))
```

### `wbb_season_sim(ratings: 'pl.DataFrame', remaining_schedule: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wbb_season_sim}

Women's remaining-schedule Monte Carlo.

Delegates to `sportsdataverse.mbb.mbb_season_sim.mbb_season_sim` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | One row per team (`season, team_id, adj_em` + optional `conference` / `current_wins`). |
| `remaining_schedule` | `DataFrame` |  | Games to simulate. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `seed` | `int` | `0` | RNG seed (deterministic output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, exp_wins, playoff_prob, conf_title_prob` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_season_sim
odds = wbb_season_sim(ratings, remaining, n_sims=5000, seed=42)
```

### `wbb_strength_of_schedule(seasons: 'list[int]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wbb_strength_of_schedule}

Women's season-level SoS / Quad / WAB résumé.

Delegates to `sportsdataverse.mbb.mbb_strength_of_schedule.mbb_strength_of_schedule` with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to compute (e.g. `[2024]`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (season, team_id): `season, team_id, sos, sos_rank, wab, quad1_w .. quad4_l, quality_wins` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_strength_of_schedule
wbb_strength_of_schedule([2024]).sort("wab", descending=True).head(20)
```

### `wbb_team_ratings(seasons: 'Union[int, list[int]]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wbb_team_ratings}

Women's opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo).

Delegates to `sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings`
with `league="womens"` (WBB loaders + women's fitted constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2024`) or list of seasons. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id) -- see the mbb core for the schema.

**Example**

```python
from sportsdataverse.wbb import wbb_team_ratings
wbb_team_ratings(2024).sort("rank").head()
```

### `weighted_avg(mutable_acc: 'LineupStatSet', obj: 'LineupStatSet') -> 'None'` {#weighted_avg}

Merge `obj` into `mutable_acc` with possession weighting.

Faithful port of `LineupUtils.weightedAvg` (`LineupUtils.ts:645`).
Mutates `mutable_acc` in place (matching the upstream mutable-state
contract) and returns `None`. Each call accumulates a **weighted
sum**, not a weighted average -- the companion `completeWeightedAvg`
(upstream `LineupUtils.ts:752`, not yet ported) divides by the
accumulated weight totals to finish the average. The per-field weight
used at each merge step is derived from `obj`'s *own* totals (e.g.
that single lineup's `total_off_fga`), not from any running total on
`mutable_acc` -- callers accumulating many lineups must call
`weighted_avg` once per lineup so every lineup contributes its own
weight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_acc` | `LineupStatSet` |  | The running accumulator (`LineupStatSet`). Mutated in place; fields absent from the accumulator are initialized to `{"value": 0.0}` (plus `old_value` / `override` when `obj`'s field carries a luck-adjustment `override` marker) before `obj`'s contribution is added. |
| `obj` | `LineupStatSet` |  | The per-lineup `LineupStatSet` document to merge in. |

**Returns**

None. `mutable_acc` is mutated in place.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg

acc: dict = {}
weighted_avg(acc, lineup_a)
weighted_avg(acc, lineup_b)
print(acc["off_poss"]["value"])  # plain sum (SUM_FIELDS)

# Two-lineup possession-weighted merge

acc = {}
for lineup in three_lineups:
    weighted_avg(acc, lineup)
# acc now holds weighted SUMS; complete_weighted_avg (not yet
# ported) is required to turn these into rate-stat averages.
```

### `win_prob_from_margin(exp_margin: 'float') -> 'float'` {#win_prob_from_margin}

Women's home win probability from an expected margin.

Delegates to `sportsdataverse.mbb.mbb_game_predict.win_prob_from_margin` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.wbb.wbb_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
```
