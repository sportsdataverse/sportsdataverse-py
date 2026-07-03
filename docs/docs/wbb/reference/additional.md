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

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

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

### `wbb_pbp_disk(game_id, path_to_json)` {#wbb_pbp_disk}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

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
