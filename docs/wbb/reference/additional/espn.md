# Women's college basketball — additional Python functions — ESPN

> Women's college basketball — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package.

### espn_wbb_calendar {#espn_wbb_calendar}

`espn_wbb_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_calendar - look up the women's college basketball calendar for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `None` | Used to define different seasons. 2001 is the earliest available season; `None` means the current season. |
| `ondays` | `boolean` | `None` | Used to return dates for calendar ondays |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing calendar dates for the requested season.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `datetime` | String | The calendar day as ESPN sends it, an ISO 8601 UTC timestamp string (e.g. 2024-11-04T08:00Z). |
| `date` | String | Date in YYYY-MM-DD format. |
| `year` | String | 4-digit year. |
| `month` | String | Month of the calendar day as a two-digit string, e.g. 11. |
| `day` | String | Day number within the month. |
| `dateURL` | String | The calendar day as YYYYMMDD, the dates= value of that day's scoreboard request. |
| `url` | String | RotoWire player page URL. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If `season` is less than 2001. |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_calendar
cal = espn_wbb_calendar(season=2024)
cal.head()

# On-days only (dates with games on the schedule)

ondays = espn_wbb_calendar(season=2024, ondays=True)
ondays.head()

# Pandas round-trip

cal_pd = espn_wbb_calendar(season=2024, return_as_pandas=True)
cal_pd.head()
```

### espn_wbb_game_officials {#espn_wbb_game_officials}

`espn_wbb_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

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
| `game_id` | Int64 | Unique game identifier. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `official_id` | String | Unique official / referee identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `position_id` | String | Unique position identifier. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | String | Position display name. |
| `order` | Int64 | Display order within the result set. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `requests.exceptions.RequestException` | Other network failures after retries. |

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

### espn_wbb_game_rosters {#espn_wbb_game_rosters}

`espn_wbb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame | pd.DataFrame | dict'`

espn_wbb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from wbb_schedule(). |
| `raw` | `bool` | `False` | If True, returns the Core v2 competitors payload (dict) as ESPN sent it; its roster, team and athlete `$ref` links are not followed. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id' plus the athlete's `position_id`, `position_name`, `position_display_name`, `position_abbreviation` and `position_leaf`. With `raw=True`, the competitors payload (dict).

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `athlete_display_name` | String | Athlete display name (full). |
| `short_name` | String | Short display name. |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `slug` | String | URL-safe identifier. |
| `jersey` | String | Jersey number worn by the player. |
| `linked` | Boolean | TRUE if the record is linked to a related entity. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `alternate_ids_sdr` | String |  |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `birth_place_country` | String | Birth place country. |
| `birth_country_alternate_id` | Int64 | Alternate identifier for the athlete's country of birth used in ESPN's country-flag system. |
| `birth_country_abbreviation` | String |  |
| `headshot_href` | String | Headshot image URL. |
| `headshot_alt` | String | Alternative-text label for the headshot. |
| `flag_href` | String | URL of the SVG or PNG flag image representing the athlete's country of birth. |
| `flag_alt` | String | Alt-text string for the athlete's country-of-birth flag image, typically the country name. |
| `flag_rel` | String | Relationship descriptor for the athlete's country-of-birth flag link (e.g., "flag"). |
| `experience_years` | Int64 | Experience years. |
| `experience_display_value` | String | Experience display value. |
| `experience_abbreviation` | String | Experience abbreviation. |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status label. |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `position_id` | Int64 | ESPN's numeric identifier of the athlete's listed position (the Core v2 position resource id). |
| `position_name` | String | Name of the athlete's listed position from ESPN's position resource, e.g. Guard or Forward. |
| `position_display_name` | String | Display form of the athlete's listed position as ESPN renders it; usually equal to position_name. |
| `position_abbreviation` | String | Short code of the athlete's listed position (e.g. G, F, RW, OL), from ESPN's position resource. |
| `position_leaf` | Boolean | Whether the position is a leaf of ESPN's position tree; False marks a grouping, not a specific role. |
| `hand_type` | String | Hand type. |
| `hand_abbreviation` | String | Hand abbreviation. |
| `hand_display_value` | String | Hand display value. |
| `age` | Int64 | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `jersey_right` | String | Jersey number displayed on the right side of the roster card, used for alternate or secondary number representations. |
| `valid` | Boolean | Valid. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `display_name` | String | Display name. |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `athlete_href` | String | ESPN API resource URL for the athlete's full profile endpoint. |
| `position_href` | String | ESPN API resource URL for the athlete's position resource. |
| `statistics_href` | String | ESPN API resource URL pointing to the athlete's statistics endpoint. |
| `team_id` | Int64 | Unique team identifier. |
| `order` | Int64 | Display order within the result set. |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `winner` | Boolean | Winner. |
| `team_guid` | String | ESPN team GUID. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | String | Team nickname. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `is_active` | Boolean | Whether the team was active in this season. |
| `is_all_star` | Boolean | Is all star. |
| `team_alternate_ids_sdr` | String | Alternate team identifier from ESPN's SDR (Sports Data Reference) system for the athlete's team. |
| `logo_href` | String | Team or league logo URL. |
| `logo_dark_href` | String | Logo URL for dark backgrounds. |
| `game_id` | Int64 | Unique game identifier. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN has no competitors, roster or athlete resource for the game (404), or every rostered athlete's `$ref` answered 404. A single 404 athlete is left out with a `UserWarning` and the rest of the roster is returned. |
| `AssetFetchError` | A fetch failed (403, 429, 5xx, a dropped connection); never treated as a missing athlete. |

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

### espn_wbb_pbp {#espn_wbb_pbp}

`espn_wbb_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

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

### espn_wbb_player_stats {#espn_wbb_player_stats}

`espn_wbb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

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
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `total` | Boolean | Total. |
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `weight` | String | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `age` | String | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `jersey` | String | Jersey number worn by the player. |
| `slug` | String | URL-safe identifier. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `position_id` | Int64 | Unique position identifier. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | String | Position display name. |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `college_name` | String | College name. |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status label. |
| `defensive_blocks` | Float64 | Short for blocked shot, number of times when a defensive player legally deflects a field goal attempt from an offensive player. |
| `defensive_defensive_rebounds` | Float64 | The number of times when the defense obtains the possession of the ball after a missed shot by the offense. |
| `defensive_steals` | Float64 | The number of times a defensive player forced a turnover by intercepting or deflecting a pass or a dribble of an offensive player. |
| `defensive_turnover_points` | Float64 | The amount of points resulting from the possession following a turnover. |
| `defensive_avg_defensive_rebounds` | Float64 | The average defensive rebounds per game. |
| `defensive_avg_blocks` | Float64 | The average blocks per game. |
| `defensive_avg_steals` | Float64 | The average steals per game. |
| `general_disqualifications` | Float64 | The number of times a player reached the foul limit. |
| `general_flagrant_fouls` | Float64 | The number of fouls that the officials thought were unnecessary or excessive. |
| `general_fouls` | Float64 | The number of times a player had illegal contact with the opponent. |
| `general_per` | Float64 | A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_ejections` | Float64 | The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_technical_fouls` | Float64 | The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_rebounds` | Float64 | The total number of rebounds (offensive and defensive). |
| `general_minutes` | Float64 | The total number of minutes played. |
| `general_avg_minutes` | Float64 | The average number of minutes per game. |
| `general_fantasy_rating` | Float64 | The Fantasy Rating of a player. |
| `general_plus_minus` | Float64 | A player's estimated on-court impact on team performance measured in point differential per 100 possessions. |
| `general_avg_rebounds` | Float64 | The average rebounds per game. |
| `general_avg_fouls` | Float64 | The average fouls committed per game. |
| `general_avg_flagrant_fouls` | Float64 | The average number of flagrant fouls per game. |
| `general_avg_technical_fouls` | Float64 | The average number of technical fouls per game. |
| `general_avg_ejections` | Float64 | The average ejections per game. |
| `general_avg_disqualifications` | Float64 | The average number of disqualifications per game. |
| `general_assist_turnover_ratio` | Float64 | The average number of assists a player or team records per turnover. |
| `general_steal_foul_ratio` | Float64 | The average number of steals a player or team records per foul committed. |
| `general_block_foul_ratio` | Float64 | The average number of blocks a player or record per foul committed. |
| `general_avg_team_rebounds` | Float64 | The average number of rebounds for a team per game. |
| `general_total_rebounds` | Float64 | The total number of rebounds for a team or player. |
| `general_total_technical_fouls` | Float64 | The total number of technical fouls for a team or player. |
| `general_steal_turnover_ratio` | Float64 | The number of steals per turnover. |
| `general_games_played` | Float64 | Games Played. |
| `general_games_started` | Float64 | The number of games started by an athlete. |
| `general_double_double` | Float64 | The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | Float64 | The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `offensive_assists` | Float64 | The number of times a player who passes the ball to a teammate in a way that leads to a score by field goal, meaning that he or she was "assisting" in the basket. There is some judgment involved in deciding whether a passer should be credited with an assist. |
| `offensive_field_goals` | Float64 | Field Goal makes and attempts. |
| `offensive_field_goals_attempted` | Float64 | The number of times a 2pt field goal was attempted. |
| `offensive_field_goals_made` | Float64 | The number of times a 2pt field goal was made. |
| `offensive_field_goal_pct` | Float64 | The ratio of field goals made to field goals attempted: FGM / FGA. |
| `offensive_free_throws` | Float64 | Free Throw makes and attempts. |
| `offensive_free_throw_pct` | Float64 | The ratio of free throws made to free throws attempted: FTM / FTA. |
| `offensive_free_throws_attempted` | Float64 | The number of times a free throw was attempted. |
| `offensive_free_throws_made` | Float64 | The number of times a free throw was made. |
| `offensive_offensive_rebounds` | Float64 | The number of times when the offense obtains the possession of the ball after a missed shot. |
| `offensive_points` | Float64 | The number of points scored. |
| `offensive_turnovers` | Float64 | The number of times a player loses possession to the other team. |
| `offensive_three_point_field_goals_attempted` | Float64 | The number of times a 3pt field goal was attempted. |
| `offensive_three_point_field_goals_made` | Float64 | The number of times a 3pt field goal was made. |
| `offensive_total_turnovers` | Float64 | The number of turnovers plus team turnovers for the team. |
| `offensive_points_in_paint` | Float64 | The amount of points scored in the area known as "the Paint"(the rectangle between the foul line and the baseline). |
| `offensive_second_chance_points` | Float64 | Points scored by the player on offensive-rebound put-back opportunities during the season. |
| `offensive_fast_break_points` | Float64 | The number of points scored on fast breaks. |
| `offensive_avg_field_goals_made` | Float64 | The average field goals made per game. |
| `offensive_avg_field_goals_attempted` | Float64 | The average field goals attempted per game. |
| `offensive_avg_three_point_field_goals_made` | Float64 | The average three point field goals made per game. |
| `offensive_avg_three_point_field_goals_attempted` | Float64 | The average three point field goals attempted per game. |
| `offensive_avg_free_throws_made` | Float64 | The average free throw shots made per game. |
| `offensive_avg_free_throws_attempted` | Float64 | The average free throw shots attempted per game. |
| `offensive_avg_points` | Float64 | The average number of points scored per game. |
| `offensive_avg_offensive_rebounds` | Float64 | The average offensive rebounds per game. |
| `offensive_avg_assists` | Float64 | The average assists per game. |
| `offensive_avg_turnovers` | Float64 | The average turnovers committed per game. |
| `offensive_offensive_rebound_pct` | Float64 | The percentage of the number of times they obtain the possession of the ball after a missed shot. |
| `offensive_estimated_possessions` | Float64 | An estimation of the number of possessions for a team or player. |
| `offensive_avg_estimated_possessions` | Float64 | The average number of estimated possessions per game for a team or player. |
| `offensive_points_per_estimated_possessions` | Float64 | The number of points per estimated possession for a team or player. |
| `offensive_avg_team_turnovers` | Float64 | The average number of turnovers for a team per game. |
| `offensive_avg_total_turnovers` | Float64 | The average number of total turnovers for a team per game. |
| `offensive_three_point_field_goal_pct` | Float64 | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_two_point_field_goals_made` | Float64 | The number of 2-point field goals made for a team or player. |
| `offensive_two_point_field_goals_attempted` | Float64 | The number of 2-point field goals attempted for a team or player. |
| `offensive_avg_two_point_field_goals_made` | Float64 | The number of 2-point field goals made per game for a team or player. |
| `offensive_avg_two_point_field_goals_attempted` | Float64 | The number of 2-point field goals attempted per game for a team or player. |
| `offensive_two_point_field_goal_pct` | Float64 | The percentage of 2-points fields goals made by a team or player. |
| `offensive_shooting_efficiency` | Float64 | The efficiency with which a team or player shoots the basketball. |
| `offensive_scoring_efficiency` | Float64 | The efficiency with which a team or player scores the basketball. |
| `team_id` | Int64 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_guid` | String | ESPN team GUID. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_is_active` | Boolean | TRUE if the team is currently active. |
| `team_logo_href` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season_type` is not `"regular"`/`"postseason"`. |
| `sportsdataverse.errors.NoDataError` | ESPN returned 404 (no season line for that athlete/season). |
| `sportsdataverse.errors.AssetFetchError` | The statistics, athlete or team fetch failed (403, 429, 5xx, a dead connection); only a 404 on the athlete or team node leaves those columns null. |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_player_stats
df = espn_wbb_player_stats(athlete_id=4433985, season=2025)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### espn_wbb_schedule {#espn_wbb_schedule}

`espn_wbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_schedule - look up the women's college basketball schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2001 is the earliest available season. |
| `groups` | `int` | `50` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. A day with no games returns a zero-row frame carrying the documented columns.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique play identification number |
| `uid` | String | ESPN UID string. |
| `date` | String | Date in YYYY-MM-DD format. |
| `attendance` | Int64 | Reported attendance. |
| `time_valid` | Boolean | Time valid. |
| `neutral_site` | Boolean | Neutral site. |
| `conference_competition` | Boolean | Conference competition. |
| `play_by_play_available` | Boolean |  |
| `recent` | Boolean | Recent. |
| `tournament_id` | Int64 | ESPN tournament identifier. |
| `start_date` | String | Start date (YYYY-MM-DD). |
| `broadcast` | String | Broadcast information string. |
| `highlights` | String |  |
| `notes_type` | String | Notes type. |
| `notes_headline` | String | Notes headline. |
| `broadcast_market` | String | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | String | Broadcast name. |
| `type_id` | Int64 | Type identifier (numeric). |
| `type_abbreviation` | String | Play type abbreviation |
| `venue_id` | Int64 | Unique venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_indoor` | Boolean | TRUE if the venue is indoors. |
| `status_clock` | Float64 | Status clock. |
| `status_display_clock` | String | Status display clock. |
| `status_period` | Int64 | Status period. |
| `status_type_id` | Int64 | Unique identifier for status type. |
| `status_type_name` | String | Status type name. |
| `status_type_state` | String | Status type state. |
| `status_type_completed` | Boolean | Status type completed. |
| `status_type_description` | String | Status type description. |
| `status_type_detail` | String | Status type detail. |
| `status_type_short_detail` | String | Status type short detail. |
| `format_regulation_periods` | Int64 | Format regulation periods. |
| `home_id` | Int64 | Unique identifier for home. |
| `home_uid` | String | Home team's uid. |
| `home_location` | String | Home team's location. |
| `home_name` | String | Home name. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home display name. |
| `home_short_display_name` | String | Home short display name. |
| `home_color` | String | Color code (hex) for home. |
| `home_alternate_color` | String | Color code (hex) for home alternate. |
| `home_is_active` | Boolean | Home team's is active. |
| `home_venue_id` | Int64 | Unique identifier for home venue. |
| `home_logo` | String | Home team logo URL. |
| `home_conference_id` | Int64 | Unique identifier for home conference. |
| `home_score` | String | Home team score at the time of the play. |
| `home_winner` | Boolean | Home team's winner. |
| `home_current_rank` | Int64 | Current AP/coaches poll ranking of the home team at the time of the game. |
| `home_linescores` | List | Points scored by the home team in each period or half of the game. |
| `home_records` | String | Win-loss record string for the home team at the time of the game. |
| `away_id` | Int64 | Unique identifier for away. |
| `away_uid` | String | Away team's uid. |
| `away_location` | String | Away team's location. |
| `away_name` | String | Away name. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away display name. |
| `away_short_display_name` | String | Away short display name. |
| `away_color` | String | Color code (hex) for away. |
| `away_alternate_color` | String | Color code (hex) for away alternate. |
| `away_is_active` | Boolean | Away team's is active. |
| `away_venue_id` | Int64 | Unique identifier for away venue. |
| `away_logo` | String | Away team logo URL. |
| `away_conference_id` | Int64 | Unique identifier for away conference. |
| `away_score` | String | Away team score at the time of the play. |
| `away_winner` | Boolean | Away team's winner. |
| `away_current_rank` | Int64 | Current AP/coaches poll ranking of the away team at the time of the game. |
| `away_linescores` | List | Points scored by the away team in each period or half of the game. |
| `away_records` | String | Win-loss record string for the away team at the time of the game. |
| `game_id` | Int64 | Unique game identifier. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int64 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `home_logo_dark` | String |  |
| `away_logo_dark` | String |  |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_schedule
day = espn_wbb_schedule(dates=20240407)
print(day.shape)

# Season-level pull (2024 season). ``dates=2024`` is not a season sweep: ESPN answers a year with its first scoreboard page (early January only), so read the released schedule

from sportsdataverse.wbb import load_wbb_schedule
season = load_wbb_schedule([2024])
print(season.shape)

# Filter to a specific team (UConn ``team_id=2509``)

import polars as pl
uconn = season.filter(
    (pl.col("home_id") == 2509) | (pl.col("away_id") == 2509)
)

# Pandas round-trip

day_pd = espn_wbb_schedule(dates=20240407, return_as_pandas=True)
day_pd.head()
```

### espn_wbb_standings {#espn_wbb_standings}

`espn_wbb_standings(season: 'int', group: 'int' = 50, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull ESPN women's-college-basketball standings for a season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year, forwarded to ESPN as `?season=YYYY`. |
| `group` | `int` | `50` | ESPN `group` filter. `50` is NCAA Division I women's basketball (the default); `51` is Division II/III. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

Polars (or pandas) DataFrame with one row per team. Documented columns include `team_id`, `team_uid`, `team_slug`, `team_location`, `team_name`, `team_abbreviation`, `team_display_name`, `team_short_display_name`, `team_color`, `conference_id`, `wins`, `losses`, `win_percent`, `games_back`, `streak`, `points_for`, `points_against`, `point_differential`, `home_wins`, `home_losses`, `road_wins`, `road_losses`, `division_wins`, `division_losses`, `season`. If `raw=True`, returns the raw response dict.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `conference_id` | Int64 | Conference identifier. |
| `conference_name` | String | Full conference name. |
| `conference_abbreviation` | String | Abbreviation of the team's conference as ESPN's standings name it. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `win_percent` | Float64 | Win percent. |
| `games_back` | Float64 | Games behind the conference leader (0 for the leader). |
| `streak` | String | Current streak (e.g. 'W3' for three-game win streak). |
| `points_for` | Float64 | Points for. |
| `points_against` | Float64 | Points allowed. |
| `point_differential` | Float64 | Point differential. |
| `avg_points_for` | Float64 | Points the team scored per game. |
| `avg_points_against` | Float64 | Points the team allowed per game. |
| `home_wins` | Int64 | Home team's wins. |
| `home_losses` | Int64 | Home team's losses. |
| `road_wins` | Int64 | Road wins. |
| `road_losses` | Int64 | Road losses. |
| `division_wins` | Int64 | Wins against division opponents, as ESPN's standings report them. |
| `division_losses` | Int64 | Losses against division opponents, as ESPN's standings report them. |
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `requests.exceptions.RequestException` | Other network failures after retries. |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_standings
standings = espn_wbb_standings(season=2024, group=50)
print(standings.shape)
standings.select(
    ["team_display_name", "wins", "losses", "win_percent"]
).head(10)

# Top teams by win percentage

import polars as pl
top10 = standings.sort("win_percent", descending=True).head(10)

# Pandas round-trip + Division II/III

d2_d3 = espn_wbb_standings(
    season=2024, group=51, return_as_pandas=True
)
d2_d3.head()

# Raw payload (skip the cleaning pipeline)

raw = espn_wbb_standings(season=2024, group=50, raw=True)
sorted(raw.keys())
```

### espn_wbb_team_roster {#espn_wbb_team_roster}

`espn_wbb_team_roster(team_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull the current ESPN team roster for a women's-college-basketball team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | ESPN team identifier (e.g. `2509` for UConn). |
| `season` | `int \| None` | `None` | Season year. Recorded as the `season` column on the output; does NOT alter the request URL because ESPN's `/teams/{id}/roster` endpoint ignores `?season=YYYY`. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

Polars (or pandas) DataFrame with one row per athlete: `athlete_id`, `athlete_uid`, `first_name`, `last_name`, `full_name`, `display_name`, `short_name`, `jersey`, `position_id`, `position_name`, `position_abbreviation`, `height`, `display_height`, `weight`, `display_weight`, `age`, `date_of_birth`, `birth_city`, `birth_state`, `headshot_href`, `link_web`, `status_name`, `team_id`, `season`. If `raw=True`, returns the raw response dict.

| col_name | type | description |
|---|---|---|
| `athlete_id` | String | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `jersey` | String | Jersey number worn by the player. |
| `position_id` | String | Unique position identifier. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `weight` | String | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `age` | String | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `birth_city` | String | Birth city. |
| `birth_state` | String | Birth state / region. |
| `headshot_href` | String | Headshot image URL. |
| `link_web` | String | Web link / URL. |
| `status_name` | String | Status label. |
| `team_id` | Int64 | Unique team identifier. |
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `requests.exceptions.RequestException` | Other network failures after retries. |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_team_roster
roster = espn_wbb_team_roster(team_id=2509, season=2025)
print(roster.shape)
roster.select(["full_name", "jersey", "position_abbreviation"]).head()

# Pandas round-trip

roster_pd = espn_wbb_team_roster(team_id=2509, season=2025, return_as_pandas=True)
roster_pd.head()

# Pipeline next step - join with team metadata

from sportsdataverse.wbb import espn_wbb_teams
teams = espn_wbb_teams()
roster.join(
    teams.select(["team_id", "team_display_name"]),
    on="team_id",
    how="left",
)

# Raw payload (skip the cleaning pipeline)

raw = espn_wbb_team_roster(team_id=2509, season=2025, raw=True)
sorted(raw.keys())
```

### espn_wbb_team_stats {#espn_wbb_team_stats}

`espn_wbb_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'`

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

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `requests.exceptions.RequestException` | Other network failures after retries. |

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

### espn_wbb_teams {#espn_wbb_teams}

`espn_wbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_teams - look up the women's college basketball teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of the women's college basketball teams, one row per team, with the columns in the returns table. A payload without teams returns a zero-row frame with those columns. The result is not cached: every call fetches (through the package's opt-in TTL response cache, `sportsdataverse.cache`, when it is on).

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_display_name` | String | Full team display name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_is_active` | Boolean | TRUE if the team is currently active. |
| `team_is_all_star` | Boolean | TRUE if the row represents an All-Star team. |
| `team_location` | String | Team city or location string. |
| `team_logos` | Int64 |  |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | String | Team nickname. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN answered 404 for the teams resource. |
| `AssetFetchError` | The fetch failed (403, 429, 5xx, a dropped connection); never returned as an empty frame. |

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

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Flatten one ESPN scoreboard event for the schedule frame, in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  | One element of the scoreboard payload's `events` list. |

**Returns**

The same event, modified: `competitions[0]` gains `home` / `away` team dicts (with `score`, `winner`, `currentRank`, `linescores`, `records`), `notes_type` / `notes_headline` and `broadcast_market` / `broadcast_name`, and loses `competitors`, `broadcasts`, `notes`, `odds`, `leaders` and the other nested blocks the schedule frame does not use.
