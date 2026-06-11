---
title: NFL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NFL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nfl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nfl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nfl_game_rosters}

espn_nfl_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nfl_schedule(). |
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
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `full_name` | character | Full name as per NFL.com |
| `athlete_display_name` | character | Athlete display name (full). |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `weight` | double | Official weight, in pounds |
| `display_weight` | character | Player weight in display format (e.g. '180 lbs'). |
| `height` | double | Official height, in inches |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `debut_year` | integer | Year of professional debut. |
| `slug` | character | URL-safe identifier. |
| `jersey` | character | Jersey number worn by the player. |
| `linked` | logical | TRUE if the record is linked to a related entity. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `headshot_href` | character | Link to ESPN Headshot of Player |
| `headshot_alt` | character | Alternative-text label for the headshot. |
| `projections_href` | character |  |
| `contracts_href` | character |  |
| `experience_years` | integer | Experience years. |
| `college_athlete_href` | character |  |
| `contract_href` | character |  |
| `contract_option_type` | integer | Contract option type. |
| `contract_salary` | integer | Contract salary. |
| `contract_bonus` | integer |  |
| `contract_years_remaining` | integer | Contract years remaining. |
| `contract_signed_through` | integer |  |
| `contract_season_href` | character |  |
| `contract_team_href` | character |  |
| `contract_active` | logical | Contract active. |
| `status_id` | character | Status identifier. |
| `status_name` | character | Status label. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `contract_salary_remaining` | integer | Contract salary remaining. |
| `draft_display_text` | character | Draft display text. |
| `draft_round` | integer | Round that player was drafted in |
| `draft_year` | integer | Year that player was drafted |
| `draft_selection` | integer | Draft selection. |
| `draft_team_href` | character |  |
| `draft_pick_href` | character |  |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `starter` | logical | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `jersey_right` | character |  |
| `valid` | logical | Valid. |
| `did_not_play` | logical | TRUE if the player did not appear in the game. |
| `display_name` | character | Full name of player |
| `athlete_href` | character |  |
| `position_href` | character |  |
| `statistics_href` | character |  |
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
| `is_active` | logical | Active contract |
| `is_all_star` | logical | Is all star. |
| `team_alternate_ids_sdr` | character |  |
| `logo_href` | character | Team or league logo URL. |
| `logo_dark_href` | character | Logo URL for dark backgrounds. |
| `game_id` | integer | Ten digit identifier for NFL game. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_game_rosters
rosters = espn_nfl_game_rosters(game_id=401220403)
rosters.shape

# Pandas round-trip with home/away split

rosters_pd = espn_nfl_game_rosters(game_id=401220403, return_as_pandas=True)
home = rosters_pd[rosters_pd["home_away"] == "home"]
away = rosters_pd[rosters_pd["home_away"] == "away"]
```

### `espn_nfl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_nfl_player_stats}

Pull an NFL athlete's ESPN **season** stat line as one wide row.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns (for football: `passing_*`, `rushing_*`, `receiving_*`,
`scoring_*`, ...), the athlete / team metadata blocks, and the
`season_type` / `total` parameters. For the richer multi-category
web-v3 payload use `sportsdataverse.nfl.espn_nfl_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NFL athlete identifier (e.g. `3139477` for Patrick Mahomes). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `total` | logical | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `athlete_id` | integer | Unique athlete identifier (ESPN). |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `full_name` | character | Full name as per NFL.com |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `weight` | double | Official weight, in pounds |
| `display_weight` | character | Player weight in display format (e.g. '180 lbs'). |
| `height` | double | Official height, in inches |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `jersey` | character | Jersey number worn by the player. |
| `slug` | character | URL-safe identifier. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `position_id` | integer | Unique position identifier. |
| `position_name` | character | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation ('G' / 'F' / 'C'). |
| `college_name` | character | Official college (usually the last one attended) |
| `status_id` | integer | Status identifier. |
| `status_name` | character | Status label. |
| `general_fumbles` | double |  |
| `general_fumbles_lost` | double |  |
| `general_fumbles_forced` | double |  |
| `general_fumbles_forced_primary` | double |  |
| `general_fumbles_recovered` | double |  |
| `general_fumbles_recovered_yards` | double |  |
| `general_fumbles_touchdowns` | double |  |
| `general_games_played` | double | Games Played. |
| `general_offensive_two_pt_returns` | double |  |
| `general_offensive_fumbles_touchdowns` | double |  |
| `general_defensive_fumbles_touchdowns` | double |  |
| `passing_avg_gain` | double |  |
| `passing_completion_pct` | double |  |
| `passing_completions` | double | Pass completions (split from CFBD's `C/ATT` field). |
| `passing_espnqb_rating` | double |  |
| `passing_interception_pct` | double |  |
| `passing_interceptions` | double |  |
| `passing_long_passing` | double |  |
| `passing_net_passing_yards` | double |  |
| `passing_net_passing_yards_per_game` | double |  |
| `passing_net_total_yards` | double |  |
| `passing_net_yards_per_game` | double |  |
| `passing_passing_attempts` | double |  |
| `passing_passing_big_plays` | double |  |
| `passing_passing_first_downs` | double |  |
| `passing_passing_fumbles` | double |  |
| `passing_passing_fumbles_lost` | double |  |
| `passing_passing_touchdown_pct` | double |  |
| `passing_passing_touchdowns` | double |  |
| `passing_passing_yards` | double |  |
| `passing_passing_yards_after_catch` | double |  |
| `passing_passing_yards_at_catch` | double |  |
| `passing_passing_yards_per_game` | double |  |
| `passing_qb_rating` | double |  |
| `passing_sacks` | double |  |
| `passing_sack_yards_lost` | double |  |
| `passing_net_passing_attempts` | double |  |
| `passing_team_games_played` | double |  |
| `passing_total_offensive_plays` | double |  |
| `passing_total_points_per_game` | double |  |
| `passing_total_touchdowns` | double |  |
| `passing_total_yards` | double |  |
| `passing_total_yards_from_scrimmage` | double |  |
| `passing_two_point_pass_convs` | double |  |
| `passing_two_pt_pass` | double |  |
| `passing_two_pt_pass_attempts` | double |  |
| `passing_yards_from_scrimmage_per_game` | double |  |
| `passing_yards_per_completion` | double |  |
| `passing_yards_per_game` | double |  |
| `passing_yards_per_pass_attempt` | double |  |
| `passing_net_yards_per_pass_attempt` | double |  |
| `passing_qbr` | double | ESPN Quarterback Rating (QBR) for the player in this game. |
| `passing_adj_qbr` | double |  |
| `passing_quarterback_rating` | double |  |
| `rushing_avg_gain` | double |  |
| `rushing_espnrb_rating` | double |  |
| `rushing_long_rushing` | double |  |
| `rushing_net_total_yards` | double |  |
| `rushing_net_yards_per_game` | double |  |
| `rushing_rushing_attempts` | double |  |
| `rushing_rushing_big_plays` | double |  |
| `rushing_rushing_first_downs` | double |  |
| `rushing_rushing_fumbles` | double |  |
| `rushing_rushing_fumbles_lost` | double |  |
| `rushing_rushing_touchdowns` | double |  |
| `rushing_rushing_yards` | double |  |
| `rushing_rushing_yards_per_game` | double |  |
| `rushing_stuffs` | double |  |
| `rushing_stuff_yards_lost` | double |  |
| `rushing_team_games_played` | double |  |
| `rushing_total_offensive_plays` | double |  |
| `rushing_total_points_per_game` | double |  |
| `rushing_total_touchdowns` | double |  |
| `rushing_total_yards` | double |  |
| `rushing_total_yards_from_scrimmage` | double |  |
| `rushing_two_point_rush_convs` | double |  |
| `rushing_two_pt_rush` | double |  |
| `rushing_two_pt_rush_attempts` | double |  |
| `rushing_yards_from_scrimmage_per_game` | double |  |
| `rushing_yards_per_game` | double |  |
| `rushing_yards_per_rush_attempt` | double |  |
| `receiving_avg_gain` | double |  |
| `receiving_espnwr_rating` | double |  |
| `receiving_long_reception` | double |  |
| `receiving_net_total_yards` | double |  |
| `receiving_net_yards_per_game` | double |  |
| `receiving_receiving_big_plays` | double |  |
| `receiving_receiving_first_downs` | double |  |
| `receiving_receiving_fumbles` | double |  |
| `receiving_receiving_fumbles_lost` | double |  |
| `receiving_receiving_targets` | double |  |
| `receiving_receiving_touchdowns` | double |  |
| `receiving_receiving_yards` | double |  |
| `receiving_receiving_yards_after_catch` | double |  |
| `receiving_receiving_yards_at_catch` | double |  |
| `receiving_receiving_yards_per_game` | double |  |
| `receiving_receptions` | double |  |
| `receiving_team_games_played` | double |  |
| `receiving_total_offensive_plays` | double |  |
| `receiving_total_points_per_game` | double |  |
| `receiving_total_touchdowns` | double |  |
| `receiving_total_yards` | double |  |
| `receiving_total_yards_from_scrimmage` | double |  |
| `receiving_two_point_rec_convs` | double |  |
| `receiving_two_pt_reception` | double |  |
| `receiving_two_pt_reception_attempts` | double |  |
| `receiving_yards_from_scrimmage_per_game` | double |  |
| `receiving_yards_per_game` | double |  |
| `receiving_yards_per_reception` | double |  |
| `defensive_assist_tackles` | double |  |
| `defensive_avg_interception_yards` | double |  |
| `defensive_avg_sack_yards` | double |  |
| `defensive_avg_stuff_yards` | double |  |
| `defensive_blocked_field_goal_touchdowns` | double |  |
| `defensive_blocked_punt_touchdowns` | double |  |
| `defensive_hurries` | double |  |
| `defensive_kicks_blocked` | double |  |
| `defensive_long_interception` | double |  |
| `defensive_misc_touchdowns` | double |  |
| `defensive_passes_batted_down` | double |  |
| `defensive_passes_defended` | double |  |
| `defensive_qb_hits` | double |  |
| `defensive_two_pt_returns` | double |  |
| `defensive_sacks` | double | Sacks credited to the player. |
| `defensive_sack_yards` | double |  |
| `defensive_safeties` | double |  |
| `defensive_solo_tackles` | double |  |
| `defensive_stuffs` | double |  |
| `defensive_stuff_yards` | double |  |
| `defensive_tackles_for_loss` | double |  |
| `defensive_tackles_yards_lost` | double |  |
| `defensive_team_games_played` | double |  |
| `defensive_total_tackles` | double |  |
| `defensive_yards_allowed` | double |  |
| `defensive_points_allowed` | double |  |
| `defensive_one_pt_safeties_made` | double |  |
| `defensive_missed_field_goal_return_td` | double |  |
| `defensive_blocked_punt_ez_rec_td` | double |  |
| `defensive_interceptions_interceptions` | double |  |
| `defensive_interceptions_interception_touchdowns` | double |  |
| `defensive_interceptions_interception_yards` | double |  |
| `scoring_defensive_points` | double |  |
| `scoring_field_goals` | double |  |
| `scoring_kick_extra_points` | double |  |
| `scoring_kick_extra_points_made` | double |  |
| `scoring_misc_points` | double |  |
| `scoring_passing_touchdowns` | double |  |
| `scoring_receiving_touchdowns` | double |  |
| `scoring_return_touchdowns` | double |  |
| `scoring_rushing_touchdowns` | double |  |
| `scoring_total_points` | double |  |
| `scoring_total_points_per_game` | double |  |
| `scoring_total_touchdowns` | double |  |
| `scoring_total_two_point_convs` | double |  |
| `scoring_two_point_pass_convs` | double |  |
| `scoring_two_point_rec_convs` | double |  |
| `scoring_two_point_rush_convs` | double |  |
| `scoring_one_pt_safeties_made` | double |  |
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
from sportsdataverse.nfl import espn_nfl_player_stats
df = espn_nfl_player_stats(athlete_id=3139477, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### `espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nfl_schedule}

espn_nfl_schedule - look up the NFL schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `week` | `int` | `None` | Week of the schedule. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `groups` |  | `None` |  |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `uid` | character | ESPN UID string. |
| `date` | character | Date in YYYY-MM-DD format. |
| `attendance` | integer | Reported attendance. |
| `time_valid` | logical | Whether the start time is confirmed. |
| `neutral_site` | logical | Neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Whether the game is recent. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `broadcast` | character | Broadcast information string. |
| `highlights` | character | Game highlight urls. |
| `notes_type` | character | Notes type. |
| `notes_headline` | character | Notes headline. |
| `broadcast_market` | character | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | character | Broadcast name. |
| `type_id` | character | Type identifier (numeric). |
| `type_abbreviation` | character | Play type abbreviation. |
| `venue_id` | character | Unique venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_address_country` | character |  |
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
| `status_is_tbd_flex` | logical |  |
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
| `home_score` | character | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `home_current_rank` | integer |  |
| `home_linescores` | integer |  |
| `home_records` | character |  |
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
| `away_score` | character | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `away_current_rank` | integer |  |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | integer | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_schedule
sched = espn_nfl_schedule(dates=20240908)

# Specific week of regular season (``season_type=2``)

wk1 = espn_nfl_schedule(dates=2024, week=1, season_type=2)

# Pandas round-trip

sched_pd = espn_nfl_schedule(dates=20240908, return_as_pandas=True)
```

## Dataset loaders

### `load_combine(return_as_pandas=False) -> 'pl.DataFrame'` {#load_combine}

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | double | Year that player was drafted |
| `draft_team` | character | Team that drafted player |
| `draft_round` | double | Round that player was drafted in |
| `draft_ovr` | double | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfb_id` | character | Sports Reference (CFB) ID for player |
| `player_name` | character | Full name of player |
| `pos` | character | Position as tracked by FP |
| `school` | character | College of player |
| `ht` | character | Height of player (feet and inches) |
| `wt` | double | Weight of player (lbs) |
| `forty` | double | Player's 40 yard dash time at combine (seconds) |
| `bench` | double | Reps benched by player at combine |
| `vertical` | double | Player's vertical jump at combine (inches) |
| `broad_jump` | double | Player's broad jump at combine (inches) |
| `cone` | double | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | double | Player's shuttle run time at combine (seconds) |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

# Filter by draft year and position

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_contracts(return_as_pandas=False) -> 'pl.DataFrame'` {#load_contracts}

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | logical | Active contract |
| `year_signed` | integer | Year the contract was signed |
| `years` | integer | Contract length |
| `value` | double | Total contract value |
| `apy` | double | Average money per contract year |
| `guaranteed` | double | Total guaranteed money |
| `apy_cap_pct` | double | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | double | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | double | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | double | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | character | Player's OverTheCap url |
| `otc_id` | integer | Over the Cap ID for player |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `height` | character | Official height, in inches |
| `weight` | character | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_overall` | integer | Overall draft selection number. |
| `draft_team` | character | Team that drafted player |
| `cols` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

# Pandas round-trip with sort by APY

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_depth_charts}

Load NFL Depth Chart data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2001 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing depth chart data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `club_code` | character |  |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `depth_team` | character |  |
| `last_name` | character | Last name of player |
| `first_name` | character | First name of player |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `formation` | character |  |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `elias_id` | character |  |
| `depth_position` | character |  |
| `full_name` | character | Full name as per NFL.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

# Multi-season range

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### `load_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'` {#load_draft_picks}

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | integer | Draft round |
| `pick` | integer | Draft overall pick |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `cfb_player_id` | character | ID from College Football Reference |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `hof` | logical | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | character | Primary position as reported by NFL.com |
| `category` | character | Broader category of player positions |
| `side` | character | O for offense, D for defense, S for special teams |
| `college` | character | Official college (usually the last one attended) |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | integer | Final season played in NFL |
| `allpro` | integer | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | integer | Number of Pro Bowls |
| `seasons_started` | integer | Number of seasons recorded as primary starter for position |
| `w_av` | integer | Weighted Approximate Value |
| `car_av` | logical | Career Approximate Value |
| `dr_av` | integer | Draft Approximate Value |
| `games` | integer | Games played in career |
| `pass_completions` | integer | Number of successful completions for a given game |
| `pass_attempts` | integer | Career pass attempts |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_tds` | integer | Career pass touchdowns thrown |
| `pass_ints` | integer | Career pass interceptions thrown |
| `rush_atts` | integer | Career rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `rush_tds` | integer | Career rushing touchdowns |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | integer | Career receiving yards |
| `rec_tds` | integer | Career receiving touchdowns |
| `def_solo_tackles` | integer | Career solo tackles |
| `def_ints` | integer | Career interceptions |
| `def_sacks` | double | Number of sacks form this player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

# Filter to a single year and round

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'` {#load_ff_opportunity}

Load NFL fantasy football opportunity data from ffverse/ffopportunity

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `posteam` | character | String abbreviation for the team with possession. |
| `week` | double | Season week. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `full_name` | character | Full name as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `rec_attempt` | double | Total number of targets for a given game |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_air_yards` | double | Total air yards thrown for a given game |
| `rec_air_yards` | double | Total air yards on receiving attempts for a given game |
| `pass_completions` | double | Number of successful completions for a given game |
| `receptions` | double | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `pass_completions_exp` | double | Expected number of pass_completions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `receptions_exp` | double | Expected number of receptions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_yards_gained` | double | Total passing yards gained for a given game |
| `rec_yards_gained` | double | Total receiving yards gained for a given game |
| `rush_yards_gained` | double | Total rushing yards gained for a given game |
| `pass_yards_gained_exp` | double | Expected number of pass_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_yards_gained_exp` | double | Expected number of rec_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_yards_gained_exp` | double | Expected number of rush_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rec_touchdown` | double | Total receiving touchdowns |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `pass_touchdown_exp` | double | Expected number of pass_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_touchdown_exp` | double | Expected number of rec_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_touchdown_exp` | double | Expected number of rush_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_two_point_conv` | double | Number of successful passing two point conversions |
| `rec_two_point_conv` | double | Number of successful receiving two point conversions |
| `rush_two_point_conv` | double | Number of successful rushing two point conversions |
| `pass_two_point_conv_exp` | double | Expected number of pass_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_two_point_conv_exp` | double | Expected number of rec_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_two_point_conv_exp` | double | Expected number of rush_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_first_down` | double | Number of passing first downs |
| `rec_first_down` | double | Number of receiving first downs |
| `rush_first_down` | double | Number of rushing first downs |
| `pass_first_down_exp` | double | Expected number of pass_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_first_down_exp` | double | Expected number of rec_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_first_down_exp` | double | Expected number of rush_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_interception` | double | Number of interceptions thrown |
| `rec_interception` | double | Number of interceptions on targets |
| `pass_interception_exp` | double | Expected number of pass_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_interception_exp` | double | Expected number of rec_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fumble_lost` | double | Number of fumbles on receiving attempts |
| `rush_fumble_lost` | double | Number of fumbles on rushing attempts |
| `pass_fantasy_points_exp` | double | Expected number of pass_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fantasy_points_exp` | double | Expected number of rec_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_fantasy_points_exp` | double | Expected number of rush_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_fantasy_points` | double | Total fantasy points from passing, assuming 0.04 points per pass yard, 4 points per pass TD, -2 points per interception |
| `rec_fantasy_points` | double | Total fantasy points from receiving, assuming PPR scoring |
| `rush_fantasy_points` | double | Total fantasy points from rushing, assuming PPR scoring |
| `total_yards_gained` | double | Total scrimmage yards (sum of pass, rush, and receiving yards) |
| `total_yards_gained_exp` | double | Expected number of total_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_touchdown` | double | Total touchdowns (sum of pass, rush, and receiving touchdowns) |
| `total_touchdown_exp` | double | Expected number of total_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_first_down` | double | Total first downs (sum of pass, rush, and receiving first downs) |
| `total_first_down_exp` | double | Expected number of total_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_fantasy_points` | double | Total fantasy points (sum of pass, rush, and receiving fantasy points) |
| `total_fantasy_points_exp` | double | Expected number of total_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_completions_diff` | double | Difference between actual and expected number of pass_completions - often interpreted as efficiency for a given play/game |
| `receptions_diff` | double | Difference between actual and expected number of receptions - often interpreted as efficiency for a given play/game |
| `pass_yards_gained_diff` | double | Difference between actual and expected number of pass_yards_gained - often interpreted as efficiency for a given play/game |
| `rec_yards_gained_diff` | double | Difference between actual and expected number of rec_yards_gained - often interpreted as efficiency for a given play/game |
| `rush_yards_gained_diff` | double | Difference between actual and expected number of rush_yards_gained - often interpreted as efficiency for a given play/game |
| `pass_touchdown_diff` | double | Difference between actual and expected number of pass_touchdown - often interpreted as efficiency for a given play/game |
| `rec_touchdown_diff` | double | Difference between actual and expected number of rec_touchdown - often interpreted as efficiency for a given play/game |
| `rush_touchdown_diff` | double | Difference between actual and expected number of rush_touchdown - often interpreted as efficiency for a given play/game |
| `pass_two_point_conv_diff` | double | Difference between actual and expected number of pass_two_point_conv - often interpreted as efficiency for a given play/game |
| `rec_two_point_conv_diff` | double | Difference between actual and expected number of rec_two_point_conv - often interpreted as efficiency for a given play/game |
| `rush_two_point_conv_diff` | double | Difference between actual and expected number of rush_two_point_conv - often interpreted as efficiency for a given play/game |
| `pass_first_down_diff` | double | Difference between actual and expected number of pass_first_down - often interpreted as efficiency for a given play/game |
| `rec_first_down_diff` | double | Difference between actual and expected number of rec_first_down - often interpreted as efficiency for a given play/game |
| `rush_first_down_diff` | double | Difference between actual and expected number of rush_first_down - often interpreted as efficiency for a given play/game |
| `pass_interception_diff` | double | Difference between actual and expected number of pass_interception - often interpreted as efficiency for a given play/game |
| `rec_interception_diff` | double | Difference between actual and expected number of rec_interception - often interpreted as efficiency for a given play/game |
| `pass_fantasy_points_diff` | double | Difference between actual and expected number of pass_fantasy_points - often interpreted as efficiency for a given play/game |
| `rec_fantasy_points_diff` | double | Difference between actual and expected number of rec_fantasy_points - often interpreted as efficiency for a given play/game |
| `rush_fantasy_points_diff` | double | Difference between actual and expected number of rush_fantasy_points - often interpreted as efficiency for a given play/game |
| `total_yards_gained_diff` | double | Difference between actual and expected number of total_yards_gained - often interpreted as efficiency for a given play/game |
| `total_touchdown_diff` | double | Difference between actual and expected number of total_touchdown - often interpreted as efficiency for a given play/game |
| `total_first_down_diff` | double | Difference between actual and expected number of total_first_down - often interpreted as efficiency for a given play/game |
| `total_fantasy_points_diff` | double | Difference between actual and expected number of total_fantasy_points - often interpreted as efficiency for a given play/game |
| `pass_attempt_team` | double | Team-level total pass_attempt for a game, summed across all plays/players for that team. |
| `rec_attempt_team` | double | Team-level total rec_attempt for a game, summed across all plays/players for that team. |
| `rush_attempt_team` | double | Team-level total rush_attempt for a game, summed across all plays/players for that team. |
| `pass_air_yards_team` | double | Team-level total pass_air_yards for a game, summed across all plays/players for that team. |
| `rec_air_yards_team` | double | Team-level total rec_air_yards for a game, summed across all plays/players for that team. |
| `pass_completions_team` | double | Team-level total pass_completions for a game, summed across all plays/players for that team. |
| `receptions_team` | double | Team-level total receptions for a game, summed across all plays/players for that team. |
| `pass_completions_exp_team` | double | Team-level total expected pass_completions_exp for a game, summed across all plays & players for that team. |
| `receptions_exp_team` | double | Team-level total expected receptions_exp for a game, summed across all plays & players for that team. |
| `pass_yards_gained_team` | double | Team-level total pass_yards_gained for a game, summed across all plays/players for that team. |
| `rec_yards_gained_team` | double | Team-level total rec_yards_gained for a game, summed across all plays/players for that team. |
| `rush_yards_gained_team` | double | Team-level total rush_yards_gained for a game, summed across all plays/players for that team. |
| `pass_yards_gained_exp_team` | double | Team-level total expected pass_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rec_yards_gained_exp_team` | double | Team-level total expected rec_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rush_yards_gained_exp_team` | double | Team-level total expected rush_yards_gained_exp for a game, summed across all plays & players for that team. |
| `pass_touchdown_team` | double | Team-level total pass_touchdown for a game, summed across all plays/players for that team. |
| `rec_touchdown_team` | double | Team-level total rec_touchdown for a game, summed across all plays/players for that team. |
| `rush_touchdown_team` | double | Team-level total rush_touchdown for a game, summed across all plays/players for that team. |
| `pass_touchdown_exp_team` | double | Team-level total expected pass_touchdown_exp for a game, summed across all plays & players for that team. |
| `rec_touchdown_exp_team` | double | Team-level total expected rec_touchdown_exp for a game, summed across all plays & players for that team. |
| `rush_touchdown_exp_team` | double | Team-level total expected rush_touchdown_exp for a game, summed across all plays & players for that team. |
| `pass_two_point_conv_team` | double | Team-level total pass_two_point_conv for a game, summed across all plays/players for that team. |
| `rec_two_point_conv_team` | double | Team-level total rec_two_point_conv for a game, summed across all plays/players for that team. |
| `rush_two_point_conv_team` | double | Team-level total rush_two_point_conv for a game, summed across all plays/players for that team. |
| `pass_two_point_conv_exp_team` | double | Team-level total expected pass_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rec_two_point_conv_exp_team` | double | Team-level total expected rec_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rush_two_point_conv_exp_team` | double | Team-level total expected rush_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `pass_first_down_team` | double | Team-level total pass_first_down for a game, summed across all plays/players for that team. |
| `rec_first_down_team` | double | Team-level total rec_first_down for a game, summed across all plays/players for that team. |
| `rush_first_down_team` | double | Team-level total rush_first_down for a game, summed across all plays/players for that team. |
| `pass_first_down_exp_team` | double | Team-level total expected pass_first_down_exp for a game, summed across all plays & players for that team. |
| `rec_first_down_exp_team` | double | Team-level total expected rec_first_down_exp for a game, summed across all plays & players for that team. |
| `rush_first_down_exp_team` | double | Team-level total expected rush_first_down_exp for a game, summed across all plays & players for that team. |
| `pass_interception_team` | double | Team-level total pass_interception for a game, summed across all plays/players for that team. |
| `rec_interception_team` | double | Team-level total rec_interception for a game, summed across all plays/players for that team. |
| `pass_interception_exp_team` | double | Team-level total expected pass_interception_exp for a game, summed across all plays & players for that team. |
| `rec_interception_exp_team` | double | Team-level total expected rec_interception_exp for a game, summed across all plays & players for that team. |
| `rec_fumble_lost_team` | double | Team-level total rec_fumble_lost for a game, summed across all plays/players for that team. |
| `rush_fumble_lost_team` | double | Team-level total rush_fumble_lost for a game, summed across all plays/players for that team. |
| `pass_fantasy_points_exp_team` | double | Team-level total expected pass_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rec_fantasy_points_exp_team` | double | Team-level total expected rec_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rush_fantasy_points_exp_team` | double | Team-level total expected rush_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `pass_fantasy_points_team` | double | Team-level total pass_fantasy_points for a game, summed across all plays/players for that team. |
| `rec_fantasy_points_team` | double | Team-level total rec_fantasy_points for a game, summed across all plays/players for that team. |
| `rush_fantasy_points_team` | double | Team-level total rush_fantasy_points for a game, summed across all plays/players for that team. |
| `pass_completions_diff_team` | double | Team-level difference between actual and expected number of pass_completions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `receptions_diff_team` | double | Team-level difference between actual and expected number of receptions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_yards_gained_diff_team` | double | Team-level difference between actual and expected number of pass_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rec_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rush_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_touchdown_diff_team` | double | Team-level difference between actual and expected number of pass_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_touchdown_diff_team` | double | Team-level difference between actual and expected number of rec_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_touchdown_diff_team` | double | Team-level difference between actual and expected number of rush_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of pass_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rec_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rush_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_first_down_diff_team` | double | Team-level difference between actual and expected number of pass_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_first_down_diff_team` | double | Team-level difference between actual and expected number of rec_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_first_down_diff_team` | double | Team-level difference between actual and expected number of rush_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_interception_diff_team` | double | Team-level difference between actual and expected number of pass_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_interception_diff_team` | double | Team-level difference between actual and expected number of rec_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of pass_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rec_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rush_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_yards_gained_team` | double | Team-level total total_yards_gained for a game, summed across all plays/players for that team. |
| `total_yards_gained_exp_team` | double | Team-level total expected total_yards_gained_exp for a game, summed across all plays & players for that team. |
| `total_yards_gained_diff_team` | double | Team-level difference between actual and expected number of total_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_touchdown_team` | double | Team-level total total_touchdown for a game, summed across all plays/players for that team. |
| `total_touchdown_exp_team` | double | Team-level total expected total_touchdown_exp for a game, summed across all plays & players for that team. |
| `total_touchdown_diff_team` | double | Team-level difference between actual and expected number of total_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_first_down_team` | double | Team-level total total_first_down for a game, summed across all plays/players for that team. |
| `total_first_down_exp_team` | double | Team-level total expected total_first_down_exp for a game, summed across all plays & players for that team. |
| `total_first_down_diff_team` | double | Team-level difference between actual and expected number of total_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_fantasy_points_team` | double | Team-level total total_fantasy_points for a game, summed across all plays/players for that team. |
| `total_fantasy_points_exp_team` | double | Team-level total expected total_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `total_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of total_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

# Pass play-by-play opportunity stats

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

# Rush play-by-play opportunity stats with pinned model version

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'` {#load_ff_playerids}

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

| col_name | type | description |
|---|---|---|
| `mfl_id` | integer | MyFantasyLeague.com ID - this is the primary key for this table and is unique and complete. Usually an integer of 5 digits. |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `fantasypros_id` | character | FantasyPros.com ID - usually an integer of 5 digits. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `sleeper_id` | integer | Sleeper ID - usually an integer with ~4 digits. |
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `espn_id` | integer | ESPN ID - usual format is an integer with ~5 digits |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `fleaflicker_id` | character | Fleaflicker ID - usual format is an integer with ~4 digits. Fleaflicker API also has sportradar and that's generally preferred. |
| `cbs_id` | integer | CBS ID - usual format is an integer with ~ 7 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfbref_id` | character | College Football Reference ID - usual format is firstname-lastname-integer |
| `rotowire_id` | integer | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `rotoworld_id` | character | Rotoworld ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `ktc_id` | integer | KeepTradeCut ID - usual format is an integer with ~four digits. |
| `stats_id` | integer | Stats ID - usual format is five digit integer |
| `stats_global_id` | integer | Stats Global ID - usual format is a six digit integer |
| `fantasy_data_id` | integer | FantasyData ID - usual format five digit integer |
| `swish_id` | character | Player ID for Swish Analytics |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `merge_name` | character | Name but formatted for name joins via ffscrapr::dp_cleannames() - coerced to lowercase, stripped of punctuation and suffixes, and common substitutions performed. |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `birthdate` | character | Birthdate |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_pick` | integer | Draft pick within round, i.e. 32nd pick of second round. |
| `draft_ovr` | integer | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `twitter_username` | character | Official twitter handle, if known |
| `height` | integer | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `db_season` | integer | Year of database build. Previous years may also be available via dynastyprocess. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

# Filter to active QBs

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'` {#load_ff_rankings}

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of `"draft"` (current draft rankings), `"week"` (weekly rankings), or `"all"` (full historical rankings). Defaults to `"draft"`. Kept for nflreadpy parity since its parameter is also called `type`; the forward-going preferred name is `kind`. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as `type`. If both are supplied, `kind` wins. If neither is supplied, defaults to `"draft"` via `type`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

| col_name | type | description |
|---|---|---|
| `fp_page` | character | The relative url that the data was scraped from (add the prefix https://www.fantasypros.com/ to visit the page) |
| `page_type` | character | Two word identifier separated by a dash identifying the type of fantasy ranking (best = bestball; dynasty; redraft) and what position it applies to |
| `ecr_type` | character | A two letter identifier combining the ranking type (b = bestball; d = dynasty; r = redraft) and position type (o = overall; p = positional; sf = superflex; rk = rookie) |
| `player` | character | Player name |
| `id` | integer | ID of the player in the 'name' column. |
| `pos` | character | Position as tracked by FP |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `ecr` | double | Average (mean) expert ranking for this player |
| `sd` | double | Standard deviation of expert rankings for this player |
| `best` | integer | The highest ranking given for this player by any one expert |
| `worst` | integer | The lowest ranking given for this player by any one expert |
| `sportsdata_id` | character | ID - also known as sportradar_id (they are equivalent!) |
| `player_filename` | character | base URL for this player on fantasypros.com |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `cbs_id` | character | CBS ID - usual format is an integer with ~ 7 digits. |
| `player_owned_avg` | double | The average percentage this player is rostered across ESPN and Yahoo |
| `player_owned_espn` | character | The percentage that this player is rostered in ESPN leagues |
| `player_owned_yahoo` | character | The percentage that this player is rostered in Yahoo leagues |
| `player_image_url` | character | An image of the player |
| `player_square_image_url` | character | An square image of the player |
| `rank_delta` | integer | Change in ranks over a recent period |
| `bye` | integer | NFL bye week |
| `mergename` | character | Player name after being cleaned by dp_cleannames - generally strips punctuation and suffixes as well as performing common name substitutions. |
| `scrape_date` | character | Date this dataframe was last updated |
| `tm` | character | Team ID as used on MyFantasyLeague.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

# Weekly rankings

weekly = load_nfl_ff_rankings(kind="week")

# Full historical rankings (parquet)

history = load_nfl_ff_rankings(kind="all")

# nflreadpy-parity ``type=`` parameter (still supported)

draft = load_nfl_ff_rankings(type="draft")
```

### `load_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_ftn_charting}

Load NFL FTN charting data going back to 2022

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2022 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing FTN charting data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `ftn_game_id` | integer | FTN game ID |
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `ftn_play_id` | integer | FTN play ID |
| `nflverse_play_id` | integer | Play ID used by nflverse, corresponds to GSIS play ID |
| `starting_hash` | character | hash the ball was place(L = left, M = middle, R = right) |
| `qb_location` | character | pre-snap position of quarterback(U = under center, S = shotgun, P = pistol) |
| `n_offense_backfield` | integer | number of players in the backfield at the snap |
| `n_defense_box` | integer |  |
| `is_no_huddle` | logical | no huddle |
| `is_motion` | logical | motion occurred on the play before or at the time of the snap |
| `is_play_action` | logical | play-action pass |
| `is_screen_pass` | logical | screen pass |
| `is_rpo` | logical | play is considered run-pass option |
| `is_trick_play` | logical | trick play |
| `is_qb_out_of_pocket` | logical | quarterback moved out of pocket |
| `is_interception_worthy` | logical | interception worthy pass |
| `is_throw_away` | logical | quarterback thrown away |
| `read_thrown` | character | read the ball was thrown |
| `is_catchable_ball` | logical | catchable ball(defined by throws that are generally on target that are not defended away) |
| `is_contested_ball` | logical | contested ball(defined by whether or not the receiver is facing physical contact at the time of the catch) |
| `is_created_reception` | logical | created reception(defined by a reception that only occurs due to an exceptional play by the receiver) |
| `is_drop` | logical | receiver drop |
| `is_qb_sneak` | logical | quarterback sneak |
| `n_blitzers` | integer | number of blitzers |
| `n_pass_rushers` | integer | number of pass rushers |
| `is_qb_fault_sack` | logical | sack that is the fault of the quarterback |
| `date_pulled` | character | Date the data was retrieved from the FTN Data API by nflverse jobs |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

# Multi-season range

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

# Filter to plays with motion

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### `load_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_injuries}

Load NFL injuries data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2009 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing injuries data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `week` | integer | Season week. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `position` | character | Primary position as reported by NFL.com |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `report_primary_injury` | character | Primary injury listed on official injury report |
| `report_secondary_injury` | character | Secondary injury listed on official injury report |
| `report_status` | character | Player's status for game on official injury report |
| `practice_primary_injury` | character | Primary injury listed on practice injury report |
| `practice_secondary_injury` | character | Secondary injury listed on practice injury report |
| `practice_status` | character | Player's participation in practice |
| `date_modified` | character | Date and time that injury information was updated |

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

# Multi-season range with team filter

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### `load_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nextgen_stats}

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats
accessors. Mirrors the API surface of nflreadpy's
`load_nextgen_stats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — `seasons` is applied as a post-filter on the `season` column. |
| `stat_type` | `str` | `'passing'` | One of `"passing"`, `"rushing"`, `"receiving"`. Defaults to `"passing"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested `stat_type` and `seasons`.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_time_to_throw` | double | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | double | Average air yards on completed passes |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `avg_air_yards_differential` | double | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | double | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | double | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_touchdowns` | integer | Number of touchdowns scored on pass plays |
| `interceptions` | integer | The number of interceptions thrown. |
| `passer_rating` | double | Overall NFL passer rating |
| `completions` | integer | The number of completed passes. |
| `completion_percentage` | double | Percentage of completed passes |
| `expected_completion_percentage` | double | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | double | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | double | A receiver's average depth of target |
| `max_air_distance` | double | A receiver's maximum depth of target |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

# Rushing NextGen stats

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

# Receiving NextGen stats with a follow-up filter

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

# Pandas round-trip

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_combine(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_combine}

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | double | Year that player was drafted |
| `draft_team` | character | Team that drafted player |
| `draft_round` | double | Round that player was drafted in |
| `draft_ovr` | double | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfb_id` | character | Sports Reference (CFB) ID for player |
| `player_name` | character | Full name of player |
| `pos` | character | Position as tracked by FP |
| `school` | character | College of player |
| `ht` | character | Height of player (feet and inches) |
| `wt` | double | Weight of player (lbs) |
| `forty` | double | Player's 40 yard dash time at combine (seconds) |
| `bench` | double | Reps benched by player at combine |
| `vertical` | double | Player's vertical jump at combine (inches) |
| `broad_jump` | double | Player's broad jump at combine (inches) |
| `cone` | double | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | double | Player's shuttle run time at combine (seconds) |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

# Filter by draft year and position

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_nfl_contracts(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_contracts}

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | logical | Active contract |
| `year_signed` | integer | Year the contract was signed |
| `years` | integer | Contract length |
| `value` | double | Total contract value |
| `apy` | double | Average money per contract year |
| `guaranteed` | double | Total guaranteed money |
| `apy_cap_pct` | double | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | double | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | double | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | double | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | character | Player's OverTheCap url |
| `otc_id` | integer | Over the Cap ID for player |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `height` | character | Official height, in inches |
| `weight` | character | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_overall` | integer | Overall draft selection number. |
| `draft_team` | character | Team that drafted player |
| `cols` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

# Pandas round-trip with sort by APY

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_nfl_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_depth_charts}

Load NFL Depth Chart data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2001 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing depth chart data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `club_code` | character |  |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `depth_team` | character |  |
| `last_name` | character | Last name of player |
| `first_name` | character | First name of player |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `formation` | character |  |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `elias_id` | character |  |
| `depth_position` | character |  |
| `full_name` | character | Full name as per NFL.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

# Multi-season range

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### `load_nfl_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_draft_picks}

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | integer | Draft round |
| `pick` | integer | Draft overall pick |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `cfb_player_id` | character | ID from College Football Reference |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `hof` | logical | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | character | Primary position as reported by NFL.com |
| `category` | character | Broader category of player positions |
| `side` | character | O for offense, D for defense, S for special teams |
| `college` | character | Official college (usually the last one attended) |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | integer | Final season played in NFL |
| `allpro` | integer | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | integer | Number of Pro Bowls |
| `seasons_started` | integer | Number of seasons recorded as primary starter for position |
| `w_av` | integer | Weighted Approximate Value |
| `car_av` | logical | Career Approximate Value |
| `dr_av` | integer | Draft Approximate Value |
| `games` | integer | Games played in career |
| `pass_completions` | integer | Number of successful completions for a given game |
| `pass_attempts` | integer | Career pass attempts |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_tds` | integer | Career pass touchdowns thrown |
| `pass_ints` | integer | Career pass interceptions thrown |
| `rush_atts` | integer | Career rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `rush_tds` | integer | Career rushing touchdowns |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | integer | Career receiving yards |
| `rec_tds` | integer | Career receiving touchdowns |
| `def_solo_tackles` | integer | Career solo tackles |
| `def_ints` | integer | Career interceptions |
| `def_sacks` | double | Number of sacks form this player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

# Filter to a single year and round

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_nfl_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_ff_opportunity}

Load NFL fantasy football opportunity data from ffverse/ffopportunity

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `posteam` | character | String abbreviation for the team with possession. |
| `week` | double | Season week. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `full_name` | character | Full name as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `rec_attempt` | double | Total number of targets for a given game |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_air_yards` | double | Total air yards thrown for a given game |
| `rec_air_yards` | double | Total air yards on receiving attempts for a given game |
| `pass_completions` | double | Number of successful completions for a given game |
| `receptions` | double | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `pass_completions_exp` | double | Expected number of pass_completions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `receptions_exp` | double | Expected number of receptions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_yards_gained` | double | Total passing yards gained for a given game |
| `rec_yards_gained` | double | Total receiving yards gained for a given game |
| `rush_yards_gained` | double | Total rushing yards gained for a given game |
| `pass_yards_gained_exp` | double | Expected number of pass_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_yards_gained_exp` | double | Expected number of rec_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_yards_gained_exp` | double | Expected number of rush_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rec_touchdown` | double | Total receiving touchdowns |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `pass_touchdown_exp` | double | Expected number of pass_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_touchdown_exp` | double | Expected number of rec_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_touchdown_exp` | double | Expected number of rush_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_two_point_conv` | double | Number of successful passing two point conversions |
| `rec_two_point_conv` | double | Number of successful receiving two point conversions |
| `rush_two_point_conv` | double | Number of successful rushing two point conversions |
| `pass_two_point_conv_exp` | double | Expected number of pass_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_two_point_conv_exp` | double | Expected number of rec_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_two_point_conv_exp` | double | Expected number of rush_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_first_down` | double | Number of passing first downs |
| `rec_first_down` | double | Number of receiving first downs |
| `rush_first_down` | double | Number of rushing first downs |
| `pass_first_down_exp` | double | Expected number of pass_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_first_down_exp` | double | Expected number of rec_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_first_down_exp` | double | Expected number of rush_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_interception` | double | Number of interceptions thrown |
| `rec_interception` | double | Number of interceptions on targets |
| `pass_interception_exp` | double | Expected number of pass_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_interception_exp` | double | Expected number of rec_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fumble_lost` | double | Number of fumbles on receiving attempts |
| `rush_fumble_lost` | double | Number of fumbles on rushing attempts |
| `pass_fantasy_points_exp` | double | Expected number of pass_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fantasy_points_exp` | double | Expected number of rec_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_fantasy_points_exp` | double | Expected number of rush_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_fantasy_points` | double | Total fantasy points from passing, assuming 0.04 points per pass yard, 4 points per pass TD, -2 points per interception |
| `rec_fantasy_points` | double | Total fantasy points from receiving, assuming PPR scoring |
| `rush_fantasy_points` | double | Total fantasy points from rushing, assuming PPR scoring |
| `total_yards_gained` | double | Total scrimmage yards (sum of pass, rush, and receiving yards) |
| `total_yards_gained_exp` | double | Expected number of total_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_touchdown` | double | Total touchdowns (sum of pass, rush, and receiving touchdowns) |
| `total_touchdown_exp` | double | Expected number of total_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_first_down` | double | Total first downs (sum of pass, rush, and receiving first downs) |
| `total_first_down_exp` | double | Expected number of total_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_fantasy_points` | double | Total fantasy points (sum of pass, rush, and receiving fantasy points) |
| `total_fantasy_points_exp` | double | Expected number of total_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_completions_diff` | double | Difference between actual and expected number of pass_completions - often interpreted as efficiency for a given play/game |
| `receptions_diff` | double | Difference between actual and expected number of receptions - often interpreted as efficiency for a given play/game |
| `pass_yards_gained_diff` | double | Difference between actual and expected number of pass_yards_gained - often interpreted as efficiency for a given play/game |
| `rec_yards_gained_diff` | double | Difference between actual and expected number of rec_yards_gained - often interpreted as efficiency for a given play/game |
| `rush_yards_gained_diff` | double | Difference between actual and expected number of rush_yards_gained - often interpreted as efficiency for a given play/game |
| `pass_touchdown_diff` | double | Difference between actual and expected number of pass_touchdown - often interpreted as efficiency for a given play/game |
| `rec_touchdown_diff` | double | Difference between actual and expected number of rec_touchdown - often interpreted as efficiency for a given play/game |
| `rush_touchdown_diff` | double | Difference between actual and expected number of rush_touchdown - often interpreted as efficiency for a given play/game |
| `pass_two_point_conv_diff` | double | Difference between actual and expected number of pass_two_point_conv - often interpreted as efficiency for a given play/game |
| `rec_two_point_conv_diff` | double | Difference between actual and expected number of rec_two_point_conv - often interpreted as efficiency for a given play/game |
| `rush_two_point_conv_diff` | double | Difference between actual and expected number of rush_two_point_conv - often interpreted as efficiency for a given play/game |
| `pass_first_down_diff` | double | Difference between actual and expected number of pass_first_down - often interpreted as efficiency for a given play/game |
| `rec_first_down_diff` | double | Difference between actual and expected number of rec_first_down - often interpreted as efficiency for a given play/game |
| `rush_first_down_diff` | double | Difference between actual and expected number of rush_first_down - often interpreted as efficiency for a given play/game |
| `pass_interception_diff` | double | Difference between actual and expected number of pass_interception - often interpreted as efficiency for a given play/game |
| `rec_interception_diff` | double | Difference between actual and expected number of rec_interception - often interpreted as efficiency for a given play/game |
| `pass_fantasy_points_diff` | double | Difference between actual and expected number of pass_fantasy_points - often interpreted as efficiency for a given play/game |
| `rec_fantasy_points_diff` | double | Difference between actual and expected number of rec_fantasy_points - often interpreted as efficiency for a given play/game |
| `rush_fantasy_points_diff` | double | Difference between actual and expected number of rush_fantasy_points - often interpreted as efficiency for a given play/game |
| `total_yards_gained_diff` | double | Difference between actual and expected number of total_yards_gained - often interpreted as efficiency for a given play/game |
| `total_touchdown_diff` | double | Difference between actual and expected number of total_touchdown - often interpreted as efficiency for a given play/game |
| `total_first_down_diff` | double | Difference between actual and expected number of total_first_down - often interpreted as efficiency for a given play/game |
| `total_fantasy_points_diff` | double | Difference between actual and expected number of total_fantasy_points - often interpreted as efficiency for a given play/game |
| `pass_attempt_team` | double | Team-level total pass_attempt for a game, summed across all plays/players for that team. |
| `rec_attempt_team` | double | Team-level total rec_attempt for a game, summed across all plays/players for that team. |
| `rush_attempt_team` | double | Team-level total rush_attempt for a game, summed across all plays/players for that team. |
| `pass_air_yards_team` | double | Team-level total pass_air_yards for a game, summed across all plays/players for that team. |
| `rec_air_yards_team` | double | Team-level total rec_air_yards for a game, summed across all plays/players for that team. |
| `pass_completions_team` | double | Team-level total pass_completions for a game, summed across all plays/players for that team. |
| `receptions_team` | double | Team-level total receptions for a game, summed across all plays/players for that team. |
| `pass_completions_exp_team` | double | Team-level total expected pass_completions_exp for a game, summed across all plays & players for that team. |
| `receptions_exp_team` | double | Team-level total expected receptions_exp for a game, summed across all plays & players for that team. |
| `pass_yards_gained_team` | double | Team-level total pass_yards_gained for a game, summed across all plays/players for that team. |
| `rec_yards_gained_team` | double | Team-level total rec_yards_gained for a game, summed across all plays/players for that team. |
| `rush_yards_gained_team` | double | Team-level total rush_yards_gained for a game, summed across all plays/players for that team. |
| `pass_yards_gained_exp_team` | double | Team-level total expected pass_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rec_yards_gained_exp_team` | double | Team-level total expected rec_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rush_yards_gained_exp_team` | double | Team-level total expected rush_yards_gained_exp for a game, summed across all plays & players for that team. |
| `pass_touchdown_team` | double | Team-level total pass_touchdown for a game, summed across all plays/players for that team. |
| `rec_touchdown_team` | double | Team-level total rec_touchdown for a game, summed across all plays/players for that team. |
| `rush_touchdown_team` | double | Team-level total rush_touchdown for a game, summed across all plays/players for that team. |
| `pass_touchdown_exp_team` | double | Team-level total expected pass_touchdown_exp for a game, summed across all plays & players for that team. |
| `rec_touchdown_exp_team` | double | Team-level total expected rec_touchdown_exp for a game, summed across all plays & players for that team. |
| `rush_touchdown_exp_team` | double | Team-level total expected rush_touchdown_exp for a game, summed across all plays & players for that team. |
| `pass_two_point_conv_team` | double | Team-level total pass_two_point_conv for a game, summed across all plays/players for that team. |
| `rec_two_point_conv_team` | double | Team-level total rec_two_point_conv for a game, summed across all plays/players for that team. |
| `rush_two_point_conv_team` | double | Team-level total rush_two_point_conv for a game, summed across all plays/players for that team. |
| `pass_two_point_conv_exp_team` | double | Team-level total expected pass_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rec_two_point_conv_exp_team` | double | Team-level total expected rec_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rush_two_point_conv_exp_team` | double | Team-level total expected rush_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `pass_first_down_team` | double | Team-level total pass_first_down for a game, summed across all plays/players for that team. |
| `rec_first_down_team` | double | Team-level total rec_first_down for a game, summed across all plays/players for that team. |
| `rush_first_down_team` | double | Team-level total rush_first_down for a game, summed across all plays/players for that team. |
| `pass_first_down_exp_team` | double | Team-level total expected pass_first_down_exp for a game, summed across all plays & players for that team. |
| `rec_first_down_exp_team` | double | Team-level total expected rec_first_down_exp for a game, summed across all plays & players for that team. |
| `rush_first_down_exp_team` | double | Team-level total expected rush_first_down_exp for a game, summed across all plays & players for that team. |
| `pass_interception_team` | double | Team-level total pass_interception for a game, summed across all plays/players for that team. |
| `rec_interception_team` | double | Team-level total rec_interception for a game, summed across all plays/players for that team. |
| `pass_interception_exp_team` | double | Team-level total expected pass_interception_exp for a game, summed across all plays & players for that team. |
| `rec_interception_exp_team` | double | Team-level total expected rec_interception_exp for a game, summed across all plays & players for that team. |
| `rec_fumble_lost_team` | double | Team-level total rec_fumble_lost for a game, summed across all plays/players for that team. |
| `rush_fumble_lost_team` | double | Team-level total rush_fumble_lost for a game, summed across all plays/players for that team. |
| `pass_fantasy_points_exp_team` | double | Team-level total expected pass_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rec_fantasy_points_exp_team` | double | Team-level total expected rec_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rush_fantasy_points_exp_team` | double | Team-level total expected rush_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `pass_fantasy_points_team` | double | Team-level total pass_fantasy_points for a game, summed across all plays/players for that team. |
| `rec_fantasy_points_team` | double | Team-level total rec_fantasy_points for a game, summed across all plays/players for that team. |
| `rush_fantasy_points_team` | double | Team-level total rush_fantasy_points for a game, summed across all plays/players for that team. |
| `pass_completions_diff_team` | double | Team-level difference between actual and expected number of pass_completions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `receptions_diff_team` | double | Team-level difference between actual and expected number of receptions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_yards_gained_diff_team` | double | Team-level difference between actual and expected number of pass_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rec_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rush_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_touchdown_diff_team` | double | Team-level difference between actual and expected number of pass_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_touchdown_diff_team` | double | Team-level difference between actual and expected number of rec_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_touchdown_diff_team` | double | Team-level difference between actual and expected number of rush_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of pass_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rec_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rush_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_first_down_diff_team` | double | Team-level difference between actual and expected number of pass_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_first_down_diff_team` | double | Team-level difference between actual and expected number of rec_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_first_down_diff_team` | double | Team-level difference between actual and expected number of rush_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_interception_diff_team` | double | Team-level difference between actual and expected number of pass_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_interception_diff_team` | double | Team-level difference between actual and expected number of rec_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of pass_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rec_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rush_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_yards_gained_team` | double | Team-level total total_yards_gained for a game, summed across all plays/players for that team. |
| `total_yards_gained_exp_team` | double | Team-level total expected total_yards_gained_exp for a game, summed across all plays & players for that team. |
| `total_yards_gained_diff_team` | double | Team-level difference between actual and expected number of total_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_touchdown_team` | double | Team-level total total_touchdown for a game, summed across all plays/players for that team. |
| `total_touchdown_exp_team` | double | Team-level total expected total_touchdown_exp for a game, summed across all plays & players for that team. |
| `total_touchdown_diff_team` | double | Team-level difference between actual and expected number of total_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_first_down_team` | double | Team-level total total_first_down for a game, summed across all plays/players for that team. |
| `total_first_down_exp_team` | double | Team-level total expected total_first_down_exp for a game, summed across all plays & players for that team. |
| `total_first_down_diff_team` | double | Team-level difference between actual and expected number of total_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_fantasy_points_team` | double | Team-level total total_fantasy_points for a game, summed across all plays/players for that team. |
| `total_fantasy_points_exp_team` | double | Team-level total expected total_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `total_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of total_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

# Pass play-by-play opportunity stats

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

# Rush play-by-play opportunity stats with pinned model version

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_nfl_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_ff_playerids}

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

| col_name | type | description |
|---|---|---|
| `mfl_id` | integer | MyFantasyLeague.com ID - this is the primary key for this table and is unique and complete. Usually an integer of 5 digits. |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `fantasypros_id` | character | FantasyPros.com ID - usually an integer of 5 digits. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `sleeper_id` | integer | Sleeper ID - usually an integer with ~4 digits. |
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `espn_id` | integer | ESPN ID - usual format is an integer with ~5 digits |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `fleaflicker_id` | character | Fleaflicker ID - usual format is an integer with ~4 digits. Fleaflicker API also has sportradar and that's generally preferred. |
| `cbs_id` | integer | CBS ID - usual format is an integer with ~ 7 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfbref_id` | character | College Football Reference ID - usual format is firstname-lastname-integer |
| `rotowire_id` | integer | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `rotoworld_id` | character | Rotoworld ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `ktc_id` | integer | KeepTradeCut ID - usual format is an integer with ~four digits. |
| `stats_id` | integer | Stats ID - usual format is five digit integer |
| `stats_global_id` | integer | Stats Global ID - usual format is a six digit integer |
| `fantasy_data_id` | integer | FantasyData ID - usual format five digit integer |
| `swish_id` | character | Player ID for Swish Analytics |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `merge_name` | character | Name but formatted for name joins via ffscrapr::dp_cleannames() - coerced to lowercase, stripped of punctuation and suffixes, and common substitutions performed. |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `birthdate` | character | Birthdate |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_pick` | integer | Draft pick within round, i.e. 32nd pick of second round. |
| `draft_ovr` | integer | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `twitter_username` | character | Official twitter handle, if known |
| `height` | integer | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `db_season` | integer | Year of database build. Previous years may also be available via dynastyprocess. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

# Filter to active QBs

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_nfl_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_ff_rankings}

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of `"draft"` (current draft rankings), `"week"` (weekly rankings), or `"all"` (full historical rankings). Defaults to `"draft"`. Kept for nflreadpy parity since its parameter is also called `type`; the forward-going preferred name is `kind`. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as `type`. If both are supplied, `kind` wins. If neither is supplied, defaults to `"draft"` via `type`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

| col_name | type | description |
|---|---|---|
| `fp_page` | character | The relative url that the data was scraped from (add the prefix https://www.fantasypros.com/ to visit the page) |
| `page_type` | character | Two word identifier separated by a dash identifying the type of fantasy ranking (best = bestball; dynasty; redraft) and what position it applies to |
| `ecr_type` | character | A two letter identifier combining the ranking type (b = bestball; d = dynasty; r = redraft) and position type (o = overall; p = positional; sf = superflex; rk = rookie) |
| `player` | character | Player name |
| `id` | integer | ID of the player in the 'name' column. |
| `pos` | character | Position as tracked by FP |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `ecr` | double | Average (mean) expert ranking for this player |
| `sd` | double | Standard deviation of expert rankings for this player |
| `best` | integer | The highest ranking given for this player by any one expert |
| `worst` | integer | The lowest ranking given for this player by any one expert |
| `sportsdata_id` | character | ID - also known as sportradar_id (they are equivalent!) |
| `player_filename` | character | base URL for this player on fantasypros.com |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `cbs_id` | character | CBS ID - usual format is an integer with ~ 7 digits. |
| `player_owned_avg` | double | The average percentage this player is rostered across ESPN and Yahoo |
| `player_owned_espn` | character | The percentage that this player is rostered in ESPN leagues |
| `player_owned_yahoo` | character | The percentage that this player is rostered in Yahoo leagues |
| `player_image_url` | character | An image of the player |
| `player_square_image_url` | character | An square image of the player |
| `rank_delta` | integer | Change in ranks over a recent period |
| `bye` | integer | NFL bye week |
| `mergename` | character | Player name after being cleaned by dp_cleannames - generally strips punctuation and suffixes as well as performing common name substitutions. |
| `scrape_date` | character | Date this dataframe was last updated |
| `tm` | character | Team ID as used on MyFantasyLeague.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

# Weekly rankings

weekly = load_nfl_ff_rankings(kind="week")

# Full historical rankings (parquet)

history = load_nfl_ff_rankings(kind="all")

# nflreadpy-parity ``type=`` parameter (still supported)

draft = load_nfl_ff_rankings(type="draft")
```

### `load_nfl_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_ftn_charting}

Load NFL FTN charting data going back to 2022

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2022 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing FTN charting data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `ftn_game_id` | integer | FTN game ID |
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `ftn_play_id` | integer | FTN play ID |
| `nflverse_play_id` | integer | Play ID used by nflverse, corresponds to GSIS play ID |
| `starting_hash` | character | hash the ball was place(L = left, M = middle, R = right) |
| `qb_location` | character | pre-snap position of quarterback(U = under center, S = shotgun, P = pistol) |
| `n_offense_backfield` | integer | number of players in the backfield at the snap |
| `n_defense_box` | integer |  |
| `is_no_huddle` | logical | no huddle |
| `is_motion` | logical | motion occurred on the play before or at the time of the snap |
| `is_play_action` | logical | play-action pass |
| `is_screen_pass` | logical | screen pass |
| `is_rpo` | logical | play is considered run-pass option |
| `is_trick_play` | logical | trick play |
| `is_qb_out_of_pocket` | logical | quarterback moved out of pocket |
| `is_interception_worthy` | logical | interception worthy pass |
| `is_throw_away` | logical | quarterback thrown away |
| `read_thrown` | character | read the ball was thrown |
| `is_catchable_ball` | logical | catchable ball(defined by throws that are generally on target that are not defended away) |
| `is_contested_ball` | logical | contested ball(defined by whether or not the receiver is facing physical contact at the time of the catch) |
| `is_created_reception` | logical | created reception(defined by a reception that only occurs due to an exceptional play by the receiver) |
| `is_drop` | logical | receiver drop |
| `is_qb_sneak` | logical | quarterback sneak |
| `n_blitzers` | integer | number of blitzers |
| `n_pass_rushers` | integer | number of pass rushers |
| `is_qb_fault_sack` | logical | sack that is the fault of the quarterback |
| `date_pulled` | character | Date the data was retrieved from the FTN Data API by nflverse jobs |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

# Multi-season range

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

# Filter to plays with motion

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### `load_nfl_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_injuries}

Load NFL injuries data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2009 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing injuries data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `week` | integer | Season week. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `position` | character | Primary position as reported by NFL.com |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `report_primary_injury` | character | Primary injury listed on official injury report |
| `report_secondary_injury` | character | Secondary injury listed on official injury report |
| `report_status` | character | Player's status for game on official injury report |
| `practice_primary_injury` | character | Primary injury listed on practice injury report |
| `practice_secondary_injury` | character | Secondary injury listed on practice injury report |
| `practice_status` | character | Player's participation in practice |
| `date_modified` | character | Date and time that injury information was updated |

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

# Multi-season range with team filter

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### `load_nfl_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_nextgen_stats}

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats
accessors. Mirrors the API surface of nflreadpy's
`load_nextgen_stats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — `seasons` is applied as a post-filter on the `season` column. |
| `stat_type` | `str` | `'passing'` | One of `"passing"`, `"rushing"`, `"receiving"`. Defaults to `"passing"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested `stat_type` and `seasons`.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_time_to_throw` | double | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | double | Average air yards on completed passes |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `avg_air_yards_differential` | double | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | double | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | double | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_touchdowns` | integer | Number of touchdowns scored on pass plays |
| `interceptions` | integer | The number of interceptions thrown. |
| `passer_rating` | double | Overall NFL passer rating |
| `completions` | integer | The number of completed passes. |
| `completion_percentage` | double | Percentage of completed passes |
| `expected_completion_percentage` | double | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | double | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | double | A receiver's average depth of target |
| `max_air_distance` | double | A receiver's maximum depth of target |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

# Rushing NextGen stats

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

# Receiving NextGen stats with a follow-up filter

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

# Pandas round-trip

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_ngs_passing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_ngs_passing}

Deprecated alias for `load_nfl_nextgen_stats(stat_type='passing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_time_to_throw` | double | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | double | Average air yards on completed passes |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `avg_air_yards_differential` | double | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | double | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | double | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_touchdowns` | integer | Number of touchdowns scored on pass plays |
| `interceptions` | integer | The number of interceptions thrown. |
| `passer_rating` | double | Overall NFL passer rating |
| `completions` | integer | The number of completed passes. |
| `completion_percentage` | double | Percentage of completed passes |
| `expected_completion_percentage` | double | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | double | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | double | A receiver's average depth of target |
| `max_air_distance` | double | A receiver's maximum depth of target |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
```

### `load_nfl_ngs_receiving(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_ngs_receiving}

Deprecated alias for `load_nfl_nextgen_stats(stat_type='receiving')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_cushion` | double | The distance (in yards) measured between a WR/TE and the defender they're lined up against at the time of snap on all targets. |
| `avg_separation` | double | The distance (in yards) measured between a WR/TE and the nearest defender at the time of catch or incompletion. |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `percent_share_of_intended_air_yards` | double | The sum of the receivers total intended air yards (all attempts) over the sum of his team's total intended air yards. Represented as a percentage, this statistic represents how much of a team's deep yards does the player account for. |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `catch_percentage` | double | Percentage of caught passes relative to targets |
| `yards` | integer | The number of receiving yards |
| `rec_touchdowns` | integer | The number of touchdown receptions |
| `avg_yac` | double | Average yards gained after catch by a receiver. |
| `avg_expected_yac` | double | Average expected yards after catch, based on numerous factors using tracking data such as how open the receiver is, how fast they're traveling, how many defenders/blockers are in space, etc |
| `avg_yac_above_expectation` | double | A receiver's YAC compared to their Expected YAC. |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
```

### `load_nfl_ngs_rushing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_ngs_rushing}

Deprecated alias for `load_nfl_nextgen_stats(stat_type='rushing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `efficiency` | double | Rushing efficiency is calculated by taking the total distance a player traveled on rushing plays as a ball carrier according to Next Gen Stats (measured in yards) per rushing yards gained. The lower the number, the more of a North/South runner. |
| `percent_attempts_gte_eight_defenders` | double | On every play, Next Gen Stats calculates how many defenders are stacked in the box at snap. Using that logic, DIB% calculates how often does a rusher see 8 or more defenders in the box against them. |
| `avg_time_to_los` | double | Next Gen Stats measures the amount of time a ball carrier spends (measured to the 10th of a second) before crossing the Line of Scrimmage. TLOS is the average time behind the LOS on all rushing plays where the player is the rusher. |
| `rush_attempts` | integer | The number of rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `avg_rush_yards` | double | AVerage rush yards gained |
| `rush_touchdowns` | integer | The number of scored rushing touchdowns |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |
| `expected_rush_yards` | double | Expected rushing yards based on Nextgenstats' Big Data Bowl model |
| `rush_yards_over_expected` | double | A rusher's rush yards gained compared to the expected rush yards |
| `rush_yards_over_expected_per_att` | double | Average rush yards above expectation |
| `rush_pct_over_expected` | double | Rushing percentage above expectation |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
```

### `load_nfl_officials(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_officials}

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character |  |
| `official_name` | character | Official name. |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character | Unique official / referee identifier. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

# Pandas round-trip

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_nfl_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_pbp}

Load NFL play by play data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the play-by-plays available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `old_game_id` | character | Legacy NFL game ID. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `away_team` | character | String abbreviation for the away team. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `posteam` | character | String abbreviation for the team with possession. |
| `posteam_type` | character | String indicating whether the posteam team is home or away. |
| `defteam` | character | String abbreviation for the team on defense. |
| `side_of_field` | character | String abbreviation for which team's side of the field the team with possession is currently on. |
| `yardline_100` | double | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `game_date` | character | Date of the game. |
| `quarter_seconds_remaining` | double | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | double | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | double | Numeric seconds remaining in the game. |
| `game_half` | character | String indicating which half the play is in, either Half1, Half2, or Overtime. |
| `quarter_end` | double | Binary indicator for whether or not the row of the data is marking the end of a quarter. |
| `drive` | double | Numeric drive number in the game. |
| `sp` | double | Binary indicator for whether or not a score occurred on the play. |
| `qtr` | double | Quarter of the game (5 is overtime). |
| `down` | double | The down for the given play. |
| `goal_to_go` | integer | Binary indicator for whether or not the posteam is in a goal down situation. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `yrdln` | character | String indicating the current field position for a given play. |
| `ydstogo` | double | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `ydsnet` | double | Numeric value for total yards gained on the given drive. |
| `desc` | character | Detailed string description for the given play. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | double | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
| `shotgun` | double | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | double | Binary indicator for whether or not the play was in no_huddle formation. |
| `qb_dropback` | double | Binary indicator for whether or not the QB dropped back on the play (pass attempt, sack, or scrambled). |
| `qb_kneel` | double | Binary indicator for whether or not the QB took a knee. |
| `qb_spike` | double | Binary indicator for whether or not the QB spiked the ball. |
| `qb_scramble` | double | Binary indicator for whether or not the QB scrambled. |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `air_yards` | double | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | double | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `run_location` | character | String indicator for location of run: left, middle, or right. |
| `run_gap` | character | String indicator for line gap of run: end, guard, or tackle |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `kick_distance` | double | Numeric distance in yards for kickoffs, field goals, and punts. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | character | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `home_timeouts_remaining` | double | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | double | Numeric timeouts remaining in the half for the away team. |
| `timeout` | double | Binary indicator for whether or not a timeout was called by either team. |
| `timeout_team` | character | String abbreviation for which team called the timeout. |
| `td_team` | character | String abbreviation for which team scored the touchdown. |
| `td_player_name` | character | String name of the player who scored a touchdown. |
| `td_player_id` | character | Unique identifier of the player who scored a touchdown. |
| `posteam_timeouts_remaining` | double | Number of timeouts remaining for the possession team. |
| `defteam_timeouts_remaining` | double | Number of timeouts remaining for the team on defense. |
| `total_home_score` | double | Score for the home team at the start of the play. |
| `total_away_score` | double | Score for the away team at the start of the play. |
| `posteam_score` | double | Score the posteam at the start of the play. |
| `defteam_score` | double | Score the defteam at the start of the play. |
| `score_differential` | double | Score differential between the posteam and defteam at the start of the play. |
| `posteam_score_post` | double | Score for the posteam at the end of the play. |
| `defteam_score_post` | double | Score for the defteam at the end of the play. |
| `score_differential_post` | double | Score differential between the posteam and defteam at the end of the play. |
| `no_score_prob` | double | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `opp_fg_prob` | double | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | double | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | double | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | double | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | double | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `td_prob` | double | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `extra_point_prob` | double | Predicted probability of the posteam scoring an extra point. |
| `two_point_conversion_prob` | double | Predicted probability of the posteam scoring the two point conversion. |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `total_home_epa` | double | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | double | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | double | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | double | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | double | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | double | Cumulative total passing EPA for the away team in the game so far. |
| `air_epa` | double | EPA from the air yards alone. For completions this represents the actual value provided through the air. For incompletions this represents the hypothetical value that could've been added through the air if the pass was completed. |
| `yac_epa` | double | EPA from the yards after catch alone. For completions this represents the actual value provided after the catch. For incompletions this represents the difference between the hypothetical air_epa and the play's raw observed EPA (how much the incomplete pass cost the posteam). |
| `comp_air_epa` | double | EPA from the air yards alone only for completions. |
| `comp_yac_epa` | double | EPA from the yards after catch alone only for completions. |
| `total_home_comp_air_epa` | double | Cumulative total completions air EPA for the home team in the game so far. |
| `total_away_comp_air_epa` | double | Cumulative total completions air EPA for the away team in the game so far. |
| `total_home_comp_yac_epa` | double | Cumulative total completions yac EPA for the home team in the game so far. |
| `total_away_comp_yac_epa` | double | Cumulative total completions yac EPA for the away team in the game so far. |
| `total_home_raw_air_epa` | double | Cumulative total raw air EPA for the home team in the game so far. |
| `total_away_raw_air_epa` | double | Cumulative total raw air EPA for the away team in the game so far. |
| `total_home_raw_yac_epa` | double | Cumulative total raw yac EPA for the home team in the game so far. |
| `total_away_raw_yac_epa` | double | Cumulative total raw yac EPA for the away team in the game so far. |
| `wp` | double | Estimated win probabiity for the posteam given the current situation at the start of the given play. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `vegas_wpa` | double | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `vegas_home_wpa` | double | Win probability added (WPA) for the home team: spread_adjusted model. |
| `home_wp_post` | double | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | double | Estimated win probability for the away team at the end of the play. |
| `vegas_wp` | double | Estimated win probabiity for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `vegas_home_wp` | double | Estimated win probability for the home team incorporating pre-game Vegas line. |
| `total_home_rush_wpa` | double | Cumulative total rushing WPA for the home team in the game so far. |
| `total_away_rush_wpa` | double | Cumulative total rushing WPA for the away team in the game so far. |
| `total_home_pass_wpa` | double | Cumulative total passing WPA for the home team in the game so far. |
| `total_away_pass_wpa` | double | Cumulative total passing WPA for the away team in the game so far. |
| `air_wpa` | double | WPA through the air (same logic as air_epa). |
| `yac_wpa` | double | WPA from yards after the catch (same logic as yac_epa). |
| `comp_air_wpa` | double | The air_wpa for completions only. |
| `comp_yac_wpa` | double | The yac_wpa for completions only. |
| `total_home_comp_air_wpa` | double | Cumulative total completions air WPA for the home team in the game so far. |
| `total_away_comp_air_wpa` | double | Cumulative total completions air WPA for the away team in the game so far. |
| `total_home_comp_yac_wpa` | double | Cumulative total completions yac WPA for the home team in the game so far. |
| `total_away_comp_yac_wpa` | double | Cumulative total completions yac WPA for the away team in the game so far. |
| `total_home_raw_air_wpa` | double | Cumulative total raw air WPA for the home team in the game so far. |
| `total_away_raw_air_wpa` | double | Cumulative total raw air WPA for the away team in the game so far. |
| `total_home_raw_yac_wpa` | double | Cumulative total raw yac WPA for the home team in the game so far. |
| `total_away_raw_yac_wpa` | double | Cumulative total raw yac WPA for the away team in the game so far. |
| `punt_blocked` | double | Binary indicator for if the punt was blocked. |
| `first_down_rush` | double | Binary indicator for if a running play converted the first down. |
| `first_down_pass` | double | Binary indicator for if a passing play converted the first down. |
| `first_down_penalty` | double | Binary indicator for if a penalty converted the first down. |
| `third_down_converted` | double | Binary indicator for if the first down was converted on third down. |
| `third_down_failed` | double | Binary indicator for if the posteam failed to convert first down on third down. |
| `fourth_down_converted` | double | Binary indicator for if the first down was converted on fourth down. |
| `fourth_down_failed` | double | Binary indicator for if the posteam failed to convert first down on fourth down. |
| `incomplete_pass` | double | Binary indicator for if the pass was incomplete. |
| `touchback` | double | Binary indicator for if a touchback occurred on the play. |
| `interception` | double | Binary indicator for if the pass was intercepted. |
| `punt_inside_twenty` | double | Binary indicator for if the punt ended inside the twenty yard line. |
| `punt_in_endzone` | double | Binary indicator for if the punt was in the endzone. |
| `punt_out_of_bounds` | double | Binary indicator for if the punt went out of bounds. |
| `punt_downed` | double | Binary indicator for if the punt was downed. |
| `punt_fair_catch` | double | Binary indicator for if the punt was caught with a fair catch. |
| `kickoff_inside_twenty` | double | Binary indicator for if the kickoff ended inside the twenty yard line. |
| `kickoff_in_endzone` | double | Binary indicator for if the kickoff was in the endzone. |
| `kickoff_out_of_bounds` | double | Binary indicator for if the kickoff went out of bounds. |
| `kickoff_downed` | double | Binary indicator for if the kickoff was downed. |
| `kickoff_fair_catch` | double | Binary indicator for if the kickoff was caught with a fair catch. |
| `fumble_forced` | double | Binary indicator for if the fumble was forced. |
| `fumble_not_forced` | double | Binary indicator for if the fumble was not forced. |
| `fumble_out_of_bounds` | double | Binary indicator for if the fumble went out of bounds. |
| `solo_tackle` | double | Binary indicator if the play had a solo tackle (could be multiple due to fumbles). |
| `safety` | double | Binary indicator for whether or not a safety occurred. |
| `penalty` | double | Binary indicator for whether or not a penalty occurred. |
| `tackled_for_loss` | double | Binary indicator for whether or not a tackle for loss on a run play occurred. |
| `fumble_lost` | double | Binary indicator for if the fumble was lost. |
| `own_kickoff_recovery` | double | Binary indicator for if the kicking team recovered the kickoff. |
| `own_kickoff_recovery_td` | double | Binary indicator for if the kicking team recovered the kickoff and scored a TD. |
| `qb_hit` | double | Binary indicator if the QB was hit on the play. |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `sack` | double | Binary indicator for if the play ended in a sack. |
| `touchdown` | double | Binary indicator for if the play resulted in a TD. |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `return_touchdown` | double | Binary indicator for if the play resulted in a return TD. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `extra_point_attempt` | double | Binary indicator for extra point attempt. |
| `two_point_attempt` | double | Binary indicator for two point conversion attempt. |
| `field_goal_attempt` | double | Binary indicator for field goal attempt. |
| `kickoff_attempt` | double | Binary indicator for kickoff. |
| `punt_attempt` | double | Binary indicator for punts. |
| `fumble` | double | Binary indicator for if a fumble occurred. |
| `complete_pass` | double | Binary indicator for if the pass was completed. |
| `assist_tackle` | double | Binary indicator for if an assist tackle occurred. |
| `lateral_reception` | double | Binary indicator for if a lateral occurred on the reception. |
| `lateral_rush` | double | Binary indicator for if a lateral occurred on a run. |
| `lateral_return` | double | Binary indicator for if a lateral occurred on a return. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `lateral_recovery` | double | Binary indicator for if a lateral occurred on a fumble recovery. |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `passer_player_name` | character | String name for the player that attempted the pass. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `receiver_player_name` | character | String name for the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `rusher_player_name` | character | String name for the player that attempted the run. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_receiver_player_id` | character | Unique identifier for the player that received the last(!) lateral on a pass play. |
| `lateral_receiver_player_name` | character | String name for the player that received the last(!) lateral on a pass play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral receiving yards. |
| `lateral_receiving_yards` | double | Numeric yards by the `lateral_receiver_player_name` in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `lateral_rusher_player_id` | character | Unique identifier for the player that received the last(!) lateral on a run play. |
| `lateral_rusher_player_name` | character | String name for the player that received the last(!) lateral on a run play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral rushing yards. |
| `lateral_rushing_yards` | double | Numeric yards by the `lateral_rusher_player_name` in run plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_sack_player_id` | character | Unique identifier for the player that received the lateral on a sack. |
| `lateral_sack_player_name` | character | String name for the player that received the lateral on a sack. |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `interception_player_name` | character | String name for the player that intercepted the pass. |
| `lateral_interception_player_id` | character | Unique indentifier for the player that received the lateral on an interception. |
| `lateral_interception_player_name` | character | String name for the player that received the lateral on an interception. |
| `punt_returner_player_id` | character | Unique identifier for the punt returner. |
| `punt_returner_player_name` | character | String name for the punt returner. |
| `lateral_punt_returner_player_id` | character | Unique identifier for the player that received the lateral on a punt return. |
| `lateral_punt_returner_player_name` | character | String name for the player that received the lateral on a punt return. |
| `kickoff_returner_player_name` | character | String name for the kickoff returner. |
| `kickoff_returner_player_id` | character | Unique identifier for the kickoff returner. |
| `lateral_kickoff_returner_player_id` | character | Unique identifier for the player that received the lateral on a kickoff return. |
| `lateral_kickoff_returner_player_name` | character | String name for the player that received the lateral on a kickoff return. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `punter_player_name` | character | String name for the punter. |
| `kicker_player_name` | character | String name for the kicker on FG or kickoff. |
| `kicker_player_id` | character | Unique identifier for the kicker on FG or kickoff. |
| `own_kickoff_recovery_player_id` | character | Unique identifier for the player that recovered their own kickoff. |
| `own_kickoff_recovery_player_name` | character | String name for the player that recovered their own kickoff. |
| `blocked_player_id` | character | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | character | String name for the player that blocked the punt or FG. |
| `tackle_for_loss_1_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_1_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `qb_hit_1_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_1_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `forced_fumble_player_1_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | character | String name of one of the players with a forced fumble. |
| `forced_fumble_player_2_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_name` | character | String name of one of the players with a forced fumble. |
| `solo_tackle_1_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_2_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_1_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_2_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_1_player_name` | character | String name of one of the players with a solo tackle. |
| `solo_tackle_2_player_name` | character | String name of one of the players with a solo tackle. |
| `assist_tackle_1_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_1_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_1_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_2_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_2_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_2_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_3_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_3_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_3_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_4_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_4_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_4_team` | character | Team of one of the players with a tackle assist. |
| `tackle_with_assist` | double | Binary indicator for if there has been a tackle with assist. |
| `tackle_with_assist_1_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_1_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_1_team` | character | Team of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_2_team` | character | Team of one of the players with a tackle with assist. |
| `pass_defense_1_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | character | String name of one of the players with a pass defense. |
| `pass_defense_2_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_2_player_name` | character | String name of one of the players with a pass defense. |
| `fumbled_1_team` | character | Team of one of the first player with a fumble. |
| `fumbled_1_player_id` | character | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | character | String name of one of the first player who fumbled on the play. |
| `fumbled_2_player_id` | character | Unique identifier of the second player who fumbled on the play. |
| `fumbled_2_player_name` | character | String name of one of the second player who fumbled on the play. |
| `fumbled_2_team` | character | Team of one of the second player with a fumble. |
| `fumble_recovery_1_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_1_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_1_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | character | String name of one of the players with a fumble recovery. |
| `fumble_recovery_2_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_2_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_2_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_2_player_name` | character | String name of one of the players with a fumble recovery. |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `sack_player_name` | character | String name of the player who recorded a solo sack. |
| `half_sack_1_player_id` | character | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | character | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | character | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | character | String name of the second player who recorded half a sack. |
| `return_team` | character | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `return_yards` | double | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `penalty_team` | character | String abbreviation of the team with the penalty. |
| `penalty_player_id` | character | Unique identifier for the player with the penalty. |
| `penalty_player_name` | character | String name for the player with the penalty. |
| `penalty_yards` | double | Yards gained (or lost) by the posteam from the penalty. |
| `replay_or_challenge` | double | Binary indicator for whether or not a replay or challenge. |
| `replay_or_challenge_result` | character | String indicating the result of the replay or challenge. |
| `penalty_type` | character | String indicating the penalty type of the first penalty in the given play. Will be `NA` if `desc` is missing the type. |
| `defensive_two_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | double | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `defensive_extra_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on an extra point attempt, this results following a blocked attempt that the defense recovers the ball. |
| `defensive_extra_point_conv` | double | Binary indicator whether or not the defense successfully scored on an extra point attempt. |
| `safety_player_name` | character | String name for the player who scored a safety. |
| `safety_player_id` | character | Unique identifier for the player who scored a safety. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `cp` | double | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | double | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `series` | double | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_success` | double | 1: scored touchdown, gained enough yards for first down. |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `order_sequence` | double | Column provided by NFL to fix out-of-order plays. Available 2011 and beyond with source "nfl". |
| `start_time` | character | Kickoff time in eastern time zone. |
| `time_of_day` | character | Time of day of play in UTC "HH:MM:SS" format. Available 2011 and beyond with source "nfl". |
| `stadium` | character | Name of the stadium |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `nfl_api_id` | character | UUID of the game in the new NFL API. |
| `play_clock` | character | Time on the playclock when the ball was snapped. |
| `play_deleted` | double | Binary indicator for deleted plays. |
| `play_type_nfl` | character | Play type as listed in the NFL source. Slightly different to the regular play_type variable. |
| `special_teams_play` | double | Binary indicator for whether play is special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `st_play_type` | character | Type of special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `end_clock_time` | character | Game time at the end of a given play. |
| `end_yard_line` | character | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `fixed_drive` | double | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `drive_real_start_time` | character | Local day time when the drive started (currently not used by the NFL and therefore mostly 'NA'). |
| `drive_play_count` | double | Numeric value of how many regular plays happened in a given drive. |
| `drive_time_of_possession` | character | Time of possession in a given drive. |
| `drive_first_downs` | double | Number of first downs in a given drive. |
| `drive_inside20` | double | Binary indicator if the offense was able to get inside the opponents 20 yard line. |
| `drive_ended_with_score` | double | Binary indicator the drive ended with a score. |
| `drive_quarter_start` | double | Numeric value indicating in which quarter the given drive has started. |
| `drive_quarter_end` | double | Numeric value indicating in which quarter the given drive has ended. |
| `drive_yards_penalized` | double | Numeric value of how many yards the offense gained or lost through penalties in the given drive. |
| `drive_start_transition` | character | String indicating how the offense got the ball. |
| `drive_end_transition` | character | String indicating how the offense lost the ball. |
| `drive_game_clock_start` | character | Game time at the beginning of a given drive. |
| `drive_game_clock_end` | character | Game time at the end of a given drive. |
| `drive_start_yard_line` | character | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | character | String indicating where a given drive ended consisting of team half and yard line number. |
| `drive_play_id_started` | double | Play_id of the first play in the given drive. |
| `drive_play_id_ended` | double | Play_id of the last play in the given drive. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `game_stadium` | character | Name of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `aborted_play` | double | Binary indicator if the play description indicates "Aborted". |
| `success` | double | Binary indicator wheter epa > 0 in the given play. |
| `passer` | character | Name of the dropback player (scrambles included) including plays with penalties. |
| `passer_jersey_number` | integer | Jersey number of the passer. |
| `rusher` | character | Name of the rusher (no scrambles) including plays with penalties. |
| `rusher_jersey_number` | integer | Jersey number of the rusher. |
| `receiver` | character | Name of the receiver including plays with penalties. |
| `receiver_jersey_number` | integer | Jersey number of the receiver. |
| `pass` | double | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | double | Binary indicator if the play was a rushing play. |
| `first_down` | double | Binary indicator if the play ended in a first down. |
| `special` | double | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `play` | double | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `passer_id` | character | ID of the player in the 'passer' column. |
| `rusher_id` | character | ID of the player in the 'rusher' column. |
| `receiver_id` | character | ID of the player in the 'receiver' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `id` | character | ID of the player in the 'name' column. |
| `fantasy_player_name` | character | Name of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy_player_id` | character | ID of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy` | character | Name of the rusher on rush plays or receiver on pass plays. |
| `fantasy_id` | character | ID of the rusher on rush plays or receiver on pass plays. |
| `out_of_bounds` | double | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `home_opening_kickoff` | double | 1 if the home team received the opening kickoff, 0 otherwise. |
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `xyac_epa` | double | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | double | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | integer | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | double | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | double | Probability play earns a first down based on where the ball was caught. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

# Multi-season range

pbp = load_nfl_pbp(seasons=range(2020, 2025))

# With cache off (development workflow)

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

# Pandas round-trip

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

### `load_nfl_pbp_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_pbp_participation}

Load NFL play-by-play participation data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2016 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing play-by-play participation data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `old_game_id` | character | Legacy NFL game ID. |
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `possession_team` | character | String abbreviation for the team with possession. |
| `offense_formation` | character | Formation the offense lines up in to snap the ball. |
| `offense_personnel` | character | The positions of the offensive personnel lined up on the field for a play. |
| `defenders_in_box` | integer | Number of defensive players lined up in the box at the snap. |
| `defense_personnel` | character | The positions of the defensive personnel lined up on the field for a play. |
| `number_of_pass_rushers` | integer | Number of defensive player who rushed the passer. |
| `players_on_play` | character | A list of every player on the field for the play, by gsis_id |
| `offense_players` | character | A list of every offensive player on the field for the play, by gsis_id |
| `defense_players` | character | A list of every defensive player on the field for the play, by gsis_id |
| `n_offense` | integer | Number of offensive players on the field for the play |
| `n_defense` | integer | Number of defensive players on the field for the play |
| `ngs_air_yards` | double | Legacy column. For 2023 and prior years, reflects the distance (in yards) that the ball traveled in the air on a given passing play as tracked by NGS. Is NA for 2024 on--we advise instead using the air_yards column from nflreadr::load_pbp() moving forward. |
| `time_to_throw` | double | Duration (in seconds) between the time of the ball being snapped and the time of release of a pass attempt |
| `was_pressure` | logical | A boolean indicating whether or not the QB was pressured on a play |
| `route` | character | A string indicating the route the primary receiver on a play took. Has the following possible values: "CORNER", "DEEP OUT", "GO", "HITCH/CURL", "IN/DIG", "POST", "QUICK OUT", "SCREEN", "SHALLOW CROSS/DRAG", "SLANT", "SWING", "TEXAS/ANGLE", "WHEEL". |
| `defense_man_zone_type` | character | A string indicating whether the defense was in man or zone coverage on a play |
| `defense_coverage_type` | character | A string indicating what type of cover the defense was in on a play. Has one of the following values: "COVER_0", "COVER_1", "COVER_2", "2_MAN", "COVER_3", "COVER_4", "COVER_6", "COVER_9", "COMBO", "BLOWN". |
| `offense_names` | character | A string listing all of the names of offensive players in the order of their gsis_ids in offense_players. |
| `defense_names` | character | A string listing all of the names of defensive players in the order of their gsis_ids in defense_players. |
| `offense_positions` | character | A string listing all of the positions of offensive players in the order of their gsis_ids in offense_players. |
| `defense_positions` | character | A string listing all of the positions of defensive players in the order of their gsis_ids in defense_players. |
| `offense_numbers` | character | A string listing all of the numbers of offensive players in the order of their gsis_ids in offense_players. |
| `defense_numbers` | character | A string listing all of the numbers of defensive players in the order of their gsis_ids in defense_players. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

# Multi-season range

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### `load_nfl_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_advstats}

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level
PFR advstats accessors. Mirrors the API surface of nflreadpy's
`load_pfr_advstats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For `summary_level='week'` this drives the per-season parquet fan-out; for `summary_level='season'` it post-filters the combined parquet by the `season` column. |
| `stat_type` | `str` | `'pass'` | One of `"pass"`, `"rush"`, `"rec"`, `"def"`. Defaults to `"pass"`. |
| `summary_level` | `str` | `'week'` | One of `"week"` or `"season"`. Defaults to `"week"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested `stat_type`, `summary_level`, and `seasons`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double |  |
| `passing_drop_pct` | double |  |
| `receiving_drop` | double |  |
| `receiving_drop_pct` | double |  |
| `passing_bad_throws` | double |  |
| `passing_bad_throw_pct` | double |  |
| `times_sacked` | double |  |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double |  |
| `def_times_blitzed` | double |  |
| `def_times_hurried` | double |  |
| `def_times_hitqb` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

# Season-level rushing summaries (one row per player per season)

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

# Defensive stats with a follow-up filter

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

# Pandas round-trip

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_nfl_pfr_def(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_def}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double | Goals (skaters). |
| `gs` | double |  |
| `int` | double | Binary flag for an interception. |
| `tgt` | double |  |
| `cmp` | double |  |
| `cmp_percent` | double |  |
| `yds` | double |  |
| `yds_cmp` | double |  |
| `yds_tgt` | double |  |
| `td` | double |  |
| `rat` | double |  |
| `dadot` | double |  |
| `air` | double |  |
| `yac` | double |  |
| `bltz` | double |  |
| `hrry` | double |  |
| `qbkd` | double |  |
| `sk` | double |  |
| `prss` | double |  |
| `comb` | double |  |
| `m_tkl` | double |  |
| `m_tkl_percent` | double |  |
| `loaded` | character |  |
| `bats` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="season"
)
```

### `load_nfl_pfr_pass(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_pass}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `pass_attempts` | double | Career pass attempts |
| `throwaways` | double | Throwaways |
| `spikes` | double | Spikes |
| `drops` | double | Throws dropped |
| `drop_pct` | double | Percent of throws dropped |
| `bad_throws` | double | Bad throws |
| `bad_throw_pct` | double | Percent of throws that were bad |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pocket_time` | double | Average time in pocket |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `pressure_pct` | double | Percent of the time pressured |
| `batted_balls` | double | Batted balls |
| `on_tgt_throws` | double | On target throws |
| `on_tgt_pct` | double | Percent of throws on target |
| `rpo_plays` | double | Number of RPO plays |
| `rpo_yards` | double | Yards on RPOs |
| `rpo_pass_att` | double | Number of pass attempts on RPOs |
| `rpo_pass_yards` | double | Passing yards on RPOs |
| `rpo_rush_att` | double | Rush attempts on RPOs |
| `rpo_rush_yards` | double | Rushing yards on RPOs |
| `pa_pass_att` | double | Play action pass attempts |
| `pa_pass_yards` | double | Play action passing yards |
| `intended_air_yards` | double |  |
| `intended_air_yards_per_pass_attempt` | double |  |
| `completed_air_yards` | double |  |
| `completed_air_yards_per_completion` | double |  |
| `completed_air_yards_per_pass_attempt` | double |  |
| `pass_yards_after_catch` | double |  |
| `pass_yards_after_catch_per_completion` | double |  |
| `scrambles` | double |  |
| `scramble_yards_per_attempt` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="season"
)
```

### `load_nfl_pfr_rec(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_rec}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double | Goals (skaters). |
| `gs` | double |  |
| `tgt` | double |  |
| `rec` | double |  |
| `yds` | double |  |
| `td` | double |  |
| `x1d` | double |  |
| `ybc` | double |  |
| `ybc_r` | double |  |
| `yac` | double |  |
| `yac_r` | double |  |
| `adot` | double |  |
| `brk_tkl` | double |  |
| `rec_br` | double |  |
| `drop` | double |  |
| `drop_percent` | double |  |
| `int` | double | Binary flag for an interception. |
| `rat` | double |  |
| `loaded` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="season"
)
```

### `load_nfl_pfr_rush(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_rush}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double | Goals (skaters). |
| `gs` | double |  |
| `att` | double |  |
| `yds` | double |  |
| `td` | double |  |
| `x1d` | double |  |
| `ybc` | double |  |
| `ybc_att` | double |  |
| `yac` | double |  |
| `yac_att` | double |  |
| `brk_tkl` | double |  |
| `att_br` | double |  |
| `loaded` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)
```

### `load_nfl_pfr_weekly_def(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_def}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `def_ints` | double | Career interceptions |
| `def_targets` | double |  |
| `def_completions_allowed` | double |  |
| `def_completion_pct` | double |  |
| `def_yards_allowed` | double |  |
| `def_yards_allowed_per_cmp` | double |  |
| `def_yards_allowed_per_tgt` | double |  |
| `def_receiving_td_allowed` | double |  |
| `def_passer_rating_allowed` | double |  |
| `def_adot` | double |  |
| `def_air_yards_completed` | double |  |
| `def_yards_after_catch` | double |  |
| `def_times_blitzed` | double |  |
| `def_times_hurried` | double |  |
| `def_times_hitqb` | double |  |
| `def_sacks` | double | Number of sacks form this player |
| `def_pressures` | double |  |
| `def_tackles_combined` | double |  |
| `def_missed_tackles` | double |  |
| `def_missed_tackle_pct` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="week"
)
```

### `load_nfl_pfr_weekly_pass(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_pass}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double |  |
| `passing_drop_pct` | double |  |
| `receiving_drop` | double |  |
| `receiving_drop_pct` | double |  |
| `passing_bad_throws` | double |  |
| `passing_bad_throw_pct` | double |  |
| `times_sacked` | double |  |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double |  |
| `def_times_blitzed` | double |  |
| `def_times_hurried` | double |  |
| `def_times_hitqb` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)
```

### `load_nfl_pfr_weekly_rec(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_rec}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `rushing_broken_tackles` | double |  |
| `receiving_broken_tackles` | double |  |
| `passing_drops` | double |  |
| `passing_drop_pct` | double |  |
| `receiving_drop` | double |  |
| `receiving_drop_pct` | double |  |
| `receiving_int` | double |  |
| `receiving_rat` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="week"
)
```

### `load_nfl_pfr_weekly_rush(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_rush}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `carries` | double | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards_before_contact` | double |  |
| `rushing_yards_before_contact_avg` | double |  |
| `rushing_yards_after_contact` | double |  |
| `rushing_yards_after_contact_avg` | double |  |
| `rushing_broken_tackles` | double |  |
| `receiving_broken_tackles` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="week"
)
```

### `load_nfl_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_player_stats}

Load NFL player stats data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing player stats.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | character | Most recent team player appears in `pbp` with. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character |  |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `interceptions` | double | The number of interceptions thrown. |
| `sacks` | double | The Number of times sacked. |
| `sack_yards` | double | Yards lost on sack plays. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | double | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `pacr` | double | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | double | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | double | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | double | The number of rushes with a lost fumble. |
| `rushing_first_downs` | double | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | double | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | double | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | double | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `racr` | double | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | double | "Player's share of team receiving targets in this game" |
| `air_yards_share` | double | Player's share of the team's air yards in this game |
| `wopr` | double | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | double | Total number of kick/punt return touchdowns |
| `fantasy_points` | double | Standard fantasy points. |
| `fantasy_points_ppr` | double | PPR fantasy points. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

# Kicking-only stats

kicking = load_nfl_player_stats(kicking=True)

# Filter to a single season after load

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_nfl_players(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_players}

Load NFL Player ID information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing players available.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character |  |
| `official_name` | character | Official name. |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character | Unique official / referee identifier. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
players.shape

# Pandas round-trip

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()
```

### `load_nfl_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_rosters}

Load NFL roster data for all seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1920 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing rosters available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `depth_chart_position` | character | Position assigned on depth chart. Not always accurate! |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `status` | character | Status label. |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `height` | double | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `rotowire_id` | character | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `fantasy_data_id` | character | FantasyData ID - usual format five digit integer |
| `sleeper_id` | character | Sleeper ID - usually an integer with ~4 digits. |
| `years_exp` | integer | Years played in league |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `status_description_abbr` | character | A code corresponding to a particular NFL status. |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `gsis_it_id` | character | Player ID for the GSIS IT API |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `entry_year` | integer | The year a player first became eligible to play in the NFL. |
| `rookie_year` | integer | The year a player lost their rookie eligibility. |
| `draft_club` | character | The team that originally drafted a player. NA if a player went undrafted in their draft-eligible year. |
| `draft_number` | integer | The number pick that was used to select a given player. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

# Multi-season range

rosters = load_nfl_rosters(seasons=range(2020, 2025))

# Filter to a single team

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")
```

### `load_nfl_schedule(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_schedule}

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `gameday` | character | The date on which the game occurred. |
| `weekday` | character | The day of the week on which the game occcured. |
| `gametime` | character | The kickoff time of the game. This is represented in 24-hour time and the Eastern time zone, regardless of what time zone the game was being played in. |
| `away_team` | character | String abbreviation for the away team. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `overtime` | integer | Binary indicator of whether or not game went to overtime. |
| `old_game_id` | character | Legacy NFL game ID. |
| `gsis` | integer | The id of the game issued by the NFL Game Statistics & Information System. |
| `nfl_detail_id` | character | The id of the game issued by NFL Detail. |
| `pfr` | character | The id of the game issued by [Pro-Football-Reference](https://www.pro-football-reference.com/) |
| `pff` | integer | The id of the game issued by [Pro Football Focus](https://www.pff.com/) |
| `espn` | character | The id of the game issued by [ESPN](https://www.espn.com/) |
| `ftn` | integer |  |
| `away_rest` | integer | Days of rest that the away team is coming off of. |
| `home_rest` | integer | Days of rest that the home team is coming off of. |
| `away_moneyline` | integer | Odds for away team to win the game. |
| `home_moneyline` | integer | Odds for home team to win the game. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `away_spread_odds` | integer | Odds for away team to cover the spread. |
| `home_spread_odds` | integer | Odds for home team to cover the spread. |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `under_odds` | integer | Odds that total score of game would be under the total_line. |
| `over_odds` | integer | Odds that total score of game would be over the total_ine. |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `away_qb_id` | character | GSIS Player ID for away team starting quarterback. |
| `home_qb_id` | character | GSIS Player ID for home team starting quarterback. |
| `away_qb_name` | character | Name of away team starting QB. |
| `home_qb_name` | character | Name of home team starting QB. |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `referee` | character | Name of the game's referee (head official) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium` | character | Name of the stadium |

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

# Multi-season range

schedule = load_nfl_schedule(seasons=range(2020, 2025))

# Filter to a single week

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

# Pandas round-trip

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_nfl_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_snap_counts}

Load NFL snap counts data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2012 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing snap counts available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `player` | character | Player name |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `offense_snaps` | double | Number of snaps on offense |
| `offense_pct` | double | Percent of offensive snaps taken |
| `defense_snaps` | double | Number of snaps on defense |
| `defense_pct` | double | Percent of defensive snaps taken |
| `st_snaps` | double | Number of snaps on special teams |
| `st_pct` | double | Percent of special teams snaps taken |

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

# Multi-season range with offense-only filter

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

### `load_nfl_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_team_stats}

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing team stats available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character |  |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `passing_interceptions` | integer |  |
| `sacks_suffered` | integer |  |
| `sack_yards_lost` | integer |  |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | integer | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | integer | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | double |  |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | integer | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | integer | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | integer | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | integer | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | integer | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | integer | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `special_teams_tds` | integer | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | integer | Total number of solo tackles for this player |
| `def_tackles_with_assist` | integer | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | integer | Number of assisted tackles for this player |
| `def_tackles_for_loss` | integer | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | integer | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | integer | Number of times a fumble was forced from this player |
| `def_sacks` | double | Number of sacks form this player |
| `def_sack_yards` | double | Yards lost from sacks forced by this player |
| `def_qb_hits` | integer | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | integer | Number of interceptions forced by this player |
| `def_interception_yards` | integer | yards gained/lost by interception returns from this player |
| `def_pass_defended` | integer | Number of passes defended/broken up by this player |
| `def_tds` | integer | Number of defensive touchdowns scored by this player |
| `def_fumbles` | integer | Number of fumbles by this player |
| `def_safeties` | integer |  |
| `misc_yards` | integer |  |
| `fumble_recovery_own` | integer |  |
| `fumble_recovery_yards_own` | integer |  |
| `fumble_recovery_opp` | integer |  |
| `fumble_recovery_yards_opp` | integer |  |
| `fumble_recovery_tds` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer |  |
| `punt_returns` | integer | Number of punt returns. |
| `punt_return_yards` | integer | Team punt return yards. |
| `kickoff_returns` | integer |  |
| `kickoff_return_yards` | integer |  |
| `fg_made` | integer | TRUE when the field goal attempt was successful. |
| `fg_att` | integer |  |
| `fg_missed` | integer |  |
| `fg_blocked` | integer |  |
| `fg_long` | integer |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg_made_0_19` | integer |  |
| `fg_made_20_29` | integer |  |
| `fg_made_30_39` | integer |  |
| `fg_made_40_49` | integer |  |
| `fg_made_50_59` | integer |  |
| `fg_made_60_` | integer |  |
| `fg_missed_0_19` | integer |  |
| `fg_missed_20_29` | integer |  |
| `fg_missed_30_39` | integer |  |
| `fg_missed_40_49` | integer |  |
| `fg_missed_50_59` | integer |  |
| `fg_missed_60_` | integer |  |
| `fg_made_list` | character |  |
| `fg_missed_list` | character |  |
| `fg_blocked_list` | character |  |
| `fg_made_distance` | integer |  |
| `fg_missed_distance` | integer |  |
| `fg_blocked_distance` | integer |  |
| `pat_made` | integer |  |
| `pat_att` | integer |  |
| `pat_missed` | integer |  |
| `pat_blocked` | integer |  |
| `pat_pct` | double |  |
| `gwfg_made` | integer |  |
| `gwfg_att` | integer |  |
| `gwfg_missed` | integer |  |
| `gwfg_blocked` | integer |  |
| `gwfg_distance` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

# Regular-season-only team stats

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

# Combined regular + post-season at season grain

combined = load_nfl_team_stats(seasons=[2023, 2024], summary_level="reg+post")
```

### `load_nfl_teams(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_teams}

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_abbr` | character | Official team abbreveation |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `team_nick` | character |  |
| `team_conf` | character |  |
| `team_division` | character |  |
| `team_color` | character | Team primary color (hex without leading '#'). |
| `team_color2` | character |  |
| `team_color3` | character |  |
| `team_color4` | character |  |
| `team_logo_wikipedia` | character |  |
| `team_logo_espn` | character |  |
| `team_wordmark` | character |  |
| `team_conference_logo` | character |  |
| `team_league_logo` | character |  |
| `team_logo_squared` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_nfl_trades(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_trades}

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

| col_name | type | description |
|---|---|---|
| `trade_id` | integer | ID of Trade |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `trade_date` | character | Exact date that trade occurred |
| `gave` | character | Team that gave pick/player in row |
| `received` | character | Team that received pick/player in row |
| `pick_season` | integer | Draft in which traded pick was in |
| `pick_round` | integer | Round in which traded pick was in |
| `pick_number` | integer | Pick number of traded pick |
| `conditional` | integer | Binary indicator of whether or not traded pick was conditional |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pfr_name` | character | Full name of traded player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

# Filter to a single season

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

### `load_nfl_weekly_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_weekly_rosters}

Load NFL weekly roster data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2002 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing weekly rosters available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `depth_chart_position` | character | Position assigned on depth chart. Not always accurate! |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `status` | character | Status label. |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `height` | double | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `rotowire_id` | character | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `fantasy_data_id` | character | FantasyData ID - usual format five digit integer |
| `sleeper_id` | character | Sleeper ID - usually an integer with ~4 digits. |
| `years_exp` | integer | Years played in league |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `status_description_abbr` | character | A code corresponding to a particular NFL status. |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `gsis_it_id` | character | Player ID for the GSIS IT API |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `entry_year` | integer | The year a player first became eligible to play in the NFL. |
| `rookie_year` | integer | The year a player lost their rookie eligibility. |
| `draft_club` | character | The team that originally drafted a player. NA if a player went undrafted in their draft-eligible year. |
| `draft_number` | integer | The number pick that was used to select a given player. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

# Multi-season range with a follow-up week filter

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### `load_officials(return_as_pandas=False) -> 'pl.DataFrame'` {#load_officials}

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character |  |
| `official_name` | character | Official name. |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character | Unique official / referee identifier. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

# Pandas round-trip

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_participation}

Load NFL play-by-play participation data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2016 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing play-by-play participation data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `old_game_id` | character | Legacy NFL game ID. |
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `possession_team` | character | String abbreviation for the team with possession. |
| `offense_formation` | character | Formation the offense lines up in to snap the ball. |
| `offense_personnel` | character | The positions of the offensive personnel lined up on the field for a play. |
| `defenders_in_box` | integer | Number of defensive players lined up in the box at the snap. |
| `defense_personnel` | character | The positions of the defensive personnel lined up on the field for a play. |
| `number_of_pass_rushers` | integer | Number of defensive player who rushed the passer. |
| `players_on_play` | character | A list of every player on the field for the play, by gsis_id |
| `offense_players` | character | A list of every offensive player on the field for the play, by gsis_id |
| `defense_players` | character | A list of every defensive player on the field for the play, by gsis_id |
| `n_offense` | integer | Number of offensive players on the field for the play |
| `n_defense` | integer | Number of defensive players on the field for the play |
| `ngs_air_yards` | double | Legacy column. For 2023 and prior years, reflects the distance (in yards) that the ball traveled in the air on a given passing play as tracked by NGS. Is NA for 2024 on--we advise instead using the air_yards column from nflreadr::load_pbp() moving forward. |
| `time_to_throw` | double | Duration (in seconds) between the time of the ball being snapped and the time of release of a pass attempt |
| `was_pressure` | logical | A boolean indicating whether or not the QB was pressured on a play |
| `route` | character | A string indicating the route the primary receiver on a play took. Has the following possible values: "CORNER", "DEEP OUT", "GO", "HITCH/CURL", "IN/DIG", "POST", "QUICK OUT", "SCREEN", "SHALLOW CROSS/DRAG", "SLANT", "SWING", "TEXAS/ANGLE", "WHEEL". |
| `defense_man_zone_type` | character | A string indicating whether the defense was in man or zone coverage on a play |
| `defense_coverage_type` | character | A string indicating what type of cover the defense was in on a play. Has one of the following values: "COVER_0", "COVER_1", "COVER_2", "2_MAN", "COVER_3", "COVER_4", "COVER_6", "COVER_9", "COMBO", "BLOWN". |
| `offense_names` | character | A string listing all of the names of offensive players in the order of their gsis_ids in offense_players. |
| `defense_names` | character | A string listing all of the names of defensive players in the order of their gsis_ids in defense_players. |
| `offense_positions` | character | A string listing all of the positions of offensive players in the order of their gsis_ids in offense_players. |
| `defense_positions` | character | A string listing all of the positions of defensive players in the order of their gsis_ids in defense_players. |
| `offense_numbers` | character | A string listing all of the numbers of offensive players in the order of their gsis_ids in offense_players. |
| `defense_numbers` | character | A string listing all of the numbers of defensive players in the order of their gsis_ids in defense_players. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

# Multi-season range

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### `load_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_pbp}

Load NFL play by play data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the play-by-plays available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `old_game_id` | character | Legacy NFL game ID. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `away_team` | character | String abbreviation for the away team. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `posteam` | character | String abbreviation for the team with possession. |
| `posteam_type` | character | String indicating whether the posteam team is home or away. |
| `defteam` | character | String abbreviation for the team on defense. |
| `side_of_field` | character | String abbreviation for which team's side of the field the team with possession is currently on. |
| `yardline_100` | double | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `game_date` | character | Date of the game. |
| `quarter_seconds_remaining` | double | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | double | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | double | Numeric seconds remaining in the game. |
| `game_half` | character | String indicating which half the play is in, either Half1, Half2, or Overtime. |
| `quarter_end` | double | Binary indicator for whether or not the row of the data is marking the end of a quarter. |
| `drive` | double | Numeric drive number in the game. |
| `sp` | double | Binary indicator for whether or not a score occurred on the play. |
| `qtr` | double | Quarter of the game (5 is overtime). |
| `down` | double | The down for the given play. |
| `goal_to_go` | integer | Binary indicator for whether or not the posteam is in a goal down situation. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `yrdln` | character | String indicating the current field position for a given play. |
| `ydstogo` | double | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `ydsnet` | double | Numeric value for total yards gained on the given drive. |
| `desc` | character | Detailed string description for the given play. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | double | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
| `shotgun` | double | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | double | Binary indicator for whether or not the play was in no_huddle formation. |
| `qb_dropback` | double | Binary indicator for whether or not the QB dropped back on the play (pass attempt, sack, or scrambled). |
| `qb_kneel` | double | Binary indicator for whether or not the QB took a knee. |
| `qb_spike` | double | Binary indicator for whether or not the QB spiked the ball. |
| `qb_scramble` | double | Binary indicator for whether or not the QB scrambled. |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `air_yards` | double | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | double | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `run_location` | character | String indicator for location of run: left, middle, or right. |
| `run_gap` | character | String indicator for line gap of run: end, guard, or tackle |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `kick_distance` | double | Numeric distance in yards for kickoffs, field goals, and punts. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | character | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `home_timeouts_remaining` | double | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | double | Numeric timeouts remaining in the half for the away team. |
| `timeout` | double | Binary indicator for whether or not a timeout was called by either team. |
| `timeout_team` | character | String abbreviation for which team called the timeout. |
| `td_team` | character | String abbreviation for which team scored the touchdown. |
| `td_player_name` | character | String name of the player who scored a touchdown. |
| `td_player_id` | character | Unique identifier of the player who scored a touchdown. |
| `posteam_timeouts_remaining` | double | Number of timeouts remaining for the possession team. |
| `defteam_timeouts_remaining` | double | Number of timeouts remaining for the team on defense. |
| `total_home_score` | double | Score for the home team at the start of the play. |
| `total_away_score` | double | Score for the away team at the start of the play. |
| `posteam_score` | double | Score the posteam at the start of the play. |
| `defteam_score` | double | Score the defteam at the start of the play. |
| `score_differential` | double | Score differential between the posteam and defteam at the start of the play. |
| `posteam_score_post` | double | Score for the posteam at the end of the play. |
| `defteam_score_post` | double | Score for the defteam at the end of the play. |
| `score_differential_post` | double | Score differential between the posteam and defteam at the end of the play. |
| `no_score_prob` | double | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `opp_fg_prob` | double | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | double | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | double | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | double | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | double | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `td_prob` | double | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `extra_point_prob` | double | Predicted probability of the posteam scoring an extra point. |
| `two_point_conversion_prob` | double | Predicted probability of the posteam scoring the two point conversion. |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `total_home_epa` | double | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | double | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | double | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | double | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | double | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | double | Cumulative total passing EPA for the away team in the game so far. |
| `air_epa` | double | EPA from the air yards alone. For completions this represents the actual value provided through the air. For incompletions this represents the hypothetical value that could've been added through the air if the pass was completed. |
| `yac_epa` | double | EPA from the yards after catch alone. For completions this represents the actual value provided after the catch. For incompletions this represents the difference between the hypothetical air_epa and the play's raw observed EPA (how much the incomplete pass cost the posteam). |
| `comp_air_epa` | double | EPA from the air yards alone only for completions. |
| `comp_yac_epa` | double | EPA from the yards after catch alone only for completions. |
| `total_home_comp_air_epa` | double | Cumulative total completions air EPA for the home team in the game so far. |
| `total_away_comp_air_epa` | double | Cumulative total completions air EPA for the away team in the game so far. |
| `total_home_comp_yac_epa` | double | Cumulative total completions yac EPA for the home team in the game so far. |
| `total_away_comp_yac_epa` | double | Cumulative total completions yac EPA for the away team in the game so far. |
| `total_home_raw_air_epa` | double | Cumulative total raw air EPA for the home team in the game so far. |
| `total_away_raw_air_epa` | double | Cumulative total raw air EPA for the away team in the game so far. |
| `total_home_raw_yac_epa` | double | Cumulative total raw yac EPA for the home team in the game so far. |
| `total_away_raw_yac_epa` | double | Cumulative total raw yac EPA for the away team in the game so far. |
| `wp` | double | Estimated win probabiity for the posteam given the current situation at the start of the given play. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `vegas_wpa` | double | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `vegas_home_wpa` | double | Win probability added (WPA) for the home team: spread_adjusted model. |
| `home_wp_post` | double | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | double | Estimated win probability for the away team at the end of the play. |
| `vegas_wp` | double | Estimated win probabiity for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `vegas_home_wp` | double | Estimated win probability for the home team incorporating pre-game Vegas line. |
| `total_home_rush_wpa` | double | Cumulative total rushing WPA for the home team in the game so far. |
| `total_away_rush_wpa` | double | Cumulative total rushing WPA for the away team in the game so far. |
| `total_home_pass_wpa` | double | Cumulative total passing WPA for the home team in the game so far. |
| `total_away_pass_wpa` | double | Cumulative total passing WPA for the away team in the game so far. |
| `air_wpa` | double | WPA through the air (same logic as air_epa). |
| `yac_wpa` | double | WPA from yards after the catch (same logic as yac_epa). |
| `comp_air_wpa` | double | The air_wpa for completions only. |
| `comp_yac_wpa` | double | The yac_wpa for completions only. |
| `total_home_comp_air_wpa` | double | Cumulative total completions air WPA for the home team in the game so far. |
| `total_away_comp_air_wpa` | double | Cumulative total completions air WPA for the away team in the game so far. |
| `total_home_comp_yac_wpa` | double | Cumulative total completions yac WPA for the home team in the game so far. |
| `total_away_comp_yac_wpa` | double | Cumulative total completions yac WPA for the away team in the game so far. |
| `total_home_raw_air_wpa` | double | Cumulative total raw air WPA for the home team in the game so far. |
| `total_away_raw_air_wpa` | double | Cumulative total raw air WPA for the away team in the game so far. |
| `total_home_raw_yac_wpa` | double | Cumulative total raw yac WPA for the home team in the game so far. |
| `total_away_raw_yac_wpa` | double | Cumulative total raw yac WPA for the away team in the game so far. |
| `punt_blocked` | double | Binary indicator for if the punt was blocked. |
| `first_down_rush` | double | Binary indicator for if a running play converted the first down. |
| `first_down_pass` | double | Binary indicator for if a passing play converted the first down. |
| `first_down_penalty` | double | Binary indicator for if a penalty converted the first down. |
| `third_down_converted` | double | Binary indicator for if the first down was converted on third down. |
| `third_down_failed` | double | Binary indicator for if the posteam failed to convert first down on third down. |
| `fourth_down_converted` | double | Binary indicator for if the first down was converted on fourth down. |
| `fourth_down_failed` | double | Binary indicator for if the posteam failed to convert first down on fourth down. |
| `incomplete_pass` | double | Binary indicator for if the pass was incomplete. |
| `touchback` | double | Binary indicator for if a touchback occurred on the play. |
| `interception` | double | Binary indicator for if the pass was intercepted. |
| `punt_inside_twenty` | double | Binary indicator for if the punt ended inside the twenty yard line. |
| `punt_in_endzone` | double | Binary indicator for if the punt was in the endzone. |
| `punt_out_of_bounds` | double | Binary indicator for if the punt went out of bounds. |
| `punt_downed` | double | Binary indicator for if the punt was downed. |
| `punt_fair_catch` | double | Binary indicator for if the punt was caught with a fair catch. |
| `kickoff_inside_twenty` | double | Binary indicator for if the kickoff ended inside the twenty yard line. |
| `kickoff_in_endzone` | double | Binary indicator for if the kickoff was in the endzone. |
| `kickoff_out_of_bounds` | double | Binary indicator for if the kickoff went out of bounds. |
| `kickoff_downed` | double | Binary indicator for if the kickoff was downed. |
| `kickoff_fair_catch` | double | Binary indicator for if the kickoff was caught with a fair catch. |
| `fumble_forced` | double | Binary indicator for if the fumble was forced. |
| `fumble_not_forced` | double | Binary indicator for if the fumble was not forced. |
| `fumble_out_of_bounds` | double | Binary indicator for if the fumble went out of bounds. |
| `solo_tackle` | double | Binary indicator if the play had a solo tackle (could be multiple due to fumbles). |
| `safety` | double | Binary indicator for whether or not a safety occurred. |
| `penalty` | double | Binary indicator for whether or not a penalty occurred. |
| `tackled_for_loss` | double | Binary indicator for whether or not a tackle for loss on a run play occurred. |
| `fumble_lost` | double | Binary indicator for if the fumble was lost. |
| `own_kickoff_recovery` | double | Binary indicator for if the kicking team recovered the kickoff. |
| `own_kickoff_recovery_td` | double | Binary indicator for if the kicking team recovered the kickoff and scored a TD. |
| `qb_hit` | double | Binary indicator if the QB was hit on the play. |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `sack` | double | Binary indicator for if the play ended in a sack. |
| `touchdown` | double | Binary indicator for if the play resulted in a TD. |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `return_touchdown` | double | Binary indicator for if the play resulted in a return TD. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `extra_point_attempt` | double | Binary indicator for extra point attempt. |
| `two_point_attempt` | double | Binary indicator for two point conversion attempt. |
| `field_goal_attempt` | double | Binary indicator for field goal attempt. |
| `kickoff_attempt` | double | Binary indicator for kickoff. |
| `punt_attempt` | double | Binary indicator for punts. |
| `fumble` | double | Binary indicator for if a fumble occurred. |
| `complete_pass` | double | Binary indicator for if the pass was completed. |
| `assist_tackle` | double | Binary indicator for if an assist tackle occurred. |
| `lateral_reception` | double | Binary indicator for if a lateral occurred on the reception. |
| `lateral_rush` | double | Binary indicator for if a lateral occurred on a run. |
| `lateral_return` | double | Binary indicator for if a lateral occurred on a return. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `lateral_recovery` | double | Binary indicator for if a lateral occurred on a fumble recovery. |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `passer_player_name` | character | String name for the player that attempted the pass. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `receiver_player_name` | character | String name for the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `rusher_player_name` | character | String name for the player that attempted the run. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_receiver_player_id` | character | Unique identifier for the player that received the last(!) lateral on a pass play. |
| `lateral_receiver_player_name` | character | String name for the player that received the last(!) lateral on a pass play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral receiving yards. |
| `lateral_receiving_yards` | double | Numeric yards by the `lateral_receiver_player_name` in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `lateral_rusher_player_id` | character | Unique identifier for the player that received the last(!) lateral on a run play. |
| `lateral_rusher_player_name` | character | String name for the player that received the last(!) lateral on a run play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral rushing yards. |
| `lateral_rushing_yards` | double | Numeric yards by the `lateral_rusher_player_name` in run plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_sack_player_id` | character | Unique identifier for the player that received the lateral on a sack. |
| `lateral_sack_player_name` | character | String name for the player that received the lateral on a sack. |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `interception_player_name` | character | String name for the player that intercepted the pass. |
| `lateral_interception_player_id` | character | Unique indentifier for the player that received the lateral on an interception. |
| `lateral_interception_player_name` | character | String name for the player that received the lateral on an interception. |
| `punt_returner_player_id` | character | Unique identifier for the punt returner. |
| `punt_returner_player_name` | character | String name for the punt returner. |
| `lateral_punt_returner_player_id` | character | Unique identifier for the player that received the lateral on a punt return. |
| `lateral_punt_returner_player_name` | character | String name for the player that received the lateral on a punt return. |
| `kickoff_returner_player_name` | character | String name for the kickoff returner. |
| `kickoff_returner_player_id` | character | Unique identifier for the kickoff returner. |
| `lateral_kickoff_returner_player_id` | character | Unique identifier for the player that received the lateral on a kickoff return. |
| `lateral_kickoff_returner_player_name` | character | String name for the player that received the lateral on a kickoff return. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `punter_player_name` | character | String name for the punter. |
| `kicker_player_name` | character | String name for the kicker on FG or kickoff. |
| `kicker_player_id` | character | Unique identifier for the kicker on FG or kickoff. |
| `own_kickoff_recovery_player_id` | character | Unique identifier for the player that recovered their own kickoff. |
| `own_kickoff_recovery_player_name` | character | String name for the player that recovered their own kickoff. |
| `blocked_player_id` | character | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | character | String name for the player that blocked the punt or FG. |
| `tackle_for_loss_1_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_1_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `qb_hit_1_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_1_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `forced_fumble_player_1_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | character | String name of one of the players with a forced fumble. |
| `forced_fumble_player_2_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_name` | character | String name of one of the players with a forced fumble. |
| `solo_tackle_1_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_2_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_1_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_2_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_1_player_name` | character | String name of one of the players with a solo tackle. |
| `solo_tackle_2_player_name` | character | String name of one of the players with a solo tackle. |
| `assist_tackle_1_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_1_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_1_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_2_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_2_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_2_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_3_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_3_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_3_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_4_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_4_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_4_team` | character | Team of one of the players with a tackle assist. |
| `tackle_with_assist` | double | Binary indicator for if there has been a tackle with assist. |
| `tackle_with_assist_1_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_1_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_1_team` | character | Team of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_2_team` | character | Team of one of the players with a tackle with assist. |
| `pass_defense_1_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | character | String name of one of the players with a pass defense. |
| `pass_defense_2_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_2_player_name` | character | String name of one of the players with a pass defense. |
| `fumbled_1_team` | character | Team of one of the first player with a fumble. |
| `fumbled_1_player_id` | character | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | character | String name of one of the first player who fumbled on the play. |
| `fumbled_2_player_id` | character | Unique identifier of the second player who fumbled on the play. |
| `fumbled_2_player_name` | character | String name of one of the second player who fumbled on the play. |
| `fumbled_2_team` | character | Team of one of the second player with a fumble. |
| `fumble_recovery_1_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_1_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_1_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | character | String name of one of the players with a fumble recovery. |
| `fumble_recovery_2_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_2_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_2_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_2_player_name` | character | String name of one of the players with a fumble recovery. |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `sack_player_name` | character | String name of the player who recorded a solo sack. |
| `half_sack_1_player_id` | character | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | character | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | character | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | character | String name of the second player who recorded half a sack. |
| `return_team` | character | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `return_yards` | double | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `penalty_team` | character | String abbreviation of the team with the penalty. |
| `penalty_player_id` | character | Unique identifier for the player with the penalty. |
| `penalty_player_name` | character | String name for the player with the penalty. |
| `penalty_yards` | double | Yards gained (or lost) by the posteam from the penalty. |
| `replay_or_challenge` | double | Binary indicator for whether or not a replay or challenge. |
| `replay_or_challenge_result` | character | String indicating the result of the replay or challenge. |
| `penalty_type` | character | String indicating the penalty type of the first penalty in the given play. Will be `NA` if `desc` is missing the type. |
| `defensive_two_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | double | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `defensive_extra_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on an extra point attempt, this results following a blocked attempt that the defense recovers the ball. |
| `defensive_extra_point_conv` | double | Binary indicator whether or not the defense successfully scored on an extra point attempt. |
| `safety_player_name` | character | String name for the player who scored a safety. |
| `safety_player_id` | character | Unique identifier for the player who scored a safety. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `cp` | double | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | double | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `series` | double | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_success` | double | 1: scored touchdown, gained enough yards for first down. |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `order_sequence` | double | Column provided by NFL to fix out-of-order plays. Available 2011 and beyond with source "nfl". |
| `start_time` | character | Kickoff time in eastern time zone. |
| `time_of_day` | character | Time of day of play in UTC "HH:MM:SS" format. Available 2011 and beyond with source "nfl". |
| `stadium` | character | Name of the stadium |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `nfl_api_id` | character | UUID of the game in the new NFL API. |
| `play_clock` | character | Time on the playclock when the ball was snapped. |
| `play_deleted` | double | Binary indicator for deleted plays. |
| `play_type_nfl` | character | Play type as listed in the NFL source. Slightly different to the regular play_type variable. |
| `special_teams_play` | double | Binary indicator for whether play is special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `st_play_type` | character | Type of special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `end_clock_time` | character | Game time at the end of a given play. |
| `end_yard_line` | character | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `fixed_drive` | double | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `drive_real_start_time` | character | Local day time when the drive started (currently not used by the NFL and therefore mostly 'NA'). |
| `drive_play_count` | double | Numeric value of how many regular plays happened in a given drive. |
| `drive_time_of_possession` | character | Time of possession in a given drive. |
| `drive_first_downs` | double | Number of first downs in a given drive. |
| `drive_inside20` | double | Binary indicator if the offense was able to get inside the opponents 20 yard line. |
| `drive_ended_with_score` | double | Binary indicator the drive ended with a score. |
| `drive_quarter_start` | double | Numeric value indicating in which quarter the given drive has started. |
| `drive_quarter_end` | double | Numeric value indicating in which quarter the given drive has ended. |
| `drive_yards_penalized` | double | Numeric value of how many yards the offense gained or lost through penalties in the given drive. |
| `drive_start_transition` | character | String indicating how the offense got the ball. |
| `drive_end_transition` | character | String indicating how the offense lost the ball. |
| `drive_game_clock_start` | character | Game time at the beginning of a given drive. |
| `drive_game_clock_end` | character | Game time at the end of a given drive. |
| `drive_start_yard_line` | character | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | character | String indicating where a given drive ended consisting of team half and yard line number. |
| `drive_play_id_started` | double | Play_id of the first play in the given drive. |
| `drive_play_id_ended` | double | Play_id of the last play in the given drive. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `game_stadium` | character | Name of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `aborted_play` | double | Binary indicator if the play description indicates "Aborted". |
| `success` | double | Binary indicator wheter epa > 0 in the given play. |
| `passer` | character | Name of the dropback player (scrambles included) including plays with penalties. |
| `passer_jersey_number` | integer | Jersey number of the passer. |
| `rusher` | character | Name of the rusher (no scrambles) including plays with penalties. |
| `rusher_jersey_number` | integer | Jersey number of the rusher. |
| `receiver` | character | Name of the receiver including plays with penalties. |
| `receiver_jersey_number` | integer | Jersey number of the receiver. |
| `pass` | double | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | double | Binary indicator if the play was a rushing play. |
| `first_down` | double | Binary indicator if the play ended in a first down. |
| `special` | double | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `play` | double | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `passer_id` | character | ID of the player in the 'passer' column. |
| `rusher_id` | character | ID of the player in the 'rusher' column. |
| `receiver_id` | character | ID of the player in the 'receiver' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `id` | character | ID of the player in the 'name' column. |
| `fantasy_player_name` | character | Name of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy_player_id` | character | ID of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy` | character | Name of the rusher on rush plays or receiver on pass plays. |
| `fantasy_id` | character | ID of the rusher on rush plays or receiver on pass plays. |
| `out_of_bounds` | double | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `home_opening_kickoff` | double | 1 if the home team received the opening kickoff, 0 otherwise. |
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `xyac_epa` | double | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | double | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | integer | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | double | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | double | Probability play earns a first down based on where the ball was caught. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

# Multi-season range

pbp = load_nfl_pbp(seasons=range(2020, 2025))

# With cache off (development workflow)

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

# Pandas round-trip

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

### `load_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_pfr_advstats}

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level
PFR advstats accessors. Mirrors the API surface of nflreadpy's
`load_pfr_advstats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For `summary_level='week'` this drives the per-season parquet fan-out; for `summary_level='season'` it post-filters the combined parquet by the `season` column. |
| `stat_type` | `str` | `'pass'` | One of `"pass"`, `"rush"`, `"rec"`, `"def"`. Defaults to `"pass"`. |
| `summary_level` | `str` | `'week'` | One of `"week"` or `"season"`. Defaults to `"week"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested `stat_type`, `summary_level`, and `seasons`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double |  |
| `passing_drop_pct` | double |  |
| `receiving_drop` | double |  |
| `receiving_drop_pct` | double |  |
| `passing_bad_throws` | double |  |
| `passing_bad_throw_pct` | double |  |
| `times_sacked` | double |  |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double |  |
| `def_times_blitzed` | double |  |
| `def_times_hurried` | double |  |
| `def_times_hitqb` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

# Season-level rushing summaries (one row per player per season)

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

# Defensive stats with a follow-up filter

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

# Pandas round-trip

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'` {#load_player_stats}

Load NFL player stats data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing player stats.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | character | Most recent team player appears in `pbp` with. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character |  |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `interceptions` | double | The number of interceptions thrown. |
| `sacks` | double | The Number of times sacked. |
| `sack_yards` | double | Yards lost on sack plays. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | double | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `pacr` | double | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | double | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | double | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | double | The number of rushes with a lost fumble. |
| `rushing_first_downs` | double | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | double | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | double | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | double | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `racr` | double | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | double | "Player's share of team receiving targets in this game" |
| `air_yards_share` | double | Player's share of the team's air yards in this game |
| `wopr` | double | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | double | Total number of kick/punt return touchdowns |
| `fantasy_points` | double | Standard fantasy points. |
| `fantasy_points_ppr` | double | PPR fantasy points. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

# Kicking-only stats

kicking = load_nfl_player_stats(kicking=True)

# Filter to a single season after load

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_players(return_as_pandas=False) -> 'pl.DataFrame'` {#load_players}

Load NFL Player ID information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing players available.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character |  |
| `official_name` | character | Official name. |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character | Unique official / referee identifier. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
players.shape

# Pandas round-trip

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()
```

### `load_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_rosters}

Load NFL roster data for all seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1920 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing rosters available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `depth_chart_position` | character | Position assigned on depth chart. Not always accurate! |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `status` | character | Status label. |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `height` | double | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `rotowire_id` | character | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `fantasy_data_id` | character | FantasyData ID - usual format five digit integer |
| `sleeper_id` | character | Sleeper ID - usually an integer with ~4 digits. |
| `years_exp` | integer | Years played in league |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `status_description_abbr` | character | A code corresponding to a particular NFL status. |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `gsis_it_id` | character | Player ID for the GSIS IT API |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `entry_year` | integer | The year a player first became eligible to play in the NFL. |
| `rookie_year` | integer | The year a player lost their rookie eligibility. |
| `draft_club` | character | The team that originally drafted a player. NA if a player went undrafted in their draft-eligible year. |
| `draft_number` | integer | The number pick that was used to select a given player. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

# Multi-season range

rosters = load_nfl_rosters(seasons=range(2020, 2025))

# Filter to a single team

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")
```

### `load_rosters_weekly(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_rosters_weekly}

Load NFL weekly roster data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2002 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing weekly rosters available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `depth_chart_position` | character | Position assigned on depth chart. Not always accurate! |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `status` | character | Status label. |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `height` | double | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `rotowire_id` | character | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `fantasy_data_id` | character | FantasyData ID - usual format five digit integer |
| `sleeper_id` | character | Sleeper ID - usually an integer with ~4 digits. |
| `years_exp` | integer | Years played in league |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `status_description_abbr` | character | A code corresponding to a particular NFL status. |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `gsis_it_id` | character | Player ID for the GSIS IT API |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `entry_year` | integer | The year a player first became eligible to play in the NFL. |
| `rookie_year` | integer | The year a player lost their rookie eligibility. |
| `draft_club` | character | The team that originally drafted a player. NA if a player went undrafted in their draft-eligible year. |
| `draft_number` | integer | The number pick that was used to select a given player. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

# Multi-season range with a follow-up week filter

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### `load_schedules(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_schedules}

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `gameday` | character | The date on which the game occurred. |
| `weekday` | character | The day of the week on which the game occcured. |
| `gametime` | character | The kickoff time of the game. This is represented in 24-hour time and the Eastern time zone, regardless of what time zone the game was being played in. |
| `away_team` | character | String abbreviation for the away team. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `overtime` | integer | Binary indicator of whether or not game went to overtime. |
| `old_game_id` | character | Legacy NFL game ID. |
| `gsis` | integer | The id of the game issued by the NFL Game Statistics & Information System. |
| `nfl_detail_id` | character | The id of the game issued by NFL Detail. |
| `pfr` | character | The id of the game issued by [Pro-Football-Reference](https://www.pro-football-reference.com/) |
| `pff` | integer | The id of the game issued by [Pro Football Focus](https://www.pff.com/) |
| `espn` | character | The id of the game issued by [ESPN](https://www.espn.com/) |
| `ftn` | integer |  |
| `away_rest` | integer | Days of rest that the away team is coming off of. |
| `home_rest` | integer | Days of rest that the home team is coming off of. |
| `away_moneyline` | integer | Odds for away team to win the game. |
| `home_moneyline` | integer | Odds for home team to win the game. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `away_spread_odds` | integer | Odds for away team to cover the spread. |
| `home_spread_odds` | integer | Odds for home team to cover the spread. |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `under_odds` | integer | Odds that total score of game would be under the total_line. |
| `over_odds` | integer | Odds that total score of game would be over the total_ine. |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `away_qb_id` | character | GSIS Player ID for away team starting quarterback. |
| `home_qb_id` | character | GSIS Player ID for home team starting quarterback. |
| `away_qb_name` | character | Name of away team starting QB. |
| `home_qb_name` | character | Name of home team starting QB. |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `referee` | character | Name of the game's referee (head official) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium` | character | Name of the stadium |

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

# Multi-season range

schedule = load_nfl_schedule(seasons=range(2020, 2025))

# Filter to a single week

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

# Pandas round-trip

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_snap_counts}

Load NFL snap counts data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2012 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing snap counts available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `player` | character | Player name |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `offense_snaps` | double | Number of snaps on offense |
| `offense_pct` | double | Percent of offensive snaps taken |
| `defense_snaps` | double | Number of snaps on defense |
| `defense_pct` | double | Percent of defensive snaps taken |
| `st_snaps` | double | Number of snaps on special teams |
| `st_pct` | double | Percent of special teams snaps taken |

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

# Multi-season range with offense-only filter

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

### `load_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'` {#load_team_stats}

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing team stats available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character |  |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `passing_interceptions` | integer |  |
| `sacks_suffered` | integer |  |
| `sack_yards_lost` | integer |  |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | integer | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | integer | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | double |  |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | integer | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | integer | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | integer | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | integer | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | integer | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | integer | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `special_teams_tds` | integer | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | integer | Total number of solo tackles for this player |
| `def_tackles_with_assist` | integer | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | integer | Number of assisted tackles for this player |
| `def_tackles_for_loss` | integer | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | integer | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | integer | Number of times a fumble was forced from this player |
| `def_sacks` | double | Number of sacks form this player |
| `def_sack_yards` | double | Yards lost from sacks forced by this player |
| `def_qb_hits` | integer | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | integer | Number of interceptions forced by this player |
| `def_interception_yards` | integer | yards gained/lost by interception returns from this player |
| `def_pass_defended` | integer | Number of passes defended/broken up by this player |
| `def_tds` | integer | Number of defensive touchdowns scored by this player |
| `def_fumbles` | integer | Number of fumbles by this player |
| `def_safeties` | integer |  |
| `misc_yards` | integer |  |
| `fumble_recovery_own` | integer |  |
| `fumble_recovery_yards_own` | integer |  |
| `fumble_recovery_opp` | integer |  |
| `fumble_recovery_yards_opp` | integer |  |
| `fumble_recovery_tds` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer |  |
| `punt_returns` | integer | Number of punt returns. |
| `punt_return_yards` | integer | Team punt return yards. |
| `kickoff_returns` | integer |  |
| `kickoff_return_yards` | integer |  |
| `fg_made` | integer | TRUE when the field goal attempt was successful. |
| `fg_att` | integer |  |
| `fg_missed` | integer |  |
| `fg_blocked` | integer |  |
| `fg_long` | integer |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg_made_0_19` | integer |  |
| `fg_made_20_29` | integer |  |
| `fg_made_30_39` | integer |  |
| `fg_made_40_49` | integer |  |
| `fg_made_50_59` | integer |  |
| `fg_made_60_` | integer |  |
| `fg_missed_0_19` | integer |  |
| `fg_missed_20_29` | integer |  |
| `fg_missed_30_39` | integer |  |
| `fg_missed_40_49` | integer |  |
| `fg_missed_50_59` | integer |  |
| `fg_missed_60_` | integer |  |
| `fg_made_list` | character |  |
| `fg_missed_list` | character |  |
| `fg_blocked_list` | character |  |
| `fg_made_distance` | integer |  |
| `fg_missed_distance` | integer |  |
| `fg_blocked_distance` | integer |  |
| `pat_made` | integer |  |
| `pat_att` | integer |  |
| `pat_missed` | integer |  |
| `pat_blocked` | integer |  |
| `pat_pct` | double |  |
| `gwfg_made` | integer |  |
| `gwfg_att` | integer |  |
| `gwfg_missed` | integer |  |
| `gwfg_blocked` | integer |  |
| `gwfg_distance` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

# Regular-season-only team stats

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

# Combined regular + post-season at season grain

combined = load_nfl_team_stats(seasons=[2023, 2024], summary_level="reg+post")
```

### `load_teams(return_as_pandas=False) -> 'pl.DataFrame'` {#load_teams}

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_abbr` | character | Official team abbreveation |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `team_nick` | character |  |
| `team_conf` | character |  |
| `team_division` | character |  |
| `team_color` | character | Team primary color (hex without leading '#'). |
| `team_color2` | character |  |
| `team_color3` | character |  |
| `team_color4` | character |  |
| `team_logo_wikipedia` | character |  |
| `team_logo_espn` | character |  |
| `team_wordmark` | character |  |
| `team_conference_logo` | character |  |
| `team_league_logo` | character |  |
| `team_logo_squared` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_trades(return_as_pandas=False) -> 'pl.DataFrame'` {#load_trades}

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

| col_name | type | description |
|---|---|---|
| `trade_id` | integer | ID of Trade |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `trade_date` | character | Exact date that trade occurred |
| `gave` | character | Team that gave pick/player in row |
| `received` | character | Team that received pick/player in row |
| `pick_season` | integer | Draft in which traded pick was in |
| `pick_round` | integer | Round in which traded pick was in |
| `pick_number` | integer | Pick number of traded pick |
| `conditional` | integer | Binary indicator of whether or not traded pick was conditional |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pfr_name` | character | Full name of traded player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

# Filter to a single season

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

## Utilities & helpers

### `NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, **kwargs)` {#NFLPlayProcess}

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN `summary` endpoint (or a local JSON dump) and pipes the
result through a chain of feature-engineering steps -- down/distance,
play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced
box score. Use `run_processing_pipeline()` for the full feature set
or `run_cleaning_pipeline()` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` | `int` | `0` | ESPN `event` id (e.g. `401671801`). |
| `raw` | `bool` | `False` | If `True`, `espn_nfl_pbp()` returns the ESPN payload untouched. If `False` (default), it normalizes keys. |
| `path_to_json` | `str` | `'/'` | Directory containing `{gameId}.json` for the `nfl_pbp_disk()` flow (offline replay). |
| `return_keys` | `list[str] \| None` | `None` | If supplied, `run_processing_pipeline` returns only the listed keys from the result dict. |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

# Offline replay from a JSON dump

proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
cleaned = proc.run_cleaning_pipeline()

# Subset the return payload

proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())  # ['boxscore', 'plays']
```

**Methods**

#### `NFLPlayProcess.corrupt_pbp_check()`

Detect ESPN payloads that look corrupt or partial.

Returns `True` when one of three guard conditions trips:

* No plays at all.
* Fewer than 50 plays for a game ESPN reports as completed.
* More than 500 plays for a game ESPN reports as completed.

`run_processing_pipeline()` and `run_cleaning_pipeline()` use
this to skip feature engineering on obviously broken payloads.

**Returns**

`True` if the payload looks corrupt; `False` otherwise.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
if not proc.corrupt_pbp_check():
    result = proc.run_processing_pipeline()
```

#### `NFLPlayProcess.create_box_score(play_df)`

Build the advanced box score (passer / rusher / receiver / team / situational / defensive / turnover / drives)

from a feature-engineered plays DataFrame.

This is normally called by `run_processing_pipeline()` -- it
auto-runs the pipeline first if it hasn't been triggered yet, so
callers can also invoke it directly on a freshly-instantiated
processor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | The plays frame produced after the full feature-engineering chain (downs, play-type flags, EPA, WPA, drive aggregation). |

**Returns**

Box score keyed by `"pass"`, `"rush"`, `"receiver"`, `"team"`, `"situational"`, `"defensive"`, `"turnover"`, `"drives"` -- each value a list of dicts ready to be serialized.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
box = result["advBoxScore"]
sorted(box.keys())
```

#### `NFLPlayProcess.espn_nfl_pbp(**kwargs)`

espn_nfl_pbp() - Pull the game by id. Data from API endpoints: `nfl/playbyplay`, `nfl/summary`

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder", "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
payload = proc.espn_nfl_pbp()
sorted(payload.keys())[:5]

# Raw ESPN passthrough (no key normalization)

proc_raw = NFLPlayProcess(gameId=401220403, raw=True)
espn_dump = proc_raw.espn_nfl_pbp()

# Chain into the full processing pipeline

proc = NFLPlayProcess(gameId=401220403)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
```

#### `NFLPlayProcess.nfl_pbp_disk()`

Load a previously-saved ESPN payload from `{path_to_json}/{gameId}.json`.

Use this to replay an old game offline without hitting the ESPN
endpoint -- handy for snapshot-driven tests and reproducible
feature engineering.

**Returns**

The parsed JSON content; also stored on `self.json`.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
result = proc.run_processing_pipeline()
```

#### `NFLPlayProcess.nfl_pbp_json(**kwargs)`

Set `self.json` to the imported `json` module reference (legacy stub).

Retained for API compatibility. Prefer `espn_nfl_pbp()` (live)
or `nfl_pbp_disk()` (offline) to populate `self.json` with an
actual ESPN payload.

**Returns**

The Python `json` module reference (mirrors legacy behavior).

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
proc.nfl_pbp_json()  # populates `self.json` with the json module
```

#### `NFLPlayProcess.run_cleaning_pipeline()`

Run the lighter cleaning pipeline against `self.json`.

Identical to `run_processing_pipeline()` up through the
add_spread_time` step but stops short of EPA / WPA / QBR /
drive aggregation and the advanced box score. Use this when you
want clean play structure without the modeled features.

**Returns**

The cleaned game dict (or the subset specified by `return_keys` at construction).

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
cleaned = proc.run_cleaning_pipeline()
"plays" in cleaned and "advBoxScore" not in cleaned
```

#### `NFLPlayProcess.run_processing_pipeline()`

Run the full feature-engineering pipeline against `self.json`.

Pipes the plays frame through the chain of helpers: downs,
play-type flags, rush/pass flags, team-score variables, new play
types, penalties, play-category flags, yardage cols, player cols,
post-play cols, spread time, EPA, WPA, drive data, and QBR --
followed by the advanced box score build.

**Returns**

Dict | None: The full processed game dict (or the subset specified by `return_keys` at construction). Returns the partial result when `corrupt_pbp_check()` short-circuits.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"]), len(result["drives"])

# Subset returned keys for downstream serialization

proc = NFLPlayProcess(
    gameId=401671801,
    return_keys=["plays", "advBoxScore", "winprobability"],
)
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())
```

### `get_current_nfl_season(roster: 'bool' = False) -> 'int'` {#get_current_nfl_season}

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_nfl_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'` {#get_current_nfl_week}

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `get_current_season(roster: 'bool' = False) -> 'int'` {#get_current_season}

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'` {#get_current_week}

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `most_recent_nfl_season(roster: 'bool' = False) -> 'int'` {#most_recent_nfl_season}

Alias for `get_current_nfl_season()` mirroring nflreadr's

`most_recent_season()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl.utils_date import most_recent_nfl_season
season = most_recent_nfl_season()

# Roster-year flavor

roster_year = most_recent_nfl_season(roster=True)
```

## Other

### `NflConfig(cache_mode: 'CacheMode' = 'memory', cache_dir: 'Optional[Path]' = None, cache_duration: 'int' = 86400, verbose: 'bool' = True, timeout: 'int' = 30, user_agent: 'str' = 'sportsdataverse-py-nfl') -> None` {#NflConfig}

Runtime configuration for sdv-py NFL loaders.

Fields mirror nflreadpy's `NflreadpyConfig` so users can swap engines
without changing call sites. The defaults are conservative: in-memory
caching with a 24-hour TTL, verbose progress bars on, 30-second
HTTP timeout.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache_mode` | `CacheMode` | `'memory'` |  |
| `cache_dir` | `Optional[Path]` | `None` |  |
| `cache_duration` | `int` | `86400` |  |
| `verbose` | `bool` | `True` |  |
| `timeout` | `int` | `30` |  |
| `user_agent` | `str` | `'sportsdataverse-py-nfl'` |  |

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()  # NflConfig instance
cfg.cache_mode      # "memory"
cfg.cache_duration  # 86400 (24h)
cfg.timeout         # 30 (seconds)

# Construct a fresh instance directly (rarely needed -- prefer ``update_config``)

from sportsdataverse.nfl import NflConfig
cfg = NflConfig(cache_mode="off", timeout=10)
```

### `cached_loader(func: 'F') -> 'F'` {#cached_loader}

Decorator that adds caching to a `load_nfl_*` function.

Honors the active `NflConfig.cache_mode`:

- `memory`: dict-based per-process cache.
- `filesystem`: parquet-based cross-process cache under `cache_dir`.
- `off`: no caching, function runs every time.

The cache key is the hash of `(qualified_name, args, kwargs)` with
`return_as_pandas` excluded so memory / disk hits work regardless of
which return shape the caller asked for. The cache always stores the
polars frame internally and converts to pandas on read when requested.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `func` | `F` |  |  |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.cache import cached_loader

@cached_loader
def load_my_thing(season: int, return_as_pandas: bool = False):
    # ... fetch parquet, build a polars frame ...
    return pl.DataFrame({"season": [season]})

df1 = load_my_thing(2024)            # network hit, populates cache
df2 = load_my_thing(2024)            # served from cache
df_pd = load_my_thing(2024, return_as_pandas=True)
# `return_as_pandas` is excluded from the cache key, so the
# polars hit is reused and converted to pandas on the way out.

# Switch caching modes at runtime

from sportsdataverse.nfl import clear_cache, update_config

update_config(cache_mode="filesystem")  # parquet-on-disk reuse
df3 = load_my_thing(2024)               # writes parquet under cache_dir
clear_cache()                           # wipe both memory + filesystem
update_config(cache_mode="off")         # bypass cache entirely
```

### `clear_cache() -> 'None'` {#clear_cache}

Clear both memory and filesystem caches.

Memory: empties the in-process dict.
Filesystem: removes all entries under `config.cache_dir`. The
directory itself is preserved so subsequent writes succeed without
needing `mkdir`.

**Example**

```python
from sportsdataverse.nfl import clear_cache, load_nfl_pbp
clear_cache()
pbp = load_nfl_pbp(seasons=[2024])

# Pair with a cache-mode switch

from sportsdataverse.nfl import clear_cache, update_config
update_config(cache_mode="filesystem")
# ... lots of cached calls accumulate parquet files on disk ...
clear_cache()  # wipe disk + memory together
```

### `espn_nfl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nfl_teams}

espn_nfl_teams - look up NFL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nfl.espn_nfl_teams.clear_cache().

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
from sportsdataverse.nfl import espn_nfl_teams
teams = espn_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = espn_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbreviation", "team_display_name"]].head()

# Force a refresh after upstream ESPN updates

espn_nfl_teams.cache_clear()  # underlying lru_cache
teams = espn_nfl_teams()
```

### `get_config() -> 'NflConfig'` {#get_config}

Return the live `NflConfig` singleton.

The same object is returned on every call; mutate via `update_config`
rather than reassigning fields directly so future hooks (e.g. logging
on config change) have a single choke point.

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()
print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)

# Pair with ``update_config`` to verify a change took effect

from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="off")
assert get_config().cache_mode == "off"
```

### `nfl_game_details(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'` {#nfl_game_details}

Pull full `api.nfl.com` game details (drives + plays) by game id.

Hits `/experience/v1/gamedetails/{game_id}`; the payload is the shield
`data.viewer.gameDetail` object (plays, drives, scoring summaries, line
scores, possession, weather, attendance, ...).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | the uuid game id from `nfl_game_schedule` (e.g. `'7d3e8f84-1312-11ef-afd1-646009f18b2e'`). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict. Defaults to a fresh `nfl_headers_gen` call. |
| `raw` | `bool` | `False` | If True, return the full envelope (`{"data": {...}}`) untouched. If False (default), unwrap to the `gameDetail` object. |

**Returns**

the `gameDetail` object (or the raw envelope when `raw=True`). Empty `dict` if the game has no detail payload.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_details
detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
len(detail["plays"]), len(detail["drives"])

# Reuse headers across many calls (avoids re-minting tokens)

from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
hdrs = nfl_headers_gen()
detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e", headers=hdrs)
```

### `nfl_game_pbp(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_as_pandas: 'bool' = False)` {#nfl_game_pbp}

Parsed `api.nfl.com` play-by-play -- one row per play (polars/pandas frame).

Tidy wrapper over `nfl_game_details`: flattens `gameDetail.plays` into a
DataFrame (`playId`, `quarter`, `down`, `yardsToGo`, `yardLine`,
`playType`, `playDescription`, `possessionTeam_*`, ...) and prepends the
game context (`game_id`, `home_team`, `visitor_team`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | uuid game id from `nfl_week_games` / `nfl_game_schedule`. |
| `headers` | `Optional[Dict[str, str]]` | `None` | reuse a `nfl_headers_gen` dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per play (empty frame if the game has no play-by-play yet).

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `visitor_team` | character |  |
| `clockTime` | character |  |
| `down` | integer | The down for the given play. |
| `driveNetYards` | integer |  |
| `drivePlayCount` | integer |  |
| `driveSequenceNumber` | integer |  |
| `driveTimeOfPossession` | character |  |
| `endClockTime` | character |  |
| `endYardLine` | character |  |
| `firstDown` | logical |  |
| `goalToGo` | logical |  |
| `isBigPlay` | character |  |
| `latestPlay` | character |  |
| `nextPlayIsGoalToGo` | logical |  |
| `nextPlayType` | character |  |
| `orderSequence` | integer |  |
| `penaltyOnPlay` | logical |  |
| `playClock` | integer |  |
| `playReviewStatus` | character |  |
| `playDeleted` | logical |  |
| `playDescription` | character |  |
| `playDescriptionWithJerseyNumbers` | character |  |
| `playId` | integer | Unique play event identifier (UUID). |
| `playStats` | integer |  |
| `playType` | character |  |
| `prePlayByPlay` | character |  |
| `quarter` | integer |  |
| `scoringPlay` | logical |  |
| `scoringPlayType` | character |  |
| `scoringTeam` | character |  |
| `shortDescription` | character |  |
| `specialTeamsPlay` | logical |  |
| `stPlayType` | character |  |
| `timeOfDay` | character |  |
| `yardLine` | character |  |
| `yards` | integer | The number of receiving yards |
| `yardsToGo` | integer |  |
| `possessionTeam_id` | character |  |
| `possessionTeam_abbreviation` | character |  |
| `possessionTeam_nickName` | character |  |
| `possessionTeam_franchise_primaryColor` | character |  |
| `possessionTeam_franchise_secondaryColor` | character |  |
| `possessionTeam_franchise_tertiaryColor` | character |  |
| `possessionTeam_franchise_currentLogo` | character |  |
| `possessionTeam_franchise_largeTypeColor` | character |  |
| `possessionTeam_franchise_decorativeElementsColor` | character |  |
| `scoringTeam_id` | character |  |
| `scoringTeam_abbreviation` | character |  |
| `scoringTeam_nickName` | character |  |
| `scoringTeam_franchise_primaryColor` | character |  |
| `scoringTeam_franchise_secondaryColor` | character |  |
| `scoringTeam_franchise_tertiaryColor` | character |  |
| `scoringTeam_franchise_currentLogo` | character |  |
| `scoringTeam_franchise_largeTypeColor` | character |  |
| `scoringTeam_franchise_decorativeElementsColor` | character |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_game_pbp
>>> pbp = nfl_game_pbp(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
>>> pbp.select(["quarter", "down", "yardsToGo", "playType", "playDescription"]).head()
```

### `nfl_game_schedule(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'int' = 1, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'` {#nfl_game_schedule}

List `api.nfl.com` games for a season/week slice (`/football/v2/games`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year (e.g. `2024`). |
| `season_type` | `str` | `'REG'` | season type. One of `"PRE"`, `"REG"`, `"POST"`. |
| `week` | `int` | `1` | week number (1-18 regular season, 1-4 post-season). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict (skip the auth roundtrip). Defaults to a fresh `nfl_headers_gen` call. |
| `raw` | `bool` | `False` | currently ignored; the function returns the parsed JSON payload. |

**Returns**

payload with the games list under `"games"` plus `"pagination"`. Each game carries `id` (the uuid game id used by `nfl_game_details`), `homeTeam`/`awayTeam`, `date`, `status`, `externalIds` (gsis etc.).

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_schedule
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)
first_id = week_one["games"][0]["id"]
```

### `nfl_headers_gen(token: 'Optional[str]' = None) -> 'Dict[str, str]'` {#nfl_headers_gen}

Build the request-header dict expected by `api.nfl.com`.

Mints a fresh bearer token via `nfl_token_gen` (unless `token` is
supplied) and combines it with the browser-style headers the NFL.com web app
sends. Reuse the returned dict across calls to avoid re-minting tokens.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `token` | `Optional[str]` | `None` | An existing access token to reuse; mints a fresh one when `None`. |

**Returns**

Header dict ready to drop into `requests.get`.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_headers_gen, nfl_game_schedule
hdrs = nfl_headers_gen()
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
```

### `nfl_ngs_gamecenter_overview(game_id, group: 'str' = 'passers', return_as_pandas: 'bool' = False)` {#nfl_ngs_gamecenter_overview}

NGS gamecenter overview for one game -- one row per player on a side.

Wraps `/api/gamecenter/overview` (keyed by NGS `gameId`). The payload
splits each stat `group` into `home` and `visitor` entries; this function
stacks both and tags every row with `side` (`"home"`/`"visitor"`) plus the
game's `gameId`. Note `passers` carries a single primary QB per side (two
rows total) while `rushers`/`receivers`/`passRushers` are full lists.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  | NGS `gameId` (e.g. `"2024090500"`) from `nfl_ngs_league_schedule`. |
| `group` | `str` | `'passers'` | which player group -- one of `"passers"`, `"rushers"`, `"receivers"`, `"passRushers"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per player (both teams), with `side` and `gameId` columns prepended.

| col_name | type | description |
|---|---|---|
| `side` | character | O for offense, D for defense, S for special teams |
| `gameId` | character |  |
| `esbId` | character |  |
| `teamId` | character |  |
| `teamAbbr` | character |  |
| `shortName` | character |  |
| `position` | character | Primary position as reported by NFL.com |
| `jerseyNumber` | integer |  |
| `playerName` | character |  |
| `zones` | integer |  |
| `passYards` | integer |  |
| `touchdowns` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `completions` | integer | The number of completed passes. |
| `headshot` | character | NFL headshot url for player |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_gamecenter_overview
>>> ov = nfl_ngs_gamecenter_overview(game_id="2024090500", group="passers")
>>> ov.select(["side", "playerName", "position"]).head()
```

### `nfl_ngs_leaders(category: 'str' = 'speed', season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_leaders}

NGS top-N "leaders" board for a single category (one row per leader play).

One parameterized wrapper over the single-list leader endpoints. Each record
nests a `leader` (player/stat) block and a `play` (the play that produced
the highlight) block, flattened to `leader_*` / `play_*` columns.

Categories (`category=` value -> endpoint):

* `"speed"` -> `/leaders/speed/ballCarrier` (fastest ball-carrier speeds)
* `"distance_ballcarrier"` -> `/leaders/distance/ballCarrier`
* `"distance_tackle"` -> `/leaders/distance/tackle`
* `"time_sack"` -> `/leaders/time/sack`
* `"completion_season"` / `"completion_week"` ->
  `/leaders/expectation/completion/{season,week}` (most-improbable completions)
* `"ery_season"` / `"ery_week"` -> `/leaders/expectation/ery/{season,week}`
  (expected rush yards over expectation)
* `"yac_season"` / `"yac_week"` -> `/leaders/expectation/yac/{season,week}`
  (yards after catch over expectation)

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'speed'` | one of the keys above. Defaults to `"speed"`. |
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | week filter -- required (and only used) by the `*_week` categories; ignored by season/non-expectation boards. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per leader entry.

| col_name | type | description |
|---|---|---|
| `leader_esbId` | character |  |
| `leader_firstName` | character |  |
| `leader_gsisId` | character |  |
| `leader_jerseyNumber` | integer |  |
| `leader_lastName` | character |  |
| `leader_playerName` | character |  |
| `leader_position` | character |  |
| `leader_positionGroup` | character |  |
| `leader_shortName` | character |  |
| `leader_teamAbbr` | character |  |
| `leader_teamId` | character |  |
| `leader_week` | integer |  |
| `leader_yards` | integer |  |
| `leader_inPlayDist` | double |  |
| `leader_maxSpeed` | double |  |
| `leader_headshot` | character |  |
| `play_gameId` | integer |  |
| `play_playId` | integer |  |
| `play_sequence` | integer |  |
| `play_down` | integer |  |
| `play_gameClock` | character |  |
| `play_gameKey` | integer |  |
| `play_health_playerTracking` | character |  |
| `play_health_ballTracking` | character |  |
| `play_homeScore` | integer |  |
| `play_isBigPlay` | logical |  |
| `play_isEndQuarter` | logical |  |
| `play_isGoalToGo` | logical |  |
| `play_isPenalty` | logical |  |
| `play_isSTPlay` | logical |  |
| `play_isScoring` | logical |  |
| `play_playDescription` | character |  |
| `play_playState` | character |  |
| `play_playStats` | integer |  |
| `play_playType` | character |  |
| `play_playTypeCode` | integer |  |
| `play_possessionTeamId` | character |  |
| `play_preSnapHomeScore` | integer |  |
| `play_preSnapVisitorScore` | integer |  |
| `play_quarter` | integer |  |
| `play_timeOfDayUTC` | character |  |
| `play_visitorScore` | integer |  |
| `play_yardline` | character |  |
| `play_yardlineNumber` | integer |  |
| `play_yardlineSide` | character |  |
| `play_yardsToGo` | integer |  |
| `play_absoluteYardlineNumber` | integer |  |
| `play_actualYardlineForFirstDown` | double |  |
| `play_actualYardsToGo` | double |  |
| `play_endGameClock` | character |  |
| `play_isChangeOfPossession` | logical |  |
| `play_playDirection` | character |  |
| `play_startGameClock` | character |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_leaders
>>> fast = nfl_ngs_leaders(category="speed", season=2024, season_type="REG")
>>> fast.select(["leader_playerName", "leader_maxSpeed", "play_playDescription"]).head()
```

### `nfl_ngs_league_schedule(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_league_schedule}

NGS league schedule -- one row per game; source of NGS `gameId` values.

Wraps `/api/league/schedule` (which returns a top-level list of games).
Each row carries `gameId` (the `YYYYMMDDNN` id used by the game-scoped
functions here), `gameKey`, `smartId` (the api.nfl.com uuid), team
abbreviations/ids/names, kickoff times, `ngsGame` (tracking-data flag) and
`season`/`seasonType`/`week`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | optional single-week filter. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per scheduled game.

| col_name | type | description |
|---|---|---|
| `gameKey` | integer |  |
| `gameDate` | character | Game date-time (ISO 8601, UTC). |
| `gameId` | integer |  |
| `gameTime` | character |  |
| `gameTimeEastern` | character |  |
| `gameType` | character | Game type identifier (3 for playoffs). |
| `homeDisplayName` | character |  |
| `homeNickname` | character |  |
| `homeTeamAbbr` | character |  |
| `homeTeamId` | character |  |
| `isoTime` | integer |  |
| `networkChannel` | character |  |
| `ngsGame` | logical |  |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character |  |
| `smartId` | character |  |
| `visitorDisplayName` | character |  |
| `visitorNickname` | character |  |
| `visitorTeamAbbr` | character |  |
| `visitorTeamId` | character |  |
| `week` | integer | Season week. |
| `weekNameAbbr` | character |  |
| `liveDotsGame` | logical |  |
| `validated` | logical |  |
| `releasedToClubs` | logical |  |
| `homeTeam_teamId` | character |  |
| `homeTeam_smartId` | character |  |
| `homeTeam_logo` | character |  |
| `homeTeam_abbr` | character |  |
| `homeTeam_cityState` | character |  |
| `homeTeam_fullName` | character |  |
| `homeTeam_nick` | character |  |
| `homeTeam_teamType` | character |  |
| `homeTeam_conferenceAbbr` | character |  |
| `homeTeam_divisionAbbr` | character |  |
| `site_smartId` | character |  |
| `site_siteId` | integer |  |
| `site_siteFullName` | character |  |
| `site_siteCity` | character |  |
| `site_siteState` | character |  |
| `site_postalCode` | character |  |
| `site_roofType` | character |  |
| `visitorTeam_teamId` | character |  |
| `visitorTeam_smartId` | character |  |
| `visitorTeam_logo` | character |  |
| `visitorTeam_abbr` | character |  |
| `visitorTeam_cityState` | character |  |
| `visitorTeam_fullName` | character |  |
| `visitorTeam_nick` | character |  |
| `visitorTeam_teamType` | character |  |
| `visitorTeam_conferenceAbbr` | character |  |
| `visitorTeam_divisionAbbr` | character |  |
| `score_time` | character |  |
| `score_phase` | character |  |
| `score_visitorTeamScore_pointTotal` | integer |  |
| `score_visitorTeamScore_pointQ1` | integer |  |
| `score_visitorTeamScore_pointQ2` | integer |  |
| `score_visitorTeamScore_pointQ3` | integer |  |
| `score_visitorTeamScore_pointQ4` | integer |  |
| `score_visitorTeamScore_pointOT` | integer |  |
| `score_visitorTeamScore_timeoutsRemaining` | integer |  |
| `score_homeTeamScore_pointTotal` | integer |  |
| `score_homeTeamScore_pointQ1` | integer |  |
| `score_homeTeamScore_pointQ2` | integer |  |
| `score_homeTeamScore_pointQ3` | integer |  |
| `score_homeTeamScore_pointQ4` | integer |  |
| `score_homeTeamScore_pointOT` | integer |  |
| `score_homeTeamScore_timeoutsRemaining` | integer |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_league_schedule
>>> sched = nfl_ngs_league_schedule(season=2024, season_type="REG", week=1)
>>> first_game_id = sched["gameId"][0]
```

### `nfl_ngs_league_schedule_current(return_as_pandas: 'bool' = False)` {#nfl_ngs_league_schedule_current}

NGS schedule for the *current* week -- one row per game.

Wraps `/api/league/schedule/current`; the games are under the `games` key
(alongside scalar `season`/`seasonType`/`week` describing the slice).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per game in the current week.

| col_name | type | description |
|---|---|---|
| `gameKey` | integer |  |
| `gameDate` | character | Game date-time (ISO 8601, UTC). |
| `gameId` | integer |  |
| `gameTime` | character |  |
| `gameTimeEastern` | character |  |
| `gameType` | character | Game type identifier (3 for playoffs). |
| `homeDisplayName` | character |  |
| `homeNickname` | character |  |
| `homeTeamAbbr` | character |  |
| `homeTeamId` | character |  |
| `isoTime` | integer |  |
| `networkChannel` | character |  |
| `ngsGame` | logical |  |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character |  |
| `smartId` | character |  |
| `visitorDisplayName` | character |  |
| `visitorNickname` | character |  |
| `visitorTeamAbbr` | character |  |
| `visitorTeamId` | character |  |
| `week` | integer | Season week. |
| `weekNameAbbr` | character |  |
| `releasedToClubs` | logical |  |
| `validated` | logical |  |
| `homeTeam_teamId` | character |  |
| `homeTeam_smartId` | character |  |
| `homeTeam_logo` | character |  |
| `homeTeam_abbr` | character |  |
| `homeTeam_cityState` | character |  |
| `homeTeam_fullName` | character |  |
| `homeTeam_nick` | character |  |
| `homeTeam_teamType` | character |  |
| `homeTeam_conferenceAbbr` | character |  |
| `homeTeam_divisionAbbr` | character |  |
| `site_smartId` | character |  |
| `site_siteId` | integer |  |
| `site_siteFullName` | character |  |
| `site_siteCity` | character |  |
| `site_siteState` | character |  |
| `site_postalCode` | character |  |
| `site_roofType` | character |  |
| `visitorTeam_teamId` | character |  |
| `visitorTeam_smartId` | character |  |
| `visitorTeam_logo` | character |  |
| `visitorTeam_abbr` | character |  |
| `visitorTeam_cityState` | character |  |
| `visitorTeam_fullName` | character |  |
| `visitorTeam_nick` | character |  |
| `visitorTeam_teamType` | character |  |
| `visitorTeam_conferenceAbbr` | character |  |
| `visitorTeam_divisionAbbr` | character |  |
| `score_time` | character |  |
| `score_phase` | character |  |
| `score_visitorTeamScore_pointTotal` | integer |  |
| `score_visitorTeamScore_pointQ1` | integer |  |
| `score_visitorTeamScore_pointQ2` | integer |  |
| `score_visitorTeamScore_pointQ3` | integer |  |
| `score_visitorTeamScore_pointQ4` | integer |  |
| `score_visitorTeamScore_pointOT` | integer |  |
| `score_visitorTeamScore_timeoutsRemaining` | integer |  |
| `score_homeTeamScore_pointTotal` | integer |  |
| `score_homeTeamScore_pointQ1` | integer |  |
| `score_homeTeamScore_pointQ2` | integer |  |
| `score_homeTeamScore_pointQ3` | integer |  |
| `score_homeTeamScore_pointQ4` | integer |  |
| `score_homeTeamScore_pointOT` | integer |  |
| `score_homeTeamScore_timeoutsRemaining` | integer |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_league_schedule_current
>>> cur = nfl_ngs_league_schedule_current()
>>> cur.select(["gameId", "homeTeamAbbr", "visitorTeamAbbr"]).head()
```

### `nfl_ngs_league_teams(return_as_pandas: 'bool' = False)` {#nfl_ngs_league_teams}

NGS team directory -- one row per team.

Wraps `/api/league/teams` (top-level list). Each row carries `teamId`,
`abbr`, `fullName`, `nick`, `conference`/`division`, `cityState`,
`stadiumName`, `smartId`, `logo` and site/ticket URLs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per team.

| col_name | type | description |
|---|---|---|
| `abbr` | character |  |
| `cityState` | character |  |
| `conferenceAbbr` | character |  |
| `fullName` | character | Full name of the probable starting pitcher. |
| `logo` | character | Team or league logo URL. |
| `nick` | character |  |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `smartId` | character |  |
| `stadiumName` | character |  |
| `teamId` | character |  |
| `teamSiteTicketUrl` | character |  |
| `teamSiteUrl` | character |  |
| `teamType` | character |  |
| `ticketPhoneNumber` | character |  |
| `yearFound` | integer |  |
| `conference_id` | character | Conference identifier. |
| `conference_abbr` | character | Conference abbreviation. |
| `conference_fullName` | character |  |
| `division_id` | character | Division MLBAM ID. |
| `division_abbr` | character | Division abbreviation. |
| `division_fullName` | character |  |
| `divisionAbbr` | character |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_league_teams
>>> teams = nfl_ngs_league_teams()
>>> teams.select(["teamId", "abbr", "fullName", "conferenceAbbr"]).head()
```

### `nfl_ngs_microsite_chart(season: 'int' = 2024, season_type: 'str' = 'REG', week=None, chart_type=None, team_id=None, limit: 'int' = 100, offset: 'int' = 0, return_as_pandas: 'bool' = False)` {#nfl_ngs_microsite_chart}

NGS microsite chart catalogue -- one row per rendered player chart image.

Wraps `/api/content/microsite/chart`; records live under `charts` and each
carries the chart `imageName`/`type` (`qb-grid`, `pass`, `route`,
`carry`) plus the player and headline stats (`passerRating`,
`completions`, etc.) and image-size URLs. Supports server-side paging.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` |  | `None` | optional week filter (the API accepts `"all"` by default). |
| `chart_type` |  | `None` | optional chart-type filter (e.g. `"qb-grid"`, `"pass"`). |
| `team_id` |  | `None` | optional team-id filter. |
| `limit` | `int` | `100` | page size (passed as `limit`). |
| `offset` | `int` | `0` | page offset. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per chart in the page.

| col_name | type | description |
|---|---|---|
| `imageName` | character |  |
| `esbId` | character |  |
| `firstName` | character | Scorer first name (localized list). |
| `gameId` | integer |  |
| `headshot` | character | NFL headshot url for player |
| `lastName` | character | Scorer last name (localized list). |
| `playerName` | character |  |
| `position` | character | Primary position as reported by NFL.com |
| `receivingYards` | integer |  |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character |  |
| `teamId` | character |  |
| `timestamp` | integer | Response timestamp (ISO 8601). |
| `touchdowns` | integer |  |
| `type` | character | Record type / category. |
| `week` | integer | Season week. |
| `extraLargeImg` | character |  |
| `playerNameSlug` | character |  |
| `smallImg` | character |  |
| `mediumImg` | character |  |
| `largeImg` | character |  |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushingYards` | integer |  |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `completionPercentage` | double |  |
| `completions` | integer | The number of completed passes. |
| `interceptions` | integer | The number of interceptions thrown. |
| `passerRating` | double |  |
| `passingYards` | integer |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_microsite_chart
>>> charts = nfl_ngs_microsite_chart(season=2024, season_type="REG", limit=25)
>>> charts.select(["playerName", "type", "imageName"]).head()
```

### `nfl_ngs_microsite_chart_players(season: 'int' = 2024, season_type: 'str' = 'REG', return_as_pandas: 'bool' = False)` {#nfl_ngs_microsite_chart_players}

NGS microsite chart player index -- one row per player with a chart.

Wraps `/api/content/microsite/chart/players`; records live under `players`
and carry `esbId`, `firstName`, `lastName` and `playerName`. Useful as
the lookup list of who has charts available for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per player.

| col_name | type | description |
|---|---|---|
| `esbId` | character |  |
| `firstName` | character | Scorer first name (localized list). |
| `lastName` | character | Scorer last name (localized list). |
| `playerName` | character |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_microsite_chart_players
>>> who = nfl_ngs_microsite_chart_players(season=2024, season_type="REG")
>>> who.select(["playerName", "esbId"]).head()
```

### `nfl_ngs_play_is_highlight(game_id, play_id, return_as_pandas: 'bool' = False)` {#nfl_ngs_play_is_highlight}

Look up whether a single play is an NGS highlight -- one-row frame.

Wraps `/api/plays/isHighlight` (keyed by NGS `gameId` + `playId`). When
the play is a highlight, the response's nested `highlight` block (the play
metadata, the `players` involved, season/week/team) is flattened onto the
row alongside the top-level `gameId`/`playId`/`isHighlight` flag. Pull a
real `(gameId, playId)` pair from `nfl_ngs_leaders` -- each leader
entry's `play_gameId` / `play_playId` is a known highlight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  | NGS `gameId` (e.g. `"2024111800"`). |
| `play_id` |  |  | the play id within that game (e.g. `1214`). |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A one-row polars (or pandas) `DataFrame` with `gameId`, `playId`, `isHighlight` and (when true) flattened `highlight_*` columns.

| col_name | type | description |
|---|---|---|
| `gameId` | integer |  |
| `playId` | integer | Unique play event identifier (UUID). |
| `isHighlight` | logical |  |
| `highlight_gameId` | integer |  |
| `highlight_playId` | integer |  |
| `highlight_play_playType` | character |  |
| `highlight_play_gameId` | integer |  |
| `highlight_play_gameKey` | integer |  |
| `highlight_play_yardlineSide` | character |  |
| `highlight_play_absoluteYardlineNumber` | integer |  |
| `highlight_play_yardlineNumber` | integer |  |
| `highlight_play_timeOfDayUTC` | character |  |
| `highlight_play_isPenalty` | logical |  |
| `highlight_play_homeScore` | integer |  |
| `highlight_play_visitorScore` | integer |  |
| `highlight_play_playStats` | integer |  |
| `highlight_play_playId` | integer |  |
| `highlight_play_playDescription` | character |  |
| `highlight_play_playTypeCode` | integer |  |
| `highlight_play_quarter` | integer |  |
| `highlight_play_down` | integer |  |
| `highlight_play_yardsToGo` | integer |  |
| `highlight_play_actualYardsToGo` | double |  |
| `highlight_play_actualYardlineForFirstDown` | double |  |
| `highlight_play_possessionTeamId` | character |  |
| `highlight_play_isGoalToGo` | logical |  |
| `highlight_play_health` | character |  |
| `highlight_play_endGameClock` | character |  |
| `highlight_play_startGameClock` | character |  |
| `highlight_play_playState` | character |  |
| `highlight_play_preSnapHomeScore` | integer |  |
| `highlight_play_preSnapVisitorScore` | integer |  |
| `highlight_play_sequence` | integer |  |
| `highlight_play_gameClock` | character |  |
| `highlight_play_yardline` | character |  |
| `highlight_play_isScoring` | logical |  |
| `highlight_play_isEndQuarter` | logical |  |
| `highlight_play_isSTPlay` | logical |  |
| `highlight_play_playDirection` | character |  |
| `highlight_play_isBigPlay` | logical |  |
| `highlight_play_isChangeOfPossession` | logical |  |
| `highlight_players` | integer |  |
| `highlight_season` | integer |  |
| `highlight_seasonType` | character |  |
| `highlight_teamAbbr` | character |  |
| `highlight_teamId` | character |  |
| `highlight_week` | integer |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_leaders, nfl_ngs_play_is_highlight
>>> lead = nfl_ngs_leaders(category="speed", season=2024, season_type="REG")
>>> gid, pid = lead["play_gameId"][0], lead["play_playId"][0]
>>> hl = nfl_ngs_play_is_highlight(game_id=gid, play_id=pid)
>>> hl.select(["gameId", "playId", "isHighlight"]).head()
```

### `nfl_ngs_statboard(stat_type: 'str' = 'passing', season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_statboard}

NGS season/week statboard leaderboard for a stat family (one row per player).

Wraps `/api/statboard/{passing,receiving,rushing}`. Each record is a flat
per-player stat line (e.g. for passing: `completionPercentageAboveExpectation`,
`avgTimeToThrow`, `aggressiveness`, `passerRating` ...). The player's bio
is nested under a `player` object and is flattened to `player_*` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` | `'passing'` | one of `"passing"`, `"receiving"`, `"rushing"`. (For the cross-stat highlight board use `nfl_ngs_statboard_leaders`.) |
| `season` | `int` | `2024` | season year, e.g. `2024`. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | single week to filter to; `None` returns the full-season board. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per qualifying player.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `avgAirDistance` | double |  |
| `avgAirYardsDifferential` | double |  |
| `avgAirYardsToSticks` | double |  |
| `avgCompletedAirYards` | double |  |
| `avgIntendedAirYards` | double |  |
| `avgTimeToThrow` | double |  |
| `completionPercentage` | double |  |
| `completionPercentageAboveExpectation` | double |  |
| `completions` | integer | The number of completed passes. |
| `expectedCompletionPercentage` | double |  |
| `gamesPlayed` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `maxAirDistance` | double |  |
| `maxCompletedAirDistance` | double |  |
| `passTouchdowns` | integer |  |
| `passYards` | integer |  |
| `passerRating` | double |  |
| `playerName` | character |  |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character |  |
| `position` | character | Primary position as reported by NFL.com |
| `teamId` | character |  |
| `player_season` | integer |  |
| `player_currentTeamId` | character |  |
| `player_displayName` | character |  |
| `player_esbId` | character |  |
| `player_firstName` | character |  |
| `player_footballName` | character |  |
| `player_gsisId` | character |  |
| `player_gsisItId` | integer |  |
| `player_headshot` | character | URL to the player headshot image. |
| `player_jerseyNumber` | integer |  |
| `player_lastName` | character |  |
| `player_position` | character | Position of the player accordinng to NGS |
| `player_positionGroup` | character |  |
| `player_shortName` | character |  |
| `player_smartId` | character |  |
| `player_status` | character |  |
| `player_uniformNumber` | character |  |
| `player_ngsPosition` | character |  |
| `player_ngsPositionGroup` | character |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_statboard
>>> qb = nfl_ngs_statboard(stat_type="passing", season=2024, season_type="REG")
>>> qb.select(["playerName", "passerRating", "completionPercentageAboveExpectation"]).head()
```

### `nfl_ngs_statboard_leaders(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_statboard_leaders}

NGS cross-stat "leaders" board, stacked long with a `category` column.

Wraps `/api/statboard/leaders`, which bundles several short top-N lists of
mixed shape (`fastestBallCarriers`, `fastestSacks`, `longestCompletions`,
`highestSeparation`, `rushYardsOverExpected`, `completionPctAboveExpected`,
`avgYACAboveExpected`). Each list is normalized separately and concatenated
diagonally (union of columns; missing cells become null), with a `category`
column recording which board each row came from.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | optional single-week filter. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` stacking every leader list, with a `category` column. Empty frame if no lists are present.

| col_name | type | description |
|---|---|---|
| `category` | character | Broader category of player positions |
| `leader_esbId` | character |  |
| `leader_firstName` | character |  |
| `leader_gsisId` | character |  |
| `leader_jerseyNumber` | integer |  |
| `leader_lastName` | character |  |
| `leader_playerName` | character |  |
| `leader_position` | character |  |
| `leader_positionGroup` | character |  |
| `leader_shortName` | character |  |
| `leader_teamAbbr` | character |  |
| `leader_teamId` | character |  |
| `leader_week` | integer |  |
| `leader_yards` | integer |  |
| `leader_inPlayDist` | double |  |
| `leader_maxSpeed` | double |  |
| `leader_headshot` | character |  |
| `play_gameId` | integer |  |
| `play_playId` | integer |  |
| `play_sequence` | integer |  |
| `play_down` | integer |  |
| `play_gameClock` | character |  |
| `play_gameKey` | integer |  |
| `play_health_playerTracking` | character |  |
| `play_health_ballTracking` | character |  |
| `play_homeScore` | integer |  |
| `play_isBigPlay` | logical |  |
| `play_isEndQuarter` | logical |  |
| `play_isGoalToGo` | logical |  |
| `play_isPenalty` | logical |  |
| `play_isSTPlay` | logical |  |
| `play_isScoring` | logical |  |
| `play_playDescription` | character |  |
| `play_playState` | character |  |
| `play_playStats` | integer |  |
| `play_playType` | character |  |
| `play_playTypeCode` | integer |  |
| `play_possessionTeamId` | character |  |
| `play_preSnapHomeScore` | integer |  |
| `play_preSnapVisitorScore` | integer |  |
| `play_quarter` | integer |  |
| `play_timeOfDayUTC` | character |  |
| `play_visitorScore` | integer |  |
| `play_yardline` | character |  |
| `play_yardlineNumber` | integer |  |
| `play_yardlineSide` | character |  |
| `play_yardsToGo` | integer |  |
| `play_absoluteYardlineNumber` | integer |  |
| `play_actualYardlineForFirstDown` | double |  |
| `play_actualYardsToGo` | double |  |
| `play_endGameClock` | character |  |
| `play_isChangeOfPossession` | logical |  |
| `play_playDirection` | character |  |
| `play_startGameClock` | character |  |
| `leader_time` | double |  |
| `leader_seasonAvg` | double |  |
| `leader_teamAvg` | double |  |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `avgAirDistance` | double |  |
| `avgAirYardsDifferential` | double |  |
| `avgAirYardsToSticks` | double |  |
| `avgCompletedAirYards` | double |  |
| `avgIntendedAirYards` | double |  |
| `avgTimeToThrow` | double |  |
| `completionPercentage` | double |  |
| `completionPercentageAboveExpectation` | double |  |
| `completions` | integer | The number of completed passes. |
| `expectedCompletionPercentage` | double |  |
| `gamesPlayed` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `maxAirDistance` | double |  |
| `maxCompletedAirDistance` | double |  |
| `passTouchdowns` | integer |  |
| `passYards` | integer |  |
| `passerRating` | double |  |
| `playerName` | character |  |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character |  |
| `position` | character | Primary position as reported by NFL.com |
| `teamId` | character |  |
| `player_season` | integer |  |
| `player_currentTeamId` | character |  |
| `player_displayName` | character |  |
| `player_esbId` | character |  |
| `player_firstName` | character |  |
| `player_footballName` | character |  |
| `player_gsisId` | character |  |
| `player_gsisItId` | integer |  |
| `player_jerseyNumber` | integer |  |
| `player_lastName` | character |  |
| `player_position` | character | Position of the player accordinng to NGS |
| `player_positionGroup` | character |  |
| `player_shortName` | character |  |
| `player_status` | character |  |
| `player_uniformNumber` | character |  |
| `player_headshot` | character | URL to the player headshot image. |
| `player_smartId` | character |  |
| `player_ngsPosition` | character |  |
| `player_ngsPositionGroup` | character |  |
| `avgCushion` | double |  |
| `avgExpectedYAC` | double |  |
| `avgSeparation` | double |  |
| `avgYAC` | double |  |
| `avgYACAboveExpectation` | double |  |
| `catchPercentage` | double |  |
| `percentShareOfIntendedAirYards` | double |  |
| `recTouchdowns` | integer |  |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `yards` | integer | The number of receiving yards |
| `avgTimeToLos` | double |  |
| `expectedRushYards` | double |  |
| `rushAttempts` | integer |  |
| `rushPctOverExpected` | double |  |
| `rushTouchdowns` | integer |  |
| `rushYards` | integer |  |
| `rushYardsOverExpected` | double |  |
| `rushYardsOverExpectedPerAtt` | double |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_ngs_statboard_leaders
>>> bd = nfl_ngs_statboard_leaders(season=2024, season_type="REG")
>>> bd["category"].unique().to_list()
```

### `nfl_token_gen(client_key: 'Optional[str]' = None, client_secret: 'Optional[str]' = None) -> 'str'` {#nfl_token_gen}

Mint a fresh `api.nfl.com` access token via `/identity/v3/token`.

Wraps the anonymous device-token grant the NFL.com web app uses. Credentials
resolve in this order: explicit `client_key`/`client_secret` args ->
`NFL_CLIENT_KEY`/`NFL_CLIENT_SECRET` env vars -> the bundled public
`WEB_DESKTOP` web-app credentials.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `client_key` | `Optional[str]` | `None` | Override the client key (else env var, else the web default). |
| `client_secret` | `Optional[str]` | `None` | Override the client secret (else env var, else the default). |

**Returns**

The bearer `accessToken` string.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_token_gen
token = nfl_token_gen()
assert isinstance(token, str) and token.startswith("ey")
```

### `nfl_week_games(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'int' = 1, headers: 'Optional[Dict[str, str]]' = None, return_as_pandas: 'bool' = False)` {#nfl_week_games}

Parsed `api.nfl.com` week schedule -- one row per game (polars/pandas frame).

Tidy wrapper over `nfl_game_schedule`: flattens the `games` list into a
DataFrame with `id` (uuid game id), `season`/`seasonType`/`week`,
`date`, `status_*`, and `homeTeam_*` / `awayTeam_*` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. season_type (str): `"PRE"`/`"REG"`/`"POST"`. |
| `season_type` | `str` | `'REG'` |  |
| `week` | `int` | `1` | week number. headers: reuse a `nfl_headers_gen` dict. |
| `headers` | `Optional[Dict[str, str]]` | `None` |  |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per game.

| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `category` | character | Broader category of player positions |
| `date` | character | Date in YYYY-MM-DD format. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `gameType` | character | Game type identifier (3 for playoffs). |
| `international` | logical |  |
| `neutralSite` | logical | Whether the game is at a neutral site. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character |  |
| `status` | character | Status label. |
| `week` | integer | Season week. |
| `weekType` | character |  |
| `externalIds` | character |  |
| `ticketUrl` | character |  |
| `ticketVendors` | character |  |
| `extensions` | character |  |
| `version` | integer |  |
| `homeTeam_id` | character |  |
| `homeTeam_currentLogo` | character |  |
| `homeTeam_fullName` | character |  |
| `awayTeam_id` | character |  |
| `awayTeam_currentLogo` | character |  |
| `awayTeam_fullName` | character |  |
| `broadcastInfo_homeNetworkChannels` | character |  |
| `broadcastInfo_awayNetworkChannels` | character |  |
| `broadcastInfo_internationalWatchOptions` | character |  |
| `broadcastInfo_streamingNetworks` | character |  |
| `broadcastInfo_territory` | character |  |
| `broadcastInfo_audioNetworks` | character |  |
| `venue_id` | character | Unique venue identifier. |
| `venue_name` | character | Venue name. |
| `venue_city` | character | Venue city. |
| `venue_country` | character |  |

**Example**

```python
>>> from sportsdataverse.nfl import nfl_week_games
>>> sched = nfl_week_games(season=2024, season_type="REG", week=1)
>>> sched.select(["id", "homeTeam_fullName", "awayTeam_fullName"]).head()
```

### `reset_config() -> 'NflConfig'` {#reset_config}

Reset the active config to its env-var-derived defaults.

Convenience for tests / interactive sessions that want to undo a chain
of `update_config()` calls without restarting the interpreter.

**Example**

```python
from sportsdataverse.nfl import update_config, reset_config
update_config(cache_mode="off", timeout=5)
# ... do work ...
reset_config()  # back to env-derived defaults
```

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

Normalize one ESPN scoreboard `event` into a flatter shape.

Splits the competitors list into `home` / `away` siblings, hoists
notes / broadcast metadata onto the competition root, and drops the
fields the schedule helper does not need (`odds`, `leaders`,
`geoBroadcasts`, etc.). Used internally by
`espn_nfl_schedule`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `Dict` |  | A single `events[i]` dict from the ESPN scoreboard endpoint. |

**Returns**

The mutated event dict with normalized `home` / `away` / broadcast keys.

**Example**

```python
from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nfl_schedule import scoreboard_event_parsing
url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
payload = download(url=url).json()
for ev in payload.get("events", []):
    scoreboard_event_parsing(ev)
    ev["competitions"][0]["home"]["abbreviation"]
```

### `update_config(**kwargs: 'object') -> 'NflConfig'` {#update_config}

Update the active config in place.

**Returns**

The (mutated) global config object, for chaining or inspection.

**Example**

```python
from sportsdataverse.nfl import update_config
update_config(cache_mode="filesystem", cache_duration=3600)

# Disable caching for development

update_config(cache_mode="off")

# Point cache at a custom directory

update_config(cache_dir="~/sdv-cache")
```
