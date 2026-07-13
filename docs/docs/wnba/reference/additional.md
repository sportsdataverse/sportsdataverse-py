---
title: WNBA — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# WNBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wnba`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_wnba_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_wnba_game_officials}

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

### `espn_wnba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_wnba_player_stats}

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
| `defensive_avg48_defensive_rebounds` | double | Player's defensive rebounds per 48 minutes of play, drawn from the defensive statistical category. |
| `defensive_avg48_blocks` | double | Player's blocked shots per 48 minutes of play, drawn from the defensive statistical category. |
| `defensive_avg48_steals` | double | Player's steals per 48 minutes of play, drawn from the defensive statistical category. |
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
| `general_avg48_rebounds` | double | Player's total rebounds (offensive plus defensive) per 48 minutes of play, drawn from the general statistical category. |
| `general_avg48_fouls` | double | Player's personal fouls per 48 minutes of play, drawn from the general statistical category. |
| `general_avg48_flagrant_fouls` | double | Player's flagrant fouls per 48 minutes of play, drawn from the general statistical category. |
| `general_avg48_technical_fouls` | double | Player's technical fouls per 48 minutes of play, drawn from the general statistical category. |
| `general_avg48_ejections` | double | Player's ejections per 48 minutes of play, drawn from the general statistical category. |
| `general_avg48_disqualifications` | double | Player's disqualifications (fouling out) per 48 minutes of play, drawn from the general statistical category. |
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
| `offensive_avg48_field_goals_made` | double | Player's field goals made per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_field_goals_attempted` | double | Player's field goal attempts per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_three_point_field_goals_made` | double | Player's three-point field goals made per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_three_point_field_goals_attempted` | double | Player's three-point field goal attempts per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_free_throws_made` | double | Player's free throws made per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_free_throws_attempted` | double | Player's free throw attempts per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_points` | double | Player's points scored per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_offensive_rebounds` | double | Player's offensive rebounds per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_assists` | double | Player's assists per 48 minutes of play, drawn from the offensive statistical category. |
| `offensive_avg48_turnovers` | double | Player's turnovers per 48 minutes of play, drawn from the offensive statistical category. |
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

### `espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_wnba_schedule}

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
| `time_valid` | logical | Whether the start time is confirmed. |
| `neutral_site` | logical | Neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Whether the game is recent. |
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
| `home_score` | character | Home team score at the time of the play. |
| `home_winner` | logical | Whether the home team won. |
| `home_linescores` | list | Period-by-period point totals for the home team, stored as a list of integer scores. |
| `home_records` | character | Win-loss record strings for the home team across relevant splits (e.g., overall, home/away, conference). |
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
| `away_score` | character | Away team score at the time of the play. |
| `away_winner` | logical | Whether the away team won. |
| `away_linescores` | list | Period-by-period point totals for the away team, stored as a list of integer scores. |
| `away_records` | character | Win-loss record strings for the away team across relevant splits (e.g., overall, home/away, conference). |
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

### `espn_wnba_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'` {#espn_wnba_team_stats}

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

## Dataset loaders

### `load_wnba_stats_lineups(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_wnba_stats_lineups}

Load season-level WNBA 5-man lineup statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per lineup-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `lineups_{base, advanced}` assets filtered to `group_quantity == 5` — matching the old `wnba_stats_lineups` tag's 5-man-only, Base+Advanced-only coverage. Call the cube's `lineups_*` assets directly (unfiltered) for 2/3/4-man lineups or the other 4 measure types.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_lineups
df = load_wnba_stats_lineups(seasons=2026)
print(df.shape)
```

### `load_wnba_stats_player_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_wnba_stats_player_season_stats}

Load season-level WNBA player statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per player-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `player_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Usage`/`Defense` — matches the old `wnba_stats_player_season_stats` tag's coverage; player-level `Opponent`/`Four Factors` are empty upstream and were never populated by either version).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_player_season_stats
df = load_wnba_stats_player_season_stats(seasons=2026)
print(df.shape)

# Pipeline next step (Advanced-only rows)

import polars as pl
adv = df.filter(pl.col("measure_type") == "Advanced")
```

### `load_wnba_stats_standings(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_wnba_stats_standings}

Load season-level WNBA standings (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season, read from the `wnba_stats_leaguedash` cube's `standings` asset -- the same underlying `leaguestandingsv3` endpoint/params as the old `wnba_stats_standings` tag, so this is close to a pure passthrough.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_standings
df = load_wnba_stats_standings(seasons=2026)
print(df.shape)
```

### `load_wnba_stats_team_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_wnba_stats_team_season_stats}

Load season-level WNBA team statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `team_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Defense`/ `Opponent` — matches the old `wnba_stats_team_season_stats` tag's coverage; team-level `Usage`/`Four Factors` are empty upstream).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_team_season_stats
df = load_wnba_stats_team_season_stats(seasons=2026)
print(df.shape)
```

## Utilities & helpers

### `most_recent_wnba_season()` {#most_recent_wnba_season}

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

### `build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'` {#build_athlete_identity_lookup}

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### `espn_wnba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_wnba_teams}

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
| `team_logos` | integer | Team logo metadata. |
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

### `make_prob_by_context(ptshots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"` {#make_prob_by_context}

Marginal FG% tables by defender distance and by shot clock.

The public API exposes defender-distance and shot-clock only as aggregate
bucket tables (`playerdashptshots`), not per-shot fields, so this
aggregates `Σfgm/Σfga` across players within each bucket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ptshots` | `DataFrame` |  | The stacked `playerdashptshots` fixture — one frame with a `result_set` tag (`ClosestDefenderShooting` / `ShotClockShooting`) plus `bucket, fga, fgm`. |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

`{"defender": frame, "shot_clock": frame}` each with rows per `bucket` (`bucket, fga, fgm, fg_pct`). Missing result sets return the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context
tables = make_prob_by_context(ptshots)
tables["defender"].sort("fg_pct")
```

### `make_prob_joint(defender: 'pl.DataFrame', shot_clock: 'pl.DataFrame', overall_fg_pct: 'float', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#make_prob_joint}

Independence-combined defender x shot-clock make probability.

Combines the two marginal FG% tables under a conditional-independence
assumption via odds multipliers: `odds(p) = p/(1-p)`;
`odds_joint = odds_overall * (odds_def/odds_overall) *
(odds_clock/odds_overall)`; `joint = odds_joint/(1+odds_joint)`. This
assumes defender distance and shot-clock effects are independent given the
league baseline — a simplification (a late clock correlates with tighter
defense), documented here so callers weigh it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `defender` | `DataFrame` |  | The `"defender"` marginal table from `make_prob_by_context` (`bucket, fg_pct`). |
| `shot_clock` | `DataFrame` |  | The `"shot_clock"` marginal table (`bucket, fg_pct`). |
| `overall_fg_pct` | `float` |  | The league overall FG% baseline. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(close_def_dist_range, shot_clock_range)`: `close_def_dist_range:Utf8, shot_clock_range:Utf8, joint_fg_pct:Float64`. Empty inputs return the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context, make_prob_joint
t = make_prob_by_context(ptshots)
joint = make_prob_joint(t["defender"], t["shot_clock"], 0.47)
```

### `score_shot_xpoints(shots: 'pl.DataFrame', league_avgs: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#score_shot_xpoints}

Score each shot with expected points from the league-average baseline.

Joins the per-shot frame to the zone baseline (falling back to the
within-`shot_zone_range` mean when a zone triple is unmatched) and adds
`shot_value` (3 for a `3PT` shot else 2), `xpoints = base_fg_pct *
shot_value`, and `actual_points = shot_made_flag * shot_value`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Per-shot `Shot_Chart_Detail` frame (needs `shot_type` + the three zone keys + `shot_made_flag`). |
| `league_avgs` | `DataFrame` |  | The `LeagueAverages` frame (see `xpoints_baseline`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The input `shots` plus `shot_value:Int64, base_fg_pct:Float64, xpoints:Float64, actual_points:Float64`. Empty input returns the augmented schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
scored = score_shot_xpoints(shots, league_avgs)

# Pipeline next step (one line)

scored.group_by("player_id").agg(pl.col("xpoints").sum())
```

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### `shooter_talent(scored_shots: 'pl.DataFrame', *, league_id: 'str' = '00', min_attempts: 'int' = 50, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#shooter_talent}

Regressed shooter true-talent: make%-above-expected, shrunk to the mean.

Aggregates `score_shot_xpoints` output per shooter and regresses the
raw over-expected rate toward zero by `n/(n+k)` (`k =
get_shrinkage_k(league_id)`, fitted split-half). **As-of leakage
boundary:** to score a shooter's talent for shots after date *D*, pass
only that shooter's shots before *D* -- this function does not enforce the
cut itself.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `shot_made_flag`, `base_fg_pct`, `xpoints`, `actual_points`). |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `min_attempts` | `int` | `50` | Drop shooters with fewer attempts (unstable estimate). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `player_id`: `player_id:Int64, n_att:Int64, actual_makes:Int64, exp_makes:Float64, points_above_expected:Float64, raw_above_pct:Float64, talent_pct:Float64`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shooter_talent
talent = shooter_talent(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

talent.sort("talent_pct", descending=True).head(15)
```

### `shot_selection_quality(scored_shots: 'pl.DataFrame', *, min_attempts: 'int' = 50, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#shot_selection_quality}

Player shot-selection quality: mean expected value vs the league mean.

`xev_per_shot` is a player's mean `xpoints` (the value of the LOOKS
they take, independent of makes); `selection_quality` is that minus the
league-wide mean `xpoints` over the same frame -- a rim-and-three diet
scores positive, a mid-range diet negative.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `xpoints`). |
| `min_attempts` | `int` | `50` | Drop players with fewer attempts. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `player_id`: `player_id:Int64, n_att:Int64, xev_per_shot:Float64, league_xev_per_shot:Float64, selection_quality:Float64`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shot_selection_quality
sel = shot_selection_quality(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

sel.sort("selection_quality", descending=True).head(15)
```

### `wnba_aging_curve(*, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#wnba_aging_curve}

WNBA aging curve -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_aging_curve.nba_aging_curve` for the
full contract; this is a by-reference re-export, same algorithm, women's
bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

Frame `age:Int64, rel_value:Float64, peak_age:Float64`.

**Example**

```python
from sportsdataverse.wnba import wnba_aging_curve
curve = wnba_aging_curve()
```

### `wnba_availability(seasons: "'int | list[int]'", *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#wnba_availability}

WNBA availability -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_availability.nba_availability` for the
full contract; `avail_pct` is availability, not skill.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (start year) or list of seasons. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, season:Int64, avail_pct:Float64`.

**Example**

```python
from sportsdataverse.wnba import wnba_availability
proj = wnba_availability(2023)
```

### `wnba_career_trajectory(player_values: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#wnba_career_trajectory}

WNBA career trajectory -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_aging_curve.nba_career_trajectory` for
the full contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_values` | `DataFrame` |  | Frame `player_id, age:Int64, value:Float64`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`player_values` plus `age_adjusted_value` and `proj_next_value`.

**Example**

```python
import polars as pl
from sportsdataverse.wnba import wnba_career_trajectory
player_values = pl.DataFrame({"player_id": ["1"], "age": [26], "value": [10.0]})
wnba_career_trajectory(player_values)
```

### `wnba_draft_model(draft_year: "'int | list[int]'", *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#wnba_draft_model}

Project WNBA prospect career value + draft probability from draft slot.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year (e.g. `2023`) or list of years. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_career_value:Float64, draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8`. Empty input returns the zero-row schema, never raises.

**Example**

```python
from sportsdataverse.wnba import wnba_draft_model
board = wnba_draft_model(2023)
```

### `wnba_enhanced_pbp(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_enhanced_pbp}

Return a normalised enhanced play-by-play frame for a WNBA game.

Fetches the raw `playbyplayv3` payload from `stats.wnba.com` via
`~sportsdataverse.wnba.wnba_stats.wnba_stats_playbyplayv3` then
delegates all transformation to the league-agnostic
`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
core with `league_id="10"`.  Never raises on malformed or empty
payloads — returns a zero-row frame instead.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema `sportsdataverse.nba.nba_enhanced_pbp.ENHANCED_PBP_SCHEMA`. Key columns include `game_id` (Utf8), `action_number` (Int64), `period` (Int64), `seconds_remaining` (Float64), `team_id` (Int64), `person_id` (Int64), `is_substitution` (Boolean), and one Boolean flag per event type.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
df = wnba_enhanced_pbp("1022400001")
print(df.shape)

# Pandas output

df_pd = wnba_enhanced_pbp("1022400001", return_as_pandas=True)
print(type(df_pd))

# Filter substitution events

subs = df.filter(df["is_substitution"] == True)  # noqa: E712
print(subs.select(["period", "seconds_remaining", "person_id"]))
```

### `wnba_expected_turnovers(season: 'str', *, base: "'Optional[pl.DataFrame]'" = None, player_mix: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_expected_turnovers}

WNBA expected turnovers / ball-security skill (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_expected_turnovers.nba_expected_turnovers`
to the women's league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base`) frame. |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_expected_turnovers.nba_expected_turnovers`.

**Example**

```python
from sportsdataverse.wnba import wnba_expected_turnovers
t = wnba_expected_turnovers("2024")
print(t.sort("ball_security_skill", descending=True).head())
```

### `wnba_foul_drawing(season: 'str', *, base: "'Optional[pl.DataFrame]'" = None, advanced: "'Optional[pl.DataFrame]'" = None, player_mix: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_foul_drawing}

WNBA foul-drawing / FT-generation (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_foul_drawing.nba_foul_drawing` to the
women's league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base`) frame. |
| `advanced` | `Optional[DataFrame]` | `None` | Injected `Advanced`-measure frame. |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_foul_drawing.nba_foul_drawing`.

**Example**

```python
from sportsdataverse.wnba import wnba_foul_drawing
f = wnba_foul_drawing("2024")
print(f.sort("foul_draw_skill", descending=True).head())
```

### `wnba_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_in_game_win_prob}

WNBA in-game win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  |  |
| `pregame_home_prob` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `wnba_matchup_drapm(season: 'str', *, matchups: "'Optional[pl.DataFrame]'" = None, config: "'Optional[PlaytypeConfig]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_matchup_drapm}

WNBA matchup defensive RAPM (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm` to the
women's league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame. |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig` override. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm`.

**Example**

```python
from sportsdataverse.wnba import wnba_matchup_drapm
d = wnba_matchup_drapm("2024")
print(d.sort("matchup_drapm", descending=True).head())
```

### `wnba_on_court(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_on_court}

Return the rotation-keyed on-court player frame for a WNBA game.

Makes three network calls (play-by-play v3, game rotation,
box-score traditional v3), infers on-court rosters from the rotation
stints via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
and returns one row per PBP action with ten Int64 player-ID columns
(`home_player_1..5` / `away_player_1..5`).  All transformation is
performed by the shared `nba/` core with `league_id="10"` forwarded
to the rotation endpoint.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per PBP action and columns `home_player_1` … `home_player_5`, `away_player_1` … `away_player_5` (all Int64), plus the `action_number` join key.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_on_court
oc = wnba_on_court("1022400001")
print(oc.select(["action_number", "home_player_1"]).head())

# Pandas output

oc_pd = wnba_on_court("1022400001", return_as_pandas=True)
print(type(oc_pd))

# Join on enhanced PBP

from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
enh = wnba_enhanced_pbp("1022400001")
joined = enh.join(oc, on="action_number", how="left")
```

### `wnba_pbp_disk(game_id, path_to_json)` {#wnba_pbp_disk}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### `wnba_play_context(game_id: 'str', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_play_context}

Return a WNBA game's possessions with the full CTG play-context surface.

WNBA sibling of
`~sportsdataverse.nba.nba_play_context.nba_play_context` — the
Cleaning the Glass recreation (possession start-type taxonomy + the
halfcourt / transition / putback contexts + CTG's garbage-time and heave
filters). One network call (`playbyplayv3` on `stats.wnba.com`); every
transformation is done by the league-agnostic
`~sportsdataverse.nba.nba_play_context.add_play_context` core, so
there is **no WNBA-specific classification logic** to drift.

Two caveats worth stating plainly:

* **CTG is NBA-only.** There is no published WNBA play-context table to
  calibrate against, so `transition_seconds` inherits the NBA's fitted
  6.0 s default. The WNBA fixtures land inside the NBA's transition-frequency
  gate at that value (`tests/wnba/test_wnba_play_context_shim.py`), which
  is a sanity check on the shared engine — not evidence that 6.0 s is the
  *right* WNBA cutoff. Re-fit it if a WNBA oracle ever appears.
* Shot-zone boundaries are league-agnostic (feet from the rim), and the
  corner-three test uses the same legacy coordinates, which the WNBA feed
  also ships.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier (e.g. `"1022400001"`). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff, in seconds. |
| `transition_variant` | `str` | `'hoop_math'` | See `~sportsdataverse.nba.nba_play_context.add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The possession frame (`POSSESSIONS_SCHEMA`) plus `~sportsdataverse.nba.nba_play_context.PLAY_CONTEXT_POSSESSIONS_SCHEMA`. Empty or malformed payloads return a zero-row frame — never raises.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_play_context
poss = wnba_play_context("1022400001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate (CTG's default filtered view)

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False)  # noqa: E712
    & (pl.col("is_heave_possession") == False)  # noqa: E712
)
print(clean["is_transition"].mean())
```

### `wnba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_player_props}

WNBA player props (league_id='10'). See sportsdataverse.nba.nba_player_props.nba_player_props.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `game_id` | `str` |  |  |
| `home_team_id` | `str` |  |  |
| `away_team_id` | `str` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `wnba_playtype_ratings(season: 'str', *, off_team: "'Optional[pl.DataFrame]'" = None, def_team: "'Optional[pl.DataFrame]'" = None, schedule: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_playtype_ratings}

WNBA Synergy play-type-adjusted offense/defense (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_playtype.nba_playtype_ratings` to the
women's league. Synergy coverage is sparse for the WNBA; an empty upstream
fetch degrades to a zero-row frame (never raises).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_playtype.nba_playtype_ratings`.

**Example**

```python
from sportsdataverse.wnba import wnba_playtype_ratings
r = wnba_playtype_ratings("2024")
print(r.sort("adj_off", descending=True).head())
```

### `wnba_possessions(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_possessions}

Return the possession-level lineup stint matrix for a WNBA game.

Builds possessions from the enhanced PBP via
`~sportsdataverse.nba.nba_possessions.build_possessions`, resolves
on-court rosters via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
then attaches the 5v5 lineups via
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.
All transformation is performed by the shared `nba/` cores — no WNBA-
specific logic.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema combining `POSSESSIONS_SCHEMA` and ten lineup columns: `off_player_1` … `off_player_5`, `def_player_1` … `def_player_5` (all Int64). One row per possession. Empty or malformed inputs return a zero-row frame.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_possessions
poss = wnba_possessions("1022400001")
print(poss.shape)

# Pandas output

poss_pd = wnba_possessions("1022400001", return_as_pandas=True)
print(type(poss_pd))

# Total points check

total = int(poss["points"].sum())
print(f"Total points scored: {total}")
```

### `wnba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_predict_games}

WNBA vectorized pregame predictions (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_predict_games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  |  |
| `ratings` | `DataFrame` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `wnba_predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'` {#wnba_predict_margin}

WNBA expected margin (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  |  |
| `away_net` | `float` |  |  |
| `home_pace` | `float` |  |  |
| `away_pace` | `float` |  |  |
| `neutral` | `bool` | `False` |  |
| `league_id` | `str` | `'00'` |  |

### `wnba_predict_total(home_off: 'float', home_def: 'float', away_off: 'float', away_def: 'float', home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'` {#wnba_predict_total}

WNBA expected total (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_total.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  |  |
| `home_def` | `float` |  |  |
| `away_off` | `float` |  |  |
| `away_def` | `float` |  |  |
| `home_pace` | `float` |  |  |
| `away_pace` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |

### `wnba_rapm_from_games(game_ids: 'Sequence[str]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_rapm_from_games}

Compute per-player RAPM estimates over a sequence of WNBA games.

Iterates *game_ids*, builds the possession-level stint matrix for each
via `wnba_possessions`, concatenates the results, and fits a
ridge-regression RAPM model via
`~sportsdataverse.nba.nba_rapm.nba_rapm`.  Games whose possession
frame is empty (e.g. a malformed payload) are silently skipped.  Returns
a zero-row frame when no valid possessions are found.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[str]` |  | Sequence of WNBA game identifier strings. |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per player and columns `player_id` (Int64), `o_rapm` (Float64), `d_rapm` (Float64), `rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_rapm_from_games
rapm = wnba_rapm_from_games(["1022400001", "1022400003"])
print(rapm.sort("rapm", descending=True).head())

# Pandas output

rapm_pd = wnba_rapm_from_games(["1022400001"], return_as_pandas=True)
print(type(rapm_pd))

# Multi-season aggregation

import polars as pl
game_ids = pl.read_parquet("wnba_schedule.parquet")["game_id"].to_list()
rapm = wnba_rapm_from_games(game_ids)
print(rapm.sort("rapm", descending=True).head(10))
```

### `wnba_rookie_projection(draft_year: "'int | list[int]'", *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#wnba_rookie_projection}

WNBA rookie/sophomore projection -- composes the WNBA draft/aging/availability pieces.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year or list of years. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64, proj_soph_value:Float64, proj_rookie_min:Float64, proj_avail_pct:Float64, pro_tier:Utf8`. Empty input -> zero-row schema.

**Example**

```python
from sportsdataverse.wnba import wnba_rookie_projection
board = wnba_rookie_projection(2023)
```

### `wnba_shot_value(player_ids: "'list[int]'", season: 'str', *, include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"` {#wnba_shot_value}

WNBA one-call shot-value spine (`league_id="10"`).

Thin wrapper binding `sportsdataverse.nba.nba_shot_value.nba_shot_value`
to the women's league; fetches each player's `shotchartdetail`, scores
per-shot expected points from the free `LeagueAverages` zone table, and
returns the scored shots plus shooter talent, selection quality, and
zone-value maps (and the defender/shot-clock context tables when
`include_context=True`). Women's court geometry + shrinkage constant are
keyed `"10"` in `nba_shot_value_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.wnba import wnba_shot_value
out = wnba_shot_value([1628886], "2024")
out["talent"].head()
```

### `wnba_team_clutch(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#wnba_team_clutch}

WNBA clutch skill (league_id='10'). See sportsdataverse.nba.nba_clutch.nba_team_clutch.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `wnba_team_ratings(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#wnba_team_ratings}

WNBA team ratings (league_id='10'). See sportsdataverse.nba.nba_team_ratings.nba_team_ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  |  |
| `league_id` | `str` | `'00'` |  |
| `as_of_date` | `Union[date, None]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `wnba_tracking_drive_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_tracking_drive_value}

WNBA drive value + rim-pressure (`league_id="10"` by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_drive_value`
for the full recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, drives:Float64, drive_pts:Float64, drive_baseline_rate:Float64, drive_expected:Float64, drive_pts_oe:Float64, drive_pts_oe_per_36:Float64, drive_fta:Float64, rim_pressure:Float64, drive_ast:Float64, drive_tov:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_drive_value
df = wnba_tracking_drive_value(2024)
print(df.sort("drive_pts_oe", descending=True).head())
```

### `wnba_tracking_pass_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, fetch_potential_assists: 'bool' = False, max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _pass_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_tracking_pass_value}

WNBA expected-assists / passer value (`league_id="10"` by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_pass_value`
for the full recipe (Passing-measure proxy + optional `playerdashptpass`
enrichment).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `fetch_potential_assists` | `bool` | `False` | Enrich the top passers with `playerdashptpass` potential-assist counts. |
| `max_players` | `int` | `0` | Cap on per-player enrichment fetches; `0` disables enrichment regardless of `fetch_potential_assists`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_pass_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptpass`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, ast:Float64, passes:Float64, ast_baseline_rate:Float64, ast_expected:Float64, ast_oe:Float64, ast_oe_per_36:Float64, ast_pts_created:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_pass_value
df = wnba_tracking_pass_value(2024)
print(df.sort("ast_oe", descending=True).head())
```

### `wnba_tracking_reb_oe(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_tracking_reb_oe}

WNBA rebounding-over-expected (`league_id="10"` by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_reb_oe`
for the full recipe (contest-difficulty-adjusted expected rebounds,
role-bucket baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, reb:Float64, reb_chances:Float64, reb_baseline_rate:Float64, reb_expected:Float64, reb_oe:Float64, reb_oe_per_36:Float64, oreb_oe:Float64, dreb_oe:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_reb_oe
df = wnba_tracking_reb_oe(2024)
print(df.sort("reb_oe", descending=True).head())
```

### `wnba_tracking_rim_protect_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, source: 'str' = 'leaguedash', max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _defend_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_tracking_rim_protect_value}

WNBA rim-protection / shot-defend points-saved (`league_id="10"`

by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_rim_protect_value`
for the full recipe (bucket-mean defended-rate baseline; optional
`playerdashptshotdefend` rim-band enrichment).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `source` | `str` | `'leaguedash'` | `"leaguedash"` (default) or `"shotdefend"`. |
| `max_players` | `int` | `0` | Cap on per-player `shotdefend` enrichment fetches; ignored unless `source="shotdefend"`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_defend_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptshotdefend`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, d_fga:Float64, d_fgm:Float64, d_fg_pct:Float64, normal_fg_pct:Float64, rim_protect_pts_saved:Float64, rim_protect_pts_saved_per_36:Float64, source:Utf8, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_rim_protect_value
df = wnba_tracking_rim_protect_value(2024)
print(df.sort("rim_protect_pts_saved", descending=True).head())
```

### `wnba_tracking_shot_diet_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_tracking_shot_diet_value}

WNBA catch-&-shoot vs pull-up points-over-expected (`league_id="10"`

by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_shot_diet_value`
for the full recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to each fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute each measure's baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, cs_fga:Float64, cs_pts:Float64, cs_pts_oe:Float64, pu_fga:Float64, pu_pts:Float64, pu_pts_oe:Float64, shot_diet_delta:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_shot_diet_value
df = wnba_tracking_shot_diet_value(2024)
print(df.sort("cs_pts_oe", descending=True).head())
```

### `wnba_tracking_touch_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#wnba_tracking_touch_value}

WNBA touch / possession-time value (`league_id="10"` by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_touch_value`
for the full recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, touches:Float64, pts:Float64, touch_baseline_rate:Float64, touch_expected:Float64, pts_per_touch_oe:Float64, time_of_poss:Float64, time_of_poss_eff:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_touch_value
df = wnba_tracking_touch_value(2024)
print(df.sort("pts_per_touch_oe", descending=True).head())
```

### `wnba_win_prob_from_margin(exp_margin: 'float', *, league_id: 'str' = '00') -> 'float'` {#wnba_win_prob_from_margin}

WNBA home win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.win_prob_from_margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |

### `zone_value_map(scored_shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#zone_value_map}

Per-player per-zone value map: points and expected points per shot.

Collapses `shot_zone_basic` to a canonical zone via `ZONE_COLLAPSE`
(the two corner-3 zones merge) and aggregates realized vs expected points
per shot in each zone.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `shot_zone_basic`, `shot_made_flag`, `actual_points`, `xpoints`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(player_id, zone)`: `player_id:Int64, zone:Utf8, att:Int64, makes:Int64, pts:Float64, pps:Float64, xpps:Float64, pps_above_expected:Float64` (`pps` = points per shot, `xpps` = expected). Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, zone_value_map
zmap = zone_value_map(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

zmap.filter(pl.col("zone") == "corner_3").sort("pps_above_expected", descending=True)
```
