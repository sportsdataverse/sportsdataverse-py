---
title: NBA — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nba`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_nba_player_stats}

Pull an NBA athlete's ESPN **season** stat line as one wide row.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns, the athlete / team metadata blocks, and the `season_type` /
`total` parameters. For the richer multi-category web-v3 payload use
`sportsdataverse.nba.espn_nba_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NBA athlete identifier (e.g. `1966` for LeBron James). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
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
| `college_name` | character | College / pre-draft team. |
| `status_id` | integer | Status identifier. |
| `status_name` | character | Status label. |
| `defensive_blocks` | double | Short for blocked shot, number of times when a defensive player legally deflects a field goal attempt from an offensive player. |
| `defensive_defensive_rebounds` | double | The number of times when the defense obtains the possession of the ball after a missed shot by the offense. |
| `defensive_steals` | double | The number of times a defensive player forced a turnover by intercepting or deflecting a pass or a dribble of an offensive player. |
| `defensive_def_rebound_rate` | double | The percentage of missed shots that a team rebounds defensively. Rebound Rate = (Defensive Rebounds x Team Minutes) divided by (Player Minutes x (Team Defensive Rebounds + Opponent Defensive Rebounds)). |
| `defensive_avg_defensive_rebounds` | double | The average defensive rebounds per game. |
| `defensive_avg_blocks` | double | The average blocks per game. |
| `defensive_avg_steals` | double | The average steals per game. |
| `defensive_avg48_defensive_rebounds` | double | Player's average defensive rebounds per 48 minutes played. |
| `defensive_avg48_blocks` | double | Player's average blocked shots per 48 minutes played. |
| `defensive_avg48_steals` | double | Player's average steals per 48 minutes played. |
| `defensive_drpm` | double | Defensive Real Plus-Minus. |
| `general_disqualifications` | double | The number of times a player reached the foul limit. |
| `general_flagrant_fouls` | double | The number of fouls that the officials thought were unnecessary or excessive. |
| `general_fouls` | double | The number of times a player had illegal contact with the opponent. |
| `general_per` | double | A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_rebound_rate` | double | The percentage of missed shots that a team rebounds. Rebound Rate = (Rebounds x Team Minutes) divided by (Player Minutes x (Team Rebounds + Opponent Rebounds)). |
| `general_ejections` | double | The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_technical_fouls` | double | The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_rebounds` | double | The total number of rebounds (offensive and defensive). |
| `general_vorp` | double | Value Over Replacement Player. |
| `general_warp` | double | Wins Above Replacement Player. |
| `general_rpm` | double | Real Plus-Minus. |
| `general_minutes` | double | The total number of minutes played. |
| `general_avg_minutes` | double | The average number of minutes per game. |
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
| `general_avg48_rebounds` | double | Player's average total rebounds (offensive + defensive) per 48 minutes played. |
| `general_avg48_fouls` | double | Player's average personal fouls committed per 48 minutes played. |
| `general_avg48_flagrant_fouls` | double | Player's average flagrant fouls assessed per 48 minutes played. |
| `general_avg48_technical_fouls` | double | Player's average technical fouls assessed per 48 minutes played. |
| `general_avg48_ejections` | double | Player's average ejections per 48 minutes played. |
| `general_avg48_disqualifications` | double | Player's average disqualifications (fouling out) per 48 minutes played. |
| `general_r40` | double | Rebounds Per 40 Minutes. |
| `general_games_played` | double | Games Played. |
| `general_games_started` | double | The number of games started by an athlete. |
| `general_double_double` | double | The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | double | The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `offensive_assists` | double | The number of times a player who passes the ball to a teammate in a way that leads to a score by field goal, meaning that he or she was "assisting" in the basket. There is some judgment involved in deciding whether a passer should be credited with an assist. |
| `offensive_effective_fg_pct` | double | Offensive effective field goals percentage (0-1 decimal). |
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
| `offensive_true_shooting_pct` | double | What a team's shooting percentage would be if we accounted for free throws and 3-pointers. True Shooting Percentage = (Total points x 50) divided by ((FGA + (FTA x 0.44)). |
| `offensive_total_turnovers` | double | The number of turnovers plus team turnovers for the team. |
| `offensive_assist_ratio` | double | The percentage of a team's possessions that ends in an assist. Assist Ratio = (Assists x 100) divided by ((FGA + (FTA x 0.44) + Assists + Turnovers). |
| `offensive_points_in_paint` | double | The amount of points scored in the area known as "the Paint"(the rectangle between the foul line and the baseline). |
| `offensive_off_rebound_rate` | double | The percentage of missed shots that a team rebounds offensively. Offensive Rebound Rate = (Offensive Rebounds x Team Minutes) divided by (Player Minutes x (Team Offensive Rebounds + Opponent Defensive Rebounds)). |
| `offensive_turnover_ratio` | double | The percentage of a team's possessions that end in a turnover. Turnover Ratio = (Turnover x 100) divided by ((FGA + (FTA x 0.44) + Assists + Turnovers). |
| `offensive_brick_index` | double | How many points a player costs his team with his shooting compared with the league average on a per-40-minute basis. ((52.8 - TS%) x (FGA + (FTA x 0.44))) / (Min/40) . |
| `offensive_usage_rate` | double | the number of possessions a player uses per 40 minutes. Usage Rate = ((FGA + (FT Att. x 0.44) + (Ast x 0.33) + TO) x 40 x League Pace) divided by (Minutes x Team Pace). |
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
| `offensive_avg48_field_goals_made` | double | Player's average field goals made per 48 minutes played. |
| `offensive_avg48_field_goals_attempted` | double | Player's average field goal attempts per 48 minutes played. |
| `offensive_avg48_three_point_field_goals_made` | double | Player's average three-point field goals made per 48 minutes played. |
| `offensive_avg48_three_point_field_goals_attempted` | double | Player's average three-point field goal attempts per 48 minutes played. |
| `offensive_avg48_free_throws_made` | double | Player's average free throws made per 48 minutes played. |
| `offensive_avg48_free_throws_attempted` | double | Player's average free throw attempts per 48 minutes played. |
| `offensive_avg48_points` | double | Player's average points scored per 48 minutes played. |
| `offensive_avg48_offensive_rebounds` | double | Player's average offensive rebounds per 48 minutes played. |
| `offensive_avg48_assists` | double | Player's average assists per 48 minutes played. |
| `offensive_avg48_turnovers` | double | Player's average turnovers committed per 48 minutes played. |
| `offensive_p40` | double | Points Per 40 Minutes. |
| `offensive_a40` | double | Assists Per 40 Minutes. |
| `offensive_orpm` | double | Offensive Real Plus-Minus. |
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
from sportsdataverse.nba import espn_nba_player_stats
df = espn_nba_player_stats(athlete_id=1966, season=2023)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### `espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nba_schedule}

espn_nba_schedule - look up the NBA schedule for a given date from ESPN

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `season_type` | `int` | `None` | season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | ESPN UID string. |
| `date` | character | Date in YYYY-MM-DD format. |
| `attendance` | integer | Reported attendance. |
| `time_valid` | logical | Time valid. |
| `neutral_site` | logical | Neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Recent. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `broadcast` | character | Broadcast information string. |
| `highlights` | integer | Game highlight urls. |
| `notes_type` | character | Notes type. |
| `notes_headline` | character | Notes headline. |
| `broadcast_market` | character | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | character | Broadcast name. |
| `type_id` | character | Type identifier (numeric). |
| `type_abbreviation` | character | Type abbreviation. |
| `venue_id` | character | Unique venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_indoor` | logical | TRUE if the venue is indoors. |
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
| `home_linescores` | list | Period-by-period point totals for the home team, stored as a list of integer scores. |
| `home_records` | character | Win-loss record strings for the home team across relevant splits (e.g., overall, home/away, conference). |
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
| `away_linescores` | list | Period-by-period point totals for the away team, stored as a list of integer scores. |
| `away_records` | character | Win-loss record strings for the away team across relevant splits (e.g., overall, home/away, conference). |
| `game_id` | integer | Unique game identifier. |
| `season` | integer | Season year. |
| `season_type` | integer | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

**Example**

```python
from sportsdataverse.nba import espn_nba_schedule
slate = espn_nba_schedule()
print(slate.shape)

# Pull a specific date

jan2 = espn_nba_schedule(dates=20230102, season_type=2)

# Pipeline next step (extract finals only)

import polars as pl
finals = espn_nba_schedule(dates=20230102).filter(
    pl.col("status_type_completed") == True
)
```

## Dataset loaders

### `load_darko_dpm(path: 'str') -> 'pl.DataFrame'` {#load_darko_dpm}

Parse a DARKO DPM leaderboard CSV (e.g. `2026-darko-dpm-leaderboard.csv`).

Name-keyed only (no shared player id with the model zoo) -- this is the
family `~sportsdataverse.nba.nba_model_validation.external_validity`
joins with `join="name"`. Handles two real-file quirks: a leading UTF-8
BOM (read with `encoding="utf-8-sig"`, which strips it) and
sign-prefixed integer columns (`"+7"`, not `"7"`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a DARKO DPM leaderboard CSV. |

**Returns**

Frame with schema `DARKO_DPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_darko_dpm
oracle = load_darko_dpm(f"{oracle_dir}/2026-darko-dpm-leaderboard.csv")
print(oracle.sort("dpm", descending=True).head())
```

### `load_dunks_threes_stats(path: 'str') -> 'pl.DataFrame'` {#load_dunks_threes_stats}

Parse a Dunks & Threes counting-stats CSV (e.g. `2025_Dunks_&_Threes_Stats.csv`).

Only `ewins` (estimated wins) is kept -- the WAR-layer oracle target
the spec pairs with LEBRON's `WAR` column. WP4's `nba_war` doesn't
exist yet, so this loader is built and tested standalone (see the
plan's "WP4/WP2 dependency notes").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T counting-stats CSV. |

**Returns**

Frame with schema `DT_STATS_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_dunks_threes_stats
oracle = load_dunks_threes_stats(f"{oracle_dir}/2025_Dunks_&_Threes_Stats.csv")
```

### `load_epm(path: 'str') -> 'pl.DataFrame'` {#load_epm}

Parse a Dunks & Threes EPM CSV (`{season}_EPM_data.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T EPM CSV. |

**Returns**

Frame with schema `EPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_epm
oracle = load_epm(f"{oracle_dir}/2025_EPM_data.csv")
```

### `load_lebron_daily(path: 'str') -> 'pl.DataFrame'` {#load_lebron_daily}

Parse a LEBRON daily-snapshot CSV (e.g. `lebron_daily_2026-07-02.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON daily-snapshot CSV. |

**Returns**

Frame with schema `LEBRON_DAILY_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import glob
from sportsdataverse.nba.nba_oracle_data import load_lebron_daily
latest = sorted(glob.glob(f"{oracle_dir}/lebron_daily_*.csv"))[-1]
oracle = load_lebron_daily(latest)
```

### `load_lebron_season(path: 'str') -> 'pl.DataFrame'` {#load_lebron_season}

Parse a LEBRON season-file CSV (e.g. `lebron-data-2026.csv`).

`seasons` is passed through as a raw string -- per-season files carry a
single year (`"2026"`); the combined all-years file carries a
multi-year window (`"2010-2013"`). Both parse with this one function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON season CSV. |

**Returns**

Frame with schema `LEBRON_SEASON_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_lebron_season
oracle = load_lebron_season(f"{oracle_dir}/lebron-data-2026.csv")
```

### `load_rapm_ryan_davis(path: 'str') -> 'pl.DataFrame'` {#load_rapm_ryan_davis}

Parse a Ryan Davis published RAPM CSV (single-season or multi-year window).

Serves BOTH real files -- `rapm_ryan_davis.csv` (`season` like
`"2009-10"`) and `rapm_multi_ryan_davis.csv` (`season` like
`"2011-16"`, a multi-year decay window) -- since they share an
identical header. Only the combined (not per-side Off`/Def`)
rating columns are kept, matching the model zoo's combined-rating
convention (`nba_rapm`'s `rapm` column, not separate offense/defense).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a Ryan Davis RAPM CSV. |

**Returns**

Frame with schema `RAPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis
oracle = load_rapm_ryan_davis(f"{oracle_dir}/rapm_ryan_davis.csv")
season = oracle.filter(pl.col("season") == "2022-23")
```

## Utilities & helpers

### `most_recent_nba_season()` {#most_recent_nba_season}

Return the most recent NBA season year based on today's date.

The NBA season crosses calendar years -- a season started in October of
year Y is reported as season Y+1. If today is in October or later, this
returns next calendar year; otherwise it returns the current calendar year.

**Returns**

The most recent NBA season year (e.g. 2024 for the 2023-24 season).

**Example**

```python
from sportsdataverse.nba import most_recent_nba_season
year = most_recent_nba_season()
print(year)

# Combine with the loaders for a "current season" pull

from sportsdataverse.nba import load_nba_schedule, most_recent_nba_season
sched = load_nba_schedule(seasons=[most_recent_nba_season()])
```

### `year_to_season(year)` {#year_to_season}

Convert a season START year (e.g. 2023) to the NBA's hyphenated label

(e.g. `"2023-24"`).

Callers working in the end-year convention pass `end_year - 1` (e.g.
`year_to_season(most_recent_nba_season() - 1)`).

Handles century rollover (1999 -> `"1999-00"`) and zero-pads the
second half of the label.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `int` |  | The starting calendar year of the season (e.g. 2023 for the 2023-24 season). |

**Returns**

NBA-style season label.

**Example**

```python
from sportsdataverse.nba import year_to_season
label = year_to_season(2023)
print(label)  # "2023-24"

# Century rollover

print(year_to_season(1999))  # "1999-00"
```

## Other

### `AdjRapmModel(prior: 'Dict[int, Tuple[float, float]]', alphas: 'np.ndarray' = <factory>, n_samples: 'int' = 200, seed: 'int' = 0) -> None` {#AdjRapmModel}

Prior-informed RAPM: ridge toward a per-player box prior with an RTO posterior.

Implements the `~sportsdataverse.nba.nba_model_validation.PriorModel`
protocol so the validation harness routes through `fit_with_prior` and the
resulting `~sportsdataverse.nba.nba_model_validation.FitResult` carries
a posterior — enabling Oracle ④ (interval calibration).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `prior` | `Dict[int, Tuple[float, float]]` |  | Per-player `{player_id: (o_prior, d_prior)}` in per-100 units. |
| `alphas` | `ndarray` | `<factory>` | RidgeCV alpha grid forwarded to fit_prior_ridge`. |
| `n_samples` | `int` | `200` | Number of RTO posterior samples. |
| `seed` | `int` | `0` | RNG seed for the RTO sampler. |

**Example**

```python
from sportsdataverse.nba import AdjRapmModel, nba_spm
from sportsdataverse.nba.nba_model_validation import validate_model
prior = AdjRapmModel.from_spm(nba_spm(box_feats, coef))
report = validate_model(prior, season_frames, model_name="adj_rapm")
print(report.calibration.coverage)      # non-None: the prior model has a posterior
```

**Methods**

#### `AdjRapmModel.fit_with_prior(X: 'csr_matrix', y: 'np.ndarray', prior_mean: 'np.ndarray') -> 'FitResult'`

Delegate to fit_prior_ridge` using this model's hyperparameters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `csr_matrix` |  | Sparse design `(n, 2P)` from `~sportsdataverse.nba.nba_rapm.build_rapm_design`. |
| `y` | `ndarray` |  | Possession points `(n,)`. |
| `prior_mean` | `ndarray` |  | Per-possession prior mean `(2P,)` built by the harness. |

**Returns**

`~sportsdataverse.nba.nba_model_validation.FitResult` with posterior of shape `(n_samples, 2P)`.

### `AgingCurve(delta_by_age: 'Dict[int, float]' = <factory>) -> None` {#AgingCurve}

Empirical aging deltas: `delta_by_age[a]` = expected rating change aging a -> a+1.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `delta_by_age` | `Dict[int, float]` | `<factory>` |  |

**Methods**

#### `AgingCurve.delta(age: 'float') -> 'float'`

Aging drift for a player of (rounded) `age`; 0.0 outside the fitted range.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `age` | `float` |  |  |

### `ExternalValidityResult(corr: 'float', n_matched: 'int', coverage_pct: 'float', permutation_p95: 'float', join: 'str') -> None` {#ExternalValidityResult}

Concurrent-validity correlation of model ratings against a published oracle metric.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `corr` | `float` |  | Pearson correlation of the model's rating column against the oracle's value column, over matched rows. `nan` when fewer than 3 rows matched. |
| `n_matched` | `int` |  | Number of rows successfully joined (ratings <-> oracle). |
| `coverage_pct` | `float` |  | `100 * n_matched / len(ratings)` -- how much of the model's player population the oracle covers. `0.0` when `ratings` is empty. |
| `permutation_p95` | `float` |  | 95th percentile of `\|corr\|` over `n_permutations` random shuffles of the oracle's matched value column -- a self-computed null-correlation ceiling. The spec deliberately avoids a hardcoded floor constant; compare `corr` against this instead. `nan` when fewer than 3 rows matched. |
| `join` | `str` |  | The join strategy used (`"id"` or `"name"`). |

### `ForecastResult(forecast_rmse: 'float', forecast_corr: 'float', baseline_rmse: 'float', n_forecasts: 'int') -> None` {#ForecastResult}

Forecast-accuracy metrics: predicted-vs-actual next-season rating over held-out transitions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `forecast_rmse` | `float` |  |  |
| `forecast_corr` | `float` |  |  |
| `baseline_rmse` | `float` |  |  |
| `n_forecasts` | `int` |  |  |

### `LeagueConstants(hfa: 'float', margin_sd: 'float', avg_pace: 'float', avg_off_rtg: 'float', game_minutes: 'int', in_game_wp_artifact: 'str' = 'nba_in_game_wp.ubj') -> None` {#LeagueConstants}

Per-`league_id` fitted constants for the NBA prediction & market stack.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hfa` | `float` |  | Home-court advantage in points (fitted on the as-of-date backtest for the league; see `dev/nba_prediction/fit_pregame.py`). |
| `margin_sd` | `float` |  | Std. dev. of the game-margin residual (fitted jointly with `hfa`; the Brier-minimizing sigma agrees to within a documented tolerance). |
| `avg_pace` | `float` |  | League baseline possessions per team per game (adjusted- pace anchor for `~sportsdataverse.nba.nba_team_ratings.adjust_pace`). |
| `avg_off_rtg` | `float` |  | League baseline points per 100 possessions. |
| `game_minutes` | `int` |  | Regulation game length in minutes (NBA/G-League 48, WNBA 40) -- structurally different, not a fitted number. |
| `in_game_wp_artifact` | `str` | `'nba_in_game_wp.ubj'` | Filename of the bundled in-game-WP coefficients under `sportsdataverse/nba/models` (committed in Phase 3). |

### `MeasureSpec(measure: 'str', actual: 'str', denom: 'str', out_prefix: 'str', extra_denoms: 'dict[str, tuple[str, str]]' = <factory>) -> None` {#MeasureSpec}

Per-model column map for a `leaguedashptstats` measure.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `measure` | `str` |  | `pt_measure_type` sent to `nba_stats_leaguedashptstats`. |
| `actual` | `str` |  | Realized-outcome column (snake_case). |
| `denom` | `str` |  | Opportunity column (snake_case). |
| `out_prefix` | `str` |  | Output-column prefix, e.g. `"reb"` -> `reb_oe`. |
| `extra_denoms` | `dict[str, tuple[str, str]]` | `<factory>` | Difficulty buckets: `label -> (actual_col, denom_col)`. |

### `NbaBpmModel(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', positions: 'pl.DataFrame', *, team_adjust: 'bool' = True) -> 'None'` {#NbaBpmModel}

A `RatingsModel` scoring a fold via faithful BPM 2.0.

Scores a fold via faithful BPM 2.0; position/role are estimated **fold-native**
(recomputed over the fold's games) in v1 — a full-season-position refinement is a
documented follow-up. `fit_ratings` restricts the box rate + team margin to the
fold's games (the leakage guard).

Design note: position/role are recomputed inside `nba_bpm` over the fold in v1 for
simplicity (fold-native); the spec's "position over full season" refinement is a
documented follow-up if faithfulness testing shows fold-position drift matters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines (same schema as `nba_bpm`). |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines including `plus_minus`. |
| `positions` | `DataFrame` |  | Listed positions (`player_id`, `position_num`) from `nba_player_positions`. |
| `team_adjust` | `bool` | `True` | Apply the team adjustment (`True`) or return raw box-BPM (`False`). |

**Example**

```python
from sportsdataverse.nba import NbaBpmModel
from sportsdataverse.nba.nba_model_validation import validate_model
model = NbaBpmModel(logs["player"], logs["team"], positions)
report = validate_model(model, season_frames, model_name="bpm")
```

**Methods**

#### `NbaBpmModel.fit_ratings(possessions: 'pl.DataFrame') -> 'RatingsFit'`

Score the fold's players via BPM 2.0, restricted to the fold's game_ids.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | The fold's possession+lineup frame. Only the `game_id` values it contains are used to filter `player_logs` and `team_logs` (the leakage guard). |

**Returns**

`RatingsFit` with `o_ratings` (OBPM) and `d_ratings` (DBPM) keyed by player_id. Returns empty dicts when no box data covers the fold's games.

### `NbaSpmModel(coefficients: 'SpmCoefficients', player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame') -> 'None'` {#NbaSpmModel}

A `RatingsModel` that scores a fold via fitted SPM coefficients.

Restricts its box aggregation to the fold's `game_id` (the leakage guard),
then applies the (globally pre-fit) SPM coefficients.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `coefficients` | `SpmCoefficients` |  | A `SpmCoefficients` instance from `train_spm`. |
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines used to build fold features. |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines used to estimate per-game possessions. |

**Example**

```python
from sportsdataverse.nba import NbaSpmModel, train_spm
from sportsdataverse.nba.nba_model_validation import validate_model
model = NbaSpmModel(coef, logs["player"], logs["team"])
report = validate_model(model, season_frames, model_name="spm")
```

**Methods**

#### `NbaSpmModel.fit_ratings(possessions: 'pl.DataFrame') -> 'RatingsFit'`

Aggregate the fold's box (restricted to its game_ids) and apply SPM coeffs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | The fold's possession+lineup frame. Only the `game_id` values it contains are used to filter `player_logs` and `team_logs` (the leakage guard). |

**Returns**

`RatingsFit` with `o_ratings` and `d_ratings` dicts mapping player_id to per-100 OSPM/DSPM. Returns empty dicts when no box features can be built from the fold's games.

### `RidgeRapmModel(alphas: 'np.ndarray' = array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])) -> 'None'` {#RidgeRapmModel}

Reference model: the merged plain-RAPM RidgeCV fit, adapted to `RapmModel`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `alphas` | `ndarray` | `array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])` | Ridge penalty grid for cross-validation. Defaults to the merged `DEFAULT_RAPM_ALPHAS`. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_rapm import build_rapm_design
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel

rows = {
    "off_player_1": [1, 6], "off_player_2": [2, 7],
    "off_player_3": [3, 8], "off_player_4": [4, 9],
    "off_player_5": [5, 10],
    "def_player_1": [6, 1], "def_player_2": [7, 2],
    "def_player_3": [8, 3], "def_player_4": [9, 4],
    "def_player_5": [10, 5],
    "points": [2, 0],
}
poss = pl.DataFrame(rows)
X, y, pids = build_rapm_design(poss)
fit = RidgeRapmModel().fit(X, y)
print(fit.coef.shape)    # (20,) — 10 players × 2 sides
print(fit.posterior)     # None — point estimator
```

**Methods**

#### `RidgeRapmModel.fit(X: 'csr_matrix', y: 'np.ndarray') -> 'FitResult'`

Fit RidgeCV and return coefficients + intercept (no posterior).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `csr_matrix` |  | Sparse design matrix of shape `(n_possessions, 2P)`. |
| `y` | `ndarray` |  | Target points per possession, shape `(n_possessions,)`. |

**Returns**

FitResult with `coef` shape `(2P,)`, scalar `intercept`, and `posterior=None`.

### `SpmCoefficients(o_coef: 'np.ndarray', d_coef: 'np.ndarray', o_intercept: 'float', d_intercept: 'float', feature_names: 'List[str]') -> None` {#SpmCoefficients}

Fitted SPM coefficients (box features -> offense/defense RAPM, per-100).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `o_coef` | `ndarray` |  | Coefficient vector for the offense regression (shape `[n_features]`). |
| `d_coef` | `ndarray` |  | Coefficient vector for the defense regression (shape `[n_features]`). |
| `o_intercept` | `float` |  | Intercept for the offense regression. |
| `d_intercept` | `float` |  | Intercept for the defense regression. |
| `feature_names` | `List[str]` |  | Ordered list of feature column names corresponding to the coefficient vectors. |

### `ValidationReport(model_name: 'str', n_seasons: 'int', retrodiction: 'Optional[RetrodictionResult]' = None, reliability: 'Optional[ReliabilityResult]' = None, cross_season: 'Optional[CrossSeasonResult]' = None, calibration: 'Optional[CalibrationResult]' = None, external: 'Optional[ExternalValidityResult]' = None, walk_forward: 'Optional[WalkForwardResult]' = None) -> None` {#ValidationReport}

Holds all oracle results for a single model evaluation run.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_name` | `str` |  | Human-readable label for the model being evaluated. |
| `n_seasons` | `int` |  | Number of season frames supplied to `validate_model`. |
| `retrodiction` | `Optional[RetrodictionResult]` | `None` | Result from Oracle 1, or `None` if not selected. |
| `reliability` | `Optional[ReliabilityResult]` | `None` | Result from Oracle 2, or `None` if not selected. |
| `cross_season` | `Optional[CrossSeasonResult]` | `None` | Result from Oracle 3, or `None` if not selected. |
| `calibration` | `Optional[CalibrationResult]` | `None` | Result from Oracle 4, or `None` if not selected or the model is a point estimator. |
| `external` | `Optional[ExternalValidityResult]` | `None` | Result from Oracle 5 (`external_validity`), or `None` if not selected. |
| `walk_forward` | `Optional[WalkForwardResult]` | `None` | Result from Oracle 6 (`walk_forward`), or `None` if not selected. |

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model,
)

# season_frames is a list[pl.DataFrame] of possession stints per season
rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
print(rep.model_name)                        # "plain_rapm"
print(rep.n_seasons)                         # len(season_frames)
print(rep.retrodiction.game_margin_rmse)     # float
print(rep.reliability.spearman_brown)        # float
print(rep.calibration)                       # None — point estimator
```

### `WalkForwardResult(game_margin_rmse: 'float', game_margin_corr: 'float', carry_forward_rmse: 'float', random_fold_rmse: 'float', n_checkpoints: 'int', n_test_games: 'int') -> None` {#WalkForwardResult}

Oracle 6: walk-forward ("predict tomorrow") retrodiction over a season timeline.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_margin_rmse` | `float` |  | RMSE of predicted vs actual per-(game, team) margins, pooled across all checkpoints -- the model refit through each checkpoint date, predicting the following `horizon_days` window. |
| `game_margin_corr` | `float` |  | Pearson correlation of the same pooled predictions. |
| `carry_forward_rmse` | `float` |  | RMSE using the PRIOR checkpoint's fit (no refit) applied to the current window -- isolates whether refitting through each checkpoint actually helps. `nan` when fewer than 2 checkpoints produce a valid window. |
| `random_fold_rmse` | `float` |  | Oracle 1's (`retrodiction`) pooled game-margin RMSE on the same possessions -- the non-time-ordered baseline. |
| `n_checkpoints` | `int` |  | Number of checkpoint dates that produced a non-degenerate (train, test) split. |
| `n_test_games` | `int` |  | Total distinct game_ids evaluated across all checkpoints. |

### `add_ctg_shot_zones(enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#add_ctg_shot_zones}

Append CTG's shot-location zone (`ctg_shot_zone`) to an enhanced PBP frame.

CTG's zones differ from the official NBA zones emitted by
`~sportsdataverse.nba.nba_shot_zones.add_shot_zones`: CTG splits the
midrange at the free-throw-line distance rather than at the paint boundary.

* `at_rim` — shot distance < 4 ft ("Shots within 4 feet of the basket").
* `short_mid` — 4 ft <= distance < 14 ft ("outside of 4 feet, but inside of
  ~14 feet (the free throw line distance)").
* `long_mid` — >= 14 ft, inside the arc.
* `corner_3` — a three "below the break" (`|x_legacy| >= 220` and
  `y_legacy <= 87.5`).
* `arc_3` — any other three (CTG's "non-corner three").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. |

**Returns**

The input frame with a `ctg_shot_zone` Utf8 column appended (null on non-field-goal rows). Empty input returns a zero-row frame carrying the column — never raises.

**Example**

```python
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_ctg_shot_zones
pbp = add_ctg_shot_zones(enhanced_pbp_from_payload(payload))
print(pbp.filter(pl.col("ctg_shot_zone").is_not_null())["ctg_shot_zone"].value_counts())
```

### `add_play_context(enhanced_pbp: 'pl.DataFrame', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', starters_on_court: 'Optional[dict[int, int]]' = None) -> 'pl.DataFrame'` {#add_play_context}

Build possessions and enrich them with the full CTG play-context surface.

One call: `~sportsdataverse.nba.nba_possessions.build_possessions` ->
`add_start_type_detail` -> `add_transition` ->
`flag_heave_possessions` -> `flag_garbage_time`.

The CTG filter columns are **flags, not filters** — nothing is dropped. Apply

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `starters_on_court` | `Optional[dict[int, int]]` | `None` | Optional starters-on-floor counts; see `flag_garbage_time`. |

**Returns**

The possession frame (`POSSESSIONS_SCHEMA`) plus every column in `PLAY_CONTEXT_POSSESSIONS_SCHEMA`.

**Example**

```python
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_play_context
poss = add_play_context(enhanced_pbp_from_payload(payload))
print(poss["possession_start_type_ctg"].value_counts())
```

### `add_start_type_detail(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#add_start_type_detail}

Append the full pbpstats start-type taxonomy to a possession frame.

Upgrades the engine's coarse 5-value `possession_start_type` into:

* `possession_start_type_detail` — the zone-split pbpstats vocabulary:
  `Off{AtRim|ShortMidRange|LongMidRange|Corner3|Arc3}{Make|Miss|Block}`,
  `OffFTMake` / `OffFTMiss`, `OffLiveBallTurnover`, `OffTimeout`,
  `OffDeadball`.
* `possession_start_type_ctg` — the coarse bucket CTG reports on:
  `off_made` / `off_live_rebound` / `off_steal` / `off_deadball` /
  `off_timeout` (see `~nba_play_context_constants.CTG_START_BUCKETS`).

Precedence (pbpstats): period start > timeout > previous boundary event. A
**team rebound** (`person_id == 0`) is a dead-ball start even though a
rebound row exists; a **timeout** beats a made basket (an after-timeout
possession is `OffTimeout`, not `OffMadeShot`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_possessions.build_possessions`. |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame those possessions were built from. |

**Returns**

`possessions` with the two columns appended. Empty input returns a zero-row frame carrying them — never raises.

**Example**

```python
from sportsdataverse.nba.nba_possessions import build_possessions
from sportsdataverse.nba.nba_play_context import add_start_type_detail
poss = add_start_type_detail(build_possessions(pbp), pbp)
print(poss["possession_start_type_ctg"].value_counts())
```

### `add_transition(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, transition_seconds: 'float' = 6.0, variant: 'str' = 'hoop_math') -> 'pl.DataFrame'` {#add_transition}

Flag possessions that started in transition, and time their initial play.

CTG defines transition as beginning at the possession start and ending "once
the defense is set", **without publishing a seconds threshold**. We therefore
time the possession's *initial play* — its first shot attempt, trip to the
line, or turnover (CTG's own definition of a "play") — and call the
possession transition when that play lands within `transition_seconds`.

Variants (`~nba_play_context_constants.TRANSITION_VARIANTS`):

* `hoop_math` (default) — any non-timeout start type qualifies.
* `haslametrics` — steal starts only (conservative).
* `bigballr` — the previous possession must have ended live
  (`off_made` / `off_live_rebound` / `off_steal`); a dead-ball start can
  never be transition.

The first possession of a period is never transition. After a timeout the
defense is set by construction, so `off_timeout` never qualifies under any
variant.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame carrying `possession_start_type_ctg` (i.e. the output of `add_start_type_detail`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `transition_seconds` | `float` | `6.0` | Initial-play cutoff. Default 10.0 (hoop-math). Calibrate against Synergy transition frequency (league mean ~15-16%). |
| `variant` | `str` | `'hoop_math'` | One of `~nba_play_context_constants.TRANSITION_VARIANTS`. |

**Returns**

`possessions` with `seconds_to_first_play` (Float64, null when the possession had no play), `is_transition` (Boolean), `transition_source` (Utf8: `steal` / `live_rebound` / `made` / `deadball`; null when not transition) and `possession_context` (Utf8: `transition` / `halfcourt` / `misc`) appended.

**Example**

```python
poss = add_transition(add_start_type_detail(poss, pbp), pbp)
print(poss["is_transition"].mean())          # transition frequency

# Tune the knob against the Synergy oracle

poss8 = add_transition(poss, pbp, transition_seconds=8.0)
```

### `adjust_efficiency(game_eff: 'pl.DataFrame', *, league_id: 'str' = '00', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'` {#adjust_efficiency}

Iterative opponent-adjusted rating -> AdjOffRtg / AdjDefRtg / AdjNet per team-season.

KenPom-style fixed point: initialize `adj_off = raw_off` /
`adj_def = raw_def`, then repeatedly recompute each team's rating from
its games with the opponent's *current* adjusted rating and a
home-court adjustment removed, until the largest change is below `tol`.
Ratings are computed independently per season.

The per-game offensive update is
`off_rtg - (adj_def_opp - avg) - loc_o` where `loc_o` is `+hfa/2` at
home, `-hfa/2` away, `0` neutral (defense is symmetric with the
opposite sign); `avg` is the league baseline off rating and `hfa`
comes from `~sportsdataverse.nba.nba_prediction_constants.get_constants`.

*(T7.2-shared algorithm)* -- identical fixed point to the MBB
`mbb_team_ratings.adjust_efficiency` / CFB ratings cores.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League -- selects the HFA + baseline off-rating constants. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest rating change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, raw_off_rtg, raw_def_rtg, games`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import adjust_efficiency, raw_game_efficiency
ratings = adjust_efficiency(raw_game_efficiency(sched, box))
```

### `adjust_pace(game_eff: 'pl.DataFrame', *, league_id: 'str' = '00', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'` {#adjust_pace}

Opponent-adjusted pace (possessions/game) per team-season.

Same fixed point as `adjust_efficiency`, applied to game
possessions under the additive model `poss = pace_i + pace_j - avg`: a
team's pace is recovered by removing its opponents' current adjusted
pace. `avg` is the league baseline pace from
`~sportsdataverse.nba.nba_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league_id` | `str` | `'00'` | `"00"` / `"10"` / `"20"` -- selects the pace baseline. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest pace change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_pace, raw_pace`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import adjust_pace, raw_game_efficiency
pace = adjust_pace(raw_game_efficiency(sched, box))
```

### `as_of_ratings_split(results: 'pl.DataFrame', cutoff_date: 'datetime.date') -> 'pl.DataFrame'` {#as_of_ratings_split}

Filter a results frame to games strictly before a cutoff date (leakage boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | A `polars.DataFrame` with a `date` column. |
| `cutoff_date` | `date` |  | Games on or after this date are excluded. |

**Returns**

A `polars.DataFrame` containing only rows with `date < cutoff_date`.

**Example**

```python
import datetime as dt
from sportsdataverse._common.metrics import as_of_ratings_split
as_of_ratings_split(results, dt.date(2023, 9, 8))
```

### `box_features(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', *, game_ids: 'Optional[List[str]]' = None) -> 'pl.DataFrame'` {#box_features}

Aggregate per-player per-100-possession box features over a set of games.

Restricting `game_ids` to a fold's games is the harness leakage guard.

Per-100 possessions are computed per game (so mid-window trades use each
game's own team pace), then summed — the result is fully deterministic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines (`game_id`, `team_id`, `player_id`, `min`, and the counting stats in STATS`). |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines (`game_id`, `team_id`, `min`, `fga`, `oreb`, `tov`, `fta`) for the possession estimate. |
| `game_ids` | `Optional[List[str]]` | `None` | Optional subset of `game_id` to include (default: all). |

**Returns**

One row per player: `player_id`, the STATS` per-100 rates, `min` (total), `gp` (games). Empty frame with that schema on empty input.

### `build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'` {#build_athlete_identity_lookup}

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### `build_nba_player_identity_lookup(player_box: 'pl.DataFrame') -> 'dict[str, dict[str, Any]]'` {#build_nba_player_identity_lookup}

R `build_identity_lookup(season)`: athlete_id -> identity from the

season's already-compiled `player_box` -- the authoritative "who played
in season Y" source (ESPN's team-roster endpoint is current-only and
cannot answer that for historical seasons).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | The season's compiled player_box frame (e.g. `nba/player_box/parquet/player_box_{season}.parquet`, or whatever the season builder just wrote for this pass). Must carry `athlete_id`; other identity columns are best-effort. |

**Returns**

athlete_id (str) -> identity fields for `helper_nba_player_season_stats`. When an athlete appears in multiple rows (multiple games), the LAST row (by frame order) wins -- mirroring R's `!duplicated(athlete_id, fromLast = TRUE)`, which keeps an athlete's most recent team within the season.

### `build_play_context_shots(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, putback_seconds: 'float' = 2.0) -> 'pl.DataFrame'` {#build_play_context_shots}

Build the per-shot frame carrying CTG's play context.

CTG assigns context **per play**, not per possession: one possession can
contain a transition miss, a halfcourt reset and a putback. This frame is the
play-level view — one row per field-goal attempt.

* `is_putback` — pbpstats `field_goal.py:112-144`: an **unassisted 2-point**
  attempt whose preceding event is a **real offensive rebound by the same
  player**, within `putback_seconds`. A three is never a putback.
* `is_second_chance_shot` — the shot follows an offensive rebound earlier in
  the same possession.
* `shot_context` — `transition` / `putback` / `halfcourt`. **Transition
  wins over putback**, reproducing CTG exactly: "if a team comes down in
  transition and misses a shot but gets a putback, that putback is classified
  as part of the overall transition event."

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_transition` (needs `is_transition`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `putback_seconds` | `float` | `2.0` | Rebound-to-shot window. Default 2.0 (pbpstats). |

**Returns**

Polars DataFrame with schema `PLAY_CONTEXT_SHOTS_SCHEMA` — one row per field-goal attempt. Empty input returns the zero-row schema.

**Example**

```python
shots = build_play_context_shots(poss, pbp)
print(shots.group_by("shot_context").len())
print(shots.filter(pl.col("is_putback") == True).height)
```

### `build_possession_shooting(enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#build_possession_shooting}

Build the per-shooter companion frame from an enhanced play-by-play DataFrame.

Companion to `build_possessions`: instead of one team-level row per
possession, emits one row per distinct shooter (`player_id`) per
possession, with their own `fg2a/fg2m/fg3a/fg3m/fta/ftm` counts. Shares
the same possession-group traversal as `build_possessions` via
assemble` — the two frames are always built from a single
consistent pass over the play-by-play. Consumed by WP2's luck-adjusted
shooting response.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Polars DataFrame with schema `ENHANCED_PBP_SCHEMA` (from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`). An empty or malformed frame returns a zero-row frame with `POSSESSION_SHOOTING_SCHEMA` — never raises. |

**Returns**

Polars DataFrame with schema `POSSESSION_SHOOTING_SCHEMA`. One row per `(possession_number, player_id)` pair. Events with `person_id == 0` are skipped (unattributable to a shooter — they still count toward `build_possessions`' team-level totals). Per-possession sums of the six shooting columns match the corresponding `build_possessions` columns exactly.

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possessions import build_possession_shooting

payload = json.loads(pathlib.Path("playbyplayv3.json").read_text())
pbp = enhanced_pbp_from_payload(payload)
sh = build_possession_shooting(pbp)
print(sh.shape, sh.schema["player_id"])

# Per-player shooting totals

import polars as pl
totals = sh.group_by("player_id").agg(
    pl.col("fg3m").sum(), pl.col("ftm").sum()
)
print(totals.head())
```

### `calibrate_pts_per_win(team_season: 'pl.DataFrame') -> 'float'` {#calibrate_pts_per_win}

Regress team wins on season point margin; return points-per-marginal-win.

Fits `wins ~ total_margin` via ordinary least squares over one (or more,
pooled) season's team-level rows and returns `1 / slope` — the amount of
full-season point differential associated with one additional win. This is
`nba_war`'s `pts_per_win` input.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_season` | `DataFrame` |  | One row per team-season with `team_id` (any dtype), `wins` (numeric), and `total_margin` (numeric — the team's full-season point differential: points scored minus points allowed across all its games, NOT a per-game average). |

**Returns**

`float` points of season margin per marginal win.

**Example**

```python
from sportsdataverse.nba.nba_war import calibrate_pts_per_win
pts_per_win = calibrate_pts_per_win(team_standings)  # team_id/wins/total_margin
print(pts_per_win)
```

### `calibrate_replacement_level(ratings: 'pl.DataFrame', poss: 'pl.DataFrame', *, pts_per_win: 'float', target_total_war: 'float', rating_col: 'str' = 'rating', poss_col: 'str' = 'poss') -> 'float'` {#calibrate_replacement_level}

Solve for the `replacement_level` that makes summed league WAR hit a target.

WAR is affine in `replacement_level`:
`war_i = (rating_i - replacement) * poss_i / 100 / pts_per_win`. Summed
over all players this is a single linear equation in `replacement_level`;
this function solves it in closed form (not an iterative search) for the
`replacement_level` that makes `sum(war_i) == target_total_war`.

`target_total_war` is a value the CALLER computes from real standings
(e.g. total league wins above a chosen replacement-team win percentage) —
this function does not assume or invent any such win-percentage convention.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Frame with `player_id` and `rating_col`. |
| `poss` | `DataFrame` |  | Frame with `player_id` and `poss_col` (total possessions played). |
| `pts_per_win` | `float` |  | Points of season margin per marginal win (`calibrate_pts_per_win`'s output). |
| `target_total_war` | `float` |  | The desired sum of every player's WAR. |
| `rating_col` | `str` | `'rating'` | Column in `ratings` holding the per-100-possession rating. |
| `poss_col` | `str` | `'poss'` | Column in `poss` holding total possessions played. |

**Returns**

`float` replacement_level solving the equation exactly.

**Example**

```python
from sportsdataverse.nba.nba_war import calibrate_replacement_level
repl = calibrate_replacement_level(
    ratings, poss, pts_per_win=250.0, target_total_war=300.0,
)
```

### `clutch_delta(clutch: 'pl.DataFrame', ratings: 'pl.DataFrame') -> 'pl.DataFrame'` {#clutch_delta}

Clutch net-rating delta vs a full-game baseline, per (season, team_id).

`clutch_delta = clutch_net_rating - adj_net_rtg`. Joins `clutch` to the
baseline `ratings` frame on `(season, team_id)` (asserting dtype
agreement first).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clutch` | `DataFrame` |  | Frame with `season, team_id, clutch_net_rating, clutch_poss`. |
| `ratings` | `DataFrame` |  | Full-game baseline with `season, team_id, adj_net_rtg` (the stats full-game net, or any per-team baseline). |

**Returns**

One row per matched (season, team_id): `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_poss`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_clutch import clutch_delta
d = clutch_delta(clutch_frame, baseline_frame)
```

### `compile_nba_season(season: 'int', season_type: 'str' = 'Regular Season', *, resume: 'bool' = True, cache_dir: 'Optional[str]' = None, delay_s: 'float' = 0.6, lineup_source: 'str' = 'auto', proxy_provider: 'Optional[Callable[[], Optional[str]]]' = None, raw_store_dir: 'RawStoreDir' = None, raw_store_readonly: 'Optional[bool]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#compile_nba_season}

Compile a full season's possession stint matrix (cached + resumable + throttled).

Discovers game ids, dedupes, then per game loads the cached parquet if present
(`resume`), else fetches via fetch_possessions`, caches it, and sleeps
`delay_s` (throttle; only on live fetches). A game that errors or returns no
possessions is logged and skipped (best-effort — a per-game failure never
raises; see `Raises` for the game_date integrity error). The assembled
frame is tagged with a `season` column.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season END year (e.g. 2024 for 2023-24). |
| `season_type` | `str` | `'Regular Season'` | `"Regular Season"` (default) or `"Playoffs"`. |
| `resume` | `bool` | `True` | Reuse per-game cached parquet when present. |
| `cache_dir` | `Optional[str]` | `None` | Cache root; defaults to `SDV_PY_NBA_CACHE_DIR` or `~/.sdv_py_nba_cache/possessions`. |
| `delay_s` | `float` | `0.6` | Seconds to sleep after each live fetch (rate-limit throttle). |
| `lineup_source` | `str` | `'auto'` | Which on-court lineup producer to use — `"auto"` (default; tries rotation then falls back to pbp), `"rotation"` (gamerotation endpoint only), or `"pbp"` (pbp-derived, no gamerotation fetch — useful when the gamerotation endpoint is throttled or unavailable). |
| `proxy_provider` | `Optional[Callable[[], Optional[str]]]` | `None` | Optional zero-arg callable returning a proxy URL (or `None`). **Called once for game discovery, then once per game** (`N + 1` calls for an `N`-game season), so a rotating pool spreads a season's fetches across many exit IPs rather than hammering `stats.nba.com` from one address. `stats.nba.com` rejects or hangs on datacenter/cloud IPs, so an unattended host (CI, a droplet) MUST supply one — a proxied request is judged on the proxy's exit IP, which is what makes such a host viable at all. Note discovery is proxied too: an unproxied index call returns no rows there, compiling the season to zero games without an error. Any `() -> str \| None` works; a round-robin pool's `.next` matches the signature directly:: compile_nba_season(2024, proxy_provider=round_robin.next) |
| `raw_store_dir` | `RawStoreDir` | `None` | Explicit raw JSON store root forwarded to every per-game fetch — a single path, or a per-endpoint mapping (`"*"` default key) so payload families can live in independent trees. `None` -> env vars (per-endpoint `SDV_PY_NBA_RAW_JSON_DIR_{ENDPOINT}`, then the generic `SDV_PY_NBA_RAW_JSON_DIR`); `""` force-disables. Same spirit as `cache_dir`'s arg-over-env precedence. |
| `raw_store_readonly` | `Optional[bool]` | `None` | If `True`, per-game fetches read the store but never persist misses (pure-consumer mode); `None` defers to `SDV_PY_NBA_RAW_JSON_READONLY`. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

The season possession frame (+ `season` and `game_date` cols). Empty typed frame if no games.

**Example**

```python
from sportsdataverse.nba.nba_season_compile import compile_nba_season

poss = compile_nba_season(2024)
print(poss.shape)          # (n_possessions, n_cols)
print(poss["season"][0])   # 2024

# Resume a partially completed run and return as pandas

poss_pd = compile_nba_season(2024, resume=True, return_as_pandas=True)
print(type(poss_pd))       # <class 'pandas.core.frame.DataFrame'>

# Compile Playoffs with a custom cache directory

poss = compile_nba_season(
    2024,
    season_type="Playoffs",
    cache_dir="/tmp/nba_cache",
)
```

### `darko_forecast_accuracy(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, aging_curve: "'AgingCurve | None'" = None, process_var: "'float | None'" = None, obs_base: "'float | None'" = None, min_history: 'int' = 1) -> 'ForecastResult'` {#darko_forecast_accuracy}

Holdout forecast accuracy: for each transition, forecast N+1 from history <= N vs actual.

For each player and each split at index `t` (prefix seasons `0..t` used to forecast
season `t+1`), run the Kalman filter on the prefix then forecast; the baseline is
carry-forward (`ratings[t]`).  Global `aging_curve` and `(q, obs_base)` are fit on
the full panel (low-dim parameters — standard practice; the holdout is on each player's
rating-history prefix).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id`, `season`, `rating` (+ optional `weight`) panel. |
| `ages` | `DataFrame` |  | `player_id`, `season`, `age`. |
| `aging_curve` | `AgingCurve \| None` | `None` | Fitted `AgingCurve`; fit from `panel` if None. |
| `process_var` | `float \| None` | `None` | Kalman process variance `q`; MLE-fit from `panel` if None. |
| `obs_base` | `float \| None` | `None` | Kalman base observation variance; MLE-fit from `panel` if None. |
| `min_history` | `int` | `1` | Minimum prefix length before a forecast is scored (default 1). |

**Returns**

`ForecastResult` with `forecast_rmse` / `forecast_corr` vs the actual next-season rating, `baseline_rmse` = carry-forward RMSE, and `n_forecasts` (total held-out transitions across all players).

**Example**

```python
from sportsdataverse.nba.nba_darko import darko_forecast_accuracy
res = darko_forecast_accuracy(rating_panel, ages_panel)
print(res.forecast_rmse, res.baseline_rmse, res.forecast_corr)

# Pass pre-fitted params to skip the global MLE step

from sportsdataverse.nba.nba_darko import fit_aging_curve, _fit_noise_params
curve = fit_aging_curve(panel, ages)
q, ob = _fit_noise_params(panel, ages, curve)
res = darko_forecast_accuracy(panel, ages, aging_curve=curve, process_var=q, obs_base=ob)
```

### `decay_weights(game_date: 'pl.Series', asof: 'Optional[datetime.date]', half_life_days: 'float') -> 'np.ndarray'` {#decay_weights}

Exponential time-decay sample weights `w = 0.5 ** (days_ago / half_life)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_date` | `Series` |  | Per-possession game dates (`pl.Date` Series), aligned row-for-row with the design the weights will be applied to. |
| `asof` | `Optional[date]` |  | Reference "today". `None` disables decay (all weights `1.0`). Games dated after `asof` are clamped to `days_ago = 0` (weight `1.0`); callers that want a strict as-of cutoff must filter first. |
| `half_life_days` | `float` |  | Days at which a possession's weight halves. Must be > 0. |

**Returns**

Float64 array of weights, one per row of `game_date`.

**Example**

```python
import datetime
import polars as pl
from sportsdataverse.nba.nba_rapm_variants import decay_weights

dates = pl.Series("game_date", [datetime.date(2023, 1, 1)])
w = decay_weights(dates, datetime.date(2023, 1, 31), half_life_days=30.0)
print(round(float(w[0]), 3))  # 0.5
```

### `espn_nba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nba_teams}

espn_nba_teams - look up NBA teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nba.espn_nba_teams.clear_cache().

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
from sportsdataverse.nba import espn_nba_teams
teams = espn_nba_teams()
print(teams.shape)

# Pandas round-trip

teams_pd = espn_nba_teams(return_as_pandas=True)
teams_pd.head()

# Pipeline next step (build a team_id to abbreviation map)

teams = espn_nba_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### `expected_possessions(home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'` {#expected_possessions}

Expected possessions for a matchup (Pythagorean-tempo blend).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_pace` | `float` |  | Home team's adjusted pace (possessions/game). |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League -- selects the league's baseline pace. |

**Returns**

Expected possessions for the matchup.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import expected_possessions
expected_possessions(100.0, 98.0)
```

### `external_validity(ratings: 'pl.DataFrame', oracle: 'pl.DataFrame', *, rating_col: 'str', oracle_col: 'str', join: 'str' = 'id', ratings_id_col: 'str' = 'player_id', oracle_id_col: 'str' = 'player_id', ratings_name_col: 'str' = 'player_name', oracle_name_col: 'str' = 'player_name', n_permutations: 'int' = 200, seed: 'int' = 0) -> 'ExternalValidityResult'` {#external_validity}

Oracle 5: correlate a model's ratings against a published external metric.

`join="id"` inner-joins on `ratings_id_col`/`oracle_id_col` (both
cast to Int64 defensively, per the project's join-key dtype discipline).
`join="name"` normalizes both name columns with
`~sportsdataverse.nba.nba_oracle_data.normalize_player_name` and
inner-joins on the normalized key -- the DARKO-family case: no shared id,
a brittle display-name join whose `coverage_pct` is the signal to watch.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | The model's own per-player ratings frame (from `nba_rapm`, `nba_adj_rapm`, `nba_spm`, `nba_darko`, etc.). |
| `oracle` | `DataFrame` |  | A tidy oracle frame from one of the `nba_oracle_data` loaders. |
| `rating_col` | `str` |  | Column in `ratings` holding the model's rating value. |
| `oracle_col` | `str` |  | Column in `oracle` holding the published metric value. |
| `join` | `str` | `'id'` | `"id"` (default) or `"name"`. |
| `ratings_id_col` | `str` | `'player_id'` | Player-id column name in `ratings` (`join="id"`). |
| `oracle_id_col` | `str` | `'player_id'` | Player-id column name in `oracle` (`join="id"`). |
| `ratings_name_col` | `str` | `'player_name'` | Player-name column name in `ratings` (`join="name"`). |
| `oracle_name_col` | `str` | `'player_name'` | Player-name column name in `oracle` (`join="name"`). |
| `n_permutations` | `int` | `200` | Number of random shuffles for the self-computed null ceiling. |
| `seed` | `int` | `0` | RNG seed for the permutation shuffle. |

**Returns**

`ExternalValidityResult`. `corr` and `permutation_p95` are `nan` when fewer than 3 rows matched; `coverage_pct` is `0.0` when `ratings` is empty.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis
from sportsdataverse.nba.nba_model_validation import external_validity

oracle = load_rapm_ryan_davis(f"{oracle_dir}/rapm_ryan_davis.csv").filter(
    pl.col("season") == "2022-23"
)
res = external_validity(ratings, oracle, rating_col="rapm", oracle_col="RAPM")
print(res.corr, res.coverage_pct)

# A name-keyed family (DARKO)

res = external_validity(
    ratings, darko_oracle, rating_col="projected_rating", oracle_col="dpm",
    join="name", ratings_name_col="player_name", oracle_name_col="player_name",
)
```

### `fit_aging_curve(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, smooth: 'int' = 3) -> 'AgingCurve'` {#fit_aging_curve}

Fit the aging curve by the delta method: avg YoY rating change grouped by starting age.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id`, `season`, `rating` (per-player-season ratings). |
| `ages` | `DataFrame` |  | `player_id`, `season`, `age`. |
| `smooth` | `int` | `3` | Odd window for a centered moving average over ages (1 = no smoothing). |

**Returns**

An `AgingCurve` mapping each integer starting age to its mean YoY delta.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_darko import fit_aging_curve

panel = pl.DataFrame({"player_id": [1, 1], "season": [2020, 2021], "rating": [10.0, 11.0]})
ages = pl.DataFrame({"player_id": [1, 1], "season": [2020, 2021], "age": [24.0, 25.0]})
curve = fit_aging_curve(panel, ages, smooth=1)
print(curve.delta(24))  # ~1.0
```

### `flag_garbage_time(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, starters_on_court: 'Optional[dict[int, int]]' = None) -> 'pl.DataFrame'` {#flag_garbage_time}

Flag CTG garbage time (excluded from CTG stats by default).

CTG (exact): "the game has to be in the **4th quarter**, the score
differential has to be **>= 25 for minutes 12-9, >= 20 for minutes 9-6, and
>= 10 for the remainder of the quarter**. Additionally, there have to be **two
or fewer starters on the floor combined between the two teams**. Importantly,
the game can never go back to being non-garbage time, or this clock resets."

The margin x minutes bands are reproduced exactly, evaluated on the score at
each possession's start. The reset semantics fall out of that per-possession
evaluation: if the trailing team claws back inside the band's threshold, the
condition stops holding and those possessions are NOT garbage time (CTG's own
"comeback is not counted as garbage time" example); if the lead re-expands,
the flag turns back on.

**The starters clause is applied only when `starters_on_court` is supplied**
— it needs lineup + box `START_POSITION` data this frame does not carry.
Without it the flag is the **margin-only superset** of CTG's definition (it can
flag a blowout stretch in which the starters are still on the floor), and
`garbage_time_basis` records which rule was actually used. This is a
deliberate, documented divergence — do not read a `margin_only` flag as
CTG-exact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame with `period`, `start_seconds_remaining` and `start_order_index`. |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame (supplies the running score). |
| `starters_on_court` | `Optional[dict[int, int]]` | `None` | Optional map `possession_number -> number of starters on the floor across BOTH teams`. When given, a possession is garbage time only if that count is <= `~nba_play_context_constants.GARBAGE_TIME_MAX_STARTERS`. |

**Returns**

`possessions` with Boolean `is_garbage_time` and Utf8 `garbage_time_basis` (`"margin_and_starters"` or `"margin_only"`) appended.

**Example**

```python
poss = flag_garbage_time(poss, pbp)
print(poss.filter(pl.col("is_garbage_time") == True).height)

# CTG-exact, with the starters clause

poss = flag_garbage_time(poss, pbp, starters_on_court=starters_by_possession)
```

### `flag_heave_possessions(possessions: 'pl.DataFrame') -> 'pl.DataFrame'` {#flag_heave_possessions}

Flag CTG's "projected heave possessions" (excluded from CTG stats by default).

CTG (exact): "possessions that start with **4 or fewer seconds on the game
clock at the end of one of the first three quarters**." Q4/OT are exempt — a
late Q4 possession is a real possession.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Any frame with `period` and `start_seconds_remaining`. |

**Returns**

`possessions` with a Boolean `is_heave_possession` column appended.

**Example**

```python
poss = flag_heave_possessions(poss)
clean = poss.filter(pl.col("is_heave_possession") == False)
```

### `fox_nba_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_boxscore}

NBA boxscore (long: one row per player-stat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the per-team stat tables to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_boxscore
df = fox_nba_boxscore("...")
```

### `fox_nba_league_leaders(category: 'str' = 'scoring', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_league_leaders}

NBA statistical leaders (`stats-con`); who=player|team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'scoring'` | Stat category. Defaults to `"scoring"`. |
| `who` | `str` | `'player'` | `"player"` or `"team"`. Defaults to `"player"`. |
| `page` | `int` | `0` | 0-based result page. Defaults to `0`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_league_leaders
df = fox_nba_league_leaders("scoring")
```

### `fox_nba_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_odds}

NBA game odds six-pack (spread / to-win / total per team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_odds
df = fox_nba_odds("...")
```

### `fox_nba_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_pbp}

NBA play-by-play (one row per play; period-based).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the pbp layout to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_pbp
df = fox_nba_pbp("...")
```

### `fox_nba_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_standings}

NBA standings for a team's conference/division.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_standings
df = fox_nba_standings("...")
```

### `fox_nba_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_team_gamelog}

NBA team game log (long: one row per game-stat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_team_gamelog
df = fox_nba_team_gamelog("...")
```

### `fox_nba_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_team_roster}

NBA team roster (one row per player).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the roster tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_team_roster
df = fox_nba_team_roster("...")
```

### `fox_nba_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nba_team_stats}

NBA team stat leaders by category.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader sections to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_team_stats
df = fox_nba_team_stats("...")
```

### `get_constants(league_id: 'str') -> 'LeagueConstants'` {#get_constants}

Return the `LeagueConstants` for a `league_id`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` |  | stats.nba.com league id -- `"00"` NBA, `"10"` WNBA, `"20"` G-League. |

**Returns**

The league's `LeagueConstants`.

**Example**

```python
from sportsdataverse.nba.nba_prediction_constants import get_constants
get_constants("00").hfa
```

### `get_shrinkage_k(league_id: 'str') -> 'float'` {#get_shrinkage_k}

Shooter-talent shrinkage `k` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` |  | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |

**Returns**

The pseudo-attempt shrinkage constant (fitted split-half).

**Example**

```python
from sportsdataverse.nba.nba_shot_value_constants import get_shrinkage_k
get_shrinkage_k("00")
```

### `in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'` {#in_game_features}

Per-play in-game win-probability features from a `load_nba_pbp` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame with `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id` (event team) and `home_team_id` (the `load_nba_pbp` schema). |
| `pregame_home_prob` | `float` |  | The pregame home win probability (e.g. from `win_prob_from_margin`), encoded as a constant logit column. Clipped to `[1e-6, 1 - 1e-6]` so a saturated CDF (exact 0/1) cannot crash the logit. |

**Returns**

One row per input play: `score_diff` (home - away), `sec_left` (clipped at 0 -- overtime plays count as 0 seconds left), `sqrt_sec_left`, `pregame_logit`, `home_has_ball` (`Int8`; dead-ball / unknown-team plays are 0).

**Example**

```python
from sportsdataverse.nba.nba_game_predict import in_game_features
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
feats = in_game_features(pbp, 0.62)
```

### `lineup_play_context(possessions: 'pl.DataFrame', *, min_poss: 'int' = 0, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#lineup_play_context}

Roll possessions up into a per-5-man-lineup Play-Context table.

The lineup analogue of `team_play_context`: same metric columns, grouped
by the five players on the floor **for the offense**. Lineups are identified by
`lineup_id` — the five player ids sorted ascending and hyphen-joined — so the
same five players always land in the same bucket regardless of slot order.

Requires the `off_player_1..5` columns from
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`
(which passes the play-context columns through, so the two compose in either
order).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context` **with lineups attached**. |
| `min_poss` | `int` | `0` | Drop lineups below this possession count (CTG's tables carry a minimum; 0 keeps everything, which is what the partition identity needs). |
| `league_non_transition_ppp` | `Optional[float]` | `None` | Pts+/Poss baseline; see `team_play_context`. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time / heave / non-counting possessions first. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (team, lineup) with `LINEUP_PLAY_CONTEXT_SCHEMA`. Empty input returns a zero-row frame with that schema.

**Example**

```python
poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
lu = lineup_play_context(poss, min_poss=25)
print(lu.sort("pts_per_100", descending=True).head())
```

### `luck_adjusted_response(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0) -> 'pl.DataFrame'` {#luck_adjusted_response}

Attach a per-possession `la_points` expected-points response.

**DECISION 2/4**: `la_points = 2*fg2m + 3*Σ_shooter fg3a·p̂3 + Σ_shooter fta·p̂ft`
(offense-only, "one_way"). 2-pt makes stay realized. `p̂` come from
`player_rates` when given, else shrunk_shooter_rates` on `shooting`.

**Defense-shooter exclusion (bugfix)**: `shooting`
(`~sportsdataverse.nba.nba_possessions.build_possession_shooting`)
deliberately retains defense-team shooters in a possession group — e.g. a
defensive technical free throw shooter — because it is a per-shooter
companion frame, not a team-attributed one (that's why it carries its own
`team_id` column). The expected-points sum is offense-only by
definition (**DECISION 2**), so *before* aggregating `exp_extra` this
function joins `possessions[["game_id", "possession_number",
"offense_team_id"]]` onto `shooting` and filters to
`team_id == offense_team_id`, dropping any defense-team shooter row.
Without this filter a defense tech-FT's `fta·p̂ft` term leaks into the
offense's `la_points`, inflating it (reproduced: 2.9 vs. the
offense-only-correct 2.0 for a single offense 2-pt make plus one defense
tech FT).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m` and the join keys `game_id` + `possession_number` + `offense_team_id`. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame (`build_possession_shooting`), which may include defense-team shooter rows (e.g. technical FTs) — filtered out here before aggregation. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override (e.g. planted truth in tests); `None` → shrink from `shooting`. |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to shrunk_shooter_rates` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to shrunk_shooter_rates` when `player_rates` is `None`. |

**Returns**

`possessions` with an added `la_points: Float64` column (same rows, same order). Empty `possessions` → returned unchanged with an empty `la_points` column.

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import luck_adjusted_response
out = luck_adjusted_response(possessions_df, shooting_df)
print(out["la_points"].mean())

# Planted-truth override for testing

out = luck_adjusted_response(possessions_df, shooting_df, {7: (0.4, 0.8)})
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

### `nba_adj_rapm(possessions: 'pl.DataFrame', prior: 'Dict[int, Tuple[float, float]]', *, alphas: 'np.ndarray' = array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ]), n_samples: 'int' = 200, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_adj_rapm}

One-shot prior-informed RAPM over a possession frame -> per-player ratings.

Builds the sparse design matrix via
`~sportsdataverse.nba.nba_rapm.build_rapm_design`, constructs the
per-possession `prior_mean` vector from `prior`, fits a residualized
ridge with an RTO posterior via fit_prior_ridge`, and returns the
per-player offensive, defensive, and combined adj-RAPM ratings alongside
possession counts.

Sign convention (matches `~sportsdataverse.nba.nba_rapm.nba_rapm`):
`d_adj_rapm` is positive for a good defender (lowers opponent points);
`adj_rapm = o_adj_rapm + d_adj_rapm`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | A possession+lineup frame produced by the possession engine (`game_id`, `offense_team_id`, `points`, `off_player_1..5`, `def_player_1..5`). |
| `prior` | `Dict[int, Tuple[float, float]]` |  | Per-player `{player_id: (o_prior, d_prior)}` in per-100 units. Players absent from `prior` receive a `(0.0, 0.0)` default. |
| `alphas` | `ndarray` | `array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])` | RidgeCV alpha grid for the regularisation strength (default `DEFAULT_RAPM_ALPHAS`). |
| `n_samples` | `int` | `200` | Number of RTO posterior samples (default 200). |
| `seed` | `int` | `0` | RNG seed for the RTO sampler (default 0). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with columns `player_id` (Int64), `o_adj_rapm` (Float64), `d_adj_rapm` (Float64), `adj_rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

**Example**

```python
from sportsdataverse.nba import nba_adj_rapm
ratings = nba_adj_rapm(possessions, spm_prior_dict)
print(ratings.sort("adj_rapm", descending=True).head())
```

### `nba_aging_curve(*, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#nba_aging_curve}

Load the bundled per-age value-multiplier curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `age:Int64, rel_value:Float64, peak_age:Float64` (`peak_age` repeated on every row for convenient filtering/joining).

**Example**

```python
from sportsdataverse.nba import nba_aging_curve
curve = nba_aging_curve()
print(curve.sort("rel_value", descending=True).head(1))

# Pipeline next step (one line)

curve.filter(pl.col("age").is_between(24, 30))
```

### `nba_availability(seasons: "'int | list[int]'", *, league: 'str' = 'nba', gleague_bridge: 'bool' = False, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#nba_availability}

Project games-available % for a season (or seasons) from career GP history.

`avail_pct` is **availability, not skill** -- it is the only output of
this function and is never combined into a value/rating column by this
module (`sportsdataverse.nba.nba_rookie_projection.nba_rookie_projection`
reports it as a separate column too).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (end year, e.g. `2020`) or list of seasons. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `gleague_bridge` | `bool` | `False` | When `True` (and `league != "gleague"`), also pulls each season's G-League (`league_id="20"`) bulk GP as a development-outcome bridge feature before scoring. Best-effort: gracefully absent (never raises) when the G-League bulk call returns no rows for a season; the returned schema is unaffected either way since the bridge column isn't part of the bundled artifact's scored features. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, season:Int64, avail_pct:Float64` (clipped to `[0, 1]`). Empty `seasons` -> zero-row schema.

**Example**

```python
from sportsdataverse.nba import nba_availability
proj = nba_availability(2019)
print(proj.sort("avail_pct").head())
```

### `nba_box_logs(season: 'str', *, league_id: 'str' = '00', season_type: 'str' = 'Regular Season', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Dict[str, pl.DataFrame]'` {#nba_box_logs}

Fetch per-player and per-team game logs for a season (bulk, one call each).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season in `"2023-24"` form. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `season_type` | `str` | `'Regular Season'` | SeasonType (`"Regular Season"`). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguegamelog` replacement for offline tests. |

**Returns**

`{"player": <per-player-game logs>, "team": <per-team-game logs>}` as snake-cased polars frames.

**Example**

```python
from sportsdataverse.nba.nba_box_logs import nba_box_logs
logs = nba_box_logs("2023-24")
print(logs["player"].shape)
```

### `nba_bpm(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', positions: 'pl.DataFrame', *, team_adjust: 'bool' = True, granularity: 'str' = 'season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#nba_bpm}

Faithful BPM 2.0 per player, at season or single-game granularity.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | per-player-per-game box lines (`nba_box_logs`'s `player`); must carry `game_id` when `granularity="game"`. |
| `team_logs` | `DataFrame` |  | per-team-per-game lines incl. `plus_minus` (`nba_box_logs`'s `team`); must carry `game_id` when `granularity="game"`. |
| `positions` | `DataFrame` |  | listed positions (`nba_player_positions`): player_id, position_num. |
| `team_adjust` | `bool` | `True` | apply the team adjustment (True) or return raw box-BPM (False). |
| `granularity` | `str` | `'season'` | `"season"` (default) aggregates every row in `player_logs`/ `team_logs` into one row per player. `"game"` runs the exact same pipeline independently per `game_id` (position/role are estimated game-native, mirroring `NbaBpmModel`'s existing fold-native design) and returns one row per (game_id, player_id) with a leading `game_id` column; `gp` is always 1 in this mode. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`"season"`: frame with `player_id`, `obpm`, `dbpm`, `bpm`, `min`, `gp` (Int64 player_id/gp, Float64 obpm/dbpm/bpm/min). `"game"`: the same columns prefixed with `game_id` (Utf8), one row per (game_id, player_id). Empty (that schema) input -> zero-row frame with the same schema; never raises on empty.

**Example**

```python
from sportsdataverse.nba import nba_bpm, nba_box_logs, nba_player_positions
logs = nba_box_logs("2023-24"); pos = nba_player_positions("2023-24")
bpm = nba_bpm(logs["player"], logs["team"], pos)
print(bpm.sort("bpm", descending=True).head())

# Per-game BPM

bpm_game = nba_bpm(logs["player"], logs["team"], pos, granularity="game")
print(bpm_game.filter(pl.col("game_id") == "0022300001").sort("bpm", descending=True))

# Raw (no team adjustment)

bpm_raw = nba_bpm(logs["player"], logs["team"], pos, team_adjust=False)

# Pandas output

bpm_pd = nba_bpm(logs["player"], logs["team"], pos, return_as_pandas=True)
```

### `nba_career_trajectory(player_values: 'pl.DataFrame', *, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#nba_career_trajectory}

Age-adjust player-season values with the bundled aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_values` | `DataFrame` |  | Frame `player_id, age:Int64, value:Float64`. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`player_values` plus `age_adjusted_value` (`value / rel_value(age)`, peak-centered) and `proj_next_value` (`value * rel_value(age+1) / rel_value(age)`). Ages outside the bundled curve's range fall back to `rel_value = 1.0` (no adjustment). Empty input returns the zero-row schema.

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_career_trajectory
player_values = pl.DataFrame({"player_id": ["1"], "age": [24], "value": [10.0]})
nba_career_trajectory(player_values)
```

### `nba_darko(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, aging_curve: "'AgingCurve | None'" = None, process_var: "'float | None'" = None, obs_base: "'float | None'" = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_darko}

Project each player's next-season rating via a per-player Kalman filter + aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id, season, rating` (+ optional `weight`) — a multi-season rating panel. |
| `ages` | `DataFrame` |  | `player_id, season, age` (from `nba_player_ages`). |
| `aging_curve` | `AgingCurve \| None` | `None` | an `AgingCurve`; fitted from `panel` if None. |
| `process_var` | `float \| None` | `None` | Kalman process variance `q`; MLE-fit from `panel` if None. |
| `obs_base` | `float \| None` | `None` | Kalman base observation variance; MLE-fit from `panel` if None. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`player_id, last_season, forecast_season, filtered_skill, projected_rating, projected_sd`.

**Example**

```python
from sportsdataverse.nba import nba_darko, nba_player_ages
proj = nba_darko(rating_panel, ages_panel)
print(proj.sort("projected_rating", descending=True).head())
```

### `nba_decay_rapm(possessions: 'pl.DataFrame', *, asof: 'Optional[datetime.date]' = None, half_life_days: 'float' = 180.0, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_decay_rapm}

Time-decay RAPM: ridge weighted by `0.5 ** (days_ago / half_life_days)`.

`asof=None` disables decay: every possession is weighted `1.0` and the
fit uses **exactly** plain `~sportsdataverse.nba.nba_rapm.nba_rapm`'s
own schedule (`alphas=DEFAULT_RAPM_ALPHAS`, sklearn's efficient default
LOOCV) so the two agree byte-for-byte (see
`test_decay_rapm_asof_none_equals_plain_rapm`). When `asof` is set,
possessions dated after `asof` are dropped, the remainder is
exponentially down-weighted by age, and the fit switches to the
**oracle** regularization schedule (`oracle_rapm_alphas` evaluated
at the post-filter possession count, `cv=` `ORACLE_RAPM_CV`) per
the binding WP2 ridge-schedule ruling documented in the module docstring.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Multi-season possession+lineup frame. Must carry a `game_date` (`pl.Date`) column when `asof` is not `None`. |
| `asof` | `Optional[date]` | `None` | Reference date; `None` -> unweighted, plain-RAPM-equivalent fit. |
| `half_life_days` | `float` | `180.0` | Weight half-life in days (default 180). |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS` when `asof is None` or `oracle_rapm_alphas` (evaluated at the possession count) when `asof` is set. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `DECAY_RAPM_SCHEMA`. Empty input, or an `asof` that drops every possession, -> zero-row frame.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_rapm_variants import nba_decay_rapm

df = nba_decay_rapm(season_poss, asof=datetime.date(2024, 3, 1), half_life_days=120.0)
print(df.sort("decay_rapm", descending=True).head())

# Plain-RAPM-equivalent (no decay)

df = nba_decay_rapm(season_poss)  # asof=None
```

### `nba_draft_model(draft_year: "'Union[int, list[int]]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, gleague_bridge: 'bool' = False, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_draft_model}

Project prospect career value + draft probability from combine measurements.

Loads the draft-combine wrappers for `draft_year` (or each year in the
list), builds the shared combine-feature vector
(`sportsdataverse.nba.nba_draft_constants.build_combine_features`),
and applies the bundled ridge (`proj_career_value`) / logistic
(`draft_prob`) heads fit in `dev/nba_draft/fit_draft_model.py`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `Union[int, list[int]]` |  | A draft year (e.g. `2019`) or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"` -- selects the bundled artifact and the combine-wrapper family. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional frame keyed on `player_id:Utf8` carrying the college-side MBB/WBB player-value spine's `projected_pick` / `box_bpm` / `archetype` (model ⑤, see design doc §3.5). When present and the bundled artifact has matching feature columns, it is left-joined as an extra feature block. This function **never** imports `sportsdataverse.mbb` -- callers pass the frame in. |
| `gleague_bridge` | `bool` | `False` | When `True`, left-joins G-League (`league_id="20"`) bulk production (`gleague_pts`/`gleague_gp`/`gleague_min`) for the draft year's season as extra, forward-looking feature columns. Not part of any bundled artifact's scored features today (joining it never changes `proj_career_value`/`draft_prob`); gracefully absent when the G-League bulk call returns no rows -- never raises. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_career_value:Float64, draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8` — one row per prospect with combine measurements for that class. `projected_pick` is a contiguous 1..N rank within each draft year. Empty/malformed input returns the zero-row schema, never raises.

**Example**

```python
from sportsdataverse.nba import nba_draft_model
board = nba_draft_model(2019)
print(board.sort("proj_career_value", descending=True).head())

# With a college-side prior

board = nba_draft_model(2019, college_prior=mbb_prior_df)

# Pipeline next step (one line)

board.filter(pl.col("pro_tier") == "lottery")
```

### `nba_expected_turnovers(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_expected_turnovers}

Expected TOV + residual ball-security skill from Synergy play-type mix.

`lg_to_rate_t` = poss-weighted league mean of `turnover_freq` for play
type `t`; `expected_tov = scale · Σ_t poss_t · lg_to_rate_t`;
`ball_security_skill = 100·(expected_tov − tov)/poss` (fewer turnovers
than expected ⇒ positive skill -- sign flipped vs. the foul-drawing model).

`scale = Σ actual tov / Σ raw type-mix estimate` is derived from the
fetched season itself (covers players below Synergy's per-type
classification threshold) so `Σ expected_tov ≡ Σ tov` holds exactly.
Swapping each player's own `turnover_freq` in for the league rate
reconstructs their real season TOV almost exactly (slope ~1.03, Spearman
~0.97 on the 2023-24 oracle corpus) -- confirming the column semantics are
correct. The *expected* (league-rate) version necessarily explains less
variance than *actual* (turnover-avoidance is a more individual,
less play-type-bound skill than foul-drawing), so its calibration slope
runs lower than model (3)'s -- see the oracle gate for the observed floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `tov`, `poss` (bypasses the live fetch). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `turnover_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`tov`/ `expected_tov`/`ball_security_skill` (Float64). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_expected_turnovers
t = nba_expected_turnovers("2023-24")
print(t.sort("ball_security_skill", descending=True).head(10))

# Injected offline (oracle / test) path

t = nba_expected_turnovers("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

t.filter(pl.col("poss") >= 200).sort("ball_security_skill", descending=True)
```

### `nba_foul_drawing(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, advanced: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_foul_drawing}

Expected FTA + residual foul-drawing skill from Synergy play-type mix.

`lg_ft_rate_t` = poss-weighted league mean of `ft_freq` for play type
`t` (`Σ_players(ft_freq_t·poss_t) / Σ_players(poss_t)`).
`expected_fta = scale · Σ_t poss_t · lg_ft_rate_t`;
`foul_draw_skill = 100·(fta − expected_fta)/poss`.

`ft_freq` (Synergy's `ft_poss_pct`) counts FT-drawing *trips*, not
individual free-throw attempts (a shooting foul is typically a 2-shot
trip) -- `scale = Σ actual fta / Σ raw trip-based estimate` is derived
from the fetched season itself (never hard-coded) so the trip-to-attempt
conversion self-normalizes and `Σ expected_fta ≡ Σ fta` holds exactly.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `fta`, `poss` (bypasses the live fetch). |
| `advanced` | `Optional[DataFrame]` | `None` | Injected `Advanced`-measure frame with `player_id`, `pfd` (personal fouls drawn); optional -- `pfd` is `null` when omitted (`fta` is the always-present proxy). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `ft_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`fta`/ `expected_fta`/`foul_draw_skill` (Float64), `pfd` (Float64, null when *advanced* has no data for that player or is omitted). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_foul_drawing
f = nba_foul_drawing("2023-24")
print(f.sort("foul_draw_skill", descending=True).head(10))

# Injected offline (oracle / test) path

f = nba_foul_drawing("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

f.filter(pl.col("poss") >= 200).sort("foul_draw_skill", descending=True)
```

### `nba_four_factor_rapm(possessions: 'pl.DataFrame', *, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_four_factor_rapm}

Four-factor RAPM: four independent ridge fits (efg/ftr/orbd/tov) on the SAME design.

Each factor is regressed on the identical offense/defense design matrix,
differing only in the per-possession response (FACTOR_RESPONSES`).
Output mirrors the oracle's `RA_*__Off/__Def` layout. **DECISION 5/6/7**
govern the response definitions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m, fg3m, ftm, oreb, tov` and the ten lineup columns. Must also carry a `points` column -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally) requires it unconditionally even though none of the four factor responses use it. `offense_team_id` is NOT required here (unlike `nba_la_rapm`): none of the four factor responses need the offense-only shooter join. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override, shared by all four factor fits. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `FOUR_FACTOR_SCHEMA` — `{factor}__off` / `{factor}__def` columns per factor, plus possession counts. Empty input → zero-row frame.

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_four_factor_rapm
ff = nba_four_factor_rapm(season_poss)
print(ff.sort("efg__off", descending=True).head())
```

### `nba_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_in_game_win_prob}

Per-play home win probability from the bundled in-game model.

Scores `in_game_features` through the committed artifact
(`sportsdataverse/nba/models/nba_in_game_wp.ubj` for NBA -- a shallow
xgboost booster, trained on 2022-23 so the 2023-24 calibration backtest
stays out-of-sample; escalated from a plain logistic that failed the
per-bucket calibration gate).

Gate note: the plan's concurrent oracle (stats.nba.com
`winprobabilitypbp` HOME_PCT) is a dead endpoint, so this model is
validated ONLY on realized-outcome calibration, not against a native WP
feed. See the fixtures README + SDD ledger.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play for ONE game in the `load_nba_pbp` schema (`start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`). |
| `pregame_home_prob` | `float` |  | Pregame home win probability (e.g. from `win_prob_from_margin`). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the five feature columns plus `home_win_prob`.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = nba_in_game_win_prob(pbp, 0.62)
```

### `nba_la_rapm(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, alphas: 'Optional[np.ndarray]' = None, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_la_rapm}

Luck-adjusted RAPM: ridge on an expected-points response (high-variance shooting regressed).

Replaces realized 3-point and free-throw outcomes with the shooter's shrunk
expected value (`luck_adjusted_response`); 2-pt makes stay realized.
**DECISION 2/3/4** govern the response recipe and shrinkage constants.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame with team-level `fg2m` and the ten lineup columns; join keys `game_id` + `possession_number` + `offense_team_id` (the last is required by `luck_adjusted_response`'s defense-shooter leak filter). Must also carry a `points` column even though the LA response (`la_points`) supersedes it for fitting -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally via prepare`) requires it unconditionally. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame from `build_possession_shooting`. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override; `None` → shrink from `shooting`. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule (`cv=` `ORACLE_RAPM_CV` always; there is no plain-schedule mode). |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `LA_RAPM_SCHEMA`. Empty input → zero-row frame.

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_la_rapm
df = nba_la_rapm(season_poss, season_shooting)
print(df.sort("la_rapm", descending=True).head())

# Planted-truth shooter rates (e.g. for testing)

df = nba_la_rapm(season_poss, season_shooting, {7: (0.4, 0.8)})
```

### `nba_matchup_drapm(season: 'str', *, league_id: 'str' = '00', matchups: 'Optional[pl.DataFrame]' = None, config: 'Optional[PlaytypeConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_matchup_drapm}

Matchup-based defensive RAPM (offense-quality-controlled).

Fits `points_allowed_per_100 ~ defender_FE + offense_FE` via
`~sklearn.linear_model.RidgeCV` (weighted by matchup possessions),
reusing the shipped RAPM ridge machinery on the
`build_matchup_drapm_design` two-way-FE design.

**Sign + scale:** the design target `y` is already points-allowed *per 100*
matchup possessions (`100 * player_pts / partial_poss`), so the defender
coefficient is already on the per-100 scale -- `matchup_drapm =
-(beta_defender - mean_beta_defender)` (centered, NO extra ×100, unlike
`~sportsdataverse.nba.nba_rapm.nba_rapm` whose `y` is per-*possession*
and needs the ×100). Sign is negated so higher = better defense (fewer points
allowed), matching the `d_rapm` convention. Typical magnitudes are a few to
low-double-digit points per 100 vs the league defender average.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig`; defaults to a fresh instance (`ridge_alphas` = the shared RAPM grid, `min_matchup_poss` = 25.0 inclusion floor). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per defender: `player_id` (Int64), `matchup_drapm` (Float64, points-allowed-per-100 estimate, higher = better defense), `matchup_poss` (Float64, total matchup possessions guarded). Returns a zero-row frame with this schema when the upstream fetch/injection is empty or no row survives the `min_matchup_poss` floor (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_matchup_drapm
d = nba_matchup_drapm("2023-24")
print(d.sort("matchup_drapm", descending=True).head(10))

# Injected offline (oracle / test) path

d = nba_matchup_drapm("2023-24", matchups=matchups_df)

# Pipeline next step

d.filter(pl.col("matchup_poss") >= 200).sort("matchup_drapm", descending=True)
```

### `nba_pbp_disk(game_id, path_to_json)` {#nba_pbp_disk}

Load a previously cached ESPN NBA summary JSON for a game from disk.

Reads `{path_to_json}/{game_id}.json`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `path_to_json` | `str` |  | Directory containing the cached JSON file. |

**Returns**

Parsed JSON contents.

**Example**

```python
from sportsdataverse.nba import nba_pbp_disk
pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
print(list(pbp.keys()))
```

### `nba_play_context(game_id: 'str', league_id: 'str' = '00', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_play_context}

Fetch one game and return its possessions with the full CTG play-context surface.

Single live call to `nba_stats_playbyplayv3`, then
`add_play_context`. Works for NBA (`league_id="00"`), WNBA and the
G-League — `stats.wnba.com` ships the same play-by-play shapes, and every
threshold here is league-agnostic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | Ten-character game identifier (e.g. `"0022200001"`). |
| `league_id` | `str` | `'00'` | League identifier (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Possession frame with the play-context columns. Empty/malformed payloads return a zero-row frame — never raises.

**Example**

```python
from sportsdataverse.nba.nba_play_context import nba_play_context
poss = nba_play_context("0022200001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate for the game

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False) & (pl.col("is_heave_possession") == False)
)
print(clean["is_transition"].mean())
```

### `nba_player_ages(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'` {#nba_player_ages}

Per-player age for a season (bulk), for the DARKO aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguedashplayerbiostats` replacement for offline tests. |

**Returns**

Frame `player_id:Int64, age:Float64`.

**Example**

```python
from sportsdataverse.nba import nba_player_ages
ages = nba_player_ages("2023-24")
print(ages.head())
```

### `nba_player_identity(player_logs: 'pl.DataFrame') -> 'pl.DataFrame'` {#nba_player_identity}

Human-readable identity for every player in a season's box logs.

Model outputs key on `player_id` alone, which makes them unusable without a
second lookup -- a leaderboard reads `1628983` instead of
`Shai Gilgeous-Alexander`. This derives the display columns from the season's
own game logs, so they are **season-accurate**: a player's team is what he
actually played for that year, not his current one (which is what a player
directory would give and would silently mislabel every historical season).

A traded player has rows for several teams. `team_*` is his **primary** team
by minutes -- the one a reader means when they say "his team that season" --
and `teams` lists every abbreviation he appeared for, in descending minutes,
so a trade is visible rather than silently collapsed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game rows from `leaguegamelog` (the `player_or_team="P"` variant), carrying `player_id`, `player_name`, `team_id`, `team_abbreviation`, `team_name` and `min`. |

**Returns**

One row per `player_id` with `PLAYER_IDENTITY_SCHEMA`. An empty input -- or one missing any required column, `min` included -- gives the zero-row frame with that schema, so callers can join unconditionally. `min` is required rather than optional: without it every team totals zero minutes and "primary team" quietly degrades to whichever `team_id` sorts first, which looks like an answer but is not one.

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_player_identity

logs = pl.DataFrame({
    "player_id": [1628983],
    "player_name": ["Shai Gilgeous-Alexander"],
    "team_id": [1610612760],
    "team_abbreviation": ["OKC"],
    "team_name": ["Oklahoma City Thunder"],
    "min": [34.0],
})
ratings = pl.DataFrame({"player_id": [1628983], "war": [21.9]})
named = ratings.join(nba_player_identity(logs), on="player_id", how="left")
print(named.select("player_name", "team_name", "war"))
```

### `nba_player_positions(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'` {#nba_player_positions}

Fetch league-wide listed positions for a season as numeric 1-5.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_playerindex` replacement for offline tests. |

**Returns**

Frame with columns `player_id:Int64, position_num:Float64`.

**Example**

```python
from sportsdataverse.nba import nba_player_positions
pos = nba_player_positions("2023-24")
print(pos.head())

# Offline / injectable fetch for testing

import polars as pl
stub = lambda **kw: pl.DataFrame({"person_id": [1], "position": ["PG"]})
pos = nba_player_positions("2023-24", fetch=stub)
```

### `nba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_player_props}

Per-player expected prop lines + team pace projection for a matchup.

Loads the season's player box logs + team ratings, computes per-minute
`player_rates`, and projects each player's line onto their mean
minutes and the matchup's pace factor (`exp_poss / avg_pace`). Only the
two teams in the matchup are returned.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024`). |
| `game_id` | `str` |  | The game id (passed through for the caller's join; not used to filter historical rates). |
| `home_team_id` | `str` |  | Home team id. |
| `away_team_id` | `str` |  | Away team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player on either team: `player_id, team_id, stat_pts_exp, stat_reb_exp, stat_ast_exp, stat_fg3m_exp, pace_proj`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_player_props import nba_player_props
props = nba_player_props(2024, "401585828", "2", "6")
```

### `nba_playtype_ratings(season: 'str', *, league_id: 'str' = '00', off_team: 'Optional[pl.DataFrame]' = None, def_team: 'Optional[pl.DataFrame]' = None, schedule: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_playtype_ratings}

Season play-type-adjusted offensive/defensive team ratings.

Fetches (or uses injected) Synergy offensive/defensive team frames plus the
league schedule, computes raw per-type efficiency
(`raw_playtype_efficiency`), opponent-adjusts it
(`adjust_playtype_efficiency`), then rolls up to one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id` (Int64), `adj_off`/`adj_def`/`adj_net` (Float64) roll-ups, plus per-type wide columns `adj_off_ppp_<playtype>`/`adj_def_ppp_<playtype>`/`off_freq_<playtype>` (Float64) for each play type present in the data. `adj_off = Σ_t off_freq_t · adj_off_ppp_t · 100` (symmetric for `adj_def` off `def_freq_t`); `adj_net = adj_off - adj_def`. Returns a zero-row frame with the base roll-up schema when the upstream fetch is empty (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_playtype_ratings
r = nba_playtype_ratings("2023-24")
print(r.sort("adj_off", descending=True).head(10))

# Injected offline (oracle / test) path

r = nba_playtype_ratings("2023-24", off_team=off_df, def_team=def_df, schedule=sched_df)

# Pipeline next step

r.filter(pl.col("adj_net") > 0).sort("adj_net", descending=True)
```

### `nba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_predict_games}

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home/away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` / `predict_total`
math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace` (the `~sportsdataverse.nba.nba_team_ratings.nba_team_ratings` output for one season/as-of date). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
preds = nba_predict_games(games, nba_team_ratings(2024))
```

### `nba_ratings_panel(model: 'AnyModel', possessions: 'pl.DataFrame', dates: 'Optional[Sequence[datetime.date]]' = None, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_ratings_panel}

Player-ratings-through-date long panel: one row per (player_id, date).

Refit-per-checkpoint (v1; no warm-start incrementality) — each date's row
calls `ratings_as_of` independently, so the panel is leakage-free by
construction: a possession dated after a given checkpoint can never affect
that checkpoint's row, no matter what other dates are also being computed
or what future rows exist in `possessions`. Cost is a full refit per
checkpoint date; for a season's sparse RAPM-family design this is seconds
per date, not minutes — acceptable for a nightly/daily cadence but not for
live in-game updating (out of scope; see spec non-goals).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel`. |
| `possessions` | `DataFrame` |  | A possession+lineup frame with a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `dates` | `Optional[Sequence[date]]` | `None` | Checkpoint dates to compute. `None` (default) uses every distinct `game_date` present in `possessions`, sorted ascending — a rating for every game day, matching what EPM/LEBRON publish nightly. Duplicates are deduped; input order does not matter (the output is always sorted by date). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Long frame with `RATINGS_PANEL_SCHEMA` columns (`player_id`, `date`, `o_rating`, `d_rating`, `rating`). Zero-row (that schema) when `possessions` is empty or no date yields any players.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import nba_ratings_panel

checkpoints = [datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]
panel = nba_ratings_panel(RidgeRapmModel(), season_poss, dates=checkpoints)
print(panel.filter(pl.col("player_id") == 201939).sort("date"))

# Every game day, no explicit grid

panel = nba_ratings_panel(RidgeRapmModel(), season_poss)
```

### `nba_raw_store_season_frame(endpoint: 'str', season: 'int', variant: 'Optional[str]' = None, *, result_set: 'Optional[str]' = None, raw_store_dir: 'RawStoreDir' = None) -> "Optional['pl.DataFrame']"` {#nba_raw_store_season_frame}

Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame.

The per-game half of the store is served by the read-through per-game path;
this is the season-keyed half (`leaguegamelog`, `playerindex`,
`leaguedashplayerbiostats`, ...) that the `-raw` scraper writes as

* `{endpoint}/{season}/{variant}.json` -- parameterized captures, where
  *variant* is the slugified parameter sweep (e.g. `"regular-season"`,
  `"regular-season_totals"`), and
* `{endpoint}/{season}.json` -- unparameterized captures (e.g.
  `playerindex`), i.e. `variant=None`.

Roots may be a local checkout or an `http(s)://` base (the raw repo served
over raw.githubusercontent / a CDN), so a consumer -- notably the
`hoopR-nba-stats-data` model producer -- runs clone-free in CI against the
same committed tree the per-game compile reads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` |  | stats.nba.com endpoint slug (the store subdirectory). |
| `season` | `int` |  | Season **START** year (2023 = 2023-24) -- the key this half of the store is filed under; see the warning above. |
| `variant` | `Optional[str]` | `None` | Capture variant slug, or `None` for an unparameterized capture. |
| `result_set` | `Optional[str]` | `None` | Named result set to return when the payload carries several; defaults to the first frame that parses. |
| `raw_store_dir` | `RawStoreDir` | `None` | Store root spec (dir or URL base) or per-endpoint mapping; `None` falls back to the env vars, `""` disables. |

**Returns**

The parsed `polars.DataFrame`, or `None` when the store is unset, the capture is absent, or the payload carries no usable frame -- so a caller can cleanly fall back to a live fetch.

**Example**

```python
from sportsdataverse.nba import nba_raw_store_season_frame
base = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
logs = nba_raw_store_season_frame("leaguegamelog", 2024, "regular-season", raw_store_dir=base)

# Fall back to a live fetch when the capture is absent

frame = nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=base)
positions = frame if frame is not None else nba_stats_playerindex(season="2023-24")
```

### `nba_rookie_projection(draft_year: "'int | list[int]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#nba_rookie_projection}

Project rookie/sophomore value by composing draft x aging x availability.

Composition (no re-derived features -- each term is the verbatim public
output of ①②③):

- `base = nba_draft_model(...).proj_career_value * rookie_fraction`
  (`rookie_fraction` from the bundled residual artifact -- the share of
  career value realized in a single rookie season).
- `proj_rookie_value = base * rel_value(rookie_age) / rel_value(peak_age)
  + residual[pro_tier]`; `proj_soph_value` uses `rookie_age + 1`.
- `proj_avail_pct` from `sportsdataverse.nba.nba_availability.nba_availability`
  at rookie age -- reported separately, **never** multiplied into the
  value columns (availability is availability, not skill).
- `proj_rookie_min = games_full_season * proj_avail_pct * expected_mpg(pro_tier)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional college-side prior frame, forwarded verbatim to `sportsdataverse.nba.nba_draft_model.nba_draft_model`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64, proj_soph_value:Float64, proj_rookie_min:Float64, proj_avail_pct:Float64, pro_tier:Utf8`. Empty input -> zero-row schema.

**Example**

```python
from sportsdataverse.nba import nba_rookie_projection
board = nba_rookie_projection(2019)
print(board.sort("proj_rookie_value", descending=True).head())
```

### `nba_shot_value(player_ids: "'list[int]'", season: 'str', *, league_id: 'str' = '00', include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"` {#nba_shot_value}

One-call shot-value spine: fetch, score, and run all five models.

Fetches each player's `shotchartdetail`, scores per-shot expected points
from the free `LeagueAverages` zone table, and returns the scored shots
plus shooter talent, selection quality, and zone-value maps (and the
defender/shot-clock context tables when `include_context=True`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.nba import nba_shot_value
out = nba_shot_value([201939], "2022-23")
out["talent"].head()
```

### `nba_shot_value_lineups(group_id: 'str', season: 'str', *, team_id: 'int', league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_shot_value_lineups}

Scored per-shot frame for one 5-man lineup (`shotchartlineupdetail`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `group_id` | `str` |  | The 5-man lineup group id (dash-joined player ids); kept `Utf8`. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `team_id` | `int` |  | The lineup's team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The lineup's shots scored by `score_shot_xpoints` (with `xpoints`). Empty fetch returns the augmented zero-row schema.

**Example**

```python
from sportsdataverse.nba import nba_shot_value_lineups
df = nba_shot_value_lineups("201939-202691-...", "2022-23", team_id=1610612744)
```

### `nba_spm(box_features: 'pl.DataFrame', coefficients: 'SpmCoefficients', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_spm}

Apply fitted SPM coefficients to per-100 box features -> OSPM/DSPM/SPM.

Applies a linear scoring rule:

.. code-block:: text

    ospm = X @ o_coef + o_intercept
    dspm = X @ d_coef + d_intercept
    spm  = ospm + dspm

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id`, every column in `coefficients.feature_names`, `min`, and `gp`. |
| `coefficients` | `SpmCoefficients` |  | A `SpmCoefficients` instance from `train_spm`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

Per-player frame with columns `player_id` (Int64), `ospm` (Float64), `dspm` (Float64), `spm` (Float64), `min` (Float64), `gp` (Int64).

**Example**

```python
from sportsdataverse.nba import nba_spm
ratings = nba_spm(box_feats, coef)
print(ratings.sort("spm", descending=True).head())

# Pipeline next step

ratings.filter(pl.col("min") >= 500).sort("spm", descending=True)
```

### `nba_team_clutch(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_team_clutch}

Opponent-agnostic clutch skill (shrunk clutch net-rating delta) per team.

Loads the season's clutch net rating (`nba_stats_leaguedashteamclutch`)
and full-game net baseline (`nba_stats_leaguedashteamstats`), computes
`clutch_delta`, and applies `shrink_clutch`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024` for 2023-24). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team: `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_skill_shrunk, clutch_poss`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_clutch import nba_team_clutch
skill = nba_team_clutch(2024)
skill.sort("clutch_skill_shrunk", descending=True).head()
```

### `nba_team_ratings(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nba_team_ratings}

Opponent-adjusted team ratings (AdjOffRtg/AdjDefRtg/AdjNet/AdjPace), as-of-date aware.

Loads schedule + team box score for `seasons`, optionally filters to
games strictly before `as_of_date` (the leakage boundary, via
`~sportsdataverse.nba.nba_prediction_constants.as_of_ratings_split`),
computes per-game efficiency, runs the opponent-adjustment fixed points,
and adds a per-season dense `rank` (on `adj_net_rtg` descending) and
`adj_net_z` (z-score of `adj_net_rtg`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2024`) or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `as_of_date` | `Union[date, None]` | `None` | If given, only games with `date < as_of_date` are used (predictive/backtest usage); `None` computes full-season descriptive ratings. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace, raw_off_rtg, raw_def_rtg, raw_pace, games, rank, adj_net_z`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
ratings = nba_team_ratings(2024)
ratings.sort("rank").head()

# As-of-date (leakage-safe) ratings for a backtest

import datetime as dt
ratings = nba_team_ratings(2024, as_of_date=dt.date(2024, 1, 15))

# WNBA / G-League via ``league_id``

wnba_ratings = nba_team_ratings(2024, league_id="10")
```

### `nba_tracking_drive_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_tracking_drive_value}

Drive value over expected + rim-pressure, per player-season.

Fetches the `Drives` `leaguedashptstats` measure and computes
`drive_pts_oe = drive_pts - drives * bucket_pts_per_drive`. `rim_pressure`
is the z-score of `drive_fta / drives` within the player's role bucket
(a proxy for foul-drawing pressure independent of scoring efficiency).
`drive_ast`/`drive_tov` are passed through unchanged.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, drives:Float64, drive_pts:Float64, drive_baseline_rate:Float64, drive_expected:Float64, drive_pts_oe:Float64, drive_pts_oe_per_36:Float64, drive_fta:Float64, rim_pressure:Float64, drive_ast:Float64, drive_tov:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nba import nba_tracking_drive_value
df = nba_tracking_drive_value(2024)
print(df.sort("drive_pts_oe", descending=True).head())
```

### `nba_tracking_pass_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, fetch_potential_assists: 'bool' = False, max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _pass_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_tracking_pass_value}

Expected-assists / passer value: `ast_oe` per player-season.

Fetches the `Passing` `leaguedashptstats` measure (one call) and computes
`ast_oe = ast - passes * bucket_assist_rate`. When
`fetch_potential_assists=True`, also fetches `nba_stats_playerdashptpass`
for the top-`max_players` passers (capped, optional -- never a hard
dependency) and recomputes the residual against the richer
`potential_assists` denominator for that subset; `max_players=0`
(default) makes exactly one request total. `ast_pts_created` is passed
through directly from the Passing measure (it is already computed there;
not re-derived).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
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
from sportsdataverse.nba import nba_tracking_pass_value
df = nba_tracking_pass_value(2024)
print(df.sort("ast_oe", descending=True).head())

# With potential-assist enrichment for the top 50 passers

df = nba_tracking_pass_value(2024, fetch_potential_assists=True, max_players=50)
```

### `nba_tracking_reb_oe(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_tracking_reb_oe}

Rebounding-over-expected: `reb_oe` plus OREB/DREB splits, per player-season.

Fetches the `Rebounding` `leaguedashptstats` measure, attaches a
`guard`/`wing`/`big` role bucket, and computes
`reb_oe = reb - reb_chances * bucket_rate` (contest-difficulty-adjusted
when the endpoint carries separate contested/uncontested CHANCE columns;
the live `stats.nba.com` payload currently does not, so this degrades
gracefully to the plain rate -- see the fixtures README for the finding).
OREB/DREB residuals are computed identically against their own chance
columns. Baselines are recomputed from the same season slice on every
call -- there is no fitted constant or bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season (`int` ending-year or `"YYYY-YY"` string) or a list of seasons to concatenate. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within `guard`/`wing`/`big` buckets (default). `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame (see attach_role_bucket`); mostly for injecting a fixture in tests. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats` returning the raw payload dict directly -- offline testing hook. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, reb:Float64, reb_chances:Float64, reb_baseline_rate:Float64, reb_expected:Float64, reb_oe:Float64, reb_oe_per_36:Float64, oreb_oe:Float64, dreb_oe:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nba import nba_tracking_reb_oe
df = nba_tracking_reb_oe(2024)
print(df.sort("reb_oe", descending=True).head())

# League-wide baseline (no position split)

df_all = nba_tracking_reb_oe(2024, by_position=False)

# Pandas output

df_pd = nba_tracking_reb_oe(2024, return_as_pandas=True)
```

### `nba_tracking_rim_protect_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, source: 'str' = 'leaguedash', max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _defend_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_tracking_rim_protect_value}

Rim-protection / shot-defend points-saved over expected, per player-season.

Fetches the `Defense` `leaguedashptstats` measure -- which on the live
`stats.nba.com` payload exposes only rim-band defended shooting
(`def_rim_fgm`/`def_rim_fga`/`def_rim_fg_pct`, no separate overall
figure -- see the fixtures README) -- and computes
`rim_protect_pts_saved = (normal_fg_pct - d_fg_pct) * d_fga * 2` where
`normal_fg_pct` is the bucket-mean defended rate (there is no
shooters'-own-average column on this endpoint, so the bucket mean is the
baseline; this is the same attempts-weighted construction as every other
model, just sign-flipped so a defender who holds shooters BELOW the
bucket mean gets a positive points-saved value).

`source="shotdefend"` swaps in the `Less-Than-6-Ft` band from
`nba_stats_playerdashptshotdefend` for the top-`max_players` defenders
by attempt volume (capped, optional -- never a hard dependency);
`max_players=0` (default) uses the leaguedash figures for everyone.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
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
from sportsdataverse.nba import nba_tracking_rim_protect_value
df = nba_tracking_rim_protect_value(2024)
print(df.sort("rim_protect_pts_saved", descending=True).head())
```

### `nba_tracking_shot_diet_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_tracking_shot_diet_value}

Catch-&-shoot vs pull-up points-over-expected, per player-season.

Fetches `CatchShoot` and `PullUpShot` (two calls), scores each with the
shared engine, joins on `player_id` (dtype-asserted `Utf8` both sides
first), and computes `shot_diet_delta = (cs_pts_oe / cs_fga) -
(pu_pts_oe / pu_fga)` (null-safe on zero attempts) -- positive means the
player's efficiency edge comes from catch-&-shoot, negative from
off-the-dribble.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to each fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute each measure's baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`, dispatched by the `pt_measure_type` kwarg for each of the two calls. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, cs_fga:Float64, cs_pts:Float64, cs_pts_oe:Float64, pu_fga:Float64, pu_pts:Float64, pu_pts_oe:Float64, shot_diet_delta:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nba import nba_tracking_shot_diet_value
df = nba_tracking_shot_diet_value(2024)
print(df.sort("cs_pts_oe", descending=True).head())
```

### `nba_tracking_touch_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#nba_tracking_touch_value}

Touch / possession-time value over expected, per player-season.

Fetches the `Possessions` `leaguedashptstats` measure and computes
`pts_per_touch_oe = pts - touches * bucket_pts_per_touch`.
`time_of_poss_eff` is the z-score of `pts / time_of_poss` within the
player's role bucket -- scoring economy per second of possession,
independent of touch volume.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, touches:Float64, pts:Float64, touch_baseline_rate:Float64, touch_expected:Float64, pts_per_touch_oe:Float64, time_of_poss:Float64, time_of_poss_eff:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nba import nba_tracking_touch_value
df = nba_tracking_touch_value(2024)
print(df.sort("pts_per_touch_oe", descending=True).head())
```

### `nba_v3_to_v2_pbp(pbp_v3: 'dict', box_v3: 'dict', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_v3_to_v2_pbp}

Convert a v3 `playbyplayv3` payload into the full v2-schema pbp frame.

Ports hoopR's `.v3_to_v2_format()` (`R/nba_stats_pbp.R` lines
210-810) to polars: the v3 feed (`stats.nba.com` `playbyplayv3`) is
reshaped into the older v2 schema that the committed hoopR-nba-stats-data
dataset carries and that `pbpstats`' `stats_nba` provider consumes.
This is a pure, network-free function -- both payloads must already be
fetched (e.g. via `nba_stats_playbyplayv3` / `nba_stats_boxscoretraditionalv3`).

Pipeline:

1. Build the per-`person_id` roster from `box_v3`
   (build_roster`) and recover `player2_id`/`player3_id`
   (assist/block/steal/sub-in/jump) from `pbp_v3` (
   extract_secondary_players`).
2. Drop the standalone block/steal rows consolidated into their parent
   Missed Shot / Turnover (is_dropped_block_steal`) -- the only
   row-count change versus the raw v3 action list.
3. Derive `event_type`/`event_action_type` from the module's lookup
   tables, split `description` by `location` into home/visitor/
   neutral, forward-fill the running score, and enrich `player2`/
   `player3` from the roster **by id** (see secondary_fields`
   for the deliberate divergence from hoopR's name-based re-resolution).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_v3` | `dict` |  | Raw `playbyplayv3` dict (`nba_stats_playbyplayv3` / `wnba_stats_playbyplayv3` payload shape); actions live at `pbp_v3["game"]["actions"]`. |
| `box_v3` | `dict` |  | Raw `boxscoretraditionalv3` dict, passed through to build_roster`. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of `polars.DataFrame`. |

**Returns**

Polars (or pandas) DataFrame with the full v2 schema (game/event identifiers, event/action type codes, home/visitor/neutral descriptions, forward-filled score + margin + leader, per-player columns for players 1-3, and the v3 passthrough columns). Empty or malformed input returns a zero-row frame with the same schema (never raises).

**Example**

```python
from sportsdataverse.nba.nba_v3_v2_adapter import nba_v3_to_v2_pbp
from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3, nba_stats_boxscoretraditionalv3

pbp_v3 = nba_stats_playbyplayv3(game_id="0022300001", return_parsed=False)
box_v3 = nba_stats_boxscoretraditionalv3(game_id="0022300001", return_parsed=False)
df = nba_v3_to_v2_pbp(pbp_v3, box_v3)
print(df.shape, df.columns)

# Pandas output

df_pd = nba_v3_to_v2_pbp(pbp_v3, box_v3, return_as_pandas=True)
print(type(df_pd))

# Pipeline next step (feed a pbpstats-style consumer)

df.filter(pl.col("event_type") == "1").select("player1_name", "player2_name")
```

### `nba_war(ratings: 'pl.DataFrame', poss: 'pl.DataFrame', *, replacement_level: 'float', pts_per_win: 'float', rating_col: 'str' = 'rating', poss_col: 'str' = 'poss', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nba_war}

Points-above-replacement -> wins for each player.

`war_i = (rating_i - replacement_level) * poss_i / 100 / pts_per_win`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Per-player rating frame with `player_id` and `rating_col` (e.g. `nba_rapm`'s `rapm` column renamed, a `nba_ratings_panel` row filtered to one date, or `nba_bpm`'s `bpm` column). |
| `poss` | `DataFrame` |  | Per-player possession-count frame with `player_id` and `poss_col` (e.g. `off_poss + def_poss` from `nba_rapm`). |
| `replacement_level` | `float` |  | Per-100-possession rating of a replacement-level player. No built-in default — calibrate via `calibrate_replacement_level`. |
| `pts_per_win` | `float` |  | Points of season point-margin per marginal win. No built-in default — calibrate via `calibrate_pts_per_win`. |
| `rating_col` | `str` | `'rating'` | Column in `ratings` to score. |
| `poss_col` | `str` | `'poss'` | Column in `poss` giving total possessions played. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Frame with `WAR_SCHEMA` columns (`player_id`, `war`). Empty (that schema) when either input is empty.

**Example**

```python
from sportsdataverse.nba.nba_war import nba_war
war = nba_war(rapm_df.rename({"rapm": "rating"}), poss_df,
               replacement_level=-2.0, pts_per_win=250.0)
print(war.sort("war", descending=True).head())

# Derive both required kwargs from real data first

from sportsdataverse.nba.nba_war import (
    calibrate_pts_per_win, calibrate_replacement_level, nba_war,
)
pts_per_win = calibrate_pts_per_win(team_standings)
repl = calibrate_replacement_level(
    ratings, poss, pts_per_win=pts_per_win, target_total_war=300.0,
)
war = nba_war(ratings, poss, replacement_level=repl, pts_per_win=pts_per_win)
```

### `normalize_player_name(name: 'str') -> 'str'` {#normalize_player_name}

Fold a player display name to a join-safe key.

Lower-cases, strips diacritics (`"Jokić"` -> `"jokic"` -- the real
stats.nba.com feed spells Nikola Jokic's name with the Serbian `ć`,
while the DARKO/D&T CSVs use plain ASCII), drops periods/apostrophes/
hyphens, collapses internal whitespace, and strips a trailing
Jr./Sr./II/III/IV suffix. Two names normalize equal iff they refer to
the same join key under this scheme -- it is NOT guaranteed globally
unique (rare true duplicate full names are a known, accepted residual;
`external_validity`'s `coverage_pct` surfaces the effect rather
than hiding it).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | A raw display name, e.g. `"Nikola Jokić"` or `"A.J. Green"`. |

**Returns**

The normalized key, e.g. `"nikola jokic"`, `"aj green"`. Empty string in, empty string out (never raises).

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import normalize_player_name
assert normalize_player_name("Nikola Jokić") == normalize_player_name("Nikola Jokic")
assert normalize_player_name("Gary Trent Jr.") == normalize_player_name("Gary Trent")
```

### `player_play_context(possessions: 'pl.DataFrame', *, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#player_play_context}

Per-player offensive On/Off Play-Context table (CTG's On/Off page, offense half).

For each player: their team's offensive play-context **with them on the floor**
(`on_*`), **without them** (`off_*`), and the on-minus-off difference
(`diff_*`) — which is the number CTG actually displays.

The OFF side is derived by **subtraction** (team total minus on-court), not by a
second scan. That is deliberate: it makes the partition exact by construction —
`on_poss + off_poss == team_poss` and the same for points — so a leak (a
double-counted possession, a dropped lineup slot) is impossible to hide. The
test suite asserts that identity directly.

Like CTG's on/off, this is a **raw** split: no luck adjustment, no opponent
adjustment, no minutes threshold. It is a descriptive difference, not a causal
estimate — for that, use the RAPM surface
(`~sportsdataverse.nba.nba_rapm.nba_rapm`).

Requires `off_player_1..5` from
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context` **with lineups attached**. |
| `league_non_transition_ppp` | `Optional[float]` | `None` | Pts+/Poss baseline; see `team_play_context`. One baseline is shared across the on and off sides so the diffs are comparable. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time / heave / non-counting possessions first. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player, team) with `PLAYER_PLAY_CONTEXT_SCHEMA`. Empty input returns a zero-row frame with that schema.

**Example**

```python
poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
onoff = player_play_context(poss)
print(onoff.sort("diff_pts_per_100", descending=True).head())

# Who makes their team run?

print(onoff.sort("diff_transition_freq", descending=True).head())
```

### `player_rates(box_logs: 'pl.DataFrame') -> 'pl.DataFrame'` {#player_rates}

Per-player per-minute rate stats from box logs.

Rows with null minutes (DNPs) are dropped. Rate = total stat / total
minutes across the player's games; `minutes_pg` is the mean minutes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_logs` | `DataFrame` |  | Per-player-per-game frame with `player_id, team_id, minutes, pts, reb, ast, fg3m`. |

**Returns**

One row per player: `player_id, team_id, games, minutes_pg, pts_per_min, reb_per_min, ast_per_min, fg3m_per_min`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_player_props import player_rates
rates = player_rates(box_logs)
```

### `players_on_court_from_pbp(enhanced_pbp: 'pl.DataFrame', raw_box: 'dict', *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'` {#players_on_court_from_pbp}

Reconstruct the 5-on-5 on-court lineup from pbp subs + boxscore starters.

Pure function (no network). A gamerotation-free alternative to
`players_on_court_from_rotation` returning the identical
`LINEUPS_SCHEMA` frame (one row per action, slots sorted ascending or
`None`). See the module design for the algorithm.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `enhanced_pbp_from_payload`. Must carry `game_id`, `action_number`, `order_index`, `period`, `team_id`, `person_id`, `description`, `is_substitution`. |
| `raw_box` | `dict` |  | Raw `boxscoretraditionalv3` dict (starters + name map). |
| `home_team_id` | `int` |  | Home team id (from `boxscore_home_away`). |
| `away_team_id` | `int` |  | Away team id (from `boxscore_home_away`). |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA`. Empty input returns a zero-row frame (never raises).

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, players_on_court_from_pbp,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
oc = players_on_court_from_pbp(enh, box, home_team_id=home, away_team_id=away)
print(oc.shape)
```

### `players_on_court_from_quarter_boxscores(enhanced_pbp: 'pl.DataFrame', period_boxscores: 'Dict[int, dict]', raw_box: 'Optional[dict]' = None, *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'` {#players_on_court_from_quarter_boxscores}

Reconstruct the 5-on-5 on-court lineup, seeding each period exactly where possible.

A structural sibling of `players_on_court_from_pbp` — same
`LINEUPS_SCHEMA` output, same sub-batching walk / ffill-bfill / ascending
sort tail — whose only difference is *how each period is seeded*: when
that period's range-boxscore (period_box_oncourt`) narrows to
exactly 5 on-court candidates for a team, the period is seeded EXACTLY
from it; otherwise it falls back to the same gamerotation-free
first-appearance inference `players_on_court_from_pbp` uses
(period_starters`, carrying the prior period's ending lineup as
the silent-starter fallback). See period_box_oncourt` for the
narrowing recipe (empirically re-derived against pbpstats'
`StartOfPeriod._get_starters_from_boxscore_request` — see that
function's docstring for the concrete evidence behind its zero-sentinel
polarity).

Substitution name resolution merges up to three sources via
merge_name_maps`: name_map_from_period_boxes` (the union
of every period's range-box roster), name_map_from_pbp_actors`
(every row's own actor identity — covers bench players who never touch a
period boundary but do record at least one action), and — when the
caller supplies it — boxscore_name_map` over the full-game
`raw_box` payload, the SAME full-roster source
`players_on_court_from_pbp` uses. That third source is what fixes
the one residual name-resolution gap the first two cannot cover: a
player who is subbed in and then records **zero** further pbp actions for
the rest of the game (so never appears in name_map_from_pbp_actors`)
and never happens to be on court at an exact period-opening tick (so
never appears in name_map_from_period_boxes`) is still present in the
full-game boxscore roster — which lists every player on both teams
regardless of playing time — and therefore still resolvable. Passing
`raw_box` is optional (`None` preserves the pre-existing two-source
behavior) but strongly recommended: without it this producer's per-game
agreement with the gamerotation oracle can regress well below
`players_on_court_from_pbp`'s own floor on a fixture with a
late, stat-less bench appearance (see
`tests/nba/test_nba_lineups.py::test_quarter_box_agreement_floors`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. Must carry `game_id`, `action_number`, `order_index`, `period`, `team_id`, `person_id`, `player_name`, `player_name_i`, `description`, `is_substitution`. |
| `period_boxscores` | `Dict[int, dict]` |  | `{period: raw_boxscoretraditionalv3_range_payload}` — one entry per period, captured at that period's period_start_range` window. A missing period key falls back to pbp seeding for that period only (never raises). |
| `raw_box` | `Optional[dict]` | `None` | Optional raw full-game `boxscoretraditionalv3` payload (the same one `players_on_court_from_pbp` and `boxscore_home_away` consume) — supplies the full-roster name map described above. `None` (default) falls back to resolving names from `period_boxscores` + pbp actors only. |
| `home_team_id` | `int` |  | Home team id (from `boxscore_home_away`). |
| `away_team_id` | `int` |  | Away team id (from `boxscore_home_away`). |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA`. Empty `enhanced_pbp` returns a zero-row frame (never raises).

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, players_on_court_from_quarter_boxscores,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
periods = json.loads(pathlib.Path("boxv3_periods.json").read_text())
period_boxscores = {int(k): v for k, v in periods.items()}
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
oc = players_on_court_from_quarter_boxscores(
    enh, period_boxscores, box, home_team_id=home, away_team_id=away
)
print(oc.shape)
```

### `players_on_court_from_rotation(enhanced_pbp: 'pl.DataFrame', rotation: 'dict[str, list[dict]]', *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'` {#players_on_court_from_rotation}

Reconstruct the 5-on-5 on-court lineup via the rotation (gamerotation) algorithm.

Pure function — no network calls.  Port of hoopR's `.players_on_court_v3()`
(R/nba_stats_pbp.R lines 857-1041).

The rotation dict may use either `"HomeTeam"`/`"AwayTeam"` or
`"homeTeam"`/`"awayTeam"` as keys — both are accepted.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. Must contain `game_id`, `action_number`, `period`, `seconds_remaining`, `is_substitution`, and `team_id`. |
| `rotation` | `dict[str, list[dict]]` |  | Parsed rotation dict, typically from `parse_rotation_resultsets`. Each team's list contains stint dicts with numeric `PERSON_ID`, `IN_TIME_REAL`, `OUT_TIME_REAL`. |
| `home_team_id` | `int` |  | Integer team ID of the home team. |
| `away_team_id` | `int` |  | Integer team ID of the away team. |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA` with one row per action in *enhanced_pbp* (same row count, same ordering). Never raises — empty/malformed rotation returns a zero-row frame.

**Example**

```python
import json, pathlib
import polars as pl
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, parse_rotation_resultsets,
    players_on_court_from_rotation,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
rot = json.loads(pathlib.Path("gamerotation.json").read_text())
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
rotation = parse_rotation_resultsets(rot)
df = players_on_court_from_rotation(
    enh, rotation, home_team_id=home, away_team_id=away
)
print(df.shape)
```

### `predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'` {#predict_margin}

Expected home-minus-away margin from two adjusted net ratings.

The AdjNet difference (points/100 possessions) is scaled by the
matchup's `expected_possessions` before the home-court advantage
is added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  | Home team's adjusted net rating (`adj_net_rtg`). |
| `away_net` | `float` |  | Away team's adjusted net rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted HFA. |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_margin
predict_margin(10.0, -2.0, home_pace=100.0, away_pace=98.0, neutral=False)
```

### `predict_total(home_off: 'float', home_def: 'float', away_off: 'float', away_def: 'float', home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'` {#predict_total}

Expected total points from adjusted ratings and paces.

Expected possessions come from `expected_possessions`; each side's
expected points per 100 possessions blend its offense with the
opponent's defense (`0.5 * (off + opp_def)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  | Home adjusted offensive rating (points/100 poss). |
| `home_def` | `float` |  | Home adjusted defensive rating. |
| `away_off` | `float` |  | Away adjusted offensive rating. |
| `away_def` | `float` |  | Away adjusted defensive rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the pace anchor. |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_total
predict_total(118.0, 108.0, 110.0, 112.0, 100.0, 98.0)
```

### `prob_over(exp_value: 'float', line: 'float', stat: 'str', *, league_id: 'str' = '00') -> 'float'` {#prob_over}

Probability a stat finishes strictly above `line`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_value` | `float` |  | Projected mean of the stat. |
| `line` | `float` |  | The prop line. |
| `stat` | `str` |  | One of `"pts"`, `"reb"`, `"ast"`, `"fg3m"`. |
| `league_id` | `str` | `'00'` | Accepted for parity. |

**Returns**

`P(stat > line)` in `[0, 1]`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import prob_over
prob_over(24.0, 22.5, "pts")
```

### `project_player_line(rate_row: 'dict[str, Any]', exp_minutes: 'float', pace_factor: 'float' = 1.0) -> 'dict[str, float]'` {#project_player_line}

Project a player's expected counting line from per-minute rates.

`exp_stat = rate_per_min * exp_minutes * pace_factor` -- counting stats
scale with both projected minutes and pace.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rate_row` | `dict[str, Any]` |  | One row of `player_rates` (as a dict). |
| `exp_minutes` | `float` |  | Projected minutes for the game. |
| `pace_factor` | `float` | `1.0` | Pace multiplier (`exp_poss / avg_pace`); `1.0` for a league-average-pace matchup. |

**Returns**

`{"exp_pts", "exp_reb", "exp_ast", "exp_fg3m"}`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import player_rates, project_player_line
r = player_rates(box_logs).row(0, named=True)
line = project_player_line(r, exp_minutes=32.0, pace_factor=1.02)
```

### `prop_distribution(exp_value: 'float', stat: 'str', *, league_id: 'str' = '00') -> 'tuple[str, dict[str, float]]'` {#prop_distribution}

Distribution family + parameters for a projected stat mean.

Points -> Normal `(mu, sd)` with `sd = a + b*sqrt(mu)`; counts
(reb/ast/fg3m) -> Negative-Binomial `(r, p)` matching mean `mu` and
variance `dispersion*mu` (Poisson if dispersion <= 1).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_value` | `float` |  | Projected mean of the stat. |
| `stat` | `str` |  | One of `"pts"`, `"reb"`, `"ast"`, `"fg3m"`. |
| `league_id` | `str` | `'00'` | Accepted for parity (dispersion is currently league-shared). |

**Returns**

`(family, params)` where family is `"normal"`, `"nbinom"` or `"poisson"`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import prop_distribution
fam, par = prop_distribution(24.0, "pts")
```

### `ratings_as_of(model: 'AnyModel', possessions: 'pl.DataFrame', asof: 'datetime.date') -> 'RatingsFit'` {#ratings_as_of}

Fit `model` on every possession dated on or before `asof` and return ratings.

This is the through-date primitive: possessions with `game_date > asof`
are excluded from the fit entirely (never merely down-weighted), which is
what makes the panel built from repeated calls to this function leakage-free
by construction — see `tests/nba/test_nba_ratings_panel.py::test_ratings_as_of_is_leakage_free_append_invariant`.
NOTE: the leakage property is proven by the append-invariance test TOGETHER
with the panel's per-date-parity test — neither alone covers
cross-checkpoint-window leaks; do not prune one without the other.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel` (a `RapmModel`, `RatingsModel`, or `PriorModel`). |
| `possessions` | `DataFrame` |  | A possession+lineup frame that MUST carry a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `asof` | `date` |  | The through-date checkpoint (inclusive). |

**Returns**

`RatingsFit` with per-player offense/defense ratings (per-100-possession scale, same sign convention as `nba_rapm`: positive `d_ratings` means good defense). Empty dicts when no possessions fall on or before `asof` or when `possessions` is empty.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import ratings_as_of

rf = ratings_as_of(RidgeRapmModel(), season_poss, datetime.date(2023, 12, 1))
print(rf.o_ratings[201939])   # per-100 offensive rating through Dec 1
```

### `raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'` {#raw_game_efficiency}

Per-team, per-game possessions + raw offensive/defensive rating.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team box score with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_rtg, def_rtg`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_schedule, load_nba_team_boxscore
from sportsdataverse.nba.nba_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_nba_schedule([2024]), load_nba_team_boxscore([2024]))
```

### `render_report(report: 'ValidationReport') -> 'str'` {#render_report}

Render a `ValidationReport` as a human-readable markdown validation card.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `report` | `ValidationReport` |  | A populated `ValidationReport` from `validate_model`. |

**Returns**

A multi-section markdown string with one `##` heading per oracle. Sections whose oracle result is `None` (either skipped or not applicable for a point-estimate model) are rendered as `- n/a`.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model, render_report,
)

rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
md = render_report(rep)
print(md)

# Capture the markdown string for downstream use

with open("validation_card.md", "w") as f:
    f.write(render_report(rep))
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

Internal helper that flattens an ESPN NBA scoreboard event dict into a

shape suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN NBA scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.nba import espn_nba_schedule
sched = espn_nba_schedule(dates=20230102)
```

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

### `shrink_clutch(delta: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'pl.DataFrame'` {#shrink_clutch}

Empirical-Bayes / James-Stein shrinkage of `clutch_delta` toward zero.

Per-team sampling variance is `σ²_i = scale / clutch_poss` (small samples
shrink harder); the between-team signal variance `τ²` is the observed
variance of `clutch_delta` net of mean sampling variance; the shrink
factor `k_i = τ² / (τ² + σ²_i)` and `clutch_skill_shrunk = k_i · delta_i`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `delta` | `DataFrame` |  | Output of `clutch_delta` (needs `clutch_delta` + `clutch_poss`). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` (accepted for parity; the scale is currently league-shared). |

**Returns**

`delta` with an added `clutch_skill_shrunk` column. Empty input returns the input schema plus that column.

**Example**

```python
from sportsdataverse.nba.nba_clutch import clutch_delta, shrink_clutch
skill = shrink_clutch(clutch_delta(clutch_frame, baseline_frame))
```

### `starters_on_court_counts(possessions: 'pl.DataFrame', starters: 'dict[int, list[int]]') -> 'dict[int, int]'` {#starters_on_court_counts}

Count, per possession, how many **starters** are on the floor across BOTH teams.

This supplies the second half of CTG's garbage-time rule — "there have to be
**two or fewer starters on the floor combined between the two teams**" — which
`flag_garbage_time` cannot evaluate on its own (the possession frame does
not carry who is on the floor).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession frame with the ten on-court columns `off_player_1..5` **and** `def_player_1..5` (from `~sportsdataverse.nba.nba_possessions.attach_possession_lineups`). |
| `starters` | `dict[int, list[int]]` |  | `{team_id: [player_id, ...]}` — e.g. from `~sportsdataverse.nba.nba_lineups._starters_from_boxscore_v3`. Player ids are matched across both teams' starting fives, so the offense/defense split of the lineup columns does not matter. |

**Returns**

`{possession_number: starters_on_floor}`, each value in `0..10`. An **empty** *starters* map yields all-zero counts, which would make CTG's `<= 2` clause vacuously true and flag every margin-qualifying possession. The counts are reported honestly rather than guessed — do not pass an empty map and then read the result as CTG-exact.

**Example**

```python
counts = starters_on_court_counts(poss, _starters_from_boxscore_v3(box))
print(max(counts.values()))  # 10 at the opening tip
```

### `team_pace_projection(home_team_id: 'str', away_team_id: 'str', ratings: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'float'` {#team_pace_projection}

Expected possessions for a matchup (Phase-3 `expected_possessions`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_team_id` | `str` |  | Home team id (matched against `ratings['team_id']`). |
| `away_team_id` | `str` |  | Away team id. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_pace`. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |

**Returns**

Expected possessions for the game.

**Example**

```python
from sportsdataverse.nba.nba_player_props import team_pace_projection
poss = team_pace_projection("1", "2", ratings)
```

### `team_play_context(possessions: 'pl.DataFrame', *, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#team_play_context}

Roll possessions up into CTG's team Play-Context table.

Reproduces the offensive half of CTG's `/stats/league/context` page.

Columns: `poss`, `points`, `pts_per_100`, `transition_poss`,
`transition_points`, `transition_freq`, `transition_pts_per_100`
(CTG's "Eff"), `non_transition_pts_per_100`, `transition_pts_added_per_100`
(CTG's "Pts+/Poss"), plus `halfcourt_*` twins and per-source transition
frequencies (`freq_off_steal` / `freq_off_live_rebound`).

**Pts+/Poss** is the subtle one. CTG: "CTG takes a team's points per
possession that starts with transition, and subtracts out **what an average
team does** in a possession that did not start with transition. ... We take the

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context`. |
| `league_non_transition_ppp` | `Optional[float]` | `None` | League-average points per 100 possessions on non-transition-start possessions. Computed from the frame when omitted. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time, heave and non-counting possessions first (CTG's default view). Set `False` for the unfiltered totals. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `offense_team_id`. Empty input returns a zero-row frame.

**Example**

```python
ctx = team_play_context(add_play_context(pbp))
print(ctx.select("offense_team_id", "transition_freq", "transition_pts_added_per_100"))

# Season-comparable Pts+/Poss

ctx = team_play_context(season_poss, league_non_transition_ppp=104.8)
```

### `train_spm(box_features: 'pl.DataFrame', rapm_target: 'pl.DataFrame', *, feature_names: 'Optional[List[str]]' = None, alpha: 'float' = 100.0) -> 'SpmCoefficients'` {#train_spm}

Ridge-fit box features onto `o_rapm` and `d_rapm` (two regressions).

The two models share the same feature matrix but separate target vectors,
producing independent offense and defense coefficient vectors.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id` and every column in *feature_names*. |
| `rapm_target` | `DataFrame` |  | Per-player RAPM target frame with columns `player_id`, `o_rapm`, and `d_rapm`. Only the rows whose `player_id` appears in *box_features* are used (inner join). |
| `feature_names` | `Optional[List[str]]` | `None` | Ordered list of feature columns to regress on. Defaults to `SPM_FEATURES` (= STATS` from `nba_box_logs`). |
| `alpha` | `float` | `100.0` | Ridge regularization strength (`sklearn.linear_model.Ridge`). Lower values approach OLS; higher values shrink toward zero. |

**Returns**

`SpmCoefficients` with offense and defense coefficient vectors, intercepts, and the ordered `feature_names`.

**Example**

```python
from sportsdataverse.nba import train_spm
coef = train_spm(box_feats, rapm_ratings)

# With custom regularization

coef = train_spm(box_feats, rapm_ratings, alpha=50.0)
```

### `validate_model(model: 'AnyModel', season_frames: 'List[pl.DataFrame]', *, model_name: 'str' = 'model', oracles: 'Tuple[str, ...]' = ('retrodiction', 'reliability', 'cross_season', 'calibration'), seed: 'int' = 0, external_ratings: 'Optional[pl.DataFrame]' = None, external_oracle: 'Optional[pl.DataFrame]' = None, external_rating_col: 'str' = 'rating', external_oracle_col: 'str' = 'oracle_value', external_join: 'str' = 'id', walk_forward_horizon_days: 'int' = 14, walk_forward_min_games: 'int' = 15) -> 'ValidationReport'` {#validate_model}

Run the selected oracles and assemble a `ValidationReport`.

`retrodiction`/`reliability`/`calibration` run on the pooled possessions
(all seasons concatenated); `cross_season` runs on the ordered per-season
frames. Any oracle not selected is left `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A fitted or unfitted RAPM-family estimator (`fit(X, y)` protocol). |
| `season_frames` | `List[DataFrame]` |  | Ordered list of per-season possession frames. All frames are concatenated into a single pooled frame for Oracles 1, 2, and 4. |
| `model_name` | `str` | `'model'` | Label written into the returned report and markdown card. |
| `oracles` | `Tuple[str, ...]` | `('retrodiction', 'reliability', 'cross_season', 'calibration')` | Tuple of oracle names to run. Omit a name to skip that oracle and leave its result field `None`. Accepts `"external"` and `"walk_forward"` in addition to the four original names; the default tuple is unchanged, so existing callers are unaffected. |
| `seed` | `int` | `0` | RNG seed forwarded to each oracle for determinism. |
| `external_ratings` | `Optional[DataFrame]` | `None` | The model's own ratings frame -- required when `"external"` is in `oracles`. |
| `external_oracle` | `Optional[DataFrame]` | `None` | A loaded oracle frame (from `nba_oracle_data`) -- required when `"external"` is in `oracles`. |
| `external_rating_col` | `str` | `'rating'` | Rating column name in `external_ratings`. |
| `external_oracle_col` | `str` | `'oracle_value'` | Value column name in `external_oracle`. |
| `external_join` | `str` | `'id'` | `"id"` or `"name"`, forwarded to `external_validity`. |
| `walk_forward_horizon_days` | `int` | `14` | Forwarded to `walk_forward`. |
| `walk_forward_min_games` | `int` | `15` | Forwarded to `walk_forward` as `min_games_before_first_checkpoint`. |

**Returns**

A `ValidationReport` whose fields are populated for every selected oracle and `None` for every skipped oracle.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model,
)

# season_frames is a list[pl.DataFrame] of possession stints
rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
print(rep.retrodiction.game_margin_rmse)   # out-of-sample margin RMSE
print(rep.reliability.spearman_brown)      # split-half Spearman-Brown
print(rep.calibration)                     # None — RidgeRapmModel has no posterior

# Skip slow oracles when iterating quickly

rep = validate_model(
    RidgeRapmModel(), season_frames,
    oracles=("retrodiction", "reliability"),
)
print(rep.cross_season)   # None — not selected
```

### `walk_forward(model: 'AnyModel', possessions: 'pl.DataFrame', *, checkpoint_dates: 'Optional[List[datetime.date]]' = None, horizon_days: 'int' = 14, min_games_before_first_checkpoint: 'int' = 15) -> 'WalkForwardResult'` {#walk_forward}

Oracle 6: time-ordered "predict tomorrow" retrodiction.

For each checkpoint date D: fit on games with `game_date <= D`, predict
games with `D < game_date <= D + horizon_days`, aggregate to
per-(game, team) margins -- reusing fit_on` / design_with_ids`
/ `predict_points` / team_game_margins` (the same machinery
`retrodiction` uses). `carry_forward_rmse` reapplies the PREVIOUS
checkpoint's fit (no refit) to the current window. `random_fold_rmse` is
`retrodiction`'s pooled game-margin RMSE on the same possessions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model (`RapmModel`/`RatingsModel`/`PriorModel`). |
| `possessions` | `DataFrame` |  | A season possession+lineup frame with `game_date` (from `compile_nba_season`), `game_id`, `offense_team_id`, `points`, and the ten lineup columns. |
| `checkpoint_dates` | `Optional[List[date]]` | `None` | Explicit checkpoint grid; derived from `possessions` via `horizon_days`/`min_games_before_first_checkpoint` when `None` (default). |
| `horizon_days` | `int` | `14` | Days-ahead prediction window per checkpoint (default 14). |
| `min_games_before_first_checkpoint` | `int` | `15` | Distinct-game-date index of the first checkpoint when deriving the default grid (default 15, "~game 15 of the season"). |

**Returns**

`WalkForwardResult`. All metrics `nan` and counts `0` when `possessions` is empty, lacks a `game_date` column, or the derived/given grid produces zero non-degenerate checkpoints.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel, walk_forward
res = walk_forward(RidgeRapmModel(), season_possessions)
print(res.game_margin_rmse, res.carry_forward_rmse, res.random_fold_rmse)
```

### `win_prob_from_margin(exp_margin: 'float', *, league_id: 'str' = '00') -> 'float'` {#win_prob_from_margin}

Home win probability from an expected margin (normal-CDF closed form).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted margin sigma. |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
```

### `xpoints_baseline(league_avgs: 'pl.DataFrame') -> 'pl.DataFrame'` {#xpoints_baseline}

League-average FG% baseline table keyed by the three shot-zone columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_avgs` | `DataFrame` |  | The `LeagueAverages` result set from `nba_stats_shotchartdetail` (`shot_zone_basic` / `shot_zone_area` / `shot_zone_range` / `fga` / `fgm` / `fg_pct`). |

**Returns**

One row per `(shot_zone_basic, shot_zone_area, shot_zone_range)`: `... base_fg_pct:Float64, is_three:Boolean` (`is_three` = the basic zone names a three). Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.nba import nba_stats
from sportsdataverse.nba.nba_shot_value import xpoints_baseline
base = xpoints_baseline(league_avgs)
```

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
