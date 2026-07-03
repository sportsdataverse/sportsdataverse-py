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

Convert a season-end year (e.g. 2024) to the NBA's hyphenated label

(e.g. `"2023-24"`).

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

### `ForecastResult(forecast_rmse: 'float', forecast_corr: 'float', baseline_rmse: 'float', n_forecasts: 'int') -> None` {#ForecastResult}

Forecast-accuracy metrics: predicted-vs-actual next-season rating over held-out transitions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `forecast_rmse` | `float` |  |  |
| `forecast_corr` | `float` |  |  |
| `baseline_rmse` | `float` |  |  |
| `n_forecasts` | `int` |  |  |

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

### `ValidationReport(model_name: 'str', n_seasons: 'int', retrodiction: 'Optional[RetrodictionResult]' = None, reliability: 'Optional[ReliabilityResult]' = None, cross_season: 'Optional[CrossSeasonResult]' = None, calibration: 'Optional[CalibrationResult]' = None) -> None` {#ValidationReport}

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

### `compile_nba_season(season: 'int', season_type: 'str' = 'Regular Season', *, resume: 'bool' = True, cache_dir: 'Optional[str]' = None, delay_s: 'float' = 0.6, lineup_source: 'str' = 'auto', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#compile_nba_season}

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
| `season` | `int` |  | Season start year (e.g. 2023 for 2023-24). |
| `season_type` | `str` | `'Regular Season'` | `"Regular Season"` (default) or `"Playoffs"`. |
| `resume` | `bool` | `True` | Reuse per-game cached parquet when present. |
| `cache_dir` | `Optional[str]` | `None` | Cache root; defaults to `SDV_PY_NBA_CACHE_DIR` or `~/.sdv_py_nba_cache/possessions`. |
| `delay_s` | `float` | `0.6` | Seconds to sleep after each live fetch (rate-limit throttle). |
| `lineup_source` | `str` | `'auto'` | Which on-court lineup producer to use — `"auto"` (default; tries rotation then falls back to pbp), `"rotation"` (gamerotation endpoint only), or `"pbp"` (pbp-derived, no gamerotation fetch — useful when the gamerotation endpoint is throttled or unavailable). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

The season possession frame (+ `season` and `game_date` cols). Empty typed frame if no games.

**Example**

```python
from sportsdataverse.nba.nba_season_compile import compile_nba_season

poss = compile_nba_season(2023)
print(poss.shape)          # (n_possessions, n_cols)
print(poss["season"][0])   # 2023

# Resume a partially completed run and return as pandas

poss_pd = compile_nba_season(2023, resume=True, return_as_pandas=True)
print(type(poss_pd))       # <class 'pandas.core.frame.DataFrame'>

# Compile Playoffs with a custom cache directory

poss = compile_nba_season(
    2023,
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

<per-player-game logs>, "team": <per-team-game logs>}`` as snake-cased polars frames.

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

frame with `player_id`, `obpm`, `dbpm`, `bpm`, `min`, `gp` (Int64 player_id/gp, Float64 obpm/dbpm/bpm/min). `"game"`: the same columns prefixed with `game_id` (Utf8), one row per (game_id, player_id). Empty (that schema) input -> zero-row frame with the same schema; never raises on empty.

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

### `ratings_as_of(model: 'AnyModel', possessions: 'pl.DataFrame', asof: 'datetime.date') -> 'RatingsFit'` {#ratings_as_of}

Fit `model` on every possession dated on or before `asof` and return ratings.

This is the through-date primitive: possessions with `game_date > asof`
are excluded from the fit entirely (never merely down-weighted), which is
what makes the panel built from repeated calls to this function leakage-free
by construction — see `tests/nba/test_nba_ratings_panel.py::test_ratings_as_of_is_leakage_free_append_invariant`.

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

### `validate_model(model: 'AnyModel', season_frames: 'List[pl.DataFrame]', *, model_name: 'str' = 'model', oracles: 'Tuple[str, ...]' = ('retrodiction', 'reliability', 'cross_season', 'calibration'), seed: 'int' = 0) -> 'ValidationReport'` {#validate_model}

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
| `oracles` | `Tuple[str, ...]` | `('retrodiction', 'reliability', 'cross_season', 'calibration')` | Tuple of oracle names to run. Omit a name to skip that oracle and leave its result field `None`. |
| `seed` | `int` | `0` | RNG seed forwarded to each oracle for determinism. |

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
