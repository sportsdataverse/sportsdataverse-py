---
title: NFL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NFL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nfl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nfl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

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
| `athlete_id` | integer |  |
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `full_name` | character |  |
| `athlete_display_name` | character |  |
| `short_name` | character |  |
| `weight` | double |  |
| `display_weight` | character |  |
| `height` | double |  |
| `display_height` | character |  |
| `age` | integer |  |
| `date_of_birth` | character |  |
| `debut_year` | integer |  |
| `slug` | character |  |
| `jersey` | character |  |
| `linked` | logical |  |
| `active` | logical |  |
| `alternate_ids_sdr` | character |  |
| `birth_place_city` | character |  |
| `birth_place_state` | character |  |
| `birth_place_country` | character |  |
| `headshot_href` | character |  |
| `headshot_alt` | character |  |
| `projections_href` | character |  |
| `contracts_href` | character |  |
| `experience_years` | integer |  |
| `college_athlete_href` | character |  |
| `contract_href` | character |  |
| `contract_option_type` | integer |  |
| `contract_salary` | integer |  |
| `contract_bonus` | integer |  |
| `contract_years_remaining` | integer |  |
| `contract_signed_through` | integer |  |
| `contract_season_href` | character |  |
| `contract_team_href` | character |  |
| `contract_active` | logical |  |
| `status_id` | character |  |
| `status_name` | character |  |
| `status_type` | character |  |
| `status_abbreviation` | character |  |
| `contract_salary_remaining` | integer |  |
| `draft_display_text` | character |  |
| `draft_round` | integer |  |
| `draft_year` | integer |  |
| `draft_selection` | integer |  |
| `draft_team_href` | character |  |
| `draft_pick_href` | character |  |
| `hand_type` | character |  |
| `hand_abbreviation` | character |  |
| `hand_display_value` | character |  |
| `starter` | logical |  |
| `jersey_right` | character |  |
| `valid` | logical |  |
| `did_not_play` | logical |  |
| `display_name` | character |  |
| `athlete_href` | character |  |
| `position_href` | character |  |
| `statistics_href` | character |  |
| `team_id` | integer |  |
| `order` | integer |  |
| `home_away` | character |  |
| `winner` | logical |  |
| `team_guid` | character |  |
| `team_uid` | character |  |
| `team_slug` | character |  |
| `team_location` | character |  |
| `team_name` | character |  |
| `team_nickname` | character |  |
| `team_abbreviation` | character |  |
| `team_display_name` | character |  |
| `team_short_display_name` | character |  |
| `team_color` | character |  |
| `team_alternate_color` | character |  |
| `is_active` | logical |  |
| `is_all_star` | logical |  |
| `team_alternate_ids_sdr` | character |  |
| `logo_href` | character |  |
| `logo_dark_href` | character |  |
| `game_id` | integer |  |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_game_rosters
rosters = espn_nfl_game_rosters(game_id=401220403)
rosters.shape

Pandas round-trip with home/away split::

rosters_pd = espn_nfl_game_rosters(game_id=401220403, return_as_pandas=True)
home = rosters_pd[rosters_pd["home_away"] == "home"]
away = rosters_pd[rosters_pd["home_away"] == "away"]
```

### `espn_nfl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NFL athlete's ESPN **season** stat line as one wide row.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns (for football: ``passing_*``, ``rushing_*``, ``receiving_*``, ``scoring_*``, ...), the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters. For the richer multi-category web-v3 payload use :func:`sportsdataverse.nfl.espn_nfl_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NFL athlete identifier (e.g. ``3139477`` for Patrick Mahomes). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | ``"regular"`` (type 2) or ``"postseason"`` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When ``raw=True`` returns the raw statistics JSON ``dict``.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `total` | logical |  |
| `athlete_id` | integer |  |
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `full_name` | character |  |
| `display_name` | character |  |
| `short_name` | character |  |
| `weight` | double |  |
| `display_weight` | character |  |
| `height` | double |  |
| `display_height` | character |  |
| `age` | integer |  |
| `date_of_birth` | character |  |
| `jersey` | character |  |
| `slug` | character |  |
| `active` | logical |  |
| `position_id` | integer |  |
| `position_name` | character |  |
| `position_display_name` | character |  |
| `position_abbreviation` | character |  |
| `college_name` | character |  |
| `status_id` | integer |  |
| `status_name` | character |  |
| `general_fumbles` | double |  |
| `general_fumbles_lost` | double |  |
| `general_fumbles_forced` | double |  |
| `general_fumbles_forced_primary` | double |  |
| `general_fumbles_recovered` | double |  |
| `general_fumbles_recovered_yards` | double |  |
| `general_fumbles_touchdowns` | double |  |
| `general_games_played` | double |  |
| `general_offensive_two_pt_returns` | double |  |
| `general_offensive_fumbles_touchdowns` | double |  |
| `general_defensive_fumbles_touchdowns` | double |  |
| `passing_avg_gain` | double |  |
| `passing_completion_pct` | double |  |
| `passing_completions` | double |  |
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
| `passing_qbr` | double |  |
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
| `defensive_sacks` | double |  |
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
| `team_id` | integer |  |
| `team_uid` | character |  |
| `team_guid` | character |  |
| `team_slug` | character |  |
| `team_location` | character |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_display_name` | character |  |
| `team_short_display_name` | character |  |
| `team_color` | character |  |
| `team_alternate_color` | character |  |
| `team_is_active` | logical |  |
| `team_logo_href` | character |  |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_player_stats
df = espn_nfl_player_stats(athlete_id=3139477, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### `espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

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
| `id` | character |  |
| `uid` | character |  |
| `date` | character |  |
| `attendance` | integer |  |
| `time_valid` | logical |  |
| `neutral_site` | logical |  |
| `conference_competition` | logical |  |
| `play_by_play_available` | logical |  |
| `recent` | logical |  |
| `start_date` | character |  |
| `broadcast` | character |  |
| `highlights` | character |  |
| `notes_type` | character |  |
| `notes_headline` | character |  |
| `broadcast_market` | character |  |
| `broadcast_name` | character |  |
| `type_id` | character |  |
| `type_abbreviation` | character |  |
| `venue_id` | character |  |
| `venue_full_name` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_address_country` | character |  |
| `venue_indoor` | logical |  |
| `status_clock` | double |  |
| `status_display_clock` | character |  |
| `status_period` | integer |  |
| `status_type_id` | character |  |
| `status_type_name` | character |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `status_type_detail` | character |  |
| `status_type_short_detail` | character |  |
| `status_is_tbd_flex` | logical |  |
| `format_regulation_periods` | integer |  |
| `home_id` | character |  |
| `home_uid` | character |  |
| `home_location` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_short_display_name` | character |  |
| `home_color` | character |  |
| `home_alternate_color` | character |  |
| `home_is_active` | logical |  |
| `home_venue_id` | character |  |
| `home_logo` | character |  |
| `home_score` | character |  |
| `home_current_rank` | integer |  |
| `home_linescores` | integer |  |
| `home_records` | character |  |
| `away_id` | character |  |
| `away_uid` | character |  |
| `away_location` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_short_display_name` | character |  |
| `away_color` | character |  |
| `away_alternate_color` | character |  |
| `away_is_active` | logical |  |
| `away_venue_id` | character |  |
| `away_logo` | character |  |
| `away_score` | character |  |
| `away_current_rank` | integer |  |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer |  |
| `season` | integer |  |
| `season_type` | integer |  |
| `week` | integer |  |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_schedule
sched = espn_nfl_schedule(dates=20240908)

Specific week of regular season (``season_type=2``)::

wk1 = espn_nfl_schedule(dates=2024, week=1, season_type=2)

Pandas round-trip::

sched_pd = espn_nfl_schedule(dates=20240908, return_as_pandas=True)
```

## Dataset loaders

### `load_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `draft_year` | double |  |
| `draft_team` | character |  |
| `draft_round` | double |  |
| `draft_ovr` | double |  |
| `pfr_id` | character |  |
| `cfb_id` | character |  |
| `player_name` | character |  |
| `pos` | character |  |
| `school` | character |  |
| `ht` | character |  |
| `wt` | double |  |
| `forty` | double |  |
| `bench` | double |  |
| `vertical` | double |  |
| `broad_jump` | double |  |
| `cone` | double |  |
| `shuttle` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

Filter by draft year and position::

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character |  |
| `position` | character |  |
| `team` | character |  |
| `is_active` | logical |  |
| `year_signed` | integer |  |
| `years` | integer |  |
| `value` | double |  |
| `apy` | double |  |
| `guaranteed` | double |  |
| `apy_cap_pct` | double |  |
| `inflated_value` | double |  |
| `inflated_apy` | double |  |
| `inflated_guaranteed` | double |  |
| `player_page` | character |  |
| `otc_id` | integer |  |
| `gsis_id` | character |  |
| `date_of_birth` | character |  |
| `height` | character |  |
| `weight` | character |  |
| `college` | character |  |
| `draft_year` | integer |  |
| `draft_round` | integer |  |
| `draft_overall` | integer |  |
| `draft_team` | character |  |
| `cols` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

Pandas round-trip with sort by APY::

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `club_code` | character |  |
| `week` | integer |  |
| `game_type` | character |  |
| `depth_team` | character |  |
| `last_name` | character |  |
| `first_name` | character |  |
| `football_name` | character |  |
| `formation` | character |  |
| `gsis_id` | character |  |
| `jersey_number` | character |  |
| `position` | character |  |
| `elias_id` | character |  |
| `depth_position` | character |  |
| `full_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

Multi-season range::

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### `load_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `round` | integer |  |
| `pick` | integer |  |
| `team` | character |  |
| `gsis_id` | character |  |
| `pfr_player_id` | character |  |
| `cfb_player_id` | character |  |
| `pfr_player_name` | character |  |
| `hof` | logical |  |
| `position` | character |  |
| `category` | character |  |
| `side` | character |  |
| `college` | character |  |
| `age` | integer |  |
| `to` | integer |  |
| `allpro` | integer |  |
| `probowls` | integer |  |
| `seasons_started` | integer |  |
| `w_av` | integer |  |
| `car_av` | logical |  |
| `dr_av` | integer |  |
| `games` | integer |  |
| `pass_completions` | integer |  |
| `pass_attempts` | integer |  |
| `pass_yards` | integer |  |
| `pass_tds` | integer |  |
| `pass_ints` | integer |  |
| `rush_atts` | integer |  |
| `rush_yards` | integer |  |
| `rush_tds` | integer |  |
| `receptions` | integer |  |
| `rec_yards` | integer |  |
| `rec_tds` | integer |  |
| `def_solo_tackles` | integer |  |
| `def_ints` | integer |  |
| `def_sacks` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

Filter to a single year and round::

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | character |  |
| `posteam` | character |  |
| `week` | double |  |
| `game_id` | character |  |
| `player_id` | character |  |
| `full_name` | character |  |
| `position` | character |  |
| `pass_attempt` | double |  |
| `rec_attempt` | double |  |
| `rush_attempt` | double |  |
| `pass_air_yards` | double |  |
| `rec_air_yards` | double |  |
| `pass_completions` | double |  |
| `receptions` | double |  |
| `pass_completions_exp` | double |  |
| `receptions_exp` | double |  |
| `pass_yards_gained` | double |  |
| `rec_yards_gained` | double |  |
| `rush_yards_gained` | double |  |
| `pass_yards_gained_exp` | double |  |
| `rec_yards_gained_exp` | double |  |
| `rush_yards_gained_exp` | double |  |
| `pass_touchdown` | double |  |
| `rec_touchdown` | double |  |
| `rush_touchdown` | double |  |
| `pass_touchdown_exp` | double |  |
| `rec_touchdown_exp` | double |  |
| `rush_touchdown_exp` | double |  |
| `pass_two_point_conv` | double |  |
| `rec_two_point_conv` | double |  |
| `rush_two_point_conv` | double |  |
| `pass_two_point_conv_exp` | double |  |
| `rec_two_point_conv_exp` | double |  |
| `rush_two_point_conv_exp` | double |  |
| `pass_first_down` | double |  |
| `rec_first_down` | double |  |
| `rush_first_down` | double |  |
| `pass_first_down_exp` | double |  |
| `rec_first_down_exp` | double |  |
| `rush_first_down_exp` | double |  |
| `pass_interception` | double |  |
| `rec_interception` | double |  |
| `pass_interception_exp` | double |  |
| `rec_interception_exp` | double |  |
| `rec_fumble_lost` | double |  |
| `rush_fumble_lost` | double |  |
| `pass_fantasy_points_exp` | double |  |
| `rec_fantasy_points_exp` | double |  |
| `rush_fantasy_points_exp` | double |  |
| `pass_fantasy_points` | double |  |
| `rec_fantasy_points` | double |  |
| `rush_fantasy_points` | double |  |
| `total_yards_gained` | double |  |
| `total_yards_gained_exp` | double |  |
| `total_touchdown` | double |  |
| `total_touchdown_exp` | double |  |
| `total_first_down` | double |  |
| `total_first_down_exp` | double |  |
| `total_fantasy_points` | double |  |
| `total_fantasy_points_exp` | double |  |
| `pass_completions_diff` | double |  |
| `receptions_diff` | double |  |
| `pass_yards_gained_diff` | double |  |
| `rec_yards_gained_diff` | double |  |
| `rush_yards_gained_diff` | double |  |
| `pass_touchdown_diff` | double |  |
| `rec_touchdown_diff` | double |  |
| `rush_touchdown_diff` | double |  |
| `pass_two_point_conv_diff` | double |  |
| `rec_two_point_conv_diff` | double |  |
| `rush_two_point_conv_diff` | double |  |
| `pass_first_down_diff` | double |  |
| `rec_first_down_diff` | double |  |
| `rush_first_down_diff` | double |  |
| `pass_interception_diff` | double |  |
| `rec_interception_diff` | double |  |
| `pass_fantasy_points_diff` | double |  |
| `rec_fantasy_points_diff` | double |  |
| `rush_fantasy_points_diff` | double |  |
| `total_yards_gained_diff` | double |  |
| `total_touchdown_diff` | double |  |
| `total_first_down_diff` | double |  |
| `total_fantasy_points_diff` | double |  |
| `pass_attempt_team` | double |  |
| `rec_attempt_team` | double |  |
| `rush_attempt_team` | double |  |
| `pass_air_yards_team` | double |  |
| `rec_air_yards_team` | double |  |
| `pass_completions_team` | double |  |
| `receptions_team` | double |  |
| `pass_completions_exp_team` | double |  |
| `receptions_exp_team` | double |  |
| `pass_yards_gained_team` | double |  |
| `rec_yards_gained_team` | double |  |
| `rush_yards_gained_team` | double |  |
| `pass_yards_gained_exp_team` | double |  |
| `rec_yards_gained_exp_team` | double |  |
| `rush_yards_gained_exp_team` | double |  |
| `pass_touchdown_team` | double |  |
| `rec_touchdown_team` | double |  |
| `rush_touchdown_team` | double |  |
| `pass_touchdown_exp_team` | double |  |
| `rec_touchdown_exp_team` | double |  |
| `rush_touchdown_exp_team` | double |  |
| `pass_two_point_conv_team` | double |  |
| `rec_two_point_conv_team` | double |  |
| `rush_two_point_conv_team` | double |  |
| `pass_two_point_conv_exp_team` | double |  |
| `rec_two_point_conv_exp_team` | double |  |
| `rush_two_point_conv_exp_team` | double |  |
| `pass_first_down_team` | double |  |
| `rec_first_down_team` | double |  |
| `rush_first_down_team` | double |  |
| `pass_first_down_exp_team` | double |  |
| `rec_first_down_exp_team` | double |  |
| `rush_first_down_exp_team` | double |  |
| `pass_interception_team` | double |  |
| `rec_interception_team` | double |  |
| `pass_interception_exp_team` | double |  |
| `rec_interception_exp_team` | double |  |
| `rec_fumble_lost_team` | double |  |
| `rush_fumble_lost_team` | double |  |
| `pass_fantasy_points_exp_team` | double |  |
| `rec_fantasy_points_exp_team` | double |  |
| `rush_fantasy_points_exp_team` | double |  |
| `pass_fantasy_points_team` | double |  |
| `rec_fantasy_points_team` | double |  |
| `rush_fantasy_points_team` | double |  |
| `pass_completions_diff_team` | double |  |
| `receptions_diff_team` | double |  |
| `pass_yards_gained_diff_team` | double |  |
| `rec_yards_gained_diff_team` | double |  |
| `rush_yards_gained_diff_team` | double |  |
| `pass_touchdown_diff_team` | double |  |
| `rec_touchdown_diff_team` | double |  |
| `rush_touchdown_diff_team` | double |  |
| `pass_two_point_conv_diff_team` | double |  |
| `rec_two_point_conv_diff_team` | double |  |
| `rush_two_point_conv_diff_team` | double |  |
| `pass_first_down_diff_team` | double |  |
| `rec_first_down_diff_team` | double |  |
| `rush_first_down_diff_team` | double |  |
| `pass_interception_diff_team` | double |  |
| `rec_interception_diff_team` | double |  |
| `pass_fantasy_points_diff_team` | double |  |
| `rec_fantasy_points_diff_team` | double |  |
| `rush_fantasy_points_diff_team` | double |  |
| `total_yards_gained_team` | double |  |
| `total_yards_gained_exp_team` | double |  |
| `total_yards_gained_diff_team` | double |  |
| `total_touchdown_team` | double |  |
| `total_touchdown_exp_team` | double |  |
| `total_touchdown_diff_team` | double |  |
| `total_first_down_team` | double |  |
| `total_first_down_exp_team` | double |  |
| `total_first_down_diff_team` | double |  |
| `total_fantasy_points_team` | double |  |
| `total_fantasy_points_exp_team` | double |  |
| `total_fantasy_points_diff_team` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

Pass play-by-play opportunity stats::

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

Rush play-by-play opportunity stats with pinned model version::

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

| col_name | type | description |
|---|---|---|
| `mfl_id` | integer |  |
| `sportradar_id` | character |  |
| `fantasypros_id` | character |  |
| `gsis_id` | character |  |
| `pff_id` | character |  |
| `sleeper_id` | integer |  |
| `nfl_id` | character |  |
| `espn_id` | integer |  |
| `yahoo_id` | character |  |
| `fleaflicker_id` | character |  |
| `cbs_id` | integer |  |
| `pfr_id` | character |  |
| `cfbref_id` | character |  |
| `rotowire_id` | integer |  |
| `rotoworld_id` | character |  |
| `ktc_id` | integer |  |
| `stats_id` | integer |  |
| `stats_global_id` | integer |  |
| `fantasy_data_id` | integer |  |
| `swish_id` | character |  |
| `name` | character |  |
| `merge_name` | character |  |
| `position` | character |  |
| `team` | character |  |
| `birthdate` | character |  |
| `age` | double |  |
| `draft_year` | integer |  |
| `draft_round` | integer |  |
| `draft_pick` | integer |  |
| `draft_ovr` | integer |  |
| `twitter_username` | character |  |
| `height` | integer |  |
| `weight` | integer |  |
| `college` | character |  |
| `db_season` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

Filter to active QBs::

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of ``"draft"`` (current draft rankings), ``"week"`` (weekly rankings), or ``"all"`` (full historical rankings). Defaults to ``"draft"``. Kept for nflreadpy parity since its parameter is also called ``type``; the forward-going preferred name is ``kind``. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as ``type``. If both are supplied, ``kind`` wins. If neither is supplied, defaults to ``"draft"`` via ``type``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

| col_name | type | description |
|---|---|---|
| `fp_page` | character |  |
| `page_type` | character |  |
| `ecr_type` | character |  |
| `player` | character |  |
| `id` | integer |  |
| `pos` | character |  |
| `team` | character |  |
| `ecr` | double |  |
| `sd` | double |  |
| `best` | integer |  |
| `worst` | integer |  |
| `sportsdata_id` | character |  |
| `player_filename` | character |  |
| `yahoo_id` | character |  |
| `cbs_id` | character |  |
| `player_owned_avg` | double |  |
| `player_owned_espn` | character |  |
| `player_owned_yahoo` | character |  |
| `player_image_url` | character |  |
| `player_square_image_url` | character |  |
| `rank_delta` | integer |  |
| `bye` | integer |  |
| `mergename` | character |  |
| `scrape_date` | character |  |
| `tm` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

Weekly rankings::

weekly = load_nfl_ff_rankings(kind="week")

Full historical rankings (parquet)::

history = load_nfl_ff_rankings(kind="all")

nflreadpy-parity ``type=`` parameter (still supported)::

draft = load_nfl_ff_rankings(type="draft")
```

### `load_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `ftn_game_id` | integer |  |
| `nflverse_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `ftn_play_id` | integer |  |
| `nflverse_play_id` | integer |  |
| `starting_hash` | character |  |
| `qb_location` | character |  |
| `n_offense_backfield` | integer |  |
| `n_defense_box` | integer |  |
| `is_no_huddle` | logical |  |
| `is_motion` | logical |  |
| `is_play_action` | logical |  |
| `is_screen_pass` | logical |  |
| `is_rpo` | logical |  |
| `is_trick_play` | logical |  |
| `is_qb_out_of_pocket` | logical |  |
| `is_interception_worthy` | logical |  |
| `is_throw_away` | logical |  |
| `read_thrown` | character |  |
| `is_catchable_ball` | logical |  |
| `is_contested_ball` | logical |  |
| `is_created_reception` | logical |  |
| `is_drop` | logical |  |
| `is_qb_sneak` | logical |  |
| `n_blitzers` | integer |  |
| `n_pass_rushers` | integer |  |
| `is_qb_fault_sack` | logical |  |
| `date_pulled` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

Multi-season range::

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

Filter to plays with motion::

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### `load_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `week` | integer |  |
| `gsis_id` | character |  |
| `position` | character |  |
| `full_name` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `report_primary_injury` | character |  |
| `report_secondary_injury` | character |  |
| `report_status` | character |  |
| `practice_primary_injury` | character |  |
| `practice_secondary_injury` | character |  |
| `practice_status` | character |  |
| `date_modified` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

Multi-season range with team filter::

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### `load_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats accessors. Mirrors the API surface of nflreadpy's ``load_nextgen_stats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — ``seasons`` is applied as a post-filter on the ``season`` column. |
| `stat_type` | `str` | `'passing'` | One of ``"passing"``, ``"rushing"``, ``"receiving"``. Defaults to ``"passing"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested ``stat_type`` and ``seasons``.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |
| `player_display_name` | character |  |
| `player_position` | character |  |
| `team_abbr` | character |  |
| `avg_time_to_throw` | double |  |
| `avg_completed_air_yards` | double |  |
| `avg_intended_air_yards` | double |  |
| `avg_air_yards_differential` | double |  |
| `aggressiveness` | double |  |
| `max_completed_air_distance` | double |  |
| `avg_air_yards_to_sticks` | double |  |
| `attempts` | integer |  |
| `pass_yards` | integer |  |
| `pass_touchdowns` | integer |  |
| `interceptions` | integer |  |
| `passer_rating` | double |  |
| `completions` | integer |  |
| `completion_percentage` | double |  |
| `expected_completion_percentage` | double |  |
| `completion_percentage_above_expectation` | double |  |
| `avg_air_distance` | double |  |
| `max_air_distance` | double |  |
| `player_gsis_id` | character |  |
| `player_first_name` | character |  |
| `player_last_name` | character |  |
| `player_jersey_number` | integer |  |
| `player_short_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

Rushing NextGen stats::

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

Receiving NextGen stats with a follow-up filter::

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

Pandas round-trip::

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `draft_year` | double |  |
| `draft_team` | character |  |
| `draft_round` | double |  |
| `draft_ovr` | double |  |
| `pfr_id` | character |  |
| `cfb_id` | character |  |
| `player_name` | character |  |
| `pos` | character |  |
| `school` | character |  |
| `ht` | character |  |
| `wt` | double |  |
| `forty` | double |  |
| `bench` | double |  |
| `vertical` | double |  |
| `broad_jump` | double |  |
| `cone` | double |  |
| `shuttle` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

Filter by draft year and position::

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_nfl_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character |  |
| `position` | character |  |
| `team` | character |  |
| `is_active` | logical |  |
| `year_signed` | integer |  |
| `years` | integer |  |
| `value` | double |  |
| `apy` | double |  |
| `guaranteed` | double |  |
| `apy_cap_pct` | double |  |
| `inflated_value` | double |  |
| `inflated_apy` | double |  |
| `inflated_guaranteed` | double |  |
| `player_page` | character |  |
| `otc_id` | integer |  |
| `gsis_id` | character |  |
| `date_of_birth` | character |  |
| `height` | character |  |
| `weight` | character |  |
| `college` | character |  |
| `draft_year` | integer |  |
| `draft_round` | integer |  |
| `draft_overall` | integer |  |
| `draft_team` | character |  |
| `cols` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

Pandas round-trip with sort by APY::

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_nfl_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `club_code` | character |  |
| `week` | integer |  |
| `game_type` | character |  |
| `depth_team` | character |  |
| `last_name` | character |  |
| `first_name` | character |  |
| `football_name` | character |  |
| `formation` | character |  |
| `gsis_id` | character |  |
| `jersey_number` | character |  |
| `position` | character |  |
| `elias_id` | character |  |
| `depth_position` | character |  |
| `full_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

Multi-season range::

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### `load_nfl_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `round` | integer |  |
| `pick` | integer |  |
| `team` | character |  |
| `gsis_id` | character |  |
| `pfr_player_id` | character |  |
| `cfb_player_id` | character |  |
| `pfr_player_name` | character |  |
| `hof` | logical |  |
| `position` | character |  |
| `category` | character |  |
| `side` | character |  |
| `college` | character |  |
| `age` | integer |  |
| `to` | integer |  |
| `allpro` | integer |  |
| `probowls` | integer |  |
| `seasons_started` | integer |  |
| `w_av` | integer |  |
| `car_av` | logical |  |
| `dr_av` | integer |  |
| `games` | integer |  |
| `pass_completions` | integer |  |
| `pass_attempts` | integer |  |
| `pass_yards` | integer |  |
| `pass_tds` | integer |  |
| `pass_ints` | integer |  |
| `rush_atts` | integer |  |
| `rush_yards` | integer |  |
| `rush_tds` | integer |  |
| `receptions` | integer |  |
| `rec_yards` | integer |  |
| `rec_tds` | integer |  |
| `def_solo_tackles` | integer |  |
| `def_ints` | integer |  |
| `def_sacks` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

Filter to a single year and round::

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_nfl_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | character |  |
| `posteam` | character |  |
| `week` | double |  |
| `game_id` | character |  |
| `player_id` | character |  |
| `full_name` | character |  |
| `position` | character |  |
| `pass_attempt` | double |  |
| `rec_attempt` | double |  |
| `rush_attempt` | double |  |
| `pass_air_yards` | double |  |
| `rec_air_yards` | double |  |
| `pass_completions` | double |  |
| `receptions` | double |  |
| `pass_completions_exp` | double |  |
| `receptions_exp` | double |  |
| `pass_yards_gained` | double |  |
| `rec_yards_gained` | double |  |
| `rush_yards_gained` | double |  |
| `pass_yards_gained_exp` | double |  |
| `rec_yards_gained_exp` | double |  |
| `rush_yards_gained_exp` | double |  |
| `pass_touchdown` | double |  |
| `rec_touchdown` | double |  |
| `rush_touchdown` | double |  |
| `pass_touchdown_exp` | double |  |
| `rec_touchdown_exp` | double |  |
| `rush_touchdown_exp` | double |  |
| `pass_two_point_conv` | double |  |
| `rec_two_point_conv` | double |  |
| `rush_two_point_conv` | double |  |
| `pass_two_point_conv_exp` | double |  |
| `rec_two_point_conv_exp` | double |  |
| `rush_two_point_conv_exp` | double |  |
| `pass_first_down` | double |  |
| `rec_first_down` | double |  |
| `rush_first_down` | double |  |
| `pass_first_down_exp` | double |  |
| `rec_first_down_exp` | double |  |
| `rush_first_down_exp` | double |  |
| `pass_interception` | double |  |
| `rec_interception` | double |  |
| `pass_interception_exp` | double |  |
| `rec_interception_exp` | double |  |
| `rec_fumble_lost` | double |  |
| `rush_fumble_lost` | double |  |
| `pass_fantasy_points_exp` | double |  |
| `rec_fantasy_points_exp` | double |  |
| `rush_fantasy_points_exp` | double |  |
| `pass_fantasy_points` | double |  |
| `rec_fantasy_points` | double |  |
| `rush_fantasy_points` | double |  |
| `total_yards_gained` | double |  |
| `total_yards_gained_exp` | double |  |
| `total_touchdown` | double |  |
| `total_touchdown_exp` | double |  |
| `total_first_down` | double |  |
| `total_first_down_exp` | double |  |
| `total_fantasy_points` | double |  |
| `total_fantasy_points_exp` | double |  |
| `pass_completions_diff` | double |  |
| `receptions_diff` | double |  |
| `pass_yards_gained_diff` | double |  |
| `rec_yards_gained_diff` | double |  |
| `rush_yards_gained_diff` | double |  |
| `pass_touchdown_diff` | double |  |
| `rec_touchdown_diff` | double |  |
| `rush_touchdown_diff` | double |  |
| `pass_two_point_conv_diff` | double |  |
| `rec_two_point_conv_diff` | double |  |
| `rush_two_point_conv_diff` | double |  |
| `pass_first_down_diff` | double |  |
| `rec_first_down_diff` | double |  |
| `rush_first_down_diff` | double |  |
| `pass_interception_diff` | double |  |
| `rec_interception_diff` | double |  |
| `pass_fantasy_points_diff` | double |  |
| `rec_fantasy_points_diff` | double |  |
| `rush_fantasy_points_diff` | double |  |
| `total_yards_gained_diff` | double |  |
| `total_touchdown_diff` | double |  |
| `total_first_down_diff` | double |  |
| `total_fantasy_points_diff` | double |  |
| `pass_attempt_team` | double |  |
| `rec_attempt_team` | double |  |
| `rush_attempt_team` | double |  |
| `pass_air_yards_team` | double |  |
| `rec_air_yards_team` | double |  |
| `pass_completions_team` | double |  |
| `receptions_team` | double |  |
| `pass_completions_exp_team` | double |  |
| `receptions_exp_team` | double |  |
| `pass_yards_gained_team` | double |  |
| `rec_yards_gained_team` | double |  |
| `rush_yards_gained_team` | double |  |
| `pass_yards_gained_exp_team` | double |  |
| `rec_yards_gained_exp_team` | double |  |
| `rush_yards_gained_exp_team` | double |  |
| `pass_touchdown_team` | double |  |
| `rec_touchdown_team` | double |  |
| `rush_touchdown_team` | double |  |
| `pass_touchdown_exp_team` | double |  |
| `rec_touchdown_exp_team` | double |  |
| `rush_touchdown_exp_team` | double |  |
| `pass_two_point_conv_team` | double |  |
| `rec_two_point_conv_team` | double |  |
| `rush_two_point_conv_team` | double |  |
| `pass_two_point_conv_exp_team` | double |  |
| `rec_two_point_conv_exp_team` | double |  |
| `rush_two_point_conv_exp_team` | double |  |
| `pass_first_down_team` | double |  |
| `rec_first_down_team` | double |  |
| `rush_first_down_team` | double |  |
| `pass_first_down_exp_team` | double |  |
| `rec_first_down_exp_team` | double |  |
| `rush_first_down_exp_team` | double |  |
| `pass_interception_team` | double |  |
| `rec_interception_team` | double |  |
| `pass_interception_exp_team` | double |  |
| `rec_interception_exp_team` | double |  |
| `rec_fumble_lost_team` | double |  |
| `rush_fumble_lost_team` | double |  |
| `pass_fantasy_points_exp_team` | double |  |
| `rec_fantasy_points_exp_team` | double |  |
| `rush_fantasy_points_exp_team` | double |  |
| `pass_fantasy_points_team` | double |  |
| `rec_fantasy_points_team` | double |  |
| `rush_fantasy_points_team` | double |  |
| `pass_completions_diff_team` | double |  |
| `receptions_diff_team` | double |  |
| `pass_yards_gained_diff_team` | double |  |
| `rec_yards_gained_diff_team` | double |  |
| `rush_yards_gained_diff_team` | double |  |
| `pass_touchdown_diff_team` | double |  |
| `rec_touchdown_diff_team` | double |  |
| `rush_touchdown_diff_team` | double |  |
| `pass_two_point_conv_diff_team` | double |  |
| `rec_two_point_conv_diff_team` | double |  |
| `rush_two_point_conv_diff_team` | double |  |
| `pass_first_down_diff_team` | double |  |
| `rec_first_down_diff_team` | double |  |
| `rush_first_down_diff_team` | double |  |
| `pass_interception_diff_team` | double |  |
| `rec_interception_diff_team` | double |  |
| `pass_fantasy_points_diff_team` | double |  |
| `rec_fantasy_points_diff_team` | double |  |
| `rush_fantasy_points_diff_team` | double |  |
| `total_yards_gained_team` | double |  |
| `total_yards_gained_exp_team` | double |  |
| `total_yards_gained_diff_team` | double |  |
| `total_touchdown_team` | double |  |
| `total_touchdown_exp_team` | double |  |
| `total_touchdown_diff_team` | double |  |
| `total_first_down_team` | double |  |
| `total_first_down_exp_team` | double |  |
| `total_first_down_diff_team` | double |  |
| `total_fantasy_points_team` | double |  |
| `total_fantasy_points_exp_team` | double |  |
| `total_fantasy_points_diff_team` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

Pass play-by-play opportunity stats::

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

Rush play-by-play opportunity stats with pinned model version::

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_nfl_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

| col_name | type | description |
|---|---|---|
| `mfl_id` | integer |  |
| `sportradar_id` | character |  |
| `fantasypros_id` | character |  |
| `gsis_id` | character |  |
| `pff_id` | character |  |
| `sleeper_id` | integer |  |
| `nfl_id` | character |  |
| `espn_id` | integer |  |
| `yahoo_id` | character |  |
| `fleaflicker_id` | character |  |
| `cbs_id` | integer |  |
| `pfr_id` | character |  |
| `cfbref_id` | character |  |
| `rotowire_id` | integer |  |
| `rotoworld_id` | character |  |
| `ktc_id` | integer |  |
| `stats_id` | integer |  |
| `stats_global_id` | integer |  |
| `fantasy_data_id` | integer |  |
| `swish_id` | character |  |
| `name` | character |  |
| `merge_name` | character |  |
| `position` | character |  |
| `team` | character |  |
| `birthdate` | character |  |
| `age` | double |  |
| `draft_year` | integer |  |
| `draft_round` | integer |  |
| `draft_pick` | integer |  |
| `draft_ovr` | integer |  |
| `twitter_username` | character |  |
| `height` | integer |  |
| `weight` | integer |  |
| `college` | character |  |
| `db_season` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

Filter to active QBs::

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_nfl_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of ``"draft"`` (current draft rankings), ``"week"`` (weekly rankings), or ``"all"`` (full historical rankings). Defaults to ``"draft"``. Kept for nflreadpy parity since its parameter is also called ``type``; the forward-going preferred name is ``kind``. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as ``type``. If both are supplied, ``kind`` wins. If neither is supplied, defaults to ``"draft"`` via ``type``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

| col_name | type | description |
|---|---|---|
| `fp_page` | character |  |
| `page_type` | character |  |
| `ecr_type` | character |  |
| `player` | character |  |
| `id` | integer |  |
| `pos` | character |  |
| `team` | character |  |
| `ecr` | double |  |
| `sd` | double |  |
| `best` | integer |  |
| `worst` | integer |  |
| `sportsdata_id` | character |  |
| `player_filename` | character |  |
| `yahoo_id` | character |  |
| `cbs_id` | character |  |
| `player_owned_avg` | double |  |
| `player_owned_espn` | character |  |
| `player_owned_yahoo` | character |  |
| `player_image_url` | character |  |
| `player_square_image_url` | character |  |
| `rank_delta` | integer |  |
| `bye` | integer |  |
| `mergename` | character |  |
| `scrape_date` | character |  |
| `tm` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

Weekly rankings::

weekly = load_nfl_ff_rankings(kind="week")

Full historical rankings (parquet)::

history = load_nfl_ff_rankings(kind="all")

nflreadpy-parity ``type=`` parameter (still supported)::

draft = load_nfl_ff_rankings(type="draft")
```

### `load_nfl_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `ftn_game_id` | integer |  |
| `nflverse_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `ftn_play_id` | integer |  |
| `nflverse_play_id` | integer |  |
| `starting_hash` | character |  |
| `qb_location` | character |  |
| `n_offense_backfield` | integer |  |
| `n_defense_box` | integer |  |
| `is_no_huddle` | logical |  |
| `is_motion` | logical |  |
| `is_play_action` | logical |  |
| `is_screen_pass` | logical |  |
| `is_rpo` | logical |  |
| `is_trick_play` | logical |  |
| `is_qb_out_of_pocket` | logical |  |
| `is_interception_worthy` | logical |  |
| `is_throw_away` | logical |  |
| `read_thrown` | character |  |
| `is_catchable_ball` | logical |  |
| `is_contested_ball` | logical |  |
| `is_created_reception` | logical |  |
| `is_drop` | logical |  |
| `is_qb_sneak` | logical |  |
| `n_blitzers` | integer |  |
| `n_pass_rushers` | integer |  |
| `is_qb_fault_sack` | logical |  |
| `date_pulled` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

Multi-season range::

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

Filter to plays with motion::

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### `load_nfl_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `week` | integer |  |
| `gsis_id` | character |  |
| `position` | character |  |
| `full_name` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `report_primary_injury` | character |  |
| `report_secondary_injury` | character |  |
| `report_status` | character |  |
| `practice_primary_injury` | character |  |
| `practice_secondary_injury` | character |  |
| `practice_status` | character |  |
| `date_modified` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

Multi-season range with team filter::

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### `load_nfl_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats accessors. Mirrors the API surface of nflreadpy's ``load_nextgen_stats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — ``seasons`` is applied as a post-filter on the ``season`` column. |
| `stat_type` | `str` | `'passing'` | One of ``"passing"``, ``"rushing"``, ``"receiving"``. Defaults to ``"passing"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested ``stat_type`` and ``seasons``.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |
| `player_display_name` | character |  |
| `player_position` | character |  |
| `team_abbr` | character |  |
| `avg_time_to_throw` | double |  |
| `avg_completed_air_yards` | double |  |
| `avg_intended_air_yards` | double |  |
| `avg_air_yards_differential` | double |  |
| `aggressiveness` | double |  |
| `max_completed_air_distance` | double |  |
| `avg_air_yards_to_sticks` | double |  |
| `attempts` | integer |  |
| `pass_yards` | integer |  |
| `pass_touchdowns` | integer |  |
| `interceptions` | integer |  |
| `passer_rating` | double |  |
| `completions` | integer |  |
| `completion_percentage` | double |  |
| `expected_completion_percentage` | double |  |
| `completion_percentage_above_expectation` | double |  |
| `avg_air_distance` | double |  |
| `max_air_distance` | double |  |
| `player_gsis_id` | character |  |
| `player_first_name` | character |  |
| `player_last_name` | character |  |
| `player_jersey_number` | integer |  |
| `player_short_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

Rushing NextGen stats::

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

Receiving NextGen stats with a follow-up filter::

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

Pandas round-trip::

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_ngs_passing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='passing')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_nextgen_stats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |
| `player_display_name` | character |  |
| `player_position` | character |  |
| `team_abbr` | character |  |
| `avg_time_to_throw` | double |  |
| `avg_completed_air_yards` | double |  |
| `avg_intended_air_yards` | double |  |
| `avg_air_yards_differential` | double |  |
| `aggressiveness` | double |  |
| `max_completed_air_distance` | double |  |
| `avg_air_yards_to_sticks` | double |  |
| `attempts` | integer |  |
| `pass_yards` | integer |  |
| `pass_touchdowns` | integer |  |
| `interceptions` | integer |  |
| `passer_rating` | double |  |
| `completions` | integer |  |
| `completion_percentage` | double |  |
| `expected_completion_percentage` | double |  |
| `completion_percentage_above_expectation` | double |  |
| `avg_air_distance` | double |  |
| `max_air_distance` | double |  |
| `player_gsis_id` | character |  |
| `player_first_name` | character |  |
| `player_last_name` | character |  |
| `player_jersey_number` | integer |  |
| `player_short_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
```

### `load_nfl_ngs_receiving(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='receiving')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_nextgen_stats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |
| `player_display_name` | character |  |
| `player_position` | character |  |
| `team_abbr` | character |  |
| `avg_cushion` | double |  |
| `avg_separation` | double |  |
| `avg_intended_air_yards` | double |  |
| `percent_share_of_intended_air_yards` | double |  |
| `receptions` | integer |  |
| `targets` | integer |  |
| `catch_percentage` | double |  |
| `yards` | integer |  |
| `rec_touchdowns` | integer |  |
| `avg_yac` | double |  |
| `avg_expected_yac` | double |  |
| `avg_yac_above_expectation` | double |  |
| `player_gsis_id` | character |  |
| `player_first_name` | character |  |
| `player_last_name` | character |  |
| `player_jersey_number` | integer |  |
| `player_short_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
```

### `load_nfl_ngs_rushing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='rushing')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_nextgen_stats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |
| `player_display_name` | character |  |
| `player_position` | character |  |
| `team_abbr` | character |  |
| `efficiency` | double |  |
| `percent_attempts_gte_eight_defenders` | double |  |
| `avg_time_to_los` | double |  |
| `rush_attempts` | integer |  |
| `rush_yards` | integer |  |
| `avg_rush_yards` | double |  |
| `rush_touchdowns` | integer |  |
| `player_gsis_id` | character |  |
| `player_first_name` | character |  |
| `player_last_name` | character |  |
| `player_jersey_number` | integer |  |
| `player_short_name` | character |  |
| `expected_rush_yards` | double |  |
| `rush_yards_over_expected` | double |  |
| `rush_yards_over_expected_per_att` | double |  |
| `rush_pct_over_expected` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
```

### `load_nfl_officials(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `game_key` | character |  |
| `official_name` | character |  |
| `position` | character |  |
| `jersey_number` | integer |  |
| `official_id` | character |  |
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

Pandas round-trip::

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_nfl_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `play_id` | double |  |
| `game_id` | character |  |
| `old_game_id` | character |  |
| `home_team` | character |  |
| `away_team` | character |  |
| `season_type` | character |  |
| `week` | integer |  |
| `posteam` | character |  |
| `posteam_type` | character |  |
| `defteam` | character |  |
| `side_of_field` | character |  |
| `yardline_100` | double |  |
| `game_date` | character |  |
| `quarter_seconds_remaining` | double |  |
| `half_seconds_remaining` | double |  |
| `game_seconds_remaining` | double |  |
| `game_half` | character |  |
| `quarter_end` | double |  |
| `drive` | double |  |
| `sp` | double |  |
| `qtr` | double |  |
| `down` | double |  |
| `goal_to_go` | integer |  |
| `time` | character |  |
| `yrdln` | character |  |
| `ydstogo` | double |  |
| `ydsnet` | double |  |
| `desc` | character |  |
| `play_type` | character |  |
| `yards_gained` | double |  |
| `shotgun` | double |  |
| `no_huddle` | double |  |
| `qb_dropback` | double |  |
| `qb_kneel` | double |  |
| `qb_spike` | double |  |
| `qb_scramble` | double |  |
| `pass_length` | character |  |
| `pass_location` | character |  |
| `air_yards` | double |  |
| `yards_after_catch` | double |  |
| `run_location` | character |  |
| `run_gap` | character |  |
| `field_goal_result` | character |  |
| `kick_distance` | double |  |
| `extra_point_result` | character |  |
| `two_point_conv_result` | character |  |
| `home_timeouts_remaining` | double |  |
| `away_timeouts_remaining` | double |  |
| `timeout` | double |  |
| `timeout_team` | character |  |
| `td_team` | character |  |
| `td_player_name` | character |  |
| `td_player_id` | character |  |
| `posteam_timeouts_remaining` | double |  |
| `defteam_timeouts_remaining` | double |  |
| `total_home_score` | double |  |
| `total_away_score` | double |  |
| `posteam_score` | double |  |
| `defteam_score` | double |  |
| `score_differential` | double |  |
| `posteam_score_post` | double |  |
| `defteam_score_post` | double |  |
| `score_differential_post` | double |  |
| `no_score_prob` | double |  |
| `opp_fg_prob` | double |  |
| `opp_safety_prob` | double |  |
| `opp_td_prob` | double |  |
| `fg_prob` | double |  |
| `safety_prob` | double |  |
| `td_prob` | double |  |
| `extra_point_prob` | double |  |
| `two_point_conversion_prob` | double |  |
| `ep` | double |  |
| `epa` | double |  |
| `total_home_epa` | double |  |
| `total_away_epa` | double |  |
| `total_home_rush_epa` | double |  |
| `total_away_rush_epa` | double |  |
| `total_home_pass_epa` | double |  |
| `total_away_pass_epa` | double |  |
| `air_epa` | double |  |
| `yac_epa` | double |  |
| `comp_air_epa` | double |  |
| `comp_yac_epa` | double |  |
| `total_home_comp_air_epa` | double |  |
| `total_away_comp_air_epa` | double |  |
| `total_home_comp_yac_epa` | double |  |
| `total_away_comp_yac_epa` | double |  |
| `total_home_raw_air_epa` | double |  |
| `total_away_raw_air_epa` | double |  |
| `total_home_raw_yac_epa` | double |  |
| `total_away_raw_yac_epa` | double |  |
| `wp` | double |  |
| `def_wp` | double |  |
| `home_wp` | double |  |
| `away_wp` | double |  |
| `wpa` | double |  |
| `vegas_wpa` | double |  |
| `vegas_home_wpa` | double |  |
| `home_wp_post` | double |  |
| `away_wp_post` | double |  |
| `vegas_wp` | double |  |
| `vegas_home_wp` | double |  |
| `total_home_rush_wpa` | double |  |
| `total_away_rush_wpa` | double |  |
| `total_home_pass_wpa` | double |  |
| `total_away_pass_wpa` | double |  |
| `air_wpa` | double |  |
| `yac_wpa` | double |  |
| `comp_air_wpa` | double |  |
| `comp_yac_wpa` | double |  |
| `total_home_comp_air_wpa` | double |  |
| `total_away_comp_air_wpa` | double |  |
| `total_home_comp_yac_wpa` | double |  |
| `total_away_comp_yac_wpa` | double |  |
| `total_home_raw_air_wpa` | double |  |
| `total_away_raw_air_wpa` | double |  |
| `total_home_raw_yac_wpa` | double |  |
| `total_away_raw_yac_wpa` | double |  |
| `punt_blocked` | double |  |
| `first_down_rush` | double |  |
| `first_down_pass` | double |  |
| `first_down_penalty` | double |  |
| `third_down_converted` | double |  |
| `third_down_failed` | double |  |
| `fourth_down_converted` | double |  |
| `fourth_down_failed` | double |  |
| `incomplete_pass` | double |  |
| `touchback` | double |  |
| `interception` | double |  |
| `punt_inside_twenty` | double |  |
| `punt_in_endzone` | double |  |
| `punt_out_of_bounds` | double |  |
| `punt_downed` | double |  |
| `punt_fair_catch` | double |  |
| `kickoff_inside_twenty` | double |  |
| `kickoff_in_endzone` | double |  |
| `kickoff_out_of_bounds` | double |  |
| `kickoff_downed` | double |  |
| `kickoff_fair_catch` | double |  |
| `fumble_forced` | double |  |
| `fumble_not_forced` | double |  |
| `fumble_out_of_bounds` | double |  |
| `solo_tackle` | double |  |
| `safety` | double |  |
| `penalty` | double |  |
| `tackled_for_loss` | double |  |
| `fumble_lost` | double |  |
| `own_kickoff_recovery` | double |  |
| `own_kickoff_recovery_td` | double |  |
| `qb_hit` | double |  |
| `rush_attempt` | double |  |
| `pass_attempt` | double |  |
| `sack` | double |  |
| `touchdown` | double |  |
| `pass_touchdown` | double |  |
| `rush_touchdown` | double |  |
| `return_touchdown` | double |  |
| `extra_point_attempt` | double |  |
| `two_point_attempt` | double |  |
| `field_goal_attempt` | double |  |
| `kickoff_attempt` | double |  |
| `punt_attempt` | double |  |
| `fumble` | double |  |
| `complete_pass` | double |  |
| `assist_tackle` | double |  |
| `lateral_reception` | double |  |
| `lateral_rush` | double |  |
| `lateral_return` | double |  |
| `lateral_recovery` | double |  |
| `passer_player_id` | character |  |
| `passer_player_name` | character |  |
| `passing_yards` | double |  |
| `receiver_player_id` | character |  |
| `receiver_player_name` | character |  |
| `receiving_yards` | double |  |
| `rusher_player_id` | character |  |
| `rusher_player_name` | character |  |
| `rushing_yards` | double |  |
| `lateral_receiver_player_id` | character |  |
| `lateral_receiver_player_name` | character |  |
| `lateral_receiving_yards` | double |  |
| `lateral_rusher_player_id` | character |  |
| `lateral_rusher_player_name` | character |  |
| `lateral_rushing_yards` | double |  |
| `lateral_sack_player_id` | character |  |
| `lateral_sack_player_name` | character |  |
| `interception_player_id` | character |  |
| `interception_player_name` | character |  |
| `lateral_interception_player_id` | character |  |
| `lateral_interception_player_name` | character |  |
| `punt_returner_player_id` | character |  |
| `punt_returner_player_name` | character |  |
| `lateral_punt_returner_player_id` | character |  |
| `lateral_punt_returner_player_name` | character |  |
| `kickoff_returner_player_name` | character |  |
| `kickoff_returner_player_id` | character |  |
| `lateral_kickoff_returner_player_id` | character |  |
| `lateral_kickoff_returner_player_name` | character |  |
| `punter_player_id` | character |  |
| `punter_player_name` | character |  |
| `kicker_player_name` | character |  |
| `kicker_player_id` | character |  |
| `own_kickoff_recovery_player_id` | character |  |
| `own_kickoff_recovery_player_name` | character |  |
| `blocked_player_id` | character |  |
| `blocked_player_name` | character |  |
| `tackle_for_loss_1_player_id` | character |  |
| `tackle_for_loss_1_player_name` | character |  |
| `tackle_for_loss_2_player_id` | character |  |
| `tackle_for_loss_2_player_name` | character |  |
| `qb_hit_1_player_id` | character |  |
| `qb_hit_1_player_name` | character |  |
| `qb_hit_2_player_id` | character |  |
| `qb_hit_2_player_name` | character |  |
| `forced_fumble_player_1_team` | character |  |
| `forced_fumble_player_1_player_id` | character |  |
| `forced_fumble_player_1_player_name` | character |  |
| `forced_fumble_player_2_team` | character |  |
| `forced_fumble_player_2_player_id` | character |  |
| `forced_fumble_player_2_player_name` | character |  |
| `solo_tackle_1_team` | character |  |
| `solo_tackle_2_team` | character |  |
| `solo_tackle_1_player_id` | character |  |
| `solo_tackle_2_player_id` | character |  |
| `solo_tackle_1_player_name` | character |  |
| `solo_tackle_2_player_name` | character |  |
| `assist_tackle_1_player_id` | character |  |
| `assist_tackle_1_player_name` | character |  |
| `assist_tackle_1_team` | character |  |
| `assist_tackle_2_player_id` | character |  |
| `assist_tackle_2_player_name` | character |  |
| `assist_tackle_2_team` | character |  |
| `assist_tackle_3_player_id` | character |  |
| `assist_tackle_3_player_name` | character |  |
| `assist_tackle_3_team` | character |  |
| `assist_tackle_4_player_id` | character |  |
| `assist_tackle_4_player_name` | character |  |
| `assist_tackle_4_team` | character |  |
| `tackle_with_assist` | double |  |
| `tackle_with_assist_1_player_id` | character |  |
| `tackle_with_assist_1_player_name` | character |  |
| `tackle_with_assist_1_team` | character |  |
| `tackle_with_assist_2_player_id` | character |  |
| `tackle_with_assist_2_player_name` | character |  |
| `tackle_with_assist_2_team` | character |  |
| `pass_defense_1_player_id` | character |  |
| `pass_defense_1_player_name` | character |  |
| `pass_defense_2_player_id` | character |  |
| `pass_defense_2_player_name` | character |  |
| `fumbled_1_team` | character |  |
| `fumbled_1_player_id` | character |  |
| `fumbled_1_player_name` | character |  |
| `fumbled_2_player_id` | character |  |
| `fumbled_2_player_name` | character |  |
| `fumbled_2_team` | character |  |
| `fumble_recovery_1_team` | character |  |
| `fumble_recovery_1_yards` | double |  |
| `fumble_recovery_1_player_id` | character |  |
| `fumble_recovery_1_player_name` | character |  |
| `fumble_recovery_2_team` | character |  |
| `fumble_recovery_2_yards` | double |  |
| `fumble_recovery_2_player_id` | character |  |
| `fumble_recovery_2_player_name` | character |  |
| `sack_player_id` | character |  |
| `sack_player_name` | character |  |
| `half_sack_1_player_id` | character |  |
| `half_sack_1_player_name` | character |  |
| `half_sack_2_player_id` | character |  |
| `half_sack_2_player_name` | character |  |
| `return_team` | character |  |
| `return_yards` | double |  |
| `penalty_team` | character |  |
| `penalty_player_id` | character |  |
| `penalty_player_name` | character |  |
| `penalty_yards` | double |  |
| `replay_or_challenge` | double |  |
| `replay_or_challenge_result` | character |  |
| `penalty_type` | character |  |
| `defensive_two_point_attempt` | double |  |
| `defensive_two_point_conv` | double |  |
| `defensive_extra_point_attempt` | double |  |
| `defensive_extra_point_conv` | double |  |
| `safety_player_name` | character |  |
| `safety_player_id` | character |  |
| `season` | integer |  |
| `cp` | double |  |
| `cpoe` | double |  |
| `series` | double |  |
| `series_success` | double |  |
| `series_result` | character |  |
| `order_sequence` | double |  |
| `start_time` | character |  |
| `time_of_day` | character |  |
| `stadium` | character |  |
| `weather` | character |  |
| `nfl_api_id` | character |  |
| `play_clock` | character |  |
| `play_deleted` | double |  |
| `play_type_nfl` | character |  |
| `special_teams_play` | double |  |
| `st_play_type` | character |  |
| `end_clock_time` | character |  |
| `end_yard_line` | character |  |
| `fixed_drive` | double |  |
| `fixed_drive_result` | character |  |
| `drive_real_start_time` | character |  |
| `drive_play_count` | double |  |
| `drive_time_of_possession` | character |  |
| `drive_first_downs` | double |  |
| `drive_inside20` | double |  |
| `drive_ended_with_score` | double |  |
| `drive_quarter_start` | double |  |
| `drive_quarter_end` | double |  |
| `drive_yards_penalized` | double |  |
| `drive_start_transition` | character |  |
| `drive_end_transition` | character |  |
| `drive_game_clock_start` | character |  |
| `drive_game_clock_end` | character |  |
| `drive_start_yard_line` | character |  |
| `drive_end_yard_line` | character |  |
| `drive_play_id_started` | double |  |
| `drive_play_id_ended` | double |  |
| `away_score` | integer |  |
| `home_score` | integer |  |
| `location` | character |  |
| `result` | integer |  |
| `total` | integer |  |
| `spread_line` | double |  |
| `total_line` | double |  |
| `div_game` | integer |  |
| `roof` | character |  |
| `surface` | character |  |
| `temp` | integer |  |
| `wind` | integer |  |
| `home_coach` | character |  |
| `away_coach` | character |  |
| `stadium_id` | character |  |
| `game_stadium` | character |  |
| `aborted_play` | double |  |
| `success` | double |  |
| `passer` | character |  |
| `passer_jersey_number` | integer |  |
| `rusher` | character |  |
| `rusher_jersey_number` | integer |  |
| `receiver` | character |  |
| `receiver_jersey_number` | integer |  |
| `pass` | double |  |
| `rush` | double |  |
| `first_down` | double |  |
| `special` | double |  |
| `play` | double |  |
| `passer_id` | character |  |
| `rusher_id` | character |  |
| `receiver_id` | character |  |
| `name` | character |  |
| `jersey_number` | integer |  |
| `id` | character |  |
| `fantasy_player_name` | character |  |
| `fantasy_player_id` | character |  |
| `fantasy` | character |  |
| `fantasy_id` | character |  |
| `out_of_bounds` | double |  |
| `home_opening_kickoff` | double |  |
| `qb_epa` | double |  |
| `xyac_epa` | double |  |
| `xyac_mean_yardage` | double |  |
| `xyac_median_yardage` | integer |  |
| `xyac_success` | double |  |
| `xyac_fd` | double |  |
| `xpass` | double |  |
| `pass_oe` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

Multi-season range::

pbp = load_nfl_pbp(seasons=range(2020, 2025))

With cache off (development workflow)::

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

Pandas round-trip::

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

### `load_nfl_pbp_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `nflverse_game_id` | character |  |
| `old_game_id` | character |  |
| `play_id` | double |  |
| `possession_team` | character |  |
| `offense_formation` | character |  |
| `offense_personnel` | character |  |
| `defenders_in_box` | integer |  |
| `defense_personnel` | character |  |
| `number_of_pass_rushers` | integer |  |
| `players_on_play` | character |  |
| `offense_players` | character |  |
| `defense_players` | character |  |
| `n_offense` | integer |  |
| `n_defense` | integer |  |
| `ngs_air_yards` | double |  |
| `time_to_throw` | double |  |
| `was_pressure` | logical |  |
| `route` | character |  |
| `defense_man_zone_type` | character |  |
| `defense_coverage_type` | character |  |
| `offense_names` | character |  |
| `defense_names` | character |  |
| `offense_positions` | character |  |
| `defense_positions` | character |  |
| `offense_numbers` | character |  |
| `defense_numbers` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

Multi-season range::

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### `load_nfl_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level PFR advstats accessors. Mirrors the API surface of nflreadpy's ``load_pfr_advstats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For ``summary_level='week'`` this drives the per-season parquet fan-out; for ``summary_level='season'`` it post-filters the combined parquet by the ``season`` column. |
| `stat_type` | `str` | `'pass'` | One of ``"pass"``, ``"rush"``, ``"rec"``, ``"def"``. Defaults to ``"pass"``. |
| `summary_level` | `str` | `'week'` | One of ``"week"`` or ``"season"``. Defaults to ``"week"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested ``stat_type``, ``summary_level``, and ``seasons``.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `pfr_player_name` | character |  |
| `pfr_player_id` | character |  |
| `passing_drops` | double |  |
| `passing_drop_pct` | double |  |
| `receiving_drop` | double |  |
| `receiving_drop_pct` | double |  |
| `passing_bad_throws` | double |  |
| `passing_bad_throw_pct` | double |  |
| `times_sacked` | double |  |
| `times_blitzed` | double |  |
| `times_hurried` | double |  |
| `times_hit` | double |  |
| `times_pressured` | double |  |
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

Season-level rushing summaries (one row per player per season)::

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

Defensive stats with a follow-up filter::

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

Pandas round-trip::

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_nfl_pfr_def(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `player` | character |  |
| `pfr_id` | character |  |
| `tm` | character |  |
| `age` | double |  |
| `pos` | character |  |
| `g` | double |  |
| `gs` | double |  |
| `int` | double |  |
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

### `load_nfl_pfr_pass(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `player` | character |  |
| `team` | character |  |
| `pass_attempts` | double |  |
| `throwaways` | double |  |
| `spikes` | double |  |
| `drops` | double |  |
| `drop_pct` | double |  |
| `bad_throws` | double |  |
| `bad_throw_pct` | double |  |
| `season` | integer |  |
| `pfr_id` | character |  |
| `pocket_time` | double |  |
| `times_blitzed` | double |  |
| `times_hurried` | double |  |
| `times_hit` | double |  |
| `times_pressured` | double |  |
| `pressure_pct` | double |  |
| `batted_balls` | double |  |
| `on_tgt_throws` | double |  |
| `on_tgt_pct` | double |  |
| `rpo_plays` | double |  |
| `rpo_yards` | double |  |
| `rpo_pass_att` | double |  |
| `rpo_pass_yards` | double |  |
| `rpo_rush_att` | double |  |
| `rpo_rush_yards` | double |  |
| `pa_pass_att` | double |  |
| `pa_pass_yards` | double |  |
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

### `load_nfl_pfr_rec(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `player` | character |  |
| `pfr_id` | character |  |
| `tm` | character |  |
| `age` | double |  |
| `pos` | character |  |
| `g` | double |  |
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
| `int` | double |  |
| `rat` | double |  |
| `loaded` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="season"
)
```

### `load_nfl_pfr_rush(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `player` | character |  |
| `pfr_id` | character |  |
| `tm` | character |  |
| `age` | double |  |
| `pos` | character |  |
| `g` | double |  |
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

### `load_nfl_pfr_weekly_def(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `pfr_player_name` | character |  |
| `pfr_player_id` | character |  |
| `def_ints` | double |  |
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
| `def_sacks` | double |  |
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

### `load_nfl_pfr_weekly_pass(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `pfr_player_name` | character |  |
| `pfr_player_id` | character |  |
| `passing_drops` | double |  |
| `passing_drop_pct` | double |  |
| `receiving_drop` | double |  |
| `receiving_drop_pct` | double |  |
| `passing_bad_throws` | double |  |
| `passing_bad_throw_pct` | double |  |
| `times_sacked` | double |  |
| `times_blitzed` | double |  |
| `times_hurried` | double |  |
| `times_hit` | double |  |
| `times_pressured` | double |  |
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

### `load_nfl_pfr_weekly_rec(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `pfr_player_name` | character |  |
| `pfr_player_id` | character |  |
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

### `load_nfl_pfr_weekly_rush(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `pfr_player_name` | character |  |
| `pfr_player_id` | character |  |
| `carries` | double |  |
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

### `load_nfl_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'`

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
| `player_id` | character |  |
| `player_name` | character |  |
| `player_display_name` | character |  |
| `position` | character |  |
| `position_group` | character |  |
| `headshot_url` | character |  |
| `recent_team` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `season_type` | character |  |
| `opponent_team` | character |  |
| `completions` | integer |  |
| `attempts` | integer |  |
| `passing_yards` | double |  |
| `passing_tds` | integer |  |
| `interceptions` | double |  |
| `sacks` | double |  |
| `sack_yards` | double |  |
| `sack_fumbles` | integer |  |
| `sack_fumbles_lost` | integer |  |
| `passing_air_yards` | double |  |
| `passing_yards_after_catch` | double |  |
| `passing_first_downs` | double |  |
| `passing_epa` | double |  |
| `passing_2pt_conversions` | integer |  |
| `pacr` | double |  |
| `dakota` | double |  |
| `carries` | integer |  |
| `rushing_yards` | double |  |
| `rushing_tds` | integer |  |
| `rushing_fumbles` | double |  |
| `rushing_fumbles_lost` | double |  |
| `rushing_first_downs` | double |  |
| `rushing_epa` | double |  |
| `rushing_2pt_conversions` | integer |  |
| `receptions` | integer |  |
| `targets` | integer |  |
| `receiving_yards` | double |  |
| `receiving_tds` | integer |  |
| `receiving_fumbles` | double |  |
| `receiving_fumbles_lost` | double |  |
| `receiving_air_yards` | double |  |
| `receiving_yards_after_catch` | double |  |
| `receiving_first_downs` | double |  |
| `receiving_epa` | double |  |
| `receiving_2pt_conversions` | integer |  |
| `racr` | double |  |
| `target_share` | double |  |
| `air_yards_share` | double |  |
| `wopr` | double |  |
| `special_teams_tds` | double |  |
| `fantasy_points` | double |  |
| `fantasy_points_ppr` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

Kicking-only stats::

kicking = load_nfl_player_stats(kicking=True)

Filter to a single season after load::

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_nfl_players(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Player ID information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing players available.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `game_key` | character |  |
| `official_name` | character |  |
| `position` | character |  |
| `jersey_number` | integer |  |
| `official_id` | character |  |
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
players.shape

Pandas round-trip::

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()
```

### `load_nfl_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `team` | character |  |
| `position` | character |  |
| `depth_chart_position` | character |  |
| `jersey_number` | integer |  |
| `status` | character |  |
| `full_name` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `birth_date` | character |  |
| `height` | double |  |
| `weight` | integer |  |
| `college` | character |  |
| `gsis_id` | character |  |
| `espn_id` | character |  |
| `sportradar_id` | character |  |
| `yahoo_id` | character |  |
| `rotowire_id` | character |  |
| `pff_id` | character |  |
| `pfr_id` | character |  |
| `fantasy_data_id` | character |  |
| `sleeper_id` | character |  |
| `years_exp` | integer |  |
| `headshot_url` | character |  |
| `ngs_position` | character |  |
| `week` | integer |  |
| `game_type` | character |  |
| `status_description_abbr` | character |  |
| `football_name` | character |  |
| `esb_id` | character |  |
| `gsis_it_id` | character |  |
| `smart_id` | character |  |
| `entry_year` | integer |  |
| `rookie_year` | integer |  |
| `draft_club` | character |  |
| `draft_number` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

Multi-season range::

rosters = load_nfl_rosters(seasons=range(2020, 2025))

Filter to a single team::

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")
```

### `load_nfl_schedule(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `game_id` | character |  |
| `season` | integer |  |
| `game_type` | character |  |
| `week` | integer |  |
| `gameday` | character |  |
| `weekday` | character |  |
| `gametime` | character |  |
| `away_team` | character |  |
| `away_score` | integer |  |
| `home_team` | character |  |
| `home_score` | integer |  |
| `location` | character |  |
| `result` | integer |  |
| `total` | integer |  |
| `overtime` | integer |  |
| `old_game_id` | character |  |
| `gsis` | integer |  |
| `nfl_detail_id` | character |  |
| `pfr` | character |  |
| `pff` | integer |  |
| `espn` | character |  |
| `ftn` | integer |  |
| `away_rest` | integer |  |
| `home_rest` | integer |  |
| `away_moneyline` | integer |  |
| `home_moneyline` | integer |  |
| `spread_line` | double |  |
| `away_spread_odds` | integer |  |
| `home_spread_odds` | integer |  |
| `total_line` | double |  |
| `under_odds` | integer |  |
| `over_odds` | integer |  |
| `div_game` | integer |  |
| `roof` | character |  |
| `surface` | character |  |
| `temp` | integer |  |
| `wind` | integer |  |
| `away_qb_id` | character |  |
| `home_qb_id` | character |  |
| `away_qb_name` | character |  |
| `home_qb_name` | character |  |
| `away_coach` | character |  |
| `home_coach` | character |  |
| `referee` | character |  |
| `stadium_id` | character |  |
| `stadium` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

Multi-season range::

schedule = load_nfl_schedule(seasons=range(2020, 2025))

Filter to a single week::

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

Pandas round-trip::

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_nfl_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `game_type` | character |  |
| `week` | integer |  |
| `player` | character |  |
| `pfr_player_id` | character |  |
| `position` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `offense_snaps` | double |  |
| `offense_pct` | double |  |
| `defense_snaps` | double |  |
| `defense_pct` | double |  |
| `st_snaps` | double |  |
| `st_pct` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

Multi-season range with offense-only filter::

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

### `load_nfl_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `week` | integer |  |
| `team` | character |  |
| `season_type` | character |  |
| `opponent_team` | character |  |
| `completions` | integer |  |
| `attempts` | integer |  |
| `passing_yards` | integer |  |
| `passing_tds` | integer |  |
| `passing_interceptions` | integer |  |
| `sacks_suffered` | integer |  |
| `sack_yards_lost` | integer |  |
| `sack_fumbles` | integer |  |
| `sack_fumbles_lost` | integer |  |
| `passing_air_yards` | integer |  |
| `passing_yards_after_catch` | integer |  |
| `passing_first_downs` | integer |  |
| `passing_epa` | double |  |
| `passing_cpoe` | double |  |
| `passing_2pt_conversions` | integer |  |
| `carries` | integer |  |
| `rushing_yards` | integer |  |
| `rushing_tds` | integer |  |
| `rushing_fumbles` | integer |  |
| `rushing_fumbles_lost` | integer |  |
| `rushing_first_downs` | integer |  |
| `rushing_epa` | double |  |
| `rushing_2pt_conversions` | integer |  |
| `receptions` | integer |  |
| `targets` | integer |  |
| `receiving_yards` | integer |  |
| `receiving_tds` | integer |  |
| `receiving_fumbles` | integer |  |
| `receiving_fumbles_lost` | integer |  |
| `receiving_air_yards` | integer |  |
| `receiving_yards_after_catch` | integer |  |
| `receiving_first_downs` | integer |  |
| `receiving_epa` | double |  |
| `receiving_2pt_conversions` | integer |  |
| `special_teams_tds` | integer |  |
| `def_tackles_solo` | integer |  |
| `def_tackles_with_assist` | integer |  |
| `def_tackle_assists` | integer |  |
| `def_tackles_for_loss` | integer |  |
| `def_tackles_for_loss_yards` | integer |  |
| `def_fumbles_forced` | integer |  |
| `def_sacks` | double |  |
| `def_sack_yards` | double |  |
| `def_qb_hits` | integer |  |
| `def_interceptions` | integer |  |
| `def_interception_yards` | integer |  |
| `def_pass_defended` | integer |  |
| `def_tds` | integer |  |
| `def_fumbles` | integer |  |
| `def_safeties` | integer |  |
| `misc_yards` | integer |  |
| `fumble_recovery_own` | integer |  |
| `fumble_recovery_yards_own` | integer |  |
| `fumble_recovery_opp` | integer |  |
| `fumble_recovery_yards_opp` | integer |  |
| `fumble_recovery_tds` | integer |  |
| `penalties` | integer |  |
| `penalty_yards` | integer |  |
| `timeouts` | integer |  |
| `punt_returns` | integer |  |
| `punt_return_yards` | integer |  |
| `kickoff_returns` | integer |  |
| `kickoff_return_yards` | integer |  |
| `fg_made` | integer |  |
| `fg_att` | integer |  |
| `fg_missed` | integer |  |
| `fg_blocked` | integer |  |
| `fg_long` | integer |  |
| `fg_pct` | double |  |
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

Regular-season-only team stats::

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

Combined regular + post-season at season grain::

combined = load_nfl_team_stats(seasons=[2023, 2024], summary_level="reg+post")
```

### `load_nfl_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_abbr` | character |  |
| `team_name` | character |  |
| `team_id` | integer |  |
| `team_nick` | character |  |
| `team_conf` | character |  |
| `team_division` | character |  |
| `team_color` | character |  |
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

Pandas round-trip::

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_nfl_trades(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

| col_name | type | description |
|---|---|---|
| `trade_id` | integer |  |
| `season` | integer |  |
| `trade_date` | character |  |
| `gave` | character |  |
| `received` | character |  |
| `pick_season` | integer |  |
| `pick_round` | integer |  |
| `pick_number` | integer |  |
| `conditional` | integer |  |
| `pfr_id` | character |  |
| `pfr_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

Filter to a single season::

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

### `load_nfl_weekly_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `team` | character |  |
| `position` | character |  |
| `depth_chart_position` | character |  |
| `jersey_number` | integer |  |
| `status` | character |  |
| `full_name` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `birth_date` | character |  |
| `height` | double |  |
| `weight` | integer |  |
| `college` | character |  |
| `gsis_id` | character |  |
| `espn_id` | character |  |
| `sportradar_id` | character |  |
| `yahoo_id` | character |  |
| `rotowire_id` | character |  |
| `pff_id` | character |  |
| `pfr_id` | character |  |
| `fantasy_data_id` | character |  |
| `sleeper_id` | character |  |
| `years_exp` | integer |  |
| `headshot_url` | character |  |
| `ngs_position` | character |  |
| `week` | integer |  |
| `game_type` | character |  |
| `status_description_abbr` | character |  |
| `football_name` | character |  |
| `esb_id` | character |  |
| `gsis_it_id` | character |  |
| `smart_id` | character |  |
| `entry_year` | integer |  |
| `rookie_year` | integer |  |
| `draft_club` | character |  |
| `draft_number` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

Multi-season range with a follow-up week filter::

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### `load_officials(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `game_key` | character |  |
| `official_name` | character |  |
| `position` | character |  |
| `jersey_number` | integer |  |
| `official_id` | character |  |
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

Pandas round-trip::

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `nflverse_game_id` | character |  |
| `old_game_id` | character |  |
| `play_id` | double |  |
| `possession_team` | character |  |
| `offense_formation` | character |  |
| `offense_personnel` | character |  |
| `defenders_in_box` | integer |  |
| `defense_personnel` | character |  |
| `number_of_pass_rushers` | integer |  |
| `players_on_play` | character |  |
| `offense_players` | character |  |
| `defense_players` | character |  |
| `n_offense` | integer |  |
| `n_defense` | integer |  |
| `ngs_air_yards` | double |  |
| `time_to_throw` | double |  |
| `was_pressure` | logical |  |
| `route` | character |  |
| `defense_man_zone_type` | character |  |
| `defense_coverage_type` | character |  |
| `offense_names` | character |  |
| `defense_names` | character |  |
| `offense_positions` | character |  |
| `defense_positions` | character |  |
| `offense_numbers` | character |  |
| `defense_numbers` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

Multi-season range::

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### `load_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `play_id` | double |  |
| `game_id` | character |  |
| `old_game_id` | character |  |
| `home_team` | character |  |
| `away_team` | character |  |
| `season_type` | character |  |
| `week` | integer |  |
| `posteam` | character |  |
| `posteam_type` | character |  |
| `defteam` | character |  |
| `side_of_field` | character |  |
| `yardline_100` | double |  |
| `game_date` | character |  |
| `quarter_seconds_remaining` | double |  |
| `half_seconds_remaining` | double |  |
| `game_seconds_remaining` | double |  |
| `game_half` | character |  |
| `quarter_end` | double |  |
| `drive` | double |  |
| `sp` | double |  |
| `qtr` | double |  |
| `down` | double |  |
| `goal_to_go` | integer |  |
| `time` | character |  |
| `yrdln` | character |  |
| `ydstogo` | double |  |
| `ydsnet` | double |  |
| `desc` | character |  |
| `play_type` | character |  |
| `yards_gained` | double |  |
| `shotgun` | double |  |
| `no_huddle` | double |  |
| `qb_dropback` | double |  |
| `qb_kneel` | double |  |
| `qb_spike` | double |  |
| `qb_scramble` | double |  |
| `pass_length` | character |  |
| `pass_location` | character |  |
| `air_yards` | double |  |
| `yards_after_catch` | double |  |
| `run_location` | character |  |
| `run_gap` | character |  |
| `field_goal_result` | character |  |
| `kick_distance` | double |  |
| `extra_point_result` | character |  |
| `two_point_conv_result` | character |  |
| `home_timeouts_remaining` | double |  |
| `away_timeouts_remaining` | double |  |
| `timeout` | double |  |
| `timeout_team` | character |  |
| `td_team` | character |  |
| `td_player_name` | character |  |
| `td_player_id` | character |  |
| `posteam_timeouts_remaining` | double |  |
| `defteam_timeouts_remaining` | double |  |
| `total_home_score` | double |  |
| `total_away_score` | double |  |
| `posteam_score` | double |  |
| `defteam_score` | double |  |
| `score_differential` | double |  |
| `posteam_score_post` | double |  |
| `defteam_score_post` | double |  |
| `score_differential_post` | double |  |
| `no_score_prob` | double |  |
| `opp_fg_prob` | double |  |
| `opp_safety_prob` | double |  |
| `opp_td_prob` | double |  |
| `fg_prob` | double |  |
| `safety_prob` | double |  |
| `td_prob` | double |  |
| `extra_point_prob` | double |  |
| `two_point_conversion_prob` | double |  |
| `ep` | double |  |
| `epa` | double |  |
| `total_home_epa` | double |  |
| `total_away_epa` | double |  |
| `total_home_rush_epa` | double |  |
| `total_away_rush_epa` | double |  |
| `total_home_pass_epa` | double |  |
| `total_away_pass_epa` | double |  |
| `air_epa` | double |  |
| `yac_epa` | double |  |
| `comp_air_epa` | double |  |
| `comp_yac_epa` | double |  |
| `total_home_comp_air_epa` | double |  |
| `total_away_comp_air_epa` | double |  |
| `total_home_comp_yac_epa` | double |  |
| `total_away_comp_yac_epa` | double |  |
| `total_home_raw_air_epa` | double |  |
| `total_away_raw_air_epa` | double |  |
| `total_home_raw_yac_epa` | double |  |
| `total_away_raw_yac_epa` | double |  |
| `wp` | double |  |
| `def_wp` | double |  |
| `home_wp` | double |  |
| `away_wp` | double |  |
| `wpa` | double |  |
| `vegas_wpa` | double |  |
| `vegas_home_wpa` | double |  |
| `home_wp_post` | double |  |
| `away_wp_post` | double |  |
| `vegas_wp` | double |  |
| `vegas_home_wp` | double |  |
| `total_home_rush_wpa` | double |  |
| `total_away_rush_wpa` | double |  |
| `total_home_pass_wpa` | double |  |
| `total_away_pass_wpa` | double |  |
| `air_wpa` | double |  |
| `yac_wpa` | double |  |
| `comp_air_wpa` | double |  |
| `comp_yac_wpa` | double |  |
| `total_home_comp_air_wpa` | double |  |
| `total_away_comp_air_wpa` | double |  |
| `total_home_comp_yac_wpa` | double |  |
| `total_away_comp_yac_wpa` | double |  |
| `total_home_raw_air_wpa` | double |  |
| `total_away_raw_air_wpa` | double |  |
| `total_home_raw_yac_wpa` | double |  |
| `total_away_raw_yac_wpa` | double |  |
| `punt_blocked` | double |  |
| `first_down_rush` | double |  |
| `first_down_pass` | double |  |
| `first_down_penalty` | double |  |
| `third_down_converted` | double |  |
| `third_down_failed` | double |  |
| `fourth_down_converted` | double |  |
| `fourth_down_failed` | double |  |
| `incomplete_pass` | double |  |
| `touchback` | double |  |
| `interception` | double |  |
| `punt_inside_twenty` | double |  |
| `punt_in_endzone` | double |  |
| `punt_out_of_bounds` | double |  |
| `punt_downed` | double |  |
| `punt_fair_catch` | double |  |
| `kickoff_inside_twenty` | double |  |
| `kickoff_in_endzone` | double |  |
| `kickoff_out_of_bounds` | double |  |
| `kickoff_downed` | double |  |
| `kickoff_fair_catch` | double |  |
| `fumble_forced` | double |  |
| `fumble_not_forced` | double |  |
| `fumble_out_of_bounds` | double |  |
| `solo_tackle` | double |  |
| `safety` | double |  |
| `penalty` | double |  |
| `tackled_for_loss` | double |  |
| `fumble_lost` | double |  |
| `own_kickoff_recovery` | double |  |
| `own_kickoff_recovery_td` | double |  |
| `qb_hit` | double |  |
| `rush_attempt` | double |  |
| `pass_attempt` | double |  |
| `sack` | double |  |
| `touchdown` | double |  |
| `pass_touchdown` | double |  |
| `rush_touchdown` | double |  |
| `return_touchdown` | double |  |
| `extra_point_attempt` | double |  |
| `two_point_attempt` | double |  |
| `field_goal_attempt` | double |  |
| `kickoff_attempt` | double |  |
| `punt_attempt` | double |  |
| `fumble` | double |  |
| `complete_pass` | double |  |
| `assist_tackle` | double |  |
| `lateral_reception` | double |  |
| `lateral_rush` | double |  |
| `lateral_return` | double |  |
| `lateral_recovery` | double |  |
| `passer_player_id` | character |  |
| `passer_player_name` | character |  |
| `passing_yards` | double |  |
| `receiver_player_id` | character |  |
| `receiver_player_name` | character |  |
| `receiving_yards` | double |  |
| `rusher_player_id` | character |  |
| `rusher_player_name` | character |  |
| `rushing_yards` | double |  |
| `lateral_receiver_player_id` | character |  |
| `lateral_receiver_player_name` | character |  |
| `lateral_receiving_yards` | double |  |
| `lateral_rusher_player_id` | character |  |
| `lateral_rusher_player_name` | character |  |
| `lateral_rushing_yards` | double |  |
| `lateral_sack_player_id` | character |  |
| `lateral_sack_player_name` | character |  |
| `interception_player_id` | character |  |
| `interception_player_name` | character |  |
| `lateral_interception_player_id` | character |  |
| `lateral_interception_player_name` | character |  |
| `punt_returner_player_id` | character |  |
| `punt_returner_player_name` | character |  |
| `lateral_punt_returner_player_id` | character |  |
| `lateral_punt_returner_player_name` | character |  |
| `kickoff_returner_player_name` | character |  |
| `kickoff_returner_player_id` | character |  |
| `lateral_kickoff_returner_player_id` | character |  |
| `lateral_kickoff_returner_player_name` | character |  |
| `punter_player_id` | character |  |
| `punter_player_name` | character |  |
| `kicker_player_name` | character |  |
| `kicker_player_id` | character |  |
| `own_kickoff_recovery_player_id` | character |  |
| `own_kickoff_recovery_player_name` | character |  |
| `blocked_player_id` | character |  |
| `blocked_player_name` | character |  |
| `tackle_for_loss_1_player_id` | character |  |
| `tackle_for_loss_1_player_name` | character |  |
| `tackle_for_loss_2_player_id` | character |  |
| `tackle_for_loss_2_player_name` | character |  |
| `qb_hit_1_player_id` | character |  |
| `qb_hit_1_player_name` | character |  |
| `qb_hit_2_player_id` | character |  |
| `qb_hit_2_player_name` | character |  |
| `forced_fumble_player_1_team` | character |  |
| `forced_fumble_player_1_player_id` | character |  |
| `forced_fumble_player_1_player_name` | character |  |
| `forced_fumble_player_2_team` | character |  |
| `forced_fumble_player_2_player_id` | character |  |
| `forced_fumble_player_2_player_name` | character |  |
| `solo_tackle_1_team` | character |  |
| `solo_tackle_2_team` | character |  |
| `solo_tackle_1_player_id` | character |  |
| `solo_tackle_2_player_id` | character |  |
| `solo_tackle_1_player_name` | character |  |
| `solo_tackle_2_player_name` | character |  |
| `assist_tackle_1_player_id` | character |  |
| `assist_tackle_1_player_name` | character |  |
| `assist_tackle_1_team` | character |  |
| `assist_tackle_2_player_id` | character |  |
| `assist_tackle_2_player_name` | character |  |
| `assist_tackle_2_team` | character |  |
| `assist_tackle_3_player_id` | character |  |
| `assist_tackle_3_player_name` | character |  |
| `assist_tackle_3_team` | character |  |
| `assist_tackle_4_player_id` | character |  |
| `assist_tackle_4_player_name` | character |  |
| `assist_tackle_4_team` | character |  |
| `tackle_with_assist` | double |  |
| `tackle_with_assist_1_player_id` | character |  |
| `tackle_with_assist_1_player_name` | character |  |
| `tackle_with_assist_1_team` | character |  |
| `tackle_with_assist_2_player_id` | character |  |
| `tackle_with_assist_2_player_name` | character |  |
| `tackle_with_assist_2_team` | character |  |
| `pass_defense_1_player_id` | character |  |
| `pass_defense_1_player_name` | character |  |
| `pass_defense_2_player_id` | character |  |
| `pass_defense_2_player_name` | character |  |
| `fumbled_1_team` | character |  |
| `fumbled_1_player_id` | character |  |
| `fumbled_1_player_name` | character |  |
| `fumbled_2_player_id` | character |  |
| `fumbled_2_player_name` | character |  |
| `fumbled_2_team` | character |  |
| `fumble_recovery_1_team` | character |  |
| `fumble_recovery_1_yards` | double |  |
| `fumble_recovery_1_player_id` | character |  |
| `fumble_recovery_1_player_name` | character |  |
| `fumble_recovery_2_team` | character |  |
| `fumble_recovery_2_yards` | double |  |
| `fumble_recovery_2_player_id` | character |  |
| `fumble_recovery_2_player_name` | character |  |
| `sack_player_id` | character |  |
| `sack_player_name` | character |  |
| `half_sack_1_player_id` | character |  |
| `half_sack_1_player_name` | character |  |
| `half_sack_2_player_id` | character |  |
| `half_sack_2_player_name` | character |  |
| `return_team` | character |  |
| `return_yards` | double |  |
| `penalty_team` | character |  |
| `penalty_player_id` | character |  |
| `penalty_player_name` | character |  |
| `penalty_yards` | double |  |
| `replay_or_challenge` | double |  |
| `replay_or_challenge_result` | character |  |
| `penalty_type` | character |  |
| `defensive_two_point_attempt` | double |  |
| `defensive_two_point_conv` | double |  |
| `defensive_extra_point_attempt` | double |  |
| `defensive_extra_point_conv` | double |  |
| `safety_player_name` | character |  |
| `safety_player_id` | character |  |
| `season` | integer |  |
| `cp` | double |  |
| `cpoe` | double |  |
| `series` | double |  |
| `series_success` | double |  |
| `series_result` | character |  |
| `order_sequence` | double |  |
| `start_time` | character |  |
| `time_of_day` | character |  |
| `stadium` | character |  |
| `weather` | character |  |
| `nfl_api_id` | character |  |
| `play_clock` | character |  |
| `play_deleted` | double |  |
| `play_type_nfl` | character |  |
| `special_teams_play` | double |  |
| `st_play_type` | character |  |
| `end_clock_time` | character |  |
| `end_yard_line` | character |  |
| `fixed_drive` | double |  |
| `fixed_drive_result` | character |  |
| `drive_real_start_time` | character |  |
| `drive_play_count` | double |  |
| `drive_time_of_possession` | character |  |
| `drive_first_downs` | double |  |
| `drive_inside20` | double |  |
| `drive_ended_with_score` | double |  |
| `drive_quarter_start` | double |  |
| `drive_quarter_end` | double |  |
| `drive_yards_penalized` | double |  |
| `drive_start_transition` | character |  |
| `drive_end_transition` | character |  |
| `drive_game_clock_start` | character |  |
| `drive_game_clock_end` | character |  |
| `drive_start_yard_line` | character |  |
| `drive_end_yard_line` | character |  |
| `drive_play_id_started` | double |  |
| `drive_play_id_ended` | double |  |
| `away_score` | integer |  |
| `home_score` | integer |  |
| `location` | character |  |
| `result` | integer |  |
| `total` | integer |  |
| `spread_line` | double |  |
| `total_line` | double |  |
| `div_game` | integer |  |
| `roof` | character |  |
| `surface` | character |  |
| `temp` | integer |  |
| `wind` | integer |  |
| `home_coach` | character |  |
| `away_coach` | character |  |
| `stadium_id` | character |  |
| `game_stadium` | character |  |
| `aborted_play` | double |  |
| `success` | double |  |
| `passer` | character |  |
| `passer_jersey_number` | integer |  |
| `rusher` | character |  |
| `rusher_jersey_number` | integer |  |
| `receiver` | character |  |
| `receiver_jersey_number` | integer |  |
| `pass` | double |  |
| `rush` | double |  |
| `first_down` | double |  |
| `special` | double |  |
| `play` | double |  |
| `passer_id` | character |  |
| `rusher_id` | character |  |
| `receiver_id` | character |  |
| `name` | character |  |
| `jersey_number` | integer |  |
| `id` | character |  |
| `fantasy_player_name` | character |  |
| `fantasy_player_id` | character |  |
| `fantasy` | character |  |
| `fantasy_id` | character |  |
| `out_of_bounds` | double |  |
| `home_opening_kickoff` | double |  |
| `qb_epa` | double |  |
| `xyac_epa` | double |  |
| `xyac_mean_yardage` | double |  |
| `xyac_median_yardage` | integer |  |
| `xyac_success` | double |  |
| `xyac_fd` | double |  |
| `xpass` | double |  |
| `pass_oe` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

Multi-season range::

pbp = load_nfl_pbp(seasons=range(2020, 2025))

With cache off (development workflow)::

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

Pandas round-trip::

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

### `load_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level PFR advstats accessors. Mirrors the API surface of nflreadpy's ``load_pfr_advstats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For ``summary_level='week'`` this drives the per-season parquet fan-out; for ``summary_level='season'`` it post-filters the combined parquet by the ``season`` column. |
| `stat_type` | `str` | `'pass'` | One of ``"pass"``, ``"rush"``, ``"rec"``, ``"def"``. Defaults to ``"pass"``. |
| `summary_level` | `str` | `'week'` | One of ``"week"`` or ``"season"``. Defaults to ``"week"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested ``stat_type``, ``summary_level``, and ``seasons``.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `game_type` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `pfr_player_name` | character |  |
| `pfr_player_id` | character |  |
| `passing_drops` | double |  |
| `passing_drop_pct` | double |  |
| `receiving_drop` | double |  |
| `receiving_drop_pct` | double |  |
| `passing_bad_throws` | double |  |
| `passing_bad_throw_pct` | double |  |
| `times_sacked` | double |  |
| `times_blitzed` | double |  |
| `times_hurried` | double |  |
| `times_hit` | double |  |
| `times_pressured` | double |  |
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

Season-level rushing summaries (one row per player per season)::

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

Defensive stats with a follow-up filter::

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

Pandas round-trip::

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'`

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
| `player_id` | character |  |
| `player_name` | character |  |
| `player_display_name` | character |  |
| `position` | character |  |
| `position_group` | character |  |
| `headshot_url` | character |  |
| `recent_team` | character |  |
| `season` | integer |  |
| `week` | integer |  |
| `season_type` | character |  |
| `opponent_team` | character |  |
| `completions` | integer |  |
| `attempts` | integer |  |
| `passing_yards` | double |  |
| `passing_tds` | integer |  |
| `interceptions` | double |  |
| `sacks` | double |  |
| `sack_yards` | double |  |
| `sack_fumbles` | integer |  |
| `sack_fumbles_lost` | integer |  |
| `passing_air_yards` | double |  |
| `passing_yards_after_catch` | double |  |
| `passing_first_downs` | double |  |
| `passing_epa` | double |  |
| `passing_2pt_conversions` | integer |  |
| `pacr` | double |  |
| `dakota` | double |  |
| `carries` | integer |  |
| `rushing_yards` | double |  |
| `rushing_tds` | integer |  |
| `rushing_fumbles` | double |  |
| `rushing_fumbles_lost` | double |  |
| `rushing_first_downs` | double |  |
| `rushing_epa` | double |  |
| `rushing_2pt_conversions` | integer |  |
| `receptions` | integer |  |
| `targets` | integer |  |
| `receiving_yards` | double |  |
| `receiving_tds` | integer |  |
| `receiving_fumbles` | double |  |
| `receiving_fumbles_lost` | double |  |
| `receiving_air_yards` | double |  |
| `receiving_yards_after_catch` | double |  |
| `receiving_first_downs` | double |  |
| `receiving_epa` | double |  |
| `receiving_2pt_conversions` | integer |  |
| `racr` | double |  |
| `target_share` | double |  |
| `air_yards_share` | double |  |
| `wopr` | double |  |
| `special_teams_tds` | double |  |
| `fantasy_points` | double |  |
| `fantasy_points_ppr` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

Kicking-only stats::

kicking = load_nfl_player_stats(kicking=True)

Filter to a single season after load::

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_players(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Player ID information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing players available.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `game_key` | character |  |
| `official_name` | character |  |
| `position` | character |  |
| `jersey_number` | integer |  |
| `official_id` | character |  |
| `season` | integer |  |
| `season_type` | character |  |
| `week` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
players.shape

Pandas round-trip::

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()
```

### `load_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `team` | character |  |
| `position` | character |  |
| `depth_chart_position` | character |  |
| `jersey_number` | integer |  |
| `status` | character |  |
| `full_name` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `birth_date` | character |  |
| `height` | double |  |
| `weight` | integer |  |
| `college` | character |  |
| `gsis_id` | character |  |
| `espn_id` | character |  |
| `sportradar_id` | character |  |
| `yahoo_id` | character |  |
| `rotowire_id` | character |  |
| `pff_id` | character |  |
| `pfr_id` | character |  |
| `fantasy_data_id` | character |  |
| `sleeper_id` | character |  |
| `years_exp` | integer |  |
| `headshot_url` | character |  |
| `ngs_position` | character |  |
| `week` | integer |  |
| `game_type` | character |  |
| `status_description_abbr` | character |  |
| `football_name` | character |  |
| `esb_id` | character |  |
| `gsis_it_id` | character |  |
| `smart_id` | character |  |
| `entry_year` | integer |  |
| `rookie_year` | integer |  |
| `draft_club` | character |  |
| `draft_number` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

Multi-season range::

rosters = load_nfl_rosters(seasons=range(2020, 2025))

Filter to a single team::

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")
```

### `load_rosters_weekly(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `team` | character |  |
| `position` | character |  |
| `depth_chart_position` | character |  |
| `jersey_number` | integer |  |
| `status` | character |  |
| `full_name` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `birth_date` | character |  |
| `height` | double |  |
| `weight` | integer |  |
| `college` | character |  |
| `gsis_id` | character |  |
| `espn_id` | character |  |
| `sportradar_id` | character |  |
| `yahoo_id` | character |  |
| `rotowire_id` | character |  |
| `pff_id` | character |  |
| `pfr_id` | character |  |
| `fantasy_data_id` | character |  |
| `sleeper_id` | character |  |
| `years_exp` | integer |  |
| `headshot_url` | character |  |
| `ngs_position` | character |  |
| `week` | integer |  |
| `game_type` | character |  |
| `status_description_abbr` | character |  |
| `football_name` | character |  |
| `esb_id` | character |  |
| `gsis_it_id` | character |  |
| `smart_id` | character |  |
| `entry_year` | integer |  |
| `rookie_year` | integer |  |
| `draft_club` | character |  |
| `draft_number` | integer |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

Multi-season range with a follow-up week filter::

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### `load_schedules(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `game_id` | character |  |
| `season` | integer |  |
| `game_type` | character |  |
| `week` | integer |  |
| `gameday` | character |  |
| `weekday` | character |  |
| `gametime` | character |  |
| `away_team` | character |  |
| `away_score` | integer |  |
| `home_team` | character |  |
| `home_score` | integer |  |
| `location` | character |  |
| `result` | integer |  |
| `total` | integer |  |
| `overtime` | integer |  |
| `old_game_id` | character |  |
| `gsis` | integer |  |
| `nfl_detail_id` | character |  |
| `pfr` | character |  |
| `pff` | integer |  |
| `espn` | character |  |
| `ftn` | integer |  |
| `away_rest` | integer |  |
| `home_rest` | integer |  |
| `away_moneyline` | integer |  |
| `home_moneyline` | integer |  |
| `spread_line` | double |  |
| `away_spread_odds` | integer |  |
| `home_spread_odds` | integer |  |
| `total_line` | double |  |
| `under_odds` | integer |  |
| `over_odds` | integer |  |
| `div_game` | integer |  |
| `roof` | character |  |
| `surface` | character |  |
| `temp` | integer |  |
| `wind` | integer |  |
| `away_qb_id` | character |  |
| `home_qb_id` | character |  |
| `away_qb_name` | character |  |
| `home_qb_name` | character |  |
| `away_coach` | character |  |
| `home_coach` | character |  |
| `referee` | character |  |
| `stadium_id` | character |  |
| `stadium` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

Multi-season range::

schedule = load_nfl_schedule(seasons=range(2020, 2025))

Filter to a single week::

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

Pandas round-trip::

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `game_id` | character |  |
| `pfr_game_id` | character |  |
| `season` | integer |  |
| `game_type` | character |  |
| `week` | integer |  |
| `player` | character |  |
| `pfr_player_id` | character |  |
| `position` | character |  |
| `team` | character |  |
| `opponent` | character |  |
| `offense_snaps` | double |  |
| `offense_pct` | double |  |
| `defense_snaps` | double |  |
| `defense_pct` | double |  |
| `st_snaps` | double |  |
| `st_pct` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

Multi-season range with offense-only filter::

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

### `load_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `season` | integer |  |
| `week` | integer |  |
| `team` | character |  |
| `season_type` | character |  |
| `opponent_team` | character |  |
| `completions` | integer |  |
| `attempts` | integer |  |
| `passing_yards` | integer |  |
| `passing_tds` | integer |  |
| `passing_interceptions` | integer |  |
| `sacks_suffered` | integer |  |
| `sack_yards_lost` | integer |  |
| `sack_fumbles` | integer |  |
| `sack_fumbles_lost` | integer |  |
| `passing_air_yards` | integer |  |
| `passing_yards_after_catch` | integer |  |
| `passing_first_downs` | integer |  |
| `passing_epa` | double |  |
| `passing_cpoe` | double |  |
| `passing_2pt_conversions` | integer |  |
| `carries` | integer |  |
| `rushing_yards` | integer |  |
| `rushing_tds` | integer |  |
| `rushing_fumbles` | integer |  |
| `rushing_fumbles_lost` | integer |  |
| `rushing_first_downs` | integer |  |
| `rushing_epa` | double |  |
| `rushing_2pt_conversions` | integer |  |
| `receptions` | integer |  |
| `targets` | integer |  |
| `receiving_yards` | integer |  |
| `receiving_tds` | integer |  |
| `receiving_fumbles` | integer |  |
| `receiving_fumbles_lost` | integer |  |
| `receiving_air_yards` | integer |  |
| `receiving_yards_after_catch` | integer |  |
| `receiving_first_downs` | integer |  |
| `receiving_epa` | double |  |
| `receiving_2pt_conversions` | integer |  |
| `special_teams_tds` | integer |  |
| `def_tackles_solo` | integer |  |
| `def_tackles_with_assist` | integer |  |
| `def_tackle_assists` | integer |  |
| `def_tackles_for_loss` | integer |  |
| `def_tackles_for_loss_yards` | integer |  |
| `def_fumbles_forced` | integer |  |
| `def_sacks` | double |  |
| `def_sack_yards` | double |  |
| `def_qb_hits` | integer |  |
| `def_interceptions` | integer |  |
| `def_interception_yards` | integer |  |
| `def_pass_defended` | integer |  |
| `def_tds` | integer |  |
| `def_fumbles` | integer |  |
| `def_safeties` | integer |  |
| `misc_yards` | integer |  |
| `fumble_recovery_own` | integer |  |
| `fumble_recovery_yards_own` | integer |  |
| `fumble_recovery_opp` | integer |  |
| `fumble_recovery_yards_opp` | integer |  |
| `fumble_recovery_tds` | integer |  |
| `penalties` | integer |  |
| `penalty_yards` | integer |  |
| `timeouts` | integer |  |
| `punt_returns` | integer |  |
| `punt_return_yards` | integer |  |
| `kickoff_returns` | integer |  |
| `kickoff_return_yards` | integer |  |
| `fg_made` | integer |  |
| `fg_att` | integer |  |
| `fg_missed` | integer |  |
| `fg_blocked` | integer |  |
| `fg_long` | integer |  |
| `fg_pct` | double |  |
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

Regular-season-only team stats::

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

Combined regular + post-season at season grain::

combined = load_nfl_team_stats(seasons=[2023, 2024], summary_level="reg+post")
```

### `load_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_abbr` | character |  |
| `team_name` | character |  |
| `team_id` | integer |  |
| `team_nick` | character |  |
| `team_conf` | character |  |
| `team_division` | character |  |
| `team_color` | character |  |
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

Pandas round-trip::

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_trades(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

| col_name | type | description |
|---|---|---|
| `trade_id` | integer |  |
| `season` | integer |  |
| `trade_date` | character |  |
| `gave` | character |  |
| `received` | character |  |
| `pick_season` | integer |  |
| `pick_round` | integer |  |
| `pick_number` | integer |  |
| `conditional` | integer |  |
| `pfr_id` | character |  |
| `pfr_name` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

Filter to a single season::

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

## Utilities & helpers

### `NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, **kwargs)`

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN ``summary`` endpoint (or a local JSON dump) and pipes the result through a chain of feature-engineering steps -- down/distance, play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced box score. Use ``run_processing_pipeline()`` for the full feature set or ``run_cleaning_pipeline()`` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` | `int` | `0` | ESPN ``event`` id (e.g. ``401671801``). |
| `raw` | `bool` | `False` | If ``True``, ``espn_nfl_pbp()`` returns the ESPN payload untouched. If ``False`` (default), it normalizes keys. |
| `path_to_json` | `str` | `'/'` | Directory containing ``{gameId}.json`` for the ``nfl_pbp_disk()`` flow (offline replay). |
| `return_keys` | `list[str] \| None` | `None` | If supplied, ``run_processing_pipeline`` returns only the listed keys from the result dict. |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

Offline replay from a JSON dump::

proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
cleaned = proc.run_cleaning_pipeline()

Subset the return payload::

proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())  # ['boxscore', 'plays']
```

### `get_current_nfl_season(roster: 'bool' = False) -> 'int'`

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

Roster-year semantics (March 15 cutover)::

roster_year = get_current_nfl_season(roster=True)

Pair with a loader to fetch only the active season::

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_nfl_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

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

Schedule-driven week (hits the live schedule parquet)::

week_live = get_current_nfl_week(use_date=False)

Roster-year season inference::

week_roster = get_current_nfl_week(roster=True)

Pair with a PBP fetch to grab only the most recent season+week::

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `get_current_season(roster: 'bool' = False) -> 'int'`

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

Roster-year semantics (March 15 cutover)::

roster_year = get_current_nfl_season(roster=True)

Pair with a loader to fetch only the active season::

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

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

Schedule-driven week (hits the live schedule parquet)::

week_live = get_current_nfl_week(use_date=False)

Roster-year season inference::

week_roster = get_current_nfl_week(roster=True)

Pair with a PBP fetch to grab only the most recent season+week::

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `most_recent_nfl_season(roster: 'bool' = False) -> 'int'`

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

Roster-year flavor::

roster_year = most_recent_nfl_season(roster=True)
```

## Other

### `NflConfig(cache_mode: 'CacheMode' = 'memory', cache_dir: 'Optional[Path]' = None, cache_duration: 'int' = 86400, verbose: 'bool' = True, timeout: 'int' = 30, user_agent: 'str' = 'sportsdataverse-py-nfl') -> None`

Runtime configuration for sdv-py NFL loaders.

Fields mirror nflreadpy's ``NflreadpyConfig`` so users can swap engines without changing call sites. The defaults are conservative: in-memory caching with a 24-hour TTL, verbose progress bars on, 30-second HTTP timeout.

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

Construct a fresh instance directly (rarely needed -- prefer
``update_config``)::

from sportsdataverse.nfl import NflConfig
cfg = NflConfig(cache_mode="off", timeout=10)
```

### `cached_loader(func: 'F') -> 'F'`

Decorator that adds caching to a ``load_nfl_*`` function.

Honors the active ``NflConfig.cache_mode``: - ``memory``: dict-based per-process cache. - ``filesystem``: parquet-based cross-process cache under ``cache_dir``. - ``off``: no caching, function runs every time. The cache key is the hash of ``(qualified_name, args, kwargs)`` with ``return_as_pandas`` excluded so memory / disk hits work regardless of which return shape the caller asked for. The cache always stores the polars frame internally and converts to pandas on read when requested.

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

Switch caching modes at runtime::

from sportsdataverse.nfl import clear_cache, update_config

update_config(cache_mode="filesystem")  # parquet-on-disk reuse
df3 = load_my_thing(2024)               # writes parquet under cache_dir
clear_cache()                           # wipe both memory + filesystem
update_config(cache_mode="off")         # bypass cache entirely
```

### `clear_cache() -> 'None'`

Clear both memory and filesystem caches.

Memory: empties the in-process dict. Filesystem: removes all entries under ``config.cache_dir``. The directory itself is preserved so subsequent writes succeed without needing ``mkdir``.

**Example**

```python
from sportsdataverse.nfl import clear_cache, load_nfl_pbp
clear_cache()
pbp = load_nfl_pbp(seasons=[2024])

Pair with a cache-mode switch::

from sportsdataverse.nfl import clear_cache, update_config
update_config(cache_mode="filesystem")
# ... lots of cached calls accumulate parquet files on disk ...
clear_cache()  # wipe disk + memory together
```

### `espn_nfl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_teams - look up NFL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nfl.espn_nfl_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character |  |
| `team_alternate_color` | character |  |
| `team_color` | character |  |
| `team_display_name` | character |  |
| `team_id` | character |  |
| `team_is_active` | logical |  |
| `team_is_all_star` | logical |  |
| `team_location` | character |  |
| `team_logos` | integer |  |
| `team_name` | character |  |
| `team_nickname` | character |  |
| `team_short_display_name` | character |  |
| `team_slug` | character |  |
| `team_uid` | character |  |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_teams
teams = espn_nfl_teams()
teams.shape

Pandas round-trip::

teams_pd = espn_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbreviation", "team_display_name"]].head()

Force a refresh after upstream ESPN updates::

espn_nfl_teams.cache_clear()  # underlying lru_cache
teams = espn_nfl_teams()
```

### `get_config() -> 'NflConfig'`

Return the live ``NflConfig`` singleton.

The same object is returned on every call; mutate via ``update_config`` rather than reassigning fields directly so future hooks (e.g. logging on config change) have a single choke point.

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()
print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)

Pair with ``update_config`` to verify a change took effect::

from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="off")
assert get_config().cache_mode == "off"
```

### `nfl_game_details(game_id=None, headers=None, raw=False) -> 'Dict'`

nfl_game_details() -- pull full ``api.nfl.com`` game details by game id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | UUID-style game id from ``api.nfl.com`` (e.g. ``'7ae87c4c-d24c-11ec-b23d-d15a91047884'``). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict (skip the auth roundtrip). Defaults to a fresh ``nfl_headers_gen()`` call. |
| `raw` | `bool` | `False` | If True, return the ESPN payload untouched. If False (default), normalize keys to the expected schema (filling missing keys with empty dicts/lists). |

**Returns**

Dictionary of game details (drives, plays, scoring summaries, timeouts, weather, attendance, etc.).

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_details
details = nfl_game_details(game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884")
sorted(details.keys())[:5]

Reuse headers across many calls (avoids re-minting tokens)::

from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
hdrs = nfl_headers_gen()
details = nfl_game_details(
    game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", headers=hdrs
)

Raw passthrough::

raw = nfl_game_details(
    game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", raw=True
)
```

### `nfl_game_schedule(season=2021, season_type='REG', week=1, headers=None, raw=False) -> 'Dict'`

nfl_game_schedule() -- list ``api.nfl.com`` games for a season/week slice.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2021` | season year (e.g. ``2024``). |
| `season_type` | `str` | `'REG'` | season type. One of ``"REG"`` or ``"POST"``. |
| `week` | `int` | `1` | week number (1-18 regular season, 1-4 post-season). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict. Defaults to a fresh ``nfl_headers_gen()`` call. |
| `raw` | `bool` | `False` | Currently ignored -- the function always returns the raw NFL.com summary payload. |

**Returns**

Dictionary with the games list under ``"games"`` plus pagination metadata.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_schedule
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)

Wild Card weekend (post-season)::

wild_card = nfl_game_schedule(season=2023, season_type="POST", week=1)

Reuse headers across many calls::

from sportsdataverse.nfl.nfl_games import nfl_game_schedule, nfl_headers_gen
hdrs = nfl_headers_gen()
for week in range(1, 19):
    summary = nfl_game_schedule(
        season=2024, season_type="REG", week=week, headers=hdrs,
    )
```

### `nfl_headers_gen()`

Build the full request-header dict expected by ``api.nfl.com``.

Mints a fresh bearer token via :func:`nfl_token_gen` and combines it with the browser-style headers (``Origin``, ``Referer``, ``User-Agent``, ``Sec-Fetch-*``, etc.) the NFL.com web app sends on every request.

**Returns**

Header dict ready to drop into ``requests.get``.

**Example**

```python
from sportsdataverse.nfl.nfl_games import (
    nfl_headers_gen, nfl_game_schedule,
)
hdrs = nfl_headers_gen()
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
```

### `nfl_token_gen()`

Mint a fresh ``api.nfl.com`` access token via the public reroute endpoint.

Wraps the unauthenticated ``client_credentials`` grant the NFL.com web app uses. The returned bearer token is what ``nfl_headers_gen()`` puts on the ``Authorization`` header.

**Returns**

The access token string.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_token_gen
token = nfl_token_gen()
assert isinstance(token, str)

Pair with a downstream call (``nfl_headers_gen`` does this for you)::

import requests
token = nfl_token_gen()
headers = {"Authorization": f"Bearer {token}"}
```

### `reset_config() -> 'NflConfig'`

Reset the active config to its env-var-derived defaults.

Convenience for tests / interactive sessions that want to undo a chain of ``update_config()`` calls without restarting the interpreter.

**Example**

```python
from sportsdataverse.nfl import update_config, reset_config
update_config(cache_mode="off", timeout=5)
# ... do work ...
reset_config()  # back to env-derived defaults
```

### `scoreboard_event_parsing(event)`

Normalize one ESPN scoreboard ``event`` into a flatter shape.

Splits the competitors list into ``home`` / ``away`` siblings, hoists notes / broadcast metadata onto the competition root, and drops the fields the schedule helper does not need (``odds``, ``leaders``, ``geoBroadcasts``, etc.). Used internally by :func:`espn_nfl_schedule`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `Dict` |  | A single ``events[i]`` dict from the ESPN scoreboard endpoint. |

**Returns**

The mutated event dict with normalized ``home`` / ``away`` / broadcast keys.

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

### `update_config(**kwargs: 'object') -> 'NflConfig'`

Update the active config in place.

Pass keyword arguments matching ``NflConfig`` fields:: update_config(cache_mode="filesystem", cache_duration=3600) String values for ``cache_dir`` are coerced to ``pathlib.Path`` and ``~`` is expanded for convenience.

**Returns**

The (mutated) global config object, for chaining or inspection.

**Example**

```python
from sportsdataverse.nfl import update_config
update_config(cache_mode="filesystem", cache_duration=3600)

Disable caching for development::

update_config(cache_mode="off")

Point cache at a custom directory::

update_config(cache_dir="~/sdv-cache")
```
