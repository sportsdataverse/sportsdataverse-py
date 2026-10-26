---
title: "NHL — additional Python functions — ESPN"
sidebar_label: "ESPN"
sidebar_position: 1
description: "NHL — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — ESPN

### espn_nhl_game_rosters {#espn_nhl_game_rosters}

`espn_nhl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame | pd.DataFrame | dict'`

espn_nhl_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nhl_schedule(). |
| `raw` | `bool` | `False` | If True, returns the Core v2 competitors payload (dict) as ESPN sent it; its roster, team and athlete `$ref` links are not followed. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'logo_href', 'logo_dark_href', 'game_id' plus the athlete's `position_id`, `position_name`, `position_display_name`, `position_abbreviation` and `position_leaf` (a leaf position also `position_parent_href`). With `raw=True`, the competitors payload (dict).

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | ESPN athlete identifier (echoed from arg). |
| `athlete_uid` | String |  |
| `athlete_guid` | String |  |
| `athlete_type` | String |  |
| `alternate_id` | Int64 | Alternate player identifier. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `full_name` | String | Player full name. |
| `athlete_display_name` | String | Player display name. |
| `short_name` | String | Short game name. |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Formatted weight string. |
| `height` | Float64 | Player height in inches. |
| `display_height` | String | Formatted height string. |
| `age` | Int64 | Player age. |
| `date_of_birth` | String | Date of birth (ISO 8601). |
| `debut_year` | Int64 | Year of NHL debut. |
| `slug` | String | URL slug. |
| `jersey` | String | Jersey number. |
| `linked` | Boolean |  |
| `active` | Boolean | Whether athlete is currently active. |
| `alternate_ids_sdr` | String | Alternate ids sdr. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `birth_place_country` | String | Birth place country. |
| `birth_country_abbreviation` | String | Birth country abbreviation. |
| `headshot_href` | String | Player headshot image URL. |
| `headshot_alt` | String | Headshot alt text. |
| `hand_type` | String | Shooting/catching hand type. |
| `hand_abbreviation` | String | Hand abbreviation. |
| `hand_display_value` | String | Hand display value. |
| `contracts_href` | String | ESPN API hypermedia URL pointing to the contract history resource for this player. |
| `experience_years` | Int64 | Experience years. |
| `draft_display_text` | String |  |
| `draft_round` | Int64 | Draft round. |
| `draft_year` | Int64 | Draft year the lottery applies to. |
| `draft_selection` | Int64 |  |
| `draft_team_href` | String | ESPN API hypermedia URL linking to the team that originally drafted this player. |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status name. |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `position_id` | Int64 | ESPN's numeric identifier of the athlete's listed position (the Core v2 position resource id). |
| `position_name` | String | Name of the athlete's listed position from ESPN's position resource, e.g. Right Wing or Defense. |
| `position_display_name` | String | Display form of the athlete's listed position as ESPN renders it; usually equal to position_name. |
| `position_abbreviation` | String | Short code of the athlete's listed position (e.g. G, F, RW, OL), from ESPN's position resource. |
| `position_leaf` | Boolean | Whether the position is a leaf of ESPN's position tree; False marks a grouping, not a specific role. |
| `position_parent_href` | String | ESPN API URL of the broader position group above a leaf position; null for a grouping position. |
| `jersey_right` | String | Right-aligned display string for the player's jersey number, used in ESPN scoreboard rendering contexts. |
| `display_name` | String | Player display name. |
| `scratched` | Boolean | Whether the player was a healthy scratch. |
| `scratch_reason` | String | Reason for scratch (if applicable). |
| `athlete_href` | String | ESPN API hypermedia URL linking to the full athlete resource for this player, usable to fetch detailed biographical and statistical data. |
| `position_href` | String | ESPN API hypermedia URL pointing to the position resource that defines this player's positional classification. |
| `statistics_href` | String | ESPN API hypermedia URL linking to the statistics resource for this player's season or career totals. |
| `team_id` | Int64 | Unique team identifier. |
| `order` | Int64 | Display order within officials list. |
| `home_away` | String | Home or away indicator. |
| `winner` | Boolean | Whether this competitor won the game. |
| `team_guid` | String |  |
| `team_uid` | String | ESPN team uid. |
| `team_slug` | String | Team URL slug. |
| `team_location` | String | Team city/location. |
| `team_name` | String | Team name. |
| `team_nickname` | String | Team nickname. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_short_display_name` | String | Team short display name. |
| `team_color` | String | Team primary color hex. |
| `team_alternate_color` | String | Team alternate color hex. |
| `is_active` | Boolean | Whether the team is active. |
| `is_all_star` | Boolean | Whether the team is an all-star team. |
| `team_alternate_ids_sdr` | String | Alternate team identifier from the ESPN SDR (Sports Data Repository) system, used to cross-reference team records across ESPN data sources. |
| `logo_href` | String |  |
| `logo_dark_href` | String |  |
| `game_id` | Int64 | Unique game identifier. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN has no competitors, roster or athlete resource for the game (404), or every rostered athlete's `$ref` answered 404. A single 404 athlete is left out with a `UserWarning` and the rest of the roster is returned. |
| `AssetFetchError` | A fetch failed (403, 429, 5xx, a dropped connection); never treated as a missing athlete. |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_game_rosters
rosters = espn_nhl_game_rosters(game_id=401559395)
print(rosters.shape)
rosters.select(["athlete_display_name", "jersey", "team_abbreviation", "starter"]).head(10)

# Just the starters

import polars as pl
rosters.filter(pl.col("starter") == True).select(["athlete_display_name", "team_abbreviation"])

# Pandas round-trip

rosters_pd = espn_nhl_game_rosters(game_id=401559395, return_as_pandas=True)
rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()
```

### espn_nhl_pbp {#espn_nhl_pbp}

`espn_nhl_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_nhl_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique ESPN event id (NOT the NHL native game id), can be obtained from nhl_schedule(). |
| `raw` |  | `False` |  |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "pickcenter", "againstTheSpread", "odds", "onIce", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nhl import espn_nhl_pbp
game = espn_nhl_pbp(game_id=401559395)
list(game.keys())  # 'gameId', 'plays', 'boxscore', ...

# Inspect parsed plays and a quick filter on goal events

import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
goals = plays.filter(pl.col("type.text") == "Goal")
goals.select(["period", "time", "text"]).head()

# Pull the unparsed payload for custom downstream parsing

raw = espn_nhl_pbp(game_id=401559395, raw=True)
sorted(raw.keys())[:5]
```

### espn_nhl_player_stats {#espn_nhl_player_stats}

`espn_nhl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NHL athlete's ESPN **season** stat line as one wide row.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns (for hockey: `offensive_*`, `defensive_*`, `penalties_*`,
...), the athlete / team metadata blocks, and the `season_type` /
`total` parameters. For the richer multi-category web-v3 payload use
`sportsdataverse.nhl.espn_nhl_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NHL athlete identifier (e.g. `3895074` for Connor McDavid). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year (echoed from arg). |
| `season_type` | String | Season type code (echoed from arg). |
| `total` | Boolean |  |
| `athlete_id` | Int64 | ESPN athlete identifier (echoed from arg). |
| `athlete_uid` | String |  |
| `athlete_guid` | String |  |
| `athlete_type` | String |  |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `full_name` | String | Player full name. |
| `display_name` | String | Player display name. |
| `short_name` | String | Short game name. |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Formatted weight string. |
| `height` | Float64 | Player height in inches. |
| `display_height` | String | Formatted height string. |
| `age` | Int64 | Player age. |
| `date_of_birth` | String | Date of birth (ISO 8601). |
| `jersey` | String | Jersey number. |
| `slug` | String | URL slug. |
| `active` | Boolean | Whether athlete is currently active. |
| `position_id` | Int64 | Official position identifier. |
| `position_name` | String | Official position name (e.g. "Referee", "Linesman"). |
| `position_display_name` | String | Position display name. |
| `position_abbreviation` | String | Position abbreviation. |
| `college_name` | String | College name. |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status name. |
| `defensive_goals_against` | Float64 | Total goals allowed by a goaltender over the selected season and season type. |
| `defensive_avg_goals_against` | Float64 | Average goals allowed per game by a goaltender over the selected season and season type, equivalent to goals-against average (GAA). |
| `defensive_shots_against` | Float64 | Total shots on goal faced by a goaltender across all game situations over the selected season and season type. |
| `defensive_avg_shots_against` | Float64 | Average number of shots on goal faced by a goaltender per game over the selected season and season type. |
| `defensive_shootout_saves` | String | Total number of shootout attempts stopped by a goaltender over the selected season and season type. |
| `defensive_shootout_shots_against` | Float64 | Total number of penalty-shootout attempts faced by a goaltender over the selected season and season type. |
| `defensive_shootout_save_pct` | Float64 | Percentage of shootout attempts stopped by a goaltender, calculated as shootout saves divided by shootout shots faced over the season. |
| `defensive_empty_net_goals_against` | String | Number of goals allowed by a goaltender into an empty net (opponent pulled goalie) over the selected season and season type. |
| `defensive_shutouts` | Float64 | Total number of games in which a goaltender allowed zero goals over the selected season and season type. |
| `defensive_saves` | Float64 | Total saves made by a goaltender across all game situations over the selected season and season type. |
| `defensive_save_pct` | Float64 | Percentage of shots stopped by a goaltender, calculated as saves divided by shots against, over the selected season and season type. |
| `defensive_overtime_losses` | Float64 | Number of games a goaltender's team lost in overtime (OT or shootout) while the goaltender was the decision goalie over the selected season. |
| `defensive_blocked_shots` | Float64 | Total number of shots blocked by a skater before reaching the goaltender over the selected season and season type. |
| `defensive_hits` | Float64 | Total body checks delivered by a skater over the selected season and season type as tracked by ESPN. |
| `defensive_even_strength_saves` | Float64 | Total saves made by a goaltender while both teams were at full strength (5-on-5) over the selected season and season type. |
| `defensive_power_play_saves` | Float64 | Total saves made by a goaltender while the opponent had a power-play advantage over the selected season and season type. |
| `defensive_short_handed_saves` | Float64 | Total saves made by a goaltender while the goaltender's team was short-handed (killing a penalty) over the selected season and season type. |
| `general_games` | Float64 | Career games played. |
| `general_game_started` | Float64 | Number of games in which the player was the starting goaltender over the selected season and season type. |
| `general_team_games_played` | Float64 | Total number of regular-season or playoff games played by the player's team during the selected season and season type. |
| `general_wins` | Float64 | Total wins recorded by a goaltender as the decision goalie over the selected season and season type. |
| `general_losses` | Float64 | Total regulation-time losses recorded by a goaltender as the decision goalie over the selected season and season type. |
| `general_ties` | String | Number of games that ended in a tie credited to a goaltender, applicable to seasons before the NHL eliminated ties in 2005-06. |
| `general_plus_minus` | Float64 |  |
| `general_time_on_ice` | Float64 | Cumulative time on ice for a skater or goaltender across all games in the selected season and season type, in total seconds or minutes as provided by ESPN. |
| `general_time_on_ice_per_game` | Float64 | Average time on ice per game for a skater or goaltender over the selected season and season type. |
| `general_shifts` | Float64 | Total number of shifts a skater took during the selected season and season type. |
| `general_shifts_per_game` | Float64 | Average number of shifts per game taken by a skater over the selected season and season type. |
| `general_production` | Float64 | Composite production metric combining goals, assists, and other scoring contributions for a skater, as defined by ESPN, over the selected season. |
| `offensive_goals` | Float64 | Goals (offensive category). |
| `offensive_avg_goals` | Float64 | Average goals scored per game by a skater over the selected season and season type. |
| `offensive_assists` | Float64 | Career assists. |
| `offensive_shots_total` | Float64 | Shots on goal. |
| `offensive_avg_shots` | Float64 | Average shots on goal taken per game by a skater over the selected season and season type. |
| `offensive_points` | Float64 | Career points. |
| `offensive_points_per_game` | Float64 | Average points (goals plus assists) earned per game by a skater over the selected season and season type. |
| `offensive_power_play_goals` | Float64 | Power-play goals. |
| `offensive_power_play_assists` | Float64 | Total assists recorded by a skater while on the power play over the selected season and season type. |
| `offensive_short_handed_goals` | Float64 | Total goals scored by a skater while the player's team was shorthanded over the selected season and season type. |
| `offensive_short_handed_assists` | Float64 | Total assists recorded by a skater while killing a penalty (shorthanded) over the selected season and season type. |
| `offensive_shootout_attempts` | Float64 | Total number of penalty-shootout attempts taken by a skater over the selected season and season type. |
| `offensive_shootout_goals` | Float64 | Total goals scored by a skater in penalty shootouts over the selected season and season type. |
| `offensive_shootout_shot_pct` | Float64 | Percentage of penalty-shootout attempts by a skater that resulted in a goal over the selected season and season type. |
| `offensive_shooting_pct` | Float64 | Shooting percentage. |
| `offensive_total_face_offs` | Float64 | Total number of faceoffs taken by the player across all situations over the selected season and season type. |
| `offensive_faceoffs_won` | Float64 | Total number of faceoffs won by the player over the selected season and season type. |
| `offensive_faceoffs_lost` | Float64 | Total number of faceoffs lost by the player over the selected season and season type. |
| `offensive_faceoff_percent` | Float64 | Percentage of faceoffs won by the player, calculated as faceoffs won divided by total faceoffs taken, over the selected season and season type. |
| `offensive_game_tying_goals` | String | Number of goals scored by a skater that tied the game at the time of the goal, over the selected season and season type. |
| `offensive_game_winning_goals` | Float64 | Game-winning goals. |
| `penalties_penalty_minutes` | Float64 | Career penalty minutes. |
| `penalties_major_penalties` | Float64 | Total number of five-minute major penalties assessed to the player over the selected season and season type. |
| `penalties_minor_penalties` | Float64 | Total number of two-minute minor penalties assessed to the player over the selected season and season type. |
| `penalties_match_penalties` | Float64 | Total number of match penalties assessed to the player for deliberately injuring an opponent, resulting in ejection, over the selected season and season type. |
| `penalties_misconducts` | Float64 | Total number of ten-minute misconduct penalties assessed to the player over the selected season and season type. |
| `penalties_game_misconducts` | Float64 | Total number of game misconduct penalties assessed to the player, resulting in ejection, over the selected season and season type. |
| `penalties_boarding_penalties` | Float64 | Total number of boarding infractions (hitting an opponent into the boards from behind) called against the player over the selected season and season type. |
| `penalties_unsportsmanlike_penalties` | Float64 | Total number of unsportsmanlike conduct penalties assessed to the player over the selected season and season type. |
| `penalties_fighting_penalties` | Float64 | Total number of fighting majors assessed to the player over the selected season and season type. |
| `penalties_avg_fights` | Float64 | Average number of fights per game involving the player over the selected season and season type, as tracked by ESPN. |
| `penalties_time_between_fights` | String | Average time elapsed between fights involving the player during the selected season and season type, as tracked by ESPN. |
| `penalties_instigator_penalties` | Float64 | Total number of instigator penalties assessed to the player for initiating a fight over the selected season and season type. |
| `penalties_charging_penalties` | Float64 | Total number of charging infractions (skating excessive distance to deliver a hit) called against the player over the selected season and season type. |
| `penalties_hooking_penalties` | Float64 | Total number of hooking infractions (using the stick to impede an opponent's movement) called against the player over the selected season and season type. |
| `penalties_tripping_penalties` | Float64 | Total number of tripping infractions (using a stick, arm, or leg to cause an opponent to fall) called against the player over the selected season and season type. |
| `penalties_roughing_penalties` | Float64 | Total number of roughing infractions (unnecessary physical altercations after the whistle) called against the player over the selected season and season type. |
| `penalties_holding_penalties` | Float64 | Total number of holding infractions (impeding an opponent with the hands or arms) called against the player over the selected season and season type. |
| `penalties_interference_penalties` | Float64 | Total number of interference infractions (impeding a player not in possession of the puck) called against the player over the selected season and season type. |
| `penalties_slashing_penalties` | Float64 | Total number of slashing infractions (swinging the stick at an opponent) called against the player over the selected season and season type. |
| `penalties_high_sticking_penalties` | Float64 | Total number of high-sticking infractions (stick contacting an opponent above the shoulders) called against the player over the selected season and season type. |
| `penalties_cross_checking_penalties` | Float64 | Total number of cross-checking infractions (using the shaft of the stick to check an opponent) called against the player over the selected season and season type. |
| `penalties_stick_holding_penalties` | Float64 | Total number of stick-holding infractions called against the player for grabbing an opponent's stick over the selected season and season type. |
| `penalties_goalie_interference_penalties` | Float64 | Total number of goalie interference infractions called against the player for impeding the goaltender over the selected season and season type. |
| `penalties_elbowing_penalties` | Float64 | Total number of elbowing infractions (using the elbow to check an opponent) called against the player over the selected season and season type. |
| `penalties_diving_penalties` | Float64 | Total number of diving or embellishment infractions called against the player over the selected season and season type. |
| `rpi_wins` | Float64 | Total wins recorded under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_losses` | Float64 | Total losses recorded under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_ot_losses` | String | Overtime losses recorded under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_points` | Float64 | Standings points accumulated under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_rpi` | String | Rating Percentage Index (RPI) value for the player's team, reflecting strength of schedule and win/loss record, over the selected season and season type. |
| `rpi_sos` | String | Strength of Schedule (SOS) component of the ESPN RPI calculation for the player's team over the selected season and season type. |
| `rpi_power_rank` | String | ESPN Power Rank position for the player's team within the selected season and season type, derived from the RPI standings model. |
| `rpi_points_for` | String | Total goals or points scored used in ESPN's RPI-based standings computation for the player's team over the selected season and season type. |
| `rpi_points_against` | String | Total goals or points allowed used in ESPN's RPI-based standings computation for the player's team over the selected season and season type. |
| `team_id` | Int64 | Unique team identifier. |
| `team_uid` | String | ESPN team uid. |
| `team_guid` | String |  |
| `team_slug` | String | Team URL slug. |
| `team_location` | String | Team city/location. |
| `team_name` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_short_display_name` | String | Team short display name. |
| `team_color` | String | Team primary color hex. |
| `team_alternate_color` | String | Team alternate color hex. |
| `team_is_active` | Boolean |  |
| `team_logo_href` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season_type` is not `"regular"`/`"postseason"`. |
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `sportsdataverse.errors.AssetFetchError` | The statistics, athlete or team fetch failed (403, 429, 5xx, a dead connection); only a 404 on the athlete or team node leaves those columns null. |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_player_stats
df = espn_nhl_player_stats(athlete_id=3895074, season=2023)
df.select(["full_name", "team_display_name", "offensive_goals"])
```

### espn_nhl_schedule {#espn_nhl_schedule}

`espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nhl_schedule - look up the NHL schedule for a given date

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `season_type` | `int` | `None` | season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. A day with no games returns a zero-row frame carrying the documented columns.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `uid` | String | Competitor uid string. |
| `date` | String | Game date (ISO 8601 datetime string). |
| `attendance` | Int64 | Game attendance. |
| `time_valid` | Boolean | Whether the start time is confirmed. |
| `neutral_site` | Boolean | Whether the game is at a neutral site. |
| `play_by_play_available` | Boolean | Whether play-by-play data is available. |
| `recent` | Boolean | Whether the game is recent. |
| `start_date` | String | Season start date. |
| `broadcast` | String | Broadcast network(s). |
| `highlights` | Int64 |  |
| `notes_type` | String |  |
| `notes_headline` | String |  |
| `broadcast_market` | String |  |
| `broadcast_name` | String |  |
| `type_id` | Int64 | Play type id. |
| `type_abbreviation` | String | Play type abbreviation. |
| `venue_id` | Int64 | Venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_address_country` | String | Country name or code for the country in which the game venue is located, as provided by ESPN's schedule endpoint. |
| `venue_indoor` | Boolean | Whether the venue is indoors. |
| `status_clock` | Float64 | Game clock in seconds. |
| `status_display_clock` | String | Display clock string. |
| `status_period` | Int64 | Current period. |
| `status_type_id` | Int64 | Status type identifier. |
| `status_type_name` | String | Status type name. |
| `status_type_state` | String | Status state (pre/in/post). |
| `status_type_completed` | Boolean | Whether the game is complete. |
| `status_type_description` | String | Status description. |
| `status_type_detail` | String | Status detail text. |
| `status_type_short_detail` | String | Short status detail. |
| `format_regulation_periods` | Int64 |  |
| `home_id` | Int64 | Home team ESPN identifier. |
| `home_uid` | String |  |
| `home_location` | String | Home team city. |
| `home_name` | String | Home team display name. |
| `home_abbreviation` | String | Home team abbreviation. |
| `home_display_name` | String | Home team display name. |
| `home_short_display_name` | String |  |
| `home_color` | String | Home team primary color hex. |
| `home_alternate_color` | String | Home team alternate color hex. |
| `home_is_active` | Boolean |  |
| `home_venue_id` | Int64 |  |
| `home_logo` | String | Home team logo URL. |
| `home_score` | String | Home team final score. |
| `home_linescores` | List | Period-by-period goal totals for the home team, stored as an array of integer scores indexed by period. |
| `home_records` | String | Serialized win-loss-overtime record string for the home team at the time of the scheduled game. |
| `away_id` | Int64 | Away team ESPN identifier. |
| `away_uid` | String |  |
| `away_location` | String | Away team city. |
| `away_name` | String | Away team display name. |
| `away_abbreviation` | String | Away team abbreviation. |
| `away_display_name` | String | Away team display name. |
| `away_short_display_name` | String |  |
| `away_color` | String | Away team primary color hex. |
| `away_alternate_color` | String | Away team alternate color hex. |
| `away_is_active` | Boolean |  |
| `away_venue_id` | Int64 |  |
| `away_logo` | String | Away team logo URL. |
| `away_score` | String | Away team final score. |
| `away_linescores` | List | Period-by-period goal totals for the away team, stored as an array of integer scores indexed by period. |
| `away_records` | String | Serialized win-loss-overtime record string for the away team at the time of the scheduled game. |
| `game_id` | Int64 | Unique game identifier. |
| `season` | Int64 | Season year (echoed from arg). |
| `season_type` | Int64 | Season type code (echoed from arg). |
| `status_featured_athletes` | Int64 |  |
| `home_logo_dark` | String |  |
| `home_winner` | Boolean | Whether the home team won. |
| `away_logo_dark` | String |  |
| `away_winner` | Boolean | Whether the away team won. |
| `status_type_alt_detail` | String | ESPN's short extra-time label on a final ("OT", "SO"); null when the game ended in regulation. |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_schedule
sched = espn_nhl_schedule(dates=20230613)  # 2023 Stanley Cup Final game date
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

# Pull a regular-season slate from a season-year

reg = espn_nhl_schedule(dates=2023, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)

# Pandas round-trip for one date

espn_nhl_schedule(dates=20230613, return_as_pandas=True).head()
```

### espn_nhl_teams {#espn_nhl_teams}

`espn_nhl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nhl_teams - look up NHL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of the NHL teams, one row per team, with the columns in the returns table. A payload without teams returns a zero-row frame with those columns. The result is not cached: every call fetches (through the package's opt-in TTL response cache, `sportsdataverse.cache`, when it is on).

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Team abbreviation. |
| `team_alternate_color` | String | Team alternate color hex. |
| `team_color` | String | Team primary color hex. |
| `team_display_name` | String | Team display name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_is_active` | Boolean |  |
| `team_is_all_star` | Boolean |  |
| `team_location` | String | Team city/location. |
| `team_logos` | Int64 | Team logo metadata. |
| `team_name` | String | Team name. |
| `team_nickname` | String | Team nickname. |
| `team_short_display_name` | String | Team short display name. |
| `team_slug` | String | Team URL slug. |
| `team_uid` | String | ESPN team uid. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN answered 404 for the teams resource. |
| `AssetFetchError` | The fetch failed (403, 429, 5xx, a dropped connection); never returned as an empty frame. |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_teams
teams = espn_nhl_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Tampa Bay Lightning (team_id 14)

import polars as pl
teams.filter(pl.col("team_id") == 14).to_dicts()

# Pandas round-trip

teams_pd = espn_nhl_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
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
