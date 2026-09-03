---
title: NHL — additional Python functions
sidebar_label: Additional functions
description: "NHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# NHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nhl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nhl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nhl_game_rosters}

espn_nhl_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nhl_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'logo_href', 'logo_dark_href', 'game_id'

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer | ESPN athlete identifier (echoed from arg). |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `alternate_id` | character | Alternate player identifier. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `full_name` | character | Player full name. |
| `athlete_display_name` | character | Player display name. |
| `short_name` | character | Short game name. |
| `weight` | double | Player weight in pounds. |
| `display_weight` | character | Formatted weight string. |
| `height` | double | Player height in inches. |
| `display_height` | character | Formatted height string. |
| `age` | integer | Player age. |
| `date_of_birth` | character | Date of birth (ISO 8601). |
| `debut_year` | integer | Year of NHL debut. |
| `slug` | character | URL slug. |
| `jersey` | character | Jersey number. |
| `linked` | logical | TRUE if the record is linked to a related entity. |
| `active` | logical | Whether athlete is currently active. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_country_abbreviation` | character | Birth country abbreviation. |
| `headshot_href` | character | Player headshot image URL. |
| `headshot_alt` | character | Headshot alt text. |
| `hand_type` | character | Shooting/catching hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `contracts_href` | character | ESPN API hypermedia URL pointing to the contract history resource for this player. |
| `experience_years` | integer | Experience years. |
| `draft_display_text` | character | Draft display text. |
| `draft_round` | integer | Draft round. |
| `draft_year` | integer | Draft year the lottery applies to. |
| `draft_selection` | integer | Draft selection. |
| `draft_team_href` | character | ESPN API hypermedia URL linking to the team that originally drafted this player. |
| `status_id` | character | Status identifier. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `jersey_right` | character | Right-aligned display string for the player's jersey number, used in ESPN scoreboard rendering contexts. |
| `display_name` | character | Player display name. |
| `scratched` | logical | Whether the player was a healthy scratch. |
| `scratch_reason` | character | Reason for scratch (if applicable). |
| `athlete_href` | character | ESPN API hypermedia URL linking to the full athlete resource for this player, usable to fetch detailed biographical and statistical data. |
| `position_href` | character | ESPN API hypermedia URL pointing to the position resource that defines this player's positional classification. |
| `statistics_href` | character | ESPN API hypermedia URL linking to the statistics resource for this player's season or career totals. |
| `team_id` | integer | Unique team identifier. |
| `order` | integer | Display order within officials list. |
| `home_away` | character | Home or away indicator. |
| `winner` | logical | Whether this competitor won the game. |
| `team_guid` | character | ESPN team GUID. |
| `team_uid` | character | ESPN team uid. |
| `team_slug` | character | Team URL slug. |
| `team_location` | character | Team city/location. |
| `team_name` | character | Team name. |
| `team_nickname` | character | Team nickname. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character | Team short display name. |
| `team_color` | character | Team primary color hex. |
| `team_alternate_color` | character | Team alternate color hex. |
| `is_active` | logical | Whether the team is active. |
| `is_all_star` | logical | Whether the team is an all-star team. |
| `team_alternate_ids_sdr` | character | Alternate team identifier from the ESPN SDR (Sports Data Repository) system, used to cross-reference team records across ESPN data sources. |
| `logo_href` | character | Team or league logo URL. |
| `logo_dark_href` | character | Logo URL for dark backgrounds. |
| `game_id` | integer | Unique game identifier. |

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

### `espn_nhl_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'` {#espn_nhl_pbp}

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

### `espn_nhl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_nhl_player_stats}

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
| `season` | integer | Season year (echoed from arg). |
| `season_type` | character | Season type code (echoed from arg). |
| `total` | logical | Total. |
| `athlete_id` | integer | ESPN athlete identifier (echoed from arg). |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `full_name` | character | Player full name. |
| `display_name` | character | Player display name. |
| `short_name` | character | Short game name. |
| `weight` | double | Player weight in pounds. |
| `display_weight` | character | Formatted weight string. |
| `height` | double | Player height in inches. |
| `display_height` | character | Formatted height string. |
| `age` | integer | Player age. |
| `date_of_birth` | character | Date of birth (ISO 8601). |
| `jersey` | character | Jersey number. |
| `slug` | character | URL slug. |
| `active` | logical | Whether athlete is currently active. |
| `position_id` | integer | Official position identifier. |
| `position_name` | character | Official position name (e.g. "Referee", "Linesman"). |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation. |
| `college_name` | character | College name. |
| `status_id` | integer | Status identifier. |
| `status_name` | character | Status name. |
| `defensive_goals_against` | double | Total goals allowed by a goaltender over the selected season and season type. |
| `defensive_avg_goals_against` | double | Average goals allowed per game by a goaltender over the selected season and season type, equivalent to goals-against average (GAA). |
| `defensive_shots_against` | double | Total shots on goal faced by a goaltender across all game situations over the selected season and season type. |
| `defensive_avg_shots_against` | double | Average number of shots on goal faced by a goaltender per game over the selected season and season type. |
| `defensive_shootout_saves` | character | Total number of shootout attempts stopped by a goaltender over the selected season and season type. |
| `defensive_shootout_shots_against` | double | Total number of penalty-shootout attempts faced by a goaltender over the selected season and season type. |
| `defensive_shootout_save_pct` | double | Percentage of shootout attempts stopped by a goaltender, calculated as shootout saves divided by shootout shots faced over the season. |
| `defensive_empty_net_goals_against` | character | Number of goals allowed by a goaltender into an empty net (opponent pulled goalie) over the selected season and season type. |
| `defensive_shutouts` | double | Total number of games in which a goaltender allowed zero goals over the selected season and season type. |
| `defensive_saves` | double | Total saves made by a goaltender across all game situations over the selected season and season type. |
| `defensive_save_pct` | double | Percentage of shots stopped by a goaltender, calculated as saves divided by shots against, over the selected season and season type. |
| `defensive_overtime_losses` | double | Number of games a goaltender's team lost in overtime (OT or shootout) while the goaltender was the decision goalie over the selected season. |
| `defensive_blocked_shots` | double | Total number of shots blocked by a skater before reaching the goaltender over the selected season and season type. |
| `defensive_hits` | double | Total body checks delivered by a skater over the selected season and season type as tracked by ESPN. |
| `defensive_even_strength_saves` | double | Total saves made by a goaltender while both teams were at full strength (5-on-5) over the selected season and season type. |
| `defensive_power_play_saves` | double | Total saves made by a goaltender while the opponent had a power-play advantage over the selected season and season type. |
| `defensive_short_handed_saves` | double | Total saves made by a goaltender while the goaltender's team was short-handed (killing a penalty) over the selected season and season type. |
| `general_games` | double | Career games played. |
| `general_game_started` | double | Number of games in which the player was the starting goaltender over the selected season and season type. |
| `general_team_games_played` | double | Total number of regular-season or playoff games played by the player's team during the selected season and season type. |
| `general_wins` | double | Total wins recorded by a goaltender as the decision goalie over the selected season and season type. |
| `general_losses` | double | Total regulation-time losses recorded by a goaltender as the decision goalie over the selected season and season type. |
| `general_ties` | character | Number of games that ended in a tie credited to a goaltender, applicable to seasons before the NHL eliminated ties in 2005-06. |
| `general_plus_minus` | double | A player's estimated on-court impact on team performance measured in point differential per 100 possessions. |
| `general_time_on_ice` | double | Cumulative time on ice for a skater or goaltender across all games in the selected season and season type, in total seconds or minutes as provided by ESPN. |
| `general_time_on_ice_per_game` | double | Average time on ice per game for a skater or goaltender over the selected season and season type. |
| `general_shifts` | double | Total number of shifts a skater took during the selected season and season type. |
| `general_shifts_per_game` | double | Average number of shifts per game taken by a skater over the selected season and season type. |
| `general_production` | double | Composite production metric combining goals, assists, and other scoring contributions for a skater, as defined by ESPN, over the selected season. |
| `offensive_goals` | double | Goals (offensive category). |
| `offensive_avg_goals` | double | Average goals scored per game by a skater over the selected season and season type. |
| `offensive_assists` | double | Career assists. |
| `offensive_shots_total` | double | Shots on goal. |
| `offensive_avg_shots` | double | Average shots on goal taken per game by a skater over the selected season and season type. |
| `offensive_points` | double | Career points. |
| `offensive_points_per_game` | double | Average points (goals plus assists) earned per game by a skater over the selected season and season type. |
| `offensive_power_play_goals` | double | Power-play goals. |
| `offensive_power_play_assists` | double | Total assists recorded by a skater while on the power play over the selected season and season type. |
| `offensive_short_handed_goals` | double | Total goals scored by a skater while the player's team was shorthanded over the selected season and season type. |
| `offensive_short_handed_assists` | double | Total assists recorded by a skater while killing a penalty (shorthanded) over the selected season and season type. |
| `offensive_shootout_attempts` | double | Total number of penalty-shootout attempts taken by a skater over the selected season and season type. |
| `offensive_shootout_goals` | double | Total goals scored by a skater in penalty shootouts over the selected season and season type. |
| `offensive_shootout_shot_pct` | double | Percentage of penalty-shootout attempts by a skater that resulted in a goal over the selected season and season type. |
| `offensive_shooting_pct` | double | Shooting percentage. |
| `offensive_total_face_offs` | double | Total number of faceoffs taken by the player across all situations over the selected season and season type. |
| `offensive_faceoffs_won` | double | Total number of faceoffs won by the player over the selected season and season type. |
| `offensive_faceoffs_lost` | double | Total number of faceoffs lost by the player over the selected season and season type. |
| `offensive_faceoff_percent` | double | Percentage of faceoffs won by the player, calculated as faceoffs won divided by total faceoffs taken, over the selected season and season type. |
| `offensive_game_tying_goals` | character | Number of goals scored by a skater that tied the game at the time of the goal, over the selected season and season type. |
| `offensive_game_winning_goals` | double | Game-winning goals. |
| `penalties_penalty_minutes` | double | Career penalty minutes. |
| `penalties_major_penalties` | double | Total number of five-minute major penalties assessed to the player over the selected season and season type. |
| `penalties_minor_penalties` | double | Total number of two-minute minor penalties assessed to the player over the selected season and season type. |
| `penalties_match_penalties` | double | Total number of match penalties assessed to the player for deliberately injuring an opponent, resulting in ejection, over the selected season and season type. |
| `penalties_misconducts` | double | Total number of ten-minute misconduct penalties assessed to the player over the selected season and season type. |
| `penalties_game_misconducts` | double | Total number of game misconduct penalties assessed to the player, resulting in ejection, over the selected season and season type. |
| `penalties_boarding_penalties` | double | Total number of boarding infractions (hitting an opponent into the boards from behind) called against the player over the selected season and season type. |
| `penalties_unsportsmanlike_penalties` | double | Total number of unsportsmanlike conduct penalties assessed to the player over the selected season and season type. |
| `penalties_fighting_penalties` | double | Total number of fighting majors assessed to the player over the selected season and season type. |
| `penalties_avg_fights` | double | Average number of fights per game involving the player over the selected season and season type, as tracked by ESPN. |
| `penalties_time_between_fights` | character | Average time elapsed between fights involving the player during the selected season and season type, as tracked by ESPN. |
| `penalties_instigator_penalties` | double | Total number of instigator penalties assessed to the player for initiating a fight over the selected season and season type. |
| `penalties_charging_penalties` | double | Total number of charging infractions (skating excessive distance to deliver a hit) called against the player over the selected season and season type. |
| `penalties_hooking_penalties` | double | Total number of hooking infractions (using the stick to impede an opponent's movement) called against the player over the selected season and season type. |
| `penalties_tripping_penalties` | double | Total number of tripping infractions (using a stick, arm, or leg to cause an opponent to fall) called against the player over the selected season and season type. |
| `penalties_roughing_penalties` | double | Total number of roughing infractions (unnecessary physical altercations after the whistle) called against the player over the selected season and season type. |
| `penalties_holding_penalties` | double | Total number of holding infractions (impeding an opponent with the hands or arms) called against the player over the selected season and season type. |
| `penalties_interference_penalties` | double | Total number of interference infractions (impeding a player not in possession of the puck) called against the player over the selected season and season type. |
| `penalties_slashing_penalties` | double | Total number of slashing infractions (swinging the stick at an opponent) called against the player over the selected season and season type. |
| `penalties_high_sticking_penalties` | double | Total number of high-sticking infractions (stick contacting an opponent above the shoulders) called against the player over the selected season and season type. |
| `penalties_cross_checking_penalties` | double | Total number of cross-checking infractions (using the shaft of the stick to check an opponent) called against the player over the selected season and season type. |
| `penalties_stick_holding_penalties` | double | Total number of stick-holding infractions called against the player for grabbing an opponent's stick over the selected season and season type. |
| `penalties_goalie_interference_penalties` | double | Total number of goalie interference infractions called against the player for impeding the goaltender over the selected season and season type. |
| `penalties_elbowing_penalties` | double | Total number of elbowing infractions (using the elbow to check an opponent) called against the player over the selected season and season type. |
| `penalties_diving_penalties` | double | Total number of diving or embellishment infractions called against the player over the selected season and season type. |
| `rpi_wins` | double | Total wins recorded under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_losses` | double | Total losses recorded under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_ot_losses` | character | Overtime losses recorded under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_points` | double | Standings points accumulated under ESPN's RPI-based standings metric for the player's team over the selected season and season type. |
| `rpi_rpi` | character | Rating Percentage Index (RPI) value for the player's team, reflecting strength of schedule and win/loss record, over the selected season and season type. |
| `rpi_sos` | character | Strength of Schedule (SOS) component of the ESPN RPI calculation for the player's team over the selected season and season type. |
| `rpi_power_rank` | character | ESPN Power Rank position for the player's team within the selected season and season type, derived from the RPI standings model. |
| `rpi_points_for` | character | Total goals or points scored used in ESPN's RPI-based standings computation for the player's team over the selected season and season type. |
| `rpi_points_against` | character | Total goals or points allowed used in ESPN's RPI-based standings computation for the player's team over the selected season and season type. |
| `team_id` | integer | Unique team identifier. |
| `team_uid` | character | ESPN team uid. |
| `team_guid` | character | ESPN team GUID. |
| `team_slug` | character | Team URL slug. |
| `team_location` | character | Team city/location. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character | Team short display name. |
| `team_color` | character | Team primary color hex. |
| `team_alternate_color` | character | Team alternate color hex. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_logo_href` | character | Default team logo URL; `team_detail = TRUE` only. |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_player_stats
df = espn_nhl_player_stats(athlete_id=3895074, season=2023)
df.select(["full_name", "team_display_name", "offensive_goals"])
```

### `espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nhl_schedule}

espn_nhl_schedule - look up the NHL schedule for a given date

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
| `id` | character | Unique player identifier. |
| `uid` | character | Competitor uid string. |
| `date` | character | Game date (ISO 8601 datetime string). |
| `attendance` | integer | Game attendance. |
| `time_valid` | logical | Whether the start time is confirmed. |
| `neutral_site` | logical | Whether the game is at a neutral site. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Whether the game is recent. |
| `start_date` | character | Season start date. |
| `broadcast` | character | Broadcast network(s). |
| `highlights` | character | Game highlight urls. |
| `notes_type` | character | Notes type. |
| `notes_headline` | character | Notes headline. |
| `broadcast_market` | character | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | character | Broadcast name. |
| `type_id` | character | Play type id. |
| `type_abbreviation` | character | Play type abbreviation. |
| `venue_id` | character | Venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_address_country` | character | Country name or code for the country in which the game venue is located, as provided by ESPN's schedule endpoint. |
| `venue_indoor` | logical | Whether the venue is indoors. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Display clock string. |
| `status_period` | integer | Current period. |
| `status_type_id` | character | Status type identifier. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status description. |
| `status_type_detail` | character | Status detail text. |
| `status_type_short_detail` | character | Short status detail. |
| `format_regulation_periods` | integer | Format regulation periods. |
| `home_id` | character | Home team ESPN identifier. |
| `home_uid` | character | Home team's uid. |
| `home_location` | character | Home team city. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_short_display_name` | character | Home short display name. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Home team alternate color hex. |
| `home_is_active` | logical | Home team's is active. |
| `home_venue_id` | character | Unique identifier for home venue. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team final score. |
| `home_linescores` | list | Period-by-period goal totals for the home team, stored as an array of integer scores indexed by period. |
| `home_records` | character | Serialized win-loss-overtime record string for the home team at the time of the scheduled game. |
| `away_id` | character | Away team ESPN identifier. |
| `away_uid` | character | Away team's uid. |
| `away_location` | character | Away team city. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_short_display_name` | character | Away short display name. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Away team alternate color hex. |
| `away_is_active` | logical | Away team's is active. |
| `away_venue_id` | character | Unique identifier for away venue. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team final score. |
| `away_linescores` | list | Period-by-period goal totals for the away team, stored as an array of integer scores indexed by period. |
| `away_records` | character | Serialized win-loss-overtime record string for the away team at the time of the scheduled game. |
| `game_id` | integer | Unique game identifier. |
| `season` | integer | Season year (echoed from arg). |
| `season_type` | integer | Season type code (echoed from arg). |

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

## NHL native

### `nhl_edge_skating_value(*, season: 'int', league: 'str' = 'nhl', detail_frames: 'pl.DataFrame | None' = None, method: "Literal['zscore', 'percentile']" = 'zscore', include_zone_balance: 'bool' = False, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nhl_edge_skating_value}

Per-skater EDGE skating-value composite (z-score or percentile blend).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season end-year (e.g. `2024` for 2023-24). |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. PWHL short-circuits to a zero-row frame (no EDGE feed) BEFORE any network access. |
| `detail_frames` | `DataFrame \| None` | `None` | Pre-parsed EDGE aggregate (one row per skater with the COMPONENTS` columns) for offline use. When `None` on the NHL path, the live per-skater `nhl_edge_skater_*_detail` fetch would run -- not implemented offline; supply `detail_frames`. |
| `method` | `Literal['zscore', 'percentile']` | `'zscore'` | `"zscore"` (default, original composite) or `"percentile"` -- see the module docstring's flesh-out note. |
| `include_zone_balance` | `bool` | `False` | add the derived `oz_dz_time_balance` component when `dz_time_pct` is present in `detail_frames` (default `False` -- preserves the original 4-component output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-skater frame: `player_id`, `season`, `top_speed`, `distance_km`, `speed_bursts_20`, `oz_time_pct`, `skating_value`, `skating_value_rank` (1 = fastest composite), plus `oz_dz_time_balance` when `include_zone_balance=True` and derivable. PWHL (or empty/absent input) returns a zero-row frame with the base schema.

**Example**

```python
from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value

out = nhl_edge_skating_value(season=2024, detail_frames=edge_df)

# Percentile composite + zone-balance component

out = nhl_edge_skating_value(
    season=2024, detail_frames=edge_df, method="percentile", include_zone_balance=True
)
```

### `nhl_expected_assists(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', xg_model: 'ShotXGModel | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nhl_expected_assists}

Per-player expected primary/secondary assists from xG-weighted goal credit.

Each goal credits its `assist1` player its **relative danger**
`goal_xg / mean_goal_xg` as x_primary (and `assist2` likewise as
x_secondary). Normalizing to the league-mean goal xG is what makes the
total credit **unbiased** -- `Sum(x_primary + x_secondary) ~= Sum(actual
assists)` -- while still rewarding a playmaker who sets up high-danger
goals (relative danger > 1) over one who feeds tap-ins (< 1). Crediting
raw `goal_xg` (~0.1-0.2) instead would put expected assists on the xG
scale, an order of magnitude below the assist count, and could never be
unbiased against actual assists.
`assists_above_expected = (primary + secondary) - (x_primary + x_secondary)`
(positive = the player's assisted goals were lower-danger than average, so
they out-assisted their shot quality); `primary_share = primary /
(primary + secondary)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key (unused today -- assist credit is league-agnostic; kept for signature parity with the other microstat models and the PWHL shim). |
| `xg_model` | `ShotXGModel \| None` | `None` | A fitted `~sportsdataverse.nhl.nhl_microstat_constants.ShotXGModel`; fit on `pbp` when `None`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `primary_assists`, `secondary_assists`, `x_primary_assists`, `x_secondary_assists`, `assists_above_expected`, `primary_share`. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_expected_assists import nhl_expected_assists

out = nhl_expected_assists(pbp)

# PWHL

out_pwhl = nhl_expected_assists(pwhl_pbp, league="pwhl")
```

### `nhl_faceoff_value(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nhl_faceoff_value}

Per-player context-adjusted faceoff-win value.

Fits `fit_faceoff_context` on the taker-perspective expansion of
every faceoff in `pbp`, then aggregates each player's win rate above
the context expectation and a zone-weighted `faceoff_value` using
`get_constants(league).faceoff_zone_weights`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key for `~sportsdataverse.nhl.nhl_microstat_constants.get_constants` (`"nhl"` or `"pwhl"`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `faceoffs_taken`, `faceoffs_won`, `fo_win_pct`, `fo_win_pct_above_exp`, `faceoff_value`. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_faceoff_value import nhl_faceoff_value

out = nhl_faceoff_value(pbp)

# PWHL

out_pwhl = nhl_faceoff_value(pwhl_pbp, league="pwhl")
```

### `nhl_game_total(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nhl_game_total}

Per-game expected total goals -- a thin re-export of model ②'s expected-goals helper.

Satisfies the "props + total" grouping of model ③ (the brief's player-prop
surface) without a second implementation of the goals math: this calls
the exact same `sportsdataverse.nhl.nhl_market.predict_total` that
`sportsdataverse.nhl.nhl_market.nhl_predict_games` uses internally.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | a schedule-shaped frame with `game_id`, `home_team`, `away_team`, `neutral_site`. |
| `ratings` | `DataFrame` |  | the output of `sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`. |
| `league` | `str` | `'nhl'` | resolves HFA/sigma/total_scale via `get_constants`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame with `game_id`, `exp_total`. |col_name |type | |:---------|:------| |game_id |String | |exp_total |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_player_props import nhl_game_total
nhl_game_total(games, ratings)
```

### `nhl_goalie_gsax(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#nhl_goalie_gsax}

Per-goalie goals-saved-above-expected (GSAx) for the games in `pbp`.

Scores every unblocked shot via `nhl_xg`, attributes each shot to the defending
goalie (attribute_goalie`), and aggregates `xga = sum(xg)`, `ga =
count(goals)`, `gsax = xga - ga`. `gsax_per_60` uses an on-ice-seconds proxy
derived from the pbp event span each goalie is credited on (see
toi_seconds_by_goalie`) -- `shifts` is accepted for interface parity with the
rest of the player-impact spine but is not currently required for TOI.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame (or an already `nhl_xg`-scored one -- re-scoring is idempotent since the prior `xg` column is dropped first). |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame (currently unused; accepted for interface parity -- see the module docstring). |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg` (booster directory). |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

`player_id:Int64, goalie:Utf8, shots:Int64, xga:Float64, ga:Int64, gsax:Float64, gsax_per_60:Float64`. League-wide `sum(gsax) == sum(xga) - sum(goals)`, which is `~= 0` at large sample and exactly zero only under perfect league-wide xG calibration. Empty/malformed input returns a zero-row frame with this schema -- never raises.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_gsax import nhl_goalie_gsax
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
gsax = nhl_goalie_gsax(pbp, pl.DataFrame(), model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(gsax.sort("gsax", descending=True))

# Pipeline next step

gsax.filter(pl.col("shots") >= 10).sort("gsax_per_60", descending=True).head()
```

### `nhl_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nhl_in_game_win_prob}

Per-play live home win probability from the bundled in-game logistic.

Builds `in_game_features` from `pbp`, scores them with the
committed logistic (`sportsdataverse/nhl/models/<league>_in_game_wp.json`
-- no first-use download; the coefficients are trained offline by
`dev/nhl_prediction/train_in_game_wp.py` and committed), and returns
`sigmoid(coef . features + intercept)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a play-by-play frame shaped like `load_nhl_pbp_full`. |
| `pregame_home_prob` | `float` |  | the model (2) pregame home win probability anchor. |
| `league` | `str` | `'nhl'` | resolves the bundled artifact filename via `get_constants`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per play, with a single `home_win_prob: Float64` column.

**Example**

```python
from sportsdataverse.nhl.nhl_market import nhl_in_game_win_prob, win_prob_from_margin

pregame_p = win_prob_from_margin(0.3)
wp = nhl_in_game_win_prob(pbp, pregame_home_prob=pregame_p)
print(wp.tail())
```

### `nhl_pbp_disk(game_id, path_to_json)` {#nhl_pbp_disk}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### `nhl_penalty_value(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nhl_penalty_value}

Per-player net penalty drawn/taken value.

Tallies each player's penalties drawn (`player_id == drawn_player_id`)
and taken (`player_id == committed_player_id`), split minor/major, and
converts the net into expected goals:
`net_penalty_value = (minors_drawn - minors_taken) * pp_goal_value +
(majors_drawn - majors_taken) * major_penalty_value` using
`get_constants(league)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key for `~sportsdataverse.nhl.nhl_microstat_constants.get_constants`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `penalties_drawn`, `penalties_taken`, `minors_drawn`, `minors_taken`, `majors_drawn`, `majors_taken`, `net_penalties`, `net_penalty_value`. League-wide `net_penalty_value` sums to (approximately) zero by construction -- every penalty taken by one player is drawn by another. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_penalty_value import nhl_penalty_value

out = nhl_penalty_value(pbp)

# PWHL

out_pwhl = nhl_penalty_value(pwhl_pbp, league="pwhl")
```

### `nhl_player_props(seasons: 'Union[int, list[int]]', *, league: 'str' = 'nhl', as_of_date: '_dt.date | None' = None, stats: 'tuple[str, ...]' = ('shots', 'points'), return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nhl_player_props}

Empirical-Bayes shots/points player-prop projections.

For every player-game in `load_nhl_skater_boxscores`, projects that
game's shots-on-goal / points from the player's **strictly prior** games
(leakage-safe per row by construction), EB-shrunk toward a position prior,
then adjusted by an opponent matchup multiplier (model ① `adj_xga`) and a
team game-script tilt (model ② native `exp_margin` -- never the market
line). **See the module docstring's leakage-scope note:** the per-player
rate is strictly as-of, but the matchup/game-script ratings are a single
snapshot (as-of `as_of_date` if given, else full-season), not
per-projected-game ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | an int or iterable of seasons (`load_nhl_skater_boxscores` only publishes seasons >= 2024). |
| `league` | `str` | `'nhl'` | resolves `prop_kappa`/`pos_priors`/`prop_team_volume_slope` via `get_constants`. |
| `as_of_date` | `date \| None` | `None` | if given, only games strictly before this date are projected AND the matchup/game-script ratings snapshot is computed as-of this cutoff. NOTE: the per-player usage rate is strictly-prior regardless of this arg (it never needed a cutoff); this arg tightens *which* games are projected and the *single* ratings snapshot, but does not make the ratings per-projected-game as-of (a documented approximation -- see the module docstring). |
| `stats` | `tuple[str, ...]` | `('shots', 'points')` | which stat families to project (`"shots"`, `"points"`). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per (player, game, stat). Empty/malformed input returns a zero-row frame with the documented schema. |col_name |type | |:---------|:------| |season |Int64 | |game_id |String | |player_id |String | |team |String | |opp_team |String | |stat |String | |proj_mean |Float64| |proj_sd |Float64| |p_over |Float64| |line |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_player_props import nhl_player_props

props = nhl_player_props(2024, stats=("shots",))
print(props.sort("proj_mean", descending=True).head())

# Pipeline next step (one line)

props.filter(pl.col("player_id") == "8478402")
```

### `nhl_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'nhl', odds: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nhl_predict_games}

Vectorized pregame margin/win-prob/total (+ market edge) over a schedule.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | a schedule-shaped frame with `game_id`, `home_team`, `away_team`, `neutral_site` (`home_team`/`away_team` must share `nhl_team_ratings`'s `team` dtype -- asserted below). |
| `ratings` | `DataFrame` |  | the output of `sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings` (`team`, `adj_xgf`, `adj_xga`). |
| `league` | `str` | `'nhl'` | resolves HFA/sigma via `get_constants`. |
| `odds` | `Optional[DataFrame]` | `None` | optional frame with `game_id`, `close_puck_line_home`; when supplied, `market_edge = exp_margin - close_puck_line_home`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per game. |col_name |type | |:-------------|:------| |game_id |String | |home_team |String | |away_team |String | |neutral_site |Boolean| |exp_margin |Float64| |home_win_prob |Float64| |exp_total |Float64| |market_edge |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_market import nhl_predict_games
preds = nhl_predict_games(games, ratings)
print(preds.sort("home_win_prob", descending=True).head())
```

### `nhl_records_coach_milestone_wins(wins: 'int', playoffs: 'bool' = False, **filters) -> 'Dict'` {#nhl_records_coach_milestone_wins}

Coaches who reached a wins milestone in fewest games.

Wraps one of the `/coach-fewest-games-to-{N}-wins` or
`/coach-fewest-games-to-{N}-playoff-wins` paths.

Supported *wins* values: `50, 100, 150, 200, 300, 400, 500, 600, 700,
800, 900, 1000` (regular season); `50, 100, 150` (playoffs).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `wins` | `int` |  | Milestone win total (e.g. `100`). |
| `playoffs` | `bool` | `False` | If `True`, use the playoff-wins path. |

**Returns**

Coaches who hit the milestone, sorted by games needed.

### `nhl_records_comeback_wins(scope: 'str' = 'league', **filters) -> 'Dict'` {#nhl_records_comeback_wins}

Comeback wins from a multi-goal deficit.

Wraps:
  * `GET /comeback-league-wins` when *scope* is `"league"`.
  * `GET /comeback-franchise-wins` when *scope* is `"franchise"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scope` | `str` | `'league'` | `"league"` (default) or `"franchise"`. |

**Returns**

Games where the team overcame a deficit to win.

### `nhl_records_consecutive_goal_seasons(goals: 'int' = 50, **filters) -> 'Dict'` {#nhl_records_consecutive_goal_seasons}

Skaters with the most consecutive N-goal seasons.

Wraps one of:
  * `GET /consecutive-20-goal-seasons`
  * `GET /consecutive-30-goal-seasons`
  * `GET /consecutive-40-goal-seasons`
  * `GET /consecutive-50-goal-seasons`
  * `GET /consecutive-60-goal-seasons`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `goals` | `int` | `50` | Goal threshold — one of `20, 30, 40, 50, 60`. |

**Returns**

Skaters sorted by consecutive-season streak.

### `nhl_records_fastest_goals(n_goals: 'int' = 2, **filters) -> 'Dict'` {#nhl_records_fastest_goals}

Fastest N goals by one team in a single game.

Wraps one of:
  * `GET /fastest-2-goals-one-team`
  * `GET /fastest-3-goals-one-team`
  * `GET /fastest-4-goals-one-team`
  * `GET /fastest-5-goals-one-team`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Goal count — one of `2, 3, 4, 5`. |

**Returns**

Games where the milestone was set, sorted by elapsed time (fastest first).

### `nhl_records_fastest_goals_both_teams(n_goals: 'int' = 2, **filters) -> 'Dict'` {#nhl_records_fastest_goals_both_teams}

Fastest N goals combined (both teams) in a single game.

Wraps one of:
  * `GET /fastest-2-goals-both-teams`
  * `GET /fastest-3-goals-both-teams`
  * `GET /fastest-4-goals-both-teams`
  * `GET /fastest-5-goals-both-teams`
  * `GET /fastest-6-goals-both-teams`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Combined goal count — one of `2, 3, 4, 5, 6`. |

**Returns**

Sorted by elapsed time (fastest first).

### `nhl_records_games_played_streak_skaters(active_only: 'bool' = False, **filters) -> 'Dict'` {#nhl_records_games_played_streak_skaters}

Consecutive games-played streaks for skaters.

Wraps `GET /games-played-streak-skaters` (career) or
`GET /games-played-active-streak-skaters` (currently active streaks).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `active_only` | `bool` | `False` | If `True`, return only active streaks. |

**Returns**

Skaters sorted by streak length.

### `nhl_scoreboard(date: 'Optional[str]' = None, team: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Dict'` {#nhl_scoreboard}

In-game scoreboard payload (renamed from `nhl_web_scoreboard`).

Picks among three mutually-exclusive NHL api-web forms (kept hand-written
because the URL-builder codegen can't represent the 3-way branch):

* `GET /v1/scoreboard/{team}/now` -- team-scoped now (when `team` set),
* `GET /v1/scoreboard/{date}` -- league-wide on a date,
* `GET /v1/scoreboard/now` -- league-wide now (both args None).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `YYYY-MM-DD`; `None` -> `/now`. Mutually exclusive with `team`. |
| `team` | `Optional[str]` | `None` | 3-letter abbreviation; takes precedence over `date`. |
| `return_parsed` | `bool` | `True` | dispatch the raw payload through `parse_nhl_web_scoreboard`. |
| `return_as_pandas` | `bool` | `False` | with `return_parsed`, return pandas instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `scoreboard_date` | character | Calendar date (YYYY-MM-DD) for which this scoreboard snapshot was retrieved from the NHL api-web feed. |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `game_center_link` | character | Link to the NHL game center page. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tickets_link` | character | URL to the English-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `tickets_link_fr` | character | URL to the French-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `period` | double | Period number. |
| `three_min_recap` | character | Link to the three-minute recap. |
| `three_min_recap_fr` | character | Link to the French three-minute recap. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_name_default` | character | Full English-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_name_fr` | character | Full French-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | double | Away team final score. |
| `away_team_logo` | character | URL to the away team logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_name_default` | character | Full English-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_name_fr` | character | Full French-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | double | Home team final score. |
| `home_team_logo` | character | URL to the home team logo. |
| `period_descriptor_number` | double | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | double | Maximum number of regulation periods. |
| `series_status_round` | integer | Playoff round number for this game's series (1 = first round, 2 = second round, etc.). |
| `series_status_series_abbrev` | character | Short abbreviation string identifying the specific playoff series matchup (e.g., 'A1' for a particular bracket slot). |
| `series_status_game` | integer | Game number within the current playoff series (e.g., 1 through 7) for the game represented in this scoreboard row. |
| `series_status_top_seed_team_abbrev` | character | Three-letter abbreviation for the higher-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in the current playoff series as of this scoreboard snapshot. |
| `series_status_bottom_seed_team_abbrev` | character | Three-letter abbreviation for the lower-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in the current playoff series as of this scoreboard snapshot. |
| `period_descriptor_ot_periods` | double | Number of overtime periods played when the game extended beyond regulation, as reported in the scoreboard period descriptor. |
| `away_team_record` | character | Away team's win-loss record. |
| `home_team_record` | character | Home team's win-loss record. |

**Example**

```python
nhl_scoreboard(date="2024-03-01")
```

### `nhl_skater_rapm(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', lam: 'float | None' = None, as_of: 'int | None' = None, strength_states: 'list[str] | None' = None, return_as_pandas: 'bool' = False, _stints: 'pl.DataFrame | None' = None) -> "'pl.DataFrame | pd.DataFrame'"` {#nhl_skater_rapm}

Per-skater xG-based Regularized Adjusted Plus-Minus (RAPM), per 60 minutes.

Builds shift stints (`build_stints`), the sparse off/def design matrix
(`build_design`), and solves the weighted ridge (`weighted_ridge`). Offensive
rating is the `off_<player>` coefficient; defensive rating is the **negated**
`def_<player>` coefficient (suppressing xG-against is positive value) --
`xg_rapm = xg_rapm_off + xg_rapm_def`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects the ridge lambda-grid via `LEAGUE_CONSTANTS` when `lam` is not given. |
| `lam` | `float \| None` | `None` | an explicit ridge penalty; `None` selects via k-fold CV over `LEAGUE_CONSTANTS[league].rapm_lambda_grid`. |
| `as_of` | `int \| None` | `None` | forwarded to `build_stints` -- the leakage-boundary cutoff. |
| `strength_states` | `list[str] \| None` | `None` | restrict the design matrix to these `strength_state` values (e.g. `["5v5"]` for an even-strength-only fit, as used by `nhl_skater_war`'s `ev_off`/`ev_def` components so they don't overlap with `nhl_special_teams_value`'s PP/PK components). `None` (default) uses every strength state, matching the general-purpose all-situations RAPM. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |
| `_stints` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built stints frame, bypassing `pbp`/`shifts`/scoring (not part of the public contract). |

**Returns**

`player_id:Int64, xg_rapm_off:Float64, xg_rapm_def:Float64, xg_rapm:Float64, toi_minutes:Float64`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_rapm import nhl_skater_rapm
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
rapm = nhl_skater_rapm(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(rapm.sort("xg_rapm", descending=True).head(10))
```

### `nhl_skater_war(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#nhl_skater_war}

Per-skater GAR/WAR composite -- EV + special-teams + faceoffs + penalties.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`/`nhl_skater_rapm`/ `nhl_special_teams_value`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

`player_id:Int64, ev_off:Float64, ev_def:Float64, pp:Float64, pk:Float64, pens:Float64, faceoffs:Float64, gar:Float64, war:Float64`. `ev_off`/`ev_def` are `(5v5-only RAPM rate - replacement level) * EV TOI/60`; `gar` sums every component; `war = gar / goals_per_win`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_war import nhl_skater_war
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
war = nhl_skater_war(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(war.sort("war", descending=True).head(10))
```

### `nhl_special_teams_value(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False, _stints: 'pl.DataFrame | None' = None) -> "'pl.DataFrame | pd.DataFrame'"` {#nhl_special_teams_value}

Per-skater power-play/penalty-kill value (goals) above/below league baseline.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects `league_xg_rate_pp`/pk` via `LEAGUE_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |
| `_stints` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built stints frame. |

**Returns**

`player_id:Int64, pp_toi_minutes:Float64, pk_toi_minutes:Float64, pp_value:Float64, pk_value:Float64`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
st = nhl_special_teams_value(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(st.sort("pp_value", descending=True).head(10))
```

### `nhl_team_ratings(seasons: 'Union[int, list[int]]', *, league: 'str' = 'nhl', as_of_date: '_dt.date | None' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nhl_team_ratings}

Opponent-adjusted, shrunk even-strength xG (+ goal) team ratings.

Loads pbp + schedule for `seasons`, restricts to even strength, applies
the as-of-date leakage split if requested, opponent-adjusts + shrinks both
the xG rate (primary) and the realized-goal rate (concurrent sanity
rating) via `adjust_rate_opponent`, and derives off/def/net ranks.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | an int or iterable of seasons. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- resolves HFA/avg/shrink_k via `sportsdataverse.nhl.nhl_prediction_constants.get_constants`. |
| `as_of_date` | `date \| None` | `None` | if given, only games strictly before this date are used (the leakage boundary for a predictive backtest). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per (season, team). Empty input seasons return a zero-row frame with the documented schema. |col_name |type | |:----------|:------| |season |Int64 | |team |String | |adj_xgf |Float64| |adj_xga |Float64| |adj_xg_net |Float64| |adj_gf |Float64| |adj_ga |Float64| |games |Int64 | |off_rank |Int64 | |def_rank |Int64 | |net_rank |Int64 | |net_z |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

ratings = nhl_team_ratings(2023)
print(ratings.sort("net_rank").head())

# As-of-date leakage-safe rating

import datetime as dt
ratings = nhl_team_ratings(2023, as_of_date=dt.date(2023, 1, 1))

# Pipeline next step (one line)

ratings.filter(pl.col("team") == "TOR")
```

### `nhl_unit_ratings(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', unit_type: 'str' = 'forward_line', min_toi: 'float' = 20.0, return_as_pandas: 'bool' = False, _stints: 'pl.DataFrame | None' = None, _rapm: 'pl.DataFrame | None' = None) -> "'pl.DataFrame | pd.DataFrame'"` {#nhl_unit_ratings}

Per on-ice skater combination: observed xGF/xGA + shrinkage-blended summed RAPM.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`/`nhl_skater_rapm`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. |
| `unit_type` | `str` | `'forward_line'` | `"forward_line"` (3-skater combinations) or `"defense_pair"` (2-skater combinations) -- see the module's data-availability caveat. |
| `min_toi` | `float` | `20.0` | minimum minutes-together for a unit to be reported. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |
| `_stints` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built stints frame. |
| `_rapm` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built skater-RAPM frame (paired with stints`; both must be given together to bypass real computation). |

**Returns**

`team:Utf8, unit_ids:Utf8 (sorted "id-id-id"), unit_players:Utf8, toi_minutes:Float64, on_ice_xgf:Float64, on_ice_xga:Float64, on_ice_xgf_pct:Float64, summed_rapm:Float64, unit_value:Float64`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
units = nhl_unit_ratings(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(units.sort("unit_value", descending=True).head(10))
```

### `nhl_xg(pbp: 'pl.DataFrame', *, model_dir: 'str | Path | None' = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#nhl_xg}

Score every unblocked shot in `pbp` with the published `nhl_xg_models` boosters.

Ports fastRhockey's `helper_nhl_calculate_xg` -- routes 5v5 shots to the 5v5
booster and every other strength state to the special-teams booster, overrides
penalty shots with the constant `xg_model_ps`, then left-joins `xg` back onto
`pbp` by `event_id`. Attaches the danger/distance/angle expansion
(`add_shot_geometry`) after scoring.

**Known issue -- the published boosters over-predict for seasons through 2023-24.**
Measured 2026-09-02 against the 2026-04 boosters currently in the `nhl_xg_models`
release: observed goals / sum(`xg`) is **0.771** at 5v5 (n=1,724,290 shots) and
**0.768** on special teams (n=349,232), where a correctly-levelled model gives 1.0 --
i.e. `xg` is inflated by roughly 25-30% for every season from 2009-10 through
2023-24. Seasons 2024-25 (0.949) and 2025-26 (0.913) are much closer. The cause is
the boosters' training corpus, which carried no `MISSED_SHOT` events for the
affected seasons; it is not a defect in the feature frame this function builds.
Shot RANKING is far less affected (rank AUC 0.778 / 0.760), so `xg` is still usable
for ordering chances -- but any SUM of `xg` (per game, per player, team totals,
goals-above-expected, and `nhl_gsax` downstream) is inflated for pre-2024-25
seasons. Tracking:
[sportsdataverse-py#444](https://github.com/sportsdataverse/sportsdataverse-py/issues/444);
evidence:
[fastRhockey-nhl-data#11](https://github.com/sportsdataverse/fastRhockey-nhl-data/pull/11).
To check whether this still applies to the boosters you have, sum `xg` over a
season and compare against actual goals -- a corrected booster gives a ratio near
1.0 -- or run `nhl_data_build.xg_parity.artifact_calibration` in
`fastRhockey-nhl-data`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `model_dir` | `str \| Path \| None` | `None` | booster directory; `None` downloads-and-caches on first use (see `ensure_xg_models`). Offline callers should pass the committed fixture dir. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects the danger-zone geometry bands (the PWHL borrows the NHL boosters themselves; see `xg_booster_league`). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

`pbp` with `xg:Float64`, `distance_to_net:Float64`, `shot_angle:Float64`, `shot_danger:Utf8` appended (null/absent for non-shot rows). Empty/malformed input returns the input frame with a null `xg` column -- never raises.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import nhl_xg
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
scored = nhl_xg(pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(scored.filter(pl.col("xg").is_not_null()).height)

# Pandas round-trip

scored_pd = nhl_xg(pbp, return_as_pandas=True)
```

### `nhl_zone_transitions(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', tags: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nhl_zone_transitions}

Per-player controlled/dump entry & exit rates + xG-weighted values.

`entry_value = controlled_entries * zone_entry_value_controlled +
dump_entries * zone_entry_value_dump`; `exit_value = exits *
zone_exit_value` -- all from `get_constants(league)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key for the value constants. |
| `tags` | `DataFrame \| None` | `None` | Optional ground-truth controlled/dump override (see `infer_zone_transitions`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `controlled_entries`, `dump_entries`, `exits`, `controlled_entry_rate`, `entry_value`, `exit_value`. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_zone_transitions import nhl_zone_transitions

out = nhl_zone_transitions(pbp)

# PWHL

out_pwhl = nhl_zone_transitions(pwhl_pbp, league="pwhl")
```

## Dataset loaders

### `load_nhl_games(return_as_pandas: 'bool' = False)` {#load_nhl_games}

Load the NHL games-in-data-repo manifest (no `seasons` argument).

Mirrors fastRhockey (R) `load_nhl_games()` which reads a manifest of every
NHL game that has processed data in the data repository.

Tries the sportsdataverse-data release asset first; falls back to the raw
fastRhockey-data GitHub path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of all games in the data repository.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `season_full` | character | Full season label (e.g. 20212022). |
| `game_type` | character | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `game_time` | character | Scheduled start time of the game. |
| `home_team_abbr` | character | Home team abbreviation. |
| `away_team_abbr` | character | Away team abbreviation. |
| `home_team_name` | character | Home team name. |
| `away_team_name` | character | Away team name. |
| `home_score` | integer | Home team final score. |
| `away_score` | integer | Away team final score. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `venue` | character | Venue where the game was played. |
| `series_letter` | character | Single-letter identifier for the playoff series to which this game belongs, used to group games within the same bracket matchup in the NHL games dataset. |
| `playoff_round` | integer | Playoff round identifier. |
| `series_game_number` | integer | Series game number. |
| `season` | integer | Season year (echoed from arg). |
| `game_json` | logical | Whether processed game JSON is available. |
| `game_json_url` | character | URL to the processed game JSON. |
| `PBP` | logical | Whether play-by-play data is available. |
| `team_box` | logical | Whether team box score data is available. |
| `player_box` | logical | Whether player box score data is available. |
| `skater_box` | logical | Whether skater box data is available. |
| `goalie_box` | logical | Whether goalie box data is available. |
| `game_info` | logical | Whether game info data is available. |
| `game_rosters` | logical | Whether game rosters data is available. |
| `scoring` | logical | TRUE when the play results in a score (TD, FG, safety, two-point conversion). |
| `penalties` | logical | Penalty count. |
| `scratches` | logical | Logical flag indicating whether a scratches list (players healthy-scratched and not dressing) is available for this game in the NHL games loader output. |
| `linescore` | logical | Logical flag indicating whether linescore data (period-by-period scoring breakdown) is available for this game in the NHL games loader output. |
| `three_stars` | logical | Whether three stars data is available. |
| `shifts` | logical | Number of shifts. |
| `officials` | logical | Whether officials data is available. |
| `shots_by_period` | logical | Whether shots-by-period data is available. |
| `shootout` | logical | Whether shootout data is available. |

**Example**

```python
load_nhl_games()
```

### `load_nhl_goalie_box(seasons, return_as_pandas: 'bool' = False)` {#load_nhl_goalie_box}

Alias of load_nhl_goalie_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_nhl_player_box(seasons, return_as_pandas: 'bool' = False)` {#load_nhl_player_box}

Alias of load_nhl_player_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_nhl_skater_box(seasons, return_as_pandas: 'bool' = False)` {#load_nhl_skater_box}

Alias of load_nhl_skater_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_nhl_team_box(seasons, return_as_pandas: 'bool' = False)` {#load_nhl_team_box}

Alias of load_nhl_team_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_xg_models(model_dir: 'str | Path | None' = None) -> 'dict'` {#load_xg_models}

Load the two published boosters (+ embedded feature names) and the penalty-shot constant.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_dir` | `str \| Path \| None` | `None` | `None` downloads the canonical `nhl_xg_models` release on first use and caches under `booster_cache_dir()`; pass a dir to use local models (the offline test suite always passes the committed fixture dir). |

**Returns**

dict with keys `m5v5`/`mst` (`xgboost.Booster`), `feats_5v5`/`feats_st` (embedded feature-name lists), and `ps` (penalty-shot constant probability).

**Example**

```python
from sportsdataverse.nhl.nhl_xg import load_xg_models
models = load_xg_models("tests/fixtures/nhl_player_impact/xg_models")
```

## Utilities & helpers

### `most_recent_nhl_season()` {#most_recent_nhl_season}

most_recent_nhl_season - return the season year for "today".

NHL seasons are labeled by the year they end in. October flips the
label to next calendar year (the new season just started), otherwise
the current calendar year is returned.

**Returns**

A season year suitable for season-aware loaders / schedule helpers.

**Example**

```python
from sportsdataverse.nhl import most_recent_nhl_season, espn_nhl_calendar
season = most_recent_nhl_season()
cal = espn_nhl_calendar(season=season)
print(season, cal.height)
```

### `year_to_season(year)` {#year_to_season}

year_to_season - format a starting year as the canonical `YYYY-YY` season string.

NHL season strings (used by `statsapi` / `api-web.nhle.com`) are of the form
`"2023-24"`. This helper converts a starting year (`2023`) into that string.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` |  |  | Starting calendar year of the season (e.g. `2023`). |

**Returns**

Season string formatted as `"YYYY-YY"`.

**Example**

```python
from sportsdataverse.nhl import year_to_season
year_to_season(2023)  # '2023-24'
year_to_season(2009)  # '2009-10'
year_to_season(1999)  # '1999-00'
```

## Other

### `ImpactConfig(goals_per_win: 'float', replacement_ev_off: 'float', replacement_ev_def: 'float', league_xg_rate_ev: 'float', league_xg_rate_pp: 'float', league_xg_rate_pk: 'float', rapm_lambda_grid: 'list[float]' = <factory>, penalty_goal_weight: 'float' = 0.18, faceoff_goal_weight: 'float' = 0.02, rink_x_goal_line: 'float' = 89.0, danger_high: 'dict' = <factory>, danger_medium: 'dict' = <factory>, xg_booster_league: 'str' = 'nhl') -> None` {#ImpactConfig}

League-specific constants consumed by every player-impact engine function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `goals_per_win` | `float` |  | goals-per-win denominator for GAR->WAR (Task 6.2 fits the NHL value from team wins vs goal differential; seeded here until fit). |
| `replacement_ev_off` | `float` |  | EV offense replacement-level rate (xG/60), subtracted before summing GAR. |
| `replacement_ev_def` | `float` |  | EV defense replacement-level rate (xGA/60 suppressed). |
| `league_xg_rate_ev` | `float` |  | league-average even-strength xG rate (per 60), used as the RAPM intercept sanity check. |
| `league_xg_rate_pp` | `float` |  | league-average power-play xGF rate (per 60). |
| `league_xg_rate_pk` | `float` |  | league-average penalty-kill xGA rate (per 60). |
| `rapm_lambda_grid` | `list[float]` | `<factory>` | candidate ridge penalties for the skater RAPM CV. |
| `penalty_goal_weight` | `float` | `0.18` | goals-per-(penalty drawn - taken) conversion. |
| `faceoff_goal_weight` | `float` | `0.02` | goals-per-(faceoff win - 0.5) conversion. |
| `rink_x_goal_line` | `float` | `89.0` | absolute rink x-coordinate of the goal line (feet), used by the shot-geometry expansion. |
| `danger_high` | `dict` | `<factory>` | `{"max_distance": float, "max_angle": float}` band for "high" danger. |
| `danger_medium` | `dict` | `<factory>` | same shape, wider band for "medium" danger; outside both -> "low". |
| `xg_booster_league` | `str` | `'nhl'` | which league's published boosters back this league's `nhl_xg` scoring (the PWHL borrows the NHL boosters -- a documented approximation). |

### `LeagueConstants(hfa: 'float', margin_sd: 'float', avg_xgf: 'float', avg_total_goals: 'float', total_scale: 'float', shrink_k: 'float', prop_kappa: 'dict', pos_priors: 'dict', prop_team_volume_slope: 'float', in_game_wp_artifact: 'str', min_season: 'int') -> None` {#LeagueConstants}

Fitted, league-specific constants for the NHL/PWHL prediction spine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hfa` | `float` |  | home-ice edge, expected-goals units. |
| `margin_sd` | `float` |  | standard deviation of the final goal margin (deliberately WIDE for hockey). |
| `avg_xgf` | `float` |  | league mean even-strength xG-for, per game. |
| `avg_total_goals` | `float` |  | league mean total goals per game. |
| `total_scale` | `float` |  | multiplier converting rating differential to total-goals deviation. |
| `shrink_k` | `float` |  | games-played prior strength for rating shrinkage. |
| `prop_kappa` | `dict` |  | empirical-Bayes shrinkage strength per player-prop stat family. |
| `pos_priors` | `dict` |  | per-position (F/D) per-stat-family prior rates. |
| `prop_team_volume_slope` | `float` |  | game-script tilt on a player-prop projection (favored team -> fewer late shots-for). SEEDED PLACEHOLDER (~0.04), not yet fitted -- a future prop-fit task should estimate it from the realized shots-vs-exp_margin slope, mirroring how fit_props.py fits prop_kappa/pos_priors. |
| `in_game_wp_artifact` | `str` |  | filename of the bundled in-game win-probability model under `sportsdataverse/nhl/models/`. |
| `min_season` | `int` |  | earliest season this league's prediction spine supports. |

### `add_shot_geometry(df: 'pl.DataFrame', *, league: 'str' = 'nhl') -> 'pl.DataFrame'` {#add_shot_geometry}

Attach `distance_to_net` / `shot_angle` / `shot_danger` (descriptive output only).

Distance/angle are computed off `x_fixed`/`y` against the rink goal-line
x-coordinate in `LEAGUE_CONSTANTS[league].rink_x_goal_line`; `shot_danger` buckets
into `high`/`medium`/`low` using the `danger_high`/`danger_medium`
distance+angle bands from the same config. These are output columns only -- never
fed back into the boosters (Decision D2; a new feature would force a retrain).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | any frame carrying `x_fixed` and `y` columns. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects the danger-zone bands. |

**Returns**

`df` with `distance_to_net:Float64`, `shot_angle:Float64`, `shot_danger:Utf8` appended.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import add_shot_geometry
out = add_shot_geometry(pl.DataFrame({"x_fixed": [80], "y": [0]}))
```

### `adjust_rate_opponent(game_rates: 'pl.DataFrame', *, for_col: 'str', against_col: 'str', hfa: 'float', avg: 'float', shrink_k: 'float', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'` {#adjust_rate_opponent}

Opponent-adjust a per-game for/against rate by iterative fixed-point, then shrink.

League-agnostic: every constant (`hfa`, `avg`, `shrink_k`) is passed
in -- no NHL/PWHL number is hard-coded here. This is the flagged T7.2
"rate-iterative + shrinkage" shared-solver candidate (the hockey
counterpart of the NFL/CFB per-play ridge); `for_col`/`against_col`
are symmetric (offense sees opponent defense).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_rates` | `DataFrame` |  | one row per (team, opponent, game) with columns `season`, `team`, `opp_team`, `is_home`, `neutral_site`, and the two numeric rate columns named by `for_col`/`against_col`. |
| `for_col` | `str` |  | name of the team's own-side rate column (e.g. `"xgf"`). |
| `against_col` | `str` |  | name of the team's against-side rate column (e.g. `"xga"`). |
| `hfa` | `float` |  | home-ice edge added to the home side / subtracted from the away side. |
| `avg` | `float` |  | league mean rate to adjust and shrink toward. |
| `shrink_k` | `float` |  | games-played prior strength for the post-convergence shrink. |
| `max_iter` | `int` | `100` | maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | convergence tolerance on the max absolute update. |

**Returns**

A polars DataFrame, one row per (season, team). |col_name |type | |:------------|:------| |season |Int64 | |team |String | |adj_for |Float64| |adj_against |Float64| |adj_net |Float64| |raw_for |Float64| |raw_against |Float64| |games |Int64 |

**Example**

```python
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
adjust_rate_opponent(
    game_rates, for_col="xgf", against_col="xga",
    hfa=0.2, avg=2.55, shrink_k=15.0,
)
```

### `as_of_ratings_split(df: 'pl.DataFrame', cutoff_date: '_dt.date', *, date_col: 'str' = 'date') -> 'pl.DataFrame'` {#as_of_ratings_split}

Filter a frame to rows strictly before `cutoff_date` (the leakage boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | a polars DataFrame with a date column. |
| `cutoff_date` | `date` |  | the game date being predicted; only strictly-earlier rows are kept. |
| `date_col` | `str` | `'date'` | name of the date column (default `"date"`). |

**Returns**

The subset of `df` with `df[date_col] < cutoff_date`.

**Example**

```python
import datetime as dt
import polars as pl
from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split
df = pl.DataFrame({"date": [dt.date(2023, 1, 1), dt.date(2023, 1, 2)]})
as_of_ratings_split(df, dt.date(2023, 1, 2))
```

### `booster_cache_dir(override: 'str | Path | None' = None) -> 'Path'` {#booster_cache_dir}

Resolve the local cache directory for the downloaded `nhl_xg_models` boosters.

Precedence: explicit `override` argument > `NHL_XG_MODEL_DIR` env var >
`~/.cache/nhl_xg_models`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `override` | `str \| Path \| None` | `None` | an explicit directory (e.g. a committed test-fixture dir); wins over the env var when given. |

**Returns**

The resolved `pathlib.Path` (not created here -- `ensure_xg_models` creates it on first download).

**Example**

```python
from sportsdataverse.nhl.nhl_player_impact_constants import booster_cache_dir
d = booster_cache_dir()
```

### `brier_score(y_true: 'np.ndarray', p_pred: 'np.ndarray') -> 'float'` {#brier_score}

Mean squared error between predicted probabilities and binary outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |

**Returns**

The Brier score (0.0 is a perfect forecast).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import brier_score
brier_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### `build_design(stints: 'pl.DataFrame') -> "tuple['sp.csr_matrix', np.ndarray, np.ndarray, list[int]]"` {#build_design}

Build the sparse RAPM design matrix -- two rows per stint (one per attacking team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stints` | `DataFrame` |  | a `build_stints`-shaped frame. |

**Returns**

`(X, y, w, player_index)` where `X` is a `scipy.sparse.csr_matrix` with columns `off_<player>` (all on-ice attackers), `def_<player>` (all on-ice defenders), then a trailing home-ice indicator and intercept column; `y` is the attacking team's xGF per 60; `w` is stint duration (seconds); `player_index` maps each `off_`/`def_` column pair's position to a `player_id` (so column `j` is `off_<player_index[j]>` and column `j + n_players` is `def_<player_index[j]>`).

**Example**

```python
from sportsdataverse.nhl.nhl_rapm import build_design
X, y, w, player_index = build_design(stints)
```

### `build_stints(shifts: 'pl.DataFrame', scored: 'pl.DataFrame', *, as_of: 'int | None' = None) -> 'pl.DataFrame'` {#build_stints}

Fold `load_nhl_shifts` CHANGE events into contiguous constant-personnel intervals.

Per game: resolves each shift row's full team name (`event_team`) to home/away via
`team_fullname_to_abbr` + the game's `home_abbr`/`away_abbr` (from `scored`),
then folds `ids_on`/`ids_off` deltas chronologically into a running on-ice set per
side. A new interval begins at every distinct `game_seconds` boundary; the final
interval is closed at the last `scored` event's `game_seconds` + 1 for that game
(there is no explicit "end of game" CHANGE row in the shift-chart feed).

Known simplification: shift-chart id lists do not distinguish position, so
`home_ids`/`away_ids` may include the on-ice goalie's id alongside skaters;
`home_goalie`/`away_goalie` are instead sourced from the overlapping `scored`
events' `home_goalie_id`/`away_goalie_id` (the modal value in the interval).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `scored` | `DataFrame` |  | an `nhl_xg`-scored frame (for the game's `home_abbr`/`away_abbr` and each interval's on-ice xG-for and goalie). |
| `as_of` | `int \| None` | `None` | an optional per-game `game_seconds` cutoff -- intervals starting at or after `as_of` are dropped. This is the leakage boundary for any forward-looking use: features for a game/date must use only stints strictly before that game's cutoff. |

**Returns**

one row per interval -- `game_id:Int64, period:Int64, start_s:Int64, end_s:Int64, duration:Int64, home_ids:List(Int64), away_ids:List(Int64), home_goalie:Int64, away_goalie:Int64, strength_state:Utf8, xgf_home:Float64, xgf_away:Float64`. Empty/malformed `shifts` returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import nhl_xg
from sportsdataverse.nhl.nhl_rapm import build_stints
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
scored = nhl_xg(pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
stints = build_stints(shifts, scored)
```

### `calibration_table(y_true: 'np.ndarray', p_pred: 'np.ndarray', n_bins: 'int' = 10) -> 'pl.DataFrame'` {#calibration_table}

Bucket predicted probabilities into bins and compare to actual outcome rates.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `n_bins` | `int` | `10` | Number of equal-width probability bins. |

**Returns**

A `polars.DataFrame` with columns `bin_mid`, `mean_pred`, `mean_actual`, `n` (one row per non-empty bin).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import calibration_table
calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
```

### `ensure_xg_models(model_dir: 'str | Path | None' = None) -> 'Path'` {#ensure_xg_models}

Return a dir holding the 3 published booster files, downloading any missing ones.

Mirrors the fastRhockey/nflverse download-on-demand + cache pattern -- the documented
exception to "no first-use download" (the boosters are a large, already-published,
already-validated artifact; see Decision D1 in the design spec). An explicit
`model_dir` whose files already exist (e.g. the committed offline test fixtures)
never touches the network.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_dir` | `str \| Path \| None` | `None` | directory to check/populate; `None` resolves via `booster_cache_dir()` (env `NHL_XG_MODEL_DIR` override, else `~/.cache/nhl_xg_models`). |

**Returns**

The resolved directory containing all 3 booster files.

**Example**

```python
from sportsdataverse.nhl.nhl_xg import ensure_xg_models
d = ensure_xg_models()  # downloads on first use, cached after
```

### `espn_nhl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nhl_teams}

espn_nhl_teams - look up NHL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nhl.espn_nhl_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Team abbreviation. |
| `team_alternate_color` | character | Team alternate color hex. |
| `team_color` | character | Team primary color hex. |
| `team_display_name` | character | Team display name. |
| `team_id` | character | Unique team identifier. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_is_all_star` | logical | TRUE if the row represents an All-Star team. |
| `team_location` | character | Team city/location. |
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Team name. |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Team short display name. |
| `team_slug` | character | Team URL slug. |
| `team_uid` | character | ESPN team uid. |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_teams
teams = espn_nhl_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Tampa Bay Lightning (team_id 14)

import polars as pl
teams.filter(pl.col("team_id") == "14").to_dicts()

# Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas

espn_nhl_teams.cache_clear()
teams_pd = espn_nhl_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```

### `expected_goals(adj_xgf_home: 'float', adj_xga_home: 'float', adj_xgf_away: 'float', adj_xga_away: 'float', neutral: 'bool', *, league: 'str' = 'nhl') -> 'tuple[float, float]'` {#expected_goals}

Per-team expected goals, blending own offense with opponent defense.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_xgf_home` | `float` |  | home team's opponent-adjusted xG-for rate. |
| `adj_xga_home` | `float` |  | home team's opponent-adjusted xG-against rate. |
| `adj_xgf_away` | `float` |  | away team's opponent-adjusted xG-for rate. |
| `adj_xga_away` | `float` |  | away team's opponent-adjusted xG-against rate. |
| `neutral` | `bool` |  | whether the game is at a neutral site (drops HFA). |
| `league` | `str` | `'nhl'` | resolves `hfa` via `get_constants`. |

**Returns**

A `(eg_home, eg_away)` tuple of expected goals.

**Example**

```python
from sportsdataverse.nhl.nhl_market import expected_goals
expected_goals(2.8, 2.2, 2.5, 2.4, False)
```

### `fox_nhl_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_boxscore}

NHL boxscore (long: one row per player-stat).

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
from sportsdataverse.nhl import fox_nhl_boxscore
df = fox_nhl_boxscore("...")
```

### `fox_nhl_league_leaders(category: 'str' = 'scoring', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_league_leaders}

NHL statistical leaders (`stats-con`); who=player|team.

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
from sportsdataverse.nhl import fox_nhl_league_leaders
df = fox_nhl_league_leaders("scoring")
```

### `fox_nhl_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_odds}

NHL game odds six-pack (spread / to-win / total per team).

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
from sportsdataverse.nhl import fox_nhl_odds
df = fox_nhl_odds("...")
```

### `fox_nhl_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_pbp}

NHL play-by-play (one row per play; period-based).

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
from sportsdataverse.nhl import fox_nhl_pbp
df = fox_nhl_pbp("...")
```

### `fox_nhl_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_standings}

NHL standings for a team's conference/division.

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
from sportsdataverse.nhl import fox_nhl_standings
df = fox_nhl_standings("...")
```

### `fox_nhl_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_team_gamelog}

NHL team game log (long: one row per game-stat).

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
from sportsdataverse.nhl import fox_nhl_team_gamelog
df = fox_nhl_team_gamelog("...")
```

### `fox_nhl_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_team_roster}

NHL team roster (one row per player).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the position-group tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_team_roster
df = fox_nhl_team_roster("...")
```

### `fox_nhl_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_nhl_team_stats}

NHL team stat leaders by category.

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
from sportsdataverse.nhl import fox_nhl_team_stats
df = fox_nhl_team_stats("...")
```

### `get_constants(league: 'str') -> 'LeagueConstants'` {#get_constants}

Resolve the fitted-constants row for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"nhl"` or `"pwhl"`. |

**Returns**

The `LeagueConstants` row for `league`.

**Example**

```python
from sportsdataverse.nhl.nhl_prediction_constants import get_constants
get_constants("nhl").margin_sd
```

### `in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'` {#in_game_features}

Per-play in-game win-probability features from game state.

Reads the `load_nhl_pbp_full` schema (`home_score`/`away_score`,
`game_seconds_remaining`, `home_skaters`/`away_skaters`,
`home_goalie_in`/`away_goalie_in`). A live feed can populate the same
five features via a documented column map from
`sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_pbp`
(`homeScore`/`awayScore`/`timeRemaining`/`situationCode`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a play-by-play frame shaped like `load_nhl_pbp_full`. |
| `pregame_home_prob` | `float` |  | the model (2) pregame home win probability (e.g. from `win_prob_from_margin`), converted to a logit and carried as a constant per-play anchor feature. |

**Returns**

A polars DataFrame, one row per play. |col_name |type | |:-------------------|:------| |score_diff |Int32 | |sec_remaining |Float64| |sqrt_sec_remaining |Float64| |strength_diff |Int32 | |home_goalie_pulled |Int8 | |away_goalie_pulled |Int8 | |pregame_logit |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_market import in_game_features, win_prob_from_margin
pregame_p = win_prob_from_margin(0.3)
feats = in_game_features(pbp, pregame_home_prob=pregame_p)
```

### `log_loss_score(y_true: 'np.ndarray', p_pred: 'np.ndarray', eps: 'float' = 1e-15) -> 'float'` {#log_loss_score}

Binary cross-entropy loss between predicted probabilities and outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `eps` | `float` | `1e-15` | Clipping bound to avoid `log(0)`. |

**Returns**

The mean log loss.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import log_loss_score
log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### `mae(a: 'np.ndarray', b: 'np.ndarray') -> 'float'` {#mae}

Mean absolute error between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array of values. |
| `b` | `ndarray` |  | Second array of values (same length as `a`). |

**Returns**

The mean absolute error.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import mae
mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
```

### `predict_margin(adj_xgf_home: 'float', adj_xga_home: 'float', adj_xgf_away: 'float', adj_xga_away: 'float', neutral: 'bool', *, league: 'str' = 'nhl') -> 'float'` {#predict_margin}

Expected home-minus-away goal margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_xgf_home` | `float` |  | home team's opponent-adjusted xG-for rate. |
| `adj_xga_home` | `float` |  | home team's opponent-adjusted xG-against rate. |
| `adj_xgf_away` | `float` |  | away team's opponent-adjusted xG-for rate. |
| `adj_xga_away` | `float` |  | away team's opponent-adjusted xG-against rate. |
| `neutral` | `bool` |  | whether the game is at a neutral site. |
| `league` | `str` | `'nhl'` | resolves `hfa` via `get_constants`. |

**Returns**

`eg_home - eg_away`.

**Example**

```python
from sportsdataverse.nhl.nhl_market import predict_margin
predict_margin(2.8, 2.2, 2.5, 2.4, False)
```

### `predict_total(adj_xgf_home: 'float', adj_xga_home: 'float', adj_xgf_away: 'float', adj_xga_away: 'float', neutral: 'bool', *, league: 'str' = 'nhl') -> 'float'` {#predict_total}

Expected total goals, variance-corrected by the fitted `total_scale`.

The raw `eg_home + eg_away` sum is built from opponent-adjusted,
**shrunk** ratings, which systematically compress the total's spread
below the real-world variance (confirmed at fitting time: the OLS slope
of realized total on the raw sum is ~1.91, not 1.0). `total_scale`
corrects for that: the raw total's deviation from the league-average
total is stretched by `total_scale` before adding back the league mean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_xgf_home` | `float` |  | home team's opponent-adjusted xG-for rate. |
| `adj_xga_home` | `float` |  | home team's opponent-adjusted xG-against rate. |
| `adj_xgf_away` | `float` |  | away team's opponent-adjusted xG-for rate. |
| `adj_xga_away` | `float` |  | away team's opponent-adjusted xG-against rate. |
| `neutral` | `bool` |  | whether the game is at a neutral site. |
| `league` | `str` | `'nhl'` | resolves `hfa`/`avg_total_goals`/`total_scale` via `get_constants`. |

**Returns**

The variance-corrected expected combined goal total for the game.

**Example**

```python
from sportsdataverse.nhl.nhl_market import predict_total
predict_total(2.8, 2.2, 2.5, 2.4, False)
```

### `prepare_xg_features(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#prepare_xg_features}

Port of `helper_nhl_prepare_xg_data` -- one row per unblocked shot, model features.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame (`x`, `x_fixed`, `strength_state`, `home_skaters`/`away_skaters`, `game_seconds`, `event_id`, `secondary_type`, `event_team_abbr`, `home_abbr`/`away_abbr`, `season`, `empty_net` -- see `load_nhl_pbp_full`'s returns table). |

**Returns**

one row per unblocked shot (`SHOT`/`MISSED_SHOT`/`GOAL`) carrying every era one-hot, shot-type one-hot, last-event one-hot, and the derived `rebound`/`rush`/`cross_ice_event`/`total_skaters_on`/ `event_team_advantage`/`empty_net` columns the boosters expect. Empty/ malformed input returns a zero-row frame (never raises).

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import prepare_xg_features
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
feat = prepare_xg_features(pbp)
print(feat.shape)
```

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### `spearman_corr(a: 'np.ndarray', b: 'np.ndarray') -> 'float'` {#spearman_corr}

Spearman rank correlation between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array of values. |
| `b` | `ndarray` |  | Second array of values (same length as `a`). |

**Returns**

The Spearman rank correlation coefficient.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import spearman_corr
spearman_corr(np.array([1, 2, 3]), np.array([3, 1, 2]))
```

### `team_fullname_to_abbr(name: 'str') -> 'str | None'` {#team_fullname_to_abbr}

Map an NHL full team display name to its abbreviation, or `None` if unknown.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | a full team display name as it appears in `load_nhl_shifts`'s `event_team` column (e.g. `"Buffalo Sabres"`). |

**Returns**

The team abbreviation matching `load_nhl_pbp_full`'s `event_team_abbr` / `home_abbr` / `away_abbr` convention, or `None` for an unmapped name.

**Example**

```python
from sportsdataverse.nhl.nhl_player_impact_constants import team_fullname_to_abbr
team_fullname_to_abbr("Buffalo Sabres")  # "BUF"
```

### `team_game_xg_rates(pbp: 'pl.DataFrame', schedule: 'pl.DataFrame', *, even_strength_only: 'bool' = True) -> 'pl.DataFrame'` {#team_game_xg_rates}

Per-(game, team) even-strength xG-for/against + realized goals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a play-by-play frame shaped like `load_nhl_pbp_full`/`load_nhl_pbp_lite` (needs `game_id`, `event_team_abbr`, `home_abbr`, `away_abbr`, `home_skaters`, `away_skaters`, `home_goalie_in`, `away_goalie_in`, `xg`). |
| `schedule` | `DataFrame` |  | a schedule frame with `game_id`, `season`, `date`, `home_abbr`, `away_abbr`, `neutral_site` (`home_goals`/`away_goals` are accepted but ignored -- realized `gf`/`ga` are derived from the pbp's own GOAL events, never from schedule scores; see the module note on the `load_nhl_schedule(s)` placeholder-score bug for seasons <= 2023). |
| `even_strength_only` | `bool` | `True` | restrict to `home_skaters == away_skaters == 5` with both goalies in net (filters out PP/PK/empty-net distortion). |

**Returns**

A polars DataFrame, one row per (game_id, team), both home and away. |col_name |type | |:------------|:------| |game_id |String | |season |Int64 | |date |Date | |team |String | |opp_team |String | |is_home |Boolean| |neutral_site |Boolean| |xgf |Float64| |xga |Float64| |gf |Int64 | |ga |Int64 |

**Example**

```python
from sportsdataverse.nhl.nhl_team_ratings import team_game_xg_rates
from sportsdataverse.nhl import load_nhl_pbp_full, load_nhl_schedules

pbp = load_nhl_pbp_full([2023])
sched = load_nhl_schedules([2023])
rates = team_game_xg_rates(pbp, sched)
print(rates.filter(pl.col("team") == "TOR").head())
```

### `weighted_ridge(X: 'Any', y: 'np.ndarray', w: 'np.ndarray', lam: 'float') -> 'np.ndarray'` {#weighted_ridge}

Solve the weighted ridge normal equations `(X'WX + lam*I)^-1 X'Wy`.

Dense path (`numpy.linalg.solve`) for small/dense `X`; conjugate-gradient
(`scipy.sparse.linalg.cg`) for `scipy.sparse` `X` (the skater-RAPM design
matrix, ~thousands of columns).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `Any` |  | design matrix, dense `numpy.ndarray` or any `scipy.sparse` matrix. |
| `y` | `ndarray` |  | response vector. |
| `w` | `ndarray` |  | nonnegative observation weights (e.g. stint duration in seconds). |
| `lam` | `float` |  | ridge penalty. |

**Returns**

The fitted coefficient vector.

**Example**

```python
import numpy as np
from sportsdataverse.nhl.nhl_player_impact_constants import weighted_ridge
X = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
y = np.array([2.0, -1.0, 1.0])
beta = weighted_ridge(X, y, np.ones(3), lam=1e-6)
```

### `win_prob_from_margin(exp_margin: 'float', *, league: 'str' = 'nhl') -> 'float'` {#win_prob_from_margin}

Convert an expected goal margin to a home win probability via Phi(margin/sigma).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | expected home-minus-away goal margin. |
| `league` | `str` | `'nhl'` | resolves `margin_sd` via `get_constants`. |

**Returns**

`P(home win)` in (0, 1).

**Example**

```python
from sportsdataverse.nhl.nhl_market import win_prob_from_margin
win_prob_from_margin(0.35)
```
