---
title: "NHL — additional Python functions — Play-by-play, schedule & rosters"
sidebar_label: "Play-by-play, schedule & rosters"
sidebar_position: 3
description: "NHL — additional Python functions — Play-by-play, schedule & rosters — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — Play-by-play, schedule & rosters

### espn_nhl_game_rosters {#espn_nhl_game_rosters}

`espn_nhl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

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
