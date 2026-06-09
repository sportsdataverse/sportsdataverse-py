---
title: NHL — additional Python functions
sidebar_label: Additional functions
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
| `contracts_href` | character |  |
| `experience_years` | integer | Experience years. |
| `draft_display_text` | character | Draft display text. |
| `draft_round` | integer | Draft round. |
| `draft_year` | integer | Draft year the lottery applies to. |
| `draft_selection` | integer | Draft selection. |
| `draft_team_href` | character |  |
| `status_id` | character | Status identifier. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `jersey_right` | character |  |
| `display_name` | character | Player display name. |
| `scratched` | logical | Whether the player was a healthy scratch. |
| `scratch_reason` | character | Reason for scratch (if applicable). |
| `athlete_href` | character |  |
| `position_href` | character |  |
| `statistics_href` | character |  |
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
| `team_alternate_ids_sdr` | character |  |
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
| `defensive_goals_against` | double |  |
| `defensive_avg_goals_against` | double |  |
| `defensive_shots_against` | double |  |
| `defensive_avg_shots_against` | double |  |
| `defensive_shootout_saves` | character |  |
| `defensive_shootout_shots_against` | double |  |
| `defensive_shootout_save_pct` | double |  |
| `defensive_empty_net_goals_against` | character |  |
| `defensive_shutouts` | double |  |
| `defensive_saves` | double |  |
| `defensive_save_pct` | double |  |
| `defensive_overtime_losses` | double |  |
| `defensive_blocked_shots` | double |  |
| `defensive_hits` | double |  |
| `defensive_even_strength_saves` | double |  |
| `defensive_power_play_saves` | double |  |
| `defensive_short_handed_saves` | double |  |
| `general_games` | double | Career games played. |
| `general_game_started` | double |  |
| `general_team_games_played` | double |  |
| `general_wins` | double |  |
| `general_losses` | double |  |
| `general_ties` | character |  |
| `general_plus_minus` | double | A player's estimated on-court impact on team performance measured in point differential per 100 possessions. |
| `general_time_on_ice` | double |  |
| `general_time_on_ice_per_game` | double |  |
| `general_shifts` | double |  |
| `general_shifts_per_game` | double |  |
| `general_production` | double |  |
| `offensive_goals` | double | Goals (offensive category). |
| `offensive_avg_goals` | double |  |
| `offensive_assists` | double | Career assists. |
| `offensive_shots_total` | double | Shots on goal. |
| `offensive_avg_shots` | double |  |
| `offensive_points` | double | Career points. |
| `offensive_points_per_game` | double |  |
| `offensive_power_play_goals` | double | Power-play goals. |
| `offensive_power_play_assists` | double |  |
| `offensive_short_handed_goals` | double |  |
| `offensive_short_handed_assists` | double |  |
| `offensive_shootout_attempts` | double |  |
| `offensive_shootout_goals` | double |  |
| `offensive_shootout_shot_pct` | double |  |
| `offensive_shooting_pct` | double | Shooting percentage. |
| `offensive_total_face_offs` | double |  |
| `offensive_faceoffs_won` | double |  |
| `offensive_faceoffs_lost` | double |  |
| `offensive_faceoff_percent` | double |  |
| `offensive_game_tying_goals` | character |  |
| `offensive_game_winning_goals` | double | Game-winning goals. |
| `penalties_penalty_minutes` | double | Career penalty minutes. |
| `penalties_major_penalties` | double |  |
| `penalties_minor_penalties` | double |  |
| `penalties_match_penalties` | double |  |
| `penalties_misconducts` | double |  |
| `penalties_game_misconducts` | double |  |
| `penalties_boarding_penalties` | double |  |
| `penalties_unsportsmanlike_penalties` | double |  |
| `penalties_fighting_penalties` | double |  |
| `penalties_avg_fights` | double |  |
| `penalties_time_between_fights` | character |  |
| `penalties_instigator_penalties` | double |  |
| `penalties_charging_penalties` | double |  |
| `penalties_hooking_penalties` | double |  |
| `penalties_tripping_penalties` | double |  |
| `penalties_roughing_penalties` | double |  |
| `penalties_holding_penalties` | double |  |
| `penalties_interference_penalties` | double |  |
| `penalties_slashing_penalties` | double |  |
| `penalties_high_sticking_penalties` | double |  |
| `penalties_cross_checking_penalties` | double |  |
| `penalties_stick_holding_penalties` | double |  |
| `penalties_goalie_interference_penalties` | double |  |
| `penalties_elbowing_penalties` | double |  |
| `penalties_diving_penalties` | double |  |
| `rpi_wins` | double |  |
| `rpi_losses` | double |  |
| `rpi_ot_losses` | character |  |
| `rpi_points` | double |  |
| `rpi_rpi` | character |  |
| `rpi_sos` | character |  |
| `rpi_power_rank` | character |  |
| `rpi_points_for` | character |  |
| `rpi_points_against` | character |  |
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
| `venue_address_country` | character |  |
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
| `home_linescores` | integer |  |
| `home_records` | character |  |
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
| `away_linescores` | integer |  |
| `away_records` | character |  |
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

### `nhl_pbp_disk(game_id, path_to_json)` {#nhl_pbp_disk}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

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
| `scoreboard_date` | character |  |
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
| `tickets_link` | character |  |
| `tickets_link_fr` | character |  |
| `period` | double | Period number. |
| `three_min_recap` | character | Link to the three-minute recap. |
| `three_min_recap_fr` | character | Link to the French three-minute recap. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_name_default` | character |  |
| `away_team_name_fr` | character |  |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | double | Away team final score. |
| `away_team_logo` | character | URL to the away team logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_name_default` | character |  |
| `home_team_name_fr` | character |  |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | double | Home team final score. |
| `home_team_logo` | character | URL to the home team logo. |
| `period_descriptor_number` | double | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | double | Maximum number of regulation periods. |
| `series_status_round` | integer |  |
| `series_status_series_abbrev` | character |  |
| `series_status_game` | integer |  |
| `series_status_top_seed_team_abbrev` | character |  |
| `series_status_top_seed_wins` | integer |  |
| `series_status_bottom_seed_team_abbrev` | character |  |
| `series_status_bottom_seed_wins` | integer |  |
| `period_descriptor_ot_periods` | double |  |
| `away_team_record` | character | Away team's win-loss record. |
| `home_team_record` | character | Home team's win-loss record. |

**Example**

```python
>>> nhl_scoreboard(date="2024-03-01")
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
| `series_letter` | character |  |
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
| `scratches` | logical |  |
| `linescore` | logical |  |
| `three_stars` | logical | Whether three stars data is available. |
| `shifts` | logical | Number of shifts. |
| `officials` | logical | Whether officials data is available. |
| `shots_by_period` | logical | Whether shots-by-period data is available. |
| `shootout` | logical | Whether shootout data is available. |

**Example**

```python
>>> load_nhl_games()
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

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |
