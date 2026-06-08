---
title: NHL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nhl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nhl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

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
| `athlete_id` | integer |  |
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `alternate_id` | character |  |
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
| `birth_country_abbreviation` | character |  |
| `headshot_href` | character |  |
| `headshot_alt` | character |  |
| `hand_type` | character |  |
| `hand_abbreviation` | character |  |
| `hand_display_value` | character |  |
| `contracts_href` | character |  |
| `experience_years` | integer |  |
| `draft_display_text` | character |  |
| `draft_round` | integer |  |
| `draft_year` | integer |  |
| `draft_selection` | integer |  |
| `draft_team_href` | character |  |
| `status_id` | character |  |
| `status_name` | character |  |
| `status_type` | character |  |
| `status_abbreviation` | character |  |
| `jersey_right` | character |  |
| `display_name` | character |  |
| `scratched` | logical |  |
| `scratch_reason` | character |  |
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
from sportsdataverse.nhl import espn_nhl_game_rosters
rosters = espn_nhl_game_rosters(game_id=401559395)
print(rosters.shape)
rosters.select(["athlete_display_name", "jersey", "team_abbreviation", "starter"]).head(10)

Just the starters::

import polars as pl
rosters.filter(pl.col("starter") == True).select(["athlete_display_name", "team_abbreviation"])

Pandas round-trip::

rosters_pd = espn_nhl_game_rosters(game_id=401559395, return_as_pandas=True)
rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()
```

### `espn_nhl_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_nhl_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary`

.. note:: This is the **ESPN** NHL play-by-play, not the modern NHL api-web one. The two surfaces have different ID spaces and different schemas — they are NOT interchangeable: - ``espn_nhl_pbp(game_id)`` uses **ESPN event IDs** (e.g. ``401559395``). Returns a Dict of ~17 sub-frames matching the ESPN Site v2 summary shape (boxscore / plays / leaders / standings / etc.). Useful for historical alignment with the hoopR / wehoop R-package data stack. - ``nhl_web_pbp(game_id)`` + ``parse_nhl_web_pbp(payload)`` uses **NHL native game IDs** (e.g. ``2023030417``). Returns the modern api-web.nhle.com PBP shape (``plays[]`` with ``eventId``, ``typeCode``, ``typeDescKey``, ``periodDescriptor``, nested ``details``). Use this for live games + modern NHL.com source-of-truth data. Pick the surface that matches your ID space + downstream join keys. The two cannot be cross-referenced by ``game_id``.

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

Inspect parsed plays and a quick filter on goal events::

import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
goals = plays.filter(pl.col("type.text") == "Goal")
goals.select(["period", "time", "text"]).head()

Pull the unparsed payload for custom downstream parsing::

raw = espn_nhl_pbp(game_id=401559395, raw=True)
sorted(raw.keys())[:5]
```

### `espn_nhl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NHL athlete's ESPN **season** stat line as one wide row.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns (for hockey: ``offensive_*``, ``defensive_*``, ``penalties_*``, ...), the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters. For the richer multi-category web-v3 payload use :func:`sportsdataverse.nhl.espn_nhl_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NHL athlete identifier (e.g. ``3895074`` for Connor McDavid). |
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
| `general_games` | double |  |
| `general_game_started` | double |  |
| `general_team_games_played` | double |  |
| `general_wins` | double |  |
| `general_losses` | double |  |
| `general_ties` | character |  |
| `general_plus_minus` | double |  |
| `general_time_on_ice` | double |  |
| `general_time_on_ice_per_game` | double |  |
| `general_shifts` | double |  |
| `general_shifts_per_game` | double |  |
| `general_production` | double |  |
| `offensive_goals` | double |  |
| `offensive_avg_goals` | double |  |
| `offensive_assists` | double |  |
| `offensive_shots_total` | double |  |
| `offensive_avg_shots` | double |  |
| `offensive_points` | double |  |
| `offensive_points_per_game` | double |  |
| `offensive_power_play_goals` | double |  |
| `offensive_power_play_assists` | double |  |
| `offensive_short_handed_goals` | double |  |
| `offensive_short_handed_assists` | double |  |
| `offensive_shootout_attempts` | double |  |
| `offensive_shootout_goals` | double |  |
| `offensive_shootout_shot_pct` | double |  |
| `offensive_shooting_pct` | double |  |
| `offensive_total_face_offs` | double |  |
| `offensive_faceoffs_won` | double |  |
| `offensive_faceoffs_lost` | double |  |
| `offensive_faceoff_percent` | double |  |
| `offensive_game_tying_goals` | character |  |
| `offensive_game_winning_goals` | double |  |
| `penalties_penalty_minutes` | double |  |
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
from sportsdataverse.nhl import espn_nhl_player_stats
df = espn_nhl_player_stats(athlete_id=3895074, season=2023)
df.select(["full_name", "team_display_name", "offensive_goals"])
```

### `espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

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
| `id` | character |  |
| `uid` | character |  |
| `date` | character |  |
| `attendance` | integer |  |
| `time_valid` | logical |  |
| `neutral_site` | logical |  |
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
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer |  |
| `season` | integer |  |
| `season_type` | integer |  |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_schedule
sched = espn_nhl_schedule(dates=20230613)  # 2023 Stanley Cup Final game date
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

Pull a regular-season slate from a season-year::

reg = espn_nhl_schedule(dates=2023, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)

Pandas round-trip for one date::

espn_nhl_schedule(dates=20230613, return_as_pandas=True).head()
```

## NHL native

### `nhl_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### `nhl_records_coach_milestone_wins(wins: 'int', playoffs: 'bool' = False, **filters) -> 'Dict'`

Coaches who reached a wins milestone in fewest games.

Wraps one of the ``/coach-fewest-games-to-{N}-wins`` or ``/coach-fewest-games-to-{N}-playoff-wins`` paths. Supported *wins* values: ``50, 100, 150, 200, 300, 400, 500, 600, 700, 800, 900, 1000`` (regular season); ``50, 100, 150`` (playoffs).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `wins` | `int` |  | Milestone win total (e.g. ``100``). |
| `playoffs` | `bool` | `False` | If ``True``, use the playoff-wins path. |

**Returns**

Coaches who hit the milestone, sorted by games needed.

### `nhl_records_comeback_wins(scope: 'str' = 'league', **filters) -> 'Dict'`

Comeback wins from a multi-goal deficit.

Wraps: * ``GET /comeback-league-wins`` when *scope* is ``"league"``. * ``GET /comeback-franchise-wins`` when *scope* is ``"franchise"``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scope` | `str` | `'league'` | ``"league"`` (default) or ``"franchise"``. |

**Returns**

Games where the team overcame a deficit to win.

### `nhl_records_consecutive_goal_seasons(goals: 'int' = 50, **filters) -> 'Dict'`

Skaters with the most consecutive N-goal seasons.

Wraps one of: * ``GET /consecutive-20-goal-seasons`` * ``GET /consecutive-30-goal-seasons`` * ``GET /consecutive-40-goal-seasons`` * ``GET /consecutive-50-goal-seasons`` * ``GET /consecutive-60-goal-seasons``

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `goals` | `int` | `50` | Goal threshold — one of ``20, 30, 40, 50, 60``. |

**Returns**

Skaters sorted by consecutive-season streak.

### `nhl_records_fastest_goals(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals by one team in a single game.

Wraps one of: * ``GET /fastest-2-goals-one-team`` * ``GET /fastest-3-goals-one-team`` * ``GET /fastest-4-goals-one-team`` * ``GET /fastest-5-goals-one-team``

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Goal count — one of ``2, 3, 4, 5``. |

**Returns**

Games where the milestone was set, sorted by elapsed time (fastest first).

### `nhl_records_fastest_goals_both_teams(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals combined (both teams) in a single game.

Wraps one of: * ``GET /fastest-2-goals-both-teams`` * ``GET /fastest-3-goals-both-teams`` * ``GET /fastest-4-goals-both-teams`` * ``GET /fastest-5-goals-both-teams`` * ``GET /fastest-6-goals-both-teams``

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Combined goal count — one of ``2, 3, 4, 5, 6``. |

**Returns**

Sorted by elapsed time (fastest first).

### `nhl_records_games_played_streak_skaters(active_only: 'bool' = False, **filters) -> 'Dict'`

Consecutive games-played streaks for skaters.

Wraps ``GET /games-played-streak-skaters`` (career) or ``GET /games-played-active-streak-skaters`` (currently active streaks).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `active_only` | `bool` | `False` | If ``True``, return only active streaks. |

**Returns**

Skaters sorted by streak length.

### `nhl_scoreboard(date: 'Optional[str]' = None, team: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Dict'`

In-game scoreboard payload (renamed from ``nhl_web_scoreboard``).

Picks among three mutually-exclusive NHL api-web forms (kept hand-written because the URL-builder codegen can't represent the 3-way branch): * ``GET /v1/scoreboard/{team}/now`` -- team-scoped now (when ``team`` set), * ``GET /v1/scoreboard/{date}`` -- league-wide on a date, * ``GET /v1/scoreboard/now`` -- league-wide now (both args None).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | ``YYYY-MM-DD``; ``None`` -> ``/now``. Mutually exclusive with ``team``. |
| `team` | `Optional[str]` | `None` | 3-letter abbreviation; takes precedence over ``date``. |
| `return_parsed` | `bool` | `True` | dispatch the raw payload through ``parse_nhl_web_scoreboard``. |
| `return_as_pandas` | `bool` | `False` | with ``return_parsed``, return pandas instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON ``Dict`` when ``return_parsed=False``.

| col_name | type | description |
|---|---|---|
| `scoreboard_date` | character |  |
| `id` | integer |  |
| `season` | integer |  |
| `game_type` | integer |  |
| `game_date` | character |  |
| `game_center_link` | character |  |
| `start_time_utc` | character |  |
| `eastern_utc_offset` | character |  |
| `venue_utc_offset` | character |  |
| `tv_broadcasts` | character |  |
| `game_state` | character |  |
| `game_schedule_state` | character |  |
| `tickets_link` | character |  |
| `tickets_link_fr` | character |  |
| `period` | double |  |
| `three_min_recap` | character |  |
| `three_min_recap_fr` | character |  |
| `venue_default` | character |  |
| `away_team_id` | integer |  |
| `away_team_name_default` | character |  |
| `away_team_name_fr` | character |  |
| `away_team_common_name_default` | character |  |
| `away_team_place_name_with_preposition_default` | character |  |
| `away_team_place_name_with_preposition_fr` | character |  |
| `away_team_abbrev` | character |  |
| `away_team_score` | double |  |
| `away_team_logo` | character |  |
| `home_team_id` | integer |  |
| `home_team_name_default` | character |  |
| `home_team_name_fr` | character |  |
| `home_team_common_name_default` | character |  |
| `home_team_place_name_with_preposition_default` | character |  |
| `home_team_place_name_with_preposition_fr` | character |  |
| `home_team_abbrev` | character |  |
| `home_team_score` | double |  |
| `home_team_logo` | character |  |
| `period_descriptor_number` | double |  |
| `period_descriptor_period_type` | character |  |
| `period_descriptor_max_regulation_periods` | double |  |
| `series_status_round` | integer |  |
| `series_status_series_abbrev` | character |  |
| `series_status_game` | integer |  |
| `series_status_top_seed_team_abbrev` | character |  |
| `series_status_top_seed_wins` | integer |  |
| `series_status_bottom_seed_team_abbrev` | character |  |
| `series_status_bottom_seed_wins` | integer |  |
| `period_descriptor_ot_periods` | double |  |
| `away_team_record` | character |  |
| `home_team_record` | character |  |

**Example**

```python
>>> nhl_scoreboard(date="2024-03-01")
```

## Utilities & helpers

### `most_recent_nhl_season()`

most_recent_nhl_season - return the season year for "today".

NHL seasons are labeled by the year they end in. October flips the label to next calendar year (the new season just started), otherwise the current calendar year is returned.

**Returns**

A season year suitable for season-aware loaders / schedule helpers.

**Example**

```python
from sportsdataverse.nhl import most_recent_nhl_season, espn_nhl_calendar
season = most_recent_nhl_season()
cal = espn_nhl_calendar(season=season)
print(season, cal.height)
```

### `year_to_season(year)`

year_to_season - format a starting year as the canonical ``YYYY-YY`` season string.

NHL season strings (used by ``statsapi`` / ``api-web.nhle.com``) are of the form ``"2023-24"``. This helper converts a starting year (``2023``) into that string.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` |  |  | Starting calendar year of the season (e.g. ``2023``). |

**Returns**

Season string formatted as ``"YYYY-YY"``.

**Example**

```python
from sportsdataverse.nhl import year_to_season
year_to_season(2023)  # '2023-24'
year_to_season(2009)  # '2009-10'
year_to_season(1999)  # '1999-00'
```

## Other

### `espn_nhl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nhl_teams - look up NHL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nhl.espn_nhl_teams.clear_cache().

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
from sportsdataverse.nhl import espn_nhl_teams
teams = espn_nhl_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

Find Tampa Bay Lightning (team_id 14)::

import polars as pl
teams.filter(pl.col("team_id") == "14").to_dicts()

Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas::

espn_nhl_teams.cache_clear()
teams_pd = espn_nhl_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```

### `scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |
