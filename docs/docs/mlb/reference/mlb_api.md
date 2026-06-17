---
title: MLB — MLB Stats API
sidebar_label: MLB Stats API
sidebar_position: 10
---
# MLB — MLB Stats API

`sportsdataverse.mlb` — 64 endpoints.

## `mlb_schedule_postseason`

GET /api/v1/schedule/postseason — postseason-only schedule for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason](https://statsapi.mlb.com/api/v1/schedule/postseason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `schedule_date` | character |  |
| `game_pk` | integer | Unique game identifier. |
| `game_guid` | character | Globally unique game identifier (GUID). |
| `link` | character | API link to the game feed. |
| `game_type` | character | Game type code (R, P, etc.). |
| `season` | character | Season year. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `official_date` | character | Official game date (YYYY-MM-DD). |
| `is_tie` | logical | Whether the game ended in a tie. |
| `is_featured_game` | logical | Whether the game is a featured game. |
| `game_number` | integer | Game number within a doubleheader. |
| `public_facing` | logical | Whether the game is public-facing. |
| `double_header` | character | Doubleheader indicator ('N', 'S', 'Y'). |
| `gameday_type` | character | Gameday data feed type. |
| `tiebreaker` | character | Whether the game is a tiebreaker. |
| `calendar_event_id` | character | Calendar event identifier. |
| `season_display` | character | Display string for the season. |
| `day_night` | character | Day or night game indicator. |
| `description` | character | Long-form description text. |
| `scheduled_innings` | integer | Scheduled number of innings. |
| `reverse_home_away_status` | logical | Whether home/away teams are reversed. |
| `inning_break_length` | integer | Length of inning breaks in seconds. |
| `games_in_series` | integer | Number of games in the series. |
| `series_game_number` | integer | Game number within the series. |
| `series_description` | character | Description of the series. |
| `record_source` | character | Source of the schedule record. |
| `if_necessary` | character | Whether the game is played only if necessary. |
| `if_necessary_description` | character | Description of the if-necessary status. |
| `status_abstract_game_state` | character | Abstract game state (e.g. 'Final'). |
| `status_coded_game_state` | character | Coded game state. |
| `status_detailed_state` | character | Detailed game state. |
| `status_status_code` | character | Status code for the game. |
| `status_start_time_tbd` | logical | Whether the start time is TBD. |
| `status_abstract_game_code` | character | Abstract game state code. |
| `teams_away_team_id` | integer | Away team MLBAM ID. |
| `teams_away_team_name` | character | Away team name. |
| `teams_away_team_link` | character | API link to the away team. |
| `teams_away_league_record_wins` | integer | Away team league-record wins. |
| `teams_away_league_record_losses` | integer | Away team league-record losses. |
| `teams_away_league_record_ties` | integer | Away team league-record ties. |
| `teams_away_league_record_pct` | character | Away team winning percentage. |
| `teams_away_score` | integer | Away team score. |
| `teams_away_is_winner` | logical | Whether the away team won. |
| `teams_away_split_squad` | logical | Whether the away team is a split squad. |
| `teams_away_series_number` | integer | Away team's series number. |
| `teams_home_team_id` | integer | Home team MLBAM ID. |
| `teams_home_team_name` | character | Home team name. |
| `teams_home_team_link` | character | API link to the home team. |
| `teams_home_league_record_wins` | integer | Home team league-record wins. |
| `teams_home_league_record_losses` | integer | Home team league-record losses. |
| `teams_home_league_record_ties` | integer | Home team league-record ties. |
| `teams_home_league_record_pct` | character | Home team winning percentage. |
| `teams_home_score` | integer | Home team score. |
| `teams_home_is_winner` | logical | Whether the home team won. |
| `teams_home_split_squad` | logical | Whether the home team is a split squad. |
| `teams_home_series_number` | integer | Home team's series number. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `content_link` | character | API link to the game content. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_schedule_postseason()
```

_Last validated n/a._

## `mlb_pbp`

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live`

**Valid URL:** [https://statsapi.mlb.com/api/v1.1/game/716390/feed/live](https://statsapi.mlb.com/api/v1.1/game/716390/feed/live)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  |  |
| `language` | `language` |  |  | `Y` |  |
| `language` | `timecode` |  |  | `Y` |  |
| `hydrate` | `hydrate` |  |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_pbp(game_pk=716390)
```

_Last validated n/a._

## `mlb_boxscore`

GET /api/v1/game/{gamePk}/boxscore — team + player boxscore for one game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/boxscore](https://statsapi.mlb.com/api/v1/game/716390/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_side` | character | Home or away indicator. |
| `team_id` | integer | Unique ESPN team identifier. |
| `team_name` | character | Team name. |
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `parent_team_id` | integer |  |
| `batting_order` | character | Spot in the batting order (1-9). |
| `all_positions` | character |  |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |
| `person_boxscore_name` | character | Name as shown in box scores. |
| `position_code` | character | Numeric scorekeeping position code. |
| `position_name` | character | Position name. |
| `position_type` | character | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | character | Position abbreviation. |
| `status_code` | character | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | character | Roster status description (e.g. 'Active'). |
| `stats_batting_summary` | character |  |
| `stats_batting_games_played` | double |  |
| `stats_batting_fly_outs` | double |  |
| `stats_batting_ground_outs` | double |  |
| `stats_batting_air_outs` | double |  |
| `stats_batting_runs` | double |  |
| `stats_batting_doubles` | double |  |
| `stats_batting_triples` | double |  |
| `stats_batting_home_runs` | double |  |
| `stats_batting_strike_outs` | double |  |
| `stats_batting_base_on_balls` | double |  |
| `stats_batting_intentional_walks` | double |  |
| `stats_batting_hits` | double |  |
| `stats_batting_hit_by_pitch` | double |  |
| `stats_batting_at_bats` | double |  |
| `stats_batting_caught_stealing` | double |  |
| `stats_batting_stolen_bases` | double |  |
| `stats_batting_stolen_base_percentage` | character |  |
| `stats_batting_ground_into_double_play` | double |  |
| `stats_batting_ground_into_triple_play` | double |  |
| `stats_batting_plate_appearances` | double |  |
| `stats_batting_total_bases` | double |  |
| `stats_batting_rbi` | double |  |
| `stats_batting_left_on_base` | double |  |
| `stats_batting_sac_bunts` | double |  |
| `stats_batting_sac_flies` | double |  |
| `stats_batting_catchers_interference` | double |  |
| `stats_batting_pickoffs` | double |  |
| `stats_batting_at_bats_per_home_run` | character |  |
| `stats_batting_pop_outs` | double |  |
| `stats_batting_line_outs` | double |  |
| `stats_fielding_caught_stealing` | double |  |
| `stats_fielding_stolen_bases` | double |  |
| `stats_fielding_stolen_base_percentage` | character |  |
| `stats_fielding_caught_stealing_percentage` | character |  |
| `stats_fielding_assists` | double |  |
| `stats_fielding_put_outs` | double |  |
| `stats_fielding_errors` | double |  |
| `stats_fielding_chances` | double |  |
| `stats_fielding_fielding` | character |  |
| `stats_fielding_passed_ball` | double |  |
| `stats_fielding_pickoffs` | double |  |
| `season_stats_batting_games_played` | integer |  |
| `season_stats_batting_fly_outs` | integer |  |
| `season_stats_batting_ground_outs` | integer |  |
| `season_stats_batting_air_outs` | integer |  |
| `season_stats_batting_runs` | integer |  |
| `season_stats_batting_doubles` | integer |  |
| `season_stats_batting_triples` | integer |  |
| `season_stats_batting_home_runs` | integer |  |
| `season_stats_batting_strike_outs` | integer |  |
| `season_stats_batting_base_on_balls` | integer |  |
| `season_stats_batting_intentional_walks` | integer |  |
| `season_stats_batting_hits` | integer |  |
| `season_stats_batting_hit_by_pitch` | integer |  |
| `season_stats_batting_avg` | character |  |
| `season_stats_batting_at_bats` | integer |  |
| `season_stats_batting_obp` | character |  |
| `season_stats_batting_slg` | character |  |
| `season_stats_batting_ops` | character |  |
| `season_stats_batting_caught_stealing` | integer |  |
| `season_stats_batting_stolen_bases` | integer |  |
| `season_stats_batting_stolen_base_percentage` | character |  |
| `season_stats_batting_caught_stealing_percentage` | character |  |
| `season_stats_batting_ground_into_double_play` | integer |  |
| `season_stats_batting_ground_into_triple_play` | integer |  |
| `season_stats_batting_plate_appearances` | integer |  |
| `season_stats_batting_total_bases` | integer |  |
| `season_stats_batting_rbi` | integer |  |
| `season_stats_batting_left_on_base` | integer |  |
| `season_stats_batting_sac_bunts` | integer |  |
| `season_stats_batting_sac_flies` | integer |  |
| `season_stats_batting_babip` | character |  |
| `season_stats_batting_ground_outs_to_airouts` | character |  |
| `season_stats_batting_catchers_interference` | integer |  |
| `season_stats_batting_pickoffs` | integer |  |
| `season_stats_batting_at_bats_per_home_run` | character |  |
| `season_stats_batting_pop_outs` | integer |  |
| `season_stats_batting_line_outs` | integer |  |
| `season_stats_pitching_games_played` | integer |  |
| `season_stats_pitching_games_started` | integer |  |
| `season_stats_pitching_fly_outs` | integer |  |
| `season_stats_pitching_ground_outs` | integer |  |
| `season_stats_pitching_air_outs` | integer |  |
| `season_stats_pitching_runs` | integer |  |
| `season_stats_pitching_doubles` | integer |  |
| `season_stats_pitching_triples` | integer |  |
| `season_stats_pitching_home_runs` | integer |  |
| `season_stats_pitching_strike_outs` | integer |  |
| `season_stats_pitching_base_on_balls` | integer |  |
| `season_stats_pitching_intentional_walks` | integer |  |
| `season_stats_pitching_hits` | integer |  |
| `season_stats_pitching_hit_by_pitch` | integer |  |
| `season_stats_pitching_at_bats` | integer |  |
| `season_stats_pitching_obp` | character |  |
| `season_stats_pitching_caught_stealing` | integer |  |
| `season_stats_pitching_stolen_bases` | integer |  |
| `season_stats_pitching_stolen_base_percentage` | character |  |
| `season_stats_pitching_caught_stealing_percentage` | character |  |
| `season_stats_pitching_number_of_pitches` | integer |  |
| `season_stats_pitching_era` | character |  |
| `season_stats_pitching_innings_pitched` | character |  |
| `season_stats_pitching_wins` | integer |  |
| `season_stats_pitching_losses` | integer |  |
| `season_stats_pitching_saves` | integer |  |
| `season_stats_pitching_save_opportunities` | integer |  |
| `season_stats_pitching_holds` | integer |  |
| `season_stats_pitching_blown_saves` | integer |  |
| `season_stats_pitching_earned_runs` | integer |  |
| `season_stats_pitching_whip` | character |  |
| `season_stats_pitching_batters_faced` | integer |  |
| `season_stats_pitching_outs` | integer |  |
| `season_stats_pitching_games_pitched` | integer |  |
| `season_stats_pitching_complete_games` | integer |  |
| `season_stats_pitching_shutouts` | integer |  |
| `season_stats_pitching_balls` | integer |  |
| `season_stats_pitching_strikes` | integer |  |
| `season_stats_pitching_strike_percentage` | character |  |
| `season_stats_pitching_hit_batsmen` | integer |  |
| `season_stats_pitching_balks` | integer |  |
| `season_stats_pitching_wild_pitches` | integer |  |
| `season_stats_pitching_pickoffs` | integer |  |
| `season_stats_pitching_ground_outs_to_airouts` | character |  |
| `season_stats_pitching_rbi` | integer |  |
| `season_stats_pitching_win_percentage` | character |  |
| `season_stats_pitching_pitches_per_inning` | character |  |
| `season_stats_pitching_games_finished` | integer |  |
| `season_stats_pitching_strikeout_walk_ratio` | character |  |
| `season_stats_pitching_strikeouts_per9_inn` | character |  |
| `season_stats_pitching_walks_per9_inn` | character |  |
| `season_stats_pitching_hits_per9_inn` | character |  |
| `season_stats_pitching_runs_scored_per9` | character |  |
| `season_stats_pitching_home_runs_per9` | character |  |
| `season_stats_pitching_inherited_runners` | integer |  |
| `season_stats_pitching_inherited_runners_scored` | integer |  |
| `season_stats_pitching_catchers_interference` | integer |  |
| `season_stats_pitching_sac_bunts` | integer |  |
| `season_stats_pitching_sac_flies` | integer |  |
| `season_stats_pitching_passed_ball` | integer |  |
| `season_stats_pitching_pop_outs` | integer |  |
| `season_stats_pitching_line_outs` | integer |  |
| `season_stats_fielding_caught_stealing` | integer |  |
| `season_stats_fielding_stolen_bases` | integer |  |
| `season_stats_fielding_stolen_base_percentage` | character |  |
| `season_stats_fielding_caught_stealing_percentage` | character |  |
| `season_stats_fielding_assists` | integer |  |
| `season_stats_fielding_put_outs` | integer |  |
| `season_stats_fielding_errors` | integer |  |
| `season_stats_fielding_chances` | integer |  |
| `season_stats_fielding_fielding` | character |  |
| `season_stats_fielding_passed_ball` | integer |  |
| `season_stats_fielding_pickoffs` | integer |  |
| `game_status_is_current_batter` | logical |  |
| `game_status_is_current_pitcher` | logical |  |
| `game_status_is_on_bench` | logical |  |
| `game_status_is_substitute` | logical |  |
| `stats_fielding_games_started` | double |  |
| `season_stats_fielding_games_started` | double |  |
| `season_stats_pitching_pitches_thrown` | double |  |
| `stats_pitching_summary` | character |  |
| `stats_pitching_games_played` | double |  |
| `stats_pitching_games_started` | double |  |
| `stats_pitching_fly_outs` | double |  |
| `stats_pitching_ground_outs` | double |  |
| `stats_pitching_air_outs` | double |  |
| `stats_pitching_runs` | double |  |
| `stats_pitching_doubles` | double |  |
| `stats_pitching_triples` | double |  |
| `stats_pitching_home_runs` | double |  |
| `stats_pitching_strike_outs` | double |  |
| `stats_pitching_base_on_balls` | double |  |
| `stats_pitching_intentional_walks` | double |  |
| `stats_pitching_hits` | double |  |
| `stats_pitching_hit_by_pitch` | double |  |
| `stats_pitching_at_bats` | double |  |
| `stats_pitching_caught_stealing` | double |  |
| `stats_pitching_stolen_bases` | double |  |
| `stats_pitching_stolen_base_percentage` | character |  |
| `stats_pitching_number_of_pitches` | double |  |
| `stats_pitching_innings_pitched` | character |  |
| `stats_pitching_wins` | double |  |
| `stats_pitching_losses` | double |  |
| `stats_pitching_saves` | double |  |
| `stats_pitching_save_opportunities` | double |  |
| `stats_pitching_holds` | double |  |
| `stats_pitching_blown_saves` | double |  |
| `stats_pitching_earned_runs` | double |  |
| `stats_pitching_batters_faced` | double |  |
| `stats_pitching_outs` | double |  |
| `stats_pitching_games_pitched` | double |  |
| `stats_pitching_complete_games` | double |  |
| `stats_pitching_shutouts` | double |  |
| `stats_pitching_pitches_thrown` | double |  |
| `stats_pitching_balls` | double |  |
| `stats_pitching_strikes` | double |  |
| `stats_pitching_strike_percentage` | character |  |
| `stats_pitching_hit_batsmen` | double |  |
| `stats_pitching_balks` | double |  |
| `stats_pitching_wild_pitches` | double |  |
| `stats_pitching_pickoffs` | double |  |
| `stats_pitching_rbi` | double |  |
| `stats_pitching_games_finished` | double |  |
| `stats_pitching_runs_scored_per9` | character |  |
| `stats_pitching_home_runs_per9` | character |  |
| `stats_pitching_inherited_runners` | double |  |
| `stats_pitching_inherited_runners_scored` | double |  |
| `stats_pitching_catchers_interference` | double |  |
| `stats_pitching_sac_bunts` | double |  |
| `stats_pitching_sac_flies` | double |  |
| `stats_pitching_passed_ball` | double |  |
| `stats_pitching_pop_outs` | double |  |
| `stats_pitching_line_outs` | double |  |
| `stats_pitching_note` | character |  |
| `stats_batting_note` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_boxscore(game_pk=716390)
```

_Last validated n/a._

## `mlb_linescore`

GET /api/v1/game/{gamePk}/linescore — inning-by-inning + current game state.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/linescore`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/linescore](https://statsapi.mlb.com/api/v1/game/716390/linescore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `num` | integer | Inning number. |
| `ordinal_num` | character | Inning ordinal label (e.g. 1st). |
| `home_runs` | integer | Home runs. |
| `home_hits` | integer | Home hits in the inning. |
| `home_errors` | integer | Home errors in the inning. |
| `home_left_on_base` | integer | Home runners left on base in the inning. |
| `away_runs` | integer | Away runs scored in the inning. |
| `away_hits` | integer | Away hits in the inning. |
| `away_errors` | integer | Away errors in the inning. |
| `away_left_on_base` | integer | Away runners left on base in the inning. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_linescore(game_pk=716390)
```

_Last validated n/a._

## `mlb_play_by_play`

GET /api/v1/game/{gamePk}/playByPlay — play-by-play with at-bat detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/playByPlay`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/playByPlay](https://statsapi.mlb.com/api/v1/game/716390/playByPlay)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pitch_index` | character |  |
| `action_index` | character |  |
| `runner_index` | character |  |
| `runners` | character |  |
| `play_events` | character |  |
| `play_end_time` | character |  |
| `at_bat_index` | integer | Zero-based index of the at-bat within the game. |
| `result_type` | character |  |
| `result_event` | character |  |
| `result_event_type` | character |  |
| `result_description` | character |  |
| `result_rbi` | integer |  |
| `result_away_score` | integer |  |
| `result_home_score` | integer |  |
| `result_is_out` | logical |  |
| `about_at_bat_index` | integer |  |
| `about_half_inning` | character |  |
| `about_is_top_inning` | logical |  |
| `about_inning` | integer |  |
| `about_start_time` | character |  |
| `about_end_time` | character |  |
| `about_is_complete` | logical |  |
| `about_is_scoring_play` | logical |  |
| `about_has_review` | logical |  |
| `about_has_out` | logical |  |
| `about_captivating_index` | integer |  |
| `count_balls` | integer |  |
| `count_strikes` | integer |  |
| `count_outs` | integer |  |
| `matchup_batter_id` | integer |  |
| `matchup_batter_full_name` | character |  |
| `matchup_batter_link` | character |  |
| `matchup_bat_side_code` | character |  |
| `matchup_bat_side_description` | character |  |
| `matchup_pitcher_id` | integer |  |
| `matchup_pitcher_full_name` | character |  |
| `matchup_pitcher_link` | character |  |
| `matchup_pitch_hand_code` | character |  |
| `matchup_pitch_hand_description` | character |  |
| `matchup_post_on_first_id` | double |  |
| `matchup_post_on_first_full_name` | character |  |
| `matchup_post_on_first_link` | character |  |
| `matchup_batter_hot_cold_zones` | character |  |
| `matchup_pitcher_hot_cold_zones` | character |  |
| `matchup_splits_batter` | character |  |
| `matchup_splits_pitcher` | character |  |
| `matchup_splits_men_on_base` | character |  |
| `matchup_post_on_second_id` | double |  |
| `matchup_post_on_second_full_name` | character |  |
| `matchup_post_on_second_link` | character |  |
| `matchup_post_on_third_id` | double |  |
| `matchup_post_on_third_full_name` | character |  |
| `matchup_post_on_third_link` | character |  |
| `review_details_is_overturned` | logical |  |
| `review_details_in_progress` | logical |  |
| `review_details_review_type` | character |  |
| `review_details_challenge_team_id` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_play_by_play(game_pk=716390)
```

_Last validated n/a._

## `mlb_game_context_metrics`

GET /api/v1/game/{gamePk}/contextMetrics — WP, leverage index, in-game context.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/contextMetrics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/contextMetrics](https://statsapi.mlb.com/api/v1/game/716390/contextMetrics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_context_metrics(game_pk=716390)
```

_Last validated n/a._

## `mlb_win_probability`

GET /api/v1/game/{gamePk}/winProbability — per-play WP timeline.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/winProbability`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/winProbability](https://statsapi.mlb.com/api/v1/game/716390/winProbability)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pitch_index` | character |  |
| `action_index` | character |  |
| `runner_index` | character |  |
| `runners` | character |  |
| `play_events` | character |  |
| `credits` | character |  |
| `flags` | character |  |
| `home_team_win_probability` | double | Home team win probability (percent) entering the at-bat. |
| `away_team_win_probability` | double | Away team win probability (percent) entering the at-bat. |
| `home_team_win_probability_added` | double | Change in home team win probability attributed to the at-bat. |
| `play_end_time` | character |  |
| `at_bat_index` | integer | Zero-based index of the at-bat within the game. |
| `result_type` | character |  |
| `result_event` | character |  |
| `result_event_type` | character |  |
| `result_description` | character |  |
| `result_rbi` | integer |  |
| `result_away_score` | integer |  |
| `result_home_score` | integer |  |
| `result_is_out` | logical |  |
| `about_at_bat_index` | integer |  |
| `about_half_inning` | character |  |
| `about_is_top_inning` | logical |  |
| `about_inning` | integer |  |
| `about_start_time` | character |  |
| `about_end_time` | character |  |
| `about_is_complete` | logical |  |
| `about_is_scoring_play` | logical |  |
| `about_has_review` | logical |  |
| `about_has_out` | logical |  |
| `about_captivating_index` | integer |  |
| `count_balls` | integer |  |
| `count_strikes` | integer |  |
| `count_outs` | integer |  |
| `matchup_batter_id` | integer |  |
| `matchup_batter_full_name` | character |  |
| `matchup_batter_link` | character |  |
| `matchup_bat_side_code` | character |  |
| `matchup_bat_side_description` | character |  |
| `matchup_pitcher_id` | integer |  |
| `matchup_pitcher_full_name` | character |  |
| `matchup_pitcher_link` | character |  |
| `matchup_pitch_hand_code` | character |  |
| `matchup_pitch_hand_description` | character |  |
| `matchup_post_on_first_id` | double |  |
| `matchup_post_on_first_full_name` | character |  |
| `matchup_post_on_first_link` | character |  |
| `matchup_batter_hot_cold_zones` | character |  |
| `matchup_pitcher_hot_cold_zones` | character |  |
| `matchup_splits_batter` | character |  |
| `matchup_splits_pitcher` | character |  |
| `matchup_splits_men_on_base` | character |  |
| `leverage_index` | double | Leverage index quantifying the importance of the at-bat situation. |
| `drama_index` | double |  |
| `matchup_post_on_second_id` | double |  |
| `matchup_post_on_second_full_name` | character |  |
| `matchup_post_on_second_link` | character |  |
| `matchup_post_on_third_id` | double |  |
| `matchup_post_on_third_full_name` | character |  |
| `matchup_post_on_third_link` | character |  |
| `review_details_is_overturned` | logical |  |
| `review_details_in_progress` | logical |  |
| `review_details_review_type` | character |  |
| `review_details_challenge_team_id` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_win_probability(game_pk=716390)
```

_Last validated n/a._

## `mlb_game_content`

GET /api/v1/game/{gamePk}/content — articles, highlights, editorial content.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/content`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/content](https://statsapi.mlb.com/api/v1/game/716390/content)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_content(game_pk=716390)
```

_Last validated n/a._

## `mlb_team`

GET /api/v1/teams/{teamId} — single team detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10](https://statsapi.mlb.com/api/v1/teams/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `all_star_status` | character | All-star status flag. |
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season` | integer | Season year. |
| `team_code` | character | Internal team code. |
| `file_code` | character | File code abbreviation. |
| `abbreviation` | character | Short abbreviation. |
| `team_name` | character | Team name. |
| `location_name` | character | Team location (city). |
| `first_year_of_play` | character | First year the franchise played. |
| `short_name` | character | Short display name. |
| `franchise_name` | character | Franchise name. |
| `club_name` | character | Club name. |
| `active` | logical | Whether the player is currently active. |
| `spring_league_id` | integer | Spring league MLBAM ID. |
| `spring_league_name` | character | Spring league name. |
| `spring_league_link` | character | API link to the spring league. |
| `spring_league_abbreviation` | character | Spring league abbreviation. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `spring_venue_id` | integer | Spring training venue MLBAM ID. |
| `spring_venue_link` | character | API link to the spring venue. |
| `league_id` | integer | League MLBAM ID. |
| `league_name` | character | League name. |
| `league_link` | character | API link to the league. |
| `division_id` | integer | Division MLBAM ID. |
| `division_name` | character | Division name. |
| `division_link` | character | API link to the division. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_team(team_id=10)
```

_Last validated n/a._

## `mlb_team_roster`

GET /api/v1/teams/{teamId}/roster — team roster.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/roster`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/roster](https://statsapi.mlb.com/api/v1/teams/10/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `rosterType` | `roster_type` |  |  | `Y` | rosterType query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |
| `position_code` | character | Numeric scorekeeping position code. |
| `position_name` | character | Full position name. |
| `position_type` | character | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | character | Position abbreviation. |
| `status_code` | character | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | character | Roster status description (e.g. 'Active'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_team_roster(team_id=10)
```

_Last validated n/a._

## `mlb_team_alumni`

GET /api/v1/teams/{teamId}/alumni — players who played for this team in a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/alumni`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/alumni](https://statsapi.mlb.com/api/v1/teams/10/alumni)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `alumni_last_season` | character | Last season the player was with the team. |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `birth_state_province` | character | State or province of birth. |
| `draft_year` | double | Year the player was drafted. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_team_alumni(team_id=10)
```

_Last validated n/a._

## `mlb_team_affiliates`

GET /api/v1/teams/affiliates — org affiliates (MLB parent → minor league chain).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/affiliates`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/affiliates](https://statsapi.mlb.com/api/v1/teams/affiliates)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `all_star_status` | character | All-star status flag. |
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season` | integer | Season year. |
| `team_code` | character | Internal team code. |
| `file_code` | character | File code abbreviation. |
| `abbreviation` | character | Short abbreviation. |
| `team_name` | character | Team name. |
| `location_name` | character | Team location (city). |
| `first_year_of_play` | character | First year the franchise played. |
| `short_name` | character | Short display name. |
| `franchise_name` | character | Franchise name. |
| `club_name` | character | Club name. |
| `active` | logical | Whether the player is currently active. |
| `spring_league_id` | double | Spring league MLBAM ID. |
| `spring_league_name` | character | Spring league name. |
| `spring_league_link` | character | API link to the spring league. |
| `spring_league_abbreviation` | character | Spring league abbreviation. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `spring_venue_id` | double | Spring training venue MLBAM ID. |
| `spring_venue_link` | character | API link to the spring venue. |
| `league_id` | double | League MLBAM ID. |
| `league_name` | character | League name. |
| `league_link` | character | API link to the league. |
| `division_id` | double | Division MLBAM ID. |
| `division_name` | character | Division name. |
| `division_link` | character | API link to the division. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `parent_org_name` | character | Parent organization name. |
| `parent_org_id` | double | Parent organization MLBAM ID. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_team_affiliates()
```

_Last validated n/a._

## `mlb_people`

GET /api/v1/people?personIds=... — bulk person lookup by MLBAM id.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people](https://statsapi.mlb.com/api/v1/people)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `personIds` | `person_ids` |  |  | `Y` | personIds query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_people()
```

_Last validated n/a._

## `mlb_person`

GET /api/v1/people/{personId} — single person detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/{person_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/660271](https://statsapi.mlb.com/api/v1/people/660271)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | person_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_person(person_id=660271)
```

_Last validated n/a._

## `mlb_person_game_stats`

GET /api/v1/people/{personId}/stats/game/{gamePk} — one player, one game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/{person_id}/stats/game/{game_pk}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/660271/stats/game/716390](https://statsapi.mlb.com/api/v1/people/660271/stats/game/716390)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | person_id path parameter. |
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_person_game_stats(person_id=660271, game_pk=716390)
```

_Last validated n/a._

## `mlb_sport_players`

GET /api/v1/sports/{sportId}/players — every player in a sport for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}/players`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports](https://statsapi.mlb.com/api/v1/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  |  | `Y` | sport_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_state_province` | character | State or province of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `draft_year` | double | Year the player was drafted. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `name_matrilineal` | character | Maternal family name. |
| `nick_name` | character | Player nickname. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_title` | character | Name title. |
| `name_suffix` | character | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_sport_players()
```

_Last validated n/a._

## `mlb_sports`

GET /api/v1/sports — list known sports (MLB, MiLB, KBO, NPB, …).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports](https://statsapi.mlb.com/api/v1/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `code` | character | Fielder detail type code. |
| `link` | character | API link to the game feed. |
| `name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | integer | Display sort order for the sport. |
| `active_status` | logical | Whether the sport/level is active. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_sports()
```

_Last validated n/a._

## `mlb_leagues`

GET /api/v1/leagues — list leagues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/leagues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagues](https://statsapi.mlb.com/api/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `abbreviation` | character | Short abbreviation. |
| `name_short` | character | Short name of player (First Initial, Last Name) |
| `season_state` | character |  |
| `has_wild_card` | logical |  |
| `has_split_season` | logical |  |
| `num_games` | double |  |
| `has_playoff_points` | logical |  |
| `num_teams` | double | Number of teams the player appeared for. |
| `num_wildcard_teams` | double |  |
| `season` | character | Season year. |
| `org_code` | character |  |
| `conferences_in_use` | logical | Whether conferences were in use that season. |
| `divisions_in_use` | logical | Whether divisions were in use that season. |
| `sort_order` | integer | Display sort order for the sport. |
| `active` | logical | Whether the player is currently active. |
| `season_date_info_season_id` | character | Season identifier for the date info block. |
| `season_date_info_pre_season_start_date` | character | Preseason start date (YYYY-MM-DD). |
| `season_date_info_pre_season_end_date` | character | Preseason end date (YYYY-MM-DD). |
| `season_date_info_season_start_date` | character | Season start date (YYYY-MM-DD). |
| `season_date_info_spring_start_date` | character | Spring training start date (YYYY-MM-DD). |
| `season_date_info_spring_end_date` | character | Spring training end date (YYYY-MM-DD). |
| `season_date_info_regular_season_start_date` | character | Regular season start date (YYYY-MM-DD). |
| `season_date_info_last_date1st_half` | character | Last date of the first half (YYYY-MM-DD). |
| `season_date_info_all_star_date` | character | All-Star Game date (YYYY-MM-DD). |
| `season_date_info_first_date2nd_half` | character | First date of the second half (YYYY-MM-DD). |
| `season_date_info_regular_season_end_date` | character | Regular season end date (YYYY-MM-DD). |
| `season_date_info_post_season_start_date` | character | Postseason start date (YYYY-MM-DD). |
| `season_date_info_post_season_end_date` | character | Postseason end date (YYYY-MM-DD). |
| `season_date_info_season_end_date` | character | Season end date (YYYY-MM-DD). |
| `season_date_info_offseason_start_date` | character | Offseason start date (YYYY-MM-DD). |
| `season_date_info_off_season_end_date` | character | Offseason end date (YYYY-MM-DD). |
| `season_date_info_season_level_gameday_type` | character | Season-level Gameday data type code. |
| `season_date_info_game_level_gameday_type` | character | Game-level Gameday data type code. |
| `season_date_info_qualifier_plate_appearances` | double | Plate appearances per game needed to qualify. |
| `season_date_info_qualifier_outs_pitched` | double | Outs pitched per game needed to qualify. |
| `sport_id` | double | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_leagues()
```

_Last validated n/a._

## `mlb_season`

GET /api/v1/seasons/{seasonId} — single season detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/{season_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/X](https://statsapi.mlb.com/api/v1/seasons/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Season year identifier. |
| `has_wildcard` | logical | Whether the season has a wild card round. |
| `pre_season_start_date` | character | Pre-season start date. |
| `pre_season_end_date` | character | Pre-season end date. |
| `season_start_date` | character | Season start date. |
| `spring_start_date` | character | Spring training start date. |
| `spring_end_date` | character | Spring training end date. |
| `regular_season_start_date` | character | Regular season start date. |
| `last_date1st_half` | character | Last date of the first half. |
| `all_star_date` | character | All-Star Game date. |
| `first_date2nd_half` | character | First date of the second half. |
| `regular_season_end_date` | character | Regular season end date. |
| `post_season_start_date` | character | Post-season start date. |
| `post_season_end_date` | character | Post-season end date. |
| `season_end_date` | character | Season end date. |
| `offseason_start_date` | character | Off-season start date. |
| `off_season_end_date` | character | Off-season end date. |
| `season_level_gameday_type` | character | Season-level Gameday data feed type. |
| `game_level_gameday_type` | character | Game-level Gameday data feed type. |
| `qualifier_plate_appearances` | double | Plate appearances per team game to qualify. |
| `qualifier_outs_pitched` | double | Outs pitched per team game to qualify. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_season(season_id='X')
```

_Last validated n/a._

## `mlb_venues`

GET /api/v1/venues — list venues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/venues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/venues](https://statsapi.mlb.com/api/v1/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `active` | logical | Whether the player is currently active. |
| `season` | character | Season year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_venues()
```

_Last validated n/a._

## `mlb_venue`

GET /api/v1/venues/{venueId} — single venue detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/venues/{venue_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/venues/15](https://statsapi.mlb.com/api/v1/venues/15)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `active` | logical | Whether the player is currently active. |
| `season` | character | Season year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_venue(venue_id=15)
```

_Last validated n/a._

## `mlb_meta`

GET /api/v1/{metaType} — enum lookup (the API's self-describing surface).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/{meta_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagueLeaderTypes](https://statsapi.mlb.com/api/v1/leagueLeaderTypes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `meta_type` | `meta_type` |  | `Y` |  | meta_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_meta(meta_type='leagueLeaderTypes')
```

_Last validated n/a._

## `mlb_awards`

GET /api/v1/awards — list award IDs (call with no params to enumerate).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards](https://statsapi.mlb.com/api/v1/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `name` | character | Display name. |
| `description` | character | Long-form description text. |
| `sort_order` | double | Display sort order for the sport. |
| `active` | logical | Whether the player is currently active. |
| `sport_id` | double | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |
| `league_id` | double | League MLBAM ID. |
| `league_link` | character | API link to the league. |
| `notes` | character | Notes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_awards()
```

_Last validated n/a._

## `mlb_award_recipients`

GET /api/v1/awards/{awardId}/recipients — historical winners of one award.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards/{award_id}/recipients`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients](https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `name` | character | Display name. |
| `date` | character | Date in YYYY-MM-DD format. |
| `season` | character | Season year. |
| `team_id` | integer | Unique ESPN team identifier. |
| `team_link` | character | API link to the team. |
| `player_id` | integer | MLBAM player ID. |
| `player_link` | character | API relative link to the player. |
| `player_primary_position_code` | character | Recipient primary fielding position code. |
| `player_primary_position_name` | character | Recipient primary fielding position name. |
| `player_primary_position_type` | character | Participant primary position type (e.g. 'Hitter'). |
| `player_primary_position_abbreviation` | character | Participant primary position abbreviation (e.g. 'DH'). |
| `player_name_first_last` | character | Participant name in first-last order. |
| `votes` | double | Number of votes received. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_award_recipients(award_id='MLBHOF')
```

_Last validated n/a._

## `mlb_draft`

GET /api/v1/draft/{year} — draft results for a year (optionally one round).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/draft/{year}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/draft/2024](https://statsapi.mlb.com/api/v1/draft/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `round` | `round_` |  |  | `Y` | round query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_draft(year=2024)
```

_Last validated n/a._

## `mlb_umpires`

GET /api/v1/jobs/umpires — current umpire crew assignments.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/umpires`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/umpires](https://statsapi.mlb.com/api/v1/jobs/umpires)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_umpires()
```

_Last validated n/a._

## `mlb_conferences`

View all PCL conferences.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences](https://statsapi.mlb.com/api/v1/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `conferenceId` | `conference_id` |  |  | `Y` | conferenceId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `abbreviation` | character | Short abbreviation. |
| `has_wildcard` | logical | Whether the season has a wild card round. |
| `name_short` | character | Short name of player (First Initial, Last Name) |
| `league_id` | integer | League MLBAM ID. |
| `league_link` | character | API link to the league. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_conferences()
```

_Last validated n/a._

## `mlb_conference`

View PCL conferences by conferenceId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences/{conference_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences/301](https://statsapi.mlb.com/api/v1/conferences/301)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `conference_id` | `conference_id` |  | `Y` |  | conference_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `abbreviation` | character | Short abbreviation. |
| `has_wildcard` | logical | Whether the season has a wild card round. |
| `name_short` | character | Short name of player (First Initial, Last Name) |
| `league_id` | integer | League MLBAM ID. |
| `league_link` | character | API link to the league. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_conference(conference_id=301)
```

_Last validated n/a._

## `mlb_draft_latest`

View latest player drafted, endpoint best used when draft is currently open.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/draft/{year}/latest`

**Valid URL:** [https://statsapi.mlb.com/api/v1/draft/2023/latest](https://statsapi.mlb.com/api/v1/draft/2023/latest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `number` | integer | Week number as returned by the API. |
| `next_up` | character |  |
| `pick_pick_round` | character |  |
| `pick_pick_number` | integer |  |
| `pick_display_pick_number` | integer |  |
| `pick_round_pick_number` | integer |  |
| `pick_signing_bonus` | character |  |
| `pick_home_city` | character |  |
| `pick_home_state` | character |  |
| `pick_home_country` | character |  |
| `pick_school_name` | character |  |
| `pick_school_school_class` | character |  |
| `pick_school_city` | character |  |
| `pick_school_country` | character |  |
| `pick_school_state` | character |  |
| `pick_headshot_link` | character |  |
| `pick_person_id` | integer |  |
| `pick_person_full_name` | character |  |
| `pick_person_link` | character |  |
| `pick_person_first_name` | character |  |
| `pick_person_last_name` | character |  |
| `pick_person_birth_date` | character |  |
| `pick_person_current_age` | integer |  |
| `pick_person_birth_city` | character |  |
| `pick_person_birth_state_province` | character |  |
| `pick_person_birth_country` | character |  |
| `pick_person_height` | character |  |
| `pick_person_weight` | integer |  |
| `pick_person_active` | logical |  |
| `pick_person_primary_position_code` | character |  |
| `pick_person_primary_position_name` | character |  |
| `pick_person_primary_position_type` | character |  |
| `pick_person_primary_position_abbreviation` | character |  |
| `pick_person_use_name` | character |  |
| `pick_person_use_last_name` | character |  |
| `pick_person_middle_name` | character |  |
| `pick_person_boxscore_name` | character |  |
| `pick_person_gender` | character |  |
| `pick_person_is_player` | logical |  |
| `pick_person_is_verified` | logical |  |
| `pick_person_draft_year` | integer |  |
| `pick_person_bat_side_code` | character |  |
| `pick_person_bat_side_description` | character |  |
| `pick_person_pitch_hand_code` | character |  |
| `pick_person_pitch_hand_description` | character |  |
| `pick_person_name_first_last` | character |  |
| `pick_person_name_slug` | character |  |
| `pick_person_first_last_name` | character |  |
| `pick_person_last_first_name` | character |  |
| `pick_person_last_init_name` | character |  |
| `pick_person_init_last_name` | character |  |
| `pick_person_full_fml_name` | character |  |
| `pick_person_full_lfm_name` | character |  |
| `pick_person_strike_zone_top` | double |  |
| `pick_person_strike_zone_bottom` | double |  |
| `pick_person_xref_ids` | character |  |
| `pick_team_spring_league_id` | integer |  |
| `pick_team_spring_league_name` | character |  |
| `pick_team_spring_league_link` | character |  |
| `pick_team_spring_league_abbreviation` | character |  |
| `pick_team_all_star_status` | character |  |
| `pick_team_id` | integer |  |
| `pick_team_name` | character |  |
| `pick_team_link` | character |  |
| `pick_team_season` | integer |  |
| `pick_team_venue_id` | integer |  |
| `pick_team_venue_name` | character |  |
| `pick_team_venue_link` | character |  |
| `pick_team_spring_venue_id` | integer |  |
| `pick_team_spring_venue_link` | character |  |
| `pick_team_team_code` | character |  |
| `pick_team_file_code` | character |  |
| `pick_team_abbreviation` | character |  |
| `pick_team_team_name` | character |  |
| `pick_team_location_name` | character |  |
| `pick_team_first_year_of_play` | character |  |
| `pick_team_league_id` | integer |  |
| `pick_team_league_name` | character |  |
| `pick_team_league_link` | character |  |
| `pick_team_division_id` | integer |  |
| `pick_team_division_name` | character |  |
| `pick_team_division_link` | character |  |
| `pick_team_sport_id` | integer |  |
| `pick_team_sport_link` | character |  |
| `pick_team_sport_name` | character |  |
| `pick_team_short_name` | character |  |
| `pick_team_franchise_name` | character |  |
| `pick_team_club_name` | character |  |
| `pick_team_active` | logical |  |
| `pick_draft_type_code` | character |  |
| `pick_draft_type_description` | character |  |
| `pick_is_drafted` | logical |  |
| `pick_is_pass` | logical |  |
| `pick_year` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_draft_latest(year=2023)
```

_Last validated n/a._

## `mlb_game_timestamps`

Retrieve all of the play timecodes for a game in GUMBO feed.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live/timestamps`

**Valid URL:** [https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps](https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `timecode` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_timestamps(game_pk=716390)
```

_Last validated n/a._

## `mlb_game_changes`

View corrected non Statcast information for games

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/changes`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/changes?updatedSince=2023-09-01T00%3A00%3A00Z&sportId=1](https://statsapi.mlb.com/api/v1/game/changes?updatedSince=2023-09-01T00%3A00%3A00Z&sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `updatedSince` | `updated_since` |  |  | `Y` | updatedSince query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `schedule_date` | character |  |
| `game_pk` | integer | Unique game identifier. |
| `game_guid` | character | Globally unique game identifier (GUID). |
| `link` | character | API link to the game feed. |
| `game_type` | character | Game type code (R, P, etc.). |
| `season` | character | Season year. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `official_date` | character | Official game date (YYYY-MM-DD). |
| `is_tie` | logical | Whether the game ended in a tie. |
| `game_number` | integer | Game number within a doubleheader. |
| `public_facing` | logical | Whether the game is public-facing. |
| `double_header` | character | Doubleheader indicator ('N', 'S', 'Y'). |
| `gameday_type` | character | Gameday data feed type. |
| `tiebreaker` | character | Whether the game is a tiebreaker. |
| `calendar_event_id` | character | Calendar event identifier. |
| `season_display` | character | Display string for the season. |
| `day_night` | character | Day or night game indicator. |
| `scheduled_innings` | integer | Scheduled number of innings. |
| `reverse_home_away_status` | logical | Whether home/away teams are reversed. |
| `inning_break_length` | integer | Length of inning breaks in seconds. |
| `games_in_series` | double | Number of games in the series. |
| `series_game_number` | double | Game number within the series. |
| `series_description` | character | Description of the series. |
| `record_source` | character | Source of the schedule record. |
| `if_necessary` | character | Whether the game is played only if necessary. |
| `if_necessary_description` | character | Description of the if-necessary status. |
| `status_abstract_game_state` | character | Abstract game state (e.g. 'Final'). |
| `status_coded_game_state` | character | Coded game state. |
| `status_detailed_state` | character | Detailed game state. |
| `status_status_code` | character | Status code for the game. |
| `status_start_time_tbd` | logical | Whether the start time is TBD. |
| `status_abstract_game_code` | character | Abstract game state code. |
| `teams_away_team_id` | integer | Away team MLBAM ID. |
| `teams_away_team_name` | character | Away team name. |
| `teams_away_team_link` | character | API link to the away team. |
| `teams_away_league_record_wins` | integer | Away team league-record wins. |
| `teams_away_league_record_losses` | integer | Away team league-record losses. |
| `teams_away_league_record_ties` | integer | Away team league-record ties. |
| `teams_away_league_record_pct` | character | Away team winning percentage. |
| `teams_away_score` | integer | Away team score. |
| `teams_away_is_winner` | logical | Whether the away team won. |
| `teams_away_split_squad` | logical | Whether the away team is a split squad. |
| `teams_away_series_number` | double | Away team's series number. |
| `teams_home_team_id` | integer | Home team MLBAM ID. |
| `teams_home_team_name` | character | Home team name. |
| `teams_home_team_link` | character | API link to the home team. |
| `teams_home_league_record_wins` | integer | Home team league-record wins. |
| `teams_home_league_record_losses` | integer | Home team league-record losses. |
| `teams_home_league_record_ties` | integer | Home team league-record ties. |
| `teams_home_league_record_pct` | character | Home team winning percentage. |
| `teams_home_score` | integer | Home team score. |
| `teams_home_is_winner` | logical | Whether the home team won. |
| `teams_home_split_squad` | logical | Whether the home team is a split squad. |
| `teams_home_series_number` | double | Home team's series number. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `content_link` | character | API link to the game content. |
| `rescheduled_from` | character | Original date-time the game was rescheduled from. |
| `rescheduled_from_date` | character | Original date the game was rescheduled from. |
| `description` | character | Long-form description text. |
| `status_reason` | character | Reason for the game status (e.g. 'Rain'). |
| `resumed_from` | character | Original date-time if the game was resumed. |
| `resumed_from_date` | character | Original date if the game was resumed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_changes(sport_id=1, updated_since='2023-09-01T00:00:00Z')
```

_Last validated n/a._

## `mlb_analytics_games`

View timestamps of most recent data corrections made to games.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/analytics/game`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/analytics/game](https://statsapi.mlb.com/api/v1/game/analytics/game)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `sortBy` | `sort_by` |  |  | `Y` | sortBy query parameter. |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` | isNonStatcast query parameter. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_analytics_games()
```

_Last validated n/a._

## `mlb_analytics_guids`

View timestamps of most recent data corrections made to GUIDs.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/analytics/guids`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/analytics/guids](https://statsapi.mlb.com/api/v1/game/analytics/guids)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `sortBy` | `sort_by` |  |  | `Y` | sortBy query parameter. |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` | isNonStatcast query parameter. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_analytics_guids()
```

_Last validated n/a._

## `mlb_game_guids`

View Statcast data for a specific game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/guids`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/guids](https://statsapi.mlb.com/api/v1/game/716390/guids)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `updatedSince` | `updated_since` |  |  | `Y` | updatedSince query parameter. |
| `isPitch` | `is_pitch` |  |  | `Y` | isPitch query parameter. |
| `isHit` | `is_hit` |  |  | `Y` | isHit query parameter. |
| `isPickoff` | `is_pickoff` |  |  | `Y` | isPickoff query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `parsed/raw` | `parsed_raw` |  |  | `Y` | parsed/raw query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_guids(game_pk=716390)
```

_Last validated n/a._

## `mlb_play_analytics`

View Statcast data for a specific play.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/{guid}/analytics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/analytics](https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/analytics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `guid` | `guid` |  | `Y` |  | guid path parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_play_analytics(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```

_Last validated n/a._

## `mlb_play_context_metrics_averages`

View Statcast contextMetrics data for a specific play.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/{guid}/contextMetricsAverages`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/contextMetricsAverages](https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/contextMetricsAverages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `guid` | `guid` |  | `Y` |  | guid path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_play_context_metrics_averages(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```

_Last validated n/a._

## `mlb_game_color`

View game color commentary info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color](https://statsapi.mlb.com/api/v1/game/716390/feed/color)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_color(game_pk=716390)
```

_Last validated n/a._

## `mlb_game_color_diff`

View game color feed.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color/diffPatch`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color/diffPatch](https://statsapi.mlb.com/api/v1/game/716390/feed/color/diffPatch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `startTimecode` | `start_timecode` |  |  | `Y` | startTimecode query parameter. |
| `endTimecode` | `end_timecode` |  |  | `Y` | endTimecode query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_color_diff(game_pk=716390)
```

_Last validated n/a._

## `mlb_game_color_timestamps`

View all of the color timecodes for a game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color/timestamps`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color/timestamps](https://statsapi.mlb.com/api/v1/game/716390/feed/color/timestamps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_timecodes`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_color_timestamps(game_pk=716390)
```

_Last validated n/a._

## `mlb_game_pace`

View time of game info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/gamePace`

**Valid URL:** [https://statsapi.mlb.com/api/v1/gamePace?season=2023](https://statsapi.mlb.com/api/v1/gamePace?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |
| `leagueListId` | `league_list_id` |  |  | `Y` | leagueListId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `venueIds` | `venue_ids` |  |  | `Y` | venueIds query parameter. |
| `orgType` | `org_type` |  |  | `Y` | orgType query parameter. |
| `includeChildren` | `include_children` |  |  | `Y` | includeChildren query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `hits_per9_inn` | double |  |
| `runs_per9_inn` | double |  |
| `pitches_per9_inn` | double |  |
| `plate_appearances_per9_inn` | double |  |
| `hits_per_game` | double | Hits per game. |
| `runs_per_game` | double | Runs per game. |
| `innings_played_per_game` | double | Innings played per game. |
| `pitches_per_game` | double | Pitches per game. |
| `pitchers_per_game` | double | Pitchers used per game. |
| `plate_appearances_per_game` | double | Plate appearances per game. |
| `total_game_time` | character | Total game time (HHH:MM:SS). |
| `total_innings_played` | double | Total innings played. |
| `total_hits` | integer | Total hits. |
| `total_runs` | integer | Total runs. |
| `total_plate_appearances` | integer | Total plate appearances. |
| `total_pitchers` | integer | Total pitchers used. |
| `total_pitches` | integer | Total pitches thrown. |
| `total_games` | integer | Total games on the date. |
| `total7_inn_games` | integer |  |
| `total9_inn_games` | double |  |
| `total_extra_inn_games` | integer | Total extra-inning games. |
| `time_per_game` | character | Average time per game (HH:MM:SS). |
| `time_per_pitch` | character | Average time per pitch (HH:MM:SS). |
| `time_per_hit` | character | Average time per hit (HH:MM:SS). |
| `time_per_run` | character | Average time per run (HH:MM:SS). |
| `time_per_plate_appearance` | character | Average time per plate appearance (HH:MM:SS). |
| `time_per9_inn` | character |  |
| `time_per77_plate_appearances` | character |  |
| `total_extra_inn_time` | character | Total extra-inning time (HHH:MM:SS). |
| `time_per7_inn_game_without_extra_inn` | character |  |
| `total9_inn_games_completed_early` | integer |  |
| `total9_inn_games_without_extra_inn` | double |  |
| `total9_inn_games_scheduled` | integer |  |
| `hits_per_run` | double | Hits per run. |
| `pitches_per_pitcher` | double | Pitches per pitcher. |
| `season` | character | Season year. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_code` | character | Short sport code (e.g. 'mlb', 'aaa'). |
| `sport_link` | character | API link to the sport. |
| `pr_portal_calculated_fields_total7_inn_games` | integer |  |
| `pr_portal_calculated_fields_total9_inn_games` | double |  |
| `pr_portal_calculated_fields_total_extra_inn_games` | integer | Portal-calculated total extra-inning games. |
| `pr_portal_calculated_fields_time_per7_inn_game` | character |  |
| `pr_portal_calculated_fields_time_per9_inn_game` | character |  |
| `pr_portal_calculated_fields_time_per_extra_inn_game` | character | Portal-calculated time per extra-inning game. |
| `time_per7_inn_game` | character |  |
| `total7_inn_games_scheduled` | double |  |
| `total7_inn_games_without_extra_inn` | double |  |
| `total7_inn_games_completed_early` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_game_pace(season='2023')
```

_Last validated n/a._

## `mlb_high_low`

View high/low stats by player or team.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/highLow/{org_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/highLow/player?statGroup=hitting&sortStat=homeRuns&season=2023](https://statsapi.mlb.com/api/v1/highLow/player?statGroup=hitting&sortStat=homeRuns&season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_type` | `org_type` |  | `Y` |  | org_type path parameter. |
| `statGroup` | `stat_group` |  |  | `Y` | statGroup query parameter. |
| `sortStat` | `sort_stat` |  |  | `Y` | sortStat query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `total_splits` | integer | Total number of splits in the leaderboard. |
| `exemptions` | character |  |
| `splits` | character | Splits. |
| `splits_tied_with_offset` | character | Players tied at the offset boundary. |
| `splits_tied_with_limit` | character | Players tied at the limit boundary. |
| `season` | character | Season year. |
| `combined_stats` | logical | Whether the stat combines multiple split sources. |
| `group_display_name` | character | Stat group display name. |
| `game_type_id` | character | Game type code (e.g., R for regular season). |
| `game_type_description` | character | Game type description. |
| `sort_stat_name` | character | Snake-case name of the sorted statistic (e.g. 'at_bats'). |
| `sort_stat_lookup_param` | character | API lookup parameter for the sorted statistic (e.g. 'atBats'). |
| `sort_stat_is_counting` | logical | Whether the sorted statistic is a counting stat. |
| `sort_stat_label` | character | Human-readable label of the sorted statistic (e.g. 'At bats'). |
| `sort_stat_stat_groups` | character |  |
| `sort_stat_org_types` | character |  |
| `sort_stat_high_low_types` | character |  |
| `sort_stat_streak_levels` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_high_low(org_type='player', stat_group='hitting', sort_stat='homeRuns', season='2023')
```

_Last validated n/a._

## `mlb_home_run_derby`

View a home run derby object based on gamePk.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101](https://statsapi.mlb.com/api/v1/homeRunDerby/511101)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_state_province` | character | State or province of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `draft_year` | double | Year the player was drafted. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `stats` | character | Stats. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `current_team_spring_league_id` | double |  |
| `current_team_spring_league_name` | character |  |
| `current_team_spring_league_link` | character |  |
| `current_team_spring_league_abbreviation` | character |  |
| `current_team_all_star_status` | character |  |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer |  |
| `current_team_venue_id` | integer |  |
| `current_team_venue_name` | character |  |
| `current_team_venue_link` | character |  |
| `current_team_spring_venue_id` | double |  |
| `current_team_spring_venue_link` | character |  |
| `current_team_team_code` | character |  |
| `current_team_file_code` | character |  |
| `current_team_abbreviation` | character |  |
| `current_team_team_name` | character |  |
| `current_team_location_name` | character |  |
| `current_team_first_year_of_play` | character |  |
| `current_team_league_id` | integer |  |
| `current_team_league_name` | character |  |
| `current_team_league_link` | character |  |
| `current_team_division_id` | double |  |
| `current_team_division_name` | character |  |
| `current_team_division_link` | character |  |
| `current_team_sport_id` | integer |  |
| `current_team_sport_link` | character |  |
| `current_team_sport_name` | character |  |
| `current_team_short_name` | character |  |
| `current_team_franchise_name` | character |  |
| `current_team_club_name` | character |  |
| `current_team_active` | logical |  |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |
| `current_team_parent_org_name` | character |  |
| `current_team_parent_org_id` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_home_run_derby(game_pk=511101)
```

_Last validated n/a._

## `mlb_home_run_derby_bracket`

View a home run derby object based on bracket.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/bracket`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101/bracket](https://statsapi.mlb.com/api/v1/homeRunDerby/511101/bracket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_state_province` | character | State or province of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `draft_year` | double | Year the player was drafted. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `stats` | character | Stats. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `current_team_spring_league_id` | double |  |
| `current_team_spring_league_name` | character |  |
| `current_team_spring_league_link` | character |  |
| `current_team_spring_league_abbreviation` | character |  |
| `current_team_all_star_status` | character |  |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer |  |
| `current_team_venue_id` | integer |  |
| `current_team_venue_name` | character |  |
| `current_team_venue_link` | character |  |
| `current_team_spring_venue_id` | double |  |
| `current_team_spring_venue_link` | character |  |
| `current_team_team_code` | character |  |
| `current_team_file_code` | character |  |
| `current_team_abbreviation` | character |  |
| `current_team_team_name` | character |  |
| `current_team_location_name` | character |  |
| `current_team_first_year_of_play` | character |  |
| `current_team_league_id` | integer |  |
| `current_team_league_name` | character |  |
| `current_team_league_link` | character |  |
| `current_team_division_id` | double |  |
| `current_team_division_name` | character |  |
| `current_team_division_link` | character |  |
| `current_team_sport_id` | integer |  |
| `current_team_sport_link` | character |  |
| `current_team_sport_name` | character |  |
| `current_team_short_name` | character |  |
| `current_team_franchise_name` | character |  |
| `current_team_club_name` | character |  |
| `current_team_active` | logical |  |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |
| `current_team_parent_org_name` | character |  |
| `current_team_parent_org_id` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_home_run_derby_bracket(game_pk=511101)
```

_Last validated n/a._

## `mlb_home_run_derby_pool`

View a home run derby object based on pool.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/pool`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101/pool](https://statsapi.mlb.com/api/v1/homeRunDerby/511101/pool)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_state_province` | character | State or province of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `draft_year` | double | Year the player was drafted. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `stats` | character | Stats. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `current_team_spring_league_id` | double |  |
| `current_team_spring_league_name` | character |  |
| `current_team_spring_league_link` | character |  |
| `current_team_spring_league_abbreviation` | character |  |
| `current_team_all_star_status` | character |  |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer |  |
| `current_team_venue_id` | integer |  |
| `current_team_venue_name` | character |  |
| `current_team_venue_link` | character |  |
| `current_team_spring_venue_id` | double |  |
| `current_team_spring_venue_link` | character |  |
| `current_team_team_code` | character |  |
| `current_team_file_code` | character |  |
| `current_team_abbreviation` | character |  |
| `current_team_team_name` | character |  |
| `current_team_location_name` | character |  |
| `current_team_first_year_of_play` | character |  |
| `current_team_league_id` | integer |  |
| `current_team_league_name` | character |  |
| `current_team_league_link` | character |  |
| `current_team_division_id` | double |  |
| `current_team_division_name` | character |  |
| `current_team_division_link` | character |  |
| `current_team_sport_id` | integer |  |
| `current_team_sport_link` | character |  |
| `current_team_sport_name` | character |  |
| `current_team_short_name` | character |  |
| `current_team_franchise_name` | character |  |
| `current_team_club_name` | character |  |
| `current_team_active` | logical |  |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |
| `current_team_parent_org_name` | character |  |
| `current_team_parent_org_id` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_home_run_derby_pool(game_pk=511101)
```

_Last validated n/a._

## `mlb_all_star_ballot`

View All-Star Ballots per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarBallot`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarBallot?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarBallot?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `name_matrilineal` | character | Maternal family name. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `last_played_date` | character | Date of last MLB game played. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `birth_state_province` | character | State or province of birth. |
| `draft_year` | double | Year the player was drafted. |
| `name_title` | character | Name title. |
| `name_suffix` | character | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_all_star_ballot(league_id='103', season='2023')
```

_Last validated n/a._

## `mlb_all_star_write_ins`

View All-Star Write-ins per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarWriteIns`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_state_province` | character | State or province of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `primary_number` | character | Player uniform number. |
| `draft_year` | double | Year the player was drafted. |
| `middle_name` | character | Player middle name. |
| `name_matrilineal` | character | Maternal family name. |
| `last_played_date` | character | Date of last MLB game played. |
| `nick_name` | character | Player nickname. |
| `name_title` | character | Name title. |
| `name_suffix` | character | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_all_star_write_ins(league_id='103', season='2023')
```

_Last validated n/a._

## `mlb_all_star_final_vote`

View All-Star Final Vote per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarFinalVote`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `name_matrilineal` | character | Maternal family name. |
| `birth_state_province` | character | State or province of birth. |
| `name_title` | character | Name title. |
| `name_suffix` | character | Name suffix (e.g. Jr., Sr., III). |
| `middle_name` | character | Player middle name. |
| `draft_year` | double | Year the player was drafted. |
| `last_played_date` | character | Date of last MLB game played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_all_star_final_vote(league_id='103', season='2023')
```

_Last validated n/a._

## `mlb_free_agents`

View biographical information and stats for Free Agents.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/freeAgents`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/freeAgents?season=2023](https://statsapi.mlb.com/api/v1/people/freeAgents?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `order` | `order` |  |  | `Y` | order query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `notes` | character | Notes. |
| `date_declared` | character | Date the player declared free agency (YYYY-MM-DD). |
| `player_id` | integer | MLBAM player ID. |
| `player_full_name` | character | Player full name. |
| `player_link` | character | API relative link to the player. |
| `original_team_id` | double | Team id the player left. |
| `original_team_name` | character | Name of the team the player left. |
| `original_team_link` | character | API relative link to the original team. |
| `new_team_link` | character | API relative link to the new team. |
| `position_code` | character | Numeric scorekeeping position code. |
| `position_name` | character | Position name. |
| `position_type` | character | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | character | Position abbreviation. |
| `date_signed` | character | Date the player signed a new contract (YYYY-MM-DD). |
| `new_team_id` | double | Team id the player signed with. |
| `new_team_name` | character | Name of the team the player signed with. |
| `sort_order` | double | Display sort order for the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_free_agents(season='2023')
```

_Last validated n/a._

## `mlb_jobs`

View directory by jobType.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs?jobType=UMPR](https://statsapi.mlb.com/api/v1/jobs?jobType=UMPR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `jobType` | `job_type` |  |  | `Y` | jobType query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_jobs(job_type='UMPR')
```

_Last validated n/a._

## `mlb_datacasters`

View datacasters directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/datacasters`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/datacasters](https://statsapi.mlb.com/api/v1/jobs/datacasters)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_datacasters()
```

_Last validated n/a._

## `mlb_official_scorers`

View official scorer directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/officialScorers`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/officialScorers](https://statsapi.mlb.com/api/v1/jobs/officialScorers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_official_scorers()
```

_Last validated n/a._

## `mlb_umpire_games`

Get umpires and associated game for umpireId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/umpires/games/{umpire_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/umpires/games/596809?season=2023](https://statsapi.mlb.com/api/v1/jobs/umpires/games/596809?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `umpire_id` | `umpire_id` |  | `Y` |  | umpire_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_umpire_games(umpire_id=596809, season='2023')
```

_Last validated n/a._

## `mlb_schedule_tied`

View tied game schedule info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/games/tied`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/games/tied?season=2016](https://statsapi.mlb.com/api/v1/schedule/games/tied?season=2016)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `schedule_date` | character |  |
| `game_pk` | integer | Unique game identifier. |
| `game_guid` | character | Globally unique game identifier (GUID). |
| `link` | character | API link to the game feed. |
| `game_type` | character | Game type code (R, P, etc.). |
| `season` | character | Season year. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `official_date` | character | Official game date (YYYY-MM-DD). |
| `is_tie` | logical | Whether the game ended in a tie. |
| `game_number` | integer | Game number within a doubleheader. |
| `public_facing` | logical | Whether the game is public-facing. |
| `double_header` | character | Doubleheader indicator ('N', 'S', 'Y'). |
| `gameday_type` | character | Gameday data feed type. |
| `tiebreaker` | character | Whether the game is a tiebreaker. |
| `calendar_event_id` | character | Calendar event identifier. |
| `season_display` | character | Display string for the season. |
| `day_night` | character | Day or night game indicator. |
| `scheduled_innings` | integer | Scheduled number of innings. |
| `reverse_home_away_status` | logical | Whether home/away teams are reversed. |
| `inning_break_length` | integer | Length of inning breaks in seconds. |
| `games_in_series` | integer | Number of games in the series. |
| `series_game_number` | integer | Game number within the series. |
| `series_description` | character | Description of the series. |
| `record_source` | character | Source of the schedule record. |
| `if_necessary` | character | Whether the game is played only if necessary. |
| `if_necessary_description` | character | Description of the if-necessary status. |
| `status_abstract_game_state` | character | Abstract game state (e.g. 'Final'). |
| `status_coded_game_state` | character | Coded game state. |
| `status_detailed_state` | character | Detailed game state. |
| `status_status_code` | character | Status code for the game. |
| `status_start_time_tbd` | logical | Whether the start time is TBD. |
| `status_reason` | character | Reason for the game status (e.g. 'Rain'). |
| `status_abstract_game_code` | character | Abstract game state code. |
| `teams_away_team_id` | integer | Away team MLBAM ID. |
| `teams_away_team_name` | character | Away team name. |
| `teams_away_team_link` | character | API link to the away team. |
| `teams_away_league_record_wins` | integer | Away team league-record wins. |
| `teams_away_league_record_losses` | integer | Away team league-record losses. |
| `teams_away_league_record_ties` | integer | Away team league-record ties. |
| `teams_away_league_record_pct` | character | Away team winning percentage. |
| `teams_away_score` | integer | Away team score. |
| `teams_away_split_squad` | logical | Whether the away team is a split squad. |
| `teams_away_series_number` | integer | Away team's series number. |
| `teams_home_team_id` | integer | Home team MLBAM ID. |
| `teams_home_team_name` | character | Home team name. |
| `teams_home_team_link` | character | API link to the home team. |
| `teams_home_league_record_wins` | integer | Home team league-record wins. |
| `teams_home_league_record_losses` | integer | Home team league-record losses. |
| `teams_home_league_record_ties` | integer | Home team league-record ties. |
| `teams_home_league_record_pct` | character | Home team winning percentage. |
| `teams_home_score` | integer | Home team score. |
| `teams_home_split_squad` | logical | Whether the home team is a split squad. |
| `teams_home_series_number` | integer | Home team's series number. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `content_link` | character | API link to the game content. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_schedule_tied(season='2016')
```

_Last validated n/a._

## `mlb_schedule_postseason_series`

View schedule info for postseason based on series.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/series`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/series?season=2023](https://statsapi.mlb.com/api/v1/schedule/postseason/series?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `seriesNumber` | `series_number` |  |  | `Y` | seriesNumber query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `total_items` | integer | Total schedule items on the date. |
| `total_games` | integer | Total games on the date. |
| `total_games_in_progress` | integer | Games currently in progress on the date. |
| `games` | character | Number of games included in the ATS summary. |
| `sort_order` | integer | Display sort order for the sport. |
| `series_id` | character | Series identifier (e.g. 'W_1'). |
| `series_sort_number` | integer | Sort number for the series. |
| `series_is_default` | logical | Whether the series is the default series. |
| `series_game_type` | character | Game type code for the series. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_schedule_postseason_series(season='2023')
```

_Last validated n/a._

## `mlb_schedule_postseason_tunein`

View schedule info for the tuneIn application.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn?season=2023](https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_schedule_postseason_tunein(season='2023')
```

_Last validated n/a._

## `mlb_seasons_all`

View information for all seasons based on id.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/all`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/all?sportId=1](https://statsapi.mlb.com/api/v1/seasons/all?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `divisionId` | `division_id` |  |  | `Y` | divisionId query parameter. |
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `withGameTypeDates` | `with_game_type_dates` |  |  | `Y` | withGameTypeDates query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Season year identifier. |
| `has_wildcard` | logical | Whether the season has a wild card round. |
| `pre_season_start_date` | character | Pre-season start date. |
| `season_start_date` | character | Season start date. |
| `regular_season_start_date` | character | Regular season start date. |
| `regular_season_end_date` | character | Regular season end date. |
| `season_end_date` | character | Season end date. |
| `offseason_start_date` | character | Off-season start date. |
| `off_season_end_date` | character | Off-season end date. |
| `season_level_gameday_type` | character | Season-level Gameday data feed type. |
| `game_level_gameday_type` | character | Game-level Gameday data feed type. |
| `qualifier_plate_appearances` | double | Plate appearances per team game to qualify. |
| `qualifier_outs_pitched` | double | Outs pitched per team game to qualify. |
| `post_season_start_date` | character | Post-season start date. |
| `post_season_end_date` | character | Post-season end date. |
| `last_date1st_half` | character | Last date of the first half. |
| `all_star_date` | character | All-Star Game date. |
| `first_date2nd_half` | character | First date of the second half. |
| `pre_season_end_date` | character | Pre-season end date. |
| `spring_start_date` | character | Spring training start date. |
| `spring_end_date` | character | Spring training end date. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_seasons_all(sport_id=1)
```

_Last validated n/a._

## `mlb_sport`

View information for any given sportId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports/1](https://statsapi.mlb.com/api/v1/sports/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  | `Y` |  | sport_id path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `code` | character | Fielder detail type code. |
| `link` | character | API link to the game feed. |
| `name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | integer | Display sort order for the sport. |
| `active_status` | logical | Whether the sport/level is active. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_sport(sport_id=1)
```

_Last validated n/a._

## `mlb_stats_metrics`

View Statcast stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/stats/metrics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/stats/metrics](https://statsapi.mlb.com/api/v1/stats/metrics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stats` | `stats` |  |  | `Y` | stats query parameter. |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `venueId` | `venue_id` |  |  | `Y` | venueId query parameter. |
| `minOccurrences` | `min_occurrences` |  |  | `Y` | minOccurrences query parameter. |
| `percentile` | `percentile` |  |  | `Y` | percentile query parameter. |
| `personId` | `person_id` |  |  | `Y` | personId query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_stats_metrics()
```

_Last validated n/a._

## `mlb_teams_history`

View historical records for a list of teams.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/history`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/history?teamIds=147](https://statsapi.mlb.com/api/v1/teams/history?teamIds=147)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `startSeason` | `start_season` |  |  | `Y` | startSeason query parameter. |
| `endSeason` | `end_season` |  |  | `Y` | endSeason query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `all_star_status` | character | All-star status flag. |
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season` | integer | Season year. |
| `team_code` | character | Internal team code. |
| `file_code` | character | File code abbreviation. |
| `abbreviation` | character | Short abbreviation. |
| `team_name` | character | Team name. |
| `location_name` | character | Team location (city). |
| `first_year_of_play` | character | First year the franchise played. |
| `short_name` | character | Short display name. |
| `franchise_name` | character | Franchise name. |
| `club_name` | character | Club name. |
| `active` | logical | Whether the player is currently active. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `spring_venue_id` | double | Spring training venue MLBAM ID. |
| `spring_venue_link` | character | API link to the spring venue. |
| `league_id` | integer | League MLBAM ID. |
| `league_name` | character | League name. |
| `league_link` | character | API link to the league. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_teams_history(team_ids='147')
```

_Last validated n/a._

## `mlb_teams_stats`

View team stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats?season=2023&sportIds=1&group=hitting&stats=season](https://statsapi.mlb.com/api/v1/teams/stats?season=2023&sportIds=1&group=hitting&stats=season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `group` | `stat_group` |  |  | `Y` | group query parameter. |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `stats` | `stats` |  |  | `Y` | stats query parameter. |
| `order` | `order` |  |  | `Y` | order query parameter. |
| `sortStat` | `sort_stat` |  |  | `Y` | sortStat query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `total_splits` | integer | Total number of splits in the leaderboard. |
| `exemptions` | character |  |
| `splits` | character | Splits. |
| `splits_tied_with_offset` | character | Players tied at the offset boundary. |
| `splits_tied_with_limit` | character | Players tied at the limit boundary. |
| `type_display_name` | character | Stat type display name. |
| `group_display_name` | character | Stat group display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_teams_stats(season='2023', sport_ids='1', stat_group='hitting', stats='season')
```

_Last validated n/a._

## `mlb_teams_stats_leaders`

View leaders for a statistic.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats/leaders`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats/leaders?leaderCategories=homeRuns&season=2023](https://statsapi.mlb.com/api/v1/teams/stats/leaders?leaderCategories=homeRuns&season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leaderCategories` | `leader_categories` |  |  | `Y` | leaderCategories query parameter. |
| `sitCodes` | `sit_codes` |  |  | `Y` | sitCodes query parameter. |
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `statGroup` | `stat_group` |  |  | `Y` | statGroup query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `leader_category` | character | Team leader category (e.g., homeRuns). |
| `season` | character | Season year. |
| `leaders` | character |  |
| `stat_group` | character | Stat group (e.g., hitting). |
| `total_splits` | integer | Total number of splits in the leaderboard. |
| `game_type_id` | character | Game type code (e.g., R for regular season). |
| `game_type_description` | character | Game type description. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_teams_stats_leaders(leader_categories='homeRuns', season='2023')
```

_Last validated n/a._

## `mlb_team_coaches`

View biographical  information on all coaches for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/coaches`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/coaches?season=2023](https://statsapi.mlb.com/api/v1/teams/147/coaches?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_team_coaches(team_id=147, season='2023')
```

_Last validated n/a._

## `mlb_team_personnel`

View biographical  information on all personnel for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/personnel`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/personnel](https://statsapi.mlb.com/api/v1/teams/147/personnel)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_team_personnel(team_id=147)
```

_Last validated n/a._

## `mlb_team_roster_type`

View biographical and statistical information for a club's roster based on roster type.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/{roster_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/roster/active?season=2023](https://statsapi.mlb.com/api/v1/teams/147/roster/active?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `roster_type` | `roster_type` |  | `Y` |  | roster_type path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |
| `position_code` | character | Numeric scorekeeping position code. |
| `position_name` | character | Position name. |
| `position_type` | character | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | character | Position abbreviation. |
| `status_code` | character | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | character | Roster status description (e.g. 'Active'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_team_roster_type(team_id=147, roster_type='active', season='2023')
```

_Last validated n/a._
