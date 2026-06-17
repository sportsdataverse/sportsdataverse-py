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
| `schedule_date` | character | The calendar date grouping postseason games in this response row, as returned by the MLB Stats API schedule endpoint. |
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
| `parent_team_id` | integer | MLB Stats API identifier for the player's parent (MLB-level) organization, useful for tracking players on optional assignment. |
| `batting_order` | character | Spot in the batting order (1-9). |
| `all_positions` | character | All fielding positions played by the player during the game, as a list of position codes. |
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
| `stats_batting_summary` | character | Condensed text summary of the batter's performance line (e.g., '2-4, HR, 2 RBI') for display purposes. |
| `stats_batting_games_played` | double | Number of games played indicator for this batter's boxscore row (typically 1 for a standard game appearance). |
| `stats_batting_fly_outs` | double | Number of outs recorded by the batter on fly balls in this game. |
| `stats_batting_ground_outs` | double | Number of outs recorded by the batter on ground balls in this game. |
| `stats_batting_air_outs` | double | Number of outs recorded by the batter on balls hit in the air during this game. |
| `stats_batting_runs` | double | Number of runs scored by the batter in this game. |
| `stats_batting_doubles` | double | Number of doubles hit by the batter in this game. |
| `stats_batting_triples` | double | Number of triples hit by the batter in this game. |
| `stats_batting_home_runs` | double | Number of home runs hit by the batter in this game. |
| `stats_batting_strike_outs` | double | Number of times the batter struck out in this game. |
| `stats_batting_base_on_balls` | double | Number of walks (bases on balls) drawn by the batter in this game, including intentional walks. |
| `stats_batting_intentional_walks` | double | Number of intentional walks issued to the batter in this game. |
| `stats_batting_hits` | double | Total number of hits recorded by the batter in this game. |
| `stats_batting_hit_by_pitch` | double | Number of times the batter was hit by a pitch in this game. |
| `stats_batting_at_bats` | double | Number of official at-bats for the batter in this game. |
| `stats_batting_caught_stealing` | double | Number of times the batter was caught stealing in this game. |
| `stats_batting_stolen_bases` | double | Number of stolen bases recorded by the batter in this game. |
| `stats_batting_stolen_base_percentage` | character | Percentage of stolen base attempts that were successful for the batter in this game. |
| `stats_batting_ground_into_double_play` | double | Number of times the batter grounded into a double play in this game. |
| `stats_batting_ground_into_triple_play` | double | Number of times the batter grounded into a triple play in this game. |
| `stats_batting_plate_appearances` | double | Total number of plate appearances for the batter in this game. |
| `stats_batting_total_bases` | double | Total number of bases accumulated by the batter on hits in this game. |
| `stats_batting_rbi` | double | Number of runs batted in (RBI) credited to the batter in this game. |
| `stats_batting_left_on_base` | double | Number of runners left on base when the batter made an out or the inning ended in this game. |
| `stats_batting_sac_bunts` | double | Number of sacrifice bunts executed by the batter in this game. |
| `stats_batting_sac_flies` | double | Number of sacrifice flies hit by the batter that scored a run in this game. |
| `stats_batting_catchers_interference` | double | Number of times the batter reached base due to catcher's interference in this game. |
| `stats_batting_pickoffs` | double | Number of times the batter was picked off base in this game. |
| `stats_batting_at_bats_per_home_run` | character | At-bats per home run ratio for the batter in this game. |
| `stats_batting_pop_outs` | double | Number of outs recorded by the batter on infield pop-ups in this game. |
| `stats_batting_line_outs` | double | Number of outs recorded by the batter on line drives caught in this game. |
| `stats_fielding_caught_stealing` | double | Number of baserunners caught stealing by the player (typically a catcher) in this game. |
| `stats_fielding_stolen_bases` | double | Number of stolen bases allowed by the player while fielding in this game. |
| `stats_fielding_stolen_base_percentage` | character | Percentage of stolen base attempts against the player (catcher perspective) that were successful in this game. |
| `stats_fielding_caught_stealing_percentage` | character | Percentage of stolen base attempts that the player threw out in this game. |
| `stats_fielding_assists` | double | Number of fielding assists recorded by the player in this game. |
| `stats_fielding_put_outs` | double | Number of putouts recorded by the player in this game. |
| `stats_fielding_errors` | double | Number of fielding errors committed by the player in this game. |
| `stats_fielding_chances` | double | Total fielding chances for the player in this game (putouts + assists + errors). |
| `stats_fielding_fielding` | character | Fielding percentage for the player in this game, calculated as (putouts + assists) / total chances. |
| `stats_fielding_passed_ball` | double | Number of passed balls charged to the player (catcher-specific) in this game. |
| `stats_fielding_pickoffs` | double | Number of pickoffs credited to the player as a fielder in this game. |
| `season_stats_batting_games_played` | integer | Season-to-date number of games in which the batter appeared. |
| `season_stats_batting_fly_outs` | integer | Season-to-date number of outs recorded by the batter on fly balls caught in the outfield. |
| `season_stats_batting_ground_outs` | integer | Season-to-date number of outs recorded by the batter on ground balls. |
| `season_stats_batting_air_outs` | integer | Season-to-date number of outs recorded by the batter on balls hit in the air (fly balls and line drives caught). |
| `season_stats_batting_runs` | integer | Season-to-date number of runs scored by the batter. |
| `season_stats_batting_doubles` | integer | Season-to-date number of doubles hit by the batter. |
| `season_stats_batting_triples` | integer | Season-to-date number of triples hit by the batter. |
| `season_stats_batting_home_runs` | integer | Season-to-date number of home runs hit by the batter. |
| `season_stats_batting_strike_outs` | integer | Season-to-date number of times the batter struck out. |
| `season_stats_batting_base_on_balls` | integer | Season-to-date total walks (bases on balls) drawn by the batter, including intentional walks. |
| `season_stats_batting_intentional_walks` | integer | Season-to-date number of intentional walks (IBB) issued to the batter. |
| `season_stats_batting_hits` | integer | Season-to-date total number of hits recorded by the batter. |
| `season_stats_batting_hit_by_pitch` | integer | Season-to-date number of times the batter was hit by a pitch. |
| `season_stats_batting_avg` | character | Season-to-date batting average (hits divided by at-bats) for the batter. |
| `season_stats_batting_at_bats` | integer | Season-to-date number of official at-bats accumulated by the batter. |
| `season_stats_batting_obp` | character | Season-to-date on-base percentage (OBP), measuring how often the batter reaches base per plate appearance. |
| `season_stats_batting_slg` | character | Season-to-date slugging percentage (SLG), measuring total bases per at-bat. |
| `season_stats_batting_ops` | character | Season-to-date on-base plus slugging percentage (OPS), a combined measure of a batter's ability to get on base and hit for power. |
| `season_stats_batting_caught_stealing` | integer | Season-to-date number of times the batter was caught stealing a base. |
| `season_stats_batting_stolen_bases` | integer | Season-to-date number of bases stolen by the batter. |
| `season_stats_batting_stolen_base_percentage` | character | Season-to-date percentage of stolen base attempts that were successful for the batter. |
| `season_stats_batting_caught_stealing_percentage` | character | Season-to-date percentage of stolen base attempts that resulted in the batter being caught stealing. |
| `season_stats_batting_ground_into_double_play` | integer | Season-to-date number of times the batter grounded into a double play. |
| `season_stats_batting_ground_into_triple_play` | integer | Season-to-date number of times the batter grounded into a triple play. |
| `season_stats_batting_plate_appearances` | integer | Season-to-date total number of plate appearances for the batter, including at-bats, walks, HBP, and sacrifices. |
| `season_stats_batting_total_bases` | integer | Season-to-date total number of bases accumulated by the batter on hits. |
| `season_stats_batting_rbi` | integer | Season-to-date number of runs batted in (RBI) credited to the batter. |
| `season_stats_batting_left_on_base` | integer | Season-to-date number of runners left on base when the batter made an out or the inning ended. |
| `season_stats_batting_sac_bunts` | integer | Season-to-date number of sacrifice bunts executed by the batter. |
| `season_stats_batting_sac_flies` | integer | Season-to-date number of sacrifice flies hit by the batter that scored a run. |
| `season_stats_batting_babip` | character | Season-to-date Batting Average on Balls In Play (BABIP), measuring batting average excluding strikeouts and home runs. |
| `season_stats_batting_ground_outs_to_airouts` | character | Season-to-date ratio of ground outs to air outs, indicating the batter's tendency to hit the ball on the ground versus in the air. |
| `season_stats_batting_catchers_interference` | integer | Season-to-date number of times the batter reached base due to catcher's interference. |
| `season_stats_batting_pickoffs` | integer | Season-to-date number of times the batter was picked off base by a pitcher or catcher. |
| `season_stats_batting_at_bats_per_home_run` | character | Season-to-date ratio of at-bats per home run, reflecting the batter's home run frequency. |
| `season_stats_batting_pop_outs` | integer | Season-to-date number of outs recorded by the batter on pop-ups caught in the infield. |
| `season_stats_batting_line_outs` | integer | Season-to-date number of outs recorded by the batter on line drives caught. |
| `season_stats_pitching_games_played` | integer | Season-to-date number of games the pitcher was active on the roster (may include non-pitching appearances). |
| `season_stats_pitching_games_started` | integer | Season-to-date number of games in which the pitcher was the starting pitcher. |
| `season_stats_pitching_fly_outs` | integer | Season-to-date number of outs recorded by the pitcher on fly balls. |
| `season_stats_pitching_ground_outs` | integer | Season-to-date number of outs recorded by the pitcher on ground balls. |
| `season_stats_pitching_air_outs` | integer | Season-to-date number of outs recorded by the pitcher on balls hit in the air. |
| `season_stats_pitching_runs` | integer | Season-to-date total runs (earned and unearned) allowed by the pitcher. |
| `season_stats_pitching_doubles` | integer | Season-to-date number of doubles allowed by the pitcher. |
| `season_stats_pitching_triples` | integer | Season-to-date number of triples allowed by the pitcher. |
| `season_stats_pitching_home_runs` | integer | Season-to-date number of home runs allowed by the pitcher. |
| `season_stats_pitching_strike_outs` | integer | Season-to-date number of batters struck out by the pitcher. |
| `season_stats_pitching_base_on_balls` | integer | Season-to-date total walks (bases on balls) issued by the pitcher, including intentional walks. |
| `season_stats_pitching_intentional_walks` | integer | Season-to-date number of intentional walks (IBB) issued by the pitcher. |
| `season_stats_pitching_hits` | integer | Season-to-date number of hits allowed by the pitcher. |
| `season_stats_pitching_hit_by_pitch` | integer | Season-to-date number of hit-by-pitch events while the pitcher was pitching (alternate field name for hit_batsmen). |
| `season_stats_pitching_at_bats` | integer | Season-to-date number of at-bats faced by the pitcher (excluding walks, HBP, and sacrifices). |
| `season_stats_pitching_obp` | character | Season-to-date on-base percentage allowed by the pitcher (opponents' OBP against this pitcher). |
| `season_stats_pitching_caught_stealing` | integer | Season-to-date number of baserunners caught stealing while the pitcher was on the mound. |
| `season_stats_pitching_stolen_bases` | integer | Season-to-date number of stolen bases allowed while the pitcher was pitching. |
| `season_stats_pitching_stolen_base_percentage` | character | Season-to-date percentage of stolen base attempts that were successful while the pitcher was on the mound. |
| `season_stats_pitching_caught_stealing_percentage` | character | Season-to-date percentage of stolen base attempts that were thrown out while the pitcher was pitching. |
| `season_stats_pitching_number_of_pitches` | integer | Season-to-date total number of pitches thrown by the pitcher. |
| `season_stats_pitching_era` | character | Season-to-date Earned Run Average (ERA) for the pitcher, expressed as earned runs per nine innings. |
| `season_stats_pitching_innings_pitched` | character | Season-to-date total innings pitched, expressed as a decimal where each out is one-third of an inning. |
| `season_stats_pitching_wins` | integer | Season-to-date number of wins credited to the pitcher. |
| `season_stats_pitching_losses` | integer | Season-to-date number of losses charged to the pitcher. |
| `season_stats_pitching_saves` | integer | Season-to-date number of saves recorded by the pitcher. |
| `season_stats_pitching_save_opportunities` | integer | Season-to-date number of save opportunities the pitcher entered (leads of three runs or fewer in the seventh inning or later, or entering with the tying run on base). |
| `season_stats_pitching_holds` | integer | Season-to-date number of holds recorded by the pitcher (relief appearance maintaining a lead without a save situation). |
| `season_stats_pitching_blown_saves` | integer | Season-to-date number of blown save opportunities for the pitcher. |
| `season_stats_pitching_earned_runs` | integer | Season-to-date number of earned runs allowed by the pitcher. |
| `season_stats_pitching_whip` | character | Season-to-date Walks plus Hits per Inning Pitched (WHIP), measuring baserunners allowed per inning. |
| `season_stats_pitching_batters_faced` | integer | Season-to-date total number of batters faced by the pitcher. |
| `season_stats_pitching_outs` | integer | Season-to-date total number of outs recorded by the pitcher. |
| `season_stats_pitching_games_pitched` | integer | Season-to-date number of games in which the pitcher appeared. |
| `season_stats_pitching_complete_games` | integer | Season-to-date number of complete games pitched by the pitcher. |
| `season_stats_pitching_shutouts` | integer | Season-to-date number of complete-game shutouts pitched. |
| `season_stats_pitching_balls` | integer | Season-to-date number of ball calls recorded against the pitcher. |
| `season_stats_pitching_strikes` | integer | Season-to-date total number of strikes thrown by the pitcher. |
| `season_stats_pitching_strike_percentage` | character | Season-to-date percentage of all pitches thrown that were strikes. |
| `season_stats_pitching_hit_batsmen` | integer | Season-to-date number of batters hit by a pitch thrown by the pitcher. |
| `season_stats_pitching_balks` | integer | Season-to-date number of balks called against the pitcher. |
| `season_stats_pitching_wild_pitches` | integer | Season-to-date number of wild pitches thrown by the pitcher. |
| `season_stats_pitching_pickoffs` | integer | Season-to-date number of pickoffs executed by the pitcher. |
| `season_stats_pitching_ground_outs_to_airouts` | character | Season-to-date ratio of ground ball outs to air ball outs allowed by the pitcher. |
| `season_stats_pitching_rbi` | integer | Season-to-date number of RBI allowed (runs batted in by opposing batters) while this pitcher was pitching. |
| `season_stats_pitching_win_percentage` | character | Season-to-date winning percentage for the pitcher (wins divided by decisions). |
| `season_stats_pitching_pitches_per_inning` | character | Season-to-date average number of pitches thrown per inning by the pitcher. |
| `season_stats_pitching_games_finished` | integer | Season-to-date number of games in which the pitcher was the last pitcher used by their team. |
| `season_stats_pitching_strikeout_walk_ratio` | character | Season-to-date ratio of strikeouts to walks, measuring the pitcher's command and dominance. |
| `season_stats_pitching_strikeouts_per9_inn` | character | Season-to-date strikeouts recorded per nine innings pitched (K/9), a rate measure of strikeout ability. |
| `season_stats_pitching_walks_per9_inn` | character | Season-to-date walks issued per nine innings pitched (BB/9), a rate measure of control. |
| `season_stats_pitching_hits_per9_inn` | character | Season-to-date hits allowed per nine innings pitched, a rate stat measuring hit prevention. |
| `season_stats_pitching_runs_scored_per9` | character | Season-to-date total runs (including unearned) allowed per nine innings pitched. |
| `season_stats_pitching_home_runs_per9` | character | Season-to-date home runs allowed per nine innings pitched. |
| `season_stats_pitching_inherited_runners` | integer | Season-to-date number of baserunners already on base when the pitcher entered the game. |
| `season_stats_pitching_inherited_runners_scored` | integer | Season-to-date number of inherited runners who eventually scored while or after the pitcher was pitching. |
| `season_stats_pitching_catchers_interference` | integer | Season-to-date number of times the pitcher benefited from a catcher's interference call. |
| `season_stats_pitching_sac_bunts` | integer | Season-to-date number of sacrifice bunts allowed by the pitcher. |
| `season_stats_pitching_sac_flies` | integer | Season-to-date number of sacrifice flies allowed by the pitcher. |
| `season_stats_pitching_passed_ball` | integer | Season-to-date number of passed balls that occurred while the pitcher was pitching. |
| `season_stats_pitching_pop_outs` | integer | Season-to-date number of outs recorded by the pitcher on pop-ups caught in the infield. |
| `season_stats_pitching_line_outs` | integer | Season-to-date number of outs recorded by the pitcher on line drives caught. |
| `season_stats_fielding_caught_stealing` | integer | Season-to-date number of baserunners caught stealing by the player (typically a catcher stat). |
| `season_stats_fielding_stolen_bases` | integer | Season-to-date number of stolen bases allowed by the player while fielding (typically catcher). |
| `season_stats_fielding_stolen_base_percentage` | character | Season-to-date percentage of stolen base attempts against the player that were successful (catcher perspective). |
| `season_stats_fielding_caught_stealing_percentage` | character | Season-to-date percentage of stolen base attempts that the player (usually a catcher) threw out. |
| `season_stats_fielding_assists` | integer | Season-to-date number of fielding assists recorded by the player (touching the ball before a putout by a teammate). |
| `season_stats_fielding_put_outs` | integer | Season-to-date number of putouts recorded by the player (directly retiring a baserunner or batter). |
| `season_stats_fielding_errors` | integer | Season-to-date number of fielding errors committed by the player. |
| `season_stats_fielding_chances` | integer | Season-to-date total fielding chances for the player (putouts + assists + errors). |
| `season_stats_fielding_fielding` | character | Season-to-date fielding percentage for the player, calculated as (putouts + assists) / total chances. |
| `season_stats_fielding_passed_ball` | integer | Season-to-date number of passed balls charged to the player (catcher-specific). |
| `season_stats_fielding_pickoffs` | integer | Season-to-date number of pickoffs credited to the player as a fielder. |
| `game_status_is_current_batter` | logical | Indicates whether the player is currently at bat at the moment the boxscore was captured. |
| `game_status_is_current_pitcher` | logical | Indicates whether the player is currently pitching at the moment the boxscore was captured. |
| `game_status_is_on_bench` | logical | Indicates whether the player is currently on the bench (not in the active lineup) at time of capture. |
| `game_status_is_substitute` | logical | Indicates whether the player entered the game as a substitute for another player. |
| `stats_fielding_games_started` | double | Indicator of whether the player started at a fielding position in this game. |
| `season_stats_fielding_games_started` | double | Season-to-date number of games in which the player started at a fielding position. |
| `season_stats_pitching_pitches_thrown` | double | Season-to-date total pitches thrown by the pitcher (may differ from number_of_pitches if strikes/balls are tracked separately). |
| `stats_pitching_summary` | character | Condensed text summary of the pitcher's performance line (e.g., '6.0 IP, 2 ER, 8 K') for display purposes. |
| `stats_pitching_games_played` | double | Number of games the pitcher appeared in for this boxscore row (typically 1). |
| `stats_pitching_games_started` | double | Indicator of whether the pitcher was the starting pitcher in this game. |
| `stats_pitching_fly_outs` | double | Number of outs recorded by the pitcher on fly balls in this game. |
| `stats_pitching_ground_outs` | double | Number of outs recorded by the pitcher on ground balls in this game. |
| `stats_pitching_air_outs` | double | Number of outs recorded by the pitcher on balls hit in the air in this game. |
| `stats_pitching_runs` | double | Total runs (earned and unearned) allowed by the pitcher in this game. |
| `stats_pitching_doubles` | double | Number of doubles allowed by the pitcher in this game. |
| `stats_pitching_triples` | double | Number of triples allowed by the pitcher in this game. |
| `stats_pitching_home_runs` | double | Number of home runs allowed by the pitcher in this game. |
| `stats_pitching_strike_outs` | double | Number of batters struck out by the pitcher in this game. |
| `stats_pitching_base_on_balls` | double | Number of walks (bases on balls) issued by the pitcher in this game, including intentional walks. |
| `stats_pitching_intentional_walks` | double | Number of intentional walks (IBB) issued by the pitcher in this game. |
| `stats_pitching_hits` | double | Number of hits allowed by the pitcher in this game. |
| `stats_pitching_hit_by_pitch` | double | Number of hit-by-pitch events while the pitcher was pitching in this game (alternate field for hit_batsmen). |
| `stats_pitching_at_bats` | double | Number of at-bats faced by the pitcher (excluding walks, HBP, and sacrifices) in this game. |
| `stats_pitching_caught_stealing` | double | Number of baserunners caught stealing while the pitcher was on the mound in this game. |
| `stats_pitching_stolen_bases` | double | Number of stolen bases allowed while the pitcher was pitching in this game. |
| `stats_pitching_stolen_base_percentage` | character | Percentage of stolen base attempts that were successful while the pitcher was on the mound in this game. |
| `stats_pitching_number_of_pitches` | double | Total number of pitches thrown by the pitcher in this game. |
| `stats_pitching_innings_pitched` | character | Total innings pitched by the pitcher in this game, expressed as a decimal (each out counts as one-third of an inning). |
| `stats_pitching_wins` | double | Indicator of whether the pitcher was credited with the win in this game. |
| `stats_pitching_losses` | double | Indicator of whether the pitcher was charged with the loss in this game. |
| `stats_pitching_saves` | double | Indicator of whether the pitcher recorded a save in this game. |
| `stats_pitching_save_opportunities` | double | Number of save opportunities the pitcher entered in this game. |
| `stats_pitching_holds` | double | Number of holds recorded by the pitcher in this game. |
| `stats_pitching_blown_saves` | double | Number of blown save opportunities for the pitcher in this game. |
| `stats_pitching_earned_runs` | double | Number of earned runs allowed by the pitcher in this game. |
| `stats_pitching_batters_faced` | double | Total number of batters faced by the pitcher in this game. |
| `stats_pitching_outs` | double | Total number of outs recorded by the pitcher in this game. |
| `stats_pitching_games_pitched` | double | Number of pitching appearances for the pitcher in this game (typically 1). |
| `stats_pitching_complete_games` | double | Indicator of whether the pitcher threw a complete game in this appearance. |
| `stats_pitching_shutouts` | double | Indicator of whether the pitcher recorded a complete-game shutout in this game. |
| `stats_pitching_pitches_thrown` | double | Total pitches thrown by the pitcher in this game (may differ from number_of_pitches depending on tracking method). |
| `stats_pitching_balls` | double | Number of ball calls recorded against the pitcher in this game. |
| `stats_pitching_strikes` | double | Total number of strikes thrown by the pitcher in this game. |
| `stats_pitching_strike_percentage` | character | Percentage of all pitches thrown that were strikes in this game. |
| `stats_pitching_hit_batsmen` | double | Number of batters hit by a pitch thrown by the pitcher in this game. |
| `stats_pitching_balks` | double | Number of balks called against the pitcher in this game. |
| `stats_pitching_wild_pitches` | double | Number of wild pitches thrown by the pitcher in this game. |
| `stats_pitching_pickoffs` | double | Number of pickoffs executed by the pitcher in this game. |
| `stats_pitching_rbi` | double | Number of RBI allowed (runs batted in by opposing batters off this pitcher) in this game. |
| `stats_pitching_games_finished` | double | Indicator of whether the pitcher was the last pitcher used by their team in this game. |
| `stats_pitching_runs_scored_per9` | character | Total runs (including unearned) allowed per nine innings rate for the pitcher in this game. |
| `stats_pitching_home_runs_per9` | character | Home runs allowed per nine innings rate for the pitcher in this game. |
| `stats_pitching_inherited_runners` | double | Number of baserunners already on base when the pitcher entered the game. |
| `stats_pitching_inherited_runners_scored` | double | Number of inherited runners who scored while or after the pitcher was pitching in this game. |
| `stats_pitching_catchers_interference` | double | Number of catcher's interference calls that occurred while the pitcher was pitching in this game. |
| `stats_pitching_sac_bunts` | double | Number of sacrifice bunts allowed by the pitcher in this game. |
| `stats_pitching_sac_flies` | double | Number of sacrifice flies allowed by the pitcher in this game. |
| `stats_pitching_passed_ball` | double | Number of passed balls that occurred while the pitcher was pitching in this game. |
| `stats_pitching_pop_outs` | double | Number of outs recorded by the pitcher on infield pop-ups in this game. |
| `stats_pitching_line_outs` | double | Number of outs recorded by the pitcher on line drives caught in this game. |
| `stats_pitching_note` | character | Supplementary note or annotation attached to the pitcher's boxscore line (e.g., indicating a special circumstance). |
| `stats_batting_note` | character | Supplementary note or annotation attached to the batter's boxscore line (e.g., indicating a special circumstance). |

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
| `pitch_index` | character | A serialized list of indices identifying individual pitch events that occurred within this at-bat. |
| `action_index` | character | A serialized list of indices identifying action-type events (e.g., stolen bases, pickoffs) that occurred within the at-bat. |
| `runner_index` | character | A serialized list of indices identifying baserunner movement events that occurred during or after this play. |
| `runners` | character | A serialized representation of baserunner movement records for this play, including starting base, ending base, and relevant event types. |
| `play_events` | character | A serialized representation of the sequence of individual pitch and action events comprising this at-bat. |
| `play_end_time` | character | The ISO 8601 timestamp marking the conclusion of the entire play (as distinct from a single pitch event) within the game feed. |
| `at_bat_index` | integer | Zero-based index of the at-bat within the game. |
| `result_type` | character | The high-level category of the play result as classified by the MLB Stats API (e.g., 'atBat', 'action'). |
| `result_event` | character | The short categorical label for the play outcome as classified by the MLB Stats API (e.g., 'Strikeout', 'Home Run', 'Walk'). |
| `result_event_type` | character | The snake-cased type identifier for the play outcome used internally by the MLB Stats API (e.g., 'strikeout', 'home_run'). |
| `result_description` | character | A human-readable text description of the play result as reported by the MLB Stats API (e.g., 'Strikeout', 'Single to left field'). |
| `result_rbi` | integer | The number of runs batted in credited to the batter as a result of this play. |
| `result_away_score` | integer | The away team's cumulative run total at the conclusion of this play. |
| `result_home_score` | integer | The home team's cumulative run total at the conclusion of this play. |
| `result_is_out` | logical | Boolean flag indicating whether the play resulted in the batter being retired (i.e., an out was charged to the batter). |
| `about_at_bat_index` | integer | The sequential index of the at-bat within the game to which this play or pitch event belongs. |
| `about_half_inning` | character | Indicates whether the play occurred in the top or bottom half of the inning (e.g., 'top' or 'bottom'). |
| `about_is_top_inning` | logical | Boolean flag indicating whether this play occurred in the top half of the inning (true) or bottom half (false). |
| `about_inning` | integer | The inning number in which this play or pitch event occurred. |
| `about_start_time` | character | The ISO 8601 timestamp marking the start of the play event, used for temporal sequencing within the game feed. |
| `about_end_time` | character | The ISO 8601 timestamp marking the end of the play event, used for temporal sequencing within the game feed. |
| `about_is_complete` | logical | Boolean flag indicating whether the at-bat or play event has concluded (i.e., reached a terminal result). |
| `about_is_scoring_play` | logical | Boolean flag indicating whether this play resulted in one or more runs being scored. |
| `about_has_review` | logical | Boolean flag indicating whether this play was subject to a manager's challenge or umpire review. |
| `about_has_out` | logical | Boolean flag indicating whether this play resulted in at least one out being recorded. |
| `about_captivating_index` | integer | A numeric score assigned by the MLB Stats API reflecting how compelling or exciting a given play was, based on leverage and game context. |
| `count_balls` | integer | The ball count in the current at-bat at the time of this pitch or play event. |
| `count_strikes` | integer | The strike count in the current at-bat at the time of this pitch or play event. |
| `count_outs` | integer | The number of outs recorded in the current half-inning at the time of this pitch or play event. |
| `matchup_batter_id` | integer | The MLB Stats API (MLBAM) numeric identifier for the batter in this play's matchup. |
| `matchup_batter_full_name` | character | The full name of the batter involved in this plate appearance. |
| `matchup_batter_link` | character | The MLB Stats API relative URL linking to the batter's player resource for this matchup. |
| `matchup_bat_side_code` | character | A single-character code indicating the batter's handedness for this matchup (e.g., 'L' for left, 'R' for right, 'S' for switch). |
| `matchup_bat_side_description` | character | The human-readable description of the batter's hitting side for this matchup (e.g., 'Left', 'Right', 'Switch'). |
| `matchup_pitcher_id` | integer | The MLB Stats API (MLBAM) numeric identifier for the pitcher in this play's matchup. |
| `matchup_pitcher_full_name` | character | The full name of the pitcher involved in this plate appearance. |
| `matchup_pitcher_link` | character | The MLB Stats API relative URL linking to the pitcher's player resource for this matchup. |
| `matchup_pitch_hand_code` | character | A single-character code indicating the pitcher's throwing hand for this matchup (e.g., 'L' for left, 'R' for right). |
| `matchup_pitch_hand_description` | character | The human-readable description of the pitcher's throwing arm for this matchup (e.g., 'Left', 'Right'). |
| `matchup_post_on_first_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on first base after the play concluded. |
| `matchup_post_on_first_full_name` | character | The full name of the baserunner on first base after the play concluded, if applicable. |
| `matchup_post_on_first_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on first base after the play. |
| `matchup_batter_hot_cold_zones` | character | A serialized representation of the batter's hot and cold zone effectiveness data for this matchup context. |
| `matchup_pitcher_hot_cold_zones` | character | A serialized representation of the pitcher's hot and cold zone effectiveness data for this matchup context. |
| `matchup_splits_batter` | character | A string describing the batter's situational split relevant to this matchup (e.g., 'vs. Right' or 'vs. Left'). |
| `matchup_splits_pitcher` | character | A string describing the pitcher's situational split relevant to this matchup (e.g., 'vs. Left' or 'vs. Right'). |
| `matchup_splits_men_on_base` | character | A string describing the baserunner configuration applicable to the batter's situational split for this plate appearance. |
| `matchup_post_on_second_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on second base after the play concluded. |
| `matchup_post_on_second_full_name` | character | The full name of the baserunner on second base after the play concluded, if applicable. |
| `matchup_post_on_second_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on second base after the play. |
| `matchup_post_on_third_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on third base after the play concluded. |
| `matchup_post_on_third_full_name` | character | The full name of the baserunner on third base after the play concluded, if applicable. |
| `matchup_post_on_third_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on third base after the play. |
| `review_details_is_overturned` | logical | Boolean flag indicating whether the original on-field call was reversed as a result of the replay review. |
| `review_details_in_progress` | logical | Boolean flag indicating whether the umpire review of this play was still ongoing at the time of data capture. |
| `review_details_review_type` | character | The type of review mechanism applied to this play (e.g., 'managerChallenge', 'umpireReview'). |
| `review_details_challenge_team_id` | double | The MLB Stats API numeric identifier for the team that initiated the manager's challenge review on this play. |

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
| `pitch_index` | character | A serialized list of indices identifying individual pitch events within the at-bat for this win-probability row. |
| `action_index` | character | A serialized list of indices identifying action-type events (stolen bases, pickoffs, etc.) within the at-bat for this win-probability row. |
| `runner_index` | character | A serialized list of indices identifying baserunner movement events during the play in this win-probability row. |
| `runners` | character | A serialized representation of baserunner movement records for this play in the win-probability feed, capturing start/end base positions and event types. |
| `play_events` | character | A serialized representation of the sequence of pitch and action events comprising the at-bat for this win-probability row. |
| `credits` | character | A serialized list of player credit records for this play, linking fielding, pitching, or batting achievements to specific player identifiers. |
| `flags` | character | A serialized representation of situational boolean flags associated with this play (e.g., whether it was a big lead situation or a save opportunity). |
| `home_team_win_probability` | double | Home team win probability (percent) entering the at-bat. |
| `away_team_win_probability` | double | Away team win probability (percent) entering the at-bat. |
| `home_team_win_probability_added` | double | Change in home team win probability attributed to the at-bat. |
| `play_end_time` | character | The ISO 8601 timestamp marking the conclusion of the play associated with this win-probability snapshot. |
| `at_bat_index` | integer | Zero-based index of the at-bat within the game. |
| `result_type` | character | The high-level category of the play result in the win-probability feed (e.g., 'atBat', 'action'). |
| `result_event` | character | The short categorical label for the play outcome in the win-probability feed (e.g., 'Single', 'Home Run', 'Strikeout'). |
| `result_event_type` | character | The snake-cased type identifier for the play outcome in the win-probability feed (e.g., 'single', 'home_run'). |
| `result_description` | character | A human-readable text description of the play result as reported in the win-probability game feed. |
| `result_rbi` | integer | The number of runs batted in credited to the batter for the play in this win-probability row. |
| `result_away_score` | integer | The away team's cumulative run total at the conclusion of the play in this win-probability row. |
| `result_home_score` | integer | The home team's cumulative run total at the conclusion of the play in this win-probability row. |
| `result_is_out` | logical | Boolean flag indicating whether the batter was retired on the play recorded in this win-probability row. |
| `about_at_bat_index` | integer | The sequential index of the at-bat within the game to which this win-probability observation belongs. |
| `about_half_inning` | character | Indicates whether the win-probability observation occurred in the top or bottom half of the inning. |
| `about_is_top_inning` | logical | Boolean flag indicating whether this win-probability observation occurred in the top half of the inning. |
| `about_inning` | integer | The inning number in which this win-probability observation was recorded. |
| `about_start_time` | character | The ISO 8601 timestamp marking the start of the play event within the win-probability game feed. |
| `about_end_time` | character | The ISO 8601 timestamp marking the end of the play event within the win-probability game feed. |
| `about_is_complete` | logical | Boolean flag indicating whether the at-bat associated with this win-probability row has concluded. |
| `about_is_scoring_play` | logical | Boolean flag indicating whether this play resulted in one or more runs being scored. |
| `about_has_review` | logical | Boolean flag indicating whether this play was subject to a manager's challenge or umpire review. |
| `about_has_out` | logical | Boolean flag indicating whether this play resulted in at least one out being recorded. |
| `about_captivating_index` | integer | A numeric score reflecting how compelling or exciting this plate appearance was, based on leverage and game-state context. |
| `count_balls` | integer | The ball count in the current at-bat at the time this win-probability snapshot was recorded. |
| `count_strikes` | integer | The strike count in the current at-bat at the time this win-probability snapshot was recorded. |
| `count_outs` | integer | The number of outs in the current half-inning at the time this win-probability snapshot was recorded. |
| `matchup_batter_id` | integer | The MLB Stats API (MLBAM) numeric identifier for the batter in this win-probability matchup row. |
| `matchup_batter_full_name` | character | The full name of the batter whose plate appearance generated this win-probability observation. |
| `matchup_batter_link` | character | The MLB Stats API relative URL linking to the batter's player resource for this win-probability row. |
| `matchup_bat_side_code` | character | A single-character code indicating the batter's handedness for this matchup in the win-probability feed (e.g., 'L', 'R', 'S'). |
| `matchup_bat_side_description` | character | The human-readable description of the batter's hitting side for this win-probability matchup row. |
| `matchup_pitcher_id` | integer | The MLB Stats API (MLBAM) numeric identifier for the pitcher in this win-probability matchup row. |
| `matchup_pitcher_full_name` | character | The full name of the pitcher who delivered pitches for this win-probability observation. |
| `matchup_pitcher_link` | character | The MLB Stats API relative URL linking to the pitcher's player resource for this win-probability row. |
| `matchup_pitch_hand_code` | character | A single-character code indicating the pitcher's throwing hand for this win-probability matchup (e.g., 'L' or 'R'). |
| `matchup_pitch_hand_description` | character | The human-readable description of the pitcher's throwing arm for this win-probability matchup row. |
| `matchup_post_on_first_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on first base after the play in this win-probability row. |
| `matchup_post_on_first_full_name` | character | The full name of the runner occupying first base at the conclusion of the play in this win-probability row. |
| `matchup_post_on_first_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on first base after the play. |
| `matchup_batter_hot_cold_zones` | character | A serialized representation of the batter's hot and cold zone data applicable to this win-probability matchup. |
| `matchup_pitcher_hot_cold_zones` | character | A serialized representation of the pitcher's hot and cold zone data applicable to this win-probability matchup. |
| `matchup_splits_batter` | character | A string describing the batter's situational split for this win-probability matchup (e.g., 'vs. Right'). |
| `matchup_splits_pitcher` | character | A string describing the pitcher's situational split for this win-probability matchup (e.g., 'vs. Left'). |
| `matchup_splits_men_on_base` | character | A string describing the baserunner configuration applicable to the batter's situational split in this win-probability row. |
| `leverage_index` | double | Leverage index quantifying the importance of the at-bat situation. |
| `drama_index` | double | A numeric score quantifying the dramatic significance of this play within the game, based on win-probability swing and game leverage. |
| `matchup_post_on_second_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on second base after the play in this win-probability row. |
| `matchup_post_on_second_full_name` | character | The full name of the runner occupying second base at the conclusion of the play in this win-probability row. |
| `matchup_post_on_second_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on second base after the play. |
| `matchup_post_on_third_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on third base after the play in this win-probability row. |
| `matchup_post_on_third_full_name` | character | The full name of the runner occupying third base at the conclusion of the play in this win-probability row. |
| `matchup_post_on_third_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on third base after the play. |
| `review_details_is_overturned` | logical | Boolean flag indicating whether the original on-field ruling was reversed following the replay review for this play. |
| `review_details_in_progress` | logical | Boolean flag indicating whether a replay review of this play was still underway at the time of data capture. |
| `review_details_review_type` | character | The type of review mechanism applied to this play in the win-probability feed (e.g., 'managerChallenge', 'umpireReview'). |
| `review_details_challenge_team_id` | double | The MLB Stats API numeric identifier for the team that initiated a replay challenge on this win-probability play. |

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
| `position_name` | character | Full position name (e.g. 'Point Guard', 'Goalkeeper'). |
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
| `season_state` | character | A string describing the current phase of the league's season (e.g., 'inProgress', 'offseason', 'preseason'). |
| `has_wild_card` | logical | Boolean flag indicating whether this league includes a wild card playoff format for postseason eligibility. |
| `has_split_season` | logical | Boolean flag indicating whether this league divides its season into two halves with separate standings (as used historically in some minor leagues). |
| `num_games` | double | The total number of regular-season games scheduled per team in this league for the given season. |
| `has_playoff_points` | logical | Boolean flag indicating whether this league uses a playoff points system to determine postseason seeding. |
| `num_teams` | double | Number of teams the player appeared for. |
| `num_wildcard_teams` | double | The number of wild card berths available for postseason entry in this league for the given season. |
| `season` | character | Season year. |
| `org_code` | character | The organizational code identifying the parent body (e.g., 'MLB') governing this league within the MLB Stats API hierarchy. |
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
| `next_up` | character | Indicates whether this draft slot is the next pick to be made in the current draft. |
| `pick_pick_round` | character | Draft round in which this pick was made (e.g., '1', '2', 'CB-A' for competitive balance). |
| `pick_pick_number` | integer | Overall pick number of this selection counting sequentially across all rounds of the draft. |
| `pick_display_pick_number` | integer | The formatted overall pick number displayed publicly for this draft selection. |
| `pick_round_pick_number` | integer | Pick number within the specific draft round (i.e., the Nth pick in that round). |
| `pick_signing_bonus` | character | Reported or slotted signing bonus amount associated with this draft pick. |
| `pick_home_city` | character | City of the draftee's listed home address at the time of the draft. |
| `pick_home_state` | character | State or province of the draftee's listed home address at the time of the draft. |
| `pick_home_country` | character | Country of the draftee's listed home address at the time of the draft. |
| `pick_school_name` | character | Name of the high school or college the draftee attended before being drafted. |
| `pick_school_school_class` | character | Academic class or level of the draftee at their school (e.g., High School, Junior, Senior). |
| `pick_school_city` | character | City of the high school or college the draftee attended before being drafted. |
| `pick_school_country` | character | Country of the school the draftee attended. |
| `pick_school_state` | character | State of the school the draftee attended. |
| `pick_headshot_link` | character | URL to the headshot image of the drafted player on the MLB Stats API CDN. |
| `pick_person_id` | integer | Unique MLB Stats API (MLBAM) identifier for the drafted player. |
| `pick_person_full_name` | character | Player's complete display name as used throughout the MLB Stats API. |
| `pick_person_link` | character | Relative URL path to the player's resource in the MLB Stats API. |
| `pick_person_first_name` | character | The player's legal or preferred first name. |
| `pick_person_last_name` | character | The player's legal or preferred last name. |
| `pick_person_birth_date` | character | Date of birth of the drafted player in ISO 8601 format. |
| `pick_person_current_age` | integer | Age of the drafted player in years at the time of the data retrieval. |
| `pick_person_birth_city` | character | City where the drafted player was born. |
| `pick_person_birth_state_province` | character | State or province where the drafted player was born. |
| `pick_person_birth_country` | character | Country where the drafted player was born. |
| `pick_person_height` | character | Player's height in feet-and-inches notation (e.g., 6' 2"). |
| `pick_person_weight` | integer | Player's weight in pounds as recorded by the MLB Stats API. |
| `pick_person_active` | logical | Boolean flag indicating whether the drafted player is currently on an active MLB roster. |
| `pick_person_primary_position_code` | character | Numeric or short code identifying the player's primary fielding position. |
| `pick_person_primary_position_name` | character | Full name of the player's primary fielding position (e.g., Shortstop, Center Field). |
| `pick_person_primary_position_type` | character | Broad classification of the player's position role (e.g., Pitcher, Infielder, Outfielder). |
| `pick_person_primary_position_abbreviation` | character | Short abbreviation for the player's primary fielding position (e.g., SS, CF, SP). |
| `pick_person_use_name` | character | The first name or nickname the player prefers to use publicly. |
| `pick_person_use_last_name` | character | The last name the player prefers to use publicly, which may differ from the legal last name. |
| `pick_person_middle_name` | character | The player's middle name as recorded by the MLB Stats API. |
| `pick_person_boxscore_name` | character | Abbreviated name format used for the player on official MLB box scores. |
| `pick_person_gender` | character | Recorded gender of the drafted player. |
| `pick_person_is_player` | logical | Boolean flag indicating whether this person is classified as an active player in the MLB Stats API. |
| `pick_person_is_verified` | logical | Boolean flag indicating whether the player's profile has been verified by MLB. |
| `pick_person_draft_year` | integer | The MLB draft year in which this player was originally selected. |
| `pick_person_bat_side_code` | character | Single-letter code for the player's batting handedness (e.g., R, L, S for switch). |
| `pick_person_bat_side_description` | character | Full description of the player's batting side (e.g., Right, Left, Switch). |
| `pick_person_pitch_hand_code` | character | Single-letter code for the player's pitching handedness (e.g., R, L, S). |
| `pick_person_pitch_hand_description` | character | Full description of the player's pitching hand (e.g., Right, Left, Switch). |
| `pick_person_name_first_last` | character | Player's display name in first-last format, typically matching the broadcast name. |
| `pick_person_name_slug` | character | URL-safe slug derived from the player's name for use in web links. |
| `pick_person_first_last_name` | character | Player's name formatted as first name followed by last name. |
| `pick_person_last_first_name` | character | Player's name formatted as last name followed by first name. |
| `pick_person_last_init_name` | character | Player's name formatted as last name followed by first initial. |
| `pick_person_init_last_name` | character | Player's name formatted as first initial followed by last name (e.g., J. Smith). |
| `pick_person_full_fml_name` | character | Player's full name in first-middle-last order as recorded by the MLB Stats API. |
| `pick_person_full_lfm_name` | character | Player's full name in last-first-middle order as recorded by the MLB Stats API. |
| `pick_person_strike_zone_top` | double | Upper boundary of the player's personalized strike zone in feet from the ground. |
| `pick_person_strike_zone_bottom` | double | Lower boundary of the player's personalized strike zone in feet from the ground. |
| `pick_person_xref_ids` | character | Serialized cross-reference identifiers linking the player to external data systems. |
| `pick_team_spring_league_id` | integer | MLB Stats API identifier for the team's spring training league. |
| `pick_team_spring_league_name` | character | Full name of the spring training league the team belongs to. |
| `pick_team_spring_league_link` | character | Relative URL path to the spring training league resource in the MLB Stats API. |
| `pick_team_spring_league_abbreviation` | character | Abbreviation for the spring training league the team participates in (e.g., Cactus, Grapefruit). |
| `pick_team_all_star_status` | character | All-Star game affiliation status of the team (e.g., American League, National League). |
| `pick_team_id` | integer | Unique MLB Stats API identifier for the team that made this draft pick. |
| `pick_team_name` | character | Full official name of the MLB team that made this pick (e.g., New York Yankees). |
| `pick_team_link` | character | Relative URL path to the team resource in the MLB Stats API. |
| `pick_team_season` | integer | MLB season year for which this team's metadata snapshot applies. |
| `pick_team_venue_id` | integer | MLB Stats API identifier for the team's regular-season home ballpark. |
| `pick_team_venue_name` | character | Name of the team's regular-season home ballpark (e.g., Yankee Stadium). |
| `pick_team_venue_link` | character | Relative URL path to the team's regular-season home venue in the MLB Stats API. |
| `pick_team_spring_venue_id` | integer | MLB Stats API identifier for the team's spring training ballpark. |
| `pick_team_spring_venue_link` | character | Relative URL path to the spring training venue resource in the MLB Stats API. |
| `pick_team_team_code` | character | Short internal code used by MLB to identify the team in system contexts. |
| `pick_team_file_code` | character | Lowercase file-system-safe code used internally by MLB to identify the team. |
| `pick_team_abbreviation` | character | Standard two- or three-letter abbreviation for the MLB team that made this pick. |
| `pick_team_team_name` | character | The nickname portion of the team's full name (e.g., Yankees, Dodgers). |
| `pick_team_location_name` | character | Geographic location name (city/metro) associated with the team (e.g., New York). |
| `pick_team_first_year_of_play` | character | Year in which the selecting franchise first played as an MLB team. |
| `pick_team_league_id` | integer | MLB Stats API identifier for the league (American or National) of the selecting team. |
| `pick_team_league_name` | character | Full name of the league the selecting team belongs to (e.g., American League). |
| `pick_team_league_link` | character | Relative URL path to the league resource in the MLB Stats API. |
| `pick_team_division_id` | integer | MLB Stats API identifier for the division the selecting team belongs to. |
| `pick_team_division_name` | character | Full name of the division the selecting team belongs to (e.g., AL East). |
| `pick_team_division_link` | character | Relative URL path to the division resource in the MLB Stats API. |
| `pick_team_sport_id` | integer | MLB Stats API identifier for the sport classification (MLB = 1). |
| `pick_team_sport_link` | character | Relative URL path to the sport resource in the MLB Stats API. |
| `pick_team_sport_name` | character | Full name of the sport classification for the team (e.g., Major League Baseball). |
| `pick_team_short_name` | character | Shortened version of the team name used in space-constrained display contexts. |
| `pick_team_franchise_name` | character | Historical franchise name that persists across any team relocations or renames. |
| `pick_team_club_name` | character | Informal club or nickname portion of the team's full name (e.g., Yankees, Red Sox). |
| `pick_team_active` | logical | Boolean flag indicating whether the selecting MLB franchise is currently active. |
| `pick_draft_type_code` | character | Short code identifying the type of draft (e.g., amateur, Rule 5) for this pick. |
| `pick_draft_type_description` | character | Human-readable description of the draft type associated with this pick. |
| `pick_is_drafted` | logical | Boolean flag indicating whether this draft slot has been filled with an actual selection. |
| `pick_is_pass` | logical | Boolean flag indicating whether the selecting team passed on this pick rather than making a selection. |
| `pick_year` | character | MLB draft year for which this pick record applies. |

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
| `timecode` | character | A timestamp string representing a specific point in time used to query the MLB Stats API for game state changes. |

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
| `schedule_date` | character | The calendar date for which schedule changes are being reported, identifying when rescheduled or suspended games occurred. |
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
| `hits_per9_inn` | double | Average number of hits allowed per nine innings across all games in the sample period. |
| `runs_per9_inn` | double | Average number of runs scored per nine innings across all games in the sample period. |
| `pitches_per9_inn` | double | Average number of pitches thrown per nine innings across all games in the sample period. |
| `plate_appearances_per9_inn` | double | Average number of plate appearances occurring per nine innings across all games in the sample period. |
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
| `total7_inn_games` | integer | Total number of seven-inning games played (including doubleheader games). |
| `total9_inn_games` | double | Total number of nine-inning games played in the sample period. |
| `total_extra_inn_games` | integer | Total extra-inning games. |
| `time_per_game` | character | Average time per game (HH:MM:SS). |
| `time_per_pitch` | character | Average time per pitch (HH:MM:SS). |
| `time_per_hit` | character | Average time per hit (HH:MM:SS). |
| `time_per_run` | character | Average time per run (HH:MM:SS). |
| `time_per_plate_appearance` | character | Average time per plate appearance (HH:MM:SS). |
| `time_per9_inn` | character | Average elapsed clock time per nine-inning game formatted as hours and minutes. |
| `time_per77_plate_appearances` | character | Average time per 77 plate appearances, used as a normalized pace benchmark by MLB. |
| `total_extra_inn_time` | character | Total extra-inning time (HHH:MM:SS). |
| `time_per7_inn_game_without_extra_inn` | character | Average elapsed clock time per seven-inning game excluding games that went to extra innings. |
| `total9_inn_games_completed_early` | integer | Number of nine-inning games that were called or suspended before completing nine full innings. |
| `total9_inn_games_without_extra_inn` | double | Number of nine-inning games completed without requiring extra innings. |
| `total9_inn_games_scheduled` | integer | Total number of nine-inning games that were scheduled in the sample period. |
| `hits_per_run` | double | Hits per run. |
| `pitches_per_pitcher` | double | Pitches per pitcher. |
| `season` | character | Season year. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_code` | character | Short sport code (e.g. 'mlb', 'aaa'). |
| `sport_link` | character | API link to the sport. |
| `pr_portal_calculated_fields_total7_inn_games` | integer | Calculated total count of seven-inning games as tallied by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_total9_inn_games` | double | Calculated total count of nine-inning games as tallied by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_total_extra_inn_games` | integer | Portal-calculated total extra-inning games. |
| `pr_portal_calculated_fields_time_per7_inn_game` | character | Calculated average game time per seven-inning game as produced by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_time_per9_inn_game` | character | Calculated average game time per nine-inning game as produced by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_time_per_extra_inn_game` | character | Portal-calculated time per extra-inning game. |
| `time_per7_inn_game` | character | Average elapsed clock time per seven-inning game formatted as hours and minutes. |
| `total7_inn_games_scheduled` | double | Total number of seven-inning games that were scheduled in the sample period. |
| `total7_inn_games_without_extra_inn` | double | Number of seven-inning games completed without requiring extra innings. |
| `total7_inn_games_completed_early` | double | Number of seven-inning games that were called or completed before the full seven innings were played. |

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
| `exemptions` | character | Serialized list of exemption codes or player IDs excluded from the high/low statistical split calculation. |
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
| `sort_stat_stat_groups` | character | Serialized list of statistical group identifiers (e.g., hitting, pitching) used to filter this high/low query. |
| `sort_stat_org_types` | character | Serialized list of organization types (e.g., MLB, MiLB) in scope for this high/low stat sort. |
| `sort_stat_high_low_types` | character | Serialized list of high/low result type codes (e.g., high, low) applicable to this stat leader query. |
| `sort_stat_streak_levels` | character | Serialized list of streak level codes defining the streak lengths tracked in this high/low query. |

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
| `current_team_spring_league_id` | double | The MLB Stats API numeric identifier for the spring training league of the player's current team. |
| `current_team_spring_league_name` | character | The full name of the spring training league (e.g., 'Cactus League') for the player's current team. |
| `current_team_spring_league_link` | character | The MLB Stats API relative URL linking to the spring training league resource for the player's current team. |
| `current_team_spring_league_abbreviation` | character | The abbreviation for the Cactus League or Grapefruit League in which the player's current team participates during spring training. |
| `current_team_all_star_status` | character | The All-Star designation status of the player's current team (e.g., which league's All-Star pool the team belongs to). |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer | The MLB season year for which the player's current team metadata is reported. |
| `current_team_venue_id` | integer | The MLB Stats API numeric identifier for the regular-season home ballpark of the player's current team. |
| `current_team_venue_name` | character | The official name of the regular-season home ballpark for the player's current team. |
| `current_team_venue_link` | character | The MLB Stats API relative URL linking to the regular-season venue resource for the player's current team. |
| `current_team_spring_venue_id` | double | The MLB Stats API numeric identifier for the spring training ballpark used by the player's current team. |
| `current_team_spring_venue_link` | character | The MLB Stats API relative URL linking to the spring training venue resource for the player's current team. |
| `current_team_team_code` | character | The three-letter internal team code used by MLB in legacy data systems and some API references. |
| `current_team_file_code` | character | The lowercase alphabetic file code used by MLB for identifying the team in media and data assets. |
| `current_team_abbreviation` | character | The standard two- or three-letter abbreviation for the player's current MLB team (e.g., 'NYY', 'LAD'). |
| `current_team_team_name` | character | The full official name of the player's current team, including both city and nickname. |
| `current_team_location_name` | character | The city or metropolitan area name associated with the player's current team. |
| `current_team_first_year_of_play` | character | The calendar year in which the player's current franchise first played MLB games. |
| `current_team_league_id` | integer | The MLB Stats API numeric identifier for the league (American League or National League) of the player's current team. |
| `current_team_league_name` | character | The full name of the league (e.g., 'American League') in which the player's current team competes. |
| `current_team_league_link` | character | The MLB Stats API relative URL linking to the league resource for the player's current team. |
| `current_team_division_id` | double | The MLB Stats API numeric identifier for the division in which the player's current team competes. |
| `current_team_division_name` | character | The full name of the division in which the player's current team competes (e.g., 'American League East'). |
| `current_team_division_link` | character | The MLB Stats API relative URL linking to the division resource for the player's current team. |
| `current_team_sport_id` | integer | The MLB Stats API numeric identifier for the sport classification (e.g., 1 for MLB) of the player's current team. |
| `current_team_sport_link` | character | The MLB Stats API relative URL linking to the sport resource associated with the player's current team. |
| `current_team_sport_name` | character | The name of the sport classification for the player's current team (e.g., 'Major League Baseball'). |
| `current_team_short_name` | character | A shortened display name for the player's current team, often used in space-constrained UI contexts. |
| `current_team_franchise_name` | character | The historical franchise name for the player's current team, which may differ from the current team name for relocated clubs. |
| `current_team_club_name` | character | The short club nickname for the player's current team, typically the city-less portion of the franchise name. |
| `current_team_active` | logical | Boolean flag indicating whether the player's current team is an active MLB franchise. |
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
| `current_team_parent_org_name` | character | The name of the parent major-league organization for the player's current team. |
| `current_team_parent_org_id` | double | The MLB Stats API numeric identifier for the parent organization (major-league affiliate) of the player's current team. |

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
| `current_team_spring_league_id` | double | MLB Stats API identifier for the spring training league of the participant's current team. |
| `current_team_spring_league_name` | character | Full name of the spring training league the participant's team belongs to. |
| `current_team_spring_league_link` | character | Relative URL path to the spring training league resource in the MLB Stats API. |
| `current_team_spring_league_abbreviation` | character | Abbreviation for the spring training league the participant's team belongs to (e.g., Cactus, Grapefruit). |
| `current_team_all_star_status` | character | All-Star game league affiliation of the participant's current team (e.g., American League, National League). |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer | MLB season year for which the participant's current team metadata snapshot applies. |
| `current_team_venue_id` | integer | MLB Stats API identifier for the participant's current team's regular-season home ballpark. |
| `current_team_venue_name` | character | Name of the participant's current team's regular-season home ballpark. |
| `current_team_venue_link` | character | Relative URL path to the current team's home venue resource in the MLB Stats API. |
| `current_team_spring_venue_id` | double | MLB Stats API identifier for the spring training ballpark used by the participant's team. |
| `current_team_spring_venue_link` | character | Relative URL path to the spring training venue resource in the MLB Stats API. |
| `current_team_team_code` | character | Short internal code used by MLB to identify the participant's current team in system contexts. |
| `current_team_file_code` | character | Lowercase file-system-safe code used by MLB to identify the participant's current team. |
| `current_team_abbreviation` | character | Standard two- or three-letter abbreviation for the Home Run Derby participant's current MLB team. |
| `current_team_team_name` | character | The nickname portion of the participant's current team name (e.g., Red Sox, Braves). |
| `current_team_location_name` | character | City or metropolitan area name associated with the participant's current team. |
| `current_team_first_year_of_play` | character | Year in which the participant's current franchise first played as an MLB team. |
| `current_team_league_id` | integer | MLB Stats API identifier for the league (American or National) of the participant's current team. |
| `current_team_league_name` | character | Full name of the league the participant's current team belongs to (e.g., National League). |
| `current_team_league_link` | character | Relative URL path to the league resource for the participant's current team in the MLB Stats API. |
| `current_team_division_id` | double | MLB Stats API identifier for the division the participant's current team belongs to. |
| `current_team_division_name` | character | Full name of the division the participant's current team belongs to (e.g., AL East). |
| `current_team_division_link` | character | Relative URL path to the participant's current team's division resource in the MLB Stats API. |
| `current_team_sport_id` | integer | MLB Stats API identifier for the sport classification of the participant's current team (MLB = 1). |
| `current_team_sport_link` | character | Relative URL path to the sport resource for the participant's current team in the MLB Stats API. |
| `current_team_sport_name` | character | Full sport classification name for the participant's current team (e.g., Major League Baseball). |
| `current_team_short_name` | character | Shortened display name of the participant's current team for space-constrained contexts. |
| `current_team_franchise_name` | character | Historical franchise name for the participant's team, persisting across relocations. |
| `current_team_club_name` | character | Informal nickname portion of the participant's current team name (e.g., Yankees, Dodgers). |
| `current_team_active` | logical | Boolean flag indicating whether the participant's current franchise is an active MLB organization. |
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
| `current_team_parent_org_name` | character | Name of the parent MLB organization for the participant's current team. |
| `current_team_parent_org_id` | double | MLB Stats API identifier for the parent MLB organization of the participant's current team, relevant for minor league affiliates. |

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
| `current_team_spring_league_id` | double | The MLB Stats API numeric identifier for the spring training league of the pool participant's current team. |
| `current_team_spring_league_name` | character | The full name of the spring training league for the pool participant's current team. |
| `current_team_spring_league_link` | character | The MLB Stats API relative URL linking to the spring training league resource for the pool participant's current team. |
| `current_team_spring_league_abbreviation` | character | The abbreviation for the spring training league in which the pool participant's current team plays during spring training. |
| `current_team_all_star_status` | character | The All-Star designation status of the pool participant's current team (e.g., which league's All-Star pool the team belongs to). |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer | The MLB season year for which the pool participant's current team metadata is reported. |
| `current_team_venue_id` | integer | The MLB Stats API numeric identifier for the regular-season home ballpark of the pool participant's current team. |
| `current_team_venue_name` | character | The official name of the regular-season home ballpark for the pool participant's current team. |
| `current_team_venue_link` | character | The MLB Stats API relative URL linking to the regular-season venue resource for the pool participant's current team. |
| `current_team_spring_venue_id` | double | The MLB Stats API numeric identifier for the spring training ballpark used by the pool participant's current team. |
| `current_team_spring_venue_link` | character | The MLB Stats API relative URL linking to the spring training venue resource for the pool participant's current team. |
| `current_team_team_code` | character | The three-letter internal team code used by MLB for the pool participant's team in legacy data systems. |
| `current_team_file_code` | character | The lowercase alphabetic file code used by MLB for identifying the pool participant's team in media and data assets. |
| `current_team_abbreviation` | character | The standard two- or three-letter abbreviation for the Home Run Derby pool participant's current MLB team. |
| `current_team_team_name` | character | The full official name of the pool participant's current team, including both city and nickname. |
| `current_team_location_name` | character | The city or metropolitan area name associated with the pool participant's current team. |
| `current_team_first_year_of_play` | character | The calendar year in which the pool participant's current franchise first played MLB games. |
| `current_team_league_id` | integer | The MLB Stats API numeric identifier for the league of the pool participant's current team. |
| `current_team_league_name` | character | The full name of the league (e.g., 'National League') in which the pool participant's current team competes. |
| `current_team_league_link` | character | The MLB Stats API relative URL linking to the league resource for the pool participant's current team. |
| `current_team_division_id` | double | The MLB Stats API numeric identifier for the division in which the pool participant's current team competes. |
| `current_team_division_name` | character | The full name of the division in which the pool participant's current team competes (e.g., 'National League West'). |
| `current_team_division_link` | character | The MLB Stats API relative URL linking to the division resource for the pool participant's current team. |
| `current_team_sport_id` | integer | The MLB Stats API numeric identifier for the sport classification of the pool participant's current team. |
| `current_team_sport_link` | character | The MLB Stats API relative URL linking to the sport resource associated with the pool participant's current team. |
| `current_team_sport_name` | character | The name of the sport classification for the pool participant's current team (e.g., 'Major League Baseball'). |
| `current_team_short_name` | character | A shortened display name for the pool participant's current team, often used in space-constrained UI contexts. |
| `current_team_franchise_name` | character | The historical franchise name for the pool participant's current team, which may differ from the current team name for relocated clubs. |
| `current_team_club_name` | character | The short club nickname for the pool participant's current team, typically the city-less portion of the franchise name. |
| `current_team_active` | logical | Boolean flag indicating whether the pool participant's current team is an active MLB franchise. |
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
| `current_team_parent_org_name` | character | The name of the parent major-league organization for the pool participant's current team. |
| `current_team_parent_org_id` | double | The MLB Stats API numeric identifier for the parent organization of the pool participant's current team. |

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
| `schedule_date` | character | The calendar date grouping tied (suspended and resumed) games in this response row, as returned by the MLB Stats API schedule endpoint. |
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
| `exemptions` | character | A serialized list of any statistical exemption notes or flags associated with the team's stat splits (e.g., players exempt from qualifying thresholds). |
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
| `leaders` | character | Serialized representation of the statistical leaders entries for the team stat category returned by the MLB Stats API. |
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
