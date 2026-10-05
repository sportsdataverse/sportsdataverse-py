---
title: "MLB — MLB Stats API — Other"
sidebar_label: "Other"
sidebar_position: 8
description: "MLB — MLB Stats API — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Other

## mlb_pbp

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

### Returns {#mlb_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_pbp-example}

```python
mlb_pbp(game_pk=716390)
```

_Last validated n/a._

## mlb_boxscore

GET /api/v1/game/{gamePk}/boxscore — team + player boxscore for one game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/boxscore](https://statsapi.mlb.com/api/v1/game/716390/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_side` | character | Home or away indicator. |
| `team_id` | integer | Unique ESPN team identifier. |
| `team_name` | character | Team name. |
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `parent_team_id` | integer | MLB Stats API identifier for the player's parent (MLB-level) organization, useful for tracking players on optional assignment. |
| `batting_order` | character | Spot in the batting order (box-score row order). |
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

### Example {#mlb_boxscore-example}

```python
mlb_boxscore(game_pk=716390)
```

_Last validated n/a._

## mlb_linescore

GET /api/v1/game/{gamePk}/linescore — inning-by-inning + current game state.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/linescore`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/linescore](https://statsapi.mlb.com/api/v1/game/716390/linescore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_linescore-returns}

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

### Example {#mlb_linescore-example}

```python
mlb_linescore(game_pk=716390)
```

_Last validated n/a._

## mlb_win_probability

GET /api/v1/game/{gamePk}/winProbability — per-play WP timeline.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/winProbability`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/winProbability](https://statsapi.mlb.com/api/v1/game/716390/winProbability)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_win_probability-returns}

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

### Example {#mlb_win_probability-example}

```python
mlb_win_probability(game_pk=716390)
```

_Last validated n/a._

## mlb_people

GET /api/v1/people?personIds=... — bulk person lookup by MLBAM id.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people](https://statsapi.mlb.com/api/v1/people)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `personIds` | `person_ids` |  |  | `Y` | personIds query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_people-returns}

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
| `birth_state_province` | character | State or province of birth. |
| `middle_name` | character | Player middle name. |
| `draft_year` | double | Year the player was drafted. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_people-example}

```python
mlb_people()
```

_Last validated n/a._

## mlb_person

GET /api/v1/people/{personId} — single person detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/{person_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/660271](https://statsapi.mlb.com/api/v1/people/660271)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | person_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_person-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_person-example}

```python
mlb_person(person_id=660271)
```

_Last validated n/a._

## mlb_person_game_stats

GET /api/v1/people/{personId}/stats/game/{gamePk} — one player, one game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/{person_id}/stats/game/{game_pk}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/660271/stats/game/716390](https://statsapi.mlb.com/api/v1/people/660271/stats/game/716390)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | person_id path parameter. |
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_person_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `total_splits` | double | Total number of splits in the leaderboard. |
| `exemptions` | character | Serialized list of statistical exemptions or special-case flags applied to this player's game-log splits. |
| `splits` | character | Splits. |
| `type_display_name` | character | Stat type display name. |
| `group_display_name` | character | Stat group display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_person_game_stats-example}

```python
mlb_person_game_stats(person_id=660271, game_pk=716390)
```

_Last validated n/a._

## mlb_sport_players

GET /api/v1/sports/{sportId}/players — every player in a sport for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}/players`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports](https://statsapi.mlb.com/api/v1/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  |  | `Y` | sport_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_sport_players-returns}

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

### Example {#mlb_sport_players-example}

```python
mlb_sport_players()
```

_Last validated n/a._

## mlb_sports

GET /api/v1/sports — list known sports (MLB, MiLB, KBO, NPB, …).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports](https://statsapi.mlb.com/api/v1/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns {#mlb_sports-returns}

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

### Example {#mlb_sports-example}

```python
mlb_sports()
```

_Last validated n/a._

## mlb_leagues

GET /api/v1/leagues — list leagues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/leagues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagues](https://statsapi.mlb.com/api/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |

### Returns {#mlb_leagues-returns}

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

### Example {#mlb_leagues-example}

```python
mlb_leagues()
```

_Last validated n/a._

## mlb_season

GET /api/v1/seasons/{seasonId} — single season detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/{season_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/X](https://statsapi.mlb.com/api/v1/seasons/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns {#mlb_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | stats.ncaa.org season identifier. |
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

### Example {#mlb_season-example}

```python
mlb_season(season_id='X')
```

_Last validated n/a._

## mlb_venues

GET /api/v1/venues — list venues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/venues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/venues](https://statsapi.mlb.com/api/v1/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns {#mlb_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `active` | logical | Whether the player is currently active. |
| `season` | character | Season year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_venues-example}

```python
mlb_venues()
```

_Last validated n/a._

## mlb_venue

GET /api/v1/venues/{venueId} — single venue detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/venues/{venue_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/venues/15](https://statsapi.mlb.com/api/v1/venues/15)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns {#mlb_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `active` | logical | Whether the player is currently active. |
| `season` | character | Season year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_venue-example}

```python
mlb_venue(venue_id=15)
```

_Last validated n/a._

## mlb_meta

GET /api/v1/{metaType} — enum lookup (the API's self-describing surface).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/{meta_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagueLeaderTypes](https://statsapi.mlb.com/api/v1/leagueLeaderTypes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `meta_type` | `meta_type` |  | `Y` |  | meta_type path parameter. |

### Returns {#mlb_meta-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_meta-example}

```python
mlb_meta(meta_type='leagueLeaderTypes')
```

_Last validated n/a._

## mlb_awards

GET /api/v1/awards — list award IDs (call with no params to enumerate).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards](https://statsapi.mlb.com/api/v1/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns {#mlb_awards-returns}

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

### Example {#mlb_awards-example}

```python
mlb_awards()
```

_Last validated n/a._

## mlb_award_recipients

GET /api/v1/awards/{awardId}/recipients — historical winners of one award.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards/{award_id}/recipients`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients](https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns {#mlb_award_recipients-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `name` | character | Display name. |
| `date` | character | Date in YYYY-MM-DD format. |
| `season` | character | Season year. |
| `team_id` | integer | Unique ESPN team identifier. |
| `team_link` | character | API link to the team. |
| `player_id` | integer | stats.ncaa.org player identifier. |
| `player_link` | character | API relative link to the player. |
| `player_primary_position_code` | character | Recipient primary fielding position code. |
| `player_primary_position_name` | character | Recipient primary fielding position name. |
| `player_primary_position_type` | character | Participant primary position type (e.g. 'Hitter'). |
| `player_primary_position_abbreviation` | character | Participant primary position abbreviation (e.g. 'DH'). |
| `player_name_first_last` | character | Participant name in first-last order. |
| `votes` | double | Number of votes received. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_award_recipients-example}

```python
mlb_award_recipients(award_id='MLBHOF')
```

_Last validated n/a._
