---
title: "MLB — MLB Stats API — Other: pbp–venue"
sidebar_label: "Other: pbp–venue"
sidebar_position: 8
description: "MLB — MLB Stats API — Other: pbp–venue — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Other: pbp–venue

## mlb_pbp

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live`

**Valid URL:** [https://statsapi.mlb.com/api/v1.1/game/716390/feed/live](https://statsapi.mlb.com/api/v1.1/game/716390/feed/live)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  |  |
| `language` | `language` |  |  | `Y` |  |
| `timecode` | `timecode` |  |  | `Y` |  |
| `hydrate` | `hydrate` |  |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |  |

### Returns {#mlb_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_pbp-example}

```python
mlb_pbp(game_pk=716390)
```

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
| `team_side` | String |  |
| `team_id` | Int64 | Unique ESPN team identifier. |
| `team_name` | String | Team name. |
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `parent_team_id` | Int64 | MLB Stats API identifier for the player's parent (MLB-level) organization, useful for tracking players on optional assignment. |
| `batting_order` | String | Spot in the batting order (box-score row order). |
| `all_positions` | String | All fielding positions played by the player during the game, as a list of position codes. |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |
| `person_boxscore_name` | String | Name as shown in box scores. |
| `position_code` | String | Numeric scorekeeping position code. |
| `position_name` | String | Position name. |
| `position_type` | String | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | String | Position abbreviation. |
| `status_code` | String | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | String | Roster status description (e.g. 'Active'). |
| `stats_batting_summary` | String | Condensed text summary of the batter's performance line (e.g., '2-4, HR, 2 RBI') for display purposes. |
| `stats_batting_games_played` | Float64 | Number of games played indicator for this batter's boxscore row (typically 1 for a standard game appearance). |
| `stats_batting_fly_outs` | Float64 | Number of outs recorded by the batter on fly balls in this game. |
| `stats_batting_ground_outs` | Float64 | Number of outs recorded by the batter on ground balls in this game. |
| `stats_batting_air_outs` | Float64 | Number of outs recorded by the batter on balls hit in the air during this game. |
| `stats_batting_runs` | Float64 | Number of runs scored by the batter in this game. |
| `stats_batting_doubles` | Float64 | Number of doubles hit by the batter in this game. |
| `stats_batting_triples` | Float64 | Number of triples hit by the batter in this game. |
| `stats_batting_home_runs` | Float64 | Number of home runs hit by the batter in this game. |
| `stats_batting_strike_outs` | Float64 | Number of times the batter struck out in this game. |
| `stats_batting_base_on_balls` | Float64 | Number of walks (bases on balls) drawn by the batter in this game, including intentional walks. |
| `stats_batting_intentional_walks` | Float64 | Number of intentional walks issued to the batter in this game. |
| `stats_batting_hits` | Float64 | Total number of hits recorded by the batter in this game. |
| `stats_batting_hit_by_pitch` | Float64 | Number of times the batter was hit by a pitch in this game. |
| `stats_batting_at_bats` | Float64 | Number of official at-bats for the batter in this game. |
| `stats_batting_caught_stealing` | Float64 | Number of times the batter was caught stealing in this game. |
| `stats_batting_stolen_bases` | Float64 | Number of stolen bases recorded by the batter in this game. |
| `stats_batting_stolen_base_percentage` | String | Percentage of stolen base attempts that were successful for the batter in this game. |
| `stats_batting_ground_into_double_play` | Float64 | Number of times the batter grounded into a double play in this game. |
| `stats_batting_ground_into_triple_play` | Float64 | Number of times the batter grounded into a triple play in this game. |
| `stats_batting_plate_appearances` | Float64 | Total number of plate appearances for the batter in this game. |
| `stats_batting_total_bases` | Float64 | Total number of bases accumulated by the batter on hits in this game. |
| `stats_batting_rbi` | Float64 | Number of runs batted in (RBI) credited to the batter in this game. |
| `stats_batting_left_on_base` | Float64 | Number of runners left on base when the batter made an out or the inning ended in this game. |
| `stats_batting_sac_bunts` | Float64 | Number of sacrifice bunts executed by the batter in this game. |
| `stats_batting_sac_flies` | Float64 | Number of sacrifice flies hit by the batter that scored a run in this game. |
| `stats_batting_catchers_interference` | Float64 | Number of times the batter reached base due to catcher's interference in this game. |
| `stats_batting_pickoffs` | Float64 | Number of times the batter was picked off base in this game. |
| `stats_batting_at_bats_per_home_run` | String | At-bats per home run ratio for the batter in this game. |
| `stats_batting_pop_outs` | Float64 | Number of outs recorded by the batter on infield pop-ups in this game. |
| `stats_batting_line_outs` | Float64 | Number of outs recorded by the batter on line drives caught in this game. |
| `stats_fielding_caught_stealing` | Float64 | Number of baserunners caught stealing by the player (typically a catcher) in this game. |
| `stats_fielding_stolen_bases` | Float64 | Number of stolen bases allowed by the player while fielding in this game. |
| `stats_fielding_stolen_base_percentage` | String | Percentage of stolen base attempts against the player (catcher perspective) that were successful in this game. |
| `stats_fielding_caught_stealing_percentage` | String | Percentage of stolen base attempts that the player threw out in this game. |
| `stats_fielding_assists` | Float64 | Number of fielding assists recorded by the player in this game. |
| `stats_fielding_put_outs` | Float64 | Number of putouts recorded by the player in this game. |
| `stats_fielding_errors` | Float64 | Number of fielding errors committed by the player in this game. |
| `stats_fielding_chances` | Float64 | Total fielding chances for the player in this game (putouts + assists + errors). |
| `stats_fielding_fielding` | String | Fielding percentage for the player in this game, calculated as (putouts + assists) / total chances. |
| `stats_fielding_passed_ball` | Float64 | Number of passed balls charged to the player (catcher-specific) in this game. |
| `stats_fielding_pickoffs` | Float64 | Number of pickoffs credited to the player as a fielder in this game. |
| `season_stats_batting_games_played` | Int64 | Season-to-date number of games in which the batter appeared. |
| `season_stats_batting_fly_outs` | Int64 | Season-to-date number of outs recorded by the batter on fly balls caught in the outfield. |
| `season_stats_batting_ground_outs` | Int64 | Season-to-date number of outs recorded by the batter on ground balls. |
| `season_stats_batting_air_outs` | Int64 | Season-to-date number of outs recorded by the batter on balls hit in the air (fly balls and line drives caught). |
| `season_stats_batting_runs` | Int64 | Season-to-date number of runs scored by the batter. |
| `season_stats_batting_doubles` | Int64 | Season-to-date number of doubles hit by the batter. |
| `season_stats_batting_triples` | Int64 | Season-to-date number of triples hit by the batter. |
| `season_stats_batting_home_runs` | Int64 | Season-to-date number of home runs hit by the batter. |
| `season_stats_batting_strike_outs` | Int64 | Season-to-date number of times the batter struck out. |
| `season_stats_batting_base_on_balls` | Int64 | Season-to-date total walks (bases on balls) drawn by the batter, including intentional walks. |
| `season_stats_batting_intentional_walks` | Int64 | Season-to-date number of intentional walks (IBB) issued to the batter. |
| `season_stats_batting_hits` | Int64 | Season-to-date total number of hits recorded by the batter. |
| `season_stats_batting_hit_by_pitch` | Int64 | Season-to-date number of times the batter was hit by a pitch. |
| `season_stats_batting_avg` | String | Season-to-date batting average (hits divided by at-bats) for the batter. |
| `season_stats_batting_at_bats` | Int64 | Season-to-date number of official at-bats accumulated by the batter. |
| `season_stats_batting_obp` | String | Season-to-date on-base percentage (OBP), measuring how often the batter reaches base per plate appearance. |
| `season_stats_batting_slg` | String | Season-to-date slugging percentage (SLG), measuring total bases per at-bat. |
| `season_stats_batting_ops` | String | Season-to-date on-base plus slugging percentage (OPS), a combined measure of a batter's ability to get on base and hit for power. |
| `season_stats_batting_caught_stealing` | Int64 | Season-to-date number of times the batter was caught stealing a base. |
| `season_stats_batting_stolen_bases` | Int64 | Season-to-date number of bases stolen by the batter. |
| `season_stats_batting_stolen_base_percentage` | String | Season-to-date percentage of stolen base attempts that were successful for the batter. |
| `season_stats_batting_caught_stealing_percentage` | String | Season-to-date percentage of stolen base attempts that resulted in the batter being caught stealing. |
| `season_stats_batting_ground_into_double_play` | Int64 | Season-to-date number of times the batter grounded into a double play. |
| `season_stats_batting_ground_into_triple_play` | Int64 | Season-to-date number of times the batter grounded into a triple play. |
| `season_stats_batting_plate_appearances` | Int64 | Season-to-date total number of plate appearances for the batter, including at-bats, walks, HBP, and sacrifices. |
| `season_stats_batting_total_bases` | Int64 | Season-to-date total number of bases accumulated by the batter on hits. |
| `season_stats_batting_rbi` | Int64 | Season-to-date number of runs batted in (RBI) credited to the batter. |
| `season_stats_batting_left_on_base` | Int64 | Season-to-date number of runners left on base when the batter made an out or the inning ended. |
| `season_stats_batting_sac_bunts` | Int64 | Season-to-date number of sacrifice bunts executed by the batter. |
| `season_stats_batting_sac_flies` | Int64 | Season-to-date number of sacrifice flies hit by the batter that scored a run. |
| `season_stats_batting_babip` | String | Season-to-date Batting Average on Balls In Play (BABIP), measuring batting average excluding strikeouts and home runs. |
| `season_stats_batting_ground_outs_to_airouts` | String | Season-to-date ratio of ground outs to air outs, indicating the batter's tendency to hit the ball on the ground versus in the air. |
| `season_stats_batting_catchers_interference` | Int64 | Season-to-date number of times the batter reached base due to catcher's interference. |
| `season_stats_batting_pickoffs` | Int64 | Season-to-date number of times the batter was picked off base by a pitcher or catcher. |
| `season_stats_batting_at_bats_per_home_run` | String | Season-to-date ratio of at-bats per home run, reflecting the batter's home run frequency. |
| `season_stats_batting_pop_outs` | Int64 | Season-to-date number of outs recorded by the batter on pop-ups caught in the infield. |
| `season_stats_batting_line_outs` | Int64 | Season-to-date number of outs recorded by the batter on line drives caught. |
| `season_stats_pitching_games_played` | Int64 | Season-to-date number of games the pitcher was active on the roster (may include non-pitching appearances). |
| `season_stats_pitching_games_started` | Int64 | Season-to-date number of games in which the pitcher was the starting pitcher. |
| `season_stats_pitching_fly_outs` | Int64 | Season-to-date number of outs recorded by the pitcher on fly balls. |
| `season_stats_pitching_ground_outs` | Int64 | Season-to-date number of outs recorded by the pitcher on ground balls. |
| `season_stats_pitching_air_outs` | Int64 | Season-to-date number of outs recorded by the pitcher on balls hit in the air. |
| `season_stats_pitching_runs` | Int64 | Season-to-date total runs (earned and unearned) allowed by the pitcher. |
| `season_stats_pitching_doubles` | Int64 | Season-to-date number of doubles allowed by the pitcher. |
| `season_stats_pitching_triples` | Int64 | Season-to-date number of triples allowed by the pitcher. |
| `season_stats_pitching_home_runs` | Int64 | Season-to-date number of home runs allowed by the pitcher. |
| `season_stats_pitching_strike_outs` | Int64 | Season-to-date number of batters struck out by the pitcher. |
| `season_stats_pitching_base_on_balls` | Int64 | Season-to-date total walks (bases on balls) issued by the pitcher, including intentional walks. |
| `season_stats_pitching_intentional_walks` | Int64 | Season-to-date number of intentional walks (IBB) issued by the pitcher. |
| `season_stats_pitching_hits` | Int64 | Season-to-date number of hits allowed by the pitcher. |
| `season_stats_pitching_hit_by_pitch` | Int64 | Season-to-date number of hit-by-pitch events while the pitcher was pitching (alternate field name for hit_batsmen). |
| `season_stats_pitching_at_bats` | Int64 | Season-to-date number of at-bats faced by the pitcher (excluding walks, HBP, and sacrifices). |
| `season_stats_pitching_obp` | String | Season-to-date on-base percentage allowed by the pitcher (opponents' OBP against this pitcher). |
| `season_stats_pitching_caught_stealing` | Int64 | Season-to-date number of baserunners caught stealing while the pitcher was on the mound. |
| `season_stats_pitching_stolen_bases` | Int64 | Season-to-date number of stolen bases allowed while the pitcher was pitching. |
| `season_stats_pitching_stolen_base_percentage` | String | Season-to-date percentage of stolen base attempts that were successful while the pitcher was on the mound. |
| `season_stats_pitching_caught_stealing_percentage` | String | Season-to-date percentage of stolen base attempts that were thrown out while the pitcher was pitching. |
| `season_stats_pitching_number_of_pitches` | Int64 | Season-to-date total number of pitches thrown by the pitcher. |
| `season_stats_pitching_era` | String | Season-to-date Earned Run Average (ERA) for the pitcher, expressed as earned runs per nine innings. |
| `season_stats_pitching_innings_pitched` | String | Season-to-date total innings pitched, expressed as a decimal where each out is one-third of an inning. |
| `season_stats_pitching_wins` | Int64 | Season-to-date number of wins credited to the pitcher. |
| `season_stats_pitching_losses` | Int64 | Season-to-date number of losses charged to the pitcher. |
| `season_stats_pitching_saves` | Int64 | Season-to-date number of saves recorded by the pitcher. |
| `season_stats_pitching_save_opportunities` | Int64 | Season-to-date number of save opportunities the pitcher entered (leads of three runs or fewer in the seventh inning or later, or entering with the tying run on base). |
| `season_stats_pitching_holds` | Int64 | Season-to-date number of holds recorded by the pitcher (relief appearance maintaining a lead without a save situation). |
| `season_stats_pitching_blown_saves` | Int64 | Season-to-date number of blown save opportunities for the pitcher. |
| `season_stats_pitching_earned_runs` | Int64 | Season-to-date number of earned runs allowed by the pitcher. |
| `season_stats_pitching_whip` | String | Season-to-date Walks plus Hits per Inning Pitched (WHIP), measuring baserunners allowed per inning. |
| `season_stats_pitching_batters_faced` | Int64 | Season-to-date total number of batters faced by the pitcher. |
| `season_stats_pitching_outs` | Int64 | Season-to-date total number of outs recorded by the pitcher. |
| `season_stats_pitching_games_pitched` | Int64 | Season-to-date number of games in which the pitcher appeared. |
| `season_stats_pitching_complete_games` | Int64 | Season-to-date number of complete games pitched by the pitcher. |
| `season_stats_pitching_shutouts` | Int64 | Season-to-date number of complete-game shutouts pitched. |
| `season_stats_pitching_balls` | Int64 | Season-to-date number of ball calls recorded against the pitcher. |
| `season_stats_pitching_strikes` | Int64 | Season-to-date total number of strikes thrown by the pitcher. |
| `season_stats_pitching_strike_percentage` | String | Season-to-date percentage of all pitches thrown that were strikes. |
| `season_stats_pitching_hit_batsmen` | Int64 | Season-to-date number of batters hit by a pitch thrown by the pitcher. |
| `season_stats_pitching_balks` | Int64 | Season-to-date number of balks called against the pitcher. |
| `season_stats_pitching_wild_pitches` | Int64 | Season-to-date number of wild pitches thrown by the pitcher. |
| `season_stats_pitching_pickoffs` | Int64 | Season-to-date number of pickoffs executed by the pitcher. |
| `season_stats_pitching_ground_outs_to_airouts` | String | Season-to-date ratio of ground ball outs to air ball outs allowed by the pitcher. |
| `season_stats_pitching_rbi` | Int64 | Season-to-date number of RBI allowed (runs batted in by opposing batters) while this pitcher was pitching. |
| `season_stats_pitching_win_percentage` | String | Season-to-date winning percentage for the pitcher (wins divided by decisions). |
| `season_stats_pitching_pitches_per_inning` | String | Season-to-date average number of pitches thrown per inning by the pitcher. |
| `season_stats_pitching_games_finished` | Int64 | Season-to-date number of games in which the pitcher was the last pitcher used by their team. |
| `season_stats_pitching_strikeout_walk_ratio` | String | Season-to-date ratio of strikeouts to walks, measuring the pitcher's command and dominance. |
| `season_stats_pitching_strikeouts_per9_inn` | String | Season-to-date strikeouts recorded per nine innings pitched (K/9), a rate measure of strikeout ability. |
| `season_stats_pitching_walks_per9_inn` | String | Season-to-date walks issued per nine innings pitched (BB/9), a rate measure of control. |
| `season_stats_pitching_hits_per9_inn` | String | Season-to-date hits allowed per nine innings pitched, a rate stat measuring hit prevention. |
| `season_stats_pitching_runs_scored_per9` | String | Season-to-date total runs (including unearned) allowed per nine innings pitched. |
| `season_stats_pitching_home_runs_per9` | String | Season-to-date home runs allowed per nine innings pitched. |
| `season_stats_pitching_inherited_runners` | Int64 | Season-to-date number of baserunners already on base when the pitcher entered the game. |
| `season_stats_pitching_inherited_runners_scored` | Int64 | Season-to-date number of inherited runners who eventually scored while or after the pitcher was pitching. |
| `season_stats_pitching_catchers_interference` | Int64 | Season-to-date number of times the pitcher benefited from a catcher's interference call. |
| `season_stats_pitching_sac_bunts` | Int64 | Season-to-date number of sacrifice bunts allowed by the pitcher. |
| `season_stats_pitching_sac_flies` | Int64 | Season-to-date number of sacrifice flies allowed by the pitcher. |
| `season_stats_pitching_passed_ball` | Int64 | Season-to-date number of passed balls that occurred while the pitcher was pitching. |
| `season_stats_pitching_pop_outs` | Int64 | Season-to-date number of outs recorded by the pitcher on pop-ups caught in the infield. |
| `season_stats_pitching_line_outs` | Int64 | Season-to-date number of outs recorded by the pitcher on line drives caught. |
| `season_stats_fielding_caught_stealing` | Int64 | Season-to-date number of baserunners caught stealing by the player (typically a catcher stat). |
| `season_stats_fielding_stolen_bases` | Int64 | Season-to-date number of stolen bases allowed by the player while fielding (typically catcher). |
| `season_stats_fielding_stolen_base_percentage` | String | Season-to-date percentage of stolen base attempts against the player that were successful (catcher perspective). |
| `season_stats_fielding_caught_stealing_percentage` | String | Season-to-date percentage of stolen base attempts that the player (usually a catcher) threw out. |
| `season_stats_fielding_assists` | Int64 | Season-to-date number of fielding assists recorded by the player (touching the ball before a putout by a teammate). |
| `season_stats_fielding_put_outs` | Int64 | Season-to-date number of putouts recorded by the player (directly retiring a baserunner or batter). |
| `season_stats_fielding_errors` | Int64 | Season-to-date number of fielding errors committed by the player. |
| `season_stats_fielding_chances` | Int64 | Season-to-date total fielding chances for the player (putouts + assists + errors). |
| `season_stats_fielding_fielding` | String | Season-to-date fielding percentage for the player, calculated as (putouts + assists) / total chances. |
| `season_stats_fielding_passed_ball` | Int64 | Season-to-date number of passed balls charged to the player (catcher-specific). |
| `season_stats_fielding_pickoffs` | Int64 | Season-to-date number of pickoffs credited to the player as a fielder. |
| `game_status_is_current_batter` | Boolean | Indicates whether the player is currently at bat at the moment the boxscore was captured. |
| `game_status_is_current_pitcher` | Boolean | Indicates whether the player is currently pitching at the moment the boxscore was captured. |
| `game_status_is_on_bench` | Boolean | Indicates whether the player is currently on the bench (not in the active lineup) at time of capture. |
| `game_status_is_substitute` | Boolean | Indicates whether the player entered the game as a substitute for another player. |
| `stats_fielding_games_started` | Float64 | Indicator of whether the player started at a fielding position in this game. |
| `season_stats_fielding_games_started` | Float64 | Season-to-date number of games in which the player started at a fielding position. |
| `season_stats_pitching_pitches_thrown` | Float64 | Season-to-date total pitches thrown by the pitcher (may differ from number_of_pitches if strikes/balls are tracked separately). |
| `stats_pitching_summary` | String | Condensed text summary of the pitcher's performance line (e.g., '6.0 IP, 2 ER, 8 K') for display purposes. |
| `stats_pitching_games_played` | Float64 | Number of games the pitcher appeared in for this boxscore row (typically 1). |
| `stats_pitching_games_started` | Float64 | Indicator of whether the pitcher was the starting pitcher in this game. |
| `stats_pitching_fly_outs` | Float64 | Number of outs recorded by the pitcher on fly balls in this game. |
| `stats_pitching_ground_outs` | Float64 | Number of outs recorded by the pitcher on ground balls in this game. |
| `stats_pitching_air_outs` | Float64 | Number of outs recorded by the pitcher on balls hit in the air in this game. |
| `stats_pitching_runs` | Float64 | Total runs (earned and unearned) allowed by the pitcher in this game. |
| `stats_pitching_doubles` | Float64 | Number of doubles allowed by the pitcher in this game. |
| `stats_pitching_triples` | Float64 | Number of triples allowed by the pitcher in this game. |
| `stats_pitching_home_runs` | Float64 | Number of home runs allowed by the pitcher in this game. |
| `stats_pitching_strike_outs` | Float64 | Number of batters struck out by the pitcher in this game. |
| `stats_pitching_base_on_balls` | Float64 | Number of walks (bases on balls) issued by the pitcher in this game, including intentional walks. |
| `stats_pitching_intentional_walks` | Float64 | Number of intentional walks (IBB) issued by the pitcher in this game. |
| `stats_pitching_hits` | Float64 | Number of hits allowed by the pitcher in this game. |
| `stats_pitching_hit_by_pitch` | Float64 | Number of hit-by-pitch events while the pitcher was pitching in this game (alternate field for hit_batsmen). |
| `stats_pitching_at_bats` | Float64 | Number of at-bats faced by the pitcher (excluding walks, HBP, and sacrifices) in this game. |
| `stats_pitching_caught_stealing` | Float64 | Number of baserunners caught stealing while the pitcher was on the mound in this game. |
| `stats_pitching_stolen_bases` | Float64 | Number of stolen bases allowed while the pitcher was pitching in this game. |
| `stats_pitching_stolen_base_percentage` | String | Percentage of stolen base attempts that were successful while the pitcher was on the mound in this game. |
| `stats_pitching_number_of_pitches` | Float64 | Total number of pitches thrown by the pitcher in this game. |
| `stats_pitching_innings_pitched` | String | Total innings pitched by the pitcher in this game, expressed as a decimal (each out counts as one-third of an inning). |
| `stats_pitching_wins` | Float64 | Indicator of whether the pitcher was credited with the win in this game. |
| `stats_pitching_losses` | Float64 | Indicator of whether the pitcher was charged with the loss in this game. |
| `stats_pitching_saves` | Float64 | Indicator of whether the pitcher recorded a save in this game. |
| `stats_pitching_save_opportunities` | Float64 | Number of save opportunities the pitcher entered in this game. |
| `stats_pitching_holds` | Float64 | Number of holds recorded by the pitcher in this game. |
| `stats_pitching_blown_saves` | Float64 | Number of blown save opportunities for the pitcher in this game. |
| `stats_pitching_earned_runs` | Float64 | Number of earned runs allowed by the pitcher in this game. |
| `stats_pitching_batters_faced` | Float64 | Total number of batters faced by the pitcher in this game. |
| `stats_pitching_outs` | Float64 | Total number of outs recorded by the pitcher in this game. |
| `stats_pitching_games_pitched` | Float64 | Number of pitching appearances for the pitcher in this game (typically 1). |
| `stats_pitching_complete_games` | Float64 | Indicator of whether the pitcher threw a complete game in this appearance. |
| `stats_pitching_shutouts` | Float64 | Indicator of whether the pitcher recorded a complete-game shutout in this game. |
| `stats_pitching_pitches_thrown` | Float64 | Total pitches thrown by the pitcher in this game (may differ from number_of_pitches depending on tracking method). |
| `stats_pitching_balls` | Float64 | Number of ball calls recorded against the pitcher in this game. |
| `stats_pitching_strikes` | Float64 | Total number of strikes thrown by the pitcher in this game. |
| `stats_pitching_strike_percentage` | String | Percentage of all pitches thrown that were strikes in this game. |
| `stats_pitching_hit_batsmen` | Float64 | Number of batters hit by a pitch thrown by the pitcher in this game. |
| `stats_pitching_balks` | Float64 | Number of balks called against the pitcher in this game. |
| `stats_pitching_wild_pitches` | Float64 | Number of wild pitches thrown by the pitcher in this game. |
| `stats_pitching_pickoffs` | Float64 | Number of pickoffs executed by the pitcher in this game. |
| `stats_pitching_rbi` | Float64 | Number of RBI allowed (runs batted in by opposing batters off this pitcher) in this game. |
| `stats_pitching_games_finished` | Float64 | Indicator of whether the pitcher was the last pitcher used by their team in this game. |
| `stats_pitching_runs_scored_per9` | String | Total runs (including unearned) allowed per nine innings rate for the pitcher in this game. |
| `stats_pitching_home_runs_per9` | String | Home runs allowed per nine innings rate for the pitcher in this game. |
| `stats_pitching_inherited_runners` | Float64 | Number of baserunners already on base when the pitcher entered the game. |
| `stats_pitching_inherited_runners_scored` | Float64 | Number of inherited runners who scored while or after the pitcher was pitching in this game. |
| `stats_pitching_catchers_interference` | Float64 | Number of catcher's interference calls that occurred while the pitcher was pitching in this game. |
| `stats_pitching_sac_bunts` | Float64 | Number of sacrifice bunts allowed by the pitcher in this game. |
| `stats_pitching_sac_flies` | Float64 | Number of sacrifice flies allowed by the pitcher in this game. |
| `stats_pitching_passed_ball` | Float64 | Number of passed balls that occurred while the pitcher was pitching in this game. |
| `stats_pitching_pop_outs` | Float64 | Number of outs recorded by the pitcher on infield pop-ups in this game. |
| `stats_pitching_line_outs` | Float64 | Number of outs recorded by the pitcher on line drives caught in this game. |
| `stats_pitching_note` | String | Supplementary note or annotation attached to the pitcher's boxscore line (e.g., indicating a special circumstance). |
| `stats_batting_note` | String | Supplementary note or annotation attached to the batter's boxscore line (e.g., indicating a special circumstance). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_boxscore-example}

```python
mlb_boxscore(game_pk=716390)
```

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
| `num` | Int64 | Inning number. |
| `ordinal_num` | String | Inning ordinal label (e.g. 1st). |
| `home_runs` | Int64 | Home runs. |
| `home_hits` | Int64 | Home hits in the inning. |
| `home_errors` | Int64 | Home errors in the inning. |
| `home_left_on_base` | Int64 | Home runners left on base in the inning. |
| `away_runs` | Int64 | Away runs scored in the inning. |
| `away_hits` | Int64 | Away hits in the inning. |
| `away_errors` | Int64 | Away errors in the inning. |
| `away_left_on_base` | Int64 | Away runners left on base in the inning. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_linescore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_linescore-example}

```python
mlb_linescore(game_pk=716390)
```

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
| `pitch_index` | String | A serialized list of indices identifying individual pitch events within the at-bat for this win-probability row. |
| `action_index` | String | A serialized list of indices identifying action-type events (stolen bases, pickoffs, etc.) within the at-bat for this win-probability row. |
| `runner_index` | String | A serialized list of indices identifying baserunner movement events during the play in this win-probability row. |
| `runners` | String | A serialized representation of baserunner movement records for this play in the win-probability feed, capturing start/end base positions and event types. |
| `play_events` | String | A serialized representation of the sequence of pitch and action events comprising the at-bat for this win-probability row. |
| `credits` | String | A serialized list of player credit records for this play, linking fielding, pitching, or batting achievements to specific player identifiers. |
| `flags` | String | A serialized representation of situational boolean flags associated with this play (e.g., whether it was a big lead situation or a save opportunity). |
| `home_team_win_probability` | Float64 | Home team win probability (percent) entering the at-bat. |
| `away_team_win_probability` | Float64 | Away team win probability (percent) entering the at-bat. |
| `home_team_win_probability_added` | Float64 | Change in home team win probability attributed to the at-bat. |
| `play_end_time` | String | The ISO 8601 timestamp marking the conclusion of the play associated with this win-probability snapshot. |
| `at_bat_index` | Int64 | Zero-based index of the at-bat within the game. |
| `result_type` | String | The high-level category of the play result in the win-probability feed (e.g., 'atBat', 'action'). |
| `result_event` | String | The short categorical label for the play outcome in the win-probability feed (e.g., 'Single', 'Home Run', 'Strikeout'). |
| `result_event_type` | String | The snake-cased type identifier for the play outcome in the win-probability feed (e.g., 'single', 'home_run'). |
| `result_description` | String | A human-readable text description of the play result as reported in the win-probability game feed. |
| `result_rbi` | Int64 | The number of runs batted in credited to the batter for the play in this win-probability row. |
| `result_away_score` | Int64 | The away team's cumulative run total at the conclusion of the play in this win-probability row. |
| `result_home_score` | Int64 | The home team's cumulative run total at the conclusion of the play in this win-probability row. |
| `result_is_out` | Boolean | Boolean flag indicating whether the batter was retired on the play recorded in this win-probability row. |
| `about_at_bat_index` | Int64 | The sequential index of the at-bat within the game to which this win-probability observation belongs. |
| `about_half_inning` | String | Indicates whether the win-probability observation occurred in the top or bottom half of the inning. |
| `about_is_top_inning` | Boolean | Boolean flag indicating whether this win-probability observation occurred in the top half of the inning. |
| `about_inning` | Int64 | The inning number in which this win-probability observation was recorded. |
| `about_start_time` | String | The ISO 8601 timestamp marking the start of the play event within the win-probability game feed. |
| `about_end_time` | String | The ISO 8601 timestamp marking the end of the play event within the win-probability game feed. |
| `about_is_complete` | Boolean | Boolean flag indicating whether the at-bat associated with this win-probability row has concluded. |
| `about_is_scoring_play` | Boolean | Boolean flag indicating whether this play resulted in one or more runs being scored. |
| `about_has_review` | Boolean | Boolean flag indicating whether this play was subject to a manager's challenge or umpire review. |
| `about_has_out` | Boolean | Boolean flag indicating whether this play resulted in at least one out being recorded. |
| `about_captivating_index` | Int64 | A numeric score reflecting how compelling or exciting this plate appearance was, based on leverage and game-state context. |
| `count_balls` | Int64 | The ball count in the current at-bat at the time this win-probability snapshot was recorded. |
| `count_strikes` | Int64 | The strike count in the current at-bat at the time this win-probability snapshot was recorded. |
| `count_outs` | Int64 | The number of outs in the current half-inning at the time this win-probability snapshot was recorded. |
| `matchup_batter_id` | Int64 | The MLB Stats API (MLBAM) numeric identifier for the batter in this win-probability matchup row. |
| `matchup_batter_full_name` | String | The full name of the batter whose plate appearance generated this win-probability observation. |
| `matchup_batter_link` | String | The MLB Stats API relative URL linking to the batter's player resource for this win-probability row. |
| `matchup_bat_side_code` | String | A single-character code indicating the batter's handedness for this matchup in the win-probability feed (e.g., 'L', 'R', 'S'). |
| `matchup_bat_side_description` | String | The human-readable description of the batter's hitting side for this win-probability matchup row. |
| `matchup_pitcher_id` | Int64 | The MLB Stats API (MLBAM) numeric identifier for the pitcher in this win-probability matchup row. |
| `matchup_pitcher_full_name` | String | The full name of the pitcher who delivered pitches for this win-probability observation. |
| `matchup_pitcher_link` | String | The MLB Stats API relative URL linking to the pitcher's player resource for this win-probability row. |
| `matchup_pitch_hand_code` | String | A single-character code indicating the pitcher's throwing hand for this win-probability matchup (e.g., 'L' or 'R'). |
| `matchup_pitch_hand_description` | String | The human-readable description of the pitcher's throwing arm for this win-probability matchup row. |
| `matchup_post_on_first_id` | Float64 | The MLB Stats API (MLBAM) numeric identifier for the runner on first base after the play in this win-probability row. |
| `matchup_post_on_first_full_name` | String | The full name of the runner occupying first base at the conclusion of the play in this win-probability row. |
| `matchup_post_on_first_link` | String | The MLB Stats API relative URL linking to the player resource of the runner on first base after the play. |
| `matchup_batter_hot_cold_zones` | String | A serialized representation of the batter's hot and cold zone data applicable to this win-probability matchup. |
| `matchup_pitcher_hot_cold_zones` | String | A serialized representation of the pitcher's hot and cold zone data applicable to this win-probability matchup. |
| `matchup_splits_batter` | String | A string describing the batter's situational split for this win-probability matchup (e.g., 'vs. Right'). |
| `matchup_splits_pitcher` | String | A string describing the pitcher's situational split for this win-probability matchup (e.g., 'vs. Left'). |
| `matchup_splits_men_on_base` | String | A string describing the baserunner configuration applicable to the batter's situational split in this win-probability row. |
| `leverage_index` | Float64 | Leverage index quantifying the importance of the at-bat situation. |
| `drama_index` | Float64 | A numeric score quantifying the dramatic significance of this play within the game, based on win-probability swing and game leverage. |
| `matchup_post_on_second_id` | Float64 | The MLB Stats API (MLBAM) numeric identifier for the runner on second base after the play in this win-probability row. |
| `matchup_post_on_second_full_name` | String | The full name of the runner occupying second base at the conclusion of the play in this win-probability row. |
| `matchup_post_on_second_link` | String | The MLB Stats API relative URL linking to the player resource of the runner on second base after the play. |
| `matchup_post_on_third_id` | Float64 | The MLB Stats API (MLBAM) numeric identifier for the runner on third base after the play in this win-probability row. |
| `matchup_post_on_third_full_name` | String | The full name of the runner occupying third base at the conclusion of the play in this win-probability row. |
| `matchup_post_on_third_link` | String | The MLB Stats API relative URL linking to the player resource of the runner on third base after the play. |
| `review_details_is_overturned` | Boolean | Boolean flag indicating whether the original on-field ruling was reversed following the replay review for this play. |
| `review_details_in_progress` | Boolean | Boolean flag indicating whether a replay review of this play was still underway at the time of data capture. |
| `review_details_review_type` | String | The type of review mechanism applied to this play in the win-probability feed (e.g., 'managerChallenge', 'umpireReview'). |
| `review_details_challenge_team_id` | Float64 | The MLB Stats API numeric identifier for the team that initiated a replay challenge on this win-probability play. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_win_probability-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_win_probability-example}

```python
mlb_win_probability(game_pk=716390)
```

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
| `id` | Int64 | Id. |
| `full_name` | String | Player's full name. |
| `link` | String | API link to the game feed. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `primary_number` | String | Player uniform number. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `current_age` | Int64 | Current age in years. |
| `birth_city` | String | City of birth. |
| `birth_country` | String | Country of birth. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `active` | Boolean | Whether the player is currently active. |
| `use_name` | String | Preferred first name. |
| `use_last_name` | String | Preferred last name. |
| `boxscore_name` | String | Name as shown in box scores. |
| `nick_name` | String | Player nickname. |
| `gender` | String | Player gender. |
| `is_player` | Boolean | Whether the person is a player. |
| `is_verified` | Boolean | Whether the player profile is verified. |
| `pronunciation` | String | Phonetic name pronunciation. |
| `mlb_debut_date` | String | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | String | Name in first-last order. |
| `name_slug` | String | URL-friendly name slug. |
| `first_last_name` | String | First and last name. |
| `last_first_name` | String | Name in last, first order. |
| `last_init_name` | String | Last name with first initial. |
| `init_last_name` | String | First initial with last name. |
| `full_fml_name` | String | Full name (first-middle-last). |
| `full_lfm_name` | String | Full name (last-first-middle). |
| `strike_zone_top` | Float64 | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | Float64 | Bottom of the player's strike zone (feet). |
| `primary_position_code` | String | Primary position code. |
| `primary_position_name` | String | Primary fielding position name. |
| `primary_position_type` | String | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | String | Primary position abbreviation. |
| `bat_side_code` | String | Batting side code (L/R/S). |
| `bat_side_description` | String | Batting side description. |
| `pitch_hand_code` | String | Throwing hand code (L/R). |
| `pitch_hand_description` | String | Throwing hand description. |
| `birth_state_province` | String | State or province of birth. |
| `middle_name` | String | Player middle name. |
| `draft_year` | Float64 | Year the player was drafted. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_people-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_people-example}

```python
mlb_people()
```

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
| `id` | Int64 | Id. |
| `full_name` | String | Player's full name. |
| `link` | String | API link to the game feed. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `primary_number` | String | Player uniform number. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `current_age` | Int64 | Current age in years. |
| `birth_city` | String | City of birth. |
| `birth_country` | String | Country of birth. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `active` | Boolean | Whether the player is currently active. |
| `use_name` | String | Preferred first name. |
| `use_last_name` | String | Preferred last name. |
| `boxscore_name` | String | Name as shown in box scores. |
| `nick_name` | String | Player nickname. |
| `gender` | String | Player gender. |
| `is_player` | Boolean | Whether the person is a player. |
| `is_verified` | Boolean | Whether the player profile is verified. |
| `pronunciation` | String | Phonetic name pronunciation. |
| `mlb_debut_date` | String | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | String | Name in first-last order. |
| `name_slug` | String | URL-friendly name slug. |
| `first_last_name` | String | First and last name. |
| `last_first_name` | String | Name in last, first order. |
| `last_init_name` | String | Last name with first initial. |
| `init_last_name` | String | First initial with last name. |
| `full_fml_name` | String | Full name (first-middle-last). |
| `full_lfm_name` | String | Full name (last-first-middle). |
| `strike_zone_top` | Float64 | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | Float64 | Bottom of the player's strike zone (feet). |
| `primary_position_code` | String | Primary position code. |
| `primary_position_name` | String | Primary fielding position name. |
| `primary_position_type` | String | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | String | Primary position abbreviation. |
| `bat_side_code` | String | Batting side code (L/R/S). |
| `bat_side_description` | String | Batting side description. |
| `pitch_hand_code` | String | Throwing hand code (L/R). |
| `pitch_hand_description` | String | Throwing hand description. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_person-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_person-example}

```python
mlb_person(person_id=660271)
```

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
| `total_splits` | Float64 | Total number of splits in the leaderboard. |
| `exemptions` | String | Serialized list of statistical exemptions or special-case flags applied to this player's game-log splits. |
| `splits` | String | Splits. |
| `type_display_name` | String | Stat type display name. |
| `group_display_name` | String | Stat group display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_person_game_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_person_game_stats-example}

```python
mlb_person_game_stats(person_id=660271, game_pk=716390)
```

## mlb_sport_players

GET /api/v1/sports/{sportId}/players — every player in a sport for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}/players`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports/1/players](https://statsapi.mlb.com/api/v1/sports/1/players)

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
| `id` | Int64 | Id. |
| `full_name` | String | Player's full name. |
| `link` | String | API link to the game feed. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `primary_number` | String | Player uniform number. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `current_age` | Int64 | Current age in years. |
| `birth_city` | String | City of birth. |
| `birth_state_province` | String | State or province of birth. |
| `birth_country` | String | Country of birth. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `active` | Boolean | Whether the player is currently active. |
| `use_name` | String | Preferred first name. |
| `use_last_name` | String | Preferred last name. |
| `middle_name` | String | Player middle name. |
| `boxscore_name` | String | Name as shown in box scores. |
| `gender` | String | Player gender. |
| `is_player` | Boolean | Whether the person is a player. |
| `is_verified` | Boolean | Whether the player profile is verified. |
| `draft_year` | Float64 | Year the player was drafted. |
| `mlb_debut_date` | String | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | String | Name in first-last order. |
| `name_slug` | String | URL-friendly name slug. |
| `first_last_name` | String | First and last name. |
| `last_first_name` | String | Name in last, first order. |
| `last_init_name` | String | Last name with first initial. |
| `init_last_name` | String | First initial with last name. |
| `full_fml_name` | String | Full name (first-middle-last). |
| `full_lfm_name` | String | Full name (last-first-middle). |
| `strike_zone_top` | Float64 | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | Float64 | Bottom of the player's strike zone (feet). |
| `current_team_id` | Int64 | Current team MLBAM ID. |
| `current_team_name` | String | Current team name. |
| `current_team_link` | String | API link to the current team. |
| `primary_position_code` | String | Primary position code. |
| `primary_position_name` | String | Primary fielding position name. |
| `primary_position_type` | String | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | String | Primary position abbreviation. |
| `bat_side_code` | String | Batting side code (L/R/S). |
| `bat_side_description` | String | Batting side description. |
| `pitch_hand_code` | String | Throwing hand code (L/R). |
| `pitch_hand_description` | String | Throwing hand description. |
| `name_matrilineal` | String | Maternal family name. |
| `nick_name` | String | Player nickname. |
| `pronunciation` | String | Phonetic name pronunciation. |
| `last_played_date` | String | Date of last MLB game played. |
| `name_title` | String | Name title. |
| `name_suffix` | String | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_sport_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_sport_players-example}

```python
mlb_sport_players()
```

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
| `id` | Int64 | Id. |
| `code` | String | Fielder detail type code. |
| `link` | String | API link to the game feed. |
| `name` | String | Display name. |
| `abbreviation` | String | Short abbreviation. |
| `sort_order` | Int64 | Display sort order for the sport. |
| `active_status` | Boolean | Whether the sport/level is active. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_sports-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_sports-example}

```python
mlb_sports()
```

## mlb_leagues

GET /api/v1/leagues — list leagues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/leagues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagues?sportId=1](https://statsapi.mlb.com/api/v1/leagues?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |

### Returns {#mlb_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Numeric league id from the source API (PFF: 1 = NFL). |
| `name` | String | League display name as the source lists it (PFF: 'Pro Football'). |
| `link` | String | API link to the game feed. |
| `abbreviation` | String | League abbreviation as the source lists it (e.g. NFL). |
| `name_short` | String |  |
| `season_state` | String | A string describing the current phase of the league's season (e.g., 'inProgress', 'offseason', 'preseason'). |
| `has_wild_card` | Boolean | Boolean flag indicating whether this league includes a wild card playoff format for postseason eligibility. |
| `has_split_season` | Boolean | Boolean flag indicating whether this league divides its season into two halves with separate standings (as used historically in some minor leagues). |
| `num_games` | Float64 | The total number of regular-season games scheduled per team in this league for the given season. |
| `has_playoff_points` | Boolean | Boolean flag indicating whether this league uses a playoff points system to determine postseason seeding. |
| `num_teams` | Float64 | Number of teams the player appeared for. |
| `num_wildcard_teams` | Float64 | The number of wild card berths available for postseason entry in this league for the given season. |
| `season` | String | Season year. |
| `org_code` | String | The organizational code identifying the parent body (e.g., 'MLB') governing this league within the MLB Stats API hierarchy. |
| `conferences_in_use` | Boolean |  |
| `divisions_in_use` | Boolean |  |
| `sort_order` | Int64 | Display sort order for the sport. |
| `active` | Boolean | Whether the player is currently active. |
| `season_date_info_season_id` | String | Season identifier for the date info block. |
| `season_date_info_pre_season_start_date` | String | Preseason start date (YYYY-MM-DD). |
| `season_date_info_pre_season_end_date` | String | Preseason end date (YYYY-MM-DD). |
| `season_date_info_season_start_date` | String | Season start date (YYYY-MM-DD). |
| `season_date_info_spring_start_date` | String | Spring training start date (YYYY-MM-DD). |
| `season_date_info_spring_end_date` | String | Spring training end date (YYYY-MM-DD). |
| `season_date_info_regular_season_start_date` | String | Regular season start date (YYYY-MM-DD). |
| `season_date_info_last_date1st_half` | String | Last date of the first half (YYYY-MM-DD). |
| `season_date_info_all_star_date` | String | All-Star Game date (YYYY-MM-DD). |
| `season_date_info_first_date2nd_half` | String | First date of the second half (YYYY-MM-DD). |
| `season_date_info_regular_season_end_date` | String | Regular season end date (YYYY-MM-DD). |
| `season_date_info_post_season_start_date` | String | Postseason start date (YYYY-MM-DD). |
| `season_date_info_post_season_end_date` | String | Postseason end date (YYYY-MM-DD). |
| `season_date_info_season_end_date` | String | Season end date (YYYY-MM-DD). |
| `season_date_info_offseason_start_date` | String | Offseason start date (YYYY-MM-DD). |
| `season_date_info_off_season_end_date` | String | Offseason end date (YYYY-MM-DD). |
| `season_date_info_season_level_gameday_type` | String | Season-level Gameday data type code. |
| `season_date_info_game_level_gameday_type` | String | Game-level Gameday data type code. |
| `season_date_info_qualifier_plate_appearances` | Float64 | Plate appearances per game needed to qualify. |
| `season_date_info_qualifier_outs_pitched` | Float64 | Outs pitched per game needed to qualify. |
| `sport_id` | Float64 | Sport MLBAM ID. |
| `sport_link` | String | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_leagues-example}

```python
mlb_leagues()
```

## mlb_season

GET /api/v1/seasons/{seasonId} — single season detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/{season_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/X?sportId=1](https://statsapi.mlb.com/api/v1/seasons/X?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns {#mlb_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | String | stats.ncaa.org season identifier. |
| `has_wildcard` | Boolean | Whether the season has a wild card round. |
| `pre_season_start_date` | String | Pre-season start date. |
| `pre_season_end_date` | String | Pre-season end date. |
| `season_start_date` | String | Season start date. |
| `spring_start_date` | String | Spring training start date. |
| `spring_end_date` | String | Spring training end date. |
| `regular_season_start_date` | String | Regular season start date. |
| `last_date1st_half` | String | Last date of the first half. |
| `all_star_date` | String | All-Star Game date. |
| `first_date2nd_half` | String | First date of the second half. |
| `regular_season_end_date` | String | Regular season end date. |
| `post_season_start_date` | String | Post-season start date. |
| `post_season_end_date` | String | Post-season end date. |
| `season_end_date` | String | Season end date. |
| `offseason_start_date` | String | Off-season start date. |
| `off_season_end_date` | String | Off-season end date. |
| `season_level_gameday_type` | String | Season-level Gameday data feed type. |
| `game_level_gameday_type` | String | Game-level Gameday data feed type. |
| `qualifier_plate_appearances` | Float64 | Plate appearances per team game to qualify. |
| `qualifier_outs_pitched` | Float64 | Outs pitched per team game to qualify. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_season-example}

```python
mlb_season(season_id='X')
```

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
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `link` | String | API link to the game feed. |
| `active` | Boolean | Whether the player is currently active. |
| `season` | String | Season year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_venues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_venues-example}

```python
mlb_venues()
```

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
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `link` | String | API link to the game feed. |
| `active` | Boolean | Whether the player is currently active. |
| `season` | String | Season year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_venue-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_venue-example}

```python
mlb_venue(venue_id=15)
```
