---
title: NHL — additional Python functions
sidebar_label: Additional functions
description: "NHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# NHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nhl`
not covered by the generated API-endpoint reference above.

## ESPN

| Function | Summary |
|---|---|
| [espn_nhl_game_rosters](additional/espn.md#espn_nhl_game_rosters) | espn_nhl_game_rosters() - Pull the game by id. |
| [espn_nhl_pbp](additional/espn.md#espn_nhl_pbp) | espn_nhl_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary` |
| [espn_nhl_player_stats](additional/espn.md#espn_nhl_player_stats) | Pull an NHL athlete's ESPN **season** stat line as one wide row. |
| [espn_nhl_schedule](additional/espn.md#espn_nhl_schedule) | espn_nhl_schedule - look up the NHL schedule for a given date |
| [espn_nhl_teams](additional/espn.md#espn_nhl_teams) | espn_nhl_teams - look up NHL teams |
| [scoreboard_event_parsing](additional/espn.md#scoreboard_event_parsing) | Flatten one ESPN scoreboard event for the schedule frame, in place. |

## sportsdataverse-data releases

| Function | Summary |
|---|---|
| [load_nhl_games](additional/sportsdataverse-data-releases.md#load_nhl_games) | Load the NHL games-in-data-repo manifest (no `seasons` argument). |
| [load_nhl_goalie_box](additional/sportsdataverse-data-releases.md#load_nhl_goalie_box) | Alias of load_nhl_goalie_boxscores() for naming parity with fastRhockey (R). |
| [load_nhl_player_box](additional/sportsdataverse-data-releases.md#load_nhl_player_box) | Alias of load_nhl_player_boxscore() for naming parity with fastRhockey (R). |
| [load_nhl_skater_box](additional/sportsdataverse-data-releases.md#load_nhl_skater_box) | Alias of load_nhl_skater_boxscores() for naming parity with fastRhockey (R). |
| [load_nhl_team_box](additional/sportsdataverse-data-releases.md#load_nhl_team_box) | Alias of load_nhl_team_boxscore() for naming parity with fastRhockey (R). |

## NHL Web API

| Function | Summary |
|---|---|
| [nhl_scoreboard](additional/nhl-web-api.md#nhl_scoreboard) | In-game scoreboard payload (renamed from `nhl_web_scoreboard`). |

## NHL Records

| Function | Summary |
|---|---|
| [nhl_records_coach_milestone_wins](additional/nhl-records.md#nhl_records_coach_milestone_wins) | Coaches who reached a wins milestone in fewest games. |
| [nhl_records_comeback_wins](additional/nhl-records.md#nhl_records_comeback_wins) | Comeback wins from a multi-goal deficit. |
| [nhl_records_consecutive_goal_seasons](additional/nhl-records.md#nhl_records_consecutive_goal_seasons) | Skaters with the most consecutive N-goal seasons. |
| [nhl_records_fastest_goals](additional/nhl-records.md#nhl_records_fastest_goals) | Fastest N goals by one team in a single game. |
| [nhl_records_fastest_goals_both_teams](additional/nhl-records.md#nhl_records_fastest_goals_both_teams) | Fastest N goals combined (both teams) in a single game. |
| [nhl_records_games_played_streak_skaters](additional/nhl-records.md#nhl_records_games_played_streak_skaters) | Consecutive games-played streaks for skaters. |

## Fox Sports API

| Function | Summary |
|---|---|
| [fox_nhl_boxscore](additional/fox-sports-api.md#fox_nhl_boxscore) | NHL boxscore (long: one row per player-stat). |
| [fox_nhl_event_matchup](additional/fox-sports-api.md#fox_nhl_event_matchup) | Fox Sports nhl pregame team-stat comparison (one row per stat). |
| [fox_nhl_event_recap](additional/fox-sports-api.md#fox_nhl_event_recap) | Fox Sports nhl postgame top performers (one row per player). |
| [fox_nhl_event_standings](additional/fox-sports-api.md#fox_nhl_event_standings) | Fox Sports nhl the two teams' standings context. |
| [fox_nhl_league_conferences](additional/fox-sports-api.md#fox_nhl_league_conferences) | Fox Sports nhl conference / group directory. |
| [fox_nhl_league_header](additional/fox-sports-api.md#fox_nhl_league_header) | Fox Sports nhl league header (one row). |
| [fox_nhl_league_leaders](additional/fox-sports-api.md#fox_nhl_league_leaders) | NHL statistical leaders (`stats-con`); who=player\|team. |
| [fox_nhl_league_odds](additional/fox-sports-api.md#fox_nhl_league_odds) | Fox Sports nhl league odds board (one row per team per game). |
| [fox_nhl_league_player_news](additional/fox-sports-api.md#fox_nhl_league_player_news) | Fox Sports nhl league-wide player news feed. |
| [fox_nhl_league_polls](additional/fox-sports-api.md#fox_nhl_league_polls) | Fox Sports nhl rankings / polls rendered as standings tables. |
| [fox_nhl_league_schedule](additional/fox-sports-api.md#fox_nhl_league_schedule) | Fox Sports nhl league schedule nav selections. |
| [fox_nhl_league_scores](additional/fox-sports-api.md#fox_nhl_league_scores) | Fox Sports nhl league scores nav selections. |
| [fox_nhl_league_standings](additional/fox-sports-api.md#fox_nhl_league_standings) | Fox Sports nhl league-wide standings tables. |
| [fox_nhl_league_stat_leaders](additional/fox-sports-api.md#fox_nhl_league_stat_leaders) | Fox Sports nhl league stats landing leaders. |
| [fox_nhl_odds](additional/fox-sports-api.md#fox_nhl_odds) | NHL game odds six-pack (spread / to-win / total per team). |
| [fox_nhl_pbp](additional/fox-sports-api.md#fox_nhl_pbp) | NHL play-by-play (one row per play; period-based). |
| [fox_nhl_scoreboard](additional/fox-sports-api.md#fox_nhl_scoreboard) | Fox Sports nhl scoreboard nav selections (weeks / dates / groups). |
| [fox_nhl_scorechip](additional/fox-sports-api.md#fox_nhl_scorechip) | Fox Sports nhl compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_nhl_scores_segment](additional/fox-sports-api.md#fox_nhl_scores_segment) | Fox Sports nhl one row per game in a scoreboard segment. |
| [fox_nhl_standings](additional/fox-sports-api.md#fox_nhl_standings) | NHL standings for a team's conference/division. |
| [fox_nhl_team_gamelog](additional/fox-sports-api.md#fox_nhl_team_gamelog) | NHL team game log (long: one row per game-stat). |
| [fox_nhl_team_header](additional/fox-sports-api.md#fox_nhl_team_header) | Fox Sports nhl team header (one row). |
| [fox_nhl_team_roster](additional/fox-sports-api.md#fox_nhl_team_roster) | NHL team roster (one row per player). |
| [fox_nhl_team_stats](additional/fox-sports-api.md#fox_nhl_team_stats) | NHL team stat leaders by category. |
| [fox_nhl_teamnav](additional/fox-sports-api.md#fox_nhl_teamnav) | Fox Sports nhl team directory (one row per team). |

## Play-by-play processing

| Function | Summary |
|---|---|
| [nhl_pbp_disk](additional/play-by-play-processing.md#nhl_pbp_disk) | Read a saved ESPN NHL play-by-play payload from disk. |

## Models and calculators

| Function | Summary |
|---|---|
| [ImpactConfig](additional/models-and-calculators.md#ImpactConfig) | League-specific constants consumed by every player-impact engine function. |
| [LeagueConstants](additional/models-and-calculators.md#LeagueConstants) | Fitted, league-specific constants for the NHL/PWHL prediction spine. |
| [add_shot_geometry](additional/models-and-calculators.md#add_shot_geometry) | Attach `distance_to_net` / `shot_angle` / `shot_danger` (descriptive output only). |
| [adjust_rate_opponent](additional/models-and-calculators.md#adjust_rate_opponent) | Opponent-adjust a per-game for/against rate by iterative fixed-point, then shrink. |
| [as_of_ratings_split](additional/models-and-calculators.md#as_of_ratings_split) | Filter a frame to rows strictly before `cutoff_date` (the leakage boundary). |
| [booster_cache_dir](additional/models-and-calculators.md#booster_cache_dir) | Resolve the local cache directory for the downloaded `nhl_xg_models` boosters. |
| [brier_score](additional/models-and-calculators.md#brier_score) | Mean squared error between predicted probabilities and binary outcomes. |
| [build_design](additional/models-and-calculators.md#build_design) | Build the sparse RAPM design matrix -- two rows per stint (one per attacking team). |
| [build_stints](additional/models-and-calculators.md#build_stints) | Fold `load_nhl_shifts` CHANGE events into contiguous constant-personnel intervals. |
| [calibration_table](additional/models-and-calculators.md#calibration_table) | Bucket predicted probabilities into bins and compare to actual outcome rates. |
| [ensure_xg_models](additional/models-and-calculators.md#ensure_xg_models) | Return a dir holding the 3 published booster files, downloading any missing ones. |
| [get_constants](additional/models-and-calculators.md#get_constants) | Resolve the fitted-constants row for a league. |
| [load_xg_models](additional/models-and-calculators.md#load_xg_models) | Load the two published boosters (+ embedded feature names) and the penalty-shot constant. |
| [log_loss_score](additional/models-and-calculators.md#log_loss_score) | Binary cross-entropy loss between predicted probabilities and outcomes. |
| [mae](additional/models-and-calculators.md#mae) | Mean absolute error between two arrays. |
| [nhl_expected_assists](additional/models-and-calculators.md#nhl_expected_assists) | Per-player expected primary/secondary assists from xG-weighted goal credit. |
| [nhl_goalie_gsax](additional/models-and-calculators.md#nhl_goalie_gsax) | Per-goalie goals-saved-above-expected (GSAx) for the games in `pbp`. |
| [nhl_skater_rapm](additional/models-and-calculators.md#nhl_skater_rapm) | Per-skater xG-based Regularized Adjusted Plus-Minus (RAPM), per 60 minutes. |
| [nhl_skater_war](additional/models-and-calculators.md#nhl_skater_war) | Per-skater GAR/WAR composite -- EV + special-teams + faceoffs + penalties. |
| [nhl_team_ratings](additional/models-and-calculators.md#nhl_team_ratings) | Opponent-adjusted, shrunk even-strength xG (+ goal) team ratings. |
| [nhl_unit_ratings](additional/models-and-calculators.md#nhl_unit_ratings) | Per on-ice skater combination: observed xGF/xGA + shrinkage-blended summed RAPM. |
| [nhl_xg](additional/models-and-calculators.md#nhl_xg) | Score every unblocked shot in `pbp` with the published `nhl_xg_models` boosters. |
| [prepare_xg_features](additional/models-and-calculators.md#prepare_xg_features) | Port of `helper_nhl_prepare_xg_data` -- one row per unblocked shot, model features. |
| [spearman_corr](additional/models-and-calculators.md#spearman_corr) | Spearman rank correlation between two arrays. |
| [team_fullname_to_abbr](additional/models-and-calculators.md#team_fullname_to_abbr) | Map an NHL full team display name to its abbreviation, or `None` if unknown. |
| [team_game_xg_rates](additional/models-and-calculators.md#team_game_xg_rates) | Per-(game, team) even-strength xG-for/against + realized goals. |
| [weighted_ridge](additional/models-and-calculators.md#weighted_ridge) | Solve the weighted ridge normal equations `(X'WX + lam*I)^-1 X'Wy`. |

## Analytics

| Function | Summary |
|---|---|
| [expected_goals](additional/analytics.md#expected_goals) | Per-team expected goals, blending own offense with opponent defense. |
| [in_game_features](additional/analytics.md#in_game_features) | Per-play in-game win-probability features from game state. |
| [nhl_edge_skating_value](additional/analytics.md#nhl_edge_skating_value) | Per-skater EDGE skating-value composite (z-score or percentile blend). |
| [nhl_faceoff_value](additional/analytics.md#nhl_faceoff_value) | Per-player context-adjusted faceoff-win value. |
| [nhl_game_total](additional/analytics.md#nhl_game_total) | Per-game expected total goals -- a thin re-export of model ②'s expected-goals helper. |
| [nhl_in_game_win_prob](additional/analytics.md#nhl_in_game_win_prob) | Per-play live home win probability from the bundled in-game logistic. |
| [nhl_penalty_value](additional/analytics.md#nhl_penalty_value) | Per-player net penalty drawn/taken value. |
| [nhl_player_props](additional/analytics.md#nhl_player_props) | Empirical-Bayes shots/points player-prop projections. |
| [nhl_predict_games](additional/analytics.md#nhl_predict_games) | Vectorized pregame margin/win-prob/total (+ market edge) over a schedule. |
| [nhl_special_teams_value](additional/analytics.md#nhl_special_teams_value) | Per-skater power-play/penalty-kill value (goals) above/below league baseline. |
| [nhl_zone_transitions](additional/analytics.md#nhl_zone_transitions) | Per-player controlled/dump entry & exit rates + xG-weighted values. |
| [predict_margin](additional/analytics.md#predict_margin) | Expected home-minus-away goal margin. |
| [predict_total](additional/analytics.md#predict_total) | Expected total goals, variance-corrected by the fitted `total_scale`. |
| [win_prob_from_margin](additional/analytics.md#win_prob_from_margin) | Convert an expected goal margin to a home win probability via Phi(margin/sigma). |

## Dates and seasons

| Function | Summary |
|---|---|
| [most_recent_nhl_season](additional/dates-and-seasons.md#most_recent_nhl_season) | most_recent_nhl_season - return the season year for "today". |
| [year_to_season](additional/dates-and-seasons.md#year_to_season) | year_to_season - format a starting year as the canonical `YYYY-YY` season string. |
