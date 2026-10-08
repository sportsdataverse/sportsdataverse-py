---
title: PWHL — additional Python functions
sidebar_label: Additional functions
description: "PWHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# PWHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.pwhl`
not covered by the generated API-endpoint reference above.

## sportsdataverse-data releases

| Function | Summary |
|---|---|
| [load_pwhl_games](additional/sportsdataverse-data-releases.md#load_pwhl_games) | Load the PWHL games-in-data-repo manifest (no `seasons` argument). |
| [load_pwhl_goalie_box](additional/sportsdataverse-data-releases.md#load_pwhl_goalie_box) | Alias of load_pwhl_goalie_boxscores() for naming parity with fastRhockey (R). |
| [load_pwhl_player_box](additional/sportsdataverse-data-releases.md#load_pwhl_player_box) | Alias of load_pwhl_player_boxscores() for naming parity with fastRhockey (R). |
| [load_pwhl_schedule](additional/sportsdataverse-data-releases.md#load_pwhl_schedule) | Alias of load_pwhl_schedules() for naming parity with fastRhockey (R). |
| [load_pwhl_skater_box](additional/sportsdataverse-data-releases.md#load_pwhl_skater_box) | Alias of load_pwhl_skater_boxscores() for naming parity with fastRhockey (R). |
| [load_pwhl_team_box](additional/sportsdataverse-data-releases.md#load_pwhl_team_box) | Alias of load_pwhl_team_boxscores() for naming parity with fastRhockey (R). |

## HockeyTech / LeagueStat

| Function | Summary |
|---|---|
| [pwhl_game_corsi](additional/hockeytech-leaguestat.md#pwhl_game_corsi) | Player-level on-ice Corsi and Fenwick for a single PWHL game. |
| [pwhl_game_info](additional/hockeytech-leaguestat.md#pwhl_game_info) | PWHL single-game metadata. |
| [pwhl_game_shifts](additional/hockeytech-leaguestat.md#pwhl_game_shifts) | Parsed shift stints for a single PWHL game. |
| [pwhl_game_summary](additional/hockeytech-leaguestat.md#pwhl_game_summary) | PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars). |
| [pwhl_leaders](additional/hockeytech-leaguestat.md#pwhl_leaders) | PWHL statistical leaders for a given season. |
| [pwhl_pbp](additional/hockeytech-leaguestat.md#pwhl_pbp) | PWHL play-by-play — one row per event, fully enriched. |
| [pwhl_player_box](additional/hockeytech-leaguestat.md#pwhl_player_box) | PWHL player box score for a single game. |
| [pwhl_player_game_log](additional/hockeytech-leaguestat.md#pwhl_player_game_log) | PWHL player game-by-game log. |
| [pwhl_player_info](additional/hockeytech-leaguestat.md#pwhl_player_info) | PWHL player biographical info. |
| [pwhl_player_search](additional/hockeytech-leaguestat.md#pwhl_player_search) | Search for PWHL players by name. |
| [pwhl_player_stats](additional/hockeytech-leaguestat.md#pwhl_player_stats) | PWHL player season stats across all seasons. |
| [pwhl_player_toi](additional/hockeytech-leaguestat.md#pwhl_player_toi) | Per-player time-on-ice totals for a single PWHL game. |
| [pwhl_playoff_bracket](additional/hockeytech-leaguestat.md#pwhl_playoff_bracket) | PWHL playoff bracket for a given season. |
| [pwhl_schedule](additional/hockeytech-leaguestat.md#pwhl_schedule) | PWHL schedule — one row per game of one season (matches fastRhockey `pwhl_schedule`). |
| [pwhl_scorebar](additional/hockeytech-leaguestat.md#pwhl_scorebar) | PWHL live scorebar (today ± 3 days). |
| [pwhl_skater_rapm](additional/hockeytech-leaguestat.md#pwhl_skater_rapm) | ② PWHL skater xG RAPM -- shim over `nhl_skater_rapm` with `league='pwhl'`. |
| [pwhl_skater_war](additional/hockeytech-leaguestat.md#pwhl_skater_war) | ③ PWHL GAR/WAR composite -- shim over `nhl_skater_war` with `league='pwhl'`. |
| [pwhl_special_teams_value](additional/hockeytech-leaguestat.md#pwhl_special_teams_value) | ⑥ PWHL special-teams value -- shim over `nhl_special_teams_value` with `league='pwhl'`. |
| [pwhl_standings](additional/hockeytech-leaguestat.md#pwhl_standings) | PWHL standings — one row per team. |
| [pwhl_stats](additional/hockeytech-leaguestat.md#pwhl_stats) | PWHL aggregate stats by season and position. |
| [pwhl_streaks](additional/hockeytech-leaguestat.md#pwhl_streaks) | Current PWHL player/team streaks — **non-functional: no such upstream view**. |
| [pwhl_team_roster](additional/hockeytech-leaguestat.md#pwhl_team_roster) | PWHL team roster for a given team + season. |
| [pwhl_teams](additional/hockeytech-leaguestat.md#pwhl_teams) | PWHL teams for a given season. |
| [pwhl_transactions](additional/hockeytech-leaguestat.md#pwhl_transactions) | PWHL roster transactions. |
| [pwhl_unit_ratings](additional/hockeytech-leaguestat.md#pwhl_unit_ratings) | ⑤ PWHL line/pair ratings -- shim over `nhl_unit_ratings` with `league='pwhl'`. |

## Models and calculators

| Function | Summary |
|---|---|
| [LeagueConstants](additional/models-and-calculators.md#LeagueConstants) | Fitted, league-specific constants for the NHL/PWHL prediction spine. |
| [as_of_ratings_split](additional/models-and-calculators.md#as_of_ratings_split) | Filter a frame to rows strictly before `cutoff_date` (the leakage boundary). |
| [brier_score](additional/models-and-calculators.md#brier_score) | Mean squared error between predicted probabilities and binary outcomes. |
| [calibration_table](additional/models-and-calculators.md#calibration_table) | Bucket predicted probabilities into bins and compare to actual outcome rates. |
| [log_loss_score](additional/models-and-calculators.md#log_loss_score) | Binary cross-entropy loss between predicted probabilities and outcomes. |
| [mae](additional/models-and-calculators.md#mae) | Mean absolute error between two arrays. |
| [pwhl_team_ratings](additional/models-and-calculators.md#pwhl_team_ratings) | PWHL opponent-adjusted, shrunk even-strength xG team ratings. |
| [spearman_corr](additional/models-and-calculators.md#spearman_corr) | Spearman rank correlation between two arrays. |

## Analytics

| Function | Summary |
|---|---|
| [pwhl_game_total](additional/analytics.md#pwhl_game_total) | PWHL per-game expected total goals (re-export of the expected-goals helper). |
| [pwhl_in_game_win_prob](additional/analytics.md#pwhl_in_game_win_prob) | PWHL per-play live home win probability from the bundled in-game model. |
| [pwhl_player_props](additional/analytics.md#pwhl_player_props) | PWHL empirical-Bayes shots/points player-prop projections. |
| [pwhl_predict_games](additional/analytics.md#pwhl_predict_games) | PWHL vectorized pregame margin/win-prob/total (+ market edge). |

## Dates and seasons

| Function | Summary |
|---|---|
| [most_recent_pwhl_season](additional/dates-and-seasons.md#most_recent_pwhl_season) | Newest PWHL regular season as an end-year integer. |
| [pwhl_season_id](additional/dates-and-seasons.md#pwhl_season_id) | All PWHL seasons with end-year + game-type labels (HockeyTech `seasons`). |
