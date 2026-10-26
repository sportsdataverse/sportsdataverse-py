---
title: Men's college basketball — KenPom (kenpom.com, subscription)
sidebar_label: KenPom (kenpom.com, subscription)
description: "Men's college basketball — KenPom (kenpom.com, subscription) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# Men's college basketball — KenPom (kenpom.com, subscription)

`sportsdataverse.mbb` — 30 endpoints.

## Team

| Function | Summary |
|---|---|
| [kenpom_team_stats](kenpom/team.md#kenpom_team_stats) | GET /teamstats.php - team shooting and style splits; side='o' for offense, 'd' for defense. Port of hoopR kp_teamstats(). |
| [kenpom_team](kenpom/team.md#kenpom_team) | GET /team.php - a team's full season page. Returns EVERY table on it, so one call covers hoopR's kp_team_schedule(), kp_team_players() and kp_team_lineups(), … |
| [kenpom_team_players_expanded](kenpom/team.md#kenpom_team_players_expanded) | GET /player-expanded.php - a team's expanded per-player table plus the minutes matrix. Covers hoopR's kp_team_player_stats() and kp_minutes_matrix() in one … |
| [kenpom_team_history](kenpom/team.md#kenpom_team_history) | GET /history.php?t= - a program's season-by-season history. Port of hoopR kp_team_history(). |

## Other

| Function | Summary |
|---|---|
| [kenpom_ratings](kenpom/other.md#kenpom_ratings) | GET /index.php - Pomeroy season ratings (AdjEM/AdjO/AdjD/AdjT plus SOS, one row per team). Port of hoopR kp_pomeroy_ratings(). |
| [kenpom_efficiency](kenpom/other.md#kenpom_efficiency) | GET /summary.php - efficiency and tempo summary (adjusted and raw O/D/T, average possession length). Port of hoopR kp_efficiency(). |
| [kenpom_four_factors](kenpom/other.md#kenpom_four_factors) | GET /stats.php - four-factors rankings (eFG%, TO%, OR%, FTRate on offense and defense). Port of hoopR kp_fourfactors(). |
| [kenpom_point_distribution](kenpom/other.md#kenpom_point_distribution) | GET /pointdist.php - share of points scored from 2s, 3s and free throws, offense and defense. Port of hoopR kp_pointdist(). |
| [kenpom_height](kenpom/other.md#kenpom_height) | GET /height.php - team height, effective height, experience, bench minutes and continuity. Port of hoopR kp_height(). |
| [kenpom_foul_trouble](kenpom/other.md#kenpom_foul_trouble) | GET /foul_trouble.php - team foul-trouble splits (minutes and efficiency with starters in foul trouble). Port of hoopR kp_foul_trouble(). |
| [kenpom_player_stats](kenpom/other.md#kenpom_player_stats) | GET /playerstats.php - national player leaderboard for one metric. Port of hoopR kp_playerstats(). |
| [kenpom_kpoy](kenpom/other.md#kenpom_kpoy) | GET /kpoy.php - KenPom Player of the Year standings and the game-MVP table. Port of hoopR kp_kpoy(). |
| [kenpom_game_plan](kenpom/other.md#kenpom_game_plan) | GET /gameplan.php - a team's game-plan page (per-game four factors and personnel splits). Port of hoopR kp_gameplan(). |
| [kenpom_opponent_tracker](kenpom/other.md#kenpom_opponent_tracker) | GET /opptracker.php - opponent tracker; side='o' for offense, 'd' for defense. Port of hoopR kp_opptracker(). |
| [kenpom_player_career](kenpom/other.md#kenpom_player_career) | GET /player.php - one player's career page (season-by-season stats and game log). Port of hoopR kp_player_career(). |
| [kenpom_box](kenpom/other.md#kenpom_box) | GET /box.php - box-score detail for one game (per-team four factors, player lines, scoring runs). Port of hoopR kp_box(). |
| [kenpom_win_probability](kenpom/other.md#kenpom_win_probability) | GET /winprob.php - in-game win-probability table for one game. Port of hoopR kp_winprob(). |
| [kenpom_fan_match](kenpom/other.md#kenpom_fan_match) | GET /fanmatch.php - the FanMatch slate for one date (predictions, thrill score, results). Port of hoopR kp_fanmatch(). |
| [kenpom_coach_history](kenpom/other.md#kenpom_coach_history) | GET /history.php?c= - a coach's season-by-season history. Port of hoopR kp_coach_history(). |
| [kenpom_program_ratings](kenpom/other.md#kenpom_program_ratings) | GET /programs.php - program-level ratings across the full KenPom era. Port of hoopR kp_program_ratings(). |
| [kenpom_archive_ratings](kenpom/other.md#kenpom_archive_ratings) | GET /archive.php - the Pomeroy ratings as they stood on a past date. Port of hoopR kp_pomeroy_archive_ratings(). |
| [kenpom_conference](kenpom/other.md#kenpom_conference) | GET /conf.php - one conference's season page (standings, efficiency, per-team splits). Port of hoopR kp_conf(). |
| [kenpom_conference_stats](kenpom/other.md#kenpom_conference_stats) | GET /confstats.php - league-wide conference comparison for one season. Port of hoopR kp_confstats(). |
| [kenpom_conference_history](kenpom/other.md#kenpom_conference_history) | GET /confhistory.php - one conference's season-by-season history. Port of hoopR kp_confhistory(). |
| [kenpom_trends](kenpom/other.md#kenpom_trends) | GET /trends.php - national Division I trends by season (tempo, efficiency, shooting, fouls). Port of hoopR kp_trends(). |
| [kenpom_home_court_advantage](kenpom/other.md#kenpom_home_court_advantage) | GET /hca.php - per-team home-court advantage estimates. Port of hoopR kp_hca(). |
| [kenpom_arenas](kenpom/other.md#kenpom_arenas) | GET /arenas.php - arena reference (name, capacity, average attendance) by team. Port of hoopR kp_arenas(). |
| [kenpom_officials](kenpom/other.md#kenpom_officials) | GET /officials.php - referee ratings for one season. Port of hoopR kp_officials(). |
| [kenpom_referee](kenpom/other.md#kenpom_referee) | GET /referee.php - one referee's game log and splits for a season. Port of hoopR kp_referee(). |
| [kenpom_game_attributes](kenpom/other.md#kenpom_game_attributes) | GET /game_attrs.php - season game leaderboards by attribute (thrill score, comebacks, upsets, ...). Port of hoopR kp_game_attrs(). |
