---
title: SIHF (Swiss hockey)
sidebar_label: SIHF (Swiss hockey)
description: "sdv-py SIHF (Swiss hockey): endpoint references, dataset loaders and parsers for SIHF (Swiss hockey) in the SportsDataverse Python package."
---
# SIHF (Swiss hockey) (`sportsdataverse.hockey.sihf`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [SIHF statistics API](#sihf-statistics-api) | `data.sihf.ch` | 33 | none |

## SIHF statistics API {#sihf-statistics-api}

| Reference | Functions |
|---|---:|
| [SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league)](reference/sihf_api_web) | 33 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.sihf` (Python) | `fastRhockey` (R) |
|---|---|
| [`sihf_game_goalie_stats`](reference/sihf_api_web/game#sihf_game_goalie_stats) | [`sihf_game_goalie_stats`](https://fastRhockey.sportsdataverse.org/reference/sihf_game_goalie_stats.html) |
| [`sihf_game_info`](reference/sihf_api_web/game#sihf_game_info) | [`sihf_game_info`](https://fastRhockey.sportsdataverse.org/reference/sihf_game_info.html) |
| [`sihf_game_lineups`](reference/sihf_api_web/game#sihf_game_lineups) | [`sihf_game_lineups`](https://fastRhockey.sportsdataverse.org/reference/sihf_game_lineups.html) |
| [`sihf_game_player_stats`](reference/sihf_api_web/game#sihf_game_player_stats) | [`sihf_game_player_stats`](https://fastRhockey.sportsdataverse.org/reference/sihf_game_player_stats.html) |
| [`sihf_game_team_stats`](reference/sihf_api_web/game#sihf_game_team_stats) | [`sihf_game_team_stats`](https://fastRhockey.sportsdataverse.org/reference/sihf_game_team_stats.html) |
| [`sihf_goalie_shootouts`](reference/sihf_api_web/goalie#sihf_goalie_shootouts) | [`sihf_goalie_shootouts`](https://fastRhockey.sportsdataverse.org/reference/sihf_goalie_shootouts.html) |
| [`sihf_goalie_shots`](reference/sihf_api_web/goalie#sihf_goalie_shots) | [`sihf_goalie_shots`](https://fastRhockey.sportsdataverse.org/reference/sihf_goalie_shots.html) |
| [`sihf_goalie_stats`](reference/sihf_api_web/goalie#sihf_goalie_stats) | [`sihf_goalie_stats`](https://fastRhockey.sportsdataverse.org/reference/sihf_goalie_stats.html) |
| [`sihf_goalie_wins`](reference/sihf_api_web/goalie#sihf_goalie_wins) | [`sihf_goalie_wins`](https://fastRhockey.sportsdataverse.org/reference/sihf_goalie_wins.html) |
| [`sihf_pbp`](reference/sihf_api_web/other#sihf_pbp) | [`sihf_pbp`](https://fastRhockey.sportsdataverse.org/reference/sihf_pbp.html) |
| [`sihf_phases`](reference/sihf_api_web/other#sihf_phases) | [`sihf_phases`](https://fastRhockey.sportsdataverse.org/reference/sihf_phases.html) |
| [`sihf_player_faceoffs`](reference/sihf_api_web/player#sihf_player_faceoffs) | [`sihf_player_faceoffs`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_faceoffs.html) |
| [`sihf_player_faceoffs_by_zone`](reference/sihf_api_web/player#sihf_player_faceoffs_by_zone) | [`sihf_player_faceoffs_by_zone`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_faceoffs_by_zone.html) |
| [`sihf_player_faceoffs_by_zone_per_game`](reference/sihf_api_web/player#sihf_player_faceoffs_by_zone_per_game) | [`sihf_player_faceoffs_by_zone_per_game`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_faceoffs_by_zone_per_game.html) |
| [`sihf_player_goals_assists`](reference/sihf_api_web/player#sihf_player_goals_assists) | [`sihf_player_goals_assists`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_goals_assists.html) |
| [`sihf_player_penalties`](reference/sihf_api_web/player#sihf_player_penalties) | [`sihf_player_penalties`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_penalties.html) |
| [`sihf_player_shootouts`](reference/sihf_api_web/player#sihf_player_shootouts) | [`sihf_player_shootouts`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_shootouts.html) |
| [`sihf_player_shots`](reference/sihf_api_web/player#sihf_player_shots) | [`sihf_player_shots`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_shots.html) |
| [`sihf_player_stats`](reference/sihf_api_web/player#sihf_player_stats) | [`sihf_player_stats`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_stats.html) |
| [`sihf_player_time_on_ice`](reference/sihf_api_web/player#sihf_player_time_on_ice) | [`sihf_player_time_on_ice`](https://fastRhockey.sportsdataverse.org/reference/sihf_player_time_on_ice.html) |
| [`sihf_results`](reference/sihf_api_web/other#sihf_results) | [`sihf_results`](https://fastRhockey.sportsdataverse.org/reference/sihf_results.html) |
| [`sihf_standings`](reference/sihf_api_web/other#sihf_standings) | [`sihf_standings`](https://fastRhockey.sportsdataverse.org/reference/sihf_standings.html) |
| [`sihf_team_attendance`](reference/sihf_api_web/team#sihf_team_attendance) | [`sihf_team_attendance`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_attendance.html) |
| [`sihf_team_faceoffs`](reference/sihf_api_web/team#sihf_team_faceoffs) | [`sihf_team_faceoffs`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_faceoffs.html) |
| [`sihf_team_faceoffs_by_zone`](reference/sihf_api_web/team#sihf_team_faceoffs_by_zone) | [`sihf_team_faceoffs_by_zone`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_faceoffs_by_zone.html) |
| [`sihf_team_faceoffs_by_zone_per_game`](reference/sihf_api_web/team#sihf_team_faceoffs_by_zone_per_game) | [`sihf_team_faceoffs_by_zone_per_game`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_faceoffs_by_zone_per_game.html) |
| [`sihf_team_goals`](reference/sihf_api_web/team#sihf_team_goals) | [`sihf_team_goals`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_goals.html) |
| [`sihf_team_goals_by_position`](reference/sihf_api_web/team#sihf_team_goals_by_position) | [`sihf_team_goals_by_position`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_goals_by_position.html) |
| [`sihf_team_penalties`](reference/sihf_api_web/team#sihf_team_penalties) | [`sihf_team_penalties`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_penalties.html) |
| [`sihf_team_penalty_kill`](reference/sihf_api_web/team#sihf_team_penalty_kill) | [`sihf_team_penalty_kill`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_penalty_kill.html) |
| [`sihf_team_power_play`](reference/sihf_api_web/team#sihf_team_power_play) | [`sihf_team_power_play`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_power_play.html) |
| [`sihf_team_shootouts`](reference/sihf_api_web/team#sihf_team_shootouts) | [`sihf_team_shootouts`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_shootouts.html) |
| [`sihf_team_shots`](reference/sihf_api_web/team#sihf_team_shots) | [`sihf_team_shots`](https://fastRhockey.sportsdataverse.org/reference/sihf_team_shots.html) |
