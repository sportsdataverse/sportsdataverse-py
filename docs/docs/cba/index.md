---
title: CBA
sidebar_label: CBA
description: "sdv-py CBA: endpoint references, dataset loaders and parsers for CBA in the SportsDataverse Python package."
---
# CBA (`sportsdataverse.basketball.cba`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [CBA data API (China)](#cba-data-api-china) | `data-server.cbaleague.com` | 27 | none |

## CBA data API (China) {#cba-data-api-china}

| Reference | Functions |
|---|---:|
| [CBA data API (data-server.cbaleague.com, China; encrypted bodies)](reference/cba) | 27 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.cba` (Python) | `hoopR` (R) |
|---|---|
| [`cba_best_player`](reference/cba/other#cba_best_player) | [`cba_best_player`](https://hoopR.sportsdataverse.org/reference/cba_best_player.html) |
| [`cba_best_stats`](reference/cba/other#cba_best_stats) | [`cba_best_stats`](https://hoopR.sportsdataverse.org/reference/cba_best_stats.html) |
| [`cba_game_info`](reference/cba/game#cba_game_info) | [`cba_game_info`](https://hoopR.sportsdataverse.org/reference/cba_game_info.html) |
| [`cba_game_team_four`](reference/cba/game#cba_game_team_four) | [`cba_game_team_four`](https://hoopR.sportsdataverse.org/reference/cba_game_team_four.html) |
| [`cba_game_team_shots`](reference/cba/game#cba_game_team_shots) | [`cba_game_team_shots`](https://hoopR.sportsdataverse.org/reference/cba_game_team_shots.html) |
| [`cba_match_types`](reference/cba/other#cba_match_types) | [`cba_match_types`](https://hoopR.sportsdataverse.org/reference/cba_match_types.html) |
| [`cba_pbp`](reference/cba/other#cba_pbp) | [`cba_pbp`](https://hoopR.sportsdataverse.org/reference/cba_pbp.html) |
| [`cba_period_scores`](reference/cba/other#cba_period_scores) | [`cba_period_scores`](https://hoopR.sportsdataverse.org/reference/cba_period_scores.html) |
| [`cba_player_advanced`](reference/cba/player#cba_player_advanced) | [`cba_player_advanced`](https://hoopR.sportsdataverse.org/reference/cba_player_advanced.html) |
| [`cba_player_base`](reference/cba/player#cba_player_base) | [`cba_player_base`](https://hoopR.sportsdataverse.org/reference/cba_player_base.html) |
| [`cba_player_box`](reference/cba/player#cba_player_box) | [`cba_player_box`](https://hoopR.sportsdataverse.org/reference/cba_player_box.html) |
| [`cba_player_defensive`](reference/cba/player-2#cba_player_defensive) | [`cba_player_defensive`](https://hoopR.sportsdataverse.org/reference/cba_player_defensive.html) |
| [`cba_player_extras`](reference/cba/player#cba_player_extras) | [`cba_player_extras`](https://hoopR.sportsdataverse.org/reference/cba_player_extras.html) |
| [`cba_player_offensive`](reference/cba/player#cba_player_offensive) | [`cba_player_offensive`](https://hoopR.sportsdataverse.org/reference/cba_player_offensive.html) |
| [`cba_player_shots`](reference/cba/player-2#cba_player_shots) | [`cba_player_shots`](https://hoopR.sportsdataverse.org/reference/cba_player_shots.html) |
| [`cba_report_status`](reference/cba/other#cba_report_status) | [`cba_report_status`](https://hoopR.sportsdataverse.org/reference/cba_report_status.html) |
| [`cba_rosters`](reference/cba/other#cba_rosters) | [`cba_rosters`](https://hoopR.sportsdataverse.org/reference/cba_rosters.html) |
| [`cba_schedule_day`](reference/cba/other#cba_schedule_day) | [`cba_schedule_day`](https://hoopR.sportsdataverse.org/reference/cba_schedule_day.html) |
| [`cba_schedule_month`](reference/cba/other#cba_schedule_month) | [`cba_schedule_month`](https://hoopR.sportsdataverse.org/reference/cba_schedule_month.html) |
| [`cba_seasons`](reference/cba/other#cba_seasons) | [`cba_seasons`](https://hoopR.sportsdataverse.org/reference/cba_seasons.html) |
| [`cba_team_basic`](reference/cba/team#cba_team_basic) | [`cba_team_basic`](https://hoopR.sportsdataverse.org/reference/cba_team_basic.html) |
| [`cba_team_counts`](reference/cba/team#cba_team_counts) | [`cba_team_counts`](https://hoopR.sportsdataverse.org/reference/cba_team_counts.html) |
| [`cba_team_four`](reference/cba/team#cba_team_four) | [`cba_team_four`](https://hoopR.sportsdataverse.org/reference/cba_team_four.html) |
| [`cba_team_options`](reference/cba/team#cba_team_options) | [`cba_team_options`](https://hoopR.sportsdataverse.org/reference/cba_team_options.html) |
| [`cba_team_overview`](reference/cba/team#cba_team_overview) | [`cba_team_overview`](https://hoopR.sportsdataverse.org/reference/cba_team_overview.html) |
| [`cba_team_shots`](reference/cba/team#cba_team_shots) | [`cba_team_shots`](https://hoopR.sportsdataverse.org/reference/cba_team_shots.html) |
| [`cba_teams`](reference/cba/team#cba_teams) | [`cba_teams`](https://hoopR.sportsdataverse.org/reference/cba_teams.html) |
