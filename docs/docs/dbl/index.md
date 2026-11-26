---
title: Basketligaen
sidebar_label: Basketligaen
description: "sdv-py Basketligaen: endpoint references, dataset loaders and parsers for Basketligaen in the SportsDataverse Python package."
---
# Basketligaen (`sportsdataverse.basketball.dbl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [Basketligaen (Denmark, Sportality)](#basketligaen-denmark-sportality) | `www.basketligaen.dk` | 27 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 2 | — |

## Basketligaen (Denmark, Sportality) {#basketligaen-denmark-sportality}

| Reference | Functions |
|---|---:|
| [Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality)](reference/sportality) | 25 |
| [Hand-written wrappers](reference/additional) | 2 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.dbl` (Python) | `hoopR` (R) |
|---|---|
| [`sportality_athlete_career`](reference/sportality/athlete#sportality_athlete_career) | [`sportality_athlete_career`](https://hoopR.sportsdataverse.org/reference/sportality_athlete_career.html) |
| [`sportality_athlete_details`](reference/sportality/athlete#sportality_athlete_details) | [`sportality_athlete_details`](https://hoopR.sportsdataverse.org/reference/sportality_athlete_details.html) |
| [`sportality_athlete_profile`](reference/sportality/athlete#sportality_athlete_profile) | [`sportality_athlete_profile`](https://hoopR.sportsdataverse.org/reference/sportality_athlete_profile.html) |
| [`sportality_boxscore`](reference/sportality/other#sportality_boxscore) | [`sportality_boxscore`](https://hoopR.sportsdataverse.org/reference/sportality_boxscore.html) |
| [`sportality_game_header`](reference/sportality/game#sportality_game_header) | [`sportality_game_header`](https://hoopR.sportsdataverse.org/reference/sportality_game_header.html) |
| [`sportality_game_overview`](reference/sportality/game#sportality_game_overview) | [`sportality_game_overview`](https://hoopR.sportsdataverse.org/reference/sportality_game_overview.html) |
| [`sportality_game_summary`](reference/sportality/game#sportality_game_summary) | [`sportality_game_summary`](https://hoopR.sportsdataverse.org/reference/sportality_game_summary.html) |
| [`sportality_last_game_day`](reference/sportality/other#sportality_last_game_day) | [`sportality_last_game_day`](https://hoopR.sportsdataverse.org/reference/sportality_last_game_day.html) |
| [`sportality_layout_info`](reference/sportality/other#sportality_layout_info) | [`sportality_layout_info`](https://hoopR.sportsdataverse.org/reference/sportality_layout_info.html) |
| [`sportality_next_game_day`](reference/sportality/other#sportality_next_game_day) | [`sportality_next_game_day`](https://hoopR.sportsdataverse.org/reference/sportality_next_game_day.html) |
| [`sportality_pbp`](reference/sportality/other#sportality_pbp) | [`sportality_pbp`](https://hoopR.sportsdataverse.org/reference/sportality_pbp.html) |
| [`sportality_player_box`](reference/sportality/player#sportality_player_box) | [`sportality_player_box`](https://hoopR.sportsdataverse.org/reference/sportality_player_box.html) |
| [`sportality_player_crosswalk`](reference/additional#sportality_player_crosswalk) | [`sportality_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/sportality_player_crosswalk.html) |
| [`sportality_player_h2h`](reference/sportality/player#sportality_player_h2h) | [`sportality_player_h2h`](https://hoopR.sportsdataverse.org/reference/sportality_player_h2h.html) |
| [`sportality_post_game_team_stats`](reference/sportality/other#sportality_post_game_team_stats) | [`sportality_post_game_team_stats`](https://hoopR.sportsdataverse.org/reference/sportality_post_game_team_stats.html) |
| [`sportality_roster`](reference/sportality/other#sportality_roster) | [`sportality_roster`](https://hoopR.sportsdataverse.org/reference/sportality_roster.html) |
| [`sportality_schedule`](reference/sportality/other#sportality_schedule) | [`sportality_schedule`](https://hoopR.sportsdataverse.org/reference/sportality_schedule.html) |
| [`sportality_season_filter`](reference/sportality/other#sportality_season_filter) | [`sportality_season_filter`](https://hoopR.sportsdataverse.org/reference/sportality_season_filter.html) |
| [`sportality_sites`](reference/sportality/other#sportality_sites) | [`sportality_sites`](https://hoopR.sportsdataverse.org/reference/sportality_sites.html) |
| [`sportality_ssgt_provider`](reference/sportality/other#sportality_ssgt_provider) | [`sportality_ssgt_provider`](https://hoopR.sportsdataverse.org/reference/sportality_ssgt_provider.html) |
| [`sportality_ssgt_uuid`](reference/additional#sportality_ssgt_uuid) | [`sportality_ssgt_uuid`](https://hoopR.sportsdataverse.org/reference/sportality_ssgt_uuid.html) |
| [`sportality_standings`](reference/sportality/other#sportality_standings) | [`sportality_standings`](https://hoopR.sportsdataverse.org/reference/sportality_standings.html) |
| [`sportality_stats_info`](reference/sportality/other#sportality_stats_info) | [`sportality_stats_info`](https://hoopR.sportsdataverse.org/reference/sportality_stats_info.html) |
| [`sportality_team`](reference/sportality/team#sportality_team) | [`sportality_team`](https://hoopR.sportsdataverse.org/reference/sportality_team.html) |
| [`sportality_team_played_games`](reference/sportality/team#sportality_team_played_games) | [`sportality_team_played_games`](https://hoopR.sportsdataverse.org/reference/sportality_team_played_games.html) |
| [`sportality_team_stats`](reference/sportality/team#sportality_team_stats) | [`sportality_team_stats`](https://hoopR.sportsdataverse.org/reference/sportality_team_stats.html) |
| [`sportality_team_upcoming_games`](reference/sportality/team#sportality_team_upcoming_games) | [`sportality_team_upcoming_games`](https://hoopR.sportsdataverse.org/reference/sportality_team_upcoming_games.html) |
