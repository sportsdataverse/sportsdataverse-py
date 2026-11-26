---
title: VTB
sidebar_label: VTB
description: "sdv-py VTB: endpoint references, dataset loaders and parsers for VTB in the SportsDataverse Python package."
---
# VTB (`sportsdataverse.basketball.vtb`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [VTB United League](#vtb-united-league) | `api.vtb-league.com`, `org.infobasket.su` | 38 | none |

## VTB United League {#vtb-united-league}

| Reference | Functions |
|---|---:|
| [VTB United League Infobasket (Russia)](reference/vtb_infobasket) | 35 |
| [VTB United League API (api.vtb-league.com)](reference/vtb_league) | 3 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.vtb` (Python) | `hoopR` (R) |
|---|---|
| [`vtb_infobasket_best_player_parameters`](reference/vtb_infobasket/best#vtb_infobasket_best_player_parameters) | [`vtb_infobasket_best_player_parameters`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_best_player_parameters.html) |
| [`vtb_infobasket_best_players`](reference/vtb_infobasket/best#vtb_infobasket_best_players) | [`vtb_infobasket_best_players`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_best_players.html) |
| [`vtb_infobasket_best_team_parameters`](reference/vtb_infobasket/best#vtb_infobasket_best_team_parameters) | [`vtb_infobasket_best_team_parameters`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_best_team_parameters.html) |
| [`vtb_infobasket_best_teams`](reference/vtb_infobasket/best#vtb_infobasket_best_teams) | [`vtb_infobasket_best_teams`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_best_teams.html) |
| [`vtb_infobasket_box_score`](reference/vtb_infobasket/other#vtb_infobasket_box_score) | [`vtb_infobasket_box_score`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_box_score.html) |
| [`vtb_infobasket_calendar`](reference/vtb_infobasket/other#vtb_infobasket_calendar) | [`vtb_infobasket_calendar`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_calendar.html) |
| [`vtb_infobasket_coach`](reference/vtb_infobasket/other#vtb_infobasket_coach) | [`vtb_infobasket_coach`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_coach.html) |
| [`vtb_infobasket_comp_issue`](reference/vtb_infobasket/other#vtb_infobasket_comp_issue) | [`vtb_infobasket_comp_issue`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_comp_issue.html) |
| [`vtb_infobasket_cross_table`](reference/vtb_infobasket/other#vtb_infobasket_cross_table) | [`vtb_infobasket_cross_table`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_cross_table.html) |
| [`vtb_infobasket_game`](reference/vtb_infobasket/other#vtb_infobasket_game) | [`vtb_infobasket_game`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_game.html) |
| [`vtb_infobasket_game_page`](reference/vtb_infobasket/other#vtb_infobasket_game_page) | [`vtb_infobasket_game_page`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_game_page.html) |
| [`vtb_infobasket_game_staff`](reference/vtb_infobasket/other#vtb_infobasket_game_staff) | [`vtb_infobasket_game_staff`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_game_staff.html) |
| [`vtb_infobasket_pbp`](reference/vtb_infobasket/other#vtb_infobasket_pbp) | [`vtb_infobasket_pbp`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_pbp.html) |
| [`vtb_infobasket_player`](reference/vtb_infobasket/player#vtb_infobasket_player) | [`vtb_infobasket_player`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_player.html) |
| [`vtb_infobasket_player_career_stats`](reference/vtb_infobasket/player#vtb_infobasket_player_career_stats) | [`vtb_infobasket_player_career_stats`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_player_career_stats.html) |
| [`vtb_infobasket_player_game_log`](reference/vtb_infobasket/player#vtb_infobasket_player_game_log) | [`vtb_infobasket_player_game_log`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_player_game_log.html) |
| [`vtb_infobasket_player_season_stats`](reference/vtb_infobasket/player#vtb_infobasket_player_season_stats) | [`vtb_infobasket_player_season_stats`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_player_season_stats.html) |
| [`vtb_infobasket_playoff`](reference/vtb_infobasket/other#vtb_infobasket_playoff) | [`vtb_infobasket_playoff`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_playoff.html) |
| [`vtb_infobasket_playoff_games`](reference/vtb_infobasket/other#vtb_infobasket_playoff_games) | [`vtb_infobasket_playoff_games`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_playoff_games.html) |
| [`vtb_infobasket_roster`](reference/vtb_infobasket/other#vtb_infobasket_roster) | [`vtb_infobasket_roster`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_roster.html) |
| [`vtb_infobasket_roster_coaches`](reference/vtb_infobasket/other#vtb_infobasket_roster_coaches) | [`vtb_infobasket_roster_coaches`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_roster_coaches.html) |
| [`vtb_infobasket_seasons`](reference/vtb_infobasket/other#vtb_infobasket_seasons) | [`vtb_infobasket_seasons`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_seasons.html) |
| [`vtb_infobasket_standings`](reference/vtb_infobasket/other#vtb_infobasket_standings) | [`vtb_infobasket_standings`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_standings.html) |
| [`vtb_infobasket_team`](reference/vtb_infobasket/team#vtb_infobasket_team) | [`vtb_infobasket_team`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team.html) |
| [`vtb_infobasket_team_box_score`](reference/vtb_infobasket/team#vtb_infobasket_team_box_score) | [`vtb_infobasket_team_box_score`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_box_score.html) |
| [`vtb_infobasket_team_coach_stats`](reference/vtb_infobasket/team-2#vtb_infobasket_team_coach_stats) | [`vtb_infobasket_team_coach_stats`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_coach_stats.html) |
| [`vtb_infobasket_team_comps`](reference/vtb_infobasket/team#vtb_infobasket_team_comps) | [`vtb_infobasket_team_comps`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_comps.html) |
| [`vtb_infobasket_team_games`](reference/vtb_infobasket/team#vtb_infobasket_team_games) | [`vtb_infobasket_team_games`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_games.html) |
| [`vtb_infobasket_team_player_stats`](reference/vtb_infobasket/team-2#vtb_infobasket_team_player_stats) | [`vtb_infobasket_team_player_stats`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_player_stats.html) |
| [`vtb_infobasket_team_preview`](reference/vtb_infobasket/team#vtb_infobasket_team_preview) | [`vtb_infobasket_team_preview`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_preview.html) |
| [`vtb_infobasket_team_season_stats`](reference/vtb_infobasket/team#vtb_infobasket_team_season_stats) | [`vtb_infobasket_team_season_stats`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_season_stats.html) |
| [`vtb_infobasket_team_seasons`](reference/vtb_infobasket/team#vtb_infobasket_team_seasons) | [`vtb_infobasket_team_seasons`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_seasons.html) |
| [`vtb_infobasket_team_stats`](reference/vtb_infobasket/team#vtb_infobasket_team_stats) | [`vtb_infobasket_team_stats`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_stats.html) |
| [`vtb_infobasket_team_stats_coaches`](reference/vtb_infobasket/team#vtb_infobasket_team_stats_coaches) | [`vtb_infobasket_team_stats_coaches`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_stats_coaches.html) |
| [`vtb_infobasket_team_stats_players`](reference/vtb_infobasket/team#vtb_infobasket_team_stats_players) | [`vtb_infobasket_team_stats_players`](https://hoopR.sportsdataverse.org/reference/vtb_infobasket_team_stats_players.html) |
| [`vtb_league_leagues`](reference/vtb_league#vtb_league_leagues) | [`vtb_league_leagues`](https://hoopR.sportsdataverse.org/reference/vtb_league_leagues.html) |
| [`vtb_league_matches`](reference/vtb_league#vtb_league_matches) | [`vtb_league_matches`](https://hoopR.sportsdataverse.org/reference/vtb_league_matches.html) |
| [`vtb_league_seasons`](reference/vtb_league#vtb_league_seasons) | [`vtb_league_seasons`](https://hoopR.sportsdataverse.org/reference/vtb_league_seasons.html) |
