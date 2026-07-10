---
title: PWHL
sidebar_label: PWHL
---
# PWHL (`sportsdataverse.pwhl`)

| Reference | Functions | Base URL |
|---|---:|---|
| [Dataset loaders](reference/loaders) | 15 | sportsdataverse-data releases |
| [Additional functions](reference/additional) | 43 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [PWHL tutorial](../tutorials/10_pwhl_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.pwhl` (Python) | `fastRhockey` (R) |
|---|---|
| [`load_pwhl_game_info`](reference/loaders#load_pwhl_game_info) | [`load_pwhl_game_info`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_game_info.html) |
| [`load_pwhl_game_rosters`](reference/loaders#load_pwhl_game_rosters) | [`load_pwhl_game_rosters`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_game_rosters.html) |
| [`load_pwhl_goalie_box`](reference/additional#load_pwhl_goalie_box) | [`load_pwhl_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_goalie_box.html) |
| [`load_pwhl_goalie_boxscores`](reference/loaders#load_pwhl_goalie_boxscores) | [`load_pwhl_goalie_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_goalie_boxscores.html) |
| [`load_pwhl_officials`](reference/loaders#load_pwhl_officials) | [`load_pwhl_officials`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_officials.html) |
| [`load_pwhl_pbp`](reference/loaders#load_pwhl_pbp) | [`load_pwhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_pbp.html) |
| [`load_pwhl_penalty_summary`](reference/loaders#load_pwhl_penalty_summary) | [`load_pwhl_penalty_summary`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_penalty_summary.html) |
| [`load_pwhl_player_box`](reference/additional#load_pwhl_player_box) | [`load_pwhl_player_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_player_box.html) |
| [`load_pwhl_player_boxscores`](reference/loaders#load_pwhl_player_boxscores) | [`load_pwhl_player_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_player_boxscores.html) |
| [`load_pwhl_rosters`](reference/loaders#load_pwhl_rosters) | [`load_pwhl_rosters`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_rosters.html) |
| [`load_pwhl_schedule`](reference/additional#load_pwhl_schedule) | [`load_pwhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_schedule.html) |
| [`load_pwhl_schedules`](reference/loaders#load_pwhl_schedules) | [`load_pwhl_schedules`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_schedules.html) |
| [`load_pwhl_scoring_summary`](reference/loaders#load_pwhl_scoring_summary) | [`load_pwhl_scoring_summary`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_scoring_summary.html) |
| [`load_pwhl_shootout`](reference/loaders#load_pwhl_shootout) | [`load_pwhl_shootout`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_shootout.html) |
| [`load_pwhl_shots_by_period`](reference/loaders#load_pwhl_shots_by_period) | [`load_pwhl_shots_by_period`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_shots_by_period.html) |
| [`load_pwhl_skater_box`](reference/additional#load_pwhl_skater_box) | [`load_pwhl_skater_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_skater_box.html) |
| [`load_pwhl_skater_boxscores`](reference/loaders#load_pwhl_skater_boxscores) | [`load_pwhl_skater_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_skater_boxscores.html) |
| [`load_pwhl_team_box`](reference/additional#load_pwhl_team_box) | [`load_pwhl_team_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_team_box.html) |
| [`load_pwhl_team_boxscores`](reference/loaders#load_pwhl_team_boxscores) | [`load_pwhl_team_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_team_boxscores.html) |
| [`load_pwhl_three_stars`](reference/loaders#load_pwhl_three_stars) | [`load_pwhl_three_stars`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_three_stars.html) |
| [`most_recent_pwhl_season`](reference/additional#most_recent_pwhl_season) | [`most_recent_pwhl_season`](https://fastRhockey.sportsdataverse.org/reference/most_recent_pwhl_season.html) |
| [`pwhl_game_corsi`](reference/additional#pwhl_game_corsi) | [`pwhl_game_corsi`](https://fastRhockey.sportsdataverse.org/reference/pwhl_game_corsi.html) |
| [`pwhl_game_shifts`](reference/additional#pwhl_game_shifts) | [`pwhl_game_shifts`](https://fastRhockey.sportsdataverse.org/reference/pwhl_game_shifts.html) |
| [`pwhl_game_summary`](reference/additional#pwhl_game_summary) | [`pwhl_game_summary`](https://fastRhockey.sportsdataverse.org/reference/pwhl_game_summary.html) |
| [`pwhl_leaders`](reference/additional#pwhl_leaders) | [`pwhl_leaders`](https://fastRhockey.sportsdataverse.org/reference/pwhl_leaders.html) |
| [`pwhl_player_box`](reference/additional#pwhl_player_box) | [`pwhl_player_box`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_box.html) |
| [`pwhl_player_game_log`](reference/additional#pwhl_player_game_log) | [`pwhl_player_game_log`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_game_log.html) |
| [`pwhl_player_info`](reference/additional#pwhl_player_info) | [`pwhl_player_info`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_info.html) |
| [`pwhl_player_search`](reference/additional#pwhl_player_search) | [`pwhl_player_search`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_search.html) |
| [`pwhl_player_stats`](reference/additional#pwhl_player_stats) | [`pwhl_player_stats`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_stats.html) |
| [`pwhl_player_toi`](reference/additional#pwhl_player_toi) | [`pwhl_player_toi`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_toi.html) |
| [`pwhl_playoff_bracket`](reference/additional#pwhl_playoff_bracket) | [`pwhl_playoff_bracket`](https://fastRhockey.sportsdataverse.org/reference/pwhl_playoff_bracket.html) |
| [`pwhl_schedule`](reference/additional#pwhl_schedule) | [`pwhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/pwhl_schedule.html) |
| [`pwhl_scorebar`](reference/additional#pwhl_scorebar) | [`pwhl_scorebar`](https://fastRhockey.sportsdataverse.org/reference/pwhl_scorebar.html) |
| [`pwhl_season_id`](reference/additional#pwhl_season_id) | [`pwhl_season_id`](https://fastRhockey.sportsdataverse.org/reference/pwhl_season_id.html) |
| [`pwhl_standings`](reference/additional#pwhl_standings) | [`pwhl_standings`](https://fastRhockey.sportsdataverse.org/reference/pwhl_standings.html) |
| [`pwhl_stats`](reference/additional#pwhl_stats) | [`pwhl_stats`](https://fastRhockey.sportsdataverse.org/reference/pwhl_stats.html) |
| [`pwhl_streaks`](reference/additional#pwhl_streaks) | [`pwhl_streaks`](https://fastRhockey.sportsdataverse.org/reference/pwhl_streaks.html) |
| [`pwhl_team_roster`](reference/additional#pwhl_team_roster) | [`pwhl_team_roster`](https://fastRhockey.sportsdataverse.org/reference/pwhl_team_roster.html) |
| [`pwhl_teams`](reference/additional#pwhl_teams) | [`pwhl_teams`](https://fastRhockey.sportsdataverse.org/reference/pwhl_teams.html) |
| [`pwhl_transactions`](reference/additional#pwhl_transactions) | [`pwhl_transactions`](https://fastRhockey.sportsdataverse.org/reference/pwhl_transactions.html) |
