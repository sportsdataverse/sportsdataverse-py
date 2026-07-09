---
title: WNBA
sidebar_label: WNBA
---
# WNBA (`sportsdataverse.wnba`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 24 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 80 | `https://sports.core.api.espn.com/v2/sports` |
| [WNBA Stats API (stats.wnba.com)](reference/wnba_stats) | 95 | `https://stats.wnba.com` |
| [Dataset loaders](reference/loaders) | 25 | sportsdataverse-data releases |
| [Additional functions](reference/additional) | 27 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [WNBA tutorial](../tutorials/08_wnba_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`wehoop`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.wnba` (Python) | `wehoop` (R) |
|---|---|
| [`espn_wnba_award`](reference/core#espn_wnba_award) | [`espn_wnba_award`](https://wehoop.sportsdataverse.org/reference/espn_wnba_award.html) |
| [`espn_wnba_calendar`](reference/site#espn_wnba_calendar) | [`espn_wnba_calendar`](https://wehoop.sportsdataverse.org/reference/espn_wnba_calendar.html) |
| [`espn_wnba_coach_season`](reference/core#espn_wnba_coach_season) | [`espn_wnba_coach_season`](https://wehoop.sportsdataverse.org/reference/espn_wnba_coach_season.html) |
| [`espn_wnba_conferences`](reference/site#espn_wnba_conferences) | [`espn_wnba_conferences`](https://wehoop.sportsdataverse.org/reference/espn_wnba_conferences.html) |
| [`espn_wnba_draft`](reference/site#espn_wnba_draft) | [`espn_wnba_draft`](https://wehoop.sportsdataverse.org/reference/espn_wnba_draft.html) |
| [`espn_wnba_franchise`](reference/core#espn_wnba_franchise) | [`espn_wnba_franchise`](https://wehoop.sportsdataverse.org/reference/espn_wnba_franchise.html) |
| [`espn_wnba_franchises`](reference/core#espn_wnba_franchises) | [`espn_wnba_franchises`](https://wehoop.sportsdataverse.org/reference/espn_wnba_franchises.html) |
| [`espn_wnba_game_broadcasts`](reference/core#espn_wnba_game_broadcasts) | [`espn_wnba_game_broadcasts`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_broadcasts.html) |
| [`espn_wnba_game_odds`](reference/core#espn_wnba_game_odds) | [`espn_wnba_game_odds`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_odds.html) |
| [`espn_wnba_game_official_detail`](reference/core#espn_wnba_game_official_detail) | [`espn_wnba_game_official_detail`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_official_detail.html) |
| [`espn_wnba_game_officials`](reference/additional#espn_wnba_game_officials) | [`espn_wnba_game_officials`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_officials.html) |
| [`espn_wnba_game_play`](reference/core#espn_wnba_game_play) | [`espn_wnba_game_play`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_play.html) |
| [`espn_wnba_game_play_personnel`](reference/core#espn_wnba_game_play_personnel) | [`espn_wnba_game_play_personnel`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_play_personnel.html) |
| [`espn_wnba_game_powerindex`](reference/core#espn_wnba_game_powerindex) | [`espn_wnba_game_powerindex`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_powerindex.html) |
| [`espn_wnba_game_predictor`](reference/core#espn_wnba_game_predictor) | [`espn_wnba_game_predictor`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_predictor.html) |
| [`espn_wnba_game_probabilities`](reference/core#espn_wnba_game_probabilities) | [`espn_wnba_game_probabilities`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_probabilities.html) |
| [`espn_wnba_game_propbets`](reference/core#espn_wnba_game_propbets) | [`espn_wnba_game_propbets`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_propbets.html) |
| [`espn_wnba_game_situation`](reference/core#espn_wnba_game_situation) | [`espn_wnba_game_situation`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_situation.html) |
| [`espn_wnba_game_team_leaders`](reference/core#espn_wnba_game_team_leaders) | [`espn_wnba_game_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_leaders.html) |
| [`espn_wnba_game_team_linescores`](reference/core#espn_wnba_game_team_linescores) | [`espn_wnba_game_team_linescores`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_linescores.html) |
| [`espn_wnba_game_team_roster`](reference/core#espn_wnba_game_team_roster) | [`espn_wnba_game_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_roster.html) |
| [`espn_wnba_game_team_statistics`](reference/core#espn_wnba_game_team_statistics) | [`espn_wnba_game_team_statistics`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_statistics.html) |
| [`espn_wnba_injuries`](reference/site#espn_wnba_injuries) | [`espn_wnba_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wnba_injuries.html) |
| [`espn_wnba_leaders`](reference/web#espn_wnba_leaders) | [`espn_wnba_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wnba_leaders.html) |
| [`espn_wnba_news`](reference/site#espn_wnba_news) | [`espn_wnba_news`](https://wehoop.sportsdataverse.org/reference/espn_wnba_news.html) |
| [`espn_wnba_player_awards`](reference/core#espn_wnba_player_awards) | [`espn_wnba_player_awards`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_awards.html) |
| [`espn_wnba_player_career_stats`](reference/core#espn_wnba_player_career_stats) | [`espn_wnba_player_career_stats`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_career_stats.html) |
| [`espn_wnba_player_eventlog`](reference/core#espn_wnba_player_eventlog) | [`espn_wnba_player_eventlog`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_eventlog.html) |
| [`espn_wnba_player_gamelog`](reference/web#espn_wnba_player_gamelog) | [`espn_wnba_player_gamelog`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_gamelog.html) |
| [`espn_wnba_player_info`](reference/site#espn_wnba_player_info) | [`espn_wnba_player_info`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_info.html) |
| [`espn_wnba_player_overview`](reference/web#espn_wnba_player_overview) | [`espn_wnba_player_overview`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_overview.html) |
| [`espn_wnba_player_seasons`](reference/core#espn_wnba_player_seasons) | [`espn_wnba_player_seasons`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_seasons.html) |
| [`espn_wnba_player_splits`](reference/web#espn_wnba_player_splits) | [`espn_wnba_player_splits`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_splits.html) |
| [`espn_wnba_player_statisticslog`](reference/core#espn_wnba_player_statisticslog) | [`espn_wnba_player_statisticslog`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_statisticslog.html) |
| [`espn_wnba_player_stats`](reference/additional#espn_wnba_player_stats) | [`espn_wnba_player_stats`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_stats.html) |
| [`espn_wnba_player_stats_v3`](reference/web#espn_wnba_player_stats_v3) | [`espn_wnba_player_stats_v3`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_stats_v3.html) |
| [`espn_wnba_position`](reference/core#espn_wnba_position) | [`espn_wnba_position`](https://wehoop.sportsdataverse.org/reference/espn_wnba_position.html) |
| [`espn_wnba_positions`](reference/core#espn_wnba_positions) | [`espn_wnba_positions`](https://wehoop.sportsdataverse.org/reference/espn_wnba_positions.html) |
| [`espn_wnba_scoreboard`](reference/site#espn_wnba_scoreboard) | [`espn_wnba_scoreboard`](https://wehoop.sportsdataverse.org/reference/espn_wnba_scoreboard.html) |
| [`espn_wnba_season_awards`](reference/core#espn_wnba_season_awards) | [`espn_wnba_season_awards`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_awards.html) |
| [`espn_wnba_season_draft`](reference/core#espn_wnba_season_draft) | [`espn_wnba_season_draft`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_draft.html) |
| [`espn_wnba_season_group`](reference/core#espn_wnba_season_group) | [`espn_wnba_season_group`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_group.html) |
| [`espn_wnba_season_group_children`](reference/core#espn_wnba_season_group_children) | [`espn_wnba_season_group_children`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_group_children.html) |
| [`espn_wnba_season_group_teams`](reference/core#espn_wnba_season_group_teams) | [`espn_wnba_season_group_teams`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_group_teams.html) |
| [`espn_wnba_season_groups`](reference/core#espn_wnba_season_groups) | [`espn_wnba_season_groups`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_groups.html) |
| [`espn_wnba_season_info`](reference/core#espn_wnba_season_info) | [`espn_wnba_season_info`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_info.html) |
| [`espn_wnba_season_type`](reference/core#espn_wnba_season_type) | [`espn_wnba_season_type`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_type.html) |
| [`espn_wnba_season_types`](reference/core#espn_wnba_season_types) | [`espn_wnba_season_types`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_types.html) |
| [`espn_wnba_season_week`](reference/core#espn_wnba_season_week) | [`espn_wnba_season_week`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_week.html) |
| [`espn_wnba_season_weeks`](reference/core#espn_wnba_season_weeks) | [`espn_wnba_season_weeks`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_weeks.html) |
| [`espn_wnba_seasons`](reference/core#espn_wnba_seasons) | [`espn_wnba_seasons`](https://wehoop.sportsdataverse.org/reference/espn_wnba_seasons.html) |
| [`espn_wnba_standings`](reference/site#espn_wnba_standings) | [`espn_wnba_standings`](https://wehoop.sportsdataverse.org/reference/espn_wnba_standings.html) |
| [`espn_wnba_team`](reference/site#espn_wnba_team) | [`espn_wnba_team`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team.html) |
| [`espn_wnba_team_injuries`](reference/site#espn_wnba_team_injuries) | [`espn_wnba_team_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_injuries.html) |
| [`espn_wnba_team_leaders`](reference/site#espn_wnba_team_leaders) | [`espn_wnba_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_leaders.html) |
| [`espn_wnba_team_news`](reference/site#espn_wnba_team_news) | [`espn_wnba_team_news`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_news.html) |
| [`espn_wnba_team_record`](reference/site#espn_wnba_team_record) | [`espn_wnba_team_record`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_record.html) |
| [`espn_wnba_team_roster`](reference/site#espn_wnba_team_roster) | [`espn_wnba_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_roster.html) |
| [`espn_wnba_team_schedule`](reference/site#espn_wnba_team_schedule) | [`espn_wnba_team_schedule`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_schedule.html) |
| [`espn_wnba_team_stats`](reference/additional#espn_wnba_team_stats) | [`espn_wnba_team_stats`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_stats.html) |
| [`espn_wnba_teams`](reference/additional#espn_wnba_teams) | [`espn_wnba_teams`](https://wehoop.sportsdataverse.org/reference/espn_wnba_teams.html) |
| [`espn_wnba_transactions`](reference/site#espn_wnba_transactions) | [`espn_wnba_transactions`](https://wehoop.sportsdataverse.org/reference/espn_wnba_transactions.html) |
| [`espn_wnba_venues`](reference/core#espn_wnba_venues) | [`espn_wnba_venues`](https://wehoop.sportsdataverse.org/reference/espn_wnba_venues.html) |
| [`load_wnba_draft`](reference/loaders#load_wnba_draft) | [`load_wnba_draft`](https://wehoop.sportsdataverse.org/reference/load_wnba_draft.html) |
| [`load_wnba_game_rosters`](reference/loaders#load_wnba_game_rosters) | [`load_wnba_game_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_game_rosters.html) |
| [`load_wnba_officials`](reference/loaders#load_wnba_officials) | [`load_wnba_officials`](https://wehoop.sportsdataverse.org/reference/load_wnba_officials.html) |
| [`load_wnba_pbp`](reference/loaders#load_wnba_pbp) | [`load_wnba_pbp`](https://wehoop.sportsdataverse.org/reference/load_wnba_pbp.html) |
| [`load_wnba_rosters`](reference/loaders#load_wnba_rosters) | [`load_wnba_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_rosters.html) |
| [`load_wnba_schedule`](reference/loaders#load_wnba_schedule) | [`load_wnba_schedule`](https://wehoop.sportsdataverse.org/reference/load_wnba_schedule.html) |
| [`load_wnba_shots`](reference/loaders#load_wnba_shots) | [`load_wnba_shots`](https://wehoop.sportsdataverse.org/reference/load_wnba_shots.html) |
| [`load_wnba_standings`](reference/loaders#load_wnba_standings) | [`load_wnba_standings`](https://wehoop.sportsdataverse.org/reference/load_wnba_standings.html) |
| [`load_wnba_stats_coaches`](reference/loaders#load_wnba_stats_coaches) | [`load_wnba_stats_coaches`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_coaches.html) |
| [`load_wnba_stats_draft`](reference/loaders#load_wnba_stats_draft) | [`load_wnba_stats_draft`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_draft.html) |
| [`load_wnba_stats_game_rosters`](reference/loaders#load_wnba_stats_game_rosters) | [`load_wnba_stats_game_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_game_rosters.html) |
| [`load_wnba_stats_lineups`](reference/loaders#load_wnba_stats_lineups) | [`load_wnba_stats_lineups`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_lineups.html) |
| [`load_wnba_stats_officials`](reference/loaders#load_wnba_stats_officials) | [`load_wnba_stats_officials`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_officials.html) |
| [`load_wnba_stats_pbp`](reference/loaders#load_wnba_stats_pbp) | [`load_wnba_stats_pbp`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_pbp.html) |
| [`load_wnba_stats_player_game_logs`](reference/loaders#load_wnba_stats_player_game_logs) | [`load_wnba_stats_player_game_logs`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_player_game_logs.html) |
| [`load_wnba_stats_rosters`](reference/loaders#load_wnba_stats_rosters) | [`load_wnba_stats_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_rosters.html) |
| [`load_wnba_stats_shots`](reference/loaders#load_wnba_stats_shots) | [`load_wnba_stats_shots`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_shots.html) |
| [`load_wnba_stats_standings`](reference/loaders#load_wnba_stats_standings) | [`load_wnba_stats_standings`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_standings.html) |
| [`most_recent_wnba_season`](reference/additional#most_recent_wnba_season) | [`most_recent_wnba_season`](https://wehoop.sportsdataverse.org/reference/most_recent_wnba_season.html) |
