---
title: NBA
sidebar_label: NBA
---
# NBA (`sportsdataverse.nba`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 24 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 81 | `https://sports.core.api.espn.com/v2/sports` |
| [NBA Stats API (stats.nba.com)](reference/nba_stats) | 112 | `https://stats.nba.com` |
| [Dataset loaders](reference/loaders) | 13 | sportsdataverse-data releases |
| [Additional functions](reference/additional) | 124 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [NBA tutorial](../tutorials/04_nba_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.nba` (Python) | `hoopR` (R) |
|---|---|
| [`espn_nba_award`](reference/core#espn_nba_award) | [`espn_nba_award`](https://hoopR.sportsdataverse.org/reference/espn_nba_award.html) |
| [`espn_nba_calendar`](reference/site#espn_nba_calendar) | [`espn_nba_calendar`](https://hoopR.sportsdataverse.org/reference/espn_nba_calendar.html) |
| [`espn_nba_coach`](reference/core#espn_nba_coach) | [`espn_nba_coach`](https://hoopR.sportsdataverse.org/reference/espn_nba_coach.html) |
| [`espn_nba_coach_record`](reference/core#espn_nba_coach_record) | [`espn_nba_coach_record`](https://hoopR.sportsdataverse.org/reference/espn_nba_coach_record.html) |
| [`espn_nba_coach_season`](reference/core#espn_nba_coach_season) | [`espn_nba_coach_season`](https://hoopR.sportsdataverse.org/reference/espn_nba_coach_season.html) |
| [`espn_nba_conferences`](reference/site#espn_nba_conferences) | [`espn_nba_conferences`](https://hoopR.sportsdataverse.org/reference/espn_nba_conferences.html) |
| [`espn_nba_draft`](reference/site#espn_nba_draft) | [`espn_nba_draft`](https://hoopR.sportsdataverse.org/reference/espn_nba_draft.html) |
| [`espn_nba_franchise`](reference/core#espn_nba_franchise) | [`espn_nba_franchise`](https://hoopR.sportsdataverse.org/reference/espn_nba_franchise.html) |
| [`espn_nba_franchises`](reference/core#espn_nba_franchises) | [`espn_nba_franchises`](https://hoopR.sportsdataverse.org/reference/espn_nba_franchises.html) |
| [`espn_nba_game_broadcasts`](reference/core#espn_nba_game_broadcasts) | [`espn_nba_game_broadcasts`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_broadcasts.html) |
| [`espn_nba_game_odds`](reference/core#espn_nba_game_odds) | [`espn_nba_game_odds`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_odds.html) |
| [`espn_nba_game_official_detail`](reference/core#espn_nba_game_official_detail) | [`espn_nba_game_official_detail`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_official_detail.html) |
| [`espn_nba_game_officials`](reference/core#espn_nba_game_officials) | [`espn_nba_game_officials`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_officials.html) |
| [`espn_nba_game_play`](reference/core#espn_nba_game_play) | [`espn_nba_game_play`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_play.html) |
| [`espn_nba_game_play_personnel`](reference/core#espn_nba_game_play_personnel) | [`espn_nba_game_play_personnel`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_play_personnel.html) |
| [`espn_nba_game_powerindex`](reference/core#espn_nba_game_powerindex) | [`espn_nba_game_powerindex`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_powerindex.html) |
| [`espn_nba_game_predictor`](reference/core#espn_nba_game_predictor) | [`espn_nba_game_predictor`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_predictor.html) |
| [`espn_nba_game_probabilities`](reference/core#espn_nba_game_probabilities) | [`espn_nba_game_probabilities`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_probabilities.html) |
| [`espn_nba_game_propbets`](reference/core#espn_nba_game_propbets) | [`espn_nba_game_propbets`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_propbets.html) |
| [`espn_nba_game_situation`](reference/core#espn_nba_game_situation) | [`espn_nba_game_situation`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_situation.html) |
| [`espn_nba_game_team_leaders`](reference/core#espn_nba_game_team_leaders) | [`espn_nba_game_team_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_leaders.html) |
| [`espn_nba_game_team_linescores`](reference/core#espn_nba_game_team_linescores) | [`espn_nba_game_team_linescores`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_linescores.html) |
| [`espn_nba_game_team_roster`](reference/core#espn_nba_game_team_roster) | [`espn_nba_game_team_roster`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_roster.html) |
| [`espn_nba_game_team_statistics`](reference/core#espn_nba_game_team_statistics) | [`espn_nba_game_team_statistics`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_statistics.html) |
| [`espn_nba_injuries`](reference/site#espn_nba_injuries) | [`espn_nba_injuries`](https://hoopR.sportsdataverse.org/reference/espn_nba_injuries.html) |
| [`espn_nba_leaders`](reference/web#espn_nba_leaders) | [`espn_nba_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_leaders.html) |
| [`espn_nba_news`](reference/site#espn_nba_news) | [`espn_nba_news`](https://hoopR.sportsdataverse.org/reference/espn_nba_news.html) |
| [`espn_nba_player_awards`](reference/core#espn_nba_player_awards) | [`espn_nba_player_awards`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_awards.html) |
| [`espn_nba_player_career_stats`](reference/core#espn_nba_player_career_stats) | [`espn_nba_player_career_stats`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_career_stats.html) |
| [`espn_nba_player_contracts`](reference/core#espn_nba_player_contracts) | [`espn_nba_player_contracts`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_contracts.html) |
| [`espn_nba_player_eventlog`](reference/core#espn_nba_player_eventlog) | [`espn_nba_player_eventlog`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_eventlog.html) |
| [`espn_nba_player_gamelog`](reference/web#espn_nba_player_gamelog) | [`espn_nba_player_gamelog`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_gamelog.html) |
| [`espn_nba_player_info`](reference/site#espn_nba_player_info) | [`espn_nba_player_info`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_info.html) |
| [`espn_nba_player_overview`](reference/web#espn_nba_player_overview) | [`espn_nba_player_overview`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_overview.html) |
| [`espn_nba_player_seasons`](reference/core#espn_nba_player_seasons) | [`espn_nba_player_seasons`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_seasons.html) |
| [`espn_nba_player_splits`](reference/web#espn_nba_player_splits) | [`espn_nba_player_splits`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_splits.html) |
| [`espn_nba_player_statisticslog`](reference/core#espn_nba_player_statisticslog) | [`espn_nba_player_statisticslog`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_statisticslog.html) |
| [`espn_nba_player_stats`](reference/additional#espn_nba_player_stats) | [`espn_nba_player_stats`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_stats.html) |
| [`espn_nba_player_stats_v3`](reference/web#espn_nba_player_stats_v3) | [`espn_nba_player_stats_v3`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_stats_v3.html) |
| [`espn_nba_position`](reference/core#espn_nba_position) | [`espn_nba_position`](https://hoopR.sportsdataverse.org/reference/espn_nba_position.html) |
| [`espn_nba_positions`](reference/core#espn_nba_positions) | [`espn_nba_positions`](https://hoopR.sportsdataverse.org/reference/espn_nba_positions.html) |
| [`espn_nba_scoreboard`](reference/site#espn_nba_scoreboard) | [`espn_nba_scoreboard`](https://hoopR.sportsdataverse.org/reference/espn_nba_scoreboard.html) |
| [`espn_nba_season_awards`](reference/core#espn_nba_season_awards) | [`espn_nba_season_awards`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_awards.html) |
| [`espn_nba_season_draft`](reference/core#espn_nba_season_draft) | [`espn_nba_season_draft`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_draft.html) |
| [`espn_nba_season_group`](reference/core#espn_nba_season_group) | [`espn_nba_season_group`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_group.html) |
| [`espn_nba_season_group_children`](reference/core#espn_nba_season_group_children) | [`espn_nba_season_group_children`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_group_children.html) |
| [`espn_nba_season_group_teams`](reference/core#espn_nba_season_group_teams) | [`espn_nba_season_group_teams`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_group_teams.html) |
| [`espn_nba_season_groups`](reference/core#espn_nba_season_groups) | [`espn_nba_season_groups`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_groups.html) |
| [`espn_nba_season_info`](reference/core#espn_nba_season_info) | [`espn_nba_season_info`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_info.html) |
| [`espn_nba_season_type`](reference/core#espn_nba_season_type) | [`espn_nba_season_type`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_type.html) |
| [`espn_nba_season_types`](reference/core#espn_nba_season_types) | [`espn_nba_season_types`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_types.html) |
| [`espn_nba_season_week`](reference/core#espn_nba_season_week) | [`espn_nba_season_week`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_week.html) |
| [`espn_nba_season_weeks`](reference/core#espn_nba_season_weeks) | [`espn_nba_season_weeks`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_weeks.html) |
| [`espn_nba_seasons`](reference/core#espn_nba_seasons) | [`espn_nba_seasons`](https://hoopR.sportsdataverse.org/reference/espn_nba_seasons.html) |
| [`espn_nba_standings`](reference/site#espn_nba_standings) | [`espn_nba_standings`](https://hoopR.sportsdataverse.org/reference/espn_nba_standings.html) |
| [`espn_nba_team`](reference/site#espn_nba_team) | [`espn_nba_team`](https://hoopR.sportsdataverse.org/reference/espn_nba_team.html) |
| [`espn_nba_team_injuries`](reference/site#espn_nba_team_injuries) | [`espn_nba_team_injuries`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_injuries.html) |
| [`espn_nba_team_leaders`](reference/site#espn_nba_team_leaders) | [`espn_nba_team_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_leaders.html) |
| [`espn_nba_team_news`](reference/site#espn_nba_team_news) | [`espn_nba_team_news`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_news.html) |
| [`espn_nba_team_record`](reference/site#espn_nba_team_record) | [`espn_nba_team_record`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_record.html) |
| [`espn_nba_team_roster`](reference/site#espn_nba_team_roster) | [`espn_nba_team_roster`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_roster.html) |
| [`espn_nba_team_schedule`](reference/site#espn_nba_team_schedule) | [`espn_nba_team_schedule`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_schedule.html) |
| [`espn_nba_teams`](reference/additional#espn_nba_teams) | [`espn_nba_teams`](https://hoopR.sportsdataverse.org/reference/espn_nba_teams.html) |
| [`espn_nba_tournaments`](reference/core#espn_nba_tournaments) | [`espn_nba_tournaments`](https://hoopR.sportsdataverse.org/reference/espn_nba_tournaments.html) |
| [`espn_nba_transactions`](reference/site#espn_nba_transactions) | [`espn_nba_transactions`](https://hoopR.sportsdataverse.org/reference/espn_nba_transactions.html) |
| [`espn_nba_venues`](reference/core#espn_nba_venues) | [`espn_nba_venues`](https://hoopR.sportsdataverse.org/reference/espn_nba_venues.html) |
| [`load_nba_draft`](reference/loaders#load_nba_draft) | [`load_nba_draft`](https://hoopR.sportsdataverse.org/reference/load_nba_draft.html) |
| [`load_nba_game_rosters`](reference/loaders#load_nba_game_rosters) | [`load_nba_game_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_game_rosters.html) |
| [`load_nba_officials`](reference/loaders#load_nba_officials) | [`load_nba_officials`](https://hoopR.sportsdataverse.org/reference/load_nba_officials.html) |
| [`load_nba_pbp`](reference/loaders#load_nba_pbp) | [`load_nba_pbp`](https://hoopR.sportsdataverse.org/reference/load_nba_pbp.html) |
| [`load_nba_rosters`](reference/loaders#load_nba_rosters) | [`load_nba_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_rosters.html) |
| [`load_nba_schedule`](reference/loaders#load_nba_schedule) | [`load_nba_schedule`](https://hoopR.sportsdataverse.org/reference/load_nba_schedule.html) |
| [`load_nba_standings`](reference/loaders#load_nba_standings) | [`load_nba_standings`](https://hoopR.sportsdataverse.org/reference/load_nba_standings.html) |
| [`most_recent_nba_season`](reference/additional#most_recent_nba_season) | [`most_recent_nba_season`](https://hoopR.sportsdataverse.org/reference/most_recent_nba_season.html) |
| [`year_to_season`](reference/additional#year_to_season) | [`year_to_season`](https://hoopR.sportsdataverse.org/reference/year_to_season.html) |
