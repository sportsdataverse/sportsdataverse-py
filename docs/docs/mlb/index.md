---
title: MLB
sidebar_label: MLB
---
# MLB (`sportsdataverse.mlb`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 24 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 82 | `https://sports.core.api.espn.com/v2/sports` |
| [MLB Stats API](reference/mlb_api) | 64 | `https://statsapi.mlb.com` |
| [MLB Statcast (Baseball Savant)](reference/mlb_statcast) | 39 | `https://baseballsavant.mlb.com` |
| [Additional functions](reference/additional) | 89 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [MLB tutorial](../tutorials/09_mlb_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`baseballr`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.mlb` (Python) | `baseballr` (R) |
|---|---|
| [`espn_mlb_award`](reference/core#espn_mlb_award) | [`espn_mlb_award`](https://billpetti.github.io/baseballr/reference/espn_mlb_award.html) |
| [`espn_mlb_calendar`](reference/site#espn_mlb_calendar) | [`espn_mlb_calendar`](https://billpetti.github.io/baseballr/reference/espn_mlb_calendar.html) |
| [`espn_mlb_coach`](reference/core#espn_mlb_coach) | [`espn_mlb_coach`](https://billpetti.github.io/baseballr/reference/espn_mlb_coach.html) |
| [`espn_mlb_coach_record`](reference/core#espn_mlb_coach_record) | [`espn_mlb_coach_record`](https://billpetti.github.io/baseballr/reference/espn_mlb_coach_record.html) |
| [`espn_mlb_coach_season`](reference/core#espn_mlb_coach_season) | [`espn_mlb_coach_season`](https://billpetti.github.io/baseballr/reference/espn_mlb_coach_season.html) |
| [`espn_mlb_conferences`](reference/site#espn_mlb_conferences) | [`espn_mlb_conferences`](https://billpetti.github.io/baseballr/reference/espn_mlb_conferences.html) |
| [`espn_mlb_draft`](reference/site#espn_mlb_draft) | [`espn_mlb_draft`](https://billpetti.github.io/baseballr/reference/espn_mlb_draft.html) |
| [`espn_mlb_franchise`](reference/core#espn_mlb_franchise) | [`espn_mlb_franchise`](https://billpetti.github.io/baseballr/reference/espn_mlb_franchise.html) |
| [`espn_mlb_franchises`](reference/core#espn_mlb_franchises) | [`espn_mlb_franchises`](https://billpetti.github.io/baseballr/reference/espn_mlb_franchises.html) |
| [`espn_mlb_game_broadcasts`](reference/core#espn_mlb_game_broadcasts) | [`espn_mlb_game_broadcasts`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_broadcasts.html) |
| [`espn_mlb_game_odds`](reference/core#espn_mlb_game_odds) | [`espn_mlb_game_odds`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_odds.html) |
| [`espn_mlb_game_official_detail`](reference/core#espn_mlb_game_official_detail) | [`espn_mlb_game_official_detail`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_official_detail.html) |
| [`espn_mlb_game_officials`](reference/core#espn_mlb_game_officials) | [`espn_mlb_game_officials`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_officials.html) |
| [`espn_mlb_game_play`](reference/core#espn_mlb_game_play) | [`espn_mlb_game_play`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_play.html) |
| [`espn_mlb_game_play_personnel`](reference/core#espn_mlb_game_play_personnel) | [`espn_mlb_game_play_personnel`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_play_personnel.html) |
| [`espn_mlb_game_powerindex`](reference/core#espn_mlb_game_powerindex) | [`espn_mlb_game_powerindex`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_powerindex.html) |
| [`espn_mlb_game_predictor`](reference/core#espn_mlb_game_predictor) | [`espn_mlb_game_predictor`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_predictor.html) |
| [`espn_mlb_game_probabilities`](reference/core#espn_mlb_game_probabilities) | [`espn_mlb_game_probabilities`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_probabilities.html) |
| [`espn_mlb_game_propbets`](reference/core#espn_mlb_game_propbets) | [`espn_mlb_game_propbets`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_propbets.html) |
| [`espn_mlb_game_rosters`](reference/additional#espn_mlb_game_rosters) | [`espn_mlb_game_rosters`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_rosters.html) |
| [`espn_mlb_game_situation`](reference/core#espn_mlb_game_situation) | [`espn_mlb_game_situation`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_situation.html) |
| [`espn_mlb_game_team_leaders`](reference/core#espn_mlb_game_team_leaders) | [`espn_mlb_game_team_leaders`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_team_leaders.html) |
| [`espn_mlb_game_team_linescores`](reference/core#espn_mlb_game_team_linescores) | [`espn_mlb_game_team_linescores`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_team_linescores.html) |
| [`espn_mlb_game_team_roster`](reference/core#espn_mlb_game_team_roster) | [`espn_mlb_game_team_roster`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_team_roster.html) |
| [`espn_mlb_game_team_statistics`](reference/core#espn_mlb_game_team_statistics) | [`espn_mlb_game_team_statistics`](https://billpetti.github.io/baseballr/reference/espn_mlb_game_team_statistics.html) |
| [`espn_mlb_injuries`](reference/site#espn_mlb_injuries) | [`espn_mlb_injuries`](https://billpetti.github.io/baseballr/reference/espn_mlb_injuries.html) |
| [`espn_mlb_leaders`](reference/web#espn_mlb_leaders) | [`espn_mlb_leaders`](https://billpetti.github.io/baseballr/reference/espn_mlb_leaders.html) |
| [`espn_mlb_news`](reference/site#espn_mlb_news) | [`espn_mlb_news`](https://billpetti.github.io/baseballr/reference/espn_mlb_news.html) |
| [`espn_mlb_pbp`](reference/additional#espn_mlb_pbp) | [`espn_mlb_pbp`](https://billpetti.github.io/baseballr/reference/espn_mlb_pbp.html) |
| [`espn_mlb_player_awards`](reference/core#espn_mlb_player_awards) | [`espn_mlb_player_awards`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_awards.html) |
| [`espn_mlb_player_career_stats`](reference/core#espn_mlb_player_career_stats) | [`espn_mlb_player_career_stats`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_career_stats.html) |
| [`espn_mlb_player_contracts`](reference/core#espn_mlb_player_contracts) | [`espn_mlb_player_contracts`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_contracts.html) |
| [`espn_mlb_player_eventlog`](reference/core#espn_mlb_player_eventlog) | [`espn_mlb_player_eventlog`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_eventlog.html) |
| [`espn_mlb_player_gamelog`](reference/web#espn_mlb_player_gamelog) | [`espn_mlb_player_gamelog`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_gamelog.html) |
| [`espn_mlb_player_info`](reference/site#espn_mlb_player_info) | [`espn_mlb_player_info`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_info.html) |
| [`espn_mlb_player_overview`](reference/web#espn_mlb_player_overview) | [`espn_mlb_player_overview`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_overview.html) |
| [`espn_mlb_player_seasons`](reference/core#espn_mlb_player_seasons) | [`espn_mlb_player_seasons`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_seasons.html) |
| [`espn_mlb_player_splits`](reference/web#espn_mlb_player_splits) | [`espn_mlb_player_splits`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_splits.html) |
| [`espn_mlb_player_statisticslog`](reference/core#espn_mlb_player_statisticslog) | [`espn_mlb_player_statisticslog`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_statisticslog.html) |
| [`espn_mlb_player_stats`](reference/additional#espn_mlb_player_stats) | [`espn_mlb_player_stats`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_stats.html) |
| [`espn_mlb_player_stats_v3`](reference/web#espn_mlb_player_stats_v3) | [`espn_mlb_player_stats_v3`](https://billpetti.github.io/baseballr/reference/espn_mlb_player_stats_v3.html) |
| [`espn_mlb_position`](reference/core#espn_mlb_position) | [`espn_mlb_position`](https://billpetti.github.io/baseballr/reference/espn_mlb_position.html) |
| [`espn_mlb_positions`](reference/core#espn_mlb_positions) | [`espn_mlb_positions`](https://billpetti.github.io/baseballr/reference/espn_mlb_positions.html) |
| [`espn_mlb_scoreboard`](reference/site#espn_mlb_scoreboard) | [`espn_mlb_scoreboard`](https://billpetti.github.io/baseballr/reference/espn_mlb_scoreboard.html) |
| [`espn_mlb_season_awards`](reference/core#espn_mlb_season_awards) | [`espn_mlb_season_awards`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_awards.html) |
| [`espn_mlb_season_draft`](reference/core#espn_mlb_season_draft) | [`espn_mlb_season_draft`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_draft.html) |
| [`espn_mlb_season_group`](reference/core#espn_mlb_season_group) | [`espn_mlb_season_group`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_group.html) |
| [`espn_mlb_season_group_children`](reference/core#espn_mlb_season_group_children) | [`espn_mlb_season_group_children`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_group_children.html) |
| [`espn_mlb_season_group_teams`](reference/core#espn_mlb_season_group_teams) | [`espn_mlb_season_group_teams`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_group_teams.html) |
| [`espn_mlb_season_groups`](reference/core#espn_mlb_season_groups) | [`espn_mlb_season_groups`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_groups.html) |
| [`espn_mlb_season_info`](reference/core#espn_mlb_season_info) | [`espn_mlb_season_info`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_info.html) |
| [`espn_mlb_season_type`](reference/core#espn_mlb_season_type) | [`espn_mlb_season_type`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_type.html) |
| [`espn_mlb_season_types`](reference/core#espn_mlb_season_types) | [`espn_mlb_season_types`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_types.html) |
| [`espn_mlb_season_week`](reference/core#espn_mlb_season_week) | [`espn_mlb_season_week`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_week.html) |
| [`espn_mlb_season_weeks`](reference/core#espn_mlb_season_weeks) | [`espn_mlb_season_weeks`](https://billpetti.github.io/baseballr/reference/espn_mlb_season_weeks.html) |
| [`espn_mlb_seasons`](reference/core#espn_mlb_seasons) | [`espn_mlb_seasons`](https://billpetti.github.io/baseballr/reference/espn_mlb_seasons.html) |
| [`espn_mlb_standings`](reference/site#espn_mlb_standings) | [`espn_mlb_standings`](https://billpetti.github.io/baseballr/reference/espn_mlb_standings.html) |
| [`espn_mlb_team`](reference/site#espn_mlb_team) | [`espn_mlb_team`](https://billpetti.github.io/baseballr/reference/espn_mlb_team.html) |
| [`espn_mlb_team_injuries`](reference/site#espn_mlb_team_injuries) | [`espn_mlb_team_injuries`](https://billpetti.github.io/baseballr/reference/espn_mlb_team_injuries.html) |
| [`espn_mlb_team_leaders`](reference/site#espn_mlb_team_leaders) | [`espn_mlb_team_leaders`](https://billpetti.github.io/baseballr/reference/espn_mlb_team_leaders.html) |
| [`espn_mlb_team_news`](reference/site#espn_mlb_team_news) | [`espn_mlb_team_news`](https://billpetti.github.io/baseballr/reference/espn_mlb_team_news.html) |
| [`espn_mlb_team_record`](reference/site#espn_mlb_team_record) | [`espn_mlb_team_record`](https://billpetti.github.io/baseballr/reference/espn_mlb_team_record.html) |
| [`espn_mlb_team_roster`](reference/site#espn_mlb_team_roster) | [`espn_mlb_team_roster`](https://billpetti.github.io/baseballr/reference/espn_mlb_team_roster.html) |
| [`espn_mlb_team_schedule`](reference/site#espn_mlb_team_schedule) | [`espn_mlb_team_schedule`](https://billpetti.github.io/baseballr/reference/espn_mlb_team_schedule.html) |
| [`espn_mlb_teams`](reference/additional#espn_mlb_teams) | [`espn_mlb_teams`](https://billpetti.github.io/baseballr/reference/espn_mlb_teams.html) |
| [`espn_mlb_tournaments`](reference/core#espn_mlb_tournaments) | [`espn_mlb_tournaments`](https://billpetti.github.io/baseballr/reference/espn_mlb_tournaments.html) |
| [`espn_mlb_transactions`](reference/site#espn_mlb_transactions) | [`espn_mlb_transactions`](https://billpetti.github.io/baseballr/reference/espn_mlb_transactions.html) |
| [`espn_mlb_venues`](reference/core#espn_mlb_venues) | [`espn_mlb_venues`](https://billpetti.github.io/baseballr/reference/espn_mlb_venues.html) |
| [`mlb_all_star_final_vote`](reference/mlb_api#mlb_all_star_final_vote) | [`mlb_all_star_final_vote`](https://billpetti.github.io/baseballr/reference/mlb_all_star_final_vote.html) |
| [`mlb_all_star_write_ins`](reference/mlb_api#mlb_all_star_write_ins) | [`mlb_all_star_write_ins`](https://billpetti.github.io/baseballr/reference/mlb_all_star_write_ins.html) |
| [`mlb_attendance`](reference/additional#mlb_attendance) | [`mlb_attendance`](https://billpetti.github.io/baseballr/reference/mlb_attendance.html) |
| [`mlb_awards`](reference/mlb_api#mlb_awards) | [`mlb_awards`](https://billpetti.github.io/baseballr/reference/mlb_awards.html) |
| [`mlb_conferences`](reference/mlb_api#mlb_conferences) | [`mlb_conferences`](https://billpetti.github.io/baseballr/reference/mlb_conferences.html) |
| [`mlb_divisions`](reference/additional#mlb_divisions) | [`mlb_divisions`](https://billpetti.github.io/baseballr/reference/mlb_divisions.html) |
| [`mlb_draft`](reference/mlb_api#mlb_draft) | [`mlb_draft`](https://billpetti.github.io/baseballr/reference/mlb_draft.html) |
| [`mlb_draft_latest`](reference/mlb_api#mlb_draft_latest) | [`mlb_draft_latest`](https://billpetti.github.io/baseballr/reference/mlb_draft_latest.html) |
| [`mlb_draft_prospects`](reference/additional#mlb_draft_prospects) | [`mlb_draft_prospects`](https://billpetti.github.io/baseballr/reference/mlb_draft_prospects.html) |
| [`mlb_game_changes`](reference/mlb_api#mlb_game_changes) | [`mlb_game_changes`](https://billpetti.github.io/baseballr/reference/mlb_game_changes.html) |
| [`mlb_game_content`](reference/mlb_api#mlb_game_content) | [`mlb_game_content`](https://billpetti.github.io/baseballr/reference/mlb_game_content.html) |
| [`mlb_game_context_metrics`](reference/mlb_api#mlb_game_context_metrics) | [`mlb_game_context_metrics`](https://billpetti.github.io/baseballr/reference/mlb_game_context_metrics.html) |
| [`mlb_game_pace`](reference/mlb_api#mlb_game_pace) | [`mlb_game_pace`](https://billpetti.github.io/baseballr/reference/mlb_game_pace.html) |
| [`mlb_jobs`](reference/mlb_api#mlb_jobs) | [`mlb_jobs`](https://billpetti.github.io/baseballr/reference/mlb_jobs.html) |
| [`mlb_pbp`](reference/mlb_api#mlb_pbp) | [`mlb_pbp`](https://billpetti.github.io/baseballr/reference/mlb_pbp.html) |
| [`mlb_pbp_diff`](reference/additional#mlb_pbp_diff) | [`mlb_pbp_diff`](https://billpetti.github.io/baseballr/reference/mlb_pbp_diff.html) |
| [`mlb_people`](reference/mlb_api#mlb_people) | [`mlb_people`](https://billpetti.github.io/baseballr/reference/mlb_people.html) |
| [`mlb_schedule`](reference/additional#mlb_schedule) | [`mlb_schedule`](https://billpetti.github.io/baseballr/reference/mlb_schedule.html) |
| [`mlb_schedule_postseason`](reference/mlb_api#mlb_schedule_postseason) | [`mlb_schedule_postseason`](https://billpetti.github.io/baseballr/reference/mlb_schedule_postseason.html) |
| [`mlb_schedule_postseason_series`](reference/mlb_api#mlb_schedule_postseason_series) | [`mlb_schedule_postseason_series`](https://billpetti.github.io/baseballr/reference/mlb_schedule_postseason_series.html) |
| [`mlb_seasons`](reference/additional#mlb_seasons) | [`mlb_seasons`](https://billpetti.github.io/baseballr/reference/mlb_seasons.html) |
| [`mlb_seasons_all`](reference/mlb_api#mlb_seasons_all) | [`mlb_seasons_all`](https://billpetti.github.io/baseballr/reference/mlb_seasons_all.html) |
| [`mlb_sports`](reference/mlb_api#mlb_sports) | [`mlb_sports`](https://billpetti.github.io/baseballr/reference/mlb_sports.html) |
| [`mlb_standings`](reference/additional#mlb_standings) | [`mlb_standings`](https://billpetti.github.io/baseballr/reference/mlb_standings.html) |
| [`mlb_stats`](reference/additional#mlb_stats) | [`mlb_stats`](https://billpetti.github.io/baseballr/reference/mlb_stats.html) |
| [`mlb_stats_leaders`](reference/additional#mlb_stats_leaders) | [`mlb_stats_leaders`](https://billpetti.github.io/baseballr/reference/mlb_stats_leaders.html) |
| [`mlb_team_affiliates`](reference/mlb_api#mlb_team_affiliates) | [`mlb_team_affiliates`](https://billpetti.github.io/baseballr/reference/mlb_team_affiliates.html) |
| [`mlb_team_alumni`](reference/mlb_api#mlb_team_alumni) | [`mlb_team_alumni`](https://billpetti.github.io/baseballr/reference/mlb_team_alumni.html) |
| [`mlb_team_coaches`](reference/mlb_api#mlb_team_coaches) | [`mlb_team_coaches`](https://billpetti.github.io/baseballr/reference/mlb_team_coaches.html) |
| [`mlb_team_leaders`](reference/additional#mlb_team_leaders) | [`mlb_team_leaders`](https://billpetti.github.io/baseballr/reference/mlb_team_leaders.html) |
| [`mlb_team_personnel`](reference/mlb_api#mlb_team_personnel) | [`mlb_team_personnel`](https://billpetti.github.io/baseballr/reference/mlb_team_personnel.html) |
| [`mlb_team_stats`](reference/additional#mlb_team_stats) | [`mlb_team_stats`](https://billpetti.github.io/baseballr/reference/mlb_team_stats.html) |
| [`mlb_teams`](reference/additional#mlb_teams) | [`mlb_teams`](https://billpetti.github.io/baseballr/reference/mlb_teams.html) |
| [`mlb_teams_stats`](reference/mlb_api#mlb_teams_stats) | [`mlb_teams_stats`](https://billpetti.github.io/baseballr/reference/mlb_teams_stats.html) |
| [`mlb_teams_stats_leaders`](reference/mlb_api#mlb_teams_stats_leaders) | [`mlb_teams_stats_leaders`](https://billpetti.github.io/baseballr/reference/mlb_teams_stats_leaders.html) |
| [`mlb_venues`](reference/mlb_api#mlb_venues) | [`mlb_venues`](https://billpetti.github.io/baseballr/reference/mlb_venues.html) |
| [`most_recent_mlb_season`](reference/additional#most_recent_mlb_season) | [`most_recent_mlb_season`](https://billpetti.github.io/baseballr/reference/most_recent_mlb_season.html) |
