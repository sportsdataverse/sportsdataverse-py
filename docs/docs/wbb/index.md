---
title: WBB
sidebar_label: WBB
description: "sdv-py WBB: endpoint references, dataset loaders and parsers for WBB in the SportsDataverse Python package."
---
# WBB (`sportsdataverse.wbb`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 25 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 86 | `https://sports.core.api.espn.com/v2/sports` |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 | `https://site.web.api.espn.com/apis/fitt/v3/sports` |
| [Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)](reference/bart_wbb) | 1 | `https://barttorvik.com/ncaaw` |
| [Dataset loaders](reference/loaders) | 34 | sportsdataverse-data releases |
| [Additional functions](reference/additional) | 314 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [WBB tutorial](../tutorials/05_wbb_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`wehoop`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.wbb` (Python) | `wehoop` (R) |
|---|---|
| [`bart_wbb_ratings`](reference/bart_wbb#bart_wbb_ratings) | [`bart_wbb_ratings`](https://wehoop.sportsdataverse.org/reference/bart_wbb_ratings.html) |
| [`espn_wbb_award`](reference/core/other#espn_wbb_award) | [`espn_wbb_award`](https://wehoop.sportsdataverse.org/reference/espn_wbb_award.html) |
| [`espn_wbb_calendar`](reference/site#espn_wbb_calendar) | [`espn_wbb_calendar`](https://wehoop.sportsdataverse.org/reference/espn_wbb_calendar.html) |
| [`espn_wbb_coach`](reference/core/other#espn_wbb_coach) | [`espn_wbb_coach`](https://wehoop.sportsdataverse.org/reference/espn_wbb_coach.html) |
| [`espn_wbb_coach_record`](reference/core/other#espn_wbb_coach_record) | [`espn_wbb_coach_record`](https://wehoop.sportsdataverse.org/reference/espn_wbb_coach_record.html) |
| [`espn_wbb_coach_season`](reference/core/other#espn_wbb_coach_season) | [`espn_wbb_coach_season`](https://wehoop.sportsdataverse.org/reference/espn_wbb_coach_season.html) |
| [`espn_wbb_conferences`](reference/site#espn_wbb_conferences) | [`espn_wbb_conferences`](https://wehoop.sportsdataverse.org/reference/espn_wbb_conferences.html) |
| [`espn_wbb_franchise`](reference/core/other#espn_wbb_franchise) | [`espn_wbb_franchise`](https://wehoop.sportsdataverse.org/reference/espn_wbb_franchise.html) |
| [`espn_wbb_franchises`](reference/core/other#espn_wbb_franchises) | [`espn_wbb_franchises`](https://wehoop.sportsdataverse.org/reference/espn_wbb_franchises.html) |
| [`espn_wbb_game_broadcasts`](reference/core/game#espn_wbb_game_broadcasts) | [`espn_wbb_game_broadcasts`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_broadcasts.html) |
| [`espn_wbb_game_odds`](reference/core/game#espn_wbb_game_odds) | [`espn_wbb_game_odds`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_odds.html) |
| [`espn_wbb_game_official_detail`](reference/core/game#espn_wbb_game_official_detail) | [`espn_wbb_game_official_detail`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_official_detail.html) |
| [`espn_wbb_game_officials`](reference/additional/play-by-play-schedule-rosters#espn_wbb_game_officials) | [`espn_wbb_game_officials`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_officials.html) |
| [`espn_wbb_game_play`](reference/core/game#espn_wbb_game_play) | [`espn_wbb_game_play`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_play.html) |
| [`espn_wbb_game_play_personnel`](reference/core/game#espn_wbb_game_play_personnel) | [`espn_wbb_game_play_personnel`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_play_personnel.html) |
| [`espn_wbb_game_powerindex`](reference/core/game#espn_wbb_game_powerindex) | [`espn_wbb_game_powerindex`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_powerindex.html) |
| [`espn_wbb_game_predictor`](reference/core/game#espn_wbb_game_predictor) | [`espn_wbb_game_predictor`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_predictor.html) |
| [`espn_wbb_game_probabilities`](reference/core/game#espn_wbb_game_probabilities) | [`espn_wbb_game_probabilities`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_probabilities.html) |
| [`espn_wbb_game_propbets`](reference/core/game#espn_wbb_game_propbets) | [`espn_wbb_game_propbets`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_propbets.html) |
| [`espn_wbb_game_rosters`](reference/additional/play-by-play-schedule-rosters#espn_wbb_game_rosters) | [`espn_wbb_game_rosters`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_rosters.html) |
| [`espn_wbb_game_situation`](reference/core/game#espn_wbb_game_situation) | [`espn_wbb_game_situation`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_situation.html) |
| [`espn_wbb_game_team_leaders`](reference/core/game#espn_wbb_game_team_leaders) | [`espn_wbb_game_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_leaders.html) |
| [`espn_wbb_game_team_linescores`](reference/core/game#espn_wbb_game_team_linescores) | [`espn_wbb_game_team_linescores`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_linescores.html) |
| [`espn_wbb_game_team_roster`](reference/core/game#espn_wbb_game_team_roster) | [`espn_wbb_game_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_roster.html) |
| [`espn_wbb_game_team_statistics`](reference/core/game#espn_wbb_game_team_statistics) | [`espn_wbb_game_team_statistics`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_statistics.html) |
| [`espn_wbb_injuries`](reference/site#espn_wbb_injuries) | [`espn_wbb_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wbb_injuries.html) |
| [`espn_wbb_leaders`](reference/web#espn_wbb_leaders) | [`espn_wbb_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wbb_leaders.html) |
| [`espn_wbb_news`](reference/site#espn_wbb_news) | [`espn_wbb_news`](https://wehoop.sportsdataverse.org/reference/espn_wbb_news.html) |
| [`espn_wbb_pbp`](reference/additional/play-by-play-schedule-rosters#espn_wbb_pbp) | [`espn_wbb_pbp`](https://wehoop.sportsdataverse.org/reference/espn_wbb_pbp.html) |
| [`espn_wbb_player_awards`](reference/core/player#espn_wbb_player_awards) | [`espn_wbb_player_awards`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_awards.html) |
| [`espn_wbb_player_career_stats`](reference/core/player#espn_wbb_player_career_stats) | [`espn_wbb_player_career_stats`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_career_stats.html) |
| [`espn_wbb_player_eventlog`](reference/core/player#espn_wbb_player_eventlog) | [`espn_wbb_player_eventlog`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_eventlog.html) |
| [`espn_wbb_player_gamelog`](reference/web#espn_wbb_player_gamelog) | [`espn_wbb_player_gamelog`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_gamelog.html) |
| [`espn_wbb_player_info`](reference/site#espn_wbb_player_info) | [`espn_wbb_player_info`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_info.html) |
| [`espn_wbb_player_overview`](reference/web#espn_wbb_player_overview) | [`espn_wbb_player_overview`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_overview.html) |
| [`espn_wbb_player_seasons`](reference/core/player#espn_wbb_player_seasons) | [`espn_wbb_player_seasons`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_seasons.html) |
| [`espn_wbb_player_splits`](reference/web#espn_wbb_player_splits) | [`espn_wbb_player_splits`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_splits.html) |
| [`espn_wbb_player_statisticslog`](reference/core/player#espn_wbb_player_statisticslog) | [`espn_wbb_player_statisticslog`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_statisticslog.html) |
| [`espn_wbb_player_stats`](reference/additional/play-by-play-schedule-rosters#espn_wbb_player_stats) | [`espn_wbb_player_stats`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_stats.html) |
| [`espn_wbb_player_stats_v3`](reference/web#espn_wbb_player_stats_v3) | [`espn_wbb_player_stats_v3`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_stats_v3.html) |
| [`espn_wbb_position`](reference/core/other#espn_wbb_position) | [`espn_wbb_position`](https://wehoop.sportsdataverse.org/reference/espn_wbb_position.html) |
| [`espn_wbb_positions`](reference/core/other#espn_wbb_positions) | [`espn_wbb_positions`](https://wehoop.sportsdataverse.org/reference/espn_wbb_positions.html) |
| [`espn_wbb_rankings`](reference/site#espn_wbb_rankings) | [`espn_wbb_rankings`](https://wehoop.sportsdataverse.org/reference/espn_wbb_rankings.html) |
| [`espn_wbb_scoreboard`](reference/site#espn_wbb_scoreboard) | [`espn_wbb_scoreboard`](https://wehoop.sportsdataverse.org/reference/espn_wbb_scoreboard.html) |
| [`espn_wbb_season_awards`](reference/core/season#espn_wbb_season_awards) | [`espn_wbb_season_awards`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_awards.html) |
| [`espn_wbb_season_group`](reference/core/season#espn_wbb_season_group) | [`espn_wbb_season_group`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_group.html) |
| [`espn_wbb_season_group_children`](reference/core/season#espn_wbb_season_group_children) | [`espn_wbb_season_group_children`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_group_children.html) |
| [`espn_wbb_season_group_teams`](reference/core/season#espn_wbb_season_group_teams) | [`espn_wbb_season_group_teams`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_group_teams.html) |
| [`espn_wbb_season_groups`](reference/core/season#espn_wbb_season_groups) | [`espn_wbb_season_groups`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_groups.html) |
| [`espn_wbb_season_info`](reference/core/season#espn_wbb_season_info) | [`espn_wbb_season_info`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_info.html) |
| [`espn_wbb_season_type`](reference/core/season#espn_wbb_season_type) | [`espn_wbb_season_type`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_type.html) |
| [`espn_wbb_season_types`](reference/core/season#espn_wbb_season_types) | [`espn_wbb_season_types`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_types.html) |
| [`espn_wbb_season_week`](reference/core/season#espn_wbb_season_week) | [`espn_wbb_season_week`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_week.html) |
| [`espn_wbb_season_weeks`](reference/core/season#espn_wbb_season_weeks) | [`espn_wbb_season_weeks`](https://wehoop.sportsdataverse.org/reference/espn_wbb_season_weeks.html) |
| [`espn_wbb_seasons`](reference/core/other#espn_wbb_seasons) | [`espn_wbb_seasons`](https://wehoop.sportsdataverse.org/reference/espn_wbb_seasons.html) |
| [`espn_wbb_standings`](reference/site#espn_wbb_standings) | [`espn_wbb_standings`](https://wehoop.sportsdataverse.org/reference/espn_wbb_standings.html) |
| [`espn_wbb_team`](reference/site#espn_wbb_team) | [`espn_wbb_team`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team.html) |
| [`espn_wbb_team_injuries`](reference/site#espn_wbb_team_injuries) | [`espn_wbb_team_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team_injuries.html) |
| [`espn_wbb_team_leaders`](reference/site#espn_wbb_team_leaders) | [`espn_wbb_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team_leaders.html) |
| [`espn_wbb_team_news`](reference/site#espn_wbb_team_news) | [`espn_wbb_team_news`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team_news.html) |
| [`espn_wbb_team_roster`](reference/site#espn_wbb_team_roster) | [`espn_wbb_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team_roster.html) |
| [`espn_wbb_team_schedule`](reference/site#espn_wbb_team_schedule) | [`espn_wbb_team_schedule`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team_schedule.html) |
| [`espn_wbb_team_stats`](reference/additional/play-by-play-schedule-rosters#espn_wbb_team_stats) | [`espn_wbb_team_stats`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team_stats.html) |
| [`espn_wbb_teams`](reference/additional/other-2#espn_wbb_teams) | [`espn_wbb_teams`](https://wehoop.sportsdataverse.org/reference/espn_wbb_teams.html) |
| [`espn_wbb_tournaments`](reference/core/other#espn_wbb_tournaments) | [`espn_wbb_tournaments`](https://wehoop.sportsdataverse.org/reference/espn_wbb_tournaments.html) |
| [`espn_wbb_venues`](reference/core/other#espn_wbb_venues) | [`espn_wbb_venues`](https://wehoop.sportsdataverse.org/reference/espn_wbb_venues.html) |
| [`fox_wbb_boxscore`](reference/additional/fox#fox_wbb_boxscore) | [`fox_wbb_boxscore`](https://wehoop.sportsdataverse.org/reference/fox_wbb_boxscore.html) |
| [`fox_wbb_league_leaders`](reference/additional/fox#fox_wbb_league_leaders) | [`fox_wbb_league_leaders`](https://wehoop.sportsdataverse.org/reference/fox_wbb_league_leaders.html) |
| [`fox_wbb_odds`](reference/additional/fox#fox_wbb_odds) | [`fox_wbb_odds`](https://wehoop.sportsdataverse.org/reference/fox_wbb_odds.html) |
| [`fox_wbb_pbp`](reference/additional/fox#fox_wbb_pbp) | [`fox_wbb_pbp`](https://wehoop.sportsdataverse.org/reference/fox_wbb_pbp.html) |
| [`fox_wbb_standings`](reference/additional/fox#fox_wbb_standings) | [`fox_wbb_standings`](https://wehoop.sportsdataverse.org/reference/fox_wbb_standings.html) |
| [`fox_wbb_team_gamelog`](reference/additional/fox#fox_wbb_team_gamelog) | [`fox_wbb_team_gamelog`](https://wehoop.sportsdataverse.org/reference/fox_wbb_team_gamelog.html) |
| [`fox_wbb_team_roster`](reference/additional/fox#fox_wbb_team_roster) | [`fox_wbb_team_roster`](https://wehoop.sportsdataverse.org/reference/fox_wbb_team_roster.html) |
| [`fox_wbb_team_stats`](reference/additional/fox#fox_wbb_team_stats) | [`fox_wbb_team_stats`](https://wehoop.sportsdataverse.org/reference/fox_wbb_team_stats.html) |
| [`fox_wbb_teams`](reference/additional/fox#fox_wbb_teams) | [`fox_wbb_teams`](https://wehoop.sportsdataverse.org/reference/fox_wbb_teams.html) |
| [`fox_wbb_teams_all`](reference/additional/fox#fox_wbb_teams_all) | [`fox_wbb_teams_all`](https://wehoop.sportsdataverse.org/reference/fox_wbb_teams_all.html) |
| [`load_ncaa_wbb_lineups`](reference/loaders/ncaa#load_ncaa_wbb_lineups) | [`load_ncaa_wbb_lineups`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_lineups.html) |
| [`load_ncaa_wbb_matchup_stints`](reference/loaders/ncaa#load_ncaa_wbb_matchup_stints) | [`load_ncaa_wbb_matchup_stints`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_matchup_stints.html) |
| [`load_ncaa_wbb_pbp`](reference/loaders/ncaa#load_ncaa_wbb_pbp) | [`load_ncaa_wbb_pbp`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_pbp.html) |
| [`load_ncaa_wbb_player_box`](reference/loaders/ncaa#load_ncaa_wbb_player_box) | [`load_ncaa_wbb_player_box`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_player_box.html) |
| [`load_ncaa_wbb_possessions`](reference/loaders/ncaa#load_ncaa_wbb_possessions) | [`load_ncaa_wbb_possessions`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_possessions.html) |
| [`load_ncaa_wbb_rapm`](reference/loaders/ncaa#load_ncaa_wbb_rapm) | [`load_ncaa_wbb_rapm`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_rapm.html) |
| [`load_ncaa_wbb_rapm_within_team`](reference/loaders/ncaa#load_ncaa_wbb_rapm_within_team) | [`load_ncaa_wbb_rapm_within_team`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_rapm_within_team.html) |
| [`load_ncaa_wbb_rosters`](reference/loaders/ncaa#load_ncaa_wbb_rosters) | [`load_ncaa_wbb_rosters`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_rosters.html) |
| [`load_ncaa_wbb_schedule`](reference/loaders/ncaa#load_ncaa_wbb_schedule) | [`load_ncaa_wbb_schedule`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_schedule.html) |
| [`load_ncaa_wbb_shots`](reference/loaders/ncaa#load_ncaa_wbb_shots) | [`load_ncaa_wbb_shots`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_shots.html) |
| [`load_ncaa_wbb_team_box`](reference/loaders/ncaa#load_ncaa_wbb_team_box) | [`load_ncaa_wbb_team_box`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_team_box.html) |
| [`load_ncaa_wbb_team_ids`](reference/loaders/ncaa#load_ncaa_wbb_team_ids) | [`load_ncaa_wbb_team_ids`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_team_ids.html) |
| [`load_ncaa_wbb_team_rosters`](reference/loaders/ncaa#load_ncaa_wbb_team_rosters) | [`load_ncaa_wbb_team_rosters`](https://wehoop.sportsdataverse.org/reference/load_ncaa_wbb_team_rosters.html) |
| [`load_wbb_game_rosters`](reference/loaders/other#load_wbb_game_rosters) | [`load_wbb_game_rosters`](https://wehoop.sportsdataverse.org/reference/load_wbb_game_rosters.html) |
| [`load_wbb_group_aliases`](reference/loaders/other#load_wbb_group_aliases) | [`load_wbb_group_aliases`](https://wehoop.sportsdataverse.org/reference/load_wbb_group_aliases.html) |
| [`load_wbb_group_seasons`](reference/loaders/other#load_wbb_group_seasons) | [`load_wbb_group_seasons`](https://wehoop.sportsdataverse.org/reference/load_wbb_group_seasons.html) |
| [`load_wbb_groups`](reference/loaders/other#load_wbb_groups) | [`load_wbb_groups`](https://wehoop.sportsdataverse.org/reference/load_wbb_groups.html) |
| [`load_wbb_officials`](reference/loaders/other#load_wbb_officials) | [`load_wbb_officials`](https://wehoop.sportsdataverse.org/reference/load_wbb_officials.html) |
| [`load_wbb_pbp`](reference/loaders/other#load_wbb_pbp) | [`load_wbb_pbp`](https://wehoop.sportsdataverse.org/reference/load_wbb_pbp.html) |
| [`load_wbb_player_core`](reference/loaders/player#load_wbb_player_core) | [`load_wbb_player_core`](https://wehoop.sportsdataverse.org/reference/load_wbb_player_core.html) |
| [`load_wbb_player_crosswalk`](reference/loaders/player#load_wbb_player_crosswalk) | [`load_wbb_player_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wbb_player_crosswalk.html) |
| [`load_wbb_player_value`](reference/loaders/player#load_wbb_player_value) | [`load_wbb_player_value`](https://wehoop.sportsdataverse.org/reference/load_wbb_player_value.html) |
| [`load_wbb_ratings`](reference/loaders/other#load_wbb_ratings) | [`load_wbb_ratings`](https://wehoop.sportsdataverse.org/reference/load_wbb_ratings.html) |
| [`load_wbb_rosters`](reference/loaders/other#load_wbb_rosters) | [`load_wbb_rosters`](https://wehoop.sportsdataverse.org/reference/load_wbb_rosters.html) |
| [`load_wbb_schedule`](reference/loaders/other#load_wbb_schedule) | [`load_wbb_schedule`](https://wehoop.sportsdataverse.org/reference/load_wbb_schedule.html) |
| [`load_wbb_schedule_crosswalk`](reference/loaders/other#load_wbb_schedule_crosswalk) | [`load_wbb_schedule_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wbb_schedule_crosswalk.html) |
| [`load_wbb_shots`](reference/loaders/other#load_wbb_shots) | [`load_wbb_shots`](https://wehoop.sportsdataverse.org/reference/load_wbb_shots.html) |
| [`load_wbb_standings`](reference/loaders/other#load_wbb_standings) | [`load_wbb_standings`](https://wehoop.sportsdataverse.org/reference/load_wbb_standings.html) |
| [`load_wbb_team_crosswalk`](reference/loaders/team#load_wbb_team_crosswalk) | [`load_wbb_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wbb_team_crosswalk.html) |
| [`load_wbb_team_group_seasons`](reference/loaders/team#load_wbb_team_group_seasons) | [`load_wbb_team_group_seasons`](https://wehoop.sportsdataverse.org/reference/load_wbb_team_group_seasons.html) |
| [`most_recent_wbb_season`](reference/additional/other#most_recent_wbb_season) | [`most_recent_wbb_season`](https://wehoop.sportsdataverse.org/reference/most_recent_wbb_season.html) |
| [`wbb_player_crosswalk`](reference/additional/wbb#wbb_player_crosswalk) | [`wbb_player_crosswalk`](https://wehoop.sportsdataverse.org/reference/wbb_player_crosswalk.html) |
| [`wbb_schedule_crosswalk`](reference/additional/wbb#wbb_schedule_crosswalk) | [`wbb_schedule_crosswalk`](https://wehoop.sportsdataverse.org/reference/wbb_schedule_crosswalk.html) |
| [`wbb_team_crosswalk`](reference/additional/wbb#wbb_team_crosswalk) | [`wbb_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/wbb_team_crosswalk.html) |
