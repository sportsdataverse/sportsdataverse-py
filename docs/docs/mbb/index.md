---
title: MBB
sidebar_label: MBB
description: "sdv-py MBB: endpoint references, dataset loaders and parsers for MBB in the SportsDataverse Python package."
---
# MBB (`sportsdataverse.mbb`)

| Reference | Functions | Base URL |
|---|---:|---|
| [Highlights](reference/additional#highlights) | 9 | curated "start here" functions |
| [ESPN site API (v2)](reference/site) | 25 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 87 | `https://sports.core.api.espn.com/v2/sports` |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 | `https://site.web.api.espn.com/apis/fitt/v3/sports` |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 | `https://cdn.espn.com/core` |
| [Bart Torvik T-Rank (barttorvik.com)](reference/torvik) | 5 | `https://barttorvik.com` |
| [KenPom (kenpom.com, subscription)](reference/kenpom) | 30 | `https://kenpom.com` |
| [Dataset loaders](reference/loaders) | 34 | sportsdataverse-data releases |
| [Additional functions](reference/additional) | 320 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [MBB tutorial](../tutorials/06_mbb_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.mbb` (Python) | `hoopR` (R) |
|---|---|
| [`espn_mbb_award`](reference/core/other#espn_mbb_award) | [`espn_mbb_award`](https://hoopR.sportsdataverse.org/reference/espn_mbb_award.html) |
| [`espn_mbb_calendar`](reference/site#espn_mbb_calendar) | [`espn_mbb_calendar`](https://hoopR.sportsdataverse.org/reference/espn_mbb_calendar.html) |
| [`espn_mbb_coach`](reference/core/other#espn_mbb_coach) | [`espn_mbb_coach`](https://hoopR.sportsdataverse.org/reference/espn_mbb_coach.html) |
| [`espn_mbb_coach_record`](reference/core/other#espn_mbb_coach_record) | [`espn_mbb_coach_record`](https://hoopR.sportsdataverse.org/reference/espn_mbb_coach_record.html) |
| [`espn_mbb_coach_season`](reference/core/other#espn_mbb_coach_season) | [`espn_mbb_coach_season`](https://hoopR.sportsdataverse.org/reference/espn_mbb_coach_season.html) |
| [`espn_mbb_conferences`](reference/site#espn_mbb_conferences) | [`espn_mbb_conferences`](https://hoopR.sportsdataverse.org/reference/espn_mbb_conferences.html) |
| [`espn_mbb_franchise`](reference/core/other#espn_mbb_franchise) | [`espn_mbb_franchise`](https://hoopR.sportsdataverse.org/reference/espn_mbb_franchise.html) |
| [`espn_mbb_franchises`](reference/core/other#espn_mbb_franchises) | [`espn_mbb_franchises`](https://hoopR.sportsdataverse.org/reference/espn_mbb_franchises.html) |
| [`espn_mbb_game_broadcasts`](reference/core/game#espn_mbb_game_broadcasts) | [`espn_mbb_game_broadcasts`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_broadcasts.html) |
| [`espn_mbb_game_odds`](reference/core/game#espn_mbb_game_odds) | [`espn_mbb_game_odds`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_odds.html) |
| [`espn_mbb_game_official_detail`](reference/core/game#espn_mbb_game_official_detail) | [`espn_mbb_game_official_detail`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_official_detail.html) |
| [`espn_mbb_game_officials`](reference/core/game#espn_mbb_game_officials) | [`espn_mbb_game_officials`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_officials.html) |
| [`espn_mbb_game_play`](reference/core/game#espn_mbb_game_play) | [`espn_mbb_game_play`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_play.html) |
| [`espn_mbb_game_play_personnel`](reference/core/game#espn_mbb_game_play_personnel) | [`espn_mbb_game_play_personnel`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_play_personnel.html) |
| [`espn_mbb_game_powerindex`](reference/core/game#espn_mbb_game_powerindex) | [`espn_mbb_game_powerindex`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_powerindex.html) |
| [`espn_mbb_game_predictor`](reference/core/game#espn_mbb_game_predictor) | [`espn_mbb_game_predictor`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_predictor.html) |
| [`espn_mbb_game_probabilities`](reference/core/game#espn_mbb_game_probabilities) | [`espn_mbb_game_probabilities`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_probabilities.html) |
| [`espn_mbb_game_propbets`](reference/core/game#espn_mbb_game_propbets) | [`espn_mbb_game_propbets`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_propbets.html) |
| [`espn_mbb_game_rosters`](reference/additional/highlights#espn_mbb_game_rosters) | [`espn_mbb_game_rosters`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_rosters.html) |
| [`espn_mbb_game_situation`](reference/core/game#espn_mbb_game_situation) | [`espn_mbb_game_situation`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_situation.html) |
| [`espn_mbb_game_team_leaders`](reference/core/game#espn_mbb_game_team_leaders) | [`espn_mbb_game_team_leaders`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_team_leaders.html) |
| [`espn_mbb_game_team_linescores`](reference/core/game#espn_mbb_game_team_linescores) | [`espn_mbb_game_team_linescores`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_team_linescores.html) |
| [`espn_mbb_game_team_roster`](reference/core/game#espn_mbb_game_team_roster) | [`espn_mbb_game_team_roster`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_team_roster.html) |
| [`espn_mbb_game_team_statistics`](reference/core/game#espn_mbb_game_team_statistics) | [`espn_mbb_game_team_statistics`](https://hoopR.sportsdataverse.org/reference/espn_mbb_game_team_statistics.html) |
| [`espn_mbb_injuries`](reference/site#espn_mbb_injuries) | [`espn_mbb_injuries`](https://hoopR.sportsdataverse.org/reference/espn_mbb_injuries.html) |
| [`espn_mbb_leaders`](reference/web#espn_mbb_leaders) | [`espn_mbb_leaders`](https://hoopR.sportsdataverse.org/reference/espn_mbb_leaders.html) |
| [`espn_mbb_news`](reference/site#espn_mbb_news) | [`espn_mbb_news`](https://hoopR.sportsdataverse.org/reference/espn_mbb_news.html) |
| [`espn_mbb_pbp`](reference/additional/highlights#espn_mbb_pbp) | [`espn_mbb_pbp`](https://hoopR.sportsdataverse.org/reference/espn_mbb_pbp.html) |
| [`espn_mbb_player_awards`](reference/core/player#espn_mbb_player_awards) | [`espn_mbb_player_awards`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_awards.html) |
| [`espn_mbb_player_career_stats`](reference/core/player#espn_mbb_player_career_stats) | [`espn_mbb_player_career_stats`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_career_stats.html) |
| [`espn_mbb_player_eventlog`](reference/core/player#espn_mbb_player_eventlog) | [`espn_mbb_player_eventlog`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_eventlog.html) |
| [`espn_mbb_player_gamelog`](reference/web#espn_mbb_player_gamelog) | [`espn_mbb_player_gamelog`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_gamelog.html) |
| [`espn_mbb_player_info`](reference/site#espn_mbb_player_info) | [`espn_mbb_player_info`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_info.html) |
| [`espn_mbb_player_overview`](reference/web#espn_mbb_player_overview) | [`espn_mbb_player_overview`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_overview.html) |
| [`espn_mbb_player_seasons`](reference/core/player#espn_mbb_player_seasons) | [`espn_mbb_player_seasons`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_seasons.html) |
| [`espn_mbb_player_splits`](reference/web#espn_mbb_player_splits) | [`espn_mbb_player_splits`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_splits.html) |
| [`espn_mbb_player_statisticslog`](reference/core/player#espn_mbb_player_statisticslog) | [`espn_mbb_player_statisticslog`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_statisticslog.html) |
| [`espn_mbb_player_stats`](reference/additional/highlights#espn_mbb_player_stats) | [`espn_mbb_player_stats`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_stats.html) |
| [`espn_mbb_player_stats_v3`](reference/web#espn_mbb_player_stats_v3) | [`espn_mbb_player_stats_v3`](https://hoopR.sportsdataverse.org/reference/espn_mbb_player_stats_v3.html) |
| [`espn_mbb_position`](reference/core/other#espn_mbb_position) | [`espn_mbb_position`](https://hoopR.sportsdataverse.org/reference/espn_mbb_position.html) |
| [`espn_mbb_positions`](reference/core/other#espn_mbb_positions) | [`espn_mbb_positions`](https://hoopR.sportsdataverse.org/reference/espn_mbb_positions.html) |
| [`espn_mbb_rankings`](reference/site#espn_mbb_rankings) | [`espn_mbb_rankings`](https://hoopR.sportsdataverse.org/reference/espn_mbb_rankings.html) |
| [`espn_mbb_scoreboard`](reference/site#espn_mbb_scoreboard) | [`espn_mbb_scoreboard`](https://hoopR.sportsdataverse.org/reference/espn_mbb_scoreboard.html) |
| [`espn_mbb_season_awards`](reference/core/season#espn_mbb_season_awards) | [`espn_mbb_season_awards`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_awards.html) |
| [`espn_mbb_season_group`](reference/core/season#espn_mbb_season_group) | [`espn_mbb_season_group`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_group.html) |
| [`espn_mbb_season_group_children`](reference/core/season#espn_mbb_season_group_children) | [`espn_mbb_season_group_children`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_group_children.html) |
| [`espn_mbb_season_group_teams`](reference/core/season#espn_mbb_season_group_teams) | [`espn_mbb_season_group_teams`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_group_teams.html) |
| [`espn_mbb_season_groups`](reference/core/season#espn_mbb_season_groups) | [`espn_mbb_season_groups`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_groups.html) |
| [`espn_mbb_season_info`](reference/core/season#espn_mbb_season_info) | [`espn_mbb_season_info`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_info.html) |
| [`espn_mbb_season_type`](reference/core/season#espn_mbb_season_type) | [`espn_mbb_season_type`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_type.html) |
| [`espn_mbb_season_types`](reference/core/season#espn_mbb_season_types) | [`espn_mbb_season_types`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_types.html) |
| [`espn_mbb_season_week`](reference/core/season#espn_mbb_season_week) | [`espn_mbb_season_week`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_week.html) |
| [`espn_mbb_season_weeks`](reference/core/season#espn_mbb_season_weeks) | [`espn_mbb_season_weeks`](https://hoopR.sportsdataverse.org/reference/espn_mbb_season_weeks.html) |
| [`espn_mbb_seasons`](reference/core/other#espn_mbb_seasons) | [`espn_mbb_seasons`](https://hoopR.sportsdataverse.org/reference/espn_mbb_seasons.html) |
| [`espn_mbb_standings`](reference/site#espn_mbb_standings) | [`espn_mbb_standings`](https://hoopR.sportsdataverse.org/reference/espn_mbb_standings.html) |
| [`espn_mbb_team`](reference/site#espn_mbb_team) | [`espn_mbb_team`](https://hoopR.sportsdataverse.org/reference/espn_mbb_team.html) |
| [`espn_mbb_team_injuries`](reference/site#espn_mbb_team_injuries) | [`espn_mbb_team_injuries`](https://hoopR.sportsdataverse.org/reference/espn_mbb_team_injuries.html) |
| [`espn_mbb_team_leaders`](reference/site#espn_mbb_team_leaders) | [`espn_mbb_team_leaders`](https://hoopR.sportsdataverse.org/reference/espn_mbb_team_leaders.html) |
| [`espn_mbb_team_news`](reference/site#espn_mbb_team_news) | [`espn_mbb_team_news`](https://hoopR.sportsdataverse.org/reference/espn_mbb_team_news.html) |
| [`espn_mbb_team_record`](reference/site#espn_mbb_team_record) | [`espn_mbb_team_record`](https://hoopR.sportsdataverse.org/reference/espn_mbb_team_record.html) |
| [`espn_mbb_team_roster`](reference/site#espn_mbb_team_roster) | [`espn_mbb_team_roster`](https://hoopR.sportsdataverse.org/reference/espn_mbb_team_roster.html) |
| [`espn_mbb_team_schedule`](reference/site#espn_mbb_team_schedule) | [`espn_mbb_team_schedule`](https://hoopR.sportsdataverse.org/reference/espn_mbb_team_schedule.html) |
| [`espn_mbb_teams`](reference/additional/highlights#espn_mbb_teams) | [`espn_mbb_teams`](https://hoopR.sportsdataverse.org/reference/espn_mbb_teams.html) |
| [`espn_mbb_tournaments`](reference/core/other#espn_mbb_tournaments) | [`espn_mbb_tournaments`](https://hoopR.sportsdataverse.org/reference/espn_mbb_tournaments.html) |
| [`espn_mbb_venues`](reference/core/other#espn_mbb_venues) | [`espn_mbb_venues`](https://hoopR.sportsdataverse.org/reference/espn_mbb_venues.html) |
| [`fox_mbb_boxscore`](reference/additional/fox#fox_mbb_boxscore) | [`fox_mbb_boxscore`](https://hoopR.sportsdataverse.org/reference/fox_mbb_boxscore.html) |
| [`fox_mbb_league_leaders`](reference/additional/fox#fox_mbb_league_leaders) | [`fox_mbb_league_leaders`](https://hoopR.sportsdataverse.org/reference/fox_mbb_league_leaders.html) |
| [`fox_mbb_odds`](reference/additional/fox#fox_mbb_odds) | [`fox_mbb_odds`](https://hoopR.sportsdataverse.org/reference/fox_mbb_odds.html) |
| [`fox_mbb_pbp`](reference/additional/fox#fox_mbb_pbp) | [`fox_mbb_pbp`](https://hoopR.sportsdataverse.org/reference/fox_mbb_pbp.html) |
| [`fox_mbb_standings`](reference/additional/fox#fox_mbb_standings) | [`fox_mbb_standings`](https://hoopR.sportsdataverse.org/reference/fox_mbb_standings.html) |
| [`fox_mbb_team_gamelog`](reference/additional/fox#fox_mbb_team_gamelog) | [`fox_mbb_team_gamelog`](https://hoopR.sportsdataverse.org/reference/fox_mbb_team_gamelog.html) |
| [`fox_mbb_team_roster`](reference/additional/fox#fox_mbb_team_roster) | [`fox_mbb_team_roster`](https://hoopR.sportsdataverse.org/reference/fox_mbb_team_roster.html) |
| [`fox_mbb_team_stats`](reference/additional/fox#fox_mbb_team_stats) | [`fox_mbb_team_stats`](https://hoopR.sportsdataverse.org/reference/fox_mbb_team_stats.html) |
| [`fox_mbb_teams`](reference/additional/fox#fox_mbb_teams) | [`fox_mbb_teams`](https://hoopR.sportsdataverse.org/reference/fox_mbb_teams.html) |
| [`fox_mbb_teams_all`](reference/additional/fox#fox_mbb_teams_all) | [`fox_mbb_teams_all`](https://hoopR.sportsdataverse.org/reference/fox_mbb_teams_all.html) |
| [`load_mbb_game_rosters`](reference/loaders/other#load_mbb_game_rosters) | [`load_mbb_game_rosters`](https://hoopR.sportsdataverse.org/reference/load_mbb_game_rosters.html) |
| [`load_mbb_group_aliases`](reference/loaders/other#load_mbb_group_aliases) | [`load_mbb_group_aliases`](https://hoopR.sportsdataverse.org/reference/load_mbb_group_aliases.html) |
| [`load_mbb_group_seasons`](reference/loaders/other#load_mbb_group_seasons) | [`load_mbb_group_seasons`](https://hoopR.sportsdataverse.org/reference/load_mbb_group_seasons.html) |
| [`load_mbb_groups`](reference/loaders/other#load_mbb_groups) | [`load_mbb_groups`](https://hoopR.sportsdataverse.org/reference/load_mbb_groups.html) |
| [`load_mbb_officials`](reference/loaders/other#load_mbb_officials) | [`load_mbb_officials`](https://hoopR.sportsdataverse.org/reference/load_mbb_officials.html) |
| [`load_mbb_pbp`](reference/loaders/other#load_mbb_pbp) | [`load_mbb_pbp`](https://hoopR.sportsdataverse.org/reference/load_mbb_pbp.html) |
| [`load_mbb_player_core`](reference/loaders/player#load_mbb_player_core) | [`load_mbb_player_core`](https://hoopR.sportsdataverse.org/reference/load_mbb_player_core.html) |
| [`load_mbb_player_crosswalk`](reference/loaders/player#load_mbb_player_crosswalk) | [`load_mbb_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_mbb_player_crosswalk.html) |
| [`load_mbb_player_value`](reference/loaders/player#load_mbb_player_value) | [`load_mbb_player_value`](https://hoopR.sportsdataverse.org/reference/load_mbb_player_value.html) |
| [`load_mbb_ratings`](reference/loaders/other#load_mbb_ratings) | [`load_mbb_ratings`](https://hoopR.sportsdataverse.org/reference/load_mbb_ratings.html) |
| [`load_mbb_rosters`](reference/loaders/other#load_mbb_rosters) | [`load_mbb_rosters`](https://hoopR.sportsdataverse.org/reference/load_mbb_rosters.html) |
| [`load_mbb_schedule`](reference/loaders/other#load_mbb_schedule) | [`load_mbb_schedule`](https://hoopR.sportsdataverse.org/reference/load_mbb_schedule.html) |
| [`load_mbb_schedule_crosswalk`](reference/loaders/other#load_mbb_schedule_crosswalk) | [`load_mbb_schedule_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_mbb_schedule_crosswalk.html) |
| [`load_mbb_shots`](reference/loaders/other#load_mbb_shots) | [`load_mbb_shots`](https://hoopR.sportsdataverse.org/reference/load_mbb_shots.html) |
| [`load_mbb_standings`](reference/loaders/other#load_mbb_standings) | [`load_mbb_standings`](https://hoopR.sportsdataverse.org/reference/load_mbb_standings.html) |
| [`load_mbb_team_crosswalk`](reference/loaders/team#load_mbb_team_crosswalk) | [`load_mbb_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_mbb_team_crosswalk.html) |
| [`load_mbb_team_group_seasons`](reference/loaders/team#load_mbb_team_group_seasons) | [`load_mbb_team_group_seasons`](https://hoopR.sportsdataverse.org/reference/load_mbb_team_group_seasons.html) |
| [`load_ncaa_mbb_lineups`](reference/loaders/ncaa#load_ncaa_mbb_lineups) | [`load_ncaa_mbb_lineups`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_lineups.html) |
| [`load_ncaa_mbb_matchup_stints`](reference/loaders/ncaa#load_ncaa_mbb_matchup_stints) | [`load_ncaa_mbb_matchup_stints`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_matchup_stints.html) |
| [`load_ncaa_mbb_pbp`](reference/loaders/ncaa#load_ncaa_mbb_pbp) | [`load_ncaa_mbb_pbp`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_pbp.html) |
| [`load_ncaa_mbb_player_box`](reference/loaders/ncaa#load_ncaa_mbb_player_box) | [`load_ncaa_mbb_player_box`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_player_box.html) |
| [`load_ncaa_mbb_possessions`](reference/loaders/ncaa#load_ncaa_mbb_possessions) | [`load_ncaa_mbb_possessions`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_possessions.html) |
| [`load_ncaa_mbb_rapm`](reference/loaders/ncaa#load_ncaa_mbb_rapm) | [`load_ncaa_mbb_rapm`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_rapm.html) |
| [`load_ncaa_mbb_rapm_within_team`](reference/loaders/ncaa#load_ncaa_mbb_rapm_within_team) | [`load_ncaa_mbb_rapm_within_team`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_rapm_within_team.html) |
| [`load_ncaa_mbb_rosters`](reference/loaders/ncaa#load_ncaa_mbb_rosters) | [`load_ncaa_mbb_rosters`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_rosters.html) |
| [`load_ncaa_mbb_schedule`](reference/loaders/ncaa#load_ncaa_mbb_schedule) | [`load_ncaa_mbb_schedule`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_schedule.html) |
| [`load_ncaa_mbb_shots`](reference/loaders/ncaa#load_ncaa_mbb_shots) | [`load_ncaa_mbb_shots`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_shots.html) |
| [`load_ncaa_mbb_team_box`](reference/loaders/ncaa#load_ncaa_mbb_team_box) | [`load_ncaa_mbb_team_box`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_team_box.html) |
| [`load_ncaa_mbb_team_ids`](reference/loaders/ncaa#load_ncaa_mbb_team_ids) | [`load_ncaa_mbb_team_ids`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_team_ids.html) |
| [`load_ncaa_mbb_team_rosters`](reference/loaders/ncaa#load_ncaa_mbb_team_rosters) | [`load_ncaa_mbb_team_rosters`](https://hoopR.sportsdataverse.org/reference/load_ncaa_mbb_team_rosters.html) |
| [`mbb_player_crosswalk`](reference/additional/mbb#mbb_player_crosswalk) | [`mbb_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/mbb_player_crosswalk.html) |
| [`mbb_schedule_crosswalk`](reference/additional/mbb#mbb_schedule_crosswalk) | [`mbb_schedule_crosswalk`](https://hoopR.sportsdataverse.org/reference/mbb_schedule_crosswalk.html) |
| [`mbb_team_crosswalk`](reference/additional/mbb#mbb_team_crosswalk) | [`mbb_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/mbb_team_crosswalk.html) |
| [`most_recent_mbb_season`](reference/additional/highlights#most_recent_mbb_season) | [`most_recent_mbb_season`](https://hoopR.sportsdataverse.org/reference/most_recent_mbb_season.html) |
| [`torvik_game_schedule`](reference/torvik#torvik_game_schedule) | [`torvik_game_schedule`](https://hoopR.sportsdataverse.org/reference/torvik_game_schedule.html) |
| [`torvik_game_stats`](reference/torvik#torvik_game_stats) | [`torvik_game_stats`](https://hoopR.sportsdataverse.org/reference/torvik_game_stats.html) |
| [`torvik_player_stats`](reference/torvik#torvik_player_stats) | [`torvik_player_stats`](https://hoopR.sportsdataverse.org/reference/torvik_player_stats.html) |
| [`torvik_ratings`](reference/torvik#torvik_ratings) | [`torvik_ratings`](https://hoopR.sportsdataverse.org/reference/torvik_ratings.html) |
| [`torvik_team_factors`](reference/torvik#torvik_team_factors) | [`torvik_team_factors`](https://hoopR.sportsdataverse.org/reference/torvik_team_factors.html) |
