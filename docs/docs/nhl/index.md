---
title: NHL
sidebar_label: NHL
---
# NHL (`sportsdataverse.nhl`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 28 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 82 | `https://sports.core.api.espn.com/v2/sports` |
| [NHL Web API](reference/nhl_api_web) | 27 | `https://api-web.nhle.com` |
| [NHL EDGE API](reference/nhl_edge) | 35 | `https://api-web.nhle.com` |
| [NHL Stats REST API](reference/nhl_stats_rest) | 21 | `https://api.nhle.com/stats/rest` |
| [NHL Records API](reference/nhl_records) | 44 | `https://records.nhl.com/site/api` |
| [Dataset loaders](reference/loaders) | 23 | sportsdataverse-data releases |
| [Additional functions](reference/additional) | 21 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [NHL tutorial](../tutorials/07_nhl_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.nhl` (Python) | `fastRhockey` (R) |
|---|---|
| [`espn_nhl_award`](reference/core#espn_nhl_award) | [`espn_nhl_award`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_award.html) |
| [`espn_nhl_awards`](reference/core#espn_nhl_awards) | [`espn_nhl_awards`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_awards.html) |
| [`espn_nhl_calendar`](reference/site#espn_nhl_calendar) | [`espn_nhl_calendar`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_calendar.html) |
| [`espn_nhl_calendar_offseason`](reference/site#espn_nhl_calendar_offseason) | [`espn_nhl_calendar_offseason`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_calendar_offseason.html) |
| [`espn_nhl_calendar_ondays`](reference/site#espn_nhl_calendar_ondays) | [`espn_nhl_calendar_ondays`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_calendar_ondays.html) |
| [`espn_nhl_calendar_postseason`](reference/site#espn_nhl_calendar_postseason) | [`espn_nhl_calendar_postseason`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_calendar_postseason.html) |
| [`espn_nhl_calendar_regular_season`](reference/site#espn_nhl_calendar_regular_season) | [`espn_nhl_calendar_regular_season`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_calendar_regular_season.html) |
| [`espn_nhl_coach`](reference/core#espn_nhl_coach) | [`espn_nhl_coach`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_coach.html) |
| [`espn_nhl_coach_record`](reference/core#espn_nhl_coach_record) | [`espn_nhl_coach_record`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_coach_record.html) |
| [`espn_nhl_coach_season`](reference/core#espn_nhl_coach_season) | [`espn_nhl_coach_season`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_coach_season.html) |
| [`espn_nhl_coaches`](reference/core#espn_nhl_coaches) | [`espn_nhl_coaches`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_coaches.html) |
| [`espn_nhl_conferences`](reference/site#espn_nhl_conferences) | [`espn_nhl_conferences`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_conferences.html) |
| [`espn_nhl_draft`](reference/site#espn_nhl_draft) | [`espn_nhl_draft`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_draft.html) |
| [`espn_nhl_franchise`](reference/core#espn_nhl_franchise) | [`espn_nhl_franchise`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_franchise.html) |
| [`espn_nhl_franchises`](reference/core#espn_nhl_franchises) | [`espn_nhl_franchises`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_franchises.html) |
| [`espn_nhl_game`](reference/core#espn_nhl_game) | [`espn_nhl_game`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game.html) |
| [`espn_nhl_game_broadcasts`](reference/core#espn_nhl_game_broadcasts) | [`espn_nhl_game_broadcasts`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_broadcasts.html) |
| [`espn_nhl_game_competition`](reference/core#espn_nhl_game_competition) | [`espn_nhl_game_competition`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_competition.html) |
| [`espn_nhl_game_leaders`](reference/core#espn_nhl_game_leaders) | [`espn_nhl_game_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_leaders.html) |
| [`espn_nhl_game_odds`](reference/core#espn_nhl_game_odds) | [`espn_nhl_game_odds`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_odds.html) |
| [`espn_nhl_game_official_detail`](reference/core#espn_nhl_game_official_detail) | [`espn_nhl_game_official_detail`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_official_detail.html) |
| [`espn_nhl_game_officials`](reference/core#espn_nhl_game_officials) | [`espn_nhl_game_officials`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_officials.html) |
| [`espn_nhl_game_play`](reference/core#espn_nhl_game_play) | [`espn_nhl_game_play`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_play.html) |
| [`espn_nhl_game_play_personnel`](reference/core#espn_nhl_game_play_personnel) | [`espn_nhl_game_play_personnel`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_play_personnel.html) |
| [`espn_nhl_game_plays`](reference/core#espn_nhl_game_plays) | [`espn_nhl_game_plays`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_plays.html) |
| [`espn_nhl_game_powerindex`](reference/core#espn_nhl_game_powerindex) | [`espn_nhl_game_powerindex`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_powerindex.html) |
| [`espn_nhl_game_predictor`](reference/core#espn_nhl_game_predictor) | [`espn_nhl_game_predictor`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_predictor.html) |
| [`espn_nhl_game_probabilities`](reference/core#espn_nhl_game_probabilities) | [`espn_nhl_game_probabilities`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_probabilities.html) |
| [`espn_nhl_game_propbets`](reference/core#espn_nhl_game_propbets) | [`espn_nhl_game_propbets`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_propbets.html) |
| [`espn_nhl_game_scoringplays`](reference/core#espn_nhl_game_scoringplays) | [`espn_nhl_game_scoringplays`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_scoringplays.html) |
| [`espn_nhl_game_situation`](reference/core#espn_nhl_game_situation) | [`espn_nhl_game_situation`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_situation.html) |
| [`espn_nhl_game_status`](reference/core#espn_nhl_game_status) | [`espn_nhl_game_status`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_status.html) |
| [`espn_nhl_game_team`](reference/core#espn_nhl_game_team) | [`espn_nhl_game_team`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_team.html) |
| [`espn_nhl_game_team_leaders`](reference/core#espn_nhl_game_team_leaders) | [`espn_nhl_game_team_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_team_leaders.html) |
| [`espn_nhl_game_team_linescores`](reference/core#espn_nhl_game_team_linescores) | [`espn_nhl_game_team_linescores`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_team_linescores.html) |
| [`espn_nhl_game_team_record`](reference/core#espn_nhl_game_team_record) | [`espn_nhl_game_team_record`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_team_record.html) |
| [`espn_nhl_game_team_roster`](reference/core#espn_nhl_game_team_roster) | [`espn_nhl_game_team_roster`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_team_roster.html) |
| [`espn_nhl_game_team_statistics`](reference/core#espn_nhl_game_team_statistics) | [`espn_nhl_game_team_statistics`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_team_statistics.html) |
| [`espn_nhl_game_teams`](reference/core#espn_nhl_game_teams) | [`espn_nhl_game_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_game_teams.html) |
| [`espn_nhl_games`](reference/core#espn_nhl_games) | [`espn_nhl_games`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_games.html) |
| [`espn_nhl_injuries`](reference/site#espn_nhl_injuries) | [`espn_nhl_injuries`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_injuries.html) |
| [`espn_nhl_leaders`](reference/web#espn_nhl_leaders) | [`espn_nhl_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_leaders.html) |
| [`espn_nhl_leaders_core`](reference/core#espn_nhl_leaders_core) | [`espn_nhl_leaders_core`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_leaders_core.html) |
| [`espn_nhl_league_notes`](reference/core#espn_nhl_league_notes) | [`espn_nhl_league_notes`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_league_notes.html) |
| [`espn_nhl_league_root`](reference/core#espn_nhl_league_root) | [`espn_nhl_league_root`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_league_root.html) |
| [`espn_nhl_news`](reference/site#espn_nhl_news) | [`espn_nhl_news`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_news.html) |
| [`espn_nhl_pbp`](reference/additional#espn_nhl_pbp) | [`espn_nhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_pbp.html) |
| [`espn_nhl_player_awards`](reference/core#espn_nhl_player_awards) | [`espn_nhl_player_awards`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_awards.html) |
| [`espn_nhl_player_bio`](reference/site#espn_nhl_player_bio) | [`espn_nhl_player_bio`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_bio.html) |
| [`espn_nhl_player_career_stats`](reference/core#espn_nhl_player_career_stats) | [`espn_nhl_player_career_stats`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_career_stats.html) |
| [`espn_nhl_player_contracts`](reference/core#espn_nhl_player_contracts) | [`espn_nhl_player_contracts`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_contracts.html) |
| [`espn_nhl_player_core`](reference/core#espn_nhl_player_core) | [`espn_nhl_player_core`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_core.html) |
| [`espn_nhl_player_eventlog`](reference/core#espn_nhl_player_eventlog) | [`espn_nhl_player_eventlog`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_eventlog.html) |
| [`espn_nhl_player_gamelog`](reference/web#espn_nhl_player_gamelog) | [`espn_nhl_player_gamelog`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_gamelog.html) |
| [`espn_nhl_player_info`](reference/site#espn_nhl_player_info) | [`espn_nhl_player_info`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_info.html) |
| [`espn_nhl_player_injuries`](reference/core#espn_nhl_player_injuries) | [`espn_nhl_player_injuries`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_injuries.html) |
| [`espn_nhl_player_news`](reference/site#espn_nhl_player_news) | [`espn_nhl_player_news`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_news.html) |
| [`espn_nhl_player_notes`](reference/core#espn_nhl_player_notes) | [`espn_nhl_player_notes`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_notes.html) |
| [`espn_nhl_player_overview`](reference/web#espn_nhl_player_overview) | [`espn_nhl_player_overview`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_overview.html) |
| [`espn_nhl_player_records`](reference/core#espn_nhl_player_records) | [`espn_nhl_player_records`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_records.html) |
| [`espn_nhl_player_seasons`](reference/core#espn_nhl_player_seasons) | [`espn_nhl_player_seasons`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_seasons.html) |
| [`espn_nhl_player_splits`](reference/web#espn_nhl_player_splits) | [`espn_nhl_player_splits`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_splits.html) |
| [`espn_nhl_player_statisticslog`](reference/core#espn_nhl_player_statisticslog) | [`espn_nhl_player_statisticslog`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_statisticslog.html) |
| [`espn_nhl_player_stats_v3`](reference/web#espn_nhl_player_stats_v3) | [`espn_nhl_player_stats_v3`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_stats_v3.html) |
| [`espn_nhl_player_vs_player`](reference/core#espn_nhl_player_vs_player) | [`espn_nhl_player_vs_player`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_player_vs_player.html) |
| [`espn_nhl_players_index`](reference/core#espn_nhl_players_index) | [`espn_nhl_players_index`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_players_index.html) |
| [`espn_nhl_position`](reference/core#espn_nhl_position) | [`espn_nhl_position`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_position.html) |
| [`espn_nhl_positions`](reference/core#espn_nhl_positions) | [`espn_nhl_positions`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_positions.html) |
| [`espn_nhl_schedule`](reference/additional#espn_nhl_schedule) | [`espn_nhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_schedule.html) |
| [`espn_nhl_scoreboard`](reference/site#espn_nhl_scoreboard) | [`espn_nhl_scoreboard`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_scoreboard.html) |
| [`espn_nhl_season_awards`](reference/core#espn_nhl_season_awards) | [`espn_nhl_season_awards`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_awards.html) |
| [`espn_nhl_season_coaches`](reference/core#espn_nhl_season_coaches) | [`espn_nhl_season_coaches`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_coaches.html) |
| [`espn_nhl_season_draft`](reference/core#espn_nhl_season_draft) | [`espn_nhl_season_draft`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_draft.html) |
| [`espn_nhl_season_draft_round_picks`](reference/core#espn_nhl_season_draft_round_picks) | [`espn_nhl_season_draft_round_picks`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_draft_round_picks.html) |
| [`espn_nhl_season_freeagents`](reference/core#espn_nhl_season_freeagents) | [`espn_nhl_season_freeagents`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_freeagents.html) |
| [`espn_nhl_season_futures`](reference/core#espn_nhl_season_futures) | [`espn_nhl_season_futures`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_futures.html) |
| [`espn_nhl_season_group`](reference/core#espn_nhl_season_group) | [`espn_nhl_season_group`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_group.html) |
| [`espn_nhl_season_group_children`](reference/core#espn_nhl_season_group_children) | [`espn_nhl_season_group_children`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_group_children.html) |
| [`espn_nhl_season_group_teams`](reference/core#espn_nhl_season_group_teams) | [`espn_nhl_season_group_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_group_teams.html) |
| [`espn_nhl_season_groups`](reference/core#espn_nhl_season_groups) | [`espn_nhl_season_groups`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_groups.html) |
| [`espn_nhl_season_info`](reference/core#espn_nhl_season_info) | [`espn_nhl_season_info`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_info.html) |
| [`espn_nhl_season_players`](reference/core#espn_nhl_season_players) | [`espn_nhl_season_players`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_players.html) |
| [`espn_nhl_season_pointer`](reference/core#espn_nhl_season_pointer) | [`espn_nhl_season_pointer`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_pointer.html) |
| [`espn_nhl_season_powerindex`](reference/core#espn_nhl_season_powerindex) | [`espn_nhl_season_powerindex`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_powerindex.html) |
| [`espn_nhl_season_powerindex_leaders`](reference/core#espn_nhl_season_powerindex_leaders) | [`espn_nhl_season_powerindex_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_powerindex_leaders.html) |
| [`espn_nhl_season_team`](reference/core#espn_nhl_season_team) | [`espn_nhl_season_team`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_team.html) |
| [`espn_nhl_season_teams`](reference/core#espn_nhl_season_teams) | [`espn_nhl_season_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_teams.html) |
| [`espn_nhl_season_type`](reference/core#espn_nhl_season_type) | [`espn_nhl_season_type`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_type.html) |
| [`espn_nhl_season_type_corrections`](reference/core#espn_nhl_season_type_corrections) | [`espn_nhl_season_type_corrections`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_type_corrections.html) |
| [`espn_nhl_season_type_leaders`](reference/core#espn_nhl_season_type_leaders) | [`espn_nhl_season_type_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_type_leaders.html) |
| [`espn_nhl_season_types`](reference/core#espn_nhl_season_types) | [`espn_nhl_season_types`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_types.html) |
| [`espn_nhl_season_week`](reference/core#espn_nhl_season_week) | [`espn_nhl_season_week`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_week.html) |
| [`espn_nhl_season_week_games`](reference/core#espn_nhl_season_week_games) | [`espn_nhl_season_week_games`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_week_games.html) |
| [`espn_nhl_season_weeks`](reference/core#espn_nhl_season_weeks) | [`espn_nhl_season_weeks`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_season_weeks.html) |
| [`espn_nhl_seasons`](reference/core#espn_nhl_seasons) | [`espn_nhl_seasons`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_seasons.html) |
| [`espn_nhl_standings`](reference/site#espn_nhl_standings) | [`espn_nhl_standings`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_standings.html) |
| [`espn_nhl_standings_core`](reference/core#espn_nhl_standings_core) | [`espn_nhl_standings_core`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_standings_core.html) |
| [`espn_nhl_statistics_league`](reference/site#espn_nhl_statistics_league) | [`espn_nhl_statistics_league`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_statistics_league.html) |
| [`espn_nhl_summary`](reference/site#espn_nhl_summary) | [`espn_nhl_summary`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_summary.html) |
| [`espn_nhl_talentpicks`](reference/core#espn_nhl_talentpicks) | [`espn_nhl_talentpicks`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_talentpicks.html) |
| [`espn_nhl_team`](reference/site#espn_nhl_team) | [`espn_nhl_team`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team.html) |
| [`espn_nhl_team_core`](reference/core#espn_nhl_team_core) | [`espn_nhl_team_core`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_core.html) |
| [`espn_nhl_team_depthcharts`](reference/site#espn_nhl_team_depthcharts) | [`espn_nhl_team_depthcharts`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_depthcharts.html) |
| [`espn_nhl_team_history`](reference/site#espn_nhl_team_history) | [`espn_nhl_team_history`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_history.html) |
| [`espn_nhl_team_injuries`](reference/site#espn_nhl_team_injuries) | [`espn_nhl_team_injuries`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_injuries.html) |
| [`espn_nhl_team_leaders`](reference/site#espn_nhl_team_leaders) | [`espn_nhl_team_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_leaders.html) |
| [`espn_nhl_team_news`](reference/site#espn_nhl_team_news) | [`espn_nhl_team_news`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_news.html) |
| [`espn_nhl_team_record`](reference/site#espn_nhl_team_record) | [`espn_nhl_team_record`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_record.html) |
| [`espn_nhl_team_roster`](reference/site#espn_nhl_team_roster) | [`espn_nhl_team_roster`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_roster.html) |
| [`espn_nhl_team_schedule`](reference/site#espn_nhl_team_schedule) | [`espn_nhl_team_schedule`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_schedule.html) |
| [`espn_nhl_team_transactions`](reference/site#espn_nhl_team_transactions) | [`espn_nhl_team_transactions`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_team_transactions.html) |
| [`espn_nhl_teams`](reference/additional#espn_nhl_teams) | [`espn_nhl_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_teams.html) |
| [`espn_nhl_teams_core`](reference/core#espn_nhl_teams_core) | [`espn_nhl_teams_core`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_teams_core.html) |
| [`espn_nhl_teams_site`](reference/site#espn_nhl_teams_site) | [`espn_nhl_teams_site`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_teams_site.html) |
| [`espn_nhl_tournaments`](reference/core#espn_nhl_tournaments) | [`espn_nhl_tournaments`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_tournaments.html) |
| [`espn_nhl_transactions`](reference/site#espn_nhl_transactions) | [`espn_nhl_transactions`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_transactions.html) |
| [`espn_nhl_venue`](reference/core#espn_nhl_venue) | [`espn_nhl_venue`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_venue.html) |
| [`espn_nhl_venues`](reference/core#espn_nhl_venues) | [`espn_nhl_venues`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_venues.html) |
| [`load_nhl_game_info`](reference/loaders#load_nhl_game_info) | [`load_nhl_game_info`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_game_info.html) |
| [`load_nhl_game_rosters`](reference/loaders#load_nhl_game_rosters) | [`load_nhl_game_rosters`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_game_rosters.html) |
| [`load_nhl_goalie_box`](reference/additional#load_nhl_goalie_box) | [`load_nhl_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_goalie_box.html) |
| [`load_nhl_goalie_boxscores`](reference/loaders#load_nhl_goalie_boxscores) | [`load_nhl_goalie_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_goalie_boxscores.html) |
| [`load_nhl_linescore`](reference/loaders#load_nhl_linescore) | [`load_nhl_linescore`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_linescore.html) |
| [`load_nhl_officials`](reference/loaders#load_nhl_officials) | [`load_nhl_officials`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_officials.html) |
| [`load_nhl_pbp`](reference/loaders#load_nhl_pbp) | [`load_nhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_pbp.html) |
| [`load_nhl_pbp_full`](reference/loaders#load_nhl_pbp_full) | [`load_nhl_pbp_full`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_pbp_full.html) |
| [`load_nhl_pbp_lite`](reference/loaders#load_nhl_pbp_lite) | [`load_nhl_pbp_lite`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_pbp_lite.html) |
| [`load_nhl_penalties`](reference/loaders#load_nhl_penalties) | [`load_nhl_penalties`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_penalties.html) |
| [`load_nhl_player_box`](reference/additional#load_nhl_player_box) | [`load_nhl_player_box`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_player_box.html) |
| [`load_nhl_player_boxscore`](reference/loaders#load_nhl_player_boxscore) | [`load_nhl_player_boxscore`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_player_boxscore.html) |
| [`load_nhl_player_boxscores`](reference/loaders#load_nhl_player_boxscores) | [`load_nhl_player_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_player_boxscores.html) |
| [`load_nhl_rosters`](reference/loaders#load_nhl_rosters) | [`load_nhl_rosters`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_rosters.html) |
| [`load_nhl_schedule`](reference/loaders#load_nhl_schedule) | [`load_nhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_schedule.html) |
| [`load_nhl_schedules`](reference/loaders#load_nhl_schedules) | [`load_nhl_schedules`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_schedules.html) |
| [`load_nhl_scoring`](reference/loaders#load_nhl_scoring) | [`load_nhl_scoring`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_scoring.html) |
| [`load_nhl_scratches`](reference/loaders#load_nhl_scratches) | [`load_nhl_scratches`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_scratches.html) |
| [`load_nhl_shifts`](reference/loaders#load_nhl_shifts) | [`load_nhl_shifts`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_shifts.html) |
| [`load_nhl_shootout`](reference/loaders#load_nhl_shootout) | [`load_nhl_shootout`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_shootout.html) |
| [`load_nhl_shots_by_period`](reference/loaders#load_nhl_shots_by_period) | [`load_nhl_shots_by_period`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_shots_by_period.html) |
| [`load_nhl_skater_box`](reference/additional#load_nhl_skater_box) | [`load_nhl_skater_box`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_skater_box.html) |
| [`load_nhl_skater_boxscores`](reference/loaders#load_nhl_skater_boxscores) | [`load_nhl_skater_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_skater_boxscores.html) |
| [`load_nhl_team_box`](reference/additional#load_nhl_team_box) | [`load_nhl_team_box`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_team_box.html) |
| [`load_nhl_team_boxscore`](reference/loaders#load_nhl_team_boxscore) | [`load_nhl_team_boxscore`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_team_boxscore.html) |
| [`load_nhl_team_boxscores`](reference/loaders#load_nhl_team_boxscores) | [`load_nhl_team_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_team_boxscores.html) |
| [`load_nhl_three_stars`](reference/loaders#load_nhl_three_stars) | [`load_nhl_three_stars`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_three_stars.html) |
| [`most_recent_nhl_season`](reference/additional#most_recent_nhl_season) | [`most_recent_nhl_season`](https://fastRhockey.sportsdataverse.org/reference/most_recent_nhl_season.html) |
| [`nhl_club_stats_season`](reference/nhl_api_web#nhl_club_stats_season) | [`nhl_club_stats_season`](https://fastRhockey.sportsdataverse.org/reference/nhl_club_stats_season.html) |
| [`nhl_draft_rankings`](reference/nhl_api_web#nhl_draft_rankings) | [`nhl_draft_rankings`](https://fastRhockey.sportsdataverse.org/reference/nhl_draft_rankings.html) |
| [`nhl_edge_goalie_5v5_detail`](reference/nhl_edge#nhl_edge_goalie_5v5_detail) | [`nhl_edge_goalie_5v5_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_5v5_detail.html) |
| [`nhl_edge_goalie_5v5_top_10`](reference/nhl_edge#nhl_edge_goalie_5v5_top_10) | [`nhl_edge_goalie_5v5_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_5v5_top_10.html) |
| [`nhl_edge_goalie_comparison`](reference/nhl_edge#nhl_edge_goalie_comparison) | [`nhl_edge_goalie_comparison`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_comparison.html) |
| [`nhl_edge_goalie_detail`](reference/nhl_edge#nhl_edge_goalie_detail) | [`nhl_edge_goalie_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_detail.html) |
| [`nhl_edge_goalie_edge_save_pctg_top_10`](reference/nhl_edge#nhl_edge_goalie_edge_save_pctg_top_10) | [`nhl_edge_goalie_edge_save_pctg_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_edge_save_pctg_top_10.html) |
| [`nhl_edge_goalie_landing`](reference/nhl_edge#nhl_edge_goalie_landing) | [`nhl_edge_goalie_landing`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_landing.html) |
| [`nhl_edge_goalie_save_percentage_detail`](reference/nhl_edge#nhl_edge_goalie_save_percentage_detail) | [`nhl_edge_goalie_save_percentage_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_save_percentage_detail.html) |
| [`nhl_edge_goalie_shot_location_detail`](reference/nhl_edge#nhl_edge_goalie_shot_location_detail) | [`nhl_edge_goalie_shot_location_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_shot_location_detail.html) |
| [`nhl_edge_goalie_shot_location_top_10`](reference/nhl_edge#nhl_edge_goalie_shot_location_top_10) | [`nhl_edge_goalie_shot_location_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_goalie_shot_location_top_10.html) |
| [`nhl_edge_skater_comparison`](reference/nhl_edge#nhl_edge_skater_comparison) | [`nhl_edge_skater_comparison`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_comparison.html) |
| [`nhl_edge_skater_detail`](reference/nhl_edge#nhl_edge_skater_detail) | [`nhl_edge_skater_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_detail.html) |
| [`nhl_edge_skater_distance_top_10`](reference/nhl_edge#nhl_edge_skater_distance_top_10) | [`nhl_edge_skater_distance_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_distance_top_10.html) |
| [`nhl_edge_skater_landing`](reference/nhl_edge#nhl_edge_skater_landing) | [`nhl_edge_skater_landing`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_landing.html) |
| [`nhl_edge_skater_shot_location_detail`](reference/nhl_edge#nhl_edge_skater_shot_location_detail) | [`nhl_edge_skater_shot_location_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_shot_location_detail.html) |
| [`nhl_edge_skater_shot_location_top_10`](reference/nhl_edge#nhl_edge_skater_shot_location_top_10) | [`nhl_edge_skater_shot_location_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_shot_location_top_10.html) |
| [`nhl_edge_skater_shot_speed_detail`](reference/nhl_edge#nhl_edge_skater_shot_speed_detail) | [`nhl_edge_skater_shot_speed_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_shot_speed_detail.html) |
| [`nhl_edge_skater_shot_speed_top_10`](reference/nhl_edge#nhl_edge_skater_shot_speed_top_10) | [`nhl_edge_skater_shot_speed_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_shot_speed_top_10.html) |
| [`nhl_edge_skater_skating_distance_detail`](reference/nhl_edge#nhl_edge_skater_skating_distance_detail) | [`nhl_edge_skater_skating_distance_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_skating_distance_detail.html) |
| [`nhl_edge_skater_skating_speed_detail`](reference/nhl_edge#nhl_edge_skater_skating_speed_detail) | [`nhl_edge_skater_skating_speed_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_skating_speed_detail.html) |
| [`nhl_edge_skater_speed_top_10`](reference/nhl_edge#nhl_edge_skater_speed_top_10) | [`nhl_edge_skater_speed_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_speed_top_10.html) |
| [`nhl_edge_skater_zone_time`](reference/nhl_edge#nhl_edge_skater_zone_time) | [`nhl_edge_skater_zone_time`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_zone_time.html) |
| [`nhl_edge_skater_zone_time_top_10`](reference/nhl_edge#nhl_edge_skater_zone_time_top_10) | [`nhl_edge_skater_zone_time_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_zone_time_top_10.html) |
| [`nhl_edge_team_detail`](reference/nhl_edge#nhl_edge_team_detail) | [`nhl_edge_team_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_detail.html) |
| [`nhl_edge_team_landing`](reference/nhl_edge#nhl_edge_team_landing) | [`nhl_edge_team_landing`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_landing.html) |
| [`nhl_edge_team_shot_location_detail`](reference/nhl_edge#nhl_edge_team_shot_location_detail) | [`nhl_edge_team_shot_location_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_shot_location_detail.html) |
| [`nhl_edge_team_shot_location_top_10`](reference/nhl_edge#nhl_edge_team_shot_location_top_10) | [`nhl_edge_team_shot_location_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_shot_location_top_10.html) |
| [`nhl_edge_team_shot_speed_detail`](reference/nhl_edge#nhl_edge_team_shot_speed_detail) | [`nhl_edge_team_shot_speed_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_shot_speed_detail.html) |
| [`nhl_edge_team_skating_distance_detail`](reference/nhl_edge#nhl_edge_team_skating_distance_detail) | [`nhl_edge_team_skating_distance_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_skating_distance_detail.html) |
| [`nhl_edge_team_skating_distance_top_10`](reference/nhl_edge#nhl_edge_team_skating_distance_top_10) | [`nhl_edge_team_skating_distance_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_skating_distance_top_10.html) |
| [`nhl_edge_team_skating_speed_detail`](reference/nhl_edge#nhl_edge_team_skating_speed_detail) | [`nhl_edge_team_skating_speed_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_skating_speed_detail.html) |
| [`nhl_edge_team_skating_speed_top_10`](reference/nhl_edge#nhl_edge_team_skating_speed_top_10) | [`nhl_edge_team_skating_speed_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_skating_speed_top_10.html) |
| [`nhl_edge_team_zone_time_details`](reference/nhl_edge#nhl_edge_team_zone_time_details) | [`nhl_edge_team_zone_time_details`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_zone_time_details.html) |
| [`nhl_edge_team_zone_time_top_10`](reference/nhl_edge#nhl_edge_team_zone_time_top_10) | [`nhl_edge_team_zone_time_top_10`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_team_zone_time_top_10.html) |
| [`nhl_player_game_log`](reference/nhl_api_web#nhl_player_game_log) | [`nhl_player_game_log`](https://fastRhockey.sportsdataverse.org/reference/nhl_player_game_log.html) |
| [`nhl_player_spotlight`](reference/nhl_api_web#nhl_player_spotlight) | [`nhl_player_spotlight`](https://fastRhockey.sportsdataverse.org/reference/nhl_player_spotlight.html) |
| [`nhl_records_attendance`](reference/nhl_records#nhl_records_attendance) | [`nhl_records_attendance`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_attendance.html) |
| [`nhl_records_draft`](reference/nhl_records#nhl_records_draft) | [`nhl_records_draft`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_draft.html) |
| [`nhl_records_draft_lottery_odds`](reference/nhl_records#nhl_records_draft_lottery_odds) | [`nhl_records_draft_lottery_odds`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_draft_lottery_odds.html) |
| [`nhl_records_draft_prospect`](reference/nhl_records#nhl_records_draft_prospect) | [`nhl_records_draft_prospect`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_draft_prospect.html) |
| [`nhl_records_franchise_detail`](reference/nhl_records#nhl_records_franchise_detail) | [`nhl_records_franchise_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_franchise_detail.html) |
| [`nhl_records_franchise_playoff_appearances`](reference/nhl_records#nhl_records_franchise_playoff_appearances) | [`nhl_records_franchise_playoff_appearances`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_franchise_playoff_appearances.html) |
| [`nhl_records_franchise_season_results`](reference/nhl_records#nhl_records_franchise_season_results) | [`nhl_records_franchise_season_results`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_franchise_season_results.html) |
| [`nhl_records_franchise_team_totals`](reference/nhl_records#nhl_records_franchise_team_totals) | [`nhl_records_franchise_team_totals`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_franchise_team_totals.html) |
| [`nhl_records_franchise_totals`](reference/nhl_records#nhl_records_franchise_totals) | [`nhl_records_franchise_totals`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_franchise_totals.html) |
| [`nhl_records_goalie_career_stats`](reference/nhl_records#nhl_records_goalie_career_stats) | [`nhl_records_goalie_career_stats`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_goalie_career_stats.html) |
| [`nhl_records_goalie_season_stats`](reference/nhl_records#nhl_records_goalie_season_stats) | [`nhl_records_goalie_season_stats`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_goalie_season_stats.html) |
| [`nhl_records_goalie_shutout_streak`](reference/nhl_records#nhl_records_goalie_shutout_streak) | [`nhl_records_goalie_shutout_streak`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_goalie_shutout_streak.html) |
| [`nhl_records_hof_players`](reference/nhl_records#nhl_records_hof_players) | [`nhl_records_hof_players`](https://fastRhockey.sportsdataverse.org/reference/nhl_records_hof_players.html) |
| [`nhl_roster_season`](reference/nhl_api_web#nhl_roster_season) | [`nhl_roster_season`](https://fastRhockey.sportsdataverse.org/reference/nhl_roster_season.html) |
| [`nhl_schedule_calendar`](reference/nhl_api_web#nhl_schedule_calendar) | [`nhl_schedule_calendar`](https://fastRhockey.sportsdataverse.org/reference/nhl_schedule_calendar.html) |
| [`nhl_scoreboard`](reference/additional#nhl_scoreboard) | [`nhl_scoreboard`](https://fastRhockey.sportsdataverse.org/reference/nhl_scoreboard.html) |
| [`nhl_standings`](reference/nhl_api_web#nhl_standings) | [`nhl_standings`](https://fastRhockey.sportsdataverse.org/reference/nhl_standings.html) |
| [`nhl_standings_season`](reference/nhl_api_web#nhl_standings_season) | [`nhl_standings_season`](https://fastRhockey.sportsdataverse.org/reference/nhl_standings_season.html) |
| [`nhl_web_schedule`](reference/nhl_api_web#nhl_web_schedule) | [`nhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/nhl_schedule.html) |
