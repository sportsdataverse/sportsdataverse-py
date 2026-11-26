---
title: Women's college hockey
sidebar_label: Women's college hockey
description: "sdv-py Women's college hockey: endpoint references, dataset loaders and parsers for Women's college hockey in the SportsDataverse Python package."
---
# Women's college hockey (`sportsdataverse.hockey.wch`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `site.web.api.espn.com`, `sports.core.api.espn.com` | 118 | none |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 87 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |

## See also

- [sdvplot](https://sdvplot.sportsdataverse.org/) — `surface("ncaa_whockey")` draws the NCAA rink and `team_colors("ncaa_whockey", ...)` the school colors
- [sportypy](https://sportypy.sportsdataverse.org/) — the regulation NCAA rink sdvplot draws with

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.wch` (Python) | `fastRhockey` (R) |
|---|---|
| [`espn_wch_award`](reference/core/other#espn_wch_award) | [`espn_wch_award`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_award.html) |
| [`espn_wch_awards`](reference/core/other#espn_wch_awards) | [`espn_wch_awards`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_awards.html) |
| [`espn_wch_calendar`](reference/site#espn_wch_calendar) | [`espn_wch_calendar`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_calendar.html) |
| [`espn_wch_coach`](reference/core/other#espn_wch_coach) | [`espn_wch_coach`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_coach.html) |
| [`espn_wch_coach_record`](reference/core/other#espn_wch_coach_record) | [`espn_wch_coach_record`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_coach_record.html) |
| [`espn_wch_coach_season`](reference/core/other#espn_wch_coach_season) | [`espn_wch_coach_season`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_coach_season.html) |
| [`espn_wch_conferences`](reference/site#espn_wch_conferences) | [`espn_wch_conferences`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_conferences.html) |
| [`espn_wch_draft`](reference/site#espn_wch_draft) | [`espn_wch_draft`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_draft.html) |
| [`espn_wch_fpi`](reference/fitt#espn_wch_fpi) | [`espn_wch_fpi`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_fpi.html) |
| [`espn_wch_franchise`](reference/core/other#espn_wch_franchise) | [`espn_wch_franchise`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_franchise.html) |
| [`espn_wch_franchises`](reference/core/other#espn_wch_franchises) | [`espn_wch_franchises`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_franchises.html) |
| [`espn_wch_game`](reference/core/other#espn_wch_game) | [`espn_wch_game`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game.html) |
| [`espn_wch_game_broadcasts`](reference/core/game#espn_wch_game_broadcasts) | [`espn_wch_game_broadcasts`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_broadcasts.html) |
| [`espn_wch_game_competition`](reference/core/game#espn_wch_game_competition) | [`espn_wch_game_competition`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_competition.html) |
| [`espn_wch_game_leaders`](reference/core/game#espn_wch_game_leaders) | [`espn_wch_game_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_leaders.html) |
| [`espn_wch_game_odds`](reference/core/game#espn_wch_game_odds) | [`espn_wch_game_odds`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_odds.html) |
| [`espn_wch_game_official_detail`](reference/core/game#espn_wch_game_official_detail) | [`espn_wch_game_official_detail`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_official_detail.html) |
| [`espn_wch_game_officials`](reference/core/game#espn_wch_game_officials) | [`espn_wch_game_officials`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_officials.html) |
| [`espn_wch_game_play`](reference/core/game#espn_wch_game_play) | [`espn_wch_game_play`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_play.html) |
| [`espn_wch_game_play_personnel`](reference/core/game#espn_wch_game_play_personnel) | [`espn_wch_game_play_personnel`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_play_personnel.html) |
| [`espn_wch_game_plays`](reference/core/game#espn_wch_game_plays) | [`espn_wch_game_plays`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_plays.html) |
| [`espn_wch_game_powerindex`](reference/core/game#espn_wch_game_powerindex) | [`espn_wch_game_powerindex`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_powerindex.html) |
| [`espn_wch_game_predictor`](reference/core/game#espn_wch_game_predictor) | [`espn_wch_game_predictor`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_predictor.html) |
| [`espn_wch_game_probabilities`](reference/core/game#espn_wch_game_probabilities) | [`espn_wch_game_probabilities`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_probabilities.html) |
| [`espn_wch_game_propbets`](reference/core/game#espn_wch_game_propbets) | [`espn_wch_game_propbets`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_propbets.html) |
| [`espn_wch_game_scoringplays`](reference/core/game#espn_wch_game_scoringplays) | [`espn_wch_game_scoringplays`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_scoringplays.html) |
| [`espn_wch_game_situation`](reference/core/game#espn_wch_game_situation) | [`espn_wch_game_situation`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_situation.html) |
| [`espn_wch_game_status`](reference/core/game#espn_wch_game_status) | [`espn_wch_game_status`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_status.html) |
| [`espn_wch_game_team`](reference/core/game#espn_wch_game_team) | [`espn_wch_game_team`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_team.html) |
| [`espn_wch_game_team_leaders`](reference/core/game#espn_wch_game_team_leaders) | [`espn_wch_game_team_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_team_leaders.html) |
| [`espn_wch_game_team_linescores`](reference/core/game#espn_wch_game_team_linescores) | [`espn_wch_game_team_linescores`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_team_linescores.html) |
| [`espn_wch_game_team_record`](reference/core/game#espn_wch_game_team_record) | [`espn_wch_game_team_record`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_team_record.html) |
| [`espn_wch_game_team_roster`](reference/core/game#espn_wch_game_team_roster) | [`espn_wch_game_team_roster`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_team_roster.html) |
| [`espn_wch_game_team_statistics`](reference/core/game#espn_wch_game_team_statistics) | [`espn_wch_game_team_statistics`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_team_statistics.html) |
| [`espn_wch_game_teams`](reference/core/game#espn_wch_game_teams) | [`espn_wch_game_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_game_teams.html) |
| [`espn_wch_games`](reference/core/other#espn_wch_games) | [`espn_wch_games`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_games.html) |
| [`espn_wch_injuries`](reference/site#espn_wch_injuries) | [`espn_wch_injuries`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_injuries.html) |
| [`espn_wch_leaders`](reference/web#espn_wch_leaders) | [`espn_wch_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_leaders.html) |
| [`espn_wch_leaders_core`](reference/core/other#espn_wch_leaders_core) | [`espn_wch_leaders_core`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_leaders_core.html) |
| [`espn_wch_league_notes`](reference/core/other#espn_wch_league_notes) | [`espn_wch_league_notes`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_league_notes.html) |
| [`espn_wch_league_root`](reference/core/other#espn_wch_league_root) | [`espn_wch_league_root`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_league_root.html) |
| [`espn_wch_news`](reference/site#espn_wch_news) | [`espn_wch_news`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_news.html) |
| [`espn_wch_player_awards`](reference/core/player#espn_wch_player_awards) | [`espn_wch_player_awards`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_awards.html) |
| [`espn_wch_player_bio`](reference/site#espn_wch_player_bio) | [`espn_wch_player_bio`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_bio.html) |
| [`espn_wch_player_career_stats`](reference/core/player#espn_wch_player_career_stats) | [`espn_wch_player_career_stats`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_career_stats.html) |
| [`espn_wch_player_contracts`](reference/core/player#espn_wch_player_contracts) | [`espn_wch_player_contracts`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_contracts.html) |
| [`espn_wch_player_core`](reference/core/player#espn_wch_player_core) | [`espn_wch_player_core`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_core.html) |
| [`espn_wch_player_eventlog`](reference/core/player#espn_wch_player_eventlog) | [`espn_wch_player_eventlog`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_eventlog.html) |
| [`espn_wch_player_gamelog`](reference/web#espn_wch_player_gamelog) | [`espn_wch_player_gamelog`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_gamelog.html) |
| [`espn_wch_player_info`](reference/site#espn_wch_player_info) | [`espn_wch_player_info`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_info.html) |
| [`espn_wch_player_injuries`](reference/core/player#espn_wch_player_injuries) | [`espn_wch_player_injuries`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_injuries.html) |
| [`espn_wch_player_news`](reference/site#espn_wch_player_news) | [`espn_wch_player_news`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_news.html) |
| [`espn_wch_player_notes`](reference/core/player#espn_wch_player_notes) | [`espn_wch_player_notes`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_notes.html) |
| [`espn_wch_player_overview`](reference/web#espn_wch_player_overview) | [`espn_wch_player_overview`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_overview.html) |
| [`espn_wch_player_records`](reference/core/player#espn_wch_player_records) | [`espn_wch_player_records`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_records.html) |
| [`espn_wch_player_seasons`](reference/core/player#espn_wch_player_seasons) | [`espn_wch_player_seasons`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_seasons.html) |
| [`espn_wch_player_splits`](reference/web#espn_wch_player_splits) | [`espn_wch_player_splits`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_splits.html) |
| [`espn_wch_player_statisticslog`](reference/core/player#espn_wch_player_statisticslog) | [`espn_wch_player_statisticslog`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_statisticslog.html) |
| [`espn_wch_player_stats`](reference/web#espn_wch_player_stats) | [`espn_wch_player_stats`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_stats.html) |
| [`espn_wch_player_vs_player`](reference/core/player#espn_wch_player_vs_player) | [`espn_wch_player_vs_player`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_player_vs_player.html) |
| [`espn_wch_players_index`](reference/core/player#espn_wch_players_index) | [`espn_wch_players_index`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_players_index.html) |
| [`espn_wch_position`](reference/core/other#espn_wch_position) | [`espn_wch_position`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_position.html) |
| [`espn_wch_positions`](reference/core/other#espn_wch_positions) | [`espn_wch_positions`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_positions.html) |
| [`espn_wch_rankings`](reference/site#espn_wch_rankings) | [`espn_wch_rankings`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_rankings.html) |
| [`espn_wch_recruiting_players`](reference/core/other#espn_wch_recruiting_players) | [`espn_wch_recruiting_players`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_recruiting_players.html) |
| [`espn_wch_recruiting_rankings`](reference/core/other#espn_wch_recruiting_rankings) | [`espn_wch_recruiting_rankings`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_recruiting_rankings.html) |
| [`espn_wch_recruiting_years`](reference/core/other#espn_wch_recruiting_years) | [`espn_wch_recruiting_years`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_recruiting_years.html) |
| [`espn_wch_scoreboard`](reference/site#espn_wch_scoreboard) | [`espn_wch_scoreboard`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_scoreboard.html) |
| [`espn_wch_season_awards`](reference/core/season#espn_wch_season_awards) | [`espn_wch_season_awards`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_awards.html) |
| [`espn_wch_season_coaches`](reference/core/season#espn_wch_season_coaches) | [`espn_wch_season_coaches`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_coaches.html) |
| [`espn_wch_season_draft`](reference/core/season#espn_wch_season_draft) | [`espn_wch_season_draft`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_draft.html) |
| [`espn_wch_season_draft_round_picks`](reference/core/season#espn_wch_season_draft_round_picks) | [`espn_wch_season_draft_round_picks`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_draft_round_picks.html) |
| [`espn_wch_season_freeagents`](reference/core/season#espn_wch_season_freeagents) | [`espn_wch_season_freeagents`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_freeagents.html) |
| [`espn_wch_season_futures`](reference/core/season#espn_wch_season_futures) | [`espn_wch_season_futures`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_futures.html) |
| [`espn_wch_season_group`](reference/core/season#espn_wch_season_group) | [`espn_wch_season_group`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_group.html) |
| [`espn_wch_season_group_children`](reference/core/season#espn_wch_season_group_children) | [`espn_wch_season_group_children`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_group_children.html) |
| [`espn_wch_season_group_teams`](reference/core/season#espn_wch_season_group_teams) | [`espn_wch_season_group_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_group_teams.html) |
| [`espn_wch_season_groups`](reference/core/season#espn_wch_season_groups) | [`espn_wch_season_groups`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_groups.html) |
| [`espn_wch_season_info`](reference/core/season#espn_wch_season_info) | [`espn_wch_season_info`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_info.html) |
| [`espn_wch_season_players`](reference/core/season#espn_wch_season_players) | [`espn_wch_season_players`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_players.html) |
| [`espn_wch_season_pointer`](reference/core/season#espn_wch_season_pointer) | [`espn_wch_season_pointer`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_pointer.html) |
| [`espn_wch_season_powerindex`](reference/core/season#espn_wch_season_powerindex) | [`espn_wch_season_powerindex`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_powerindex.html) |
| [`espn_wch_season_powerindex_leaders`](reference/core/season#espn_wch_season_powerindex_leaders) | [`espn_wch_season_powerindex_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_powerindex_leaders.html) |
| [`espn_wch_season_recruits`](reference/core/season#espn_wch_season_recruits) | [`espn_wch_season_recruits`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_recruits.html) |
| [`espn_wch_season_team`](reference/core/season#espn_wch_season_team) | [`espn_wch_season_team`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_team.html) |
| [`espn_wch_season_teams`](reference/core/season#espn_wch_season_teams) | [`espn_wch_season_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_teams.html) |
| [`espn_wch_season_type`](reference/core/season#espn_wch_season_type) | [`espn_wch_season_type`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_type.html) |
| [`espn_wch_season_type_corrections`](reference/core/season#espn_wch_season_type_corrections) | [`espn_wch_season_type_corrections`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_type_corrections.html) |
| [`espn_wch_season_type_leaders`](reference/core/season#espn_wch_season_type_leaders) | [`espn_wch_season_type_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_type_leaders.html) |
| [`espn_wch_season_types`](reference/core/season#espn_wch_season_types) | [`espn_wch_season_types`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_types.html) |
| [`espn_wch_season_week`](reference/core/season#espn_wch_season_week) | [`espn_wch_season_week`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_week.html) |
| [`espn_wch_season_week_games`](reference/core/season#espn_wch_season_week_games) | [`espn_wch_season_week_games`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_week_games.html) |
| [`espn_wch_season_week_powerindex`](reference/core/season#espn_wch_season_week_powerindex) | [`espn_wch_season_week_powerindex`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_week_powerindex.html) |
| [`espn_wch_season_week_rankings`](reference/core/season#espn_wch_season_week_rankings) | [`espn_wch_season_week_rankings`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_week_rankings.html) |
| [`espn_wch_season_weeks`](reference/core/season#espn_wch_season_weeks) | [`espn_wch_season_weeks`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_season_weeks.html) |
| [`espn_wch_seasons`](reference/core/other#espn_wch_seasons) | [`espn_wch_seasons`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_seasons.html) |
| [`espn_wch_standings`](reference/site#espn_wch_standings) | [`espn_wch_standings`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_standings.html) |
| [`espn_wch_standings_core`](reference/core/other#espn_wch_standings_core) | [`espn_wch_standings_core`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_standings_core.html) |
| [`espn_wch_statistics_league`](reference/site#espn_wch_statistics_league) | [`espn_wch_statistics_league`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_statistics_league.html) |
| [`espn_wch_summary`](reference/site#espn_wch_summary) | [`espn_wch_summary`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_summary.html) |
| [`espn_wch_talentpicks`](reference/core/other#espn_wch_talentpicks) | [`espn_wch_talentpicks`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_talentpicks.html) |
| [`espn_wch_team`](reference/site#espn_wch_team) | [`espn_wch_team`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team.html) |
| [`espn_wch_team_core`](reference/core/other#espn_wch_team_core) | [`espn_wch_team_core`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_core.html) |
| [`espn_wch_team_depthcharts`](reference/site#espn_wch_team_depthcharts) | [`espn_wch_team_depthcharts`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_depthcharts.html) |
| [`espn_wch_team_history`](reference/site#espn_wch_team_history) | [`espn_wch_team_history`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_history.html) |
| [`espn_wch_team_injuries`](reference/site#espn_wch_team_injuries) | [`espn_wch_team_injuries`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_injuries.html) |
| [`espn_wch_team_leaders`](reference/site#espn_wch_team_leaders) | [`espn_wch_team_leaders`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_leaders.html) |
| [`espn_wch_team_news`](reference/site#espn_wch_team_news) | [`espn_wch_team_news`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_news.html) |
| [`espn_wch_team_record`](reference/site#espn_wch_team_record) | [`espn_wch_team_record`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_record.html) |
| [`espn_wch_team_roster`](reference/site#espn_wch_team_roster) | [`espn_wch_team_roster`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_roster.html) |
| [`espn_wch_team_schedule`](reference/site#espn_wch_team_schedule) | [`espn_wch_team_schedule`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_schedule.html) |
| [`espn_wch_team_transactions`](reference/site#espn_wch_team_transactions) | [`espn_wch_team_transactions`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_team_transactions.html) |
| [`espn_wch_teams_core`](reference/core/other#espn_wch_teams_core) | [`espn_wch_teams_core`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_teams_core.html) |
| [`espn_wch_teams_site`](reference/site#espn_wch_teams_site) | [`espn_wch_teams_site`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_teams_site.html) |
| [`espn_wch_tournaments`](reference/core/other#espn_wch_tournaments) | [`espn_wch_tournaments`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_tournaments.html) |
| [`espn_wch_transactions`](reference/site#espn_wch_transactions) | [`espn_wch_transactions`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_transactions.html) |
| [`espn_wch_venue`](reference/core/other#espn_wch_venue) | [`espn_wch_venue`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_venue.html) |
| [`espn_wch_venues`](reference/core/other#espn_wch_venues) | [`espn_wch_venues`](https://fastRhockey.sportsdataverse.org/reference/espn_wch_venues.html) |
