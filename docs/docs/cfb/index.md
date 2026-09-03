---
title: CFB
sidebar_label: CFB
description: "sdv-py CFB: endpoint references, dataset loaders and parsers for CFB in the SportsDataverse Python package."
---
# CFB (`sportsdataverse.cfb`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 25 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 89 | `https://sports.core.api.espn.com/v2/sports` |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 | `https://site.web.api.espn.com/apis/fitt/v3/sports` |
| [On3 Recruit Database (api.on3.com)](reference/on3) | 78 | `https://api.on3.com/public/rdb/v1` |
| [247Sports Recruit Database (ipa.247sports.com)](reference/sports247) | 12 | `https://ipa.247sports.com` |
| [247Sports Site Pages (247sports.com)](reference/sports247_site_pages) | 35 | `https://247sports.com` |
| [College Football Data API (api.collegefootballdata.com, free API key)](reference/cfbd) | 58 | `https://api.collegefootballdata.com` |
| [Dataset loaders](reference/loaders) | 42 | sportsdataverse raw data / sportsdataverse-data releases |
| [Additional functions](reference/additional) | 92 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [CFB tutorial](../tutorials/02_cfb_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`cfbfastR`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.cfb` (Python) | `cfbfastR` (R) |
|---|---|
| [`cfbd_calendar`](reference/cfbd#cfbd_calendar) | [`cfbd_calendar`](https://cfbfastR.sportsdataverse.org/reference/cfbd_calendar.html) |
| [`cfbd_coaches`](reference/cfbd#cfbd_coaches) | [`cfbd_coaches`](https://cfbfastR.sportsdataverse.org/reference/cfbd_coaches.html) |
| [`cfbd_conferences`](reference/cfbd#cfbd_conferences) | [`cfbd_conferences`](https://cfbfastR.sportsdataverse.org/reference/cfbd_conferences.html) |
| [`cfbd_draft_picks`](reference/cfbd#cfbd_draft_picks) | [`cfbd_draft_picks`](https://cfbfastR.sportsdataverse.org/reference/cfbd_draft_picks.html) |
| [`cfbd_draft_positions`](reference/cfbd#cfbd_draft_positions) | [`cfbd_draft_positions`](https://cfbfastR.sportsdataverse.org/reference/cfbd_draft_positions.html) |
| [`cfbd_draft_teams`](reference/cfbd#cfbd_draft_teams) | [`cfbd_draft_teams`](https://cfbfastR.sportsdataverse.org/reference/cfbd_draft_teams.html) |
| [`cfbd_drives`](reference/cfbd#cfbd_drives) | [`cfbd_drives`](https://cfbfastR.sportsdataverse.org/reference/cfbd_drives.html) |
| [`cfbd_game_box_advanced`](reference/cfbd#cfbd_game_box_advanced) | [`cfbd_game_box_advanced`](https://cfbfastR.sportsdataverse.org/reference/cfbd_game_box_advanced.html) |
| [`cfbd_live_plays`](reference/cfbd#cfbd_live_plays) | [`cfbd_live_plays`](https://cfbfastR.sportsdataverse.org/reference/cfbd_live_plays.html) |
| [`cfbd_metrics_fg_ep`](reference/cfbd#cfbd_metrics_fg_ep) | [`cfbd_metrics_fg_ep`](https://cfbfastR.sportsdataverse.org/reference/cfbd_metrics_fg_ep.html) |
| [`cfbd_metrics_wp`](reference/cfbd#cfbd_metrics_wp) | [`cfbd_metrics_wp`](https://cfbfastR.sportsdataverse.org/reference/cfbd_metrics_wp.html) |
| [`cfbd_metrics_wp_pregame`](reference/cfbd#cfbd_metrics_wp_pregame) | [`cfbd_metrics_wp_pregame`](https://cfbfastR.sportsdataverse.org/reference/cfbd_metrics_wp_pregame.html) |
| [`cfbd_player_returning`](reference/cfbd#cfbd_player_returning) | [`cfbd_player_returning`](https://cfbfastR.sportsdataverse.org/reference/cfbd_player_returning.html) |
| [`cfbd_player_usage`](reference/cfbd#cfbd_player_usage) | [`cfbd_player_usage`](https://cfbfastR.sportsdataverse.org/reference/cfbd_player_usage.html) |
| [`cfbd_plays`](reference/cfbd#cfbd_plays) | [`cfbd_plays`](https://cfbfastR.sportsdataverse.org/reference/cfbd_plays.html) |
| [`cfbd_rankings`](reference/cfbd#cfbd_rankings) | [`cfbd_rankings`](https://cfbfastR.sportsdataverse.org/reference/cfbd_rankings.html) |
| [`cfbd_ratings_elo`](reference/cfbd#cfbd_ratings_elo) | [`cfbd_ratings_elo`](https://cfbfastR.sportsdataverse.org/reference/cfbd_ratings_elo.html) |
| [`cfbd_ratings_fpi`](reference/cfbd#cfbd_ratings_fpi) | [`cfbd_ratings_fpi`](https://cfbfastR.sportsdataverse.org/reference/cfbd_ratings_fpi.html) |
| [`cfbd_ratings_sp`](reference/cfbd#cfbd_ratings_sp) | [`cfbd_ratings_sp`](https://cfbfastR.sportsdataverse.org/reference/cfbd_ratings_sp.html) |
| [`cfbd_ratings_srs`](reference/cfbd#cfbd_ratings_srs) | [`cfbd_ratings_srs`](https://cfbfastR.sportsdataverse.org/reference/cfbd_ratings_srs.html) |
| [`cfbd_stats_categories`](reference/cfbd#cfbd_stats_categories) | [`cfbd_stats_categories`](https://cfbfastR.sportsdataverse.org/reference/cfbd_stats_categories.html) |
| [`cfbd_stats_game_advanced`](reference/cfbd#cfbd_stats_game_advanced) | [`cfbd_stats_game_advanced`](https://cfbfastR.sportsdataverse.org/reference/cfbd_stats_game_advanced.html) |
| [`cfbd_stats_season_advanced`](reference/cfbd#cfbd_stats_season_advanced) | [`cfbd_stats_season_advanced`](https://cfbfastR.sportsdataverse.org/reference/cfbd_stats_season_advanced.html) |
| [`cfbd_venues`](reference/cfbd#cfbd_venues) | [`cfbd_venues`](https://cfbfastR.sportsdataverse.org/reference/cfbd_venues.html) |
| [`espn_cfb_award`](reference/core#espn_cfb_award) | [`espn_cfb_award`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_award.html) |
| [`espn_cfb_awards`](reference/core#espn_cfb_awards) | [`espn_cfb_awards`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_awards.html) |
| [`espn_cfb_calendar`](reference/site#espn_cfb_calendar) | [`espn_cfb_calendar`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_calendar.html) |
| [`espn_cfb_coach`](reference/core#espn_cfb_coach) | [`espn_cfb_coach`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_coach.html) |
| [`espn_cfb_coach_record`](reference/core#espn_cfb_coach_record) | [`espn_cfb_coach_record`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_coach_record.html) |
| [`espn_cfb_franchise`](reference/core#espn_cfb_franchise) | [`espn_cfb_franchise`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_franchise.html) |
| [`espn_cfb_franchises`](reference/core#espn_cfb_franchises) | [`espn_cfb_franchises`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_franchises.html) |
| [`espn_cfb_futures`](reference/core#espn_cfb_futures) | [`espn_cfb_futures`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_futures.html) |
| [`espn_cfb_game_broadcasts`](reference/core#espn_cfb_game_broadcasts) | [`espn_cfb_game_broadcasts`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_broadcasts.html) |
| [`espn_cfb_game_leaders`](reference/core#espn_cfb_game_leaders) | [`espn_cfb_game_leaders`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_leaders.html) |
| [`espn_cfb_game_odds`](reference/core#espn_cfb_game_odds) | [`espn_cfb_game_odds`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_odds.html) |
| [`espn_cfb_game_play`](reference/core#espn_cfb_game_play) | [`espn_cfb_game_play`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_play.html) |
| [`espn_cfb_game_powerindex`](reference/core#espn_cfb_game_powerindex) | [`espn_cfb_game_powerindex`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_powerindex.html) |
| [`espn_cfb_game_predictor`](reference/core#espn_cfb_game_predictor) | [`espn_cfb_game_predictor`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_predictor.html) |
| [`espn_cfb_game_probabilities`](reference/core#espn_cfb_game_probabilities) | [`espn_cfb_game_probabilities`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_probabilities.html) |
| [`espn_cfb_game_situation`](reference/core#espn_cfb_game_situation) | [`espn_cfb_game_situation`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_situation.html) |
| [`espn_cfb_game_status`](reference/core#espn_cfb_game_status) | [`espn_cfb_game_status`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_status.html) |
| [`espn_cfb_game_team_leaders`](reference/core#espn_cfb_game_team_leaders) | [`espn_cfb_game_team_leaders`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_leaders.html) |
| [`espn_cfb_game_team_linescores`](reference/core#espn_cfb_game_team_linescores) | [`espn_cfb_game_team_linescores`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_linescores.html) |
| [`espn_cfb_game_team_roster`](reference/core#espn_cfb_game_team_roster) | [`espn_cfb_game_team_roster`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_roster.html) |
| [`espn_cfb_game_team_statistics`](reference/core#espn_cfb_game_team_statistics) | [`espn_cfb_game_team_statistics`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_statistics.html) |
| [`espn_cfb_game_teams`](reference/core#espn_cfb_game_teams) | [`espn_cfb_game_teams`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_teams.html) |
| [`espn_cfb_groups`](reference/core#espn_cfb_groups) | [`espn_cfb_groups`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_groups.html) |
| [`espn_cfb_player_career_stats`](reference/core#espn_cfb_player_career_stats) | [`espn_cfb_player_career_stats`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_career_stats.html) |
| [`espn_cfb_player_eventlog`](reference/core#espn_cfb_player_eventlog) | [`espn_cfb_player_eventlog`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_eventlog.html) |
| [`espn_cfb_player_gamelog`](reference/web#espn_cfb_player_gamelog) | [`espn_cfb_player_gamelog`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_gamelog.html) |
| [`espn_cfb_player_overview`](reference/web#espn_cfb_player_overview) | [`espn_cfb_player_overview`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_overview.html) |
| [`espn_cfb_player_seasons`](reference/core#espn_cfb_player_seasons) | [`espn_cfb_player_seasons`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_seasons.html) |
| [`espn_cfb_player_splits`](reference/web#espn_cfb_player_splits) | [`espn_cfb_player_splits`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_splits.html) |
| [`espn_cfb_player_stats`](reference/additional#espn_cfb_player_stats) | [`espn_cfb_player_stats`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_stats.html) |
| [`espn_cfb_player_stats_v3`](reference/web#espn_cfb_player_stats_v3) | [`espn_cfb_player_stats_v3`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_stats_v3.html) |
| [`espn_cfb_position`](reference/core#espn_cfb_position) | [`espn_cfb_position`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_position.html) |
| [`espn_cfb_positions`](reference/core#espn_cfb_positions) | [`espn_cfb_positions`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_positions.html) |
| [`espn_cfb_rankings`](reference/site#espn_cfb_rankings) | [`espn_cfb_rankings`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_rankings.html) |
| [`espn_cfb_recruits`](reference/core#espn_cfb_recruits) | [`espn_cfb_recruits`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_recruits.html) |
| [`espn_cfb_schedule`](reference/additional#espn_cfb_schedule) | [`espn_cfb_schedule`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_schedule.html) |
| [`espn_cfb_scoreboard`](reference/site#espn_cfb_scoreboard) | [`espn_cfb_scoreboard`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_scoreboard.html) |
| [`espn_cfb_season_info`](reference/core#espn_cfb_season_info) | [`espn_cfb_season_info`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_season_info.html) |
| [`espn_cfb_season_types`](reference/core#espn_cfb_season_types) | [`espn_cfb_season_types`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_season_types.html) |
| [`espn_cfb_season_weeks`](reference/core#espn_cfb_season_weeks) | [`espn_cfb_season_weeks`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_season_weeks.html) |
| [`espn_cfb_seasons`](reference/core#espn_cfb_seasons) | [`espn_cfb_seasons`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_seasons.html) |
| [`espn_cfb_standings`](reference/site#espn_cfb_standings) | [`espn_cfb_standings`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_standings.html) |
| [`espn_cfb_team`](reference/site#espn_cfb_team) | [`espn_cfb_team`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team.html) |
| [`espn_cfb_team_leaders`](reference/site#espn_cfb_team_leaders) | [`espn_cfb_team_leaders`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_leaders.html) |
| [`espn_cfb_team_powerindex`](reference/core#espn_cfb_team_powerindex) | [`espn_cfb_team_powerindex`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_powerindex.html) |
| [`espn_cfb_team_record`](reference/site#espn_cfb_team_record) | [`espn_cfb_team_record`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_record.html) |
| [`espn_cfb_team_roster`](reference/site#espn_cfb_team_roster) | [`espn_cfb_team_roster`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_roster.html) |
| [`espn_cfb_team_schedule`](reference/site#espn_cfb_team_schedule) | [`espn_cfb_team_schedule`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_schedule.html) |
| [`espn_cfb_venue`](reference/core#espn_cfb_venue) | [`espn_cfb_venue`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_venue.html) |
| [`espn_cfb_venues`](reference/core#espn_cfb_venues) | [`espn_cfb_venues`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_venues.html) |
| [`espn_cfb_week_rankings`](reference/core#espn_cfb_week_rankings) | [`espn_cfb_week_rankings`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_week_rankings.html) |
| [`load_cfb_pbp`](reference/loaders#load_cfb_pbp) | [`load_cfb_pbp`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_pbp.html) |
| [`load_cfb_rosters`](reference/loaders#load_cfb_rosters) | [`load_cfb_rosters`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_rosters.html) |
| [`load_cfb_teams`](reference/loaders#load_cfb_teams) | [`load_cfb_teams`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_teams.html) |
