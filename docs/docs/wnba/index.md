---
title: WNBA
sidebar_label: WNBA
description: "sdv-py WNBA: endpoint references, dataset loaders and parsers for WNBA in the SportsDataverse Python package."
---
# WNBA (`sportsdataverse.wnba`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 123 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 39 | none |
| [WNBA Stats API](#wnba-stats-api) | `stats.wnba.com` | 113 | none (curl_cffi chrome TLS impersonation) |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 26 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 84 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 81 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |
| [Hand-written wrappers](reference/additional/espn) | 8 |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 34 |
| [Hand-written wrappers](reference/additional/sportsdataverse-data-releases) | 5 |

## WNBA Stats API {#wnba-stats-api}

| Reference | Functions |
|---|---:|
| [WNBA Stats API (stats.wnba.com)](reference/wnba_stats) | 111 |
| [Hand-written wrappers](reference/additional/wnba-stats-api) | 2 |

## Fox Sports API {#fox-sports-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/fox-sports-api) | 26 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`build_athlete_identity_lookup`](reference/additional/play-by-play-processing#build_athlete_identity_lookup)
- [`wnba_enhanced_pbp`](reference/additional/play-by-play-processing#wnba_enhanced_pbp)
- [`wnba_on_court`](reference/additional/play-by-play-processing#wnba_on_court)
- [`wnba_pbp_disk`](reference/additional/play-by-play-processing#wnba_pbp_disk)
- [`wnba_play_context`](reference/additional/play-by-play-processing#wnba_play_context)
- [`wnba_possessions`](reference/additional/play-by-play-processing#wnba_possessions)
- [`wnba_rapm_from_games`](reference/additional/play-by-play-processing#wnba_rapm_from_games)

### Models and calculators {#models-and-calculators}

- [`build_wnba_season_wp`](reference/additional/models-and-calculators#build_wnba_season_wp)
- [`wnba_aging_curve`](reference/additional/models-and-calculators#wnba_aging_curve)
- [`wnba_career_trajectory`](reference/additional/models-and-calculators#wnba_career_trajectory)
- [`wnba_draft_model`](reference/additional/models-and-calculators#wnba_draft_model)
- [`wnba_in_game_win_prob`](reference/additional/models-and-calculators#wnba_in_game_win_prob)
- [`wnba_predict_games`](reference/additional/models-and-calculators#wnba_predict_games)
- [`wnba_predict_margin`](reference/additional/models-and-calculators#wnba_predict_margin)
- [`wnba_predict_total`](reference/additional/models-and-calculators#wnba_predict_total)
- [`wnba_rookie_projection`](reference/additional/models-and-calculators#wnba_rookie_projection)
- [`wnba_team_ratings`](reference/additional/models-and-calculators#wnba_team_ratings)
- [`wnba_win_prob_from_margin`](reference/additional/models-and-calculators#wnba_win_prob_from_margin)

### Analytics {#analytics}

- [`make_prob_by_context`](reference/additional/analytics#make_prob_by_context)
- [`make_prob_joint`](reference/additional/analytics#make_prob_joint)
- [`score_shot_xpoints`](reference/additional/analytics#score_shot_xpoints)
- [`shooter_talent`](reference/additional/analytics#shooter_talent)
- [`shot_selection_quality`](reference/additional/analytics#shot_selection_quality)
- [`wnba_availability`](reference/additional/analytics#wnba_availability)
- [`wnba_expected_turnovers`](reference/additional/analytics#wnba_expected_turnovers)
- [`wnba_foul_drawing`](reference/additional/analytics#wnba_foul_drawing)
- [`wnba_matchup_drapm`](reference/additional/analytics#wnba_matchup_drapm)
- [`wnba_player_props`](reference/additional/analytics#wnba_player_props)
- [`wnba_playtype_ratings`](reference/additional/analytics#wnba_playtype_ratings)
- [`wnba_referee_assignments`](reference/additional/analytics#wnba_referee_assignments)
- [`wnba_shot_value`](reference/additional/analytics#wnba_shot_value)
- [`wnba_team_clutch`](reference/additional/analytics#wnba_team_clutch)
- [`wnba_tracking_drive_value`](reference/additional/analytics#wnba_tracking_drive_value)
- [`wnba_tracking_pass_value`](reference/additional/analytics#wnba_tracking_pass_value)
- [`wnba_tracking_reb_oe`](reference/additional/analytics#wnba_tracking_reb_oe)
- [`wnba_tracking_rim_protect_value`](reference/additional/analytics#wnba_tracking_rim_protect_value)
- [`wnba_tracking_shot_diet_value`](reference/additional/analytics#wnba_tracking_shot_diet_value)
- [`wnba_tracking_touch_value`](reference/additional/analytics#wnba_tracking_touch_value)
- [`zone_value_map`](reference/additional/analytics#zone_value_map)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_wnba_season`](reference/additional/dates-and-seasons#most_recent_wnba_season)

### IDs and crosswalks {#ids-and-crosswalks}

- [`wnba_player_crosswalk`](reference/additional/ids-and-crosswalks#wnba_player_crosswalk)
- [`wnba_schedule_crosswalk`](reference/additional/ids-and-crosswalks#wnba_schedule_crosswalk)
- [`wnba_team_crosswalk`](reference/additional/ids-and-crosswalks#wnba_team_crosswalk)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [WNBA tutorial](../tutorials/08_wnba_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`wehoop`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.wnba` (Python) | `wehoop` (R) |
|---|---|
| [`espn_wnba_award`](reference/core/other#espn_wnba_award) | [`espn_wnba_award`](https://wehoop.sportsdataverse.org/reference/espn_wnba_award.html) |
| [`espn_wnba_calendar`](reference/site#espn_wnba_calendar) | [`espn_wnba_calendar`](https://wehoop.sportsdataverse.org/reference/espn_wnba_calendar.html) |
| [`espn_wnba_coach_season`](reference/core/other#espn_wnba_coach_season) | [`espn_wnba_coach_season`](https://wehoop.sportsdataverse.org/reference/espn_wnba_coach_season.html) |
| [`espn_wnba_conferences`](reference/site#espn_wnba_conferences) | [`espn_wnba_conferences`](https://wehoop.sportsdataverse.org/reference/espn_wnba_conferences.html) |
| [`espn_wnba_draft`](reference/site#espn_wnba_draft) | [`espn_wnba_draft`](https://wehoop.sportsdataverse.org/reference/espn_wnba_draft.html) |
| [`espn_wnba_franchise`](reference/core/other#espn_wnba_franchise) | [`espn_wnba_franchise`](https://wehoop.sportsdataverse.org/reference/espn_wnba_franchise.html) |
| [`espn_wnba_franchises`](reference/core/other#espn_wnba_franchises) | [`espn_wnba_franchises`](https://wehoop.sportsdataverse.org/reference/espn_wnba_franchises.html) |
| [`espn_wnba_game_broadcasts`](reference/core/game#espn_wnba_game_broadcasts) | [`espn_wnba_game_broadcasts`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_broadcasts.html) |
| [`espn_wnba_game_odds`](reference/core/game#espn_wnba_game_odds) | [`espn_wnba_game_odds`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_odds.html) |
| [`espn_wnba_game_official_detail`](reference/core/game#espn_wnba_game_official_detail) | [`espn_wnba_game_official_detail`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_official_detail.html) |
| [`espn_wnba_game_officials`](reference/additional/espn#espn_wnba_game_officials) | [`espn_wnba_game_officials`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_officials.html) |
| [`espn_wnba_game_play`](reference/core/game#espn_wnba_game_play) | [`espn_wnba_game_play`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_play.html) |
| [`espn_wnba_game_play_personnel`](reference/core/game#espn_wnba_game_play_personnel) | [`espn_wnba_game_play_personnel`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_play_personnel.html) |
| [`espn_wnba_game_powerindex`](reference/core/game#espn_wnba_game_powerindex) | [`espn_wnba_game_powerindex`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_powerindex.html) |
| [`espn_wnba_game_predictor`](reference/core/game#espn_wnba_game_predictor) | [`espn_wnba_game_predictor`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_predictor.html) |
| [`espn_wnba_game_probabilities`](reference/core/game#espn_wnba_game_probabilities) | [`espn_wnba_game_probabilities`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_probabilities.html) |
| [`espn_wnba_game_propbets`](reference/core/game#espn_wnba_game_propbets) | [`espn_wnba_game_propbets`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_propbets.html) |
| [`espn_wnba_game_rosters`](reference/additional/espn#espn_wnba_game_rosters) | [`espn_wnba_game_rosters`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_rosters.html) |
| [`espn_wnba_game_situation`](reference/core/game#espn_wnba_game_situation) | [`espn_wnba_game_situation`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_situation.html) |
| [`espn_wnba_game_team_leaders`](reference/core/game#espn_wnba_game_team_leaders) | [`espn_wnba_game_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_leaders.html) |
| [`espn_wnba_game_team_linescores`](reference/core/game#espn_wnba_game_team_linescores) | [`espn_wnba_game_team_linescores`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_linescores.html) |
| [`espn_wnba_game_team_roster`](reference/core/game#espn_wnba_game_team_roster) | [`espn_wnba_game_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_roster.html) |
| [`espn_wnba_game_team_statistics`](reference/core/game#espn_wnba_game_team_statistics) | [`espn_wnba_game_team_statistics`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_team_statistics.html) |
| [`espn_wnba_injuries`](reference/site#espn_wnba_injuries) | [`espn_wnba_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wnba_injuries.html) |
| [`espn_wnba_leaders`](reference/web#espn_wnba_leaders) | [`espn_wnba_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wnba_leaders.html) |
| [`espn_wnba_news`](reference/site#espn_wnba_news) | [`espn_wnba_news`](https://wehoop.sportsdataverse.org/reference/espn_wnba_news.html) |
| [`espn_wnba_pbp`](reference/additional/espn#espn_wnba_pbp) | [`espn_wnba_pbp`](https://wehoop.sportsdataverse.org/reference/espn_wnba_pbp.html) |
| [`espn_wnba_player_awards`](reference/core/player#espn_wnba_player_awards) | [`espn_wnba_player_awards`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_awards.html) |
| [`espn_wnba_player_career_stats`](reference/core/player#espn_wnba_player_career_stats) | [`espn_wnba_player_career_stats`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_career_stats.html) |
| [`espn_wnba_player_eventlog`](reference/core/player#espn_wnba_player_eventlog) | [`espn_wnba_player_eventlog`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_eventlog.html) |
| [`espn_wnba_player_gamelog`](reference/web#espn_wnba_player_gamelog) | [`espn_wnba_player_gamelog`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_gamelog.html) |
| [`espn_wnba_player_info`](reference/site#espn_wnba_player_info) | [`espn_wnba_player_info`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_info.html) |
| [`espn_wnba_player_overview`](reference/web#espn_wnba_player_overview) | [`espn_wnba_player_overview`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_overview.html) |
| [`espn_wnba_player_seasons`](reference/core/player#espn_wnba_player_seasons) | [`espn_wnba_player_seasons`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_seasons.html) |
| [`espn_wnba_player_splits`](reference/web#espn_wnba_player_splits) | [`espn_wnba_player_splits`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_splits.html) |
| [`espn_wnba_player_statisticslog`](reference/core/player#espn_wnba_player_statisticslog) | [`espn_wnba_player_statisticslog`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_statisticslog.html) |
| [`espn_wnba_player_stats`](reference/additional/espn#espn_wnba_player_stats) | [`espn_wnba_player_stats`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_stats.html) |
| [`espn_wnba_player_stats_v3`](reference/web#espn_wnba_player_stats_v3) | [`espn_wnba_player_stats_v3`](https://wehoop.sportsdataverse.org/reference/espn_wnba_player_stats_v3.html) |
| [`espn_wnba_position`](reference/core/other#espn_wnba_position) | [`espn_wnba_position`](https://wehoop.sportsdataverse.org/reference/espn_wnba_position.html) |
| [`espn_wnba_positions`](reference/core/other#espn_wnba_positions) | [`espn_wnba_positions`](https://wehoop.sportsdataverse.org/reference/espn_wnba_positions.html) |
| [`espn_wnba_scoreboard`](reference/site#espn_wnba_scoreboard) | [`espn_wnba_scoreboard`](https://wehoop.sportsdataverse.org/reference/espn_wnba_scoreboard.html) |
| [`espn_wnba_season_awards`](reference/core/season#espn_wnba_season_awards) | [`espn_wnba_season_awards`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_awards.html) |
| [`espn_wnba_season_draft`](reference/core/season#espn_wnba_season_draft) | [`espn_wnba_season_draft`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_draft.html) |
| [`espn_wnba_season_group`](reference/core/season#espn_wnba_season_group) | [`espn_wnba_season_group`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_group.html) |
| [`espn_wnba_season_group_children`](reference/core/season#espn_wnba_season_group_children) | [`espn_wnba_season_group_children`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_group_children.html) |
| [`espn_wnba_season_group_teams`](reference/core/season#espn_wnba_season_group_teams) | [`espn_wnba_season_group_teams`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_group_teams.html) |
| [`espn_wnba_season_groups`](reference/core/season#espn_wnba_season_groups) | [`espn_wnba_season_groups`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_groups.html) |
| [`espn_wnba_season_info`](reference/core/season#espn_wnba_season_info) | [`espn_wnba_season_info`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_info.html) |
| [`espn_wnba_season_type`](reference/core/season#espn_wnba_season_type) | [`espn_wnba_season_type`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_type.html) |
| [`espn_wnba_season_types`](reference/core/season#espn_wnba_season_types) | [`espn_wnba_season_types`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_types.html) |
| [`espn_wnba_season_week`](reference/core/season#espn_wnba_season_week) | [`espn_wnba_season_week`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_week.html) |
| [`espn_wnba_season_weeks`](reference/core/season#espn_wnba_season_weeks) | [`espn_wnba_season_weeks`](https://wehoop.sportsdataverse.org/reference/espn_wnba_season_weeks.html) |
| [`espn_wnba_seasons`](reference/core/other#espn_wnba_seasons) | [`espn_wnba_seasons`](https://wehoop.sportsdataverse.org/reference/espn_wnba_seasons.html) |
| [`espn_wnba_standings`](reference/site#espn_wnba_standings) | [`espn_wnba_standings`](https://wehoop.sportsdataverse.org/reference/espn_wnba_standings.html) |
| [`espn_wnba_team`](reference/site#espn_wnba_team) | [`espn_wnba_team`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team.html) |
| [`espn_wnba_team_injuries`](reference/site#espn_wnba_team_injuries) | [`espn_wnba_team_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_injuries.html) |
| [`espn_wnba_team_leaders`](reference/site#espn_wnba_team_leaders) | [`espn_wnba_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_leaders.html) |
| [`espn_wnba_team_news`](reference/site#espn_wnba_team_news) | [`espn_wnba_team_news`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_news.html) |
| [`espn_wnba_team_record`](reference/site#espn_wnba_team_record) | [`espn_wnba_team_record`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_record.html) |
| [`espn_wnba_team_roster`](reference/site#espn_wnba_team_roster) | [`espn_wnba_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_roster.html) |
| [`espn_wnba_team_schedule`](reference/site#espn_wnba_team_schedule) | [`espn_wnba_team_schedule`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_schedule.html) |
| [`espn_wnba_team_stats`](reference/additional/espn#espn_wnba_team_stats) | [`espn_wnba_team_stats`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_stats.html) |
| [`espn_wnba_teams`](reference/additional/espn#espn_wnba_teams) | [`espn_wnba_teams`](https://wehoop.sportsdataverse.org/reference/espn_wnba_teams.html) |
| [`espn_wnba_transactions`](reference/site#espn_wnba_transactions) | [`espn_wnba_transactions`](https://wehoop.sportsdataverse.org/reference/espn_wnba_transactions.html) |
| [`espn_wnba_venues`](reference/core/other#espn_wnba_venues) | [`espn_wnba_venues`](https://wehoop.sportsdataverse.org/reference/espn_wnba_venues.html) |
| [`fox_wnba_boxscore`](reference/additional/fox-sports-api#fox_wnba_boxscore) | [`fox_wnba_boxscore`](https://wehoop.sportsdataverse.org/reference/fox_wnba_boxscore.html) |
| [`fox_wnba_league_leaders`](reference/additional/fox-sports-api#fox_wnba_league_leaders) | [`fox_wnba_league_leaders`](https://wehoop.sportsdataverse.org/reference/fox_wnba_league_leaders.html) |
| [`fox_wnba_odds`](reference/additional/fox-sports-api#fox_wnba_odds) | [`fox_wnba_odds`](https://wehoop.sportsdataverse.org/reference/fox_wnba_odds.html) |
| [`fox_wnba_pbp`](reference/additional/fox-sports-api#fox_wnba_pbp) | [`fox_wnba_pbp`](https://wehoop.sportsdataverse.org/reference/fox_wnba_pbp.html) |
| [`fox_wnba_standings`](reference/additional/fox-sports-api#fox_wnba_standings) | [`fox_wnba_standings`](https://wehoop.sportsdataverse.org/reference/fox_wnba_standings.html) |
| [`fox_wnba_team_gamelog`](reference/additional/fox-sports-api#fox_wnba_team_gamelog) | [`fox_wnba_team_gamelog`](https://wehoop.sportsdataverse.org/reference/fox_wnba_team_gamelog.html) |
| [`fox_wnba_team_roster`](reference/additional/fox-sports-api#fox_wnba_team_roster) | [`fox_wnba_team_roster`](https://wehoop.sportsdataverse.org/reference/fox_wnba_team_roster.html) |
| [`fox_wnba_team_stats`](reference/additional/fox-sports-api#fox_wnba_team_stats) | [`fox_wnba_team_stats`](https://wehoop.sportsdataverse.org/reference/fox_wnba_team_stats.html) |
| [`fox_wnba_teams`](reference/additional/fox-sports-api#fox_wnba_teams) | [`fox_wnba_teams`](https://wehoop.sportsdataverse.org/reference/fox_wnba_teams.html) |
| [`load_wnba_draft`](reference/loaders/other#load_wnba_draft) | [`load_wnba_draft`](https://wehoop.sportsdataverse.org/reference/load_wnba_draft.html) |
| [`load_wnba_game_rosters`](reference/loaders/other#load_wnba_game_rosters) | [`load_wnba_game_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_game_rosters.html) |
| [`load_wnba_group_aliases`](reference/loaders/other#load_wnba_group_aliases) | [`load_wnba_group_aliases`](https://wehoop.sportsdataverse.org/reference/load_wnba_group_aliases.html) |
| [`load_wnba_group_seasons`](reference/loaders/other#load_wnba_group_seasons) | [`load_wnba_group_seasons`](https://wehoop.sportsdataverse.org/reference/load_wnba_group_seasons.html) |
| [`load_wnba_groups`](reference/loaders/other#load_wnba_groups) | [`load_wnba_groups`](https://wehoop.sportsdataverse.org/reference/load_wnba_groups.html) |
| [`load_wnba_officials`](reference/loaders/other#load_wnba_officials) | [`load_wnba_officials`](https://wehoop.sportsdataverse.org/reference/load_wnba_officials.html) |
| [`load_wnba_pbp`](reference/loaders/other#load_wnba_pbp) | [`load_wnba_pbp`](https://wehoop.sportsdataverse.org/reference/load_wnba_pbp.html) |
| [`load_wnba_player_core`](reference/loaders/player#load_wnba_player_core) | [`load_wnba_player_core`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_core.html) |
| [`load_wnba_player_crosswalk`](reference/loaders/player#load_wnba_player_crosswalk) | [`load_wnba_player_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_crosswalk.html) |
| [`load_wnba_player_impact`](reference/loaders/player#load_wnba_player_impact) | [`load_wnba_player_impact`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_impact.html) |
| [`load_wnba_rosters`](reference/loaders/other#load_wnba_rosters) | [`load_wnba_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_rosters.html) |
| [`load_wnba_schedule`](reference/loaders/other#load_wnba_schedule) | [`load_wnba_schedule`](https://wehoop.sportsdataverse.org/reference/load_wnba_schedule.html) |
| [`load_wnba_schedule_crosswalk`](reference/loaders/other#load_wnba_schedule_crosswalk) | [`load_wnba_schedule_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wnba_schedule_crosswalk.html) |
| [`load_wnba_shots`](reference/loaders/other#load_wnba_shots) | [`load_wnba_shots`](https://wehoop.sportsdataverse.org/reference/load_wnba_shots.html) |
| [`load_wnba_standings`](reference/loaders/other#load_wnba_standings) | [`load_wnba_standings`](https://wehoop.sportsdataverse.org/reference/load_wnba_standings.html) |
| [`load_wnba_stats_coaches`](reference/loaders/stats#load_wnba_stats_coaches) | [`load_wnba_stats_coaches`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_coaches.html) |
| [`load_wnba_stats_draft`](reference/loaders/stats#load_wnba_stats_draft) | [`load_wnba_stats_draft`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_draft.html) |
| [`load_wnba_stats_game_rosters`](reference/loaders/stats#load_wnba_stats_game_rosters) | [`load_wnba_stats_game_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_game_rosters.html) |
| [`load_wnba_stats_leaguedash`](reference/additional/sportsdataverse-data-releases#load_wnba_stats_leaguedash) | [`load_wnba_stats_leaguedash`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_leaguedash.html) |
| [`load_wnba_stats_lineups`](reference/additional/sportsdataverse-data-releases#load_wnba_stats_lineups) | [`load_wnba_stats_lineups`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_lineups.html) |
| [`load_wnba_stats_officials`](reference/loaders/stats#load_wnba_stats_officials) | [`load_wnba_stats_officials`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_officials.html) |
| [`load_wnba_stats_pbp`](reference/loaders/stats#load_wnba_stats_pbp) | [`load_wnba_stats_pbp`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_pbp.html) |
| [`load_wnba_stats_player_game_logs`](reference/loaders/stats#load_wnba_stats_player_game_logs) | [`load_wnba_stats_player_game_logs`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_player_game_logs.html) |
| [`load_wnba_stats_possessions`](reference/loaders/stats#load_wnba_stats_possessions) | [`load_wnba_stats_possessions`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_possessions.html) |
| [`load_wnba_stats_rosters`](reference/loaders/stats#load_wnba_stats_rosters) | [`load_wnba_stats_rosters`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_rosters.html) |
| [`load_wnba_stats_shots`](reference/loaders/stats#load_wnba_stats_shots) | [`load_wnba_stats_shots`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_shots.html) |
| [`load_wnba_stats_standings`](reference/additional/sportsdataverse-data-releases#load_wnba_stats_standings) | [`load_wnba_stats_standings`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_standings.html) |
| [`load_wnba_team_crosswalk`](reference/loaders/other#load_wnba_team_crosswalk) | [`load_wnba_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wnba_team_crosswalk.html) |
| [`load_wnba_team_group_seasons`](reference/loaders/other#load_wnba_team_group_seasons) | [`load_wnba_team_group_seasons`](https://wehoop.sportsdataverse.org/reference/load_wnba_team_group_seasons.html) |
| [`most_recent_wnba_season`](reference/additional/dates-and-seasons#most_recent_wnba_season) | [`most_recent_wnba_season`](https://wehoop.sportsdataverse.org/reference/most_recent_wnba_season.html) |
| [`wnba_live_boxscore`](reference/additional/wnba-stats-api#wnba_live_boxscore) | [`wnba_live_boxscore`](https://wehoop.sportsdataverse.org/reference/wnba_live_boxscore.html) |
| [`wnba_live_pbp`](reference/additional/wnba-stats-api#wnba_live_pbp) | [`wnba_live_pbp`](https://wehoop.sportsdataverse.org/reference/wnba_live_pbp.html) |
| [`wnba_player_crosswalk`](reference/additional/ids-and-crosswalks#wnba_player_crosswalk) | [`wnba_player_crosswalk`](https://wehoop.sportsdataverse.org/reference/wnba_player_crosswalk.html) |
| [`wnba_referee_assignments`](reference/additional/analytics#wnba_referee_assignments) | [`wnba_referee_assignments`](https://wehoop.sportsdataverse.org/reference/wnba_referee_assignments.html) |
| [`wnba_schedule_crosswalk`](reference/additional/ids-and-crosswalks#wnba_schedule_crosswalk) | [`wnba_schedule_crosswalk`](https://wehoop.sportsdataverse.org/reference/wnba_schedule_crosswalk.html) |
| [`wnba_team_crosswalk`](reference/additional/ids-and-crosswalks#wnba_team_crosswalk) | [`wnba_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/wnba_team_crosswalk.html) |
