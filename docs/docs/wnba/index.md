---
title: WNBA
sidebar_label: WNBA
description: "sdv-py WNBA: endpoint references, dataset loaders and parsers for WNBA in the SportsDataverse Python package."
---
# WNBA (`sportsdataverse.wnba`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 127 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 39 | none |
| [WNBA Stats API](#wnba-stats-api) | `stats.wnba.com` | 113 | none (curl_cffi chrome TLS impersonation) |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 26 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 89 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 81 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |
| [Hand-written wrappers](reference/additional/espn) | 12 |

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

- [`wnba_crosswalks_from_archive`](reference/additional/ids-and-crosswalks#wnba_crosswalks_from_archive)
- [`wnba_player_crosswalk`](reference/additional/ids-and-crosswalks#wnba_player_crosswalk)
- [`wnba_schedule_crosswalk`](reference/additional/ids-and-crosswalks#wnba_schedule_crosswalk)
- [`wnba_team_crosswalk`](reference/additional/ids-and-crosswalks#wnba_team_crosswalk)


## See also

- [sdvplot](https://sdvplot.sportsdataverse.org/) — `court_coords()` puts stats-API `x_legacy` / `y_legacy` shots on the court `surface("wnba")` draws
- [sportypy](https://sportypy.sportsdataverse.org/) — the regulation WNBA court sdvplot draws with

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [WNBA tutorial](../tutorials/08_wnba_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`wehoop`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.wnba` (Python) | `wehoop` (R) |
|---|---|
| [`espn_wnba_award`](reference/core/other#espn_wnba_award) | [`espn_wnba_award`](https://wehoop.sportsdataverse.org/reference/espn_wnba_award.html) |
| [`espn_wnba_calendar`](reference/additional/espn#espn_wnba_calendar) | [`espn_wnba_calendar`](https://wehoop.sportsdataverse.org/reference/espn_wnba_calendar.html) |
| [`espn_wnba_coach_season`](reference/core/other#espn_wnba_coach_season) | [`espn_wnba_coach_season`](https://wehoop.sportsdataverse.org/reference/espn_wnba_coach_season.html) |
| [`espn_wnba_conferences`](reference/site#espn_wnba_conferences) | [`espn_wnba_conferences`](https://wehoop.sportsdataverse.org/reference/espn_wnba_conferences.html) |
| [`espn_wnba_draft`](reference/additional/espn#espn_wnba_draft) | [`espn_wnba_draft`](https://wehoop.sportsdataverse.org/reference/espn_wnba_draft.html) |
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
| [`espn_wnba_standings`](reference/additional/espn#espn_wnba_standings) | [`espn_wnba_standings`](https://wehoop.sportsdataverse.org/reference/espn_wnba_standings.html) |
| [`espn_wnba_summary`](reference/site#espn_wnba_summary) | [`espn_wnba_game_all`](https://wehoop.sportsdataverse.org/reference/espn_wnba_game_all.html) |
| [`espn_wnba_team`](reference/site#espn_wnba_team) | [`espn_wnba_team`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team.html) |
| [`espn_wnba_team_injuries`](reference/site#espn_wnba_team_injuries) | [`espn_wnba_team_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_injuries.html) |
| [`espn_wnba_team_leaders`](reference/site#espn_wnba_team_leaders) | [`espn_wnba_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_leaders.html) |
| [`espn_wnba_team_news`](reference/site#espn_wnba_team_news) | [`espn_wnba_team_news`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_news.html) |
| [`espn_wnba_team_record`](reference/site#espn_wnba_team_record) | [`espn_wnba_team_record`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_record.html) |
| [`espn_wnba_team_roster`](reference/additional/espn#espn_wnba_team_roster) | [`espn_wnba_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_roster.html) |
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
| [`load_wnba_player_boxscore`](reference/loaders/player#load_wnba_player_boxscore) | [`load_wnba_player_box`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_box.html) |
| [`load_wnba_player_core`](reference/loaders/player#load_wnba_player_core) | [`load_wnba_player_core`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_core.html) |
| [`load_wnba_player_crosswalk`](reference/loaders/player#load_wnba_player_crosswalk) | [`load_wnba_player_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_crosswalk.html) |
| [`load_wnba_player_impact`](reference/loaders/player#load_wnba_player_impact) | [`load_wnba_player_impact`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_impact.html) |
| [`load_wnba_player_season_stats`](reference/loaders/player#load_wnba_player_season_stats) | [`load_wnba_player_stats`](https://wehoop.sportsdataverse.org/reference/load_wnba_player_stats.html) |
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
| [`load_wnba_stats_schedules`](reference/loaders/stats#load_wnba_stats_schedules) | [`load_wnba_stats_schedule`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_schedule.html) |
| [`load_wnba_stats_shots`](reference/loaders/stats#load_wnba_stats_shots) | [`load_wnba_stats_shots`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_shots.html) |
| [`load_wnba_stats_standings`](reference/additional/sportsdataverse-data-releases#load_wnba_stats_standings) | [`load_wnba_stats_standings`](https://wehoop.sportsdataverse.org/reference/load_wnba_stats_standings.html) |
| [`load_wnba_team_boxscore`](reference/loaders/team#load_wnba_team_boxscore) | [`load_wnba_team_box`](https://wehoop.sportsdataverse.org/reference/load_wnba_team_box.html) |
| [`load_wnba_team_crosswalk`](reference/loaders/team#load_wnba_team_crosswalk) | [`load_wnba_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wnba_team_crosswalk.html) |
| [`load_wnba_team_group_seasons`](reference/loaders/team#load_wnba_team_group_seasons) | [`load_wnba_team_group_seasons`](https://wehoop.sportsdataverse.org/reference/load_wnba_team_group_seasons.html) |
| [`load_wnba_team_season_stats`](reference/loaders/team#load_wnba_team_season_stats) | [`load_wnba_team_stats`](https://wehoop.sportsdataverse.org/reference/load_wnba_team_stats.html) |
| [`most_recent_wnba_season`](reference/additional/dates-and-seasons#most_recent_wnba_season) | [`most_recent_wnba_season`](https://wehoop.sportsdataverse.org/reference/most_recent_wnba_season.html) |
| [`wnba_live_boxscore`](reference/additional/wnba-stats-api#wnba_live_boxscore) | [`wnba_live_boxscore`](https://wehoop.sportsdataverse.org/reference/wnba_live_boxscore.html) |
| [`wnba_live_pbp`](reference/additional/wnba-stats-api#wnba_live_pbp) | [`wnba_live_pbp`](https://wehoop.sportsdataverse.org/reference/wnba_live_pbp.html) |
| [`wnba_player_crosswalk`](reference/additional/ids-and-crosswalks#wnba_player_crosswalk) | [`wnba_player_crosswalk`](https://wehoop.sportsdataverse.org/reference/wnba_player_crosswalk.html) |
| [`wnba_referee_assignments`](reference/additional/analytics#wnba_referee_assignments) | [`wnba_referee_assignments`](https://wehoop.sportsdataverse.org/reference/wnba_referee_assignments.html) |
| [`wnba_schedule_crosswalk`](reference/additional/ids-and-crosswalks#wnba_schedule_crosswalk) | [`wnba_schedule_crosswalk`](https://wehoop.sportsdataverse.org/reference/wnba_schedule_crosswalk.html) |
| [`wnba_stats_alltimeleadersgrids`](reference/wnba_stats/other#wnba_stats_alltimeleadersgrids) | [`wnba_alltimeleadersgrids`](https://wehoop.sportsdataverse.org/reference/wnba_alltimeleadersgrids.html) |
| [`wnba_stats_assistleaders`](reference/wnba_stats/other#wnba_stats_assistleaders) | [`wnba_assistleaders`](https://wehoop.sportsdataverse.org/reference/wnba_assistleaders.html) |
| [`wnba_stats_assisttracker`](reference/wnba_stats/other#wnba_stats_assisttracker) | [`wnba_assisttracker`](https://wehoop.sportsdataverse.org/reference/wnba_assisttracker.html) |
| [`wnba_stats_boxscoreadvancedv2`](reference/wnba_stats/boxscore#wnba_stats_boxscoreadvancedv2) | [`wnba_boxscoreadvancedv2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoreadvancedv2.html) |
| [`wnba_stats_boxscoreadvancedv3`](reference/wnba_stats/boxscore#wnba_stats_boxscoreadvancedv3) | [`wnba_boxscoreadvancedv3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoreadvancedv3.html) |
| [`wnba_stats_boxscorefourfactorsv2`](reference/wnba_stats/boxscore#wnba_stats_boxscorefourfactorsv2) | [`wnba_boxscorefourfactorsv2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscorefourfactorsv2.html) |
| [`wnba_stats_boxscorefourfactorsv3`](reference/wnba_stats/boxscore#wnba_stats_boxscorefourfactorsv3) | [`wnba_boxscorefourfactorsv3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscorefourfactorsv3.html) |
| [`wnba_stats_boxscorehustlev2`](reference/wnba_stats/boxscore#wnba_stats_boxscorehustlev2) | [`wnba_boxscorehustlev2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscorehustlev2.html) |
| [`wnba_stats_boxscoremiscv2`](reference/wnba_stats/boxscore#wnba_stats_boxscoremiscv2) | [`wnba_boxscoremiscv2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoremiscv2.html) |
| [`wnba_stats_boxscoremiscv3`](reference/wnba_stats/boxscore#wnba_stats_boxscoremiscv3) | [`wnba_boxscoremiscv3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoremiscv3.html) |
| [`wnba_stats_boxscoreplayertrackv3`](reference/wnba_stats/boxscore#wnba_stats_boxscoreplayertrackv3) | [`wnba_boxscoreplayertrackv3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoreplayertrackv3.html) |
| [`wnba_stats_boxscorescoringv2`](reference/wnba_stats/boxscore#wnba_stats_boxscorescoringv2) | [`wnba_boxscorescoringv2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscorescoringv2.html) |
| [`wnba_stats_boxscorescoringv3`](reference/wnba_stats/boxscore#wnba_stats_boxscorescoringv3) | [`wnba_boxscorescoringv3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscorescoringv3.html) |
| [`wnba_stats_boxscoresummaryv2`](reference/wnba_stats/boxscore-2#wnba_stats_boxscoresummaryv2) | [`wnba_boxscoresummaryv2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoresummaryv2.html) |
| [`wnba_stats_boxscoresummaryv3`](reference/wnba_stats/boxscore-2#wnba_stats_boxscoresummaryv3) | [`wnba_boxscoresummaryv3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoresummaryv3.html) |
| [`wnba_stats_boxscoretraditionalv2`](reference/wnba_stats/boxscore-2#wnba_stats_boxscoretraditionalv2) | [`wnba_boxscoretraditionalv2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoretraditionalv2.html) |
| [`wnba_stats_boxscoretraditionalv3`](reference/wnba_stats/boxscore-2#wnba_stats_boxscoretraditionalv3) | [`wnba_boxscoretraditionalv3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoretraditionalv3.html) |
| [`wnba_stats_boxscoreusagev2`](reference/wnba_stats/boxscore-2#wnba_stats_boxscoreusagev2) | [`wnba_boxscoreusagev2`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoreusagev2.html) |
| [`wnba_stats_boxscoreusagev3`](reference/wnba_stats/boxscore-2#wnba_stats_boxscoreusagev3) | [`wnba_boxscoreusagev3`](https://wehoop.sportsdataverse.org/reference/wnba_boxscoreusagev3.html) |
| [`wnba_stats_commonallplayers`](reference/wnba_stats/common#wnba_stats_commonallplayers) | [`wnba_commonallplayers`](https://wehoop.sportsdataverse.org/reference/wnba_commonallplayers.html) |
| [`wnba_stats_commonplayerinfo`](reference/wnba_stats/common#wnba_stats_commonplayerinfo) | [`wnba_commonplayerinfo`](https://wehoop.sportsdataverse.org/reference/wnba_commonplayerinfo.html) |
| [`wnba_stats_commonplayoffseries`](reference/wnba_stats/common#wnba_stats_commonplayoffseries) | [`wnba_commonplayoffseries`](https://wehoop.sportsdataverse.org/reference/wnba_commonplayoffseries.html) |
| [`wnba_stats_commonteamroster`](reference/wnba_stats/common#wnba_stats_commonteamroster) | [`wnba_commonteamroster`](https://wehoop.sportsdataverse.org/reference/wnba_commonteamroster.html) |
| [`wnba_stats_cumestatsplayer`](reference/wnba_stats/cumestats#wnba_stats_cumestatsplayer) | [`wnba_cumestatsplayer`](https://wehoop.sportsdataverse.org/reference/wnba_cumestatsplayer.html) |
| [`wnba_stats_cumestatsplayergames`](reference/wnba_stats/cumestats#wnba_stats_cumestatsplayergames) | [`wnba_cumestatsplayergames`](https://wehoop.sportsdataverse.org/reference/wnba_cumestatsplayergames.html) |
| [`wnba_stats_cumestatsteam`](reference/wnba_stats/cumestats#wnba_stats_cumestatsteam) | [`wnba_cumestatsteam`](https://wehoop.sportsdataverse.org/reference/wnba_cumestatsteam.html) |
| [`wnba_stats_cumestatsteamgames`](reference/wnba_stats/cumestats#wnba_stats_cumestatsteamgames) | [`wnba_cumestatsteamgames`](https://wehoop.sportsdataverse.org/reference/wnba_cumestatsteamgames.html) |
| [`wnba_stats_draftcombinestats`](reference/wnba_stats/other#wnba_stats_draftcombinestats) | [`wnba_draftcombinestats`](https://wehoop.sportsdataverse.org/reference/wnba_draftcombinestats.html) |
| [`wnba_stats_drafthistory`](reference/wnba_stats/other#wnba_stats_drafthistory) | [`wnba_drafthistory`](https://wehoop.sportsdataverse.org/reference/wnba_drafthistory.html) |
| [`wnba_stats_fantasywidget`](reference/wnba_stats/other#wnba_stats_fantasywidget) | [`wnba_fantasywidget`](https://wehoop.sportsdataverse.org/reference/wnba_fantasywidget.html) |
| [`wnba_stats_franchisehistory`](reference/wnba_stats/franchise#wnba_stats_franchisehistory) | [`wnba_franchisehistory`](https://wehoop.sportsdataverse.org/reference/wnba_franchisehistory.html) |
| [`wnba_stats_franchiseleaders`](reference/wnba_stats/franchise#wnba_stats_franchiseleaders) | [`wnba_franchiseleaders`](https://wehoop.sportsdataverse.org/reference/wnba_franchiseleaders.html) |
| [`wnba_stats_franchiseleaderswrank`](reference/wnba_stats/franchise#wnba_stats_franchiseleaderswrank) | [`wnba_franchiseleaderswrank`](https://wehoop.sportsdataverse.org/reference/wnba_franchiseleaderswrank.html) |
| [`wnba_stats_franchiseplayers`](reference/wnba_stats/franchise#wnba_stats_franchiseplayers) | [`wnba_franchiseplayers`](https://wehoop.sportsdataverse.org/reference/wnba_franchiseplayers.html) |
| [`wnba_stats_gamerotation`](reference/wnba_stats/other#wnba_stats_gamerotation) | [`wnba_gamerotation`](https://wehoop.sportsdataverse.org/reference/wnba_gamerotation.html) |
| [`wnba_stats_homepageleaders`](reference/wnba_stats/other#wnba_stats_homepageleaders) | [`wnba_homepageleaders`](https://wehoop.sportsdataverse.org/reference/wnba_homepageleaders.html) |
| [`wnba_stats_homepagev2`](reference/wnba_stats/other#wnba_stats_homepagev2) | [`wnba_homepagev2`](https://wehoop.sportsdataverse.org/reference/wnba_homepagev2.html) |
| [`wnba_stats_hustlestatsboxscore`](reference/wnba_stats/other#wnba_stats_hustlestatsboxscore) | [`wnba_hustlestatsboxscore`](https://wehoop.sportsdataverse.org/reference/wnba_hustlestatsboxscore.html) |
| [`wnba_stats_infographicfanduelplayer`](reference/wnba_stats/other#wnba_stats_infographicfanduelplayer) | [`wnba_infographicfanduelplayer`](https://wehoop.sportsdataverse.org/reference/wnba_infographicfanduelplayer.html) |
| [`wnba_stats_leaderstiles`](reference/wnba_stats/other#wnba_stats_leaderstiles) | [`wnba_leaderstiles`](https://wehoop.sportsdataverse.org/reference/wnba_leaderstiles.html) |
| [`wnba_stats_leaguedashlineups`](reference/wnba_stats/leaguedash#wnba_stats_leaguedashlineups) | [`wnba_leaguedashlineups`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashlineups.html) |
| [`wnba_stats_leaguedashplayerbiostats`](reference/wnba_stats/leaguedash#wnba_stats_leaguedashplayerbiostats) | [`wnba_leaguedashplayerbiostats`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashplayerbiostats.html) |
| [`wnba_stats_leaguedashplayerclutch`](reference/wnba_stats/leaguedash#wnba_stats_leaguedashplayerclutch) | [`wnba_leaguedashplayerclutch`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashplayerclutch.html) |
| [`wnba_stats_leaguedashplayershotlocations`](reference/wnba_stats/leaguedash#wnba_stats_leaguedashplayershotlocations) | [`wnba_leaguedashplayershotlocations`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashplayershotlocations.html) |
| [`wnba_stats_leaguedashplayerstats`](reference/wnba_stats/leaguedash#wnba_stats_leaguedashplayerstats) | [`wnba_leaguedashplayerstats`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashplayerstats.html) |
| [`wnba_stats_leaguedashteamclutch`](reference/wnba_stats/leaguedash#wnba_stats_leaguedashteamclutch) | [`wnba_leaguedashteamclutch`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashteamclutch.html) |
| [`wnba_stats_leaguedashteamshotlocations`](reference/wnba_stats/leaguedash-2#wnba_stats_leaguedashteamshotlocations) | [`wnba_leaguedashteamshotlocations`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashteamshotlocations.html) |
| [`wnba_stats_leaguedashteamstats`](reference/wnba_stats/leaguedash-2#wnba_stats_leaguedashteamstats) | [`wnba_leaguedashteamstats`](https://wehoop.sportsdataverse.org/reference/wnba_leaguedashteamstats.html) |
| [`wnba_stats_leaguegamefinder`](reference/wnba_stats/league#wnba_stats_leaguegamefinder) | [`wnba_leaguegamefinder`](https://wehoop.sportsdataverse.org/reference/wnba_leaguegamefinder.html) |
| [`wnba_stats_leaguegamelog`](reference/wnba_stats/league#wnba_stats_leaguegamelog) | [`wnba_leaguegamelog`](https://wehoop.sportsdataverse.org/reference/wnba_leaguegamelog.html) |
| [`wnba_stats_leagueleaders`](reference/wnba_stats/league#wnba_stats_leagueleaders) | [`wnba_leagueleaders`](https://wehoop.sportsdataverse.org/reference/wnba_leagueleaders.html) |
| [`wnba_stats_leaguelineupviz`](reference/wnba_stats/league#wnba_stats_leaguelineupviz) | [`wnba_leaguelineupviz`](https://wehoop.sportsdataverse.org/reference/wnba_leaguelineupviz.html) |
| [`wnba_stats_leagueplayerondetails`](reference/wnba_stats/league#wnba_stats_leagueplayerondetails) | [`wnba_leagueplayerondetails`](https://wehoop.sportsdataverse.org/reference/wnba_leagueplayerondetails.html) |
| [`wnba_stats_leagueseasonmatchups`](reference/wnba_stats/league#wnba_stats_leagueseasonmatchups) | [`wnba_leagueseasonmatchups`](https://wehoop.sportsdataverse.org/reference/wnba_leagueseasonmatchups.html) |
| [`wnba_stats_leaguestandingsv3`](reference/wnba_stats/league#wnba_stats_leaguestandingsv3) | [`wnba_leaguestandingsv3`](https://wehoop.sportsdataverse.org/reference/wnba_leaguestandingsv3.html) |
| [`wnba_stats_playbyplayv3`](reference/wnba_stats/other#wnba_stats_playbyplayv3) | [`wnba_playbyplayv3`](https://wehoop.sportsdataverse.org/reference/wnba_playbyplayv3.html) |
| [`wnba_stats_playerawards`](reference/wnba_stats/player#wnba_stats_playerawards) | [`wnba_playerawards`](https://wehoop.sportsdataverse.org/reference/wnba_playerawards.html) |
| [`wnba_stats_playercareerbycollegerollup`](reference/wnba_stats/player#wnba_stats_playercareerbycollegerollup) | [`wnba_playercareerbycollegerollup`](https://wehoop.sportsdataverse.org/reference/wnba_playercareerbycollegerollup.html) |
| [`wnba_stats_playercareerstats`](reference/wnba_stats/player#wnba_stats_playercareerstats) | [`wnba_playercareerstats`](https://wehoop.sportsdataverse.org/reference/wnba_playercareerstats.html) |
| [`wnba_stats_playercompare`](reference/wnba_stats/player#wnba_stats_playercompare) | [`wnba_playercompare`](https://wehoop.sportsdataverse.org/reference/wnba_playercompare.html) |
| [`wnba_stats_playerdashboardbyclutch`](reference/wnba_stats/playerdash#wnba_stats_playerdashboardbyclutch) | [`wnba_playerdashboardbyclutch`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbyclutch.html) |
| [`wnba_stats_playerdashboardbygamesplits`](reference/wnba_stats/playerdash-2#wnba_stats_playerdashboardbygamesplits) | [`wnba_playerdashboardbygamesplits`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbygamesplits.html) |
| [`wnba_stats_playerdashboardbygeneralsplits`](reference/wnba_stats/playerdash-3#wnba_stats_playerdashboardbygeneralsplits) | [`wnba_playerdashboardbygeneralsplits`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbygeneralsplits.html) |
| [`wnba_stats_playerdashboardbylastngames`](reference/wnba_stats/playerdash-4#wnba_stats_playerdashboardbylastngames) | [`wnba_playerdashboardbylastngames`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbylastngames.html) |
| [`wnba_stats_playerdashboardbyopponent`](reference/wnba_stats/playerdash-4#wnba_stats_playerdashboardbyopponent) | [`wnba_playerdashboardbyopponent`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbyopponent.html) |
| [`wnba_stats_playerdashboardbyshootingsplits`](reference/wnba_stats/playerdash-5#wnba_stats_playerdashboardbyshootingsplits) | [`wnba_playerdashboardbyshootingsplits`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbyshootingsplits.html) |
| [`wnba_stats_playerdashboardbyteamperformance`](reference/wnba_stats/playerdash-5#wnba_stats_playerdashboardbyteamperformance) | [`wnba_playerdashboardbyteamperformance`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbyteamperformance.html) |
| [`wnba_stats_playerdashboardbyyearoveryear`](reference/wnba_stats/playerdash-6#wnba_stats_playerdashboardbyyearoveryear) | [`wnba_playerdashboardbyyearoveryear`](https://wehoop.sportsdataverse.org/reference/wnba_playerdashboardbyyearoveryear.html) |
| [`wnba_stats_playerestimatedmetrics`](reference/wnba_stats/player#wnba_stats_playerestimatedmetrics) | [`wnba_playerestimatedmetrics`](https://wehoop.sportsdataverse.org/reference/wnba_playerestimatedmetrics.html) |
| [`wnba_stats_playerfantasyprofile`](reference/wnba_stats/player#wnba_stats_playerfantasyprofile) | [`wnba_playerfantasyprofile`](https://wehoop.sportsdataverse.org/reference/wnba_playerfantasyprofile.html) |
| [`wnba_stats_playerfantasyprofilebargraph`](reference/wnba_stats/player#wnba_stats_playerfantasyprofilebargraph) | [`wnba_playerfantasyprofilebargraph`](https://wehoop.sportsdataverse.org/reference/wnba_playerfantasyprofilebargraph.html) |
| [`wnba_stats_playergamelog`](reference/wnba_stats/player#wnba_stats_playergamelog) | [`wnba_playergamelog`](https://wehoop.sportsdataverse.org/reference/wnba_playergamelog.html) |
| [`wnba_stats_playergamelogs`](reference/wnba_stats/player-2#wnba_stats_playergamelogs) | [`wnba_playergamelogs`](https://wehoop.sportsdataverse.org/reference/wnba_playergamelogs.html) |
| [`wnba_stats_playergamestreakfinder`](reference/wnba_stats/player-2#wnba_stats_playergamestreakfinder) | [`wnba_playergamestreakfinder`](https://wehoop.sportsdataverse.org/reference/wnba_playergamestreakfinder.html) |
| [`wnba_stats_playerindex`](reference/wnba_stats/player-2#wnba_stats_playerindex) | [`wnba_playerindex`](https://wehoop.sportsdataverse.org/reference/wnba_playerindex.html) |
| [`wnba_stats_playernextngames`](reference/wnba_stats/player-2#wnba_stats_playernextngames) | [`wnba_playernextngames`](https://wehoop.sportsdataverse.org/reference/wnba_playernextngames.html) |
| [`wnba_stats_playerprofilev2`](reference/wnba_stats/player-2#wnba_stats_playerprofilev2) | [`wnba_playerprofilev2`](https://wehoop.sportsdataverse.org/reference/wnba_playerprofilev2.html) |
| [`wnba_stats_playervsplayer`](reference/wnba_stats/player-2#wnba_stats_playervsplayer) | [`wnba_playervsplayer`](https://wehoop.sportsdataverse.org/reference/wnba_playervsplayer.html) |
| [`wnba_stats_scoreboardv2`](reference/wnba_stats/scoreboard#wnba_stats_scoreboardv2) | [`wnba_scoreboardv2`](https://wehoop.sportsdataverse.org/reference/wnba_scoreboardv2.html) |
| [`wnba_stats_scoreboardv3`](reference/wnba_stats/scoreboard#wnba_stats_scoreboardv3) | [`wnba_scoreboardv3`](https://wehoop.sportsdataverse.org/reference/wnba_scoreboardv3.html) |
| [`wnba_stats_shotchartdetail`](reference/wnba_stats/shotchart#wnba_stats_shotchartdetail) | [`wnba_shotchartdetail`](https://wehoop.sportsdataverse.org/reference/wnba_shotchartdetail.html) |
| [`wnba_stats_shotchartleaguewide`](reference/wnba_stats/shotchart#wnba_stats_shotchartleaguewide) | [`wnba_shotchartleaguewide`](https://wehoop.sportsdataverse.org/reference/wnba_shotchartleaguewide.html) |
| [`wnba_stats_shotchartlineupdetail`](reference/wnba_stats/shotchart#wnba_stats_shotchartlineupdetail) | [`wnba_shotchartlineupdetail`](https://wehoop.sportsdataverse.org/reference/wnba_shotchartlineupdetail.html) |
| [`wnba_stats_teamdashboardbyclutch`](reference/wnba_stats/teamdash#wnba_stats_teamdashboardbyclutch) | [`wnba_teamdashboardbyclutch`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbyclutch.html) |
| [`wnba_stats_teamdashboardbygamesplits`](reference/wnba_stats/teamdash-2#wnba_stats_teamdashboardbygamesplits) | [`wnba_teamdashboardbygamesplits`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbygamesplits.html) |
| [`wnba_stats_teamdashboardbygeneralsplits`](reference/wnba_stats/teamdash-2#wnba_stats_teamdashboardbygeneralsplits) | [`wnba_teamdashboardbygeneralsplits`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbygeneralsplits.html) |
| [`wnba_stats_teamdashboardbylastngames`](reference/wnba_stats/teamdash-3#wnba_stats_teamdashboardbylastngames) | [`wnba_teamdashboardbylastngames`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbylastngames.html) |
| [`wnba_stats_teamdashboardbyopponent`](reference/wnba_stats/teamdash-3#wnba_stats_teamdashboardbyopponent) | [`wnba_teamdashboardbyopponent`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbyopponent.html) |
| [`wnba_stats_teamdashboardbyshootingsplits`](reference/wnba_stats/teamdash-4#wnba_stats_teamdashboardbyshootingsplits) | [`wnba_teamdashboardbyshootingsplits`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbyshootingsplits.html) |
| [`wnba_stats_teamdashboardbyteamperformance`](reference/wnba_stats/teamdash-4#wnba_stats_teamdashboardbyteamperformance) | [`wnba_teamdashboardbyteamperformance`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbyteamperformance.html) |
| [`wnba_stats_teamdashboardbyyearoveryear`](reference/wnba_stats/teamdash-5#wnba_stats_teamdashboardbyyearoveryear) | [`wnba_teamdashboardbyyearoveryear`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashboardbyyearoveryear.html) |
| [`wnba_stats_teamdashlineups`](reference/wnba_stats/teamdash-5#wnba_stats_teamdashlineups) | [`wnba_teamdashlineups`](https://wehoop.sportsdataverse.org/reference/wnba_teamdashlineups.html) |
| [`wnba_stats_teamdetails`](reference/wnba_stats/team#wnba_stats_teamdetails) | [`wnba_teamdetails`](https://wehoop.sportsdataverse.org/reference/wnba_teamdetails.html) |
| [`wnba_stats_teamestimatedmetrics`](reference/wnba_stats/team#wnba_stats_teamestimatedmetrics) | [`wnba_teamestimatedmetrics`](https://wehoop.sportsdataverse.org/reference/wnba_teamestimatedmetrics.html) |
| [`wnba_stats_teamgamelog`](reference/wnba_stats/team#wnba_stats_teamgamelog) | [`wnba_teamgamelog`](https://wehoop.sportsdataverse.org/reference/wnba_teamgamelog.html) |
| [`wnba_stats_teamgamelogs`](reference/wnba_stats/team#wnba_stats_teamgamelogs) | [`wnba_teamgamelogs`](https://wehoop.sportsdataverse.org/reference/wnba_teamgamelogs.html) |
| [`wnba_stats_teaminfocommon`](reference/wnba_stats/team#wnba_stats_teaminfocommon) | [`wnba_teaminfocommon`](https://wehoop.sportsdataverse.org/reference/wnba_teaminfocommon.html) |
| [`wnba_stats_teamplayerdashboard`](reference/wnba_stats/teamplayer#wnba_stats_teamplayerdashboard) | [`wnba_teamplayerdashboard`](https://wehoop.sportsdataverse.org/reference/wnba_teamplayerdashboard.html) |
| [`wnba_stats_teamplayeronoffdetails`](reference/wnba_stats/teamplayer#wnba_stats_teamplayeronoffdetails) | [`wnba_teamplayeronoffdetails`](https://wehoop.sportsdataverse.org/reference/wnba_teamplayeronoffdetails.html) |
| [`wnba_stats_teamplayeronoffsummary`](reference/wnba_stats/teamplayer#wnba_stats_teamplayeronoffsummary) | [`wnba_teamplayeronoffsummary`](https://wehoop.sportsdataverse.org/reference/wnba_teamplayeronoffsummary.html) |
| [`wnba_stats_teamvsplayer`](reference/wnba_stats/team#wnba_stats_teamvsplayer) | [`wnba_teamvsplayer`](https://wehoop.sportsdataverse.org/reference/wnba_teamvsplayer.html) |
| [`wnba_stats_teamyearbyyearstats`](reference/wnba_stats/team#wnba_stats_teamyearbyyearstats) | [`wnba_teamyearbyyearstats`](https://wehoop.sportsdataverse.org/reference/wnba_teamyearbyyearstats.html) |
| [`wnba_stats_videostatus`](reference/wnba_stats/other#wnba_stats_videostatus) | [`wnba_videostatus`](https://wehoop.sportsdataverse.org/reference/wnba_videostatus.html) |
| [`wnba_team_crosswalk`](reference/additional/ids-and-crosswalks#wnba_team_crosswalk) | [`wnba_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/wnba_team_crosswalk.html) |
