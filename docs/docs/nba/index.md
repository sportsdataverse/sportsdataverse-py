---
title: NBA
sidebar_label: NBA
description: "sdv-py NBA: endpoint references, dataset loaders and parsers for NBA in the SportsDataverse Python package."
---
# NBA (`sportsdataverse.nba`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [Highlights](reference/additional#highlights) | curated "start here" functions | 8 | — |
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 121 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 42 | none |
| [NBA Stats API](#nba-stats-api) | `stats.nba.com` | 130 | none (curl_cffi chrome TLS impersonation) |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 26 | none |
| [Basketball-Reference](#basketball-reference) | `www.basketball-reference.com` | 9 | none (slow, owner agreement) |
| [RealGM](#realgm) | `basketball.realgm.com` | 18 | none |
| [Public model datasets](#public-model-datasets) | `dunksandthrees.com` | 7 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 180 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 82 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |
| [Hand-written wrappers](reference/additional) | 5 |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 41 |
| [Hand-written wrappers](reference/additional) | 1 |

## NBA Stats API {#nba-stats-api}

| Reference | Functions |
|---|---:|
| [NBA Stats API (stats.nba.com)](reference/nba_stats) | 128 |
| [Hand-written wrappers](reference/additional) | 2 |

## Fox Sports API {#fox-sports-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 26 |

## Basketball-Reference {#basketball-reference}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 9 |

## RealGM {#realgm}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 18 |

## Public model datasets {#public-model-datasets}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 7 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`build_athlete_identity_lookup`](reference/additional#build_athlete_identity_lookup)
- [`build_nba_player_identity_lookup`](reference/additional#build_nba_player_identity_lookup)
- [`build_play_context_shots`](reference/additional#build_play_context_shots)
- [`build_possession_shooting`](reference/additional#build_possession_shooting)
- [`compile_nba_season`](reference/additional#compile_nba_season)
- [`espn_nba_pbp`](reference/additional#espn_nba_pbp)
- [`nba_pbp_disk`](reference/additional#nba_pbp_disk)
- [`nba_v3_to_v2_pbp`](reference/additional#nba_v3_to_v2_pbp)

### Models and calculators {#models-and-calculators}

- [`AdjRapmModel`](reference/additional#AdjRapmModel)
- [`AgingCurve`](reference/additional#AgingCurve)
- [`ExternalValidityResult`](reference/additional#ExternalValidityResult)
- [`ForecastResult`](reference/additional#ForecastResult)
- [`LeagueConstants`](reference/additional#LeagueConstants)
- [`MeasureSpec`](reference/additional#MeasureSpec)
- [`NbaBpmModel`](reference/additional#NbaBpmModel)
- [`NbaSpmModel`](reference/additional#NbaSpmModel)
- [`RidgeRapmModel`](reference/additional#RidgeRapmModel)
- [`SpmCoefficients`](reference/additional#SpmCoefficients)
- [`ValidationReport`](reference/additional#ValidationReport)
- [`WalkForwardResult`](reference/additional#WalkForwardResult)
- [`adjust_efficiency`](reference/additional#adjust_efficiency)
- [`adjust_pace`](reference/additional#adjust_pace)
- [`as_of_ratings_split`](reference/additional#as_of_ratings_split)
- [`calibrate_pts_per_win`](reference/additional#calibrate_pts_per_win)
- [`calibrate_replacement_level`](reference/additional#calibrate_replacement_level)
- [`darko_forecast_accuracy`](reference/additional#darko_forecast_accuracy)
- [`decay_weights`](reference/additional#decay_weights)
- [`expected_possessions`](reference/additional#expected_possessions)
- [`external_validity`](reference/additional#external_validity)
- [`fit_aging_curve`](reference/additional#fit_aging_curve)
- [`get_constants`](reference/additional#get_constants)
- [`get_shrinkage_k`](reference/additional#get_shrinkage_k)
- [`in_game_features`](reference/additional#in_game_features)
- [`luck_adjusted_response`](reference/additional#luck_adjusted_response)
- [`nba_adj_rapm`](reference/additional#nba_adj_rapm)
- [`nba_aging_curve`](reference/additional#nba_aging_curve)
- [`nba_bpm`](reference/additional#nba_bpm)
- [`nba_career_trajectory`](reference/additional#nba_career_trajectory)
- [`nba_darko`](reference/additional#nba_darko)
- [`nba_decay_rapm`](reference/additional#nba_decay_rapm)
- [`nba_draft_model`](reference/additional#nba_draft_model)
- [`nba_expected_turnovers`](reference/additional#nba_expected_turnovers)
- [`nba_four_factor_rapm`](reference/additional#nba_four_factor_rapm)
- [`nba_in_game_win_prob`](reference/additional#nba_in_game_win_prob)
- [`nba_la_rapm`](reference/additional#nba_la_rapm)
- [`nba_matchup_drapm`](reference/additional#nba_matchup_drapm)
- [`nba_predict_games`](reference/additional#nba_predict_games)
- [`nba_rookie_projection`](reference/additional#nba_rookie_projection)
- [`nba_spm`](reference/additional#nba_spm)
- [`nba_team_ratings`](reference/additional#nba_team_ratings)
- [`nba_war`](reference/additional#nba_war)
- [`predict_margin`](reference/additional#predict_margin)
- [`predict_total`](reference/additional#predict_total)
- [`raw_game_efficiency`](reference/additional#raw_game_efficiency)
- [`render_report`](reference/additional#render_report)
- [`train_spm`](reference/additional#train_spm)
- [`validate_model`](reference/additional#validate_model)
- [`walk_forward`](reference/additional#walk_forward)
- [`win_prob_from_margin`](reference/additional#win_prob_from_margin)

### Analytics {#analytics}

- [`add_ctg_shot_zones`](reference/additional#add_ctg_shot_zones)
- [`add_play_context`](reference/additional#add_play_context)
- [`add_start_type_detail`](reference/additional#add_start_type_detail)
- [`add_transition`](reference/additional#add_transition)
- [`box_features`](reference/additional#box_features)
- [`clutch_delta`](reference/additional#clutch_delta)
- [`flag_garbage_time`](reference/additional#flag_garbage_time)
- [`flag_heave_possessions`](reference/additional#flag_heave_possessions)
- [`hoopshype_salaries`](reference/additional#hoopshype_salaries)
- [`lineup_play_context`](reference/additional#lineup_play_context)
- [`make_prob_by_context`](reference/additional#make_prob_by_context)
- [`make_prob_joint`](reference/additional#make_prob_joint)
- [`nba_availability`](reference/additional#nba_availability)
- [`nba_box_logs`](reference/additional#nba_box_logs)
- [`nba_foul_drawing`](reference/additional#nba_foul_drawing)
- [`nba_l2m`](reference/additional#nba_l2m)
- [`nba_l2m_games`](reference/additional#nba_l2m_games)
- [`nba_play_context`](reference/additional#nba_play_context)
- [`nba_player_ages`](reference/additional#nba_player_ages)
- [`nba_player_identity`](reference/additional#nba_player_identity)
- [`nba_player_positions`](reference/additional#nba_player_positions)
- [`nba_player_props`](reference/additional#nba_player_props)
- [`nba_playtype_ratings`](reference/additional#nba_playtype_ratings)
- [`nba_ratings_panel`](reference/additional#nba_ratings_panel)
- [`nba_raw_store_season_frame`](reference/additional#nba_raw_store_season_frame)
- [`nba_referee_assignments`](reference/additional#nba_referee_assignments)
- [`nba_shot_value`](reference/additional#nba_shot_value)
- [`nba_shot_value_lineups`](reference/additional#nba_shot_value_lineups)
- [`nba_team_clutch`](reference/additional#nba_team_clutch)
- [`nba_tracking_drive_value`](reference/additional#nba_tracking_drive_value)
- [`nba_tracking_pass_value`](reference/additional#nba_tracking_pass_value)
- [`nba_tracking_reb_oe`](reference/additional#nba_tracking_reb_oe)
- [`nba_tracking_rim_protect_value`](reference/additional#nba_tracking_rim_protect_value)
- [`nba_tracking_shot_diet_value`](reference/additional#nba_tracking_shot_diet_value)
- [`nba_tracking_touch_value`](reference/additional#nba_tracking_touch_value)
- [`nbadraft_mock_draft`](reference/additional#nbadraft_mock_draft)
- [`player_play_context`](reference/additional#player_play_context)
- [`player_rates`](reference/additional#player_rates)
- [`players_on_court_from_pbp`](reference/additional#players_on_court_from_pbp)
- [`players_on_court_from_quarter_boxscores`](reference/additional#players_on_court_from_quarter_boxscores)
- [`players_on_court_from_rotation`](reference/additional#players_on_court_from_rotation)
- [`prob_over`](reference/additional#prob_over)
- [`project_player_line`](reference/additional#project_player_line)
- [`prop_distribution`](reference/additional#prop_distribution)
- [`ratings_as_of`](reference/additional#ratings_as_of)
- [`rotowire_injuries`](reference/additional#rotowire_injuries)
- [`score_shot_xpoints`](reference/additional#score_shot_xpoints)
- [`shooter_talent`](reference/additional#shooter_talent)
- [`shot_selection_quality`](reference/additional#shot_selection_quality)
- [`shrink_clutch`](reference/additional#shrink_clutch)
- [`spotrac_team_cap`](reference/additional#spotrac_team_cap)
- [`starters_on_court_counts`](reference/additional#starters_on_court_counts)
- [`team_pace_projection`](reference/additional#team_pace_projection)
- [`team_play_context`](reference/additional#team_play_context)
- [`xpoints_baseline`](reference/additional#xpoints_baseline)
- [`zone_value_map`](reference/additional#zone_value_map)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_nba_season`](reference/additional#most_recent_nba_season)
- [`year_to_season`](reference/additional#year_to_season)

### IDs and crosswalks {#ids-and-crosswalks}

- [`nba_player_crosswalk`](reference/additional#nba_player_crosswalk)
- [`nba_schedule_crosswalk`](reference/additional#nba_schedule_crosswalk)
- [`nba_team_crosswalk`](reference/additional#nba_team_crosswalk)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [NBA tutorial](../tutorials/04_nba_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.nba` (Python) | `hoopR` (R) |
|---|---|
| [`bref_awards`](reference/additional/basketball-reference#bref_awards) | [`bref_awards`](https://hoopR.sportsdataverse.org/reference/bref_awards.html) |
| [`bref_draft`](reference/additional/basketball-reference#bref_draft) | [`bref_draft`](https://hoopR.sportsdataverse.org/reference/bref_draft.html) |
| [`bref_injuries`](reference/additional/basketball-reference#bref_injuries) | [`bref_injuries`](https://hoopR.sportsdataverse.org/reference/bref_injuries.html) |
| [`bref_player_bios`](reference/additional/basketball-reference#bref_player_bios) | [`bref_player_bios`](https://hoopR.sportsdataverse.org/reference/bref_player_bios.html) |
| [`bref_player_game_log`](reference/additional/basketball-reference#bref_player_game_log) | [`bref_player_game_log`](https://hoopR.sportsdataverse.org/reference/bref_player_game_log.html) |
| [`bref_players_stats`](reference/additional/highlights#bref_players_stats) | [`bref_players_stats`](https://hoopR.sportsdataverse.org/reference/bref_players_stats.html) |
| [`bref_standings`](reference/additional/highlights#bref_standings) | [`bref_standings`](https://hoopR.sportsdataverse.org/reference/bref_standings.html) |
| [`bref_team_roster`](reference/additional/basketball-reference#bref_team_roster) | [`bref_team_roster`](https://hoopR.sportsdataverse.org/reference/bref_team_roster.html) |
| [`bref_teams_stats`](reference/additional/highlights#bref_teams_stats) | [`bref_teams_stats`](https://hoopR.sportsdataverse.org/reference/bref_teams_stats.html) |
| [`espn_nba_award`](reference/core/other#espn_nba_award) | [`espn_nba_award`](https://hoopR.sportsdataverse.org/reference/espn_nba_award.html) |
| [`espn_nba_calendar`](reference/site#espn_nba_calendar) | [`espn_nba_calendar`](https://hoopR.sportsdataverse.org/reference/espn_nba_calendar.html) |
| [`espn_nba_coach`](reference/core/other#espn_nba_coach) | [`espn_nba_coach`](https://hoopR.sportsdataverse.org/reference/espn_nba_coach.html) |
| [`espn_nba_coach_record`](reference/core/other#espn_nba_coach_record) | [`espn_nba_coach_record`](https://hoopR.sportsdataverse.org/reference/espn_nba_coach_record.html) |
| [`espn_nba_coach_season`](reference/core/other#espn_nba_coach_season) | [`espn_nba_coach_season`](https://hoopR.sportsdataverse.org/reference/espn_nba_coach_season.html) |
| [`espn_nba_conferences`](reference/site#espn_nba_conferences) | [`espn_nba_conferences`](https://hoopR.sportsdataverse.org/reference/espn_nba_conferences.html) |
| [`espn_nba_draft`](reference/site#espn_nba_draft) | [`espn_nba_draft`](https://hoopR.sportsdataverse.org/reference/espn_nba_draft.html) |
| [`espn_nba_franchise`](reference/core/other#espn_nba_franchise) | [`espn_nba_franchise`](https://hoopR.sportsdataverse.org/reference/espn_nba_franchise.html) |
| [`espn_nba_franchises`](reference/core/other#espn_nba_franchises) | [`espn_nba_franchises`](https://hoopR.sportsdataverse.org/reference/espn_nba_franchises.html) |
| [`espn_nba_game_broadcasts`](reference/core/game#espn_nba_game_broadcasts) | [`espn_nba_game_broadcasts`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_broadcasts.html) |
| [`espn_nba_game_odds`](reference/core/game#espn_nba_game_odds) | [`espn_nba_game_odds`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_odds.html) |
| [`espn_nba_game_official_detail`](reference/core/game#espn_nba_game_official_detail) | [`espn_nba_game_official_detail`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_official_detail.html) |
| [`espn_nba_game_officials`](reference/core/game#espn_nba_game_officials) | [`espn_nba_game_officials`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_officials.html) |
| [`espn_nba_game_play`](reference/core/game#espn_nba_game_play) | [`espn_nba_game_play`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_play.html) |
| [`espn_nba_game_play_personnel`](reference/core/game#espn_nba_game_play_personnel) | [`espn_nba_game_play_personnel`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_play_personnel.html) |
| [`espn_nba_game_powerindex`](reference/core/game#espn_nba_game_powerindex) | [`espn_nba_game_powerindex`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_powerindex.html) |
| [`espn_nba_game_predictor`](reference/core/game#espn_nba_game_predictor) | [`espn_nba_game_predictor`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_predictor.html) |
| [`espn_nba_game_probabilities`](reference/core/game#espn_nba_game_probabilities) | [`espn_nba_game_probabilities`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_probabilities.html) |
| [`espn_nba_game_propbets`](reference/core/game#espn_nba_game_propbets) | [`espn_nba_game_propbets`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_propbets.html) |
| [`espn_nba_game_rosters`](reference/additional/other#espn_nba_game_rosters) | [`espn_nba_game_rosters`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_rosters.html) |
| [`espn_nba_game_situation`](reference/core/game#espn_nba_game_situation) | [`espn_nba_game_situation`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_situation.html) |
| [`espn_nba_game_team_leaders`](reference/core/game#espn_nba_game_team_leaders) | [`espn_nba_game_team_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_leaders.html) |
| [`espn_nba_game_team_linescores`](reference/core/game#espn_nba_game_team_linescores) | [`espn_nba_game_team_linescores`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_linescores.html) |
| [`espn_nba_game_team_roster`](reference/core/game#espn_nba_game_team_roster) | [`espn_nba_game_team_roster`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_roster.html) |
| [`espn_nba_game_team_statistics`](reference/core/game#espn_nba_game_team_statistics) | [`espn_nba_game_team_statistics`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_statistics.html) |
| [`espn_nba_injuries`](reference/site#espn_nba_injuries) | [`espn_nba_injuries`](https://hoopR.sportsdataverse.org/reference/espn_nba_injuries.html) |
| [`espn_nba_leaders`](reference/web#espn_nba_leaders) | [`espn_nba_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_leaders.html) |
| [`espn_nba_news`](reference/site#espn_nba_news) | [`espn_nba_news`](https://hoopR.sportsdataverse.org/reference/espn_nba_news.html) |
| [`espn_nba_pbp`](reference/additional/other#espn_nba_pbp) | [`espn_nba_pbp`](https://hoopR.sportsdataverse.org/reference/espn_nba_pbp.html) |
| [`espn_nba_player_awards`](reference/core/player#espn_nba_player_awards) | [`espn_nba_player_awards`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_awards.html) |
| [`espn_nba_player_career_stats`](reference/core/player#espn_nba_player_career_stats) | [`espn_nba_player_career_stats`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_career_stats.html) |
| [`espn_nba_player_contracts`](reference/core/player#espn_nba_player_contracts) | [`espn_nba_player_contracts`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_contracts.html) |
| [`espn_nba_player_eventlog`](reference/core/player#espn_nba_player_eventlog) | [`espn_nba_player_eventlog`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_eventlog.html) |
| [`espn_nba_player_gamelog`](reference/web#espn_nba_player_gamelog) | [`espn_nba_player_gamelog`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_gamelog.html) |
| [`espn_nba_player_info`](reference/site#espn_nba_player_info) | [`espn_nba_player_info`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_info.html) |
| [`espn_nba_player_overview`](reference/web#espn_nba_player_overview) | [`espn_nba_player_overview`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_overview.html) |
| [`espn_nba_player_seasons`](reference/core/player#espn_nba_player_seasons) | [`espn_nba_player_seasons`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_seasons.html) |
| [`espn_nba_player_splits`](reference/web#espn_nba_player_splits) | [`espn_nba_player_splits`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_splits.html) |
| [`espn_nba_player_statisticslog`](reference/core/player#espn_nba_player_statisticslog) | [`espn_nba_player_statisticslog`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_statisticslog.html) |
| [`espn_nba_player_stats`](reference/additional/highlights#espn_nba_player_stats) | [`espn_nba_player_stats`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_stats.html) |
| [`espn_nba_player_stats_v3`](reference/web#espn_nba_player_stats_v3) | [`espn_nba_player_stats_v3`](https://hoopR.sportsdataverse.org/reference/espn_nba_player_stats_v3.html) |
| [`espn_nba_position`](reference/core/other#espn_nba_position) | [`espn_nba_position`](https://hoopR.sportsdataverse.org/reference/espn_nba_position.html) |
| [`espn_nba_positions`](reference/core/other#espn_nba_positions) | [`espn_nba_positions`](https://hoopR.sportsdataverse.org/reference/espn_nba_positions.html) |
| [`espn_nba_scoreboard`](reference/site#espn_nba_scoreboard) | [`espn_nba_scoreboard`](https://hoopR.sportsdataverse.org/reference/espn_nba_scoreboard.html) |
| [`espn_nba_season_awards`](reference/core/season#espn_nba_season_awards) | [`espn_nba_season_awards`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_awards.html) |
| [`espn_nba_season_draft`](reference/core/season#espn_nba_season_draft) | [`espn_nba_season_draft`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_draft.html) |
| [`espn_nba_season_group`](reference/core/season#espn_nba_season_group) | [`espn_nba_season_group`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_group.html) |
| [`espn_nba_season_group_children`](reference/core/season#espn_nba_season_group_children) | [`espn_nba_season_group_children`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_group_children.html) |
| [`espn_nba_season_group_teams`](reference/core/season#espn_nba_season_group_teams) | [`espn_nba_season_group_teams`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_group_teams.html) |
| [`espn_nba_season_groups`](reference/core/season#espn_nba_season_groups) | [`espn_nba_season_groups`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_groups.html) |
| [`espn_nba_season_info`](reference/core/season#espn_nba_season_info) | [`espn_nba_season_info`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_info.html) |
| [`espn_nba_season_type`](reference/core/season#espn_nba_season_type) | [`espn_nba_season_type`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_type.html) |
| [`espn_nba_season_types`](reference/core/season#espn_nba_season_types) | [`espn_nba_season_types`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_types.html) |
| [`espn_nba_season_week`](reference/core/season#espn_nba_season_week) | [`espn_nba_season_week`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_week.html) |
| [`espn_nba_season_weeks`](reference/core/season#espn_nba_season_weeks) | [`espn_nba_season_weeks`](https://hoopR.sportsdataverse.org/reference/espn_nba_season_weeks.html) |
| [`espn_nba_seasons`](reference/core/other#espn_nba_seasons) | [`espn_nba_seasons`](https://hoopR.sportsdataverse.org/reference/espn_nba_seasons.html) |
| [`espn_nba_standings`](reference/site#espn_nba_standings) | [`espn_nba_standings`](https://hoopR.sportsdataverse.org/reference/espn_nba_standings.html) |
| [`espn_nba_team`](reference/site#espn_nba_team) | [`espn_nba_team`](https://hoopR.sportsdataverse.org/reference/espn_nba_team.html) |
| [`espn_nba_team_injuries`](reference/site#espn_nba_team_injuries) | [`espn_nba_team_injuries`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_injuries.html) |
| [`espn_nba_team_leaders`](reference/site#espn_nba_team_leaders) | [`espn_nba_team_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_leaders.html) |
| [`espn_nba_team_news`](reference/site#espn_nba_team_news) | [`espn_nba_team_news`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_news.html) |
| [`espn_nba_team_record`](reference/site#espn_nba_team_record) | [`espn_nba_team_record`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_record.html) |
| [`espn_nba_team_roster`](reference/site#espn_nba_team_roster) | [`espn_nba_team_roster`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_roster.html) |
| [`espn_nba_team_schedule`](reference/site#espn_nba_team_schedule) | [`espn_nba_team_schedule`](https://hoopR.sportsdataverse.org/reference/espn_nba_team_schedule.html) |
| [`espn_nba_teams`](reference/additional/highlights#espn_nba_teams) | [`espn_nba_teams`](https://hoopR.sportsdataverse.org/reference/espn_nba_teams.html) |
| [`espn_nba_tournaments`](reference/core/other#espn_nba_tournaments) | [`espn_nba_tournaments`](https://hoopR.sportsdataverse.org/reference/espn_nba_tournaments.html) |
| [`espn_nba_transactions`](reference/site#espn_nba_transactions) | [`espn_nba_transactions`](https://hoopR.sportsdataverse.org/reference/espn_nba_transactions.html) |
| [`espn_nba_venues`](reference/core/other#espn_nba_venues) | [`espn_nba_venues`](https://hoopR.sportsdataverse.org/reference/espn_nba_venues.html) |
| [`fox_nba_boxscore`](reference/additional/fox-sports-api#fox_nba_boxscore) | [`fox_nba_boxscore`](https://hoopR.sportsdataverse.org/reference/fox_nba_boxscore.html) |
| [`fox_nba_league_leaders`](reference/additional/fox-sports-api#fox_nba_league_leaders) | [`fox_nba_league_leaders`](https://hoopR.sportsdataverse.org/reference/fox_nba_league_leaders.html) |
| [`fox_nba_odds`](reference/additional/fox-sports-api#fox_nba_odds) | [`fox_nba_odds`](https://hoopR.sportsdataverse.org/reference/fox_nba_odds.html) |
| [`fox_nba_pbp`](reference/additional/fox-sports-api#fox_nba_pbp) | [`fox_nba_pbp`](https://hoopR.sportsdataverse.org/reference/fox_nba_pbp.html) |
| [`fox_nba_standings`](reference/additional/fox-sports-api#fox_nba_standings) | [`fox_nba_standings`](https://hoopR.sportsdataverse.org/reference/fox_nba_standings.html) |
| [`fox_nba_team_gamelog`](reference/additional/fox-sports-api#fox_nba_team_gamelog) | [`fox_nba_team_gamelog`](https://hoopR.sportsdataverse.org/reference/fox_nba_team_gamelog.html) |
| [`fox_nba_team_roster`](reference/additional/fox-sports-api#fox_nba_team_roster) | [`fox_nba_team_roster`](https://hoopR.sportsdataverse.org/reference/fox_nba_team_roster.html) |
| [`fox_nba_team_stats`](reference/additional/fox-sports-api#fox_nba_team_stats) | [`fox_nba_team_stats`](https://hoopR.sportsdataverse.org/reference/fox_nba_team_stats.html) |
| [`fox_nba_teams`](reference/additional/fox-sports-api#fox_nba_teams) | [`fox_nba_teams`](https://hoopR.sportsdataverse.org/reference/fox_nba_teams.html) |
| [`hoopshype_salaries`](reference/additional/analytics#hoopshype_salaries) | [`hoopshype_salaries`](https://hoopR.sportsdataverse.org/reference/hoopshype_salaries.html) |
| [`load_nba_draft`](reference/loaders/other#load_nba_draft) | [`load_nba_draft`](https://hoopR.sportsdataverse.org/reference/load_nba_draft.html) |
| [`load_nba_game_rosters`](reference/loaders/other#load_nba_game_rosters) | [`load_nba_game_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_game_rosters.html) |
| [`load_nba_group_aliases`](reference/loaders/other#load_nba_group_aliases) | [`load_nba_group_aliases`](https://hoopR.sportsdataverse.org/reference/load_nba_group_aliases.html) |
| [`load_nba_group_seasons`](reference/loaders/other#load_nba_group_seasons) | [`load_nba_group_seasons`](https://hoopR.sportsdataverse.org/reference/load_nba_group_seasons.html) |
| [`load_nba_groups`](reference/loaders/other#load_nba_groups) | [`load_nba_groups`](https://hoopR.sportsdataverse.org/reference/load_nba_groups.html) |
| [`load_nba_officials`](reference/loaders/other#load_nba_officials) | [`load_nba_officials`](https://hoopR.sportsdataverse.org/reference/load_nba_officials.html) |
| [`load_nba_pbp`](reference/loaders/other#load_nba_pbp) | [`load_nba_pbp`](https://hoopR.sportsdataverse.org/reference/load_nba_pbp.html) |
| [`load_nba_player_core`](reference/loaders/player#load_nba_player_core) | [`load_nba_player_core`](https://hoopR.sportsdataverse.org/reference/load_nba_player_core.html) |
| [`load_nba_player_crosswalk`](reference/loaders/player#load_nba_player_crosswalk) | [`load_nba_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_nba_player_crosswalk.html) |
| [`load_nba_player_impact`](reference/loaders/player#load_nba_player_impact) | [`load_nba_player_impact`](https://hoopR.sportsdataverse.org/reference/load_nba_player_impact.html) |
| [`load_nba_rosters`](reference/loaders/other#load_nba_rosters) | [`load_nba_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_rosters.html) |
| [`load_nba_schedule`](reference/loaders/other#load_nba_schedule) | [`load_nba_schedule`](https://hoopR.sportsdataverse.org/reference/load_nba_schedule.html) |
| [`load_nba_schedule_crosswalk`](reference/loaders/other#load_nba_schedule_crosswalk) | [`load_nba_schedule_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_nba_schedule_crosswalk.html) |
| [`load_nba_shots`](reference/loaders/other#load_nba_shots) | [`load_nba_shots`](https://hoopR.sportsdataverse.org/reference/load_nba_shots.html) |
| [`load_nba_standings`](reference/loaders/other#load_nba_standings) | [`load_nba_standings`](https://hoopR.sportsdataverse.org/reference/load_nba_standings.html) |
| [`load_nba_stats_coaches`](reference/loaders/stats#load_nba_stats_coaches) | [`load_nba_stats_coaches`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_coaches.html) |
| [`load_nba_stats_game_lineups`](reference/loaders/stats#load_nba_stats_game_lineups) | [`load_nba_stats_game_lineups`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_game_lineups.html) |
| [`load_nba_stats_game_rosters`](reference/loaders/stats#load_nba_stats_game_rosters) | [`load_nba_stats_game_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_game_rosters.html) |
| [`load_nba_stats_leaguedash`](reference/additional/other#load_nba_stats_leaguedash) | [`load_nba_stats_leaguedash`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_leaguedash.html) |
| [`load_nba_stats_lineups`](reference/loaders/stats#load_nba_stats_lineups) | [`load_nba_stats_lineups`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_lineups.html) |
| [`load_nba_stats_officials`](reference/loaders/stats#load_nba_stats_officials) | [`load_nba_stats_officials`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_officials.html) |
| [`load_nba_stats_pbp`](reference/loaders/stats#load_nba_stats_pbp) | [`load_nba_stats_pbp`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_pbp.html) |
| [`load_nba_stats_player_boxscores`](reference/loaders/stats#load_nba_stats_player_boxscores) | [`load_nba_stats_player_boxscores`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_player_boxscores.html) |
| [`load_nba_stats_player_game_logs`](reference/loaders/stats#load_nba_stats_player_game_logs) | [`load_nba_stats_player_game_logs`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_player_game_logs.html) |
| [`load_nba_stats_player_season_stats`](reference/loaders/stats#load_nba_stats_player_season_stats) | [`load_nba_stats_player_season_stats`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_player_season_stats.html) |
| [`load_nba_stats_possessions`](reference/loaders/stats#load_nba_stats_possessions) | [`load_nba_stats_possessions`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_possessions.html) |
| [`load_nba_stats_rosters`](reference/loaders/stats-2#load_nba_stats_rosters) | [`load_nba_stats_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_rosters.html) |
| [`load_nba_stats_shots`](reference/loaders/stats-2#load_nba_stats_shots) | [`load_nba_stats_shots`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_shots.html) |
| [`load_nba_stats_standings`](reference/loaders/stats-2#load_nba_stats_standings) | [`load_nba_stats_standings`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_standings.html) |
| [`load_nba_stats_team_boxscores`](reference/loaders/stats-2#load_nba_stats_team_boxscores) | [`load_nba_stats_team_boxscores`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_team_boxscores.html) |
| [`load_nba_stats_team_season_stats`](reference/loaders/stats-2#load_nba_stats_team_season_stats) | [`load_nba_stats_team_season_stats`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_team_season_stats.html) |
| [`load_nba_team_crosswalk`](reference/loaders/team#load_nba_team_crosswalk) | [`load_nba_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_nba_team_crosswalk.html) |
| [`load_nba_team_group_seasons`](reference/loaders/team#load_nba_team_group_seasons) | [`load_nba_team_group_seasons`](https://hoopR.sportsdataverse.org/reference/load_nba_team_group_seasons.html) |
| [`most_recent_nba_season`](reference/additional/highlights#most_recent_nba_season) | [`most_recent_nba_season`](https://hoopR.sportsdataverse.org/reference/most_recent_nba_season.html) |
| [`nba_l2m`](reference/additional/analytics#nba_l2m) | [`nba_l2m`](https://hoopR.sportsdataverse.org/reference/nba_l2m.html) |
| [`nba_l2m_games`](reference/additional/analytics#nba_l2m_games) | [`nba_l2m_games`](https://hoopR.sportsdataverse.org/reference/nba_l2m_games.html) |
| [`nba_live_boxscore`](reference/additional/nba-stats-api#nba_live_boxscore) | [`nba_live_boxscore`](https://hoopR.sportsdataverse.org/reference/nba_live_boxscore.html) |
| [`nba_live_pbp`](reference/additional/nba-stats-api#nba_live_pbp) | [`nba_live_pbp`](https://hoopR.sportsdataverse.org/reference/nba_live_pbp.html) |
| [`nba_player_crosswalk`](reference/additional/other#nba_player_crosswalk) | [`nba_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/nba_player_crosswalk.html) |
| [`nba_referee_assignments`](reference/additional/analytics#nba_referee_assignments) | [`nba_referee_assignments`](https://hoopR.sportsdataverse.org/reference/nba_referee_assignments.html) |
| [`nba_schedule_crosswalk`](reference/additional/other#nba_schedule_crosswalk) | [`nba_schedule_crosswalk`](https://hoopR.sportsdataverse.org/reference/nba_schedule_crosswalk.html) |
| [`nba_team_crosswalk`](reference/additional/other#nba_team_crosswalk) | [`nba_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/nba_team_crosswalk.html) |
| [`nbadraft_mock_draft`](reference/additional/analytics-2#nbadraft_mock_draft) | [`nbadraft_mock_draft`](https://hoopR.sportsdataverse.org/reference/nbadraft_mock_draft.html) |
| [`realgm_coaches`](reference/additional/realgm#realgm_coaches) | [`realgm_coaches`](https://hoopR.sportsdataverse.org/reference/realgm_coaches.html) |
| [`realgm_draft`](reference/additional/realgm#realgm_draft) | [`realgm_draft`](https://hoopR.sportsdataverse.org/reference/realgm_draft.html) |
| [`realgm_draft_prospects`](reference/additional/realgm#realgm_draft_prospects) | [`realgm_draft_prospects`](https://hoopR.sportsdataverse.org/reference/realgm_draft_prospects.html) |
| [`realgm_early_entry`](reference/additional/realgm#realgm_early_entry) | [`realgm_early_entry`](https://hoopR.sportsdataverse.org/reference/realgm_early_entry.html) |
| [`realgm_future_free_agents`](reference/additional/realgm#realgm_future_free_agents) | [`realgm_future_free_agents`](https://hoopR.sportsdataverse.org/reference/realgm_future_free_agents.html) |
| [`realgm_gms`](reference/additional/realgm#realgm_gms) | [`realgm_gms`](https://hoopR.sportsdataverse.org/reference/realgm_gms.html) |
| [`realgm_individual_games`](reference/additional/realgm#realgm_individual_games) | [`realgm_individual_games`](https://hoopR.sportsdataverse.org/reference/realgm_individual_games.html) |
| [`realgm_individual_seasons`](reference/additional/realgm#realgm_individual_seasons) | [`realgm_individual_seasons`](https://hoopR.sportsdataverse.org/reference/realgm_individual_seasons.html) |
| [`realgm_player_stats`](reference/additional/realgm#realgm_player_stats) | [`realgm_player_stats`](https://hoopR.sportsdataverse.org/reference/realgm_player_stats.html) |
| [`realgm_players`](reference/additional/realgm#realgm_players) | [`realgm_players`](https://hoopR.sportsdataverse.org/reference/realgm_players.html) |
| [`realgm_players_abroad`](reference/additional/realgm#realgm_players_abroad) | [`realgm_players_abroad`](https://hoopR.sportsdataverse.org/reference/realgm_players_abroad.html) |
| [`realgm_rookie_scale`](reference/additional/realgm#realgm_rookie_scale) | [`realgm_rookie_scale`](https://hoopR.sportsdataverse.org/reference/realgm_rookie_scale.html) |
| [`realgm_salary_cap`](reference/additional/realgm#realgm_salary_cap) | [`realgm_salary_cap`](https://hoopR.sportsdataverse.org/reference/realgm_salary_cap.html) |
| [`realgm_standings`](reference/additional/realgm#realgm_standings) | [`realgm_standings`](https://hoopR.sportsdataverse.org/reference/realgm_standings.html) |
| [`realgm_team_stats`](reference/additional/realgm#realgm_team_stats) | [`realgm_team_stats`](https://hoopR.sportsdataverse.org/reference/realgm_team_stats.html) |
| [`realgm_teams`](reference/additional/realgm#realgm_teams) | [`realgm_teams`](https://hoopR.sportsdataverse.org/reference/realgm_teams.html) |
| [`realgm_transactions`](reference/additional/realgm#realgm_transactions) | [`realgm_transactions`](https://hoopR.sportsdataverse.org/reference/realgm_transactions.html) |
| [`rotowire_injuries`](reference/additional/analytics-2#rotowire_injuries) | [`rotowire_injuries`](https://hoopR.sportsdataverse.org/reference/rotowire_injuries.html) |
| [`spotrac_team_cap`](reference/additional/analytics-2#spotrac_team_cap) | [`spotrac_team_cap`](https://hoopR.sportsdataverse.org/reference/spotrac_team_cap.html) |
| [`year_to_season`](reference/additional/other#year_to_season) | [`year_to_season`](https://hoopR.sportsdataverse.org/reference/year_to_season.html) |
