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
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 123 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 54 | none |
| [NBA Stats API](#nba-stats-api) | `stats.nba.com` | 130 | none (curl_cffi chrome TLS impersonation) |
| [NBA Official Injury Report](#nba-official-injury-report) | `official.nba.com` | 3 | none (static PDFs on ak-static.cms.nba.com) |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 26 | none |
| [Basketball-Reference](#basketball-reference) | `www.basketball-reference.com` | 9 | none (slow, owner agreement) |
| [RealGM](#realgm) | `basketball.realgm.com` | 18 | none |
| [Public model datasets](#public-model-datasets) | `dunksandthrees.com` | 7 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 206 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 82 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |
| [Hand-written wrappers](reference/additional/espn) | 7 |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 43 |
| [Hand-written wrappers](reference/additional/sportsdataverse-data-releases) | 11 |

## NBA Stats API {#nba-stats-api}

| Reference | Functions |
|---|---:|
| [NBA Stats API (stats.nba.com)](reference/nba_stats) | 128 |
| [Hand-written wrappers](reference/additional/nba-stats-api) | 2 |

## NBA Official Injury Report {#nba-official-injury-report}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/nba-official-injury-report) | 3 |

## Fox Sports API {#fox-sports-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/fox-sports-api) | 26 |

## Basketball-Reference {#basketball-reference}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/basketball-reference) | 9 |

## RealGM {#realgm}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/realgm) | 18 |

## Public model datasets {#public-model-datasets}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/public-model-datasets) | 7 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`build_athlete_identity_lookup`](reference/additional/play-by-play-processing#build_athlete_identity_lookup)
- [`build_nba_player_identity_lookup`](reference/additional/play-by-play-processing#build_nba_player_identity_lookup)
- [`compile_nba_season`](reference/additional/highlights#compile_nba_season)
- [`nba_pbp_disk`](reference/additional/play-by-play-processing#nba_pbp_disk)
- [`nba_v3_to_v2_pbp`](reference/additional/play-by-play-processing#nba_v3_to_v2_pbp)

### Models and calculators {#models-and-calculators}

- [`AdjRapmModel`](reference/additional/models-and-calculators#AdjRapmModel)
- [`AgingCurve`](reference/additional/models-and-calculators#AgingCurve)
- [`ExternalValidityResult`](reference/additional/models-and-calculators#ExternalValidityResult)
- [`ForecastResult`](reference/additional/models-and-calculators#ForecastResult)
- [`LeagueConstants`](reference/additional/models-and-calculators#LeagueConstants)
- [`MeasureSpec`](reference/additional/models-and-calculators#MeasureSpec)
- [`NbaBpmModel`](reference/additional/models-and-calculators#NbaBpmModel)
- [`NbaSpmModel`](reference/additional/models-and-calculators#NbaSpmModel)
- [`RidgeRapmModel`](reference/additional/models-and-calculators#RidgeRapmModel)
- [`SpmCoefficients`](reference/additional/models-and-calculators#SpmCoefficients)
- [`ValidationReport`](reference/additional/models-and-calculators#ValidationReport)
- [`WalkForwardResult`](reference/additional/models-and-calculators#WalkForwardResult)
- [`adjust_efficiency`](reference/additional/models-and-calculators#adjust_efficiency)
- [`adjust_pace`](reference/additional/models-and-calculators#adjust_pace)
- [`as_of_ratings_split`](reference/additional/models-and-calculators#as_of_ratings_split)
- [`calibrate_pts_per_win`](reference/additional/models-and-calculators#calibrate_pts_per_win)
- [`calibrate_replacement_level`](reference/additional/models-and-calculators#calibrate_replacement_level)
- [`darko_forecast_accuracy`](reference/additional/models-and-calculators#darko_forecast_accuracy)
- [`decay_weights`](reference/additional/models-and-calculators#decay_weights)
- [`expected_possessions`](reference/additional/models-and-calculators#expected_possessions)
- [`external_validity`](reference/additional/models-and-calculators#external_validity)
- [`fit_aging_curve`](reference/additional/models-and-calculators#fit_aging_curve)
- [`game_minutes`](reference/additional/models-and-calculators#game_minutes)
- [`game_page_frames`](reference/additional/models-and-calculators#game_page_frames)
- [`game_page_tables`](reference/additional/models-and-calculators#game_page_tables)
- [`game_regulation_periods`](reference/additional/models-and-calculators#game_regulation_periods)
- [`get_constants`](reference/additional/models-and-calculators#get_constants)
- [`get_shrinkage_k`](reference/additional/models-and-calculators#get_shrinkage_k)
- [`home_prior`](reference/additional/models-and-calculators#home_prior)
- [`in_game_features`](reference/additional/models-and-calculators#in_game_features)
- [`league_pbp_functions`](reference/additional/models-and-calculators#league_pbp_functions)
- [`luck_adjusted_response`](reference/additional/models-and-calculators#luck_adjusted_response)
- [`nba_adj_rapm`](reference/additional/models-and-calculators#nba_adj_rapm)
- [`nba_aging_curve`](reference/additional/models-and-calculators#nba_aging_curve)
- [`nba_bpm`](reference/additional/models-and-calculators#nba_bpm)
- [`nba_career_trajectory`](reference/additional/models-and-calculators#nba_career_trajectory)
- [`nba_darko`](reference/additional/models-and-calculators#nba_darko)
- [`nba_decay_rapm`](reference/additional/models-and-calculators#nba_decay_rapm)
- [`nba_draft_model`](reference/additional/models-and-calculators#nba_draft_model)
- [`nba_expected_turnovers`](reference/additional/models-and-calculators#nba_expected_turnovers)
- [`nba_four_factor_rapm`](reference/additional/models-and-calculators-2#nba_four_factor_rapm)
- [`nba_in_game_win_prob`](reference/additional/models-and-calculators-2#nba_in_game_win_prob)
- [`nba_la_rapm`](reference/additional/models-and-calculators-2#nba_la_rapm)
- [`nba_matchup_drapm`](reference/additional/models-and-calculators-2#nba_matchup_drapm)
- [`nba_predict_games`](reference/additional/models-and-calculators-2#nba_predict_games)
- [`nba_rookie_projection`](reference/additional/models-and-calculators-2#nba_rookie_projection)
- [`nba_spm`](reference/additional/models-and-calculators-2#nba_spm)
- [`nba_team_ratings`](reference/additional/models-and-calculators-2#nba_team_ratings)
- [`nba_war`](reference/additional/models-and-calculators-2#nba_war)
- [`ot_adjusted_seconds`](reference/additional/models-and-calculators-2#ot_adjusted_seconds)
- [`player_game_rates`](reference/additional/models-and-calculators-2#player_game_rates)
- [`predict_margin`](reference/additional/models-and-calculators-2#predict_margin)
- [`predict_total`](reference/additional/models-and-calculators-2#predict_total)
- [`raw_game_efficiency`](reference/additional/models-and-calculators-2#raw_game_efficiency)
- [`render_report`](reference/additional/models-and-calculators-2#render_report)
- [`score_win_prob`](reference/additional/models-and-calculators-2#score_win_prob)
- [`shot_coordinates`](reference/additional/models-and-calculators-2#shot_coordinates)
- [`team_game_rates`](reference/additional/models-and-calculators-2#team_game_rates)
- [`train_spm`](reference/additional/models-and-calculators-2#train_spm)
- [`validate_model`](reference/additional/models-and-calculators-2#validate_model)
- [`walk_forward`](reference/additional/models-and-calculators-2#walk_forward)
- [`win_prob_from_margin`](reference/additional/models-and-calculators-2#win_prob_from_margin)

### Analytics {#analytics}

- [`add_ctg_shot_zones`](reference/additional/analytics#add_ctg_shot_zones)
- [`add_play_context`](reference/additional/analytics#add_play_context)
- [`add_start_type_detail`](reference/additional/analytics#add_start_type_detail)
- [`add_transition`](reference/additional/analytics#add_transition)
- [`box_features`](reference/additional/analytics#box_features)
- [`build_play_context_shots`](reference/additional/analytics#build_play_context_shots)
- [`build_possession_shooting`](reference/additional/analytics#build_possession_shooting)
- [`clutch_delta`](reference/additional/analytics#clutch_delta)
- [`flag_garbage_time`](reference/additional/analytics#flag_garbage_time)
- [`flag_heave_possessions`](reference/additional/analytics#flag_heave_possessions)
- [`hoopshype_salaries`](reference/additional/analytics#hoopshype_salaries)
- [`lineup_play_context`](reference/additional/analytics#lineup_play_context)
- [`make_prob_by_context`](reference/additional/analytics#make_prob_by_context)
- [`make_prob_joint`](reference/additional/analytics#make_prob_joint)
- [`nba_availability`](reference/additional/analytics#nba_availability)
- [`nba_box_logs`](reference/additional/analytics#nba_box_logs)
- [`nba_foul_drawing`](reference/additional/analytics#nba_foul_drawing)
- [`nba_l2m`](reference/additional/analytics#nba_l2m)
- [`nba_l2m_games`](reference/additional/analytics#nba_l2m_games)
- [`nba_play_context`](reference/additional/analytics#nba_play_context)
- [`nba_player_ages`](reference/additional/analytics#nba_player_ages)
- [`nba_player_identity`](reference/additional/analytics#nba_player_identity)
- [`nba_player_positions`](reference/additional/analytics#nba_player_positions)
- [`nba_player_props`](reference/additional/analytics#nba_player_props)
- [`nba_playtype_ratings`](reference/additional/analytics#nba_playtype_ratings)
- [`nba_ratings_panel`](reference/additional/analytics#nba_ratings_panel)
- [`nba_raw_store_season_frame`](reference/additional/analytics#nba_raw_store_season_frame)
- [`nba_referee_assignments`](reference/additional/analytics-2#nba_referee_assignments)
- [`nba_shot_value`](reference/additional/analytics-2#nba_shot_value)
- [`nba_shot_value_lineups`](reference/additional/analytics-2#nba_shot_value_lineups)
- [`nba_team_clutch`](reference/additional/analytics-2#nba_team_clutch)
- [`nba_tracking_drive_value`](reference/additional/analytics-2#nba_tracking_drive_value)
- [`nba_tracking_pass_value`](reference/additional/analytics-2#nba_tracking_pass_value)
- [`nba_tracking_reb_oe`](reference/additional/analytics-2#nba_tracking_reb_oe)
- [`nba_tracking_rim_protect_value`](reference/additional/analytics-2#nba_tracking_rim_protect_value)
- [`nba_tracking_shot_diet_value`](reference/additional/analytics-2#nba_tracking_shot_diet_value)
- [`nba_tracking_touch_value`](reference/additional/analytics-2#nba_tracking_touch_value)
- [`nbadraft_mock_draft`](reference/additional/analytics-2#nbadraft_mock_draft)
- [`player_play_context`](reference/additional/analytics-2#player_play_context)
- [`player_rates`](reference/additional/analytics-2#player_rates)
- [`players_on_court_from_pbp`](reference/additional/analytics-2#players_on_court_from_pbp)
- [`players_on_court_from_quarter_boxscores`](reference/additional/analytics-2#players_on_court_from_quarter_boxscores)
- [`players_on_court_from_rotation`](reference/additional/analytics-2#players_on_court_from_rotation)
- [`prob_over`](reference/additional/analytics-2#prob_over)
- [`project_player_line`](reference/additional/analytics-2#project_player_line)
- [`prop_distribution`](reference/additional/analytics-2#prop_distribution)
- [`ratings_as_of`](reference/additional/analytics-2#ratings_as_of)
- [`rotowire_injuries`](reference/additional/analytics-2#rotowire_injuries)
- [`score_shot_xpoints`](reference/additional/analytics-2#score_shot_xpoints)
- [`shooter_talent`](reference/additional/analytics-2#shooter_talent)
- [`shot_selection_quality`](reference/additional/analytics-2#shot_selection_quality)
- [`shrink_clutch`](reference/additional/analytics-2#shrink_clutch)
- [`spotrac_team_cap`](reference/additional/analytics-2#spotrac_team_cap)
- [`starters_on_court_counts`](reference/additional/analytics-2#starters_on_court_counts)
- [`team_pace_projection`](reference/additional/analytics-2#team_pace_projection)
- [`team_play_context`](reference/additional/analytics-2#team_play_context)
- [`xpoints_baseline`](reference/additional/analytics-2#xpoints_baseline)
- [`zone_value_map`](reference/additional/analytics-2#zone_value_map)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_nba_season`](reference/additional/highlights#most_recent_nba_season)
- [`year_to_season`](reference/additional/dates-and-seasons#year_to_season)

### IDs and crosswalks {#ids-and-crosswalks}

- [`nba_crosswalks_from_archive`](reference/additional/ids-and-crosswalks#nba_crosswalks_from_archive)
- [`nba_player_crosswalk`](reference/additional/ids-and-crosswalks#nba_player_crosswalk)
- [`nba_schedule_crosswalk`](reference/additional/ids-and-crosswalks#nba_schedule_crosswalk)
- [`nba_team_crosswalk`](reference/additional/ids-and-crosswalks#nba_team_crosswalk)


## See also

- [sdvplot](https://sdvplot.sportsdataverse.org/) — `court_coords()` puts stats-API `x_legacy` / `y_legacy` shots on the court `surface("nba")` draws
- [sportypy](https://sportypy.sportsdataverse.org/) — the regulation NBA court (and every other surface) sdvplot draws with

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
| [`espn_nba_calendar`](reference/additional/espn#espn_nba_calendar) | [`espn_nba_calendar`](https://hoopR.sportsdataverse.org/reference/espn_nba_calendar.html) |
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
| [`espn_nba_game_rosters`](reference/additional/espn#espn_nba_game_rosters) | [`espn_nba_game_rosters`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_rosters.html) |
| [`espn_nba_game_situation`](reference/core/game#espn_nba_game_situation) | [`espn_nba_game_situation`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_situation.html) |
| [`espn_nba_game_team_leaders`](reference/core/game#espn_nba_game_team_leaders) | [`espn_nba_game_team_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_leaders.html) |
| [`espn_nba_game_team_linescores`](reference/core/game#espn_nba_game_team_linescores) | [`espn_nba_game_team_linescores`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_linescores.html) |
| [`espn_nba_game_team_roster`](reference/core/game#espn_nba_game_team_roster) | [`espn_nba_game_team_roster`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_roster.html) |
| [`espn_nba_game_team_statistics`](reference/core/game#espn_nba_game_team_statistics) | [`espn_nba_game_team_statistics`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_team_statistics.html) |
| [`espn_nba_injuries`](reference/site#espn_nba_injuries) | [`espn_nba_injuries`](https://hoopR.sportsdataverse.org/reference/espn_nba_injuries.html) |
| [`espn_nba_leaders`](reference/web#espn_nba_leaders) | [`espn_nba_leaders`](https://hoopR.sportsdataverse.org/reference/espn_nba_leaders.html) |
| [`espn_nba_news`](reference/site#espn_nba_news) | [`espn_nba_news`](https://hoopR.sportsdataverse.org/reference/espn_nba_news.html) |
| [`espn_nba_pbp`](reference/additional/espn#espn_nba_pbp) | [`espn_nba_pbp`](https://hoopR.sportsdataverse.org/reference/espn_nba_pbp.html) |
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
| [`espn_nba_summary`](reference/site#espn_nba_summary) | [`espn_nba_game_all`](https://hoopR.sportsdataverse.org/reference/espn_nba_game_all.html) |
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
| [`load_nba_player_boxscore`](reference/loaders/player#load_nba_player_boxscore) | [`load_nba_player_box`](https://hoopR.sportsdataverse.org/reference/load_nba_player_box.html) |
| [`load_nba_player_core`](reference/loaders/player#load_nba_player_core) | [`load_nba_player_core`](https://hoopR.sportsdataverse.org/reference/load_nba_player_core.html) |
| [`load_nba_player_crosswalk`](reference/loaders/player#load_nba_player_crosswalk) | [`load_nba_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_nba_player_crosswalk.html) |
| [`load_nba_player_impact`](reference/loaders/player#load_nba_player_impact) | [`load_nba_player_impact`](https://hoopR.sportsdataverse.org/reference/load_nba_player_impact.html) |
| [`load_nba_player_season_stats`](reference/loaders/player#load_nba_player_season_stats) | [`load_nba_player_stats`](https://hoopR.sportsdataverse.org/reference/load_nba_player_stats.html) |
| [`load_nba_rosters`](reference/loaders/other#load_nba_rosters) | [`load_nba_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_rosters.html) |
| [`load_nba_schedule`](reference/loaders/other#load_nba_schedule) | [`load_nba_schedule`](https://hoopR.sportsdataverse.org/reference/load_nba_schedule.html) |
| [`load_nba_schedule_crosswalk`](reference/loaders/other#load_nba_schedule_crosswalk) | [`load_nba_schedule_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_nba_schedule_crosswalk.html) |
| [`load_nba_shots`](reference/loaders/other#load_nba_shots) | [`load_nba_shots`](https://hoopR.sportsdataverse.org/reference/load_nba_shots.html) |
| [`load_nba_standings`](reference/loaders/other#load_nba_standings) | [`load_nba_standings`](https://hoopR.sportsdataverse.org/reference/load_nba_standings.html) |
| [`load_nba_stats_coaches`](reference/loaders/stats#load_nba_stats_coaches) | [`load_nba_stats_coaches`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_coaches.html) |
| [`load_nba_stats_draft`](reference/loaders/stats#load_nba_stats_draft) | [`load_nba_stats_draft`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_draft.html) |
| [`load_nba_stats_game_lineups`](reference/loaders/stats#load_nba_stats_game_lineups) | [`load_nba_stats_game_lineups`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_game_lineups.html) |
| [`load_nba_stats_game_rosters`](reference/loaders/stats#load_nba_stats_game_rosters) | [`load_nba_stats_game_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_game_rosters.html) |
| [`load_nba_stats_leaguedash`](reference/additional/sportsdataverse-data-releases#load_nba_stats_leaguedash) | [`load_nba_stats_leaguedash`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_leaguedash.html) |
| [`load_nba_stats_lineups`](reference/loaders/stats#load_nba_stats_lineups) | [`load_nba_stats_lineups`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_lineups.html) |
| [`load_nba_stats_officials`](reference/loaders/stats#load_nba_stats_officials) | [`load_nba_stats_officials`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_officials.html) |
| [`load_nba_stats_pbp`](reference/loaders/stats#load_nba_stats_pbp) | [`load_nba_stats_pbp`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_pbp.html) |
| [`load_nba_stats_player_boxscores`](reference/loaders/stats#load_nba_stats_player_boxscores) | [`load_nba_stats_player_boxscores`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_player_boxscores.html) |
| [`load_nba_stats_player_game_logs`](reference/loaders/stats#load_nba_stats_player_game_logs) | [`load_nba_stats_player_game_logs`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_player_game_logs.html) |
| [`load_nba_stats_player_season_stats`](reference/loaders/stats-2#load_nba_stats_player_season_stats) | [`load_nba_stats_player_season_stats`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_player_season_stats.html) |
| [`load_nba_stats_possessions`](reference/loaders/stats#load_nba_stats_possessions) | [`load_nba_stats_possessions`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_possessions.html) |
| [`load_nba_stats_rosters`](reference/loaders/stats-2#load_nba_stats_rosters) | [`load_nba_stats_rosters`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_rosters.html) |
| [`load_nba_stats_schedules`](reference/loaders/stats#load_nba_stats_schedules) | [`load_nba_stats_schedule`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_schedule.html) |
| [`load_nba_stats_shots`](reference/loaders/stats-2#load_nba_stats_shots) | [`load_nba_stats_shots`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_shots.html) |
| [`load_nba_stats_standings`](reference/loaders/stats-2#load_nba_stats_standings) | [`load_nba_stats_standings`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_standings.html) |
| [`load_nba_stats_team_boxscores`](reference/loaders/stats-2#load_nba_stats_team_boxscores) | [`load_nba_stats_team_boxscores`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_team_boxscores.html) |
| [`load_nba_stats_team_season_stats`](reference/loaders/stats-2#load_nba_stats_team_season_stats) | [`load_nba_stats_team_season_stats`](https://hoopR.sportsdataverse.org/reference/load_nba_stats_team_season_stats.html) |
| [`load_nba_team_boxscore`](reference/loaders/team#load_nba_team_boxscore) | [`load_nba_team_box`](https://hoopR.sportsdataverse.org/reference/load_nba_team_box.html) |
| [`load_nba_team_crosswalk`](reference/loaders/team#load_nba_team_crosswalk) | [`load_nba_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_nba_team_crosswalk.html) |
| [`load_nba_team_group_seasons`](reference/loaders/team#load_nba_team_group_seasons) | [`load_nba_team_group_seasons`](https://hoopR.sportsdataverse.org/reference/load_nba_team_group_seasons.html) |
| [`load_nba_team_season_stats`](reference/loaders/team#load_nba_team_season_stats) | [`load_nba_team_stats`](https://hoopR.sportsdataverse.org/reference/load_nba_team_stats.html) |
| [`most_recent_nba_season`](reference/additional/highlights#most_recent_nba_season) | [`most_recent_nba_season`](https://hoopR.sportsdataverse.org/reference/most_recent_nba_season.html) |
| [`nba_l2m`](reference/additional/analytics#nba_l2m) | [`nba_l2m`](https://hoopR.sportsdataverse.org/reference/nba_l2m.html) |
| [`nba_l2m_games`](reference/additional/analytics#nba_l2m_games) | [`nba_l2m_games`](https://hoopR.sportsdataverse.org/reference/nba_l2m_games.html) |
| [`nba_live_boxscore`](reference/additional/nba-stats-api#nba_live_boxscore) | [`nba_live_boxscore`](https://hoopR.sportsdataverse.org/reference/nba_live_boxscore.html) |
| [`nba_live_pbp`](reference/additional/nba-stats-api#nba_live_pbp) | [`nba_live_pbp`](https://hoopR.sportsdataverse.org/reference/nba_live_pbp.html) |
| [`nba_player_crosswalk`](reference/additional/ids-and-crosswalks#nba_player_crosswalk) | [`nba_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/nba_player_crosswalk.html) |
| [`nba_referee_assignments`](reference/additional/analytics-2#nba_referee_assignments) | [`nba_referee_assignments`](https://hoopR.sportsdataverse.org/reference/nba_referee_assignments.html) |
| [`nba_schedule_crosswalk`](reference/additional/ids-and-crosswalks#nba_schedule_crosswalk) | [`nba_schedule_crosswalk`](https://hoopR.sportsdataverse.org/reference/nba_schedule_crosswalk.html) |
| [`nba_stats_alltimeleadersgrids`](reference/nba_stats/other#nba_stats_alltimeleadersgrids) | [`nba_alltimeleadersgrids`](https://hoopR.sportsdataverse.org/reference/nba_alltimeleadersgrids.html) |
| [`nba_stats_assistleaders`](reference/nba_stats/other#nba_stats_assistleaders) | [`nba_assistleaders`](https://hoopR.sportsdataverse.org/reference/nba_assistleaders.html) |
| [`nba_stats_assisttracker`](reference/nba_stats/other#nba_stats_assisttracker) | [`nba_assisttracker`](https://hoopR.sportsdataverse.org/reference/nba_assisttracker.html) |
| [`nba_stats_boxscoreadvancedv3`](reference/nba_stats/boxscore#nba_stats_boxscoreadvancedv3) | [`nba_boxscoreadvancedv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscoreadvancedv3.html) |
| [`nba_stats_boxscoredefensivev2`](reference/nba_stats/boxscore#nba_stats_boxscoredefensivev2) | [`nba_boxscoredefensivev2`](https://hoopR.sportsdataverse.org/reference/nba_boxscoredefensivev2.html) |
| [`nba_stats_boxscorefourfactorsv3`](reference/nba_stats/boxscore#nba_stats_boxscorefourfactorsv3) | [`nba_boxscorefourfactorsv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscorefourfactorsv3.html) |
| [`nba_stats_boxscorehustlev2`](reference/nba_stats/boxscore#nba_stats_boxscorehustlev2) | [`nba_boxscorehustlev2`](https://hoopR.sportsdataverse.org/reference/nba_boxscorehustlev2.html) |
| [`nba_stats_boxscorematchupsv3`](reference/nba_stats/boxscore#nba_stats_boxscorematchupsv3) | [`nba_boxscorematchupsv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscorematchupsv3.html) |
| [`nba_stats_boxscoremiscv3`](reference/nba_stats/boxscore#nba_stats_boxscoremiscv3) | [`nba_boxscoremiscv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscoremiscv3.html) |
| [`nba_stats_boxscoreplayertrackv3`](reference/nba_stats/boxscore#nba_stats_boxscoreplayertrackv3) | [`nba_boxscoreplayertrackv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscoreplayertrackv3.html) |
| [`nba_stats_boxscorescoringv3`](reference/nba_stats/boxscore#nba_stats_boxscorescoringv3) | [`nba_boxscorescoringv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscorescoringv3.html) |
| [`nba_stats_boxscoresummaryv2`](reference/nba_stats/boxscore#nba_stats_boxscoresummaryv2) | [`nba_boxscoresummaryv2`](https://hoopR.sportsdataverse.org/reference/nba_boxscoresummaryv2.html) |
| [`nba_stats_boxscoresummaryv3`](reference/nba_stats/boxscore#nba_stats_boxscoresummaryv3) | [`nba_boxscoresummaryv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscoresummaryv3.html) |
| [`nba_stats_boxscoretraditionalv2`](reference/nba_stats/boxscore-2#nba_stats_boxscoretraditionalv2) | [`nba_boxscoretraditionalv2`](https://hoopR.sportsdataverse.org/reference/nba_boxscoretraditionalv2.html) |
| [`nba_stats_boxscoretraditionalv3`](reference/nba_stats/boxscore-2#nba_stats_boxscoretraditionalv3) | [`nba_boxscoretraditionalv3`](https://hoopR.sportsdataverse.org/reference/nba_boxscoretraditionalv3.html) |
| [`nba_stats_boxscoreusagev3`](reference/nba_stats/boxscore-2#nba_stats_boxscoreusagev3) | [`nba_boxscoreusagev3`](https://hoopR.sportsdataverse.org/reference/nba_boxscoreusagev3.html) |
| [`nba_stats_commonallplayers`](reference/nba_stats/common#nba_stats_commonallplayers) | [`nba_commonallplayers`](https://hoopR.sportsdataverse.org/reference/nba_commonallplayers.html) |
| [`nba_stats_commonplayerinfo`](reference/nba_stats/common#nba_stats_commonplayerinfo) | [`nba_commonplayerinfo`](https://hoopR.sportsdataverse.org/reference/nba_commonplayerinfo.html) |
| [`nba_stats_commonplayoffseries`](reference/nba_stats/common#nba_stats_commonplayoffseries) | [`nba_commonplayoffseries`](https://hoopR.sportsdataverse.org/reference/nba_commonplayoffseries.html) |
| [`nba_stats_commonteamroster`](reference/nba_stats/common#nba_stats_commonteamroster) | [`nba_commonteamroster`](https://hoopR.sportsdataverse.org/reference/nba_commonteamroster.html) |
| [`nba_stats_commonteamyears`](reference/nba_stats/common#nba_stats_commonteamyears) | [`nba_commonteamyears`](https://hoopR.sportsdataverse.org/reference/nba_commonteamyears.html) |
| [`nba_stats_cumestatsplayer`](reference/nba_stats/cumestats#nba_stats_cumestatsplayer) | [`nba_cumestatsplayer`](https://hoopR.sportsdataverse.org/reference/nba_cumestatsplayer.html) |
| [`nba_stats_cumestatsplayergames`](reference/nba_stats/cumestats#nba_stats_cumestatsplayergames) | [`nba_cumestatsplayergames`](https://hoopR.sportsdataverse.org/reference/nba_cumestatsplayergames.html) |
| [`nba_stats_cumestatsteam`](reference/nba_stats/cumestats#nba_stats_cumestatsteam) | [`nba_cumestatsteam`](https://hoopR.sportsdataverse.org/reference/nba_cumestatsteam.html) |
| [`nba_stats_cumestatsteamgames`](reference/nba_stats/cumestats#nba_stats_cumestatsteamgames) | [`nba_cumestatsteamgames`](https://hoopR.sportsdataverse.org/reference/nba_cumestatsteamgames.html) |
| [`nba_stats_draftcombinedrillresults`](reference/nba_stats/draftcombine#nba_stats_draftcombinedrillresults) | [`nba_draftcombinedrillresults`](https://hoopR.sportsdataverse.org/reference/nba_draftcombinedrillresults.html) |
| [`nba_stats_draftcombinenonstationaryshooting`](reference/nba_stats/draftcombine#nba_stats_draftcombinenonstationaryshooting) | [`nba_draftcombinenonstationaryshooting`](https://hoopR.sportsdataverse.org/reference/nba_draftcombinenonstationaryshooting.html) |
| [`nba_stats_draftcombineplayeranthro`](reference/nba_stats/draftcombine#nba_stats_draftcombineplayeranthro) | [`nba_draftcombineplayeranthro`](https://hoopR.sportsdataverse.org/reference/nba_draftcombineplayeranthro.html) |
| [`nba_stats_draftcombinespotshooting`](reference/nba_stats/draftcombine#nba_stats_draftcombinespotshooting) | [`nba_draftcombinespotshooting`](https://hoopR.sportsdataverse.org/reference/nba_draftcombinespotshooting.html) |
| [`nba_stats_draftcombinestats`](reference/nba_stats/draftcombine#nba_stats_draftcombinestats) | [`nba_draftcombinestats`](https://hoopR.sportsdataverse.org/reference/nba_draftcombinestats.html) |
| [`nba_stats_drafthistory`](reference/nba_stats/other#nba_stats_drafthistory) | [`nba_drafthistory`](https://hoopR.sportsdataverse.org/reference/nba_drafthistory.html) |
| [`nba_stats_fantasywidget`](reference/nba_stats/other#nba_stats_fantasywidget) | [`nba_fantasywidget`](https://hoopR.sportsdataverse.org/reference/nba_fantasywidget.html) |
| [`nba_stats_franchisehistory`](reference/nba_stats/franchise#nba_stats_franchisehistory) | [`nba_franchisehistory`](https://hoopR.sportsdataverse.org/reference/nba_franchisehistory.html) |
| [`nba_stats_franchiseleaders`](reference/nba_stats/franchise#nba_stats_franchiseleaders) | [`nba_franchiseleaders`](https://hoopR.sportsdataverse.org/reference/nba_franchiseleaders.html) |
| [`nba_stats_franchiseleaderswrank`](reference/nba_stats/franchise#nba_stats_franchiseleaderswrank) | [`nba_franchiseleaderswrank`](https://hoopR.sportsdataverse.org/reference/nba_franchiseleaderswrank.html) |
| [`nba_stats_franchiseplayers`](reference/nba_stats/franchise#nba_stats_franchiseplayers) | [`nba_franchiseplayers`](https://hoopR.sportsdataverse.org/reference/nba_franchiseplayers.html) |
| [`nba_stats_gamerotation`](reference/nba_stats/other#nba_stats_gamerotation) | [`nba_gamerotation`](https://hoopR.sportsdataverse.org/reference/nba_gamerotation.html) |
| [`nba_stats_homepageleaders`](reference/nba_stats/other#nba_stats_homepageleaders) | [`nba_homepageleaders`](https://hoopR.sportsdataverse.org/reference/nba_homepageleaders.html) |
| [`nba_stats_homepagev2`](reference/nba_stats/other#nba_stats_homepagev2) | [`nba_homepagev2`](https://hoopR.sportsdataverse.org/reference/nba_homepagev2.html) |
| [`nba_stats_hustlestatsboxscore`](reference/nba_stats/other#nba_stats_hustlestatsboxscore) | [`nba_hustlestatsboxscore`](https://hoopR.sportsdataverse.org/reference/nba_hustlestatsboxscore.html) |
| [`nba_stats_infographicfanduelplayer`](reference/nba_stats/other#nba_stats_infographicfanduelplayer) | [`nba_infographicfanduelplayer`](https://hoopR.sportsdataverse.org/reference/nba_infographicfanduelplayer.html) |
| [`nba_stats_leaderstiles`](reference/nba_stats/other#nba_stats_leaderstiles) | [`nba_leaderstiles`](https://hoopR.sportsdataverse.org/reference/nba_leaderstiles.html) |
| [`nba_stats_leaguedashlineups`](reference/nba_stats/leaguedash#nba_stats_leaguedashlineups) | [`nba_leaguedashlineups`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashlineups.html) |
| [`nba_stats_leaguedashoppptshot`](reference/nba_stats/leaguedash#nba_stats_leaguedashoppptshot) | [`nba_leaguedashoppptshot`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashoppptshot.html) |
| [`nba_stats_leaguedashplayerbiostats`](reference/nba_stats/leaguedash#nba_stats_leaguedashplayerbiostats) | [`nba_leaguedashplayerbiostats`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashplayerbiostats.html) |
| [`nba_stats_leaguedashplayerclutch`](reference/nba_stats/leaguedash#nba_stats_leaguedashplayerclutch) | [`nba_leaguedashplayerclutch`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashplayerclutch.html) |
| [`nba_stats_leaguedashplayerptshot`](reference/nba_stats/leaguedash#nba_stats_leaguedashplayerptshot) | [`nba_leaguedashplayerptshot`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashplayerptshot.html) |
| [`nba_stats_leaguedashplayershotlocations`](reference/nba_stats/leaguedash#nba_stats_leaguedashplayershotlocations) | [`nba_leaguedashplayershotlocations`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashplayershotlocations.html) |
| [`nba_stats_leaguedashplayerstats`](reference/nba_stats/leaguedash#nba_stats_leaguedashplayerstats) | [`nba_leaguedashplayerstats`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashplayerstats.html) |
| [`nba_stats_leaguedashptdefend`](reference/nba_stats/leaguedash#nba_stats_leaguedashptdefend) | [`nba_leaguedashptdefend`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashptdefend.html) |
| [`nba_stats_leaguedashptstats`](reference/nba_stats/leaguedash-2#nba_stats_leaguedashptstats) | [`nba_leaguedashptstats`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashptstats.html) |
| [`nba_stats_leaguedashptteamdefend`](reference/nba_stats/leaguedash-2#nba_stats_leaguedashptteamdefend) | [`nba_leaguedashptteamdefend`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashptteamdefend.html) |
| [`nba_stats_leaguedashteamclutch`](reference/nba_stats/leaguedash-2#nba_stats_leaguedashteamclutch) | [`nba_leaguedashteamclutch`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashteamclutch.html) |
| [`nba_stats_leaguedashteamptshot`](reference/nba_stats/leaguedash-2#nba_stats_leaguedashteamptshot) | [`nba_leaguedashteamptshot`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashteamptshot.html) |
| [`nba_stats_leaguedashteamshotlocations`](reference/nba_stats/leaguedash-2#nba_stats_leaguedashteamshotlocations) | [`nba_leaguedashteamshotlocations`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashteamshotlocations.html) |
| [`nba_stats_leaguedashteamstats`](reference/nba_stats/leaguedash-2#nba_stats_leaguedashteamstats) | [`nba_leaguedashteamstats`](https://hoopR.sportsdataverse.org/reference/nba_leaguedashteamstats.html) |
| [`nba_stats_leaguegamefinder`](reference/nba_stats/league#nba_stats_leaguegamefinder) | [`nba_leaguegamefinder`](https://hoopR.sportsdataverse.org/reference/nba_leaguegamefinder.html) |
| [`nba_stats_leaguegamelog`](reference/nba_stats/league#nba_stats_leaguegamelog) | [`nba_leaguegamelog`](https://hoopR.sportsdataverse.org/reference/nba_leaguegamelog.html) |
| [`nba_stats_leaguehustlestatsplayer`](reference/nba_stats/league#nba_stats_leaguehustlestatsplayer) | [`nba_leaguehustlestatsplayer`](https://hoopR.sportsdataverse.org/reference/nba_leaguehustlestatsplayer.html) |
| [`nba_stats_leaguehustlestatsteam`](reference/nba_stats/league#nba_stats_leaguehustlestatsteam) | [`nba_leaguehustlestatsteam`](https://hoopR.sportsdataverse.org/reference/nba_leaguehustlestatsteam.html) |
| [`nba_stats_leagueleaders`](reference/nba_stats/league#nba_stats_leagueleaders) | [`nba_leagueleaders`](https://hoopR.sportsdataverse.org/reference/nba_leagueleaders.html) |
| [`nba_stats_leaguelineupviz`](reference/nba_stats/league#nba_stats_leaguelineupviz) | [`nba_leaguelineupviz`](https://hoopR.sportsdataverse.org/reference/nba_leaguelineupviz.html) |
| [`nba_stats_leagueplayerondetails`](reference/nba_stats/league#nba_stats_leagueplayerondetails) | [`nba_leagueplayerondetails`](https://hoopR.sportsdataverse.org/reference/nba_leagueplayerondetails.html) |
| [`nba_stats_leagueseasonmatchups`](reference/nba_stats/league#nba_stats_leagueseasonmatchups) | [`nba_leagueseasonmatchups`](https://hoopR.sportsdataverse.org/reference/nba_leagueseasonmatchups.html) |
| [`nba_stats_leaguestandings`](reference/nba_stats/league-2#nba_stats_leaguestandings) | [`nba_leaguestandings`](https://hoopR.sportsdataverse.org/reference/nba_leaguestandings.html) |
| [`nba_stats_leaguestandingsv3`](reference/nba_stats/league-2#nba_stats_leaguestandingsv3) | [`nba_leaguestandingsv3`](https://hoopR.sportsdataverse.org/reference/nba_leaguestandingsv3.html) |
| [`nba_stats_matchupsrollup`](reference/nba_stats/other#nba_stats_matchupsrollup) | [`nba_matchupsrollup`](https://hoopR.sportsdataverse.org/reference/nba_matchupsrollup.html) |
| [`nba_stats_playbyplayv3`](reference/nba_stats/other#nba_stats_playbyplayv3) | [`nba_playbyplayv3`](https://hoopR.sportsdataverse.org/reference/nba_playbyplayv3.html) |
| [`nba_stats_playerawards`](reference/nba_stats/player#nba_stats_playerawards) | [`nba_playerawards`](https://hoopR.sportsdataverse.org/reference/nba_playerawards.html) |
| [`nba_stats_playercareerbycollegerollup`](reference/nba_stats/player#nba_stats_playercareerbycollegerollup) | [`nba_playercareerbycollegerollup`](https://hoopR.sportsdataverse.org/reference/nba_playercareerbycollegerollup.html) |
| [`nba_stats_playercareerstats`](reference/nba_stats/player#nba_stats_playercareerstats) | [`nba_playercareerstats`](https://hoopR.sportsdataverse.org/reference/nba_playercareerstats.html) |
| [`nba_stats_playercompare`](reference/nba_stats/player#nba_stats_playercompare) | [`nba_playercompare`](https://hoopR.sportsdataverse.org/reference/nba_playercompare.html) |
| [`nba_stats_playerdashboardbyclutch`](reference/nba_stats/playerdash#nba_stats_playerdashboardbyclutch) | [`nba_playerdashboardbyclutch`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbyclutch.html) |
| [`nba_stats_playerdashboardbygamesplits`](reference/nba_stats/playerdash-2#nba_stats_playerdashboardbygamesplits) | [`nba_playerdashboardbygamesplits`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbygamesplits.html) |
| [`nba_stats_playerdashboardbygeneralsplits`](reference/nba_stats/playerdash-3#nba_stats_playerdashboardbygeneralsplits) | [`nba_playerdashboardbygeneralsplits`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbygeneralsplits.html) |
| [`nba_stats_playerdashboardbylastngames`](reference/nba_stats/playerdash-4#nba_stats_playerdashboardbylastngames) | [`nba_playerdashboardbylastngames`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbylastngames.html) |
| [`nba_stats_playerdashboardbyopponent`](reference/nba_stats/playerdash-4#nba_stats_playerdashboardbyopponent) | [`nba_playerdashboardbyopponent`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbyopponent.html) |
| [`nba_stats_playerdashboardbyshootingsplits`](reference/nba_stats/playerdash-5#nba_stats_playerdashboardbyshootingsplits) | [`nba_playerdashboardbyshootingsplits`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbyshootingsplits.html) |
| [`nba_stats_playerdashboardbyteamperformance`](reference/nba_stats/playerdash-5#nba_stats_playerdashboardbyteamperformance) | [`nba_playerdashboardbyteamperformance`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbyteamperformance.html) |
| [`nba_stats_playerdashboardbyyearoveryear`](reference/nba_stats/playerdash-6#nba_stats_playerdashboardbyyearoveryear) | [`nba_playerdashboardbyyearoveryear`](https://hoopR.sportsdataverse.org/reference/nba_playerdashboardbyyearoveryear.html) |
| [`nba_stats_playerdashptpass`](reference/nba_stats/playerdash-6#nba_stats_playerdashptpass) | [`nba_playerdashptpass`](https://hoopR.sportsdataverse.org/reference/nba_playerdashptpass.html) |
| [`nba_stats_playerdashptreb`](reference/nba_stats/playerdash-6#nba_stats_playerdashptreb) | [`nba_playerdashptreb`](https://hoopR.sportsdataverse.org/reference/nba_playerdashptreb.html) |
| [`nba_stats_playerdashptshotdefend`](reference/nba_stats/playerdash-6#nba_stats_playerdashptshotdefend) | [`nba_playerdashptshotdefend`](https://hoopR.sportsdataverse.org/reference/nba_playerdashptshotdefend.html) |
| [`nba_stats_playerdashptshots`](reference/nba_stats/playerdash-6#nba_stats_playerdashptshots) | [`nba_playerdashptshots`](https://hoopR.sportsdataverse.org/reference/nba_playerdashptshots.html) |
| [`nba_stats_playerestimatedmetrics`](reference/nba_stats/player#nba_stats_playerestimatedmetrics) | [`nba_playerestimatedmetrics`](https://hoopR.sportsdataverse.org/reference/nba_playerestimatedmetrics.html) |
| [`nba_stats_playerfantasyprofile`](reference/nba_stats/player#nba_stats_playerfantasyprofile) | [`nba_playerfantasyprofile`](https://hoopR.sportsdataverse.org/reference/nba_playerfantasyprofile.html) |
| [`nba_stats_playerfantasyprofilebargraph`](reference/nba_stats/player#nba_stats_playerfantasyprofilebargraph) | [`nba_playerfantasyprofilebargraph`](https://hoopR.sportsdataverse.org/reference/nba_playerfantasyprofilebargraph.html) |
| [`nba_stats_playergamelog`](reference/nba_stats/player#nba_stats_playergamelog) | [`nba_playergamelog`](https://hoopR.sportsdataverse.org/reference/nba_playergamelog.html) |
| [`nba_stats_playergamelogs`](reference/nba_stats/player-2#nba_stats_playergamelogs) | [`nba_playergamelogs`](https://hoopR.sportsdataverse.org/reference/nba_playergamelogs.html) |
| [`nba_stats_playergamestreakfinder`](reference/nba_stats/player-2#nba_stats_playergamestreakfinder) | [`nba_playergamestreakfinder`](https://hoopR.sportsdataverse.org/reference/nba_playergamestreakfinder.html) |
| [`nba_stats_playerindex`](reference/nba_stats/player-2#nba_stats_playerindex) | [`nba_playerindex`](https://hoopR.sportsdataverse.org/reference/nba_playerindex.html) |
| [`nba_stats_playerprofilev2`](reference/nba_stats/player-2#nba_stats_playerprofilev2) | [`nba_playerprofilev2`](https://hoopR.sportsdataverse.org/reference/nba_playerprofilev2.html) |
| [`nba_stats_playervsplayer`](reference/nba_stats/player-2#nba_stats_playervsplayer) | [`nba_playervsplayer`](https://hoopR.sportsdataverse.org/reference/nba_playervsplayer.html) |
| [`nba_stats_playoffpicture`](reference/nba_stats/other-2#nba_stats_playoffpicture) | [`nba_playoffpicture`](https://hoopR.sportsdataverse.org/reference/nba_playoffpicture.html) |
| [`nba_stats_scheduleleaguev2`](reference/nba_stats/schedule#nba_stats_scheduleleaguev2) | [`nba_schedule`](https://hoopR.sportsdataverse.org/reference/nba_schedule.html) |
| [`nba_stats_scheduleleaguev2int`](reference/nba_stats/schedule#nba_stats_scheduleleaguev2int) | [`nba_scheduleleaguev2int`](https://hoopR.sportsdataverse.org/reference/nba_scheduleleaguev2int.html) |
| [`nba_stats_scoreboardv2`](reference/nba_stats/scoreboard#nba_stats_scoreboardv2) | [`nba_scoreboardv2`](https://hoopR.sportsdataverse.org/reference/nba_scoreboardv2.html) |
| [`nba_stats_scoreboardv3`](reference/nba_stats/scoreboard#nba_stats_scoreboardv3) | [`nba_scoreboardv3`](https://hoopR.sportsdataverse.org/reference/nba_scoreboardv3.html) |
| [`nba_stats_shotchartdetail`](reference/nba_stats/shotchart#nba_stats_shotchartdetail) | [`nba_shotchartdetail`](https://hoopR.sportsdataverse.org/reference/nba_shotchartdetail.html) |
| [`nba_stats_shotchartleaguewide`](reference/nba_stats/shotchart#nba_stats_shotchartleaguewide) | [`nba_shotchartleaguewide`](https://hoopR.sportsdataverse.org/reference/nba_shotchartleaguewide.html) |
| [`nba_stats_shotchartlineupdetail`](reference/nba_stats/shotchart#nba_stats_shotchartlineupdetail) | [`nba_shotchartlineupdetail`](https://hoopR.sportsdataverse.org/reference/nba_shotchartlineupdetail.html) |
| [`nba_stats_synergyplaytypes`](reference/nba_stats/other-2#nba_stats_synergyplaytypes) | [`nba_synergyplaytypes`](https://hoopR.sportsdataverse.org/reference/nba_synergyplaytypes.html) |
| [`nba_stats_teamdashboardbyclutch`](reference/nba_stats/teamdash#nba_stats_teamdashboardbyclutch) | [`nba_teamdashboardbyclutch`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbyclutch.html) |
| [`nba_stats_teamdashboardbygamesplits`](reference/nba_stats/teamdash-2#nba_stats_teamdashboardbygamesplits) | [`nba_teamdashboardbygamesplits`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbygamesplits.html) |
| [`nba_stats_teamdashboardbygeneralsplits`](reference/nba_stats/teamdash-2#nba_stats_teamdashboardbygeneralsplits) | [`nba_teamdashboardbygeneralsplits`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbygeneralsplits.html) |
| [`nba_stats_teamdashboardbylastngames`](reference/nba_stats/teamdash-3#nba_stats_teamdashboardbylastngames) | [`nba_teamdashboardbylastngames`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbylastngames.html) |
| [`nba_stats_teamdashboardbyopponent`](reference/nba_stats/teamdash-3#nba_stats_teamdashboardbyopponent) | [`nba_teamdashboardbyopponent`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbyopponent.html) |
| [`nba_stats_teamdashboardbyshootingsplits`](reference/nba_stats/teamdash-4#nba_stats_teamdashboardbyshootingsplits) | [`nba_teamdashboardbyshootingsplits`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbyshootingsplits.html) |
| [`nba_stats_teamdashboardbyteamperformance`](reference/nba_stats/teamdash-4#nba_stats_teamdashboardbyteamperformance) | [`nba_teamdashboardbyteamperformance`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbyteamperformance.html) |
| [`nba_stats_teamdashboardbyyearoveryear`](reference/nba_stats/teamdash-4#nba_stats_teamdashboardbyyearoveryear) | [`nba_teamdashboardbyyearoveryear`](https://hoopR.sportsdataverse.org/reference/nba_teamdashboardbyyearoveryear.html) |
| [`nba_stats_teamdashlineups`](reference/nba_stats/teamdash-5#nba_stats_teamdashlineups) | [`nba_teamdashlineups`](https://hoopR.sportsdataverse.org/reference/nba_teamdashlineups.html) |
| [`nba_stats_teamdashptpass`](reference/nba_stats/teamdash-5#nba_stats_teamdashptpass) | [`nba_teamdashptpass`](https://hoopR.sportsdataverse.org/reference/nba_teamdashptpass.html) |
| [`nba_stats_teamdashptreb`](reference/nba_stats/teamdash-5#nba_stats_teamdashptreb) | [`nba_teamdashptreb`](https://hoopR.sportsdataverse.org/reference/nba_teamdashptreb.html) |
| [`nba_stats_teamdashptshots`](reference/nba_stats/teamdash-5#nba_stats_teamdashptshots) | [`nba_teamdashptshots`](https://hoopR.sportsdataverse.org/reference/nba_teamdashptshots.html) |
| [`nba_stats_teamdetails`](reference/nba_stats/team#nba_stats_teamdetails) | [`nba_teamdetails`](https://hoopR.sportsdataverse.org/reference/nba_teamdetails.html) |
| [`nba_stats_teamestimatedmetrics`](reference/nba_stats/team#nba_stats_teamestimatedmetrics) | [`nba_teamestimatedmetrics`](https://hoopR.sportsdataverse.org/reference/nba_teamestimatedmetrics.html) |
| [`nba_stats_teamgamelog`](reference/nba_stats/team#nba_stats_teamgamelog) | [`nba_teamgamelog`](https://hoopR.sportsdataverse.org/reference/nba_teamgamelog.html) |
| [`nba_stats_teamgamelogs`](reference/nba_stats/team#nba_stats_teamgamelogs) | [`nba_teamgamelogs`](https://hoopR.sportsdataverse.org/reference/nba_teamgamelogs.html) |
| [`nba_stats_teaminfocommon`](reference/nba_stats/team#nba_stats_teaminfocommon) | [`nba_teaminfocommon`](https://hoopR.sportsdataverse.org/reference/nba_teaminfocommon.html) |
| [`nba_stats_teamplayerdashboard`](reference/nba_stats/teamplayer#nba_stats_teamplayerdashboard) | [`nba_teamplayerdashboard`](https://hoopR.sportsdataverse.org/reference/nba_teamplayerdashboard.html) |
| [`nba_stats_teamplayeronoffdetails`](reference/nba_stats/teamplayer#nba_stats_teamplayeronoffdetails) | [`nba_teamplayeronoffdetails`](https://hoopR.sportsdataverse.org/reference/nba_teamplayeronoffdetails.html) |
| [`nba_stats_teamplayeronoffsummary`](reference/nba_stats/teamplayer#nba_stats_teamplayeronoffsummary) | [`nba_teamplayeronoffsummary`](https://hoopR.sportsdataverse.org/reference/nba_teamplayeronoffsummary.html) |
| [`nba_stats_teamvsplayer`](reference/nba_stats/team#nba_stats_teamvsplayer) | [`nba_teamvsplayer`](https://hoopR.sportsdataverse.org/reference/nba_teamvsplayer.html) |
| [`nba_stats_teamyearbyyearstats`](reference/nba_stats/team#nba_stats_teamyearbyyearstats) | [`nba_teamyearbyyearstats`](https://hoopR.sportsdataverse.org/reference/nba_teamyearbyyearstats.html) |
| [`nba_stats_videodetailsasset`](reference/nba_stats/video#nba_stats_videodetailsasset) | [`nba_videodetailsasset`](https://hoopR.sportsdataverse.org/reference/nba_videodetailsasset.html) |
| [`nba_stats_videoevents`](reference/nba_stats/video#nba_stats_videoevents) | [`nba_videoevents`](https://hoopR.sportsdataverse.org/reference/nba_videoevents.html) |
| [`nba_stats_videoeventsasset`](reference/nba_stats/video#nba_stats_videoeventsasset) | [`nba_videoeventsasset`](https://hoopR.sportsdataverse.org/reference/nba_videoeventsasset.html) |
| [`nba_stats_videostatus`](reference/nba_stats/video#nba_stats_videostatus) | [`nba_videostatus`](https://hoopR.sportsdataverse.org/reference/nba_videostatus.html) |
| [`nba_team_crosswalk`](reference/additional/ids-and-crosswalks#nba_team_crosswalk) | [`nba_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/nba_team_crosswalk.html) |
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
| [`year_to_season`](reference/additional/dates-and-seasons#year_to_season) | [`year_to_season`](https://hoopR.sportsdataverse.org/reference/year_to_season.html) |
