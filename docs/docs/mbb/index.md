---
title: MBB
sidebar_label: MBB
description: "sdv-py MBB: endpoint references, dataset loaders and parsers for MBB in the SportsDataverse Python package."
---
# MBB (`sportsdataverse.mbb`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [Highlights](reference/additional#highlights) | curated "start here" functions | 9 | — |
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 128 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 34 | none |
| [stats.ncaa.org](#stats-ncaa-org) | `stats.ncaa.org` | 118 | none (Terms gate + rate rotation) |
| [KenPom](#kenpom) | `kenpom.com` | 32 | subscription (KENPOM_EMAIL / KENPOM_PASSWORD) |
| [Bart Torvik T-Rank](#bart-torvik-t-rank) | `barttorvik.com` | 5 | none |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 27 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 320 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 87 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |
| [Hand-written wrappers](reference/additional/highlights) | 6 |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 34 |

## stats.ncaa.org {#stats-ncaa-org}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/stats-ncaa-org) | 118 |

## KenPom {#kenpom}

| Reference | Functions |
|---|---:|
| [KenPom (kenpom.com, subscription)](reference/kenpom) | 30 |
| [Hand-written wrappers](reference/additional/kenpom) | 2 |

## Bart Torvik T-Rank {#bart-torvik-t-rank}

| Reference | Functions |
|---|---:|
| [Bart Torvik T-Rank (barttorvik.com)](reference/torvik) | 5 |

## Fox Sports API {#fox-sports-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/fox-sports-api) | 27 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`build_athlete_identity_lookup`](reference/additional/play-by-play-processing#build_athlete_identity_lookup)
- [`build_mbb_player_identity_lookup`](reference/additional/play-by-play-processing#build_mbb_player_identity_lookup)
- [`classify_point_value`](reference/additional/play-by-play-processing#classify_point_value)
- [`classify_zone_geometry`](reference/additional/play-by-play-processing#classify_zone_geometry)
- [`classify_zone_type`](reference/additional/play-by-play-processing#classify_zone_type)
- [`espn_shots_to_canonical`](reference/additional/play-by-play-processing#espn_shots_to_canonical)
- [`fit_espn_court_scale`](reference/additional/play-by-play-processing#fit_espn_court_scale)
- [`mbb_pbp_disk`](reference/additional/play-by-play-processing#mbb_pbp_disk)
- [`mbb_shot_data`](reference/additional/play-by-play-processing#mbb_shot_data)
- [`ncaa_mbb_game_pbp`](reference/additional/play-by-play-processing#ncaa_mbb_game_pbp)
- [`ncaa_mbb_play_by_play`](reference/additional/highlights#ncaa_mbb_play_by_play)
- [`shot_events_to_frame`](reference/additional/play-by-play-processing#shot_events_to_frame)

### Models and calculators {#models-and-calculators}

- [`AssistEvent`](reference/additional/models-and-calculators#AssistEvent)
- [`AssistInfo`](reference/additional/models-and-calculators#AssistInfo)
- [`ConferenceId`](reference/additional/models-and-calculators#ConferenceId)
- [`CutdownShotEvent`](reference/additional/models-and-calculators#CutdownShotEvent)
- [`Direction`](reference/additional/models-and-calculators#Direction)
- [`FieldGoalStats`](reference/additional/models-and-calculators#FieldGoalStats)
- [`LeagueConstants`](reference/additional/models-and-calculators#LeagueConstants)
- [`LineupEvent`](reference/additional/models-and-calculators#LineupEvent)
- [`LineupEventStats`](reference/additional/models-and-calculators#LineupEventStats)
- [`LineupId`](reference/additional/models-and-calculators#LineupId)
- [`LocationType`](reference/additional/models-and-calculators#LocationType)
- [`PlayerCodeId`](reference/additional/models-and-calculators#PlayerCodeId)
- [`PlayerEvent`](reference/additional/models-and-calculators#PlayerEvent)
- [`PlayerShotInfo`](reference/additional/models-and-calculators#PlayerShotInfo)
- [`PlayerValueConstants`](reference/additional/models-and-calculators#PlayerValueConstants)
- [`PossCalcFragment`](reference/additional/models-and-calculators#PossCalcFragment)
- [`PossessionEvent`](reference/additional/models-and-calculators#PossessionEvent)
- [`RapmConfig`](reference/additional/models-and-calculators#RapmConfig)
- [`RapmPlayerContext`](reference/additional/models-and-calculators#RapmPlayerContext)
- [`RapmPreProcDiagnostics`](reference/additional/models-and-calculators#RapmPreProcDiagnostics)
- [`RapmPriorInfo`](reference/additional/models-and-calculators#RapmPriorInfo)
- [`RapmProcessingInputs`](reference/additional/models-and-calculators#RapmProcessingInputs)
- [`RawGameEvent`](reference/additional/models-and-calculators#RawGameEvent)
- [`RosterEntry`](reference/additional/models-and-calculators#RosterEntry)
- [`ScoreInfo`](reference/additional/models-and-calculators#ScoreInfo)
- [`ShotClockStats`](reference/additional/models-and-calculators#ShotClockStats)
- [`ShotEvent`](reference/additional/models-and-calculators#ShotEvent)
- [`ShotGeo`](reference/additional/models-and-calculators#ShotGeo)
- [`ShotLocation`](reference/additional/models-and-calculators#ShotLocation)
- [`TeamId`](reference/additional/models-and-calculators#TeamId)
- [`TeamSeasonId`](reference/additional/models-and-calculators#TeamSeasonId)
- [`adjust_efficiency`](reference/additional/models-and-calculators#adjust_efficiency)
- [`adjust_off_rating_stats`](reference/additional/models-and-calculators#adjust_off_rating_stats)
- [`adjust_tempo`](reference/additional/models-and-calculators#adjust_tempo)
- [`aggregate_player_seasons`](reference/additional/models-and-calculators#aggregate_player_seasons)
- [`apply_weak_priors`](reference/additional/models-and-calculators#apply_weak_priors)
- [`as_of_ratings_split`](reference/additional/models-and-calculators#as_of_ratings_split)
- [`as_of_season_split`](reference/additional/models-and-calculators#as_of_season_split)
- [`bootstrap_ari`](reference/additional/models-and-calculators#bootstrap_ari)
- [`brier_score`](reference/additional/models-and-calculators#brier_score)
- [`build_d_rtg`](reference/additional/models-and-calculators#build_d_rtg)
- [`build_mbb_season_wp`](reference/additional/models-and-calculators#build_mbb_season_wp)
- [`build_net_points`](reference/additional/models-and-calculators#build_net_points)
- [`build_o_rtg`](reference/additional/models-and-calculators#build_o_rtg)
- [`build_player_context`](reference/additional/models-and-calculators#build_player_context)
- [`build_priors`](reference/additional/models-and-calculators#build_priors)
- [`build_productivity`](reference/additional/models-and-calculators#build_productivity)
- [`build_weak_prior_from_rapm`](reference/additional/models-and-calculators#build_weak_prior_from_rapm)
- [`calc_collinearity_diag`](reference/additional/models-and-calculators#calc_collinearity_diag)
- [`calc_lineup_outputs`](reference/additional/models-and-calculators#calc_lineup_outputs)
- [`calc_player_weights`](reference/additional/models-and-calculators#calc_player_weights)
- [`calc_slow_pseudo_inverse`](reference/additional/models-and-calculators#calc_slow_pseudo_inverse)
- [`calculate_predicted_out`](reference/additional/models-and-calculators-2#calculate_predicted_out)
- [`calculate_rapm`](reference/additional/models-and-calculators-2#calculate_rapm)
- [`calculate_residual_error`](reference/additional/models-and-calculators-2#calculate_residual_error)
- [`calculate_sd_rapm`](reference/additional/models-and-calculators-2#calculate_sd_rapm)
- [`calibration_table`](reference/additional/models-and-calculators-2#calibration_table)
- [`fit_shrinkage_k`](reference/additional/models-and-calculators-2#fit_shrinkage_k)
- [`get_constants`](reference/additional/models-and-calculators-2#get_constants)
- [`get_player_value_constants`](reference/additional/models-and-calculators-2#get_player_value_constants)
- [`in_game_features`](reference/additional/models-and-calculators-2#in_game_features)
- [`inject_rapm_into_players`](reference/additional/models-and-calculators-2#inject_rapm_into_players)
- [`kmeans_fit`](reference/additional/models-and-calculators-2#kmeans_fit)
- [`load_artifact`](reference/additional/models-and-calculators-2#load_artifact)
- [`log_loss_score`](reference/additional/models-and-calculators-2#log_loss_score)
- [`logistic_fit`](reference/additional/models-and-calculators-2#logistic_fit)
- [`mae`](reference/additional/models-and-calculators-2#mae)
- [`mbb_box_bpm`](reference/additional/models-and-calculators-2#mbb_box_bpm)
- [`mbb_bracket_sim`](reference/additional/models-and-calculators-2#mbb_bracket_sim)
- [`mbb_draft_projection`](reference/additional/models-and-calculators-2#mbb_draft_projection)
- [`mbb_in_game_win_prob`](reference/additional/models-and-calculators-2#mbb_in_game_win_prob)
- [`mbb_predict_games`](reference/additional/models-and-calculators-2#mbb_predict_games)
- [`mbb_recruiting_projection`](reference/additional/models-and-calculators-2#mbb_recruiting_projection)
- [`mbb_season_sim`](reference/additional/models-and-calculators-2#mbb_season_sim)
- [`mbb_shooter_talent`](reference/additional/models-and-calculators-2#mbb_shooter_talent)
- [`mbb_shot_quality`](reference/additional/models-and-calculators-2#mbb_shot_quality)
- [`mbb_shot_quality_model`](reference/additional/models-and-calculators-2#mbb_shot_quality_model)
- [`mbb_team_ratings`](reference/additional/models-and-calculators-2#mbb_team_ratings)
- [`mbb_transfer_projection`](reference/additional/models-and-calculators-2#mbb_transfer_projection)
- [`pick_ridge_regression`](reference/additional/models-and-calculators-2#pick_ridge_regression)
- [`player_per100_features`](reference/additional/models-and-calculators-2#player_per100_features)
- [`poss_calc_fragment_sum`](reference/additional/models-and-calculators-2#poss_calc_fragment_sum)
- [`predict_margin`](reference/additional/models-and-calculators-2#predict_margin)
- [`predict_total`](reference/additional/models-and-calculators-2#predict_total)
- [`rank_corr`](reference/additional/models-and-calculators-2#rank_corr)
- [`raw_game_efficiency`](reference/additional/models-and-calculators-2#raw_game_efficiency)
- [`ridge_cv_lambda`](reference/additional/models-and-calculators-2#ridge_cv_lambda)
- [`ridge_fit`](reference/additional/models-and-calculators-2#ridge_fit)
- [`roc_auc`](reference/additional/models-and-calculators-2#roc_auc)
- [`save_artifact`](reference/additional/models-and-calculators-2#save_artifact)
- [`score_to_tuple`](reference/additional/models-and-calculators-2#score_to_tuple)
- [`simulate_game`](reference/additional/models-and-calculators-2#simulate_game)
- [`slow_regression`](reference/additional/models-and-calculators-2#slow_regression)
- [`spearman_corr`](reference/additional/models-and-calculators-2#spearman_corr)
- [`talent_split_mse`](reference/additional/models-and-calculators-2#talent_split_mse)
- [`transfer_cohort`](reference/additional/models-and-calculators-2#transfer_cohort)
- [`win_prob_from_margin`](reference/additional/models-and-calculators-2#win_prob_from_margin)

### Analytics {#analytics}

- [`ConcurrentClump`](reference/additional/analytics#ConcurrentClump)
- [`PossState`](reference/additional/analytics#PossState)
- [`apply_relative_positional_overrides`](reference/additional/analytics#apply_relative_positional_overrides)
- [`assign_to_right_lineup`](reference/additional/analytics#assign_to_right_lineup)
- [`build_3p_shot_info`](reference/additional/analytics#build_3p_shot_info)
- [`build_adjusted_3p`](reference/additional/analytics#build_adjusted_3p)
- [`build_efficiency_margins`](reference/additional/analytics#build_efficiency_margins)
- [`build_exp_3p`](reference/additional/analytics#build_exp_3p)
- [`build_position`](reference/additional/analytics#build_position)
- [`build_position_confidences`](reference/additional/analytics#build_position_confidences)
- [`build_positional_aware_filter`](reference/additional/analytics#build_positional_aware_filter)
- [`calc_def_player_luck_adj`](reference/additional/analytics#calc_def_player_luck_adj)
- [`calc_def_team_luck_adj`](reference/additional/analytics#calc_def_team_luck_adj)
- [`calc_off_player_luck_adj`](reference/additional/analytics#calc_off_player_luck_adj)
- [`calc_off_team_luck_adj`](reference/additional/analytics#calc_off_team_luck_adj)
- [`calculate_aggregated_lineup_stats`](reference/additional/analytics#calculate_aggregated_lineup_stats)
- [`calculate_possessions`](reference/additional/analytics#calculate_possessions)
- [`calculate_possessions_by_event`](reference/additional/analytics#calculate_possessions_by_event)
- [`calculate_stats`](reference/additional/analytics#calculate_stats)
- [`complete_weighted_avg`](reference/additional/analytics#complete_weighted_avg)
- [`concurrent_event_handler`](reference/additional/analytics#concurrent_event_handler)
- [`count_matching`](reference/additional/analytics#count_matching)
- [`get_stats_diff`](reference/additional/analytics#get_stats_diff)
- [`incorporate_height`](reference/additional/analytics#incorporate_height)
- [`inject_luck`](reference/additional/analytics#inject_luck)
- [`lineup_as_raw_clumps`](reference/additional/analytics#lineup_as_raw_clumps)
- [`lineup_balancer`](reference/additional/analytics#lineup_balancer)
- [`lineup_fixer`](reference/additional/analytics#lineup_fixer)
- [`lineup_to_team_report`](reference/additional/analytics#lineup_to_team_report)
- [`mbb_archetypes`](reference/additional/analytics#mbb_archetypes)
- [`mbb_bracketology`](reference/additional/analytics#mbb_bracketology)
- [`mbb_shot_selection`](reference/additional/analytics#mbb_shot_selection)
- [`mbb_strength_of_schedule`](reference/additional/analytics#mbb_strength_of_schedule)
- [`ncaa_mbb_lineups`](reference/additional/analytics#ncaa_mbb_lineups)
- [`ncaa_mbb_on_off`](reference/additional/analytics#ncaa_mbb_on_off)
- [`ncaa_mbb_player_combos`](reference/additional/analytics#ncaa_mbb_player_combos)
- [`ncaa_mbb_player_lineups`](reference/additional/analytics#ncaa_mbb_player_lineups)
- [`order_lineup`](reference/additional/analytics#order_lineup)
- [`pos_class_to_score`](reference/additional/analytics#pos_class_to_score)
- [`project_bracket`](reference/additional/analytics#project_bracket)
- [`regress_shot_quality`](reference/additional/analytics#regress_shot_quality)
- [`strength_of_schedule`](reference/additional/analytics#strength_of_schedule)
- [`test_positional_aware_filter`](reference/additional/analytics#test_positional_aware_filter)
- [`using_roster_pos`](reference/additional/analytics#using_roster_pos)
- [`weighted_avg`](reference/additional/analytics#weighted_avg)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_mbb_season`](reference/additional/highlights#most_recent_mbb_season)

### IDs and crosswalks {#ids-and-crosswalks}

- [`FuzzyMatchError`](reference/additional/ids-and-crosswalks#FuzzyMatchError)
- [`NoSurnameMatch`](reference/additional/ids-and-crosswalks#NoSurnameMatch)
- [`StrongSurnameMatch`](reference/additional/ids-and-crosswalks#StrongSurnameMatch)
- [`TidyPlayerContext`](reference/additional/ids-and-crosswalks#TidyPlayerContext)
- [`WeakSurnameMatch`](reference/additional/ids-and-crosswalks#WeakSurnameMatch)
- [`box_aware_compare`](reference/additional/ids-and-crosswalks#box_aware_compare)
- [`build_tidy_player_context`](reference/additional/ids-and-crosswalks#build_tidy_player_context)
- [`code_from_box`](reference/additional/ids-and-crosswalks#code_from_box)
- [`convert_from_digits`](reference/additional/ids-and-crosswalks#convert_from_digits)
- [`convert_from_initials`](reference/additional/ids-and-crosswalks#convert_from_initials)
- [`display_name_to_roster_key`](reference/additional/ids-and-crosswalks#display_name_to_roster_key)
- [`fuzzy_box_match`](reference/additional/ids-and-crosswalks#fuzzy_box_match)
- [`mbb_player_crosswalk`](reference/additional/ids-and-crosswalks#mbb_player_crosswalk)
- [`mbb_schedule_crosswalk`](reference/additional/ids-and-crosswalks#mbb_schedule_crosswalk)
- [`mbb_team_crosswalk`](reference/additional/ids-and-crosswalks#mbb_team_crosswalk)
- [`name_is_initials`](reference/additional/ids-and-crosswalks#name_is_initials)
- [`ncaa_espn_team_crosswalk`](reference/additional/ids-and-crosswalks#ncaa_espn_team_crosswalk)
- [`ncaa_mbb_team_ids`](reference/additional/ids-and-crosswalks#ncaa_mbb_team_ids)
- [`refresh_ncaa_team_ids`](reference/additional/ids-and-crosswalks#refresh_ncaa_team_ids)
- [`resolve_ncaa_team_id`](reference/additional/ids-and-crosswalks#resolve_ncaa_team_id)
- [`tidy_player`](reference/additional/ids-and-crosswalks#tidy_player)


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
| [`fox_mbb_boxscore`](reference/additional/fox-sports-api#fox_mbb_boxscore) | [`fox_mbb_boxscore`](https://hoopR.sportsdataverse.org/reference/fox_mbb_boxscore.html) |
| [`fox_mbb_league_leaders`](reference/additional/fox-sports-api#fox_mbb_league_leaders) | [`fox_mbb_league_leaders`](https://hoopR.sportsdataverse.org/reference/fox_mbb_league_leaders.html) |
| [`fox_mbb_odds`](reference/additional/fox-sports-api#fox_mbb_odds) | [`fox_mbb_odds`](https://hoopR.sportsdataverse.org/reference/fox_mbb_odds.html) |
| [`fox_mbb_pbp`](reference/additional/fox-sports-api#fox_mbb_pbp) | [`fox_mbb_pbp`](https://hoopR.sportsdataverse.org/reference/fox_mbb_pbp.html) |
| [`fox_mbb_standings`](reference/additional/fox-sports-api#fox_mbb_standings) | [`fox_mbb_standings`](https://hoopR.sportsdataverse.org/reference/fox_mbb_standings.html) |
| [`fox_mbb_team_gamelog`](reference/additional/fox-sports-api#fox_mbb_team_gamelog) | [`fox_mbb_team_gamelog`](https://hoopR.sportsdataverse.org/reference/fox_mbb_team_gamelog.html) |
| [`fox_mbb_team_roster`](reference/additional/fox-sports-api#fox_mbb_team_roster) | [`fox_mbb_team_roster`](https://hoopR.sportsdataverse.org/reference/fox_mbb_team_roster.html) |
| [`fox_mbb_team_stats`](reference/additional/fox-sports-api#fox_mbb_team_stats) | [`fox_mbb_team_stats`](https://hoopR.sportsdataverse.org/reference/fox_mbb_team_stats.html) |
| [`fox_mbb_teams`](reference/additional/fox-sports-api#fox_mbb_teams) | [`fox_mbb_teams`](https://hoopR.sportsdataverse.org/reference/fox_mbb_teams.html) |
| [`fox_mbb_teams_all`](reference/additional/fox-sports-api#fox_mbb_teams_all) | [`fox_mbb_teams_all`](https://hoopR.sportsdataverse.org/reference/fox_mbb_teams_all.html) |
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
| [`load_mbb_team_crosswalk`](reference/loaders/other#load_mbb_team_crosswalk) | [`load_mbb_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/load_mbb_team_crosswalk.html) |
| [`load_mbb_team_group_seasons`](reference/loaders/other#load_mbb_team_group_seasons) | [`load_mbb_team_group_seasons`](https://hoopR.sportsdataverse.org/reference/load_mbb_team_group_seasons.html) |
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
| [`mbb_player_crosswalk`](reference/additional/ids-and-crosswalks#mbb_player_crosswalk) | [`mbb_player_crosswalk`](https://hoopR.sportsdataverse.org/reference/mbb_player_crosswalk.html) |
| [`mbb_schedule_crosswalk`](reference/additional/ids-and-crosswalks#mbb_schedule_crosswalk) | [`mbb_schedule_crosswalk`](https://hoopR.sportsdataverse.org/reference/mbb_schedule_crosswalk.html) |
| [`mbb_team_crosswalk`](reference/additional/ids-and-crosswalks#mbb_team_crosswalk) | [`mbb_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/mbb_team_crosswalk.html) |
| [`most_recent_mbb_season`](reference/additional/highlights#most_recent_mbb_season) | [`most_recent_mbb_season`](https://hoopR.sportsdataverse.org/reference/most_recent_mbb_season.html) |
| [`torvik_game_schedule`](reference/torvik#torvik_game_schedule) | [`torvik_game_schedule`](https://hoopR.sportsdataverse.org/reference/torvik_game_schedule.html) |
| [`torvik_game_stats`](reference/torvik#torvik_game_stats) | [`torvik_game_stats`](https://hoopR.sportsdataverse.org/reference/torvik_game_stats.html) |
| [`torvik_player_stats`](reference/torvik#torvik_player_stats) | [`torvik_player_stats`](https://hoopR.sportsdataverse.org/reference/torvik_player_stats.html) |
| [`torvik_ratings`](reference/torvik#torvik_ratings) | [`torvik_ratings`](https://hoopR.sportsdataverse.org/reference/torvik_ratings.html) |
| [`torvik_team_factors`](reference/torvik#torvik_team_factors) | [`torvik_team_factors`](https://hoopR.sportsdataverse.org/reference/torvik_team_factors.html) |
