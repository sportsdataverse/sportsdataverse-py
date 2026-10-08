---
title: WBB
sidebar_label: WBB
description: "sdv-py WBB: endpoint references, dataset loaders and parsers for WBB in the SportsDataverse Python package."
---
# WBB (`sportsdataverse.wbb`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 129 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 34 | none |
| [stats.ncaa.org](#stats-ncaa-org) | `stats.ncaa.org` | 111 | none (Terms gate + rate rotation) |
| [Bart Torvik Women's T-Rank](#bart-torvik-women-s-t-rank) | `barttorvik.com` | 1 | none |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 27 | none |
| [Her Hoop Stats](#her-hoop-stats) | `herhoopstats.com` | 5 | subscription |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 316 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 86 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |
| [Hand-written wrappers](reference/additional/espn) | 8 |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 34 |

## stats.ncaa.org {#stats-ncaa-org}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/stats-ncaa-org) | 111 |

## Bart Torvik Women's T-Rank {#bart-torvik-women-s-t-rank}

| Reference | Functions |
|---|---:|
| [Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)](reference/bart_wbb) | 1 |

## Fox Sports API {#fox-sports-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/fox-sports-api) | 27 |

## Her Hoop Stats {#her-hoop-stats}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/her-hoop-stats) | 5 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`build_athlete_identity_lookup`](reference/additional/play-by-play-processing#build_athlete_identity_lookup)
- [`classify_point_value`](reference/additional/play-by-play-processing#classify_point_value)
- [`classify_zone_geometry`](reference/additional/play-by-play-processing#classify_zone_geometry)
- [`classify_zone_type`](reference/additional/play-by-play-processing#classify_zone_type)
- [`espn_shots_to_canonical`](reference/additional/play-by-play-processing#espn_shots_to_canonical)
- [`fit_espn_court_scale`](reference/additional/play-by-play-processing#fit_espn_court_scale)
- [`ncaa_wbb_game_pbp`](reference/additional/play-by-play-processing#ncaa_wbb_game_pbp)
- [`ncaa_wbb_play_by_play`](reference/additional/play-by-play-processing#ncaa_wbb_play_by_play)
- [`shot_events_to_frame`](reference/additional/play-by-play-processing#shot_events_to_frame)
- [`wbb_pbp_disk`](reference/additional/play-by-play-processing#wbb_pbp_disk)

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
- [`PlayerId`](reference/additional/models-and-calculators#PlayerId)
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
- [`ShotQualityConstants`](reference/additional/models-and-calculators#ShotQualityConstants)
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
- [`build_net_points`](reference/additional/models-and-calculators#build_net_points)
- [`build_o_rtg`](reference/additional/models-and-calculators#build_o_rtg)
- [`build_player_context`](reference/additional/models-and-calculators#build_player_context)
- [`build_priors`](reference/additional/models-and-calculators#build_priors)
- [`build_productivity`](reference/additional/models-and-calculators#build_productivity)
- [`build_wbb_season_wp`](reference/additional/models-and-calculators-2#build_wbb_season_wp)
- [`build_weak_prior_from_rapm`](reference/additional/models-and-calculators-2#build_weak_prior_from_rapm)
- [`calc_collinearity_diag`](reference/additional/models-and-calculators-2#calc_collinearity_diag)
- [`calc_lineup_outputs`](reference/additional/models-and-calculators-2#calc_lineup_outputs)
- [`calc_player_weights`](reference/additional/models-and-calculators-2#calc_player_weights)
- [`calc_slow_pseudo_inverse`](reference/additional/models-and-calculators-2#calc_slow_pseudo_inverse)
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
- [`pick_ridge_regression`](reference/additional/models-and-calculators-2#pick_ridge_regression)
- [`player_per100_features`](reference/additional/models-and-calculators-2#player_per100_features)
- [`poss_calc_fragment_sum`](reference/additional/models-and-calculators-2#poss_calc_fragment_sum)
- [`predict_margin`](reference/additional/models-and-calculators-2#predict_margin)
- [`predict_total`](reference/additional/models-and-calculators-2#predict_total)
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
- [`three_point_radius`](reference/additional/models-and-calculators-2#three_point_radius)
- [`transfer_cohort`](reference/additional/models-and-calculators-2#transfer_cohort)
- [`wbb_bracket_sim`](reference/additional/models-and-calculators-2#wbb_bracket_sim)
- [`wbb_in_game_win_prob`](reference/additional/models-and-calculators-2#wbb_in_game_win_prob)
- [`wbb_predict_games`](reference/additional/models-and-calculators-2#wbb_predict_games)
- [`wbb_season_sim`](reference/additional/models-and-calculators-2#wbb_season_sim)
- [`wbb_team_ratings`](reference/additional/models-and-calculators-2#wbb_team_ratings)
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
- [`ncaa_wbb_lineups`](reference/additional/analytics#ncaa_wbb_lineups)
- [`ncaa_wbb_on_off`](reference/additional/analytics#ncaa_wbb_on_off)
- [`ncaa_wbb_player_combos`](reference/additional/analytics#ncaa_wbb_player_combos)
- [`ncaa_wbb_player_lineups`](reference/additional/analytics#ncaa_wbb_player_lineups)
- [`order_lineup`](reference/additional/analytics#order_lineup)
- [`pos_class_to_score`](reference/additional/analytics#pos_class_to_score)
- [`project_bracket`](reference/additional/analytics#project_bracket)
- [`regress_shot_quality`](reference/additional/analytics#regress_shot_quality)
- [`strength_of_schedule`](reference/additional/analytics#strength_of_schedule)
- [`test_positional_aware_filter`](reference/additional/analytics#test_positional_aware_filter)
- [`using_roster_pos`](reference/additional/analytics#using_roster_pos)
- [`wbb_bracketology`](reference/additional/analytics#wbb_bracketology)
- [`wbb_strength_of_schedule`](reference/additional/analytics-2#wbb_strength_of_schedule)
- [`weighted_avg`](reference/additional/analytics-2#weighted_avg)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_wbb_season`](reference/additional/dates-and-seasons#most_recent_wbb_season)

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
- [`ncaa_espn_team_crosswalk`](reference/additional/ids-and-crosswalks#ncaa_espn_team_crosswalk)
- [`ncaa_wbb_team_ids`](reference/additional/ids-and-crosswalks#ncaa_wbb_team_ids)
- [`resolve_ncaa_team_id`](reference/additional/ids-and-crosswalks#resolve_ncaa_team_id)
- [`tidy_player`](reference/additional/ids-and-crosswalks#tidy_player)
- [`wbb_player_crosswalk`](reference/additional/ids-and-crosswalks#wbb_player_crosswalk)
- [`wbb_schedule_crosswalk`](reference/additional/ids-and-crosswalks#wbb_schedule_crosswalk)
- [`wbb_team_crosswalk`](reference/additional/ids-and-crosswalks#wbb_team_crosswalk)


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
| [`espn_wbb_game_officials`](reference/additional/espn#espn_wbb_game_officials) | [`espn_wbb_game_officials`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_officials.html) |
| [`espn_wbb_game_play`](reference/core/game#espn_wbb_game_play) | [`espn_wbb_game_play`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_play.html) |
| [`espn_wbb_game_play_personnel`](reference/core/game#espn_wbb_game_play_personnel) | [`espn_wbb_game_play_personnel`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_play_personnel.html) |
| [`espn_wbb_game_powerindex`](reference/core/game#espn_wbb_game_powerindex) | [`espn_wbb_game_powerindex`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_powerindex.html) |
| [`espn_wbb_game_predictor`](reference/core/game#espn_wbb_game_predictor) | [`espn_wbb_game_predictor`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_predictor.html) |
| [`espn_wbb_game_probabilities`](reference/core/game#espn_wbb_game_probabilities) | [`espn_wbb_game_probabilities`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_probabilities.html) |
| [`espn_wbb_game_propbets`](reference/core/game#espn_wbb_game_propbets) | [`espn_wbb_game_propbets`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_propbets.html) |
| [`espn_wbb_game_rosters`](reference/additional/espn#espn_wbb_game_rosters) | [`espn_wbb_game_rosters`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_rosters.html) |
| [`espn_wbb_game_situation`](reference/core/game#espn_wbb_game_situation) | [`espn_wbb_game_situation`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_situation.html) |
| [`espn_wbb_game_team_leaders`](reference/core/game#espn_wbb_game_team_leaders) | [`espn_wbb_game_team_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_leaders.html) |
| [`espn_wbb_game_team_linescores`](reference/core/game#espn_wbb_game_team_linescores) | [`espn_wbb_game_team_linescores`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_linescores.html) |
| [`espn_wbb_game_team_roster`](reference/core/game#espn_wbb_game_team_roster) | [`espn_wbb_game_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_roster.html) |
| [`espn_wbb_game_team_statistics`](reference/core/game#espn_wbb_game_team_statistics) | [`espn_wbb_game_team_statistics`](https://wehoop.sportsdataverse.org/reference/espn_wbb_game_team_statistics.html) |
| [`espn_wbb_injuries`](reference/site#espn_wbb_injuries) | [`espn_wbb_injuries`](https://wehoop.sportsdataverse.org/reference/espn_wbb_injuries.html) |
| [`espn_wbb_leaders`](reference/web#espn_wbb_leaders) | [`espn_wbb_leaders`](https://wehoop.sportsdataverse.org/reference/espn_wbb_leaders.html) |
| [`espn_wbb_news`](reference/site#espn_wbb_news) | [`espn_wbb_news`](https://wehoop.sportsdataverse.org/reference/espn_wbb_news.html) |
| [`espn_wbb_pbp`](reference/additional/espn#espn_wbb_pbp) | [`espn_wbb_pbp`](https://wehoop.sportsdataverse.org/reference/espn_wbb_pbp.html) |
| [`espn_wbb_player_awards`](reference/core/player#espn_wbb_player_awards) | [`espn_wbb_player_awards`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_awards.html) |
| [`espn_wbb_player_career_stats`](reference/core/player#espn_wbb_player_career_stats) | [`espn_wbb_player_career_stats`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_career_stats.html) |
| [`espn_wbb_player_eventlog`](reference/core/player#espn_wbb_player_eventlog) | [`espn_wbb_player_eventlog`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_eventlog.html) |
| [`espn_wbb_player_gamelog`](reference/web#espn_wbb_player_gamelog) | [`espn_wbb_player_gamelog`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_gamelog.html) |
| [`espn_wbb_player_info`](reference/site#espn_wbb_player_info) | [`espn_wbb_player_info`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_info.html) |
| [`espn_wbb_player_overview`](reference/web#espn_wbb_player_overview) | [`espn_wbb_player_overview`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_overview.html) |
| [`espn_wbb_player_seasons`](reference/core/player#espn_wbb_player_seasons) | [`espn_wbb_player_seasons`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_seasons.html) |
| [`espn_wbb_player_splits`](reference/web#espn_wbb_player_splits) | [`espn_wbb_player_splits`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_splits.html) |
| [`espn_wbb_player_statisticslog`](reference/core/player#espn_wbb_player_statisticslog) | [`espn_wbb_player_statisticslog`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_statisticslog.html) |
| [`espn_wbb_player_stats`](reference/additional/espn#espn_wbb_player_stats) | [`espn_wbb_player_stats`](https://wehoop.sportsdataverse.org/reference/espn_wbb_player_stats.html) |
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
| [`espn_wbb_team_stats`](reference/additional/espn#espn_wbb_team_stats) | [`espn_wbb_team_stats`](https://wehoop.sportsdataverse.org/reference/espn_wbb_team_stats.html) |
| [`espn_wbb_teams`](reference/additional/espn#espn_wbb_teams) | [`espn_wbb_teams`](https://wehoop.sportsdataverse.org/reference/espn_wbb_teams.html) |
| [`espn_wbb_tournaments`](reference/core/other#espn_wbb_tournaments) | [`espn_wbb_tournaments`](https://wehoop.sportsdataverse.org/reference/espn_wbb_tournaments.html) |
| [`espn_wbb_venues`](reference/core/other#espn_wbb_venues) | [`espn_wbb_venues`](https://wehoop.sportsdataverse.org/reference/espn_wbb_venues.html) |
| [`fox_wbb_boxscore`](reference/additional/fox-sports-api#fox_wbb_boxscore) | [`fox_wbb_boxscore`](https://wehoop.sportsdataverse.org/reference/fox_wbb_boxscore.html) |
| [`fox_wbb_league_leaders`](reference/additional/fox-sports-api#fox_wbb_league_leaders) | [`fox_wbb_league_leaders`](https://wehoop.sportsdataverse.org/reference/fox_wbb_league_leaders.html) |
| [`fox_wbb_odds`](reference/additional/fox-sports-api#fox_wbb_odds) | [`fox_wbb_odds`](https://wehoop.sportsdataverse.org/reference/fox_wbb_odds.html) |
| [`fox_wbb_pbp`](reference/additional/fox-sports-api#fox_wbb_pbp) | [`fox_wbb_pbp`](https://wehoop.sportsdataverse.org/reference/fox_wbb_pbp.html) |
| [`fox_wbb_standings`](reference/additional/fox-sports-api#fox_wbb_standings) | [`fox_wbb_standings`](https://wehoop.sportsdataverse.org/reference/fox_wbb_standings.html) |
| [`fox_wbb_team_gamelog`](reference/additional/fox-sports-api#fox_wbb_team_gamelog) | [`fox_wbb_team_gamelog`](https://wehoop.sportsdataverse.org/reference/fox_wbb_team_gamelog.html) |
| [`fox_wbb_team_roster`](reference/additional/fox-sports-api#fox_wbb_team_roster) | [`fox_wbb_team_roster`](https://wehoop.sportsdataverse.org/reference/fox_wbb_team_roster.html) |
| [`fox_wbb_team_stats`](reference/additional/fox-sports-api#fox_wbb_team_stats) | [`fox_wbb_team_stats`](https://wehoop.sportsdataverse.org/reference/fox_wbb_team_stats.html) |
| [`fox_wbb_teams`](reference/additional/fox-sports-api#fox_wbb_teams) | [`fox_wbb_teams`](https://wehoop.sportsdataverse.org/reference/fox_wbb_teams.html) |
| [`fox_wbb_teams_all`](reference/additional/fox-sports-api#fox_wbb_teams_all) | [`fox_wbb_teams_all`](https://wehoop.sportsdataverse.org/reference/fox_wbb_teams_all.html) |
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
| [`load_wbb_team_crosswalk`](reference/loaders/other#load_wbb_team_crosswalk) | [`load_wbb_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/load_wbb_team_crosswalk.html) |
| [`load_wbb_team_group_seasons`](reference/loaders/other#load_wbb_team_group_seasons) | [`load_wbb_team_group_seasons`](https://wehoop.sportsdataverse.org/reference/load_wbb_team_group_seasons.html) |
| [`most_recent_wbb_season`](reference/additional/dates-and-seasons#most_recent_wbb_season) | [`most_recent_wbb_season`](https://wehoop.sportsdataverse.org/reference/most_recent_wbb_season.html) |
| [`wbb_player_crosswalk`](reference/additional/ids-and-crosswalks#wbb_player_crosswalk) | [`wbb_player_crosswalk`](https://wehoop.sportsdataverse.org/reference/wbb_player_crosswalk.html) |
| [`wbb_schedule_crosswalk`](reference/additional/ids-and-crosswalks#wbb_schedule_crosswalk) | [`wbb_schedule_crosswalk`](https://wehoop.sportsdataverse.org/reference/wbb_schedule_crosswalk.html) |
| [`wbb_team_crosswalk`](reference/additional/ids-and-crosswalks#wbb_team_crosswalk) | [`wbb_team_crosswalk`](https://wehoop.sportsdataverse.org/reference/wbb_team_crosswalk.html) |
