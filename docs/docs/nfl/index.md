---
title: NFL
sidebar_label: NFL
description: "sdv-py NFL: endpoint references, dataset loaders and parsers for NFL in the SportsDataverse Python package."
---
# NFL (`sportsdataverse.nfl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 126 | none |
| [NFL.com Shield API](#nfl-com-shield-api) | `api.nfl.com` | 22 | WEB_DESKTOP bearer token |
| [NFL Pro](#nfl-pro) | `pro.nfl.com` | 32 | none |
| [Sleeper fantasy API](#sleeper-fantasy-api) | `api.sleeper.app` | 15 | none |
| [PFF Developer API](#pff-developer-api) | `api.pff.com` | 68 | API key (SDV_PY_PFF_API_KEY) |
| [PFF Premium Stats (LEGACY)](#pff-premium-stats-legacy) | `premium.pff.com` | 46 | cookie (legacy) |
| [nflverse data releases](#nflverse-data-releases) | `github.com` | 57 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 21 | none |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 25 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 177 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 84 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |
| [Hand-written wrappers](reference/additional) | 8 |

## NFL.com Shield API {#nfl-com-shield-api}

| Reference | Functions |
|---|---:|
| [NFL.com API](reference/nfl_api) | 15 |
| [Hand-written wrappers](reference/additional) | 7 |

## NFL Pro {#nfl-pro}

| Reference | Functions |
|---|---:|
| [nflpro](reference/nflpro) | 16 |
| [Hand-written wrappers](reference/additional) | 16 |

## Sleeper fantasy API {#sleeper-fantasy-api}

| Reference | Functions |
|---|---:|
| [Sleeper fantasy API v1 (api.sleeper.app)](reference/sleeper) | 15 |

## PFF Developer API {#pff-developer-api}

| Reference | Functions |
|---|---:|
| [PFF Developer API (api.pff.com, API key)](reference/pff_api) | 68 |

## PFF Premium Stats (LEGACY) {#pff-premium-stats-legacy}

| Reference | Functions |
|---|---:|
| [PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API)](reference/pff_core) | 46 |

## nflverse data releases {#nflverse-data-releases}

| Reference | Functions |
|---|---:|
| [nflverse data releases](reference/loaders) | 8 |
| [Hand-written wrappers](reference/additional) | 49 |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 21 |

## Fox Sports API {#fox-sports-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 25 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`NFLPlayProcess`](reference/additional#NFLPlayProcess)
- [`build_nfl_player_stats`](reference/additional#build_nfl_player_stats)
- [`build_nfl_player_stats_def`](reference/additional#build_nfl_player_stats_def)
- [`build_nfl_player_stats_kicking`](reference/additional#build_nfl_player_stats_kicking)
- [`build_nfl_players`](reference/additional#build_nfl_players)
- [`build_nfl_rosters`](reference/additional#build_nfl_rosters)
- [`build_nfl_season`](reference/additional#build_nfl_season)
- [`build_nfl_team_stats`](reference/additional#build_nfl_team_stats)
- [`clean_nfl_pbp`](reference/additional#clean_nfl_pbp)
- [`shield_nfl_pbp`](reference/additional#shield_nfl_pbp)
- [`shield_to_espn_summary`](reference/additional#shield_to_espn_summary)
- [`team_name_fn`](reference/additional#team_name_fn)

### Models and calculators {#models-and-calculators}

- [`adjust_pressure_pairs`](reference/additional#adjust_pressure_pairs)
- [`calculate_completion_probability`](reference/additional#calculate_completion_probability)
- [`calculate_epa`](reference/additional#calculate_epa)
- [`calculate_expected_points`](reference/additional#calculate_expected_points)
- [`calculate_nfl_series_conversion_rates`](reference/additional#calculate_nfl_series_conversion_rates)
- [`calculate_nfl_standings`](reference/additional#calculate_nfl_standings)
- [`calculate_win_probability`](reference/additional#calculate_win_probability)
- [`calculate_wpa`](reference/additional#calculate_wpa)
- [`calculate_xpass`](reference/additional#calculate_xpass)
- [`calculate_xyac`](reference/additional#calculate_xyac)
- [`efficiency_ratings`](reference/additional#efficiency_ratings)
- [`env_adjusted_make_prob`](reference/additional#env_adjusted_make_prob)
- [`fg_make_probability`](reference/additional#fg_make_probability)
- [`fit_nfl_field_position_ep`](reference/additional#fit_nfl_field_position_ep)
- [`get_2pt_probs`](reference/additional#get_2pt_probs)
- [`get_2pt_wp`](reference/additional#get_2pt_wp)
- [`get_4th_down_probs`](reference/additional#get_4th_down_probs)
- [`get_fg_wp`](reference/additional#get_fg_wp)
- [`get_go_wp`](reference/additional#get_go_wp)
- [`get_punt_wp`](reference/additional#get_punt_wp)
- [`load_nfl_fp_curve`](reference/additional#load_nfl_fp_curve)
- [`nfl_compute_results`](reference/additional#nfl_compute_results)
- [`nfl_draft_projection`](reference/additional#nfl_draft_projection)
- [`nfl_fantasy_projection`](reference/additional#nfl_fantasy_projection)
- [`nfl_kicker_rating`](reference/additional#nfl_kicker_rating)
- [`nfl_line_grades`](reference/additional#nfl_line_grades)
- [`nfl_player_projection`](reference/additional#nfl_player_projection)
- [`nfl_ratings`](reference/additional#nfl_ratings)
- [`nfl_simulations`](reference/additional#nfl_simulations)
- [`nfl_usage_projection`](reference/additional#nfl_usage_projection)
- [`opponent_adjusted_ridge`](reference/additional#opponent_adjusted_ridge)
- [`pressure_pairs`](reference/additional#pressure_pairs)
- [`special_teams_ratings`](reference/additional#special_teams_ratings)
- [`team_pressure_rates`](reference/additional#team_pressure_rates)

### Analytics {#analytics}

- [`compose_counting_projection`](reference/additional#compose_counting_projection)
- [`nfl_availability_projection`](reference/additional#nfl_availability_projection)
- [`nfl_game_script`](reference/additional#nfl_game_script)
- [`nfl_play_call_probabilities`](reference/additional#nfl_play_call_probabilities)
- [`nfl_play_call_tendencies`](reference/additional#nfl_play_call_tendencies)
- [`nfl_player_props`](reference/additional#nfl_player_props)
- [`nfl_predict_games`](reference/additional#nfl_predict_games)
- [`nfl_season_standings`](reference/additional#nfl_season_standings)
- [`playcall_features`](reference/additional#playcall_features)
- [`player_usage_efficiency`](reference/additional#player_usage_efficiency)
- [`predict_margin`](reference/additional#predict_margin)
- [`predict_total`](reference/additional#predict_total)
- [`team_game_pace`](reference/additional#team_game_pace)
- [`win_prob_from_margin`](reference/additional#win_prob_from_margin)

### Cache and configuration {#cache-and-configuration}

- [`NflConfig`](reference/additional#NflConfig)
- [`cached_loader`](reference/additional#cached_loader)
- [`clear_cache`](reference/additional#clear_cache)
- [`get_config`](reference/additional#get_config)
- [`reset_config`](reference/additional#reset_config)
- [`update_config`](reference/additional#update_config)

### Dates and seasons {#dates-and-seasons}

- [`get_current_nfl_season`](reference/additional#get_current_nfl_season)
- [`get_current_nfl_week`](reference/additional#get_current_nfl_week)
- [`get_current_season`](reference/additional#get_current_season)
- [`get_current_week`](reference/additional#get_current_week)
- [`most_recent_nfl_season`](reference/additional#most_recent_nfl_season)

### IDs and crosswalks {#ids-and-crosswalks}

- [`nfl_players_crosswalk`](reference/additional#nfl_players_crosswalk)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [NFL tutorial](../tutorials/03_nfl_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`nflreadr`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.nfl` (Python) | `nflreadr` (R) |
|---|---|
| [`clear_cache`](reference/additional/other#clear_cache) | [`clear_cache`](https://nflreadr.nflverse.com/reference/clear_cache.html) |
| [`get_current_season`](reference/additional/other#get_current_season) | [`get_current_season`](https://nflreadr.nflverse.com/reference/get_current_season.html) |
| [`get_current_week`](reference/additional/other#get_current_week) | [`get_current_week`](https://nflreadr.nflverse.com/reference/get_current_week.html) |
| [`load_combine`](reference/additional/nflverse-data-releases#load_combine) | [`load_combine`](https://nflreadr.nflverse.com/reference/load_combine.html) |
| [`load_contracts`](reference/additional/nflverse-data-releases#load_contracts) | [`load_contracts`](https://nflreadr.nflverse.com/reference/load_contracts.html) |
| [`load_depth_charts`](reference/additional/nflverse-data-releases#load_depth_charts) | [`load_depth_charts`](https://nflreadr.nflverse.com/reference/load_depth_charts.html) |
| [`load_draft_picks`](reference/additional/nflverse-data-releases#load_draft_picks) | [`load_draft_picks`](https://nflreadr.nflverse.com/reference/load_draft_picks.html) |
| [`load_espn_qbr`](reference/additional/nflverse-data-releases#load_espn_qbr) | [`load_espn_qbr`](https://nflreadr.nflverse.com/reference/load_espn_qbr.html) |
| [`load_ff_opportunity`](reference/additional/nflverse-data-releases#load_ff_opportunity) | [`load_ff_opportunity`](https://nflreadr.nflverse.com/reference/load_ff_opportunity.html) |
| [`load_ff_playerids`](reference/additional/nflverse-data-releases#load_ff_playerids) | [`load_ff_playerids`](https://nflreadr.nflverse.com/reference/load_ff_playerids.html) |
| [`load_ff_rankings`](reference/additional/nflverse-data-releases#load_ff_rankings) | [`load_ff_rankings`](https://nflreadr.nflverse.com/reference/load_ff_rankings.html) |
| [`load_ftn_charting`](reference/additional/nflverse-data-releases#load_ftn_charting) | [`load_ftn_charting`](https://nflreadr.nflverse.com/reference/load_ftn_charting.html) |
| [`load_injuries`](reference/additional/nflverse-data-releases#load_injuries) | [`load_injuries`](https://nflreadr.nflverse.com/reference/load_injuries.html) |
| [`load_nextgen_stats`](reference/additional/nflverse-data-releases#load_nextgen_stats) | [`load_nextgen_stats`](https://nflreadr.nflverse.com/reference/load_nextgen_stats.html) |
| [`load_nfl_combine`](reference/additional/nflverse-data-releases#load_nfl_combine) | [`load_combine`](https://nflreadr.nflverse.com/reference/load_combine.html) |
| [`load_nfl_contracts`](reference/additional/nflverse-data-releases#load_nfl_contracts) | [`load_contracts`](https://nflreadr.nflverse.com/reference/load_contracts.html) |
| [`load_nfl_depth_charts`](reference/loaders/other#load_nfl_depth_charts) | [`load_depth_charts`](https://nflreadr.nflverse.com/reference/load_depth_charts.html) |
| [`load_nfl_draft_picks`](reference/additional/nflverse-data-releases#load_nfl_draft_picks) | [`load_draft_picks`](https://nflreadr.nflverse.com/reference/load_draft_picks.html) |
| [`load_nfl_espn_qbr`](reference/additional/nflverse-data-releases#load_nfl_espn_qbr) | [`load_espn_qbr`](https://nflreadr.nflverse.com/reference/load_espn_qbr.html) |
| [`load_nfl_ff_opportunity`](reference/additional/nflverse-data-releases-2#load_nfl_ff_opportunity) | [`load_ff_opportunity`](https://nflreadr.nflverse.com/reference/load_ff_opportunity.html) |
| [`load_nfl_ff_playerids`](reference/additional/nflverse-data-releases-2#load_nfl_ff_playerids) | [`load_ff_playerids`](https://nflreadr.nflverse.com/reference/load_ff_playerids.html) |
| [`load_nfl_ff_rankings`](reference/additional/nflverse-data-releases-2#load_nfl_ff_rankings) | [`load_ff_rankings`](https://nflreadr.nflverse.com/reference/load_ff_rankings.html) |
| [`load_nfl_ftn_charting`](reference/loaders/other#load_nfl_ftn_charting) | [`load_ftn_charting`](https://nflreadr.nflverse.com/reference/load_ftn_charting.html) |
| [`load_nfl_injuries`](reference/loaders/other#load_nfl_injuries) | [`load_injuries`](https://nflreadr.nflverse.com/reference/load_injuries.html) |
| [`load_nfl_nextgen_stats`](reference/additional/nflverse-data-releases-2#load_nfl_nextgen_stats) | [`load_nextgen_stats`](https://nflreadr.nflverse.com/reference/load_nextgen_stats.html) |
| [`load_nfl_officials`](reference/additional/nflverse-data-releases-2#load_nfl_officials) | [`load_officials`](https://nflreadr.nflverse.com/reference/load_officials.html) |
| [`load_nfl_pbp`](reference/loaders/pbp#load_nfl_pbp) | [`load_pbp`](https://nflreadr.nflverse.com/reference/load_pbp.html) |
| [`load_nfl_pbp_participation`](reference/loaders/pbp#load_nfl_pbp_participation) | [`load_participation`](https://nflreadr.nflverse.com/reference/load_participation.html) |
| [`load_nfl_pfr_advstats`](reference/additional/nflverse-data-releases-2#load_nfl_pfr_advstats) | [`load_pfr_advstats`](https://nflreadr.nflverse.com/reference/load_pfr_advstats.html) |
| [`load_nfl_player_stats`](reference/additional/nflverse-data-releases-3#load_nfl_player_stats) | [`load_player_stats`](https://nflreadr.nflverse.com/reference/load_player_stats.html) |
| [`load_nfl_players`](reference/additional/nflverse-data-releases-3#load_nfl_players) | [`load_players`](https://nflreadr.nflverse.com/reference/load_players.html) |
| [`load_nfl_rosters`](reference/loaders/other#load_nfl_rosters) | [`load_rosters`](https://nflreadr.nflverse.com/reference/load_rosters.html) |
| [`load_nfl_schedule`](reference/additional/nflverse-data-releases-3#load_nfl_schedule) | [`load_schedules`](https://nflreadr.nflverse.com/reference/load_schedules.html) |
| [`load_nfl_snap_counts`](reference/loaders/other#load_nfl_snap_counts) | [`load_snap_counts`](https://nflreadr.nflverse.com/reference/load_snap_counts.html) |
| [`load_nfl_team_stats`](reference/additional/nflverse-data-releases-3#load_nfl_team_stats) | [`load_team_stats`](https://nflreadr.nflverse.com/reference/load_team_stats.html) |
| [`load_nfl_teams`](reference/additional/nflverse-data-releases-3#load_nfl_teams) | [`load_teams`](https://nflreadr.nflverse.com/reference/load_teams.html) |
| [`load_nfl_trades`](reference/additional/nflverse-data-releases-3#load_nfl_trades) | [`load_trades`](https://nflreadr.nflverse.com/reference/load_trades.html) |
| [`load_nfl_weekly_rosters`](reference/loaders/other#load_nfl_weekly_rosters) | [`load_rosters_weekly`](https://nflreadr.nflverse.com/reference/load_rosters_weekly.html) |
| [`load_officials`](reference/additional/nflverse-data-releases-3#load_officials) | [`load_officials`](https://nflreadr.nflverse.com/reference/load_officials.html) |
| [`load_participation`](reference/additional/nflverse-data-releases-3#load_participation) | [`load_participation`](https://nflreadr.nflverse.com/reference/load_participation.html) |
| [`load_pfr_advstats`](reference/additional/nflverse-data-releases-3#load_pfr_advstats) | [`load_pfr_advstats`](https://nflreadr.nflverse.com/reference/load_pfr_advstats.html) |
| [`load_player_stats`](reference/additional/nflverse-data-releases-3#load_player_stats) | [`load_player_stats`](https://nflreadr.nflverse.com/reference/load_player_stats.html) |
| [`load_players`](reference/additional/nflverse-data-releases-4#load_players) | [`load_players`](https://nflreadr.nflverse.com/reference/load_players.html) |
| [`load_rosters_weekly`](reference/additional/nflverse-data-releases-4#load_rosters_weekly) | [`load_rosters_weekly`](https://nflreadr.nflverse.com/reference/load_rosters_weekly.html) |
| [`load_schedules`](reference/additional/nflverse-data-releases-4#load_schedules) | [`load_schedules`](https://nflreadr.nflverse.com/reference/load_schedules.html) |
| [`load_snap_counts`](reference/additional/nflverse-data-releases-4#load_snap_counts) | [`load_snap_counts`](https://nflreadr.nflverse.com/reference/load_snap_counts.html) |
| [`load_team_stats`](reference/additional/nflverse-data-releases-4#load_team_stats) | [`load_team_stats`](https://nflreadr.nflverse.com/reference/load_team_stats.html) |
| [`load_teams`](reference/additional/nflverse-data-releases-4#load_teams) | [`load_teams`](https://nflreadr.nflverse.com/reference/load_teams.html) |
| [`load_trades`](reference/additional/nflverse-data-releases-4#load_trades) | [`load_trades`](https://nflreadr.nflverse.com/reference/load_trades.html) |
