---
title: NFL
sidebar_label: NFL
description: "sdv-py NFL: endpoint references, dataset loaders and parsers for NFL in the SportsDataverse Python package."
---
# NFL (`sportsdataverse.nfl`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 24 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 84 | `https://sports.core.api.espn.com/v2/sports` |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 | `https://site.web.api.espn.com/apis/fitt/v3/sports` |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 | `https://cdn.espn.com/core` |
| [NFL.com API](reference/nfl_api) | 15 | `https://api.nfl.com` |
| [nflpro](reference/nflpro) | 16 | `https://pro.nfl.com` |
| [PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API)](reference/pff_core) | 46 | `https://premium.pff.com` |
| [PFF Developer API (api.pff.com, API key)](reference/pff_api) | 68 | `https://api.pff.com` |
| [Sleeper fantasy API v1 (api.sleeper.app)](reference/sleeper) | 15 | `https://api.sleeper.app/v1` |
| [Dataset loaders](reference/loaders) | 29 | nflverse data releases / sportsdataverse-data releases |
| [Additional functions](reference/additional) | 177 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [NFL tutorial](../tutorials/03_nfl_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`nflreadr`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.nfl` (Python) | `nflreadr` (R) |
|---|---|
| [`clear_cache`](reference/additional/other#clear_cache) | [`clear_cache`](https://nflreadr.nflverse.com/reference/clear_cache.html) |
| [`get_current_season`](reference/additional/utilities-helpers#get_current_season) | [`get_current_season`](https://nflreadr.nflverse.com/reference/get_current_season.html) |
| [`get_current_week`](reference/additional/utilities-helpers#get_current_week) | [`get_current_week`](https://nflreadr.nflverse.com/reference/get_current_week.html) |
| [`load_combine`](reference/additional/dataset-loaders#load_combine) | [`load_combine`](https://nflreadr.nflverse.com/reference/load_combine.html) |
| [`load_contracts`](reference/additional/dataset-loaders#load_contracts) | [`load_contracts`](https://nflreadr.nflverse.com/reference/load_contracts.html) |
| [`load_depth_charts`](reference/additional/dataset-loaders#load_depth_charts) | [`load_depth_charts`](https://nflreadr.nflverse.com/reference/load_depth_charts.html) |
| [`load_draft_picks`](reference/additional/dataset-loaders#load_draft_picks) | [`load_draft_picks`](https://nflreadr.nflverse.com/reference/load_draft_picks.html) |
| [`load_espn_qbr`](reference/additional/dataset-loaders#load_espn_qbr) | [`load_espn_qbr`](https://nflreadr.nflverse.com/reference/load_espn_qbr.html) |
| [`load_ff_opportunity`](reference/additional/dataset-loaders#load_ff_opportunity) | [`load_ff_opportunity`](https://nflreadr.nflverse.com/reference/load_ff_opportunity.html) |
| [`load_ff_playerids`](reference/additional/dataset-loaders#load_ff_playerids) | [`load_ff_playerids`](https://nflreadr.nflverse.com/reference/load_ff_playerids.html) |
| [`load_ff_rankings`](reference/additional/dataset-loaders#load_ff_rankings) | [`load_ff_rankings`](https://nflreadr.nflverse.com/reference/load_ff_rankings.html) |
| [`load_ftn_charting`](reference/additional/dataset-loaders#load_ftn_charting) | [`load_ftn_charting`](https://nflreadr.nflverse.com/reference/load_ftn_charting.html) |
| [`load_injuries`](reference/additional/dataset-loaders#load_injuries) | [`load_injuries`](https://nflreadr.nflverse.com/reference/load_injuries.html) |
| [`load_nextgen_stats`](reference/additional/dataset-loaders#load_nextgen_stats) | [`load_nextgen_stats`](https://nflreadr.nflverse.com/reference/load_nextgen_stats.html) |
| [`load_nfl_combine`](reference/additional/dataset-loaders#load_nfl_combine) | [`load_combine`](https://nflreadr.nflverse.com/reference/load_combine.html) |
| [`load_nfl_contracts`](reference/additional/dataset-loaders#load_nfl_contracts) | [`load_contracts`](https://nflreadr.nflverse.com/reference/load_contracts.html) |
| [`load_nfl_depth_charts`](reference/loaders/other#load_nfl_depth_charts) | [`load_depth_charts`](https://nflreadr.nflverse.com/reference/load_depth_charts.html) |
| [`load_nfl_draft_picks`](reference/additional/dataset-loaders#load_nfl_draft_picks) | [`load_draft_picks`](https://nflreadr.nflverse.com/reference/load_draft_picks.html) |
| [`load_nfl_espn_qbr`](reference/additional/dataset-loaders#load_nfl_espn_qbr) | [`load_espn_qbr`](https://nflreadr.nflverse.com/reference/load_espn_qbr.html) |
| [`load_nfl_ff_opportunity`](reference/additional/dataset-loaders-2#load_nfl_ff_opportunity) | [`load_ff_opportunity`](https://nflreadr.nflverse.com/reference/load_ff_opportunity.html) |
| [`load_nfl_ff_playerids`](reference/additional/dataset-loaders-2#load_nfl_ff_playerids) | [`load_ff_playerids`](https://nflreadr.nflverse.com/reference/load_ff_playerids.html) |
| [`load_nfl_ff_rankings`](reference/additional/dataset-loaders-2#load_nfl_ff_rankings) | [`load_ff_rankings`](https://nflreadr.nflverse.com/reference/load_ff_rankings.html) |
| [`load_nfl_ftn_charting`](reference/loaders/other#load_nfl_ftn_charting) | [`load_ftn_charting`](https://nflreadr.nflverse.com/reference/load_ftn_charting.html) |
| [`load_nfl_injuries`](reference/loaders/other#load_nfl_injuries) | [`load_injuries`](https://nflreadr.nflverse.com/reference/load_injuries.html) |
| [`load_nfl_nextgen_stats`](reference/additional/dataset-loaders-2#load_nfl_nextgen_stats) | [`load_nextgen_stats`](https://nflreadr.nflverse.com/reference/load_nextgen_stats.html) |
| [`load_nfl_officials`](reference/additional/dataset-loaders-2#load_nfl_officials) | [`load_officials`](https://nflreadr.nflverse.com/reference/load_officials.html) |
| [`load_nfl_pbp`](reference/loaders/pbp#load_nfl_pbp) | [`load_pbp`](https://nflreadr.nflverse.com/reference/load_pbp.html) |
| [`load_nfl_pbp_participation`](reference/loaders/pbp#load_nfl_pbp_participation) | [`load_participation`](https://nflreadr.nflverse.com/reference/load_participation.html) |
| [`load_nfl_pfr_advstats`](reference/additional/dataset-loaders-2#load_nfl_pfr_advstats) | [`load_pfr_advstats`](https://nflreadr.nflverse.com/reference/load_pfr_advstats.html) |
| [`load_nfl_player_stats`](reference/additional/dataset-loaders-3#load_nfl_player_stats) | [`load_player_stats`](https://nflreadr.nflverse.com/reference/load_player_stats.html) |
| [`load_nfl_players`](reference/additional/dataset-loaders-3#load_nfl_players) | [`load_players`](https://nflreadr.nflverse.com/reference/load_players.html) |
| [`load_nfl_rosters`](reference/loaders/other#load_nfl_rosters) | [`load_rosters`](https://nflreadr.nflverse.com/reference/load_rosters.html) |
| [`load_nfl_schedule`](reference/additional/dataset-loaders-3#load_nfl_schedule) | [`load_schedules`](https://nflreadr.nflverse.com/reference/load_schedules.html) |
| [`load_nfl_snap_counts`](reference/loaders/other#load_nfl_snap_counts) | [`load_snap_counts`](https://nflreadr.nflverse.com/reference/load_snap_counts.html) |
| [`load_nfl_team_stats`](reference/additional/dataset-loaders-3#load_nfl_team_stats) | [`load_team_stats`](https://nflreadr.nflverse.com/reference/load_team_stats.html) |
| [`load_nfl_teams`](reference/additional/dataset-loaders-3#load_nfl_teams) | [`load_teams`](https://nflreadr.nflverse.com/reference/load_teams.html) |
| [`load_nfl_trades`](reference/additional/dataset-loaders-3#load_nfl_trades) | [`load_trades`](https://nflreadr.nflverse.com/reference/load_trades.html) |
| [`load_nfl_weekly_rosters`](reference/loaders/other#load_nfl_weekly_rosters) | [`load_rosters_weekly`](https://nflreadr.nflverse.com/reference/load_rosters_weekly.html) |
| [`load_officials`](reference/additional/dataset-loaders-3#load_officials) | [`load_officials`](https://nflreadr.nflverse.com/reference/load_officials.html) |
| [`load_participation`](reference/additional/dataset-loaders-3#load_participation) | [`load_participation`](https://nflreadr.nflverse.com/reference/load_participation.html) |
| [`load_pfr_advstats`](reference/additional/dataset-loaders-3#load_pfr_advstats) | [`load_pfr_advstats`](https://nflreadr.nflverse.com/reference/load_pfr_advstats.html) |
| [`load_player_stats`](reference/additional/dataset-loaders-3#load_player_stats) | [`load_player_stats`](https://nflreadr.nflverse.com/reference/load_player_stats.html) |
| [`load_players`](reference/additional/dataset-loaders-4#load_players) | [`load_players`](https://nflreadr.nflverse.com/reference/load_players.html) |
| [`load_rosters_weekly`](reference/additional/dataset-loaders-4#load_rosters_weekly) | [`load_rosters_weekly`](https://nflreadr.nflverse.com/reference/load_rosters_weekly.html) |
| [`load_schedules`](reference/additional/dataset-loaders-4#load_schedules) | [`load_schedules`](https://nflreadr.nflverse.com/reference/load_schedules.html) |
| [`load_snap_counts`](reference/additional/dataset-loaders-4#load_snap_counts) | [`load_snap_counts`](https://nflreadr.nflverse.com/reference/load_snap_counts.html) |
| [`load_team_stats`](reference/additional/dataset-loaders-4#load_team_stats) | [`load_team_stats`](https://nflreadr.nflverse.com/reference/load_team_stats.html) |
| [`load_teams`](reference/additional/dataset-loaders-4#load_teams) | [`load_teams`](https://nflreadr.nflverse.com/reference/load_teams.html) |
| [`load_trades`](reference/additional/dataset-loaders-4#load_trades) | [`load_trades`](https://nflreadr.nflverse.com/reference/load_trades.html) |
