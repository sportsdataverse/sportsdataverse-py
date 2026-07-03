---
title: NFL
sidebar_label: NFL
---
# NFL (`sportsdataverse.nfl`)

| Reference | Functions | Base URL |
|---|---:|---|
| [ESPN site API (v2)](reference/site) | 24 | `https://site.api.espn.com/apis/site/v2/sports` |
| [ESPN web API (v3)](reference/web) | 5 | `https://site.web.api.espn.com/apis/common/v3/sports` |
| [ESPN core API (v2)](reference/core) | 83 | `https://sports.core.api.espn.com/v2/sports` |
| [NFL.com API](reference/nfl_api) | 11 | `https://api.nfl.com` |
| [Dataset loaders](reference/loaders) | 8 | nflverse data releases |
| [Additional functions](reference/additional) | 108 | hand-written wrappers, loaders & helpers |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [NFL tutorial](../tutorials/03_nfl_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`nflreadr`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.nfl` (Python) | `nflreadr` (R) |
|---|---|
| [`clear_cache`](reference/additional#clear_cache) | [`clear_cache`](https://nflreadr.nflverse.com/reference/clear_cache.html) |
| [`get_current_season`](reference/additional#get_current_season) | [`get_current_season`](https://nflreadr.nflverse.com/reference/get_current_season.html) |
| [`get_current_week`](reference/additional#get_current_week) | [`get_current_week`](https://nflreadr.nflverse.com/reference/get_current_week.html) |
| [`load_combine`](reference/additional#load_combine) | [`load_combine`](https://nflreadr.nflverse.com/reference/load_combine.html) |
| [`load_contracts`](reference/additional#load_contracts) | [`load_contracts`](https://nflreadr.nflverse.com/reference/load_contracts.html) |
| [`load_depth_charts`](reference/additional#load_depth_charts) | [`load_depth_charts`](https://nflreadr.nflverse.com/reference/load_depth_charts.html) |
| [`load_draft_picks`](reference/additional#load_draft_picks) | [`load_draft_picks`](https://nflreadr.nflverse.com/reference/load_draft_picks.html) |
| [`load_espn_qbr`](reference/additional#load_espn_qbr) | [`load_espn_qbr`](https://nflreadr.nflverse.com/reference/load_espn_qbr.html) |
| [`load_ff_opportunity`](reference/additional#load_ff_opportunity) | [`load_ff_opportunity`](https://nflreadr.nflverse.com/reference/load_ff_opportunity.html) |
| [`load_ff_playerids`](reference/additional#load_ff_playerids) | [`load_ff_playerids`](https://nflreadr.nflverse.com/reference/load_ff_playerids.html) |
| [`load_ff_rankings`](reference/additional#load_ff_rankings) | [`load_ff_rankings`](https://nflreadr.nflverse.com/reference/load_ff_rankings.html) |
| [`load_ftn_charting`](reference/additional#load_ftn_charting) | [`load_ftn_charting`](https://nflreadr.nflverse.com/reference/load_ftn_charting.html) |
| [`load_injuries`](reference/additional#load_injuries) | [`load_injuries`](https://nflreadr.nflverse.com/reference/load_injuries.html) |
| [`load_nextgen_stats`](reference/additional#load_nextgen_stats) | [`load_nextgen_stats`](https://nflreadr.nflverse.com/reference/load_nextgen_stats.html) |
| [`load_nfl_combine`](reference/additional#load_nfl_combine) | [`load_combine`](https://nflreadr.nflverse.com/reference/load_combine.html) |
| [`load_nfl_contracts`](reference/additional#load_nfl_contracts) | [`load_contracts`](https://nflreadr.nflverse.com/reference/load_contracts.html) |
| [`load_nfl_depth_charts`](reference/loaders#load_nfl_depth_charts) | [`load_depth_charts`](https://nflreadr.nflverse.com/reference/load_depth_charts.html) |
| [`load_nfl_draft_picks`](reference/additional#load_nfl_draft_picks) | [`load_draft_picks`](https://nflreadr.nflverse.com/reference/load_draft_picks.html) |
| [`load_nfl_espn_qbr`](reference/additional#load_nfl_espn_qbr) | [`load_espn_qbr`](https://nflreadr.nflverse.com/reference/load_espn_qbr.html) |
| [`load_nfl_ff_opportunity`](reference/additional#load_nfl_ff_opportunity) | [`load_ff_opportunity`](https://nflreadr.nflverse.com/reference/load_ff_opportunity.html) |
| [`load_nfl_ff_playerids`](reference/additional#load_nfl_ff_playerids) | [`load_ff_playerids`](https://nflreadr.nflverse.com/reference/load_ff_playerids.html) |
| [`load_nfl_ff_rankings`](reference/additional#load_nfl_ff_rankings) | [`load_ff_rankings`](https://nflreadr.nflverse.com/reference/load_ff_rankings.html) |
| [`load_nfl_ftn_charting`](reference/loaders#load_nfl_ftn_charting) | [`load_ftn_charting`](https://nflreadr.nflverse.com/reference/load_ftn_charting.html) |
| [`load_nfl_injuries`](reference/loaders#load_nfl_injuries) | [`load_injuries`](https://nflreadr.nflverse.com/reference/load_injuries.html) |
| [`load_nfl_nextgen_stats`](reference/additional#load_nfl_nextgen_stats) | [`load_nextgen_stats`](https://nflreadr.nflverse.com/reference/load_nextgen_stats.html) |
| [`load_nfl_officials`](reference/additional#load_nfl_officials) | [`load_officials`](https://nflreadr.nflverse.com/reference/load_officials.html) |
| [`load_nfl_pbp`](reference/loaders#load_nfl_pbp) | [`load_pbp`](https://nflreadr.nflverse.com/reference/load_pbp.html) |
| [`load_nfl_pbp_participation`](reference/loaders#load_nfl_pbp_participation) | [`load_participation`](https://nflreadr.nflverse.com/reference/load_participation.html) |
| [`load_nfl_pfr_advstats`](reference/additional#load_nfl_pfr_advstats) | [`load_pfr_advstats`](https://nflreadr.nflverse.com/reference/load_pfr_advstats.html) |
| [`load_nfl_player_stats`](reference/additional#load_nfl_player_stats) | [`load_player_stats`](https://nflreadr.nflverse.com/reference/load_player_stats.html) |
| [`load_nfl_players`](reference/additional#load_nfl_players) | [`load_players`](https://nflreadr.nflverse.com/reference/load_players.html) |
| [`load_nfl_rosters`](reference/loaders#load_nfl_rosters) | [`load_rosters`](https://nflreadr.nflverse.com/reference/load_rosters.html) |
| [`load_nfl_schedule`](reference/additional#load_nfl_schedule) | [`load_schedules`](https://nflreadr.nflverse.com/reference/load_schedules.html) |
| [`load_nfl_snap_counts`](reference/loaders#load_nfl_snap_counts) | [`load_snap_counts`](https://nflreadr.nflverse.com/reference/load_snap_counts.html) |
| [`load_nfl_team_stats`](reference/additional#load_nfl_team_stats) | [`load_team_stats`](https://nflreadr.nflverse.com/reference/load_team_stats.html) |
| [`load_nfl_teams`](reference/additional#load_nfl_teams) | [`load_teams`](https://nflreadr.nflverse.com/reference/load_teams.html) |
| [`load_nfl_trades`](reference/additional#load_nfl_trades) | [`load_trades`](https://nflreadr.nflverse.com/reference/load_trades.html) |
| [`load_nfl_weekly_rosters`](reference/loaders#load_nfl_weekly_rosters) | [`load_rosters_weekly`](https://nflreadr.nflverse.com/reference/load_rosters_weekly.html) |
| [`load_officials`](reference/additional#load_officials) | [`load_officials`](https://nflreadr.nflverse.com/reference/load_officials.html) |
| [`load_participation`](reference/additional#load_participation) | [`load_participation`](https://nflreadr.nflverse.com/reference/load_participation.html) |
| [`load_pbp`](reference/additional#load_pbp) | [`load_pbp`](https://nflreadr.nflverse.com/reference/load_pbp.html) |
| [`load_pfr_advstats`](reference/additional#load_pfr_advstats) | [`load_pfr_advstats`](https://nflreadr.nflverse.com/reference/load_pfr_advstats.html) |
| [`load_player_stats`](reference/additional#load_player_stats) | [`load_player_stats`](https://nflreadr.nflverse.com/reference/load_player_stats.html) |
| [`load_players`](reference/additional#load_players) | [`load_players`](https://nflreadr.nflverse.com/reference/load_players.html) |
| [`load_rosters`](reference/additional#load_rosters) | [`load_rosters`](https://nflreadr.nflverse.com/reference/load_rosters.html) |
| [`load_rosters_weekly`](reference/additional#load_rosters_weekly) | [`load_rosters_weekly`](https://nflreadr.nflverse.com/reference/load_rosters_weekly.html) |
| [`load_schedules`](reference/additional#load_schedules) | [`load_schedules`](https://nflreadr.nflverse.com/reference/load_schedules.html) |
| [`load_snap_counts`](reference/additional#load_snap_counts) | [`load_snap_counts`](https://nflreadr.nflverse.com/reference/load_snap_counts.html) |
| [`load_team_stats`](reference/additional#load_team_stats) | [`load_team_stats`](https://nflreadr.nflverse.com/reference/load_team_stats.html) |
| [`load_teams`](reference/additional#load_teams) | [`load_teams`](https://nflreadr.nflverse.com/reference/load_teams.html) |
| [`load_trades`](reference/additional#load_trades) | [`load_trades`](https://nflreadr.nflverse.com/reference/load_trades.html) |
