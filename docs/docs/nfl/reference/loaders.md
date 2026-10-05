---
title: NFL dataset loaders
sidebar_label: Loaders
description: "NFL dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets."
sidebar_position: 1
toc_max_heading_level: 2
---
# NFL dataset loaders

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_nfl_pbp` | [pbp](https://github.com/nflverse/nflverse-data/releases/tag/pbp) | — |
| `load_nfl_model_pbp` | [nfl_model_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_model_pbp) | — |
| `load_nfl_ratings_weekly` | [nfl_ratings_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_ratings_weekly) | — |
| `load_nfl_ngs` | [nfl_ngs_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_ngs_passing) | — |
| `load_nfl_rosters` | [rosters](https://github.com/nflverse/nflverse-data/releases/tag/rosters) | — |
| `load_nfl_weekly_rosters` | [weekly_rosters](https://github.com/nflverse/nflverse-data/releases/tag/weekly_rosters) | — |
| `load_nfl_depth_charts` | [depth_charts](https://github.com/nflverse/nflverse-data/releases/tag/depth_charts) | — |
| `load_nfl_injuries` | [injuries](https://github.com/nflverse/nflverse-data/releases/tag/injuries) | — |
| `load_nfl_snap_counts` | [snap_counts](https://github.com/nflverse/nflverse-data/releases/tag/snap_counts) | — |
| `load_nfl_pbp_participation` | [pbp_participation](https://github.com/nflverse/nflverse-data/releases/tag/pbp_participation) | — |
| `load_nfl_ftn_charting` | [ftn_charting](https://github.com/nflverse/nflverse-data/releases/tag/ftn_charting) | — |
| `load_nfl_usage_players` | [espn_nfl_usage_players](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_players) | — |
| `load_nfl_usage_position_groups` | [espn_nfl_usage_position_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_position_groups) | — |
| `load_nfl_usage_tackles` | [espn_nfl_usage_tackles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_tackles) | — |
| `load_nfl_usage_position_group_tackles` | [espn_nfl_usage_position_group_tackles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_position_group_tackles) | — |
| `load_nfl_usage_teams` | [espn_nfl_usage_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_teams) | — |
| `load_nfl_usage_drive_scripting` | [espn_nfl_usage_drive_scripting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_drive_scripting) | — |
| `load_nfl_usage_st_kickers` | [espn_nfl_usage_st_kickers](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_kickers) | — |
| `load_nfl_usage_st_punters` | [espn_nfl_usage_st_punters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_punters) | — |
| `load_nfl_usage_st_returners` | [espn_nfl_usage_st_returners](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_returners) | — |
| `load_nfl_usage_st_blocks` | [espn_nfl_usage_st_blocks](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_blocks) | — |
| `load_nfl_usage_st_team` | [espn_nfl_usage_st_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_team) | — |
| `load_nfl_team_tendencies` | [espn_nfl_team_tendencies](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_team_tendencies) | — |
| `load_nfl_coach_tendencies` | [espn_nfl_coach_tendencies](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_coach_tendencies) | — |
| `load_nfl_coach_careers` | [espn_nfl_coach_careers](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_coach_careers) | — |
| `load_nfl_groups` | [nfl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_groups) | — |
| `load_nfl_group_seasons` | [nfl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_groups) | — |
| `load_nfl_group_aliases` | [nfl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_groups) | — |
| `load_nfl_team_group_seasons` | [nfl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_groups) | — |

## Coach

| Function | Summary |
|---|---|
| [load_nfl_coach_tendencies](loaders/coach.md#load_nfl_coach_tendencies) | Release: espn_nfl_coach_tendencies · asset … |
| [load_nfl_coach_careers](loaders/coach-2.md#load_nfl_coach_careers) | Release: espn_nfl_coach_careers · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_coach_careers/coach_careers.parquet` |

## Play-by-play

| Function | Summary |
|---|---|
| [load_nfl_pbp](loaders/pbp.md#load_nfl_pbp) | Release: pbp · asset `https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{season}.parquet` |
| [load_nfl_pbp_participation](loaders/pbp.md#load_nfl_pbp_participation) | Release: pbp_participation · asset `https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{season}.parquet` |

## Team

| Function | Summary |
|---|---|
| [load_nfl_team_tendencies](loaders/team.md#load_nfl_team_tendencies) | Release: espn_nfl_team_tendencies · asset … |
| [load_nfl_team_group_seasons](loaders/team-2.md#load_nfl_team_group_seasons) | Release: nfl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_groups/nfl_team_group_seasons_{season}.parquet` |

## Usage

| Function | Summary |
|---|---|
| [load_nfl_usage_players](loaders/usage.md#load_nfl_usage_players) | Release: espn_nfl_usage_players · asset … |
| [load_nfl_usage_position_groups](loaders/usage.md#load_nfl_usage_position_groups) | Release: espn_nfl_usage_position_groups · asset … |
| [load_nfl_usage_tackles](loaders/usage.md#load_nfl_usage_tackles) | Release: espn_nfl_usage_tackles · asset … |
| [load_nfl_usage_position_group_tackles](loaders/usage.md#load_nfl_usage_position_group_tackles) | Release: espn_nfl_usage_position_group_tackles · asset … |
| [load_nfl_usage_teams](loaders/usage.md#load_nfl_usage_teams) | Release: espn_nfl_usage_teams · asset … |
| [load_nfl_usage_drive_scripting](loaders/usage.md#load_nfl_usage_drive_scripting) | Release: espn_nfl_usage_drive_scripting · asset … |
| [load_nfl_usage_st_kickers](loaders/usage.md#load_nfl_usage_st_kickers) | Release: espn_nfl_usage_st_kickers · asset … |
| [load_nfl_usage_st_punters](loaders/usage.md#load_nfl_usage_st_punters) | Release: espn_nfl_usage_st_punters · asset … |
| [load_nfl_usage_st_returners](loaders/usage.md#load_nfl_usage_st_returners) | Release: espn_nfl_usage_st_returners · asset … |
| [load_nfl_usage_st_blocks](loaders/usage.md#load_nfl_usage_st_blocks) | Release: espn_nfl_usage_st_blocks · asset … |
| [load_nfl_usage_st_team](loaders/usage.md#load_nfl_usage_st_team) | Release: espn_nfl_usage_st_team · asset … |

## Other

| Function | Summary |
|---|---|
| [load_nfl_model_pbp](loaders/other.md#load_nfl_model_pbp) | Release: nfl_model_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_model_pbp/model_pbp_{season}.parquet` |
| [load_nfl_ratings_weekly](loaders/other.md#load_nfl_ratings_weekly) | Release: nfl_ratings_weekly · asset … |
| [load_nfl_ngs](loaders/other.md#load_nfl_ngs) | Release: nfl_ngs_passing · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_ngs_passing/ngs_passing_{season}.parquet` |
| [load_nfl_rosters](loaders/other.md#load_nfl_rosters) | Release: rosters · asset `https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{season}.parquet` |
| [load_nfl_weekly_rosters](loaders/other.md#load_nfl_weekly_rosters) | Release: weekly_rosters · asset `https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{season}.parquet` |
| [load_nfl_depth_charts](loaders/other.md#load_nfl_depth_charts) | Release: depth_charts · asset `https://github.com/nflverse/nflverse-data/releases/download/depth_charts/depth_charts_{season}.parquet` |
| [load_nfl_injuries](loaders/other.md#load_nfl_injuries) | Release: injuries · asset `https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{season}.parquet` |
| [load_nfl_snap_counts](loaders/other.md#load_nfl_snap_counts) | Release: snap_counts · asset `https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_{season}.parquet` |
| [load_nfl_ftn_charting](loaders/other.md#load_nfl_ftn_charting) | Release: ftn_charting · asset `https://github.com/nflverse/nflverse-data/releases/download/ftn_charting/ftn_charting_{season}.parquet` |
| [load_nfl_groups](loaders/other.md#load_nfl_groups) | Release: nfl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_groups/nfl_groups.parquet` |
| [load_nfl_group_seasons](loaders/other.md#load_nfl_group_seasons) | Release: nfl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_groups/nfl_group_seasons.parquet` |
| [load_nfl_group_aliases](loaders/other.md#load_nfl_group_aliases) | Release: nfl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_groups/nfl_group_aliases.parquet` |
