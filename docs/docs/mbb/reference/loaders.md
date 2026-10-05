---
title: MBB dataset loaders
sidebar_label: Loaders
description: "MBB dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets."
sidebar_position: 1
toc_max_heading_level: 2
---
# MBB dataset loaders

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_mbb_pbp` | [espn_mens_college_basketball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_pbp) | — |
| `load_mbb_player_boxscore` | [espn_mens_college_basketball_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores) | — |
| `load_mbb_schedule` | [espn_mens_college_basketball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules) | — |
| `load_mbb_team_boxscore` | [espn_mens_college_basketball_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores) | — |
| `load_mbb_ratings` | [mbb_ratings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_ratings) | — |
| `load_mbb_player_value` | [mbb_player_value](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_player_value) | — |
| `load_mbb_shots` | [espn_mens_college_basketball_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_shots) | — |
| `load_mbb_standings` | [espn_mens_college_basketball_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_standings) | — |
| `load_mbb_player_season_stats` | [espn_mens_college_basketball_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) | — |
| `load_mbb_rosters` | [espn_mens_college_basketball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters) | — |
| `load_mbb_officials` | [espn_mens_college_basketball_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials) | — |
| `load_mbb_game_rosters` | [espn_mens_college_basketball_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) | — |
| `load_mbb_team_season_stats` | [espn_mens_college_basketball_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_season_stats) | — |
| `load_mbb_player_crosswalk` | [mbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) | — |
| `load_mbb_schedule_crosswalk` | [mbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) | — |
| `load_mbb_team_crosswalk` | [mbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) | — |
| `load_mbb_player_core` | [espn_mens_college_basketball_player_core](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_core) | — |
| `load_ncaa_mbb_pbp` | [ncaa_mbb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_pbp) | — |
| `load_ncaa_mbb_schedule` | [ncaa_mbb_schedule](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_schedule) | — |
| `load_ncaa_mbb_player_box` | [ncaa_mbb_player_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_player_box) | — |
| `load_ncaa_mbb_team_box` | [ncaa_mbb_team_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_team_box) | — |
| `load_ncaa_mbb_rosters` | [ncaa_mbb_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_rosters) | — |
| `load_ncaa_mbb_team_rosters` | [ncaa_mbb_team_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_team_rosters) | — |
| `load_ncaa_mbb_team_ids` | [ncaa_mbb_team_ids](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_team_ids) | — |
| `load_ncaa_mbb_possessions` | [ncaa_mbb_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_possessions) | — |
| `load_ncaa_mbb_lineups` | [ncaa_mbb_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_lineups) | — |
| `load_ncaa_mbb_matchup_stints` | [ncaa_mbb_matchup_stints](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_matchup_stints) | — |
| `load_ncaa_mbb_shots` | [ncaa_mbb_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_shots) | — |
| `load_ncaa_mbb_rapm_within_team` | [ncaa_mbb_rapm_within_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_rapm_within_team) | — |
| `load_ncaa_mbb_rapm` | [ncaa_mbb_rapm](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mbb_rapm) | — |
| `load_mbb_groups` | [mbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_groups) | — |
| `load_mbb_group_seasons` | [mbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_groups) | — |
| `load_mbb_group_aliases` | [mbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_groups) | — |
| `load_mbb_team_group_seasons` | [mbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_groups) | — |

## NCAA (stats.ncaa.org)

| Function | Summary |
|---|---|
| [load_ncaa_mbb_pbp](loaders/ncaa.md#load_ncaa_mbb_pbp) | Release: ncaa_mbb_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mbb_pbp/ncaa_mbb_pbp_{season}.parquet` |
| [load_ncaa_mbb_schedule](loaders/ncaa.md#load_ncaa_mbb_schedule) | Release: ncaa_mbb_schedule · asset … |
| [load_ncaa_mbb_player_box](loaders/ncaa.md#load_ncaa_mbb_player_box) | Release: ncaa_mbb_player_box · asset … |
| [load_ncaa_mbb_team_box](loaders/ncaa.md#load_ncaa_mbb_team_box) | Release: ncaa_mbb_team_box · asset … |
| [load_ncaa_mbb_rosters](loaders/ncaa.md#load_ncaa_mbb_rosters) | Release: ncaa_mbb_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mbb_rosters/ncaa_mbb_rosters_{season}.parquet` |
| [load_ncaa_mbb_team_rosters](loaders/ncaa.md#load_ncaa_mbb_team_rosters) | Release: ncaa_mbb_team_rosters · asset … |
| [load_ncaa_mbb_team_ids](loaders/ncaa.md#load_ncaa_mbb_team_ids) | Release: ncaa_mbb_team_ids · asset … |
| [load_ncaa_mbb_possessions](loaders/ncaa.md#load_ncaa_mbb_possessions) | Release: ncaa_mbb_possessions · asset … |
| [load_ncaa_mbb_lineups](loaders/ncaa.md#load_ncaa_mbb_lineups) | Release: ncaa_mbb_lineups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mbb_lineups/ncaa_mbb_lineups_{season}.parquet` |
| [load_ncaa_mbb_matchup_stints](loaders/ncaa.md#load_ncaa_mbb_matchup_stints) | Release: ncaa_mbb_matchup_stints · asset … |
| [load_ncaa_mbb_shots](loaders/ncaa.md#load_ncaa_mbb_shots) | Release: ncaa_mbb_shots · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mbb_shots/ncaa_mbb_shots_{season}.parquet` |
| [load_ncaa_mbb_rapm_within_team](loaders/ncaa.md#load_ncaa_mbb_rapm_within_team) | Release: ncaa_mbb_rapm_within_team · asset … |
| [load_ncaa_mbb_rapm](loaders/ncaa.md#load_ncaa_mbb_rapm) | Release: ncaa_mbb_rapm · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mbb_rapm/ncaa_mbb_rapm_{season}.parquet` |

## Player

| Function | Summary |
|---|---|
| [load_mbb_player_boxscore](loaders/player.md#load_mbb_player_boxscore) | Release: espn_mens_college_basketball_player_boxscores · asset … |
| [load_mbb_player_value](loaders/player.md#load_mbb_player_value) | Release: mbb_player_value · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_player_value/mbb_player_value_{season}.parquet` |
| [load_mbb_player_season_stats](loaders/player.md#load_mbb_player_season_stats) | Release: espn_mens_college_basketball_player_season_stats · asset … |
| [load_mbb_player_crosswalk](loaders/player.md#load_mbb_player_crosswalk) | Release: mbb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_crosswalk/mbb_player_crosswalk_{season}.parquet` |
| [load_mbb_player_core](loaders/player.md#load_mbb_player_core) | Release: espn_mens_college_basketball_player_core · asset … |

## Team

| Function | Summary |
|---|---|
| [load_mbb_team_boxscore](loaders/team.md#load_mbb_team_boxscore) | Release: espn_mens_college_basketball_team_boxscores · asset … |
| [load_mbb_team_season_stats](loaders/team.md#load_mbb_team_season_stats) | Release: espn_mens_college_basketball_team_season_stats · asset … |
| [load_mbb_team_crosswalk](loaders/team.md#load_mbb_team_crosswalk) | Release: mbb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_crosswalk/mbb_team_crosswalk_{season}.parquet` |
| [load_mbb_team_group_seasons](loaders/team.md#load_mbb_team_group_seasons) | Release: mbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_groups/mbb_team_group_seasons_{season}.parquet` |

## Other

| Function | Summary |
|---|---|
| [load_mbb_pbp](loaders/other.md#load_mbb_pbp) | Release: espn_mens_college_basketball_pbp · asset … |
| [load_mbb_schedule](loaders/other.md#load_mbb_schedule) | Release: espn_mens_college_basketball_schedules · asset … |
| [load_mbb_ratings](loaders/other.md#load_mbb_ratings) | Release: mbb_ratings · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_ratings/mbb_ratings_{season}.parquet` |
| [load_mbb_shots](loaders/other.md#load_mbb_shots) | Release: espn_mens_college_basketball_shots · asset … |
| [load_mbb_standings](loaders/other.md#load_mbb_standings) | Release: espn_mens_college_basketball_standings · asset … |
| [load_mbb_rosters](loaders/other.md#load_mbb_rosters) | Release: espn_mens_college_basketball_rosters · asset … |
| [load_mbb_officials](loaders/other.md#load_mbb_officials) | Release: espn_mens_college_basketball_officials · asset … |
| [load_mbb_game_rosters](loaders/other.md#load_mbb_game_rosters) | Release: espn_mens_college_basketball_game_rosters · asset … |
| [load_mbb_schedule_crosswalk](loaders/other.md#load_mbb_schedule_crosswalk) | Release: mbb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_crosswalk/mbb_schedule_crosswalk_{season}.parquet` |
| [load_mbb_groups](loaders/other.md#load_mbb_groups) | Release: mbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_groups/mbb_groups.parquet` |
| [load_mbb_group_seasons](loaders/other.md#load_mbb_group_seasons) | Release: mbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_groups/mbb_group_seasons.parquet` |
| [load_mbb_group_aliases](loaders/other.md#load_mbb_group_aliases) | Release: mbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_groups/mbb_group_aliases.parquet` |
