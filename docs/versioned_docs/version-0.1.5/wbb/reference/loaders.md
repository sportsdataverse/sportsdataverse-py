---
title: WBB dataset loaders
sidebar_label: Loaders
description: "WBB dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets."
sidebar_position: 1
toc_max_heading_level: 2
---
# WBB dataset loaders

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_wbb_pbp` | [espn_womens_college_basketball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_pbp) | — |
| `load_wbb_player_boxscore` | [espn_womens_college_basketball_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_player_boxscores) | — |
| `load_wbb_schedule` | [espn_womens_college_basketball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_schedules) | — |
| `load_wbb_team_boxscore` | [espn_womens_college_basketball_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_team_boxscores) | — |
| `load_wbb_ratings` | [wbb_ratings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_ratings) | — |
| `load_wbb_player_value` | [wbb_player_value](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_player_value) | — |
| `load_wbb_game_rosters` | [espn_womens_college_basketball_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_game_rosters) | — |
| `load_wbb_officials` | [espn_womens_college_basketball_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_officials) | — |
| `load_wbb_player_season_stats` | [espn_womens_college_basketball_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_player_season_stats) | — |
| `load_wbb_rosters` | [espn_womens_college_basketball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_rosters) | — |
| `load_wbb_shots` | [espn_womens_college_basketball_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_shots) | — |
| `load_wbb_standings` | [espn_womens_college_basketball_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_standings) | — |
| `load_wbb_team_season_stats` | [espn_womens_college_basketball_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_team_season_stats) | — |
| `load_wbb_player_crosswalk` | [wbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_crosswalk) | — |
| `load_wbb_schedule_crosswalk` | [wbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_crosswalk) | — |
| `load_wbb_team_crosswalk` | [wbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_crosswalk) | — |
| `load_wbb_player_core` | [espn_womens_college_basketball_player_core](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_player_core) | — |
| `load_ncaa_wbb_rapm` | [ncaa_wbb_rapm](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_rapm) | — |
| `load_ncaa_wbb_pbp` | [ncaa_wbb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_pbp) | — |
| `load_ncaa_wbb_schedule` | [ncaa_wbb_schedule](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_schedule) | — |
| `load_ncaa_wbb_player_box` | [ncaa_wbb_player_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_player_box) | — |
| `load_ncaa_wbb_team_box` | [ncaa_wbb_team_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_team_box) | — |
| `load_ncaa_wbb_rosters` | [ncaa_wbb_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_rosters) | — |
| `load_ncaa_wbb_team_rosters` | [ncaa_wbb_team_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_team_rosters) | — |
| `load_ncaa_wbb_team_ids` | [ncaa_wbb_team_ids](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_team_ids) | — |
| `load_ncaa_wbb_possessions` | [ncaa_wbb_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_possessions) | — |
| `load_ncaa_wbb_lineups` | [ncaa_wbb_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_lineups) | — |
| `load_ncaa_wbb_matchup_stints` | [ncaa_wbb_matchup_stints](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_matchup_stints) | — |
| `load_ncaa_wbb_shots` | [ncaa_wbb_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_shots) | — |
| `load_ncaa_wbb_rapm_within_team` | [ncaa_wbb_rapm_within_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_rapm_within_team) | — |
| `load_wbb_groups` | [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) | — |
| `load_wbb_group_seasons` | [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) | — |
| `load_wbb_group_aliases` | [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) | — |
| `load_wbb_team_group_seasons` | [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) | — |

## NCAA (stats.ncaa.org)

| Function | Summary |
|---|---|
| [load_ncaa_wbb_rapm](loaders/ncaa.md#load_ncaa_wbb_rapm) | Release: ncaa_wbb_rapm · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_rapm/ncaa_wbb_rapm_{season}.parquet` |
| [load_ncaa_wbb_pbp](loaders/ncaa.md#load_ncaa_wbb_pbp) | Release: ncaa_wbb_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_pbp/ncaa_wbb_pbp_{season}.parquet` |
| [load_ncaa_wbb_schedule](loaders/ncaa.md#load_ncaa_wbb_schedule) | Release: ncaa_wbb_schedule · asset … |
| [load_ncaa_wbb_player_box](loaders/ncaa.md#load_ncaa_wbb_player_box) | Release: ncaa_wbb_player_box · asset … |
| [load_ncaa_wbb_team_box](loaders/ncaa.md#load_ncaa_wbb_team_box) | Release: ncaa_wbb_team_box · asset … |
| [load_ncaa_wbb_rosters](loaders/ncaa.md#load_ncaa_wbb_rosters) | Release: ncaa_wbb_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_rosters/ncaa_wbb_rosters_{season}.parquet` |
| [load_ncaa_wbb_team_rosters](loaders/ncaa.md#load_ncaa_wbb_team_rosters) | Release: ncaa_wbb_team_rosters · asset … |
| [load_ncaa_wbb_team_ids](loaders/ncaa.md#load_ncaa_wbb_team_ids) | Release: ncaa_wbb_team_ids · asset … |
| [load_ncaa_wbb_possessions](loaders/ncaa.md#load_ncaa_wbb_possessions) | Release: ncaa_wbb_possessions · asset … |
| [load_ncaa_wbb_lineups](loaders/ncaa.md#load_ncaa_wbb_lineups) | Release: ncaa_wbb_lineups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_lineups/ncaa_wbb_lineups_{season}.parquet` |
| [load_ncaa_wbb_matchup_stints](loaders/ncaa.md#load_ncaa_wbb_matchup_stints) | Release: ncaa_wbb_matchup_stints · asset … |
| [load_ncaa_wbb_shots](loaders/ncaa.md#load_ncaa_wbb_shots) | Release: ncaa_wbb_shots · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_shots/ncaa_wbb_shots_{season}.parquet` |
| [load_ncaa_wbb_rapm_within_team](loaders/ncaa.md#load_ncaa_wbb_rapm_within_team) | Release: ncaa_wbb_rapm_within_team · asset … |

## Player

| Function | Summary |
|---|---|
| [load_wbb_player_boxscore](loaders/player.md#load_wbb_player_boxscore) | Release: espn_womens_college_basketball_player_boxscores · asset … |
| [load_wbb_player_value](loaders/player.md#load_wbb_player_value) | Release: wbb_player_value · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_player_value/wbb_player_value_{season}.parquet` |
| [load_wbb_player_season_stats](loaders/player.md#load_wbb_player_season_stats) | Release: espn_womens_college_basketball_player_season_stats · asset … |
| [load_wbb_player_crosswalk](loaders/player.md#load_wbb_player_crosswalk) | Release: wbb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_crosswalk/wbb_player_crosswalk_{season}.parquet` |
| [load_wbb_player_core](loaders/player.md#load_wbb_player_core) | Release: espn_womens_college_basketball_player_core · asset … |

## Other

| Function | Summary |
|---|---|
| [load_wbb_pbp](loaders/other.md#load_wbb_pbp) | Release: espn_womens_college_basketball_pbp · asset … |
| [load_wbb_schedule](loaders/other.md#load_wbb_schedule) | Release: espn_womens_college_basketball_schedules · asset … |
| [load_wbb_team_boxscore](loaders/other.md#load_wbb_team_boxscore) | Release: espn_womens_college_basketball_team_boxscores · asset … |
| [load_wbb_ratings](loaders/other.md#load_wbb_ratings) | Release: wbb_ratings · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_ratings/wbb_ratings_{season}.parquet` |
| [load_wbb_game_rosters](loaders/other.md#load_wbb_game_rosters) | Release: espn_womens_college_basketball_game_rosters · asset … |
| [load_wbb_officials](loaders/other.md#load_wbb_officials) | Release: espn_womens_college_basketball_officials · asset … |
| [load_wbb_rosters](loaders/other.md#load_wbb_rosters) | Release: espn_womens_college_basketball_rosters · asset … |
| [load_wbb_shots](loaders/other.md#load_wbb_shots) | Release: espn_womens_college_basketball_shots · asset … |
| [load_wbb_standings](loaders/other.md#load_wbb_standings) | Release: espn_womens_college_basketball_standings · asset … |
| [load_wbb_team_season_stats](loaders/other.md#load_wbb_team_season_stats) | Release: espn_womens_college_basketball_team_season_stats · asset … |
| [load_wbb_schedule_crosswalk](loaders/other.md#load_wbb_schedule_crosswalk) | Release: wbb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_crosswalk/wbb_schedule_crosswalk_{season}.parquet` |
| [load_wbb_team_crosswalk](loaders/other.md#load_wbb_team_crosswalk) | Release: wbb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_crosswalk/wbb_team_crosswalk_{season}.parquet` |
| [load_wbb_groups](loaders/other.md#load_wbb_groups) | Release: wbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_groups.parquet` |
| [load_wbb_group_seasons](loaders/other.md#load_wbb_group_seasons) | Release: wbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_group_seasons.parquet` |
| [load_wbb_group_aliases](loaders/other.md#load_wbb_group_aliases) | Release: wbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_group_aliases.parquet` |
| [load_wbb_team_group_seasons](loaders/other.md#load_wbb_team_group_seasons) | Release: wbb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_team_group_seasons_{season}.parquet` |
