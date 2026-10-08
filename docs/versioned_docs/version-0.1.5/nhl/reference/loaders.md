---
title: NHL dataset loaders
sidebar_label: Loaders
description: "NHL dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets."
sidebar_position: 1
toc_max_heading_level: 2
---
# NHL dataset loaders

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_nhl_pbp` | [nhl_pbp_full](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_full) | — |
| `load_nhl_player_boxscore` | [nhl_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_player_boxscores) | — |
| `load_nhl_schedule` | [nhl_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_schedules) | — |
| `load_nhl_team_boxscore` | [nhl_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_team_boxscores) | — |
| `load_nhl_game_info` | [nhl_game_info](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_game_info) | — |
| `load_nhl_game_rosters` | [nhl_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_game_rosters) | — |
| `load_nhl_goalie_boxscores` | [nhl_goalie_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_goalie_boxscores) | — |
| `load_nhl_linescore` | [nhl_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_linescore) | — |
| `load_nhl_officials` | [nhl_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_officials) | — |
| `load_nhl_pbp_full` | [nhl_pbp_full](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_full) | — |
| `load_nhl_pbp_lite` | [nhl_pbp_lite](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_lite) | — |
| `load_nhl_penalties` | [nhl_penalties](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_penalties) | — |
| `load_nhl_player_boxscores` | [nhl_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_player_boxscores) | — |
| `load_nhl_rosters` | [nhl_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_rosters) | — |
| `load_nhl_schedules` | [nhl_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_schedules) | — |
| `load_nhl_scoring` | [nhl_scoring](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_scoring) | — |
| `load_nhl_scratches` | [nhl_scratches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_scratches) | — |
| `load_nhl_shifts` | [nhl_shifts](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shifts) | — |
| `load_nhl_shootout` | [nhl_shootout](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shootout) | — |
| `load_nhl_shots_by_period` | [nhl_shots_by_period](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shots_by_period) | — |
| `load_nhl_skater_boxscores` | [nhl_skater_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_skater_boxscores) | — |
| `load_nhl_team_boxscores` | [nhl_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_team_boxscores) | — |
| `load_nhl_three_stars` | [nhl_three_stars](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_three_stars) | — |
| `load_nhl_groups` | [nhl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_groups) | — |
| `load_nhl_group_seasons` | [nhl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_groups) | — |
| `load_nhl_group_aliases` | [nhl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_groups) | — |
| `load_nhl_team_group_seasons` | [nhl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_groups) | — |

## Play-by-play

| Function | Summary |
|---|---|
| [load_nhl_pbp](loaders/pbp.md#load_nhl_pbp) | Release: nhl_pbp_full · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_full/play_by_play_{season}.parquet` |
| [load_nhl_pbp_full](loaders/pbp.md#load_nhl_pbp_full) | Release: nhl_pbp_full · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_full/play_by_play_{season}.parquet` |
| [load_nhl_pbp_lite](loaders/pbp.md#load_nhl_pbp_lite) | Release: nhl_pbp_lite · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_lite/play_by_play_{season}_lite.parquet` |

## Other

| Function | Summary |
|---|---|
| [load_nhl_player_boxscore](loaders/other.md#load_nhl_player_boxscore) | Release: nhl_player_boxscores · asset … |
| [load_nhl_schedule](loaders/other.md#load_nhl_schedule) | Release: nhl_schedules · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_schedules/nhl_schedule_{season}.parquet` |
| [load_nhl_team_boxscore](loaders/other.md#load_nhl_team_boxscore) | Release: nhl_team_boxscores · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_team_boxscores/team_box_{season}.parquet` |
| [load_nhl_game_info](loaders/other.md#load_nhl_game_info) | Release: nhl_game_info · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_game_info/game_info_{season}.parquet` |
| [load_nhl_game_rosters](loaders/other.md#load_nhl_game_rosters) | Release: nhl_game_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_game_rosters/game_rosters_{season}.parquet` |
| [load_nhl_goalie_boxscores](loaders/other.md#load_nhl_goalie_boxscores) | Release: nhl_goalie_boxscores · asset … |
| [load_nhl_linescore](loaders/other.md#load_nhl_linescore) | Release: nhl_linescore · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_linescore/linescore_{season}.parquet` |
| [load_nhl_officials](loaders/other.md#load_nhl_officials) | Release: nhl_officials · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_officials/officials_{season}.parquet` |
| [load_nhl_penalties](loaders/other.md#load_nhl_penalties) | Release: nhl_penalties · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_penalties/penalties_{season}.parquet` |
| [load_nhl_player_boxscores](loaders/other.md#load_nhl_player_boxscores) | Release: nhl_player_boxscores · asset … |
| [load_nhl_rosters](loaders/other.md#load_nhl_rosters) | Release: nhl_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_rosters/rosters_{season}.parquet` |
| [load_nhl_schedules](loaders/other.md#load_nhl_schedules) | Release: nhl_schedules · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_schedules/nhl_schedule_{season}.parquet` |
| [load_nhl_scoring](loaders/other.md#load_nhl_scoring) | Release: nhl_scoring · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_scoring/scoring_{season}.parquet` |
| [load_nhl_scratches](loaders/other.md#load_nhl_scratches) | Release: nhl_scratches · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_scratches/scratches_{season}.parquet` |
| [load_nhl_shifts](loaders/other.md#load_nhl_shifts) | Release: nhl_shifts · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_shifts/shifts_{season}.parquet` |
| [load_nhl_shootout](loaders/other.md#load_nhl_shootout) | Release: nhl_shootout · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_shootout/shootout_summary_{season}.parquet` |
| [load_nhl_shots_by_period](loaders/other.md#load_nhl_shots_by_period) | Release: nhl_shots_by_period · asset … |
| [load_nhl_skater_boxscores](loaders/other.md#load_nhl_skater_boxscores) | Release: nhl_skater_boxscores · asset … |
| [load_nhl_team_boxscores](loaders/other.md#load_nhl_team_boxscores) | Release: nhl_team_boxscores · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_team_boxscores/team_box_{season}.parquet` |
| [load_nhl_three_stars](loaders/other.md#load_nhl_three_stars) | Release: nhl_three_stars · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_three_stars/three_stars_{season}.parquet` |
| [load_nhl_groups](loaders/other.md#load_nhl_groups) | Release: nhl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_groups/nhl_groups.parquet` |
| [load_nhl_group_seasons](loaders/other.md#load_nhl_group_seasons) | Release: nhl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_groups/nhl_group_seasons.parquet` |
| [load_nhl_group_aliases](loaders/other.md#load_nhl_group_aliases) | Release: nhl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_groups/nhl_group_aliases.parquet` |
| [load_nhl_team_group_seasons](loaders/other.md#load_nhl_team_group_seasons) | Release: nhl_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_groups/nhl_team_group_seasons_{season}.parquet` |
