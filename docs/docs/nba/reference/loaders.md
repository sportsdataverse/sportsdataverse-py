---
title: NBA dataset loaders
sidebar_label: Loaders
description: "NBA dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets."
sidebar_position: 1
toc_max_heading_level: 2
---
# NBA dataset loaders

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_nba_pbp` | [espn_nba_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_pbp) | — |
| `load_nba_player_boxscore` | [espn_nba_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_boxscores) | — |
| `load_nba_schedule` | [espn_nba_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_schedules) | — |
| `load_nba_team_boxscore` | [espn_nba_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_boxscores) | — |
| `load_nba_game_rosters` | [espn_nba_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_game_rosters) | — |
| `load_nba_officials` | [espn_nba_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_officials) | — |
| `load_nba_shots` | [espn_nba_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_shots) | — |
| `load_nba_standings` | [espn_nba_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_standings) | — |
| `load_nba_player_season_stats` | [espn_nba_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_season_stats) | — |
| `load_nba_team_season_stats` | [espn_nba_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_season_stats) | — |
| `load_nba_draft` | [espn_nba_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_draft) | — |
| `load_nba_rosters` | [espn_nba_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_rosters) | — |
| `load_nba_stats_schedules` | [nba_stats_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_schedules) | — |
| `load_nba_stats_coaches` | [nba_stats_coaches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_coaches) | — |
| `load_nba_stats_game_rosters` | [nba_stats_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_rosters) | — |
| `load_nba_stats_lineups` | [nba_stats_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_lineups) | — |
| `load_nba_stats_lineups_v3` | [nba_stats_game_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_lineups) | — |
| `load_nba_stats_officials` | [nba_stats_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_officials) | — |
| `load_nba_stats_pbp` | [nba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_pbp) | — |
| `load_nba_stats_possessions` | [nba_stats_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_possessions) | — |
| `load_nba_stats_game_lineups` | [nba_stats_game_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_lineups) | — |
| `load_nba_stats_game_matchups` | [nba_stats_game_matchups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_matchups) | — |
| `load_nba_stats_pbp_v3` | [nba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_pbp) | — |
| `load_nba_stats_player_boxscores` | [nba_stats_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_player_boxscores) | — |
| `load_nba_stats_player_game_logs` | [nba_stats_player_game_logs](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_player_game_logs) | — |
| `load_nba_stats_player_season_stats` | [nba_stats_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_player_season_stats) | — |
| `load_nba_stats_possessions_v3` | [nba_stats_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_possessions) | — |
| `load_nba_stats_rosters` | [nba_stats_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_rosters) | — |
| `load_nba_stats_shots` | [nba_stats_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_shots) | — |
| `load_nba_stats_standings` | [nba_stats_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_standings) | — |
| `load_nba_stats_team_boxscores` | [nba_stats_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_team_boxscores) | — |
| `load_nba_stats_team_season_stats` | [nba_stats_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_team_season_stats) | — |
| `load_nba_player_crosswalk` | [nba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) | — |
| `load_nba_schedule_crosswalk` | [nba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) | — |
| `load_nba_team_crosswalk` | [nba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) | — |
| `load_nba_player_core` | [espn_nba_player_core](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_core) | — |
| `load_nba_player_impact` | [nba_player_impact](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_player_impact) | — |
| `load_nba_groups` | [nba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_groups) | — |
| `load_nba_group_seasons` | [nba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_groups) | — |
| `load_nba_group_aliases` | [nba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_groups) | — |
| `load_nba_team_group_seasons` | [nba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_groups) | — |

## Player

| Function | Summary |
|---|---|
| [load_nba_player_boxscore](loaders/player.md#load_nba_player_boxscore) | Release: espn_nba_player_boxscores · asset … |
| [load_nba_player_season_stats](loaders/player.md#load_nba_player_season_stats) | Release: espn_nba_player_season_stats · asset … |
| [load_nba_player_crosswalk](loaders/player.md#load_nba_player_crosswalk) | Release: nba_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_crosswalk/nba_player_crosswalk_{season}.parquet` |
| [load_nba_player_core](loaders/player.md#load_nba_player_core) | Release: espn_nba_player_core · asset … |
| [load_nba_player_impact](loaders/player.md#load_nba_player_impact) | Release: nba_player_impact · asset … |

## Stats

| Function | Summary |
|---|---|
| [load_nba_stats_schedules](loaders/stats.md#load_nba_stats_schedules) | Release: nba_stats_schedules · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_schedules/nba_schedule_{season + …` |
| [load_nba_stats_coaches](loaders/stats.md#load_nba_stats_coaches) | Release: nba_stats_coaches · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_coaches/coaches_{season + 1}.parquet` |
| [load_nba_stats_game_rosters](loaders/stats.md#load_nba_stats_game_rosters) | Release: nba_stats_game_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_game_rosters/game_rosters_{season …` |
| [load_nba_stats_lineups](loaders/stats.md#load_nba_stats_lineups) | Release: nba_stats_lineups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_lineups/lineups_{season + 1}.parquet` |
| [load_nba_stats_lineups_v3](loaders/stats.md#load_nba_stats_lineups_v3) | Release: nba_stats_game_lineups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_game_lineups/nba_lineups_{season + …` |
| [load_nba_stats_officials](loaders/stats.md#load_nba_stats_officials) | Release: nba_stats_officials · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_officials/officials_{season + …` |
| [load_nba_stats_pbp](loaders/stats.md#load_nba_stats_pbp) | Release: nba_stats_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_pbp/nba_play_by_play_{season + 1}.parquet` |
| [load_nba_stats_possessions](loaders/stats.md#load_nba_stats_possessions) | Release: nba_stats_possessions · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_possessions/nba_possessions_{season …` |
| [load_nba_stats_game_lineups](loaders/stats.md#load_nba_stats_game_lineups) | Release: nba_stats_game_lineups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_game_lineups/nba_lineups_{season + …` |
| [load_nba_stats_game_matchups](loaders/stats.md#load_nba_stats_game_matchups) | Release: nba_stats_game_matchups · asset … |
| [load_nba_stats_pbp_v3](loaders/stats.md#load_nba_stats_pbp_v3) | Release: nba_stats_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_pbp/nba_play_by_play_{season + 1}.parquet` |
| [load_nba_stats_player_boxscores](loaders/stats.md#load_nba_stats_player_boxscores) | Release: nba_stats_player_boxscores · asset … |
| [load_nba_stats_player_game_logs](loaders/stats.md#load_nba_stats_player_game_logs) | Release: nba_stats_player_game_logs · asset … |
| [load_nba_stats_player_season_stats](loaders/stats.md#load_nba_stats_player_season_stats) | Release: nba_stats_player_season_stats · asset … |
| [load_nba_stats_possessions_v3](loaders/stats-2.md#load_nba_stats_possessions_v3) | Release: nba_stats_possessions · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_possessions/nba_possessions_{season …` |
| [load_nba_stats_rosters](loaders/stats-2.md#load_nba_stats_rosters) | Release: nba_stats_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_rosters/rosters_{season + 1}.parquet` |
| [load_nba_stats_shots](loaders/stats-2.md#load_nba_stats_shots) | Release: nba_stats_shots · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_shots/shots_{season + 1}.parquet` |
| [load_nba_stats_standings](loaders/stats-2.md#load_nba_stats_standings) | Release: nba_stats_standings · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_standings/standings_{season + …` |
| [load_nba_stats_team_boxscores](loaders/stats-2.md#load_nba_stats_team_boxscores) | Release: nba_stats_team_boxscores · asset … |
| [load_nba_stats_team_season_stats](loaders/stats-2.md#load_nba_stats_team_season_stats) | Release: nba_stats_team_season_stats · asset … |

## Team

| Function | Summary |
|---|---|
| [load_nba_team_boxscore](loaders/team.md#load_nba_team_boxscore) | Release: espn_nba_team_boxscores · asset … |
| [load_nba_team_season_stats](loaders/team.md#load_nba_team_season_stats) | Release: espn_nba_team_season_stats · asset … |
| [load_nba_team_crosswalk](loaders/team.md#load_nba_team_crosswalk) | Release: nba_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_crosswalk/nba_team_crosswalk_{season}.parquet` |
| [load_nba_team_group_seasons](loaders/team.md#load_nba_team_group_seasons) | Release: nba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_groups/nba_team_group_seasons_{season}.parquet` |

## Other

| Function | Summary |
|---|---|
| [load_nba_pbp](loaders/other.md#load_nba_pbp) | Release: espn_nba_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_pbp/play_by_play_{season}.parquet` |
| [load_nba_schedule](loaders/other.md#load_nba_schedule) | Release: espn_nba_schedules · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_schedules/nba_schedule_{season}.parquet` |
| [load_nba_game_rosters](loaders/other.md#load_nba_game_rosters) | Release: espn_nba_game_rosters · asset … |
| [load_nba_officials](loaders/other.md#load_nba_officials) | Release: espn_nba_officials · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_officials/officials_{season}.parquet` |
| [load_nba_shots](loaders/other.md#load_nba_shots) | Release: espn_nba_shots · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_shots/shots_{season}.parquet` |
| [load_nba_standings](loaders/other.md#load_nba_standings) | Release: espn_nba_standings · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_standings/standings_{season}.parquet` |
| [load_nba_draft](loaders/other.md#load_nba_draft) | Release: espn_nba_draft · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_draft/draft_{season}.parquet` |
| [load_nba_rosters](loaders/other.md#load_nba_rosters) | Release: espn_nba_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_rosters/rosters_{season}.parquet` |
| [load_nba_schedule_crosswalk](loaders/other.md#load_nba_schedule_crosswalk) | Release: nba_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_crosswalk/nba_schedule_crosswalk_{season}.parquet` |
| [load_nba_groups](loaders/other.md#load_nba_groups) | Release: nba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_groups/nba_groups.parquet` |
| [load_nba_group_seasons](loaders/other.md#load_nba_group_seasons) | Release: nba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_groups/nba_group_seasons.parquet` |
| [load_nba_group_aliases](loaders/other.md#load_nba_group_aliases) | Release: nba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_groups/nba_group_aliases.parquet` |
