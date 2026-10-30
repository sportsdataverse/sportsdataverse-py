# WNBA dataset loaders

> WNBA dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets.

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_wnba_pbp` | [espn_wnba_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_pbp) | — |
| `load_wnba_player_boxscore` | [espn_wnba_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_boxscores) | — |
| `load_wnba_schedule` | [espn_wnba_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_schedules) | — |
| `load_wnba_team_boxscore` | [espn_wnba_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_boxscores) | — |
| `load_wnba_draft` | [espn_wnba_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_draft) | — |
| `load_wnba_game_rosters` | [espn_wnba_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_game_rosters) | — |
| `load_wnba_officials` | [espn_wnba_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_officials) | — |
| `load_wnba_player_season_stats` | [espn_wnba_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_season_stats) | — |
| `load_wnba_rosters` | [espn_wnba_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_rosters) | — |
| `load_wnba_shots` | [espn_wnba_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_shots) | — |
| `load_wnba_standings` | [espn_wnba_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_standings) | — |
| `load_wnba_team_season_stats` | [espn_wnba_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_season_stats) | — |
| `load_wnba_player_crosswalk` | [wnba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) | — |
| `load_wnba_schedule_crosswalk` | [wnba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) | — |
| `load_wnba_team_crosswalk` | [wnba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) | — |
| `load_wnba_player_core` | [espn_wnba_player_core](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_core) | — |
| `load_wnba_player_impact` | [wnba_player_impact](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_player_impact) | — |
| `load_wnba_stats_coaches` | [wnba_stats_coaches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_coaches) | — |
| `load_wnba_stats_draft` | [wnba_stats_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_draft) | — |
| `load_wnba_stats_game_rosters` | [wnba_stats_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_game_rosters) | — |
| `load_wnba_stats_officials` | [wnba_stats_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_officials) | — |
| `load_wnba_stats_pbp` | [wnba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_pbp) | — |
| `load_wnba_stats_possessions` | [wnba_stats_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_possessions) | — |
| `load_wnba_stats_game_lineups` | [wnba_stats_game_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_game_lineups) | — |
| `load_wnba_stats_player_boxscores` | [wnba_stats_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_player_boxscores) | — |
| `load_wnba_stats_player_game_logs` | [wnba_stats_player_game_logs](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_player_game_logs) | — |
| `load_wnba_stats_rosters` | [wnba_stats_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_rosters) | — |
| `load_wnba_stats_schedules` | [wnba_stats_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_schedules) | — |
| `load_wnba_stats_shots` | [wnba_stats_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_shots) | — |
| `load_wnba_stats_team_boxscores` | [wnba_stats_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_team_boxscores) | — |
| `load_wnba_groups` | [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) | — |
| `load_wnba_group_seasons` | [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) | — |
| `load_wnba_group_aliases` | [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) | — |
| `load_wnba_team_group_seasons` | [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) | — |

## Player

| Function | Summary |
|---|---|
| [load_wnba_player_boxscore](loaders/player.md#load_wnba_player_boxscore) | Release: espn_wnba_player_boxscores · asset … |
| [load_wnba_player_season_stats](loaders/player.md#load_wnba_player_season_stats) | Release: espn_wnba_player_season_stats · asset … |
| [load_wnba_player_crosswalk](loaders/player.md#load_wnba_player_crosswalk) | Release: wnba_crosswalk · asset … |
| [load_wnba_player_core](loaders/player.md#load_wnba_player_core) | Release: espn_wnba_player_core · asset … |
| [load_wnba_player_impact](loaders/player.md#load_wnba_player_impact) | Release: wnba_player_impact · asset … |

## Stats

| Function | Summary |
|---|---|
| [load_wnba_stats_coaches](loaders/stats.md#load_wnba_stats_coaches) | Release: wnba_stats_coaches · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_coaches/coaches_{season}.parquet` |
| [load_wnba_stats_draft](loaders/stats.md#load_wnba_stats_draft) | Release: wnba_stats_draft · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_draft/draft_{season}.parquet` |
| [load_wnba_stats_game_rosters](loaders/stats.md#load_wnba_stats_game_rosters) | Release: wnba_stats_game_rosters · asset … |
| [load_wnba_stats_officials](loaders/stats.md#load_wnba_stats_officials) | Release: wnba_stats_officials · asset … |
| [load_wnba_stats_pbp](loaders/stats.md#load_wnba_stats_pbp) | Release: wnba_stats_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_pbp/wnba_play_by_play_{season}.parquet` |
| [load_wnba_stats_possessions](loaders/stats.md#load_wnba_stats_possessions) | Release: wnba_stats_possessions · asset … |
| [load_wnba_stats_game_lineups](loaders/stats.md#load_wnba_stats_game_lineups) | Release: wnba_stats_game_lineups · asset … |
| [load_wnba_stats_player_boxscores](loaders/stats.md#load_wnba_stats_player_boxscores) | Release: wnba_stats_player_boxscores · asset … |
| [load_wnba_stats_player_game_logs](loaders/stats.md#load_wnba_stats_player_game_logs) | Release: wnba_stats_player_game_logs · asset … |
| [load_wnba_stats_rosters](loaders/stats.md#load_wnba_stats_rosters) | Release: wnba_stats_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_rosters/rosters_{season}.parquet` |
| [load_wnba_stats_schedules](loaders/stats.md#load_wnba_stats_schedules) | Release: wnba_stats_schedules · asset … |
| [load_wnba_stats_shots](loaders/stats.md#load_wnba_stats_shots) | Release: wnba_stats_shots · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_shots/shots_{season}.parquet` |
| [load_wnba_stats_team_boxscores](loaders/stats.md#load_wnba_stats_team_boxscores) | Release: wnba_stats_team_boxscores · asset … |

## Other

| Function | Summary |
|---|---|
| [load_wnba_pbp](loaders/other.md#load_wnba_pbp) | Release: espn_wnba_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_pbp/play_by_play_{season}.parquet` |
| [load_wnba_schedule](loaders/other.md#load_wnba_schedule) | Release: espn_wnba_schedules · asset … |
| [load_wnba_team_boxscore](loaders/other.md#load_wnba_team_boxscore) | Release: espn_wnba_team_boxscores · asset … |
| [load_wnba_draft](loaders/other.md#load_wnba_draft) | Release: espn_wnba_draft · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_draft/draft_{season}.parquet` |
| [load_wnba_game_rosters](loaders/other.md#load_wnba_game_rosters) | Release: espn_wnba_game_rosters · asset … |
| [load_wnba_officials](loaders/other.md#load_wnba_officials) | Release: espn_wnba_officials · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_officials/officials_{season}.parquet` |
| [load_wnba_rosters](loaders/other.md#load_wnba_rosters) | Release: espn_wnba_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_rosters/rosters_{season}.parquet` |
| [load_wnba_shots](loaders/other.md#load_wnba_shots) | Release: espn_wnba_shots · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_shots/shots_{season}.parquet` |
| [load_wnba_standings](loaders/other.md#load_wnba_standings) | Release: espn_wnba_standings · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_standings/standings_{season}.parquet` |
| [load_wnba_team_season_stats](loaders/other.md#load_wnba_team_season_stats) | Release: espn_wnba_team_season_stats · asset … |
| [load_wnba_schedule_crosswalk](loaders/other.md#load_wnba_schedule_crosswalk) | Release: wnba_crosswalk · asset … |
| [load_wnba_team_crosswalk](loaders/other.md#load_wnba_team_crosswalk) | Release: wnba_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_crosswalk/wnba_team_crosswalk_{season}.parquet` |
| [load_wnba_groups](loaders/other.md#load_wnba_groups) | Release: wnba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_groups.parquet` |
| [load_wnba_group_seasons](loaders/other.md#load_wnba_group_seasons) | Release: wnba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_group_seasons.parquet` |
| [load_wnba_group_aliases](loaders/other.md#load_wnba_group_aliases) | Release: wnba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_group_aliases.parquet` |
| [load_wnba_team_group_seasons](loaders/other.md#load_wnba_team_group_seasons) | Release: wnba_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_team_group_seasons_{season}.parquet` |
