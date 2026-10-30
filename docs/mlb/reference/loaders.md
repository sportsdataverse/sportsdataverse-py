# MLB dataset loaders

> MLB dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets.

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_mlb_re24_matrix` | [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) | — |
| `load_mlb_we_table` | [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) | — |
| `load_mlb_wpa` | [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) | — |
| `load_mlb_pbp` | [mlb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pbp) | — |
| `load_mlb_pitches` | [mlb_pitches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitches) | — |
| `load_mlb_runners` | [mlb_runners](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_runners) | — |
| `load_mlb_statcast` | [mlb_statcast](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_statcast) | — |
| `load_mlb_schedule` | [mlb_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_schedules) | — |
| `load_mlb_expected_stats` | [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) | — |
| `load_mlb_expected_hr` | [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) | — |
| `load_mlb_batter_projection` | [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) | — |
| `load_mlb_oaa` | [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) | — |
| `load_mlb_catcher_framing` | [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) | — |
| `load_mlb_xera` | [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) | — |
| `load_mlb_stuff_plus` | [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) | — |
| `load_mlb_command_plus` | [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) | — |
| `load_ncaa_baseball_pbp` | [ncaa_baseball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_pbp) | — |
| `load_ncaa_baseball_schedule` | [ncaa_baseball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_schedules) | — |
| `load_ncaa_baseball_teams` | [ncaa_baseball_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_teams) | — |
| `load_ncaa_baseball_rosters` | [ncaa_baseball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_rosters) | — |
| `load_ncaa_baseball_linescore` | [ncaa_baseball_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_linescore) | — |
| `load_ncaa_baseball_team_stats` | [ncaa_baseball_team_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_team_stats) | — |
| `load_ncaa_baseball_player_stats` | [ncaa_baseball_player_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_player_stats) | — |
| `load_ncaa_baseball_situational_stats` | [ncaa_baseball_situational_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_situational_stats) | — |
| `load_ncaa_baseball_games` | [ncaa_baseball_games](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_games) | — |
| `load_mlb_groups` | [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) | — |
| `load_mlb_group_seasons` | [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) | — |
| `load_mlb_group_aliases` | [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) | — |
| `load_mlb_team_group_seasons` | [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) | — |
| `load_ncaa_baseball_groups` | [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) | — |
| `load_ncaa_baseball_group_seasons` | [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) | — |
| `load_ncaa_baseball_group_aliases` | [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) | — |
| `load_ncaa_baseball_team_group_seasons` | [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) | — |
| `load_mlb_park_dimensions` | [mlb_parks](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_parks) | — |

## NCAA (stats.ncaa.org)

| Function | Summary |
|---|---|
| [load_ncaa_baseball_pbp](loaders/ncaa.md#load_ncaa_baseball_pbp) | Release: ncaa_baseball_pbp · asset … |
| [load_ncaa_baseball_schedule](loaders/ncaa.md#load_ncaa_baseball_schedule) | Release: ncaa_baseball_schedules · asset … |
| [load_ncaa_baseball_teams](loaders/ncaa.md#load_ncaa_baseball_teams) | Release: ncaa_baseball_teams · asset … |
| [load_ncaa_baseball_rosters](loaders/ncaa.md#load_ncaa_baseball_rosters) | Release: ncaa_baseball_rosters · asset … |
| [load_ncaa_baseball_linescore](loaders/ncaa.md#load_ncaa_baseball_linescore) | Release: ncaa_baseball_linescore · asset … |
| [load_ncaa_baseball_team_stats](loaders/ncaa.md#load_ncaa_baseball_team_stats) | Release: ncaa_baseball_team_stats · asset … |
| [load_ncaa_baseball_player_stats](loaders/ncaa.md#load_ncaa_baseball_player_stats) | Release: ncaa_baseball_player_stats · asset … |
| [load_ncaa_baseball_situational_stats](loaders/ncaa.md#load_ncaa_baseball_situational_stats) | Release: ncaa_baseball_situational_stats · asset … |
| [load_ncaa_baseball_games](loaders/ncaa.md#load_ncaa_baseball_games) | Release: ncaa_baseball_games · asset … |
| [load_ncaa_baseball_groups](loaders/ncaa.md#load_ncaa_baseball_groups) | Release: ncaa_baseball_groups · asset … |
| [load_ncaa_baseball_group_seasons](loaders/ncaa.md#load_ncaa_baseball_group_seasons) | Release: ncaa_baseball_groups · asset … |
| [load_ncaa_baseball_group_aliases](loaders/ncaa.md#load_ncaa_baseball_group_aliases) | Release: ncaa_baseball_groups · asset … |
| [load_ncaa_baseball_team_group_seasons](loaders/ncaa.md#load_ncaa_baseball_team_group_seasons) | Release: ncaa_baseball_groups · asset … |

## Other

| Function | Summary |
|---|---|
| [load_mlb_re24_matrix](loaders/other.md#load_mlb_re24_matrix) | Release: mlb_game_state · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_re24_matrix_{season}.parquet` |
| [load_mlb_we_table](loaders/other.md#load_mlb_we_table) | Release: mlb_game_state · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_we_table_{season}.parquet` |
| [load_mlb_wpa](loaders/other.md#load_mlb_wpa) | Release: mlb_game_state · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_wpa_{season}.parquet` |
| [load_mlb_pbp](loaders/other.md#load_mlb_pbp) | Release: mlb_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pbp/mlb_pbp_{season}.parquet` |
| [load_mlb_pitches](loaders/other.md#load_mlb_pitches) | Release: mlb_pitches · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitches/mlb_pitches_{season}.parquet` |
| [load_mlb_runners](loaders/other.md#load_mlb_runners) | Release: mlb_runners · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_runners/mlb_runners_{season}.parquet` |
| [load_mlb_statcast](loaders/other.md#load_mlb_statcast) | Release: mlb_statcast · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_statcast/mlb_statcast_{season}.parquet` |
| [load_mlb_schedule](loaders/other.md#load_mlb_schedule) | Release: mlb_schedules · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_schedules/mlb_schedule_{season}.parquet` |
| [load_mlb_expected_stats](loaders/other.md#load_mlb_expected_stats) | Release: mlb_hitting_models · asset … |
| [load_mlb_expected_hr](loaders/other.md#load_mlb_expected_hr) | Release: mlb_hitting_models · asset … |
| [load_mlb_batter_projection](loaders/other.md#load_mlb_batter_projection) | Release: mlb_hitting_models · asset … |
| [load_mlb_oaa](loaders/other.md#load_mlb_oaa) | Release: mlb_fielding_models · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_fielding_models/mlb_oaa_{season}.parquet` |
| [load_mlb_catcher_framing](loaders/other.md#load_mlb_catcher_framing) | Release: mlb_fielding_models · asset … |
| [load_mlb_xera](loaders/other.md#load_mlb_xera) | Release: mlb_pitching_models · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitching_models/mlb_xera_{season}.parquet` |
| [load_mlb_stuff_plus](loaders/other.md#load_mlb_stuff_plus) | Release: mlb_pitching_models · asset … |
| [load_mlb_command_plus](loaders/other.md#load_mlb_command_plus) | Release: mlb_pitching_models · asset … |
| [load_mlb_groups](loaders/other.md#load_mlb_groups) | Release: mlb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_groups.parquet` |
| [load_mlb_group_seasons](loaders/other.md#load_mlb_group_seasons) | Release: mlb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_group_seasons.parquet` |
| [load_mlb_group_aliases](loaders/other.md#load_mlb_group_aliases) | Release: mlb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_group_aliases.parquet` |
| [load_mlb_team_group_seasons](loaders/other.md#load_mlb_team_group_seasons) | Release: mlb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_team_group_seasons_{season}.parquet` |
| [load_mlb_park_dimensions](loaders/other.md#load_mlb_park_dimensions) | Release: mlb_parks · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_parks/mlb_park_dimensions.parquet` |
