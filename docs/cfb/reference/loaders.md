# CFB dataset loaders

> CFB dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets.

Pipeline: scrape / raw → enrich → release asset → `load_*()`

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_cfb_pbp` | [espn_cfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_pbp) | — |
| `load_cfb_ratings` | [cfb_ratings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings) | — |
| `load_cfb_recruiting_proj` | [cfb_recruiting_proj](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_recruiting_proj) | — |
| `load_cfb_recruits` | [cfb_recruits](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_recruits) | — |
| `load_cfb_returning_production` | [cfb_returning_production](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_returning_production) | — |
| `load_cfb_rosters` | [espn_cfb_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_rosters) | — |
| `load_cfb_rosters_cfbd` | [cfbfastR-data](https://github.com/sportsdataverse/cfbfastR-data) | — |
| `load_cfb_schedule` | [cfb_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_schedules) | — |
| `load_cfb_team_info` | [cfb_team_info](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_info) | — |
| `load_cfb_teams` | [espn_cfb_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_teams) | — |
| `load_cfb_team_portal` | [cfb_team_portal](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_portal) | — |
| `load_cfb_team_talent` | [cfb_team_talent](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_talent) | — |
| `load_cfb_teams_crosswalk` | [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) | — |
| `load_cfb_schedule_crosswalk` | [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) | — |
| `load_cfb_team_box` | [espn_cfb_team_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_box) | — |
| `load_cfb_player_box` | [espn_cfb_player_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_player_box) | — |
| `load_cfb_drives` | [espn_cfb_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_drives) | — |
| `load_cfb_play_participants` | [espn_cfb_play_participants](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_play_participants) | — |
| `load_cfb_game_rosters` | [espn_cfb_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_game_rosters) | — |
| `load_cfb_linescores` | [espn_cfb_linescores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_linescores) | — |
| `load_cfb_betting` | [espn_cfb_betting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_betting) | — |
| `load_cfb_fpi_weekly` | [cfb_fpi_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_fpi_weekly) | — |
| `load_cfb_power_index` | [espn_cfb_power_index](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_power_index) | — |
| `load_cfb_adv_team` | [espn_cfb_adv_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team) | — |
| `load_cfb_adv_passing` | [espn_cfb_adv_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_passing) | — |
| `load_cfb_adv_rushing` | [espn_cfb_adv_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_rushing) | — |
| `load_cfb_adv_receiving` | [espn_cfb_adv_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_receiving) | — |
| `load_cfb_adv_defensive` | [espn_cfb_adv_defensive](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive) | — |
| `load_cfb_adv_defensive_players` | [espn_cfb_adv_defensive_players](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive_players) | — |
| `load_cfb_adv_drives` | [espn_cfb_adv_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_drives) | — |
| `load_cfb_adv_situational` | [espn_cfb_adv_situational](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_situational) | — |
| `load_cfb_adv_specialists` | [espn_cfb_adv_specialists](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_specialists) | — |
| `load_cfb_adv_turnover` | [espn_cfb_adv_turnover](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_turnover) | — |
| `load_cfb_model_pbp` | [espn_cfb_model_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_model_pbp) | — |
| `load_cfb_passing` | [espn_cfb_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_passing) | — |
| `load_cfb_percentiles` | [espn_cfb_percentiles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_percentiles) | — |
| `load_cfb_receiving` | [espn_cfb_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_receiving) | — |
| `load_cfb_rushing` | [espn_cfb_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_rushing) | — |
| `load_cfb_team_summaries` | [espn_cfb_team_summaries](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_summaries) | — |
| `load_cfb_adv_team_gamelog` | [espn_cfb_adv_team_gamelog](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team_gamelog) | — |
| `load_cfb_ratings_weekly` | [cfb_ratings_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings_weekly) | — |
| `load_cfb_team_summaries_weekly` | [cfb_team_summaries_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_summaries_weekly) | — |
| `load_cfb_usage_players` | [espn_cfb_usage_players](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_players) | — |
| `load_cfb_usage_position_groups` | [espn_cfb_usage_position_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_position_groups) | — |
| `load_cfb_usage_tackles` | [espn_cfb_usage_tackles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_tackles) | — |
| `load_cfb_usage_position_group_tackles` | [espn_cfb_usage_position_group_tackles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_position_group_tackles) | — |
| `load_cfb_usage_teams` | [espn_cfb_usage_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_teams) | — |
| `load_cfb_usage_drive_scripting` | [espn_cfb_usage_drive_scripting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_drive_scripting) | — |
| `load_cfb_usage_st_kickers` | [espn_cfb_usage_st_kickers](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_st_kickers) | — |
| `load_cfb_usage_st_punters` | [espn_cfb_usage_st_punters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_st_punters) | — |
| `load_cfb_usage_st_returners` | [espn_cfb_usage_st_returners](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_st_returners) | — |
| `load_cfb_usage_st_blocks` | [espn_cfb_usage_st_blocks](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_st_blocks) | — |
| `load_cfb_usage_st_team` | [espn_cfb_usage_st_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_usage_st_team) | — |
| `load_cfb_team_tendencies` | [espn_cfb_team_tendencies](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_tendencies) | — |
| `load_cfb_coach_tendencies` | [espn_cfb_coach_tendencies](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_coach_tendencies) | — |
| `load_cfb_coach_careers` | [espn_cfb_coach_careers](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_coach_careers) | — |
| `load_cfb_pbp_r` | [cfbfastR_cfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR_cfb_pbp) | — |
| `load_ncaa_mfb_pbp` | [ncaa_mfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_pbp) | — |
| `load_ncaa_mfb_pbp_cfbfastr` | [ncaa_mfb_pbp_cfbfastr](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_pbp_cfbfastr) | — |
| `load_ncaa_mfb_drives` | [ncaa_mfb_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_drives) | — |
| `load_ncaa_mfb_schedule` | [ncaa_mfb_schedule](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_schedule) | — |
| `load_ncaa_mfb_rosters` | [ncaa_mfb_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_rosters) | — |
| `load_ncaa_mfb_teams` | [ncaa_mfb_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_teams) | — |
| `load_ncaa_mfb_team_stats` | [ncaa_mfb_team_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_team_stats) | — |
| `load_ncaa_mfb_player_stats` | [ncaa_mfb_player_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_player_stats) | — |
| `load_ncaa_mfb_officials` | [ncaa_mfb_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_officials) | — |
| `load_ncaa_mfb_linescore` | [ncaa_mfb_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_linescore) | — |
| `load_cfb_groups` | [cfb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_groups) | — |
| `load_cfb_group_seasons` | [cfb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_groups) | — |
| `load_cfb_group_aliases` | [cfb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_groups) | — |
| `load_cfb_team_group_seasons` | [cfb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_groups) | — |

## Advanced stats

| Function | Summary |
|---|---|
| [load_cfb_adv_team](loaders/adv.md#load_cfb_adv_team) | Release: espn_cfb_adv_team · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_team/adv_team_{season}.parquet` |
| [load_cfb_adv_passing](loaders/adv.md#load_cfb_adv_passing) | Release: espn_cfb_adv_passing · asset … |
| [load_cfb_adv_rushing](loaders/adv.md#load_cfb_adv_rushing) | Release: espn_cfb_adv_rushing · asset … |
| [load_cfb_adv_receiving](loaders/adv.md#load_cfb_adv_receiving) | Release: espn_cfb_adv_receiving · asset … |
| [load_cfb_adv_defensive](loaders/adv.md#load_cfb_adv_defensive) | Release: espn_cfb_adv_defensive · asset … |
| [load_cfb_adv_defensive_players](loaders/adv.md#load_cfb_adv_defensive_players) | Release: espn_cfb_adv_defensive_players · asset … |
| [load_cfb_adv_drives](loaders/adv.md#load_cfb_adv_drives) | Release: espn_cfb_adv_drives · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_drives/adv_drives_{season}.parquet` |
| [load_cfb_adv_situational](loaders/adv.md#load_cfb_adv_situational) | Release: espn_cfb_adv_situational · asset … |
| [load_cfb_adv_specialists](loaders/adv.md#load_cfb_adv_specialists) | Release: espn_cfb_adv_specialists · asset … |
| [load_cfb_adv_turnover](loaders/adv.md#load_cfb_adv_turnover) | Release: espn_cfb_adv_turnover · asset … |
| [load_cfb_adv_team_gamelog](loaders/adv.md#load_cfb_adv_team_gamelog) | Release: espn_cfb_adv_team_gamelog · asset … |

## Coach

| Function | Summary |
|---|---|
| [load_cfb_coach_tendencies](loaders/coach.md#load_cfb_coach_tendencies) | Release: espn_cfb_coach_tendencies · asset … |
| [load_cfb_coach_careers](loaders/coach-2.md#load_cfb_coach_careers) | Release: espn_cfb_coach_careers · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_coach_careers/coach_careers.parquet` |

## NCAA (stats.ncaa.org)

| Function | Summary |
|---|---|
| [load_ncaa_mfb_pbp](loaders/ncaa.md#load_ncaa_mfb_pbp) | Release: ncaa_mfb_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_pbp/ncaa_mfb_pbp_{season}.parquet` |
| [load_ncaa_mfb_pbp_cfbfastr](loaders/ncaa.md#load_ncaa_mfb_pbp_cfbfastr) | Release: ncaa_mfb_pbp_cfbfastr · asset … |
| [load_ncaa_mfb_drives](loaders/ncaa.md#load_ncaa_mfb_drives) | Release: ncaa_mfb_drives · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_drives/ncaa_mfb_drives_{season}.parquet` |
| [load_ncaa_mfb_schedule](loaders/ncaa.md#load_ncaa_mfb_schedule) | Release: ncaa_mfb_schedule · asset … |
| [load_ncaa_mfb_rosters](loaders/ncaa.md#load_ncaa_mfb_rosters) | Release: ncaa_mfb_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_rosters/ncaa_mfb_rosters_{season}.parquet` |
| [load_ncaa_mfb_teams](loaders/ncaa.md#load_ncaa_mfb_teams) | Release: ncaa_mfb_teams · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_teams/ncaa_mfb_teams_{season}.parquet` |
| [load_ncaa_mfb_team_stats](loaders/ncaa.md#load_ncaa_mfb_team_stats) | Release: ncaa_mfb_team_stats · asset … |
| [load_ncaa_mfb_player_stats](loaders/ncaa.md#load_ncaa_mfb_player_stats) | Release: ncaa_mfb_player_stats · asset … |
| [load_ncaa_mfb_officials](loaders/ncaa.md#load_ncaa_mfb_officials) | Release: ncaa_mfb_officials · asset … |
| [load_ncaa_mfb_linescore](loaders/ncaa.md#load_ncaa_mfb_linescore) | Release: ncaa_mfb_linescore · asset … |

## Play-by-play

| Function | Summary |
|---|---|
| [load_cfb_pbp](loaders/pbp.md#load_cfb_pbp) | Release: espn_cfb_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_pbp/play_by_play_{season}.parquet` |
| [load_cfb_pbp_r](loaders/pbp-2.md#load_cfb_pbp_r) | Release: cfbfastR_cfb_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfbfastR_cfb_pbp/play_by_play_{season}.parquet` |

## Rosters

| Function | Summary |
|---|---|
| [load_cfb_rosters](loaders/rosters.md#load_cfb_rosters) | Release: espn_cfb_rosters · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_rosters/cfb_rosters_{season}.parquet` |
| [load_cfb_rosters_cfbd](loaders/rosters.md#load_cfb_rosters_cfbd) | Release: cfbfastR-data · asset `https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/rosters/parquet/cfb_rosters_{season}.parquet` |

## Schedule

| Function | Summary |
|---|---|
| [load_cfb_schedule](loaders/schedule.md#load_cfb_schedule) | Release: cfb_schedules · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_schedules/cfb_schedules_{season}.parquet` |
| [load_cfb_schedule_crosswalk](loaders/schedule.md#load_cfb_schedule_crosswalk) | Release: cfb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/cfb_schedule_crosswalk_{season}.parquet` |

## Team

| Function | Summary |
|---|---|
| [load_cfb_team_info](loaders/team.md#load_cfb_team_info) | Release: cfb_team_info · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_info/cfb_team_info_{season}.parquet` |
| [load_cfb_teams](loaders/team.md#load_cfb_teams) | Release: espn_cfb_teams · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_teams/cfb_teams_{season}.parquet` |
| [load_cfb_team_portal](loaders/team.md#load_cfb_team_portal) | Release: cfb_team_portal · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_portal/cfb_team_portal_{season}.parquet` |
| [load_cfb_team_talent](loaders/team.md#load_cfb_team_talent) | Release: cfb_team_talent · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_talent/cfb_team_talent_{season}.parquet` |
| [load_cfb_teams_crosswalk](loaders/team.md#load_cfb_teams_crosswalk) | Release: cfb_crosswalk · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/cfb_teams_crosswalk_{season}.parquet` |
| [load_cfb_team_box](loaders/team.md#load_cfb_team_box) | Release: espn_cfb_team_box · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_team_box/team_box_{season}.parquet` |
| [load_cfb_team_summaries](loaders/team-2.md#load_cfb_team_summaries) | Release: espn_cfb_team_summaries · asset … |
| [load_cfb_team_summaries_weekly](loaders/team-3.md#load_cfb_team_summaries_weekly) | Release: cfb_team_summaries_weekly · asset … |
| [load_cfb_team_tendencies](loaders/team-4.md#load_cfb_team_tendencies) | Release: espn_cfb_team_tendencies · asset … |
| [load_cfb_team_group_seasons](loaders/team-5.md#load_cfb_team_group_seasons) | Release: cfb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_groups/cfb_team_group_seasons_{season}.parquet` |

## Usage

| Function | Summary |
|---|---|
| [load_cfb_usage_players](loaders/usage.md#load_cfb_usage_players) | Release: espn_cfb_usage_players · asset … |
| [load_cfb_usage_position_groups](loaders/usage.md#load_cfb_usage_position_groups) | Release: espn_cfb_usage_position_groups · asset … |
| [load_cfb_usage_tackles](loaders/usage.md#load_cfb_usage_tackles) | Release: espn_cfb_usage_tackles · asset … |
| [load_cfb_usage_position_group_tackles](loaders/usage.md#load_cfb_usage_position_group_tackles) | Release: espn_cfb_usage_position_group_tackles · asset … |
| [load_cfb_usage_teams](loaders/usage.md#load_cfb_usage_teams) | Release: espn_cfb_usage_teams · asset … |
| [load_cfb_usage_drive_scripting](loaders/usage.md#load_cfb_usage_drive_scripting) | Release: espn_cfb_usage_drive_scripting · asset … |
| [load_cfb_usage_st_kickers](loaders/usage.md#load_cfb_usage_st_kickers) | Release: espn_cfb_usage_st_kickers · asset … |
| [load_cfb_usage_st_punters](loaders/usage.md#load_cfb_usage_st_punters) | Release: espn_cfb_usage_st_punters · asset … |
| [load_cfb_usage_st_returners](loaders/usage.md#load_cfb_usage_st_returners) | Release: espn_cfb_usage_st_returners · asset … |
| [load_cfb_usage_st_blocks](loaders/usage.md#load_cfb_usage_st_blocks) | Release: espn_cfb_usage_st_blocks · asset … |
| [load_cfb_usage_st_team](loaders/usage.md#load_cfb_usage_st_team) | Release: espn_cfb_usage_st_team · asset … |

## Other

| Function | Summary |
|---|---|
| [load_cfb_ratings](loaders/other.md#load_cfb_ratings) | Release: cfb_ratings · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_ratings/cfb_ratings_{season}.parquet` |
| [load_cfb_recruiting_proj](loaders/other.md#load_cfb_recruiting_proj) | Release: cfb_recruiting_proj · asset … |
| [load_cfb_recruits](loaders/other.md#load_cfb_recruits) | Release: cfb_recruits · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_recruits/cfb_recruits_{season}.parquet` |
| [load_cfb_returning_production](loaders/other.md#load_cfb_returning_production) | Release: cfb_returning_production · asset … |
| [load_cfb_player_box](loaders/other.md#load_cfb_player_box) | Release: espn_cfb_player_box · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_player_box/player_box_{season}.parquet` |
| [load_cfb_drives](loaders/other.md#load_cfb_drives) | Release: espn_cfb_drives · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_drives/drives_{season}.parquet` |
| [load_cfb_play_participants](loaders/other.md#load_cfb_play_participants) | Release: espn_cfb_play_participants · asset … |
| [load_cfb_game_rosters](loaders/other.md#load_cfb_game_rosters) | Release: espn_cfb_game_rosters · asset … |
| [load_cfb_linescores](loaders/other.md#load_cfb_linescores) | Release: espn_cfb_linescores · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_linescores/linescores_{season}.parquet` |
| [load_cfb_betting](loaders/other.md#load_cfb_betting) | Release: espn_cfb_betting · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_betting/betting_{season}.parquet` |
| [load_cfb_fpi_weekly](loaders/other.md#load_cfb_fpi_weekly) | Release: cfb_fpi_weekly · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_fpi_weekly/cfb_fpi_weekly_{season}.parquet` |
| [load_cfb_power_index](loaders/other.md#load_cfb_power_index) | Release: espn_cfb_power_index · asset … |
| [load_cfb_model_pbp](loaders/other.md#load_cfb_model_pbp) | Release: espn_cfb_model_pbp · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_model_pbp/model_pbp_{season}.parquet` |
| [load_cfb_passing](loaders/other-2.md#load_cfb_passing) | Release: espn_cfb_passing · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_passing/cfb_passing_{season}.parquet` |
| [load_cfb_percentiles](loaders/other-2.md#load_cfb_percentiles) | Release: espn_cfb_percentiles · asset … |
| [load_cfb_receiving](loaders/other-2.md#load_cfb_receiving) | Release: espn_cfb_receiving · asset … |
| [load_cfb_rushing](loaders/other-2.md#load_cfb_rushing) | Release: espn_cfb_rushing · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_rushing/cfb_rushing_{season}.parquet` |
| [load_cfb_ratings_weekly](loaders/other-2.md#load_cfb_ratings_weekly) | Release: cfb_ratings_weekly · asset … |
| [load_cfb_groups](loaders/other-2.md#load_cfb_groups) | Release: cfb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_groups/cfb_groups.parquet` |
| [load_cfb_group_seasons](loaders/other-2.md#load_cfb_group_seasons) | Release: cfb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_groups/cfb_group_seasons.parquet` |
| [load_cfb_group_aliases](loaders/other-2.md#load_cfb_group_aliases) | Release: cfb_groups · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_groups/cfb_group_aliases.parquet` |
