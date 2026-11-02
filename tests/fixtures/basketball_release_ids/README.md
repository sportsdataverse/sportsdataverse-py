<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [basketball_release_ids](#basketball_release_ids)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# basketball_release_ids

Real rows sliced from the files the release producers commit and publish to sportsdataverse-data, written back
unchanged: same columns and same dtypes (`game_rosters.game_id`, `rosters.athlete_id`, the crosswalk and NCAA
`espn_*` ids are String, as published; so are WNBA `officials.game_id` and the WNBA crosswalk `wnba_*` team / player
ids). `tests/mbb/test_basketball_release_id_joins.py` (MBB/NBA) and `tests/wnba/test_womens_release_id_joins.py`
(WNBA/WBB) feed them to the generated loaders to check the loader-boundary id casts and the cross-loader joins offline.

MBB/NBA captured 2026-10-30; WNBA/WBB captured 2026-11-01.

| file | source | rows kept |
|---|---|---|
| `mbb_game_rosters_2026.parquet` | hoopR-mbb-data @ `75b1488f` `mbb/game_rosters/parquet/game_rosters_2026.parquet` | games 401804830, 401804831 |
| `mbb_schedule_2026.parquet` | hoopR-mbb-data @ `75b1488f` `mbb/schedules/parquet/mbb_schedule_2026.parquet` | same games |
| `mbb_player_box_2026.parquet` | hoopR-mbb-data @ `75b1488f` `mbb/player_box/parquet/player_box_2026.parquet` | same games |
| `mbb_schedule_crosswalk_2026.parquet` | hoopR-mbb-data @ `75b1488f` `mbb/crosswalk/parquet/mbb_schedule_crosswalk_2026.parquet` | same games |
| `mbb_rosters_2026.parquet` | hoopR-mbb-data @ `75b1488f` `mbb/rosters/parquet/rosters_2026.parquet` | the four teams in those games |
| `ncaa_mbb_team_box_2026.parquet` | sportsdataverse-data release `ncaa_mbb_team_box`, asset `ncaa_mbb_team_box_2026.parquet` | rows whose `espn_game_id` is one of those games |
| `nba_game_rosters_2026.parquet` | hoopR-nba-data @ `e4a0559b` `nba/game_rosters/parquet/game_rosters_2026.parquet` | game 401809234 |
| `nba_schedule_2026.parquet` | hoopR-nba-data @ `e4a0559b` `nba/schedules/parquet/nba_schedule_2026.parquet` | same game |
| `nba_player_box_2026.parquet` | hoopR-nba-data @ `e4a0559b` `nba/player_box/parquet/player_box_2026.parquet` | same game |
| `nba_rosters_2026.parquet` | hoopR-nba-data @ `e4a0559b` `nba/rosters/parquet/rosters_2026.parquet` | the two teams in that game |
| `wnba_game_rosters_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/game_rosters/parquet/game_rosters_2025.parquet` | game 401820329 (2025 Finals) |
| `wnba_officials_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/officials/parquet/officials_2025.parquet` | same game |
| `wnba_schedule_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/schedules/parquet/wnba_schedule_2025.parquet` | same game |
| `wnba_player_box_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/player_box/parquet/player_box_2025.parquet` | same game |
| `wnba_rosters_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/rosters/parquet/rosters_2025.parquet` | the two teams in that game |
| `wnba_schedule_crosswalk_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/crosswalk/parquet/wnba_schedule_crosswalk_2025.parquet` | same game |
| `wnba_team_crosswalk_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/crosswalk/parquet/wnba_team_crosswalk_2025.parquet` | the two teams |
| `wnba_player_crosswalk_2025.parquet` | wehoop-wnba-data @ `dffea434` `wnba/crosswalk/parquet/wnba_player_crosswalk_2025.parquet` | the athletes in that game's player box |
| `wnba_stats_rosters_2025.parquet` | wehoop-wnba-stats-data @ `94f761f0` `wnba_stats/rosters/parquet/rosters_2025.parquet` | the two teams' WNBA Stats `team_id` |
| `wbb_game_rosters_2005.parquet` | wehoop-wbb-data @ `19fe2a82` `wbb/game_rosters/parquet/game_rosters_2005.parquet` | game 250160087 |
| `wbb_player_box_2005.parquet` | wehoop-wbb-data @ `19fe2a82` `wbb/player_box/parquet/player_box_2005.parquet` | same game |
| `wbb_schedule_crosswalk_2026.parquet` | wehoop-wbb-data @ `19fe2a82` `wbb/crosswalk/parquet/wbb_schedule_crosswalk_2026.parquet` | first row |
| `wbb_schedule_2026.parquet` | wehoop-wbb-data @ `19fe2a82` `wbb/schedules/parquet/wbb_schedule_2026.parquet` | that row's game |
