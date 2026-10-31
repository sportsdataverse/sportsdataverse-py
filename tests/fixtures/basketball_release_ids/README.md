<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [basketball_release_ids](#basketball_release_ids)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# basketball_release_ids

Real rows sliced from the files the release producers commit and publish to sportsdataverse-data, written back
unchanged: same columns and same dtypes (`game_rosters.game_id`, `rosters.athlete_id`, the crosswalk and NCAA
`espn_*` ids are String, as published). `tests/mbb/test_basketball_release_id_joins.py` feeds them to the generated loaders to check the
loader-boundary id casts and the cross-loader joins offline.

Captured 2026-10-30.

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
