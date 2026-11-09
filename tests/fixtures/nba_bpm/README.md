<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA BPM 2.0 season corpus](#nba-bpm-20-season-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA BPM 2.0 season corpus

Real inputs for the real-season tests in `tests/nba/test_nba_bpm.py`. They were read on 2026-11-09
from the committed hoopR-nba-stats-raw store (`nba_stats/json/leaguegamelog/2025/regular-season{,_p}.json`
and `playerindex/2025.json`) through `nba_box_logs("2024-25", fetch=...)` and
`nba_player_positions("2024-25", fetch=...)` (the producer's raw-store reader,
`nba_raw_store_season_frame`). Zstd level 19.

Nothing is synthetic: rows are untouched and columns are only dropped. `nba_bpm` on the trimmed
frames matches the full frames to 2e-15.

| File | Rows | Kept |
|---|---:|---|
| `player_logs_2025.parquet` | 26,306 (2024-25 regular season, one row per player-game) | `player_id, team_id, game_id, min, fgm, fga, fg3_m, fg3_a, ftm, fta, oreb, dreb, reb, ast, stl, blk, tov, pf, pts, plus_minus` |
| `team_logs_2025.parquet` | 2,460 (1,230 games x 2) | the same columns without `player_id` |
| `positions_2025.parquet` | 5,204 (the playerindex season) | `player_id, position_num` |
| `names_2025.parquet` | 569 | `player_id, player_name` (first spelling in the logs), used only to match Basketball-Reference names |

Id dtypes: `player_id` / `team_id` Int64, `game_id` Utf8. The Basketball-Reference oracle
(`bref_nba_advanced_2025.parquet`) lives in `../bb_player_war/` and its README covers it.
