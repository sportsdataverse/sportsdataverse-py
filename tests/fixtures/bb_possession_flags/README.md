<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D7 possession-flags corpus](#bb-d7-possession-flags-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D7 possession-flags corpus

Real releases for `tests/nba/test_nba_possession_flags.py`, read on 2026-11-04 from the producer
checkouts that publish the `nba_stats_*` / `wnba_stats_*` releases (hoopR-nba-stats-data
`nba_stats/{possessions,pbp,schedules}/parquet/`, wehoop-wnba-stats-data
`wnba_stats/{possessions,pbp}/parquet/`) and written with zstd level 19. Nothing is synthetic or
edited: rows are only filtered and columns only dropped.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_possessions_2024.parquet` | 242,618 | `nba_stats_possessions/nba_possessions_2024` | The 1,230 regular-season games (`game_id` starts `002`). Cols: what `add_possession_flags` reads (`game_id, period, possession_number, number_in_period, start_order_index, end_order_index, start_seconds_remaining, end_seconds_remaining, possession_start_type, off_player_1..5, def_player_1..5`) plus `offense_team_id, defense_team_id, points, count_as_possession, season`. |
| `nba_pbp_2024.parquet` | 598,705 | `nba_stats_pbp/nba_play_by_play_2024` | The same games. Cols: `game_id, order_index, event_type, seconds_remaining, score_home, score_away` (scores left as the feed's Utf8). |
| `wnba_possessions_2025.parquet` / `wnba_pbp_2025.parquet` | 48,401 / 126,330 | `wnba_stats_possessions/wnba_possessions_2025`, `wnba_stats_pbp/wnba_play_by_play_2025` | All 310 games, same columns. |
| `wnba_possessions_2002.parquet` / `wnba_pbp_2002.parquet` | 4,405 / 12,152 | `wnba_stats_possessions/wnba_possessions_2002`, `wnba_stats_pbp/wnba_play_by_play_2002` | 30 games of the two-halves era: the first 15 (by `game_id`) with garbage time and the first 15 without. Same columns. |
| `nba_possessions_2021_0022000983.parquet` / `nba_pbp_2021_0022000983.parquet` | 206 / 508 | `nba_stats_possessions/nba_possessions_2021`, `nba_stats_pbp/nba_play_by_play_2021` | Game `0022000983`, the only game in the 1996-97 to 2026-27 NBA releases whose garbage time starts in overtime. Same columns. |
| `nba_pbp_scores_2000.parquet` | 20,587 | `nba_stats_pbp/nba_play_by_play_2000` | The first 40 regular-season games of 1999-00, where the feed writes `"0"` on non-scoring rows. Cols: `game_id, order_index, score_home, score_away`. |
| `nba_schedule_2000.parquet` | 40 | `nba_stats_schedules/nba_schedule_2000` | Those games' `game_id, home_pts, away_pts`: the final-score oracle. |

Dtypes: `game_id` is Utf8 in every file, `order_index` / `*_order_index` and the player and team
ids are Int64, scores are Utf8 as the feed ships them. Known gaps: no possession that is first in
its period has a live start type, so the `number_in_period > 1` transition guard never changes a
flag here.

The transition oracle is `../nba_playtype/synergy_off_team_2024.parquet` (stats.nba.com Synergy,
2023-24, team offense; see that folder's README). Files are named by the END year, as the releases
stamp `season`.
