<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA shot-make fixtures](#nba-shot-make-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA shot-make fixtures

Both are cut from the `nba_stats_pbp` release parquet as the producer checkout held it on 2026-11-25
(`hoopR-nba-stats-data`, last commit touching the files `ed887f15`, 2026-11-15), field goals only
(`is_field_goal == 1`), with the columns the scorer and the fitter read: `game_id` (Utf8), `action_number`
(Int64), `sub_type`, `shot_value`, `shot_distance`, `x_legacy`, `y_legacy`, `shot_result`.

| file | rows | what |
|---|---:|---|
| `nba_fga_2026.parquet` | 3,585 | the regular-season field goals of the first 20 games (by `game_id`) of 2025-26 (`nba_play_by_play_2026.parquet`), the shipped booster's holdout season. Cut by `tools/models/nba_shot_value/train_shot_make.py --league-id 00 --fixture` from the frames the gates scored; recut it whenever the booster is refit. |
| `nba_fga_2023_chart_games.parquet` | 544 | every field goal of games `0022200001`, `0022200002` and `0022200004` (2022-23, `nba_play_by_play_2023.parquet`), the first three games of the committed shot chart `tests/fixtures/nba_shot_value/shotchart_2023.parquet`, which holds 173 of their shots. It pins the shot chart's columns to the play-by-play's (`game_event_id` = `action_number`, `loc_x` = `x_legacy`, `loc_y` = `y_legacy`, `action_type` = `sub_type`). |

`tests/nba/test_nba_shot_make.py` reads both.
