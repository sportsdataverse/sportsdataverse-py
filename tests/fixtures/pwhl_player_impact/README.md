<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [PWHL player-impact and routing slices](#pwhl-player-impact-and-routing-slices)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# PWHL player-impact and routing slices

Real published PWHL data, trimmed to three 2025 games (`game_id` 105, 106, 107; 106 and 107 went
to a five-minute overtime). Used by `tests/pwhl/test_pwhl_player_impact_real.py` (the four impact
functions on HockeyTech pbp + shifts, C-NF-5) and the `routing` tests in
`tests/pwhl/test_pwhl_prediction_model_shims.py` (supp-001), both from task 2.13.

Source: the `fastRhockey-pwhl-data` checkout (sportsdataverse/fastRhockey-pwhl-data, commit
`66c07d74c8`, 2026-10-12; the pbp and shifts parquet last changed in `3d23726422`, 2026-10-08).
Those files are what the `pwhl_pbp`, `pwhl_shifts`, `pwhl_schedules`, `pwhl_game_info` and
`pwhl_skater_boxscores` sportsdataverse-data releases (read by the `load_pwhl_*` loaders) publish.
Sliced on 2026-10-12 with `polars.read_parquet(...).filter(pl.col("game_id").is_in([105, 106, 107]))`;
every column and value is as published.

| File | Source file | Rows | Notes |
|---|---|---|---|
| `play_by_play_2025_slice.parquet` | `pwhl/pbp/parquet/play_by_play_2025.parquet` | 602 | all 104 columns |
| `shifts_2025_slice.parquet` | `pwhl/shifts/parquet/shifts_2025.parquet` | 2355 | all 14 columns |
| `pwhl_schedule_2025_slice.parquet` | `pwhl/schedules/parquet/pwhl_schedule_2025.parquet` | 3 | `game_json_url` dropped (an R `glue` extension column) |
| `game_info_2025_slice.parquet` | `pwhl/game_info/parquet/game_info_2025.parquet` | 3 | all 24 columns |
| `skater_box_2025_slice.parquet` | `pwhl/skater_box/parquet/skater_box_2025.parquet` | 113 | all 22 columns |

Facts the tests rely on, measured on these slices: shift `start_s` / `end_s` are seconds left on
the period clock; the adapted pbp and shift chart both end at 3600 / 3900 / 3900 game seconds;
faceoff rows name only the home taker (`player_id`, with `home_win`); every goal row follows the
scoring `shot` row (`goal == True`).
