# rolling_windows fixtures

Real released CFB play-by-play, sliced to the 36 games (2021-2024) in which
Kyle McCord (ESPN athlete id `4433971`; Ohio State 2021-23, Syracuse 2024)
threw a pass.

## Source

- `cfbfastR-cfb-data` (published CFB pbp + schedules), HEAD
  `6e0391dc30192acb8c60de8da6c8247a42d9ad49` at capture time.
- `cfb/pbp/parquet/play_by_play_{2021,2022,2023,2024}.parquet`
- `cfb/cfb_schedules/parquet/cfb_schedules_{2021,2022,2023,2024}.parquet`

## Selection rule

1. Read the four pbp season files, projected to `FOOTBALL_PBP_COLUMNS`
   (see `sportsdataverse/rolling_windows.py`), concatenated
   `how="diagonal_relaxed"`.
2. `games` = the unique `game_id`s where `passer_player_id == 4433971`.
3. `cfb_pbp_4433971_2021_2024.parquet` = every play (any player) from those
   games — not just McCord's own plays — so team-level (`play` unit) and
   receiver/rusher-side events reflect the real game population.
4. `cfb_schedule_4433971_2021_2024.parquet` = the matching `game_id` +
   `start_date` rows, deduped on `game_id`.

## Expected counts

- `pbp.height` = 6048
- `pbp["game_id"].n_unique()` = 36
- `sched.height` = 36

Reproduced exactly against the source above.
