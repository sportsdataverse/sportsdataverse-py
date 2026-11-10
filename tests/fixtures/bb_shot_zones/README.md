<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D35 shot-zone corpus](#bb-d35-shot-zone-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D35 shot-zone corpus

Real captures for `tests/nba/test_nba_shot_zone_profiles.py` and
`tests/mbb/test_mbb_shot_zone_profiles.py`, built on 2026-11-10 from the sportsdataverse-data
releases (zstd level 19). Nothing is synthetic: rows are only filtered, with columns dropped, in
the release's own order (the college assist link reads that order).

| File | Rows | Source | Kept |
|---|---:|---|---|
| `nba_stats_shots_2025.parquet` | 21,267 | `nba_stats_shots` 2024-25 (`load_nba_stats_shots(2024)`) | Every attempt of the first 120 regular-season games by `game_id` (`002...`). Cols: `game_id, season, team_id, person_id, player_name, shot_result, shot_value, shot_distance, x_legacy, y_legacy, description`. |
| `wnba_stats_shots_2010.parquet` / `wnba_stats_shots_2025.parquet` | 5,356 / 5,405 | `wnba_stats_shots` 2010 / 2025 | The first 40 regular-season games (`102...`), same columns. 2010 holds 944 hoop-centre placeholder attempts at `(0, 0)`; 2025 holds one attempt with `team_id` 0. |
| `ncaa_{mbb,wbb}_shots_2025.parquet` | 9,809 / 9,248 | `ncaa_{mbb,wbb}_shots` 2024-25 (`load_ncaa_{mbb,wbb}_shots(2025)`) | Every chart attempt of the release's first 80 contests, in release order. Cols: `season, contest_id, team_id` (NCAA team name)`, espn_team_id, shooter_player_id, shooter_clean_name, shot_zone, shot_x, shot_y, dist_ft, made, point_value`. |
| `ncaa_{mbb,wbb}_pbp_fg_2025.parquet` | 9,809 / 9,248 | `ncaa_{mbb,wbb}_pbp` 2024-25 | The field-goal rows (`shot_value` 2 or 3) of the same contests, in release order. Cols: `contest_id, shot_value, event_result, player_1_id, assist_player`. |

Id dtypes: stats.nba.com / stats.wnba.com `team_id` and `person_id` Int64, `game_id` String with
its leading zeros; NCAA `contest_id`, `shooter_player_id` and `player_1_id` String, `espn_team_id`
Int32.

`../bb_shot_zone_gates/{nba,wnba,mbb,wbb}_fits.csv` are the split-half reliability curves
`uv run python tools/fit_shot_zone_gates.py --league <lg> --write` saved from the full releases
(train and holdout spans in `SPANS` there); `tests/tools/test_fit_shot_zone_gates.py` re-fits the
frozen gates from them offline.
