<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D24 EPV corpus](#bb-d24-epv-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D24 EPV corpus

Real releases for `tests/nba/test_nba_epv.py`, written by `tools/models/nba_epv/train_epv.py`
(`write_fixture`) on 2026-11-05 from the producer checkouts that publish the `nba_stats_*` /
`wnba_stats_*` releases (hoopR-nba-stats-data `nba_stats/{pbp,possessions,game_lineups}/parquet/`,
wehoop-wnba-stats-data `wnba_stats/{pbp,possessions,game_lineups}/parquet/`), zstd level 19.
Nothing is synthetic or edited: rows are only filtered and columns only dropped.

2024 is the holdout season of both bundled EPV boosters (trained on 2021-2023), so every
calibration check on these files is out of sample.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_pbp_2024.parquet` | 19,839 | `nba_stats_pbp/nba_play_by_play_2024` | The first 40 regular-season games of 2023-24 by `game_id` (`0022300001`-`0022300040`). Cols: what `epv_states` reads (`season, game_id, order_index, action_number, period, seconds_remaining, event_type, sub_type, description, shot_distance, shot_value, team_id, person_id, location, score_home, score_away`) plus the release's `off_player_1..5` / `def_player_1..5`. |
| `nba_possessions_2024.parquet` | 7,948 | `nba_stats_possessions/nba_possessions_2024` | The same games. Cols: `game_id, period, possession_number, number_in_period, start_order_index, end_order_index, start_seconds_remaining, end_seconds_remaining, possession_start_type, offense_team_id, defense_team_id, points, off_player_1..5, def_player_1..5`. |
| `nba_game_lineups_2024.parquet` | 19,839 | `nba_stats_game_lineups/nba_lineups_2024` | The same games. Cols: `game_id, action_number, home_player_1..5, away_player_1..5`. |
| `wnba_pbp_2024.parquet` / `wnba_possessions_2024.parquet` / `wnba_game_lineups_2024.parquet` | 8,742 / 3,281 / 8,742 | `wnba_stats_pbp/wnba_play_by_play_2024`, `wnba_stats_possessions/wnba_possessions_2024`, `wnba_stats_game_lineups/wnba_lineups_2024` | The first 20 regular-season games of 2024 by `game_id`. Same columns (the WNBA pbp release has no player slots). |

Dtypes: `game_id` is Utf8 in every file, `order_index`, `action_number`, the team and player ids are
Int64, scores are Utf8 as the feed ships them. Known gap: the possessions release assigns about
0.16% of 2023-24 NBA possessions to the wrong offense (a shot by the defending team inside them);
EPV inherits that, and `start_margin` (read from the feed's running score) stays right.

Files are named by the END year, as the releases stamp `season`. Regenerate by rerunning the fitting
script (it rewrites these files from the holdout season after the gates pass).
