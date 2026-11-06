<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D24 EPV corpus](#bb-d24-epv-corpus)
  - [WNBA (`tests/wnba/test_wnba_epv.py`)](#wnba-testswnbatest_wnba_epvpy)
  - [College (`tests/mbb/test_mbb_epv.py`)](#college-testsmbbtest_mbb_epvpy)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D24 EPV corpus

Real releases for `tests/nba/test_nba_epv.py`, written by `tools/models/nba_epv/train_epv.py`
(`write_fixture`) on 2026-11-05 from the hoopR-nba-stats-data checkout that publishes the
`nba_stats_*` releases (`nba_stats/{pbp,possessions,game_lineups}/parquet/`), zstd level 19.
Nothing is synthetic or edited: rows are only filtered and columns only dropped.

2025 (2024-25) is the holdout season of the bundled booster (trained on 2022-2024), so every
calibration check on these files is out of sample.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_pbp_2025.parquet` | 20,056 | `nba_stats_pbp/nba_play_by_play_2025` | The first 40 regular-season games of 2024-25 by `game_id` (`0022400001`-`0022400040`). Cols: what `epv_states` reads (`season, game_id, order_index, action_number, period, seconds_remaining, event_type, sub_type, description, shot_distance, shot_value, team_id, person_id, location, score_home, score_away`) plus the release's `off_player_1..5` / `def_player_1..5` (which `epv_states` ignores: each event takes its possession's starting ten). |
| `nba_possessions_2025.parquet` | 8,012 | `nba_stats_possessions/nba_possessions_2025` | The same games. Cols: `game_id, period, possession_number, number_in_period, start_order_index, end_order_index, start_seconds_remaining, end_seconds_remaining, possession_start_type, offense_team_id, defense_team_id, points, off_player_1..5, def_player_1..5`. |
| `nba_game_lineups_2025.parquet` | 20,056 | `nba_stats_game_lineups/nba_lineups_2025` | The same games. Cols: `game_id, action_number, home_player_1..5, away_player_1..5`. |

Dtypes: `game_id` is Utf8 in every file, `order_index`, `action_number`, the team and player ids are
Int64, scores are Utf8 as the feed ships them. Known gaps: the possessions release assigns about
0.16% of NBA possessions to the wrong offense (a shot by the defending team inside them), which EPV
inherits; and one basket in game `0022400030` is lowered by a replay review, which the running score
(the feed's cumulative max, BB-D7's rule) reads high for two possessions.

Files are named by the END year, as the releases stamp `season`. Regenerate by rerunning the fitting
script (it rewrites these files from the holdout season after the gates pass).

`nba_{pbp,possessions,game_lineups}_2025_technicals.parquet` (1,059 / 421 / 1,059 rows): games `0022400072` and
`0022400337` of the same releases, the same columns, cut by hand on 2026-11-05: in both a technical foul (and its free
throw) falls between an ordinary "1 of 2" and "2 of 2", and in `0022400337` a substitution comes between the shooting
foul and the technicals (order_index 140-146), so the trip's last free throw and its lineup can be checked.

## WNBA (`tests/wnba/test_wnba_epv.py`)

Written by `train_epv.py --league-id 10` (`write_fixture`) on 2026-11-06 from the wehoop-wnba-stats-data checkout:
the first 20 regular-season games of 2025 by `game_id` (`1022500001`-`1022500020`, played 2025-05-16 to 2025-05-23
on 8 dates), the holdout season of the bundled WNBA booster.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `wnba_pbp_2025.parquet` | 8,529 | `wnba_stats_pbp/wnba_play_by_play_2025` | The columns `epv_states` reads (the WNBA pbp has no player slots). |
| `wnba_possessions_2025.parquet` | 3,191 | `wnba_stats_possessions/wnba_possessions_2025` | The NBA fixture's columns. |
| `wnba_game_lineups_2025.parquet` | 8,529 | `wnba_stats_game_lineups/wnba_lineups_2025` | `game_id, action_number, home_player_1..5, away_player_1..5`. |
| `wnba_schedule_2025.parquet` | 20 | `wnba_stats_schedules/wnba_stats_schedule_2025` | `game_id` and `game_order`, the `game_date` parsed to a Date (what the level reads). |

## College (`tests/mbb/test_mbb_epv.py`)

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `mbb_pbp_2025.parquet` | 9,972 | `ncaa_mbb_pbp/ncaa_mbb_pbp_2025` | The first 20 games of 2024-25 by `contest_id`, written by `train_epv.py --league-id mbb` (`write_college_fixture`) on 2026-11-05 from the ncaa-mbb-hoops-data checkout. Cols: what `ncaa_epv_inputs` reads (`mbb_epv._PBP_COLS`). Two games have a non-D1 opponent (no ESPN team id). |
| `wbb_pbp_2025.parquet` | 10,082 | `ncaa_wbb_pbp/ncaa_wbb_pbp_2025` | The same for the women (ncaa-wbb-hoops-data), `--league-id wbb`. |
| `mbb_pbp_2025_quirks.parquet` | 916 | `ncaa_mbb_pbp/ncaa_mbb_pbp_2025` | Cut by hand on 2026-11-05: game `5722807` (Ole Miss v Colorado St.), whose `home_score` is the away team's and which has a wrong-basket two, and `5722014`, where the free throws after a foul on a turnover are stamped on the fouled-out side's possession. |

2025 is the holdout season of both college boosters (trained on 2022-2024). Nothing is synthetic or edited.
