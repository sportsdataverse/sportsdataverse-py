<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D24 EPV corpus](#bb-d24-epv-corpus)
  - [WNBA (`tests/wnba/test_wnba_epv.py`)](#wnba-testswnbatest_wnba_epvpy)
  - [College (`tests/mbb/test_mbb_epv.py`)](#college-testsmbbtest_mbb_epvpy)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D24 EPV corpus

Real releases for `tests/nba/test_nba_epv.py`, written by `tools/models/nba_epv/train_epv.py`
(`write_fixture`) on 2026-11-17 from the `nba_stats_*` releases as published that day (the loaders;
`--data-root` reads the hoopR-nba-stats-data checkout's `nba_stats/{pbp,possessions,game_lineups}/parquet/`
instead, the same files), zstd level 19.
Nothing is synthetic or edited: rows are only filtered and columns only dropped.

2025 (2024-25) is the holdout season of the bundled booster (trained on 2022-2024), so every
calibration check on these files is out of sample.

**Recaptured 2026-11-17, with the label fix.** The republished possessions releases credit a score
by the defense inside a possession (a technical or flagrant free throw, or a basket a misplaced
boundary puts there) to the scoring team's next possession, so each game's `points` add up to its
final score. The EPV start label (`remaining_points` at `step` 0) now does the same
(`nba_epv._carried_points`); before that fix it missed the point. 4 of the 8,012 NBA possessions
here carry one (`0022400002` 210, `0022400019` 209, `0022400022` 133, `0022400030` 93) and 3 of the
3,191 WNBA ones (`1022500002` 115, `1022500013` 71 and 129); the tests pin them.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_pbp_2025.parquet` | 20,056 | `nba_stats_pbp/nba_play_by_play_2025` | The first 40 regular-season games of 2024-25 by `game_id` (`0022400001`-`0022400040`). Cols: what `epv_states` reads (`season, game_id, order_index, action_number, period, seconds_remaining, event_type, sub_type, description, shot_distance, shot_value, team_id, person_id, location, score_home, score_away`) plus the release's `off_player_1..5` / `def_player_1..5` (which `epv_states` ignores: each event takes its possession's starting ten). |
| `nba_possessions_2025.parquet` | 8,012 | `nba_stats_possessions/nba_possessions_2025` | The same games. Cols: `game_id, period, possession_number, number_in_period, start_order_index, end_order_index, start_seconds_remaining, end_seconds_remaining, possession_start_type, offense_team_id, defense_team_id, points, count_as_possession, off_player_1..5, def_player_1..5`. |
| `nba_game_lineups_2025.parquet` | 20,056 | `nba_stats_game_lineups/nba_lineups_2025` | The same games. Cols: `game_id, action_number, home_player_1..5, away_player_1..5`. |

Dtypes: `game_id` is Utf8 in every file, `order_index`, `action_number`, the team and player ids are
Int64, scores are Utf8 as the feed ships them. Known gaps: the possessions release assigns 0.013% of
the 2024-25 NBA possessions to the wrong offense (a shot by the defending team inside them, 31 of
243,735; none in these games; 0.16% in the 2026-11-05 capture), which EPV inherits; and one basket in game `0022400030` is lowered by a replay review, which the running score
(the feed's cumulative max, BB-D7's rule) reads high for two possessions.

Files are named by the END year, as the releases stamp `season`. Regenerate by rerunning the fitting
script (it rewrites these files from the holdout season after the gates pass).

`nba_{pbp,possessions}_2025_carries.parquet` (991 / 381 rows): games `0022400267` and `0022400770` of the 2026-11-17
releases, the columns of the files above, cut by hand that day: in both the defense scores where the scoring team has
no later possession in the period, so the release credits it to an earlier one (195 and 96).

`nba_{pbp,possessions,game_lineups}_2025_technicals.parquet` (1,059 / 421 / 1,059 rows): games `0022400072` and
`0022400337` of the 2026-11-05 releases (not recaptured; no `count_as_possession`), cut by hand that day: in both a technical foul (and its free
throw) falls between an ordinary "1 of 2" and "2 of 2", and in `0022400337` a substitution comes between the shooting
foul and the technicals (order_index 140-146), so the trip's last free throw and its lineup can be checked.

## WNBA (`tests/wnba/test_wnba_epv.py`)

Written by `train_epv.py --league-id 10` (`write_fixture`) on 2026-11-17 from that day's `wnba_stats_*` releases:
the first 20 regular-season games of 2025 by `game_id` (`1022500001`-`1022500020`, played 2025-05-16 to 2025-05-23
on 8 dates), the holdout season of the bundled WNBA booster.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `wnba_pbp_2025.parquet` | 8,529 | `wnba_stats_pbp/wnba_play_by_play_2025` | The columns `epv_states` reads (the WNBA pbp has no player slots). |
| `wnba_possessions_2025.parquet` | 3,191 | `wnba_stats_possessions/wnba_possessions_2025` | The NBA fixture's columns. |
| `wnba_game_lineups_2025.parquet` | 8,529 | `wnba_stats_game_lineups/wnba_lineups_2025` | `game_id, action_number, home_player_1..5, away_player_1..5`. |
| `wnba_schedule_2025.parquet` | 20 | `wnba_stats_schedules/wnba_schedule_2025` (one row per game) | `game_id, season, season_type, game_date` as released; the test parses `game_date` with `wnba_epv._game_order`. |

## College (`tests/mbb/test_mbb_epv.py`)

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `mbb_pbp_2025.parquet` | 9,972 | `ncaa_mbb_pbp/ncaa_mbb_pbp_2025` | The first 20 games of 2024-25 by `contest_id`, written by `train_epv.py --league-id mbb` (`write_college_fixture`) on 2026-11-05 from the ncaa-mbb-hoops-data checkout. Cols: what `ncaa_epv_inputs` reads (`mbb_epv._PBP_COLS`). Two games have a non-D1 opponent (no ESPN team id). |
| `wbb_pbp_2025.parquet` | 10,082 | `ncaa_wbb_pbp/ncaa_wbb_pbp_2025` | The same for the women (ncaa-wbb-hoops-data), `--league-id wbb`. |
| `mbb_pbp_2025_quirks.parquet` | 916 | `ncaa_mbb_pbp/ncaa_mbb_pbp_2025` | Cut by hand on 2026-11-05: game `5722807` (Ole Miss v Colorado St.), whose `home_score` is the away team's and which has a wrong-basket two, and `5722014`, where the free throws after a foul on a turnover are stamped on the fouled-out side's possession. |

2025 is the holdout season of both college boosters (trained on 2022-2024). Nothing is synthetic or edited.
