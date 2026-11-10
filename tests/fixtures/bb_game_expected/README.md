<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D40 game expected points fixtures](#bb-d40-game-expected-points-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D40 game expected points fixtures

Used by `tests/nba/test_nba_game_expected.py` (`sportsdataverse.nba.nba_game_expected`).

| File | Source | Why |
|---|---|---|
| `wnba_shots_2024_2025.parquet` | sdv-db `tests/fixtures/shot_quality/` at `8ed58ef` (BB-D9, sdv-db#195), copied unchanged: every WNBA 2024 and 2025 regular-season shot of the `wnba_stats_shots` release assets (71,295 rows; season, game_id, team_id, team_tricode, person_id, shot_result, shot_value, shot_distance, x_legacy, y_legacy) | the league history the as-of cells count; 2024 is the window of 2025 |
| `wnba_game_logs_2024_2025.parquet` | the same sdv-db fixture directory: distinct (season, game_id, team_id, game_date) of the `wnba_stats_player_game_logs` assets (1,144 rows) | the game dates BB-D9 reads |
| `fit_wnba_stats.json` | the same sdv-db fixture directory: `scripts/fit_shot_quality.py`'s WNBA output | `holdout_scores["2025"]` is BB-D9's production SQL over the real 2025 season (n, Brier, calibration): the port must reproduce it |
| `wnba_401736238.json.gz`, `wnba_401736239.json.gz`, `wnba_401736240.json.gz` | `espn_wnba_pbp(game_id, raw=True)` captured 2026-11-10: 2025-07-10 MIN @ LA (stats 1022500127), 2025-07-10 LV @ WSH (1022500128), 2025-07-11 ATL @ IND (1022500129) | ESPN plays of games the shots fixture holds, for the ESPN-to-BB-D9 total check |
| `espn_vs_d9_wnba_2025.csv` | `tools/measure_game_expected_espn.py` (ESPN fetch, 2026-11-10): the first final game of every other day of June-August 2025, matched to its stats game by date and both teams' (FGA, field-goal points); 38 of 40 matched | the ESPN tolerance: per game the ESPN-located expected points run 0.978-1.006 of BB-D9's feed-located total (mean 0.996), with equal FGA in all 38 |

ESPN places shots to the whole foot, the league feed to the inch, so the two totals agree to about
a percent, not to the digit; the shot distance is read from the play text where it is given (the
feed's own distance), which moved the mean ratio from 0.990 to 0.996.
