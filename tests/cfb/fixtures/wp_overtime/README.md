# WP overtime holdout rows

Written by `tools/fit_cfb_wp_overtime.py --fixture-dir tests/cfb/fixtures/wp_overtime`
from the published `cfbfastR-cfb-data` play-by-play (`cfb/pbp/parquet/play_by_play_{season}.parquet`)
and schedules (`home_winner`, final games only), seasons 2022-2025. That is the holdout: the
overtime correction is fitted on 2004-2021 only.

- `tied_last_2m_2022_2025.parquet`: fourth-quarter scrimmage snaps with the score tied and
  `start.adj_TimeSecsRem <= 120`. It holds the WP feature columns (`model_vars.wp_start_columns`),
  the published pre-fix `wp_before` / `wp_before_naive` (the regulation booster's output), and
  `won` (the team with the ball won).
- `q4_one_score_2022_2025.parquet`: every fourth-quarter scrimmage snap within eight points
  (43,182 rows, 1,830 games), with the WP feature columns, the published `wp_before` and `won`.
  The correction must hold or improve calibration across all of it, not only in tied states.
- `overtime_2022_2025.parquet`: overtime scrimmage snaps (`period >= 5`, 1 to 99 yards to go). It
  holds the margin, down, distance, yards to goal, pregame spread, `ot_second` (the team is second
  in its period, by the rule in `cfb_wp_overtime.ot_first_team`), the published `wp_before`, and
  `won`.

`tests/cfb/test_cfb_wp_endgame.py` scores these rows through the shipped runtime
(`cfb_wp_overtime.adjust_wp` / `ot_live_wp`). A change to the overtime algebra or its
constants therefore shows up there, not only in the card.
