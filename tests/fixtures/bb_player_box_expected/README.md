<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [bb_player_box_expected fixtures](#bb_player_box_expected-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# bb_player_box_expected fixtures

Real captures for `tests/nba/test_nba_player_box_expected.py` (`sportsdataverse._common.player_box_expected`).

- `<lg>_player_box_<season>_team<id>.parquet`: every row of one team's season in the ESPN player
  box release (`load_<lg>_player_boxscore`, sportsdataverse-data), the columns the core reads plus
  `athlete_display_name`. NBA 2025 Boston (2), WNBA 2025 Las Vegas (17), MBB 2026 Duke (150),
  WBB 2026 UConn (41); regular season and postseason. Captured 2026-11-13.
- `<lg>_fit.json`: the output of `tools/fit_player_box_expected.py --league <lg> --write` on the
  releases of 2026-11-13: the train / holdout spans, the model coefficients (`coefs`, pasted into
  `EXPECTED_COEFS`), the per-36 gates (`per36_gates`, pasted into `PER36_MIN_N`) with each curve's
  `M`, and the holdout scores of the model against the per-minute baseline.
