<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ncaa_mbb_possessions](#ncaa_mbb_possessions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ncaa_mbb_possessions

Two contests of the `ncaa_mbb_possessions` release (sportsdataverse-data release tag
`ncaa_mbb_possessions`, file `ncaa_mbb_possessions_2026.parquet`), read on 2026-11-25, for the
phantom-possession check (`tests/contracts/test_phantom_possessions.py`).

| file | rows kept | columns kept |
| --- | --- | --- |
| `phantom_contests_2026.parquet` | contest 6465160 (Howard and Hampton, 2025-12-13, ESPN 401827524; the season's most phantom possessions) and contest 6388769 (Buffalo and Southern Miss., 2025-11-03, ESPN 401823425; one phantom), 288 rows | `season, contest_id, espn_game_id, game_date, home, away, period, poss_num, poss_team, pts, start_event_type, first_shot_type, last_event_time, last_event_type` |

The rows are the release's, unmodified; only the other columns are dropped.
