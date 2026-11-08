<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Legacy score-string dialect (pre-2017-18)](#legacy-score-string-dialect-pre-2017-18)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Legacy score-string dialect (pre-2017-18)

Real rows, not edited: three games of the `nba_stats_pbp` / `wnba_stats_pbp` and
`nba_stats_schedules` / `wnba_stats_schedules` sportsdataverse-data releases, read 2026-11-06 and
written with zstd level 19.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `pbp_2010.parquet` | 1,333 | `nba_play_by_play_2010` (2009-10) and `wnba_play_by_play_2010` | Every column of games `0020900009` (POR @ MIA), `0020900017` (UTA @ DEN) and `1021000022` (IND @ ATL). |
| `schedule_2010.parquet` | 3 | `nba_schedule_2010`, `wnba_schedule_2010` | `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts`. |

Before 2017-18 (the WNBA before 2017) stats.nba.com writes the literal string `"0"` as both
scores on every non-scoring row, missed free throws included. The published possessions for these
games count every free throw as made (`ftm == fta`); with the fix each team's made-shot points
equal the final score (`tests/nba/test_nba_possessions.py::test_legacy_dialect_missed_free_throws_are_misses`).
