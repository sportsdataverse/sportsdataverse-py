<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WNBA season-loader fixtures](#wnba-season-loader-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WNBA season-loader fixtures

- `leaguegamelog_2026_sample.parquet` — 6 real rows (3 games) captured
  2026-07-22 from the published `wnba_stats_schedules` 2026 release
  (leaguegamelog shape: two team-rows per game, UPPERCASE columns,
  stats.wnba.com game-id namespace). Gates the
  `games_from_leaguegamelog` pivot offline; the full season path is
  live-gated in `tests/nba/test_nba_season_glue.py`.
- `nba_schedule_2025_sample.parquet` — 3 real rows (3 completed 2025-26
  regular-season games) captured 2026-07-22 from the published
  `nba_stats_schedules` release (league-schedule shape: one row per game,
  `home_team_*`/`away_team_*` columns, `game_status` 3 = final). Gates
  the `games_from_nba_schedule` pivot offline.
