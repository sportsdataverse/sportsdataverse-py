<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D22 team-luck corpus](#bb-d22-team-luck-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D22 team-luck corpus

Real captures for `tests/nba/test_nba_team_luck.py`, `tests/mbb/test_mbb_team_luck.py` and
`tests/tools/test_fit_team_luck_pythag.py`, built on 2026-11-04 from the sportsdataverse-data
releases (zstd level 19). Nothing is synthetic: the records are sums of real final scores, and
the ESPN windows are real plays, only filtered, with columns dropped and the score before each
play carried from the previous play.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `{nba,wnba,mbb,wbb}_season_records.parquet` | 892 / 365 / 15,325 / 12,478 | `nba_stats_schedules`, `wnba_stats_schedules` (regular season), `espn_{mens,womens}_college_basketball_schedules/{mbb,wbb}_schedule_*` (every completed game) | `tools/fit_team_luck_pythag.py --league <lg> --write`: one row per team-season of the fit's train + holdout span (`SPANS` there: NBA 1997-2026, WNBA 1997-2025, college 2002-2026), via `season_records`. Cols: `season, team_id, games, wins, losses, pts, opp_pts`. 0-0 rows (games never played) and two tied women's college rows are dropped by `season_records`. |
| `nba_espn_close_window_2026.parquet` / `wnba_espn_close_window_2025.parquet` | 71,409 / 15,910 | `espn_nba_pbp/play_by_play_2026`, `espn_wnba_pbp/play_by_play_2025` (`load_nba_pbp(2026)`, `load_wnba_pbp(2025)`), regular season (`season_type == 2`) | Every play of period 4 or later with `clock_minutes * 60 + clock_seconds <= 300`, for the 1,229 / 286 games matched to a stats.nba.com game of `../bb_team_season_rates/{nba,wnba}_schedule_*.parquet` by ET date and home team (`home_team_name + " " + home_team_mascot` = the stats `home_team_name`; 1 NBA game unmatched). Cols: `game_id` (ESPN), `stats_game_id`, `period_number`, `clock` (seconds left in the period), `home_before`, `away_before` (the score after the game's previous play, ESPN scores forward-filled within the game). |
| `mbb_espn_close_window_2025.parquet` / `wbb_espn_close_window_2025.parquet` | 23,495 / 21,867 | `espn_mens_college_basketball_pbp/play_by_play_2025`, `espn_womens_college_basketball_pbp/play_by_play_2025` (`load_mbb_pbp(2025)`, `load_wbb_pbp(2025)`) | The same window in period 2 or later (men) / 4 or later (women), for the 412 / 390 ESPN games of `../bb_team_season_rates/ncaa_{mbb,wbb}_possessions_2025.parquet` (the Big Ten teams' games). Same columns, no `stats_game_id`. |

Id dtypes: `team_id` Int64 (stats.nba.com franchise id, or ESPN team id in college); the stats
`game_id` / `stats_game_id` String (leading zeros kept, `0022500001`); the ESPN `game_id` String
here, cast from the releases' Int32 `game_id` / `espn_game_id` (no float origin, so no `.0`).

The ESPN windows are the independent recount of the close games: a different source from the
stats.nba.com / stats.ncaa.org possessions, with its own clock and score.
