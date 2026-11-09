<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WNBA 2003 leaguegamelog slice](#wnba-2003-leaguegamelog-slice)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WNBA 2003 leaguegamelog slice

Real rows for the team-minutes fallback test in `tests/nba/test_nba_box_logs.py`. They were read on
2026-11-09 from the committed wehoop-wnba-stats-raw store (`wnba_stats/json/leaguegamelog/2003/regular-season{,_p}.json`)
through `nba_box_logs("2003", league_id="10", fetch=...)`, keeping the first four games (`1020300001` to `1020300004`).
Rows are untouched and columns are only dropped.

| File | Rows | Note |
|---|---:|---|
| `wnba_2003_player_logs_slice.parquet` | 78 | one row per player-game |
| `wnba_2003_team_logs_slice.parquet` | 8 | every team-game `min` is 0 in this capture (1997-2004 have many 0 or 12); the players' minutes sum to 200 |
