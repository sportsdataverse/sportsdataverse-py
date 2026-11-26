<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [bb_tournament_runs](#bb_tournament_runs)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# bb_tournament_runs

Real release rows for BB-D43's college tournament runs (`sportsdataverse/_common/tournament_runs.py`),
captured 2026-11-25 with the package loaders and trimmed to the columns the cores read. Nothing is
synthetic.

| file | loader | rows kept |
|---|---|---|
| `mbb_schedule_2025.parquet` | `load_mbb_schedule(seasons=[2025])` | every row with a `tournament_id` or `season_type` 3: the postseason, the conference tournaments and the early-season events (636 rows) |
| `mbb_team_box_2025.parquet` | `load_mbb_team_boxscore(seasons=[2025])` | the tournament games, both sides (plus the early-season games that carry a conference group) |
| `mbb_player_box_2025.parquet` | `load_mbb_player_boxscore(seasons=[2025])` | every row of the four Final Four teams (Auburn 2, Florida 57, Duke 150, Houston 248), regular season and tournaments |
| `wbb_*_2025.parquet` | the `load_wbb_*` loaders, `seasons=[2025]` | the same cuts for 2024-25 women's (Final Four: UCLA 26, UConn 41, Texas 251, South Carolina 2579) |

Rebuild: load each season with the loader above, keep the rows in the last column and the columns
the tests and the cores read, and write with `compression="zstd"`.
