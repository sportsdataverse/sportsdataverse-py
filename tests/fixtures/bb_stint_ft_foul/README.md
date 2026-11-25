<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Free throws on the lineup at their foul](#free-throws-on-the-lineup-at-their-foul)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Free throws on the lineup at their foul

Real captures for `tests/nba/test_nba_game_stints_ft_foul.py`, downloaded from the
sportsdataverse-data releases (<https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_pbp>,
`.../tag/nba_stats_game_lineups`, `.../tag/nba_stats_possessions`, `.../tag/nba_stats_schedules`,
`.../tag/nba_stats_player_boxscores`) on 2026-11-24 with the package loaders (zstd level 19). Nothing is
synthetic or edited: rows are only filtered to whole games, and every column is kept. Files are named
by the loader season (the start year), one season per file.

| Game | Season files | What it pins |
|---|---|---|
| `0022500024` (2025-26) | `nba_*_2025.parquet` | LaRavia FOR Smart and Knecht FOR Doncic are recorded between Reaves' two free throws at 0:07.5 of the 4th. The lineup at his second (order 491) no longer lists him, so the free throw's box counts must follow its points onto the lineup at the foul. |
| `0022400149` (2024-25) | `nba_*_2024.parquet` | Durant's and-one at 11:33 of the 2nd: the free throw (152) comes one row before its shooting foul (153). The game's most recent foul before it is from the 1st quarter (140), which is not its own: clock has run in the 2nd. |

| File | Loader |
|---|---|
| `nba_pbp_<s>.parquet` | `load_nba_stats_pbp(s)` |
| `nba_game_lineups_<s>.parquet` | `load_nba_stats_game_lineups(s)` |
| `nba_possessions_<s>.parquet` | `load_nba_stats_possessions(s)` |
| `nba_schedules_<s>.parquet` | `load_nba_stats_schedules(s)` |
| `nba_player_box_<s>.parquet` | `load_nba_stats_player_boxscores(s)` (stats.nba.com's box, the oracle) |
