<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [bb_playoff_series](#bb_playoff_series)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# bb_playoff_series

Real release rows for BB-D43 (`sportsdataverse/_common/playoff_series.py`), captured 2026-11-25
with the package loaders and trimmed to the columns the cores read. Nothing is synthetic.

| file | loader | rows kept |
|---|---|---|
| `nba_schedule_2025.parquet` | `load_nba_schedule(seasons=[2025])` | every non-regular-season game: 84 postseason (`season_type` 3) and 6 play-in (`season_type` 5) |
| `nba_team_box_2025.parquet` | `load_nba_team_boxscore(seasons=[2025])` | the postseason games (168 rows) |
| `nba_player_box_2025.parquet` | `load_nba_player_boxscore(seasons=[2025])` | regular season and postseason rows of every player with a postseason row |
| `wnba_*_2025.parquet` | the `load_wnba_*` loaders, `seasons=[2025]` | the same cuts for the 2025 WNBA season (24 postseason games, no play-in) |

Rebuild: load each season with the loader above, keep the rows in the last column and the
columns the tests and the cores read, and write with `compression="zstd"`.
