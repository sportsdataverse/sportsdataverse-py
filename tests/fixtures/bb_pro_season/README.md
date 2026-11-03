<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D18 pro season corpus](#bb-d18-pro-season-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D18 pro season corpus

Real captures for `tests/nba/test_nba_season_sim.py`, downloaded from the sportsdataverse-data
releases on 2026-11-03 and projected to the columns the season sim reads (zstd level 19).
Nothing is synthetic or edited: rows are only filtered and columns only dropped.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_schedule_2024.parquet` | 1,323 | `nba_stats_schedules/nba_schedule_2024` | Every `season_type` but `preseason`: regular season, NBA Cup final, All-Star, play-in, playoffs. |
| `nba_schedule_2026.parquet` | 1,329 | `nba_stats_schedules/nba_schedule_2026` | The same. |
| `wnba_schedule_2024.parquet` | 262 | `wnba_stats_schedules/wnba_schedule_2024` | Regular season and playoffs (the prior season for the 2025 lottery's two-season record). |
| `wnba_schedule_2025.parquet` | 310 | `wnba_stats_schedules/wnba_schedule_2025` | Regular season and playoffs. |

Columns: `game_id, season, season_type, game_date, home_team_id, away_team_id, home_pts, away_pts`.

The possessions are BB-D3's `../bb_team_season_rates` captures (NBA 2024 and 2026, WNBA 2025).
Season naming: files are named by the END year, as the releases stamp `season` (the NBA loaders
take the start year).
