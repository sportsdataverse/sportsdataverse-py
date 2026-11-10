<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [bb_official_seasons](#bb_official_seasons)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# bb_official_seasons

Real ESPN release captures for the BB-D34 official season profiles
(`tests/nba/test_nba_official_seasons.py`), read from the sportsdataverse-data release files on 2026-11-10:

| file | release | rows kept |
| --- | --- | --- |
| `{nba,wnba}_officials_2025.parquet` | `espn_{nba,wnba}_officials/officials_2025` | the whole season |
| `{nba,wnba}_team_box_2025.parquet` | `espn_{nba,wnba}_team_boxscores/team_box_2025` | the whole season, 13 columns |
| `{nba,wnba}_schedule_2025.parquet` | `espn_{nba,wnba}_schedules/{nba,wnba}_schedule_2025` | the whole season, 8 columns |
| `{mbb,wbb}_*_2026.parquet` | the matching `espn_{mens,womens}_college_basketball_*` 2026 files | games dated in November 2025 |

Columns are trimmed to what the profile reads; values are untouched. WBB officials have no 2024 or
2025 release file, hence 2026 for both college leagues.
