<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Defense scores inside a possession (2025-26)](#defense-scores-inside-a-possession-2025-26)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Defense scores inside a possession (2025-26)

Real rows, not edited: four games of the `nba_stats_pbp` and `nba_stats_schedules`
sportsdataverse-data releases, read 2026-11-08 and written with zstd level 19.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `pbp_2026.parquet` | 2,086 | `nba_play_by_play_2026` (2025-26) | Every column of games `0022500003`, `0022500102`, `0022500157` and `0022500290`. |
| `schedule_2026.parquet` | 4 | `nba_schedule_2026` | `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts` and the two abbreviations. |

In each game the team on defense scores inside the other team's possession, and the published
`nba_stats_possessions` rows (built 2026-08-13) miss it, so the made-shot points
(`2*fg2m + 3*fg3m + ftm`) fall short of the final score:

| Game | Final (home-away) | Published made-shot points | What the defense scored |
|---|---|---|---|
| `0022500003` CLE @ NYK | 119-111 | 118-111 | A technical free throw. |
| `0022500102` CHA @ PHI | 125-121 | 123-121 | Two free throws after a same-clock foul kept the possession open. |
| `0022500157` MIL @ IND | 115-117 | 112-117 | A jumper: the pbp logs a putback before its own offensive rebound, which opens a phantom possession. |
| `0022500290` HOU @ PHX | 92-114 | 90-114 | A jumper: a team rebound after a shot-clock turnover opens a possession for the wrong team. |

With `credit_defense_scoring` each team's made-shot points equal the final score
(`tests/nba/test_nba_possessions.py::test_defense_scores_are_credited_to_the_scoring_team`).
