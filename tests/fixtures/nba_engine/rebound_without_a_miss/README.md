<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Rebounds that follow no miss (2025-26)](#rebounds-that-follow-no-miss-2025-26)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Rebounds that follow no miss (2025-26)

Real rows, not edited: four games of the `nba_stats_pbp` and `nba_stats_schedules`
sportsdataverse-data releases, read 2026-11-08 and written with zstd level 19.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `pbp_2026.parquet` | 2,048 | `nba_play_by_play_2026` (2025-26) | Every column of games `0022500119`, `0022500157`, `0022500290` and `0022501144`. |
| `schedule_2026.parquet` | 4 | `nba_schedule_2026` | `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts` and the two abbreviations. |

In each game a rebound row that follows no missed shot opens a possession and decides its offense,
so the other team's next basket lands in a possession credited to the wrong team and is lost. The
published `nba_stats_possessions` made-shot points (`2*fg2m + 3*fg3m + ftm`) fall short:

| Game | Final (home-away) | Published made-shot points | The rebound row |
|---|---|---|---|
| `0022500157` MIL @ IND | 115-117 | 112-117 | Order 467, logged after its own team's made putback (order 466, same clock). |
| `0022500290` HOU @ PHX | 92-114 | 90-114 | Order 235, `Rockets Rebound` after `Turnover: Shot Clock` (a placeholder). |
| `0022501144` DET @ ORL | 123-107 | 121-107 | Order 54, a rebound by the team that just lost the ball on a steal. |

`0022500119` OKC @ DAL (94-101) is the other side: its last group is a team field-goal attempt at
0.3 s, a team rebound (order 487) and the period end, so the rebound is the only row with a side and
still names the offense (`test_a_rebound_that_follows_no_miss_sets_the_offense_when_nothing_else_can`).

With the fix each team's made-shot points equal the final score and the possession count is
unchanged (`tests/nba/test_nba_possessions.py::test_rebounds_that_follow_no_miss_do_not_open_a_possession`).
