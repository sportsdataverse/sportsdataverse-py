<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D27 game-flow corpus](#bb-d27-game-flow-corpus)
  - [Games](#games)
  - [Id dtypes](#id-dtypes)
  - [Coverage on the full releases (2025-26 college, 2025-26 NBA, 2025 WNBA)](#coverage-on-the-full-releases-2025-26-college-2025-26-nba-2025-wnba)
  - [Known gaps (left in on purpose)](#known-gaps-left-in-on-purpose)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D27 game-flow corpus

Real captures for `tests/nba/test_nba_game_flow.py` and `tests/mbb/test_mbb_game_flow.py`, downloaded
on 2026-11-04 and projected to the columns the game-flow builders read (zstd level 19). Nothing is
synthetic or edited: rows are only filtered (whole games) and columns only dropped.

| File | Rows | Release asset / source | Kept |
|---|---:|---|---|
| `nba_pbp_2026.parquet` | 51,778 | `nba_stats_pbp/nba_play_by_play_2026` | 103 games of 2025-26. Cols: `season, game_id, order_index, period, seconds_remaining, event_type, team_id, score_home, score_away`. |
| `nba_possessions_2026.parquet` | 20,621 | `nba_stats_possessions/nba_possessions_2026` | The same games. Cols: `season, game_id, period, possession_number, offense_team_id, defense_team_id, end_seconds_remaining, fg2m, fg3m, ftm, fg2a, fg3a, fta, oreb, dreb, tov`. |
| `nba_schedule_2026.parquet` | 103 | `nba_stats_schedules/nba_schedule_2026` | Cols: `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts`. |
| `nba_live_boxscore_flow_2026.parquet` | 206 | `https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json` via `nba_live_boxscore` | The official team flow stats, the oracle: `game_id, team_id, score, biggest_lead, biggest_scoring_run, lead_changes, time_leading, times_tied`. |
| `wnba_{pbp,possessions,schedule,live_boxscore_flow}_2025.parquet` | 25,242 / 9,682 / 62 / 124 | the `wnba_stats_*` `_2025` assets; `cdn.wnba.com` via `wnba_live_boxscore` | 62 games of the 2025 WNBA season, same columns. |
| `wnba_{pbp,possessions,schedule}_2002.parquet` | 933 / 334 / 2 | the `wnba_stats_*` `_2002` assets | Two 2002 games, played in two 20-minute halves (1020200001 went to overtime). |
| `ncaa_mbb_pbp_2026.parquet` / `ncaa_wbb_pbp_2026.parquet` | 20,060 / 20,702 | `ncaa_{mbb,wbb}_pbp/ncaa_{mbb,wbb}_pbp_2026` | 42 contests each. Cols: `season, contest_id, period, game_seconds, home_score, away_score, event_team_espn_team_id, home_espn_team_id, away_espn_team_id, espn_game_id`. |
| `ncaa_mbb_possessions_2026.parquet` / `ncaa_wbb_possessions_2026.parquet` | 5,794 / 5,966 | `ncaa_{mbb,wbb}_possessions/ncaa_{mbb,wbb}_possessions_2026` | The same contests. Cols: `season, contest_id, espn_game_id, period, poss_num, last_event_time, poss_team, home, away, poss_team_espn_team_id, home_espn_team_id, away_espn_team_id, pts`. |
| `{mbb,wbb}_schedule_2026.parquet` | 42 / 42 | `espn_{mbb,wbb}_schedules` 2026 | The contests' ESPN games: `game_id, neutral_site`. |
| `{mbb,wbb}_team_box_2026.parquet` | 84 / 84 | `espn_{mbb,wbb}_team_boxscores` 2026 | ESPN's team box, the college oracle: `game_id, team_id, team_score, largest_lead, lead_changes, lead_percentage`. |

## Games

- NBA: the 100 games `schedule.filter(season_type in (regular-season, playoffs), home_pts not null)
  ["game_id"].sort().sample(100, seed=27)` (95 regular season, 5 playoffs; the sample holds
  `0022500048`, a 12-2 official run, and `0022500109`, a stale replay score, which the tests name),
  plus three forced in: `0022500232` (a stale end-of-3rd score) and `0022500013` / `0022500125` (rows
  where both scores move at once).
- WNBA 2025: the same rule, `sample(60, seed=27)` (55 regular season, 5 playoffs), plus `1022500008`
  (a review takes 3 points off mid-run; the official run is 11) and `1022500151` (both scores moving
  on one row).
- MBB / WBB: 40 contests with two ESPN team ids and an ESPN `lead_changes`, `sample(40, seed=27)` of the
  sorted contest ids, plus the first neutral-site one and the first with a non-D1 side (MBB `6398358`,
  WBB `6387146`). Seven MBB contests are at neutral sites.

## Id dtypes

| Column | NBA / WNBA | MBB | WBB |
|---|---|---|---|
| game id | `game_id` String | `contest_id` String; `espn_game_id` Int32 | `contest_id` String; `espn_game_id` String |
| team id | Int64 (stats.nba.com) | `*_espn_team_id` Int32 | `*_espn_team_id` String |
| score | `score_home` / `score_away` String (cast, carried forward) | `home_score` Int64 | `home_score` Int64 |
| ESPN schedule / team box | | `game_id` / `team_id` Int32 | `game_id` / `team_id` Int32 |

The builders cast every team id and `espn_game_id` to Int64.

## Coverage on the full releases (2025-26 college, 2025-26 NBA, 2025 WNBA)

- Every college contest of the pbp release has an `espn_game_id` that is in the ESPN schedule but one
  (6,297 / 6,297 men's, 6,018 / 6,019 women's). The fixture's 100% join to ESPN's team box is
  pre-filtered; on the full release ESPN's box carries `lead_changes` for 75% of team-games.
- NBA: 1,315 of 1,315 games have a cdn.nba.com box score; WNBA 310 of 310.

## Known gaps (left in on purpose)

- ESPN writes `lead_changes` 0 and `lead_percentage` 0.0 for some games (MBB `6459088`, a team that led
  96% of it); the tests list and recompute the set.
- MBB `6407871`: a neutral-site contest whose NCAA home/away sides are the reverse of ESPN's; the
  release's ESPN ids follow ESPN and its scores follow the NCAA, so the teams' rows are swapped (its
  points disagree with ESPN's box) and it is left out of the ESPN comparison. On the full 2025-26
  release that happens in 45 of 6,297 men's and 65 of 6,018 women's contests.
- The official `biggestScoringRun` lets the opponent score a little (a 12-2 run counts 12), so
  `max_run` (an n-0 run) is checked to be no larger, and equal on the observed count.

Season naming: the NBA files are named by the season's end year (`load_nba_stats_pbp(2025)` reads the
`_2026` assets, whose `season` column is 2026).
