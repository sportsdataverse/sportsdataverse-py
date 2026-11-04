<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D6 game-impact corpus](#bb-d6-game-impact-corpus)
  - [Games](#games)
  - [Id dtypes](#id-dtypes)
  - [Known gaps (left in on purpose)](#known-gaps-left-in-on-purpose)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D6 game-impact corpus

Real captures for `tests/nba/test_nba_game_impact.py` and `tests/mbb/test_mbb_game_impact.py`,
downloaded from the sportsdataverse-data releases on 2026-11-04 and projected to the columns the
game-impact builders read (zstd level 19). Nothing is synthetic or edited: rows are only filtered
(whole games) and columns only dropped.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_pbp_2025.parquet` | 7,932 | `nba_stats_pbp/nba_play_by_play_2026` | 16 games of 2025-26 (15 regular season, 1 playoff). Cols: `game_id, order_index, action_number, period, seconds_remaining, score_home, score_away, is_free_throw, is_foul, season`. |
| `nba_game_lineups_2025.parquet` | 7,932 | `nba_stats_game_lineups/nba_lineups_2026` | The same games, every column. |
| `nba_possessions_2025.parquet` | 3,175 | `nba_stats_possessions/nba_possessions_2026` | The same games. Cols: `game_id, offense_team_id, defense_team_id, off_player_1..5, def_player_1..5, season`. |
| `nba_schedule_2025.parquet` | 16 | `nba_stats_schedules/nba_schedule_2026` | Cols: `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts`. |
| `nba_player_box_2025.parquet` | 435 | `nba_stats_player_boxscores/player_boxscores_2026` | stats.nba.com's box score, the +/- oracle. Cols: `game_id, team_id, person_id, minutes, plus_minus_points`. |
| `wnba_{pbp,game_lineups,possessions,schedule,player_box}_2026.parquet` | 6,704 / 6,704 / 2,499 / 16 / 391 | `wnba_stats_pbp/wnba_play_by_play_2026`, `wnba_stats_game_lineups/wnba_lineups_2026`, `wnba_stats_possessions/wnba_possessions_2026`, `wnba_stats_schedules/wnba_schedule_2026`, `wnba_stats_player_boxscores/player_boxscores_2026` | 16 games of the 2026 WNBA season (15 regular season, 1 playoff), same columns. |
| `ncaa_mbb_pbp_2025.parquet` / `ncaa_wbb_pbp_2025.parquet` | 6,258 / 5,944 | `ncaa_{mbb,wbb}_pbp/ncaa_{mbb,wbb}_pbp_2025` | 12 D1-vs-D1 games each, plus one men's D1-vs-non-D1 game. Cols: `contest_id, season, period, game_seconds, home_score, away_score, event_type, foul_class, {home,away}_{1..5}_player_id, home_espn_team_id, away_espn_team_id`. |
| `ncaa_{mbb,wbb}_possessions_2025.parquet` | 1,827 / 1,762 | `ncaa_{mbb,wbb}_possessions/ncaa_{mbb,wbb}_possessions_2025` | The same contests. Cols: `contest_id, season, poss_team, home, away, {home,away}_{1..5}_player_id, home_espn_team_id, away_espn_team_id`. |
| `ncaa_{mbb,wbb}_team_box_2025.parquet` | 26 / 24 | `ncaa_{mbb,wbb}_team_box/ncaa_{mbb,wbb}_team_box_2025` | The same contests' final points, the margin oracle. Cols: `contest_id, season, team_espn_team_id, pts`. |

## Games

- NBA: `0022500013 0022500136 0022500188 0022500355 0022500559 0022500575 0022500631 0022500632
  0022500775 0022500840 0022500913 0022500986 0022501051 0022501132 0022501173 0042500402`
  (`schedule.season_type in (regular-season, playoffs)`, `game_id.sample(16, seed=6)`).
- WNBA: `1022600003 1022600010 1022600044 1022600054 1022600105 1022600157 1022600173 1022600176
  1022600213 1022600228 1022600248 1022600268 1022600285 1022600306 1022600316 1042600306` (same rule).
- MBB: `5722385 5723822 5726007 5732021 5732485 5732496 5733275 5734217 5735233 5736113 5737410
  5738251`, plus `5722009` (home ESPN 151 against a non-D1 visitor). WBB: `5722076 5723697 5725953
  5733371 5733698 5733707 5734923 5735614 5736407 5736837 5737290 5737984`. (D1-vs-D1 contests,
  sorted, then `sample(12, seed=6)`.)

## Id dtypes

| Column | NBA / WNBA | MBB | WBB |
|---|---|---|---|
| game id | `game_id` String | `contest_id` String | `contest_id` String |
| team id | Int64 (stats.nba.com) | `*_espn_team_id` Int32 | `*_espn_team_id` String (`team_box`: String) |
| player id | Int64 (stats.nba.com) | `*_player_id` String (stats.ncaa.org) | `*_player_id` String |
| score | `score_home` / `score_away` String (cast to Int64, forward-filled) | `home_score` Int64 | `home_score` Int64 |

The builders cast every team id to Int64 and keep each league's player id dtype.

## Known gaps (left in on purpose)

- Lineups with fewer than five distinct players: NBA games `0022500559` and `0022500632`; MBB
  `5722009`, `5723822`, `5737410`, `5738251`; WBB `5722076`, `5723697`, `5737290`. The identities
  in the tests are asserted on the other games, and the set is recomputed and compared.
- Duplicate `(game_id, action_number)` keys in the stats.nba.com pbp and lineups (402 NBA, 365
  WNBA rows); the duplicates add no clock or score, and the builder keeps the first lineup per key.
- Negative score deltas (official score corrections): 3 NBA rows, 1 WNBA row. They are kept, so a
  team's points over a game equal its final score.
- NBA on-court lineups that disagree with the box score minutes (2025-26 lineups are mostly
  inferred from substitutions): the source of the 26 NBA player-games whose +/- misses the box.

Season naming: the NBA files are named by the loader's START year (`load_nba_stats_pbp(2025)`
reads the `_2026` assets, whose `season` column is 2026).
