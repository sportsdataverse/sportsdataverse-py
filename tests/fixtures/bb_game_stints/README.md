<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D32 game-stints corpus](#bb-d32-game-stints-corpus)
  - [Observed on these games](#observed-on-these-games)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D32 game-stints corpus

Real captures for `tests/nba/test_nba_game_stints.py` and `tests/mbb/test_mbb_game_stints.py`,
downloaded from the sportsdataverse-data releases on 2026-11-09 with the package loaders and
projected to the columns the stint and shot-zone builders read (zstd level 19). Nothing is
synthetic or edited: rows are only filtered (whole games) and columns only dropped.

The games are the BB-D6 ones (`tests/fixtures/bb_game_impact/README.md`, which lists them). The
tests read the lineups, schedule and box scores from that folder and the college shot-count team
box from `tests/fixtures/bb_lineup_seasons`; this folder adds only what those files lack.

| File | Rows | Release asset (loader) | Cols |
|---|---:|---|---|
| `nba_pbp_2025.parquet` | 7,932 | `nba_stats_pbp/nba_play_by_play_2026` (`load_nba_stats_pbp(2025)`) | `game_id, order_index, action_number, period, seconds_remaining, score_home, score_away, is_free_throw, is_foul, team_id, is_field_goal, is_made_shot, shot_value, shot_distance, season` |
| `nba_possessions_2025.parquet` | 3,175 | `nba_stats_possessions/nba_possessions_2026` | `game_id, period, offense_team_id, defense_team_id, start_order_index, off_player_1..5, def_player_1..5, fg2a, fg2m, fg3a, fg3m, fta, oreb, dreb, tov, season` |
| `wnba_pbp_2026.parquet` / `wnba_possessions_2026.parquet` | 6,704 / 2,499 | `wnba_stats_pbp/wnba_play_by_play_2026`, `wnba_stats_possessions/wnba_possessions_2026` | the NBA columns |
| `ncaa_{mbb,wbb}_pbp_2025.parquet` | 6,258 / 5,944 | `ncaa_{mbb,wbb}_pbp/ncaa_{mbb,wbb}_pbp_2025` | `contest_id, season, period, game_seconds, home, away, home_score, away_score, event_team, event_type, event_result, shot_value, foul_class, poss_num, {home,away}_{1..5}_player_id, home_espn_team_id, away_espn_team_id`, in release order (the BB-D6 rows, same order) |
| `ncaa_{mbb,wbb}_possessions_2025.parquet` | 1,827 / 1,762 | `ncaa_{mbb,wbb}_possessions/ncaa_{mbb,wbb}_possessions_2025` | `contest_id, season, poss_num, poss_team, home, away, {home,away}_{1..5}_player_id, home_espn_team_id, away_espn_team_id` |

## Observed on these games

- Every possession's `start_order_index` is a pbp `order_index`; the lineup at that action is the
  possession's own on 99.5% of the NBA possessions (100% WNBA), so 312 of the 330 NBA player-games
  have the impact box's possessions (all 309 WNBA ones). Points and clock match it on every one.
- Every college possession's `poss_num` has pbp events, and the first one's lineup is the
  possession's (as sets) on every possession.
- Each pbp field goal falls inside a possession of the shooting team (NBA and WNBA), and the pbp
  twos and threes per possession equal its `fg2a` / `fg3a`. On the whole 2025-26 NBA season 70 of
  247,825 field goals are credited to a possession of the other team; the shot zones skip them.

Season naming: the NBA files are named by the loader's START year (`load_nba_stats_pbp(2025)`
reads the `_2026` assets, whose `season` column is 2026).
