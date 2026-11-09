<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D21 situational-splits corpus](#bb-d21-situational-splits-corpus)
  - [Observed on these files (the test gates)](#observed-on-these-files-the-test-gates)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D21 situational-splits corpus

Real captures for `tests/nba/test_nba_situational.py` and `tests/mbb/test_mbb_situational.py`
(BB-D21), read on 2026-11-05 and written with zstd level 19. Nothing is synthetic or edited: rows
are only filtered (whole games) and columns only dropped.

The NBA tests also read three earlier corpora, the same release rows column-split across files:
`../bb_possession_flags/nba_possessions_2024.parquet` (lineups, start types, clocks),
`../bb_possession_flags/nba_pbp_2024.parquet`, `../bb_team_season_rates/nba_possessions_2024.parquet`
(made-shot counts) and `../bb_team_season_rates/nba_schedule_2024.parquet`, plus the stats.nba.com
team clutch dashboard `../nba_prediction/clutch_team_2024.parquet`. The college tests read BB-D3's
ESPN schedules `../bb_team_season_rates/{mbb,wbb}_schedule_2025.parquet`.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `nba_second_chance_2024.parquet` | 242,618 | `nba_stats_possessions/nba_possessions_2024` | The 1,230 regular-season games (`game_id` starts `002`), the same rows as the two `nba_possessions_2024` files above. Cols: `game_id, period, possession_number, is_second_chance`. |
| `nba_player_clutch_2024.parquet` | 469 | live stats.nba.com `leaguedashplayerclutch` | `nba_stats_leaguedashplayerclutch(season="2023-24", measure_type_detailed_defense="Advanced")` (Regular Season, Totals, last 5:00 within 5), captured 2026-11-05: the player clutch oracle. Cols: `season, player_id, team_id, gp, min` (per game), `poss, off_rating, def_rating, net_rating`. |
| `nba_team_period_2024.parquet` | 120 | live stats.nba.com `leaguedashteamstats` | `nba_stats_leaguedashteamstats(season="2023-24", period="1".."4")` (Base, Totals, Regular Season), captured 2026-11-05: team points by quarter. Cols: `season, team_id, period, gp, pts, plus_minus`. |
| `nba_team_misc_2024.parquet` | 30 | `nba_stats_team_season_stats/team_season_stats_2024` | `season_type == "regular-season"`, `per_mode == "totals"`, `measure_type == "misc"`: stats.nba.com's second-chance points. Cols: `season, team_id, gp, pts_2nd_chance, opp_pts_2nd_chance`. |
| `wnba_possessions_2002.parquet` | 4,405 | `wnba_stats_possessions/wnba_possessions_2002` | The 30 two-halves games of `../bb_possession_flags/wnba_possessions_2002.parquet` (read with its pbp, `../bb_possession_flags/wnba_pbp_2002.parquet`). Cols: what `nba_situation_rows` reads: `game_id, period, possession_number, number_in_period, start_order_index, end_order_index, start_seconds_remaining, end_seconds_remaining, possession_start_type, off_player_1..5, def_player_1..5, offense_team_id, defense_team_id, points, is_second_chance, count_as_possession, fg2a, fg2m, fg3a, fg3m, fta, ftm, oreb, dreb, tov, season`. |
| `wnba_schedule_2002.parquet` | 30 | `wnba_stats_schedules/wnba_schedule_2002` | The same games. Cols: `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts`. |
| `ncaa_mbb_possessions_2025.parquet` | 9,777 | `ncaa_mbb_possessions/ncaa_mbb_possessions_2025` | Every 2024-25 contest of Michigan State (ESPN 127) and Purdue (2509): 72 contests. Cols: `season, contest_id, period, poss_num, poss_team, home, away, pts, last_event_time, is_transition, is_garbage_time, poss_team_espn_team_id, home_espn_team_id, away_espn_team_id, espn_game_id, {home,away}_{1..5}_player_id`. |
| `ncaa_wbb_possessions_2025.parquet` | 10,116 | `ncaa_wbb_possessions/ncaa_wbb_possessions_2025` | Every 2024-25 contest of UCLA (26) and USC (30): 69 contests, 10-minute quarters, none to overtime. Same columns. |
| `ncaa_ratings_2025.parquet` | 1,318 | `mbb_ratings/mbb_ratings_2025`, `wbb_ratings/wbb_ratings_2025` | Both releases stacked with a `league` column (`mens` 700 rows, `womens` 618), non-D1 teams included (the qualifier drops them). Cols: `league, season, team_id` (Utf8, as released), `adj_o, adj_d, adj_em, games`. |

Season naming: every file is named by the END year, the `season` column the releases stamp (the NBA
loaders take the start year: `load_nba_stats_possessions(2023)` reads `nba_possessions_2024`).

## Observed on these files (the test gates)

- NBA 2023-24 team clutch vs stats.nba.com: possessions r 0.997, ours 12 fewer a team on average
  (5 to 20); net rating r 0.992, mean difference -0.005. The possession live at the 5:00 mark counts
  (owner decision 2026-11-06): 1,154 games have one, 328 of them within 5 as it starts, so 328 of
  the 8,401 clutch possessions; before that rule ours were 23 fewer (14 to 29), r 0.996 / 0.982. Of
  the remaining 12, about 1 a team is the margin drift (the running made-shot margin missing a
  technical free throw and the like: 342 possessions flip with the pbp score's margin) and the rest
  is the margin moving inside a possession, which stats.nba.com reads event by event.
- Player clutch: 443 of the oracle's 469 players matched by (team, player); possessions r 0.965,
  minutes r 0.969; over the 137 with 150+ oracle possessions, net rating r 0.937, median |diff| 2.7.
  The 26 unmatched are fringe rows (median 2 clutch possessions, at most 51): 13 traded players whom
  stats.nba.com files under one team while our tables keep a row per team, and 13 who never started
  a clutch possession on the floor (subbed in at a dead ball inside one). Matched traded players are
  compared per team.
- Players per team sum to five times the team, less the 269 offensive sides the source lists with a
  player twice; every possession has five known players otherwise.
- Team points by quarter: 15 of 120 team-quarters exact, mean |diff| 2.79, max 8.
- Second-chance points: r 0.998 (scored) / 0.997 (allowed); ours run 2.2% over, the points a
  possession scored before its offensive rebound.
- WNBA 2002 (30 two-halves games): 40.44 game-clock minutes a team-game, overtimes included.
- College: player sums are exactly five times the team's (ratio 5.0); Michigan State used 13 players,
  Purdue 16; 364 of the 700 men's ratings rows have at least half the most games. College has no
  external oracle for the split values: they are checked for consistency only (BB-D3's possession
  counts, the add-up, the release's flag counts, the qualified top 25).
