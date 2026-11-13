<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D4 lineup-seasons corpus](#bb-d4-lineup-seasons-corpus)
  - [Observed on the full 2024-25 NBA regular season (not in the fixture)](#observed-on-the-full-2024-25-nba-regular-season-not-in-the-fixture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D4 lineup-seasons corpus

Real captures for `tests/nba/test_nba_lineup_seasons.py` and `tests/mbb/test_mbb_lineup_seasons.py` (BB-D4), and
`tests/nba/test_nba_player_on_off.py` and `tests/mbb/test_mbb_player_on_off.py` (BB-D5),
downloaded from the sportsdataverse-data releases on 2026-11-04 and projected to the columns the
lineup-seasons builders read (zstd level 19). Nothing is synthetic or edited: rows are only filtered
(whole games) and columns only dropped.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_schedule_2025.parquet` | 238 | `nba_stats_schedules/nba_schedule_2025` | Every 2024-25 regular-season game of teams 1610612750, 1610612755 and 1610612756 (the 30 team ids sorted, then `sample(3, seed=6)`). Cols: `game_id, season, season_type, home_team_id, away_team_id`, and `game_date` (added 2026-11-13 from the same asset for BB-D5's span columns). |
| `nba_pbp_2025.parquet` | 115,618 | `nba_stats_pbp/nba_play_by_play_2025` | The same games. Cols: `game_id, order_index, action_number, period, seconds_remaining, score_home, score_away, is_free_throw, is_foul, season`. |
| `nba_game_lineups_2025.parquet` | 115,618 | `nba_stats_game_lineups/nba_lineups_2025` | The same games, every column. |
| `nba_possessions_2025.parquet` | 46,572 | `nba_stats_possessions/nba_possessions_2025` | The same games. Cols: `game_id, offense_team_id, defense_team_id, off_player_1..5, def_player_1..5, fg2a, fg2m, fg3a, fg3m, fta, oreb, dreb, tov, season`. |
| `nba_lineups_dashboard_2025.parquet` | 420 | `nba_stats_lineups/lineups_2025` | stats.nba.com's five-man lineups dashboard for the three teams, `season_type == "regular-season"`, `per_mode == "totals"`, `measure_type in (base, advanced)`: the oracle. Cols: `team_id, group_id, measure_type, gp, min, poss, pts, plus_minus, off_rating, def_rating, net_rating, efg_pct, tm_tov_pct, oreb_pct, season, season_type`. |
| `nba_on_off_summary_2025.parquet` | 140 | live: stats.nba.com `teamplayeronoffsummary` | Captured 2026-11-05 with `nba_stats_teamplayeronoffsummary(season="2024-25", team_id=..., measure_type_detailed_defense="Base")` for the three teams (Regular Season, Totals; the Advanced call returns the same columns and values): the `PlayersOnCourt...` and `PlayersOffCourt...` result sets stacked, the BB-D5 oracle. Cols (ids Int64): `team_id, player_id` (its `vs_player_id`), `court_status, gp, min, off_rating, def_rating, net_rating`. Its off-court side covers only the games a player was on the roster. |
| `wnba_{schedule,pbp,game_lineups,possessions}_2025.parquet` | 16 / 6,461 / 6,461 / 2,496 | `wnba_stats_schedules/wnba_schedule_2025`, `wnba_stats_pbp/wnba_play_by_play_2025`, `wnba_stats_game_lineups/wnba_lineups_2025`, `wnba_stats_possessions/wnba_possessions_2025` | 16 regular-season games of 2025 (`game_id.sort().sample(16, seed=6)`), the NBA columns. |
| `nba_{schedule,pbp,game_lineups,possessions}_2025_trades.parquet` | 3 / 1,442 / 1,442 / 588 | the NBA assets above | Added 2026-11-13 for BB-D5's span columns: the three 2024-25 regular-season games, none involving the fixture's teams, that bound three traded players' spans: Quentin Grimes's last Dallas game (0022400696, 2025-02-02), Josh Okogie's first Charlotte game (0022400599, 2025-01-20) and Jusuf Nurkic's (0022400770, 2025-02-12). The NBA columns, `game_date` included. |
| `wnba_{schedule,pbp,game_lineups,possessions}_2005.parquet` | 4 / 1,651 / 1,651 / 596 | the same tags, `_2005` | Four 2005 regular-season games from the two-half era (through 2005 the WNBA played two 20-minute halves): three regulation games (`sample(3, seed=6)` of the sorted non-overtime game ids: 1020500099, 1020500142, 1020500154) and one double-overtime game (1020500163), the NBA columns. |
| `ncaa_{mbb,wbb}_pbp_2025.parquet` | 6,258 / 5,944 | `ncaa_{mbb,wbb}_pbp/ncaa_{mbb,wbb}_pbp_2025` | The BB-D6 contests (`tests/fixtures/bb_game_impact/README.md`). Cols: `contest_id, season, game_date, period, game_seconds, home, away, home_score, away_score, event_team, event_type, event_result, shot_value, foul_class, {home,away}_{1..5}_player_id, home_espn_team_id, away_espn_team_id` (`game_date` added 2026-11-13 from the same asset for BB-D5's span columns). |
| `ncaa_{mbb,wbb}_possessions_2025.parquet` | 1,827 / 1,762 | `ncaa_{mbb,wbb}_possessions/ncaa_{mbb,wbb}_possessions_2025` | The same contests. Cols: `contest_id, season, poss_team, home, away, {home,away}_{1..5}_player_id, home_espn_team_id, away_espn_team_id`. |
| `ncaa_{mbb,wbb}_team_box_2025.parquet` | 26 / 24 | `ncaa_{mbb,wbb}_team_box/ncaa_{mbb,wbb}_team_box_2025` | The same contests' team totals, the count oracle. Cols: `contest_id, season, team_espn_team_id, fga, fgm, tpa, tpm, fta, orb, drb, to, rima, rimm`. |

Season naming: every file is named by the END year, the `season` column the releases stamp (the
NBA loaders take the start year: `load_nba_stats_possessions(2024)` reads `nba_possessions_2025`).

## Observed on the full 2024-25 NBA regular season (not in the fixture)

Five-man units over 200 of stats.nba.com's possessions (113 units): the net rating differs by a
median of 1.48 (p95 5.03, max 10.76; 64.6% within 2.0), the eFG% by at most 0.048. Points scored
equal stats.nba.com's on 57% of them (median difference 0) and minutes agree to the second at the
median. The rest is possession counting: stats.nba.com credits a possession to every lineup on the
floor during it, so its counts run about 4% above these, which give each possession to the lineup
that started it.
