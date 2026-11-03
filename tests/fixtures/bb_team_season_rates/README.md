<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D3 team-season-rates corpus](#bb-d3-team-season-rates-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D3 team-season-rates corpus

Real captures for `tests/nba/test_nba_team_season_rates.py` and
`tests/mbb/test_mbb_team_season_rates.py`, downloaded from the sportsdataverse-data releases on
2026-11-03 and projected to the columns `*_team_season_rates` reads (zstd level 19). Nothing is
synthetic or edited: rows are only filtered and columns only dropped.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `nba_possessions_2026.parquet` | 260,281 | `nba_stats_possessions/nba_possessions_2026` | The regular-season and preseason games of the schedule below. Cols: `game_id, period, possession_number, offense_team_id, defense_team_id, end_seconds_remaining, points, count_as_possession, fg2a, fg2m, fg3a, fg3m, fta, ftm, oreb, dreb, tov, season`. |
| `nba_schedule_2026.parquet` | 1,301 | `nba_stats_schedules/nba_schedule_2026` | `season_type` in (`regular-season`, `preseason`). Cols: `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts`. |
| `nba_team_stats_advanced_2026.parquet` | 30 | `nba_stats_leaguedash/team_stats_advanced_2026` | `season_type == "Regular Season"`: the stats.nba.com net-rating oracle. Cols: `team_id, team_name, gp, off_rating, def_rating, net_rating, pace, pace_per40, poss, efg_pct, tm_tov_pct, oreb_pct, season, season_type`. |
| `nba_possessions_2024.parquet` / `nba_schedule_2024.parquet` | 242,618 / 1,230 | the same tags, `_2024` | Regular season only, same columns. Paired with `../nba_prediction/clutch_team_2024.parquet` (stats.nba.com's clutch dashboard) for the clutch window. |
| `wnba_possessions_2025.parquet` / `wnba_schedule_2025.parquet` | 48,401 / 310 | `wnba_stats_possessions/wnba_possessions_2025`, `wnba_stats_schedules/wnba_schedule_2025` | Regular season and playoffs, same columns. |
| `wnba_team_stats_advanced_2025.parquet` | 13 | `wnba_stats_leaguedash/team_stats_advanced_2025` | Regular Season, same columns as the NBA oracle. |
| `ncaa_mbb_possessions_2025.parquet` / `ncaa_wbb_possessions_2025.parquet` | 58,364 / 59,703 | `ncaa_{mbb,wbb}_possessions/ncaa_{mbb,wbb}_possessions_2025` | Every game with a Big Ten team (the home teams of ESPN's `groups_short_name == "Big Ten"` games; 18 men's, 18 women's). ESPN ids cast to Int32 as the loader does. Cols: `season, contest_id, period, poss_num, poss_team, home, away, pts, last_event_time, poss_team_espn_team_id, home_espn_team_id, away_espn_team_id, espn_game_id`. |
| `ncaa_mbb_team_box_2025.parquet` / `ncaa_wbb_team_box_2025.parquet` | 838 / 822 | `ncaa_{mbb,wbb}_team_box/ncaa_{mbb,wbb}_team_box_2025` | The same contests. Cols: `season, contest_id, team, home, away, o_poss, fga, fgm, tpm, fta, orb, d_drb, to, home_espn_team_id, away_espn_team_id, team_espn_team_id`. |
| `mbb_schedule_2025.parquet` / `wbb_schedule_2025.parquet` | 419 / 397 | `espn_{mens,womens}_college_basketball_schedules/{mbb,wbb}_schedule_2025` | The ESPN games of those contests (women's: 14 contests have no `espn_game_id`). Cols: `game_id, season, neutral_site, home_id, away_id`. |

Season naming: the NBA files are named by the END year, as the releases stamp `season` (the
loaders take the start year: `load_nba_stats_possessions(2025)` reads `nba_possessions_2026`).
