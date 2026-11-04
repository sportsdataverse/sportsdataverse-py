<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D30 NBA Cup corpus](#bb-d30-nba-cup-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D30 NBA Cup corpus

Real captures for `tests/nba/test_nba_cup.py`, taken on 2026-11-04 and projected to the columns
the Cup module reads (zstd level 19). Nothing is synthetic or edited: rows are only filtered and
columns only dropped.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `nba_cup_schedule_{2024,2025,2026,2027}.parquet` | 67 each | stats.nba.com `scheduleleaguev2` (`nba_stats_scheduleleaguev2`, `Season` 2023-24 .. 2026-27) | `game_subtype` in (`in-season`, `in-season-knockout`): 60 Group Play games, 4 quarterfinals, 2 semifinals, the final. 2026-27 a week into Group Play, the knockout still TBD (team id 0). Cols: `game_id, game_date, game_status, game_subtype, game_sub_label, is_neutral, home_team_id, away_team_id, home_team_score, away_team_score`. |
| `nba_cup_overtime_{2024,2025,2026}.parquet` | 43 / 61 / 80 | `nba_stats_possessions/nba_possessions_{year}` release | The overtime possessions (`period > 4`) of the Group Play games: two, three and three games. Cols: `game_id, period, offense_team_id, points`. |
| `ist_standings_{2024,2025,2026}.parquet` | 30 each | stats.nba.com `iststandings` (`Section=group`), the oracle | Cols: `teamId, teamAbbreviation, conference, istGroup, istGroupRank, istWildcardRank, istKnockoutRank, clinchedIstGroup, clinchedIstWildcard, wins, losses, diff, pts`. Its 2023-24 `diff` / `pts` include overtime; 2024-25 and 2025-26 exclude it. |
| `nba_schedule_{2023,2025}.parquet` | 1,230 each | `nba_stats_schedules/nba_schedule_{year}` release | `season_type == "regular-season"`: the prior-season record for the 2024 and 2026 Cups (2025 and 2027 use `../bb_pro_season`). Cols: `game_id, season, season_type, game_date, home_team_id, away_team_id, home_pts, away_pts`. |

Season naming: files are named by the END year (2024 is the 2023-24 Cup).
