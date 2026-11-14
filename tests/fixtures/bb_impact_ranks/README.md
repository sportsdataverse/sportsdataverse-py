<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Impact axes and player ranks corpus](#impact-axes-and-player-ranks-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Impact axes and player ranks corpus

Real captures for `tests/nba/test_nba_impact_axes.py` and `tests/nba/test_nba_player_stats_ranks.py`, taken on
2026-11-13. Nothing is synthetic: rows are untouched, columns only dropped. zstd level 19.

| File | Rows | Source |
|---|---:|---|
| `nba_player_impact_2026_regular_season.parquet` | 582 | `gh release download nba_player_impact -R sportsdataverse/sportsdataverse-data -p nba_player_impact_2026.parquet` (asset of 2026-11-12 05:09 UTC, built on sportsdataverse-py `cfca9541`, after #1137 and #1148), the `Regular Season` rows; kept `season, season_type, player_id, player_name, team_id, off_poss, def_poss, min, gp, o_rapm, d_rapm, rapm, o_adj_rapm, d_adj_rapm, obpm, dbpm, bpm, war` |
| `nba_team_totals_2026.parquet` | 30 | the Data API, `GET /v1/nba_stats/team_season_stats?season=2026&season_type=regular-season&measure_type=base&per_mode=totals&select=team_id,team_name,gp,w,plus_minus&limit=40` |
| `nba_player_stats_advanced_2026.parquet` | 812 (582 Regular Season, 230 Playoffs) | `load_nba_stats_leaguedash("player_stats_advanced", seasons=2026)` (the `nba_stats_leaguedash` release); kept the ids, `season, season_type, per_mode, gp, min, poss`, the 13 measures of `ADVANCED_RANKED`, their API `_rank` columns and `gp_rank` |
| `wnba_player_stats_advanced_2025.parquet` | 273 (182 Regular Season, 91 Playoffs) | `load_wnba_stats_leaguedash("player_stats_advanced", seasons=2025)`, the same columns |

Ids are Int64 (`season` Int32 in the leaguedash files, as released). `season` is the END year.
