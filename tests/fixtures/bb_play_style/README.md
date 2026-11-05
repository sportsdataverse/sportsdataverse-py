<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D20 play-style corpus](#bb-d20-play-style-corpus)
  - [Training corpus (read by `tools/models/play_style/fit_play_style.py`)](#training-corpus-read-by-toolsmodelsplay_stylefit_play_stylepy)
  - [2024-25 raw inputs (the builders' release rows)](#2024-25-raw-inputs-the-builders-release-rows)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D20 play-style corpus

Real captures for `tests/nba/test_nba_play_style.py` and `tests/mbb/test_mbb_play_style.py` (BB-D20), read from
the sportsdataverse-data releases on 2026-11-05 (zstd level 19). Nothing is synthetic or edited: rows are only
filtered and columns only dropped; the feature files are the builders' output rounded to 6 decimals.

## Training corpus (read by `tools/models/play_style/fit_play_style.py`)

The bundled artifacts (`sportsdataverse/{nba,mbb}/models/*_play_style_*.json`) are the fit of these files and
record their sha256; the tests refit them and compare the centers. `fit_play_style.py --rebuild` rewrites them
from the releases.

| File | Rows | Seasons | Built by |
|---|---:|---|---|
| `nba_play_style_offense_player.parquet` | 3,502 | 2016-2026 | `nba_play_style_features(range(2016, 2027), side="offense", entity="player")` |
| `nba_play_style_defense_player.parquet` | 2,898 | 2018-2026 | `nba_play_style_features(range(2018, 2027), side="defense", entity="player")` |
| `nba_play_style_defense_team.parquet` | 330 | 2016-2026 | `nba_play_style_features(range(2016, 2027), side="defense", entity="team")` (every candidate feature) |
| `mbb_play_style_player.parquet` | 5,799 | 2025-2026 | `college_play_style_features([2025, 2026], league="mens")` |
| `wbb_play_style_player.parquet` | 5,661 | 2025-2026 | `college_play_style_features([2025, 2026], league="womens")` |

NBA seasons are end years (2026 = 2025-26). Training seasons are 2016-2022 (defense player 2018-2022) and the
holdout seasons 2023-2026; college trains on 2025 and holds out 2026. `k` and the team feature set are chosen
on the training seasons only.

## 2024-25 raw inputs (the builders' release rows)

| File | Rows | Release (loader) | Kept |
|---|---:|---|---|
| `nba_synergy_2025.parquet` | 5,869 | `nba_stats_synergy` (`load_nba_stats_synergy(2024, season_type="regular-season", per_mode="totals")`) | Both type groupings. Cols: `player_id, team_id, type_grouping, play_type, poss, poss_pct, season`. `poss_pct` is stats.nba.com's play-type frequency, the Synergy-mix oracle. |
| `nba_player_stats_scoring_2025.parquet` | 569 | `nba_stats_leaguedash` `player_stats_scoring_2025` | Regular Season. Cols: `player_id, season, season_type, fga, pct_fga_3_pt, pct_uast_fgm, pct_pts_paint, pct_pts_2_pt_mr`. |
| `nba_player_tracking_{drives,possessions,catchshoot,pullupshot,passing,defense}_2025.parquet` | 569 each | `nba_stats_leaguedash` `player_tracking_*_2025` | Regular Season; the columns the builders read. |
| `nba_matchups_rollup_2025.parquet` | 2,269 | `nba_stats_matchups` (`load_nba_stats_matchups_rollup(2024, ...)`) | Regular season totals. Cols: `def_player_id, season, position, percent_of_time`. |
| `nba_hustle_player_2025.parquet` / `nba_hustle_team_2025.parquet` | 567 / 30 | `nba_stats_hustle` | Regular season totals; the hustle columns the builders read. |
| `nba_team_stats_{opponent,defense}_2025.parquet` | 30 each | `nba_stats_leaguedash` `team_stats_*_2025` | Regular Season; the columns the builders read. |
| `mbb_shots_2025_team.parquet` | 2,245 | `espn_mens_college_basketball_shots` (`load_mbb_shots([2025])`) | Every 2025 shot of ESPN team 2275 (the corpus's 2025 team ids sorted, `sample(1, seed=20)`). Cols: `game_id, season, team_id, athlete_id_1, athlete_id_2, type_text, score_value, scoring_play`. |
| `mbb_player_box_2025_team.parquet` | 494 | `espn_mens_college_basketball_player_boxscores` (`_load_player_box([2025], "mens")`) | Every 2025 box row of the same team, the columns `aggregate_player_seasons` reads (`starter` included): the lineup-role hand count. |

Release facts the builders handle, found while building these tests:

- stats.nba.com publishes a player's Synergy play type only above a volume floor, so a player's published rows
  do not add up to his possessions; `poss / poss_pct` recovers the stint's total.
- The leaguedash families carry `season` as Int32 and Synergy / hustle as Int64; the builders cast every key to
  Int64 and assert the dtypes before each join.
- ESPN's college `TipShot` count doubled from 2025 to 2026 (9,177 to 19,481), a labelling change, so the
  college model has no tip-in feature.
