<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL ratings/market spine oracle corpus (season 2023)](#nfl-ratingsmarket-spine-oracle-corpus-season-2023)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL ratings/market spine oracle corpus (season 2023)

Captured 2026-07-08 by `dev/nfl_prediction/capture_oracle.py` (scratch script,
not committed). All ids are `Utf8`; `team_id` columns hold **nflverse team
abbreviations** (ESPN ids were normalized via `espn_nfl_teams()` +
`{LAR->LA, WSH->WAS}`); `game_id` is the nflverse id (`2023_01_DET_KC`).

| File | Source | Rows | Notes |
|---|---|---:|---|
| `results_2023.parquet` | `load_nfl_schedule([2023])`, completed games | 285 | REG+POST; `neutral_site = (location == "Neutral")` |
| `espn_odds_sample.parquet` | `load_nfl_schedule([2023])` closing lines | 285 | `close_spread_home` is the market's **expected home margin** (nflverse `spread_line` convention: positive = home favored), `close_total` = `total_line` |
| `pbp_2023_sample.parquet` | `load_nfl_pbp([2023])`, 12-column down-select | 49,665 | Carries `epa` from the shipped `ep_wp` pipeline; never re-scored |
| `team_stats_2023.parquet` | derived from the pbp sample | 32 | RAW (unadjusted) off/def EPA per play on competitive pass/run plays; `build_nfl_team_stats` ships no per-play EPA columns, so the raw-vs-adjusted sanity oracle is computed directly |
| `player_stats_2023.parquet` | `load_nfl_player_stats()` filtered to 2023 | 5,653 | Offense player-weeks (loader takes no seasons arg; `kicking=False`) |
| `fpi_2023.parquet` | `sports.core.api.espn.com` `/seasons/2023/powerindex?limit=50` (the `espn_nfl_season_powerindex` endpoint; fetched with `limit=50` because the wrapper's default page size is 25 of 32 teams) | 32 | `fpi` + `fpirank` from the `predictives` array; `team` holds the ESPN numeric id |
| `espn_predictor_sample.parquet` | `espn_nfl_game_predictor(<espn id>)` per game | 272 | All completed 2023 REG games; `home_win_prob = gameProjection/100`, `predicted_margin = teamPredPtDiff` (home) |
| `espn_propbets_sample.parquet` | `espn_nfl_game_propbets` | 0 | **Zero-row by construction**: ESPN purges propbets for past games (every probe 404s). Schema (`game_id, player_id, stat, line`) is the documented contract for the live-only line join |

`espn_nfl_season_powerindex_leaders(season=2023)` returns `count: 0` for past
seasons — that is why the per-team `powerindex` endpoint is the FPI source.
