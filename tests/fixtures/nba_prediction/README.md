<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA prediction-stack oracle corpus (season 2024)](#nba-prediction-stack-oracle-corpus-season-2024)
  - [Oracle notes](#oracle-notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA prediction-stack oracle corpus (season 2024)

Offline validation fixtures for the NBA/WNBA/G-League prediction & market
stack (Phase 0, Task 0.1). Captured **2026-07-08** for validation season
**2023-24** (int `2024`; stats.nba.com season string `"2023-24"`), plus
prior season **2022-23** (int `2023`; `"2022-23"`) for the Phase-4
out-of-sample clutch gate. Regenerate with
`SDV_PY_NBA_STATS_LIVE=1 SDV_PY_LIVE_TESTS=1 uv run python dev/nba_prediction/capture_oracle.py`
(script is gitignored under `dev/`, per repo `CLAUDE.md` -- `dev/` is
working notes, not tracked; this deviates from the plan's literal
"create committed: dev/nba_prediction/capture_oracle.py" wording, but
matches both `CLAUDE.md` and the actual MBB-spine precedent, whose own
fixture README says the same).

**ID convention:** every team/game/player id is `Utf8` (cast from the raw
integer via `pl.col(id).cast(pl.Int64).cast(pl.Utf8)`), so joins across
fixtures and against the ratings engine never hit a dtype mismatch.

| File | Rows | Source | Notes |
|---|---:|---|---|
| `results_2024.parquet` | 1320 | `load_nba_schedule([2024])` (ESPN release download, any IP) | Completed games only, deduped on `game_id`. Cols: `game_id, season, date, home_team_id, away_team_id, home_score, away_score, neutral_site`. Includes playoffs (regular season ends 2024-04-14). |
| `team_box_2024.parquet` | 2640 | `load_nba_team_boxscore([2024])` (ESPN) | Per-team possession inputs. Cols: `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. |
| `player_box_logs_2024.parquet` | 35028 | `load_nba_player_boxscore([2024])` (ESPN) | Per-player-per-game logs for the Phase-5 prop gate. Cols: `game_id, player_id, team_id, opp_team_id, is_home, minutes, pts, reb, ast, fg3m`. |
| `pbp_sample_2024.parquet` | 6396 | `load_nba_pbp([2024])` (ESPN) | Phase-3 in-game-WP calibration sample: every 30th play of 399 sampled regular-season games (past a 200-game as-of warmup), each with its **leakage-safe as-of-date** `pregame_home_prob` (`nba_team_ratings(as_of_date=game.date)` -> `nba_predict_games`) + realized `home_win`. Cols match `load_nba_pbp` so `in_game_features` runs offline: `game_id, start_game_seconds_remaining, home_score, away_score, team_id, home_team_id, pregame_home_prob, home_win`. The 399-game sample's home-win rate (0.541) matches the population, so the gate isn't a small-sample base-rate artifact. |
| `team_ratings_oracle_2024.parquet` | 30 | `nba_stats_leaguedashteamstats(season="2023-24", measure_type_detailed_defense="Advanced")` (stats.nba.com) + `espn_nba_season_powerindex(2024, team_id=...)` (ESPN Core v2, per-team; BPI) | **stats.nba.com and ESPN use unrelated team-id systems** (stats.nba.com's 10-digit franchise id, e.g. `1610612737`, vs ESPN's small integer, e.g. `1` for ATL) -- crosswalked by full team display name (`"Atlanta Hawks"` on both sides; unambiguous for 30 teams). `rank` computed locally (dense rank on `net_rating` descending). 0/30 teams failed to crosswalk; 0/30 missing BPI. |
| `espn_predictor_sample_2024.parquet` | 53 | `espn_nba_game_predictor(game_id)` raw payload (ESPN Core v2) | Every 25th completed game by date. **ESPN's predictor payload only ever populates `awayTeam.statistics`** (`homeTeam` carries just a team `$ref`, confirmed across multiple real games) -- `home_win_prob = 1 - away gameProjection/100`. The parsed/flattened frame stringifies the nested stat list, so this reads the raw JSON directly. |
| `espn_odds_sample_2024.parquet` | 53 | `espn_nba_game_odds(game_id)` raw payload (ESPN Core v2) | Same sampled games. Uses the first (highest-priority) book's `homeTeamOdds.close.pointSpread.value` (home-anchored) and top-level `overUnder`. |
| `winprob_sample_2024.parquet` | **0** | `winprobabilitypbp` (stats.nba.com; oracle-only helper, NOT a generated `nba_stats` wrapper) | **FINDING: this stats.nba.com endpoint is DEAD, not merely uncaptured.** Confirmed two ways: (1) correctly-formed requests (zero-padded `GameID` + valid `RunType`, matching hoopR's own request shape) return HTTP 500 with an empty body for every game tried (playoff and regular-season alike, tried against the (date, home-team-abbreviation) crosswalk to real stats.nba.com `GAME_ID`s); (2) hoopR's own `nba_winprobabilitypbp()` (`hoopR-dev/hoopR/R/nba_stats_scoreboard.R`) is itself `lifecycle::deprecate_stop()`-ed as of hoopR 3.0.0, replaced by `nba_playbyplayv3()` -- the upstream sibling R package no longer calls this endpoint either. The fixture is committed with its documented zero-row schema (`game_id, event_num, sec_left, score_diff, home_pct`) so downstream code has a stable contract; the Phase-3 gate's *(b)* "MAE vs native winprobabilitypbp" concurrent check is **not obtainable** and must be dropped or replaced with a substitute oracle in a future task. |
| `clutch_team_2024.parquet` / `clutch_team_2023.parquet` | 30 / 30 | `nba_stats_leaguedashteamclutch(season=..., measure_type_detailed_defense="Advanced")` (stats.nba.com) | Cols: `season, team_id, clutch_off_rating, clutch_def_rating, clutch_net_rating, clutch_poss`. `clutch_poss` from the Advanced measure type's `POSS` column. |

## Oracle notes

- **stats.nba.com live capture worked from this box** (residential IP; no
  TLS/JA3 hang observed) via `nba_stats_runtime._get` (curl_cffi
  `impersonate="chrome"`), gated by `SDV_PY_NBA_STATS_LIVE=1`.
- **stats.nba.com <-> ESPN team-id crosswalk is by full team name** (30
  teams, no ambiguity) -- see `team_ratings_oracle_2024` note above. The
  same technique (name, not id) is used for the `winprobabilitypbp`
  attempt's (date, home-abbreviation) game crosswalk via
  `nba_stats_leaguegamefinder`.
- **`nba_stats_leaguegamefinder` defaults to Regular Season only** (no
  `season_type_nullable="Playoffs"` was passed), so any (date,
  home-team) crosswalk against it must restrict the ESPN sample to dates
  on/before the regular-season cutoff (2024-04-14) or it will silently
  produce zero matches.
