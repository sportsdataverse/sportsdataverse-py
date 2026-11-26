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
`SDV_PY_NBA_STATS_LIVE=1 SDV_PY_LIVE_TESTS=1 uv run python tools/models/nba_prediction/capture_oracle.py`
(script is gitignored under `dev/`, per repo `CLAUDE.md` -- `dev/` is
working notes, not tracked; this deviates from the plan's literal
"create committed: tools/models/nba_prediction/capture_oracle.py" wording, but
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
| `wnba_player_box_logs_2024.parquet` | 5929 | `load_wnba_player_boxscore([2024])` + `load_wnba_team_boxscore([2024])` (ESPN `espn_wnba_player_boxscores` / `espn_wnba_team_boxscores` releases, <https://github.com/sportsdataverse/sportsdataverse-data/releases>, read 2026-11-24) | WNBA twin of `player_box_logs_2024` for the per-league prop constants (`fit_player_props.py wnba`, `tests/wnba/test_wnba_player_props_constants.py`). Same columns; regular season + playoffs; 971 DNP rows carry null minutes and stats. Written by `capture_oracle.py wnba-props`. |
| `pbp_sample_2024.parquet` | 6396 | `load_nba_pbp([2024])` (ESPN) | Phase-3 in-game-WP calibration sample: every 30th play of 399 sampled regular-season games (past a 200-game as-of warmup), each with its **leakage-safe as-of-date** `pregame_home_prob` (`nba_team_ratings(as_of_date=game.date)` -> `nba_predict_games`) + realized `home_win`. Cols match `load_nba_pbp` so `in_game_features` runs offline: `game_id, start_game_seconds_remaining, home_score, away_score, team_id, home_team_id, pregame_home_prob, home_win`. The 399-game sample's home-win rate (0.541) matches the population, so the gate isn't a small-sample base-rate artifact. |
| `team_ratings_oracle_2024.parquet` | 30 | `nba_stats_leaguedashteamstats(season="2023-24", measure_type_detailed_defense="Advanced")` (stats.nba.com) + `espn_nba_season_powerindex(2024, team_id=...)` (ESPN Core v2, per-team; BPI) | **stats.nba.com and ESPN use unrelated team-id systems** (stats.nba.com's 10-digit franchise id, e.g. `1610612737`, vs ESPN's small integer, e.g. `1` for ATL) -- crosswalked by full team display name (`"Atlanta Hawks"` on both sides; unambiguous for 30 teams). `rank` computed locally (dense rank on `net_rating` descending). 0/30 teams failed to crosswalk; 0/30 missing BPI. |
| `espn_predictor_sample_2024.parquet` | 53 | `espn_nba_game_predictor(game_id)` raw payload (ESPN Core v2) | Every 25th completed game by date. **ESPN's predictor payload only ever populates `awayTeam.statistics`** (`homeTeam` carries just a team `$ref`, confirmed across multiple real games) -- `home_win_prob = 1 - away gameProjection/100`. The parsed/flattened frame stringifies the nested stat list, so this reads the raw JSON directly. |
| `espn_odds_sample_2024.parquet` | 53 | `espn_nba_game_odds(game_id)` raw payload (ESPN Core v2) | Same sampled games. Uses the first (highest-priority) book's `homeTeamOdds.close.pointSpread.value` (home-anchored) and top-level `overUnder`. |
| `winprob_sample_2024.parquet` | **0** | `winprobabilitypbp` (stats.nba.com; oracle-only helper, NOT a generated `nba_stats` wrapper) | **FINDING: this stats.nba.com endpoint is DEAD, not merely uncaptured.** Confirmed two ways: (1) correctly-formed requests (zero-padded `GameID` + valid `RunType`, matching hoopR's own request shape) return HTTP 500 with an empty body for every game tried (playoff and regular-season alike, tried against the (date, home-team-abbreviation) crosswalk to real stats.nba.com `GAME_ID`s); (2) hoopR's own `nba_winprobabilitypbp()` (`hoopR-dev/hoopR/R/nba_stats_scoreboard.R`) is itself `lifecycle::deprecate_stop()`-ed as of hoopR 3.0.0, replaced by `nba_playbyplayv3()` -- the upstream sibling R package no longer calls this endpoint either. The fixture is committed with its documented zero-row schema (`game_id, event_num, sec_left, score_diff, home_pct`) so downstream code has a stable contract; the Phase-3 gate's *(b)* "MAE vs native winprobabilitypbp" concurrent check is **not obtainable** and must be dropped or replaced with a substitute oracle in a future task. |
| `clutch_team_2024.parquet` / `clutch_team_2023.parquet` | 30 / 30 | `nba_stats_leaguedashteamclutch(season=..., measure_type_detailed_defense="Advanced")` (stats.nba.com) | Cols: `season, team_id, clutch_off_rating, clutch_def_rating, clutch_net_rating, clutch_poss`. `clutch_poss` from the Advanced measure type's `POSS` column. Keyed on the stats.nba.com 10-digit franchise team_id. |
| `team_net_2023.parquet` / `team_net_2024.parquet` | 30 / 30 | `nba_stats_leaguedashteamstats(..., Advanced)` (stats.nba.com) | Full-game `net_rating` (aliased `adj_net_rtg`) keyed on the **stats.nba.com** team_id -- the Phase-4 clutch baseline, same id system as the clutch fixtures (no crosswalk). |
| `pbp_sample_2024.parquet` | 6396 | `load_nba_pbp([2024])` (ESPN) | Phase-3 in-game-WP calibration sample (see the earlier row). |
| `wnba_results_2024.parquet` | 264 | `load_wnba_schedule([2024])` (ESPN) | WNBA (league_id=10) Phase-6 gate. Cols mirror `results_2024`; ESPN wnba team_id. |
| `wnba_team_box_2024.parquet` | 528 | `load_wnba_team_boxscore([2024])` (ESPN) | WNBA possession inputs; cols mirror `team_box_2024`. |
| `wnba_pbp_sample_2024.parquet` | 10024 | `load_wnba_pbp([2024])` (ESPN; sportsdataverse-data `espn_wnba_pbp` release, read 2026-10-11) | supp-013 real-slice gate for `build_wnba_season_wp`: every 10th play (`game_play_number % 10 == 0`) of all 264 games, cols `game_id, game_play_number, start_game_seconds_remaining, home_score, away_score, team_id, home_team_id` + `home_win` (each game's last-play home score > away score). |
| `wnba_team_ratings_oracle_2024.parquet` | 12 | `wnba_stats_leaguedashteamstats(season="2024", Advanced)` (stats.wnba.com) | WNBA NET_RATING/PACE crosswalked to the ESPN wnba team_id by full team display name (12/12 matched). Phase-6 ratings gate: AdjNet vs NET_RATING spearman 0.965, mae 0.753. stats.wnba.com season string is the plain year `"2024"` (NOT `"2023-24"`). |
| `nba_late_game_2026.parquet` / `wnba_late_game_2025.parquet` | 39061 / 8747 | `load_nba_pbp([2026])` / `load_wnba_pbp([2025])` (ESPN `espn_<lg>_pbp` releases, read 2026-11-03) | In-game-WP holdout late-game gate (tests/nba/test_nba_in_game_wp_late_game.py): every play of the last regulation period with `start_game_seconds_remaining <= 120`, plus every overtime play, of every holdout game, scores forward-filled per game first. Written by `tools/models/nba_prediction/train_in_game_wp.py`; the holdout seasons are not in the boosters' training seasons. |
| `nba_rated_prior_games.parquet` / `wnba_rated_prior_games.parquet` | 3969 / 576 | `load_<lg>_pbp` (line, final score) + `nba_rated_priors` (ESPN and stats releases, read 2026-11-05) | BB-D25 rated-prior holdout gate (tests/nba/test_nba_wp_model_cards.py): one row per finished, untied game of NBA 2024-2026 / WNBA 2024-2025 (seasons disjoint from the season sim's constants), cols `season, game_id, neutral_site, home_team_spread, game_spread_available, rated_home_prob, y`. Written by `tools/models/nba_prediction/wp_model_cards.py evaluate`. |
| `wnba_rated_prior_asof_2025.parquet` / `wnba_rated_prior_asof_games_2025.parquet` | 7789 / 49 | `load_wnba_stats_possessions([2025])` / `load_wnba_stats_schedules([2025])` (read 2026-11-05) | As-of boundary test for `rated_home_probs`: the possession rows (BB-D3 rate rows plus `game_date`) and stats games of the first 21 days of the 2025 regular season. Written by `wp_model_cards.py asof-fixture`. `ftm` (a rate-row count since 2026-11-13) was added from `../bb_team_season_rates/wnba_possessions_2025.parquet` through `nba_possession_rate_rows`, joined on `(game_id, period, possession_number)`; every other column was checked equal. |
| `prior_miss_nba.json` / `prior_miss_wnba.json` | report | `tools/fit_pro_rating_sd.py --league <lg> --train ... --out` on the `{nba,wnba}_stats_possessions` / `_schedules` releases (read 2026-11-26) | The season sim's prior-miss pseudo-count fit (`nba_season_sim.PRIOR_MISS_POSS`): per train season the returning teams' miss variance, its mean and the `pick` (NBA 2,950, WNBA 1,060). `test_prior_miss_constants_are_the_committed_fit_or_the_penalty` pins the WNBA constant to `pick`; the NBA pick failed its fresh-season gate, so the NBA constant is the ridge penalty. |
| `wnba_player_box_2026_opening.parquet` / `wnba_team_box_2026_opening.parquet` / `wnba_schedule_2026_opening.parquet` | 562 / 48 / 24 | `player_box_2026.parquet` (`espn_wnba_player_boxscores`), `team_box_2026.parquet` (`espn_wnba_team_boxscores`), `wnba_schedule_2026.parquet` (`espn_wnba_schedules`) from <https://github.com/sportsdataverse/sportsdataverse-data/releases>, downloaded 2026-11-25 | The 24 games of the 2026 season with `game_date < 2026-05-16`, release columns and dtypes kept, cut to the ones `nba_player_props` / `nba_team_ratings` read (player: `game_id, game_date, athlete_id, team_id, minutes, points, rebounds, assists, three_point_field_goals_made`; team: `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`; schedule: `game_id, season, season_type, game_date, home_id, away_id, neutral_site`). Covers the expansion teams' first games: Toronto Tempo `131935` (2026-05-08, 401856891), Portland Fire `132052` (2026-05-09, 401856895); Golden State Valkyries is `129689`. Cold-start test: `tests/wnba/test_wnba_player_props_cold_start.py`. |
| `player_box_rate_prior_2025_2026.parquet` | 2869 | `player_box_2025.parquet` / `player_box_2026.parquet` (`espn_nba_player_boxscores`, <https://github.com/sportsdataverse/sportsdataverse-data/releases>, downloaded 2026-11-25) | Boston (`team_id` 2) and New York (18): every 2024-25 row (2,199 regular season, 428 playoffs) and the 2025-26 rows before 2025-11-08 (242), release columns and dtypes kept, cut to `game_id, season, season_type, game_date, athlete_id, team_id, minutes`, the nine prop stats and `starter`. The NBA rate-prior test (`tests/nba/test_nba_player_props_rate_prior.py`): a plain-Python oracle of the tiers and pooled rates, and the playoff rows that must stay out of the prior. |
| `player_box_rate_prior_2024.parquet` | 2163 | `player_box_2024.parquet` (`espn_nba_player_boxscores`, <https://github.com/sportsdataverse/sportsdataverse-data/releases>, downloaded 2026-11-25) | Boston (`team_id` 2) and New York (18), the 2023-24 regular season, the same columns and dtypes as `player_box_rate_prior_2025_2026.parquet`: the tier prior of 2024-25 for the recent-window mixture test (`tests/nba/test_nba_player_props_recent_mixture.py`). |
| `baseline_in_game_wp/{nba,wnba}_in_game_wp_3e967eb40d.ubj` | -- | `sportsdataverse/nba/models/*_in_game_wp.ubj` at 3e967eb40d | The pre-2026-11 boosters (flat in the final minute). The late-game gates must fail on them, and the game-page golden pins the pipeline with them. |

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
