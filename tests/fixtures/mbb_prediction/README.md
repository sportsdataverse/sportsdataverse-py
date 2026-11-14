<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MBB prediction-stack oracle corpus (season 2024)](#mbb-prediction-stack-oracle-corpus-season-2024)
  - [Oracle notes](#oracle-notes)
  - [ESPN per-game / per-team samples (captured 2026-07-07)](#espn-per-game--per-team-samples-captured-2026-07-07)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MBB prediction-stack oracle corpus (season 2024)

Offline validation fixtures for the MBB/WBB prediction & tournament stack
(Phase 0, Task 0.1). Captured **2026-07-07** for the **2024** season
(2023-24 men's Division I). Regenerate with
`uv run python dev/mbb_prediction/capture_oracle.py` (script is gitignored
under `dev/`). The in-game WP model and its late-game fixture are fit and written by the committed
`tools/models/mbb_prediction/train_in_game_wp.py`.

**ID convention:** every team / game id is `Utf8` (cast from the raw ESPN
integer via `pl.col(id).cast(pl.Int64).cast(pl.Utf8)`), so joins across
fixtures and against the ratings engine never hit a dtype mismatch. The two `shell_game_*_2011` files are the exception: they keep ESPN's raw `Int32` ids (`game_id`, `home_id`, `away_id`, `team_id`) on purpose, so the regression test also exercises `mbb_team_ratings`' own boundary cast to `Utf8`. Cast them the same way before joining them to the other fixtures.

| File | Rows | Source | Notes |
|---|---:|---|---|
| `results_2024.parquet` | 6243 | `load_mbb_schedule([2024])` (ESPN, via sportsdataverse-data) | Completed games only (`status_type_completed`), deduped on `game_id`. Cols: `game_id, season, date, home_team_id, away_team_id, home_score, away_score, neutral_site`. |
| `team_box_2024.parquet` | 12480 | `load_mbb_team_boxscore([2024])` (ESPN) | Per-team possession inputs for the ratings engine. Cols: `game_id, season, game_date, team_id, opp_team_id, team_home_away, team_score, opp_score, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted`. |
| `shell_game_schedule_2011.parquet` | 21 | `load_mbb_schedule([2011])` (ESPN), captured 2026-09-29 | Every 2011 game of New Orleans (`2443`) and Victory (`3129`), raw ESPN columns and dtypes (`game_id, season, date, game_date, home_id, away_id, neutral_site`). Regression slice for the zero-possession shell game `310573129`. |
| `shell_game_team_box_2011.parquet` | 42 | `load_mbb_team_boxscore([2011])` (ESPN), captured 2026-09-29 | Both team rows of those 21 games (16 teams): `game_id, team_id, team_display_name, team_score, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted`. Game `310573129` ships a real score with FGA/OREB/TO/FTA all `0`. |
| `torvik_2024.parquet` | 350 | barttorvik `https://barttorvik.com/2024_team_results.csv` | `adj_o`=adjoe, `adj_d`=adjde, `adj_em`=adjoe−adjde, `rank`. Keyed to ESPN `team_id` via a contracting normalizer + `St.`→`State`/`Saint` candidate keys + a small alias table (`dev/mbb_prediction/capture_oracle.py`). |
| `espn_bpi_2024.parquet` | 362 | `espn_mbb_season_powerindex(2024, team_id=...)` (ESPN Core v2, one request per team) | End-of-season BPI per D1 team: `team_id, team, bpi, bpi_rank, bpi_offense, bpi_defense, sos, sos_rank, sor, sor_rank, wins, losses` (`sos`=BPI `sospast`, the SOS-to-date used by the Phase-4 gate). The season-level list endpoint is a fixed Top-25 leaderboard regardless of `limit`, hence per-team fetches. `team` name joined from the torvik fixture (null for the 12 torvik-unmatched). |
| `espn_predictor_sample.parquet` | 313 | `espn_mbb_game_predictor(game_id)` (ESPN Core v2) | Pregame `home_win_prob` = home `gameProjection` / 100. Sampled every 20th completed game by date (stratified across the season); 0 of 313 sampled games missing predictor data. |
| `espn_odds_sample.parquet` | 294 | `espn_mbb_game_odds(game_id)` (ESPN Core v2) | Closing `close_spread_home` (home point spread, negative = home favored) + `close_total` for the same sampled games (19 of 313 had no usable book). Book preference order: ESPN BET, DraftKings, Caesars, Betfair, MGM, Unibet, SugarHouse — first with an explicit close (`homeTeamOdds.close.pointSpread` + `close.total`), else the first with a stored line snapshot (top-level `spread`/`overUnder`). |
| `pbp_sample_2024.parquet` | 57887 | `load_mbb_pbp([2024])` (ESPN) | Phase-3 in-game WP calibration sample: every 25th play of ALL 4,326 eligible games (both teams ≥ 8 prior at the as-of date), with each game's as-of `pregame_home_prob` + `home_win` label attached. A few plays from every game beats every play from few games — plays within a game are correlated, so per-decile power is driven by game count. Regenerate with `uv run python dev/mbb_prediction/capture_oracle.py wp`. |
| `ncaa_tourney_2024.parquet` | 134 | `espn_mbb_scoreboard(dates=...)` (ESPN Site v2, one call per tournament date) | Actual 2024 NCAA tournament: two rows per game (67 games, 68 teams) with `seed` = ESPN `curatedRank.current` (ESPN swaps the AP rank for the committee seed during the tournament; verified 68 teams / seeds 1–16 / 16 top-4 seeds), scores, winners, and the round in `notes_headline`. Oracle for the Phase-5 seed-order gate (the bracketmatrix archive is unreachable — self-signed TLS — so the gate uses the actual committee seeds, a stronger reference) and the Phase-6 advancement-calibration gate. Regenerate with `... capture_oracle.py tourney`. |
| `mbb_late_game_2026.parquet` | 241049 | `load_mbb_pbp([2026])` (ESPN `espn_mens_college_basketball_pbp` release, read 2026-11-04) | In-game-WP holdout late-game gate (tests/mbb/test_mbb_in_game_wp_late_game.py): every play of the second half with `start_game_seconds_remaining <= 120`, plus every overtime play, of all 6,275 holdout games, scores forward-filled per game first, with the schedule's `neutral_site`. Written by `tools/models/mbb_prediction/train_in_game_wp.py`; 2026 is not a training season. |
| `baseline_in_game_wp/mbb_in_game_wp_eb4207476e.ubj` | -- | `sportsdataverse/mbb/models/mbb_in_game_wp.ubj` at eb4207476e | The pre-2026-11 men's booster (2023-only fit, WP 0.68 with a one-point lead at 0.1 s). The late-game gates must fail on it. |
| `mbb_rated_prior_games.parquet` | 12408 | `load_mbb_pbp([2025, 2026])` + `mbb_win_prob._pregame_probs` (ESPN `espn_mens_college_basketball_pbp` / schedule / team-box releases, read 2026-11-06) | The college pregame prior gate (tests/mbb/test_mbb_wp_model_cards.py): one row per finished, untied game of 2025 and 2026, `season, game_id, neutral_site, home_team_spread` (null: no college lines), `game_spread_available` (false), `rated_home_prob` (the weekly as-of rating; null for the 1274 games of the season's first days) and `y` (home won). Written by `tools/models/mbb_prediction/wp_model_cards.py evaluate`. |
| `pregame_projection_schedule_2027.parquet` / `pregame_projection_team_box_2027.parquet` / `pregame_projection_prior_2027.parquet` | 1129 / 1376 / 365 | `load_mbb_schedule([2027])` cut to games dated through 2026-11-20 (played and scheduled), `load_mbb_team_boxscore([2027])` for those games, and `load_preseason_prior(2027, ...)` (the 2025-26 release regressed), read 2026-11-13 | The college pregame projection tests (tests/mbb/test_mbb_pregame_projection.py): the real 2026-27 men's season through its first ten days, when the prior blend matters most. Schedule cols `game_id, season, game_date, home_id, away_id, neutral_site, status_type_completed` (ESPN Int32 ids); box cols the ratings engine's possession inputs. Written by `tools/models/mbb_prediction/build_pregame_projection_fixture.py --through 2026-11-20`. |
| `mbb_holdout_gate_sums.parquet` | 257139 | `load_mbb_pbp([2026])` scored through `score_win_prob` (ESPN `espn_mens_college_basketball_pbp` release, read 2026-11-06) | The 2026 holdout gate's per-game block totals (tests/mbb/test_mbb_wp_model_cards.py recomputes the gate from them): `game_id, block` (`live`, each period, `ot`, `final_minute`, `bin0`-`bin9`), `n` (live plays), `se` (summed squared error), `sp` / `sy` (summed prediction / outcome), `prior` (`hfa` / `rated`) and `model` (`shipped` / `replaced`, the eb4207476e booster). Written by `tools/models/mbb_prediction/wp_model_cards.py evaluate`. |
| `ot_game_2022_401364358.parquet` | 342 | `load_mbb_pbp([2022])` (ESPN `espn_mens_college_basketball_pbp` release, read 2026-11-04), game `401364358` | One overtime game whose overtime plays carry the game clock (0 and below), not the period clock: 35 rows `ot_adjusted_seconds` re-clocks (14,429 rows in the 2022 release, none from 2023). Raw release columns and dtypes; the release-enrichment / game-page parity test in tests/mbb/test_mbb_win_prob.py. |

## Oracle notes

- **Torvik is the Phase-1 ratings gate** (Spearman(`adj_em`, Torvik `adj_em`)
  ≥ 0.95 over the matched set). barttorvik ratings are keyed on team **name**;
  **350 of 362** teams (96.7%) matched to an ESPN `team_id`. The 12 unmatched
  are one-off small-school naming irregularities (American, FIU, Penn, Queens,
  Seattle, LIU, St. Thomas, Saint Francis, Southeastern Louisiana, IU Indy,
  East Texas A&M, Texas A&M-Corpus Christi) — dropped from the inner-join
  comparison, which is statistically immaterial to a 350-team rank correlation.
- **Name-match priority is ordered** (re-captured 2026-07-07): the original
  capture matched name variants from a *set*, so the lossy bare-base fallback
  ("Michigan St." → "michigan") could beat the faithful "X State" expansion —
  17 State schools were keyed to their flagship's `team_id` (34 duplicate
  rows). The matcher now tries identity → "X State" → bare base in order;
  `team_id` is unique. Fixing this moved the oracle gate from Spearman 0.952 /
  MAE 2.79 to **Spearman 0.990 / MAE 2.37**.
- **`adj_tempo` is null.** barttorvik's `trank.php?csv=1` (which carries the
  adjusted-tempo column) is bot-blocked (returns HTML); the static
  `2024_team_results.csv` has no tempo. This is fine: no gate depends on the
  Torvik tempo column — adjusted tempo (Task 1.3) is validated against a
  synthetic construction, not this oracle.

## ESPN per-game / per-team samples (captured 2026-07-07)

The three ESPN oracle fixtures are a rate-limited Core v2 scrape (~990
sequential requests; ESPN Core v2 403s under aggressive rate — pace is
env-tunable via `ESPN_CAPTURE_SLEEP`, sample size via
`ESPN_GAME_SAMPLE_TARGET`). Regenerate with
`uv run python dev/mbb_prediction/capture_oracle.py espn` (reads the
committed base fixtures for the game/team universe). Consumers:

- `espn_predictor_sample.parquet` — Phase 2 win-prob Brier gate vs ESPN BPI.
- `espn_odds_sample.parquet` — Phase 2 spread/total MAE vs closing lines.
- `espn_bpi_2024.parquet` — Phase 4 SoS Spearman gate (`sos` / `sos_rank`).
