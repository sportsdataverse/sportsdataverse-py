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
under `dev/`).

**ID convention:** every team / game id is `Utf8` (cast from the raw ESPN
integer via `pl.col(id).cast(pl.Int64).cast(pl.Utf8)`), so joins across
fixtures and against the ratings engine never hit a dtype mismatch.

| File | Rows | Source | Notes |
|---|---:|---|---|
| `results_2024.parquet` | 6243 | `load_mbb_schedule([2024])` (ESPN, via sportsdataverse-data) | Completed games only (`status_type_completed`), deduped on `game_id`. Cols: `game_id, season, date, home_team_id, away_team_id, home_score, away_score, neutral_site`. |
| `team_box_2024.parquet` | 12480 | `load_mbb_team_boxscore([2024])` (ESPN) | Per-team possession inputs for the ratings engine. Cols: `game_id, season, game_date, team_id, opp_team_id, team_home_away, team_score, opp_score, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted`. |
| `torvik_2024.parquet` | 350 | barttorvik `https://barttorvik.com/2024_team_results.csv` | `adj_o`=adjoe, `adj_d`=adjde, `adj_em`=adjoe−adjde, `rank`. Keyed to ESPN `team_id` via a contracting normalizer + `St.`→`State`/`Saint` candidate keys + a small alias table (`dev/mbb_prediction/capture_oracle.py`). |
| `espn_bpi_2024.parquet` | 362 | `espn_mbb_season_powerindex(2024, team_id=...)` (ESPN Core v2, one request per team) | End-of-season BPI per D1 team: `team_id, team, bpi, bpi_rank, bpi_offense, bpi_defense, sos, sos_rank, sor, sor_rank, wins, losses` (`sos`=BPI `sospast`, the SOS-to-date used by the Phase-4 gate). The season-level list endpoint is a fixed Top-25 leaderboard regardless of `limit`, hence per-team fetches. `team` name joined from the torvik fixture (null for the 12 torvik-unmatched). |
| `espn_predictor_sample.parquet` | 313 | `espn_mbb_game_predictor(game_id)` (ESPN Core v2) | Pregame `home_win_prob` = home `gameProjection` / 100. Sampled every 20th completed game by date (stratified across the season); 0 of 313 sampled games missing predictor data. |
| `espn_odds_sample.parquet` | 294 | `espn_mbb_game_odds(game_id)` (ESPN Core v2) | Closing `close_spread_home` (home point spread, negative = home favored) + `close_total` for the same sampled games (19 of 313 had no usable book). Book preference order: ESPN BET, DraftKings, Caesars, Betfair, MGM, Unibet, SugarHouse — first with an explicit close (`homeTeamOdds.close.pointSpread` + `close.total`), else the first with a stored line snapshot (top-level `spread`/`overUnder`). |
| `pbp_sample_2024.parquet` | 48292 | `load_mbb_pbp([2024])` (ESPN) | Phase-3 in-game WP calibration sample: every play of 150 eligible games (both teams ≥ 8 prior at the as-of date, every 29th eligible game by date), with each game's as-of `pregame_home_prob` + `home_win` label attached. Regenerate with `uv run python dev/mbb_prediction/capture_oracle.py wp`. |

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
