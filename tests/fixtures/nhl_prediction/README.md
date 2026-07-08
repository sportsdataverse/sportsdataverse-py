<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NHL/PWHL prediction-spine oracle corpus (2022-23 / "season 2023")](#nhlpwhl-prediction-spine-oracle-corpus-2022-23--season-2023)
  - [Files](#files)
  - [Deliberately not committed](#deliberately-not-committed)
  - [Known gaps / adaptations (binding: gates set from what's actually observable)](#known-gaps--adaptations-binding-gates-set-from-whats-actually-observable)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NHL/PWHL prediction-spine oracle corpus (2022-23 / "season 2023")

Captured 2026-07-08 via `dev/nhl_prediction/capture_oracle.py`
(`SDV_PY_LIVE_TESTS=1 uv run python dev/nhl_prediction/capture_oracle.py`).
Validation season: **2023** (sdv-py / MoneyPuck convention for the 2022-23
NHL regular season). All fixtures are regular-season only (`game_type == "R"`)
to stay comparable with MoneyPuck's `regular` folder.

## Files

| File | Source | Rows | Notes |
|---|---|---:|---|
| `moneypuck_teams_2023.parquet` | MoneyPuck `teams.csv`, `situation=="5on5"`, `season=2022` (2022-23) | 32 | Per-game normalised (`xgf = xGoalsFor / games_played`, etc). `team: Utf8` abbreviation, matches sdv-py's own NHL abbreviations exactly (`ARI`, not `UTA` -- pre-relocation season). |
| `results_2023.parquet` | `load_nhl_schedules([2023])`, `game_type=="R"`, `game_state != "FUT"` | 1312 | `game_id: Utf8`, `date: Date`, `home_team`/`away_team: Utf8`, `home_goals`/`away_goals: Int64`, `neutral_site` hardcoded `False` (not tracked upstream; NHL had no true neutral-site games in 2022-23). |
| `espn_power_2023.parquet` | `espn_nhl_season_powerindex_leaders` | **0** | **Confirmed genuinely empty at the ESPN API** -- `count: 0` for every season tried 2021-2026 directly against `sports.core.api.espn.com/.../powerindex/leaders`. ESPN has not populated this endpoint for the NHL league. Dropped as a Phase-1 secondary oracle; the tertiary raw-vs-adjusted-xG sanity check stands in its place. |
| `espn_predictor_sample.parquet` | `espn_nhl_game_predictor` | **0** | **Confirmed permanently unsupported** -- every call returns HTTP 400 `"Predictor is not supported for [hockey/nhl]"`. ESPN does not run a win-probability predictor model for hockey. Task 2.3's win-prob gate is adapted to compare against a documented naive baseline instead of an ESPN-predictor Brier score. |
| `espn_odds_sample.parquet` | `espn_nhl_game_odds`, sampled across 7 dates spanning the season | 20 | `game_id: Utf8` (crosswalked from ESPN's own `event_id` via `(date, home_team, away_team)` after normalising ESPN's 4 divergent abbreviations: `LA->LAK`, `NJ->NJD`, `SJ->SJS`, `TB->TBL`). `close_puck_line_home`/`close_total` taken from the first odds-provider row returned (not necessarily identical across games/providers -- documented limitation, not a correctness issue since MAE floors are set from these same observed values). |
| `espn_propbets_sample.parquet` | `espn_nhl_game_propbets` | **0** | **Confirmed 404 for every 2022-23 game tried** (10+ events across multiple dates). Matches the design spec's documented expectation that prop-bet lines are ephemeral and not retained for past games. Task 4.2's propbets-MAE check is skipped when this fixture is empty (documented, not fabricated). |
| `pbp_sample_2023.parquet` | `load_nhl_pbp_full([2023])`, first 5 `game_id`s | 1678 | Full-shape (93-column) real slice for offline unit tests of `nhl_team_ratings`/`nhl_market`/`in_game_features` against the real schema (not synthetic). |

## Deliberately not committed

- **`pbp_2023.parquet` (full season, ~1.1M rows / 93 cols)** -- re-downloaded
  on demand from the `nhl_pbp_full` sportsdataverse-data release (no gate
  required, not an API call) rather than committed, to keep the repo lean.
- **`team_xg_2023.parquet`** -- this is the *output* of Task 1.1's
  `team_game_xg_rates`, not a Task-0.1 input; it is captured once that
  function exists (see the Task 1.1 commit) rather than duplicating the
  aggregation logic here.

## Known gaps / adaptations (binding: gates set from what's actually observable)

1. **ESPN power-index leaders** and **ESPN game predictor** are genuinely
   unavailable for the NHL league at ESPN's API (confirmed live, not a
   wrapper bug) -- Phase 1's secondary gate and Phase 2's "vs ESPN
   predictor" gate are adapted to use available oracles/baselines instead.
   No gate is silently dropped without a documented reason in the
   corresponding test module.
2. **ESPN propbets** are not retained for past games -- Phase 4's
   propbets-MAE check only runs when a matching line is present (currently
   none), consistent with the design spec's own acknowledged sparsity.
3. **`load_nhl_skater_boxscores` only supports seasons >= 2024** -- the
   2023 skater-boxscore realized-stats backtest for Phase 4 is not possible
   against this fixture corpus; Phase 4's fit/backtest uses **season 2024**
   for skater boxscores (documented in `dev/nhl_prediction/fit_props.py` and
   the Phase-4 tests) instead of 2023, while ratings/market (Phases 1-3)
   stay on the 2023 corpus above.
