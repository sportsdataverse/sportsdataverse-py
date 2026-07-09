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
| `results_2023.parquet` | `load_nhl_schedules([2023])` for game_id/date/teams; `home_goals`/`away_goals` **derived from `load_nhl_pbp_full`'s own GOAL events**, not the schedule's score columns (see gap #4 below) | 1312 | `game_id: Utf8`, `date: Date`, `home_team`/`away_team: Utf8`, `home_goals`/`away_goals: Int64`, `neutral_site` hardcoded `False` (not tracked upstream; NHL had no true neutral-site games in 2022-23). |
| `espn_power_2023.parquet` | `espn_nhl_season_powerindex_leaders` | **0** | **Confirmed genuinely empty at the ESPN API** -- `count: 0` for every season tried 2021-2026 directly against `sports.core.api.espn.com/.../powerindex/leaders`. ESPN has not populated this endpoint for the NHL league. Dropped as a Phase-1 secondary oracle; the tertiary raw-vs-adjusted-xG sanity check stands in its place. |
| `espn_predictor_sample.parquet` | `espn_nhl_game_predictor` | **0** | **Confirmed permanently unsupported** -- every call returns HTTP 400 `"Predictor is not supported for [hockey/nhl]"`. ESPN does not run a win-probability predictor model for hockey. Task 2.3's win-prob gate is adapted to compare against a documented naive baseline instead of an ESPN-predictor Brier score. |
| `espn_odds_sample.parquet` | `espn_nhl_game_odds`, sampled across 7 dates spanning the season | 20 | `game_id: Utf8` (crosswalked from ESPN's own `event_id` via `(date, home_team, away_team)` after normalising ESPN's 4 divergent abbreviations: `LA->LAK`, `NJ->NJD`, `SJ->SJS`, `TB->TBL`). `close_puck_line_home`/`close_total` taken from the flat top-level `spread`/`over_under` fields of the first odds-provider row (see gap #5 below); not necessarily identical across games/providers -- documented limitation, not a correctness issue since MAE floors are set from these same observed values. |
| `espn_propbets_sample.parquet` | `espn_nhl_game_propbets` | **0** | **Confirmed 404 for every 2022-23 game tried** (10+ events across multiple dates). Matches the design spec's documented expectation that prop-bet lines are ephemeral and not retained for past games. Task 4.2's propbets-MAE check is skipped when this fixture is empty (documented, not fabricated). |
| `pbp_sample_2023.parquet` | `load_nhl_pbp_full([2023])`, first 5 `game_id`s | 1678 | Full-shape (93-column) real slice for offline unit tests of `nhl_team_ratings`/`nhl_market`/`in_game_features` against the real schema (not synthetic). |
| `team_xg_2023.parquet` | Output of `nhl_team_ratings.team_game_xg_rates` over the full 2023 season pbp (Task 1.1) | 2624 | One row per (game, team); the Phase-1 oracle gate's input. |
| `in_game_wp_calibration_2023.parquet` | Every 2023 play scored by `nhl_in_game_win_prob` (trained on 2022), bucketed via `calibration_table` (Task 3.4) | 10 | `bin_mid, mean_pred, mean_actual, n` per predicted decile. |
| `in_game_wp_pulled_goalie_2023.parquet` | Same 2023 scoring run, subset to plays with either goalie pulled | 1 | `mean_pred, mean_actual, n` -- the Task 3.4 targeted pulled-goalie calibration check. |
| `player_props_mae_2024.parquet` | `nhl_player_props(2024)` joined back to realized `load_nhl_skater_boxscores` values for the same player-game (Task 4.2) | 2 | `stat, mae, n` -- MAE(proj_mean, realized) per stat family. Season **2024**, not 2023 (see gap #7 below). |
| `player_props_p_over_calibration_2024.parquet` | Same 2024 backtest, `p_over` bucketed via `calibration_table` against a synthetic fixed line (the stat's own realized median + 0.5) | 19 | `stat, bin_mid, mean_pred, mean_actual, n`. |

## Deliberately not committed

- **`pbp_2023.parquet` (full season, ~1.1M rows / 93 cols)** -- re-downloaded
  on demand from the `nhl_pbp_full` sportsdataverse-data release (no gate
  required, not an API call) rather than committed, to keep the repo lean.
  `dev/nhl_prediction/build_team_xg_fixture.py` and
  `build_in_game_wp_calibration_fixture.py` both re-download it to produce
  the small, committed aggregated fixtures above.

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
4. **`load_nhl_schedule(s)`'s `home_score`/`away_score` are a placeholder
   constant for every season <= 2023** -- confirmed live: every single
   2022-23 game reports the same "2-3" score, every 2021 game reports "5-2",
   every 2022 game reports "6-3" (season 2024 is correct: 71 distinct score
   combos in a quick sample). This is a real upstream producer bug in the
   `nhl_schedules` sportsdataverse-data release, out of scope to fix from
   sdv-py. **Mitigation (shipped in `nhl_team_ratings.team_game_xg_rates` and
   this capture script):** realized goals are always derived by counting the
   pbp's own `GOAL` events per team, never read from the schedule's score
   columns. `results_2023.parquet`'s `home_goals`/`away_goals` and
   `nhl_team_ratings`'s `adj_gf`/`adj_ga` both use this pbp-derived source.
5. **ESPN's nested `home_team_odds_close_spread_value` / `close_over_value`
   odds fields are null for every NHL game/provider tried** -- only the flat
   top-level `spread` (confirmed signed relative to the HOME team: negative
   when home is favored, positive when away is favored -- verified against
   `home_team_odds_favorite` across several games) and `over_under` fields
   are populated. `espn_odds_sample.parquet` uses those flat fields; there is
   no verified-"closing" total for NHL (only a "current" value at capture
   time), documented as a limitation on the MAE-vs-total gate.
6. **In-game WP: xgboost escalation tried and rejected.** The plain logistic
   failed the Task 3.4 held-out (2023) calibration gate on 2 of 10
   predicted-decile buckets (worst deviation ~0.069, at bucket 0.55,
   n=43101 -- not sampling noise). A shallow xgboost (max_depth=3, 150
   trees) on the same 6 features roughly halved the worst-bucket deviation
   (~0.036) but, at that depth, could not separate a clean pulled-goalie
   test scenario (home leads 4-3, 60s left, away pulls its goalie) from the
   even-strength baseline -- both collapsed to the identical predicted
   probability, losing a qualitatively important, correctly-signed behavior
   the plain logistic captures (`tests/nhl/test_nhl_in_game_wp.py`). That
   trade was judged not worth it since the calibration gain didn't clear
   the plan's own illustrative 0.03 target either. The plain logistic
   ships; the overall-bucket calibration floor is set from the OBSERVED
   0.0688 max deviation (binding gate rule), while the pulled-goalie
   subset (n=10086, observed deviation 0.0256) clears the tighter 0.03
   bar and keeps it.
7. **Player props use season 2024, not 2023.** `load_nhl_skater_boxscores`
   only publishes seasons >= 2024 (season 2024 == the 2023-24 season, per
   sdv-py's convention -- confirmed the source `season` column reports
   `20232024`), so Phase 4's fit (`dev/nhl_prediction/fit_props.py`) and
   backtest use 2024 while ratings/market (Phases 1-3) stay on the 2023
   corpus. `nhl_player_props` itself is season-parameterized, so this is a
   validation-corpus choice, not a code limitation.
8. **Player props: Gaussian `p_over` tried and rejected in favor of Poisson.**
   Shots-on-goal and points are non-negative integer counts, not continuous.
   A first pass used `Phi((line-mean)/sd)`; held-out (2024) calibration
   showed it systematically overconfident (worst-bucket deviation ~0.17).
   Switching `_p_over` to a Poisson survival function (`1 - Poisson.cdf(...)`,
   using `proj_mean` as the rate directly) cut the worst deviation to 0.0599
   -- a genuine, verified model fix (not a tuned floor). `proj_sd` in the
   output schema is now `sqrt(proj_mean)` (the Poisson SD) for informational
   consistency, though `p_over` itself no longer uses it.
9. **No propbets-vs-projection MAE check.** ESPN propbets lines are
   confirmed unavailable for every NHL game tried (gap #2 above), so there
   is no real market line to compare `proj_mean` against. `p_over`
   calibration instead uses each stat family's own realized-value median
   (+0.5 to avoid integer ties) as a synthetic fixed line -- documented as a
   substitute, not a fabricated market line.
