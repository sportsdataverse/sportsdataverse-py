<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [PWHL prediction-spine (T5.3/T5.3b) oracle corpus](#pwhl-prediction-spine-t53t53b-oracle-corpus)
  - [Held-out design (why these files split the way they do)](#held-out-design-why-these-files-split-the-way-they-do)
  - [`shots_train_2024_2025.parquet`](#shots_train_2024_2025parquet)
  - [`game_rates_heldout_2026.parquet`](#game_rates_heldout_2026parquet)
  - [`backtest_heldout_2026.parquet`](#backtest_heldout_2026parquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# PWHL prediction-spine (T5.3/T5.3b) oracle corpus

Committed offline validation corpus for
`sportsdataverse.pwhl.pwhl_xg_proxy` (the best-effort, first-of-its-kind
PWHL power-rating + win-probability spine), covering BOTH xG methods: the
T5.3b **coordinate distance/angle logistic** (`fit_pwhl_coord_xg`, the
default -- geometry from
`sportsdataverse.hockeytech._analytics.add_shot_distance_angle`) and the
original T5.3 categorical `shot_quality` -> empirical-goal-rate proxy
(`fit_shot_quality_xg`). Consumed by
`tests/pwhl/test_pwhl_xg_proxy_oracle.py`. No network access required to run
the oracle suite once these files exist.

Regenerate: `uv run python dev/pwhl_prediction/build_pwhl_xg_fixture.py`.

## Held-out design (why these files split the way they do)

The gate is an **out-of-sample** backtest. An earlier cut leaked two ways
(fixed in review): `margin_sd` was grid-searched in-sample on the same games
it scored (making the beats-naive gate non-falsifiable, since
`Phi(exp_margin/margin_sd) -> 0.5 = naive` as `margin_sd` grows), and the
shot-quality tier weights were pooled across all 3 seasons and reused inside
every season's as-of walk. The de-leaked split:

- **the xG model** (tier weights for `quality`, the coordinate logistic for
  `coords`) is fit on **strictly-prior complete seasons** — 2024 for the
  2025 (margin_sd-fitting) walk, 2024+2025 for the held-out 2026 walk — so
  no game is scored by a model fit on its own or later data. (This is a
  leak-free *subset* of what shipped `pwhl_ratings_from_proxy(as_of_date=)`
  uses; production additionally folds in intra-season pre-cutoff shots.)
- **`margin_sd`** is fit **per method** on the **2025 season only** (78
  as-of games; quality 1.21, coords 1.19 — a different xG scale changes the
  exp_margin scale), with the held-out **2026 season kept entirely out of
  the fit**.

## `shots_train_2024_2025.parquet`

- **Source:** `load_pwhl_pbp` seasons 2024 + 2025 (the training pool — the
  seasons strictly prior to the held-out 2026), narrowed to `season`,
  `game_id`, `event`, `shot_quality`, `goal`, **`x_coord`, `y_coord`** (the
  coordinate columns added in the T5.3b rebuild; rink-feet frame, nets at
  `|x| = 89` — coordinate frames documented in the HockeyTech design spec,
  `docs/superpowers/specs/2026-06-09-hockeytech-multi-league-scraper-analytics-design.md`).
- **Capture date:** 2026-07-12 (T5.3b dual-method rebuild). **Row count:**
  22,976 pbp events; 10,593 coord-complete `event == "shot"` rows.
- Fitting `fit_shot_quality_xg` on this pool gives the held-out 2026 walk's
  weights: `quality=0.1201`, `non_quality=0.0501`, `fallback=0.0835`.
- Fitting `fit_pwhl_coord_xg` on this pool gives the held-out 2026 walk's
  coordinate model: fit N 10,593, goal rate 0.0835, in-sample AUC **0.6745**,
  in-sample logloss **0.2733** vs base-rate 0.2871, fitted curve at 4/30/64
  ft straight-on = **[0.2482, 0.1152, 0.0372]** (monotone in distance;
  point-blank ~6.7x long-range). These observed values set the `COORD_*`
  gate floors in the oracle test.
- **Provenance note on `shot_quality`:** two of its four raw values
  (`"Quality goal"` / `"Non quality goal"`) ARE goals by construction (the
  categorical label encodes outcome for made shots), so the fitted weight is
  a genuine empirical goal-rate over each 2-tier quality bucket, not an
  independent predictive signal — see `pwhl_xg_proxy`'s module docstring.

## `game_rates_heldout_2026.parquet`

- **Source:** `pwhl_xg_proxy.pwhl_team_game_xg_rates` on the held-out 2026
  season, scored by the 2024+2025-fit **coordinate** model (the default
  `xg_method="coords"` since the T5.3b rebuild).
- **Dates** come from `load_pwhl_game_info`'s `game_date_iso` (ISO
  timestamp), NOT `load_pwhl_schedules`'s `game_date` (a year-less
  `"Wed, May 8"` string with no reliable chronological parse).
- **Even-strength filter:** `power_play != 1` (nulls kept). The loader's
  `power_play` tags are already the producer-side PP-window back-fill (same
  logic as `sportsdataverse.hockeytech._analytics.backfill_power_play`);
  null means "not inside a derived PP window". Re-running the Python
  back-fill per game re-derives them nearly identically (2025: 938 producer
  PP tags vs 921 re-derived on 5,671 shot rows), so the producer tags are
  used as-is.
- **Row count:** 240 (2026 regular-season games, both home+away rows).

## `backtest_heldout_2026.parquet`

- **Source:** the held-out 2026 as-of-date walk-forward, built by
  `dev/pwhl_prediction/build_pwhl_xg_fixture.py`, run for BOTH methods over
  the identical walk: for each 2026 game-date beyond a 10-date burn-in,
  `adjust_rate_opponent` (the SAME function the NHL spine uses)
  opponent-adjusts + shrinks the as-of-date-filtered `game_rates`,
  `nhl_market.expected_goals` scores the day's games, and
  `home_win_prob = Phi(exp_margin / margin_sd)` uses each method's own
  **2025-fit** `margin_sd` (quality 1.21; coords 1.19 — the coords value is
  now `LEAGUE_CONSTANTS["pwhl"].margin_sd` since coords is the default).
- **Columns:** the quality method keeps the legacy names (`exp_margin`,
  `home_win_prob`); the coordinate method adds `exp_margin_coords`,
  `home_win_prob_coords` (joined on `game_id` with an asserted 1:1 match and
  `game_id` dtype equality).
- **Row count:** 107 evaluated games (identical game set for both methods).
- **Honest held-out result (2026, 2026-07-12 rebuild):**
  - naive Brier (p=0.5) = 0.2500
  - quality Brier = 0.2449 (delta **−0.0051**, SE ≈ 0.0053)
  - coords Brier = **0.2444** (delta **−0.0056**, SE ≈ 0.0055)
  - paired coords-minus-quality per-game Brier diff = **−0.0005**
    (paired SE ≈ 0.0006, n=107) — coords is better on the identical games,
    but within noise.
  - calibration (adequately-sampled buckets, n ≥ 30): quality mean_pred
    0.5505 vs mean_actual 0.5607 (dev ~0.0102); coords 0.5492 vs 0.5437
    (dev ~0.0055, n=103 bucket).
  - **Default decision (the oracle rule):** coords gates better held-out on
    every measured axis AND carries real shot-level signal (AUC 0.6745 vs
    the 0.0835 base rate) with no outcome-tautology in its features, so
    **`xg_method="coords"` is the default**. The quality proxy path is kept
    working unchanged for API stability.
  - **Gate (see the test):** held-out CALIBRATION + no-worse-than-naive
    within noise, for BOTH methods. The beats-naive *magnitude* assertion
    remains deliberately absent — neither method's edge reaches 2 SE, exactly
    as expected for a first-of-its-kind scaffold on ~2 prior seasons of a
    6-team league with a strong shrinkage prior. A powered magnitude gate
    needs more PWHL seasons; never add a gate that cannot fail.

All frames carry `Utf8` `game_id` (cast from the raw Int64/Int32, never a
float) and `team`/`opp_team`/`home_team`/`away_team` as the schedule's clean
city names (NOT pbp's own `"PWHL "`-prefixed names) per the ID/join-key
discipline in `CLAUDE.md`.
