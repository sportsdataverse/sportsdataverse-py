<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [PWHL prediction-spine (T5.3) oracle corpus](#pwhl-prediction-spine-t53-oracle-corpus)
  - [Held-out design (why these files split the way they do)](#held-out-design-why-these-files-split-the-way-they-do)
  - [`shots_train_2024_2025.parquet`](#shots_train_2024_2025parquet)
  - [`game_rates_heldout_2026.parquet`](#game_rates_heldout_2026parquet)
  - [`backtest_heldout_2026.parquet`](#backtest_heldout_2026parquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# PWHL prediction-spine (T5.3) oracle corpus

Committed offline validation corpus for
`sportsdataverse.pwhl.pwhl_xg_proxy` (the best-effort, first-of-its-kind
PWHL power-rating + win-probability spine built on a categorical
`shot_quality` -> empirical xG proxy). Consumed by
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

- **tier weights** are fit on **strictly-prior complete seasons** — 2024 for
  the 2025 (margin_sd-fitting) walk, 2024+2025 for the held-out 2026 walk —
  so no game is scored by weights fit on its own or later data. (This is a
  leak-free *subset* of what shipped `pwhl_ratings_from_proxy(as_of_date=)`
  uses; production additionally folds in intra-season pre-cutoff shots.)
- **`margin_sd`** is fit on the **2025 season only** (78 as-of games), with
  the held-out **2026 season kept entirely out of the fit**.

## `shots_train_2024_2025.parquet`

- **Source:** `load_pwhl_pbp` seasons 2024 + 2025 (the training pool — the
  seasons strictly prior to the held-out 2026), narrowed to `season`,
  `game_id`, `event`, `shot_quality`, `goal`.
- **Capture date:** 2026-07-11. **Row count:** 22,976 pbp events.
- Fitting `fit_shot_quality_xg` on this pool gives the held-out 2026 walk's
  weights: `quality=0.1201`, `non_quality=0.0501`, `fallback=0.0835`.
- **Provenance note on `shot_quality`:** two of its four raw values
  (`"Quality goal"` / `"Non quality goal"`) ARE goals by construction (the
  categorical label encodes outcome for made shots), so the fitted weight is
  a genuine empirical goal-rate over each 2-tier quality bucket, not an
  independent predictive signal — see `pwhl_xg_proxy`'s module docstring.

## `game_rates_heldout_2026.parquet`

- **Source:** `pwhl_xg_proxy.pwhl_team_game_xg_rates` on the held-out 2026
  season, scored by the 2024+2025-fit shot-quality model.
- **Dates** come from `load_pwhl_game_info`'s `game_date_iso` (ISO
  timestamp), NOT `load_pwhl_schedules`'s `game_date` (a year-less
  `"Wed, May 8"` string with no reliable chronological parse).
- **Even-strength filter:** best-effort `power_play != 1` (nulls kept) —
  PWHL's `power_play`/`short_handed`/`empty_net` tagging covers well under
  half of shot rows.
- **Row count:** 240 (2026 regular-season games, both home+away rows).

## `backtest_heldout_2026.parquet`

- **Source:** the held-out 2026 as-of-date walk-forward, built by
  `dev/pwhl_prediction/build_pwhl_xg_fixture.py`: for each 2026 game-date
  beyond a 10-date burn-in (67/77 dates evaluated), `adjust_rate_opponent`
  (the SAME function the NHL spine uses) opponent-adjusts + shrinks the
  as-of-date-filtered `game_rates`, `nhl_market.expected_goals` scores the
  day's games, and `home_win_prob = Phi(exp_margin / margin_sd)` uses the
  **2025-fit** `margin_sd = 1.21` (now `LEAGUE_CONSTANTS["pwhl"].margin_sd`).
- **Row count:** 107 evaluated games.
- **Honest held-out result (2026, 2026-07-11):**
  - naive Brier (p=0.5) = 0.2500
  - held-out model Brier = 0.2449 — a delta of only **−0.0051, WITHIN
    sampling noise** (per-game Brier-diff SE ≈ 0.0053 on 107 games, so ≈1 SD
    from zero). The model does **not** robustly beat naive out-of-sample.
  - directionally correct: top-half-predicted games won 0.593 vs bottom-half
    0.528; predictions cluster in 0.495–0.597 (heavily shrink-compressed).
  - calibration (single adequately-sampled base-rate bucket): mean_pred
    0.5505 vs mean_actual 0.5607, dev ~0.0102.
  - **Gate (see the test):** held-out CALIBRATION + no-worse-than-naive
    within noise. The beats-naive *magnitude* assertion is deliberately
    dropped — the edge is within noise, exactly as expected for a
    first-of-its-kind scaffold on ~2 prior seasons of a 6-team league with a
    categorical (not coordinate-based) xG proxy and a strong shrinkage prior.
    A powered magnitude gate needs more PWHL seasons.

All frames carry `Utf8` `game_id` (cast from the raw Int64/Int32, never a
float) and `team`/`opp_team`/`home_team`/`away_team` as the schedule's clean
city names (NOT pbp's own `"PWHL "`-prefixed names) per the ID/join-key
discipline in `CLAUDE.md`.
