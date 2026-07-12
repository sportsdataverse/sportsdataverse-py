<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [PWHL prediction-spine (T5.3) oracle corpus](#pwhl-prediction-spine-t53-oracle-corpus)
  - [`shots_2024_2026.parquet`](#shots_2024_2026parquet)
  - [`game_rates_2024_2026.parquet`](#game_rates_2024_2026parquet)
  - [`backtest_predictions_2024_2026.parquet`](#backtest_predictions_2024_2026parquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# PWHL prediction-spine (T5.3) oracle corpus

Committed offline validation corpus for
`sportsdataverse.pwhl.pwhl_xg_proxy` (the best-effort, first-of-its-kind
PWHL power-rating + win-probability spine built on a categorical
`shot_quality` -> empirical xG proxy). Consumed by
`tests/pwhl/test_pwhl_xg_proxy_oracle.py`. No network access required to run
the oracle suite once these files exist.

Regenerate: `uv run python dev/pwhl_prediction/build_pwhl_xg_fixture.py`.

## `shots_2024_2026.parquet`

- **Source:** `sportsdataverse.pwhl.pwhl_loaders.load_pwhl_pbp` (seasons 2024,
  2025, 2026 -- the 3 PWHL seasons published as of capture date), narrowed to
  `season`, `game_id`, `event`, `shot_quality`, `goal`.
- **Capture date:** 2026-07-11.
- **Row count:** 39,279 pbp events across 3 seasons (all event types, not
  filtered to `event == "shot"` -- the fitting function does that filter
  itself). `event == "shot"` rows: 4,922 (2024) / ~5,300+ (2025) / ~5,700+
  (2026); every one carries a non-null `shot_quality`.
- **Provenance note on `shot_quality`:** two of its four raw values
  (`"Quality goal"` / `"Non quality goal"`) ARE goals by construction (the
  categorical label encodes outcome for made shots), so the fitted weight is
  a genuine empirical goal-rate over each 2-tier quality bucket, not an
  independent predictive signal -- see `pwhl_xg_proxy`'s module docstring.

## `game_rates_2024_2026.parquet`

- **Source:** `pwhl_xg_proxy.pwhl_team_game_xg_rates`, run per-season (pbp
  schema drifts season-to-season -- 95/97/95 columns for 2024/2025/2026 --
  so each season is loaded and processed independently, never concatenated
  raw) with the ONE shot-quality model fit pooled across all 3 seasons'
  shots (`fit_shot_quality_xg` on `shots_2024_2026.parquet`, weights:
  `quality=0.1213`, `non_quality=0.0467`, `fallback=0.0824`).
- **Dates** come from `load_pwhl_game_info`'s `game_date_iso` (ISO
  timestamp), NOT `load_pwhl_schedules`'s `game_date` (a year-less
  `"Wed, May 8"` string with no reliable chronological parse).
- **Even-strength filter:** best-effort `power_play != 1` (nulls kept) --
  PWHL's `power_play`/`short_handed`/`empty_net` tagging covers well under
  half of shot rows in the captured seasons.
- **Row count:** 564 (regular-season games only, both home+away rows;
  85+102+133 = 320 total scheduled games including playoffs across the 3
  seasons, 282 regular-season games x 2 = 564).

## `backtest_predictions_2024_2026.parquet`

- **Source:** an as-of-date walk-forward per season (never mixing ratings
  across seasons -- PWHL team strength resets each season), built by
  `dev/pwhl_prediction/build_pwhl_xg_fixture.py`: for each season's unique
  game-dates beyond a 10-date burn-in (2024: 41/51 dates evaluated; 2025:
  70/80; 2026: 67/77), `adjust_rate_opponent` (the SAME function the NHL
  spine uses) opponent-adjusts + shrinks the as-of-date-filtered
  `game_rates`, and `nhl_market.expected_goals` / `win_prob_from_margin`
  (also directly reused, not reimplemented) score that day's games.
- **`margin_sd` was fit** as a byproduct (grid search,
  `dev/pwhl_prediction/fit_pwhl_margin_sd.py`, minimizing Brier of
  `Phi(exp_margin/margin_sd)`): `0.83`, replacing the old `2.35` seed --
  `exp_margin`'s realized spread on this proxy is heavily shrink-compressed
  (std ~0.065, not real-world-goal-margin scale), so the old seed
  systematically under-confident every prediction toward 0.5. This is now
  `LEAGUE_CONSTANTS["pwhl"].margin_sd` in `nhl_prediction_constants.py`;
  `hfa`/`avg_xgf`/`avg_total_goals`/`total_scale`/`prop_*` remain seeded
  placeholders (out of scope for this session's fit).
- **Row count:** 244 evaluated games.
- **Observed at gate-authoring time (2026-07-11):**
  - naive Brier (p=0.5) = 0.2500
  - model Brier = 0.2438 (beats naive, modestly)
  - calibration (5-bin, n>=30 buckets only): 2 populated buckets --
    `mean_pred=0.561 / mean_actual=0.518` (n=199) and
    `mean_pred=0.616 / mean_actual=0.733` (n=45); max deviation ~0.118
    (the smaller, higher-predicted bucket -- underconfident, not
    overconfident, direction).
  - **Honest limitation:** this is a genuine but MODEST improvement over
    a coin flip, not a strong predictive model. PWHL's short single-season
    history (~12-24 games/team), a 6-8 team league (much less cross-team
    variance than a 32-team NHL), the strong shrinkage prior
    (`shrink_k=25`), and the categorical (not coordinate-based) xG proxy all
    compound to compress the signal. No external PWHL oracle exists to
    compare against (first-of-its-kind); the naive-baseline + calibration
    gate below IS the honest internal oracle.

Both `game_rates_2024_2026.parquet` and `backtest_predictions_2024_2026.parquet`
carry `Utf8` `game_id` (cast from the raw Int64/Int32, never a float) and
`team`/`opp_team`/`home_team`/`away_team` as the schedule's clean city names
(NOT pbp's own `"PWHL "`-prefixed names) per the ID/join-key discipline in
`CLAUDE.md`.
