<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MLB fielding/catching/baserunning oracle corpus (T6.3)](#mlb-fieldingcatchingbaserunning-oracle-corpus-t63)
  - [Known deviations from a naive literal read of the design docs](#known-deviations-from-a-naive-literal-read-of-the-design-docs)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MLB fielding/catching/baserunning oracle corpus (T6.3)

Captured 2026-07-10 via `dev/mlb_fielding/capture_oracle.py`
(`SDV_PY_LIVE_TESTS=1 uv run python dev/mlb_fielding/capture_oracle.py`).
All leaderboard id columns (`player_id`, `entity_id`) are cast `Utf8` from
the raw integer (never a float) at capture time.

| File | Source | Notes |
|---|---|---|
| `pitches_2024-06.parquet` | `mlb_statcast_search("2024-06-01","2024-06-30", season=2024)` | 116,355 pitches; `delta_run_exp` non-null on 99.7% -- RE288 is the primary path (no `RUN_VALUES` fallback needed for the count table). |
| `bip_2024.parquet` | Derived from the same month capture, filtered `type == "X"` (ball in play) -- no separate live pull | 20,623 balls in play |
| `lb_catcher_framing_2024.parquet` | `mlb_statcast_leaderboard_catcher_framing(year=2024)` | 60 rows; id column is `id`, NOT `player_id`; the framing-runs column is `rv_tot` (there is no `runs_extra_strikes` column in the real payload -- the plan's assumed column name was wrong) |
| `lb_catcher_blocking_2024.parquet` | `mlb_statcast_leaderboard_catcher_blocking(year=2024)` | 71 rows; `catcher_blocking_runs` is the oracle target |
| `lb_catcher_throwing_2024.parquet` | `mlb_statcast_leaderboard_catcher_throwing(year=2024)` | 74 rows; `catcher_stealing_runs` is the oracle target |
| `lb_poptime_2024.parquet` | `mlb_statcast_leaderboard_poptime(year=2024)` | 83 rows; id column is `entity_id`, NOT `player_id`; carries `pop_2b_sba` (matches this spine's default `pop_col`) |
| `lb_arm_strength_2024.parquet` | `mlb_statcast_leaderboard_arm_strength(year=2024)` | 388 rows |
| `lb_oaa_2024.parquet` | `mlb_statcast_leaderboard_outs_above_average(year=2024)` | 272 rows; `outs_above_average` is the oracle target |
| `lb_catch_probability_2024.parquet` | `mlb_statcast_leaderboard_catch_probability(year=2024)` | 102 rows; **not** a bucket-rate table -- it's a per-player 1-5 "star" difficulty breakdown (`n_fieldout_Nstars`/`n_opp_Nstars`/`n_Nstar_percent`), a different shape than this spine's own `(position, dist_b, spray_b, la_bin)` surface. Used as a secondary/soft cross-check only, never a hard gate; the surface's own internal held-out calibration is the hard check (Task 3.3). |
| `lb_sprint_speed_2024.parquet` | `mlb_statcast_leaderboard_sprint_speed(year=2024)` | 566 rows; `sprint_speed` matches this spine's expected column name |
| `lb_baserunning_rv_2024.parquet` | `mlb_statcast_leaderboard_baserunning_run_value(year=2024)` | 242 rows; `runner_runs_tot` is the oracle target |
| `lb_basestealing_rv_2024.parquet` | `mlb_statcast_leaderboard_basestealing_run_value(year=2024)` | 412 rows; `runs_stolen_on_running_act` is the oracle target |

## Known deviations from a naive literal read of the design docs

- **`events` does NOT carry `stolen_base_*`/`caught_stealing_*`/`wild_pitch`/
  `passed_ball` in the flat per-pitch `mlb_statcast_search` payload.**
  Confirmed empirically: zero such values across 116,355 pitches (a genuine
  MLB month should have several hundred SB/CS attempts and >100 WP/PB).
  Baseball Savant narrates these as a trailing clause in the terminal
  pitch's free-text `des` field, attached to whichever batter's plate
  appearance the pitch belongs to -- not tagged as an `events` value of
  their own (e.g. `"Jake Meyers strikes out swinging. Jeremy Peña to 3rd.
  Jeremy Peña steals (8) 2nd base."`). `sportsdataverse.mlb.mlb_stolen_base.sb_attempts_from_pitches`
  and `sportsdataverse.mlb.mlb_catcher_defense._block_opportunities` detect
  these via a `des` regex instead (falling back to `events` when `des` is
  absent, e.g. pre-Statcast-`des` feeds), reading the runner id off the
  pre-play occupancy column implied by the attempted base.
- **Because the narrating row's `delta_run_exp` bundles the primary batter
  outcome (the walk/strikeout) together with the SB/CS/WP/PB, it cannot
  isolate the latter's own run value.** `mlb_catcher_blocking`,
  `mlb_catcher_throwing`, and `mlb_stolen_base_value` therefore use the
  documented `RUN_VALUES` fallback constants (`"wp_pb"`, `"cs"`, `"sb"`)
  rather than `event_run_value` on these bundled rows -- a deliberate,
  documented exception to the "RE288 primary, `RUN_VALUES` fallback"
  general rule, made because the real capture proves the RE288 path is
  contaminated here, not merely absent.
- **`des`-narrated SB/CS/WP/PB volume is low relative to season norms for
  this single month.** Only 39 stolen-base + 15 caught-stealing + 13
  wild-pitch + 2 passed-ball mentions surfaced in the June capture (a full
  MLB month typically sees several hundred SB/CS attempts alone). Two
  contributing factors, both real: (1) `des` itself is populated only on
  the ~30,000 PA-terminal pitches (74% of all pitches carry a null `des`),
  and (2) the Savant leaderboards this spine gates against
  (`lb_catcher_framing_2024`, `lb_oaa_2024`, etc.) are **full-season**
  aggregates, while the pitch/BIP fixtures here are **one month** -- a
  month-vs-season scope mismatch that widens with metrics driven by rarer
  events (SB/CS attempts, dirt pitches) more than with high-volume ones
  (called-strike takes, batted balls). See each oracle test's docstring for
  the gate floor actually observed at this capture scope, and the
  month-vs-season caveat where it applies.
- **`polars.Expr.arctan2` does not exist in the installed polars version**
  (`polars>=1.0,<2.0`, resolves to 1.42) -- the top-level function
  `pl.arctan2(y, x)` is the correct call, not an `Expr` method chain.
- **`Expr.replace(..., default=...)` is deprecated** in favor of
  `Expr.replace_strict(mapping, default=..., return_dtype=...)`.
