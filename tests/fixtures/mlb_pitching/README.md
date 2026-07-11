<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MLB pitching-spine (T6.1) real-capture fixtures](#mlb-pitching-spine-t61-real-capture-fixtures)
  - [`pitches_2024-06-15.parquet`](#pitches_2024-06-15parquet)
  - [`savant_pitch_arsenal_stats_2024.parquet`](#savant_pitch_arsenal_stats_2024parquet)
  - [`savant_expected_stats_2024.parquet`](#savant_expected_stats_2024parquet)
  - [`pitcher_season_pitches_2023_sample.parquet`](#pitcher_season_pitches_2023_sampleparquet)
  - [Deferred (not captured): `stuff_plus_leaderboard_sample_2024.parquet`](#deferred-not-captured-stuff_plus_leaderboard_sample_2024parquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MLB pitching-spine (T6.1) real-capture fixtures

All fixtures below are **real Baseball Savant captures** (via the shipped `mlb_statcast_*`
wrappers), captured **2026-07-10**, ids pinned `Int64` at capture time. No synthetic data.

## `pitches_2024-06-15.parquet`

- Source: `mlb_statcast_search("2024-06-15", "2024-06-15", player_type="pitcher")`
- Rows: 4,145 (one full day of MLB pitcher-perspective pitches).
- Columns: raw Savant search columns (snake_cased), including `pitcher`, `batter`, `game_pk`,
  `game_date`, `pitch_type`, `release_speed`, `release_spin_rate`, `pfx_x`, `pfx_z`,
  `release_pos_x`, `release_pos_z`, `release_extension`, `plate_x`, `plate_z`, `sz_top`, `sz_bot`,
  `balls`, `strikes`, `stand`, `p_throws`, `description`, `events`,
  `estimated_woba_using_speedangle`, `delta_run_exp`, `at_bat_number`, `pitch_number`, `inning`.

## `savant_pitch_arsenal_stats_2024.parquet`

- Source: `mlb_statcast_leaderboard_pitch_arsenal_stats(year=2024)`.
- Rows: 534 (pitcher x pitch-type rows, full 2024 season).
- Savant ships the pitcher id as `player_id`; **renamed to `pitcher`** (Int64) at capture time so
  every oracle join in this spine uses one consistent key name. Retains Savant's native
  `run_value` / `run_value_per_100` / `pitch_type` columns — the run-value oracle for Stuff+ (①).

## `savant_expected_stats_2024.parquet`

- Source: `mlb_statcast_leaderboard_expected_stats(type="pitcher", year=2024)`.
  **`type="pitcher"` is required** — the default (`type=None`) returns the *batter*
  expected-stats shape, which has no ERA/xERA columns at all.
- Rows: 366 (qualified pitcher-seasons, 2024).
- Renamed at capture time: Savant's `player_id` -> `pitcher`, `xera` -> `x_era`,
  `est_woba` -> `x_woba`. Retains `era` verbatim — the xERA oracle for model ③.

## `pitcher_season_pitches_2023_sample.parquet`

- Source: `mlb_statcast_search("2023-03-30", "2023-10-01", player_type="pitcher",
  pitchers_lookup=[<5 real ids>])`.
- Pitcher ids were **discovered from a live one-week probe** (2023-06-01..06-07, top-5 by pitch
  count), not hand-guessed — verified via `statsapi.mlb.com/api/v1/people/{id}` to be real MLBAM
  ids: Kevin Gausman (592332), Jack Flaherty (656427), Charlie Morton (450203), Max Scherzer
  (453286), Corbin Burnes (669203).
- Rows: 14,120 pitches across the full 2023 season for those 5 pitchers.
- Used by Phase 4 (SIERA-like OLS fit input via a season-prior pull), Phase 5 (TTO/fatigue fit),
  and Phase 8 (injury-risk trailing-window features + leakage tests).

## Deferred (not captured): `stuff_plus_leaderboard_sample_2024.parquet`

The plan called for a small hand-entered sample from a **public published Stuff+ leaderboard**
(FanGraphs / PitchingBot) to rank-correlate against as a third Stuff+ oracle leg. FanGraphs'
pitch-modeling leaderboard is Cloudflare-challenge-gated and JS-rendered (confirmed via `curl` ->
`Just a moment...` challenge page, and via `WebFetch` -> "No data is available" on the rendered
shell); no other public source surfaced verifiable real Stuff+ numbers in plain text within this
session. Rather than fabricate a "published leaderboard sample" (which would violate the repo's
"validate against real captures, never synthetic" rule under a misleading label), **this fixture
and the corresponding third oracle leg (Task 2.4 §b, Spearman vs published rank) are deferred**.
The Stuff+ gate ships on the two fully-real legs: (a) internal calibration (mean = 100) and
(b) Spearman vs the real `savant_pitch_arsenal_stats_2024.parquet` run-value column. Revisit if a
scriptable, non-JS public Stuff+ source becomes available.
