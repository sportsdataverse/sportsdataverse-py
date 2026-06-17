<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL PBP ETL ↔ nflfastR Alignment — Implementation Plan](#nfl-pbp-etl-%E2%86%94-nflfastr-alignment--implementation-plan)
  - [Global Constraints](#global-constraints)
    - [Task 1: Pin the shared column contract + centralize constants](#task-1-pin-the-shared-column-contract--centralize-constants)
    - [Task 2: `calculate_epa(df)` in ep_wp.py (lift the EPA derivation)](#task-2-calculate_epadf-in-ep_wppy-lift-the-epa-derivation)
    - [Task 3: `calculate_wpa(df)` in ep_wp.py (lift WP-end/start + WPA)](#task-3-calculate_wpadf-in-ep_wppy-lift-wp-endstart--wpa)
    - [Task 4: `enrich_nfl_pbp(frame)` orchestrator](#task-4-enrich_nfl_pbpframe-orchestrator)
    - [Task 5: Efficiency in the scorers](#task-5-efficiency-in-the-scorers)
    - [Task 6: Gut `NFLPlayProcess` model steps → delegate to ep_wp](#task-6-gut-nflplayprocess-model-steps-%E2%86%92-delegate-to-ep_wp)
    - [Task 7: Add missing nflfastR construction methods (fixed_drives + series)](#task-7-add-missing-nflfastr-construction-methods-fixed_drives--series)
    - [Task 8: Per-game cache + `build_nfl_season` compile helper](#task-8-per-game-cache--build_nfl_season-compile-helper)
    - [Task 9: Pin models + docs + final regression](#task-9-pin-models--docs--final-regression)
  - [Sequencing](#sequencing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL PBP ETL ↔ nflfastR Alignment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes.

**Goal:** Collapse sdv-py's split/triplicated NFL EPA/WPA logic into a single faithful
model-application layer (`ep_wp.py`) that any construction source feeds, add the missing nflfastR
methods, and make per-game build + season compile efficient. Spec:
`docs/superpowers/specs/2026-06-17-nfl-pbp-etl-nflfastr-alignment-design.md`.

**Architecture:** 3 layers — construction (NFLPlayProcess/native_pbp/load_nfl_pbp → common frame),
model-application (`ep_wp.enrich_nfl_pbp`), training (track6). Full convergence: `NFLPlayProcess`
model steps delegate to `ep_wp`.

## Global Constraints
- polars 1.x only (run the `polars-1x-reviewer`); type-hint new funcs + keep the mypy `files` ratchet green.
- The faithful models are the published `nfl_model_artifacts` (EP 18 / WP-spread 12 / WP-naive 11 / CP 18-feat); load via `ep_wp._load_model`.
- **Regression = the track6 parity gate (EP corr ≥0.98, WP Brier ≤0.20, on the model domain) + a play-level diff vs nflfastR's `ep`/`epa`/`wp`/`wpa`/`cp`/`cpoe` (float tolerance) + cross-source (ESPN vs Shield) equality.** Run after T2–T7.
- **Do NOT break `NFLPlayProcess` `return_keys` dict contract** (offline-reprocess pipeline depends on it) — add a contract test.
- No AI co-author/footer. Conventional commits. Branch `feat/nfl-etl-nflfastr-alignment`.

---

### Task 1: Pin the shared column contract + centralize constants
**Files:** `sportsdataverse/nfl/model_vars.py`; test `tests/nfl/test_nfl_contract.py`.
- [ ] Define `NFLVERSE_FRAME_CONTRACT` — the minimal columns `enrich_nfl_pbp` requires: EP/WP/CP feature inputs (`half_seconds_remaining, yardline_100, home, retractable, dome, outdoors, ydstogo, down, era*, *_timeouts_remaining, season, posteam, defteam, …`) + EPA-derivation inputs (`game_id, game_half, play_id/sequence, play_type, sp, touchdown, td_team, field_goal_result, safety, two_point*, extra_point*, kickoff_attempt, qb_kneel, timeout, penalty, change_of_pos_team, posteam_score/defteam_score, score_differential, spread_line, qtr, *_wp inputs`). Document each source's column → contract mapping (ESPN `start./end.` adapters already in `_espn_*_features`; Shield/native_pbp underscore names; nflverse parquet native).
- [ ] Move shared constants into `model_vars.py`: `_EP_POINT_VALUES` (from ep_wp:106), the era bins (cuts 2001/2005/2013/2017 → era0..4, matching nfl-data's fixed boundaries), the **kickoff-touchback yardline `80 pre-2016 / 75 post-2016`** (nflfastR canonical — replaces the inline 2014 boundary at nfl_pbp.py:3367), and the `spread_time` formula constant (`exp(-4*elapsed_share)`).
- [ ] Test: assert the contract column set is stable; assert era/touchback constants match the track6 training assumption.
- [ ] Commit.

### Task 2: `calculate_epa(df)` in ep_wp.py (lift the EPA derivation)
**Files:** `sportsdataverse/nfl/ep_wp.py`; test `tests/nfl/test_ep_wp_epa.py`.
**Interfaces:** Produces `calculate_epa(df: pl.DataFrame) -> pl.DataFrame` adding `ep`/`epa` (+ `ep_start`/`ep_end` if not present). Consumed by `enrich_nfl_pbp` (T4) and `NFLPlayProcess.__process_epa` (T6).
- [ ] **Step 1 (test first):** synthetic 1-game frame (TD drive, FG, turnover, kickoff, half-end, penalty) → assert `epa` matches hand-computed nflfastR values (lead-difference + overlays).
- [ ] **Step 2:** implement — lift verbatim from `NFLPlayProcess.__process_epa` (nfl_pbp.py:3348–3796): build EP via `calculate_expected_points` (or accept `ep` present); `lag_EP_end = EP_end.shift(1).over("game_id")`; `EP_between` sign-flip on `change_of_pos_team`; kickoff `EP_start = EP_start_touchback`; half-end `EPA = -EP_start`; timeout `EPA=0`; penalty `EPA = EP_end - EP_start + EP_between`; turnover/onside `EP_end*-1`; scoring overlays (`points_value - ep`). **Group all shift/lead by `game_id`** so concatenated frames don't leak across games.
- [ ] **Step 3:** test passes; add a within-`game_id` no-leak test (2-game concat).
- [ ] Commit.

### Task 3: `calculate_wpa(df)` in ep_wp.py (lift WP-end/start + WPA)
**Files:** `ep_wp.py`; test `tests/nfl/test_ep_wp_wpa.py`.
**Interfaces:** `calculate_wpa(df) -> pl.DataFrame` adding `wp`/`def_wp`/`home_wp`/`away_wp`/`wpa` (first-class). Lift from `__process_wpa` (nfl_pbp.py:3842–3970): `start.ExpScoreDiff*`, predict `wp_spread.ubj` for touchback/start/end, `WPA = lead(home_wp) - home_wp` with posteam→home flip, OT two-path. Group by `game_id`.
- [ ] Step 1 (test): synthetic frame → assert `wpa` + team-flip sign.
- [ ] Step 2: implement (lift). Step 3: test + no-leak. Commit.

### Task 4: `enrich_nfl_pbp(frame)` orchestrator
**Files:** `ep_wp.py`; test `tests/nfl/test_enrich.py`.
**Interfaces:** `enrich_nfl_pbp(df, *, models_dir=None) -> pl.DataFrame` runs, in nflfastR order: `calculate_expected_points → calculate_epa → calculate_win_probability → calculate_wpa → calculate_completion_probability (+cpoe) → calculate_xyac`. Asserts the `NFLVERSE_FRAME_CONTRACT` inputs present (clear error if not). Source-agnostic.
- [ ] Step 1 (test): a fixture nflverse-shape frame → `enrich` adds all of `ep,epa,wp,def_wp,home_wp,wpa,cp,cpoe,xyac_*`; idempotent.
- [ ] Step 2: implement. Step 3: test. Commit.

### Task 5: Efficiency in the scorers
**Files:** `ep_wp.py`.
- [ ] Gate `wp_naive` predict to the null-`spread_line` subset (calculate_win_probability ~771–785) instead of predicting both for every row.
- [ ] Build the touchback EP matrix only for `kickoff_attempt==1` rows.
- [ ] Replace `tolist()` round-trips with `pl.from_numpy`/Series-from-ndarray on model outputs.
- [ ] Verify parity unchanged (T-regression) + a micro-benchmark note. Commit.

### Task 6: Gut `NFLPlayProcess` model steps → delegate to ep_wp
**Files:** `sportsdataverse/nfl/nfl_pbp.py`; test `tests/nfl/test_nflplayprocess_contract.py`.
- [ ] Split: `run_cleaning_pipeline()` runs construction through `__add_spread_time` and returns the cleaned frame (the common contract). `run_processing_pipeline()` = `run_cleaning_pipeline()` then `enrich_nfl_pbp`.
- [ ] Replace `__process_epa/__process_wpa/__process_cp/__process_xyac` bodies with thin wrappers: build the ESPN feature columns (via `_espn_*_features`) then call `ep_wp.calculate_*`/`enrich`. Delete the now-duplicated inline EPA/WPA math.
- [ ] **Preserve `return_keys` dict shape** — contract test: `NFLPlayProcess(gameId=…, path_to_json=fixture).run_processing_pipeline()` returns the same top-level keys + the `plays` frame has `ep,epa,wp,wpa,cp,cpoe` (now via the shared path).
- [ ] Run the play-level parity diff (ESPN game vs nflfastR) — confirm EPA/WP match within tolerance. Commit.

### Task 7: Add missing nflfastR construction methods (fixed_drives + series)
**Files:** `sportsdataverse/nfl/nfl_pbp.py` (or a new `nfl_construct.py` shared helper); test `tests/nfl/test_fixed_drives_series.py`.
- [ ] Port `add_fixed_drives` (posteam-change detection + PAT-after-def-TD override + onside/safety forcing) → `fixed_drive`/`fixed_drive_result`; replace the ESPN-inherited drive reliance.
- [ ] Port `add_series_data` → `series`/`series_success`/`series_result`.
- [ ] Tests vs a fixture game's nflverse `fixed_drive`/`series_success`. Commit.

### Task 8: Per-game cache + `build_nfl_season` compile helper
**Files:** `sportsdataverse/nfl/cache.py` (extend), new `sportsdataverse/nfl/nfl_build.py`; tests.
- [ ] Per-game parquet cache keyed `gameId + pipeline_version` (reuse the `cache.py` `_key`/`cached_loader` pattern + `NflConfig.cache_mode`); a re-run of an unchanged game skips fetch+predict.
- [ ] `build_nfl_season(game_ids, *, source='espn'|'shield'|'nflverse') -> pl.DataFrame`: per-game construct+enrich (cache-aware), one `schedule_lookup` join (roof/spread_line via the `odds_override` pattern — no per-game odds fetch), `concat(diagonal_relaxed)`. Parity with `build_nflfastR_pbp`.
- [ ] Tests: cache hit skips compute; `build_nfl_season` over 2 fixture games concats correctly. Commit.

### Task 9: Pin models + docs + final regression
**Files:** `sportsdataverse/nfl/model_vars.py`, `CLAUDE.md`, `CONTRIBUTING.md`.
- [ ] Pin `nfl/models/*.ubj` to the published faithful `nfl_model_artifacts` (note the version/source).
- [ ] Document the canonical flow (ep_wp owns model application + EPA/WPA; `*_pbp.py` never re-adds EPA inline; QBR is an sdv-extra applied after `enrich`, outside the nflfastR-parity contract).
- [ ] Full regression: track6 parity gate + play-level nflfastR diff + cross-source (ESPN vs Shield via native_pbp output) equality + `uv run pytest tests/nfl` + polars-1x-reviewer + mypy ratchet.
- [ ] PR.

## Sequencing
T1 → (T2, T3 parallel) → T4 → T5 → T6 → T7 → T8 → T9. T2/T3 are the load-bearing lifts; T6 is the riskiest (contract). Each task ends green + committed; the parity diff runs from T4 on.
