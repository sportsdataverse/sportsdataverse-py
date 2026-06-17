<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL PBP ETL ↔ nflfastR Alignment — Design Spec](#nfl-pbp-etl-%E2%86%94-nflfastr-alignment--design-spec)
  - [1. Goal](#1-goal)
  - [2. Problem (from the comparison)](#2-problem-from-the-comparison)
  - [3. Decisions (locked — from maintainer)](#3-decisions-locked--from-maintainer)
  - [4. Architecture (end-state, 3 layers)](#4-architecture-end-state-3-layers)
  - [5. Workstreams](#5-workstreams)
  - [6. Verification / regression](#6-verification--regression)
  - [7. Risks](#7-risks)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL PBP ETL ↔ nflfastR Alignment — Design Spec

- **Date:** 2026-06-17
- **Author:** Saiem Gilani
- **Status:** Draft (pending review)
- **Repo:** `sportsdataverse-py` (flagship library)
- **Basis:** the 6-agent comparison workflow (`wf_68245519-c24`) mapping nflfastR (R) vs sdv-py
  `NFLPlayProcess` + `ep_wp.py` vs nfl-data `native_pbp`, verified against the nflfastR source
  (`nflverse-dev/nflfastR/R/*`, `fastrmodels MODELS.R`).

## 1. Goal

Bring sdv-py's NFL play-by-play ETL **in line with nflfastR's methods** and make **individual
per-game building + season compiling efficient**, by collapsing the currently split/triplicated
EPA/WPA logic into a single faithful model-application layer that any construction source feeds.

## 2. Problem (from the comparison)

- **EPA/WPA is split + triplicated.** The nflfastR-style lead/lag derivation (scoring overlays,
  half-end `−ep`, penalty `EP_between`, kickoff-touchback swap, OT WP paths) lives **only inline in
  `NFLPlayProcess.__process_epa`/`__process_wpa`** (nfl_pbp.py:3348–3970). `ep_wp.py` is a **bare EP
  scorer** (`calculate_expected_points` = predict→dot→clip, no EPA), and the same math is re-derived
  in track6 `label.py`. There is **no shared `calculate_epa()/calculate_wpa()`**.
- **Duplication.** `spread_time = spread·exp(−4·elapsed_share)` computed 3×; era/roof/down one-hots
  restated in `_make_model_mutations`, `_make_cp_mutations`, `_espn_ep_features`, `_espn_cp_features`,
  `_espn_xyac_features` + inline in nfl_pbp.py; kickoff-touchback constant restated.
- **Method gaps vs nflfastR.** ESPN path *inherits* ESPN drive boundaries (no `add_fixed_drives`
  reconstruction) and has **no `series_success`/`series_result`**; WPA is not a first-class column on
  the non-ESPN paths; kickoff-touchback yardline uses a **2014** boundary vs nflfastR's **2016**.
- **Efficiency.** `NFLPlayProcess` has **zero caching** (≤3 ESPN network hops/game; full re-fetch on
  re-run), no built-in season compile/parallelism; `calculate_win_probability` always predicts BOTH
  wp models; the touchback EP matrix is built for ALL rows; `tolist()` round-trips on every predict.

## 3. Decisions (locked — from maintainer)

| # | Decision | Choice |
|---|---|---|
| D1 | Canonical source | **Both** — each construction source emits a common nflverse-shape frame; a single `ep_wp.enrich_nfl_pbp()` applies models so EPA/WP are identical regardless of source. |
| D2 | Scope | **Full convergence** — shared EPA/WPA + `enrich` + de-triplication + cache + compile helper + centralized constants + the missing nflfastR features (fixed_drives reconstruction, series, first-class WPA) + **gut `NFLPlayProcess`'s model steps** to delegate to `enrich`. |
| D3 | Execution | Spec → plan → **subagent-driven development**, with the track6 parity gate + a play-level nflfastR diff as the regression check. |

Remaining (resolve in the plan, not blocking): touchback boundary reconciled to **80 pre-2016 / 75
post-2016** (nflfastR canonical; NFLPlayProcess's 2014 is the bug); **QBR** stays an sdv-py-only extra
applied *after* `enrich` (nflfastR has no QBR — outside the parity contract); `native_pbp` long-term
home (its own repo per [[native-pipeline-separate-repo]]) — sdv-py's `enrich` layer imports nothing
from it; native_pbp's output schema is the contract.

## 4. Architecture (end-state, 3 layers)

```
CONSTRUCTION (source-specific, MODEL-FREE → common nflverse-shape frame)
  NFLPlayProcess.run_cleaning_pipeline()   ESPN summary  (nfl_pbp.py)
  native_pbp.build_pbp / build_season      Shield JSON   (nfl-data; output = schema contract)
  load_nfl_pbp([seasons])                  nflverse parquet (already enriched; fast batch)
                                   │  (all emit the same column contract)
                                   ▼
MODEL-APPLICATION (single owner = sportsdataverse/nfl/ep_wp.py)
  feature contracts + _EP_POINT_VALUES + ESPN→nflfastR adapters (_espn_*_features)
  scorers: calculate_expected_points / _win_probability / _completion_probability / _xyac
  NEW shared: calculate_epa(df) · calculate_wpa(df)   (lifted from __process_epa/__process_wpa)
  enrich_nfl_pbp(frame): EP → EPA → WP → WPA → CP → CPOE → xYAC  (nflfastR order)
  loads the faithful track6 nfl_model_artifacts (.ubj) via _load_model
                                   │
                                   ▼
TRAINING (nfl-data track6_nfl_ep_wp) — consumes load_nfl_pbp OR native_pbp → produces the .ubj.
  One-directional: training is never imported by the sdv-py runtime.
```

- **Per-game build:** construct (ESPN or Shield) → `enrich_nfl_pbp(frame)` → cache parquet by
  `gameId + pipeline_version`.
- **Season compile:** `build_nfl_season(game_ids, *, source=)` loops per-game build+enrich
  (cache-aware), one `schedule_lookup` join (roof/spread_line — the proven `odds_override`/native
  pattern, no per-game odds fetch), `concat(diagonal_relaxed)`. Parity with `build_nflfastR_pbp`.

## 5. Workstreams

1. **Shared EPA/WPA in `ep_wp.py`.** Add `calculate_epa(df)` + `calculate_wpa(df)`, lifting the
   lead/lag + scoring-overlay + half-end + penalty(`EP_between`) + kickoff-touchback + turnover/onside
   logic verbatim from `NFLPlayProcess.__process_epa` (nfl_pbp.py:3643–3688) and the WP-end/start +
   team-flip from `__process_wpa` (3842–3970). Group-by-game `shift` so concatenated frames are safe.
2. **`enrich_nfl_pbp(frame)` orchestrator.** EP → EPA → WP → WPA → CP → CPOE → xYAC in nflfastR order;
   loads the faithful `nfl_model_artifacts`. Works on any nflverse-shape frame (ESPN-cleaned, Shield,
   or parquet).
3. **Gut `NFLPlayProcess` model steps.** Split the pipeline: `run_cleaning_pipeline()` stops at
   `__add_spread_time` (emits the cleaned frame); `__process_epa/wpa/cp/xyac` become thin wrappers
   that build the ESPN feature matrices then call `ep_wp.calculate_*`/`enrich`. **Preserve the
   `return_keys` dict contract** the offline-reprocess pipeline depends on (`plays`, `boxscore`, …).
4. **Missing nflfastR features.** Port `add_fixed_drives` (posteam-change detection + PAT-after-def-TD
   override + onside/safety forcing) and `add_series_data` (`series_success`/`series_result`) to a
   shared construction helper used by the ESPN path; emit **WPA as a first-class column** via
   `calculate_wpa`.
5. **Centralize constants** in `model_vars.py`: era bins, kickoff-touchback yardline (80 pre-2016 /
   75 post-2016), `_EP_POINT_VALUES`, the `spread_time` formula — imported everywhere; delete the
   inline restatements (nfl_pbp.py:3367, the 5+ `_make_*`/`_espn_*` copies).
6. **Efficiency.** Per-game parquet cache (`cache.py` `@cached_loader` pattern, key = `gameId +
   pipeline_version`); gate the `wp_naive` predict to null-`spread_line` rows; build the touchback EP
   matrix only for kickoff rows; replace `tolist()` round-trips with `pl.from_numpy`/Series-from-ndarray.
7. **Compile helper** `build_nfl_season(game_ids, *, source='espn'|'shield'|'nflverse')`.
8. **Pin + document.** Pin sdv-py `nfl/models/*.ubj` to the published faithful `nfl_model_artifacts`;
   document the canonical flow in CLAUDE.md/CONTRIBUTING (ep_wp owns model application + EPA/WPA;
   `*_pbp.py` never re-adds EPA inline).

## 6. Verification / regression

- **Parity gate (primary):** the track6 gate — EP corr ≥ 0.98 vs nflfastR `ep`, WP Brier ≤ 0.20 — on
  the model's domain (valid down + non-null yardline/timeouts), every era.
- **Play-level diff:** on a sample of games, `enrich_nfl_pbp(load_nfl_pbp(...))` vs nflfastR's shipped
  `ep`/`epa`/`wp`/`wpa`/`cp`/`cpoe` columns within a documented float tolerance (EPA is lead-difference
  → tolerance for boundary plays).
- **Cross-source equality:** ESPN-constructed vs Shield-constructed (native_pbp) → identical EPA/WP
  after `enrich` on the shared columns.
- **Contract:** existing `tests/nfl/` + the `NFLPlayProcess` `return_keys` dict unchanged; VCR
  cassettes for the ESPN/Shield fetch; polars-1.x reviewer + the mypy ratchet.

## 7. Risks

- **`NFLPlayProcess` is ~3400 lines**; gutting the model steps without breaking the offline-reprocess
  `return_keys` contract is the main risk — mitigate by keeping the dict shape + adding a contract test.
- **EPA lead/lag across concatenated games**: `calculate_epa` must group-by-game (no cross-game leak)
  — the per-game→compile flow must `shift` within `game_id`.
- **Float-tolerance parity** vs nflfastR `epa` on scoring/half-end/kickoff boundary plays.
- **Cross-repo**: `native_pbp` lives in nfl-data; sdv-py must not import it — its output schema is the
  only contract.
