<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [cfb_advanced fixtures — provenance](#cfb_advanced-fixtures--provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# cfb_advanced fixtures — provenance

Oracle corpus for the CFB Advanced Efficiency & Play models
(`cfb_advanced_stats`, `cfb_field_position`, `cfb_adjusted_tempo`).
Captured **2026-07-08**, season **2021**.

> **Why 2021, not 2023:** the hosted `load_cfb_pbp` parquet
> (`cfbfastR-data/pbp/parquet/play_by_play_{Y}.parquet`) covers
> **2002–2021 only** — 2022+ assets 404 (producer gap; the successor
> `model_pbp` dataset has only 2024 so far). Escalation: cfb-data producer
> backfill. All models are validated on ≤2021 seasons.

`team_id` is **Utf8** everywhere (CFBD team ids match ESPN ids for FBS).

| file | rows | source | notes |
|---|---|---|---|
| `cfbd_advanced_2021.parquet` | 130 | `cfbfastR::cfbd_stats_season_advanced(2021, excl_garbage_time = TRUE)` joined to `cfbd_team_info(year = 2021)` for `team_id` | `avg_start_yardline` = CFBD `off_field_pos_avg_start`, yards **from own goal** (higher = better start). Includes `off_plays`/`def_plays` for the tempo gate. |
| `sp_plus_2021.parquet` | 130 | `cfbfastR::cfbd_ratings_sp(2021)` joined to `cfbd_team_info` | `sp_offense_rank`/`sp_defense_rank` from CFBD `offense_ranking`/`defense_ranking`. |
| `fp_reference.parquet` | 5 | hand-authored published EP-by-field-position anchors (net next-score EP, yardline from own goal): own-1 ≈ −0.5, own-25 ≈ 1.4, midfield ≈ 2.8, opp-25 ≈ 4.1, opp-5 ≈ 5.6 | Connelly/GameOnPaper-style reference values. |
| `pbp_slice_2021.parquet` | 137,046 | full 2021 season from the local `cfbfastR-dev/cfbfastR-data` checkout (same asset `load_cfb_pbp([2021])` serves), down-selected to 26 model columns and renamed to canonical names (see `dev/cfb_advanced/capture_oracle.py::PBP_COLS`) | zstd-compressed (~2.3 MB). |

Capture scripts: `dev/cfb_advanced/capture_oracle.R` (CFBD via
`CFBD_API_KEY` in `.Renviron`) + `dev/cfb_advanced/capture_oracle.py`
(pbp slice + fp anchors), committed with `git add -f`.
