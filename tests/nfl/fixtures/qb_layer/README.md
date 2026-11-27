<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [QB-layer test slice (nflverse 2025, regular season weeks 1-6)](#qb-layer-test-slice-nflverse-2025-regular-season-weeks-1-6)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# QB-layer test slice (nflverse 2025, regular season weeks 1-6)

Real rows, trimmed to the columns `nfl_qb_layer` reads. Rebuilt by `record_qb_layer_2025_slice.py` (this
directory) through the package loaders on 2026-11-26:

| file | loader | rows |
|---|---|---|
| `schedule_2025_w1_6.parquet` | `load_nfl_schedule([2025])`, REG weeks 1-6 | 93 |
| `pbp_2025_w1_6.parquet` | `load_nfl_pbp([2025])`, REG weeks 1-6, 17 columns | 16,011 |
| `injuries_2025_w1_6.parquet` | `load_nfl_injuries([2025])`, QB rows, weeks 1-6 | 54 |
| `depth_charts_2025_w1_6.parquet` | `load_nfl_depth_charts([2025])`, QB rows, `dt` 2025-08-20 to the last week-6 date | 4,941 |

Cases the tests read: CIN's chart moving QB1 from Burrow to Browning (2025-09-17) and to Flacco (2025-10-08); MIN's
McCarthy listed QB1 while Out in weeks 3-5, with Wentz (QB2) starting.
